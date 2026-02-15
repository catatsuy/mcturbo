package mcturbo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrNotFound is returned when a key does not exist.
	ErrNotFound = errors.New("memcache: not found")
	// ErrNotStored is returned when a conditional store is rejected.
	ErrNotStored = errors.New("memcache: not stored")
	// ErrClosed is returned when the client is already closed.
	ErrClosed = errors.New("memcache: client closed")
)

var errProtocol = errors.New("memcache: protocol error")

// Item represents a value returned by get operations.
type Item struct {
	Value []byte
	Flags uint32
}

// MultiError represents partial failures during GetMulti.
type MultiError struct {
	PerServer map[string]error
}

func (e *MultiError) Error() string {
	if e == nil || len(e.PerServer) == 0 {
		return "mcturbo: GetMulti partial failure"
	}
	return fmt.Sprintf("mcturbo: GetMulti partial failure: %d servers failed", len(e.PerServer))
}

func (e *MultiError) Unwrap() error {
	if e == nil || len(e.PerServer) == 0 {
		return nil
	}
	errs := make([]error, 0, len(e.PerServer))
	for _, err := range e.PerServer {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// Logger is an alias of slog.Logger used by options.
type Logger = slog.Logger

// Option configures Client behavior.
type Option func(*config) error

type config struct {
	workers         int
	backoffMin      time.Duration
	backoffMax      time.Duration
	dialTimeout     time.Duration
	maxSlots        int
	defaultDeadline time.Duration
	logger          *Logger
	readBufSize     int
	writeBufSize    int
}

func defaultConfig() config {
	return config{
		workers:      4,
		backoffMin:   50 * time.Millisecond,
		backoffMax:   1 * time.Second,
		dialTimeout:  5 * time.Second,
		readBufSize:  32 * 1024,
		writeBufSize: 32 * 1024,
	}
}

// WithWorkers sets the number of worker groups.
func WithWorkers(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return errors.New("memcache: workers must be > 0")
		}
		c.workers = n
		return nil
	}
}

// WithDialBackoff sets dial backoff bounds.
func WithDialBackoff(min, max time.Duration) Option {
	return func(c *config) error {
		if min <= 0 || max <= 0 || min > max {
			return errors.New("memcache: invalid dial backoff range")
		}
		c.backoffMin = min
		c.backoffMax = max
		return nil
	}
}

// WithMaxSlots limits concurrent operations per worker.
// n=0 means no slot limit.
func WithMaxSlots(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return errors.New("memcache: max slots must be >= 0")
		}
		c.maxSlots = n
		return nil
	}
}

// WithDefaultDeadline sets a fallback socket deadline per request.
func WithDefaultDeadline(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return errors.New("memcache: default deadline must be >= 0")
		}
		c.defaultDeadline = d
		return nil
	}
}

// WithLogger sets a slog logger for internal logs.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		c.logger = l
		return nil
	}
}

// WithBufferSize sets read and write buffer sizes in bytes.
func WithBufferSize(readSize, writeSize int) Option {
	return func(c *config) error {
		if readSize <= 0 || writeSize <= 0 {
			return errors.New("memcache: buffer sizes must be > 0")
		}
		c.readBufSize = readSize
		c.writeBufSize = writeSize
		return nil
	}
}

// Client is a memcached ASCII protocol client for a single server.
type Client struct {
	addr       string
	workers    []*workerConn
	closeCh    chan struct{}
	closed     atomic.Bool
	backoffMin time.Duration
	backoffMax time.Duration
}

// New creates a new single-server client.
func New(addr string, opts ...Option) (*Client, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, errors.New("memcache: addr is required")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	c := &Client{
		addr:       addr,
		workers:    make([]*workerConn, cfg.workers),
		closeCh:    make(chan struct{}),
		backoffMin: cfg.backoffMin,
		backoffMax: cfg.backoffMax,
	}
	for i := 0; i < cfg.workers; i++ {
		var slots chan struct{}
		if cfg.maxSlots > 0 {
			slots = make(chan struct{}, cfg.maxSlots)
		}
		c.workers[i] = &workerConn{
			id:              i,
			addr:            addr,
			logger:          cfg.logger,
			readBufSize:     cfg.readBufSize,
			writeBufSize:    cfg.writeBufSize,
			dialTimeout:     cfg.dialTimeout,
			defaultDeadline: cfg.defaultDeadline,
			maxIdle:         32,
			slots:           slots,
		}
	}
	return c, nil
}

// Get returns the item for key.
func (c *Client) Get(key string) (*Item, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	return c.doGetFast(opGet, key, 0)
}

// GetMulti fetches multiple keys using the memcached text protocol.
//
// Note: This method may return both a non-empty result and a non-nil error
// when the operation partially succeeds (e.g., some workers fail while others succeed).
//
// This method does not batch/split keys. If you need to limit request size,
// split keys on the caller side and call GetMulti multiple times.
func (c *Client) GetMulti(ctx context.Context, keys []string) (map[string]*Item, error) {
	out := make(map[string]*Item, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	if ctx == nil {
		return out, errors.New("memcache: nil context")
	}
	if c.closed.Load() {
		return out, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			return out, err
		}
	}

	byWorker := make(map[int][]string, len(c.workers))
	for _, key := range keys {
		w := c.pickWorker(key)
		byWorker[w.id] = append(byWorker[w.id], key)
	}

	type getMultiResult struct {
		addr  string
		items map[string]*Item
		err   error
	}
	ch := make(chan getMultiResult, len(byWorker))
	for wid, groupedKeys := range byWorker {
		w := c.workers[wid]
		ks := append([]string(nil), groupedKeys...)
		go func(w *workerConn, keys []string) {
			items, err := w.getMultiWithContext(ctx, keys)
			ch <- getMultiResult{addr: w.addr, items: items, err: err}
		}(w, ks)
	}

	var merr *MultiError
	for range byWorker {
		r := <-ch
		if r.err != nil {
			if merr == nil {
				merr = &MultiError{PerServer: map[string]error{}}
			}
			if _, exists := merr.PerServer[r.addr]; !exists {
				merr.PerServer[r.addr] = r.err
			}
			continue
		}
		maps.Copy(out, r.items)
	}
	if merr != nil {
		return out, merr
	}
	return out, nil
}

// Set stores value for key with flags and ttlSeconds.
func (c *Client) Set(key string, value []byte, flags uint32, ttlSeconds int) error {
	if err := validateStoreInput(key, ttlSeconds); err != nil {
		return err
	}
	_, err := c.doFast(opSet, key, value, flags, ttlSeconds, 0)
	return err
}

// Add stores value for key only if key does not exist.
func (c *Client) Add(key string, value []byte, flags uint32, ttlSeconds int) error {
	if err := validateStoreInput(key, ttlSeconds); err != nil {
		return err
	}
	_, err := c.doFast(opAdd, key, value, flags, ttlSeconds, 0)
	return err
}

// Replace stores value for key only if key already exists.
func (c *Client) Replace(key string, value []byte, flags uint32, ttlSeconds int) error {
	if err := validateStoreInput(key, ttlSeconds); err != nil {
		return err
	}
	_, err := c.doFast(opReplace, key, value, flags, ttlSeconds, 0)
	return err
}

// Append appends value to existing value for key.
func (c *Client) Append(key string, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	_, err := c.doFast(opAppend, key, value, 0, 0, 0)
	return err
}

// Prepend prepends value to existing value for key.
func (c *Client) Prepend(key string, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	_, err := c.doFast(opPrepend, key, value, 0, 0, 0)
	return err
}

// Delete removes key.
func (c *Client) Delete(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	_, err := c.doFast(opDelete, key, nil, 0, 0, 0)
	return err
}

// Touch updates key expiration to ttlSeconds.
func (c *Client) Touch(key string, ttlSeconds int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		return errors.New("memcache: ttlSeconds must be >= 0")
	}
	_, err := c.doFast(opTouch, key, nil, 0, ttlSeconds, 0)
	return err
}

// GetAndTouch gets key and updates key expiration to ttlSeconds.
func (c *Client) GetAndTouch(key string, ttlSeconds int) (*Item, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if ttlSeconds < 0 {
		return nil, errors.New("memcache: ttlSeconds must be >= 0")
	}
	return c.doGetFast(opGetAndTouch, key, ttlSeconds)
}

// Incr increments a numeric value by delta and returns the new value.
func (c *Client) Incr(key string, delta uint64) (uint64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	v, err := c.doFast(opIncr, key, nil, 0, 0, delta)
	if err != nil {
		return 0, err
	}
	return parseCounterValue(v)
}

// Decr decrements a numeric value by delta and returns the new value.
func (c *Client) Decr(key string, delta uint64) (uint64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	v, err := c.doFast(opDecr, key, nil, 0, 0, delta)
	if err != nil {
		return 0, err
	}
	return parseCounterValue(v)
}

// FlushAll removes all keys from the server.
func (c *Client) FlushAll() error {
	_, err := c.doFastNoKey(opFlushAll)
	return err
}

// Ping checks server availability using the version command.
func (c *Client) Ping() error {
	_, err := c.doFastNoKey(opVersion)
	return err
}

// GetWithContext returns the item for key using ctx for cancellation/deadline.
func (c *Client) GetWithContext(ctx context.Context, key string) (*Item, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("memcache: nil context")
	}
	return c.doGetWithContext(ctx, opGet, key, 0)
}

// SetWithContext stores value for key with flags and ttlSeconds using ctx.
func (c *Client) SetWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	if err := validateStoreInput(key, ttlSeconds); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opSet, key, value, flags, ttlSeconds, 0)
	return err
}

// AddWithContext stores value for key only if key does not exist.
func (c *Client) AddWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	if err := validateStoreInput(key, ttlSeconds); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opAdd, key, value, flags, ttlSeconds, 0)
	return err
}

// ReplaceWithContext stores value for key only if key already exists.
func (c *Client) ReplaceWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	if err := validateStoreInput(key, ttlSeconds); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opReplace, key, value, flags, ttlSeconds, 0)
	return err
}

// AppendWithContext appends value to existing value for key.
func (c *Client) AppendWithContext(ctx context.Context, key string, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opAppend, key, value, 0, 0, 0)
	return err
}

// PrependWithContext prepends value to existing value for key.
func (c *Client) PrependWithContext(ctx context.Context, key string, value []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opPrepend, key, value, 0, 0, 0)
	return err
}

// DeleteWithContext removes key using ctx.
func (c *Client) DeleteWithContext(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opDelete, key, nil, 0, 0, 0)
	return err
}

// TouchWithContext updates key expiration using ctx.
func (c *Client) TouchWithContext(ctx context.Context, key string, ttlSeconds int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		return errors.New("memcache: ttlSeconds must be >= 0")
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opTouch, key, nil, 0, ttlSeconds, 0)
	return err
}

// GetAndTouchWithContext gets key and updates key expiration to ttlSeconds.
func (c *Client) GetAndTouchWithContext(ctx context.Context, key string, ttlSeconds int) (*Item, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if ttlSeconds < 0 {
		return nil, errors.New("memcache: ttlSeconds must be >= 0")
	}
	if ctx == nil {
		return nil, errors.New("memcache: nil context")
	}
	return c.doGetWithContext(ctx, opGetAndTouch, key, ttlSeconds)
}

// IncrWithContext increments a numeric value by delta and returns the new value.
func (c *Client) IncrWithContext(ctx context.Context, key string, delta uint64) (uint64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if ctx == nil {
		return 0, errors.New("memcache: nil context")
	}
	v, err := c.doWithContext(ctx, opIncr, key, nil, 0, 0, delta)
	if err != nil {
		return 0, err
	}
	return parseCounterValue(v)
}

// DecrWithContext decrements a numeric value by delta and returns the new value.
func (c *Client) DecrWithContext(ctx context.Context, key string, delta uint64) (uint64, error) {
	if err := validateKey(key); err != nil {
		return 0, err
	}
	if ctx == nil {
		return 0, errors.New("memcache: nil context")
	}
	v, err := c.doWithContext(ctx, opDecr, key, nil, 0, 0, delta)
	if err != nil {
		return 0, err
	}
	return parseCounterValue(v)
}

// FlushAllWithContext removes all keys from the server using ctx.
func (c *Client) FlushAllWithContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContextNoKey(ctx, opFlushAll)
	return err
}

// PingWithContext checks server availability using ctx.
func (c *Client) PingWithContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContextNoKey(ctx, opVersion)
	return err
}

// Close closes the client and all internal connections.
func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.closeCh)
	for _, w := range c.workers {
		w.close()
	}
	return nil
}

func (c *Client) doFast(op opType, key string, value []byte, flags uint32, ttl int, delta uint64) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	w := c.pickWorker(key)
	return w.roundTripFast(request{op: op, key: key, value: value, flags: flags, ttl: ttl, delta: delta})
}

func (c *Client) doGetFast(op opType, key string, ttl int) (*Item, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	w := c.pickWorker(key)
	it, err := w.roundTripGetFast(request{op: op, key: key, ttl: ttl})
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (c *Client) doFastNoKey(op opType) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if len(c.workers) == 0 {
		return nil, ErrClosed
	}
	return c.workers[0].roundTripFast(request{op: op})
}

func (c *Client) doWithContext(ctx context.Context, op opType, key string, value []byte, flags uint32, ttl int, delta uint64) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := c.pickWorker(key)
	v, err := w.roundTripWithContext(ctx, request{op: op, key: key, value: value, flags: flags, ttl: ttl, delta: delta})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) doGetWithContext(ctx context.Context, op opType, key string, ttl int) (*Item, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := c.pickWorker(key)
	it, err := w.roundTripGetWithContext(ctx, request{op: op, key: key, ttl: ttl})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return it, nil
}

func (c *Client) doWithContextNoKey(ctx context.Context, op opType) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(c.workers) == 0 {
		return nil, ErrClosed
	}
	v, err := c.workers[0].roundTripWithContext(ctx, request{op: op})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Client) pickWorker(key string) *workerConn {
	if len(c.workers) == 1 {
		return c.workers[0]
	}
	n := len(c.workers)
	klen := len(key)
	h := uint32(klen)
	if klen > 0 {
		h = h*33 + uint32(key[0])
		h = h*33 + uint32(key[klen-1])
		h = h*33 + uint32(key[klen>>1])
	}
	return c.workers[h%uint32(n)]
}

type opType uint8

const (
	opGet opType = iota + 1
	opSet
	opAdd
	opReplace
	opAppend
	opPrepend
	opDelete
	opTouch
	opGetAndTouch
	opFlushAll
	opVersion
	opIncr
	opDecr
)

type request struct {
	op    opType
	key   string
	value []byte
	flags uint32
	ttl   int
	delta uint64
}

type workerConn struct {
	id              int
	addr            string
	logger          *Logger
	readBufSize     int
	writeBufSize    int
	dialTimeout     time.Duration
	defaultDeadline time.Duration

	poolMu  sync.Mutex
	idle    []*pooledConn
	maxIdle int
	slots   chan struct{}
}

type pooledConn struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
}

func (w *workerConn) roundTripFast(req request) ([]byte, error) {
	w.acquireSlot()
	defer w.releaseSlot()

	pc, err := w.acquireConnFast()
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		w.releaseConn(pc, keep)
	}()

	deadline := time.Time{}
	if w.defaultDeadline > 0 {
		deadline = time.Now().Add(w.defaultDeadline)
	}
	if err := pc.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	if err := writeRequest(pc.bw, req); err != nil {
		return nil, err
	}
	if err := pc.bw.Flush(); err != nil {
		return nil, err
	}

	value, err := readResponse(pc.br, req)
	if err != nil {
		return nil, err
	}
	keep = true
	return value, nil
}

func (w *workerConn) roundTripWithContext(ctx context.Context, req request) ([]byte, error) {
	if err := w.acquireSlotWithContext(ctx); err != nil {
		return nil, err
	}
	defer w.releaseSlot()

	pc, err := w.acquireConnWithContext(ctx)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		w.releaseConn(pc, keep)
	}()

	deadline := time.Time{}
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else if w.defaultDeadline > 0 {
		deadline = time.Now().Add(w.defaultDeadline)
	}
	if err := pc.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	cancel := func() {}
	if ctx.Done() != nil && deadline.IsZero() {
		done := make(chan struct{})
		cancel = func() { close(done) }
		conn := pc.conn
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.SetDeadline(time.Now())
			case <-done:
			}
		}()
	}
	defer cancel()

	if err := writeRequest(pc.bw, req); err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}
	if err := pc.bw.Flush(); err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}

	value, err := readResponse(pc.br, req)
	if err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}
	keep = true
	return value, nil
}

func (w *workerConn) roundTripGetFast(req request) (*Item, error) {
	w.acquireSlot()
	defer w.releaseSlot()

	pc, err := w.acquireConnFast()
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		w.releaseConn(pc, keep)
	}()

	deadline := time.Time{}
	if w.defaultDeadline > 0 {
		deadline = time.Now().Add(w.defaultDeadline)
	}
	if err := pc.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	if err := writeRequest(pc.bw, req); err != nil {
		return nil, err
	}
	if err := pc.bw.Flush(); err != nil {
		return nil, err
	}

	item, err := readGetItemResponse(pc.br, req)
	if err != nil {
		return nil, err
	}
	keep = true
	return item, nil
}

func (w *workerConn) roundTripGetWithContext(ctx context.Context, req request) (*Item, error) {
	if err := w.acquireSlotWithContext(ctx); err != nil {
		return nil, err
	}
	defer w.releaseSlot()

	pc, err := w.acquireConnWithContext(ctx)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		w.releaseConn(pc, keep)
	}()

	deadline := time.Time{}
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else if w.defaultDeadline > 0 {
		deadline = time.Now().Add(w.defaultDeadline)
	}
	if err := pc.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	cancel := func() {}
	if ctx.Done() != nil && deadline.IsZero() {
		done := make(chan struct{})
		cancel = func() { close(done) }
		conn := pc.conn
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.SetDeadline(time.Now())
			case <-done:
			}
		}()
	}
	defer cancel()

	if err := writeRequest(pc.bw, req); err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}
	if err := pc.bw.Flush(); err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}

	item, err := readGetItemResponse(pc.br, req)
	if err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}
	keep = true
	return item, nil
}

func (w *workerConn) getMultiWithContext(ctx context.Context, keys []string) (map[string]*Item, error) {
	if err := w.acquireSlotWithContext(ctx); err != nil {
		return nil, err
	}
	defer w.releaseSlot()

	pc, err := w.acquireConnWithContext(ctx)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		w.releaseConn(pc, keep)
	}()

	deadline := time.Time{}
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	} else if w.defaultDeadline > 0 {
		deadline = time.Now().Add(w.defaultDeadline)
	}
	if err := pc.conn.SetDeadline(deadline); err != nil {
		return nil, err
	}

	cancel := func() {}
	if ctx.Done() != nil && deadline.IsZero() {
		done := make(chan struct{})
		cancel = func() { close(done) }
		conn := pc.conn
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.SetDeadline(time.Now())
			case <-done:
			}
		}()
	}
	defer cancel()

	if err := writeGetMultiRequest(pc.bw, keys); err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}
	if err := pc.bw.Flush(); err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}
	items, err := parseGetMultiResponse(pc.br)
	if err != nil {
		return nil, normalizeContextErr(ctx, deadline, err)
	}
	keep = true
	return items, nil
}

func (w *workerConn) acquireConnFast() (*pooledConn, error) {
	if pc, ok := w.popIdle(); ok {
		return pc, nil
	}
	conn, err := net.DialTimeout("tcp", w.addr, w.dialTimeout)
	if err != nil {
		return nil, err
	}
	return &pooledConn{
		conn: conn,
		br:   bufio.NewReaderSize(conn, w.readBufSize),
		bw:   bufio.NewWriterSize(conn, w.writeBufSize),
	}, nil
}

func (w *workerConn) acquireConnWithContext(ctx context.Context) (*pooledConn, error) {
	if pc, ok := w.popIdle(); ok {
		return pc, nil
	}
	dialCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		dialCtx, cancel = context.WithTimeout(ctx, w.dialTimeout)
	}
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", w.addr)
	if err != nil {
		return nil, err
	}
	return &pooledConn{
		conn: conn,
		br:   bufio.NewReaderSize(conn, w.readBufSize),
		bw:   bufio.NewWriterSize(conn, w.writeBufSize),
	}, nil
}

func (w *workerConn) popIdle() (*pooledConn, bool) {
	w.poolMu.Lock()
	defer w.poolMu.Unlock()
	n := len(w.idle)
	if n == 0 {
		return nil, false
	}
	pc := w.idle[n-1]
	w.idle[n-1] = nil
	w.idle = w.idle[:n-1]
	return pc, true
}

func (w *workerConn) acquireSlot() {
	if w.slots == nil {
		return
	}
	w.slots <- struct{}{}
}

func (w *workerConn) acquireSlotWithContext(ctx context.Context) error {
	if w.slots == nil {
		return nil
	}
	select {
	case w.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *workerConn) releaseSlot() {
	if w.slots == nil {
		return
	}
	select {
	case <-w.slots:
	default:
	}
}

func (w *workerConn) releaseConn(pc *pooledConn, keep bool) {
	if pc == nil {
		return
	}
	if !keep {
		_ = pc.conn.Close()
		return
	}

	w.poolMu.Lock()
	if w.maxIdle <= 0 {
		w.maxIdle = 32
	}
	if len(w.idle) < w.maxIdle {
		w.idle = append(w.idle, pc)
		w.poolMu.Unlock()
		return
	}
	w.poolMu.Unlock()
	_ = pc.conn.Close()
}

func (w *workerConn) close() {
	w.poolMu.Lock()
	idle := w.idle
	w.idle = nil
	w.poolMu.Unlock()
	for _, pc := range idle {
		if pc != nil {
			_ = pc.conn.Close()
		}
	}
}

func writeRequest(bw *bufio.Writer, req request) error {
	switch req.op {
	case opGet:
		if _, err := bw.WriteString("get "); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		_, err := bw.WriteString("\r\n")
		return err
	case opSet:
		fallthrough
	case opAdd:
		fallthrough
	case opReplace:
		fallthrough
	case opAppend:
		fallthrough
	case opPrepend:
		verb := "set "
		switch req.op {
		case opAdd:
			verb = "add "
		case opReplace:
			verb = "replace "
		case opAppend:
			verb = "append "
		case opPrepend:
			verb = "prepend "
		}
		if _, err := bw.WriteString(verb); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		if _, err := bw.WriteString(" "); err != nil {
			return err
		}
		if err := writeUint32Decimal(bw, req.flags); err != nil {
			return err
		}
		if _, err := bw.WriteString(" "); err != nil {
			return err
		}
		if err := writeUintDecimal(bw, req.ttl); err != nil {
			return err
		}
		if _, err := bw.WriteString(" "); err != nil {
			return err
		}
		if err := writeUintDecimal(bw, len(req.value)); err != nil {
			return err
		}
		if _, err := bw.WriteString("\r\n"); err != nil {
			return err
		}
		if _, err := bw.Write(req.value); err != nil {
			return err
		}
		_, err := bw.WriteString("\r\n")
		return err
	case opDelete:
		if _, err := bw.WriteString("delete "); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		_, err := bw.WriteString("\r\n")
		return err
	case opTouch:
		if _, err := bw.WriteString("touch "); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		if _, err := bw.WriteString(" "); err != nil {
			return err
		}
		if err := writeUintDecimal(bw, req.ttl); err != nil {
			return err
		}
		_, err := bw.WriteString("\r\n")
		return err
	case opGetAndTouch:
		if _, err := bw.WriteString("gat "); err != nil {
			return err
		}
		if err := writeUintDecimal(bw, req.ttl); err != nil {
			return err
		}
		if _, err := bw.WriteString(" "); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		_, err := bw.WriteString("\r\n")
		return err
	case opFlushAll:
		_, err := bw.WriteString("flush_all\r\n")
		return err
	case opVersion:
		_, err := bw.WriteString("version\r\n")
		return err
	case opIncr:
		if _, err := bw.WriteString("incr "); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		if _, err := bw.WriteString(" "); err != nil {
			return err
		}
		if err := writeUint64Decimal(bw, req.delta); err != nil {
			return err
		}
		_, err := bw.WriteString("\r\n")
		return err
	case opDecr:
		if _, err := bw.WriteString("decr "); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		if _, err := bw.WriteString(" "); err != nil {
			return err
		}
		if err := writeUint64Decimal(bw, req.delta); err != nil {
			return err
		}
		_, err := bw.WriteString("\r\n")
		return err
	default:
		return errProtocol
	}
}

func writeGetMultiRequest(bw *bufio.Writer, keys []string) error {
	if _, err := bw.WriteString("get "); err != nil {
		return err
	}
	for i := range keys {
		if i > 0 {
			if err := bw.WriteByte(' '); err != nil {
				return err
			}
		}
		if _, err := bw.WriteString(keys[i]); err != nil {
			return err
		}
	}
	_, err := bw.WriteString("\r\n")
	return err
}

func readResponse(br *bufio.Reader, req request) ([]byte, error) {
	switch req.op {
	case opGet:
		return parseGetResponse(br)
	case opSet:
		fallthrough
	case opAdd:
		fallthrough
	case opReplace:
		fallthrough
	case opAppend:
		fallthrough
	case opPrepend:
		return nil, parseStoreResponse(br)
	case opDelete:
		return nil, parseDeleteResponse(br)
	case opTouch:
		return nil, parseTouchResponse(br)
	case opGetAndTouch:
		return parseGetResponse(br)
	case opFlushAll:
		return nil, parseOKResponse(br)
	case opVersion:
		return nil, parseVersionResponse(br)
	case opIncr, opDecr:
		return parseCounterResponse(br)
	default:
		return nil, errProtocol
	}
}

func parseGetResponse(br *bufio.Reader) ([]byte, error) {
	item, err := parseGetItemResponse(br)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func parseGetItemResponse(br *bufio.Reader) (*Item, error) {
	line, err := readLine(br)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(line, []byte("END")) {
		return nil, ErrNotFound
	}
	if bytes.Equal(line, []byte("ERROR")) {
		return nil, errProtocol
	}
	if bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")) {
		return nil, errors.New("memcache: " + string(line))
	}
	flags, n, err := parseValueMeta(line)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(br, buf); err != nil {
		return nil, err
	}
	var tail [2]byte
	if _, err := io.ReadFull(br, tail[:]); err != nil {
		return nil, err
	}
	if tail[0] != '\r' || tail[1] != '\n' {
		return nil, errProtocol
	}

	endLine, err := readLine(br)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(endLine, []byte("END")) {
		return nil, fmt.Errorf("%w: missing END after VALUE", errProtocol)
	}
	return &Item{
		Value: buf,
		Flags: flags,
	}, nil
}

func parseStoreResponse(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, []byte("STORED")):
		return nil
	case bytes.Equal(line, []byte("NOT_STORED")):
		return ErrNotStored
	case bytes.Equal(line, []byte("ERROR")):
		return errProtocol
	case bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")):
		return errors.New("memcache: " + string(line))
	default:
		return fmt.Errorf("%w: unexpected set response %q", errProtocol, line)
	}
}

func parseDeleteResponse(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, []byte("DELETED")):
		return nil
	case bytes.Equal(line, []byte("NOT_FOUND")):
		return ErrNotFound
	case bytes.Equal(line, []byte("ERROR")):
		return errProtocol
	case bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")):
		return errors.New("memcache: " + string(line))
	default:
		return fmt.Errorf("%w: unexpected delete response %q", errProtocol, line)
	}
}

func parseTouchResponse(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, []byte("TOUCHED")):
		return nil
	case bytes.Equal(line, []byte("NOT_FOUND")):
		return ErrNotFound
	case bytes.Equal(line, []byte("ERROR")):
		return errProtocol
	case bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")):
		return errors.New("memcache: " + string(line))
	default:
		return fmt.Errorf("%w: unexpected touch response %q", errProtocol, line)
	}
}

func parseOKResponse(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	if bytes.Equal(line, []byte("OK")) {
		return nil
	}
	if bytes.Equal(line, []byte("ERROR")) {
		return errProtocol
	}
	if bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")) {
		return errors.New("memcache: " + string(line))
	}
	return fmt.Errorf("%w: unexpected response %q", errProtocol, line)
}

func parseVersionResponse(br *bufio.Reader) error {
	line, err := readLine(br)
	if err != nil {
		return err
	}
	if bytes.HasPrefix(line, []byte("VERSION ")) {
		return nil
	}
	if bytes.Equal(line, []byte("ERROR")) {
		return errProtocol
	}
	if bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")) {
		return errors.New("memcache: " + string(line))
	}
	return fmt.Errorf("%w: unexpected version response %q", errProtocol, line)
}

func parseCounterResponse(br *bufio.Reader) ([]byte, error) {
	line, err := readLine(br)
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(line, []byte("NOT_FOUND")):
		return nil, ErrNotFound
	case bytes.Equal(line, []byte("ERROR")):
		return nil, errProtocol
	case bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")):
		return nil, errors.New("memcache: " + string(line))
	default:
		if _, ok := parseUint64(line); !ok {
			return nil, fmt.Errorf("%w: unexpected counter response %q", errProtocol, line)
		}
		return append([]byte(nil), line...), nil
	}
}

func parseGetMultiResponse(br *bufio.Reader) (map[string]*Item, error) {
	items := make(map[string]*Item)
	for {
		line, err := readLine(br)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(line, []byte("END")) {
			return items, nil
		}
		if bytes.Equal(line, []byte("ERROR")) {
			return nil, errProtocol
		}
		if bytes.HasPrefix(line, []byte("CLIENT_ERROR ")) || bytes.HasPrefix(line, []byte("SERVER_ERROR ")) {
			return nil, errors.New("memcache: " + string(line))
		}
		key, flags, n, err := parseValueHeader(line)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		var tail [2]byte
		if _, err := io.ReadFull(br, tail[:]); err != nil {
			return nil, err
		}
		if tail[0] != '\r' || tail[1] != '\n' {
			return nil, errProtocol
		}
		items[key] = &Item{
			Value: buf,
			Flags: flags,
		}
	}
}

func readGetItemResponse(br *bufio.Reader, req request) (*Item, error) {
	switch req.op {
	case opGet, opGetAndTouch:
		return parseGetItemResponse(br)
	default:
		return nil, errProtocol
	}
}

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return nil, errProtocol
	}
	return line[:len(line)-2], nil
}

func parseValueBytes(line []byte) (int, error) {
	// VALUE <key> <flags> <bytes> [cas]
	spaces := 0
	start := -1
	end := -1
	for i := len("VALUE "); i < len(line); i++ {
		if line[i] == ' ' {
			spaces++
			if spaces == 2 {
				start = i + 1
			}
			if spaces == 3 {
				end = i
				break
			}
		}
	}
	if start == -1 {
		return 0, errProtocol
	}
	if end == -1 {
		end = len(line)
	}
	n, ok := parsePositiveInt(line[start:end])
	if !ok {
		return 0, errProtocol
	}
	return n, nil
}

func parseValueMeta(line []byte) (uint32, int, error) {
	_, flags, n, err := parseValueHeaderParts(line)
	return flags, n, err
}

func parseValueHeader(line []byte) (string, uint32, int, error) {
	key, flags, n, err := parseValueHeaderParts(line)
	if err != nil {
		return "", 0, 0, err
	}
	return string(key), flags, n, nil
}

func parseValueHeaderParts(line []byte) ([]byte, uint32, int, error) {
	// VALUE <key> <flags> <bytes> [cas]
	if len(line) < 8 || !bytes.HasPrefix(line, []byte("VALUE ")) {
		return nil, 0, 0, fmt.Errorf("%w: unexpected get response %q", errProtocol, line)
	}

	i := len("VALUE ")
	keyStart := i
	for i < len(line) && line[i] != ' ' {
		i++
	}
	if i == len(line) || i == keyStart {
		return nil, 0, 0, errProtocol
	}
	key := line[keyStart:i]

	i++ // skip space
	flagsStart := i
	for i < len(line) && line[i] != ' ' {
		i++
	}
	if i == len(line) || i == flagsStart {
		return nil, 0, 0, errProtocol
	}
	flags, err := parseUint32(line[flagsStart:i])
	if err != nil {
		return nil, 0, 0, err
	}

	i++ // skip space
	bytesStart := i
	for i < len(line) && line[i] != ' ' {
		i++
	}
	if bytesStart == i {
		return nil, 0, 0, errProtocol
	}
	n, ok := parsePositiveInt(line[bytesStart:i])
	if !ok {
		return nil, 0, 0, errProtocol
	}
	return key, flags, n, nil
}

func writeUintDecimal(bw *bufio.Writer, n int) error {
	if n < 0 {
		return errProtocol
	}
	if n == 0 {
		return bw.WriteByte('0')
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	for ; i < len(buf); i++ {
		if err := bw.WriteByte(buf[i]); err != nil {
			return err
		}
	}
	return nil
}

func parsePositiveInt(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
		if n < 0 {
			return 0, false
		}
	}
	return n, true
}

func parseUint64(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		next := n*10 + uint64(c-'0')
		if next < n {
			return 0, false
		}
		n = next
	}
	return n, true
}

func parseCounterValue(v []byte) (uint64, error) {
	n, ok := parseUint64(v)
	if !ok {
		return 0, errProtocol
	}
	return n, nil
}

func parseUint32(b []byte) (uint32, error) {
	v, ok := parseUint64(b)
	if !ok || v > uint64(^uint32(0)) {
		return 0, errProtocol
	}
	return uint32(v), nil
}

func writeUint32Decimal(bw *bufio.Writer, n uint32) error {
	return writeUint64Decimal(bw, uint64(n))
}

func writeUint64Decimal(bw *bufio.Writer, n uint64) error {
	if n == 0 {
		return bw.WriteByte('0')
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + (n % 10))
		n /= 10
	}
	for ; i < len(buf); i++ {
		if err := bw.WriteByte(buf[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateKey(key string) error {
	if key == "" {
		return errors.New("memcache: key is required")
	}
	if len(key) > 250 {
		return errors.New("memcache: key too long")
	}
	for i := 0; i < len(key); i++ {
		b := key[i]
		if b <= 0x20 || b == 0x7f {
			return errors.New("memcache: key contains invalid character")
		}
	}
	return nil
}

func validateStoreInput(key string, ttlSeconds int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		return errors.New("memcache: ttlSeconds must be >= 0")
	}
	return nil
}

func isTimeoutErr(err error) bool {
	nerr, ok := errors.AsType[net.Error](err)
	return ok && nerr.Timeout()
}

func normalizeContextErr(ctx context.Context, deadline time.Time, err error) error {
	if err == nil {
		return nil
	}
	if isTimeoutErr(err) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, net.ErrClosed) {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
	}
	return err
}
