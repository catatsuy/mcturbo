package mcturbo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound  = errors.New("memcache: not found")
	ErrNotStored = errors.New("memcache: not stored")
	ErrClosed    = errors.New("memcache: client closed")
)

var errProtocol = errors.New("memcache: protocol error")

type Logger = slog.Logger

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

func WithWorkers(n int) Option {
	return func(c *config) error {
		if n <= 0 {
			return errors.New("memcache: workers must be > 0")
		}
		c.workers = n
		return nil
	}
}

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

func WithMaxSlots(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return errors.New("memcache: max slots must be >= 0")
		}
		c.maxSlots = n
		return nil
	}
}

func WithDefaultDeadline(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return errors.New("memcache: default deadline must be >= 0")
		}
		c.defaultDeadline = d
		return nil
	}
}

func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		c.logger = l
		return nil
	}
}

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

func (c *Client) Get(key string) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	return c.doFast(opGet, key, nil, 0)
}

func (c *Client) Set(key string, value []byte, ttlSeconds int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		return errors.New("memcache: ttlSeconds must be >= 0")
	}
	_, err := c.doFast(opSet, key, value, ttlSeconds)
	return err
}

func (c *Client) Delete(key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	_, err := c.doFast(opDelete, key, nil, 0)
	return err
}

func (c *Client) Touch(key string, ttlSeconds int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		return errors.New("memcache: ttlSeconds must be >= 0")
	}
	_, err := c.doFast(opTouch, key, nil, ttlSeconds)
	return err
}

func (c *Client) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, errors.New("memcache: nil context")
	}
	return c.doWithContext(ctx, opGet, key, nil, 0)
}

func (c *Client) SetWithContext(ctx context.Context, key string, value []byte, ttlSeconds int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		return errors.New("memcache: ttlSeconds must be >= 0")
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opSet, key, value, ttlSeconds)
	return err
}

func (c *Client) DeleteWithContext(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if ctx == nil {
		return errors.New("memcache: nil context")
	}
	_, err := c.doWithContext(ctx, opDelete, key, nil, 0)
	return err
}

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
	_, err := c.doWithContext(ctx, opTouch, key, nil, ttlSeconds)
	return err
}

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

func (c *Client) doFast(op opType, key string, value []byte, ttl int) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	w := c.pickWorker(key)
	return w.roundTripFast(request{op: op, key: key, value: value, ttl: ttl})
}

func (c *Client) doWithContext(ctx context.Context, op opType, key string, value []byte, ttl int) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := c.pickWorker(key)
	v, err := w.roundTripWithContext(ctx, request{op: op, key: key, value: value, ttl: ttl})
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
	opDelete
	opTouch
)

type request struct {
	op    opType
	key   string
	value []byte
	ttl   int
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
	for i := range idle {
		if idle[i] != nil {
			_ = idle[i].conn.Close()
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
		if _, err := bw.WriteString("set "); err != nil {
			return err
		}
		if _, err := bw.WriteString(req.key); err != nil {
			return err
		}
		if _, err := bw.WriteString(" 0 "); err != nil {
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
	default:
		return errProtocol
	}
}

func readResponse(br *bufio.Reader, req request) ([]byte, error) {
	switch req.op {
	case opGet:
		return parseGetResponse(br)
	case opSet:
		return nil, parseStoreResponse(br)
	case opDelete:
		return nil, parseDeleteResponse(br)
	case opTouch:
		return nil, parseTouchResponse(br)
	default:
		return nil, errProtocol
	}
}

func parseGetResponse(br *bufio.Reader) ([]byte, error) {
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
	if !bytes.HasPrefix(line, []byte("VALUE ")) {
		return nil, fmt.Errorf("%w: unexpected get response %q", errProtocol, line)
	}

	n, err := parseValueBytes(line)
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
	return buf, nil
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
	for i := 0; i < len(b); i++ {
		c := b[i]
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
