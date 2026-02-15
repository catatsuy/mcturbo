package cluster

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/catatsuy/mcturbo"
)

var (
	// ErrClosed is returned when the cluster is already closed.
	ErrClosed = errors.New("cluster: closed")
)

// Server defines one memcached server in the cluster.
type Server struct {
	// Addr is the server address, for example "127.0.0.1:11211".
	Addr string
	// Weight controls shard share for consistent hashing.
	Weight int
}

type shardClient interface {
	Get(key string) (*mcturbo.Item, error)
	Gets(key string) (*mcturbo.Item, error)
	GetMulti(keys []string) (map[string]*mcturbo.Item, error)
	GetMultiWithContext(ctx context.Context, keys []string) (map[string]*mcturbo.Item, error)
	Set(key string, value []byte, flags uint32, ttlSeconds int) error
	Add(key string, value []byte, flags uint32, ttlSeconds int) error
	Replace(key string, value []byte, flags uint32, ttlSeconds int) error
	Append(key string, value []byte) error
	Prepend(key string, value []byte) error
	Delete(key string) error
	Touch(key string, ttlSeconds int) error
	GetAndTouch(key string, ttlSeconds int) (*mcturbo.Item, error)
	Incr(key string, delta uint64) (uint64, error)
	Decr(key string, delta uint64) (uint64, error)
	CAS(key string, value []byte, flags uint32, ttlSeconds int, cas uint64) error
	GetWithContext(ctx context.Context, key string) (*mcturbo.Item, error)
	GetsWithContext(ctx context.Context, key string) (*mcturbo.Item, error)
	SetWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error
	AddWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error
	ReplaceWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error
	AppendWithContext(ctx context.Context, key string, value []byte) error
	PrependWithContext(ctx context.Context, key string, value []byte) error
	DeleteWithContext(ctx context.Context, key string) error
	TouchWithContext(ctx context.Context, key string, ttlSeconds int) error
	GetAndTouchWithContext(ctx context.Context, key string, ttlSeconds int) (*mcturbo.Item, error)
	IncrWithContext(ctx context.Context, key string, delta uint64) (uint64, error)
	DecrWithContext(ctx context.Context, key string, delta uint64) (uint64, error)
	CASWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int, cas uint64) error
	Close() error
}

type shardFactory func(addr string, opts ...mcturbo.Option) (shardClient, error)

type clusterState struct {
	servers []Server
	router  Router
	shards  []shardClient
	byAddr  map[string]shardClient
}

type shardHealth struct {
	failures  int
	deadUntil time.Time
}

// Cluster routes keys to shard clients built from mcturbo.Client.
type Cluster struct {
	closed atomic.Bool

	mu sync.Mutex

	state atomic.Value // *clusterState

	baseClientOptions   []mcturbo.Option
	vnodeFactor         int
	distribution        Distribution
	hash                Hash
	libketamaCompatible bool
	removeFailedServers bool
	serverFailureLimit  int
	retryTimeout        time.Duration
	healthByAddr        map[string]*shardHealth
	factory             shardFactory
}

// NewCluster creates a new distributed client from servers.
func NewCluster(servers []Server, opts ...ClusterOption) (*Cluster, error) {
	cfg := defaultClusterConfig()
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	c := &Cluster{
		baseClientOptions:   cfg.baseClientOptions,
		vnodeFactor:         cfg.vnodeFactor,
		distribution:        effectiveDistribution(&cfg),
		hash:                effectiveHash(&cfg),
		libketamaCompatible: cfg.libketamaCompatible,
		removeFailedServers: cfg.removeFailedServers,
		serverFailureLimit:  cfg.serverFailureLimit,
		retryTimeout:        cfg.retryTimeout,
		healthByAddr:        map[string]*shardHealth{},
		factory:             cfg.factory,
	}
	if err := c.updateServersLocked(servers); err != nil {
		return nil, err
	}
	return c, nil
}

// Get returns the value for key.
func (c *Cluster) Get(key string) (*mcturbo.Item, error) {
	return c.execItemNoContext(key, func(shard shardClient) (*mcturbo.Item, error) {
		return shard.Get(key)
	})
}

// Gets returns the item for key with CAS value.
func (c *Cluster) Gets(key string) (*mcturbo.Item, error) {
	return c.execItemNoContext(key, func(shard shardClient) (*mcturbo.Item, error) {
		return shard.Gets(key)
	})
}

// GetMultiWithContext fetches multiple keys from routed shards using ctx.
func (c *Cluster) GetMultiWithContext(ctx context.Context, keys []string) (map[string]*mcturbo.Item, error) {
	out := make(map[string]*mcturbo.Item, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	if ctx == nil {
		return out, errors.New("cluster: nil context")
	}
	if c.closed.Load() {
		return out, ErrClosed
	}
	st := c.loadState()
	if st == nil || st.router == nil || len(st.shards) == 0 {
		return out, errNoServers
	}

	byShard := make(map[int][]string, len(st.shards))
	for _, key := range keys {
		idx := st.router.Pick(key)
		if idx < 0 || idx >= len(st.shards) {
			return out, errNoServers
		}
		byShard[idx] = append(byShard[idx], key)
	}

	type multiRes struct {
		addr  string
		items map[string]*mcturbo.Item
		err   error
	}
	ch := make(chan multiRes, len(byShard))
	for idx, grouped := range byShard {
		shard := st.shards[idx]
		addr := st.servers[idx].Addr
		ks := append([]string(nil), grouped...)
		go func() {
			items, err := shard.GetMultiWithContext(ctx, ks)
			ch <- multiRes{addr: addr, items: items, err: err}
		}()
	}

	var merr *mcturbo.MultiError
	for range byShard {
		r := <-ch
		if r.err != nil {
			if merr == nil {
				merr = &mcturbo.MultiError{PerServer: map[string]error{}}
			}
			merr.PerServer[r.addr] = r.err
			continue
		}
		for k, v := range r.items {
			out[k] = v
		}
	}
	if merr != nil {
		return out, merr
	}
	return out, nil
}

// Set stores value for key with flags and ttlSeconds.
func (c *Cluster) Set(key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.Set(key, value, flags, ttlSeconds)
	})
}

// Add stores value for key only if key does not exist.
func (c *Cluster) Add(key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.Add(key, value, flags, ttlSeconds)
	})
}

// Replace stores value for key only if key already exists.
func (c *Cluster) Replace(key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.Replace(key, value, flags, ttlSeconds)
	})
}

// Append appends value to existing key value.
func (c *Cluster) Append(key string, value []byte) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.Append(key, value)
	})
}

// Prepend prepends value to existing key value.
func (c *Cluster) Prepend(key string, value []byte) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.Prepend(key, value)
	})
}

// Delete removes key.
func (c *Cluster) Delete(key string) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.Delete(key)
	})
}

// Touch updates key expiration to ttlSeconds.
func (c *Cluster) Touch(key string, ttlSeconds int) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.Touch(key, ttlSeconds)
	})
}

// GetAndTouch gets key and updates key expiration to ttlSeconds.
func (c *Cluster) GetAndTouch(key string, ttlSeconds int) (*mcturbo.Item, error) {
	return c.execItemNoContext(key, func(shard shardClient) (*mcturbo.Item, error) {
		return shard.GetAndTouch(key, ttlSeconds)
	})
}

// Incr increments a numeric value by delta and returns the new value.
func (c *Cluster) Incr(key string, delta uint64) (uint64, error) {
	return c.execUint64NoContext(key, func(shard shardClient) (uint64, error) {
		return shard.Incr(key, delta)
	})
}

// Decr decrements a numeric value by delta and returns the new value.
func (c *Cluster) Decr(key string, delta uint64) (uint64, error) {
	return c.execUint64NoContext(key, func(shard shardClient) (uint64, error) {
		return shard.Decr(key, delta)
	})
}

// CAS stores value for key only when cas matches.
func (c *Cluster) CAS(key string, value []byte, flags uint32, ttlSeconds int, cas uint64) error {
	return c.execErrNoContext(key, func(shard shardClient) error {
		return shard.CAS(key, value, flags, ttlSeconds, cas)
	})
}

// GetWithContext returns the value for key using ctx.
func (c *Cluster) GetWithContext(ctx context.Context, key string) (*mcturbo.Item, error) {
	return c.execItemWithContext(ctx, key, func(shard shardClient) (*mcturbo.Item, error) {
		return shard.GetWithContext(ctx, key)
	})
}

// GetsWithContext returns the item for key with CAS value using ctx.
func (c *Cluster) GetsWithContext(ctx context.Context, key string) (*mcturbo.Item, error) {
	return c.execItemWithContext(ctx, key, func(shard shardClient) (*mcturbo.Item, error) {
		return shard.GetsWithContext(ctx, key)
	})
}

// SetWithContext stores value for key with flags and ttlSeconds using ctx.
func (c *Cluster) SetWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.SetWithContext(ctx, key, value, flags, ttlSeconds)
	})
}

// AddWithContext stores value for key only if key does not exist.
func (c *Cluster) AddWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.AddWithContext(ctx, key, value, flags, ttlSeconds)
	})
}

// ReplaceWithContext stores value for key only if key already exists.
func (c *Cluster) ReplaceWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.ReplaceWithContext(ctx, key, value, flags, ttlSeconds)
	})
}

// AppendWithContext appends value to existing key value.
func (c *Cluster) AppendWithContext(ctx context.Context, key string, value []byte) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.AppendWithContext(ctx, key, value)
	})
}

// PrependWithContext prepends value to existing key value.
func (c *Cluster) PrependWithContext(ctx context.Context, key string, value []byte) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.PrependWithContext(ctx, key, value)
	})
}

// DeleteWithContext removes key using ctx.
func (c *Cluster) DeleteWithContext(ctx context.Context, key string) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.DeleteWithContext(ctx, key)
	})
}

// TouchWithContext updates key expiration using ctx.
func (c *Cluster) TouchWithContext(ctx context.Context, key string, ttlSeconds int) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.TouchWithContext(ctx, key, ttlSeconds)
	})
}

// GetAndTouchWithContext gets key and updates key expiration to ttlSeconds.
func (c *Cluster) GetAndTouchWithContext(ctx context.Context, key string, ttlSeconds int) (*mcturbo.Item, error) {
	return c.execItemWithContext(ctx, key, func(shard shardClient) (*mcturbo.Item, error) {
		return shard.GetAndTouchWithContext(ctx, key, ttlSeconds)
	})
}

// IncrWithContext increments a numeric value by delta and returns the new value.
func (c *Cluster) IncrWithContext(ctx context.Context, key string, delta uint64) (uint64, error) {
	return c.execUint64WithContext(ctx, key, func(shard shardClient) (uint64, error) {
		return shard.IncrWithContext(ctx, key, delta)
	})
}

// DecrWithContext decrements a numeric value by delta and returns the new value.
func (c *Cluster) DecrWithContext(ctx context.Context, key string, delta uint64) (uint64, error) {
	return c.execUint64WithContext(ctx, key, func(shard shardClient) (uint64, error) {
		return shard.DecrWithContext(ctx, key, delta)
	})
}

// CASWithContext stores value for key only when cas matches using ctx.
func (c *Cluster) CASWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int, cas uint64) error {
	return c.execErrWithContext(ctx, key, func(shard shardClient) error {
		return shard.CASWithContext(ctx, key, value, flags, ttlSeconds, cas)
	})
}

// GetNoContext is an explicit no-context alias of Get.
func (c *Cluster) GetNoContext(key string) (*mcturbo.Item, error) {
	return c.Get(key)
}

// GetsNoContext is an explicit no-context alias of Gets.
func (c *Cluster) GetsNoContext(key string) (*mcturbo.Item, error) {
	return c.Gets(key)
}

// GetMulti fetches multiple keys from routed shards without context.
func (c *Cluster) GetMulti(keys []string) (map[string]*mcturbo.Item, error) {
	out := make(map[string]*mcturbo.Item, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	if c.closed.Load() {
		return out, ErrClosed
	}
	st := c.loadState()
	if st == nil || st.router == nil || len(st.shards) == 0 {
		return out, errNoServers
	}

	byShard := make(map[int][]string, len(st.shards))
	for _, key := range keys {
		idx := st.router.Pick(key)
		if idx < 0 || idx >= len(st.shards) {
			return out, errNoServers
		}
		byShard[idx] = append(byShard[idx], key)
	}

	type multiRes struct {
		addr  string
		items map[string]*mcturbo.Item
		err   error
	}
	ch := make(chan multiRes, len(byShard))
	for idx, grouped := range byShard {
		shard := st.shards[idx]
		addr := st.servers[idx].Addr
		ks := append([]string(nil), grouped...)
		go func() {
			items, err := shard.GetMulti(ks)
			ch <- multiRes{addr: addr, items: items, err: err}
		}()
	}

	var merr *mcturbo.MultiError
	for range byShard {
		r := <-ch
		if r.err != nil {
			if merr == nil {
				merr = &mcturbo.MultiError{PerServer: map[string]error{}}
			}
			merr.PerServer[r.addr] = r.err
			continue
		}
		for k, v := range r.items {
			out[k] = v
		}
	}
	if merr != nil {
		return out, merr
	}
	return out, nil
}

// SetNoContext is an explicit no-context alias of Set.
func (c *Cluster) SetNoContext(key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.Set(key, value, flags, ttlSeconds)
}

// AddNoContext is an explicit no-context alias of Add.
func (c *Cluster) AddNoContext(key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.Add(key, value, flags, ttlSeconds)
}

// ReplaceNoContext is an explicit no-context alias of Replace.
func (c *Cluster) ReplaceNoContext(key string, value []byte, flags uint32, ttlSeconds int) error {
	return c.Replace(key, value, flags, ttlSeconds)
}

// AppendNoContext is an explicit no-context alias of Append.
func (c *Cluster) AppendNoContext(key string, value []byte) error {
	return c.Append(key, value)
}

// PrependNoContext is an explicit no-context alias of Prepend.
func (c *Cluster) PrependNoContext(key string, value []byte) error {
	return c.Prepend(key, value)
}

// DeleteNoContext is an explicit no-context alias of Delete.
func (c *Cluster) DeleteNoContext(key string) error {
	return c.Delete(key)
}

// TouchNoContext is an explicit no-context alias of Touch.
func (c *Cluster) TouchNoContext(key string, ttlSeconds int) error {
	return c.Touch(key, ttlSeconds)
}

// GetAndTouchNoContext is an explicit no-context alias of GetAndTouch.
func (c *Cluster) GetAndTouchNoContext(key string, ttlSeconds int) (*mcturbo.Item, error) {
	return c.GetAndTouch(key, ttlSeconds)
}

// IncrNoContext is an explicit no-context alias of Incr.
func (c *Cluster) IncrNoContext(key string, delta uint64) (uint64, error) {
	return c.Incr(key, delta)
}

// DecrNoContext is an explicit no-context alias of Decr.
func (c *Cluster) DecrNoContext(key string, delta uint64) (uint64, error) {
	return c.Decr(key, delta)
}

// CASNoContext is an explicit no-context alias of CAS.
func (c *Cluster) CASNoContext(key string, value []byte, flags uint32, ttlSeconds int, cas uint64) error {
	return c.CAS(key, value, flags, ttlSeconds, cas)
}

// UpdateServers updates cluster servers and rebuilds routing.
func (c *Cluster) UpdateServers(servers []Server) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	return c.updateServersLocked(servers)
}

// Close closes all shard clients.
func (c *Cluster) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	st := c.loadState()
	if st == nil {
		return nil
	}
	for _, shard := range st.byAddr {
		_ = shard.Close()
	}
	c.state.Store(&clusterState{})
	return nil
}

func (c *Cluster) updateServersLocked(servers []Server) error {
	normalized, err := validateServers(servers)
	if err != nil {
		return err
	}

	router, err := newRouter(normalized, c.distribution, c.hash, c.vnodeFactor)
	if err != nil {
		return err
	}

	oldState := c.loadState()
	oldByAddr := map[string]shardClient{}
	if oldState != nil {
		oldByAddr = oldState.byAddr
	}

	newByAddr := make(map[string]shardClient, len(normalized))
	created := make([]shardClient, 0, len(normalized))
	for _, srv := range normalized {
		if shard, ok := oldByAddr[srv.Addr]; ok {
			newByAddr[srv.Addr] = shard
			continue
		}
		shard, err := c.factory(srv.Addr, c.baseClientOptions...)
		if err != nil {
			for j := range created {
				_ = created[j].Close()
			}
			return err
		}
		created = append(created, shard)
		newByAddr[srv.Addr] = shard
	}

	newShards := make([]shardClient, len(normalized))
	for i, srv := range normalized {
		newShards[i] = newByAddr[srv.Addr]
	}

	newState := &clusterState{
		servers: normalized,
		router:  router,
		shards:  newShards,
		byAddr:  newByAddr,
	}
	c.state.Store(newState)

	nextHealth := make(map[string]*shardHealth, len(normalized))
	for _, srv := range normalized {
		addr := srv.Addr
		if h, ok := c.healthByAddr[addr]; ok {
			nextHealth[addr] = h
			continue
		}
		nextHealth[addr] = &shardHealth{}
	}
	c.healthByAddr = nextHealth

	if oldState != nil {
		for addr, shard := range oldState.byAddr {
			if _, ok := newByAddr[addr]; !ok {
				_ = shard.Close()
			}
		}
	}
	return nil
}

func (c *Cluster) loadState() *clusterState {
	v := c.state.Load()
	if v == nil {
		return nil
	}
	return v.(*clusterState)
}

func (c *Cluster) execItemNoContext(key string, fn func(shardClient) (*mcturbo.Item, error)) (*mcturbo.Item, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	var last error
	for _, cand := range c.pickCandidates(key) {
		it, err := fn(cand.shard)
		if err == nil {
			c.noteSuccess(cand.addr)
			return it, nil
		}
		last = err
		c.noteFailure(cand.addr, err)
		if !c.shouldTryNext(err) {
			return nil, err
		}
	}
	if last != nil {
		return nil, last
	}
	return nil, errNoServers
}

func (c *Cluster) execItemWithContext(ctx context.Context, key string, fn func(shardClient) (*mcturbo.Item, error)) (*mcturbo.Item, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	var last error
	for _, cand := range c.pickCandidates(key) {
		it, err := fn(cand.shard)
		if err == nil {
			c.noteSuccess(cand.addr)
			return it, nil
		}
		last = err
		c.noteFailure(cand.addr, err)
		if !c.shouldTryNext(err) || ctx.Err() != nil {
			return nil, err
		}
	}
	if last != nil {
		return nil, last
	}
	return nil, errNoServers
}

func (c *Cluster) execErrNoContext(key string, fn func(shardClient) error) error {
	if c.closed.Load() {
		return ErrClosed
	}
	var last error
	for _, cand := range c.pickCandidates(key) {
		err := fn(cand.shard)
		if err == nil {
			c.noteSuccess(cand.addr)
			return nil
		}
		last = err
		c.noteFailure(cand.addr, err)
		if !c.shouldTryNext(err) {
			return err
		}
	}
	if last != nil {
		return last
	}
	return errNoServers
}

func (c *Cluster) execErrWithContext(ctx context.Context, key string, fn func(shardClient) error) error {
	if c.closed.Load() {
		return ErrClosed
	}
	var last error
	for _, cand := range c.pickCandidates(key) {
		err := fn(cand.shard)
		if err == nil {
			c.noteSuccess(cand.addr)
			return nil
		}
		last = err
		c.noteFailure(cand.addr, err)
		if !c.shouldTryNext(err) || ctx.Err() != nil {
			return err
		}
	}
	if last != nil {
		return last
	}
	return errNoServers
}

func (c *Cluster) execUint64NoContext(key string, fn func(shardClient) (uint64, error)) (uint64, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	var last error
	for _, cand := range c.pickCandidates(key) {
		v, err := fn(cand.shard)
		if err == nil {
			c.noteSuccess(cand.addr)
			return v, nil
		}
		last = err
		c.noteFailure(cand.addr, err)
		if !c.shouldTryNext(err) {
			return 0, err
		}
	}
	if last != nil {
		return 0, last
	}
	return 0, errNoServers
}

func (c *Cluster) execUint64WithContext(ctx context.Context, key string, fn func(shardClient) (uint64, error)) (uint64, error) {
	if c.closed.Load() {
		return 0, ErrClosed
	}
	var last error
	for _, cand := range c.pickCandidates(key) {
		v, err := fn(cand.shard)
		if err == nil {
			c.noteSuccess(cand.addr)
			return v, nil
		}
		last = err
		c.noteFailure(cand.addr, err)
		if !c.shouldTryNext(err) || ctx.Err() != nil {
			return 0, err
		}
	}
	if last != nil {
		return 0, last
	}
	return 0, errNoServers
}

type shardCandidate struct {
	addr  string
	shard shardClient
}

func (c *Cluster) pickCandidates(key string) []shardCandidate {
	if c.closed.Load() {
		return nil
	}
	st := c.loadState()
	if st == nil || st.router == nil || len(st.shards) == 0 {
		return nil
	}
	start := st.router.Pick(key)
	if start < 0 || start >= len(st.shards) {
		return nil
	}
	if !c.removeFailedServers {
		return []shardCandidate{{addr: st.servers[start].Addr, shard: st.shards[start]}}
	}

	now := time.Now()
	out := make([]shardCandidate, 0, len(st.shards))
	for i := 0; i < len(st.shards); i++ {
		idx := (start + i) % len(st.shards)
		addr := st.servers[idx].Addr
		if !c.isAlive(addr, now) {
			continue
		}
		out = append(out, shardCandidate{addr: addr, shard: st.shards[idx]})
	}
	if len(out) == 0 {
		// all servers are currently ejected; fallback to all servers.
		for i := 0; i < len(st.shards); i++ {
			idx := (start + i) % len(st.shards)
			out = append(out, shardCandidate{
				addr:  st.servers[idx].Addr,
				shard: st.shards[idx],
			})
		}
	}
	return out
}

func (c *Cluster) isAlive(addr string, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.healthByAddr[addr]
	if !ok {
		return true
	}
	if h.deadUntil.IsZero() {
		return true
	}
	if now.Before(h.deadUntil) {
		return false
	}
	h.deadUntil = time.Time{}
	h.failures = 0
	return true
}

func (c *Cluster) noteSuccess(addr string) {
	if !c.removeFailedServers {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.healthByAddr[addr]
	if !ok {
		return
	}
	h.failures = 0
	h.deadUntil = time.Time{}
}

func (c *Cluster) noteFailure(addr string, err error) {
	if !c.removeFailedServers || !isCommunicationFailure(err) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.healthByAddr[addr]
	if !ok {
		h = &shardHealth{}
		c.healthByAddr[addr] = h
	}
	h.failures++
	if h.failures >= c.serverFailureLimit {
		h.deadUntil = time.Now().Add(c.retryTimeout)
		h.failures = 0
	}
}

func (c *Cluster) shouldTryNext(err error) bool {
	return c.removeFailedServers && isCommunicationFailure(err)
}

func isCommunicationFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, mcturbo.ErrNotFound) || errors.Is(err, mcturbo.ErrNotStored) || errors.Is(err, mcturbo.ErrCASConflict) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if nerr, ok := errors.AsType[net.Error](err); ok {
		return nerr.Timeout() || !nerr.Temporary()
	}
	return mcturbo.IsProtocolError(err)
}

func validateServers(servers []Server) ([]Server, error) {
	if len(servers) == 0 {
		return nil, errors.New("cluster: at least one server is required")
	}
	out := make([]Server, len(servers))
	seen := make(map[string]struct{}, len(servers))
	for i := range servers {
		s := servers[i]
		if s.Addr == "" {
			return nil, errors.New("cluster: server addr is required")
		}
		if s.Weight == 0 {
			s.Weight = 1
		}
		if s.Weight < 0 {
			return nil, errors.New("cluster: server weight must be > 0")
		}
		if _, ok := seen[s.Addr]; ok {
			return nil, errors.New("cluster: duplicate server addr")
		}
		seen[s.Addr] = struct{}{}
		out[i] = s
	}
	return out, nil
}
