package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/catatsuy/mcturbo"
)

var (
	ErrClosed = errors.New("cluster: closed")
)

type Server struct {
	Addr   string
	Weight int
}

type shardClient interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte, ttlSeconds int) error
	Delete(key string) error
	Touch(key string, ttlSeconds int) error
	GetWithContext(ctx context.Context, key string) ([]byte, error)
	SetWithContext(ctx context.Context, key string, value []byte, ttlSeconds int) error
	DeleteWithContext(ctx context.Context, key string) error
	TouchWithContext(ctx context.Context, key string, ttlSeconds int) error
	Close() error
}

type shardFactory func(addr string, opts ...mcturbo.Option) (shardClient, error)

type clusterState struct {
	servers []Server
	router  Router
	shards  []shardClient
	byAddr  map[string]shardClient
}

// Cluster routes keys to phase-1 clients.
type Cluster struct {
	closed atomic.Bool

	mu sync.Mutex

	state atomic.Value // *clusterState

	baseClientOptions   []mcturbo.Option
	vnodeFactor         int
	distribution        Distribution
	hash                Hash
	libketamaCompatible bool
	factory             shardFactory
}

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
		factory:             cfg.factory,
	}
	if err := c.updateServersLocked(servers); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cluster) Get(key string) ([]byte, error) {
	shard, err := c.pickShard(key)
	if err != nil {
		return nil, err
	}
	return shard.Get(key)
}

func (c *Cluster) Set(key string, value []byte, ttlSeconds int) error {
	shard, err := c.pickShard(key)
	if err != nil {
		return err
	}
	return shard.Set(key, value, ttlSeconds)
}

func (c *Cluster) Delete(key string) error {
	shard, err := c.pickShard(key)
	if err != nil {
		return err
	}
	return shard.Delete(key)
}

func (c *Cluster) Touch(key string, ttlSeconds int) error {
	shard, err := c.pickShard(key)
	if err != nil {
		return err
	}
	return shard.Touch(key, ttlSeconds)
}

func (c *Cluster) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	shard, err := c.pickShard(key)
	if err != nil {
		return nil, err
	}
	return shard.GetWithContext(ctx, key)
}

func (c *Cluster) SetWithContext(ctx context.Context, key string, value []byte, ttlSeconds int) error {
	shard, err := c.pickShard(key)
	if err != nil {
		return err
	}
	return shard.SetWithContext(ctx, key, value, ttlSeconds)
}

func (c *Cluster) DeleteWithContext(ctx context.Context, key string) error {
	shard, err := c.pickShard(key)
	if err != nil {
		return err
	}
	return shard.DeleteWithContext(ctx, key)
}

func (c *Cluster) TouchWithContext(ctx context.Context, key string, ttlSeconds int) error {
	shard, err := c.pickShard(key)
	if err != nil {
		return err
	}
	return shard.TouchWithContext(ctx, key, ttlSeconds)
}

func (c *Cluster) GetNoContext(key string) ([]byte, error) {
	return c.Get(key)
}

func (c *Cluster) SetNoContext(key string, value []byte, ttlSeconds int) error {
	return c.Set(key, value, ttlSeconds)
}

func (c *Cluster) DeleteNoContext(key string) error {
	return c.Delete(key)
}

func (c *Cluster) TouchNoContext(key string, ttlSeconds int) error {
	return c.Touch(key, ttlSeconds)
}

func (c *Cluster) UpdateServers(servers []Server) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrClosed
	}
	return c.updateServersLocked(servers)
}

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
	for i := range normalized {
		srv := normalized[i]
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
	for i := range normalized {
		newShards[i] = newByAddr[normalized[i].Addr]
	}

	newState := &clusterState{
		servers: normalized,
		router:  router,
		shards:  newShards,
		byAddr:  newByAddr,
	}
	c.state.Store(newState)

	if oldState != nil {
		for addr, shard := range oldState.byAddr {
			if _, ok := newByAddr[addr]; !ok {
				_ = shard.Close()
			}
		}
	}
	return nil
}

func (c *Cluster) pickShard(key string) (shardClient, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	st := c.loadState()
	if st == nil || st.router == nil {
		return nil, errNoServers
	}
	idx := st.router.Pick(key)
	if idx < 0 || idx >= len(st.shards) {
		return nil, errNoServers
	}
	return st.shards[idx], nil
}

func (c *Cluster) loadState() *clusterState {
	v := c.state.Load()
	if v == nil {
		return nil
	}
	return v.(*clusterState)
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
