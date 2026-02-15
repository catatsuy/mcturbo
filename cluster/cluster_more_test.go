package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/catatsuy/mcturbo"
)

type fakeShard struct {
	mu sync.Mutex

	getCount            int
	setCount            int
	deleteCount         int
	touchCount          int
	getWithContextCount int
	setWithContextCount int

	getErr    error
	setErr    error
	deleteErr error
	touchErr  error
	ctxErr    error

	value []byte
	last  string

	closed bool
}

func (s *fakeShard) Get(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCount++
	s.last = "Get:" + key
	if s.getErr != nil {
		return nil, s.getErr
	}
	return append([]byte(nil), s.value...), nil
}

func (s *fakeShard) Set(key string, value []byte, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setCount++
	s.last = fmt.Sprintf("Set:%s:%d", key, ttlSeconds)
	if s.setErr != nil {
		return s.setErr
	}
	s.value = append([]byte(nil), value...)
	return nil
}

func (s *fakeShard) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCount++
	s.last = "Delete:" + key
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return nil
}

func (s *fakeShard) Touch(key string, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.touchCount++
	s.last = fmt.Sprintf("Touch:%s:%d", key, ttlSeconds)
	if s.touchErr != nil {
		return s.touchErr
	}
	return nil
}

func (s *fakeShard) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getWithContextCount++
	s.last = "GetWithContext:" + key
	if s.ctxErr != nil {
		return nil, s.ctxErr
	}
	return append([]byte(nil), s.value...), nil
}

func (s *fakeShard) SetWithContext(ctx context.Context, key string, value []byte, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setWithContextCount++
	s.last = fmt.Sprintf("SetWithContext:%s:%d", key, ttlSeconds)
	if s.ctxErr != nil {
		return s.ctxErr
	}
	s.value = append([]byte(nil), value...)
	return nil
}

func (s *fakeShard) DeleteWithContext(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = "DeleteWithContext:" + key
	if s.ctxErr != nil {
		return s.ctxErr
	}
	return nil
}

func (s *fakeShard) TouchWithContext(ctx context.Context, key string, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = fmt.Sprintf("TouchWithContext:%s:%d", key, ttlSeconds)
	if s.ctxErr != nil {
		return s.ctxErr
	}
	return nil
}

func (s *fakeShard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func withTestFactory(f shardFactory) ClusterOption {
	return func(c *clusterConfig) error {
		c.factory = f
		return nil
	}
}

func TestOptionValidation(t *testing.T) {
	_, err := NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: 1}}, WithDistribution(Distribution(99)))
	if err == nil {
		t.Fatalf("expected invalid distribution error")
	}
	_, err = NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: 1}}, WithHash(Hash(99)))
	if err == nil {
		t.Fatalf("expected invalid hash error")
	}
}

func TestLibketamaOverridesRegardlessOfOptionOrder(t *testing.T) {
	c1, err := NewCluster(
		[]Server{{Addr: "127.0.0.1:1", Weight: 1}, {Addr: "127.0.0.1:2", Weight: 1}},
		WithDistribution(DistributionModula),
		WithHash(HashCRC32),
		WithLibketamaCompatible(true),
	)
	if err != nil {
		t.Fatalf("new cluster c1: %v", err)
	}
	defer c1.Close()
	if c1.distribution != DistributionConsistent || c1.hash != HashMD5 {
		t.Fatalf("libketama must force consistent+md5, got dist=%v hash=%v", c1.distribution, c1.hash)
	}

	c2, err := NewCluster(
		[]Server{{Addr: "127.0.0.1:1", Weight: 1}, {Addr: "127.0.0.1:2", Weight: 1}},
		WithLibketamaCompatible(true),
		WithDistribution(DistributionModula),
		WithHash(HashCRC32),
	)
	if err != nil {
		t.Fatalf("new cluster c2: %v", err)
	}
	defer c2.Close()
	if c2.distribution != DistributionConsistent || c2.hash != HashMD5 {
		t.Fatalf("libketama must force consistent+md5, got dist=%v hash=%v", c2.distribution, c2.hash)
	}
}

func TestValidateServersWeightZeroBecomesOne(t *testing.T) {
	c, err := NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: 0}})
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()
	st := c.loadState()
	if st.servers[0].Weight != 1 {
		t.Fatalf("weight 0 must normalize to 1, got %d", st.servers[0].Weight)
	}
}

func TestClusterRoutingAndDelegation(t *testing.T) {
	fakeByAddr := map[string]*fakeShard{}
	factory := func(addr string, opts ...mcturbo.Option) (shardClient, error) {
		s := &fakeShard{value: []byte("from-" + addr)}
		fakeByAddr[addr] = s
		return s, nil
	}
	servers := []Server{{Addr: "127.0.0.1:20001", Weight: 1}, {Addr: "127.0.0.1:20002", Weight: 1}}
	c, err := NewCluster(servers, WithDistribution(DistributionModula), withTestFactory(factory))
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	key := "route-key"
	st := c.loadState()
	idx := st.router.Pick(key)
	target := fakeByAddr[servers[idx].Addr]
	other := fakeByAddr[servers[(idx+1)%len(servers)].Addr]

	if _, err := c.GetNoContext(key); err != nil {
		t.Fatalf("GetNoContext: %v", err)
	}
	if target.getCount != 1 || other.getCount != 0 {
		t.Fatalf("expected get on target only: target=%d other=%d", target.getCount, other.getCount)
	}

	ctx := context.Background()
	if _, err := c.GetWithContext(ctx, key); err != nil {
		t.Fatalf("GetWithContext: %v", err)
	}
	if target.getWithContextCount != 1 {
		t.Fatalf("expected GetWithContext on target")
	}

	if err := c.SetNoContext(key, []byte("v1"), 10); err != nil {
		t.Fatalf("SetNoContext: %v", err)
	}
	if err := c.SetWithContext(ctx, key, []byte("v2"), 11); err != nil {
		t.Fatalf("SetWithContext: %v", err)
	}
	if target.setCount != 1 || target.setWithContextCount != 1 {
		t.Fatalf("expected both set paths on target: set=%d setCtx=%d", target.setCount, target.setWithContextCount)
	}
}

func TestClusterNoFailoverOnShardError(t *testing.T) {
	errShard := errors.New("shard failed")
	fakeByAddr := map[string]*fakeShard{}
	factory := func(addr string, opts ...mcturbo.Option) (shardClient, error) {
		s := &fakeShard{value: []byte("ok")}
		fakeByAddr[addr] = s
		return s, nil
	}
	servers := []Server{{Addr: "127.0.0.1:20101", Weight: 1}, {Addr: "127.0.0.1:20102", Weight: 1}}
	c, err := NewCluster(servers, withTestFactory(factory))
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	key := "route-key-err"
	st := c.loadState()
	idx := st.router.Pick(key)
	target := fakeByAddr[servers[idx].Addr]
	other := fakeByAddr[servers[(idx+1)%len(servers)].Addr]
	target.getErr = errShard

	_, err = c.GetNoContext(key)
	if !errors.Is(err, errShard) {
		t.Fatalf("expected target error, got %v", err)
	}
	if target.getCount != 1 || other.getCount != 0 {
		t.Fatalf("must not failover to other shard: target=%d other=%d", target.getCount, other.getCount)
	}
}

func TestUpdateServersPreservesRouterConfiguration(t *testing.T) {
	c, err := NewCluster(
		[]Server{{Addr: "127.0.0.1:21001", Weight: 1}, {Addr: "127.0.0.1:21002", Weight: 1}},
		WithDistribution(DistributionConsistent),
		WithHash(HashMD5),
	)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	if err := c.UpdateServers([]Server{{Addr: "127.0.0.1:21001", Weight: 1}, {Addr: "127.0.0.1:21003", Weight: 1}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	st := c.loadState()
	if _, ok := st.router.(*ketamaRouter); !ok {
		t.Fatalf("router type must stay ketama after update")
	}
	if c.distribution != DistributionConsistent || c.hash != HashMD5 {
		t.Fatalf("cluster routing settings must stay unchanged")
	}
}
