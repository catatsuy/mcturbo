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

	getCount                int
	getsCount               int
	setCount                int
	addCount                int
	replaceCount            int
	appendCount             int
	prependCount            int
	deleteCount             int
	touchCount              int
	incrCount               int
	decrCount               int
	getWithContextCount     int
	setWithContextCount     int
	addWithContextCount     int
	replaceWithContextCount int
	appendWithContextCount  int
	prependWithContextCount int
	incrWithContextCount    int
	decrWithContextCount    int
	casCount                int
	casWithContextCount     int

	getErr    error
	setErr    error
	deleteErr error
	touchErr  error
	ctxErr    error

	value []byte
	last  string

	closed bool
}

func (s *fakeShard) Get(key string) (*mcturbo.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCount++
	s.last = "Get:" + key
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &mcturbo.Item{Value: append([]byte(nil), s.value...)}, nil
}

func (s *fakeShard) Gets(key string) (*mcturbo.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getsCount++
	s.last = "Gets:" + key
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &mcturbo.Item{Value: append([]byte(nil), s.value...), CAS: 1}, nil
}

func (s *fakeShard) Set(key string, value []byte, flags uint32, ttlSeconds int) error {
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

func (s *fakeShard) Add(key string, value []byte, flags uint32, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addCount++
	s.last = fmt.Sprintf("Add:%s:%d", key, ttlSeconds)
	if s.setErr != nil {
		return s.setErr
	}
	s.value = append([]byte(nil), value...)
	return nil
}

func (s *fakeShard) Replace(key string, value []byte, flags uint32, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replaceCount++
	s.last = fmt.Sprintf("Replace:%s:%d", key, ttlSeconds)
	if s.setErr != nil {
		return s.setErr
	}
	s.value = append([]byte(nil), value...)
	return nil
}

func (s *fakeShard) Append(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendCount++
	s.last = "Append:" + key
	if s.setErr != nil {
		return s.setErr
	}
	s.value = append(s.value, value...)
	return nil
}

func (s *fakeShard) Prepend(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prependCount++
	s.last = "Prepend:" + key
	if s.setErr != nil {
		return s.setErr
	}
	nv := make([]byte, 0, len(value)+len(s.value))
	nv = append(nv, value...)
	nv = append(nv, s.value...)
	s.value = nv
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

func (s *fakeShard) GetAndTouch(key string, ttlSeconds int) (*mcturbo.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = fmt.Sprintf("GetAndTouch:%s:%d", key, ttlSeconds)
	if s.getErr != nil {
		return nil, s.getErr
	}
	return &mcturbo.Item{Value: append([]byte(nil), s.value...)}, nil
}

func (s *fakeShard) Incr(key string, delta uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incrCount++
	s.last = fmt.Sprintf("Incr:%s:%d", key, delta)
	if s.setErr != nil {
		return 0, s.setErr
	}
	return delta + 1, nil
}

func (s *fakeShard) Decr(key string, delta uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decrCount++
	s.last = fmt.Sprintf("Decr:%s:%d", key, delta)
	if s.setErr != nil {
		return 0, s.setErr
	}
	if delta == 0 {
		return 0, nil
	}
	return delta - 1, nil
}

func (s *fakeShard) CAS(key string, value []byte, flags uint32, ttlSeconds int, cas uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.casCount++
	s.last = fmt.Sprintf("CAS:%s:%d:%d", key, ttlSeconds, cas)
	if s.setErr != nil {
		return s.setErr
	}
	s.value = append([]byte(nil), value...)
	return nil
}

func (s *fakeShard) GetWithContext(ctx context.Context, key string) (*mcturbo.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getWithContextCount++
	s.last = "GetWithContext:" + key
	if s.ctxErr != nil {
		return nil, s.ctxErr
	}
	return &mcturbo.Item{Value: append([]byte(nil), s.value...)}, nil
}

func (s *fakeShard) GetsWithContext(ctx context.Context, key string) (*mcturbo.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getWithContextCount++
	s.last = "GetsWithContext:" + key
	if s.ctxErr != nil {
		return nil, s.ctxErr
	}
	return &mcturbo.Item{Value: append([]byte(nil), s.value...), CAS: 1}, nil
}

func (s *fakeShard) SetWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
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

func (s *fakeShard) AddWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addWithContextCount++
	s.last = fmt.Sprintf("AddWithContext:%s:%d", key, ttlSeconds)
	if s.ctxErr != nil {
		return s.ctxErr
	}
	s.value = append([]byte(nil), value...)
	return nil
}

func (s *fakeShard) ReplaceWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replaceWithContextCount++
	s.last = fmt.Sprintf("ReplaceWithContext:%s:%d", key, ttlSeconds)
	if s.ctxErr != nil {
		return s.ctxErr
	}
	s.value = append([]byte(nil), value...)
	return nil
}

func (s *fakeShard) AppendWithContext(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendWithContextCount++
	s.last = "AppendWithContext:" + key
	if s.ctxErr != nil {
		return s.ctxErr
	}
	s.value = append(s.value, value...)
	return nil
}

func (s *fakeShard) PrependWithContext(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prependWithContextCount++
	s.last = "PrependWithContext:" + key
	if s.ctxErr != nil {
		return s.ctxErr
	}
	nv := make([]byte, 0, len(value)+len(s.value))
	nv = append(nv, value...)
	nv = append(nv, s.value...)
	s.value = nv
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

func (s *fakeShard) GetAndTouchWithContext(ctx context.Context, key string, ttlSeconds int) (*mcturbo.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = fmt.Sprintf("GetAndTouchWithContext:%s:%d", key, ttlSeconds)
	if s.ctxErr != nil {
		return nil, s.ctxErr
	}
	return &mcturbo.Item{Value: append([]byte(nil), s.value...)}, nil
}

func (s *fakeShard) IncrWithContext(ctx context.Context, key string, delta uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.incrWithContextCount++
	s.last = fmt.Sprintf("IncrWithContext:%s:%d", key, delta)
	if s.ctxErr != nil {
		return 0, s.ctxErr
	}
	return delta + 1, nil
}

func (s *fakeShard) DecrWithContext(ctx context.Context, key string, delta uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decrWithContextCount++
	s.last = fmt.Sprintf("DecrWithContext:%s:%d", key, delta)
	if s.ctxErr != nil {
		return 0, s.ctxErr
	}
	if delta == 0 {
		return 0, nil
	}
	return delta - 1, nil
}

func (s *fakeShard) CASWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int, cas uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.casWithContextCount++
	s.last = fmt.Sprintf("CASWithContext:%s:%d:%d", key, ttlSeconds, cas)
	if s.ctxErr != nil {
		return s.ctxErr
	}
	s.value = append([]byte(nil), value...)
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
	if _, err := c.GetsNoContext(key); err != nil {
		t.Fatalf("GetsNoContext: %v", err)
	}

	if err := c.SetNoContext(key, []byte("v1"), 0, 10); err != nil {
		t.Fatalf("SetNoContext: %v", err)
	}
	if err := c.SetWithContext(ctx, key, []byte("v2"), 0, 11); err != nil {
		t.Fatalf("SetWithContext: %v", err)
	}
	if target.setCount != 1 || target.setWithContextCount != 1 {
		t.Fatalf("expected both set paths on target: set=%d setCtx=%d", target.setCount, target.setWithContextCount)
	}
	if err := c.AddNoContext(key, []byte("a"), 0, 12); err != nil {
		t.Fatalf("AddNoContext: %v", err)
	}
	if err := c.ReplaceWithContext(ctx, key, []byte("b"), 0, 13); err != nil {
		t.Fatalf("ReplaceWithContext: %v", err)
	}
	if err := c.AppendNoContext(key, []byte("c")); err != nil {
		t.Fatalf("AppendNoContext: %v", err)
	}
	if err := c.PrependWithContext(ctx, key, []byte("d")); err != nil {
		t.Fatalf("PrependWithContext: %v", err)
	}
	if _, err := c.GetAndTouchNoContext(key, 30); err != nil {
		t.Fatalf("GetAndTouchNoContext: %v", err)
	}
	if _, err := c.IncrNoContext(key, 3); err != nil {
		t.Fatalf("IncrNoContext: %v", err)
	}
	if _, err := c.DecrWithContext(ctx, key, 1); err != nil {
		t.Fatalf("DecrWithContext: %v", err)
	}
	if err := c.CASNoContext(key, []byte("z"), 1, 30, 1); err != nil {
		t.Fatalf("CASNoContext: %v", err)
	}
	if target.incrCount != 1 || target.decrWithContextCount != 1 {
		t.Fatalf("expected incr/decr delegation on target: incr=%d decrCtx=%d", target.incrCount, target.decrWithContextCount)
	}
	if target.addCount != 1 || target.replaceWithContextCount != 1 || target.appendCount != 1 || target.prependWithContextCount != 1 || target.casCount != 1 {
		t.Fatalf("expected add/replace/append/prepend delegation on target")
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
