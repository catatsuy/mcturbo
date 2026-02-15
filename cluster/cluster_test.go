package cluster

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/catatsuy/mcturbo"
)

type fixedRouter struct {
	idx int
}

func (r *fixedRouter) Pick(_ string) int {
	return r.idx
}

func TestDefaultDistributionIsModula(t *testing.T) {
	c, err := NewCluster([]Server{{Addr: "127.0.0.1:11111", Weight: 1}})
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	if c.distribution != DistributionModula {
		t.Fatalf("default distribution must be modula: got=%v", c.distribution)
	}
	if c.hash != HashDefault {
		t.Fatalf("default hash must be HashDefault: got=%v", c.hash)
	}
	st := c.loadState()
	if _, ok := st.router.(*modulaRouter); !ok {
		t.Fatalf("default router must be modulaRouter")
	}
}

func TestWithDistributionConsistentUsesKetamaRouter(t *testing.T) {
	c, err := NewCluster(
		[]Server{{Addr: "127.0.0.1:11111", Weight: 1}, {Addr: "127.0.0.1:11112", Weight: 1}},
		WithDistribution(DistributionConsistent),
	)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()
	st := c.loadState()
	if _, ok := st.router.(*ketamaRouter); !ok {
		t.Fatalf("router must be ketamaRouter when consistent distribution is selected")
	}
}

func TestWithLibketamaCompatibleForcesConsistentAndMD5(t *testing.T) {
	c, err := NewCluster(
		[]Server{{Addr: "127.0.0.1:11111", Weight: 1}, {Addr: "127.0.0.1:11112", Weight: 1}},
		WithDistribution(DistributionModula),
		WithHash(HashCRC32),
		WithLibketamaCompatible(true),
	)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	if c.distribution != DistributionConsistent {
		t.Fatalf("libketama must force consistent distribution: got=%v", c.distribution)
	}
	if c.hash != HashMD5 {
		t.Fatalf("libketama must force md5 hash: got=%v", c.hash)
	}
	if _, ok := c.loadState().router.(*ketamaRouter); !ok {
		t.Fatalf("libketama must use ketamaRouter")
	}
}

func TestModulaDeterministic(t *testing.T) {
	r, err := newRouter(
		[]Server{{Addr: "127.0.0.1:10001", Weight: 1}, {Addr: "127.0.0.1:10002", Weight: 1}},
		DistributionModula,
		HashDefault,
		defaultVnodeFactor,
	)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	for i := range 5000 {
		k := fmt.Sprintf("k-%d", i)
		a := r.Pick(k)
		b := r.Pick(k)
		if a != b {
			t.Fatalf("modula must be deterministic: key=%s", k)
		}
		if a < 0 || a >= 2 {
			t.Fatalf("invalid shard index: %d", a)
		}
	}
}

func TestConsistentMovementLessThanModuloOnAddServer(t *testing.T) {
	oldServers := []Server{
		{Addr: "127.0.0.1:10001", Weight: 1},
		{Addr: "127.0.0.1:10002", Weight: 1},
		{Addr: "127.0.0.1:10003", Weight: 1},
	}
	newServers := append(append([]Server{}, oldServers...), Server{Addr: "127.0.0.1:10004", Weight: 1})

	oldR, err := newRouter(oldServers, DistributionConsistent, HashMD5, defaultVnodeFactor)
	if err != nil {
		t.Fatalf("old router: %v", err)
	}
	newR, err := newRouter(newServers, DistributionConsistent, HashMD5, defaultVnodeFactor)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	movedConsistent := 0
	movedModulo := 0
	const n = 10000
	for i := range n {
		k := fmt.Sprintf("k-%d", i)
		if oldR.Pick(k) != newR.Pick(k) {
			movedConsistent++
		}
		oldMod := int(hashMD5Uint32(k) % uint32(len(oldServers)))
		newMod := int(hashMD5Uint32(k) % uint32(len(newServers)))
		if oldMod != newMod {
			movedModulo++
		}
	}
	if movedConsistent >= movedModulo {
		t.Fatalf("expected consistent to move fewer keys: consistent=%d modulo=%d", movedConsistent, movedModulo)
	}
}

func TestConsistentWeightEffect(t *testing.T) {
	r, err := newRouter(
		[]Server{{Addr: "127.0.0.1:10001", Weight: 1}, {Addr: "127.0.0.1:10002", Weight: 3}},
		DistributionConsistent,
		HashMD5,
		defaultVnodeFactor,
	)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}

	var c0, c1 int
	for i := range 20000 {
		k := fmt.Sprintf("wk-%d", i)
		if r.Pick(k) == 0 {
			c0++
		} else {
			c1++
		}
	}
	if c1 <= c0*2 {
		t.Fatalf("weight not reflected enough: w1=%d w3=%d", c0, c1)
	}
}

func TestUpdateServersReuseAndCloseRemoved(t *testing.T) {
	c, err := NewCluster([]Server{{Addr: "127.0.0.1:11111", Weight: 1}, {Addr: "127.0.0.1:11112", Weight: 1}})
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	old := c.loadState()
	oldKeep := old.byAddr["127.0.0.1:11111"]
	oldDrop := old.byAddr["127.0.0.1:11112"]

	if err := c.UpdateServers([]Server{{Addr: "127.0.0.1:11111", Weight: 1}, {Addr: "127.0.0.1:11113", Weight: 1}}); err != nil {
		t.Fatalf("update: %v", err)
	}

	st := c.loadState()
	if st.byAddr["127.0.0.1:11111"] != oldKeep {
		t.Fatalf("expected shard reuse for unchanged addr")
	}
	if st.byAddr["127.0.0.1:11113"] == nil {
		t.Fatalf("expected new shard for added addr")
	}

	_, err = oldDrop.Get("k")
	if !errors.Is(err, mcturbo.ErrClosed) {
		t.Fatalf("expected removed shard to be closed, got: %v", err)
	}
}

func TestUpdateServersConcurrentSafety(t *testing.T) {
	c, err := NewCluster([]Server{{Addr: "127.0.0.1:12001", Weight: 1}, {Addr: "127.0.0.1:12002", Weight: 1}})
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Go(func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				_ = c.UpdateServers([]Server{{Addr: "127.0.0.1:12001", Weight: 1}, {Addr: "127.0.0.1:12003", Weight: 1}})
			} else {
				_ = c.UpdateServers([]Server{{Addr: "127.0.0.1:12001", Weight: 1}, {Addr: "127.0.0.1:12002", Weight: 1}})
			}
			i++
		}
	})

	for g := range 8 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 500 {
				_, _ = c.GetNoContext(fmt.Sprintf("k-%d-%d", id, i))
			}
		}(g)
	}

	close(stop)
	wg.Wait()
}

func TestClusterClose(t *testing.T) {
	c, err := NewCluster([]Server{{Addr: "127.0.0.1:13001", Weight: 1}})
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := c.GetNoContext("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if err := c.UpdateServers([]Server{{Addr: "127.0.0.1:13001", Weight: 1}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on update, got %v", err)
	}
}

func TestValidateServers(t *testing.T) {
	_, err := NewCluster(nil)
	if err == nil {
		t.Fatalf("expected error for empty servers")
	}
	_, err = NewCluster([]Server{{Addr: "", Weight: 1}})
	if err == nil {
		t.Fatalf("expected error for empty addr")
	}
	_, err = NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: -1}})
	if err == nil {
		t.Fatalf("expected error for negative weight")
	}
	_, err = NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: 1}, {Addr: "127.0.0.1:1", Weight: 1}})
	if err == nil {
		t.Fatalf("expected error for duplicate addr")
	}
}

func TestFailoverOptionValidation(t *testing.T) {
	_, err := NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: 1}}, WithServerFailureLimit(0))
	if err == nil {
		t.Fatalf("expected error for invalid server failure limit")
	}
	_, err = NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: 1}}, WithRetryTimeout(0))
	if err == nil {
		t.Fatalf("expected error for invalid retry timeout")
	}
}

func TestWithRouterFactoryValidation(t *testing.T) {
	_, err := NewCluster([]Server{{Addr: "127.0.0.1:1", Weight: 1}}, WithRouterFactory(nil))
	if err == nil {
		t.Fatalf("expected error for nil router factory")
	}
}

func TestWithRouterFactoryIsUsed(t *testing.T) {
	var called atomic.Int32
	fakeByAddr := map[string]*fakeShard{}
	factory := func(addr string, opts ...mcturbo.Option) (shardClient, error) {
		s := &fakeShard{value: []byte("from-" + addr)}
		fakeByAddr[addr] = s
		return s, nil
	}
	routerFactory := func(servers []Server, dist Distribution, hash Hash, vnode int) (Router, error) {
		called.Add(1)
		if len(servers) != 2 {
			t.Fatalf("unexpected server count: %d", len(servers))
		}
		return &fixedRouter{idx: 1}, nil
	}

	c, err := NewCluster(
		[]Server{{Addr: "127.0.0.1:21011", Weight: 1}, {Addr: "127.0.0.1:21012", Weight: 1}},
		withTestFactory(factory),
		WithRouterFactory(routerFactory),
	)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	if called.Load() != 1 {
		t.Fatalf("router factory should be called once on NewCluster, got %d", called.Load())
	}

	if _, err := c.GetNoContext("any-key"); err != nil {
		t.Fatalf("get no context: %v", err)
	}
	if fakeByAddr["127.0.0.1:21011"].getCount != 0 || fakeByAddr["127.0.0.1:21012"].getCount != 1 {
		t.Fatalf("custom router must route to second shard")
	}

	if err := c.UpdateServers([]Server{{Addr: "127.0.0.1:21011", Weight: 1}, {Addr: "127.0.0.1:21012", Weight: 1}}); err != nil {
		t.Fatalf("update servers: %v", err)
	}
	if called.Load() != 2 {
		t.Fatalf("router factory should be called on UpdateServers, got %d", called.Load())
	}
}

func TestBuiltInRouterFactories(t *testing.T) {
	modulaFactory := ModulaRouterFactory(HashCRC32)
	r1, err := modulaFactory([]Server{{Addr: "127.0.0.1:1", Weight: 1}}, DistributionConsistent, HashMD5, 123)
	if err != nil {
		t.Fatalf("modula factory: %v", err)
	}
	if _, ok := r1.(*modulaRouter); !ok {
		t.Fatalf("ModulaRouterFactory must return modulaRouter")
	}

	consistentFactory := ConsistentRouterFactory(HashMD5, defaultVnodeFactor)
	r2, err := consistentFactory(
		[]Server{{Addr: "127.0.0.1:1", Weight: 1}, {Addr: "127.0.0.1:2", Weight: 1}},
		DistributionModula,
		HashCRC32,
		1,
	)
	if err != nil {
		t.Fatalf("consistent factory: %v", err)
	}
	if _, ok := r2.(*ketamaRouter); !ok {
		t.Fatalf("ConsistentRouterFactory must return ketamaRouter")
	}
}
