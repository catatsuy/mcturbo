//go:build integration

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/catatsuy/mcturbo"
)

var (
	integrationServers []Server
	integrationCmds    []*exec.Cmd
)

func TestMain(m *testing.M) {
	if _, err := exec.LookPath("memcached"); err != nil {
		os.Exit(m.Run())
	}

	servers, cmds, ok := startMemcachedN(3)
	if ok {
		integrationServers = servers
		integrationCmds = cmds
	}

	code := m.Run()
	for i := range integrationCmds {
		if integrationCmds[i] != nil && integrationCmds[i].Process != nil {
			_ = integrationCmds[i].Process.Kill()
			_, _ = integrationCmds[i].Process.Wait()
		}
	}
	os.Exit(code)
}

func startMemcachedN(n int) ([]Server, []*exec.Cmd, bool) {
	servers := make([]Server, 0, n)
	cmds := make([]*exec.Cmd, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, nil, false
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		addr := fmt.Sprintf("127.0.0.1:%d", port)
		cmd := exec.Command("memcached", "-l", "127.0.0.1", "-p", fmt.Sprintf("%d", port), "-U", "0")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			for j := range cmds {
				_ = cmds[j].Process.Kill()
				_, _ = cmds[j].Process.Wait()
			}
			return nil, nil, false
		}
		ready := false
		for r := 0; r < 50; r++ {
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				ready = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !ready {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
			for j := range cmds {
				_ = cmds[j].Process.Kill()
				_, _ = cmds[j].Process.Wait()
			}
			return nil, nil, false
		}
		servers = append(servers, Server{Addr: addr, Weight: 1})
		cmds = append(cmds, cmd)
	}
	return servers, cmds, true
}

func newIntegrationCluster(t *testing.T, opts ...ClusterOption) *Cluster {
	t.Helper()
	if len(integrationServers) == 0 {
		t.Skip("memcached command is not available")
	}
	c, err := NewCluster(integrationServers, opts...)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	return c
}

func TestIntegrationModulaSetGet(t *testing.T) {
	c := newIntegrationCluster(t, WithDistribution(DistributionModula))
	defer c.Close()

	if err := c.SetNoContext("cluster:modula:nc", []byte("nc-v"), 0, 5); err != nil {
		t.Fatalf("set no context: %v", err)
	}
	v, err := c.GetNoContext("cluster:modula:nc")
	if err != nil {
		t.Fatalf("get no context: %v", err)
	}
	if string(v.Value) != "nc-v" {
		t.Fatalf("unexpected value: %q", string(v.Value))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.SetWithContext(ctx, "cluster:modula:ctx", []byte("ctx-v"), 0, 5); err != nil {
		t.Fatalf("set with context: %v", err)
	}
	v, err = c.GetWithContext(ctx, "cluster:modula:ctx")
	if err != nil {
		t.Fatalf("get with context: %v", err)
	}
	if string(v.Value) != "ctx-v" {
		t.Fatalf("unexpected value: %q", string(v.Value))
	}

	if err := c.AddNoContext("cluster:modula:add", []byte("a"), 0, 5); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := c.ReplaceNoContext("cluster:modula:add", []byte("b"), 0, 5); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if err := c.AppendNoContext("cluster:modula:add", []byte("x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := c.PrependNoContext("cluster:modula:add", []byte("y")); err != nil {
		t.Fatalf("prepend: %v", err)
	}
	v, err = c.GetAndTouchNoContext("cluster:modula:add", 6)
	if err != nil {
		t.Fatalf("get and touch: %v", err)
	}
	if string(v.Value) != "ybx" {
		t.Fatalf("unexpected merged value: %q", string(v.Value))
	}

	if err := c.SetNoContext("cluster:modula:m1", []byte("mv1"), 1, 5); err != nil {
		t.Fatalf("set m1: %v", err)
	}
	if err := c.SetNoContext("cluster:modula:m2", []byte("mv2"), 2, 5); err != nil {
		t.Fatalf("set m2: %v", err)
	}
	m, err := c.GetMulti([]string{"cluster:modula:m1", "cluster:modula:m2", "cluster:modula:missing"})
	if err != nil {
		t.Fatalf("getmulti no context: %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("unexpected getmulti result size: %d", len(m))
	}
	m, err = c.GetMultiWithContext(ctx, []string{"cluster:modula:m1", "cluster:modula:m2"})
	if err != nil {
		t.Fatalf("getmulti with context: %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("unexpected getmulti(ctx) result size: %d", len(m))
	}
}

func TestIntegrationConsistentSetGet(t *testing.T) {
	c := newIntegrationCluster(t, WithDistribution(DistributionConsistent), WithHash(HashMD5))
	defer c.Close()

	if err := c.SetNoContext("cluster:consistent:nc", []byte("v"), 0, 5); err != nil {
		t.Fatalf("set no context: %v", err)
	}
	if _, err := c.GetNoContext("cluster:consistent:nc"); err != nil {
		t.Fatalf("get no context: %v", err)
	}

	if err := c.SetNoContext("cluster:consistent:counter", []byte("10"), 0, 5); err != nil {
		t.Fatalf("set counter: %v", err)
	}
	n, err := c.IncrNoContext("cluster:consistent:counter", 2)
	if err != nil {
		t.Fatalf("incr no context: %v", err)
	}
	if n != 12 {
		t.Fatalf("unexpected incr value: %d", n)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	n, err = c.DecrWithContext(ctx, "cluster:consistent:counter", 3)
	if err != nil {
		t.Fatalf("decr with context: %v", err)
	}
	if n != 9 {
		t.Fatalf("unexpected decr value: %d", n)
	}
}

func TestIntegrationPickDeterministic(t *testing.T) {
	c := newIntegrationCluster(t, WithDistribution(DistributionModula), WithHash(HashDefault))
	defer c.Close()

	st := c.loadState()
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("det:%d", i)
		a := st.router.Pick(k)
		b := st.router.Pick(k)
		if a != b {
			t.Fatalf("pick must be deterministic: key=%s", k)
		}
	}
}

func TestIntegrationDistribution(t *testing.T) {
	c := newIntegrationCluster(t, WithDistribution(DistributionConsistent), WithHash(HashMD5))
	defer c.Close()

	direct := make([]*mcturbo.Client, 0, len(integrationServers))
	for i := range integrationServers {
		dc, err := mcturbo.New(integrationServers[i].Addr)
		if err != nil {
			t.Fatalf("new direct client: %v", err)
		}
		direct = append(direct, dc)
		defer dc.Close()
	}

	const keyN = 120
	for i := 0; i < keyN; i++ {
		k := fmt.Sprintf("cluster:dist:%d", i)
		if err := c.SetNoContext(k, []byte("v"), 0, 30); err != nil {
			t.Fatalf("cluster set: %v", err)
		}
	}

	hits := make([]int, len(direct))
	for i := 0; i < keyN; i++ {
		k := fmt.Sprintf("cluster:dist:%d", i)
		found := 0
		for si := range direct {
			_, err := direct[si].Get(k)
			if err == nil {
				hits[si]++
				found++
				continue
			}
			if !errors.Is(err, mcturbo.ErrNotFound) {
				t.Fatalf("direct get: %v", err)
			}
		}
		if found != 1 {
			t.Fatalf("key %s found on %d servers", k, found)
		}
	}

	nonZero := 0
	for i := range hits {
		if hits[i] > 0 {
			nonZero++
		}
	}
	if nonZero < 2 {
		t.Fatalf("keys are not distributed: hits=%v", hits)
	}
}

func TestIntegrationUpdateServers(t *testing.T) {
	if len(integrationServers) < 3 {
		t.Skip("need 3 memcached servers")
	}
	c, err := NewCluster(integrationServers[:2], WithDistribution(DistributionConsistent), WithHash(HashMD5))
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	if err := c.SetNoContext("cluster:update:before", []byte("v1"), 0, 30); err != nil {
		t.Fatalf("set before update: %v", err)
	}
	if _, err := c.GetNoContext("cluster:update:before"); err != nil {
		t.Fatalf("get before update: %v", err)
	}

	if err := c.UpdateServers(integrationServers); err != nil {
		t.Fatalf("update servers: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.SetWithContext(ctx, "cluster:update:after", []byte("v2"), 0, 30); err != nil {
		t.Fatalf("set after update: %v", err)
	}
	if _, err := c.GetWithContext(ctx, "cluster:update:after"); err != nil {
		t.Fatalf("get after update: %v", err)
	}
}

func TestIntegrationCAS(t *testing.T) {
	c := newIntegrationCluster(t, WithDistribution(DistributionModula))
	defer c.Close()

	key := "cluster:cas:key"
	_ = c.DeleteNoContext(key)
	if err := c.SetNoContext(key, []byte("v1"), 1, 10); err != nil {
		t.Fatalf("set: %v", err)
	}
	it, err := c.GetsNoContext(key)
	if err != nil {
		t.Fatalf("gets: %v", err)
	}
	if it.CAS == 0 {
		t.Fatalf("expected cas value")
	}
	if err := c.CASNoContext(key, []byte("v2"), 2, 10, it.CAS); err != nil {
		t.Fatalf("cas: %v", err)
	}
	if err := c.CASNoContext(key, []byte("v3"), 2, 10, it.CAS); !errors.Is(err, mcturbo.ErrCASConflict) {
		t.Fatalf("expected cas conflict, got %v", err)
	}
}

func TestIntegrationPingAndFlushAll(t *testing.T) {
	c := newIntegrationCluster(t, WithDistribution(DistributionModula))
	defer c.Close()

	if err := c.PingNoContext(); err != nil {
		t.Fatalf("ping no context: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.PingWithContext(ctx); err != nil {
		t.Fatalf("ping with context: %v", err)
	}

	if err := c.SetNoContext("cluster:flush:key1", []byte("v1"), 0, 30); err != nil {
		t.Fatalf("set key1: %v", err)
	}
	if err := c.SetNoContext("cluster:flush:key2", []byte("v2"), 0, 30); err != nil {
		t.Fatalf("set key2: %v", err)
	}
	if err := c.FlushAllNoContext(); err != nil {
		t.Fatalf("flush all no context: %v", err)
	}
	if _, err := c.GetNoContext("cluster:flush:key1"); !errors.Is(err, mcturbo.ErrNotFound) {
		t.Fatalf("expected key1 not found after flush all, got %v", err)
	}

	if err := c.SetNoContext("cluster:flush:key3", []byte("v3"), 0, 30); err != nil {
		t.Fatalf("set key3: %v", err)
	}
	if err := c.FlushAllWithContext(ctx); err != nil {
		t.Fatalf("flush all with context: %v", err)
	}
	if _, err := c.GetWithContext(ctx, "cluster:flush:key3"); !errors.Is(err, mcturbo.ErrNotFound) {
		t.Fatalf("expected key3 not found after flush all with context, got %v", err)
	}
}

func TestIntegrationFailoverCommunicationError(t *testing.T) {
	if len(integrationServers) == 0 {
		t.Skip("memcached command is not available")
	}

	deadAddr := reserveUnusedAddr(t)
	servers := []Server{
		{Addr: deadAddr, Weight: 1},
		integrationServers[0],
	}
	c, err := NewCluster(
		servers,
		WithDistribution(DistributionModula),
		WithRemoveFailedServers(true),
		WithServerFailureLimit(1),
		WithRetryTimeout(200*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	defer c.Close()

	key := keyForShardIndex(t, c, 0)
	if err := c.SetNoContext(key, []byte("failover-v"), 0, 30); err != nil {
		t.Fatalf("set with failover: %v", err)
	}

	it, err := c.GetNoContext(key)
	if err != nil {
		t.Fatalf("get with failover: %v", err)
	}
	if string(it.Value) != "failover-v" {
		t.Fatalf("unexpected value after failover: %q", string(it.Value))
	}
}

func reserveUnusedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func keyForShardIndex(t *testing.T, c *Cluster, want int) string {
	t.Helper()
	st := c.loadState()
	if st == nil || st.router == nil {
		t.Fatalf("cluster state is not ready")
	}
	for i := 0; i < 100000; i++ {
		k := fmt.Sprintf("cluster:failover:key:%d", i)
		if st.router.Pick(k) == want {
			return k
		}
	}
	t.Fatalf("failed to find key for shard %d", want)
	return ""
}
