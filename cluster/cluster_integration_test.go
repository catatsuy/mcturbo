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

	if err := c.SetNoContext("cluster:modula:nc", []byte("nc-v"), 5); err != nil {
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
	if err := c.SetWithContext(ctx, "cluster:modula:ctx", []byte("ctx-v"), 5); err != nil {
		t.Fatalf("set with context: %v", err)
	}
	v, err = c.GetWithContext(ctx, "cluster:modula:ctx")
	if err != nil {
		t.Fatalf("get with context: %v", err)
	}
	if string(v.Value) != "ctx-v" {
		t.Fatalf("unexpected value: %q", string(v.Value))
	}

	if err := c.AddNoContext("cluster:modula:add", []byte("a"), 5); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := c.ReplaceNoContext("cluster:modula:add", []byte("b"), 5); err != nil {
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
}

func TestIntegrationConsistentSetGet(t *testing.T) {
	c := newIntegrationCluster(t, WithDistribution(DistributionConsistent), WithHash(HashMD5))
	defer c.Close()

	if err := c.SetNoContext("cluster:consistent:nc", []byte("v"), 5); err != nil {
		t.Fatalf("set no context: %v", err)
	}
	if _, err := c.GetNoContext("cluster:consistent:nc"); err != nil {
		t.Fatalf("get no context: %v", err)
	}

	if err := c.SetNoContext("cluster:consistent:counter", []byte("10"), 5); err != nil {
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
		if err := c.SetNoContext(k, []byte("v"), 30); err != nil {
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

	if err := c.SetNoContext("cluster:update:before", []byte("v1"), 30); err != nil {
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
	if err := c.SetWithContext(ctx, "cluster:update:after", []byte("v2"), 30); err != nil {
		t.Fatalf("set after update: %v", err)
	}
	if _, err := c.GetWithContext(ctx, "cluster:update:after"); err != nil {
		t.Fatalf("get after update: %v", err)
	}
}
