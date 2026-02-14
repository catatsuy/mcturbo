//go:build integration

package mcturbo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

var (
	integrationAddr string
	integrationCmd  *exec.Cmd
)

func TestMain(m *testing.M) {
	if _, err := exec.LookPath("memcached"); err != nil {
		os.Exit(m.Run())
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "listen:", err)
		os.Exit(1)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	integrationAddr = fmt.Sprintf("127.0.0.1:%d", port)
	integrationCmd = exec.Command("memcached", "-l", "127.0.0.1", "-p", fmt.Sprintf("%d", port), "-U", "0")
	integrationCmd.Stdout = os.Stdout
	integrationCmd.Stderr = os.Stderr
	if err := integrationCmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start memcached:", err)
		os.Exit(1)
	}

	ready := false
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", integrationAddr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			ready = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		_ = integrationCmd.Process.Kill()
		_, _ = integrationCmd.Process.Wait()
		os.Exit(1)
	}

	code := m.Run()

	_ = integrationCmd.Process.Kill()
	_, _ = integrationCmd.Process.Wait()
	os.Exit(code)
}

func newIntegrationClient(t *testing.T) *Client {
	t.Helper()
	if integrationAddr == "" {
		t.Skip("memcached command is not available")
	}
	c, err := New(integrationAddr, WithWorkers(2))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return c
}

func TestIntegrationSetGetDeleteTTL(t *testing.T) {
	c := newIntegrationClient(t)
	defer c.Close()

	if err := c.Set("int:key1", []byte("abc"), 5); err != nil {
		t.Fatalf("set: %v", err)
	}
	v, err := c.Get("int:key1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "abc" {
		t.Fatalf("value mismatch: %q", string(v))
	}

	if err := c.Delete("int:key1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = c.Get("int:key1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found after delete, got %v", err)
	}

	if err := c.Set("int:ttl", []byte("x"), 1); err != nil {
		t.Fatalf("set ttl: %v", err)
	}
	time.Sleep(1300 * time.Millisecond)
	_, err = c.Get("int:ttl")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired key, got %v", err)
	}
}

func TestIntegrationLargeAndMultilineValue(t *testing.T) {
	c := newIntegrationClient(t)
	defer c.Close()

	payload := bytes.Repeat([]byte("0123456789abcdef"), 60000)
	payload = append(payload, []byte("\nline\nvalue\n")...)

	if err := c.Set("int:large", payload, 5); err != nil {
		t.Fatalf("set large: %v", err)
	}
	got, err := c.Get("int:large")
	if err != nil {
		t.Fatalf("get large: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestIntegrationContextDeadline(t *testing.T) {
	c := newIntegrationClient(t)
	defer c.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := c.SetWithContext(ctx, "int:past", []byte("x"), 5); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
}
