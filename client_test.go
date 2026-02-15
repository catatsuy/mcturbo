package mcturbo

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type testServer struct {
	ln      net.Listener
	handler func(net.Conn)
	wg      sync.WaitGroup
}

func newTestServer(t *testing.T, handler func(net.Conn)) *testServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ts := &testServer{ln: ln, handler: handler}
	ts.wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			ts.wg.Add(1)
			go func(c net.Conn) {
				defer ts.wg.Done()
				defer c.Close()
				handler(c)
			}(conn)
		}
	})
	return ts
}

func (s *testServer) Addr() string { return s.ln.Addr().String() }

func (s *testServer) Close() {
	_ = s.ln.Close()
	s.wg.Wait()
}

func TestClientBasicCommands(t *testing.T) {
	var (
		mu   sync.Mutex
		data = map[string][]byte{}
	)
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		bw := bufio.NewWriter(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return
				}
				return
			}
			line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
			parts := strings.Split(line, " ")
			if len(parts) == 0 {
				return
			}
			switch parts[0] {
			case "set":
				fallthrough
			case "add":
				fallthrough
			case "replace":
				fallthrough
			case "append":
				fallthrough
			case "prepend":
				if len(parts) != 5 {
					_, _ = bw.WriteString("CLIENT_ERROR bad\r\n")
					_ = bw.Flush()
					continue
				}
				n, _ := strconv.Atoi(parts[4])
				buf := make([]byte, n+2)
				if _, err := io.ReadFull(br, buf); err != nil {
					return
				}
				v := append([]byte(nil), buf[:n]...)
				mu.Lock()
				old, exists := data[parts[1]]
				switch parts[0] {
				case "add":
					if exists {
						mu.Unlock()
						_, _ = bw.WriteString("NOT_STORED\r\n")
						_ = bw.Flush()
						continue
					}
					data[parts[1]] = v
				case "replace":
					if !exists {
						mu.Unlock()
						_, _ = bw.WriteString("NOT_STORED\r\n")
						_ = bw.Flush()
						continue
					}
					data[parts[1]] = v
				case "append":
					if !exists {
						mu.Unlock()
						_, _ = bw.WriteString("NOT_STORED\r\n")
						_ = bw.Flush()
						continue
					}
					data[parts[1]] = append(old, v...)
				case "prepend":
					if !exists {
						mu.Unlock()
						_, _ = bw.WriteString("NOT_STORED\r\n")
						_ = bw.Flush()
						continue
					}
					nv := make([]byte, 0, len(v)+len(old))
					nv = append(nv, v...)
					nv = append(nv, old...)
					data[parts[1]] = nv
				default:
					data[parts[1]] = v
				}
				mu.Unlock()
				_, _ = bw.WriteString("STORED\r\n")
				_ = bw.Flush()
			case "get":
				if len(parts) != 2 {
					return
				}
				mu.Lock()
				v, ok := data[parts[1]]
				mu.Unlock()
				if ok {
					_, _ = bw.WriteString(fmt.Sprintf("VALUE %s 0 %d\r\n", parts[1], len(v)))
					_, _ = bw.Write(v)
					_, _ = bw.WriteString("\r\n")
				}
				_, _ = bw.WriteString("END\r\n")
				_ = bw.Flush()
			case "delete":
				if len(parts) != 2 {
					return
				}
				mu.Lock()
				_, ok := data[parts[1]]
				if ok {
					delete(data, parts[1])
				}
				mu.Unlock()
				if ok {
					_, _ = bw.WriteString("DELETED\r\n")
				} else {
					_, _ = bw.WriteString("NOT_FOUND\r\n")
				}
				_ = bw.Flush()
			case "touch":
				if len(parts) != 3 {
					return
				}
				mu.Lock()
				_, ok := data[parts[1]]
				mu.Unlock()
				if ok {
					_, _ = bw.WriteString("TOUCHED\r\n")
				} else {
					_, _ = bw.WriteString("NOT_FOUND\r\n")
				}
				_ = bw.Flush()
			case "gat":
				if len(parts) != 3 {
					return
				}
				mu.Lock()
				v, ok := data[parts[2]]
				mu.Unlock()
				if ok {
					_, _ = bw.WriteString(fmt.Sprintf("VALUE %s 0 %d\r\n", parts[2], len(v)))
					_, _ = bw.Write(v)
					_, _ = bw.WriteString("\r\n")
				}
				_, _ = bw.WriteString("END\r\n")
				_ = bw.Flush()
			case "version":
				_, _ = bw.WriteString("VERSION test\r\n")
				_ = bw.Flush()
			case "flush_all":
				mu.Lock()
				data = map[string][]byte{}
				mu.Unlock()
				_, _ = bw.WriteString("OK\r\n")
				_ = bw.Flush()
			case "incr", "decr":
				if len(parts) != 3 {
					return
				}
				delta, err := strconv.ParseUint(parts[2], 10, 64)
				if err != nil {
					_, _ = bw.WriteString("CLIENT_ERROR bad command line format\r\n")
					_ = bw.Flush()
					continue
				}
				mu.Lock()
				v, ok := data[parts[1]]
				if !ok {
					mu.Unlock()
					_, _ = bw.WriteString("NOT_FOUND\r\n")
					_ = bw.Flush()
					continue
				}
				n, err := strconv.ParseUint(string(v), 10, 64)
				if err != nil {
					mu.Unlock()
					_, _ = bw.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
					_ = bw.Flush()
					continue
				}
				if parts[0] == "incr" {
					n += delta
				} else if delta >= n {
					n = 0
				} else {
					n -= delta
				}
				data[parts[1]] = []byte(strconv.FormatUint(n, 10))
				mu.Unlock()
				_, _ = bw.WriteString(strconv.FormatUint(n, 10) + "\r\n")
				_ = bw.Flush()
			default:
				return
			}
		}
	})
	defer server.Close()

	c, err := New(server.Addr(), WithWorkers(2))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	if err := c.Set("k1", []byte("value-1"), 10); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := c.Add("k2", []byte("v2"), 10); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := c.Add("k2", []byte("x"), 10); !errors.Is(err, ErrNotStored) {
		t.Fatalf("second add should return ErrNotStored: %v", err)
	}
	if err := c.Replace("k2", []byte("r2"), 10); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if err := c.Append("k2", []byte("A")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := c.Prepend("k2", []byte("B")); err != nil {
		t.Fatalf("prepend: %v", err)
	}
	v2, err := c.GetAndTouch("k2", 20)
	if err != nil {
		t.Fatalf("get and touch: %v", err)
	}
	if string(v2) != "Br2A" {
		t.Fatalf("unexpected merged value: %q", string(v2))
	}
	if err := c.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	v, err := c.Get("k1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v) != "value-1" {
		t.Fatalf("value mismatch: %q", string(v))
	}
	if err := c.Touch("k1", 20); err != nil {
		t.Fatalf("touch: %v", err)
	}
	if err := c.Delete("k1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = c.Get("k1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	if err := c.Set("counter", []byte("10"), 10); err != nil {
		t.Fatalf("set counter: %v", err)
	}
	n, err := c.Incr("counter", 7)
	if err != nil {
		t.Fatalf("incr: %v", err)
	}
	if n != 17 {
		t.Fatalf("incr value mismatch: %d", n)
	}
	n, err = c.Decr("counter", 20)
	if err != nil {
		t.Fatalf("decr: %v", err)
	}
	if n != 0 {
		t.Fatalf("decr value mismatch: %d", n)
	}
	ctx := context.Background()
	n, err = c.IncrWithContext(ctx, "counter", 5)
	if err != nil {
		t.Fatalf("incr with context: %v", err)
	}
	if n != 5 {
		t.Fatalf("incr with context value mismatch: %d", n)
	}
	n, err = c.DecrWithContext(ctx, "counter", 2)
	if err != nil {
		t.Fatalf("decr with context: %v", err)
	}
	if n != 3 {
		t.Fatalf("decr with context value mismatch: %d", n)
	}
	if err := c.FlushAll(); err != nil {
		t.Fatalf("flush all: %v", err)
	}
	_, err = c.Get("counter")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found after flush all: %v", err)
	}
}

func TestContextDeadlineBehavior(t *testing.T) {
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		bw := bufio.NewWriter(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "get ") {
				time.Sleep(200 * time.Millisecond)
				_, _ = bw.WriteString("END\r\n")
				_ = bw.Flush()
			}
		}
	})
	defer server.Close()

	client, err := New(server.Addr(), WithWorkers(1))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel1()
	start := time.Now()
	_, err = client.GetWithContext(ctx1, "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ctx deadline should win: %v", err)
	}
	if time.Since(start) > 170*time.Millisecond {
		t.Fatalf("ctx deadline did not win")
	}
	_ = client.Close()

	clientNoDeadline, err := New(server.Addr(), WithWorkers(1))
	if err != nil {
		t.Fatalf("new no-deadline client: %v", err)
	}
	defer clientNoDeadline.Close()
	start = time.Now()
	_, err = clientNoDeadline.Get("k")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("background context should not timeout by default: %v", err)
	}
	if time.Since(start) < 180*time.Millisecond {
		t.Fatalf("background context returned too early")
	}

	client2, err := New(server.Addr(), WithWorkers(1))
	if err != nil {
		t.Fatalf("new2: %v", err)
	}
	defer client2.Close()

	ctx3, cancel3 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel3()
	_, err = client2.GetWithContext(ctx3, "k")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected ctx timeout: %v", err)
	}
}

func TestPendingReleasedOnProtocolError(t *testing.T) {
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		for {
			if _, err := br.ReadString('\n'); err != nil {
				return
			}
			_, _ = conn.Write([]byte("BROKEN\r\n"))
			_ = conn.Close()
			return
		}
	})
	defer server.Close()

	client, err := New(server.Addr(), WithWorkers(1))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer client.Close()

	const n = 16
	errCh := make(chan error, n)
	for i := range n {
		go func(i int) {
			ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
			defer cancel()
			_, err := client.GetWithContext(ctx, fmt.Sprintf("k%d", i))
			errCh <- err
		}(i)
	}

	for range n {
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatalf("expected error")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("request stuck: pending likely leaked")
		}
	}
}

func TestCloseWhileRequestInFlight(t *testing.T) {
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		for {
			if _, err := br.ReadString('\n'); err != nil {
				return
			}
			time.Sleep(5 * time.Second)
		}
	})
	defer server.Close()

	client, err := New(server.Addr(), WithWorkers(1))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := client.GetWithContext(ctx, "k")
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("request should return with context error")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("request did not return")
	}
}
