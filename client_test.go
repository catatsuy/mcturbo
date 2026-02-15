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
		mu      sync.Mutex
		data           = map[string][]byte{}
		flags          = map[string]uint32{}
		casID          = map[string]uint64{}
		nextCAS uint64 = 1
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
			case "set", "add", "replace", "append", "prepend", "cas":
				isCAS := parts[0] == "cas"
				if (!isCAS && len(parts) != 5) || (isCAS && len(parts) != 6) {
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
				reqFlags := uint32(0)
				if fv, err := strconv.ParseUint(parts[2], 10, 32); err == nil {
					reqFlags = uint32(fv)
				}
				reqCAS := uint64(0)
				if isCAS {
					if cv, err := strconv.ParseUint(parts[5], 10, 64); err == nil {
						reqCAS = cv
					}
				}
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
					flags[parts[1]] = reqFlags
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
					flags[parts[1]] = reqFlags
				case "cas":
					if !exists {
						mu.Unlock()
						_, _ = bw.WriteString("NOT_FOUND\r\n")
						_ = bw.Flush()
						continue
					}
					if casID[parts[1]] != reqCAS {
						mu.Unlock()
						_, _ = bw.WriteString("EXISTS\r\n")
						_ = bw.Flush()
						continue
					}
					data[parts[1]] = v
					flags[parts[1]] = reqFlags
				default:
					data[parts[1]] = v
					flags[parts[1]] = reqFlags
				}
				nextCAS++
				casID[parts[1]] = nextCAS
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
					_, _ = bw.WriteString(fmt.Sprintf("VALUE %s %d %d\r\n", parts[1], flags[parts[1]], len(v)))
					_, _ = bw.Write(v)
					_, _ = bw.WriteString("\r\n")
				}
				_, _ = bw.WriteString("END\r\n")
				_ = bw.Flush()
			case "gets":
				if len(parts) != 2 {
					return
				}
				mu.Lock()
				v, ok := data[parts[1]]
				f := flags[parts[1]]
				cas := casID[parts[1]]
				mu.Unlock()
				if ok {
					_, _ = bw.WriteString(fmt.Sprintf("VALUE %s %d %d %d\r\n", parts[1], f, len(v), cas))
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
				flags = map[string]uint32{}
				casID = map[string]uint64{}
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

	if err := c.Set("k1", []byte("value-1"), 11, 10); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := c.Add("k2", []byte("v2"), 0, 10); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := c.Add("k2", []byte("x"), 0, 10); !errors.Is(err, ErrNotStored) {
		t.Fatalf("second add should return ErrNotStored: %v", err)
	}
	if err := c.Replace("k2", []byte("r2"), 0, 10); err != nil {
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
	if string(v2.Value) != "Br2A" {
		t.Fatalf("unexpected merged value: %q", string(v2.Value))
	}
	if err := c.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	v, err := c.Get("k1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(v.Value) != "value-1" {
		t.Fatalf("value mismatch: %q", string(v.Value))
	}
	if v.Flags != 11 {
		t.Fatalf("flags mismatch: %d", v.Flags)
	}
	it, err := c.Gets("k1")
	if err != nil {
		t.Fatalf("gets: %v", err)
	}
	if it.CAS == 0 {
		t.Fatalf("expected CAS")
	}
	if err := c.CAS("k1", []byte("value-2"), 12, 20, it.CAS); err != nil {
		t.Fatalf("cas: %v", err)
	}
	if err := c.CAS("k1", []byte("value-3"), 13, 20, it.CAS); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected ErrCASConflict, got %v", err)
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

	if err := c.Set("counter", []byte("10"), 0, 10); err != nil {
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

func TestGetMultiEmpty(t *testing.T) {
	c, err := New("127.0.0.1:11211", WithWorkers(1))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	got, err := c.GetMultiWithContext(context.Background(), nil)
	if err != nil {
		t.Fatalf("getmulti empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map")
	}
}

func TestGetMultiBasicAndMiss(t *testing.T) {
	var (
		mu   sync.Mutex
		data = map[string][]byte{"k1": []byte("v1"), "k3": []byte("v3")}
	)
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		bw := bufio.NewWriter(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
			parts := strings.Split(line, " ")
			if len(parts) == 0 {
				return
			}
			switch parts[0] {
			case "get":
				for i := 1; i < len(parts); i++ {
					mu.Lock()
					v, ok := data[parts[i]]
					mu.Unlock()
					if ok {
						_, _ = bw.WriteString(fmt.Sprintf("VALUE %s 0 %d\r\n", parts[i], len(v)))
						_, _ = bw.Write(v)
						_, _ = bw.WriteString("\r\n")
					}
				}
				_, _ = bw.WriteString("END\r\n")
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

	got, err := c.GetMultiWithContext(context.Background(), []string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("getmulti: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(got))
	}
	if _, ok := got["k2"]; ok {
		t.Fatalf("miss key should not be present")
	}
	if string(got["k1"].Value) != "v1" || string(got["k3"].Value) != "v3" {
		t.Fatalf("unexpected values")
	}
}

func TestGetMultiBasicAndMissNoContext(t *testing.T) {
	var (
		mu   sync.Mutex
		data = map[string][]byte{"k1": []byte("v1"), "k3": []byte("v3")}
	)
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		bw := bufio.NewWriter(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
			parts := strings.Split(line, " ")
			if len(parts) == 0 {
				return
			}
			if parts[0] != "get" {
				return
			}
			for i := 1; i < len(parts); i++ {
				mu.Lock()
				v, ok := data[parts[i]]
				mu.Unlock()
				if ok {
					_, _ = bw.WriteString(fmt.Sprintf("VALUE %s 0 %d\r\n", parts[i], len(v)))
					_, _ = bw.Write(v)
					_, _ = bw.WriteString("\r\n")
				}
			}
			_, _ = bw.WriteString("END\r\n")
			_ = bw.Flush()
		}
	})
	defer server.Close()

	c, err := New(server.Addr(), WithWorkers(2))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	got, err := c.GetMulti([]string{"k1", "k2", "k3"})
	if err != nil {
		t.Fatalf("getmulti no context: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(got))
	}
	if _, ok := got["k2"]; ok {
		t.Fatalf("miss key should not be present")
	}
}

func TestGetMultiPartialFailure(t *testing.T) {
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		bw := bufio.NewWriter(conn)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
			if strings.Contains(line, "bad") {
				_, _ = bw.WriteString("SERVER_ERROR broken\r\n")
				_ = bw.Flush()
				_ = conn.Close()
				return
			}
			parts := strings.Split(line, " ")
			if len(parts) > 1 && parts[0] == "get" {
				for i := 1; i < len(parts); i++ {
					_, _ = bw.WriteString(fmt.Sprintf("VALUE %s 0 2\r\nok\r\n", parts[i]))
				}
				_, _ = bw.WriteString("END\r\n")
				_ = bw.Flush()
			}
		}
	})
	defer server.Close()

	c, err := New(server.Addr(), WithWorkers(2))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	var goodKey, badKey string
	for i := range 1000 {
		k := fmt.Sprintf("g%d", i)
		if c.pickWorker(k).id == 0 {
			goodKey = k
			break
		}
	}
	for i := range 1000 {
		k := fmt.Sprintf("bad%d", i)
		if c.pickWorker(k).id == 1 {
			badKey = k
			break
		}
	}
	if goodKey == "" || badKey == "" {
		t.Fatalf("failed to prepare worker-separated keys")
	}

	got, err := c.GetMultiWithContext(context.Background(), []string{goodKey, badKey})
	if err == nil {
		t.Fatalf("expected partial error")
	}
	var merr *MultiError
	if !errors.As(err, &merr) {
		t.Fatalf("expected MultiError, got %T", err)
	}
	if len(got) == 0 {
		t.Fatalf("expected partial success items")
	}
	if _, ok := got[goodKey]; !ok {
		t.Fatalf("expected good key in result")
	}
	if _, ok := got[badKey]; ok {
		t.Fatalf("bad key should not be present")
	}
	if len(merr.PerServer) == 0 {
		t.Fatalf("expected per-server errors")
	}
}

func TestGetMultiAllFailure(t *testing.T) {
	server := newTestServer(t, func(conn net.Conn) {
		br := bufio.NewReader(conn)
		bw := bufio.NewWriter(conn)
		for {
			if _, err := br.ReadString('\n'); err != nil {
				return
			}
			_, _ = bw.WriteString("SERVER_ERROR fail\r\n")
			_ = bw.Flush()
			_ = conn.Close()
			return
		}
	})
	defer server.Close()

	c, err := New(server.Addr(), WithWorkers(2))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer c.Close()

	got, err := c.GetMultiWithContext(context.Background(), []string{"k1", "k2"})
	if err == nil {
		t.Fatalf("expected error")
	}
	var merr *MultiError
	if !errors.As(err, &merr) {
		t.Fatalf("expected MultiError, got %T", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty result on all failure")
	}
}
