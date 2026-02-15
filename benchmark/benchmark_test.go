package benchmark

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/catatsuy/mcturbo"
)

var (
	benchAddr      string
	benchCmd       *exec.Cmd
	benchAvailable bool
	mctWorkers     = envInt("MCT_WORKERS", 4)
	mctMaxSlots    = envInt("MCT_MAX_SLOTS", 8)
	gmMaxIdleConns = envInt("GM_MAX_IDLE_CONNS", 2)
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

	benchAddr = fmt.Sprintf("127.0.0.1:%d", port)
	benchCmd = exec.Command("memcached", "-l", "127.0.0.1", "-p", fmt.Sprintf("%d", port), "-U", "0")
	benchCmd.Stdout = os.Stdout
	benchCmd.Stderr = os.Stderr
	if err := benchCmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start memcached:", err)
		os.Exit(1)
	}

	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", benchAddr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			benchAvailable = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	code := m.Run()

	if benchCmd != nil && benchCmd.Process != nil {
		_ = benchCmd.Process.Kill()
		_, _ = benchCmd.Process.Wait()
	}
	os.Exit(code)
}

func newMCTurboClient(b *testing.B) *mcturbo.Client {
	b.Helper()
	if !benchAvailable {
		b.Skip("memcached command is not available")
	}
	c, err := mcturbo.New(
		benchAddr,
		mcturbo.WithWorkers(mctWorkers),
		mcturbo.WithMaxSlots(mctMaxSlots),
	)
	if err != nil {
		b.Fatalf("new mcturbo client: %v", err)
	}
	b.Cleanup(func() { _ = c.Close() })
	return c
}

func newGomemcacheClient(b *testing.B) *memcache.Client {
	b.Helper()
	if !benchAvailable {
		b.Skip("memcached command is not available")
	}
	c := memcache.New(benchAddr)
	c.MaxIdleConns = gmMaxIdleConns
	return c
}

func BenchmarkSetSmall(b *testing.B) {
	payload := []byte("value-1234567890")
	const keyCount = 8192
	keysMCT := makeKeySet("bench:mct:set:", keyCount)
	keysGM := makeKeySet("bench:gm:set:", keyCount)

	b.Run("mcturbo", func(b *testing.B) {
		c := newMCTurboClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keysMCT[i%keyCount]
			if err := c.Set(k, payload, 0, 60); err != nil {
				b.Fatalf("set: %v", err)
			}
		}
	})

	b.Run("gomemcache", func(b *testing.B) {
		c := newGomemcacheClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keysGM[i%keyCount]
			if err := c.Set(&memcache.Item{Key: k, Value: payload, Expiration: 60}); err != nil {
				b.Fatalf("set: %v", err)
			}
		}
	})
}

func BenchmarkGetHit(b *testing.B) {
	const keyCount = 1024
	keysMCT := make([]string, keyCount)
	keysGM := make([]string, keyCount)

	{
		seed := newGomemcacheClient(b)
		for i := 0; i < keyCount; i++ {
			k1 := fmt.Sprintf("bench:mct:get:%d", i)
			k2 := fmt.Sprintf("bench:gm:get:%d", i)
			keysMCT[i] = k1
			keysGM[i] = k2
			if err := seed.Set(&memcache.Item{Key: k1, Value: []byte("v"), Expiration: 60}); err != nil {
				b.Fatalf("seed set mct key: %v", err)
			}
			if err := seed.Set(&memcache.Item{Key: k2, Value: []byte("v"), Expiration: 60}); err != nil {
				b.Fatalf("seed set gm key: %v", err)
			}
		}
	}

	b.Run("mcturbo", func(b *testing.B) {
		c := newMCTurboClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := c.Get(keysMCT[i%keyCount])
			if err != nil {
				b.Fatalf("get: %v", err)
			}
		}
	})

	b.Run("gomemcache", func(b *testing.B) {
		c := newGomemcacheClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := c.Get(keysGM[i%keyCount])
			if err != nil {
				b.Fatalf("get: %v", err)
			}
		}
	})
}

func BenchmarkGetHitParallel(b *testing.B) {
	const keyCount = 2048
	keysMCT := make([]string, keyCount)
	keysGM := make([]string, keyCount)

	{
		seed := newGomemcacheClient(b)
		for i := 0; i < keyCount; i++ {
			k1 := fmt.Sprintf("bench:mct:pget:%d", i)
			k2 := fmt.Sprintf("bench:gm:pget:%d", i)
			keysMCT[i] = k1
			keysGM[i] = k2
			if err := seed.Set(&memcache.Item{Key: k1, Value: []byte("v"), Expiration: 60}); err != nil {
				b.Fatalf("seed set mct key: %v", err)
			}
			if err := seed.Set(&memcache.Item{Key: k2, Value: []byte("v"), Expiration: 60}); err != nil {
				b.Fatalf("seed set gm key: %v", err)
			}
		}
	}

	b.Run("mcturbo", func(b *testing.B) {
		c := newMCTurboClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				k := keysMCT[r.Intn(keyCount)]
				if _, err := c.Get(k); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	})

	b.Run("gomemcache", func(b *testing.B) {
		c := newGomemcacheClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for pb.Next() {
				k := keysGM[r.Intn(keyCount)]
				if _, err := c.Get(k); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	})
}

func BenchmarkDeleteMiss(b *testing.B) {
	const missingMCT = "bench:mct:del-miss"
	const missingGM = "bench:gm:del-miss"

	b.Run("mcturbo", func(b *testing.B) {
		c := newMCTurboClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := c.Delete(missingMCT)
			if err != nil && !errors.Is(err, mcturbo.ErrNotFound) {
				b.Fatalf("delete: %v", err)
			}
		}
	})

	b.Run("gomemcache", func(b *testing.B) {
		c := newGomemcacheClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := c.Delete(missingGM)
			if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
				b.Fatalf("delete: %v", err)
			}
		}
	})
}

func BenchmarkDeleteHit(b *testing.B) {
	const keyCount = 4096
	value := []byte("v")
	keysMCT := makeKeySet("bench:mct:del-hit:", keyCount)
	keysGM := makeKeySet("bench:gm:del-hit:", keyCount)
	seed := newGomemcacheClient(b)
	for i := 0; i < keyCount; i++ {
		if err := seed.Set(&memcache.Item{Key: keysMCT[i], Value: value, Expiration: 60}); err != nil {
			b.Fatalf("seed set mct key: %v", err)
		}
		if err := seed.Set(&memcache.Item{Key: keysGM[i], Value: value, Expiration: 60}); err != nil {
			b.Fatalf("seed set gm key: %v", err)
		}
	}

	b.Run("mcturbo", func(b *testing.B) {
		c := newMCTurboClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keysMCT[i%keyCount]
			err := c.Delete(k)
			if err != nil && !errors.Is(err, mcturbo.ErrNotFound) {
				b.Fatalf("delete: %v", err)
			}
			if err := c.Set(k, value, 0, 60); err != nil {
				b.Fatalf("set: %v", err)
			}
		}
	})

	b.Run("gomemcache", func(b *testing.B) {
		c := newGomemcacheClient(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keysGM[i%keyCount]
			err := c.Delete(k)
			if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
				b.Fatalf("delete: %v", err)
			}
			if err := c.Set(&memcache.Item{Key: k, Value: value, Expiration: 60}); err != nil {
				b.Fatalf("set: %v", err)
			}
		}
	})
}

func makeKeySet(prefix string, n int) []string {
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = prefix + strconv.Itoa(i)
	}
	return keys
}

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
