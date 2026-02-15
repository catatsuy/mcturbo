# mcturbo

`mcturbo` is a memcached ASCII (text protocol) client for Go 1.26.

It provides two clients:
- `mcturbo.Client` for a single memcached server
- `cluster.Cluster` for multi-server routing

## What This Project Supports

- Protocol: memcached ASCII only
- Commands: `get`, `gets`, `set`, `add`, `replace`, `cas`, `append`, `prepend`, `delete`, `touch`, `gat`, `incr`, `decr`, `flush_all`, `version`
- Both API styles:
  - Fast path (no `context` argument)
  - Context-aware path (`*WithContext`)

Not supported:
- binary protocol, SASL, compression, serializer

## Example (Single Server)

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/catatsuy/mcturbo"
)

func main() {
	c, err := mcturbo.New("127.0.0.1:11211", mcturbo.WithWorkers(4))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Fast path: no context argument.
	if err := c.Set("user:1", []byte("alice"), 1, 60); err != nil {
		log.Fatal(err)
	}

	// Context-aware path for deadline/cancel.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	it, err := c.GetWithContext(ctx, "user:1")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("get: value=%q flags=%d", string(it.Value), it.Flags)

	// CAS update flow.
	current, err := c.GetsWithContext(ctx, "user:1")
	if err != nil {
		log.Fatal(err)
	}
	err = c.CASWithContext(ctx, "user:1", []byte("alice-updated"), current.Flags, 60, current.CAS)
	if errors.Is(err, mcturbo.ErrCASConflict) {
		log.Printf("cas conflict: retry with latest value")
	} else if err != nil {
		log.Fatal(err)
	}

	// GetMulti may return both result and error on partial success.
	items, err := c.GetMultiWithContext(ctx, []string{"user:1", "user:2", "user:3"})
	if err != nil {
		if me, ok := errors.AsType[*mcturbo.MultiError](err); ok {
			log.Printf("getmulti partial failure: %d servers failed", len(me.PerServer))
		} else {
			log.Fatal(err)
		}
	}
	for k, v := range items {
		log.Printf("getmulti: key=%s value=%q flags=%d", k, string(v.Value), v.Flags)
	}
}
```

## Single-Server API Summary

Fast path (no context):
- `Get`, `Gets`, `GetMulti`
- `Set`, `Add`, `Replace`, `CAS`
- `Append`, `Prepend`, `Delete`, `Touch`, `GetAndTouch`
- `Incr`, `Decr`, `FlushAll`, `Ping`

Context-aware path:
- `GetWithContext`, `GetsWithContext`, `GetMultiWithContext`
- `SetWithContext`, `AddWithContext`, `ReplaceWithContext`, `CASWithContext`
- `AppendWithContext`, `PrependWithContext`, `DeleteWithContext`, `TouchWithContext`, `GetAndTouchWithContext`
- `IncrWithContext`, `DecrWithContext`, `FlushAllWithContext`, `PingWithContext`

Lifecycle:
- `Close()`

### Timeout and Cancellation

- `*WithContext` methods use `context` as the source of truth.
- Fast-path methods do not accept `context`.
- `WithDefaultDeadline(d)` sets a fallback socket deadline when you do not pass context.
- `WithMaxSlots(n)` limits per-worker concurrency (`0` = unlimited).

### Performance Tips

- Prefer fast-path methods when you do not need per-call cancellation/deadline.
- Reuse one client instance; do not create/close clients per request.
- Tune `WithWorkers(n)` based on your CPU and request concurrency.
- Start with `WithMaxSlots(0)` (unlimited), then set a limit only when protecting backend load.
- Keep value sizes moderate and avoid very large hot keys.
- Use `GetMulti` for multi-key reads to reduce network round trips.
- Use context deadlines only where needed; overly short deadlines can increase retries and error handling cost.
- Benchmark with your real key/value size distribution before changing defaults.

## Cluster Client

`cluster.Cluster` routes each key to one shard and calls the existing `mcturbo.Client` methods internally.

### Routing Options

- Distribution:
  - `DistributionModula` (default)
  - `DistributionConsistent` (Ketama-style)
- Hash:
  - `HashDefault` (default)
  - `HashMD5`
  - `HashCRC32`
- Libketama-compatible mode:
  - `WithLibketamaCompatible(true)` forces:
    - distribution = consistent
    - hash = MD5

### Server Update Behavior

- `UpdateServers` rebuilds routing.
- Existing shard clients are reused when `Addr` is unchanged.
- Removed shard clients are closed.
- Key movement can happen after server updates.

### Failover Behavior (Optional)

Default:
- no failover

Enable temporary auto-eject:
- `WithRemoveFailedServers(true)`
- `WithServerFailureLimit(n)` (default: `2`)
- `WithRetryTimeout(d)` (default: `2s`)

When enabled:
- Retry to next shard only for communication failures:
  - `io.EOF`, `net.ErrClosed`
  - timeout/non-temporary `net.Error`
  - protocol parse errors (`mcturbo.IsProtocolError(err)`)
- No failover for semantic errors:
  - `ErrNotFound`, `ErrNotStored`, `ErrCASConflict`
- If all shards are temporarily ejected, the cluster falls back to trying all shards.

`GetMulti` note:
- It keeps partial-success semantics (`result` and `error` can both be non-nil).

### Cluster Performance Tips

- Keep server weights close to actual capacity to avoid shard hotspots.
- Use `DistributionConsistent` for smoother key movement during `UpdateServers`.
- Enable failover only when needed; each retry can add latency on failure paths.
- Use `GetMulti` for read-heavy fan-out access patterns.

## Cluster API Summary

Context-aware path:
- `GetWithContext`, `GetsWithContext`, `GetMultiWithContext`
- `SetWithContext`, `AddWithContext`, `ReplaceWithContext`, `CASWithContext`
- `AppendWithContext`, `PrependWithContext`, `DeleteWithContext`, `TouchWithContext`, `GetAndTouchWithContext`
- `IncrWithContext`, `DecrWithContext`, `FlushAllWithContext`, `PingWithContext`

Fast path:
- `Get`, `Gets`, `GetMulti`
- `Set`, `Add`, `Replace`, `CAS`
- `Append`, `Prepend`, `Delete`, `Touch`, `GetAndTouch`
- `Incr`, `Decr`, `FlushAll`, `Ping`

No-context aliases:
- `GetNoContext`, `GetsNoContext`, `GetMulti`
- `SetNoContext`, `AddNoContext`, `ReplaceNoContext`, `CASNoContext`
- `AppendNoContext`, `PrependNoContext`, `DeleteNoContext`, `TouchNoContext`, `GetAndTouchNoContext`
- `IncrNoContext`, `DecrNoContext`, `FlushAllNoContext`, `PingNoContext`

Management:
- `UpdateServers([]Server)`
- `Close()`

## Example (Cluster)

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/catatsuy/mcturbo"
	"github.com/catatsuy/mcturbo/cluster"
)

func main() {
	c, err := cluster.NewCluster(
		[]cluster.Server{
			{Addr: "127.0.0.1:11211", Weight: 1},
			{Addr: "127.0.0.1:11212", Weight: 1},
		},
		cluster.WithDistribution(cluster.DistributionConsistent), // explicit ketama
		cluster.WithHash(cluster.HashMD5),
		cluster.WithBaseClientOptions(mcturbo.WithWorkers(4)),
		cluster.WithRemoveFailedServers(true), // optional failover
		cluster.WithServerFailureLimit(2),
		cluster.WithRetryTimeout(2*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// No-context API.
	if err := c.SetNoContext("session:42", []byte("token"), 0, 120); err != nil {
		log.Fatal(err)
	}
	if err := c.PingNoContext(); err != nil {
		log.Fatal(err)
	}

	// Context-aware API.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	it, err := c.GetWithContext(ctx, "session:42")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("cluster get: value=%q flags=%d", string(it.Value), it.Flags)

	// Cluster GetMulti also allows partial success.
	items, err := c.GetMultiWithContext(ctx, []string{"session:42", "session:43"})
	if err != nil {
		if me, ok := errors.AsType[*mcturbo.MultiError](err); ok {
			log.Printf("cluster getmulti partial failure: %d servers failed", len(me.PerServer))
		} else {
			log.Fatal(err)
		}
	}
	for k, v := range items {
		log.Printf("cluster getmulti: key=%s value=%q", k, string(v.Value))
	}
}
```

## Test

Unit tests:

```bash
go test ./...
```

Integration tests (requires `memcached` command):

```bash
go test -tags=integration ./...
```
