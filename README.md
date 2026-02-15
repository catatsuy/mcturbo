# mcturbo

`mcturbo` is a memcached ASCII (text) protocol client for Go 1.26.

It provides:
- A single-server client (`mcturbo.Client`)
- A distributed cluster client (`cluster.Cluster`)

## Scope

- Protocol: memcached ASCII only
- Supported commands: `get`, `gets`, `set`, `add`, `replace`, `cas`, `append`, `prepend`, `delete`, `touch`, `gat`, `incr`, `decr`
- Not in scope: binary protocol, SASL, compression, serializer

## Single-Server Client

### APIs

Fast path (no context):
- `Get(key string)`
- `Gets(key string)`
- `GetMulti(keys []string)`
- `Set(key string, value []byte, flags uint32, ttlSeconds int)`
- `Add(key string, value []byte, flags uint32, ttlSeconds int)`
- `Replace(key string, value []byte, flags uint32, ttlSeconds int)`
- `CAS(key string, value []byte, flags uint32, ttlSeconds int, cas uint64)`
- `Append(key string, value []byte)`
- `Prepend(key string, value []byte)`
- `Delete(key string)`
- `Touch(key string, ttlSeconds int)`
- `GetAndTouch(key string, ttlSeconds int)`
- `Incr(key string, delta uint64)`
- `Decr(key string, delta uint64)`
- `FlushAll()`
- `Ping()`

Context-aware path:
- `GetWithContext(ctx context.Context, key string)`
- `GetsWithContext(ctx context.Context, key string)`
- `GetMultiWithContext(ctx context.Context, keys []string)`
- `SetWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int)`
- `AddWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int)`
- `ReplaceWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int)`
- `CASWithContext(ctx context.Context, key string, value []byte, flags uint32, ttlSeconds int, cas uint64)`
- `AppendWithContext(ctx context.Context, key string, value []byte)`
- `PrependWithContext(ctx context.Context, key string, value []byte)`
- `DeleteWithContext(ctx context.Context, key string)`
- `TouchWithContext(ctx context.Context, key string, ttlSeconds int)`
- `GetAndTouchWithContext(ctx context.Context, key string, ttlSeconds int)`
- `IncrWithContext(ctx context.Context, key string, delta uint64)`
- `DecrWithContext(ctx context.Context, key string, delta uint64)`
- `FlushAllWithContext(ctx context.Context)`
- `PingWithContext(ctx context.Context)`

Lifecycle:
- `Close()`

### Timeout and Cancellation

- `*WithContext` methods use `context` as the source of truth for deadline/cancellation.
- Fast-path methods do not take `context`.
- `WithDefaultDeadline(d)` sets a fallback socket deadline per request.
- `WithMaxSlots(n)` limits per-worker concurrency. `n=0` means unlimited.

### Single-Server Example

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/catatsuy/mcturbo"
)

func main() {
	c, err := mcturbo.New(
		"127.0.0.1:11211",
		mcturbo.WithWorkers(4),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	if err := c.Set("k1", []byte("value"), 0, 10); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	v, err := c.GetWithContext(ctx, "k1")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("value=%s flags=%d", string(v.Value), v.Flags)
}
```

## Cluster Client

`cluster.Cluster` routes keys to shards and delegates operations to existing `mcturbo.Client` methods.

### Routing Features

- Distribution:
  - `DistributionModula` (default)
  - `DistributionConsistent` (Ketama-style)
- Hash:
  - `HashDefault` (default)
  - `HashMD5`
  - `HashCRC32`
- Libketama compatibility:
  - `WithLibketamaCompatible(true)` forces
    - distribution = consistent
    - hash = MD5
    - weighted Ketama behavior

### Important Defaults

- Default distribution is `MODULA` (not Ketama).
- Default hash is `DEFAULT`.
- Default libketama mode is disabled.

### Behavior Policy

- No automatic failover to another server by default.
- `UpdateServers` may move keys.
- Shards with unchanged `Addr` are reused.
- Removed shards are closed.
- Optional failover (temporary auto-eject) is enabled by:
  - `WithRemoveFailedServers(true)` (default: `false`)
  - `WithServerFailureLimit(n)` (default: `2`)
  - `WithRetryTimeout(d)` (default: `2s`)
- Failover is only used for communication-level failures:
  - network close (`io.EOF`, `net.ErrClosed`)
  - timeout/non-temporary `net.Error`
  - protocol parse errors (`mcturbo.IsProtocolError(err)`)
- Failover is not used for semantic errors:
  - `ErrNotFound`
  - `ErrNotStored`
  - `ErrCASConflict`
- If all servers are temporarily ejected, the cluster falls back to trying all servers.
- `GetMulti` keeps partial-success semantics (`result` and `error` can both be non-nil).

### Cluster APIs

Context-aware path:
- `GetWithContext`, `GetsWithContext`, `GetMultiWithContext`, `SetWithContext`, `AddWithContext`, `ReplaceWithContext`, `CASWithContext`, `AppendWithContext`, `PrependWithContext`, `DeleteWithContext`, `TouchWithContext`, `GetAndTouchWithContext`, `IncrWithContext`, `DecrWithContext`, `FlushAllWithContext`, `PingWithContext`

Fast path:
- `Get`, `Gets`, `GetMulti`, `Set`, `Add`, `Replace`, `CAS`, `Append`, `Prepend`, `Delete`, `Touch`, `GetAndTouch`, `Incr`, `Decr`, `FlushAll`, `Ping`
- Explicit aliases: `GetNoContext`, `GetsNoContext`, `SetNoContext`, `AddNoContext`, `ReplaceNoContext`, `CASNoContext`, `AppendNoContext`, `PrependNoContext`, `DeleteNoContext`, `TouchNoContext`, `GetAndTouchNoContext`, `IncrNoContext`, `DecrNoContext`, `FlushAllNoContext`, `PingNoContext`

Management:
- `UpdateServers([]Server)`
- `Close()`

### Cluster Example (Consistent + MD5)

```go
package main

import (
	"context"
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
		cluster.WithDistribution(cluster.DistributionConsistent),
		cluster.WithHash(cluster.HashMD5),
		cluster.WithBaseClientOptions(
			mcturbo.WithWorkers(4),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	if err := c.SetNoContext("k1", []byte("value"), 0, 10); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	v, err := c.GetWithContext(ctx, "k1")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("value=%s flags=%d", string(v.Value), v.Flags)
}
```

## Testing

Unit tests:

```bash
go test ./...
```

Integration tests (requires `memcached` command):

```bash
go test -tags=integration ./...
```
