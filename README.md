# mcturbo

`mcturbo` is a memcached ASCII (text) protocol client for Go 1.26.
Phase 1 provides a single-server client implementation.

## Supported Commands

- `get`
- `set`
- `delete`
- `touch`

## Timeout Model

- Default APIs (`Get`, `Set`, `Delete`, `Touch`) do not require `context.Context` and are optimized for low latency.
- Context-aware APIs are also available: `GetWithContext`, `SetWithContext`, `DeleteWithContext`, `TouchWithContext`.
- For `*WithContext` APIs, `context` is the only source of truth for cancellation and deadlines.
- `WithDefaultDeadline(d)` sets a fallback socket deadline used at round-trip time when no context deadline is present.
- `WithMaxSlots(n)` limits per-worker concurrent in-flight operations (`n=0` means unlimited, default).

## Usage

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

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if err := c.Set("k1", []byte("value"), 10); err != nil {
		log.Fatal(err)
	}
	v, err := c.GetWithContext(ctx, "k1")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("value=%s", string(v))
}
```

## Tests

- Unit tests only:

```bash
go test ./...
```

- Integration tests (requires `memcached` binary):

```bash
go test -tags=integration ./...
```
