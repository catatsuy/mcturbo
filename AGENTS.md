# Repository Guidelines

## Project Structure & Module Organization
- Root module: `github.com/catatsuy/mcturbo` (Go 1.26).
- Single-server client is implemented in `client.go` (fast path + `*WithContext` path).
- Cluster/distribution logic lives in `cluster/`:
  - `cluster.go` (public cluster API, failover behavior, shard lifecycle)
  - `options.go` (cluster options including failover and router injection)
  - `router.go` (public `Router` / `RouterFactory`)
  - `router_*.go` (built-in routing implementations)
- Tests:
  - Unit tests: `*_test.go` in root and `cluster/`
  - Integration tests: `*_integration_test.go` with `//go:build integration`
- Benchmarks are isolated in `benchmark/` (separate `go.mod`).

## Build, Test, and Development Commands
- `go test ./...`
  - Run all unit tests in the root module.
- `go test -tags=integration ./...`
  - Run integration tests (requires `memcached` binary in PATH).
- `cd benchmark && go test -run '^$' -bench . -benchmem -count=1`
  - Run benchmark suite against real memcached subprocesses.
- `gofmt -w <files>`
  - Format code before committing.

## Coding Style & Naming Conventions
- Follow standard Go style and keep code `gofmt`-clean.
- Keep APIs explicit and symmetric (`Foo`, `FooWithContext`, and `FooNoContext` where provided).
- Prefer adding options/types over breaking existing public APIs.
- Use `errors.AsType` on Go 1.26 when extracting typed errors.
- Cluster routing must stay a thin flow: `Pick(key) -> shard client method`.

## Testing Guidelines
- Use table-driven tests where practical; keep tests deterministic.
- Name tests as `TestXxxBehavior` and keep assertions specific.
- For routing changes, test:
  - deterministic pick
  - weight effect
  - server update behavior (reuse/close)
  - custom `WithRouterFactory` behavior
  - no unintended failover for semantic errors
- Integration tests must skip gracefully if `memcached` is unavailable.
- For failover changes, add both unit and integration coverage.

## Commit & Pull Request Guidelines
- Commit messages in history use concise imperative style (e.g., `Implement phase1 ...`).
- Prefer focused commits per logical change (API, router logic, tests, docs).
- PRs should include:
  - what changed and why
  - risk/compatibility notes (especially API behavior)
  - commands run (`go test ./...`, integration/benchmark if relevant)
  - README updates when behavior or public options change

## Security & Configuration Notes
- Never hardcode production endpoints or secrets.
- Benchmark/profile artifacts are local diagnostics; avoid adding large generated files unless necessary for review.
