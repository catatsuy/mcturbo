# Repository Guidelines

## Project Structure & Module Organization
- Root module: `github.com/catatsuy/mcturbo` (Go 1.26).
- Single-server client lives in `client.go`.
- Cluster/distribution logic lives in `cluster/`:
  - `cluster.go` (public cluster API)
  - `options.go` (distribution/hash/libketama options)
  - `router_*.go` (routing implementations)
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
- `gofmt -w .` (or target files)
  - Format code before committing.

## Coding Style & Naming Conventions
- Follow standard Go style and keep code `gofmt`-clean.
- Use clear, small types and explicit names (`Router`, `DistributionModula`, etc.).
- Do not change existing public APIs in phase-based work; add new types/options instead.
- Keep fast-path (no-context) and context-aware paths both available when adding client features.

## Testing Guidelines
- Use table-driven tests where practical; keep tests deterministic.
- Name tests as `TestXxxBehavior` and keep assertions specific.
- For routing changes, test:
  - deterministic pick
  - weight effect
  - server update behavior (reuse/close)
  - no unintended failover
- Integration tests must skip gracefully if `memcached` is unavailable.

## Commit & Pull Request Guidelines
- Commit messages in history use concise imperative style (e.g., `Implement phase1 ...`).
- Prefer focused commits per logical change (API, router logic, tests, docs).
- PRs should include:
  - what changed and why
  - risk/compatibility notes (especially API behavior)
  - commands run (`go test ./...`, integration/benchmark if relevant)

## Security & Configuration Notes
- Never hardcode production endpoints or secrets.
- Benchmark/profile artifacts are local diagnostics; avoid adding large generated files unless necessary for review.
