# Benchmark Results

This directory compares `mcturbo` and `bradfitz/gomemcache` with a real `memcached` process started in `TestMain`.

## How to Run

```bash
cd benchmark
MCT_WORKERS=8 MCT_MAX_SLOTS=0 GM_MAX_IDLE_CONNS=2 \
go test -run '^$' -bench '^Benchmark(SetSmall|GetHit|GetHitParallel|DeleteHit)/(mcturbo|gomemcache)$' -benchmem -count=1
```

## Environment

- Date: 2026-02-14
- OS: darwin
- Arch: arm64
- CPU: Apple M1
- memcached: started by benchmark `TestMain` on `127.0.0.1` with a random free port

## Latest Raw Results (count=1, tuned)

- `BenchmarkSetSmall/mcturbo-8`: `20439` ns/op (`9 B/op`, `0 allocs/op`)
- `BenchmarkSetSmall/gomemcache-8`: `20568` ns/op (`96 B/op`, `3 allocs/op`)
- `BenchmarkGetHit/mcturbo-8`: `19745` ns/op (`12 B/op`, `2 allocs/op`)
- `BenchmarkGetHit/gomemcache-8`: `20194` ns/op (`115 B/op`, `4 allocs/op`)
- `BenchmarkGetHitParallel/mcturbo-8`: `7580` ns/op (`25 B/op`, `2 allocs/op`)
- `BenchmarkGetHitParallel/gomemcache-8`: `7790` ns/op (`152 B/op`, `4 allocs/op`)
- `BenchmarkDeleteHit/mcturbo-8`: `39256` ns/op (`18 B/op`, `0 allocs/op`)
- `BenchmarkDeleteHit/gomemcache-8`: `40174` ns/op (`112 B/op`, `4 allocs/op`)

## Summary (count=1, tuned)

| Benchmark | mcturbo (ns/op) | gomemcache (ns/op) |
|---|---:|---:|
| SetSmall | 20,439 | 20,568 |
| GetHit | 19,745 | 20,194 |
| GetHitParallel | 7,580 | 7,790 |
| DeleteHit | 39,256 | 40,174 |

## Notes

- Tuned parameters used in this run: `MCT_WORKERS=8`, `MCT_MAX_SLOTS=0`, `GM_MAX_IDLE_CONNS=2`.
- `mcturbo` has much lower allocations across all measured cases.
- In this tuned run, `mcturbo` is faster in all measured benchmarks.
