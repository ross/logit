# 0015. jemalloc as the global allocator

## Status

Accepted.

## Context

`logit` is close to the worst case for glibc's `malloc`: a process that runs for weeks, on a
multi-threaded tokio runtime plus one OS thread per Lua component, doing nothing but allocating and
freeing small short-lived objects. `docs/design/memory.md` measures the churn — the reference nginx
pipeline costs 11 allocations to ingest one access-log line and ~180 more to encode it for
InfluxDB, all of them small and all of them freed almost immediately.

Two properties of glibc's allocator matter for that shape:

- **Per-thread arenas.** glibc creates arenas on demand as threads contend (up to 8× the core
  count by default) and assigns threads to them. Memory freed on one arena is not available to
  another, so a workload whose allocation and deallocation happen on different threads — which is
  exactly what a pipeline of channel-connected nodes does — fragments across arenas.
- **Reluctance to return memory.** glibc trims the main arena's top of heap only, and only past
  `M_TRIM_THRESHOLD`. Non-main arenas and anything below a live allocation stay resident. The
  reported effect on long-running servers is RSS drifting steadily upward over days while the
  actual working set is flat — indistinguishable from a leak without a profiler, and frequently
  mistaken for one.

The production runtime image is `debian:bookworm-slim` (`Dockerfile`), so glibc is what ships today.

There is a second, related problem: `logit` has no heap-profiling story at all. When RSS does grow
in production, there is currently nothing to point at what is holding it.

## Decision

Use **jemalloc** (`tikv-jemallocator`) as the `#[global_allocator]` in `logit-cli`, behind a
default-on Cargo feature named `jemalloc`.

jemalloc is designed for this workload: many arenas with per-thread caching to avoid the
cross-thread free problem, size-class binning that bounds external fragmentation, and background
purging with dirty/muzzy decay so pages actually go back to the OS instead of accumulating.

The feature is **opt-out rather than opt-in** so the shipped binary gets it by default while
`--no-default-features` still builds against the system allocator. That is deliberate: it keeps
"is jemalloc actually helping *this* workload?" a question with an answer, rather than an
assumption baked in permanently. `crates/logit-bench`'s `CountingAlloc` is generic over its inner
allocator for the same reason — allocation *counts* are allocator-independent, but what those
counts cost is not.

It also closes the profiling gap: jemalloc's built-in heap profiling (`MALLOC_CONF=prof:true`,
then `jeprof`) is available on the same binary, with no separate instrumented build. See
`docs/design/memory.md`'s profiling section for the recipe.

## Alternatives

- **Stay on glibc and measure first.** Rejected as the default, not as an idea: the failure mode
  here is a slow RSS drift that only shows up after days of production traffic, so "measure first"
  means "find out in production." The escape hatch is kept (`--no-default-features`) so a
  comparison is still one build away, which gets most of the value of measuring first without
  betting the default on it.
- **mimalloc.** Benchmarks competitively with jemalloc on multi-threaded churn, sometimes better,
  and is a smaller dependency. Rejected on the profiling half of the decision: jemalloc's heap
  profiler is the mature, well-documented option, and for a telemetry daemon whose whole job is
  observability, being able to profile its own heap in production is worth more than a few percent
  either way. Worth revisiting if the benches ever show a real gap.
- **An arena or slab allocator for `Event` specifically.** Rejected as premature and as the wrong
  layer. `docs/design/memory.md` shows the dominant allocation cost is in the InfluxDB encoder's
  per-line `String`s, not in `Event` itself; a bespoke allocator would add real complexity to fix
  something that isn't the problem. Reducing `Event`'s size and the encoder's churn are the
  cheaper, more direct fixes, and they help under any allocator.
- **Tuning glibc via `MALLOC_ARENA_MAX`/`M_TRIM_THRESHOLD`.** Rejected: it trades throughput for
  fragmentation rather than fixing either, and it puts load-bearing configuration in environment
  variables that a deployment can silently lose.

## Consequences

- The default `logit` binary links jemalloc statically. The builder stage already has the C
  toolchain it needs (`rust:1-bookworm`), and `debian:bookworm-slim` needs no new runtime package.
- Binary size and build time both go up modestly.
- Heap profiling becomes available on the shipped binary via `MALLOC_CONF`, with no rebuild.
- Any measurement comparing the two allocators must say which one it used. The numbers in
  `docs/design/memory.md` are allocation *counts*, which don't depend on the allocator; its timing
  numbers do, and it names the allocator they were taken under.
- `tikv-jemallocator` and `tikv-jemalloc-sys` are MIT/Apache-2.0 over jemalloc's own BSD-2-Clause,
  all already permitted by `deny.toml`'s allow-list.
