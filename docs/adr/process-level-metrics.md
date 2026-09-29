---
created: 2026-09-28
updated: 2026-09-28
---

# Process-level metrics from procfs and the allocator

## Status
Accepted

## Context
`internal` (`crates/logit-inputs/src/internal.rs`) samples two process-level gauges on each drain
tick, `logit.process.interner.strings` and `logit.process.uptime`
([`internal-telemetry.md`](../design/internal-telemetry.md)'s "`internal`: the drain" section).
Nothing else about the process is observable from `logit`'s own telemetry: not its resident memory,
thread count, open descriptors, or CPU time.

[`docs/known-gaps.md`](../known-gaps.md)'s "Internal telemetry and self-logging" section lists the
rest as open, with two obstacles:

- The thread, descriptor, and CPU facts come from `/proc`, which is Linux-specific.
- Heap statistics live in jemalloc, behind `logit-cli`'s `jemalloc` feature
  ([ADR `jemalloc-global-allocator`](jemalloc-global-allocator.md)), and `crates/logit-inputs`
  can't depend on `logit-cli`.

The gap has a concrete cost. ADR `jemalloc-global-allocator` names its failure mode as RSS drifting
upward for days while the working set is flat, mistaken for a leak. Telling that apart from a real
leak takes resident memory and allocated heap side by side, and `logit` reports neither.

## Decision
`internal` samples six more points in the `logit.process.*` namespace on each tick, in
`InternalInput::tick` beside the two it already emits. Each carries `internal`'s own
`component`/`kind`/`role` identity, like every other point.

| Name | Kind | Source |
|---|---|---|
| `logit.process.memory.resident.bytes` | gauge | `/proc/self/status` `VmRSS:` (kB × 1024) |
| `logit.process.threads` | gauge | `/proc/self/status` `Threads:` |
| `logit.process.fds` | gauge | entry count of `/proc/self/fd`, minus one for the descriptor the listing itself holds |
| `logit.process.fds.limit` | gauge | `/proc/self/limits` "Max open files" soft limit; not emitted when `unlimited` |
| `logit.process.cpu.seconds{mode=user\|system}` | delta counter | `/proc/self/stat` `utime`/`stime` in `USER_HZ` ticks, as the delta since the previous tick |
| `logit.process.memory.allocated.bytes` | gauge | jemalloc's `stats.allocated`, supplied by `logit-cli` through a hook on `InternalInput` |

No config changes. A non-Linux build emits none of the procfs points; the allocator gauge is
independent of the platform and still emits.

### procfs, not `libc`
`getrusage(RUSAGE_SELF)` gives CPU time and peak RSS, but it's a fourth raw-`libc` `unsafe` call
site for the inventory [ADR `out-of-ci-unsafe-verification`](out-of-ci-unsafe-verification.md)
maintains. `/proc/self/statm` reports pages, so converting it needs the page size from `sysconf`.
Reading `/proc/self/{status,stat,fd,limits}` needs no `unsafe` and no new dependency.

`USER_HZ` is the constant 100, not a `sysconf(_SC_CLK_TCK)` call. The kernel fixes it in
`include/asm-generic/param.h`, and glibc's `sysconf` returns the kernel-supplied `AT_CLKTCK`, which
is that constant on every architecture `logit` builds for. A Linux-only test pins the constant
against `sysconf`, so a platform that differs fails the test instead of misreporting CPU.

`/proc/self/stat` and `/proc/self/status` are read as bytes, not UTF-8. The `comm` field is up to
15 arbitrary bytes and can contain spaces, `)`, or non-UTF-8, so the `stat` parser splits at the
last `)`.

Reads run inline in `tick`, with no `spawn_blocking`. A procfs read touches no disk, and it runs
once per `interval`.

### Per-source latching
Each of the four procfs sources has its own enabled flag. The first failed read latches only that
source off and emits one diagnostic: `debug` when the platform has no procfs, `warn` when a read
failed on Linux. That's the shape `AcceptQueueSampler` (`crates/logit-inputs/src/tcp.rs`) uses for
`TCP_INFO`. One latch for all four would let a sandbox that hides one file, such as gVisor or a
seccomp profile, silence the other three.

### CPU is a delta counter
Every other `internal` counter is a delta `Sum`, and the downstream story is already built for it:

- Under `aggregate` with `temporality: cumulative` into `prometheus_out`, it becomes
  `logit_process_cpu_seconds_total{mode}`, and `rate()` over it is cores in use per `mode`, or in
  total summed over `mode`.
- Under a default delta `aggregate` into `influxdb_out`, each window's value is the CPU seconds
  spent in that window, so the value divided by the window length is utilization.

The sampler keeps its previous reading. The first tick's delta is CPU time since process start, so
a cumulative total equals the kernel's own counter.

The sampler emits every tick, a zero delta included. A cumulative `aggregate` evicts a series after
`series_retention` idle windows, and an eviction restarts the running total, which reads downstream
as a counter reset.

Without a cumulative `aggregate`, `prometheus_out` skips the delta `Sum` and counts it as
`logit.output.metrics.skipped{metric_kind="delta_sum"}`, as it does for every other `internal`
counter.

The tag key is `mode`, short like the existing `reason` and `class` keys, rather than OTel
semconv's `cpu.mode`.

### Names carry units
The memory names end in `.bytes`, as `logit.input.receive_buffer.bytes` and
`logit.component.buffer.bytes` do. They sanitize to Prometheus's conventional form, such as
`logit_process_memory_resident_bytes`.

### The allocator gauge crosses the crate boundary by injection
`logit-inputs` stays ignorant of the allocator. `InternalInput` takes an optional
`fn() -> Option<u64>` through `with_heap_stats`. Under `#[cfg(feature = "jemalloc")]`,
`logit-cli` passes `jemalloc_allocated()`, which calls `epoch::advance()` then
`stats::allocated::read()` from `tikv-jemalloc-ctl`, both safe APIs. Without the feature, no hook
is set and the gauge doesn't emit.

The `jemalloc` feature turns on `tikv-jemallocator/stats`, so the statistics are compiled in
whatever the sys crate's defaults are.

### Reading resident against allocated
The difference between `resident` and `allocated` is not a leak by itself. It contains:

- jemalloc's own metadata and fragmentation;
- LuaJIT's machine-code areas, which it maps directly with `mmap`. Each VM's GC heap is not in
  this gap: mlua creates every VM with an allocator over the Rust global allocator, so Lua objects
  count in `allocated`, and a script that retains memory raises it (`logit.script.vm.memory`
  attributes that share per VM);
- the binary's mapped pages and thread stacks;
- the UDP read slab.

`VmRSS` can lag the true value slightly, because the kernel batches per-thread RSS counters.

## Alternatives considered
- **`getrusage`.** Rejected: a new `unsafe` call site, and it reports no thread or descriptor
  counts.
- **`/proc/self/statm`.** Rejected: converting pages to bytes needs the page size from `sysconf`.
- **`/proc/self/schedstat`.** Rejected: it's per task and has no user/system split.
- **CPU as a cumulative gauge.** Rejected: it takes `rate()` over a gauge, and it's inconsistent
  with every other `internal` counter.
- **A counting `#[global_allocator]` wrapper.** Rejected: an atomic on every allocation, to learn
  what jemalloc already tracks.
- **The `sysinfo` or `procfs` crate.** Rejected: a dependency for four small files.

## Consequences
- Operators get six new series from any `internal` component, with no config change.
- The procfs points are Linux-only. A non-Linux build logs one `debug` diagnostic per source and
  emits the allocator gauge alone.
- The process-level entry in `docs/known-gaps.md`'s "Internal telemetry and self-logging" section
  closes.
- `logit-cli` gains a `tikv-jemalloc-ctl` dependency under its `jemalloc` feature. If that pulls in
  `paste`, an unmaintained proc-macro (RUSTSEC-2024-0436), `deny.toml` needs a reasoned ignore for
  it. Falling back to raw `mallctl` instead isn't acceptable, because it adds `unsafe` to
  `logit-cli`.
- jemalloc's statistics add a small cost to every allocation.
- The `internal` `interval` field's doc and `internal-telemetry.md`'s "`internal`: the drain"
  section list the new metrics.
- `demo/`'s internal dashboard gains resident-memory and CPU panels.
