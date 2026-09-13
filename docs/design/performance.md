# Performance: the load-test harness's first recorded run

The out-of-CI load-test harness ([ADR `load-test-harness`](../adr/load-test-harness.md),
[`docs/plans/load-test-harness.md`](../plans/load-test-harness.md)) spawns the real, release-profile
`logit run <config>` process against `perf/scenarios/*.yaml` and measures throughput, CPU cost, and
peak RSS — the end-to-end question [`memory.md`](memory.md) §7 named as still open, closed by
building the thing rather than guessing at it. This document is the durable, hand-curated record of
what a measured run showed, following the same convention `memory.md`'s own tables do: `perf/results/`
is gitignored, ephemeral, per-machine JSON; this file is where a run's numbers get written down.

Everything here is reproducible:

| What | Command |
|---|---|
| Every scenario, 3 repeats | `script/perf run --repeat 3 --profile release --label recorded` |
| Per-node time attribution | `script/perf attribute --scenario json-parse` / `--scenario aggregate` |
| A flamegraph | `script/perf flamegraph --scenario passthrough` |
| Before/after regression check | `script/perf compare <a.json> <b.json> --threshold 5` |
| What's discoverable | `script/perf list` |

> Numbers below were taken on a Fedora Linux 44 (Workstation Edition) host, kernel
> `7.2.4-200.fc44.x86_64`, x86-64, AMD Ryzen AI 9 HX 370 w/ Radeon 890M (24 logical CPUs), inside
> the dev container (`rustc 1.98.1 (48a229cea 2026-09-01)`), `release` profile, at commit
> `c75399d8bcccf1ef6ba5b7b2411b1b6b6876aa09` (clean working tree), on 2026-09-13, with nothing else
> heavy running on the machine at the time — a solo, uncontended run, not this box's typical state.
> `perf/results/*.json`'s own `hostname` field records the dev container's own hostname
> (`71e5c8506b05`), not the physical host, since every `script/*` command runs inside it; `cpu_model`
> and `nproc` come from `/proc/cpuinfo`/`nproc` as seen *inside* that container, which is why they're
> restated here in prose rather than only trusted from the JSON. See `docs/design/memory.md`'s own
> preamble for why this matters: a busier box has shown "~20% slower across every unchanged
> benchmark" before now, so **treat every number below as this-machine-this-day, not a portable
> constant** — `compare` warns on a host/CPU-model mismatch for exactly this reason, and CPU
> µs/event, not wall-clock events/s, is what it actually gates a regression on.

## 0. What this measures, and what it doesn't

**Measures**, per scenario, per repeat, from `wait4`'s rusage and the wall-clock gap between
spawning the process and its generator's own `generation complete` line:

- **events/s** — `count / wall_seconds`. Real, useful, but the noisiest of the three on a shared
  box (see the preamble above) — read it as a rough throughput sense, not the regression gate.
- **CPU µs/event** — `(ru_utime + ru_stime) * 1e6 / count`. **The headline signal.** CPU time this
  process actually spent doesn't care what else the box was doing at the same moment, which is what
  makes it the number `script/perf compare --threshold` gates a regression on.
- **peak RSS** — `ru_maxrss` (kibibytes on Linux, converted to bytes). Reported for every scenario,
  especially informative for `buffered` (real segment-file I/O and buffering), but never gates
  `compare`'s exit code unless `--rss-threshold` is passed explicitly.

**Doesn't measure:**

- **Allocation counts.** That's `crates/logit-bench/tests/allocations.rs`'s job — exact,
  machine-independent, thread-local counting via `CountingAlloc`, pinned in CI. This harness spawns
  a real multi-threaded process; nothing about `wait4` sees per-allocation detail, and nothing here
  should be read as a substitute for that layer. See `memory.md` for what allocates.
- **Correctness.** `script/test`/`script/validate` own that; a scenario failing to reach its
  `generation complete` line fails the harness loudly, but a scenario that *runs* and produces wrong
  output would not be caught here.
- **Tail latency per event.** Every number here is a whole-run aggregate (total wall time, total CPU
  time); nothing measures per-event or per-batch latency distribution. `attribute`'s per-node
  `process.duration`/`send.blocked.duration` sums are the closest this harness gets, and those are
  still sums over the whole run, not a distribution.
- **Isolation from the rest of the box.** This runs in the same kind of dev-container environment
  every other `script/*` command does, not a dedicated, pinned-core bench host — see the preamble's
  ~20% caveat.

## 1. Results: all nine scenarios, median of 3

`script/perf run --repeat 3 --profile release --label recorded`, solo (nothing else running on the
machine). Sorted as `script/perf list` orders them (alphabetical); `count` is each scenario's
configured `generate_in.count` at the time of this run.

| Scenario | Count | events/s | CPU µs/event | Peak RSS | Wall |
|---|---:|---:|---:|---:|---:|
| `aggregate` | 20M | 3,399,654 | 0.296 | 36.7 MiB | 5.88 s |
| `buffered` | 1.2M | 535,735 | 2.937 | 37.1 MiB | 2.24 s |
| `encode-human-devnull` | 8M | 873,892 | 1.540 | 199.8 MiB | 9.15 s |
| `encode-native-devnull` | 8M | 1,019,019 | 1.312 | 193.3 MiB | 7.85 s |
| `fanout` | 55M | 5,970,256 | 0.435 | 129.9 MiB | 9.21 s |
| `json-parse` | 7M | 772,690 | 2.478 | 294.5 MiB | 9.06 s |
| `lua` | 4M | 494,583 | 2.360 | 26.9 MiB | 8.09 s |
| `native-relay` | 7M | 1,158,077 | 1.130 | 125.8 MiB | 6.04 s |
| `passthrough` | 20M | 2,523,190 | 0.612 | 80.9 MiB | 7.93 s |

A few readings, cross-referencing `perf/scenarios/*.yaml`'s own comments for what each measures:

- **`passthrough`** (0.612 µs/event) is the runtime floor every other scenario is read relative to:
  scheduling, the `Fanout` channel hop, layer-2 telemetry, no parsing or encoding.
- **`fanout`**'s count was retuned 25M → 55M during this run — the original count cleared in ~3.2s
  solo, under this directory's 5-10s target band (see `docs/plans/load-test-harness.md`'s scenario
  table for the full note). At the new count its CPU cost per event (0.435 µs) is noticeably above
  `passthrough`'s despite doing strictly *more* work per event (three sends instead of one) for only
  a proportionally small `Arc`-refcount overhead — consistent with `memory.md`'s own finding that an
  all-`Output` fan-out is a strict win (0/1 allocations), so the gap here is scheduling three sends
  rather than allocation.
- **`json-parse`** (2.478 µs/event) and **`lua`** (2.360 µs/event) are the two most expensive
  single-hop scenarios — real parsing and a LuaJIT round trip both cost noticeably more than a
  native transform, matching `docs/known-gaps.md`'s existing account of the Lua boundary's cost
  relative to a native transform (the standing "~9× the per-event allocations" comparison there is a
  microbench number for one hop; this is the same relationship showing up end to end).
- **`encode-human-devnull`** vs **`encode-native-devnull`** (1.540 vs 1.312 µs/event): the native
  encoder is measurably cheaper than the human-readable render at the same event stream, as
  `docs/design/wire-protocol.md`'s design intent would predict — dictionary-first framing beats
  formatting text.
- **`native-relay`** (1.130 µs/event) is the full encode → loopback TCP → decode → ack round trip in
  one process, and lands between the two `encode-*-devnull` scenarios and `json-parse`/`lua` — a real
  network hop and an ack wait, but still cheaper than a parse-heavy or Lua-heavy graph.
- **`aggregate`** (0.296 µs/event) is nearly as cheap as `passthrough` per event despite sketching a
  1000-series distribution and running a 1s flush tick — see §2 for why: almost all of it is one
  node's `DdSketch::add`, and the flush tick's own cost is amortized over a 20-second run.
- **`buffered`** — see §3. Its number above is real but should not be read the same way the other
  eight are; the note there explains why.

## 2. Attribution: where a scenario's time actually goes

`script/perf attribute --scenario NAME` appends a temporary `internal → file_out format: native` leg
to a copy of the scenario, decodes the resulting dump, and groups every point by the emitting
component — see [`internal-telemetry.md`](internal-telemetry.md)'s "Reading an attribution dump"
section for the mechanism. Both tables below are from this run, at each scenario's count above.
`__perf_internal`/`__perf_dump` are the harness's own two nodes, shown for transparency but excluded
from the verdict.

### `json-parse`

```
node               kind           role         events in  events out  batch in batch out  process s  blocked s     send s  buf max
metrics            kv_metrics     transform      7000000     7000000     70000     70000    10.3054     0.0173     0.0000        -
parsed             json           transform      7000000     7000000     70000     70000     8.7432     3.8887     0.0000        -
gen                generate_in    listener             0     7000000         0     70000     0.0000    11.8632     0.0000        -
out                null_out       sink           7000000           0     70000         0     0.0000     0.0000     0.0102     0.13
```

`kv_metrics` (`metrics`) has the larger share of Σ process time: 10.3054s of 19.0485s measured
(**54%**), against `json` (`parsed`)'s 8.7432s (**46%**) — deriving four metrics from a parsed
attribute map costs slightly more than the JSON parse that fed it, close to the split
`docs/plans/load-test-harness.md` sketched going in. `gen`'s 11.8632s "blocked in send" — more than
either transform's own process time — is not generator slowness: it's `generate_in` running faster
than `json`/`kv_metrics` can drain it and spending most of the run backpressured on `Fanout::send`,
exactly the reading `internal-telemetry.md`'s verdict rule gives ("the constraint is downstream of
`gen`"). That's the expected, healthy shape for this harness: the generator should never be a
scenario's bottleneck.

### `aggregate`

```
node               kind           role         events in  events out  batch in batch out  process s  blocked s     send s  buf max
windowed           aggregate      transform     20000000       12000    200000        12     7.7434     0.0001     0.0000        -
gen                generate_in    listener             0    20000000         0    200000     0.0000     8.0095     0.0000        -
out                null_out       sink             12000           0        12         0     0.0000     0.0000     0.0000     0.00
DROPPED 20000000 events at `windowed` (reason=absorbed)
```

Only one real node, so `windowed` (`aggregate`) is trivially **100%** of measured Σ process time
(7.7434s). The `DROPPED 20000000 events (reason=absorbed)` line is expected, not a bug: every input
event is folded into a per-series `DdSketch` and never itself re-emitted — `Transform::process`
returning `None` on every call is exactly what a stateful aggregator does between flushes. Output is
12 flush-tick batches (`batch out`) carrying 12,000 events total — 1000 events per tick, one per live
series, matching `host: h{seq%1000}`'s cardinality. `gen` again shows most of its own time (8.0095s)
blocked in `send`, the same
backpressure reading as `json-parse` — `aggregate`'s `DdSketch::add` plus its periodic flush is
cheap enough per event that `generate_in` is still the faster of the two nodes.

## 3. `buffered`: variance, investigated

`buffered` is `passthrough`'s exact graph with `buffer.disk:` turned on
(`docs/adr/disk-backed-sink-buffer.md`). W7a's tuning pass saw its throughput range from roughly 16k
to 790k events/s across repeated runs at the same count — by far the widest spread of any scenario —
and this run's own solo 3-repeat spread (626,504 → 535,735 → 309,104 events/s, monotonically
falling) reproduced the same shape even with nothing else on the machine. A dedicated solo
`--repeat 5` pass, run twice, found the actual mechanism:

**With a spool already left over from the prior 3-repeat run** (`perf/results/spool/`, ~21 MB at the
start):

```
repeat 1/5: 134,051 events/s   7.922 µs/event   56.3 MiB peak RSS
repeat 2/5:  75,456 events/s  13.725 µs/event   69.8 MiB peak RSS
repeat 3/5:  53,426 events/s  19.164 µs/event   83.5 MiB peak RSS
repeat 4/5:  37,720 events/s  26.910 µs/event   96.6 MiB peak RSS
repeat 5/5:  26,720 events/s  37.771 µs/event  110.4 MiB peak RSS
```

**After deleting `perf/results/spool/` and re-running the same `--repeat 5`:**

```
repeat 1/5: 615,039 events/s   2.704 µs/event   26.5 MiB peak RSS
repeat 2/5: 939,447 events/s   1.626 µs/event   33.8 MiB peak RSS
repeat 3/5: 289,315 events/s   3.948 µs/event   42.8 MiB peak RSS
repeat 4/5: 125,908 events/s   8.428 µs/event   56.6 MiB peak RSS
repeat 5/5:  79,711 events/s  13.008 µs/event   69.9 MiB peak RSS
```

Both runs degrade monotonically after their first repeat or two, and peak RSS climbs in lockstep —
that's the signature of the same thing happening both times, just from a different starting point.
The cause: `DiskQueue::open` (`crates/logit-pipeline/src/disk_queue.rs`) unconditionally reads and
CRC-walks the **entire active segment file** to validate it for a torn tail, on every startup — an
O(segment size) cost paid whether or not the read cursor actually has anything left to replay
(`cursor.json` was caught up to end-of-segment in every case here: zero real records replayed, only
the validation scan). `buffered.yaml`'s spool is never cleared between repeats or between separate
`script/perf` invocations, and its segments are ~21 MB per 1.2M-event run against a default
`segment_bytes` rotation threshold of 64 MiB — so consecutive repeats keep appending to, and
re-validating, the *same, growing* active segment until it finally rotates. This is not the
real-disk-I/O-contention explanation the scenario's comment carried before this investigation; it's
a harness artifact, reproducible solo, with no contention required at all.

**What this means for the number in §1's table:** `535,735` events/s is a real median of three
repeats run back-to-back against a growing spool, exactly the condition that produces this
degradation — it is not a steady-state number, and re-running `buffered` alone will very likely
reproduce a different value depending on how much that run's spool has already accumulated. Deleting
`perf/results/spool/` before a solo `buffered` comparison is the practical workaround today. The real
fix — `script/perf run`/`attribute` clearing a disk-backed scenario's spool directory before each
invocation — is filed as follow-up work in `docs/known-gaps.md`, not built here: it's a harness
change, not a `crates/` one, and this workstream is docs-plus-one-count-nudge only. `buffered`'s own
comment in `perf/scenarios/buffered.yaml` now carries this same account.

## 4. Before/after: the regression workflow

```sh
script/perf run --repeat 3 --profile release --label before   # on the base commit
# ... make a change ...
script/perf run --repeat 3 --profile release --label after    # on the changed commit
script/perf compare perf/results/<before-file>.json perf/results/<after-file>.json --threshold 5
```

`compare` prints each scenario's Δ% on events/s, CPU µs/event, and peak RSS between the two files'
medians, and exits non-zero if events/s dropped or CPU µs/event rose by more than `--threshold`
percent (peak RSS is reported but never gates the exit code unless `--rss-threshold` is also given).
It also warns — not fails — on a hostname or CPU-model mismatch between the two files, since neither
number is trustworthy across machines per the preamble's ~20% caveat. A scenario present in only one
file is listed, not compared. Given §3, treat a `buffered` regression from `compare` with real
suspicion until `perf/results/spool/` is confirmed clean on both sides — today, nothing enforces
that for you.

## 5. Flamegraph

```sh
script/perf flamegraph --scenario passthrough
```

Builds `-p logit-cli --profile profiling` (a new root profile: `inherits = "release"`, plus
`line-tables-only` debug info and an unstripped symbol table `perf` needs — see the profile's own
comment in the root `Cargo.toml`), runs `perf record -F 999 -g --call-graph dwarf` against it through
the same spawn/settle/SIGTERM machinery `run` uses, then pipes `perf script | inferno-collapse-perf |
inferno-flamegraph` into an SVG. This run captured 6.6s of `passthrough` at 999 Hz, wrote
`perf/results/passthrough.svg` at **1,179,979 bytes** (~1.13 MiB, kept out of git — `/perf/results/`
is gitignored), and symbols resolve cleanly: real, demangled Rust paths throughout
(`logit_core::event::EventBatch`, `logit_inputs::generate::GenerateInput`,
`logit_core::interner::resolve`, …), not hex addresses.

**The container needs three flags, not two.** `--cap-add SYS_ADMIN` and `--security-opt
seccomp=unconfined` were predicted by the ADR (`perf_event_open`'s capability requirement, and
docker's default seccomp profile gating it); this repo's own dev box (Fedora, SELinux Enforcing)
needed a third that wasn't: `--security-opt label=disable`. With the default container label,
SELinux denies the `perf_event` class outright regardless of capabilities — `perf record` still
exits 0 but writes a zero-sample `perf.data`, which reads exactly like the
`kernel.perf_event_paranoid` problem the first two flags exist for and isn't. `script/perf`'s own
`flamegraph` path carries all three now (`docs/adr/load-test-harness.md`'s "Profiling" section has
the full account).

**`perf` re-raises SIGTERM after a clean capture.** For a scenario that doesn't self-exit
(`native-relay`), the harness's settle-then-SIGTERM goes to `perf` (the process it actually spawned),
which forwards the signal to `logit` underneath it, waits for that clean exit, writes `perf.data`,
and *then* re-raises SIGTERM on itself — verified end to end against `native-relay`, and why
`crate::run::spawn_and_measure`'s wrapper path treats a wrapper's own SIGTERM death as a completed
run rather than a failure.

## 6. Reading `attribute`'s output

Each row is one component, sorted by Σ `process.duration` descending. Columns: `events`/`batch`
in/out (received vs. sent, from the runtime's uniform layer-2 counters —
[`internal-telemetry.md`](internal-telemetry.md)); `process s` (Σ time inside
`Transform::process`/`run_lua`, transforms only); `blocked s` (Σ time a node spent inside
`Fanout::send` waiting on a downstream consumer — high here means *that node's own* consumer is the
constraint, not the node itself); `send s` (Σ sink delivery time, sinks only); `buf max` (peak
`buffer.utilization`, sinks with a queue only, `-` otherwise). The verdict line names the node with
the largest Σ process time, then calls out every node whose blocked time is notable, with the
downstream-constraint reading spelled out — see §2's two worked examples above for what that looks
like in practice.

## Open questions

- **When and how this harness runs in the ongoing development process is still deliberately
  undecided** — [ADR `load-test-harness`](../adr/load-test-harness.md)'s own open question. Nightly,
  manually triggered, gating a PR on `compare --threshold`, or some other cadence is real future
  work; nothing here assumes an answer, and nothing wires the harness into CI, a pre-merge gate, or
  a schedule yet.
- **`buffered`'s variance** is now mechanistically understood (§3) but not fixed — clearing a
  disk-backed scenario's spool before each harness invocation is filed in `docs/known-gaps.md` as
  follow-up work, not done in this workstream.
- **Templated metric names permanently grow the process-wide interner**, one entry per distinct
  rendering, for the life of the process (`generate_in`'s own module doc; `docs/design/memory.md`
  §4). `generate_in` already refuses a bare `{seq}` there for exactly this reason, and every shipped
  scenario that wants cardinality templates an *attribute* instead (`aggregate.yaml`'s `host:
  h{seq%1000}`, not a metric name) — so nothing here actually exercises the metric-name path at
  scale. Whether that path is worth keeping at all, given every real scenario avoids it, is an open
  question rather than a decision made here.
