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
| Every scenario, 3 repeats | `script/perf run --repeat 3 --profile release --label quiet` |
| Per-node time attribution | `script/perf attribute --scenario json-parse` / `--scenario aggregate` |
| A flamegraph | `script/perf flamegraph --scenario passthrough` |
| Before/after regression check | `script/perf compare <a.json> <b.json> --threshold 5` |
| What's discoverable | `script/perf list` |
| A real-socket UDP scenario | `script/perf run --scenario udp-statsd --repeat 5 --pin-sender 0,1 --pin-child 2,3` |
| Its zero-drop self-check | `script/perf run --scenario udp-statsd --verify --pin-sender 0,1 --pin-child 2,3` |

> Numbers below were taken on a Fedora Linux 44 (Workstation Edition) host, kernel
> `7.2.4-200.fc44.x86_64`, x86-64, AMD Ryzen AI 9 HX 370 w/ Radeon 890M (24 logical CPUs), inside
> the dev container (`rustc 1.98.1 (48a229cea 2026-09-01)`), `release` profile, at commit
> `fecbd9337010f95d722e89946e1a3e3aa43c007b` (clean working tree), on 2026-09-14 at roughly 01:22Z,
> with the host otherwise idle — no other `script/*` work, nothing else heavy running on the machine
> at the time. Unlike the run this table used to carry, this one was also taken **on battery power,
> not mains** — the host's CPU frequency-scaling governor can clock down under battery, which can
> depress every absolute number below relative to a plugged-in run; read events/s and CPU µs/event
> here as possibly conservative, not as a hardware ceiling, though the *relative* shape (which
> scenario costs more than which) should still hold. `perf/results/*.json`'s own `hostname` field
> records the dev container's own hostname (`ad7e7699c92c`), not the physical host, since every
> `script/*` command runs inside it; `cpu_model` and `nproc` come from `/proc/cpuinfo`/`nproc` as
> seen *inside* that container, which is why they're restated here in prose rather than only trusted
> from the JSON. See `docs/design/memory.md`'s own preamble for why this matters: a busier box has
> shown "~20% slower across every unchanged benchmark" before now, so **treat every number below as
> this-machine-this-day, not a portable constant** — `compare` warns on a host/CPU-model mismatch
> for exactly this reason, and CPU µs/event, not wall-clock events/s, is what it actually gates a
> regression on.

For continuity: the previous recorded run this table carried, taken 2026-09-13 on a busy, contended
machine (`c75399d8bccc`, `perf/results/20260913T104956Z-c75399d8bccc-recorded.json`), was 14–27%
slower across the board in events/s than this quiet run, scenario for scenario — except `aggregate`
(an apparent -15%, which the noise sub-section below shows is run-to-run variance rather than a
real slowdown) and `fanout` (-5% against its then-current 55M count, essentially flat). `buffered`'s own
before/after story is its own section, §3.

## 0. What this measures, and what it doesn't

**Measures**, per scenario, per repeat, from `wait4`'s rusage and two log lines the process itself
emits under `--log-format json`: its own readiness line (`tracing::info!(target: "logit", "ready")`,
logged once the bind pass has opened every listener's socket and every node has been spawned —
`crates/logit-pipeline/src/runtime.rs`) and its generator's `generation complete` line. `wall_s` is
the gap **from `ready` to completion**, not from spawn — process bring-up (loading the binary,
binding sockets, spawning every node) happens before `generate_in` sends a single event, so folding
it into `wall_s` would count startup as part of the graph's own per-event cost. A repeat whose
`ready` line never arrives (a binary built without the log line, or an unexpected race) falls back
to the old spawn → completion measurement for `wall_s`, loudly — `crate::run` prints a warning to
that repeat's own stderr rather than silently reporting a startup-inflated number:

- **events/s** — `count / wall_seconds`. Real, useful, but the noisiest of the three on a shared
  box (see the preamble above) — read it as a rough throughput sense, not the regression gate.
- **CPU µs/event** — `(ru_utime + ru_stime) * 1e6 / count`. **The headline signal.** CPU time this
  process actually spent doesn't care what else the box was doing at the same moment, which is what
  makes it the number `script/perf compare --threshold` gates a regression on.
- **peak RSS** — `ru_maxrss` (kibibytes on Linux, converted to bytes). Reported for every scenario,
  especially informative for `buffered` (real segment-file I/O and buffering), but never gates
  `compare`'s exit code unless `--rss-threshold` is passed explicitly. **In a short run this
  number is roughly half jemalloc's freed-but-not-yet-purged pages** — see §1's "Peak RSS" sub-
  section for the paired measurement and how to read a scenario's RSS against its live data.
- **startup_s** — spawn → `ready`, alongside the other three (a column in `run`'s table, a field
  next to `wall_s` in the results JSON). `compare` *warns* — never gates the exit code — on a
  startup regression past `--threshold`: process bring-up is a different question from the graph's
  own per-event cost, worth a human's attention without failing a throughput/CPU/RSS-focused gate.
  `null` in the JSON (`n/a` in the table) for a repeat whose `ready` line never arrived.

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
  ~20% caveat. `script/vm` (`docs/adr/disposable-azure-perf-vm.md`) provisions a disposable Azure
  VM with four homogeneous cores and nothing else running for exactly this; a number taken there
  should be captioned with its `~/logit-vm-metadata.txt` (CPU model, kernel, image version, sysctls)
  the same way a machine caption works today, and note that the VM has no virtualized PMU, so
  `flamegraph` there is a `cpu-clock`, not `cycles`, profile.

### Driven scenarios: `udp-statsd*` is measured differently, on purpose

The `udp-statsd`/`-small`/`-packed` family ([ADR
`udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md),
added 2026-09-18) is the first that isn't generator-driven. Its config has **no `generate_in`**;
real UDP datagrams arrive over a loopback socket, sent by `logit-perf` itself from a traffic model
in [`perf/load/`](../../perf/load/README.md) calibrated against a real DogStatsD/statsd client
capture. Four things about reading its numbers differ from everything else in this document:

- **The denominator is events *delivered*, never events sent.** This is the first scenario family
  where those two can honestly differ: a real socket may drop datagrams in the kernel before
  `logit` ever sees them, and the baseline is deliberately tuned into a regime where a small
  fraction does. `events_per_s` and `cpu_us_per_event` are computed over
  `logit.component.events.received` at the sink; denominating over sent would silently understate
  the per-event cost by exactly the drop rate. The results JSON carries the whole picture in a
  `udp:` block (sent / received / read syscalls / kernel-dropped / queue-dropped / delivered), and
  `run`'s table prints it -- including a `fill` column, `received / reads`, which is the mean number
  of datagrams one `recvmmsg(2)` call returned and so the only reading of whether
  `receive.read_batch` is the constraint on this workload or an irrelevance
  (ADR `udp-intake-batching-and-socket-visibility`).
- **The telemetry leg is inside the measured process.** That delivered count only exists in the
  child's own self-telemetry, so `run` attaches the same `internal → file_out format: native` leg
  `attribute` uses — meaning a driven scenario's `wait4` rusage includes two of the harness's own
  nodes. **Its absolute CPU µs/event is therefore comparable only to its own history**, never to a
  generated scenario's number, which pays neither that cost nor loopback UDP's kernel-side one.
  Relative movement run to run is the signal; the absolute figure is not a cross-scenario ranking.
- **Pinning is required, not advisory.** `--pin-sender`/`--pin-child` (`sched_setaffinity`, applied
  to the child between `fork` and `exec` so every thread it creates inherits the mask). This box's
  heterogeneous cores — `lscpu -e`'s `MAXMHZ` column separates the 5,158 MHz Zen 5 cores (CPUs 0–3
  and their SMT siblings 12–15) from the 3,289 MHz Zen 5c ones (4–11, 16–23) — make an unpinned run
  bimodal by roughly 2×. Every recorded `udp-statsd*` number below states which CPUs it used.
- **A delta is a pair taken in one sitting, interleaved, on a box in a known state.** Pinning fixes
  which cores a run gets; it says nothing about what they will do an hour later. Measured: the same
  specs, commit and pins, 90 minutes further into a session, moved CPU µs/event ~23% and a drop rate
  from 3.1% to 12.4% — confirmed as the box, not the code, by re-running the earlier commit straight
  afterwards and reproducing the later numbers. So parent/branch runs alternate within one session
  and are never diffed against a stored file from another day, and the box is checked first: on AC,
  `performance` governor, rested, nothing else building, sender and child on distinct fast physical
  cores. [`perf/load/README.md`](../../perf/load/README.md)'s "Box state" has the checklist; `run`
  records what it can of it into the results file (`box_state`) and warns before the first scenario
  on `powersave` or battery. Tables in this document are labelled by session, not presented as one
  series across days.
- **Every run self-checks before its numbers count.** `sent == received + kernel-dropped` has to
  close exactly (on loopback there is nowhere else for a datagram to go), the kernel socket sampler
  has to have reported at all (without `getsockopt(SO_MEMINFO)` the drop count is unknowable and
  would read as a flat zero), and the listener must report no decode diagnostics — otherwise the
  scenario would be benchmarking the malformed-line path, which is *faster* than the real one.
  `--verify` adds the strict form: `--rate-scale 0.25` plus an exactly-equal delivered event count
  with zero drops.
- **`--rate-scale` moves the operating point without editing a spec.** The shipped rates sit just
  above the drop knee, which is what a baseline wants and what reading a stable CPU µs/event does
  not; `--rate-scale 0.5` gets the second. The effective rate is recorded in the results file, shown
  in `run`'s driven table, and `compare` warns when two runs used different ones — they are
  different points on the load curve, not a before and after. Given alongside `--verify` it replaces
  that flag's own 0.25 derate but **not** its exact-delivery assertion, so `--verify --rate-scale
  1.0` asks "is this spec's shipped rate loss-free?" and is expected to fail whenever anything
  drops — which, since the rates are tuned to drop a little, is the healthy answer.

## 1. Results: all ten scenarios, median of 3

`script/perf run --repeat 3 --profile release --label quiet`, solo, on battery, with the host
otherwise idle (see the preamble above). Sorted as `script/perf list` orders them (alphabetical);
`count` is each scenario's configured `generate_in.count` at the time of this run. The new
**events/s (min–max)** column is the same three repeats' spread that produced the median — see the
noise sub-section right after this table for what it means when that range is wide.

| Scenario | Count | events/s | events/s (min–max) | CPU µs/event | Peak RSS | Wall |
|---|---:|---:|---:|---:|---:|---:|
| `aggregate` | 20M | 2,890,700 | 2,696,845 – 4,477,122 | 0.347 | 33.3 MiB | 6.92 s |
| `buffered` | 1.2M | 1,087,248 | 1,079,632 – 1,099,616 | 1.522 | 26.6 MiB | 1.10 s |
| `encode-human-devnull` | 8M | 999,423 | 974,599 – 1,014,536 | 1.330 | 176.8 MiB | 8.00 s |
| `encode-native-devnull` | 8M | 1,189,960 | 1,165,315 – 1,224,892 | 1.180 | 168.9 MiB | 6.72 s |
| `fanout` (re-shaped; quiet run at `7ead7a4`, see below) | 20M | 3,311,403 | 3,270,606 – 3,535,480 | 0.572 | 12.3 MiB | 6.04 s |
| `json-parse` | 7M | 945,491 | 795,169 – 991,003 | 2.054 | 294.5 MiB | 7.40 s |
| `lua` | 4M | 628,380 | 554,182 – 776,491 | 2.085 | 29.4 MiB | 6.37 s |
| `native-relay` | 7M | 1,164,963 | 691,884 – 1,465,330 | 1.118 | 130.0 MiB | 6.01 s |
| `passthrough` | 20M | 3,078,773 | 2,803,814 – 3,360,304 | 0.478 | 71.0 MiB | 6.50 s |
| `route` (added later; quiet run at `506e4ca`, see below) | 20M | 3,747,289 | 3,600,158 – 3,956,587 | 0.668 | 82.3 MiB | 5.34 s |

A few readings, cross-referencing `perf/scenarios/*.yaml`'s own comments for what each measures:

- **`passthrough`** (0.478 µs/event; 0.404 on the same quiet-battery setup at `7ead7a4`, after #189
  stopped resolving symbols in `estimated_heap_bytes` — see the `fanout` bullet) is the runtime
  floor every other scenario is read relative to: scheduling, the `Fanout` channel hop, layer-2
  telemetry, no parsing or encoding. **Most of that
  floor is the generator, not the runtime.** A 2026-09-14 `attribute` pass on `passthrough` (after
  #189, busy box) had `gen` blocked in `Fanout::send` for only 0.36 s of a ~9.3 s run and the
  sink's queue never above 5% of its 1024-batch bound — `null_out` keeps up and `generate_in`'s
  own render loop sets the pace. The matching flamegraph splits the same way: the `generate_in`
  task is ~30% of samples (`render_one` ~14%, the six sorted `AttrMap::insert_sym`s ~3.4%, the
  two templated `Bytes::copy_from_slice`s ~1%); the whole sink task is ~17.5%, and of that ~11.3%
  is dropping the batch after delivery (freeing 200 `Bytes` + the `Vec<Event>` per 100-event
  batch — the cost of owning the data, not of the channel) and ~3.6% is `estimated_heap_bytes`
  at `SinkQueue` admission. Everything else on the single-consumer path — the `mpsc` hop, the
  one `Arc::new` in `drain_inbox`, the queue's two lock/notify pairs, `deliver_with_retry`'s
  timeout registration — is under 3% of samples combined. So a change to the delivery path can
  move this number by a few percent at most; a cheaper generator would move it more.
- **`fanout`** was re-shaped on 2026-09-14, after the run the rest of this table records, and its
  row above is the one exception to the table's provenance: a separate quiet run (host idle, on
  battery, same dev container and `rustc`) at `7ead7a4`, `--repeat 3`, after #189. Before the
  re-shape it generated a one-attribute event (`host: web-{seq%20}`) at 55M, and this section read
  its 0.466 µs landing under `passthrough`'s 0.478 µs as "three sends costs barely more than one".
  That reading was wrong: the two scenarios generated different events, and `passthrough`'s six
  attributes (two templated) cost the generator roughly 6× more per event than `fanout`'s one, which
  is more than the two extra sinks cost. A 2×2 that crossed both topologies with both event
  templates (same busy box, same invocation, `--repeat 3`, medians, CPU µs per *generated* event,
  after #189) makes the actual relationship plain — one consumer is cheaper than three whichever
  event shape is held fixed:

  | CPU µs / generated event | one-attribute event | six-attribute event |
  |---|---:|---:|
  | 1 × `null_out` | 0.304 | 0.624 |
  | 3 × `null_out` | 0.488 | 0.920 |

  `fanout.yaml` now generates `passthrough.yaml`'s exact event at `passthrough`'s exact count
  (20M), so the two differ only in consumer count — which is what the scenario was always meant to
  isolate. Read `fanout` − `passthrough` as the marginal cost of two more sinks (per-edge `Arc`
  clone plus one more `drain_inbox → SinkQueue → write_loop` hop each), and halve it for one. Note
  that `cpu_us_per_event` divides by `generate_in.count` — *generated* events — for every
  scenario; a fan-out scenario delivers `count × consumers` batch-events, so its per-delivery cost
  is that much lower than the column shows. On the quiet `7ead7a4` run the two scenarios, same
  event, same count, same invocation, came out at 0.404 (`passthrough`) and 0.572 (`fanout`) µs per
  generated event — two extra sinks cost 0.168 µs, or ~0.08 µs per generated event per extra
  consumer, which is the honest per-edge price of an `Arc` clone plus a `drain_inbox → SinkQueue →
  write_loop` hop for a 100-event batch. Wall-clock throughput was essentially identical (3.27M vs
  3.31M events/s), as it should be for a generator-bound graph: the extra sinks run on otherwise
  idle workers. `passthrough`'s own 0.404 here against 0.478 in the row above is #189's saving on
  this path (six fewer interner resolves per event at queue admission), not run-to-run noise —
  the two are different commits.
- **`route`** was added on 2026-09-14 after the rest of this table, so its row is the other
  exception to the table's provenance: a quiet run (host idle, on battery, `--repeat 3`) at
  `506e4ca`, with `passthrough` re-run in the same invocation as the control (3,252,961 events/s,
  0.394 µs/event, 37.6 MiB — consistent with the `7ead7a4` number above). Same six-attribute
  event and count as `passthrough`/`fanout`, through a `route` keyed on `host` (ten values: nine
  routed three-per-target onto three `target`s, one left unrouted onto the router's own
  `null_out`), so every 100-event batch is split into four ~25-event batches and every event is
  delivered exactly once — the same data volume as `passthrough`, one hop longer.
  **0.668 vs 0.394 µs per generated event: the router topology costs 0.274 µs on top of
  `passthrough`, and about three-quarters of that is the router itself.** `attribute --scenario
  route` puts `split`'s `process s` at 4.11 s over 20M events — 0.206 µs/event inside `route_batch`
  (one `AttrMap::get_sym` plus a linear scan of nine byte-string alternatives per event, then the
  count/reserve/move passes, `crates/logit-pipeline/src/runtime.rs`), at 5 allocations per batch
  as `docs/adr/target-components.md` pins. The remaining ~0.07 µs is three more sink-side hops
  (`drain_inbox → SinkQueue → write_loop`) on quarter-size batches, where the fixed per-batch
  cost is amortized over 25 events instead of 100; `sys_s` also rises from 0.09 s to 0.94 s
  (more tasks parking and waking), the same shape `fanout` shows. Read against `fanout`
  (0.572 µs, every event delivered three times): routing a stream once costs more CPU than
  fan-out delivering it three times, because `fanout`'s extra work is refcount bumps and
  `null_out`'s empty `send`, while `route` does real per-event work. Wall throughput
  (3.75M events/s) is in the same band as `passthrough`'s, and `gen` was blocked in send only
  0.30 s of the run — still generator-bound, with the router on its own task. **The 82 MiB peak
  RSS against `passthrough`'s 38 MiB is jemalloc retention, not buffered events.** The live-data
  bound is small: the router's 64-slot inbox holds at most 64 × 100 events × 864 B ≈ 5.5 MiB, the
  four sink inboxes another ≈ 5.5 MiB between them, and `attribute` showed every sink queue
  essentially empty (`buf max` 0.00). Under immediate purge (the "Peak RSS" sub-section below)
  `route` is 24 MiB to `passthrough`'s 17.5 — that 6 MiB *is* the extra in-flight data. `route`
  retains more than any other scenario because it churns more page-sized allocations: every
  batch's 86 KiB `Vec<Event>` is freed by the router after its events are moved into four fresh
  `reserve_exact` vectors, which four sink tasks then free on whichever worker threads they happen
  to run on, so dirty pages pile up across more arenas before decay purges them.
- **`json-parse`** (2.054 µs/event) and **`lua`** (2.085 µs/event) are the two most expensive
  single-hop scenarios, essentially tied on this run — real parsing and a LuaJIT round trip both
  cost noticeably more than a native transform, matching `docs/known-gaps.md`'s existing account of
  the Lua boundary's cost relative to a native transform (the standing "~9× the per-event
  allocations" comparison there is a microbench number for one hop; this is the same relationship
  showing up end to end).
- **`encode-human-devnull`** vs **`encode-native-devnull`** (1.330 vs 1.180 µs/event): the native
  encoder is measurably cheaper than the human-readable render at the same event stream, as
  `docs/design/wire-protocol.md`'s design intent would predict — dictionary-first framing beats
  formatting text.
- **`native-relay`** (1.118 µs/event) is the full encode → loopback TCP → decode → ack round trip in
  one process, and lands below both `encode-*-devnull` scenarios and well below `json-parse`/`lua`
  — a real network hop and an ack wait, but still cheaper than a parse-heavy or Lua-heavy graph.
- **`aggregate`** (0.347 µs/event) is nearly as cheap as `passthrough` per event despite sketching a
  1000-series distribution and running a 1s flush tick — see §2 for why: almost all of it is one
  node's `DdSketch::add`, and the flush tick's own cost is amortized over several ticks a run (this
  run's own median repeat took 6.92s wall, ~6-7 ticks at this scenario's 1s interval; §2's
  attribution run is a separate, earlier invocation with the `internal` leg attached and took
  roughly 11.2s wall, hence its 12 flush-tick batches). `aggregate` is also this table's noisiest
  scenario by far — see the sub-section right below.
- **`buffered`** — see §3, now resolved: the wide run-to-run swings first seen in W7a were the
  harness's own un-cleared spool, fixed by W8 (#165) and confirmed on a quiet machine there. The
  `1,087,248` events/s above is a real quiet-machine median of three back-to-back repeats, but §3's
  own dedicated `--repeat 5` pass (632,897–873,406 events/s) is the more representative number for
  this scenario's steady-state spread — three repeats is thin for a scenario whose repeats vary by
  design.

### Noise: `aggregate`'s spread isn't a regression

Even solo, on an idle machine, `aggregate` is the noisiest scenario in this table by a wide margin.
A dedicated `aggregate`-only `--repeat 3` rerun, isolated from the rest of the suite, gave
3,894,156 / 2,588,602 / 3,396,968 events/s — median 0.296 µs/event, identical to the 2026-09-13
recorded run's own number — a spread of roughly ±25% around the median from three repeats alone.
`passthrough`, `json-parse`, and `lua` show real repeat-to-repeat spread too (this table's
min–max column), but nothing else here comes close to `aggregate`'s range; a scenario built around
a periodic flush tick (§2) is inherently more exposed to exactly where the tick boundary falls
inside a short run than one with no such boundary.

`compare`'s single `--threshold 5` therefore flags `aggregate` spuriously on nothing but its own
ordinary variance — this run's apparent -15% against the 2026-09-13 recorded run (preamble above)
is exactly that, not a regression. Two follow-ups, not built here: gate `compare` on each file's
`min` (or another variance-aware statistic) rather than a bare median-to-median diff, or give it a
per-scenario threshold so a flush-tick scenario can carry a wider band than `passthrough`'s.
Raising `--repeat` specifically for flush-tick scenarios, so the reported median is less exposed to
any one repeat's tick alignment, is a third, cheaper option worth trying before either.
`docs/known-gaps.md`'s harness entry carries the same recommendation.

### Peak RSS: what is live data and what is jemalloc retention

`logit` runs on jemalloc (ADR `jemalloc-global-allocator`), which returns freed pages to the kernel
on a decay schedule (`dirty_decay_ms` = 10 s by default) rather than at `free`. A scenario that
runs for 5–10 s therefore reports a peak RSS that includes most of what it freed along the way,
not just what it held at its high-water mark. To separate the two, the whole suite was run twice
at `a9c00c1` (host idle, on battery, `--repeat 3`, release): once as-is, once with
`_RJEM_MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0`, which purges at `free` and makes peak RSS
a close proxy for peak live data. Medians:

| Scenario | Peak RSS, default decay | Peak RSS, immediate purge | Reading |
|---|---:|---:|---|
| `aggregate` | 47.1 MiB | 24.1 MiB | 1000 live `SeriesKey`s + window state |
| `buffered` | 25.7 MiB | 18.3 MiB | disk spool; little in memory |
| `encode-human-devnull` | 181.7 MiB | 85.0 MiB | **sink queue full** (see below) |
| `encode-native-devnull` | 211.5 MiB | 84.9 MiB | **sink queue full** |
| `fanout` | 12.3 MiB | 14.5 MiB | nothing retained: one shared batch, freed once |
| `json-parse` | 39.9 MiB | 22.8 MiB | post-#187/#189, nothing backs up |
| `lua` | 27.8 MiB | 19.4 MiB | |
| `native-relay` | 136.7 MiB | 86.5 MiB | **`logit_out`'s sink queue full** (ack-bound) |
| `passthrough` | 37.2 MiB | 17.5 MiB | one 64-slot inbox ≈ 5.5 MiB + baseline |
| `route` | 80.6 MiB | 24.4 MiB | five 64-slot inboxes ≈ 11 MiB + baseline |

Two things fall out:

- **Where the sink is slower than the generator, RSS really is queue depth**, and it is the
  sink queue's default `buffer.max_bytes` of 64 MiB that sets it: a 100-event batch of this
  shape weighs ~87 KiB by `estimated_heap_bytes`, so the byte bound trips at ~770 batches, well
  before the 1024-batch bound; add the 64-slot inbox and the process baseline and you get the
  ~85 MiB the three sink-bound scenarios (`encode-*`, `native-relay`) all converge on under
  immediate purge. Their default-decay numbers are that plus what jemalloc hadn't returned yet.
  So the "in-flight buffering at default `buffer:` is large" observation stands for those, and the
  bound doing it is `max_bytes`, not `max_batches` or the channels.
- **Where the sink keeps up, RSS is mostly retention.** `passthrough`, `route`, `aggregate`,
  `json-parse` and `lua` all drop by half or more under immediate purge, down to a number that
  matches their in-flight channel data plus process baseline. `route` is the extreme case
  (80 → 24 MiB) because it churns more page-sized allocations per batch than anything else
  (its own reading above); `fanout` the opposite (no re-allocation between generator and sinks,
  the shared batch freed exactly once).

The immediate-purge run's CPU numbers are *not* comparable to anything else in this document —
purging at `free` costs an `madvise` per page-sized free and roughly doubled CPU µs/event for the
churn-heavy scenarios (`route` 0.645 → 1.239, `aggregate` 0.273 → 0.599). It is a diagnostic
setting for reading RSS, not a configuration to run with. When a peak-RSS number looks
surprising, re-run that one scenario with decay 0 before concluding anything about queue bounds.

## 2. Attribution: where a scenario's time actually goes

`script/perf attribute --scenario NAME` appends a temporary `internal → file_out format: native` leg
to a copy of the scenario, decodes the resulting dump, and groups every point by the emitting
component — see [`internal-telemetry.md`](internal-telemetry.md)'s "Reading an attribution dump"
section for the mechanism. Both tables below are from the 2026-09-13 recorded run's own invocation
(`c75399d8bccc`), not the quiet run in §1 above — attribution wasn't re-run on the quiet machine
except for `buffered` (§3) — at each scenario's count then. `__perf_internal`/`__perf_dump` are the
harness's own two nodes, shown for transparency but excluded from the verdict.

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

## 3. `buffered`: variance, resolved

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
**One real, identified contributor:** `DiskQueue::open` (`crates/logit-pipeline/src/disk_queue.rs`)
pays an un-cleared spool's cost twice at every startup, not once. It reads and CRC-walks the
*active* segment in full to validate it for a torn tail (`disk_queue.rs` ~427-441) — an O(segment
size) cost, but a *bounded* one: the active segment can't grow past roughly the default
`segment_bytes` rotation threshold (64 MiB) before a new one starts, so this pass alone is capped
and, on its own, is not obviously large enough to explain a multi-second-scale swing. It then reads
every segment at or after the read cursor a *second* time, this run's own included, to count what's
left to replay (`disk_queue.rs` ~481-497) — real work only when the cursor hasn't caught up to the
end of what's on disk, i.e. exactly what a spool the harness never clears between repeats or
invocations leaves behind. So the un-cleared spool makes every repeat pay a bounded startup
validation scan *plus* a replay of whatever the last checkpoint hadn't covered — one real
contributor to the spread above, not a full accounting of it. This is not the real-disk-I/O-contention
explanation the scenario's comment carried before this investigation, and it is reproducible solo
with no contention required — but whether it explains the *entire* observed spread (including
W7a's 16k-790k range) is not yet established.

**What this meant for the number the 2026-09-13 recorded run's table used to carry, before this
section's fix superseded it:** `535,735` events/s was a real median of three repeats run back-to-back
against a spool the harness never cleared, a condition now known to add real, if only partly
quantified, startup cost on top of whatever else was going on — it was not a steady-state number,
and re-running `buffered` alone could well have reproduced a different value depending on that
spool's prior state. §1's table today carries the post-fix, quiet-machine number instead
(`1,087,248` events/s, with §3's own dedicated `--repeat 5` pass below it as the more representative
read); `535,735` is kept here only as the historical record of what the *unfixed* harness reported.

**W8 (#165, landed here): the harness now clears the spool itself.** `script/perf run`/`attribute`/
`flamegraph` remove every `buffer.disk.path` directory a scenario declares before each spawn — every
repeat, not just once per invocation — refusing to touch anything outside `perf/results/`
(`crates/logit-perf/src/spool.rs`). Manually deleting `perf/results/spool/` before a solo comparison
is no longer necessary; the harness does it for you now, every time. Re-running `buffered` solo on
this fix, `script/perf run --repeat 5 --profile release --scenario buffered`:

```
repeat 1/5: 884,613 events/s   1.810 µs/event   28.8 MiB peak RSS   0.003s startup
repeat 2/5: 510,016 events/s   2.645 µs/event   30.6 MiB peak RSS   0.003s startup
repeat 3/5: 837,626 events/s   1.929 µs/event   27.0 MiB peak RSS   0.004s startup
repeat 4/5: 862,801 events/s   1.902 µs/event   24.8 MiB peak RSS   0.003s startup
repeat 5/5: 792,157 events/s   2.088 µs/event   26.4 MiB peak RSS   0.004s startup
```

> Taken on a busy machine — other work was running on the host concurrently — so read this as
> **indicative only, not `buffered`'s steady-state number**. The quiet-machine confirmation below
> closes the gap.

Even under that contention, the qualitative signature the fix targets is gone. Neither run above is
monotonic any more (repeat 2 dips to 510k, repeat 3 recovers to 838k) — contrast the strictly-falling
134k→75k→53k→38k→27k and 615k→939k→289k→126k→80k sequences earlier in this section, each one falling
every single repeat with no exception. Peak RSS stays flat in a 24.8–30.6 MiB band rather than
climbing 56→110 MiB or 26→70 MiB across the run — the clearest single signal that the spool is no
longer accumulating repeat over repeat. `startup_s` (spawn → `ready`, §0) is small here, 2.6–4.4 ms —
not a meaningful share of `buffered`'s own per-event cost, so process bring-up was never the
explanation; the spool accumulation this fix removes was. The remaining spread in this run (roughly
510k–885k, a repeat-2 dip of about 40% below the top) read as ordinary scheduling noise on a shared,
busy box rather than the harness's own artifact, but that reading wanted confirming quiet-machine
data before it could be treated as settled; `535,735` and the two `--repeat 5` sequences above
remain the honest record of what the *unfixed* harness reported, and 837,626 events/s (1.929
µs/event) was the fix's first post-fix data point. The quiet-machine pass below is that
confirmation.

**Quiet-machine confirmation.** A solo `script/perf run --repeat 5 --profile release --scenario
buffered --label quiet`, host idle, on battery, sha `fecbd9337010f95d722e89946e1a3e3aa43c007b`:

```
repeat 1/5: 632,897 events/s   2.626 µs/event   ~26 MiB peak RSS   ~4 ms startup
repeat 2/5: 659,892 events/s   2.531 µs/event   ~28 MiB peak RSS   ~4 ms startup
repeat 3/5: 641,560 events/s   2.568 µs/event   ~27 MiB peak RSS   ~4 ms startup
repeat 4/5: 780,888 events/s   2.044 µs/event   ~25 MiB peak RSS   ~4 ms startup
repeat 5/5: 873,406 events/s   1.842 µs/event   ~28 MiB peak RSS   ~4 ms startup
```

No monotonic decay, no RSS climb — peak RSS stays in a tight 25–28 MiB band across all five
repeats, the same signature the busy-machine post-fix run above showed, this time with nothing else
on the box to blame for the remaining spread either. `script/perf attribute --scenario buffered` on
the same build backs up where that remaining spread lives: `gen` sent all 1,200,000 events, `out`
received all 1,200,000; `gen` spent 1.3959s of the run blocked in `send`, and neither the sink nor
the listener show any process time of their own — the same downstream-of-`gen` verdict
`internal-telemetry.md`'s reading rule gives everywhere else in this doc, here pointing at the disk
queue's own write/read path as `buffered`'s actual constraint, not the harness or the box.

**The variance this section set out to investigate is resolved.** The spool-accumulation
mechanism — `DiskQueue::open` paying an un-cleared spool's cost twice at every startup, compounding
across repeats that shared one never-cleared directory — is what produced the wild, strictly-falling
16k-790k swings W7a first saw and the two monotonic five-repeat sequences at the top of this
section. With the spool cleared before every repeat (W8, #165), that signature is gone on both a
busy machine and, now, a quiet one. What's left open is narrower than it was: a roughly 1.4× spread
within five repeats even solo and idle (632,897 to 873,406 events/s), and `DiskQueue::open`'s
double-read startup scan (the bounded active-segment validation pass, still real, still there) as an
open question of how much of a *cleared* spool's own first-open cost feeds this scenario's per-repeat
variance versus ordinary scheduling noise — no longer the prime suspect for the spread this section
originally chased, just an unquantified detail. `buffered`'s own comment in
`perf/scenarios/buffered.yaml` and `docs/known-gaps.md`'s entry both carry this same account now.

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
file is listed, not compared. `run` now clears `buffered`'s spool before every repeat (§3, W8, #165), so
the specific accumulation artifact that made a `buffered` regression untrustworthy is gone; treat any
`buffered` comparison with the same ordinary caution as its still-wider-than-most repeat spread
warrants (`docs/known-gaps.md`'s "no cross-run noise model" entry) rather than the spool-specific
suspicion this note used to carry.

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

## 7. UDP intake: recvmmsg and batched queue operations

[ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)
closes two of the costs §0's "Driven scenarios" section named as still open when the `udp-statsd*`
family was added: `BoundedQueue::push_many`/`pop_many` (W3) stop `read_loop` and `decode_loop`
contending on the same per-datagram gauge-lock, and Linux `recvmmsg(2)` batched reads (W4) stop
`read_loop` paying one syscall per datagram. This section is the write-up of what moved; the ADR is
the design record of why.

### Why three driven scenarios, not one

`udp-statsd`, `udp-statsd-small`, and `udp-statsd-packed` share one traffic model
([`perf/load/README.md`](../../perf/load/README.md)) and differ only in `datagram_mix:` — the axis
that decides which half of the receive path a number is actually about:

- **`udp-statsd-small`** packs every line into its own datagram (an unbuffered client, ~40–120 B) —
  the **syscall-bound worst case**. Per-datagram fixed cost dominates a payload this small, so this
  is where both W3's gauge-lock batching and W4's `recvmmsg` should show the most: neither change
  touches decode cost, and decode cost is nearly all there is to amortize against here.
- **`udp-statsd-packed`** packs every datagram to ≤1432 B (DogStatsD's own UDP default, ~13.5
  metrics/datagram) — the **decode-bound** end. Far fewer syscalls and gauge updates per event, so
  whatever the decoder and `BatchAccumulator` cost dominates instead, and there's little left for
  either change to save.
- **`udp-statsd`** mixes both packing targets plus a ≤8192 B local-agent-style share (45% single /
  40% ≤1432 B / 15% ≤8192 B, ~17.9 lines/datagram on average) — the **headline** number, closest to
  what a real mixed-client deployment looks like.

A number from only one of the three would mislead about which half of the pipeline a change actually
helps — see the ADR's "Representative traffic, calibrated against a recorded real-client capture"
section for the full reasoning.

### Measurement protocol

Every table in this section follows the protocol `docs/plans/udp-intake.md`'s "Baseline/delta
recording protocol" and `perf/load/README.md`'s "Box state"/"Pinning" sections settle on, established
because this dev box's numbers move for reasons that have nothing to do with the code:

- **A delta is a parent/branch pair taken in one sitting, interleaved** — parent, branch, parent,
  branch — never a branch diffed against a results file from another day. Two unbroken blocks would
  put one side on the cool half of a session and the other on the warm half, which is exactly the
  artifact this guards against.
- **Pinned to distinct fast physical cores.** `--pin-sender`/`--pin-child` (`sched_setaffinity`,
  applied between `fork` and `exec` so every thread a process spawns inherits the mask). This box's
  heterogeneous cores — Zen 5 physical cores at 5,158 MHz (CPUs 0–3) vs. Zen 5c ones at 3,289 MHz
  (4–11, 16–23), per `lscpu -e`'s `MAXMHZ` column — make an unpinned run bimodal by roughly 2×. Every
  table below states its pins (`--pin-sender 0,1 --pin-child 2,3` throughout).
- **Control-to-control drift is reported alongside every delta, not assumed away.** Each delta table
  below is read next to a same-session, same-code control-vs-control comparison — the two runs that
  differ only in *when* they happened, not in what they ran. A delta smaller than that drift is
  reported as "no signal," not as an improvement.
- **A box-state checklist gates whether a number is worth writing down at all** —
  `perf/load/README.md`'s "Box state" table: AC power (not battery), `performance` governor (not
  `powersave`), a non-power-saving energy-performance preference, thermal headroom (a rested box,
  gaps between repeats), and nothing else building. `logit-perf run` records what it can of this into
  the results file's `box_state` and warns before the first scenario on `powersave` or battery.
- **The denominator is events *delivered* to `null_out`, never events sent.** `udp-statsd*` is the
  first scenario family where those two can honestly differ — a real socket can drop a datagram in
  the kernel before `logit` ever sees it, and the baseline is deliberately tuned into a regime where
  a small fraction does (§0's "Driven scenarios" section above has the self-check machinery that
  keeps this honest).
- **The telemetry leg that makes the delivered count visible runs inside the measured process.** The
  same `internal → file_out format: native` leg `attribute` uses is attached to every driven run, so
  the child's own `wait4` rusage includes two of the harness's own nodes on top of the graph under
  test. **A driven scenario's absolute CPU µs/event is therefore comparable only to its own
  history** — never to a `generate_in` scenario's number (§0/§1 above), and not even to a raw
  syscall-count argument, since the leg's own cost is baked into every number in this section
  identically.

### Measured numbers

**Everything between the markers below is provisional.** This dev box is a laptop whose power/thermal
state was not controlled across this stack's measurement sessions, and what's actually known about
that state differs by table, not uniform across all of them:

- **W2 baseline, W3 delta.** `logit-perf run`'s `box_state` capture (governor, energy-performance
  preference, AC-online) did not exist yet when these were taken — every result file behind these two
  tables predates the field, so there is no recorded power state for either. PR #252's body asserts
  "on mains, host otherwise idle" for the W2 baseline; that claim has no recorded evidence behind it
  in the results JSON, and should be read as an unverified assertion, not a checked fact.
- **W4 delta, `read_batch` sweep.** `box_state` is recorded for every file behind these two tables.
  It shows `scaling_governor: "powersave"` and `energy_performance_preference:
  "balance_performance"` throughout, and `on_ac_power` **false** for every run except the very first
  control of the W4 session (`20260918T184346Z-…-w3-control-A.json`, `on_ac_power: true`) — so the box
  was on AC for that one run and on battery for every run after it, including the entire sweep.
  Separately from anything the harness recorded: the lead checked the host directly during the
  afternoon these were taken and found it on battery under `powersave`, and the box's owner reports a
  warm room with fans running hard under load — real, but observed by hand, not by `box_state` (this
  box has no ACPI `platform_profile` file at all, per `perf/load/README.md`'s "Box state" table, so
  there is no recorded reading of it to cite either way).

Identical code measured minutes apart drifted 3–5% in CPU µs/event during this stack's sessions, and
the control-to-control rows in the W3 and W4 tables are the direct evidence of that, not a formality.
The lead re-takes every number here as interleaved one-sitting pairs on a plugged-in, cooled box (or a
cloud VM), with `box_state` recorded for every run this time, before this workstream is considered
closed for measurement purposes — that pass replaces the block below wholesale rather than amending it
in place, and nothing in the prose above or below this block should be read as depending on today's
specific figures.

<!-- udp-intake-numbers:begin PROVISIONAL 2026-09-18 laptop, powersave, power/thermal state not controlled -->

#### W2 baseline (PR #252, head `912f5574649d`, branch `udp/w2` measured post-`udp/w1`-merge)

`--pin-sender 0,1 --pin-child 2,3`, `--repeat 5`, release, 2026-09-18T15:55:46Z. `events/s` and `CPU
µs/event` denominated over events delivered.

| Scenario | Lines sent | Delivered | events/s | events/s min–max | CPU µs/event | Peak RSS | Wall | Kernel drop % (per repeat) |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `udp-statsd` | 11,074,692 | 10,826,863 | 1,571,498 | 1,563,474 – 1,606,869 | 0.678 | 47.4 MiB | 6.89 s | 2.15 (0.03 / 2.35 / 2.14 / 2.62 / 2.15) |
| `udp-statsd-packed` | 10,788,160 | 10,550,475 | 1,529,623 | 1,527,002 – 1,532,959 | 0.700 | 45.4 MiB | 6.90 s | 2.20 (1.99 / 2.24 / 2.37 / 2.04 / 2.20) |
| `udp-statsd-small` | 5,000,000 | 4,378,147 | 665,299 | 664,096 – 667,049 | 1.978 | 22.9 MiB | 6.58 s | 12.44 (12.21 / 12.61 / 12.29 / 12.49 / 12.44) |

Datagram medians: `udp-statsd` 620,000 sent / 606,677 received / 13,323 kernel-dropped;
`udp-statsd-packed` 800,000 / 782,394 / 17,606; `udp-statsd-small` 5,000,000 / 4,378,147 / 621,853.
Zero receive-queue drops and zero send errors throughout; `sent == received + kernel-dropped` closed
exactly on every repeat. Peak `receive_buffer.utilization` 0.29 / 0.07 / 1.00 —
`udp-statsd-small` sits exactly at the kernel's drop threshold, which is where it was tuned to sit.
Container `net.core.rmem_max` = 4,194,304; `receive_buffer_bytes: 1MiB` is granted as 2 MiB and not
clamped.

#### W3 delta: `push_many`/`pop_many` (PR #253, measured at `2038b29`, branch `udp/w3`)

**Measured at `2038b29`** ("merge udp/w2 into udp/w3", `udp/w3`'s first commit after branching), not
at the PR's final head `1b3228c10351`. The lost-wakeup fix (`c3ac8ae`, "push_many must announce its
prefix before it waits, not after") and the docs-only commit after it (`1b59760`) both landed on this
branch after this pair was taken. The fix does not touch this pair's steady-state measurement — under
the load this pair drives (no batch exceeds the queue's free room, so no `Block` wait is ever
provoked), the change is one extra `not_empty.notify_one()` call on the path that only runs when a
`push_many` call has to wait, never a path this pair's numbers exercise — but the pair is re-taken
regardless at the lead's final pass, on the merged head, rather than trusted on the strength of that
argument alone.

Run as **A–B–A–B in one sitting** (`udp/w2` control, `udp/w3` branch, `udp/w2` control, `udp/w3`
branch) because the drop rate here is the difference between two nearly-equal rates and moves with
the box. `--pin-sender 0,1 --pin-child 2,3`, `--repeat 5`, medians.

| Scenario | | w2 control A | **w3 run A** | w2 control B | **w3 run B** |
|---|---|---|---|---|---|
| `udp-statsd` | CPU µs/event | 0.683 | **0.661** | 0.664 | **0.646** |
| | delivered events/s | 1,573,830 | **1,573,002** | 1,599,504 | **1,601,659** |
| | kernel drop % | 2.03 | **2.04** | 0.48 | **0.32** |
| | queue drop % | 0 | **0** | 0 | **0** |
| | max rcvbuf utilization | 0.95 | **0.45** | 0.93 | **0.48** |
| `udp-statsd-packed` | CPU µs/event | 0.702 | **0.685** | 0.679 | **0.662** |
| | delivered events/s | 1,528,994 | **1,533,565** | 1,558,181 | **1,561,107** |
| | kernel drop % | 2.25 | **1.95** | 0.38 | **0.19** |
| | queue drop % | 0 | **0** | 0 | **0** |
| | max rcvbuf utilization | 1.00 | **1.00** | 0.42 | **0.71** |
| `udp-statsd-small` | CPU µs/event | 1.965 | **1.696** | 1.868 | **1.461** |
| | delivered events/s | 660,845 | **743,956** | 676,996 | **757,295** |
| | kernel drop % | 13.04 | **2.08** | 10.91 | **0.35** |
| | queue drop % | 0 | **0** | 0 | **0** |
| | max rcvbuf utilization | 1.00 | **0.81** | 1.00 | **0.44** |

`logit-perf compare`, each control against the run that followed it:

```
control A -> w3 A          events/s     us/event     peak RSS      drop rate
udp-statsd                    -0.1%        -3.1%        +1.7%      +0.02 pts
udp-statsd-packed             +0.3%        -2.5%        -2.5%      -0.29 pts
udp-statsd-small             +12.6%       -13.7%        +0.0%     -10.96 pts

control B -> w3 B          events/s     us/event     peak RSS      drop rate
udp-statsd                    +0.1%        -2.7%        -2.1%      -0.16 pts
udp-statsd-packed             +0.2%        -2.5%        -0.8%      -0.18 pts
udp-statsd-small             +11.9%       -21.8%        +0.0%     -10.56 pts
```

Control-to-control drift, same code, ~8 minutes apart — the yardstick the two deltas above are read
against:

```
control A -> control B     events/s     us/event     peak RSS      drop rate
udp-statsd                    +1.6%        -2.7%        -0.5%      -1.55 pts
udp-statsd-packed             +1.9%        -3.2%        -7.9%      -1.87 pts
udp-statsd-small              +2.4%        -4.9%        +0.0%      -2.13 pts
```

#### W4 delta: `recvmmsg` (PR #254, head `4a0c252fa530`, branch `udp/w4`)

**Recorded `box_state`: `powersave` governor, `balance_performance` EPP, on battery for every run in
this pair** (see "Measured numbers" above for the one exception and what's observed vs. recorded) —
large effects still show through, small ones should not be read; this is the pass the lead's re-take
supersedes. Interleaved A–B–A–B (`udp/w3` control, `udp/w4` branch, `udp/w3` control, `udp/w4`
branch), `--pin-sender 0,1 --pin-child 2,3`, `--repeat 5`, medians.

| Scenario | | w3 control A | **w4 A** | w3 control B | **w4 B** |
|---|---|---|---|---|---|
| `udp-statsd-small` | CPU µs/event | 1.405 | **1.127** | 1.338 | **1.218** |
| | delivered ev/s | 753,218 | **759,906** | 756,619 | **759,908** |
| | kernel drop % | 0.87 | **0.00** | 0.43 | **0.00** |
| | queue drop % | 0 | **0** | 0 | **0** |
| | max rcvbuf utilization | 0.51 | **0.01** | 0.40 | **0.02** |
| | mean fill (dg/read) | 1.0 | **3.1** | 1.0 | **3.1** |
| | peak RSS | 22.6 MiB | **22.6 MiB** | 22.8 MiB | **23.0 MiB** |
| `udp-statsd` | CPU µs/event | 0.653 | **0.674** | 0.662 | **0.668** |
| | delivered ev/s | 1,597,416 | **1,595,451** | 1,598,690 | **1,596,983** |
| | kernel drop % | 0.60 | **0.71** | 0.50 | **0.62** |
| | queue drop % | 0 | **0** | 0 | **0** |
| | max rcvbuf utilization | 0.45 | **0.35** | 0.33 | **0.26** |
| | mean fill (dg/read) | 1.0 | **24.2** | 1.0 | **24.2** |
| | peak RSS | 49.1 MiB | **52.4 MiB** | 46.4 MiB | **56.8 MiB** |
| `udp-statsd-packed` | CPU µs/event | 0.639 | **0.635** | 0.635 | **0.619** |
| | delivered ev/s | 1,558,385 | **1,559,598** | 1,558,601 | **1,556,111** |
| | kernel drop % | 0.37 | **0.29** | 0.35 | **0.51** |
| | queue drop % | 0 | **0** | 0 | **0** |
| | max rcvbuf utilization | 0.07 | **0.14** | 0.28 | **0.14** |
| | mean fill (dg/read) | 1.0 | **14.2** | 1.0 | **14.2** |
| | peak RSS | 45.3 MiB | **58.7 MiB** | 46.8 MiB | **61.8 MiB** |

(The control's `fill` is 1.0 by construction — `udp/w3` has no `logit.input.reads` counter, so it's
one datagram per `recv_from`.)

```
control A -> w4 A          events/s     us/event     peak RSS      drop rate
udp-statsd                    -0.1%        +3.1%        +6.9%      +0.11 pts
udp-statsd-packed             +0.1%        -0.6%       +29.5%      -0.08 pts
udp-statsd-small              +0.9%       -19.8%        +0.2%      -0.87 pts

control B -> w4 B          events/s     us/event     peak RSS      drop rate
udp-statsd                    -0.1%        +0.8%       +22.3%      +0.12 pts
udp-statsd-packed             -0.2%        -2.7%       +32.1%      +0.16 pts
udp-statsd-small              +0.4%        -9.0%        +1.1%      -0.43 pts

control A -> control B     events/s     us/event     peak RSS      drop rate   (drift, same code)
udp-statsd                    +0.1%        +1.4%        -5.4%      -0.10 pts
udp-statsd-packed             +0.0%        -0.5%        +3.2%      -0.01 pts
udp-statsd-small              +0.5%        -4.8%        +0.8%      -0.45 pts
```

**Below the knee, `--rate-scale 0.5` (zero drops on both sides, so this isolates pure CPU cost from
any drop-rate interaction):** `udp-statsd` −3.1%, `udp-statsd-packed` −4.5%, `udp-statsd-small`
−4.4% µs/event. Smaller than the at-rate deltas above, as expected — at half the rate the per-wakeup
fixed costs make up a larger share of the total, and there are fewer syscalls per unit time for
`recvmmsg` to be saving in the first place.

#### `read_batch` sweep (PR #254, head `4a0c252fa530`, evidence for the default)

`read_batch` ∈ {1, 16, 32, 64, 128, 256}, `--repeat 3`, `--pin-sender 0,1 --pin-child 2,3`, both
scenarios set at once, everything else held. **Every cell below — including `udp-statsd`'s `max
rcvbuf` column, which is not in PR #254's own body — is read from the six results files'
`median.udp` fields** (`cpu_us_per_event`; `received_datagrams / reads` for fill;
`kernel_dropped / sent_datagrams` for kernel drop %; `kernel_rcvbuf_utilization_max`) under
`~/lib/logit/tmp/perf/udp/w4/`:
`20260918T191029Z-unknown-w4-sweep-rb1.json`, `…-191129Z-…-rb16.json`, `…-191229Z-…-rb32.json`,
`…-191328Z-…-rb64.json`, `…-191428Z-…-rb128.json`, `…-191528Z-…-rb256.json` — checked cell by cell
against this table while addressing review, not just spot-checked.

| `read_batch` | **`udp-statsd-small`** µs/ev | fill | kernel drop % | max rcvbuf | | **`udp-statsd`** µs/ev | fill | kernel drop % | max rcvbuf |
|---|---|---|---|---|---|---|---|---|---|
| 1 | **1.718** | 1.0 | **3.65%** | **0.98** | | 0.663 | 1.0 | 0.44% | 0.39 |
| 16 | 1.107 | 3.2 | 0.00% | 0.05 | | 0.660 | 11.9 | 0.74% | 0.39 |
| 32 | 0.979 | 3.6 | 0.00% | 0.01 | | 0.652 | 18.5 | 0.34% | 0.47 |
| **64** | **1.128** | **3.1** | **0.00%** | **0.03** | | **0.648** | **24.2** | **0.44%** | **0.26** |
| 128 | 1.117 | 3.0 | 0.00% | 0.06 | | 0.660 | 26.8 | 0.35% | 0.26 |
| 256 | 1.112 | 3.7 | 0.00% | 0.04 | | 0.654 | 24.3 | 0.63% | 0.25 |

**Peak RSS across the sweep** (`udp-statsd-small`, the slab-size probe): **21.7 MiB at every one of
`read_batch` 16, 32, 64, 128 and 256** — a sixteenfold change in the slab's nominal size (1 MiB → 16
MiB) with no movement in resident memory (not separately measured at `read_batch: 1`). A direct
allocation probe agrees: `vec![0u8; 64 × 65,507]` under this repo's jemalloc adds +52 KiB RSS on
allocation (not 4 MiB), +308 KiB once every slot has held a 100-byte datagram, and +4.1 MiB only if
every byte of every slot is touched (`docs/design/memory.md` §5 carries both figures as a fixed
per-listener row).

<!-- udp-intake-numbers:end -->

### Reading the numbers

**Small datagrams are where both changes land, and that is not a coincidence.** `udp-statsd-small`
is the one scenario where W3's delta clears the control-to-control drift (−13.7%/−21.8% µs/event,
against a same-code drift of −2.7% to −4.9%) and where W4's delta clears it too (−19.8%/−9.0%
µs/event, against a same-code drift of −0.5% to −4.8%), with the kernel drop rate collapsing to
near-zero in both passes. That is exactly the prediction: `push_many`/`pop_many` removed one of the
two per-datagram gauge-lock acquisitions that `read_loop` and `decode_loop` used to contend on, and
`recvmmsg` removed the per-datagram `recv_from` syscall itself — both costs are fixed per datagram,
so both save the most where the payload is too small to amortize them against anything else.

**`udp-statsd` and `udp-statsd-packed` stay within drift at both W3 and W4.** Their few-percent
movements are the same size as (or smaller than) the control-to-control drift measured in the same
session, so they're reported as "no signal," not claimed as wins — the expected shape given how few
syscalls and gauge updates a decode-bound datagram pays per event (~13.5 metrics/datagram on
`udp-statsd-packed` alone).

**The mean-fill column is why any `read_batch` above the arrival burst is equivalent.** `fill` is
`logit.input.datagrams / logit.input.reads` — the number of datagrams one `recvmmsg` call actually
returned. It plateaus around **~3 on `udp-statsd-small`** and **~24 on `udp-statsd`** (and ~14 on
`udp-statsd-packed`) for every `read_batch` from 16 upward: the reader is never more than a few
datagrams behind the arrival rate, so raising the ceiling past that plateau has nothing left to buy.
64 was chosen as the smallest power of two comfortably above both plateaus.

**The read slab is not resident**, which is the other side of that same plateau: a sixteenfold change
in the slab's nominal size (`read_batch` 16 → 256, 1 MiB → 16 MiB of address space) moved peak RSS by
nothing on `udp-statsd-small`. `vec![0u8; n]` under this codebase's jemalloc is a fresh zeroed
mapping, and a small datagram only ever touches the one 4 KiB page of each 65,507-byte slot it
actually writes into — the slab's cost is address space, not memory.

### Open after this workstream

- **Peak RSS rose +7–32% on `udp-statsd`/`udp-statsd-packed` at W4, and the attribution is open.**
  The slab is ruled out (the sweep above holds RSS flat across a sixteenfold slab-size change); the
  working guess is more datagram bytes in flight per turn of the read loop, at roughly 1.4 KB per
  datagram. That is an inference, not a measurement — a 1 s-interval probe during W4 saw the receive
  queue only 8–64 datagrams deep, which is evidence *against* that guess, not for it. The final runs
  should settle this by comparing the receive queue's own byte high-water mark
  (`logit.component.receive.bytes`, sampled more tightly than once a second) against jemalloc's
  `stats.resident`/`stats.retained` — if the queue gauge rises in step with RSS the guess holds; if
  it stays flat while RSS still moves, the allocator's dirty-page retention under a burstier
  allocation pattern is the better explanation and wants its own reading.
- **All numbers in this section are provisional** and taken on a laptop, on battery, on the
  `powersave` governor, thermally throttling — identical code drifted 3–5% in CPU µs/event within
  minutes of itself, which is what the control-to-control rows above are for. The lead re-takes every
  table in the delimited block above as interleaved one-sitting pairs on a plugged-in, cooled box (or
  a cloud VM) and that pass replaces the block wholesale.

## Open questions

- **When and how this harness runs in the ongoing development process is still deliberately
  undecided** — [ADR `load-test-harness`](../adr/load-test-harness.md)'s own open question. Nightly,
  manually triggered, gating a PR on `compare --threshold`, or some other cadence is real future
  work; nothing here assumes an answer, and nothing wires the harness into CI, a pre-merge gate, or
  a schedule yet.
- **`buffered`'s variance is resolved**: the spool-accumulation mechanism §3 identified is fixed on
  the harness side (W8, #165 — every spawn clears a scenario's declared `buffer.disk.path` first),
  and a quiet-machine `--repeat 5` confirmation (§3) shows the same no-decay, flat-RSS signature the
  busy-machine post-fix run did. What's left is narrower and product-side, not harness-side: a
  roughly 1.4× spread within five quiet repeats, and how much of it traces to `DiskQueue::open`'s
  double-read startup scan versus ordinary noise (`docs/known-gaps.md`'s `buffered` entry) — no
  longer the prime suspect it was, just unquantified.
- **`compare` has no variance-aware threshold** (the noise sub-section in §1): `aggregate`'s ordinary
  ~±25% repeat-to-repeat spread, present even solo on an idle machine, is enough on its own to trip
  `compare --threshold 5`. Gating on each file's `min` instead of median, a per-scenario threshold,
  or a higher `--repeat` for flush-tick scenarios are the candidate fixes, none built here
  (`docs/known-gaps.md`'s harness entry).
- **Templated metric names permanently grow the process-wide interner**, one entry per distinct
  rendering, for the life of the process (`generate_in`'s own module doc; `docs/design/memory.md`
  §4). `generate_in` already refuses a bare `{seq}` there for exactly this reason, and every shipped
  scenario that wants cardinality templates an *attribute* instead (`aggregate.yaml`'s `host:
  h{seq%1000}`, not a metric name) — so nothing here actually exercises the metric-name path at
  scale. Whether that path is worth keeping at all, given every real scenario avoids it, is an open
  question rather than a decision made here.
