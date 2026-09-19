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
| A stashed binary, not a build | `script/perf run --scenario passthrough --logit-bin perf/bins/<slug>/logit` |

A multi-ref session on the disposable perf VM (`docs/adr/disposable-azure-perf-vm.md`) measures
several sources this way: `script/vm build <ref\|dir\|tarball>...` stashes one binary per source
under `perf/bins/<slug>/logit`, and `--logit-bin` is what points a run at one of them without
building anything. The results file then names itself after the *binary's* identity rather than
the checkout's — see the ADR's "Multiple sources, one VM" section.

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

**The block below is final**, taken in one session on a dedicated, isolated Azure VM
(`script/vm`) rather than the dev laptop — the re-take the previous revision of this section
promised. The earlier laptop numbers (W2 baseline, W3 delta, W4 delta, and the first `read_batch`
sweep) were always provisional and are not reproduced here; they remain only in PRs #252, #253,
and #254's descriptions and should not be read as current.

<!-- udp-intake-numbers:begin 2026-09-18 Azure Standard_F4as_v6 (script/vm), interleaved one-sitting pairs -->

#### Box facts and provenance

| Fact | Value |
|---|---|
| VM size | `Standard_F4as_v6` (4 vCPU, `vCPUsPerCore: 1` — SMT off, 4 full physical cores) |
| CPU model | AMD EPYC 9V74 80-Core Processor (Genoa, cloud SKU) |
| `nproc` | 4 |
| Kernel | `Linux 6.12.107+deb13-cloud-amd64 x86_64` |
| OS | Debian GNU/Linux 13 (trixie), image `Debian:debian-13:13-gen2`, version `0.20260914.2601` |
| `net.core.rmem_max` | set to `4194304` (4 MiB) before any run, to match the dev-container value the specs were tuned against (VM default from cloud-init was 16 MiB) |
| `net.core.rmem_default` | `212992` (stock, unchanged) |
| Granted receive buffer | not printed directly by the harness (no such JSON field), but `kernel_rcvbuf_utilization_max` reaches ~1.00–1.01 on the small-datagram scenarios exactly as designed for a 1 MiB request doubled to 2 MiB under a 4 MiB ceiling — consistent with the intended, unclamped 2 MiB grant |
| `perf stat -e cycles true` | `<not supported>` — no virtualized PMU, as the ADR documents |
| Swap | none configured |
| THP | `[always] madvise never` |
| `box_state` (governor/EPP/AC) | empty `{}` on every result file — this Azure guest exposes no `cpufreq`/`power_supply` sysfs nodes, so nothing to warn on; by construction (dedicated VM, no other tenants visible, no throttling) this is the isolation the whole exercise is for |
| Pins | `--pin-sender 0,1 --pin-child 2,3` throughout |

Binaries measured, built with a fully isolated `CARGO_TARGET_DIR` per ref:

| Ref | Head SHA | sha256 |
|---|---|---|
| `udp/w2` | `912f5574649dfe7e11b113c5743c0f251f22c355` | `840f1ef41c09eff5265750192533f10cd8a6c2721df62d96186653c86cea3caa` |
| `udp/w3` | `1b3228c10351d290d17c51b95e09a5a3521f1211` | `b7376b1ada380f6705f2102a84ed9146ba78377b73941056f1f9c3646756823f` |
| `udp/w4` | `4a0c252fa530925c43a4d5e7cb36d8c750d1993d` | `40204bf499843ebd68f20b407cfc5420a070e376de8583db0a86c2c717908746` |
| `udp/w5` (harness only, `logit-perf`) | `51d881b37e07cb5a1938da1f7e3f85f2b33d09fa` | not measured directly — same code as w4 + docs |

**Build-isolation note (a real bug caught mid-session):** the three refs were extracted via `git
archive` (run on the host, not the VM) into three separate directories, each with its own `docker
compose` project, sharing the `logit_target_cache` named volume with a **default**
`CARGO_TARGET_DIR=/work/target`. The first build (w2) genuinely compiled; the second (w3), built
minutes later into a source tree extracted at nearly the same wall-clock time, finished in
**0.11s with zero `Compiling` lines** — cargo's mtime-based fingerprint falsely matched w3's
freshly-extracted files against w2's build record, silently reusing w2's binary under the w3 label
(confirmed: byte-identical sha256, byte-identical size). Fixed by giving each ref its own
`CARGO_TARGET_DIR` (`/tmp/target-w2`, `-w3`, `-w4`, passed via `docker compose exec -e`) and
rebuilding all three from scratch; each real build compiled 39 crates in ~2m20s and produced a
distinct sha256, confirmed above. Every binary used for measurement below is from that rebuilt,
isolated-target set — the earlier fast/identical builds were discarded before any scenario was run.

**Method note:** the entire remote-git problem was avoided by running `git archive <sha>` on the
*host* (a plain, single, un-chained git command, which never touches HEAD) to produce a tarball
per ref, `scp`-ing it to the VM, and `tar -x`-ing it into `~/src-w2`, `~/src-w3`, `~/src-w4` — no
`git` command ever ran on the VM itself. `~/logit` (the clone `script/vm up` made) stayed checked
out at `udp/w5` throughout and built `logit-perf`. Binary swaps between runs used `docker cp`
against the `~/logit` project's persistent `dev` container, verified by `sha256sum` before every
run in this report (spot-checked throughout, not just once).

#### Calibration (bisection on w2, ≤6 tries/scenario, target 1–5% kernel drop)

| Scenario | Tries (scale → drop%) | Knee scale | Knee drop% | Half scale |
|---|---|---|---|---|
| `udp-statsd` | 1.0→19.5%, 0.7→0.0%, 0.85→5.6%, 0.8→0.05%, 0.83→**4.04%** | 0.83 | 4.04% | 0.415 |
| `udp-statsd-small` | 1.0→40.2%, 0.7→30.8%, 0.49→**1.07%** | 0.49 | 1.07% | 0.245 |
| `udp-statsd-packed` | 1.0→21.6%, 0.7→0.0%, 0.85→7.2%, 0.78→0.0%, 0.82→**3.24%** | 0.82 | 3.24% | 0.41 |

The shipped specs' rates (tuned for the dev laptop) were far too fast for this CPU — even
`--rate-scale 1.0` on `udp-statsd-small` dropped 40%, so calibration was necessary and non-trivial
(the knee moved much less predictably here than a smooth curve; `udp-statsd`/`udp-statsd-packed`
needed a second bisection pass after overshooting). Calibration was run on `udp/w2` only; a
per-binary capacity number is open work (see below).

#### Knee-scale results

Interleaved `w2, w3, w4, w2, w3, w4`, `--repeat 5`, medians shown, `--pin-sender 0,1 --pin-child
2,3`. `fill` only recorded from w4 onward (w2/w3 have no `logit.input.reads` counter).

| Binary/pass | Scenario | CPU µs/event | events/s | kernel drop % | max rcvbuf | peak RSS |
|---|---|---:|---:|---:|---:|---:|
| w2-A | udp-statsd | 0.902 | 1,294,569 | 2.92 | 1.01 | 78.4 MiB |
| w2-A | udp-statsd-small | 3.952 | 367,181 | 1.40 | 1.00 | 31.6 MiB |
| w2-A | udp-statsd-packed | 0.953 | 1,236,133 | 3.63 | -- | 79.4 MiB |
| w3-A | udp-statsd | 0.875 | 1,327,685 | 0.49 | 0.94 | 72.9 MiB |
| w3-A | udp-statsd-small | 3.816 | 372,303 | 0.02 | -- | 30.3 MiB |
| w3-A | udp-statsd-packed | 0.947 | 1,275,551 | 0.55 | -- | 79.8 MiB |
| w4-A | udp-statsd | 0.803 | 1,334,274 | 0.00 | -- | 58.5 MiB (fill 13.4) |
| w4-A | udp-statsd-small | 3.264 | 372,386 | 0.00 | -- | 33.2 MiB (fill 3.1) |
| w4-A | udp-statsd-packed | 0.906 | 1,282,634 | 0.00 | -- | 49.3 MiB (fill 12.0) |
| w2-B | udp-statsd | 0.897 | 1,294,208 | 3.00 | -- | 75.7 MiB |
| w2-B | udp-statsd-small | 3.958 | 367,751 | 1.25 | -- | 30.7 MiB |
| w2-B | udp-statsd-packed | 0.960 | 1,226,470 | 4.38 | -- | 81.4 MiB |
| w3-B | udp-statsd | 0.870 | 1,331,184 | 0.23 | -- | 71.9 MiB |
| w3-B | udp-statsd-small | 2.830 | 372,364 | 0.01 | -- | 29.9 MiB |
| w3-B | udp-statsd-packed | 0.987 | 1,273,621 | 0.70 | -- | 76.7 MiB |
| w4-B | udp-statsd | 0.848 | 1,334,238 | 0.00 | -- | 55.3 MiB (fill 15.7) |
| w4-B | udp-statsd-small | 3.418 | 372,390 | 0.00 | -- | 34.0 MiB (fill 3.2) |
| w4-B | udp-statsd-packed | 0.849 | 1,282,660 | 0.00 | -- | 44.2 MiB (fill 11.9) |

#### Half-scale results (0.5 × knee)

| Binary/pass | Scenario | CPU µs/event | events/s | kernel drop % | peak RSS |
|---|---|---:|---:|---:|---:|
| w2-A | udp-statsd | 0.869 | 667,149 | 0.00 | 43.4 MiB |
| w2-A | udp-statsd-small | 5.546 | 186,197 | 0.00 | 30.6 MiB |
| w2-A | udp-statsd-packed | 0.937 | 641,343 | 0.00 | 38.5 MiB |
| w3-A | udp-statsd | 0.877 | 667,148 | 0.00 | 44.4 MiB |
| w3-A | udp-statsd-small | 4.497 | 186,197 | 0.00 | 31.0 MiB |
| w3-A | udp-statsd-packed | 0.990 | 641,336 | 0.00 | 38.4 MiB |
| w4-A | udp-statsd | 0.868 | 667,138 | 0.00 | 43.1 MiB (fill 13.4) |
| w4-A | udp-statsd-small | 5.410 | 186,197 | 0.00 | 33.3 MiB (fill 1.9) |
| w4-A | udp-statsd-packed | 0.858 | 641,344 | 0.00 | 41.5 MiB (fill 9.3) |
| w2-B | udp-statsd | 0.900 | 667,146 | 0.00 | 42.7 MiB |
| w2-B | udp-statsd-small | 4.062 | 186,198 | 0.00 | 31.2 MiB |
| w2-B | udp-statsd-packed | 0.947 | 641,343 | 0.00 | 38.5 MiB |
| w3-B | udp-statsd | 0.887 | 667,147 | 0.00 | 43.2 MiB |
| w3-B | udp-statsd-small | 5.484 | 186,197 | 0.00 | 30.1 MiB |
| w3-B | udp-statsd-packed | 0.985 | 641,338 | 0.00 | 37.5 MiB |
| w4-B | udp-statsd | 0.863 | 667,139 | 0.00 | 43.8 MiB (fill 13.4) |
| w4-B | udp-statsd-small | 2.409 | 186,198 | 0.00 | 33.6 MiB (fill 2.4) |
| w4-B | udp-statsd-packed | 0.917 | 641,337 | 0.00 | 39.7 MiB (fill 12.4) |

(All half-scale queue drops are 0; every field not shown is 0.)

#### Compare: deltas vs. control-to-control drift

`+` on events/s is better, `-` on µs/event is better, `-` on RSS is smaller (usually better), drop
points are `after - before`.

**Knee scale**

| Scenario | Pair | events/s | µs/event | peak RSS | drop pts |
|---|---|---:|---:|---:|---:|
| udp-statsd | w2A→w3A | +2.6% | -3.1% | -7.0% | -2.43 |
| udp-statsd | w2B→w3B | +2.9% | -3.0% | -5.1% | -2.76 |
| udp-statsd | w3A→w4A | +0.5% | -8.2% | -19.8% | -0.49 |
| udp-statsd | w3B→w4B | +0.2% | -2.5% | -23.1% | -0.23 |
| udp-statsd | control w2A→w2B | -0.0% | -0.6% | -3.5% | +0.08 |
| udp-statsd | control w3A→w3B | +0.3% | -0.5% | -1.4% | -0.26 |
| udp-statsd | control w4A→w4B | -0.0% | **+5.6%** | -5.5% | +0.00 |
| udp-statsd-small | w2A→w3A | +1.4% | -3.4% | -4.0% | -1.38 |
| udp-statsd-small | w2B→w3B | +1.3% | **-28.5%** | -2.6% | -1.24 |
| udp-statsd-small | w3A→w4A | +0.0% | -14.5% | +9.4% | -0.02 |
| udp-statsd-small | w3B→w4B | +0.0% | **+20.8%** | +13.7% | -0.01 |
| udp-statsd-small | control w2A→w2B | +0.2% | +0.2% | -2.9% | -0.15 |
| udp-statsd-small | control w3A→w3B | +0.0% | **-25.8%** | -1.5% | -0.01 |
| udp-statsd-small | control w4A→w4B | +0.0% | +4.7% | +2.4% | +0.00 |
| udp-statsd-packed | w2A→w3A | +3.2% | -0.6% | +0.6% | -3.08 |
| udp-statsd-packed | w2B→w3B | +3.8% | +2.8% | -5.7% | -3.68 |
| udp-statsd-packed | w3A→w4A | +0.6% | -4.4% | **-38.2%** | -0.55 |
| udp-statsd-packed | w3B→w4B | +0.7% | -14.0% | **-42.3%** | -0.70 |
| udp-statsd-packed | control w2A→w2B | -0.8% | +0.7% | +2.5% | +0.75 |
| udp-statsd-packed | control w3A→w3B | -0.2% | +4.1% | -3.9% | +0.15 |
| udp-statsd-packed | control w4A→w4B | +0.0% | -6.3% | -10.3% | +0.00 |

**Half scale**

| Scenario | Pair | events/s | µs/event | peak RSS | drop pts |
|---|---|---:|---:|---:|---:|
| udp-statsd | w2A→w3A / control | ~0% | +0.9% / +3.6% (ctl) | +2.2% / -1.7% (ctl) | 0 |
| udp-statsd | w3A→w4A / control | ~0% | -1.0% / -0.5% (ctl) | -2.8% / +1.5% (ctl) | 0 |
| udp-statsd-small | w2A→w3A / control | ~0% | -18.9% / -26.8% (ctl) | +1.3% / +2.0% (ctl) | 0 |
| udp-statsd-small | w3A→w4A / control | ~0% | +20.3% / -55.5% (ctl) | +7.5% / +0.7% (ctl) | 0 |
| udp-statsd-packed | w2A→w3A / control | ~0% | +5.7% / +1.1% (ctl) | -0.4% / -0.0% (ctl) | 0 |
| udp-statsd-packed | w3A→w4A / control | ~0% | -13.3% / +6.8% (ctl) | +8.3% / -4.5% (ctl) | 0 |

(Both A/B branch deltas and both controls are given for `udp-statsd`/`-packed` above; full
per-pair numbers for `-small` are in the raw JSON under `~/lib/logit/tmp/perf/udp/vm/` — the
control drift itself swings ±20–55% here, so the branch deltas for `-small` cannot be read at
all; see the reading below.)

#### `read_batch` sweep (w4 only, `--repeat 3`, knee scale)

Temporary scenario/load copies (`udp-statsd-rbN` / `udp-statsd-small-rbN`) were placed under
`perf/scenarios/`+`perf/load/` on the VM only, deleted after (never committed).

| `read_batch` | **udp-statsd-small** µs/event | fill | drop% | rcvbuf | | **udp-statsd** µs/event | fill | drop% | rcvbuf |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 4.151 | 1.0 | 10.67% | 1.00 | | 0.928 | 1.0 | 4.66% | 1.00 |
| 16 | 3.881 | 3.4 | 0.00% | 0.01 | | 0.845 | 9.8 | 0.00% | 0.29 |
| 32 | 3.141 | 3.0 | 0.00% | 0.01 | | 0.849 | 13.5 | 0.00% | 0.16 |
| **64** | **4.210** | **3.0** | **0.00%** | **0.01** | | **0.846** | **15.3** | **0.00%** | **0.16** |
| 128 | 4.182 | 3.1 | 0.00% | 0.01 | | 0.850 | 16.5 | 0.00% | 0.18 |
| 256 | 4.144 | 3.2 | 0.00% | 0.01 | | 0.852 | 15.3 | 0.00% | 0.23 |

**Peak RSS across the sweep** (`udp-statsd-small`): 31.8 (rb1) → 30.9 → 31.6 → 32.7 → 38.2 → 49.1
MiB (rb256) — unlike the laptop's flat-RSS finding, RSS on this VM rises noticeably at
rb128/rb256. `udp-statsd`'s RSS also rises with `read_batch` (73.9 → 55.1 → 49.5 → 53.0 → 53.9 →
78.8 MiB) — non-monotonic and noisy, not a clean trend either.

`fill` plateaus at ~3 for `-small` and ~13–16 for `udp-statsd` from `read_batch=16` upward, same
qualitative shape as the laptop's finding: 64 already captures essentially all the batching
benefit, values above it buy nothing in CPU time (µs/event is flat 0.845–0.852 for `udp-statsd`,
16–256; `-small`'s µs/event is noisy across the whole sweep — see the reading below).

#### `--verify` (default scale, `udp-statsd`, one repeat each)

| Binary | Result |
|---|---|
| w2 | 620,000 datagrams, 11,074,692 lines, **11,074,692 events delivered exactly, zero drops** |
| w3 | 620,000 datagrams, 11,074,692 lines, **11,074,692 events delivered exactly, zero drops** |
| w4 | 620,000 datagrams, 11,074,692 lines, **11,074,692 events delivered exactly, zero drops** |

All three pass the strict self-check.

<!-- udp-intake-numbers:end -->

### Reading the numbers

**The robust signal is loss at a fixed offered load, not CPU per event.** At the calibrated knee,
kernel drops fall monotonically w2 → w3 → w4 on every scenario, and both interleaved passes agree:
`udp-statsd` 2.92/3.00% → 0.49/0.23% → 0.00%; `udp-statsd-small` 1.40/1.25% → 0.02/0.01% → 0.00%;
`udp-statsd-packed` 3.63/4.38% → 0.55/0.70% → 0.00%. Control-to-control drift in drop points is
≤~0.75 throughout (see the compare table above), well inside the w2→w3 and w3→w4 movements, and
max kernel-rcvbuf utilization falls in step. Decode-side batching (W3) removes most of the loss;
`recvmmsg` (W4) removes the rest, taking every scenario to exactly zero drops at the knee. For
syscall-bound UDP intake, this — not µs/event — is the number to trust.

**CPU µs/event tells a smaller, partly-inside-drift story.** On `udp-statsd`, w2→w3 is a small,
consistent gain (−3.1%/−3.0%, against a control drift of ≤0.6%) — a real if modest win. w3→w4 on
`udp-statsd` (−8.2%/−2.5%, with one control at +5.6%) and on `udp-statsd-packed` (−4.4%/−14.0%,
against controls of +4.1%/−6.3%) point the same direction but are **not** cleanly beyond drift —
read as "consistent with a small gain," not a confirmed one. At half scale, every mixed/packed
movement is within drift.

**`udp-statsd-small`'s CPU µs/event is not readable on this VM.** Control-to-control swings run
20–56% — the same order of magnitude as any branch delta — so nothing about `-small`'s timing
number should be read as a w2→w3 or w3→w4 finding (its drop-rate numbers remain trustworthy, since
those are a count, not a timing). This VM is **not** faster per event than the dev laptop: `-small`
costs roughly 3–5 µs/event here, versus ~1.1–2.0 µs/event on the laptop's 5.1 GHz cores — which is
also why the shipped load-spec rates had to be roughly halved to reach the calibration target here.
A telling shape backs a **hypothesis, not a finding**: per-event CPU is *higher* at half the
offered rate (4.1–5.5 µs/event) than at the knee (2.8–4.0 µs/event) — the opposite of what
amortizing a fixed cost over more work should give. Mean fill is lower at half scale too (1.9–2.4
vs. 3.1 datagrams/read), so each wakeup carries less work — consistent with a fixed per-wakeup
cost that a virtualized guest pays disproportionately for on idle/wake transitions. Consequence:
for syscall-bound traffic on a VM, compare loss and capacity, not µs/event.

**Peak RSS: the laptop's provisional finding does not reproduce.** At the knee, RSS *falls*
w3→w4 — `udp-statsd` −19.8%/−23.1%, `udp-statsd-packed` −38.2%/−42.3%, both far beyond their
control drift — because w4 no longer builds a receive-side backlog the way w3 did. At half scale
(no backlog for anyone), RSS is flat within drift on both scenarios. `udp-statsd-small` rises
slightly (+9.4%/+13.7%, on the order of +3 MiB) — a real but small move in the laptop's direction.
This closes the open RSS-attribution item below.

**The `read_batch` sweep confirms the plateau, and 64 stays the default** — `read_batch: 1`
reproduces the pre-`recvmmsg` loss (10.67% `-small`, 4.66% mixed); from 16 upward, drops are 0 and
`udp-statsd`'s µs/event is flat (0.845–0.852); mean fill plateaus around ~3 (`-small`) and ~13–16
(mixed). **New, and different from the laptop: peak RSS rises noticeably at large `read_batch`**
(`-small`: 32.7 MiB at 64 → 38.2 at 128 → 49.1 at 256). This VM has transparent huge pages set to
`always` (see box facts above), and the arithmetic fits a THP-backed slab: the slab is
`read_batch × 65,507` bytes — 4 / 8 / 16 MiB at 64 / 128 / 256 — and touching just one 4 KiB page
per slot, under `THP=always`, faults in whole 2 MiB huge pages, i.e. the *entire* slab becomes
resident (+16.4 MiB observed at `read_batch: 256` against a 16 MiB slab). This is the **likely**
explanation, not a confirmed one — it wants a same-box run with THP set to `madvise`/`never` to
confirm. Its practical meaning either way: "the slab is not resident" (the dev-container claim
above) holds where THP is `madvise` or `never`, but does **not** hold under `THP=always`, where the
shipped default `read_batch: 64` can cost up to ~4 MiB of real resident memory per UDP listener.

### Open after this workstream

- **A per-binary capacity metric is still open.** This session calibrated the knee on `udp/w2`
  only and ran w3/w4 at that same offered load — the honest headline for syscall-bound traffic
  would instead bisect the highest loss-free offered rate *per binary*, which likely differs
  between w2, w3, and w4. Not measured here.
- **The THP explanation for the `read_batch`-sweep RSS rise is likely, not confirmed** — it wants a
  repeat of the same sweep on the same VM shape with `transparent_hugepage` set to `madvise` or
  `never`, to check that the RSS-vs-`read_batch` curve goes flat again.
- **The wakeup-cost hypothesis for `udp-statsd-small` on VMs is a hypothesis, not a finding** — the
  higher per-event CPU at half scale than at the knee is consistent with a fixed per-wakeup cost
  that a virtualized guest pays for disproportionately at low arrival rates, but nothing here
  isolates wakeup cost directly (e.g. via a wakeup-rate probe independent of `fill`).
- **The shared-task / `SO_REUSEPORT` follow-up from earlier workstreams is still open** and
  untouched by this session — this session measured the existing single-listener-task design at
  higher fidelity, not a multi-task alternative.

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
