# Performance: the load-test harness's recorded numbers

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
| Every scenario, 5 repeats | `script/perf run --repeat 5 --profile release --label recorded` |
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

> **Every number in this document, except where a sub-section states otherwise, was taken on the
> disposable perf VM** (`docs/adr/disposable-azure-perf-vm.md`), not the dev laptop —
> `Standard_F8as_v6`: 8 dedicated AMD EPYC 9V74 (Genoa) cores, SMT off, 32 GiB, Debian GNU/Linux 13
> (trixie), kernel `6.12.107+deb13-cloud-amd64`, image version `0.20260914.2601`, `westus2`, inside
> the dev container (`rustc 1.98.1 (48a229cea 2026-09-01)`), `release` profile, at commit
> `f4967624005035b4818373904d71811e16176dc2` on 2026-09-20 — **dirty**: the retuned
> `perf/scenarios/*.yaml` counts this same PR carries were measured uncommitted, since committing
> mid-measurement-session on the VM's own checkout would have been worse provenance, not better;
> `perf/results/*.json`'s own `git.dirty: true` on every file from this session is that diff, not a
> mistake. Nothing else was running on the VM at the time — no other tenant, no other `script/*`
> work, by construction (`docs/adr/disposable-azure-perf-vm.md`'s whole reason to exist). This
> replaces the Fedora/Ryzen-laptop numbers this document carried through 2026-09-14 — see
> `docs/design/memory.md`'s "Heterogeneous cores" note and the ADR's own "Context" section for why
> that box was retired as a reference: heterogeneous Zen 5/5c cores made an unpinned run bimodal by
> roughly 2×, and the same commit measured 90 minutes apart on battery drifted CPU µs/event by
> ~23%. `perf/results/*.json`'s own `hostname` field records the dev container's own hostname
> inside the VM, not anything host-identifying; `cpu_model` and `nproc` come from `/proc/cpuinfo`/
> `nproc` as seen *inside* that container, which is why they're restated here in prose rather than
> only trusted from the JSON. `compare` warns on a host/CPU-model mismatch for exactly this reason,
> and CPU µs/event, not wall-clock events/s, is what it actually gates a regression on — a real
> consideration even on a dedicated VM, since last-level cache and memory bandwidth are still
> shared with other tenants on the physical host (the ADR's "Consequences" section).
>
> **Machine and code both changed since the last recorded run.** This isn't a clean before/after of
> the VM against the laptop: real optimizations landed in the six days between them (in-place
> `Transform::process`, the interner's `ahash`/key-cache work, `metrics-model-v2`'s TLV framing, the
> Lua `Event.new`/`to_table` surface, among others), so a row that moved could be the box, the code,
> or both. Where a specific number's story is "the code got faster" rather than "the box is
> different," this document says so; where it doesn't, read the row as simply *current*, not as an
> isolated hardware delta.

For continuity, the runs this table carried before this one: 2026-09-14 on a quiet, battery-powered
laptop (`fecbd9337010f95d722e89946e1a3e3aa43c007b`,
`perf/results/20260914T012218Z-fecbd9337010-quiet.json`) and, before that, 2026-09-13 on a busy,
contended laptop (`c75399d8bccc`, `perf/results/20260913T104956Z-c75399d8bccc-recorded.json`),
14–27% slower across the board in events/s than the quiet run, scenario for scenario. Both are
retired now — the VM's own run-to-run drift and controls (see the sub-sections below) are the
comparison that matters going forward, not a cross-machine, cross-day delta against either laptop
run. `buffered`'s own before/after story is its own section, §3.

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
- **Isolation from the rest of the box, mostly.** `script/vm` (`docs/adr/disposable-azure-perf-vm.md`)
  provisions a disposable Azure VM with eight homogeneous cores and nothing else of ours running,
  which is what every number in this document (bar an explicitly-captioned exception) is taken on
  now — captioned with its own `~/logit-vm-metadata.txt` (CPU model, kernel, image version,
  sysctls) the way a machine caption works throughout this file. What isolation still doesn't buy:
  last-level cache and memory bandwidth are shared with other tenants on the physical host, host
  maintenance can briefly freeze the guest (inflating wall-clock-derived numbers, not CPU-time ones),
  and there's no virtualized PMU, so `flamegraph` here is a `cpu-clock`, not `cycles`, profile
  (§5 below). The ADR's "Consequences" section has the full account.

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
  to the child between `fork` and `exec` so every thread it creates inherits the mask), separating
  the load sender from the measured `logit` process onto distinct physical cores. On the VM's eight
  *identical* EPYC cores this is no longer about escaping a bimodal laptop split (the reason this
  bullet used to give) — it's still needed so the sender and child don't contend for the same core's
  time, which would inflate both sides' numbers together. Every recorded `udp-statsd*` number below
  states which CPUs it used (`--pin-sender 0,1 --pin-child 2,3` throughout this session).
- **A delta is a pair taken in one sitting, interleaved, on a box in a known state.** This
  discipline predates the VM — on the old laptop, the same specs, commit and pins measured 90
  minutes apart once moved CPU µs/event ~23% and a drop rate from 3.1% to 12.4%, confirmed as the
  box, not the code, by re-running the earlier commit straight afterwards and reproducing the later
  numbers — and it's kept here for a smaller but real reason the ADR names explicitly: last-level
  cache and memory bandwidth are still shared with other tenants even on a dedicated VM, and a new
  `script/vm up` may land on different physical hardware entirely. So parent/branch runs still
  alternate within one session and are never diffed against a stored file from another day.
  [`perf/load/README.md`](../../perf/load/README.md)'s "Box state" has the checklist; `run` records
  what it can of it into the results file (`box_state`) — which comes back an empty `{}` on this
  VM, since the guest exposes no `cpufreq`/`power_supply` sysfs to read at all, itself a consequence
  of the isolation this box is for. Tables in this document are labelled by session, not presented
  as one series across days.
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

## 1. Results: all twelve scenarios, median of 5

`script/perf run --repeat 5 --profile release --label vm-recorded`, solo, on the VM, nothing else
running (see the preamble above). Sorted as `script/perf list` orders them (alphabetical); `count`
is each scenario's configured `generate_in.count` at the time of this run — the **retuned counts
this same PR ships** (§1's old laptop-tuned counts, mostly unchanged: the VM's own sizing pass
found most scenarios already sat in the "5–10 s of wall per repeat" band `docs/plans/load-test-
harness.md` targets at their existing counts; only `encode-native-devnull`, `json-parse`,
`json-parse-x3`, `logfmt-parse`, and the `passthrough`/`fanout`/`route` trio — which share one count
by design, see below — needed raising). The **events/s (min–max)** column is the same five repeats'
spread that produced the median — see the noise sub-section right after this table for what it
means when that range is wide.

| Scenario | Count | events/s | events/s (min–max) | CPU µs/event | Peak RSS | Wall |
|---|---:|---:|---:|---:|---:|---:|
| `aggregate` | 20M | 3,092,232 | 3,087,789 – 3,100,900 | 0.325 | 66.7 MiB | 6.47 s |
| `buffered` | 1.2M | 707,215 | 698,965 – 747,611 | 1.781 | 86.8 MiB | 1.70 s |
| `encode-human-devnull` | 8M | 1,226,433 | 1,219,331 – 1,255,826 | 1.098 | 258.3 MiB | 6.52 s |
| `encode-native-devnull` | 10M | 1,412,609 | 1,382,105 – 1,503,005 | 0.995 | 293.8 MiB | 7.08 s |
| `fanout` | 25M | 2,594,765 | 2,279,434 – 2,654,909 | 0.623 | 79.6 MiB | 9.63 s |
| `json-parse` | 19M | 2,072,139 | 1,868,855 – 2,106,589 | 0.904 | 300.0 MiB | 9.17 s |
| `json-parse-x3` | 9.5M | 1,439,337 | 1,402,287 – 1,490,720 | 2.248 | 114.1 MiB | 6.60 s |
| `logfmt-parse` | 9.5M | 1,601,717 | 1,588,927 – 1,611,496 | 1.012 | 84.2 MiB | 5.93 s |
| `lua` | 4M | 600,799 | 578,333 – 608,753 | 2.001 | 71.5 MiB | 6.66 s |
| `native-relay` | 7M | 884,707 | 865,785 – 897,002 | 1.401 | 214.4 MiB | 7.91 s |
| `passthrough` | 25M | 3,582,184 | 3,474,559 – 3,650,615 | 0.346 | 76.5 MiB | 6.98 s |
| `route` | 25M | 3,151,261 | 2,893,512 – 3,220,361 | 0.654 | 154.5 MiB | 7.93 s |

Three scenarios ship **without a row here**: `json-parse-app-log`, `json-parse-nested-log` and
`json-parse-access-log`, added 2026-09-21 by [`docs/plans/event-sizing.md`](../plans/event-sizing.md)'s
W1 as three widths of the same `json` parse (12, 10-with-four-nested-maps, and 30 attributes). Their
configured counts were first estimates scaled off `json-parse`'s by key count rather than tuned to
the 5–10 s band. **They have since been run** — the event-sizing bake-off (§8, same day) ran all
three on `main`, and none landed in the target band: `json-parse-app-log` 10.9 s, `json-parse-nested-
log` 13.9 s, `json-parse-access-log` 12.4 s (§8's "The three `json-parse-*` scenarios' first real
run"). That session's own protocol (median of 6, several non-`main` binaries) doesn't match this
table's (median of 5, `main` alone at one commit), so there is still deliberately no row for them
here — the first `--repeat 5`, `main`-only, retuned-count run of these three is still owed to this
table specifically, now with a real starting point for what to lower their counts to.

`json-parse-x3` and `logfmt-parse` have no row in the laptop-era version of this table at all —
they landed after it was last written (`docs/plans/load-test-harness.md`'s "Owed now" tracked this
as outstanding); this is their first recorded numbers here.

A few readings, cross-referencing `perf/scenarios/*.yaml`'s own comments for what each measures.
The deep per-instruction attribution/flamegraph narrative the laptop-era version of this section
carried for `passthrough`/`fanout`/`route` (specific sample percentages inside `generate_in`,
`SinkQueue` admission, `route_batch`) came from a 2026-09-14 busy-laptop `attribute`/`flamegraph`
pass this session didn't re-run — that reasoning is almost certainly still directionally correct
(nothing in the pipeline code between the delivery path and the router has changed since target/
route landed), but presenting its exact old percentages as current would overclaim what this
session verified. The **top-line numbers below are all fresh, this session**; a from-scratch
attribution re-pass for `fanout`/`route` is a cheap follow-up, not done here (`attribute` was run
this session for `json-parse`, `aggregate`, and `buffered` — §2 has those, current):

- **`passthrough`** (0.346 µs/event) is the runtime floor every other scenario is read relative to:
  scheduling, the `Fanout` channel hop, layer-2 telemetry, no parsing or encoding. The prior
  laptop finding — that most of this floor is the generator's own render cost, not the delivery
  path — is architectural (unchanged code) and there's no reason to expect it's stopped being true;
  it just isn't re-verified against a fresh attribution pass this session.
- **`fanout`** (0.623 µs/event) and **`route`** (0.654 µs/event) both generate `passthrough`'s exact
  event at `passthrough`'s exact count (25M, shared by design so the three differ only in topology)
  and cost more per generated event than `passthrough` alone — `fanout` for two extra sinks'
  `Arc`-clone-plus-delivery-hop cost, `route` for the router's own real per-event work
  (`route_batch`'s `AttrMap::get_sym` plus a linear scan of alternatives) on top of a similar
  sink-hop cost. Both numbers are consistent with the laptop-era relative finding (fan-out cheaper
  per delivery than routing once, `fanout` < `route` here as there) without re-deriving its exact
  per-edge cost breakdown this session.
- **`json-parse`** (0.904 µs/event) and **`lua`** (2.001 µs/event) are no longer close to tied —
  `json-parse` dropped sharply from the laptop-era 2.054 µs/event figure (the interner key-cache and
  in-place `Transform::process` work landed since, `docs/adr/in-place-transform-process.md`), while
  `lua`'s LuaJIT round trip has no equivalent optimization and remains the more expensive single-hop
  transform by a wide margin — the general "Lua costs more than a native transform" relationship
  `docs/known-gaps.md` documents still holds, just by a larger factor now that the native side got
  cheaper. `json-parse-x3` (2.248 µs/event, three parallel parsers sharing the interner) stayed
  close to `lua`'s cost, consistent with its own design intent of showing shared-interner
  contention rather than measuring a single parse.
- **`encode-human-devnull`** vs **`encode-native-devnull`** (1.098 vs 0.995 µs/event): the native
  encoder is still measurably cheaper than the human-readable render at the same event stream, as
  `docs/design/wire-protocol.md`'s design intent predicts — dictionary-first framing beats
  formatting text, the same relative shape the laptop showed (1.330 vs 1.180 µs/event there).
- **`native-relay`** (1.401 µs/event) is the full encode → loopback TCP → decode → ack round trip in
  one process, and lands below `json-parse-x3`/`lua` but above the single-parse `json-parse` and
  both `encode-*-devnull` scenarios on this run — a real network hop and an ack wait, still cheaper
  than a parse-heavy or Lua-heavy graph, though the exact ranking against `encode-*-devnull` shifted
  now that `json-parse`'s own cost fell (see above) rather than `native-relay` itself moving much.
- **`aggregate`** (0.325 µs/event) is nearly as cheap as `passthrough` per event despite sketching a
  1000-series distribution and running a 1 s flush tick — see §2 for why: almost all of it is one
  node's `DdSketch::add`, and the flush tick's own cost is amortized over several ticks a run (this
  run's own median repeat took 6.47 s wall, ~6 ticks at this scenario's 1 s interval). `aggregate`
  is also this table's noisiest scenario by far in relative terms — see the sub-section right below,
  though its *absolute* spread on the VM (3,087,789–3,100,900, under half a percent) is far tighter
  than the laptop ever showed.
- **`buffered`** — see §3, now resolved: the wide run-to-run swings first seen in W7a were the
  harness's own un-cleared spool, fixed by W8 (#165) and confirmed on a quiet machine there. The
  `1,087,248` events/s above is a real quiet-machine median of three back-to-back repeats, but §3's
  own dedicated `--repeat 5` pass (632,897–873,406 events/s) is the more representative number for
  this scenario's steady-state spread — three repeats is thin for a scenario whose repeats vary by
  design.

### Noise: `aggregate`'s laptop-era spread does not reproduce on the VM

On the laptop, `aggregate` was this table's noisiest scenario by a wide margin — three solo repeats
once gave 3,894,156 / 2,588,602 / 3,396,968 events/s, a spread of roughly ±25% around the median.
**That finding does not reproduce here.** Two independent 5-repeat samples exist from this
session's own measurement — the `aggregate` row in §1's table above (repeats 3,091,253 / 3,092,232
/ 3,087,789 / 3,100,900 / 3,093,129 events/s) and a second, unrelated 5-repeat run of the same
scenario roughly an hour later (3,086,137 / 3,079,798 / 3,100,395 / 3,091,713 / 3,083,023) — and
they agree with each other to within 0.7%, both within a run and across runs. `compare`'s
`--threshold 5` would not have flagged anything here; the flush-tick-alignment sensitivity §2
attributes the laptop's spread to is either much smaller on this box or swamped by something else
that made the laptop worse (heterogeneous-core scheduling jitter is the leading suspect, since
`aggregate`'s single hot node moving between a fast and slow core mid-run would show up exactly
this way). This is one of the concrete pieces of evidence for retiring the laptop as a reference
box, not just a noisier version of the same measurement.

`compare`'s lack of a variance-aware threshold (gate on each file's `min`, or a per-scenario
threshold) is still real future work in principle, but the motivating case — `aggregate` tripping
`--threshold 5` on nothing but its own noise — is no longer observed on the reference box.
`docs/known-gaps.md`'s harness entry is updated to reflect this.

### Peak RSS: what is live data and what is jemalloc retention

`logit` runs on jemalloc (ADR `jemalloc-global-allocator`), which returns freed pages to the kernel
on a decay schedule (`dirty_decay_ms` = 10 s by default) rather than at `free`. A scenario that
runs for 5–10 s therefore reports a peak RSS that includes most of what it freed along the way,
not just what it held at its high-water mark. To separate the two, the intended design is to run
the whole suite twice, once as-is and once with
`_RJEM_MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0` (purges at `free`, making peak RSS a close
proxy for peak live data) — the table below, from the original laptop investigation, is what that
comparison showed:

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

Two things fell out of it:

- **Where the sink is slower than the generator, RSS really is queue depth**, and it is the
  sink queue's default `buffer.max_bytes` of 64 MiB that sets it: a 100-event batch of this
  shape weighs ~87 KiB by `estimated_heap_bytes`, so the byte bound trips at ~770 batches, well
  before the 1024-batch bound; add the 64-slot inbox and the process baseline and you get the
  ~85 MiB the three sink-bound scenarios (`encode-*`, `native-relay`) all converge on under
  immediate purge. So the "in-flight buffering at default `buffer:` is large" observation stands
  for those, and the bound doing it is `max_bytes`, not `max_batches` or the channels.
- **Where the sink keeps up, RSS is mostly retention.** `passthrough`, `route`, `aggregate`,
  `json-parse` and `lua` all drop by half or more under immediate purge, down to a number that
  matches their in-flight channel data plus process baseline.

**This table is retained from the original laptop investigation and was not re-verified on the
VM this session** — attempted, but the attempt itself failed in a way worth recording rather than
quietly discarding. `script/perf run`'s own `run()` helper (`script/common.sh`) invokes
`docker compose run` with only one explicit `-e LOGIT_DEV_CONTAINER=1`; `sudo docker compose run`
strips the calling shell's environment before compose ever interpolates a `${VAR}`, which is
exactly why `LOGIT_PERF_GIT_SHA`/`LOGIT_PERF_GIT_DIRTY` are threaded through as `env VAR=... cargo
run` argv rather than an exported variable (`crates/logit-perf/src/run.rs`'s own `git_info` doc
comment explains that mechanism). `_RJEM_MALLOC_CONF` has no equivalent argv path, so `export
_RJEM_MALLOC_CONF=...; script/perf run ...` silently measured the *default*-decay condition twice
rather than default-decay-then-purge once — confirmed after the fact: the two runs' CPU µs/event
for `aggregate` (0.325 both times) and for every other scenario matched each other to within
ordinary noise, where a real immediate-purge run should show the ~2× CPU cost the table's own next
paragraph describes. **Needs**, as a follow-up: either an explicit env-passthrough flag on
`script/perf run` (e.g. `--container-env KEY=VALUE`, threaded the same way `LOGIT_PERF_GIT_SHA`
already is), or a one-off `docker compose run -e _RJEM_MALLOC_CONF=... dev ...` invocation run by
hand outside the harness. Until then, the table above is history, not a current VM measurement —
read it for the *shape* of the finding (sink-bound scenarios' RSS is queue depth, generator-bound
scenarios' RSS is mostly retention), not as this session's own numbers.

The immediate-purge run's CPU numbers are *not* comparable to anything else in this document —
purging at `free` costs an `madvise` per page-sized free and roughly doubled CPU µs/event for the
churn-heavy scenarios (`route` 0.645 → 1.239, `aggregate` 0.273 → 0.599, both laptop-era numbers).
It is a diagnostic setting for reading RSS, not a configuration to run with.

## 2. Attribution: where a scenario's time actually goes

`script/perf attribute --scenario NAME` appends a temporary `internal → file_out format: native` leg
to a copy of the scenario, decodes the resulting dump, and groups every point by the emitting
component — see [`internal-telemetry.md`](internal-telemetry.md)'s "Reading an attribution dump"
section for the mechanism. Both tables below are fresh, from this session's own VM run, at each
scenario's current (retuned) count. `__perf_internal`/`__perf_dump` are the harness's own two
nodes, shown for transparency but excluded from the verdict.

### `json-parse`

```
node               kind           role         events in  events out  batch in batch out  process s  blocked s     send s  buf max
parsed             json           transform     19000000    19000000    190000    190000     7.6780     0.0370     0.0000        -
metrics            kv_metrics     transform     19000000    19000000    190000    190000     2.0565     0.0236     0.0000        -
__perf_dump        file_out       sink               321           0         9         0     0.0000     0.0000     0.0021     0.00
__perf_internal    internal       listener             0         321         0         9     0.0000     0.0000     0.0000        -
gen                generate_in    listener             0    19000000         0    190000     0.0000     5.2125     0.0000        -
out                null_out       sink          19000000           0    190000         0     0.0000     0.0000     0.0159     0.00
```

**The split between the two transforms inverted from the laptop-era table.** `json` (`parsed`) now
has the larger share of Σ process time: 7.6780s of 9.7345s measured (**79%**), against `kv_metrics`
(`metrics`)'s 2.0565s (**21%**) — the laptop-era table had this the other way round, 46%/54%. Both
transforms got faster since (the divan `kv_metrics` bench alone dropped from 256 ns to 75 ns,
`docs/design/memory.md` §3), but `kv_metrics` dropped by roughly 5×, deriving four metrics from a
parsed attribute map has gotten disproportionately cheaper than the JSON parse that feeds it — the
interner key-cache and in-place `Transform::process` work landed since (`docs/adr/in-place-
transform-process.md`) targeted exactly this. `gen`'s 5.2125s "blocked in send" — more than either
transform's own process time — is not generator slowness: it's `generate_in` running faster than
`json`/`kv_metrics` can drain it and spending most of the run backpressured on `Fanout::send`,
exactly the reading `internal-telemetry.md`'s verdict rule gives ("the constraint is downstream of
`gen`"). That's the expected, healthy shape for this harness: the generator should never be a
scenario's bottleneck.

### `aggregate`

```
node               kind           role         events in  events out  batch in batch out  process s  blocked s     send s  buf max
windowed           aggregate      transform     20000000        7000    200000         7     4.4193     0.0000     0.0000        -
__perf_dump        file_out       sink               222           0         7         0     0.0000     0.0000     0.0011     0.00
__perf_internal    internal       listener             0         222         0         7     0.0000     0.0000     0.0000        -
gen                generate_in    listener             0    20000000         0    200000     0.0000     4.6042     0.0000        -
out                null_out       sink              7000           0         7         0     0.0000     0.0000     0.0000     0.00
DROPPED 20000000 events at `windowed` (reason=absorbed)
```

Only one real node, so `windowed` (`aggregate`) is trivially **100%** of measured Σ process time
(4.4193s — down from 7.7434s on the laptop, the VM simply being faster per event here, not a
different code path). The `DROPPED 20000000 events (reason=absorbed)` line is expected, not a bug:
every input event is folded into a per-series `DdSketch` and never itself re-emitted —
`Transform::process` returning `None` on every call is exactly what a stateful aggregator does
between flushes. Output is 7 flush-tick batches (`batch out`) carrying 7,000 events total — 1000
events per tick, one per live series, matching `host: h{seq%1000}`'s cardinality (fewer ticks than
the laptop-era table's 12 simply because this run finished faster — the same 1 s interval, a
shorter wall time). `gen` again shows most of its own time (4.6042s) blocked in `send`, the same
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
section. With the spool cleared before every repeat (W8, #165), that signature is gone on a busy
laptop, a quiet laptop, and now the reference VM.

**VM confirmation (this session, current numbers).** `script/perf run --repeat 5 --profile release
--label vm-buffered-solo --scenario buffered`, solo, on the VM:

```
repeat 1/5: 745,047 events/s   1.720 us/event   78.7 MiB peak RSS   2.4 ms startup
repeat 2/5: 757,703 events/s   1.710 us/event   79.0 MiB peak RSS   3.4 ms startup
repeat 3/5: 802,478 events/s   1.710 us/event   74.8 MiB peak RSS   2.8 ms startup
repeat 4/5: 780,068 events/s   1.709 us/event   70.9 MiB peak RSS   3.3 ms startup
repeat 5/5: 801,518 events/s   1.708 us/event   70.7 MiB peak RSS   2.8 ms startup
```

No monotonic decay, no RSS climb, and the remaining spread is far tighter than either laptop pass:
events/s spans 745,047–802,478 (about 7%, against the quiet laptop's ~38%), peak RSS a narrow
70.7–79.0 MiB band, and CPU µs/event is nearly flat across every repeat (1.708–1.720, under 1%
spread) — the tightest this scenario has ever measured. §1's table carries this run's median
(707,215 events/s — a separate 5-repeat sample from the full-suite run, consistent with this solo
one within ordinary noise) as the current number. What's left open from the laptop-era
investigation is narrower still: `DiskQueue::open`'s double-read startup scan (the bounded
active-segment validation pass, still real, still there) as an open question of how much of a
*cleared* spool's own first-open cost feeds any of this scenario's remaining spread — on this
evidence, not much. `buffered`'s own comment in `perf/scenarios/buffered.yaml` and
`docs/known-gaps.md`'s entry both carry this same account now.

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
number is trustworthy across machines (a new `script/vm up` may land on different physical hardware
entirely, per the ADR's own "Consequences" section). A scenario present in only one
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
inferno-flamegraph` into an SVG. On the VM this run captured 7.0s of `passthrough` at 999 Hz (68.0
MiB of samples), wrote `perf/results/passthrough.svg` at **1,119,236 bytes** (~1.07 MiB, kept out of
git — `/perf/results/` is gitignored), and symbols resolve cleanly: real, demangled Rust paths
throughout (`logit_core::event::EventBatch`, `logit_inputs::generate::GenerateInput`,
`logit_core::interner::resolve`, …), not hex addresses.

**This is a `cpu-clock` profile, not `cycles`** — Azure's guest exposes no virtualized PMU
(`perf stat -e cycles true` answers `<not supported>`, `docs/adr/disposable-azure-perf-vm.md`'s "no
virtualized PMU" limitation), so `perf record`'s own fallback to a software clock event is what
actually produced this capture. Sample *counts* and the resulting flamegraph shape are still
meaningful (`perf record -F 999` samples at a fixed wall-clock rate either way), but don't read a
`cpu-clock` capture's absolute sample count against a `cycles` one from a bare-metal box as if the
units matched.

**The container needs three flags, not two, and all three carry over cleanly to a non-SELinux
guest.** `--cap-add SYS_ADMIN` and `--security-opt seccomp=unconfined` were predicted by the ADR
(`perf_event_open`'s capability requirement, and docker's default seccomp profile gating it); the
third, `--security-opt label=disable`, exists for Fedora/SELinux (SELinux denies the `perf_event`
class outright regardless of capabilities under the default container label, which reads exactly
like the `kernel.perf_event_paranoid` problem the first two flags exist for and isn't). `script/perf`
passes all three unconditionally, and on this Debian VM guest — no SELinux at all — the
SELinux-specific flag is a harmless no-op rather than an error; nothing here needed to become
platform-conditional to work on both.

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
- **Pinned to distinct physical cores.** `--pin-sender`/`--pin-child` (`sched_setaffinity`, applied
  between `fork` and `exec` so every thread a process spawns inherits the mask). The reference VM's
  eight cores are identical, so this isn't escaping a heterogeneous-core split the way it was on the
  laptop — it keeps the sender and the measured child from contending for the same core's time.
  Every table below states its pins (`--pin-sender 0,1 --pin-child 2,3` throughout, leaving cores
  4–7 free — this box has room the original 4-core session didn't).
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

**The block below supersedes the previous revision's**, taken 2026-09-18 on a `Standard_F4as_v6`
(4 vCPU). This one is from 2026-09-20, on the current 8-vCPU reference VM
(`docs/adr/disposable-azure-perf-vm.md`'s "The size, and why N vCPUs is N cores" section), the same
three binaries, re-run in full — knee, half-scale, sweep, and `--verify` — plus two items the
previous revision left open: a per-binary capacity bisect, and a THP-forced-`madvise` repeat of the
sweep. The 4-vCPU numbers are not reproduced here; PRs #252, #253, and #254's descriptions are
where they remain, historical.

<!-- udp-intake-numbers:begin 2026-09-20 Azure Standard_F8as_v6 (script/vm), interleaved one-sitting pairs -->

#### Box facts and provenance

| Fact | Value |
|---|---|
| VM size | `Standard_F8as_v6` (8 vCPU, `vCPUsPerCore: 1` — SMT off, 8 full physical cores) |
| CPU model | AMD EPYC 9V74 80-Core Processor (Genoa, cloud SKU) |
| `nproc` | 8 |
| Kernel | `Linux 6.12.107+deb13-cloud-amd64 x86_64` |
| OS | Debian GNU/Linux 13 (trixie), image `Debian:debian-13:13-gen2`, version `0.20260914.2601` |
| `net.core.rmem_max` | left at the cloud-init default, `16777216` (16 MiB) — **not** clamped to match the dev container the way the previous session did; the specs' 1 MiB request is granted 2 MiB either way, so this changes nothing about what's measured, only what's recorded as the ceiling |
| `net.core.rmem_default` | `212992` (stock, unchanged) |
| Granted receive buffer | not printed directly by the harness (no such JSON field), but `kernel_rcvbuf_utilization_max` reaches ~1.00 on the small-datagram scenarios at the knee, exactly as designed for a 1 MiB request doubled to 2 MiB under a 16 MiB ceiling |
| `perf stat -e cycles true` | `<not supported>` — no virtualized PMU, as the ADR documents |
| Swap | none configured |
| THP | `[always] madvise never` (image default; left at `always` for the knee/half/sweep/verify tables — only the dedicated THP sweep below forces `madvise`, and restores `always` after) |
| `box_state` (governor/EPP/AC) | empty `{}` on every result file — this Azure guest exposes no `cpufreq`/`power_supply` sysfs nodes, so nothing to warn on; by construction (dedicated VM, no other tenants visible, no throttling) this is the isolation the whole exercise is for |
| Pins | `--pin-sender 0,1 --pin-child 2,3` throughout — cores 4–7 free, unlike the 4-vCPU session |

Binaries measured, built with a fully isolated `CARGO_TARGET_DIR` per ref (the same mtime-collision
hazard the previous session found — see its account, unchanged this session since `script/vm build`
now gives ref sources their own target dir by default, `docs/adr/disposable-azure-perf-vm.md`'s
"Multiple sources, one VM" section):

| Ref | Head SHA | sha256 |
|---|---|---|
| `udp/w2` | `912f5574649dfe7e11b113c5743c0f251f22c355` | `840f1ef41c09eff5265750192533f10cd8a6c2721df62d96186653c86cea3caa` |
| `udp/w3` | `1b3228c10351d290d17c51b95e09a5a3521f1211` | `b7376b1ada380f6705f2102a84ed9146ba78377b73941056f1f9c3646756823f` |
| `udp/w4` | `4a0c252fa530925c43a4d5e7cb36d8c750d1993d` | `40204bf499843ebd68f20b407cfc5420a070e376de8583db0a86c2c717908746` |

Three distinct sha256s confirmed before any scenario ran — identical to the previous session's own
values for the same three commits, a nice cross-session reproducibility check on top of the
distinctness one.

#### Calibration (bisection on w2, ≤6 tries/scenario, target 1–5% kernel drop)

| Scenario | Tries (scale → drop%) | Knee scale | Knee drop% | Half scale |
|---|---|---|---|---|
| `udp-statsd` | 1.0→19.5%, 0.525→0.0%, 0.7625→0.003%, 0.88125→8.9%, 0.822→**2.31%** | 0.822 | 2.31% | 0.411 |
| `udp-statsd-small` | 1.0→39.2%, 0.525→8.4%, 0.2875→0.007%, 0.40625→0.0%, 0.466→0.04%, 0.495→**2.80%** | 0.495 | 2.80% | 0.248 |
| `udp-statsd-packed` | 1.0→21.7%, 0.525→0.0%, 0.7625→0.0%, 0.88125→12.1%, 0.822→**3.97%** | 0.822 | 3.97% | 0.411 |

The shipped specs' rates (tuned for the dev laptop) are still too fast for this VM even at 8 vCPUs
— `--rate-scale 1.0` on every scenario drops well above target — so calibration is still necessary
here, and lands at knee scales close to the 4-vCPU session's own (0.82/0.50/0.82 then vs 0.82/0.50/
0.82 now for `udp-statsd`/`-small`/`-packed`) — consistent with the knee being mostly a
receive-path property of `logit`'s own decode/queue cost, not something doubling the core count
alone moves much, since the sender and receiver each still only use the two cores they're pinned
to. Calibration was run on `udp/w2` only, same as before; a per-binary capacity number (the
previous session's own named open item) is closed below.

#### Knee-scale results

Interleaved `w2, w3, w4, w2, w3, w4`, `--repeat 5`, medians shown, `--pin-sender 0,1 --pin-child
2,3`.

| Binary/pass | Scenario | CPU µs/event | events/s | kernel drop % | max rcvbuf | peak RSS | fill |
|---|---|---:|---:|---:|---:|---:|---:|
| w2-A | udp-statsd | 0.908 | 1,290,047 | 2.37 | 1.00 | 74.9 MiB | -- |
| w2-A | udp-statsd-small | 3.912 | 365,448 | 2.92 | 1.00 | 31.2 MiB | -- |
| w2-A | udp-statsd-packed | 1.010 | 1,233,980 | 4.01 | 1.00 | 79.4 MiB | -- |
| w3-A | udp-statsd | 0.879 | 1,318,282 | 0.22 | 0.77 | 67.6 MiB | -- |
| w3-A | udp-statsd-small | 3.858 | 376,380 | 0.01 | 0.02 | 29.8 MiB | -- |
| w3-A | udp-statsd-packed | 0.945 | 1,264,165 | 1.66 | 1.00 | 81.3 MiB | -- |
| w4-A | udp-statsd | 0.835 | 1,321,178 | 0.00 | 0.22 | 50.4 MiB | 15.1 |
| w4-A | udp-statsd-small | 3.054 | 376,427 | 0.00 | 0.01 | 32.9 MiB | 3.1 |
| w4-A | udp-statsd-packed | 0.853 | 1,285,575 | 0.00 | 0.06 | 47.0 MiB | 13.0 |
| w2-B | udp-statsd | 0.902 | 1,289,088 | 2.39 | 1.00 | 76.1 MiB | -- |
| w2-B | udp-statsd-small | 4.062 | 364,288 | 3.22 | 1.00 | 31.2 MiB | -- |
| w2-B | udp-statsd-packed | 0.961 | 1,224,979 | 4.71 | 1.00 | 83.6 MiB | -- |
| w3-B | udp-statsd | 0.875 | 1,318,590 | 0.20 | 0.74 | 68.4 MiB | -- |
| w3-B | udp-statsd-small | 2.965 | 376,420 | 0.00 | 0.01 | 30.1 MiB | -- |
| w3-B | udp-statsd-packed | 0.989 | 1,269,388 | 1.26 | 1.00 | 80.7 MiB | -- |
| w4-B | udp-statsd | 0.852 | 1,321,168 | 0.00 | 0.26 | 50.9 MiB | 15.7 |
| w4-B | udp-statsd-small | 3.411 | 376,428 | 0.00 | 0.01 | 33.2 MiB | 3.9 |
| w4-B | udp-statsd-packed | 0.853 | 1,285,541 | 0.00 | 0.06 | 44.2 MiB | 12.6 |

#### Half-scale results (0.5 × knee)

| Binary/pass | Scenario | CPU µs/event | events/s | kernel drop % | peak RSS |
|---|---|---:|---:|---:|---:|
| w2-A | udp-statsd | 0.849 | 660,606 | 0.00 | 44.9 MiB |
| w2-A | udp-statsd-small | 4.024 | 188,216 | 0.00 | 30.0 MiB |
| w2-A | udp-statsd-packed | 0.951 | 642,810 | 0.00 | 39.3 MiB |
| w3-A | udp-statsd | 0.877 | 660,606 | 0.00 | 43.8 MiB |
| w3-A | udp-statsd-small | 5.329 | 188,215 | 0.00 | 30.0 MiB |
| w3-A | udp-statsd-packed | 0.905 | 642,808 | 0.00 | 40.8 MiB |
| w4-A | udp-statsd | 0.866 | 660,599 | 0.00 | 44.1 MiB |
| w4-A | udp-statsd-small | 5.238 | 188,215 | 0.00 | 33.3 MiB |
| w4-A | udp-statsd-packed | 0.861 | 642,809 | 0.00 | 41.7 MiB |
| w2-B | udp-statsd | 0.935 | 660,598 | 0.00 | 42.2 MiB |
| w2-B | udp-statsd-small | 3.143 | 188,216 | 0.00 | 30.2 MiB |
| w2-B | udp-statsd-packed | 1.012 | 642,802 | 0.00 | 38.9 MiB |
| w3-B | udp-statsd | 0.914 | 660,599 | 0.00 | 41.5 MiB |
| w3-B | udp-statsd-small | 3.789 | 188,216 | 0.00 | 29.8 MiB |
| w3-B | udp-statsd-packed | 0.987 | 642,802 | 0.00 | 38.4 MiB |
| w4-B | udp-statsd | 0.836 | 660,605 | 0.00 | 45.0 MiB |
| w4-B | udp-statsd-small | 3.282 | 188,216 | 0.00 | 33.9 MiB |
| w4-B | udp-statsd-packed | 0.840 | 642,808 | 0.00 | 42.3 MiB |

(All half-scale queue drops are 0.)

#### Compare: deltas vs. control-to-control drift

`+` on events/s is better, `-` on µs/event is better, `-` on RSS is smaller (usually better), drop
points are `after - before`.

**Knee scale**

| Scenario | Pair | events/s | µs/event | peak RSS | drop pts |
|---|---|---:|---:|---:|---:|
| udp-statsd | w2-A→w3-A | +2.2% | -3.2% | -9.8% | -2.15 |
| udp-statsd | w2-B→w3-B | +2.3% | -3.0% | -10.1% | -2.19 |
| udp-statsd | w3-A→w4-A | +0.2% | -5.0% | -25.5% | -0.22 |
| udp-statsd | w3-B→w4-B | +0.2% | -2.7% | -25.5% | -0.20 |
| udp-statsd | control w2-A→w2-B | -0.1% | -0.6% | +1.6% | +0.02 |
| udp-statsd | control w3-A→w3-B | +0.0% | -0.4% | +1.2% | -0.02 |
| udp-statsd | control w4-A→w4-B | -0.0% | +2.0% | +1.1% | +0.00 |
| udp-statsd-small | w2-A→w3-A | +3.0% | -1.4% | -4.5% | -2.90 |
| udp-statsd-small | w2-B→w3-B | +3.3% | **-27.0%** | -3.4% | -3.22 |
| udp-statsd-small | w3-A→w4-A | +0.0% | -20.8% | +10.4% | -0.01 |
| udp-statsd-small | w3-B→w4-B | +0.0% | **+15.0%** | +10.1% | +0.00 |
| udp-statsd-small | control w2-A→w2-B | -0.3% | +3.8% | -0.1% | +0.31 |
| udp-statsd-small | control w3-A→w3-B | +0.0% | **-23.1%** | +1.1% | -0.01 |
| udp-statsd-small | control w4-A→w4-B | +0.0% | +11.7% | +0.8% | +0.00 |
| udp-statsd-packed | w2-A→w3-A | +2.4% | -6.5% | +2.4% | -2.35 |
| udp-statsd-packed | w2-B→w3-B | +3.6% | +3.0% | -3.5% | -3.46 |
| udp-statsd-packed | w3-A→w4-A | +1.7% | -9.8% | **-42.1%** | -1.66 |
| udp-statsd-packed | w3-B→w4-B | +1.3% | -13.8% | **-45.2%** | -1.26 |
| udp-statsd-packed | control w2-A→w2-B | -0.7% | -4.9% | +5.3% | +0.70 |
| udp-statsd-packed | control w3-A→w3-B | +0.4% | +4.6% | -0.7% | -0.41 |
| udp-statsd-packed | control w4-A→w4-B | -0.0% | +0.0% | -6.0% | +0.00 |

**Half scale**

| Scenario | Pair | events/s | µs/event | peak RSS | drop pts |
|---|---|---:|---:|---:|---:|
| udp-statsd | w2-A→w3-A / control w2-A→w2-B | -0.0% | +3.3% / +10.0% (ctl) | -2.4% / -6.0% (ctl) | 0 |
| udp-statsd | w3-A→w4-A / control w3-A→w3-B | -0.0% | -1.3% / +4.2% (ctl) | +0.6% / -5.2% (ctl) | 0 |
| udp-statsd-small | w2-A→w3-A / control w2-A→w2-B | -0.0% | **+32.4%** / -21.9% (ctl) | -0.3% / +0.6% (ctl) | 0 |
| udp-statsd-small | w3-A→w4-A / control w3-A→w3-B | -0.0% | -1.7% / -28.9% (ctl) | +11.2% / -0.4% (ctl) | 0 |
| udp-statsd-packed | w2-A→w3-A / control w2-A→w2-B | -0.0% | -4.9% / +6.5% (ctl) | +3.8% / -1.0% (ctl) | 0 |
| udp-statsd-packed | w3-A→w4-A / control w3-A→w3-B | +0.0% | -4.8% / +9.1% (ctl) | +2.2% / -5.9% (ctl) | 0 |

Full per-pair numbers (both A/B directions, every scenario) are in the raw JSON under
`~/lib/logit/tmp/perf/vm-rebase-full/` and `~/lib/logit/tmp/perf/vm/`. At half scale,
`udp-statsd-small`'s control drift (±22–29 points, control-to-control) is again wider than most
branch deltas — see the reading below for why this scenario's µs/event isn't readable at all,
knee or half.

#### Per-binary capacity: highest offered rate at ≈0% kernel drop

The previous session's own named open item — knee-scale drops on w3/w4 measured at *w2's* knee,
not each binary's own — closed this session. Bisected independently per binary (target ≤0.5% drop,
≤8 tries), as a multiplier of each scenario's shipped `rate:`:

| Binary | `udp-statsd` | `udp-statsd-small` | `udp-statsd-packed` |
|---|---:|---:|---:|
| `udp/w2` | 0.800× (71,965/s) | 0.480× (365,156/s) | 0.785× (91,033/s) |
| `udp/w3` | 0.822× (73,969/s) | 0.547× (415,922/s) | 0.807× (93,616/s) |
| `udp/w4` | 0.904× (81,316/s) | **1.992× (1,514,062/s)** | 0.911× (105,669/s) |

`udp-statsd`/`-packed` move modestly w2→w4 (~13–16% more headroom), consistent with the knee-scale
table's own small-percent CPU gains. **`udp-statsd-small` is the outlier that confirms the design
intent:** `recvmmsg` (W4) roughly *quadruples* the highest loss-free rate for the single-datagram,
syscall-bound workload specifically — exactly the scenario batched reads exist for — while barely
moving the two packing shapes where decode cost, not syscall count, already dominated.

#### `read_batch` sweep (w4 only, `--repeat 3`, knee scale)

Temporary scenario/load copies (`udp-statsd-rbN` / `udp-statsd-small-rbN`) were placed under
`perf/scenarios/`+`perf/load/` on the VM only, deleted after (never committed).

| `read_batch` | **udp-statsd-small** µs/event | fill | drop% | rcvbuf | | **udp-statsd** µs/event | fill | drop% | rcvbuf |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 4.173 | 1.0 | 11.60% | 1.00 | | 0.961 | 1.0 | 4.63% | 1.00 |
| 16 | 3.410 | 3.7 | 0.00% | 0.01 | | 0.797 | 6.3 | 0.00% | 0.28 |
| 32 | 3.618 | 3.5 | 0.00% | 0.01 | | 0.808 | 12.1 | 0.00% | 0.27 |
| **64** | **3.486** | **4.0** | **0.00%** | **0.03** | | **0.795** | **7.6** | **0.00%** | **0.28** |
| 128 | 3.386 | 4.2 | 0.00% | 0.01 | | 0.808 | 14.3 | 0.00% | 0.26 |
| 256 | 3.315 | 3.2 | 0.00% | 0.02 | | 0.795 | 6.3 | 0.00% | 0.27 |

**Peak RSS across the sweep** (`udp-statsd-small`): 31.5 (rb1) → 31.2 → 32.0 → 34.1 → 39.2 → 47.6
MiB (rb256) — rises with `read_batch`, the same qualitative shape the 4-vCPU session found and
this session's own dedicated THP experiment (below) now explains directly. `udp-statsd`'s RSS is
noisier and doesn't show as clean a trend (76.9 → 70.8 → 51.0 → 53.7 → 55.5 → 79.6 MiB) — plausible
given mixed/larger datagrams have more memory factors in play than the small-datagram slab alone.

`fill` plateaus by `read_batch=16` for both scenarios (~3–4 for `-small`, ~6–14 for `udp-statsd`,
noisier than a clean monotonic curve but not trending up with `read_batch` past 16): 64 already
captures essentially all the batching benefit, and `udp-statsd`'s µs/event is flat within noise
from 16 upward (0.795–0.808). `-small`'s µs/event is itself noisy across the whole sweep, same
reading as the calibration and per-binary tables above — see "Reading the numbers" below.

#### `read_batch` sweep under `transparent_hugepage=madvise` (new this session)

The previous session's own open item: repeat the sweep with THP forced off, to test whether
`THP=always` explains the RSS rise directly. `udp-statsd-small` only, w4, same protocol, THP
restored to `always` after:

| `read_batch` | µs/event | fill | drop% | peak RSS |
|---|---:|---:|---:|---:|
| 1 | 4.109 | 1.0 | 11.80% | 16.8 MiB |
| 16 | 3.332 | 3.1 | 0.00% | 14.4 MiB |
| 32 | 3.985 | 3.2 | 0.00% | 14.8 MiB |
| 64 | 3.178 | 4.4 | 0.00% | 14.9 MiB |
| 128 | 2.611 | 2.9 | 0.00% | 15.0 MiB |
| 256 | 4.066 | 3.2 | 0.00% | 16.1 MiB |

**Confirmed, not just likely.** Under `madvise`, peak RSS stays flat in a 14.4–16.8 MiB band across
the entire `read_batch` range — no rise at 128/256 the way the `THP=always` table above shows
(34.1 → 39.2 → 47.6 MiB over the same range). This closes `docs/design/memory.md`'s "likely, but
not confirmed" caveat directly: the `read_batch × 65,507`-byte slab really does become fully
resident under `THP=always` (one touched 4 KiB page faulting in its enclosing 2 MiB huge page) and
really does stay mostly untouched under `madvise`/`never` — not a coincidence, a same-box,
same-binary, THP-toggled repeat of the identical sweep.

#### `--verify` (default scale, one repeat each)

| Binary | Result |
|---|---|
| `udp-statsd` (w2/w3/w4) | 620,000 datagrams, 11,074,692 lines, **11,074,692 events delivered exactly, zero drops**, all three |
| `udp-statsd-small` (w2/w3/w4) | 5,000,000 datagrams, 5,000,000 lines, **5,000,000 events delivered exactly, zero drops**, all three |
| `udp-statsd-packed` (w2/w3/w4) | 800,000 datagrams, 10,788,160 lines, **10,788,160 events delivered exactly, zero drops**, all three |

All nine (three binaries × three scenarios) pass the strict self-check.

<!-- udp-intake-numbers:end -->

### Reading the numbers

**The robust signal is loss at a fixed offered load, not CPU per event** — unchanged from the
4-vCPU session's own reading. At the calibrated knee, kernel drops fall monotonically w2 → w3 → w4
on every scenario, and both interleaved passes agree: `udp-statsd` 2.37/2.39% → 0.22/0.20% →
0.00%; `udp-statsd-small` 2.92/3.22% → 0.01/0.00% → 0.00%; `udp-statsd-packed` 4.01/4.71% →
1.66/1.26% → 0.00%. Control-to-control drift in drop points stays under 1 throughout (see the
compare table above), well inside the w2→w3 and w3→w4 movements. Decode-side batching (W3) removes
most of the loss; `recvmmsg` (W4) removes the rest, taking every scenario to exactly zero drops at
the knee, same as before. For syscall-bound UDP intake, this — not µs/event — is still the number
to trust.

**CPU µs/event is a smaller, partly-inside-drift story here too**, though a real one for `packed`
this time: `udp-statsd`'s w2→w3 gain is modest and mostly within drift (−3.2%/−3.0% against a
control of ≤0.6%), and w3→w4 similarly (−5.0%/−2.7%, controls ≤2.0%). `udp-statsd-packed`'s w3→w4
gain (−9.8%/−13.8%, against controls of +4.6%/+0.0%) is the cleanest CPU win in this table —
clearly beyond its own control drift on both interleaved passes, unlike the 4-vCPU session where
the same comparison was ambiguous.

**`udp-statsd-small`'s CPU µs/event still isn't readable, on either box.** Control-to-control
drift stays large (knee: +3.8%/−23.1%/+11.7%; half scale: −21.9%/−28.9%) — the same order of
magnitude as any branch delta, same conclusion the 4-vCPU session reached: nothing about
`-small`'s *timing* number should be read as a w2→w3 or w3→w4 finding on a VM (its drop-rate and
capacity numbers remain trustworthy, since those aren't timings). The per-binary capacity table
above gives the honest headline for this scenario instead — see below.

**Peak RSS confirms the 4-vCPU session's own finding, and by a similar margin.** At the knee, RSS
falls w3→w4 on `udp-statsd` (−25.5%/−25.5%) and `udp-statsd-packed` (−42.1%/−45.2%), both far
beyond control drift, because w4 no longer builds a receive-side backlog the way w3 did — the same
shape and similar magnitude the previous session found (−19.8%/−23.1% and −38.2%/−42.3%
respectively). At half scale, RSS is flat within drift.

**Per-binary capacity — closed this session — makes the `recvmmsg` story sharper than the
knee-scale table alone can.** Running w3/w4 at *w2's* knee (the only measurement the previous
session had) understates what W4 actually bought for `udp-statsd-small`: at its own highest
loss-free rate, w4 sustains **1,514,062 datagrams/s versus w2's 365,156 — a ~4.1× capacity gain**,
not the roughly-flat drop-rate-at-a-fixed-load picture the knee-scale table shows (because that
table's fixed load was calibrated *below* w2's ceiling to begin with, so both binaries look
"zero drop" once w3/w4 clear it). `udp-statsd`/`-packed` move only 13–16% w2→w4 in the same
measurement, confirming the earlier reading that `recvmmsg`'s benefit is concentrated in the
syscall-bound small-datagram case specifically, and putting a number on "concentrated."

**The `read_batch` sweep confirms the plateau, and 64 stays the default** — `read_batch: 1`
reproduces the pre-`recvmmsg` loss (11.60% `-small`, 4.63% mixed); from 16 upward, drops are 0 and
`udp-statsd`'s µs/event is flat within noise (0.795–0.808); mean fill plateaus by 16 for both
scenarios. Peak RSS still rises noticeably at large `read_batch` for `-small` under `THP=always`
(31.2 MiB at 16 → 47.6 at 256) — same shape the 4-vCPU session found.

**The THP explanation is confirmed, not just likely, this session.** A same-box, same-binary
repeat of the identical `udp-statsd-small` sweep with `transparent_hugepage` forced to `madvise`
shows peak RSS flat in a 14.4–16.8 MiB band across the *entire* `read_batch` range — no rise at
128/256 the way `THP=always` shows. The `read_batch × 65,507`-byte slab really is what becomes
resident under `THP=always` (one touched 4 KiB page faulting in its enclosing 2 MiB huge page);
under `madvise`/`never` it stays mostly untouched, exactly as the "likely, not confirmed"
hypothesis predicted. `docs/design/memory.md` §5's caveat is updated to say so.

### Open after this workstream

- **The wakeup-cost hypothesis for `udp-statsd-small` on VMs is still a hypothesis, not a
  finding** — nothing in this session isolates wakeup cost directly (e.g. via a wakeup-rate probe
  independent of `fill`), and this session didn't re-run the half-scale-vs-knee CPU comparison the
  4-vCPU session used to motivate it. The per-binary capacity table above is a cleaner way to
  characterize this scenario's headline number regardless of whether the hypothesis holds.
- **The shared-task / `SO_REUSEPORT` follow-up from earlier workstreams is still open** and
  untouched by this session — this session measured the existing single-listener-task design at
  higher fidelity, not a multi-task alternative.

Both items the previous revision of this section named as open — a per-binary capacity metric, and
confirming the THP explanation — are closed above.

## 8. Event sizing bake-off (2026-09-21)

[ADR `event-sizing-and-allocation-strategy`](../adr/event-sizing-and-allocation-strategy.md)
records the decision this section's numbers back: `AttrMap`'s inline capacity stays 8, and
attribute maps stay unreserved, per-key `insert_sym` builds — no change to `Event`. This section is
the full measurement record the ADR draws its tables from; read the ADR for the decision and the
"why," this section for every number, recomputed from the raw results JSON and bench text rather
than copied from any intermediate table.

### Box facts and protocol

| Fact | Value |
|---|---|
| VM size | `Standard_F8as_v6` (8 vCPU, SMT off — 8 full physical cores, `docs/adr/disposable-azure-perf-vm.md`) |
| CPU model | AMD EPYC 9V74 80-Core Processor (Genoa, cloud SKU) — from `perf/results/*.json`'s `cpu_model` |
| `nproc` | 8 — from the same JSON |
| `rustc` | `rustc 1.98.1 (48a229cea 2026-09-01)` — from the same JSON |
| Profile | `release` |
| `box_state` | empty `{}` on every result file — this guest exposes no `cpufreq`/`power_supply` sysfs, same as every other VM session in this document |
| `main` at head | `8110aece154b1551d9335157b332101c7116e768`, `git.dirty: false` |
| `sizing-w4`/`9a3ebc3ba294`/experiment binaries | built from the `sizing/w3c`→`sizing/w4` stack and its throwaway experiment commits (`d78c233`, `badc1c2`, `efe9b8b`, and four `EXPERIMENT`/never-merged commits) — one `sha256` per binary, recorded in each result file's `binary` block |

**Protocol.** Every table below is two interleaved rounds of three repeats per binary (round 1:
every binary back to back; round 2: the same order repeated), pooled to six repeats and read as one
`median of 6`; `spread = (max − min) / min` over those six. Binaries were built with `script/vm
build <ref>` (one `sha256` per source, confirmed distinct before any scenario ran, the same
discipline §7's table follows) and run with `script/perf run --logit-bin perf/bins/<slug>/logit
--repeat 3`, one round per invocation. All CPU µs/event tables below were independently recomputed
by taking every repeat's `cpu_us_per_event` straight from the six `perf/results/*.json` files behind
each binary/round pair (never from an intermediate `.txt` table) and computing the median and spread
in Python; every value matched the session's own generated tables to rounding. `bench-table.txt`'s
own micro-bench figures (§8e) were independently re-derived the same way, from `sizing-w4.bench.
{size_vs_alloc,attr_arms}.{1,2,3}.txt` — three separate `cargo bench` runs, pinned `taskset -c 2`
per the bench files' own module docs — median of 3 `cargo bench`/divan medians, not divan's raw
samples. One gap found in the process: `bench-table.txt` itself is missing a row,
`alloc_free::immediate::432` (true value 4.93 ns ± 1.4%, recomputed here) — every other row in that
table matched its recomputed value to within rounding.

### (a) `AttrMap` inline capacity: five binaries, ten scenarios

`main`; `sizing-w4` (N=8, the shipped tree, plus arm P's bulk build *before* the in-order fast
path); `sizing-arm-n0`/`-n4`/`-n16` (the `sizing-w4` tree with `AttrMap`'s inline capacity changed
to 0/4/16, nothing else). CPU µs/event, median of 6, spread in parentheses; deltas in the ADR's
Decision (1) table are against the `sizing-w4` column, not `main`.

| Scenario | `main` | `sizing-w4` (N=8) | `arm-n0` (N=0) | `arm-n4` (N=4) | `arm-n16` (N=16) |
|---|--:|--:|--:|--:|--:|
| `passthrough` | 0.330 (±0.5%) | 0.328 (±2.2%) | 0.382 (±0.8%) | 0.351 (±0.7%) | 0.364 (±2.7%) |
| `aggregate` | 0.325 (±1.0%) | 0.325 (±0.7%) | 0.266 (±3.5%) | 0.299 (±1.6%) | 0.345 (±0.8%) |
| `fanout` | 0.643 (±1.9%) | 0.643 (±2.0%) | 0.726 (±0.8%) | 0.705 (±0.7%) | 0.695 (±0.8%) |
| `route` | 0.688 (±3.0%) | 0.682 (±3.5%) | 0.729 (±0.6%) | 0.735 (±0.5%) | 0.805 (±2.2%) |
| `json-parse` (~10 attr) | 0.861 (±0.9%) | 1.004 (±1.1%) | 1.092 (±2.1%) | 1.097 (±0.4%) | 1.046 (±2.7%) |
| `logfmt-parse` (9 attr) | 1.015 (±0.4%) | 0.944 (±6.1%) | 0.997 (±1.5%) | 0.992 (±1.1%) | 0.854 (±1.4%) |
| `json-parse-app-log` (12) | 1.302 (±1.0%) | 1.503 (±1.7%) | 1.649 (±3.7%) | 1.571 (±0.6%) | 1.411 (±1.6%) |
| `json-parse-nested-log` (10, 4 nested maps) | 2.215 (±1.4%) | 2.450 (±0.8%) | 2.580 (±1.7%) | 2.299 (±1.0%) | 2.414 (±0.7%) |
| `json-parse-access-log` (30) | 3.000 (±1.2%) | 3.598 (±1.7%) | 3.728 (±0.6%) | 3.695 (±1.0%) | 3.491 (±0.5%) |
| `native-relay` | 1.346 (±1.2%) | 1.351 (±0.7%) | 1.286 (±1.3%) | 1.337 (±4.2%) | 1.398 (±1.2%) |

Peak RSS, MiB (median of 6):

| Scenario | `main` | `sizing-w4` | `arm-n0` | `arm-n4` | `arm-n16` |
|---|--:|--:|--:|--:|--:|
| `passthrough` | 67.3 | 67.1 | 73.1 | 83.8 | 79.1 |
| `aggregate` | 67.3 | 65.2 | 58.4 | 62.9 | 73.0 |
| `fanout` | 46.2 | 46.2 | 46.3 | 46.1 | 46.1 |
| `route` | 104.1 | 101.7 | 132.4 | 149.7 | 97.7 |
| `json-parse` | 103.3 | 85.8 | 61.4 | 69.7 | 93.8 |
| `logfmt-parse` | 66.1 | 69.4 | 54.7 | 63.5 | 86.3 |
| `json-parse-app-log` | 61.8 | 63.3 | 54.7 | 58.9 | 71.8 |
| `json-parse-nested-log` | 67.5 | 70.5 | 54.3 | 58.5 | 79.5 |
| `json-parse-access-log` | 63.0 | 62.9 | 57.1 | 56.9 | 68.2 |
| `native-relay` | 224.5 | 237.0 | 270.0 | 261.8 | 217.6 |

Several rows carry a spread past the ~10% mark ordinarily read as clean signal:
`json-parse-app-log`/`arm-n0` (±3.7%) is fine, but `sizing-w4`/`logfmt-parse` (±6.1%) and
`arm-n4`/`native-relay` (±4.2%) sit noticeably above this table's typical ±0.5–2%; none crosses 10%
here, but they are the two noisiest cells in an otherwise tight table and the ADR's own text (0.4–6%
spread range) is bounded by the `logfmt-parse`/`sizing-w4` cell exactly.

### (b) The in-order fast path: does the bulk build's own regression hold up?

`sizing-w4`'s bulk build (arm P) was measured once, found 12–20% slower than `main` on the json
legs, and rebuilt with an in-order fast path (`efe9b8b`, "keep an in-order `AttrMap` bulk build on
an append-only path") before this re-run, tagged `9a3ebc3ba294`. All deltas below are against
`main`.

| Scenario | `main` | `sizing-w4` (before fix) | `9a3ebc3ba294` (fast path) |
|---|--:|--:|--:|
| `passthrough` | 0.333 (±2.3%) | 0.328, −1.7% (±3.0%) | 0.330, −0.9% (±2.3%) |
| `json-parse` | 0.858 (±1.9%) | 1.007, +17.4% (±0.6%) | 0.950, +10.7% (±3.4%) |
| `logfmt-parse` | 1.014 (±2.5%) | 0.944, −6.9% (±1.4%) | 0.944, −6.9% (±5.5%) |
| `json-parse-app-log` | 1.304 (±1.4%) | 1.498, +14.9% (±2.8%) | 1.464, +12.3% (±0.9%) |
| `json-parse-nested-log` | 2.210 (±1.3%) | 2.470, +11.8% (±1.4%) | 2.387, +8.1% (±1.2%) |
| `json-parse-access-log` | 2.995 (±0.8%) | 3.587, +19.8% (±0.5%) | 3.494, +16.7% (±0.8%) |
| `native-relay` | 1.348 (±1.6%) | 1.349, +0.0% (±0.7%) | 1.341, −0.5% (±1.5%) |

The fast path shaves a few points off every json leg (`json-parse` +17.4%→+10.7%,
`json-parse-access-log` +19.8%→+16.7%) but does not come close to closing the gap: every json
scenario is still double digits over `main`, `logfmt-parse`'s own ~7% gain is unaffected by which
version of the bulk build is in the tree (it doesn't touch `logfmt`'s call site), and `native-relay`
stays flat either way. This is the re-run that motivated widening the comparison in (c): is the
regression the bulk build itself, or specifically its eager up-front reservation?

### (c) `json`'s merge: reservation and bulk-build A/Bs

Table 1 — against `main`, four binaries: `main`; `9a3ebc3ba294` (the fast-path bulk build, same as
(b)); `sizing-exp-json-loop` (`json`'s merge put back to `main`'s per-key `insert_sym` loop, on the
`sizing-w4` tree — i.e. everything else about the tree unchanged, only `json`'s own call site
reverted); `sizing-exp-json-reserve-loop` (that same loop plus `reserve_exact(n)` up front).

| Scenario | `main` | `9a3ebc3ba294` | `json-loop` | `reserve-loop` |
|---|--:|--:|--:|--:|
| `json-parse` | 0.862 (±2.4%) | 0.946, +9.8% (±6.0%) | 0.854, −0.9% (±1.4%) | 0.856, −0.7% (±1.1%) |
| `json-parse-app-log` | 1.306 (±2.9%) | 1.462, +12.0% (±0.8%) | 1.332, +2.0% (±1.0%) | 1.431, +9.6% (±3.5%) |
| `json-parse-nested-log` | 2.212 (±1.1%) | 2.393, +8.2% (±1.5%) | 2.234, +1.0% (±1.7%) | 2.345, +6.0% (±0.5%) |
| `json-parse-access-log` | 3.013 (±2.3%) | 3.499, +16.1% (±0.8%) | 3.033, +0.7% (±1.2%) | 3.225, +7.0% (±2.0%) |

`json-loop` — the per-key loop, nothing else changed — lands within ±2% of `main` on every leg,
confirming the loop itself was never the problem; every point of regression in the other three
columns comes from what replaces it. `9a3ebc3ba294`'s own regression here (+8.2% to +16.1%) is the
recomputed source of the ADR's "cost the json scenarios 8–17%" line — see this section's caveats
below for why "17%" overstates the recomputed ceiling.

Table 2 — five variants at the two widest shapes, deltas against `json-loop` on the same tree (its
own baseline, not `main`): `json-loop`; `reserve-loop`; `reserve-pow2-loop` (loop +
`reserve` rounded to the next power of two); `9a3ebc3ba294` (bulk build, exact reservation, fast
path); `sizing-exp-bulk-pow2` (bulk build, power-of-two reservation).

| `json`'s merge | `json-parse-app-log` (12 attr) | `json-parse-access-log` (30 attr) |
|---|--:|--:|
| `json-loop` | 1.333 (±0.5%) | 3.036 (±1.8%) |
| `reserve-loop` (`reserve_exact`) | 1.434, +7.6% (±1.9%) | 3.229, +6.4% (±1.9%) |
| `reserve-pow2-loop` | 1.446, +8.5% (±0.7%) | 3.176, +4.6% (±1.2%) |
| `9a3ebc3ba294` (bulk, exact) | 1.461, +9.6% (±0.7%) | 3.484, +14.8% (±0.7%) |
| `sizing-exp-bulk-pow2` | 1.459, +9.5% (±2.0%) | 3.423, +12.8% (±1.2%) |

Every alternative to the untouched loop costs something, at both widths, whether it reserves eagerly
without changing the insert order (`reserve-loop`/`reserve-pow2-loop`) or bulk-builds
(`9a3ebc3ba294`/`bulk-pow2`) — reservation shape (exact vs. power-of-two) barely matters next to
*whether* it reserves early at all.

### (d) The candidate final tree vs. `main`

`sizing-exp-final`: the bulk build kept only at `logfmt` and the native decoder (the two call sites
where W1's own reasoning argued it should help without json's per-key ordering working against it),
plus an exactly-sized `AttrMap::clone`. All ten scenarios, deltas and peak RSS against `main`:

| Scenario | `main` | `sizing-exp-final` | Δ | RSS `main`→`final` (MiB) |
|---|--:|--:|--:|--:|
| `passthrough` | 0.330 (±3.1%) | 0.327 (±3.5%) | −0.7% | 67.3 → 69.3 |
| `aggregate` | 0.324 (±0.5%) | 0.326 (±4.7%) | +0.6% | 66.7 → 66.7 |
| `fanout` | 0.645 (±2.4%) | 0.641 (±1.3%) | −0.6% | 46.1 → 47.2 |
| `route` | 0.688 (±3.0%) | 0.686 (±1.3%) | −0.4% | 103.9 → 102.3 |
| `json-parse` | 0.863 (±1.3%) | 0.862 (±0.8%) | −0.2% | 105.5 → 95.7 |
| `logfmt-parse` | 1.014 (±1.0%) | 0.955 (±1.3%) | −5.8% | 66.6 → 69.8 |
| `json-parse-app-log` | 1.305 (±3.9%) | 1.357 (±2.1%) | +4.0% | 63.6 → 62.5 |
| `json-parse-nested-log` | 2.211 (±0.8%) | 2.248 (±0.8%) | +1.7% | 64.3 → 69.7 |
| `json-parse-access-log` | 2.997 (±0.9%) | 3.067 (±1.8%) | +2.4% | 60.6 → 61.8 |
| `native-relay` | 1.344 (±2.0%) | 1.352 (±1.1%) | +0.6% | 221.1 → 242.7 |

`json` no longer uses the bulk build at all in this tree, so its three scenarios' movement here
(+1.7% to +4.0%) is noise/other-changes, not the bulk-build regression — that's the point of scoping
it down to two call sites. `logfmt-parse`'s −5.8% is the "~6%" win the ADR's Decision (2) weighs
against ~300 lines of bulk-build code; `native-relay` at +0.6% is the "flat" the same sentence
claims.

Table — the four-way check, isolating the native decoder's own use of the bulk build and the
exactly-sized clone, at the two widest json legs plus `passthrough`/`native-relay`:
`sizing-exp-final`; `-final-nonative` (native decoder's own call site reverted to the loop, only
`logfmt` keeps the bulk build); `-final-derivedclone` (the derived `#[derive(Clone)]` restored in
place of the exactly-sized hand-written one).

| Scenario | `main` | `final` | `final-nonative` | `final-derivedclone` |
|---|--:|--:|--:|--:|
| `passthrough` | 0.330 (±0.6%) | 0.332, +0.4% (±2.3%) | 0.326, −1.2% (±2.1%) | 0.331, +0.2% (±3.2%) |
| `json-parse-app-log` | 1.303 (±1.5%) | 1.351, +3.7% (±1.0%) | 1.348, +3.5% (±1.0%) | 1.344, +3.1% (±2.0%) |
| `json-parse-access-log` | 2.997 (±1.6%) | 3.065, +2.3% (±0.9%) | 3.066, +2.3% (±1.6%) | 3.047, +1.7% (±1.1%) |
| `native-relay` | 1.345 (±0.8%) | 1.354, +0.7% (±0.4%) | 1.364, +1.4% (±1.5%) | 1.349, +0.3% (±1.2%) |

`final` vs. `final-nonative` is within noise on both json legs (native decode isn't on their path at
all) and inside a point on `passthrough`/`native-relay` — the native decoder's own bulk-build use
neither helps nor hurts these four scenarios measurably. `final` vs. `final-derivedclone` is the
ADR's "measured CPU-neutral (±0.6%)" claim: recomputed deltas are +0.16%, +0.55%, +0.59%, and +0.37%
across the four rows — the exactly-sized clone is not a CPU win over the ordinary derived one at any
of these widths, on this tree.

### (e) Micro-benches (pinned `taskset -c 2`, median of 3 `cargo bench` runs)

**Every batch bench below is un-batched by hand** — divan's reported median is for the *whole*
iteration, not divided by an `ItemsCount` counter, so a bench whose closure does 64 allocations (or
1000 elements) reports one number for all 64 (or 1000). Batch sizes, confirmed against
`crates/logit-bench/benches/size_vs_alloc.rs`: `alloc_free::pair_same_thread`/`pair_cross_thread`,
64 blocks/iteration; `move_value::ptr_move`, 64 moves; `move_value::vec_push_pop`, 64 pushes + 64
pops = 128 moves; `scan::touch_head`/`drop_batch`/`clone_batch`, 1000 elements (`generate_in`'s own
`receive.batch_max_events` default). `alloc_free::immediate` and `realloc_chain::*` are already
one-operation-per-iteration; no division needed.

#### Allocator cost (jemalloc, real allocator, no counting wrapper), by `AttrMap`'s own growth-ladder sizes

| Bytes | `immediate` (ns/pair) | `pair_same_thread` (ns/block, 64 live) | `pair_cross_thread` (ns/block, freed on another thread) |
|---:|--:|--:|--:|
| 432 (9 entries exact) | 4.93 (±1.4%) | 5.88 (±13.6%) | 32.69 (±12.9%) |
| 576 (12 entries exact) | 4.93 (±1.6%) | 7.62 (±5.3%) | 31.12 (±15.7%) |
| 768 (today's spill, 16×48) | 4.99 (±2.4%) | 10.40 (±11.5%) | 42.94 (±25.0%) |
| 1440 (30 entries exact) | 5.34 (±2.2%) | 14.39 (±8.6%) | 51.16 (±41.2%) |
| 1536 (first growth step, 32×48) | 5.34 (±2.2%) | 16.66 (±2.3%) | 68.77 (±5.0%) |
| 3072 (second growth step, 64×48) | 6.23 (±3.8%) | 25.48 (±0.6%) | 91.92 (±10.3%) |

`immediate`'s 432-byte row is the one missing from `bench-table.txt` (recomputed here, not copied).
`pair_cross_thread`'s spreads run well past 10% at every size but 1536 — cross-thread free, sent
down a bounded channel, is this table's least reproducible number even at median of 3×6; read the
31–92 ns range as an order of magnitude, not a precise curve.

#### Realloc growth steps, against one exactly-sized allocation

| Bench | ns |
|---|--:|
| `step_768_to_1536` (first growth step alone) | 53.22 (±13.1%) |
| `step_1536_to_3072` (second growth step alone) | 81.08 (±1.0%) |
| `ladder_768_1536_3072` (both steps, touched between) | 148.0 (±5.2%) |
| `exact_3072` (one allocation, the ladder's end size, touched once) | 57.77 (±1.7%) |
| `spill_width::432` (9 entries' exact size, touched) | 9.59 (±20.5%) |
| `spill_width::768` (today's 16-slot spill, touched) | 19.05 (±4.6%) |

The ladder (148.0 ns) against one exact allocation at the same end size (57.77 ns) is what a
`reserve` ahead of the growth ladder would save on a >32-entry map — real, but 90 ns on an event
that has already paid at least one allocation regardless.

#### `move_value::ptr_move` (per-move, ns), by candidate `size_of::<Event>()`

| N (bytes) | 496 | 672 | 864 (today) | 1056 | 1248 | 1632 |
|---|--:|--:|--:|--:|--:|--:|
| ns/move | 9.69 (±0.8%) | 12.55 (±2.5%) | 16.19 (±1.0%) | 19.16 (±2.1%) | 23.22 (±0.5%) | 30.42 (±1.0%) |

Least-squares slope across all six points: 0.0183 ns/byte, i.e. **~7.0 ns per +384 B** — the ADR's
"~7 ns more per +384 B" figure, recomputed directly rather than read off two endpoints.

#### `move_value::vec_push_pop` (per-move, ns, through safe `Vec` push/pop rather than a raw `ptr::copy`)

| N (bytes) | 496 | 672 | 864 | 1056 | 1248 | 1632 |
|---|--:|--:|--:|--:|--:|--:|
| ns/move | 20.73 (±43.1%) | 25.81 (±1.2%) | 29.88 (±1.2%) | 32.23 (±5.2%) | 35.20 (±2.7%) | 44.91 (±4.2%) |

The 496-byte row's ±43.1% spread is the widest in this whole document's micro-bench tables — one of
the three runs read roughly half the other two (noise, not a real regime change; the other five
`N` values in the same bench are all under 5.2%).

#### `scan::touch_head`/`drop_batch`/`clone_batch` (per-element, ns, 1000-element batches, 8 batches rotated so the working set exceeds L2)

| N (bytes) | 496 | 672 | 864 | 1056 | 1248 | 1632 |
|---|--:|--:|--:|--:|--:|--:|
| `touch_head` (1 cache line/element) | 1.962 (±7.8%) | 1.281 (±2.4%) | 1.281 (±1.2%) | 1.251 (±2.4%) | 1.331 (±1.5%) | 1.266 (±1.2%) |
| `drop_batch` | 0.1374 (±2.0%) | 0.1342 (±2.6%) | 0.1412 (±12.5%) | 0.1402 (±36.3%) | 0.1412 (±31.0%) | 0.1446 (±6.5%) |
| `clone_batch` | 19.08 (±96.1%) | 14.30 (±1.3%) | 18.56 (±0.2%) | 23.27 (±0.6%) | 27.72 (±0.5%) | 37.21 (±7.0%) |

`touch_head` is flat from 672 B upward (1.25–1.33 ns) — the "a 1000-event batch scan is flat in
`size_of::<Event>()`" claim, with the 496-byte row (1.96 ns) the one exception, itself likely an
artifact of that size's batch fitting differently against cache-line/page boundaries rather than a
real trend reversing direction. `drop_batch` recomputes to the ADR's "~0.14 ns per event" almost
exactly (0.134–0.145 ns across every width). **`clone_batch::496` is this document's least trustworthy
single cell**: ±96.1% spread, its three raw runs were 19,880 / 19,080 / 10,140 ns — one run read
essentially half the other two. Excluding it, a least-squares fit over 672→1632 gives **9.19 ns per
+384 B**, matching the ADR's "cloning a batch ~9 ns more per event per +384 B"; *including* it pulls
the six-point fit down to ~7.1 ns/384 B. The ADR's figure holds, but only once this one cell's noise
is set aside rather than averaged in.

#### `build_shape` — `AttrMap`'s sorted `insert_sym` vs. an append-then-sort mirror, ns/build

| Width | `sorted_insert` (today) | `append_then_sort` (reserved) | `append_then_sort_unreserved` | `bulk_build` | `bulk_build_onto_populated` |
|---:|--:|--:|--:|--:|--:|
| 9 | 169.3 (±1.1%) | 102.3 (±1.0%) | 137.4 (±2.4%) | 186.2 (±2.4%) | 217.6 (±3.5%) |
| 12 | 226.9 (±1.7%) | 133.6 (±1.4%) | 168.1 (±3.4%) | 236.2 (±1.6%) | 276.4 (±2.3%) |
| 17 | 345.2 (±8.0%) | 196.9 (±13.8%) | 257.6 (±5.5%) | 331.4 (±7.8%) | 379.1 (±4.8%) |
| 30 | 695.7 (±4.7%) | 407.9 (±1.8%) | 469.2 (±2.4%) | 617.9 (±1.6%) | 640.4 (±1.2%) |

**These numbers must be read with the caveat the ADR's Decision (2) states explicitly: every key
sequence here is fed in a fixed, deliberately *unsorted* order** (`build_shape::keys`'s own doc
comment — "a shuffled order is the realistic one"). The pipeline scenarios in (a)–(d) show the
opposite is true for a real source with a stable key order (the interner numbers keys in first-seen
order, so a logging library's keys arrive already ascending by `Symbol`): `sorted_insert`'s binary
search then lands at the end on every insert and nothing moves, which is why `append_then_sort`
"wins" this micro-bench by 32–46% at every width but the *pipeline* json scenarios in (c) show the
untouched loop matching `main` to within ±2%. Treat this table as a worst-case-ordering measurement
of the two build strategies, not as a prediction of pipeline cost — the gap it shows does not appear
in (a)–(d)'s real traffic.

#### `attr_clone::clone` — a real `AttrMap`'s own clone, `Value::I64` throughout (ns/clone)

| Width | 8 (inline) | 9 (first spill) | 12 |
|---|--:|--:|--:|
| ns | 140.5 (±2.3%) | 168.1 (±4.3%) | 206.9 (±1.9%) |

The 8→9 step (140.5 → 168.1 ns, +19.6%) is the one allocation the spill costs; every width past it
is the same one allocation, wider.

#### Arm K's pre-registered kill criterion — build + clone of the 12-attribute log, ns (K needed ≥10% under arm P's bulk build to survive)

| Arm | ns |
|---|--:|
| `bulk_build_and_clone` (arm P) | 410.3 (±3.7%) |
| `sorted_insert_and_clone` (today) | 592.9 (±7.7%) |
| `keyset_build_and_clone` (arm K) | 333.9 (±4.3%) |

Arm K beats arm P by 18.6% here — clears the kill criterion — which is the number the ADR's
Alternatives section rounds to "19%."

#### Arm K — build cost by shape, ns (`log12`=12 attr, `access30`=30, `nested10`=10 top-level)

| Build path | `log12` | `access30` | `nested10` |
|---|--:|--:|--:|
| `sorted_insert` (today) | 328.9 (±27.5%) | 960.7 (±12.2%) | 273.9 (±31.8%) |
| `bulk_sort` (arm P) | 261.3 (±0.5%) | 790.7 (±3.7%) | 213.7 (±4.4%) |
| `keyset_hit` (arm K, cache hit) | 172.4 (±3.0%) | 375.3 (±4.3%) | 151.2 (±3.4%) |
| `keyset_miss` (arm K, cache miss) | 210.6 (±3.0%) | 590.7 (±2.8%) | 183.1 (±3.4%) |

`sorted_insert`'s own spreads here (±27.5%, ±31.8%) are this table's own reminder that the *shuffled*
key order this whole `attr_arms` suite (like `build_shape` above) constructs its scratch inputs from
makes even the control noisy — a real source's ascending-`Symbol` order does not reproduce this in
the pipeline tables above.

#### Arm K — clone cost by shape, ns

| Clone path | `log12` | `access30` | `nested10` |
|---|--:|--:|--:|
| `attrmap` (today) | 235.6 (±3.7%) | 555.4 (±2.3%) | 198.7 (±3.1%) |
| `keyset` (arm K) | 143.1 (±0.5%) | 345.2 (±0.3%) | 118.9 (±1.6%) |

#### Arm K — the adversarial gateway (196 distinct key-sets, top-1 9.5%, top-5 36.1%), ns/event

| Path | ns |
|---|--:|
| `bulk_sort` (arm P, no cache to miss) | 273.8 (±1.1%) |
| `keyset_cache_64` (arm K, 64-entry bounded cache) | 432.9 (±0.3%) |
| `keyset_cache_unbounded` (arm K, room for every shape) | 193.1 (±3.0%) |

A bounded cache sized for this survey's own key-set counts *loses* to the bulk build by 58.1% here
(the ADR's "58%") — the unbounded ceiling (193.1 ns, a 29% win over the bulk build) shows the cache
itself, not the representation, is what a mixed-gateway workload needs sized correctly; 64 entries
against 196 real shapes evicts too often to pay for its own hash-and-lookup overhead.

#### Arm E — nested `Value::Map`, boxed `AttrMap` ("today") vs. an inline thin map, ns

| Shape | `build_today` | `build_thin` | `clone_today` | `clone_thin` | `drop_today` | `drop_thin` |
|---|--:|--:|--:|--:|--:|--:|
| 1 nested map | 588.1 (±1.3%) | 548.2 (±18.5%) | 266.9 (±7.7%) | 128.0 (±2.0%) | 51.2 (±32.5%) | 28.4 (±0.6%) |
| 4 nested maps | 1081.0 (±2.4%) | 981.2 (±17.1%) | 570.7 (±2.2%) | 251.3 (±1.0%) | 104.4 (±13.2%) | 59.4 (±0.5%) |
| pino-http (4 maps, the measured shape) | 1026.0 (±2.0%) | 915.9 (±17.8%) | 539.3 (±5.3%) | 235.7 (±0.8%) | 108.0 (±12.5%) | 59.5 (±0.8%) |

The thin representation clones the pino-http record 56.3% faster (539.3 → 235.7 ns — the ADR's
"56%") and drops it 45% faster; its own build numbers carry noticeably more spread (17–19%) than
"today"'s (1–2%), one of this table's own open questions rather than a settled reading.

#### Arm E — `Scope`/`Resource`-width embedding, build and clone, ns

| Width | `build_attrmap` | `build_thin` | `clone_attrmap` | `clone_thin` |
|---:|--:|--:|--:|--:|
| 0 (`Scope`'s own median) | 21.5 (±42.8%) | 4.4 (±3.2%) | 14.7 (±63.7%) | 4.1 (±0.4%) |
| 5 (a `Resource` outside a collector) | 115.3 (±46.4%) | 102.3 (±3.6%) | 101.7 (±13.1%) | 44.0 (±3.2%) |
| 17 (the measured median behind a collector) | 446.6 (±26.1%) | 357.8 (±5.5%) | 336.4 (±7.8%) | 162.4 (±2.7%) |
| 29 (the measured maximum) | 850.9 (±13.7%) | 728.4 (±0.9%) | 533.1 (±5.7%) | 290.2 (±0.5%) |

**`Resource`/`Scope` are `Arc`-shared and typically not cloned per event** — this table (and the
"future investigations" it feeds, the ADR's `SeriesKey`-embedded-`AttrMap` item) is a build/clone
cost that most pipelines pay once per batch, not once per event; the 0-width row's ±42.8%/±63.7%
spreads are close-to-zero-cost noise (single-digit-to-low-double-digit-ns absolute values), not a
real finding.

#### Arm C — clone candidates by value mix, at the 12-attribute log width, ns/clone

| Candidate | scalar | 75% str | 100% str |
|---|--:|--:|--:|
| `baseline` (today's shipped clone) | 205.6 (±12.5%) | 233.8 (±5.9%) | 243.2 (±5.5%) |
| `exact_loop` | 233.2 (±7.4%) | 250.1 (±5.3%) | 259.5 (±4.9%) |
| `scalar_branch` | 160.6 (±10.0%) | 234.4 (±2.4%) | 262.6 (±3.4%) |
| `detect_pod` | 67.6 (±13.8%) | 241.3 (±3.7%) | 257.6 (±3.4%) |
| `pod_flag` | 68.6 (±2.3%) | 231.3 (±1.3%) | 255.1 (±1.0%) |

At the all-scalar mix, `detect_pod`/`pod_flag` are a real win (67–69 ns against baseline's 205.6 —
a bitwise-copy fast path only legal when every value is scalar), but that mix is not the survey's
own measured band (0–100% string share, median toward the middle); at 75%/100% string share — where
the survey's own logs actually sit — every candidate is within a few percent of `baseline`, several
of them (`pod_flag` at 75%) marginally *under* it but inside `baseline`'s own ±5.9% spread. This is
the recomputed basis for the ADR's "no clone candidate won at realistic string shares."

### Caveats this data itself surfaces

- **Every spread past ~10% above is called out at its own row**, not folded into a single "noise"
  disclaimer — `pair_cross_thread` (every size but 1536), `vec_push_pop::496` (±43.1%),
  `clone_batch::496` (±96.1%), `sorted_insert`'s per-shape numbers in `keyset_k::build` (±12–32%),
  and several `embed_e` rows (`build_attrmap` at width 0/5, ±43–46%) are all real measurements, not
  typos, and none should be read as more precise than its own spread allows.
- **`sizing-w4`'s micro-bench `build_shape` groups (and `attr_arms`' `keyset_k::build`/
  `kill_criterion` scratch inputs) feed keys in a fixed, deliberately unsorted order** — confirmed
  from `build_shape::keys`'s own doc comment in `crates/logit-bench/benches/size_vs_alloc.rs`. The
  pipeline tables in (a)–(d) are what showed this is unrepresentative: a real source's keys arrive
  in the process interner's first-seen order (ascending `Symbol`, confirmed against
  `crates/logit-core/src/interner.rs`), which is the best case for today's sorted `insert_sym`, not
  the shuffled case these micro-benches construct. Any micro-bench number above that looks like a
  clean win for an alternative build strategy should be read against this gap before it is trusted.
- **The Azure guest exposes no virtualized PMU** (`perf stat -e cycles true` returns `<not
  supported>`, same as every other VM session in this document) — nothing here is cycle-accurate or
  branch-mispredict-attributed; every number is wall/CPU time from `wait4` or divan's own timer, and
  "why early reservation loses to late growth" (the ADR's own open question) has no counter-level
  answer available on this box.
- **The `8–17%` figure in the ADR's Decision (2)** ("the bulk build cost the json scenarios 8–17%
  end to end") recomputes, against `9a3ebc3ba294` — the fast-path bulk build, the one actually
  compared throughout (c) and (d) — to **8.2–16.1%** (table (c)'s Table 1: +9.8%, +12.0%, +8.2%,
  +16.1%). The upper bound is ~1 point over what this session's own data supports; 16% is the
  recomputed ceiling, not 17%.

### The three `json-parse-*` scenarios' first real run

`json-parse-app-log`, `json-parse-nested-log`, and `json-parse-access-log` shipped 2026-09-21 with
counts that were first estimates, scaled off `json-parse`'s by key count, explicitly never run on
the reference VM (§1 above, before this session). This session ran all three, on `main`, as part of
every table above. Their actual `wall_s` (median of 6, `main` binary):

| Scenario | Count | Median wall_s | 5–10 s band? |
|---|--:|--:|---|
| `json-parse-app-log` | 12M | 10.9 s | No — overshoots by ~1 s |
| `json-parse-nested-log` | 8M | 13.9 s | No — overshoots by ~4 s |
| `json-parse-access-log` | 5M | 12.4 s | No — overshoots by ~2.4 s |

None of the three landed in the target band on `main` at the counts this session ran them at; all
three ran long, `json-parse-nested-log` most of all. **Their shipped counts have since been lowered**
— 12M → 9M, 8M → 4.5M, 5M → 3M, each scaled to ~8 s from the wall time above, and not yet re-run at
the new value (CPU µs/event, which every table in this section reports, does not depend on the
count). The first estimates were too generous across the board and wanted lowering, not just
retuning in either direction — a first real data point for the "the first session
that runs them should expect to retune them" line these scenarios' own YAML and
`docs/plans/load-test-harness.md` already carried.

## Open questions

- **When and how this harness runs in the ongoing development process is still deliberately
  undecided** — [ADR `load-test-harness`](../adr/load-test-harness.md)'s own open question. Nightly,
  manually triggered, gating a PR on `compare --threshold`, or some other cadence is real future
  work; nothing here assumes an answer, and nothing wires the harness into CI, a pre-merge gate, or
  a schedule yet.
- **`buffered`'s variance is resolved**: the spool-accumulation mechanism §3 identified is fixed on
  the harness side (W8, #165 — every spawn clears a scenario's declared `buffer.disk.path` first),
  and both a quiet-laptop and a reference-VM `--repeat 5` confirmation (§3) show the same no-decay,
  flat-RSS signature. The VM pass narrowed the remaining spread further still (7% events/s spread,
  under 1% on CPU µs/event) — what's left is product-side, not harness-side, and smaller than the
  laptop ever suggested: `DiskQueue::open`'s double-read startup scan (`docs/known-gaps.md`'s
  `buffered` entry) is no longer the prime suspect for anything, just an unquantified detail.
- **`compare`'s lack of a variance-aware threshold is still real future work in principle, but its
  motivating case is gone.** `aggregate`'s laptop-era ~±25% repeat-to-repeat spread — the reason
  this item was opened — does not reproduce on the reference VM (§1's noise sub-section: two
  independent 5-repeat samples agree to within 0.7%). Gating on each file's `min` instead of
  median, or a per-scenario threshold, remain candidate improvements if a future scenario turns out
  to need them, but nothing currently in the suite does (`docs/known-gaps.md`'s harness entry).
- **Templated metric names permanently grow the process-wide interner**, one entry per distinct
  rendering, for the life of the process (`generate_in`'s own module doc; `docs/design/memory.md`
  §4). `generate_in` already refuses a bare `{seq}` there for exactly this reason, and every shipped
  scenario that wants cardinality templates an *attribute* instead (`aggregate.yaml`'s `host:
  h{seq%1000}`, not a metric name) — so nothing here actually exercises the metric-name path at
  scale. Whether that path is worth keeping at all, given every real scenario avoids it, is an open
  question rather than a decision made here.
