# Performance: the load-test harness's recorded numbers

This is the hand-curated record of what the out-of-CI load-test harness measured
([ADR `load-test-harness`](../adr/load-test-harness.md),
[`docs/plans/load-test-harness.md`](../plans/load-test-harness.md)). The harness spawns the real,
release-profile `logit run <config>` process against `perf/scenarios/*.yaml` and measures
throughput, CPU cost, and peak RSS: the end-to-end question [`memory.md`](memory.md) §7 left open.
`perf/results/` is gitignored, per-machine JSON; this file is where a run's numbers get written
down, the same convention `memory.md`'s tables follow.

To reproduce any number here:

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

To measure several sources in one session on the disposable perf VM
(`docs/adr/disposable-azure-perf-vm.md`), run `script/vm build <ref\|dir\|tarball>...`. It stashes
one binary per source under `perf/bins/<slug>/logit`, and `--logit-bin` points a run at one of them
without building anything. The results file is then named after the *binary's* identity, not the
checkout's; see the ADR's "Multiple sources, one VM" section.

> **Unless a sub-section says otherwise, every number here was taken on the disposable perf VM**
> (`docs/adr/disposable-azure-perf-vm.md`), not the dev laptop: `Standard_F8as_v6`, 8 dedicated
> AMD EPYC 9V74 (Genoa) cores, SMT off, 32 GiB, Debian GNU/Linux 13 (trixie), kernel
> `6.12.111+deb13-cloud-amd64`, image version `0.20261001.2618`, `westus2`, inside the dev container
> (`rustc 1.98.1 (48a229cea 2026-09-01)`), `release` profile, on 2026-10-02. The measured binary
> ("M") is `main` at `d1521c5f90078371f2e4247fdcf6c42698140a59`. The harness ran from
> `ccd76db2535eedff69cd2a9c0652e8dd15d54fdf` (`--harness-ref perf/boxstate-fields`), so every
> results file records `box_state`; the `tail` phase ran from `34bf7ec66f155aa5440f115e8f6536bb25228c0a`,
> which predates those fields, so its files carry an empty `box_state`. THP was `always` with
> `defrag` `madvise`, `net.core.rmem_max` 16777216, and `rmem_default` 212992. The `udp-verify`,
> `udp-knee`, `groups-sweep`, and `tail` phases ran while an orphaned harness sampler polled
> `/proc`; §7 says what that touches.
>
> - **The checkout was clean, and every binary came from `script/vm build`.** M's sha256 is
>   `bfb9e359a9bcedcdf8b631925beb3d1a82297f550fa90e00be4bd4d8bacdc9d0`. The previous baseline,
>   `4e08c49ef421` ("B0"), was rebuilt on this box; its sha256
>   `95b811af37e42061cfa938fe9a0cfa093475a01de1428292909a558d6644df63` matches the 2026-09-28
>   sidecar. Every run pointed `--logit-bin` at a stashed binary, and all 227 results files carry
>   `git.dirty: false`.
> - **B0 reproduced the 2026-09-28 numbers before anything else ran.** The `repro` gate measured
>   `passthrough` at 0.329 µs/event (recorded 0.327), `json-parse` at 0.917 (0.920), and
>   `native-relay` at 1.389 (1.389), each inside its spread, so B0 and M share a box class and the
>   comparison below is B0 against M from this session.
> - **Machine facts are restated here, not trusted from the JSON.** The JSON's `hostname` is the dev
>   container's hostname inside the VM, and `cpu_model`/`nproc` are read from `/proc/cpuinfo`/`nproc`
>   *inside* that container.
> - **CPU µs/event, not events/s, is the regression gate.** `compare` gates on it because even a
>   dedicated VM shares last-level cache and memory bandwidth with other tenants on the physical
>   host (the ADR's "Consequences" section), and it warns on a host/CPU-model mismatch between two
>   results files.
> - **The laptop is retired as a reference.** It was a Fedora/Ryzen laptop, and this document carried
>   its numbers through 2026-09-14. Its heterogeneous Zen 5/5c cores made an unpinned run
>   bimodal by roughly 2×, and the same commit measured 90 minutes apart on battery drifted CPU
>   µs/event by ~23% (the ADR's "Context" section).
> - **The code changed since the previous VM baseline; the box class did not.** The merges between
>   2026-09-28 (`4e08c49ef421`) and `d1521c5f` that touch measured paths include the render
>   redesign (#435), the process sampler (#441), the sink send-path stack (#457), the `tailbk`
>   stack (#447), the delivery-semantics stack, the send window (flow/w3 and w4), the native-hop
>   encode-once change (#478), and named cumulative acks (#487). §1's "What moved since
>   2026-09-28" sub-section says which rows moved and, where known, why. Read any other row as
>   *current*.

The two retired laptop runs this table carried before: 2026-09-14 on a quiet, battery-powered
laptop (`fecbd9337010f95d722e89946e1a3e3aa43c007b`,
`perf/results/20260914T012218Z-fecbd9337010-quiet.json`), and 2026-09-13 on a busy, contended
laptop (`c75399d8bccc`, `perf/results/20260913T104956Z-c75399d8bccc-recorded.json`), 14–27% slower
in events/s than the quiet run, scenario for scenario. Compare against the VM's own run-to-run drift
and controls (below), not against either laptop run. `buffered`'s before/after is §3.

## 0. What this measures, and what it doesn't

Each scenario reports four numbers per repeat, from `wait4`'s rusage and two log lines the process
emits under `--log-format json`: its readiness line (`tracing::info!(target: "logit", "ready")`,
logged once the bind pass has opened every listener's socket and every node is spawned,
`crates/logit-pipeline/src/runtime.rs`) and its generator's `generation complete` line.

`wall_s` runs **from `ready` to completion**, not from spawn. Process bring-up (loading the binary,
binding sockets, spawning nodes) happens before `generate_in` sends its first event, so counting it
would inflate the graph's per-event cost. If a repeat's `ready` line never arrives (a binary built
without it, or an unexpected race), `wall_s` falls back to spawn → completion and `crate::run`
prints a warning to that repeat's stderr rather than silently reporting a startup-inflated number.

- **events/s**: `count / wall_seconds`. A rough throughput sense, and the noisiest of the four on a
  shared box (see the preamble). Not the regression gate.
- **CPU µs/event**: `(ru_utime + ru_stime) * 1e6 / count`. **The headline signal.** The process's
  own CPU time doesn't depend on what else the box was doing, which is why
  `script/perf compare --threshold` gates on it.
- **peak RSS**: `ru_maxrss` (kibibytes on Linux, converted to bytes). Reported for every scenario
  and most informative for `buffered` (real segment-file I/O and buffering). It gates `compare`'s
  exit code only when you pass `--rss-threshold`. **In a short run, roughly half of this number is
  jemalloc's freed-but-not-yet-purged pages**; see §1's "Peak RSS" sub-section for how to read a
  scenario's RSS against its live data.
- **startup_s**: spawn → `ready`, a column in `run`'s table and a field next to `wall_s` in the
  results JSON. `null` in the JSON (`n/a` in the table) for a repeat whose `ready` line never
  arrived. `compare` *warns* on a startup regression past `--threshold` but never fails on one,
  because bring-up is a different question from per-event cost.

**What it doesn't measure:**

- **Allocation counts.** `crates/logit-bench/tests/allocations.rs` owns those: exact,
  machine-independent, thread-local counting via `CountingAlloc`, pinned in CI. `wait4` sees
  nothing per allocation. See `memory.md` for what allocates.
- **Correctness.** `script/test`/`script/validate` own that. A scenario that never reaches
  `generation complete` fails the harness loudly, but one that runs and produces wrong output passes.
- **Tail latency.** Every number is a whole-run aggregate (total wall time, total CPU time).
  `attribute`'s per-node `process.duration`/`send.blocked.duration` sums come closest, and they are
  still sums, not a distribution.
- **Full isolation from the host.** `script/vm` (`docs/adr/disposable-azure-perf-vm.md`) provisions
  a disposable Azure VM with eight homogeneous cores and nothing else of ours running, captioned by
  its `~/logit-vm-metadata.txt` (CPU model, kernel, image version, sysctls). It still shares
  last-level cache and memory bandwidth with other tenants; host maintenance can briefly freeze the
  guest (inflating wall-clock numbers, not CPU-time ones); and it has no virtualized PMU, so
  `flamegraph` there is a `cpu-clock` profile, not `cycles` (§5). The ADR's "Consequences" section
  has the full account.

### Driven scenarios: `udp-statsd*` is measured differently, on purpose

The `udp-statsd`/`-small`/`-packed` family ([ADR
`udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md),
added 2026-09-18) has **no `generate_in`**. `logit-perf` itself sends real UDP datagrams over loopback, from a traffic
model in [`perf/load/`](../../perf/load/README.md) calibrated against a real DogStatsD/statsd client
capture. Read its numbers differently from every other scenario's:

- **The denominator is events *delivered*, never events sent.** A real socket can drop datagrams in
  the kernel before `logit` sees them, and the baseline is deliberately tuned so a small fraction
  does. `events_per_s` and `cpu_us_per_event` are computed over `logit.component.events.received`
  at the sink; dividing by events sent would understate per-event cost by exactly the drop rate.
  The results JSON's `udp:` block carries sent / received / read syscalls / kernel-dropped /
  queue-dropped / delivered, and `run`'s table prints it, including a `fill` column
  (`received / reads`): the mean number of datagrams one `recvmmsg(2)` call returned. `fill` is the
  only way to tell whether `receive.read_batch` constrains a workload or is irrelevant to it (ADR
  `udp-intake-batching-and-socket-visibility`).
- **The telemetry leg runs inside the measured process.** The delivered count exists only in the
  child's self-telemetry, so `run` attaches the same `internal → file_out format: native` leg
  `attribute` uses, and `wait4`'s rusage includes those two harness nodes. **A driven scenario's
  absolute CPU µs/event is therefore comparable only to its own history**, never to a generated
  scenario's, which pays neither that leg nor loopback UDP's kernel-side cost. Read relative
  movement, not a cross-scenario ranking.
- **Pinning is required.** `--pin-sender`/`--pin-child` (`sched_setaffinity`, applied to the child
  between `fork` and `exec` so every thread it creates inherits the mask) put the load sender and
  the measured `logit` on distinct physical cores, so they don't contend for one core's time and
  inflate both sides together. Every recorded `udp-statsd*` number states its CPUs
  (`--pin-sender 0,1 --pin-child 2,3` throughout).
- **A delta is a pair taken in one sitting, interleaved, on a box in a known state.** Parent and
  branch runs alternate within one session and are never diffed against a stored file from another
  day. On the old laptop, the same specs, commit, and pins measured 90 minutes apart moved CPU
  µs/event ~23% and the drop rate from 3.1% to 12.4%; re-running the earlier commit straight
  afterwards reproduced the later numbers, so the box moved, not the code. The VM still shares
  last-level cache and memory bandwidth with other tenants, and a new `script/vm up` may land on
  different physical hardware. [`perf/load/README.md`](../../perf/load/README.md)'s "Box state" has
  the checklist. `run` records what it can into the results file's `box_state`. On the VM that holds
  THP, `rmem`, and CPU-topology facts but no governor or power fields: the guest exposes no
  `cpufreq`/`power_supply` sysfs. Tables here are labelled by
  session, not presented as one series across days.
- **Every run self-checks before its numbers count.** `sent == received + kernel-dropped` must
  close exactly (on loopback a datagram has nowhere else to go). The kernel socket sampler must have
  reported at all, because without `getsockopt(SO_MEMINFO)` the drop count is unknowable and reads
  as a flat zero. The listener must report no decode diagnostics, or the scenario would be
  benchmarking the malformed-line path, which is *faster* than the real one. `--verify` adds the
  strict form: `--rate-scale 0.25` plus an exactly-equal delivered event count with zero drops.
- **`--rate-scale` moves the operating point without editing a spec.** The shipped rates sit just
  above the drop knee, which suits a baseline but not a stable CPU µs/event reading; for that, use
  `--rate-scale 0.5`. The effective rate is recorded in the results file and shown in `run`'s
  driven table, and `compare` warns when two runs used different ones: they are different points on
  the load curve, not a before and after. With `--verify`, `--rate-scale` replaces the 0.25 derate
  but **not** the exact-delivery assertion, so `--verify --rate-scale 1.0` asks "is this spec's
  shipped rate loss-free?" Because the rates are tuned to drop a little, the healthy answer is a
  failure.

### File scenarios: `tail*` ends on a delivered count, not a log line

`tail` and `tail-rotate` (added 2026-10-02) measure `tail_in`'s read path: read, split, one raw
event per line, batch, deliver to `null_out`. Like the UDP family they have no `generate_in`.
`logit-perf` writes the tailed file before the spawn, from a slog-style JSON app-log model in
[`perf/load/`](../../perf/load/README.md) (lines of 273–330 bytes: 12M, ~3.5 GiB, for `tail`;
5M, ~1.5 GiB, for `tail-rotate`), and `tail_in` reads it from its first byte. A repeat needs
twice its file size free on the disk under `perf/results/`. `tail-rotate` splits the same lines across a file and its
replacement, and moves the old file aside and the replacement onto its path once the sink has
half of the first file. Read their numbers this way:

- **`wall_s` ends when the sink's delivered count reaches the line count.** There's no
  `generation complete` line and no sender to return. `run` attaches the `udp-statsd*` family's
  telemetry leg at a 100 ms drain interval and follows the dump while the child runs, so `wall_s`
  and events/s are resolved to about one drain, ~1–2% of a 5–10 s run. CPU µs/event comes from
  `wait4` over the whole process and has no such error.
- **The denominator is events delivered**, read from the same leg, and the telemetry leg's cost is
  in the child's rusage, as for `udp-statsd*`. Compare a `tail*` number with its own history only.
- **Every run is exact.** A file loses nothing, so the sink must receive every line written,
  `tail_in` must count every line split, it must report no diagnostics, and `tail-rotate` must count
  the one rotation the harness made. Any other count fails the repeat.
- **The file is in the page cache.** It's written right before the spawn, so the run measures CPU
  on the read path, not the disk.
- **No pinning requirement.** No sender shares the box with the child; `--pin-child` alone keeps
  the scheduler from moving it.

Their first numbers are in §1, under "`tail_in`: before and after the read-path fix".

## 1. Results: all nineteen scenarios, median of 5

`script/perf run --scenario NAME --repeat 5 --profile release --no-build --logit-bin
perf/bins/d1521c5f9007/logit --label suite_m`, solo on the VM (see the preamble), once per scenario.
B0 ran the same command against `perf/bins/4e08c49ef421/logit` (`--label suite_b0`), interleaved by
scenario with the arm order flipping every other scenario so neither side always ran second. One
throwaway `--repeat 1` of `passthrough` ran first and is excluded. Rows follow `script/perf list`'s
alphabetical order. `count` is each scenario's committed `generate_in.count`. Most counts give the
5–10 s of wall per repeat that `docs/plans/load-test-harness.md` targets; the two `buffered*` rows,
`aggregate-groups`, and `native-relay` (2.93 s) run shorter. **events/s (min–max)** is the spread of
the same five repeats the median came from; the noise sub-section after the readings explains what
a wide range means.

| Scenario | Count | events/s | events/s (min–max) | CPU µs/event | Peak RSS | Wall |
|---|---:|---:|---:|---:|---:|---:|
| `aggregate` | 20M | 3,006,997 | 2,970,278 – 3,009,881 | 0.335 | 69.3 MiB | 6.65 s |
| `aggregate-groups` | 5M | 3,498,087 | 3,451,370 – 3,532,793 | 0.290 | 71.1 MiB | 1.43 s |
| `buffered` | 1.2M | 742,262 | 735,879 – 787,888 | 1.698 | 78.7 MiB | 1.62 s |
| `buffered-small-segments` | 1.2M | 736,586 | 735,713 – 750,003 | 1.727 | 77.8 MiB | 1.63 s |
| `encode-human-devnull` | 8M | 1,513,474 | 1,501,075 – 1,521,123 | 1.027 | 250.3 MiB | 5.29 s |
| `encode-json-devnull` | 8M | 1,597,907 | 1,560,550 – 1,604,111 | 0.995 | 233.3 MiB | 5.01 s |
| `encode-native-devnull` | 10M | 1,722,464 | 1,712,064 – 1,728,582 | 0.950 | 245.6 MiB | 5.81 s |
| `fanout` | 25M | 2,573,405 | 2,568,794 – 2,618,982 | 0.651 | 46.9 MiB | 9.71 s |
| `json-parse` | 19M | 2,108,946 | 2,105,618 – 2,114,011 | 0.907 | 104.4 MiB | 9.01 s |
| `json-parse-access-log` | 3M | 371,524 | 370,693 – 373,742 | 3.251 | 60.9 MiB | 8.07 s |
| `json-parse-app-log` | 9M | 977,086 | 975,037 – 980,292 | 1.448 | 64.1 MiB | 9.21 s |
| `json-parse-nested-log` | 4.5M | 552,240 | 550,413 – 553,232 | 2.322 | 66.7 MiB | 8.15 s |
| `json-parse-x3` | 9.5M | 1,427,964 | 1,416,408 – 1,455,032 | 2.501 | 80.6 MiB | 6.65 s |
| `logfmt-parse` | 9.5M | 1,866,551 | 1,861,846 – 1,877,403 | 0.931 | 73.5 MiB | 5.09 s |
| `lua` | 4M | 560,714 | 555,360 – 564,548 | 2.078 | 67.5 MiB | 7.13 s |
| `native-relay` | 7M | 2,390,117 | 2,365,026 – 2,435,922 | 0.868 | 317.0 MiB | 2.93 s |
| `native-relay-window1` | 7M | 1,201,447 | 1,195,434 – 1,209,318 | 1.186 | 250.7 MiB | 5.83 s |
| `passthrough` | 25M | 3,724,063 | 3,705,277 – 3,761,297 | 0.332 | 67.3 MiB | 6.71 s |
| `route` | 25M | 3,341,299 | 3,323,020 – 3,353,641 | 0.687 | 101.6 MiB | 7.48 s |

`encode-json-devnull` is the first row for `format: json` ([ADR
`stream-json-format`](../adr/stream-json-format.md)), and `native-relay-window1` is `native-relay`
at `window: 1`. B0 can't run either: `format: json` doesn't exist at `4e08c49ef421`, and the
`window:` field arrives with the send window.

On the same box, the render rows read:

| Scenario | Binary | CPU µs/event | events/s | Peak RSS |
|---|---|---:|---:|---:|
| `encode-human-devnull` | `7cd52931` (before the block render) | 1.039 | 1,481,161 | 237.7 MiB |
| `encode-human-devnull` | `af557d90` (the block render, #435) | 1.025 | 1,516,289 | 252.7 MiB |
| `encode-human-devnull` | M | 1.030 | 1,503,156 | 244.8 MiB |
| `encode-json-devnull` | `af557d90` | 0.993 | 1,605,728 | 231.3 MiB |
| `encode-json-devnull` | M | 0.993 | 1,603,613 | 236.7 MiB |

Each cell is the median of six pooled repeats. The block render didn't slow the human encoder: it's
1.4% cheaper than the render before it, and the six-repeat ranges don't overlap (1.034–1.043 before,
1.022–1.033 after). On M, `format: json` costs 3.6% less CPU per event than the human render and
4.5% more than `encode-native-devnull` (0.950 in the suite).

### `tail_in`: before and after the read-path fix

These rows were measured at 5,000,000 lines of 314 B on average (1,503.2 MiB) per repeat for
both scenarios; `tail` now ships at 12M lines (§0), so a rerun of it takes longer. "Before"
is M's own binary (sha256 `bfb9e359…`, built from the scenario branch `38997c80`, which changes
only the harness). "After" is `34bf7ec6` (sha256 `65633d93814e…`), which reuses one read buffer per
`Tailer`, splits lines with `memchr`, and keeps its per-pass `Vec`s. Two passes of five repeats
each, with the before/after order reversed on the second pass, pool into 10 repeats per cell.

| Scenario | Binary | events/s | CPU µs/event (min–max) | Peak RSS | Wall |
|---|---|---:|---:|---:|---:|
| `tail` | before | 1,279,387 | 0.780 (0.771 – 0.787) | 61.5 MiB | 3.91 – 4.01 s |
| `tail` | after | 2,076,121 | 0.478 (0.472 – 0.487) | 62.3 MiB | 2.41 – 2.51 s |
| `tail-rotate` | before | 594,369 | 0.813 (0.808 – 0.828) | 65.1 MiB | 8.31 – 8.52 s |
| `tail-rotate` | after | 733,974 | 0.510 (0.508 – 0.513) | 64.2 MiB | 6.81 – 7.01 s |

CPU µs/event falls 38.7% on `tail` and 37.3% on `tail-rotate`, and the ranges don't overlap. Peak
RSS is flat. `tail`'s wall time, 2.4–4.0 s, was under the 5–10 s target at 5M lines, which is why
its count was raised to 12M; `tail-rotate` is inside the target. The `tail*` self-checks held on every
repeat: each run read all 5,000,000 lines, and the harness counted them through the telemetry leg
([§0](#file-scenarios-tail-ends-on-a-delivered-count-not-a-log-line)). These files carry an empty
`box_state`, as the preamble says.

### What moved since 2026-09-28

B0 (`4e08c49ef421`) and M ran on the same box in one sitting, so the table compares them directly
rather than against the 2026-09-28 file. Seventeen scenarios have a row in both. Δ is CPU
µs/event, `+` = slower; Δ events/s has `+` = faster. Bold marks a delta past `compare --threshold
5`.

| Scenario | B0 | M | Δ CPU | Δ events/s |
|---|---:|---:|---:|---:|
| `aggregate` | 0.326 | 0.335 | +2.47% | −2.34% |
| `aggregate-groups` | 0.282 | 0.290 | +2.70% | −3.01% |
| `buffered` | 1.706 | 1.698 | −0.47% | **−8.91%** |
| `buffered-small-segments` | 1.735 | 1.727 | −0.48% | +1.16% |
| `encode-human-devnull` | 1.035 | 1.027 | −0.69% | +1.00% |
| `encode-native-devnull` | 0.957 | 0.950 | −0.73% | +1.06% |
| `fanout` | 0.642 | 0.651 | +1.34% | −0.69% |
| `json-parse` | 0.915 | 0.907 | −0.88% | −1.52% |
| `json-parse-access-log` | 3.223 | 3.251 | +0.88% | −0.67% |
| `json-parse-app-log` | 1.412 | 1.448 | +2.52% | −2.47% |
| `json-parse-nested-log` | 2.309 | 2.322 | +0.54% | +0.04% |
| `json-parse-x3` | 2.466 | 2.501 | +1.42% | −1.11% |
| `logfmt-parse` | 1.005 | 0.931 | **−7.34%** | **+15.33%** |
| `lua` | 2.119 | 2.078 | −1.95% | +1.63% |
| `native-relay` | 1.380 | 0.868 | **−37.09%** | **+145.10%** |
| `passthrough` | 0.330 | 0.332 | +0.60% | −0.26% |
| `route` | 0.700 | 0.687 | −1.90% | +2.11% |

`script/perf compare --threshold 5` over the pooled files flags one row, `buffered` events/s. It
also warns on `startup_s` (not gated) for `aggregate` +5.3%, `fanout` +9.6%, `json-parse` +9.6%, and
`route` +10.7%. The three bold rows were bisected over the first-parent merges between the two
binaries, each built by `script/vm build`, in two interleaved rounds of `--repeat 3` (median of 6):

| Binary | Merge | `native-relay` | `native-relay-window1` | `json-parse-x3` | `logfmt-parse` |
|---|---|---:|---:|---:|---:|
| B0 | `4e08c49e`, the 2026-09-28 baseline | 1.378 | n/a | 2.467 | 1.005 |
| R- | `7cd52931`, before the render redesign | 1.393 | n/a | 2.247 | 1.003 |
| R | `af557d90`, render redesign (#435) | 1.386 | n/a | 2.248 | 1.003 |
| P | `846547e1`, process sampler (#441) | 1.392 | n/a | 2.264 | 1.006 |
| S | `d47a99b6`, sink send-path stack (#457) | 1.389 | n/a | 2.444 | 0.926 |
| D3 | `d349f9cb`, delivery-semantics W3 | 1.365 | n/a | 2.453 | 0.931 |
| D6 | `958b93e7`, delivery-semantics W4–W6 | 1.350 | n/a | 2.456 | 0.927 |
| F4 | `99615d2c`, send window (flow/w4) | 1.176 | 1.371 | 2.465 | 0.924 |
| N | `844be079`, native-hop encode-once (#478) | 1.030 | 1.241 | 2.484 | 0.931 |
| C- | `efd50c1e`, `main` before named acks | 0.958 | 1.150 | 2.474 | 0.931 |
| M | `d1521c5f`, named acks (#487) | 0.858 | 1.177 | 2.502 | 0.934 |

CPU µs/event, pooled median of six repeats (twelve for `native-relay` on M). The ladder phases ran
separately from the suite, so their cells differ from the table above in the third decimal.

- **`native-relay` −37.1% (1.380 → 0.868; 975,141 → 2,390,117 events/s).** B0 through D6 sit at
  1.350–1.393 µs/event; D3 (−1.7%) and D6 (−1.1%) are the only steps over 1%. Then four steps:
  - **F4, the send window: −12.9%** (1.350 → 1.176). At F4, `window: 1` costs 1.371 µs/event and
    988,553 events/s against `window: 32`'s 1.176 and 1,451,784.
  - **N, encode-once: −12.4%** (1.176 → 1.030). `window: 1` falls with it, −9.5% (1.371 → 1.241).
  - **C-: −7.0%** (1.030 → 0.958), and `window: 1` −7.4% (1.241 → 1.150). The only production
    change between N and C- is `FrameReadError::Malformed { reason, err }`, which runs on the
    error path only, so this step is unattributed. The saving is all user time (0.780 → 0.695
    µs/event at window 32, 0.890 → 0.798 at window 1) and system time is flat, which points at
    codegen. A pinned flamegraph pair of N against C- would confirm it.
  - **M, named acks: −10.4%** (0.958 → 0.858), at window 32 only.
- **`native-relay-window1` reads +2.3% from C- to M** (1.150 → 1.177 µs/event; 1,259,807 →
  1,205,667 events/s), where nothing coalesces at window 1 and the plan expected flat. The six
  repeats on each side don't overlap (1.148–1.155 against 1.172–1.183). It's under the 5% gate and
  stays a note, not a lead. The suite's single M row reads 1.186.
- **`json-parse-x3`: #421 restored it, and #457 put it back, by code layout.** B0 2.467 → R-
  2.247 (−8.9%) is #421's `#[cold]` retry fix. It holds through R and P (2.248, 2.264). Then
  **S rises +8.0% to 2.444** and stays there to M's 2.502, so `json-parse-x3` on M is not the
  restored number; it's B0's. The `cpu-clock` flamegraph pair at P and S (999 Hz, the same two
  binaries) shows half the earlier signature: `insert_sym` moves fully out of line, but
  `parse_object` stacks hold (49.5% → 45.8% of samples). At P, one `AttrMap::insert_sym` frame
  is inlined into `JsonParser::process` (6.94% of samples inclusive, `SmallVec::insert` inlined
  with it) beside an out-of-line call (3.81%). At S every `insert_sym` sample sits in the
  out-of-line call: out-of-line time 3.81% → 16.33%, total `insert_sym` time 10.75% → 16.33%,
  self 9.65% → 14.95%. That is what the profile showed, not what the compiler did. A laptop
  disassembly of release builds of P and `main` (byte-identical to the VM's binaries) finds
  `JsonParser::process`, `parse_object`, the `deserialize_map` visitor, and the `insert_sym`
  copy that `json` calls identical instruction for instruction, with `warn_throttled` and
  `insert_sym` out-of-line calls in both. Only code addresses moved, so the profile's
  inline-frame difference reflects layout and symbolization, not codegen. The layout is the
  cause, and #457 doesn't change the code that runs. Forcing `insert_sym`'s loop onto one
  64-byte line (`-C llvm-args=-align-loops=64`, +4.6% `.text`) didn't recover `json-parse-x3`
  (+1.1%, inside noise, while P stayed −2.4% in the same runs), so the loop's cache-line
  position alone isn't it. The same layout move makes `json-parse-x3` 2–6% slower and
  `logfmt-parse` 6–9% faster depending on clock; those laptop figures are provisional (battery,
  `powersave`). There's no source-level fix.
  [`docs/known-gaps.md`](../known-gaps.md) tracks it.
- **`logfmt-parse` −7.9% at the same merge, S** (1.006 → 0.926; 1,626,122 → 1,879,855 events/s),
  flat after. In the same pair, `Logfmt::process` self time falls 14.99% → 6.42% (inclusive
  57.81% → 52.26%), and `KeyCache::get_or_intern` becomes a separate callee at 4.07%. This is
  the other side of the same layout move: it trades against `json-parse-x3`, and the profile
  differences carry the same caveat.
- **`json-parse` is −0.9% against this B0** (0.915 → 0.907) and also 0.917 → 0.900 in the ladder.
  The earlier "+1.4% residual" against 2026-09-20 can't be read here, because B0 already carries
  it. The X1 flamegraph pair at #298 (`c860842c`) against its parent (`e82ff890`) moves no json
  self-time frame by more than 0.4 points of samples (`insert_sym` self 11.19% → 11.30%, `JsonParser::process`
  inclusive 48.69% → 47.95%), and #298 changes nothing on the parse path. The +2.3% step stays
  unattributed, most likely LTO code layout. `attribute` can't decode those two older binaries'
  dumps ("bad distribution blob: Version"), so the pair has only flamegraph shares, not a per-node
  breakdown.
- **`buffered` events/s −8.9% is noise.** CPU µs/event is 1.695–1.710 on all eleven ladder
  binaries, while B0's own five repeats span 718,192–823,165 events/s (14%) against M's
  735,879–787,888. `compare`'s "regressed" label is that spread.
- **`aggregate*` +2.5–3.7%, under the gate and in one direction.** `aggregate` +2.47%,
  `aggregate-groups` +2.70%, and `json-parse-app-log` +2.52% in the suite. The G sweep below shows
  +3.1% to +3.7% on `aggregate-groups` at every G, outside the repeat ranges. Nothing here
  attributes it.
- **Peak RSS.** `json-parse` reads +11.8% (93.5 → 104.4 MiB) in the suite, but the ladder's pooled
  `json-parse` RSS runs 99.9–109.0 MiB across all eleven binaries with no step, so it's run-to-run
  range. `native-relay` is +34% in the suite (236.1 → 317.0 MiB), while the pooled ladder reads
  B0 235.0, F4 273.4, N 344.3, C- 369.1, M 287.7: wide, and not monotonic. Under purge ("Peak
  RSS" below) M reads 129.8 MiB, the `encode-*` level; with no B0 purge run, the 236.1 → 317.0 MiB
  rise stays unattributed.
- **Everything else is inside ±2%** on CPU, which is inside this box's repeat spread.

### `native-relay` under a 10 ms round trip, and the coalescing sweep

Named acks ([ADR `native-hop-named-acks`](../adr/native-hop-named-acks.md)) make `logit_in` send
one `Ack` per run of up to `ACK_COALESCE_MAX` (32) frames. On the laptop that cost a few points
under latency, so the VM re-ran it. A sidecar container with `NET_ADMIN` ran `tc qdisc add dev lo
root netem delay 5ms` in a network namespace that the perf container joined. `ping` measured an
average RTT of 10.081 ms (min 10.031, max 10.097, mdev 0.011) against 0.033 ms on loopback, so the
ceiling at window 32 is 32 frames per 10.1 ms, 3,174 batches/s of 100 events. The scenario is a
temporary copy of `native-relay` with 3M events and `buffer.max_batches: 64`, so the queue left at
"generation complete" drains inside `--settle 2s`. Arms alternated over two passes of
`--repeat 2` each; C- and M ran in both the first phase and the sweep (8 repeats each), and the
`ACK_COALESCE_MAX` variants of 8 and 16 (M/8, M/16, built from M with only that constant changed)
ran in the sweep (4 each).

| Arm | events/s | batches/s | Share of ceiling | CPU µs/event | Peak RSS |
|---|---:|---:|---:|---:|---:|
| C- (per-frame acks) | 315,961 | 3,160 | 99.5% | 0.975 | 90.9 MiB |
| M (`ACK_COALESCE_MAX` 32) | 308,613 | 3,086 | 97.2% | 0.850 | 86.4 MiB |
| M/8 | 312,588 | 3,126 | 98.5% | 0.870 | 83.2 MiB |
| M/16 | 309,866 | 3,099 | 97.6% | 0.845 | 89.2 MiB |

M is 2.3 points below C-, and every M repeat (307,571–311,090 events/s) is below every C- repeat
(315,938–316,017), so the gap is real. It's smaller than the laptop's 95% against 91%. M costs 12.8%
less CPU per event under latency. Cutting the cap closes little of it: M/8 recovers 1.3 points,
outside M's repeat range (M/8's four repeats, 312,220–312,645 events/s, all sit above M's highest,
311,090), and M/16's +0.4 is inside it.

The reason is in the coalescing ratio, `logit.proto.frames{direction="in"}` per `logit.input.acks`
over three runs per cell (C- predates the `logit.input.acks` counter and reports none):

| Arm | Network | Frames per ack | Per run |
|---|---|---:|---|
| M | loopback | 2.54 | 2.66, 2.52, 2.45 |
| M/8 | loopback | 3.10 | 3.54, 2.80, 3.06 |
| M/16 | loopback | 3.74 | 3.18, 3.94, 4.29 |
| M | 10 ms RTT | 4.07 | 3.95, 4.65, 3.72 |
| M/8 | 10 ms RTT | 3.28 | 3.37, 3.17, 3.32 |
| M/16 | 10 ms RTT | 4.05 | 4.30, 4.13, 3.75 |

An `Ack` covers about four frames under latency and about 2.5 on loopback, so the cap of 32 never
binds, and changing it moves the ratio only slightly. The gap comes from burst handling before the
coalesced `Ack`, not from the cap. On loopback at window 32 the variants don't separate from M
(CPU µs/event 0.858 for M, 0.868 for M/8, 0.838 for M/16, all inside M's 12-repeat range of
0.823–0.882), so the constant needs no change.

### Reading the rows

The readings below cross-reference each `perf/scenarios/*.yaml`'s own comments. **The top-line
numbers are all from the 2026-10-02 session.** The flamegraph shares below come from `cpu-clock`
captures of M (`script/perf flamegraph`, 999 Hz, `--folded` kept for `perf/folded_share.py`), as the
share of samples whose stack contains a frame with the named substring in its name. They replace the
laptop-era shares (2026-09-14, busy laptop), which were not carried forward. `attribute` has run on
the VM for `json-parse`, `aggregate`, and `buffered`, and for `fanout` and `route` (§2).

- **`passthrough`** (0.332 µs/event) is the runtime floor every other scenario is read against:
  scheduling, the `Fanout` channel hop, layer-2 telemetry, and no parsing or encoding. Most of it
  is the generator, not the delivery path: 76.2% of the capture's samples sit under a `generate`
  frame, and `SinkQueue` admission is 0.14%.
- **`fanout`** (0.651 µs/event) and **`route`** (0.687 µs/event) generate `passthrough`'s exact
  event at its exact count (25M, shared by design so the three differ only in topology), and each
  costs more per generated event: `fanout` for two extra sinks' `Arc`-clone-plus-delivery-hop cost,
  `route` for the router's per-event work (`route_batch`'s `AttrMap::get_sym` plus a linear scan of
  alternatives) on top of a similar sink-hop cost. `fanout` < `route` here, as on the laptop
  (fan-out is cheaper per delivery than routing once). The generator's share falls as the graph
  grows: 57.7% of `fanout`'s samples and 41.8% of `route`'s sit under a `generate` frame, and
  `route_batch` is 15.8% of `route`. `SinkQueue` admission is 0.05% and 0.11%.
- **`json-parse`** (0.907 µs/event) and **`lua`** (2.078 µs/event) are no longer close to tied.
  `json-parse` dropped sharply from the laptop's 2.054 µs/event because of the interner key-cache
  and in-place `Transform::process` work (`docs/adr/in-place-transform-process.md`). `lua`'s LuaJIT
  round trip had no equivalent optimization, so the gap between a `lua` stage and a native
  transform that [`docs/known-gaps.md`](../known-gaps.md#transforms-predicates-and-sampling)
  documents holds by a larger factor now.
  `json-parse-x3` (2.501 µs/event, three parallel parsers sharing the interner) sits above `lua`,
  consistent with its purpose: showing shared-interner contention, not a single parse.
- **`json-parse-app-log`** (1.448 µs/event at 12 attributes), **`json-parse-nested-log`** (2.322 at
  10, four of them nested maps), and **`json-parse-access-log`** (3.251 at 30) are three widths of
  the same `json` parse. Read them against `json-parse` (0.907 at 6). Against §9's 2026-09-24
  `main`, nested-log and access-log are within 1.3% and app-log is +4.4% (1.387 → 1.448).
- **`encode-human-devnull`**, **`encode-json-devnull`**, and **`encode-native-devnull`** (1.027,
  0.995, and 0.950 µs/event): the native encoder is still cheaper than the two text renders on the
  same event stream, as `docs/design/wire-protocol.md` predicts (dictionary-first framing beats
  formatting text), and `format: json` is the cheaper of the two text forms. The laptop showed the
  same human-to-native shape (1.330 vs 1.180 µs/event).
- **`native-relay`** (0.868 µs/event, 2,390,117 events/s) is the full encode → loopback TCP →
  decode → ack round trip in one process at the default `window: 32`, and `native-relay-window1`
  (1.186 µs/event, 1,201,447 events/s) is the same graph with one frame in flight. At window 32 it
  lands below `json-parse` and the three `encode-*-devnull` rows and above `passthrough`, `fanout`,
  and `route`. Read it against its own history, not the `encode-*` rows, which go through a
  different sink path. The window, encode-once, and named acks account for the fall from 1.380
  µs/event at B0, step by step in §1's ladder. At the ladder's F4 binary, `window: 32` costs 1.176
  µs/event and runs 1,451,784 events/s against `window: 1`'s 1.371 and 988,553. Peak RSS at window
  32 is the sink queue's `max_bytes` bound plus allocator retention, as the "Peak RSS" sub-section
  reads it.
- **`aggregate`** (0.335 µs/event) costs nearly as little as `passthrough` despite sketching a
  1000-series distribution on a 1 s flush tick. §2 shows why: almost all of it is one node's
  `DdSketch::add`, and the flush cost is amortized over ~6 ticks (the median repeat took 6.65 s).
  On the laptop it was the noisiest scenario; on the VM its spread stays within a few percent
  (next sub-section).
- **`aggregate-groups`** (0.290 µs/event) runs 1000 `(resource, scope)` groups at 100 events per
  batch. It costs less than `aggregate`'s 1000-series sketching because each event carries one
  gauge into one series per group. This is the row
  [ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s "The groups bound"
  section waited on. It is flat in the group count: temporary copies of the scenario at G = 1, 100,
  and 1000 distinct resources, five repeats each on B0 and M, read

  | Groups | B0 CPU µs/event | M CPU µs/event | B0 peak RSS | M peak RSS |
  |---:|---:|---:|---:|---:|
  | 1 | 0.276 | 0.286 | 53.8 MiB | 56.7 MiB |
  | 100 | 0.278 | 0.288 | 58.1 MiB | 59.6 MiB |
  | 1000 | 0.282 | 0.291 | 71.0 MiB | 71.0 MiB |

  Going from G = 1 to G = 1000 costs +2.2% on B0 and +1.7% on M. M is +3.1% to +3.7% against B0 at
  every G, outside the repeat ranges (B0 0.275–0.287, M 0.284–0.302), which matches the suite's
  `aggregate-groups` +2.70% and `aggregate` +2.47%. The copies lived on the VM only and were
  deleted afterwards, because `perf/scenarios/` is globbed by `validate`.
- **`buffered`** (742,262 events/s): §3 resolved its wide run-to-run swings. They were the harness's
  own un-cleared spool, fixed by W8 (#165). The laptop-era table carried `1,087,248` events/s here, a
  quiet-machine median of three repeats; §3's dedicated VM `--repeat 5` pass is the better read of
  this scenario's steady-state spread. On this session's ladder its CPU µs/event reads 1.695–1.710
  on all eleven binaries while its events/s wanders 717,275–833,615, so read the CPU column.
- **`buffered-small-segments`** (1.727 µs/event, 736,586 events/s) is `buffered` with 1 MiB
  segments, so it rolls about 70 times per run. §3's "Segment rolls" sub-section has the
  before/after.
- **`tail`** (0.478 µs/event, 2,076,121 events/s) and **`tail-rotate`** (0.510, 733,974) are `tail_in`'s read
  path with nothing parsed, read against their own before-column in "`tail_in`: before and after the
  read-path fix" above, not against the generated rows.

### Noise: `aggregate`'s laptop-era spread does not reproduce on the VM

**`aggregate`'s laptop-era spread does not reproduce here.** On the laptop, three solo repeats once
gave 3,894,156 / 2,588,602 / 3,396,968 events/s, roughly ±25% around the median. On the VM, two
independent 5-repeat samples agree to within 0.7%, within and across runs: the 2026-09-20 row's
repeats (3,091,253 / 3,092,232 / 3,087,789 / 3,100,900 / 3,093,129 events/s) and a separate run of
the same scenario about an hour later (3,086,137 / 3,079,798 / 3,100,395 / 3,091,713 / 3,083,023).
The 2026-09-28 row's five repeats spread 3,019,145–3,105,921 (2.9%, from one low repeat).
`compare --threshold 5` would flag nothing. The flush-tick-alignment sensitivity §2 blamed for the
laptop's spread is either much smaller on this box or was swamped by something that made the laptop
worse. The leading suspect is heterogeneous-core scheduling: `aggregate`'s single hot node moving
between a fast and a slow core mid-run would look exactly like this. This is concrete evidence for
retiring the laptop as a reference box, not just a noisier version of the same measurement.

A variance-aware `compare` threshold (gating on each file's `min`, or per-scenario thresholds) is
still possible future work, but its motivating case, `aggregate` tripping `--threshold 5` on its
own noise, no longer happens on the reference box. `docs/known-gaps.md`'s harness entry says the
same.

### Peak RSS: what is live data and what is jemalloc retention

`logit` runs on jemalloc (ADR `jemalloc-global-allocator`), which returns freed pages to the kernel
on a decay schedule (`dirty_decay_ms` = 10 s by default), not at `free`. A 5–10 s scenario therefore
reports a peak RSS that includes most of what it freed along the way, not just its high-water mark
of live data. To separate the two, run the suite twice: once as-is, and once with
`_RJEM_MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0`, which purges at `free` and makes peak RSS a
close proxy for peak live data. The 2026-10-02 session ran both on M, five repeats per scenario:

| Scenario | RSS, default decay | RSS, immediate purge | Δ RSS | CPU µs/event, default | CPU µs/event, purge |
|---|---:|---:|---:|---:|---:|
| `aggregate` | 69.3 MiB | 61.5 MiB | −11.3% | 0.335 | 0.837 |
| `aggregate-groups` | 71.1 MiB | 61.2 MiB | −13.9% | 0.290 | 0.771 |
| `buffered` | 78.7 MiB | 68.3 MiB | −13.1% | 1.698 | 2.686 |
| `buffered-small-segments` | 77.8 MiB | 74.1 MiB | −4.8% | 1.727 | 2.756 |
| `encode-human-devnull` | 250.3 MiB | 129.8 MiB | −48.2% | 1.027 | 1.784 |
| `encode-json-devnull` | 233.3 MiB | 130.3 MiB | −44.1% | 0.995 | 1.542 |
| `encode-native-devnull` | 245.6 MiB | 130.0 MiB | −47.1% | 0.950 | 1.474 |
| `fanout` | 46.9 MiB | 49.2 MiB | +4.8% | 0.651 | 1.185 |
| `json-parse` | 104.4 MiB | 68.0 MiB | −34.9% | 0.907 | 1.885 |
| `json-parse-access-log` | 60.9 MiB | 58.5 MiB | −4.0% | 3.251 | 4.753 |
| `json-parse-app-log` | 64.1 MiB | 55.3 MiB | −13.8% | 1.448 | 2.282 |
| `json-parse-nested-log` | 66.7 MiB | 56.3 MiB | −15.6% | 2.322 | 3.991 |
| `json-parse-x3` | 80.6 MiB | 59.5 MiB | −26.2% | 2.501 | 4.211 |
| `logfmt-parse` | 73.5 MiB | 60.9 MiB | −17.2% | 0.931 | 1.656 |
| `lua` | 67.5 MiB | 58.6 MiB | −13.2% | 2.078 | 3.259 |
| `native-relay` | 317.0 MiB | 129.8 MiB | −59.1% | 0.868 | 2.025 |
| `native-relay-window1` | 250.7 MiB | 130.1 MiB | −48.1% | 1.186 | 2.256 |
| `passthrough` | 67.3 MiB | 54.1 MiB | −19.7% | 0.332 | 0.867 |
| `route` | 101.6 MiB | 55.9 MiB | −45.0% | 0.687 | 2.019 |

Three findings:

- **Where the sink is slower than the generator, RSS is queue depth**, set by the sink queue's
  default `buffer.max_bytes` of 64 MiB. A 100-event batch of this shape weighs ~87 KiB by
  `estimated_heap_bytes`, so the byte bound trips at ~770 batches, well before the 1024-batch bound.
  The five sink-bound scenarios (`encode-*` and both `native-relay` rows) all converge on
  129.8–130.3 MiB under immediate purge: the bound plus the 64-slot inbox and the process
  baseline. In-flight buffering at the default `buffer:` is large for those scenarios, and
  `max_bytes` is the bound doing it, not `max_batches` or the channels. The baseline is about
  45 MiB higher than the laptop's ~85 MiB.
- **Where the sink keeps up, RSS is mostly retention.** `json-parse` falls from 104.4 to 68.0 MiB,
  `route` from 101.6 to 55.9, `json-parse-x3` from 80.6 to 59.5, `passthrough` from 67.3 to 54.1,
  and `lua` from 67.5 to 58.6. `fanout` is the control: it retains nothing (one shared batch, freed
  once), and its RSS doesn't fall (46.9 → 49.2 MiB).
- **`native-relay`'s default-decay rise stays unattributed.** Its suite RSS rose from 236.1 MiB at
  B0 to 317.0 MiB at M. Under purge M reads 129.8 MiB, the `encode-*` level, but the session ran
  no purge on B0, so there is no reading to compare the rise against. `json-parse` is a cleaner
  case, because it compares M with itself: 104.4 MiB at default decay against a purge floor of
  68.0 MiB leaves about 36 MiB of allocator retention.

**The purge took effect this time.** The 2026-09-20 `suite-purge` never did: `script/perf run`'s
`run()` helper (`script/common.sh`) invokes `docker compose run` with only one explicit `-e
LOGIT_DEV_CONTAINER=1`, and `sudo docker compose run` strips the calling shell's environment before
compose interpolates any `${VAR}`, which is also why `LOGIT_PERF_GIT_SHA`/`LOGIT_PERF_GIT_DIRTY`
travel as `env VAR=... cargo run` argv (`crates/logit-perf/src/run.rs`'s `git_info` doc comment).
`_RJEM_MALLOC_CONF` has no such path, so `export _RJEM_MALLOC_CONF=...; script/perf run ...`
measured the default-decay condition twice, and `aggregate` read 0.325 µs/event both times. This
session set the variable with `docker compose run -e _RJEM_MALLOC_CONF=...` around `cargo run -p
logit-perf`, and a sampler read `/proc/<pid>/environ` of the spawned `logit run` to confirm the
variable arrived before any measurement counted: it saw the variable on the preflight
`passthrough` child, and on the last scenario's child (`route`). CPU µs/event rose on all 19 rows,
from +46% (`json-parse-access-log`) to +194% (`route`), and the preflight `passthrough` read
0.864 µs/event against 0.332 at default decay. A purge run whose CPU matches the default run did not
take effect.

**Don't compare immediate-purge CPU numbers to anything else here.** Purging at `free` costs an
`madvise` per page-sized free and more than doubled CPU µs/event for the churn-heavy scenarios
(`passthrough` 0.332 → 0.867, `aggregate` 0.335 → 0.837, `route` 0.687 → 2.019). It is a diagnostic
setting for reading RSS, not a configuration to run with.

## 2. Attribution: where a scenario's time actually goes

`script/perf attribute --scenario NAME` appends a temporary `internal → file_out format: native` leg
to a copy of the scenario, decodes the dump, and groups every point by emitting component; see
[`internal-telemetry.md`](internal-telemetry.md)'s "Reading an attribution dump" section for the
mechanism. Both tables below are from the 2026-09-20 VM run at each scenario's current count.
`__perf_internal`/`__perf_dump` are the harness's own two nodes, shown for transparency and
excluded from the verdict.

A 2026-09-28 pass on `main` ran `attribute` for `fanout` and `route`. `fanout` has only a listener and
three sinks, none of which records process time, so its dump shows none; `gen` spent 0.77 s
blocked in send. `route`'s `split` node is 100% of measured process time: 3.66 s over 25M
events, about 0.146 µs/event, with `gen` blocked 0.48 s in send.

`attribute` on `json-parse` at M (2026-10-02) puts `parsed` (`json`) at 8.4409 s of 10.4534 s Σ
process time (81%), against 79% above. Its output can't be produced for older binaries: a
`logit-perf` built from the current checkout can't decode a binary's native dump from before the
dump format changed (`bad distribution blob: Version`), so the `c860842c`/`e82ff890` pair in §1 has
flamegraph shares only.

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

**`json` now takes most of the time, the reverse of the laptop.** `json` (`parsed`) is 7.6780s of
9.7345s Σ process time (**79%**), `kv_metrics` (`metrics`) 2.0565s (**21%**); the laptop-era table
had 46%/54%. Both got faster, but `kv_metrics` got roughly 5× cheaper (its divan bench alone dropped
from 256 ns to 75 ns, `docs/design/memory.md` §3). Deriving four metrics from a parsed attribute
map got disproportionately cheaper than the JSON parse feeding it, which is what the interner
key-cache and in-place `Transform::process` work
(`docs/adr/in-place-transform-process.md`) targeted.

`gen`'s 5.2125s blocked in send, more than either transform's process time, is not generator
slowness. `generate_in` runs faster than `json`/`kv_metrics` drain it and spends most of the run
backpressured on `Fanout::send`, which `internal-telemetry.md`'s verdict rule reads as "the
constraint is downstream of `gen`." That is the healthy shape for this harness: the generator should
never be a scenario's bottleneck.

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

`windowed` (`aggregate`) is the only real node, so it is **100%** of Σ process time: 4.4193s, down
from 7.7434s on the laptop because the VM is faster per event, not because the code path changed.

- **The `DROPPED 20000000 events (reason=absorbed)` line is expected.** Every input event is
  folded into a per-series `DdSketch` and never re-emitted; `Transform::process` returning `None`
  between flushes is what a stateful aggregator does.
- **Output is 7 flush-tick batches (`batch out`) carrying 7,000 events**: 1000 per tick, one per live series,
  matching `host: h{seq%1000}`'s cardinality. The laptop table showed 12 ticks only because that run
  took longer at the same 1 s interval.
- **`gen` is again mostly blocked in `send` (4.6042s)**, the same backpressure reading as
  `json-parse`: `DdSketch::add` plus the periodic flush is cheap, but `generate_in` is still the
  faster node.

## 3. `buffered`: variance, resolved

**Resolved: the harness never cleared `buffered`'s disk spool between repeats, and every repeat
paid for the accumulated spool at startup.** Since W8 (#165) the harness clears it before every
spawn, and the variance is gone on a busy laptop, a quiet laptop, and the reference VM. The rest of
this section is the evidence, in the order it was gathered.

`buffered` is `passthrough`'s exact graph with `buffer.disk:` turned on
(`docs/adr/disk-backed-sink-buffer.md`). W7a's tuning pass saw its throughput range from roughly 16k
to 790k events/s across repeated runs at the same count, by far the widest spread of any scenario.
A solo 3-repeat run reproduced the shape with nothing else on the machine (626,504 → 535,735 →
309,104 events/s, monotonically falling). A dedicated solo `--repeat 5` pass, run twice, found the
mechanism:

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

Both runs degrade monotonically after their first repeat or two while peak RSS climbs in lockstep:
the same mechanism, from different starting points. **One identified contributor:**
`DiskQueue::open` (`crates/logit-pipeline/src/disk_queue.rs`) pays an un-cleared spool's cost twice
at every startup:

1. It reads and CRC-walks the *active* segment in full to find a torn tail. That is O(segment size)
   but bounded, because the active segment rotates at roughly the default `segment_bytes` (64 MiB);
   on its own it is not obviously large enough to explain a multi-second swing.
2. It then reads every segment at or after the read cursor a *second* time, the active one
   included, to count what's left to replay. This is real work only when the cursor lags the end of
   what's on disk, which is exactly what a spool the harness never clears leaves behind.

So each repeat paid a bounded validation scan *plus* a replay of whatever the last checkpoint hadn't
covered. This reproduces solo, with no contention, which rules out the real-disk-I/O-contention
explanation the scenario's comment carried before. Whether it explains the *entire* spread,
including W7a's 16k–790k range, was never fully established; it is one real contributor.

**The 2026-09-13 recorded table's `535,735` events/s was not a steady-state number.** It was a
median of three back-to-back repeats against a never-cleared spool, and a re-run could have
reported a different value depending on the spool's prior state. It stays here only as the record
of what the *unfixed* harness reported; §1 now carries the post-fix VM number.

**W8 (#165): the harness clears the spool itself.** `script/perf run`/`attribute`/`flamegraph`
remove every `buffer.disk.path` directory a scenario declares before each spawn, every repeat, and
refuse to touch anything outside `perf/results/` (`crates/logit-perf/src/spool.rs`). You no longer
need to delete `perf/results/spool/` by hand before a solo comparison. Re-running `buffered` solo on
the fix, `script/perf run --repeat 5 --profile release --scenario buffered`:

```
repeat 1/5: 884,613 events/s   1.810 µs/event   28.8 MiB peak RSS   0.003s startup
repeat 2/5: 510,016 events/s   2.645 µs/event   30.6 MiB peak RSS   0.003s startup
repeat 3/5: 837,626 events/s   1.929 µs/event   27.0 MiB peak RSS   0.004s startup
repeat 4/5: 862,801 events/s   1.902 µs/event   24.8 MiB peak RSS   0.003s startup
repeat 5/5: 792,157 events/s   2.088 µs/event   26.4 MiB peak RSS   0.004s startup
```

> Taken on a busy machine, with other work running on the host, so this is **indicative only, not
> `buffered`'s steady-state number**. The quiet-machine pass below confirms it.

Even under contention, the fix's target signature is gone:

- **No monotonic decay.** Repeat 2 dips to 510k and repeat 3 recovers to 838k, where both earlier
  sequences (134k→75k→53k→38k→27k and 615k→939k→289k→126k→80k) fell every repeat
  after their peak.
- **Flat RSS.** Peak RSS stays in a 24.8–30.6 MiB band instead of climbing 56→110 MiB or 26→70 MiB,
  the clearest sign the spool no longer accumulates.
- **Startup isn't the cause.** `startup_s` (spawn → `ready`, §0) is 2.6–4.4 ms, not a meaningful
  share of per-event cost; the spool accumulation was.

The remaining spread (roughly 510k–885k, a repeat-2 dip about 40% below the top) looked like
ordinary scheduling noise on a busy box, which needed quiet-machine data to confirm. 837,626
events/s (1.929 µs/event) was the first post-fix data point.

**Quiet-machine confirmation.** A solo `script/perf run --repeat 5 --profile release --scenario
buffered --label quiet`, host idle, on battery, sha `fecbd9337010f95d722e89946e1a3e3aa43c007b`:

```
repeat 1/5: 632,897 events/s   2.626 µs/event   ~26 MiB peak RSS   ~4 ms startup
repeat 2/5: 659,892 events/s   2.531 µs/event   ~28 MiB peak RSS   ~4 ms startup
repeat 3/5: 641,560 events/s   2.568 µs/event   ~27 MiB peak RSS   ~4 ms startup
repeat 4/5: 780,888 events/s   2.044 µs/event   ~25 MiB peak RSS   ~4 ms startup
repeat 5/5: 873,406 events/s   1.842 µs/event   ~28 MiB peak RSS   ~4 ms startup
```

No monotonic decay and no RSS climb (a tight 25–28 MiB band), with nothing else on the box to blame
for the remaining spread. `script/perf attribute --scenario buffered` on the same build shows where
that spread lives: `gen` sent all 1,200,000 events and `out` received all 1,200,000; `gen` spent
1.3959s blocked in `send`, and neither the sink nor the listener shows process time of its own.
`internal-telemetry.md`'s reading rule puts the constraint downstream of `gen`: the disk queue's
own write/read path, not the harness or the box.

**VM confirmation (current numbers).** `script/perf run --repeat 5 --profile release
--label vm-buffered-solo --scenario buffered`, solo, on the VM:

```
repeat 1/5: 745,047 events/s   1.720 us/event   78.7 MiB peak RSS   2.4 ms startup
repeat 2/5: 757,703 events/s   1.710 us/event   79.0 MiB peak RSS   3.4 ms startup
repeat 3/5: 802,478 events/s   1.710 us/event   74.8 MiB peak RSS   2.8 ms startup
repeat 4/5: 780,068 events/s   1.709 us/event   70.9 MiB peak RSS   3.3 ms startup
repeat 5/5: 801,518 events/s   1.708 us/event   70.7 MiB peak RSS   2.8 ms startup
```

The tightest this scenario has measured. Events/s spans 745,047–802,478 (about 7%, against the
quiet laptop's ~38%), peak RSS a narrow 70.7–79.0 MiB band, and CPU µs/event 1.708–1.720 (under 1%).
§1's table carries the full-suite run's median (707,215 events/s, a separate 5-repeat sample
consistent with this one within ordinary noise).

What's left open is narrow: how much `DiskQueue::open`'s bounded active-segment validation scan, still
present, contributes to a *cleared* spool's remaining spread. On this evidence, not much.
`perf/scenarios/buffered.yaml`'s comment and `docs/known-gaps.md`'s entry carry the same account.

### Segment rolls: `buffered-small-segments` (2026-09-24)

**A roll's durable cursor persist cost 16–27% of `buffered-small-segments`'s throughput while it
ran on the sink task. `dur/w8` moves it to a worker thread and recovers all of it.** `dur/w1` made
every spool cursor persist durable (write, fsync, rename, directory fsync:
[ADR `durable-checkpoint-writes-and-fault-injection`](../adr/durable-checkpoint-writes-and-fault-injection.md)),
and `roll_read_cursor` ran it inline, then unlinked the segment the cursor left, on every segment
roll. `main`'s roll was a plain write and rename. At `buffered`'s 64 MiB default a run rolls about
once; `perf/scenarios/buffered-small-segments.yaml` is `buffered` with 1 MiB segments, so it rolls
about 70 times per 1.2M-event run.

**The regression.** `origin/main` (`16dd735`) against `origin/dur/w7` (`f2f4094`),
`script/perf run --profile release`, three passes interleaved by binary on the VM. Pass 1 is
`--repeat 3`; passes 2 and 3 are `--repeat 5`. Each cell is the pass's median.

| Scenario | Pass | `main` events/s | `dur/w7` events/s | Δ events/s | `main` CPU µs/event | `dur/w7` CPU µs/event |
|---|---|---:|---:|---:|---:|---:|
| `buffered` | 1 | 791,362 | 770,616 | −2.6% | 1.693 | 1.727 |
| `buffered` | 2 | 875,213 | 851,602 | −2.7% | 1.692 | 1.730 |
| `buffered` | 3 | 880,437 | 848,812 | −3.6% | 1.692 | 1.729 |
| `buffered-small-segments` | 1 | 687,849 | 578,911 | −15.8% | 1.716 | 1.753 |
| `buffered-small-segments` | 2 | 671,681 | 492,597 | −26.7% | 1.719 | 1.757 |
| `buffered-small-segments` | 3 | 659,760 | 480,663 | −27.1% | 1.715 | 1.756 |

- **`buffered`'s events/s delta is inside noise.** `main`'s own median moved 11% between passes 1
  and 2.
- **`buffered-small-segments` regressed on every pass,** and `compare` flagged each one.
  `script/perf attribute` put the whole delta in `gen`'s time blocked in `send` (+0.63 s): wall
  time waiting on the disk, not CPU. Each roll's persist took 5–10 ms on the VM's Azure Premium disk.
- **CPU µs/event rose about 2% on both scenarios** (1.69 against 1.73 on `buffered`, under 1%
  spread across 13 repeats each). That cost doesn't depend on rolls; see "Where `dur/w7`'s CPU
  cost came from" below.

**The fix.** `dur/w8` keeps every persist durable but runs it, and the unlinks after it, on a
per-spool worker thread, so `commit` never waits on the disk (the ADR's "Amendment: the spool
persists its cursor on a worker thread"). Measured on the VM with `script/perf run --repeat 5`,
binaries in the order `main`, `dur/w7`, `dur/w8`, `main`, so the two `main` runs bracket the others
as a control. `dur/w8` is `dc5afce`. Each cell is a median.

| Scenario | Binary | events/s | CPU µs/event | Peak RSS |
|---|---|---:|---:|---:|
| `buffered` | `main` (first) | 763,329 | 1.692 | 77.4 MiB |
| `buffered` | `dur/w7` | 718,362 | 1.728 | 70.2 MiB |
| `buffered` | `dur/w8` | 857,264 | 1.701 | 77.1 MiB |
| `buffered` | `main` (second) | 877,124 | 1.691 | 75.4 MiB |
| `buffered-small-segments` | `main` (first) | 653,446 | 1.717 | 75.3 MiB |
| `buffered-small-segments` | `dur/w7` | 496,624 | 1.754 | 73.0 MiB |
| `buffered-small-segments` | `dur/w8` | 649,326 | 1.726 | 79.4 MiB |
| `buffered-small-segments` | `main` (second) | 659,922 | 1.717 | 75.2 MiB |

`compare` at a 5% threshold flagged no regression:

- **`buffered-small-segments` is back at `main`.** `dur/w7` to `dur/w8` is +30.7% events/s and
  −1.6% CPU µs/event. Against the two `main` runs, `dur/w8` is −0.6% and −1.6% events/s, +0.5% and
  +0.6% CPU µs/event, and +5.5% peak RSS.
- **On `buffered`, read the CPU µs/event column, not events/s.** The two `main` runs alone differ
  by +14.9% events/s, so every events/s delta here is inside that control's swing. CPU µs/event is
  steady within each binary: `dur/w8` is +0.5% and +0.6% over the two `main` runs.

**Where `dur/w7`'s CPU cost came from.** `dur/w7` cost +2.4% CPU µs/event on `buffered` against
`main`. A bisect across the stack on the VM (`script/perf run` per `dur/*` tip, then per commit
inside the tip that moved) puts all of it in `dur/w5`:

- About two-thirds comes with `d9807391`, which makes `push` call `roll_read_cursor` on every push
  under every overflow policy, not only `drop_oldest`.
- About +0.009 µs/event comes with `43974694`, the shutdown sweep's in-hand slot for the batch a
  dropped `drain_inbox` was pushing. `passthrough`, which has no disk spool, pays it too: +0.003
  µs/event, about 0.9%.

`dur/w8` recovers most of it: its `buffered` CPU µs/event is +0.5% over `main`'s.

## 4. Before/after: the regression workflow

```sh
script/perf run --repeat 3 --profile release --label before   # on the base commit
# ... make a change ...
script/perf run --repeat 3 --profile release --label after    # on the changed commit
script/perf compare perf/results/<before-file>.json perf/results/<after-file>.json --threshold 5
```

`compare` prints each scenario's Δ% on events/s, CPU µs/event, and peak RSS between the two files'
medians. It exits non-zero if events/s dropped or CPU µs/event rose by more than `--threshold`
percent; peak RSS gates only with `--rss-threshold`. A scenario present in only one file is listed,
not compared.

- **Run both sides on the same box.** `compare` warns, but doesn't fail, on a hostname or CPU-model
  mismatch, because neither number is trustworthy across machines, and a new `script/vm up` may land
  on different physical hardware (the ADR's "Consequences" section).
- **Treat `buffered` with ordinary caution.** `run` clears its spool before every repeat (§3, W8,
  #165), so the accumulation artifact is gone, but its repeat spread is still wider than most
  (`docs/known-gaps.md`'s "no cross-run noise model" entry).

## 5. Flamegraph

```sh
script/perf flamegraph --scenario passthrough
```

`flamegraph` builds `-p logit-cli --profile profiling` (a root profile: `inherits = "release"`, plus
`line-tables-only` debug info and the unstripped symbol table `perf` needs; see the profile's
comment in the root `Cargo.toml`). It runs `perf record -F 999 -g --call-graph dwarf` against that
build through the same spawn/settle/SIGTERM machinery `run` uses, then pipes `perf script |
inferno-collapse-perf | inferno-flamegraph` into an SVG. On the VM it captured 7.0s of
`passthrough` at 999 Hz (68.0 MiB of samples) and wrote `perf/results/passthrough.svg` at
**1,119,236 bytes** (~1.07 MiB, gitignored under `/perf/results/`). Symbols resolve to real,
demangled Rust paths (`logit_core::event::EventBatch`, `logit_inputs::generate::GenerateInput`,
`logit_core::interner::resolve`, …), not hex addresses.

**On the VM this is a `cpu-clock` profile, not `cycles`.** Azure's guest exposes no virtualized PMU
(`perf stat -e cycles true` answers `<not supported>`, `docs/adr/disposable-azure-perf-vm.md`'s "no
virtualized PMU" limitation), so `perf record` falls back to a software clock event. Relative sample
counts and the flamegraph shape are still meaningful, since `perf record -F 999` samples at a
fixed wall-clock rate either way, but don't
compare a `cpu-clock` capture's absolute sample count against a `cycles` capture from bare metal.

**The container needs three flags, and all three work on both SELinux and non-SELinux hosts.**
`--cap-add SYS_ADMIN` (`perf_event_open`'s capability requirement) and
`--security-opt seccomp=unconfined` (docker's default seccomp profile gates `perf_event_open`) were predicted by the
ADR. The third, `--security-opt label=disable`, is for Fedora/SELinux: SELinux denies the
`perf_event` class under the default container label regardless of capabilities, which looks
exactly like the `kernel.perf_event_paranoid` problem the first two flags solve but isn't.
`script/perf` passes all three unconditionally; on the Debian VM, which has no SELinux, the third is
a harmless no-op, so nothing is platform-conditional.

**`perf` re-raises SIGTERM after a clean capture.** For a scenario that doesn't self-exit
(`native-relay`), the harness's settle-then-SIGTERM goes to `perf`, the process it spawned. `perf`
forwards the signal to `logit`, waits for its clean exit, writes `perf.data`, and *then* re-raises
SIGTERM on itself. This was verified end to end against `native-relay`, and it's why
`crate::run::spawn_and_measure`'s wrapper path treats a wrapper's own SIGTERM death as a completed
run, not a failure.

## 6. Reading `attribute`'s output

Each row is one component, sorted by Σ `process.duration` descending. The columns:

- `events`/`batch` in/out: received vs. sent, from the runtime's uniform layer-2 counters
  ([`internal-telemetry.md`](internal-telemetry.md)).
- `process s`: Σ time inside `Transform::process`/`run_lua`; transforms only.
- `blocked s`: Σ time the node spent inside `Fanout::send` waiting on a downstream consumer. A high
  value means *that node's* consumer is the constraint, not the node itself.
- `send s`: Σ sink delivery time; sinks only.
- `buf max`: peak `buffer.utilization`; sinks with a queue only, `-` otherwise.

The verdict line names the node with the largest Σ process time, then calls out every node with
notable blocked time and spells out the downstream-constraint reading. §2 has two worked examples.

## 7. UDP intake: recvmmsg and batched queue operations

[ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)
removes two per-datagram costs from the UDP receive path. `BoundedQueue::push_many`/`pop_many` (W3)
stop `read_loop` and `decode_loop` contending on the same per-datagram gauge lock, and Linux
`recvmmsg(2)` batched reads (W4) stop `read_loop` paying one syscall per datagram. This section
records what moved; the ADR records why.

### Why three driven scenarios, not one

`udp-statsd`, `udp-statsd-small`, and `udp-statsd-packed` share one traffic model
([`perf/load/README.md`](../../perf/load/README.md)) and differ only in `datagram_mix:`, which
decides which half of the receive path a number is about:

- **`udp-statsd-small`** puts every line in its own datagram (an unbuffered client, ~40–120 B): the
  **syscall-bound worst case**. Per-datagram fixed cost dominates a payload this small, so both W3's
  gauge-lock batching and W4's `recvmmsg` should show most here. Neither change touches decode cost,
  and there is little decode cost to amortize against.
- **`udp-statsd-packed`** packs every datagram to ≤1432 B (DogStatsD's UDP default, ~13.5
  metrics/datagram): the **decode-bound** end. Far fewer syscalls and gauge updates per event, so
  the decoder and `BatchAccumulator` dominate and there's little left for either change to save.
- **`udp-statsd`** mixes both packing targets plus a ≤8192 B local-agent-style share (45% single /
  40% ≤1432 B / 15% ≤8192 B, ~17.9 lines/datagram on average): the **headline** number, closest to a
  real mixed-client deployment.

A number from only one of the three would mislead about which half of the pipeline a change helps;
see the ADR's "Representative traffic, calibrated against a recorded real-client capture" section.

### Measurement protocol

Every table in this section follows `docs/plans/udp-intake.md`'s "Baseline/delta recording
protocol" and `perf/load/README.md`'s "Box state"/"Pinning" sections, because a box's numbers move
for reasons that have nothing to do with the code:

- **A delta is a parent/branch pair taken in one sitting, interleaved**: parent, branch, parent,
  branch. Never diff a branch against a results file from another day. Two unbroken blocks would put
  one side on the cool half of a session and the other on the warm half.
- **Sender and child are pinned to distinct physical cores** with `--pin-sender`/`--pin-child`
  (§0's "Driven scenarios"), so they don't contend for one core's time. Every table states its pins:
  `--pin-sender 0,1 --pin-child 2,3` throughout, leaving cores 4–7 free (the original 4-core session
  had no such room).
- **Every delta is read next to control-to-control drift**: a same-session, same-code comparison of
  two runs that differ only in *when* they ran. A delta smaller than that drift is reported as "no
  signal," not as an improvement.
- **A box-state checklist gates whether a number is worth writing down**: `perf/load/README.md`'s
  "Box state" table (nothing else running on the VM; a VM freshly provisioned this session; gaps
  between repeats for host-maintenance headroom; sender and child pinned). `logit-perf run` still
  records governor, EPP, platform profile, and AC power best-effort into the results file's
  `box_state` and warns before the first scenario on `powersave` or battery, but the Azure guest
  exposes none of those sysfs nodes, so on the VM `box_state` carries only THP, `rmem`, and
  CPU-topology fields.
- **The denominator is events *delivered* to `null_out`, and the telemetry leg that counts them
  runs inside the measured process** (§0's "Driven scenarios" has both, and the self-checks). So a
  driven scenario's absolute CPU µs/event is comparable only to its own history, never to a
  `generate_in` scenario's (§0/§1), and not even to a raw syscall-count argument, because the leg's
  cost is baked into every number in this section identically.

### Measured numbers

These numbers are from 2026-09-20 on the current 8-vCPU reference VM
(`docs/adr/disposable-azure-perf-vm.md`'s "The size, and why N vCPUs is N cores" section). They
supersede a 2026-09-18 revision taken on a `Standard_F4as_v6` (4 vCPU), whose numbers survive only
in the descriptions of PRs #252, #253, and #254. Same three binaries, re-run in full (knee,
half-scale, sweep, and `--verify`), plus two items the 4-vCPU session left open: a per-binary
capacity bisect, and the sweep repeated with THP forced to `madvise`.

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
| `box_state` (governor/EPP/AC) | no governor/EPP/AC fields on a result file taken before THP/`rmem`/topology were recorded, and none on any VM file — this Azure guest exposes no `cpufreq`/`power_supply` sysfs nodes, so nothing to warn on; by construction (dedicated VM, no other tenants visible, no throttling) this is the isolation the whole exercise is for |
| Pins | `--pin-sender 0,1 --pin-child 2,3` throughout — cores 4–7 free, unlike the 4-vCPU session |

Each binary was built with its own isolated `CARGO_TARGET_DIR`, avoiding the mtime-collision hazard
the 4-vCPU session found; `script/vm build` now gives ref sources their own target dir by default
(`docs/adr/disposable-azure-perf-vm.md`'s "Multiple sources, one VM" section):

| Ref | Head SHA | sha256 |
|---|---|---|
| `udp/w2` | `912f5574649dfe7e11b113c5743c0f251f22c355` | `840f1ef41c09eff5265750192533f10cd8a6c2721df62d96186653c86cea3caa` |
| `udp/w3` | `1b3228c10351d290d17c51b95e09a5a3521f1211` | `b7376b1ada380f6705f2102a84ed9146ba78377b73941056f1f9c3646756823f` |
| `udp/w4` | `4a0c252fa530925c43a4d5e7cb36d8c750d1993d` | `40204bf499843ebd68f20b407cfc5420a070e376de8583db0a86c2c717908746` |

The three sha256s were confirmed distinct before any scenario ran, and match the 4-vCPU session's
values for the same three commits.

#### Calibration (bisection on w2, ≤6 tries/scenario, target 1–5% kernel drop)

| Scenario | Tries (scale → drop%) | Knee scale | Knee drop% | Half scale |
|---|---|---|---|---|
| `udp-statsd` | 1.0→19.5%, 0.525→0.0%, 0.7625→0.003%, 0.88125→8.9%, 0.822→**2.31%** | 0.822 | 2.31% | 0.411 |
| `udp-statsd-small` | 1.0→39.2%, 0.525→8.4%, 0.2875→0.007%, 0.40625→0.0%, 0.466→0.04%, 0.495→**2.80%** | 0.495 | 2.80% | 0.248 |
| `udp-statsd-packed` | 1.0→21.7%, 0.525→0.0%, 0.7625→0.0%, 0.88125→12.1%, 0.822→**3.97%** | 0.822 | 3.97% | 0.411 |

The shipped specs' rates, tuned on the dev laptop, are still too fast for this VM: `--rate-scale 1.0`
drops well above target on every scenario, so calibration is still needed. The knee scales match the
4-vCPU session's (0.82/0.50/0.82 then and now for `udp-statsd`/`-small`/`-packed`). That fits the
knee being a property of `logit`'s own decode/queue cost on the receive path: doubling the core
count doesn't move it much, because the sender and receiver each still use only their two pinned
cores. Calibration ran on `udp/w2` only; the per-binary capacity table below covers each binary's
own ceiling.

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
`udp-statsd-small`'s control-to-control drift (±22–29 points) is again wider than most branch
deltas; "Reading the numbers" below explains why this scenario's µs/event isn't readable at knee or
half scale.

#### Per-binary capacity: highest offered rate at ≈0% kernel drop

The knee-scale tables measure w3/w4 at *w2's* knee. This table bisects each binary independently
(target ≤0.5% drop, ≤8 tries), as a multiplier of each scenario's shipped `rate:`:

| Binary | `udp-statsd` | `udp-statsd-small` | `udp-statsd-packed` |
|---|---:|---:|---:|
| `udp/w2` | 0.800× (71,965/s) | 0.480× (365,156/s) | 0.785× (91,033/s) |
| `udp/w3` | 0.822× (73,969/s) | 0.547× (415,922/s) | 0.807× (93,616/s) |
| `udp/w4` | 0.904× (81,316/s) | **1.992× (1,514,062/s)** | 0.911× (105,669/s) |

`udp-statsd`/`-packed` gain modest headroom w2→w4 (~13–16%), consistent with the knee-scale
table's small CPU gains. **`udp-statsd-small` confirms the design intent:** `recvmmsg` (W4) roughly
*quadruples* the highest loss-free rate for the single-datagram, syscall-bound workload, exactly the
case batched reads exist for, while barely moving the two packed shapes where decode cost already
dominated.

#### `read_batch` sweep (w4 only, `--repeat 3`, knee scale)

The sweep used temporary scenario/load copies (`udp-statsd-rbN` / `udp-statsd-small-rbN`) under
`perf/scenarios/` and `perf/load/` on the VM only, deleted afterwards and never committed.

| `read_batch` | **udp-statsd-small** µs/event | fill | drop% | rcvbuf | | **udp-statsd** µs/event | fill | drop% | rcvbuf |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 4.173 | 1.0 | 11.60% | 1.00 | | 0.961 | 1.0 | 4.63% | 1.00 |
| 16 | 3.410 | 3.7 | 0.00% | 0.01 | | 0.797 | 6.3 | 0.00% | 0.28 |
| 32 | 3.618 | 3.5 | 0.00% | 0.01 | | 0.808 | 12.1 | 0.00% | 0.27 |
| **64** | **3.486** | **4.0** | **0.00%** | **0.03** | | **0.795** | **7.6** | **0.00%** | **0.28** |
| 128 | 3.386 | 4.2 | 0.00% | 0.01 | | 0.808 | 14.3 | 0.00% | 0.26 |
| 256 | 3.315 | 3.2 | 0.00% | 0.02 | | 0.795 | 6.3 | 0.00% | 0.27 |

**Peak RSS rises with `read_batch`** for `udp-statsd-small`: 31.5 (rb1) → 31.2 → 32.0 → 34.1 →
39.2 → 47.6 MiB (rb256). The 4-vCPU session found the same shape, and the THP experiment below
explains it. `udp-statsd`'s RSS is noisier, with no clean trend (76.9 → 70.8 → 51.0 → 53.7 → 55.5 →
79.6 MiB), plausibly because mixed and larger datagrams bring more memory factors into play than
the small-datagram slab alone.

**`fill` plateaus by `read_batch=16`** for both scenarios (~3–4 for `-small`, ~6–14 for
`udp-statsd`; noisy, but not trending up past 16). The default of 64 already captures essentially
all the batching benefit, and `udp-statsd`'s µs/event is flat within noise from 16 upward
(0.795–0.808). `-small`'s µs/event is noisy across the whole sweep; see "Reading the numbers" below.

#### `read_batch` sweep under `transparent_hugepage=madvise`

This tests whether `THP=always` explains the RSS rise: the same sweep, `udp-statsd-small` only, w4,
same protocol, with `transparent_hugepage` forced to `madvise` and restored to `always` afterwards.

| `read_batch` | µs/event | fill | drop% | peak RSS |
|---|---:|---:|---:|---:|
| 1 | 4.109 | 1.0 | 11.80% | 16.8 MiB |
| 16 | 3.332 | 3.1 | 0.00% | 14.4 MiB |
| 32 | 3.985 | 3.2 | 0.00% | 14.8 MiB |
| 64 | 3.178 | 4.4 | 0.00% | 14.9 MiB |
| 128 | 2.611 | 2.9 | 0.00% | 15.0 MiB |
| 256 | 4.066 | 3.2 | 0.00% | 16.1 MiB |

**Confirmed.** Under `madvise`, peak RSS stays flat at 14.4–16.8 MiB across the whole `read_batch`
range, with no rise at 128/256 like the `THP=always` table above (34.1 → 39.2 → 47.6 MiB over the
same range). Under `THP=always`, the `read_batch × 65,507`-byte slab becomes fully resident, because
touching one 4 KiB page faults in its enclosing 2 MiB huge page; under `madvise`/`never` it stays
mostly untouched. This same-box, same-binary, THP-toggled repeat closes `docs/design/memory.md`'s
"likely, but not confirmed" caveat.

#### `--verify` (default scale, one repeat each)

| Binary | Result |
|---|---|
| `udp-statsd` (w2/w3/w4) | 620,000 datagrams, 11,074,692 lines, **11,074,692 events delivered exactly, zero drops**, all three |
| `udp-statsd-small` (w2/w3/w4) | 5,000,000 datagrams, 5,000,000 lines, **5,000,000 events delivered exactly, zero drops**, all three |
| `udp-statsd-packed` (w2/w3/w4) | 800,000 datagrams, 10,788,160 lines, **10,788,160 events delivered exactly, zero drops**, all three |

All nine (three binaries × three scenarios) pass the strict self-check.

<!-- udp-intake-numbers:end -->

#### 2026-09-28 sanity check on `main`

`main` (`4e08c49ef421`) at the shipped rates, `--repeat 5`, `--pin-sender 0,1 --pin-child 2,3`:

| Scenario | CPU µs/event | events/s | Kernel drop | Peak RSS |
|---|---:|---:|---:|---:|
| `udp-statsd` | 0.871 | 1,427,195 | 2.53% | 88.9 MiB |
| `udp-statsd-small` | 2.926 | 544,782 | 0.00% | 33.4 MiB |
| `udp-statsd-packed` | 0.935 | 1,385,772 | 3.95% | 106.7 MiB |

All three `--verify` runs delivered every event sent, with zero drops. Drops at the shipped
rates sit inside the 1–5% calibration band, so the `drain/*` queue changes did not move the intake
knee. CPU per event isn't compared with the knee-scale tables above, because those ran at w2's
knee, a different operating point.

#### 2026-10-02 regression check: B0 against M

`4e08c49ef421` (B0) and `d1521c5f9007` (M) at the shipped rates, `--repeat 5`, `--pin-sender 0,1
--pin-child 2,3`, two passes interleaved in B0, M / M, B0 order. Each cell lists pass A, then pass B.

| Scenario | Binary | CPU µs/event | events/s | Kernel drop | `fill` |
|---|---|---:|---:|---:|---:|
| `udp-statsd` | B0 | 0.838 / 0.836 | 1,416,178 / 1,416,112 | 3.26% / 3.33% | 55.8 / 54.8 |
| `udp-statsd` | M | 0.842 / 0.843 | 1,419,281 / 1,406,948 | 3.09% / 3.85% | 55.2 / 54.4 |
| `udp-statsd-small` | B0 | 2.888 / 2.887 | 540,778 / 543,066 | 0.00% / 0.00% | 7.1 / 7.1 |
| `udp-statsd-small` | M | 2.888 / 2.886 | 541,007 / 543,194 | 0.00% / 0.00% | 7.1 / 7.2 |
| `udp-statsd-packed` | B0 | 0.938 / 0.899 | 1,370,826 / 1,372,613 | 4.99% / 4.86% | 59.3 / 57.7 |
| `udp-statsd-packed` | M | 0.881 / 0.893 | 1,382,948 / 1,382,706 | 4.15% / 4.17% | 57.0 / 56.1 |

B0 and M are indistinguishable: drops stay in the 1–5% calibration band (`-small` reads 0.00%, as in
the 2026-09-28 check), and control drift between a binary's two passes is at most 0.77 drop points
and 1.43% on CPU, except `udp-statsd-packed` on B0 at −4.21%. All 24 files, which include the
`--verify` and half-scale runs, passed their self-checks (`sent == received + kernel_dropped`, the
sampler reported, no decode diagnostics), and `--verify` delivered every event with zero drops on
both binaries: 11,074,692 for `udp-statsd`, 5,000,000 for `-small`, and 10,788,160 for `-packed`.
The knee didn't move, so no capacity bisect ran; the per-binary capacity and THP results above
stand.

B0's kernel drop here is 0.7–1.0 points above the 2026-09-28 check's (2.53% for `udp-statsd`,
3.95% for `-packed`). Ordinary box drift fits that, and so does the orphaned sampler described
next.

**These phases ran with an orphaned sampler polling `/proc`.** The `purge` phase's check that
`_RJEM_MALLOC_CONF` reached the child left a polling loop (`pgrep`, then a read of
`/proc/<pid>/environ`, every 0.3 s) running after the phase ended. It ran from about 23:14 to 23:45
UTC, across `udp-verify`, `udp-knee`, `groups-sweep`, and `tail`. Both binaries ran inside the same
window in each phase, so a B0-against-M comparison within a phase stands; the effect on any
absolute number is expected to be negligible, and nothing here measured it.

**`udp-statsd-small` at `--verify`'s 0.25× read 5.483 µs/event on M against 2.637 on B0** (one
repeat each, 190,000 datagrams/s, delivery exact on both). The same scenario at 1.0× and 0.5×
shows no gap (2.888 on both at 1.0×, 3.465 and 3.464 at 0.5×). It is unexplained. Treat it as one
more instance of the `-small` CPU number being unreadable (see "Reading the numbers"), not a
regression.

### Reading the numbers

**The robust signal is loss at a fixed offered load, not CPU per event**, as in the 4-vCPU session.
At the calibrated knee, kernel drops fall monotonically w2 → w3 → w4 on every scenario, and both
interleaved passes agree: `udp-statsd` 2.37/2.39% → 0.22/0.20% → 0.00%; `udp-statsd-small`
2.92/3.22% → 0.01/0.00% → 0.00%; `udp-statsd-packed` 4.01/4.71% → 1.66/1.26% → 0.00%.
Control-to-control drift stays under 1 drop point throughout, well inside those movements.
Decode-side batching (W3) removes most of the loss, and `recvmmsg` (W4) removes the rest, taking
every scenario to exactly zero drops at the knee. For syscall-bound UDP intake, trust this number,
not µs/event.

**CPU µs/event moves less, and partly inside drift, but `packed` shows a real win.** `udp-statsd`'s
w2→w3 gain (−3.2%/−3.0%, against controls of ≤0.6%) and w3→w4 gain (−5.0%/−2.7%, controls ≤2.0%)
are modest and mostly within drift. `udp-statsd-packed`'s w3→w4 gain (−9.8%/−13.8%, against
controls of +4.6%/+0.0%) is the cleanest CPU win in the table, beyond its control drift on both
passes; the same comparison was ambiguous in the 4-vCPU session.

**`udp-statsd-small`'s CPU µs/event isn't readable on either box.** Its control-to-control drift
(knee: +3.8%/−23.1%/+11.7%; half scale: −21.9%/−28.9%) is the same order of magnitude as any branch
delta, the same conclusion the 4-vCPU session reached. Don't read its *timing* as a w2→w3 or w3→w4
finding on a VM. Its drop-rate and capacity numbers aren't timings and remain trustworthy; the
per-binary capacity table is this scenario's honest headline.

**Peak RSS falls w3→w4 at the knee, by a margin similar to the 4-vCPU session's.** `udp-statsd`
drops −25.5%/−25.5% and `udp-statsd-packed` −42.1%/−45.2%, both far beyond control drift, because w4
no longer builds the receive-side backlog w3 did. The 4-vCPU session found −19.8%/−23.1% and
−38.2%/−42.3% respectively. At half scale, RSS is flat within drift.

**Per-binary capacity makes the `recvmmsg` story sharper than the knee-scale table can.** The
knee-scale load was calibrated *below* w2's ceiling, so once w3/w4 clear it every binary looks like
"zero drop," and the table understates what W4 bought. At its own highest loss-free rate, w4
sustains **1,514,062 datagrams/s on `udp-statsd-small` versus w2's 365,156: a ~4.1× capacity
gain**. `udp-statsd`/`-packed` move only 13–16% w2→w4 on the same measurement, which puts a number
on how concentrated `recvmmsg`'s benefit is in the syscall-bound small-datagram case.

**The `read_batch` sweep confirms the plateau, so 64 stays the default.** `read_batch: 1`
reproduces the pre-`recvmmsg` loss (11.60% `-small`, 4.63% mixed). From 16 upward, drops are 0,
`udp-statsd`'s µs/event is flat within noise (0.795–0.808), and mean fill plateaus for both
scenarios. Peak RSS still rises at large `read_batch` for `-small` under `THP=always` (31.2 MiB at
16 → 47.6 at 256), the same shape as the 4-vCPU session.

**THP explains that RSS rise.** The `madvise` repeat above keeps peak RSS flat at 14.4–16.8 MiB
across the *entire* `read_batch` range: the `read_batch × 65,507`-byte slab becomes resident only
under `THP=always`. `docs/design/memory.md` §5's caveat says so.

### Open after this workstream

- **The wakeup-cost hypothesis for `udp-statsd-small` on VMs is still a hypothesis.** Nothing here
  isolates wakeup cost directly (for example, a wakeup-rate probe independent of `fill`), and this
  session didn't re-run the half-scale-vs-knee CPU comparison the 4-vCPU session used to motivate
  it. The per-binary capacity table characterizes this scenario's headline either way.
- **The shared-task / `SO_REUSEPORT` follow-up is still open.** This session measured the existing
  single-listener-task design at higher fidelity, not a multi-task alternative.

Both items the 4-vCPU revision left open, a per-binary capacity metric and confirming the THP
explanation, are closed above.

## 8. Event sizing bake-off (2026-09-21)

**Result: nothing about `Event` changes.** `AttrMap`'s inline capacity stays 8, and attribute maps
stay unreserved, per-key `insert_sym` builds. [ADR
`event-sizing-and-allocation-strategy`](../adr/event-sizing-and-allocation-strategy.md) records the
decision and the why; this section is the full measurement record its tables draw from, recomputed
from the raw results JSON and bench text rather than copied from any intermediate table.

### Box facts and protocol

| Fact | Value |
|---|---|
| VM size | `Standard_F8as_v6` (8 vCPU, SMT off — 8 full physical cores, `docs/adr/disposable-azure-perf-vm.md`) |
| CPU model | AMD EPYC 9V74 80-Core Processor (Genoa, cloud SKU) — from `perf/results/*.json`'s `cpu_model` |
| `nproc` | 8 — from the same JSON |
| `rustc` | `rustc 1.98.1 (48a229cea 2026-09-01)` — from the same JSON |
| Profile | `release` |
| `box_state` | empty `{}` on every result file of this session (taken before THP/`rmem`/topology were recorded) — this guest exposes no `cpufreq`/`power_supply` sysfs, same as every other VM session in this document |
| `main` at head | `8110aece154b1551d9335157b332101c7116e768`, `git.dirty: false` |
| `sizing-w4`/`9a3ebc3ba294`/experiment binaries | built from the `sizing/w3c`→`sizing/w4` stack and its throwaway experiment commits (`d78c233`, `badc1c2`, `efe9b8b`, and four `EXPERIMENT`/never-merged commits) — one `sha256` per binary, recorded in each result file's `binary` block |

**Protocol.**

- **Pipeline tables:** two interleaved rounds of three repeats per binary (round 1: every binary
  back to back; round 2: the same order again), pooled into one `median of 6`, with
  `spread = (max − min) / min` over the six. Binaries were built with `script/vm build <ref>`, one
  `sha256` per source, confirmed distinct before any scenario ran (the same discipline as §7), and
  run with `script/perf run --logit-bin perf/bins/<slug>/logit --repeat 3`, one round per
  invocation.
- **Independent recomputation:** every CPU µs/event value was recomputed in Python from each
  repeat's `cpu_us_per_event` in the six `perf/results/*.json` files behind each binary/round pair,
  never from an intermediate `.txt` table. Every value matched the session's generated tables to
  rounding.
- **Micro-benches (§8e):** re-derived the same way from `sizing-w4.bench.
  {size_vs_alloc,attr_arms}.{1,2,3}.txt`, three separate `cargo bench` runs pinned `taskset -c 2`
  per the bench files' module docs. Each value is the median of 3 `cargo bench`/divan medians, not
  divan's raw samples.
- **One gap found:** `bench-table.txt` is missing the row `alloc_free::immediate::432` (true value
  4.93 ns ± 1.4%, recomputed here). Every other row matched its recomputed value to within
  rounding.

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

No cell here crosses the ~10% spread ordinarily treated as the limit of clean signal. The two
noisiest are `sizing-w4`/`logfmt-parse` (±6.1%) and `arm-n4`/`native-relay` (±4.2%), against this
table's typical ±0.5–2%. The `logfmt-parse`/`sizing-w4` cell sets the top of the ADR's quoted
0.4–6% spread range.

### (b) The in-order fast path: does the bulk build's own regression hold up?

`sizing-w4`'s bulk build (arm P) was measured once, found 12–20% slower than `main` on the json
legs, and rebuilt with an in-order fast path (`efe9b8b`, "keep an in-order `AttrMap` bulk build on
an append-only path") before this re-run, tagged `9a3ebc3ba294`. Deltas are against `main`.

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
`json-parse-access-log` +19.8%→+16.7%) but closes little of the gap: three of the four json
scenarios stay double digits over `main`, and `json-parse-nested-log` is still +8.1%.
`logfmt-parse`'s ~7% gain is identical under either version, because the fast path doesn't touch
`logfmt`'s call site, and `native-relay` stays flat. This re-run motivated (c): is the regression
the bulk build itself, or its eager up-front reservation?

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

`json-loop`, the per-key loop with nothing else changed, lands within ±2% of `main` on every leg:
the loop was never the problem, and every point of regression in the other columns comes from what
replaces it. `9a3ebc3ba294`'s regression here (+8.2% to +16.1%) is the recomputed source of the
ADR's "cost the json scenarios 8–17%" line; the caveats below explain why 17% overstates the
recomputed ceiling.

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

Every alternative to the untouched loop costs something at both widths, whether it reserves
eagerly without changing insert order (`reserve-loop`/`reserve-pow2-loop`) or bulk-builds
(`9a3ebc3ba294`/`bulk-pow2`). Reservation shape (exact vs. power-of-two) barely matters next to
*whether* the build reserves early at all.

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

`json` doesn't use the bulk build in this tree, so its three scenarios' movement (+1.7% to +4.0%)
is noise or other changes, not the bulk-build regression; scoping the bulk build to two call sites
was the point. `logfmt-parse`'s −5.8% is the "~6%" win the ADR's Decision (2) weighs against ~300
lines of bulk-build code, and `native-relay`'s +0.6% is the "flat" in the same sentence.

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

- **`final` vs. `final-nonative`:** within noise on both json legs (native decode isn't on their
  path) and within a point on `passthrough`/`native-relay`. The native decoder's own bulk-build use
  neither helps nor hurts these four scenarios measurably.
- **`final` vs. `final-derivedclone`:** the basis for the ADR's "measured CPU-neutral (±0.6%)"
  claim. Recomputed deltas are +0.16%, +0.55%, +0.59%, and +0.37% across the four rows: the
  exactly-sized clone is not a CPU win over the derived one at any of these widths, on this tree.

### (e) Micro-benches (pinned `taskset -c 2`, median of 3 `cargo bench` runs)

**Every batch bench below is divided by its batch size by hand.** divan reports the median of the
*whole* iteration, not divided by an `ItemsCount` counter, so a closure that does 64 allocations (or
touches 1000 elements) reports one number for all of them. Batch sizes, confirmed against
`crates/logit-bench/benches/size_vs_alloc.rs`:

- `alloc_free::pair_same_thread`/`pair_cross_thread`: 64 blocks per iteration.
- `move_value::ptr_move`: 64 moves.
- `move_value::vec_push_pop`: 64 pushes + 64 pops = 128 moves.
- `scan::touch_head`/`drop_batch`/`clone_batch`: 1000 elements (`generate_in`'s
  `receive.batch_max_events` default).
- `alloc_free::immediate` and `realloc_chain::*`: already one operation per iteration; no division.

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

Least-squares slope across all six points: 0.0183 ns/byte, or **~7.0 ns per +384 B**. This is the
ADR's "~7 ns more per +384 B," recomputed from all six points rather than read off two endpoints.

#### `move_value::vec_push_pop` (per-move, ns, through safe `Vec` push/pop rather than a raw `ptr::copy`)

| N (bytes) | 496 | 672 | 864 | 1056 | 1248 | 1632 |
|---|--:|--:|--:|--:|--:|--:|
| ns/move | 20.73 (±43.1%) | 25.81 (±1.2%) | 29.88 (±1.2%) | 32.23 (±5.2%) | 35.20 (±2.7%) | 44.91 (±4.2%) |

The 496-byte row's ±43.1% spread is the widest in this document's micro-bench tables: one of the
three runs read roughly half the other two. It is noise, not a regime change; the other five `N`
values in the same bench are all under 5.2%.

#### `scan::touch_head`/`drop_batch`/`clone_batch` (per-element, ns, 1000-element batches, 8 batches rotated so the working set exceeds L2)

| N (bytes) | 496 | 672 | 864 | 1056 | 1248 | 1632 |
|---|--:|--:|--:|--:|--:|--:|
| `touch_head` (1 cache line/element) | 1.962 (±7.8%) | 1.281 (±2.4%) | 1.281 (±1.2%) | 1.251 (±2.4%) | 1.331 (±1.5%) | 1.266 (±1.2%) |
| `drop_batch` | 0.1374 (±2.0%) | 0.1342 (±2.6%) | 0.1412 (±12.5%) | 0.1402 (±36.3%) | 0.1412 (±31.0%) | 0.1446 (±6.5%) |
| `clone_batch` | 19.08 (±96.1%) | 14.30 (±1.3%) | 18.56 (±0.2%) | 23.27 (±0.6%) | 27.72 (±0.5%) | 37.21 (±7.0%) |

- **`touch_head` is flat from 672 B upward** (1.25–1.33 ns), backing the ADR's "a 1000-event batch
  scan is flat in `size_of::<Event>()`." The 496-byte row (1.96 ns) is the one exception, likely an
  artifact of how that size's batch falls against cache-line/page boundaries, not a trend reversing.
- **`drop_batch` recomputes to the ADR's "~0.14 ns per event"** almost exactly (0.134–0.145 ns at
  every width).
- **`clone_batch::496` is this document's least trustworthy single cell**: ±96.1% spread, from
  three raw runs of 19,880 / 19,080 / 10,140 ns. Excluding it, a least-squares fit over 672→1632
  gives **9.19 ns per +384 B**, matching the ADR's "cloning a batch ~9 ns more per event per
  +384 B"; including it pulls the six-point fit down to ~7.1 ns/384 B. The ADR's figure holds only
  with this cell set aside.

#### `build_shape` — `AttrMap`'s sorted `insert_sym` vs. an append-then-sort mirror, ns/build

| Width | `sorted_insert` (today) | `append_then_sort` (reserved) | `append_then_sort_unreserved` | `bulk_build` | `bulk_build_onto_populated` |
|---:|--:|--:|--:|--:|--:|
| 9 | 169.3 (±1.1%) | 102.3 (±1.0%) | 137.4 (±2.4%) | 186.2 (±2.4%) | 217.6 (±3.5%) |
| 12 | 226.9 (±1.7%) | 133.6 (±1.4%) | 168.1 (±3.4%) | 236.2 (±1.6%) | 276.4 (±2.3%) |
| 17 | 345.2 (±8.0%) | 196.9 (±13.8%) | 257.6 (±5.5%) | 331.4 (±7.8%) | 379.1 (±4.8%) |
| 30 | 695.7 (±4.7%) | 407.9 (±1.8%) | 469.2 (±2.4%) | 617.9 (±1.6%) | 640.4 (±1.2%) |

**This table measures worst-case key order, not pipeline cost.** As the ADR's Decision (2) states,
every key sequence here is fed in a fixed, deliberately *unsorted* order (`build_shape::keys`'s doc
comment: "a shuffled order is the realistic one"). The pipeline scenarios in (a)–(d) show the
opposite for a real source with a stable key order. The interner numbers keys in first-seen order,
so a logging library's keys arrive already ascending by `Symbol`, and `sorted_insert`'s binary
search lands at the end on every insert with nothing to move. That is why `append_then_sort` "wins"
this micro-bench by 32–46% at every width, while in (c) the untouched loop matches `main` to within
±2%. The gap shown here does not appear in (a)–(d)'s real traffic.

#### `attr_clone::clone` — a real `AttrMap`'s own clone, `Value::I64` throughout (ns/clone)

| Width | 8 (inline) | 9 (first spill) | 12 |
|---|--:|--:|--:|
| ns | 140.5 (±2.3%) | 168.1 (±4.3%) | 206.9 (±1.9%) |

The 8→9 step (140.5 → 168.1 ns, +19.6%) is the one allocation the spill costs; every wider map
pays the same one allocation, just larger.

#### Arm K's pre-registered kill criterion — build + clone of the 12-attribute log, ns (K needed ≥10% under arm P's bulk build to survive)

| Arm | ns |
|---|--:|
| `bulk_build_and_clone` (arm P) | 410.3 (±3.7%) |
| `sorted_insert_and_clone` (today) | 592.9 (±7.7%) |
| `keyset_build_and_clone` (arm K) | 333.9 (±4.3%) |

Arm K beats arm P by 18.6%, clearing the kill criterion; the ADR's Alternatives section rounds this
to "19%."

#### Arm K — build cost by shape, ns (`log12`=12 attr, `access30`=30, `nested10`=10 top-level)

| Build path | `log12` | `access30` | `nested10` |
|---|--:|--:|--:|
| `sorted_insert` (today) | 328.9 (±27.5%) | 960.7 (±12.2%) | 273.9 (±31.8%) |
| `bulk_sort` (arm P) | 261.3 (±0.5%) | 790.7 (±3.7%) | 213.7 (±4.4%) |
| `keyset_hit` (arm K, cache hit) | 172.4 (±3.0%) | 375.3 (±4.3%) | 151.2 (±3.4%) |
| `keyset_miss` (arm K, cache miss) | 210.6 (±3.0%) | 590.7 (±2.8%) | 183.1 (±3.4%) |

`sorted_insert`'s spreads here (±27.5%, ±31.8%) come from the same *shuffled* key order the whole
`attr_arms` suite builds its scratch inputs from, like `build_shape` above; it makes even the
control noisy. A real source's ascending-`Symbol` order doesn't reproduce this in the pipeline
tables.

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

A 64-entry cache *loses* to the bulk build by 58.1% here (the ADR's "58%"): against 196 real
shapes it evicts too often to pay for its own hash-and-lookup overhead. The unbounded ceiling
(193.1 ns, a 29% win over the bulk build) shows that a mixed-gateway workload needs the cache sized
correctly; the representation itself isn't the problem.

#### Arm E — nested `Value::Map`, boxed `AttrMap` ("today") vs. an inline thin map, ns

| Shape | `build_today` | `build_thin` | `clone_today` | `clone_thin` | `drop_today` | `drop_thin` |
|---|--:|--:|--:|--:|--:|--:|
| 1 nested map | 588.1 (±1.3%) | 548.2 (±18.5%) | 266.9 (±7.7%) | 128.0 (±2.0%) | 51.2 (±32.5%) | 28.4 (±0.6%) |
| 4 nested maps | 1081.0 (±2.4%) | 981.2 (±17.1%) | 570.7 (±2.2%) | 251.3 (±1.0%) | 104.4 (±13.2%) | 59.4 (±0.5%) |
| pino-http (4 maps, the measured shape) | 1026.0 (±2.0%) | 915.9 (±17.8%) | 539.3 (±5.3%) | 235.7 (±0.8%) | 108.0 (±12.5%) | 59.5 (±0.8%) |

The thin representation clones the pino-http record 56.3% faster (539.3 → 235.7 ns, the ADR's
"56%") and drops it 45% faster. Its build numbers carry much more spread (17–19%) than "today"'s
(1–2%), an open question rather than a settled reading.

#### Arm E — `Scope`/`Resource`-width embedding, build and clone, ns

| Width | `build_attrmap` | `build_thin` | `clone_attrmap` | `clone_thin` |
|---:|--:|--:|--:|--:|
| 0 (`Scope`'s own median) | 21.5 (±42.8%) | 4.4 (±3.2%) | 14.7 (±63.7%) | 4.1 (±0.4%) |
| 5 (a `Resource` outside a collector) | 115.3 (±46.4%) | 102.3 (±3.6%) | 101.7 (±13.1%) | 44.0 (±3.2%) |
| 17 (the measured median behind a collector) | 446.6 (±26.1%) | 357.8 (±5.5%) | 336.4 (±7.8%) | 162.4 (±2.7%) |
| 29 (the measured maximum) | 850.9 (±13.7%) | 728.4 (±0.9%) | 533.1 (±5.7%) | 290.2 (±0.5%) |

**`Resource`/`Scope` are `Arc`-shared and typically not cloned per event**, so this table (and the
ADR's `SeriesKey`-embedded-`AttrMap` "future investigations" item it feeds) measures a cost most
pipelines pay once per batch, not once per event. The 0-width row's ±42.8%/±63.7% spreads are noise
on near-zero absolute values (single-digit to low-double-digit ns), not a finding.

#### Arm C — clone candidates by value mix, at the 12-attribute log width, ns/clone

| Candidate | scalar | 75% str | 100% str |
|---|--:|--:|--:|
| `baseline` (today's shipped clone) | 205.6 (±12.5%) | 233.8 (±5.9%) | 243.2 (±5.5%) |
| `exact_loop` | 233.2 (±7.4%) | 250.1 (±5.3%) | 259.5 (±4.9%) |
| `scalar_branch` | 160.6 (±10.0%) | 234.4 (±2.4%) | 262.6 (±3.4%) |
| `detect_pod` | 67.6 (±13.8%) | 241.3 (±3.7%) | 257.6 (±3.4%) |
| `pod_flag` | 68.6 (±2.3%) | 231.3 (±1.3%) | 255.1 (±1.0%) |

At the all-scalar mix, `detect_pod`/`pod_flag` are a real win (67–69 ns against `baseline`'s
205.6), a bitwise-copy fast path that is legal only when every value is scalar. But the survey's
measured band is 0–100% string share, with the median toward the middle. At 75%/100% string share,
where the survey's logs sit, every candidate is within a few percent of `baseline`; some (`pod_flag`
at 75%) are marginally *under* it, but inside `baseline`'s ±5.9% spread. This is the recomputed
basis for the ADR's "no clone candidate won at realistic string shares."

### Caveats this data itself surfaces

- **Every spread past ~10% is called out at its own row**, not folded into one "noise" disclaimer:
  `pair_cross_thread` (every size but 1536), `vec_push_pop::496` (±43.1%), `clone_batch::496`
  (±96.1%), `sorted_insert`'s per-shape numbers in `keyset_k::build` (±12–32%), and several
  `embed_e` rows (`build_attrmap` at width 0/5, ±43–46%). They are real measurements, not typos; read
  none as more precise than its spread allows.
- **The micro-benches feed keys in a fixed, deliberately unsorted order.** This covers `sizing-w4`'s
  `build_shape` groups and `attr_arms`' `keyset_k::build`/`kill_criterion` scratch inputs, confirmed
  from `build_shape::keys`'s doc comment in `crates/logit-bench/benches/size_vs_alloc.rs`. A real
  source's keys arrive in the interner's first-seen order (ascending `Symbol`, confirmed against
  `crates/logit-core/src/interner.rs`), the best case for today's sorted `insert_sym`. Before
  trusting a micro-bench number that looks like a clean win for another build strategy, check it
  against the pipeline tables in (a)–(d).
- **No virtualized PMU** (`perf stat -e cycles true` returns `<not supported>`, as in every VM
  session here). Nothing is cycle-accurate or branch-mispredict-attributed; every number is wall/CPU
  time from `wait4` or divan's timer. "Why early reservation loses to late growth" (the ADR's open
  question) has no counter-level answer on this box.
- **The ADR's `8–17%` is really 8.2–16.1%.** Decision (2) says "the bulk build cost the json
  scenarios 8–17% end to end." Against `9a3ebc3ba294`, the fast-path bulk build that (c) and (d)
  compare throughout, it recomputes to **8.2–16.1%** (table (c)'s Table 1: +9.8%, +12.0%, +8.2%,
  +16.1%). The recomputed ceiling is 16%, about a point under the ADR's 17%.

### The three `json-parse-*` scenarios' first real run

`json-parse-app-log`, `json-parse-nested-log`, and `json-parse-access-log` shipped 2026-09-21 with
first-estimate counts, scaled off `json-parse`'s by key count and never run on the reference VM. This
session ran all three on `main` as part of every table above. Their `wall_s` (median of 6, `main`
binary):

| Scenario | Count | Median wall_s | 5–10 s band? |
|---|--:|--:|---|
| `json-parse-app-log` | 12M | 10.9 s | No — overshoots by ~1 s |
| `json-parse-nested-log` | 8M | 13.9 s | No — overshoots by ~4 s |
| `json-parse-access-log` | 5M | 12.4 s | No — overshoots by ~2.4 s |

All three overshot the 5–10 s band, `json-parse-nested-log` most. **Their shipped counts have since
been lowered** (12M → 9M, 8M → 4.5M, 5M → 3M), each scaled to ~8 s from the wall time above. §1's
2026-09-28 run measured 8.0–9.0 s at those counts. CPU µs/event, which every table in this section reports, doesn't
depend on the count. The first estimates were too generous across the board: the first real data
point for the "the first session that runs them should expect to retune them" line these
scenarios' YAML and `docs/plans/load-test-harness.md` already carried.

## 9. Datadog stack: the sparse sketch store and serde_json's float_roundtrip (2026-09-24)

**Result: the sketch store's cost is accepted, and `float_roundtrip`'s gate is cleared.** [ADR
`datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md) hand-rolls `DdSketch` to
match the Datadog Agent's own bin mapping bin-for-bin; this session measures what that costs against
`main`, and separately closes [`docs/known-gaps.md`](../known-gaps.md#datadog)'s open
`float_roundtrip` question (`serde_json`'s exactly-rounded float parser, enabled workspace-wide for
the Datadog JSON routes).

### Box facts and protocol

| Fact | Value |
|---|---|
| VM size | `Standard_F8as_v6` (8 vCPU, SMT off — 8 full physical cores, `docs/adr/disposable-azure-perf-vm.md`) |
| CPU model | AMD EPYC 9V74 80-Core Processor (Genoa, cloud SKU) — from `perf/results/*.json`'s `cpu_model` |
| `nproc` | 8 — from the same JSON |
| `rustc` | `rustc 1.98.1 (48a229cea 2026-09-01)` — from the same JSON |
| Profile | `release` |
| `box_state` | empty `{}` on every result file of this session (taken before THP/`rmem`/topology were recorded), same as every other VM session in this document |
| `main` binary | source `main`, sha `22a6b0312fbdfbfd1fdc7e8d9a88935d67189059`, sha256 `1ffe112b2ae0bfbfc168862b3b986c1888c22d658ea745783a88f8b2c5090b8d` |
| `dd-w1` binary | source `dd/w1`, sha `d844e73bc5ebf263777e8359c70293c60508c92a`, sha256 `59e891a5895d379ab7088769a3db382767f470c30f40d469f5752722cacf00c2` |
| `dd-w2b` binary | source `dd/w2b`, sha `c17828c604b28be0534ccbf1f262505d5d4969e7`, sha256 `6349605695d5af575a54cab4cff6ba9cdf1689d50b579aa78c3537c243bf41c0` |

All three `sha256`s are distinct, confirmed before any scenario ran.

**Protocol.** `script/vm build main dd/w1 dd/w2b` built the three binaries once; seven scenarios
(`passthrough`, `aggregate`, `json-parse`, `json-parse-x3`, `json-parse-access-log`,
`json-parse-app-log`, `json-parse-nested-log`) then each ran two interleaved rounds of `script/perf
run --repeat 3 --profile release --no-build --logit-bin perf/bins/<slug>/logit` per binary (round 1:
every binary back to back; round 2: the same order again), the same discipline §7 and §8 use. The
three repeats from each round pool into one **median of 6** per scenario/binary, with
`spread = (max − min) / min` over those six read as the noise floor a delta has to clear. The
session ran detached on the VM (`nohup`/`setsid` via a driver script), so a dropped SSH session
didn't kill it; `vm-warmup`'s throwaway repeat was excluded, as it always is.

### CPU µs/event (median of 6; deltas in %, + = slower, − = faster)

| scenario | main | dd-w1 | dd-w2b | Δ w1/main | Δ w2b/w1 | Δ w2b/main | spread (max of the three) |
|---|--:|--:|--:|--:|--:|--:|--:|
| aggregate | 0.325 | 0.339 | 0.340 | **+4.18%** | +0.39% | **+4.59%** | 0.79% |
| json-parse | 0.895 | 0.934 | 0.927 | **+4.40%** | −0.76% | **+3.60%** | 1.44% |
| json-parse-access-log | 3.210 | 3.250 | 3.189 | +1.26% | **−1.89%** | −0.65% | 1.61% |
| json-parse-app-log | 1.387 | 1.399 | 1.384 | +0.89% | −1.10% | −0.23% | 2.49% |
| json-parse-nested-log | 2.331 | 2.289 | 2.330 | −1.80% | +1.82% | −0.01% | 4.83% |
| json-parse-x3 | 2.460 | 2.474 | 2.461 | +0.58% | −0.55% | +0.02% | 3.72% |
| passthrough | 0.330 | 0.330 | 0.329 | +0.02% | −0.32% | −0.30% | 0.98% |

(Bold = delta exceeds that scenario's own spread, i.e. material.)

### Peak RSS, MiB (median of 6; deltas in %, + = larger, − = smaller)

| scenario | main | dd-w1 | dd-w2b | Δ w1/main | Δ w2b/w1 | Δ w2b/main | spread (max of the three) |
|---|--:|--:|--:|--:|--:|--:|--:|
| aggregate | 66.1 | 66.2 | 68.9 | +0.19% | +4.03% | +4.23% | 9.84% |
| json-parse | 96.2 | 97.0 | 102.0 | +0.88% | +5.15% | +6.07% | 47.03% |
| json-parse-access-log | 62.2 | 58.7 | 60.3 | −5.53% | +2.65% | −3.03% | 12.32% |
| json-parse-app-log | 61.9 | 61.8 | 61.3 | −0.17% | −0.75% | −0.92% | 9.91% |
| json-parse-nested-log | 66.4 | 66.9 | 66.6 | +0.79% | −0.44% | +0.34% | 25.56% |
| json-parse-x3 | 84.5 | 80.8 | 80.0 | −4.33% | −0.99% | −5.28% | 8.20% |
| passthrough | 68.6 | 66.7 | 67.1 | −2.83% | +0.67% | −2.18% | 8.32% |

No RSS delta exceeds its scenario's own spread — every RSS delta above is inside the noise floor.

### Reading

`dd/w1`'s hand-rolled `DdSketch` store costs +4.2% on `aggregate` and +4.4% on `json-parse`, the
only two CPU deltas in either table that clear their own spread. Both scenarios have a sketch on
the hot path — `aggregate` sketches every series, and `json-parse`'s `kv_metrics` stage sketches a
`Samples` metric at the sink — while every scenario without one (`passthrough`,
`json-parse-access-log`, `-app-log`, `-nested-log`, `-x3`) is flat within noise end to end, `main`
to `dd-w2b`. This is accepted: bin-for-bin Datadog parity needs the Agent's own bin mapping, and a
cheaper store that didn't match it would relay a sketch that reads differently at Datadog's end
([`docs/known-gaps.md`](../known-gaps.md#datadog)'s sketch-store entry).

`dd/w2b`'s `serde_json` `float_roundtrip` feature is flat within noise on every `json-parse*`
scenario: the one delta that clears spread at that step, `json-parse-access-log`'s −1.89% against a
1.61% spread, is a speedup, not the slowdown the feature's ~2×-on-float-parsing cost would predict,
and no other `json-parse*` scenario moves with it. `docs/known-gaps.md`'s `float_roundtrip` entry
is closed on this evidence: the feature stays enabled workspace-wide with no measurable cost.

Peak RSS is uninformative here: every delta, at every step, falls inside that scenario's own
spread, including two very wide ones (`json-parse`'s 47% spread, `json-parse-nested-log`'s 26%). No
RSS conclusion should be drawn from this session.

## 10. `dos` stack: decoder bounds, before/after (2026-09-28)

**Result: the `dos` stack costs +2.6% on `native-relay` and about 1% on `encode-native-devnull`
and `json-parse`. Accepted, no action.**

**Protocol.** `script/vm build` built two binaries: `53eb70d05f37` (`main` at #379, before the
`dos` stack) and `f158106ff88e` (`main` at #381, the `dos` stack and nothing else between them),
sha256 prefixes `20b2d72488a5` and `e1f1f11bbebd`. Each scenario ran two interleaved rounds, A/B
then A/B, of `script/perf run --repeat 3 --profile release --no-build --logit-bin
perf/bins/<slug>/logit`. The six repeats per binary pool into one median of 6, with
`spread = (max − min) / min` over those six, the same reduction §9 uses. Box facts match the
preamble.

CPU µs/event, median of 6. A bold Δ clears the larger of the two spreads.

| scenario | before (`53eb70d05f37`) | after (`f158106ff88e`) | Δ | spread (max of the two) | n |
|---|--:|--:|--:|--:|--:|
| encode-native-devnull | 0.935 | 0.948 | **+1.37%** | 0.82% | 6/6 |
| json-parse | 0.916 | 0.926 | **+1.06%** | 0.71% | 6/6 |
| logfmt-parse | 0.998 | 1.004 | +0.54% | 0.70% | 6/6 |
| native-relay | 1.340 | 1.375 | **+2.60%** | 1.73% | 6/6 |
| passthrough | 0.325 | 0.325 | −0.24% | 0.72% | 6/6 |

### Reading

`native-relay` +2.6%, `encode-native-devnull` +1.4%, and `json-parse` +1.1% clear their sub-2%
spreads; `passthrough` and `logfmt-parse` are flat. The native rows are the native codec's bounds
paying for their checks: a writer-side cap on encode, and canonical-varint and trailing-byte checks
on decode
([ADR `untrusted-input-bounds`](../adr/untrusted-input-bounds.md),
[ADR `deployment-threat-model`](../adr/deployment-threat-model.md)); this session didn't attribute
`json-parse`'s +1.1%. The cost is accepted.

## Open questions

- **When and how the harness runs during development is deliberately undecided**, as
  [ADR `load-test-harness`](../adr/load-test-harness.md)'s open question says: nightly, manually
  triggered, gating a PR on `compare --threshold`, or another cadence. Nothing wires it into CI, a
  pre-merge gate, or a schedule yet.
- **`buffered`'s variance is resolved on the harness side.** W8 (#165) clears a scenario's declared
  `buffer.disk.path` before every spawn, and both a quiet-laptop and a reference-VM `--repeat 5`
  confirmation (§3) show no decay and flat RSS. The VM pass narrowed the remaining spread to 7% on
  events/s and under 1% on CPU µs/event. What's left is product-side and small:
  `DiskQueue::open`'s double-read startup scan (`docs/known-gaps.md`'s `buffered` entry) is no
  longer a prime suspect for anything, just an unquantified detail.
- **A variance-aware `compare` threshold is possible future work with no current motivating case.**
  `aggregate`'s laptop-era ~±25% repeat-to-repeat spread, the reason this item was opened, doesn't
  reproduce on the reference VM (§1's noise sub-section: two independent 5-repeat samples agree to
  within 0.7%). Gating on each file's `min` instead of the median, or per-scenario thresholds,
  remain options if a future scenario needs them; nothing in the suite does today
  (`docs/known-gaps.md`'s harness entry).
- **Templated metric names permanently grow the process-wide interner**, one entry per distinct
  rendering, for the life of the process (`generate_in`'s module doc; `docs/design/memory.md` §4).
  `generate_in` refuses a bare `{seq}` in a metric name for this reason, and every shipped scenario
  that wants cardinality templates an *attribute* instead (`aggregate.yaml`'s
  `host: h{seq%1000}`, not a metric name), so nothing here exercises the metric-name path at scale.
  Whether that path is worth keeping, given every real scenario avoids it, is an open question.
