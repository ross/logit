---
created: 2026-09-18
updated: 2026-09-18
---

# Enabling plan: UDP intake batching and socket visibility

## Context

[ADR `udp-intake-batching-and-socket-visibility`](../adr/udp-intake-batching-and-socket-visibility.md)
decides the shape of this effort: per-socket kernel visibility via `getsockopt(SO_MEMINFO)` (not
procfs) plus `TCP_INFO` on `LISTEN` sockets; a real-socket UDP scenario family in `logit-perf`,
driven by a traffic model calibrated against a recorded real-client capture, denominated over events
*delivered*, not sent; `BoundedQueue::push_many`/`pop_many` to stop per-datagram gauge-lock
contention on the receive side; and `recvmmsg(2)` batched reads on Linux, `read_batch` default 64
pending a sweep. This plan is the concrete build-out: which workstream builds what, in which files,
verified how, and the order the measurement work has to happen in so every later step reports a
measured delta instead of a guess. Read the ADR first — this document doesn't repeat its reasoning,
only its consequences and sequencing.

Stream key **`udp`**. Branches `udp/w0`…`udp/w5`, a linear stack — each workstream's branch is cut
from its parent workstream's branch, and its PR targets that branch, retargeted to `main` once the
parent merges (`AGENTS.md`'s "Branches and PR titles"). PR stack only: nothing in this plan merges
to `main` on its own initiative; Ross directs merging. No AI attribution anywhere, per the same
convention every other stream in this repo follows.

`docs/known-gaps.md` carries three entries this plan closes, on different workstreams:
**"No visibility into the kernel's own UDP receive-buffer drops"** (`:176-184`, W1), **"A UDP
listener reads one datagram per syscall"** (`:185-191`, W4), and **"A `ReceiveQueue`'s
depth/bytes/utilization gauges update on every datagram, not every batch"** (`:200-214`, W3). The
fourth related entry, **"One reader per UDP listener"** (`:192-199`, `SO_REUSEPORT`), stays open —
the ADR's "out of scope" section explains why.

## Workstreams

| # | Scope | Files | Tests | Verification |
|---|---|---|---|---|
| W0 | **Docs only.** This ADR and plan. | `docs/adr/udp-intake-batching-and-socket-visibility.md` (+ row atop `docs/adr/README.md`); `docs/plans/udp-intake.md` (this file, + row atop `docs/plans/README.md`) | — | Every relative link resolves; both docs follow `docs/adr/TEMPLATE.md`'s headings exactly; both README indexes gain a row in `created`-order. `script/cibuild` run once per the worker brief even though scope is docs-only. |
| W1 | **Per-socket kernel visibility.** `SockMeminfo`/`meminfo`/`listen_queue`/`DropCounter` in a new `logit-core` module; a sampled read loop wrapper pinning the socket-stats timer alongside `read_loop`/`decode_loop` in the same `select!`, with a guaranteed final sample after `read_loop` exits; a `listen_queue` sample in the TCP accept loop; five new `logit.input.*` metrics. | `crates/logit-core/src/sockstat.rs` (new, `pub mod`); `crates/logit-core/Cargo.toml` (Linux-target `libc` dep); `crates/logit-inputs/src/udp.rs` (`read_loop_sampled`, inline sync `sample_once`); `crates/logit-inputs/src/tcp.rs` (accept-loop + interval sampling); `docs/design/internal-telemetry.md:450-475`-area rows + naming note; `docs/deploying.md` "Listener intake" section; `docs/known-gaps.md` (close `:176-184`, add two follow-up entries per the ADR's "not built" list) | `DropCounter` wrap/baseline unit tests; a Linux-gated loopback test that overruns a tiny `receive_buffer_bytes` and asserts `kernel.drops > 0` (never an exact count — kernel timing isn't deterministic); a TCP listener test asserting the accept-queue depth/backlog gauges appear. | `script/cibuild` green. Manual, not automated: run `statsd_in` with a tiny `receive_buffer_bytes`, flood it, and compare `logit.input.kernel.drops` against `/proc/net/udp`'s `drops` column for the same socket (same host, same run) — the cross-check the ADR's `SO_MEMINFO`-vs-procfs comparison predicts should match. |
| W2 | **Real-socket UDP load in `logit-perf`.** `Workload::{Generated{count}, Driven(LoadSpec)}` on `Scenario`; `load.rs` (spec parsing, `target_addr`, `sendmmsg`-batched blasting, `--pin-sender`/`--pin-child`); `telemetry_leg.rs` hoisted out of `attribute.rs` unchanged, reused by both `run` and `attribute`; optional `UdpSample` fields on `Sample`/`compare`; three scenario/load-spec pairs sharing one calibrated traffic model; `tools/record-fixtures` extended with real statsd/DogStatsD clients, a small committed capture + provenance. | `crates/logit-perf/src/{scenario.rs,run.rs,load.rs,telemetry_leg.rs,result.rs,compare.rs}`; `perf/scenarios/udp-statsd{,-small,-packed}.yaml`; `perf/load/udp-statsd{,-small,-packed}.yaml` + `perf/load/README.md`; `tools/record-fixtures/raw_capture.py` extension + committed capture; `docs/design/performance.md` (repro table row); `docs/adr/load-test-harness.md`/`docs/plans/load-test-harness.md` (note the `Driven` extension) | Spec parsing; the four scenario/sidecar combinations (`Generated`, `Driven`, both-present error, neither-present error); `target_addr` (raw-YAML `bind` read, `!env`/port-0 rejection); ring cardinality; weighted line/packing mix honoured within tolerance; packed datagrams never exceed their size target; every rendered line decodes cleanly through `StatsdDecoder`; the committed real-client capture decodes cleanly too; the zero-drop self-check (a paced run delivers exactly the expected count with zero decode-error telemetry). | `script/cibuild` green; `script/perf run --scenario udp-statsd{,-small,-packed} --repeat 5 --pin-sender <cpu> --pin-child <cpu>` — this is the **baseline** the rest of the stack diffs against (see "Baseline/delta protocol" below); one full `script/perf run` proving no existing scenario's numbers moved; `script/perf attribute --scenario udp-statsd` still works (the telemetry leg stays run-time-only). |
| W3 | **`BoundedQueue::push_many`/`pop_many`.** Drains a `&mut Vec<T>` (`push_many`) / fills one (`pop_many`); per-item weight/overflow/drop counting and per-item `Block` waiting preserved; a `Peekable<Drain>` held across the await so cancellation leaves the caller's `Vec` empty; one lock per run of fitting items; one `update_gauges` per call; one `notify_one` per call, documented as an honest observation (`Notify` stores one permit) rather than a latency win. `decode_loop` pops into a reused `Vec<Datagram>` at a constant matching `read_batch`'s default until W4 threads the real config value through; `receive.latency` stays per-datagram. | `crates/logit-pipeline/src/queue.rs`; `crates/logit-inputs/src/udp.rs` (`decode_loop`'s pop call site); `crates/logit-bench/tests/allocations.rs` (new `receive_queue_push_many_then_pop_many_costs_nothing` pin) + `docs/design/memory.md` (same commit); `docs/known-gaps.md` (close `:200-214`) | Tests mirroring the existing 18 `BoundedQueue` tests, extended for the `_many` pair: each overflow policy, `Block` prefix-then-wait, an impossible-to-fit item, cancellation accounting (remainder left in/dropped from the `Vec` correctly for each direction), `max` respected by `pop_many`, exactly one gauge update per call. | `script/cibuild` green. Pinned `udp-statsd` run vs. the W2 baseline — the plan's first **delta** (see below). |
| W4 | **`recvmmsg(2)`.** Linux-gated `BatchReader` (`vlen × 65,507`-byte slab) built on `socket.async_io(Interest::READABLE, ...)` around `libc::recvmmsg(MSG_DONTWAIT)`; `mmsghdr`/`iovec` arrays built inside the closure each call (no raw pointer held across an await, future stays `Send`); one `now_nanos()` per batch; existing per-datagram `Bytes::copy_from_slice` pin untouched; `logit.input.reads` (new); `queue.push_many` call site. Config `read_batch: usize` (default 64) on `ReceiveConfig`/`UdpListenerConfig`; graph rule 57 (reject `> 1024`, alongside existing rules 17/18). | `crates/logit-inputs/src/udp.rs` (`BatchReader`, `read_batch`); `crates/logit-config/src/lib.rs:~2800`-area (`read_batch` field); `crates/logit-cli/src/pipeline.rs:1145`-area (`receive_config` mapping); `crates/logit-pipeline/src/graph.rs` (rule 57); `schema/logit.schema.json` (regenerated); `docs/deploying.md`, `docs/design/internal-telemetry.md` (`logit.input.reads`); `docs/design/memory.md` (slab RSS row, measured not assumed); `docs/known-gaps.md` (close `:185-191`) | Real-loopback-socket tests in `udp.rs`'s existing `mod tests` style: a 200-datagram burst delivered in order at `read_batch: 64`; `read_batch: 1` vs. `64` event-stream parity; shutdown mid-batch; `reads` vs. `datagrams` counter relationship; graph tests for rule 57 (and the existing 17/18 still passing alongside it). | `script/cibuild` green; `script/schema` no-diff after commit. Pinned `udp-statsd{,-small,-packed}` run vs. the W3 delta — the plan's second **delta**. A `read_batch` sweep at 16/32/64/128, pinned, recorded as the evidence the ADR's default-64 placeholder is waiting on; the ADR gets that evidence filled in as part of this workstream (confirming 64 or revising it). |
| W5 | **Closeout.** Numbers from W2 (baseline), W3 (delta), and W4 (delta) written up together: CPU µs/event, delivered events/s, drop rates, and max kernel-`rcvbuf` utilization, for all three scenarios. `docs/known-gaps.md` verified against what actually landed (not just what was planned); ADR status/consequences checked against the real file list; `AGENTS.md`'s current-state paragraph updated; this plan's own closing assessment written. | `docs/design/performance.md` (new section); `docs/known-gaps.md`; `docs/adr/udp-intake-batching-and-socket-visibility.md` (Status/Consequences, if anything drifted from plan during W1-W4); `AGENTS.md`; `docs/plans/udp-intake.md` (this file's own closing assessment) | — | The three numbers in `performance.md`'s new section trace back to real `perf/results/*.json` files (paths noted, files themselves not committed — see below); every `docs/known-gaps.md` entry this plan named as closing is actually gone or explicitly left open with a stated reason. |

Landing order is the linear stack itself: **W0 → W1 → W2 → W3 → W4 → W5**. W1 (kernel visibility)
has no dependency on W2's harness and could in principle land independently, but stays after W0 in
the stack rather than branching separately, since nothing else in this plan needs it to move faster
than the stack does. W2 must precede W3/W4 — there is no delta to measure without a baseline scenario
that touches a real socket. W3 and W4 are independently scoped (`push_many`/`pop_many` doesn't need
`recvmmsg` to exist, and vice versa) but stay sequential in the stack, each diffing against the
state immediately before it, so the measured deltas stay attributable to one change at a time rather
than a combined W3+W4 number with no way to tell which half contributed what.

## Scenario and load-spec file layout

UDP scenarios need a sidecar the existing scenario format has no room for — a `logit` config alone
says nothing about *what traffic to send it*, and both `script/validate:32` and
`crates/logit-cli/src/config.rs:234-240` already glob `perf/scenarios/*.yaml` unconditionally, so a
load spec can't live there without being mistaken for a second scenario config.

```
perf/
  scenarios/
    udp-statsd.yaml          # statsd_in (fixed loopback port) -> null_out, ordinary validating config
    udp-statsd-small.yaml    # same graph, distinct bind port
    udp-statsd-packed.yaml   # same graph, distinct bind port
  load/
    README.md                # the traffic model: how it's derived from the capture, how to regenerate it
    udp-statsd.yaml           # LoadSpec: target, datagrams, sockets, rate, lines:, datagram_mix:
    udp-statsd-small.yaml     # all single-line packing
    udp-statsd-packed.yaml    # all packed-to-≤1432B
    captures/
      statsd-dogstatsd-buffered.pcap-or-equivalent   # tools/record-fixtures output + provenance note
      statsd-plain-unbuffered.pcap-or-equivalent
```

Each scenario config under `perf/scenarios/` is an ordinary validating `statsd_in → null_out` graph
with **no `generate_in`** — the load comes from the sidecar spec instead, driven by `logit-perf`'s
own sender. A future `syslog`/`graphite` UDP scenario is one new YAML pair under this same layout,
no harness change, since `load.rs`'s spec format is protocol-agnostic (`target`, `datagrams`,
`sockets`, `rate`, `lines:`, `datagram_mix:` — the wire syntax inside `lines:` is the only
protocol-specific part). `perf/load/README.md` records the calibration method: which capture backs
which spec, what was measured from it (type-mix ratios, tag cardinality, datagram-size weights), and
how to reproduce the capture with `tools/record-fixtures/raw_capture.py`'s extended clients.

## Baseline/delta recording protocol

Every number this plan records traces back to a real `perf/results/*.json` file and states which CPUs
it pinned to — the same discipline `docs/design/performance.md` already holds every other scenario
to, extended with the pin requirement this dev box's heterogeneous cores (Zen 5 performance vs. Zen
5c efficiency) make necessary for a repeatable number.

1. **W2 establishes the baseline.** `script/perf run --scenario udp-statsd{,-small,-packed} --repeat
   5 --pin-sender <cpu> --pin-child <cpu>`, with `receive_buffer_bytes`/`rate` tuned so the baseline
   sits in a regime with a **small, non-zero drop rate** — the regime the later workstreams' fixes
   are meant to move, not a zero-drop run that couldn't show a difference either way. The container's
   `rmem_max` is recorded alongside the numbers, since it bounds what `receive_buffer_bytes` can
   actually request.
2. **W3 records a delta against the W2 baseline.** Same scenarios, same pins, same
   `receive_buffer_bytes`/`rate` tuning (unchanged, so the comparison isolates `push_many`/`pop_many`'s
   effect); `script/perf compare <w2.json> <w3.json>` reports the drop-rate delta alongside CPU
   µs/event, never folded into `is_regression`, per the ADR's harness decisions.
3. **W4 records a second delta against the W3 result**, plus the 16/32/64/128 `read_batch` sweep
   (its own set of pinned runs, feeding the ADR's evidence placeholder).
4. **W5 writes the three numbers up together** into a new `docs/design/performance.md` section —
   baseline / W3 delta / W4 delta, side by side, for all three scenarios — following that document's
   existing convention of a dated, host-and-commit-stamped preamble.

**Artifacts never enter the repo.** Every `perf/results/*.json`, and any flamegraph taken along the
way, is written under `~/lib/logit/tmp/perf/udp/` — outside the worktree, following the same
gitignored-JSON/hand-curated-docs-table split `docs/design/performance.md` and ADR `load-test-harness`
already establish for every other scenario, just relocated out of the repo entirely per this plan's
own artifact-handling convention. `docs/design/performance.md`'s table cites the result file's name
and where it lives, the same way it already cites `perf/results/*.json` filenames today, without
that file being committed.

## Verification

Per PR: `script/check` during work; `script/cibuild` before opening (isolated `docker run` form from
worktrees, `cargo clean -p` for stale-cache issues); `script/validate` + `script/schema` on any
workstream touching config types (W2 adds scenario/load YAML, W4 adds `read_batch`).

End to end, once the stack lands: `script/perf run --scenario udp-statsd{,-small,-packed} --repeat 5
--pin-sender … --pin-child …` at W2 (baseline), W3, and W4, plus one full `script/perf run` at W2
proving no pre-existing scenario's numbers moved; `script/perf attribute --scenario udp-statsd`
still works, confirming the telemetry leg stays run-time-only and never lands in the checked-in
YAML. W1's kernel-drop visibility gets the manual cross-check against `/proc/net/udp` described in
its own table row, since no automated test can assert an exact kernel drop count.

## Execution mechanics

Lead plus sub-agent workers in isolated worktrees. Model assignment: W1/W3/W4 touch unsafe code and
queue/cancellation concurrency semantics, so they get the more capable model; W0/W2/W5 are docs,
harness/config plumbing, and closeout, so they get a lighter one. `pr-review --comment` on every PR
touching non-harness code (W1, W3, W4); a lighter, delegated review on W0, W2, W5. Stack-internal
syncs are `merge udp/w<N> into udp/w<M>` merge commits, never rebases, per `AGENTS.md`'s "Workflow"
section. Allocation pins and their `docs/design/memory.md` rows land in the same commit as the code
that changes them, never split across commits.
