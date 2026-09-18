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
| W0 | **Landed** (#250). **Docs only.** This ADR and plan. | `docs/adr/udp-intake-batching-and-socket-visibility.md` (+ row atop `docs/adr/README.md`); `docs/plans/udp-intake.md` (this file, + row atop `docs/plans/README.md`) | — | Every relative link resolves; both docs follow `docs/adr/TEMPLATE.md`'s headings exactly; both README indexes gain a row in `created`-order. `script/cibuild` run once per the worker brief even though scope is docs-only. |
| W1 | **Landed** (#251). **Per-socket kernel visibility.** `SockMeminfo`/`meminfo`/`listen_queue`/`DropCounter` in a new `logit-pipeline` module (not `logit-core`, whose doc rules out I/O; see the ADR's "`sockstat` lives in `logit-pipeline`"); a new `read_loop_sampled` wrapper holding a pinned `read_loop` and a 1 s interval in one new, looped `select!` — distinct from `run_until_shutdown`'s existing one-shot read-vs-decode select and from `read_loop`'s own two per-iteration selects, so a tick never cancels an in-flight `recv_from`/`push` the way a third arm on either of those would (ADR "Sampling cadence" section) — with a guaranteed final sample after `read_loop` exits; a `listen_queue` sample in the TCP accept loop; six new `logit.input.*` metrics. | `crates/logit-pipeline/src/sockstat.rs` (new, `pub mod`); `crates/logit-pipeline/Cargo.toml` (Linux-target `libc` dep); `crates/logit-inputs/src/udp.rs` (`read_loop_sampled`, inline sync `sample_once`); `crates/logit-inputs/src/tcp.rs` (accept-loop + interval sampling); `docs/design/internal-telemetry.md:450-475`-area rows + naming note; `docs/deploying.md` "Listener intake" section; `docs/known-gaps.md` (close `:176-184`, add two follow-up entries per the ADR's "not built" list) | `DropCounter` wrap/baseline unit tests; a Linux-gated loopback test that overruns a tiny `receive_buffer_bytes` and asserts `kernel.drops > 0` (never an exact count — kernel timing isn't deterministic); a TCP listener test asserting the accept-queue depth/limit/utilization gauges appear and agree. | `script/cibuild` green. Manual, not automated: run `statsd_in` with a tiny `receive_buffer_bytes`, flood it, and compare `logit.input.kernel.drops` against `/proc/net/udp`'s `drops` column for the same socket (same host, same run) — the cross-check the ADR's `SO_MEMINFO`-vs-procfs comparison predicts should match. |
| W2 | **Landed** (#252). **Real-socket UDP load in `logit-perf`.** `Workload::{Generated{count}, Driven(LoadSpec)}` on `Scenario`; `load.rs` (spec parsing, `target_addr`, `sendmmsg`-batched blasting, `--pin-sender`/`--pin-child`); `telemetry_leg.rs` hoisted out of `attribute.rs` unchanged, reused by both `run` and `attribute`; optional `UdpSample` fields on `Sample`/`compare`; three scenario/load-spec pairs sharing one calibrated traffic model; `tools/record-fixtures` extended with real statsd/DogStatsD clients, a small committed capture + provenance. | `crates/logit-perf/src/{scenario.rs,run.rs,load.rs,telemetry_leg.rs,result.rs,compare.rs}`; `perf/scenarios/udp-statsd{,-small,-packed}.yaml`; `perf/load/udp-statsd{,-small,-packed}.yaml` + `perf/load/README.md`; `tools/record-fixtures/raw_capture.py` extension + committed capture; `docs/design/performance.md` (repro table row); `docs/adr/load-test-harness.md`/`docs/plans/load-test-harness.md` (note the `Driven` extension) | Spec parsing; the four scenario/sidecar combinations (`Generated`, `Driven`, both-present error, neither-present error); `target_addr` (raw-YAML `bind` read, `!env`/port-0 rejection); ring cardinality; weighted line/packing mix honoured within tolerance; packed datagrams never exceed their size target; every rendered line decodes cleanly through `StatsdDecoder`; the committed real-client capture decodes cleanly too; the zero-drop self-check (a paced run delivers exactly the expected count with zero decode-error telemetry). | `script/cibuild` green; `script/perf run --scenario udp-statsd{,-small,-packed} --repeat 5 --pin-sender <cpu> --pin-child <cpu>` — the first recorded numbers for this family, and the worked example of the format (see "Baseline/delta protocol" below -- each later workstream takes its own interleaved pair rather than diffing against this file); one full `script/perf run` proving no existing scenario's numbers moved; `script/perf attribute --scenario udp-statsd` still works (the telemetry leg stays run-time-only). |
| W3 | **Landed** (#253). **`BoundedQueue::push_many`/`pop_many`.** Drains a `&mut Vec<T>` (`push_many`) / fills one (`pop_many`); per-item weight/overflow/drop counting and per-item `Block` waiting preserved; a `Peekable<Drain>` held across the await so cancellation leaves the caller's `Vec` empty; one lock per run of fitting items; one `update_gauges` per call; one `notify_one` per call, documented as an honest observation (`Notify` stores one permit) rather than a latency win. `decode_loop` pops into a reused `Vec<Datagram>` at a constant matching `read_batch`'s default until W4 threads the real config value through; `receive.latency` stays per-datagram. | `crates/logit-pipeline/src/queue.rs`; `crates/logit-inputs/src/udp.rs` (`decode_loop`'s pop call site); `crates/logit-bench/tests/allocations.rs` (new `receive_queue_push_many_then_pop_many_costs_nothing` pin) + `docs/design/memory.md` (same commit); `docs/known-gaps.md` (close `:200-214`) | Tests mirroring the existing 18 `BoundedQueue` tests, extended for the `_many` pair: each overflow policy, `Block` prefix-then-wait, an impossible-to-fit item, cancellation accounting (remainder left in/dropped from the `Vec` correctly for each direction), `max` respected by `pop_many`, exactly one gauge update per call. | `script/cibuild` green. An interleaved pinned parent/branch pair over `udp-statsd`, in one sitting — the plan's first **delta** (see the protocol below). |
| W4 | **Landed** (#254). **`recvmmsg(2)`.** Linux-gated `BatchReader` (`vlen × 65,507`-byte slab) built on `socket.async_io(Interest::READABLE, ...)` around `libc::recvmmsg(MSG_DONTWAIT)`; `mmsghdr`/`iovec` arrays built inside the closure each call (no raw pointer held across an await, future stays `Send`); one `now_nanos()` per batch; existing per-datagram `Bytes::copy_from_slice` pin untouched; `logit.input.reads` (new); `queue.push_many` call site. Config `read_batch: usize` (default 64) on `ReceiveConfig`/`UdpListenerConfig`; graph rule 57 (reject `> 1024`, alongside existing rules 17/18). | `crates/logit-inputs/src/udp.rs` (`BatchReader`, `read_batch`); `crates/logit-config/src/lib.rs:~2800`-area (`read_batch` field); `crates/logit-cli/src/pipeline.rs:1145`-area (`receive_config` mapping); `crates/logit-pipeline/src/graph.rs` (rule 57); `schema/logit.schema.json` (regenerated); `docs/deploying.md`, `docs/design/internal-telemetry.md` (`logit.input.reads`); `docs/design/memory.md` (slab RSS row, measured not assumed); `docs/known-gaps.md` (close `:185-191`) | Real-loopback-socket tests in `udp.rs`'s existing `mod tests` style: a 200-datagram burst delivered in order at `read_batch: 64`; `read_batch: 1` vs. `64` event-stream parity; shutdown mid-batch; `reads` vs. `datagrams` counter relationship; graph tests for rule 57 (and the existing 17/18 still passing alongside it). | `script/cibuild` green; `script/schema` no-diff after commit. An interleaved pinned parent/branch pair over `udp-statsd{,-small,-packed}`, in one sitting — the plan's second **delta**. A `read_batch` sweep at 16/32/64/128, pinned, recorded as the evidence the ADR's default-64 placeholder is waiting on; the ADR gets that evidence filled in as part of this workstream (confirming 64 or revising it). |
| W5 | **Landed (this PR).** **Closeout.** Numbers from W2 (baseline), W3 (delta), and W4 (delta) written up together: CPU µs/event, delivered events/s, drop rates, and max kernel-`rcvbuf` utilization, for all three scenarios. `docs/known-gaps.md` verified against what actually landed (not just what was planned); ADR status/consequences checked against the real file list; `AGENTS.md`'s current-state paragraph updated; this plan's own closing assessment written. | `docs/design/performance.md` (new section); `docs/known-gaps.md`; `docs/adr/udp-intake-batching-and-socket-visibility.md` (Status/Consequences, if anything drifted from plan during W1-W4); `AGENTS.md`; `docs/plans/udp-intake.md` (this file's own closing assessment) | — | The three numbers in `performance.md`'s new section trace back to real `perf/results/*.json` files (paths noted, files themselves not committed — see below); every `docs/known-gaps.md` entry this plan named as closing is actually gone or explicitly left open with a stated reason. |

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
    statsd-app.yaml           # the shared line model all three specs reference via `model:`
    udp-statsd.yaml           # LoadSpec: target, datagrams, sockets, rate, model:, datagram_mix:
    udp-statsd-small.yaml     # all single-line packing
    udp-statsd-packed.yaml    # all packed-to-≤1432B

testdata/interop/statsd/     # the calibration capture -- NOT under perf/load/
  README.md                  # provenance table: client versions, invocation, what each file shows
  statsd-*.raw               # one file per captured UDP datagram
```

**The capture lives with the rest of the recorded-interop corpus, not beside the load specs.** An
earlier draft of this section put it under `perf/load/captures/`; it belongs under
`testdata/interop/statsd/`, where `docs/plans/recorded-interop-fixtures.md`'s conventions already
say statsd producer fixtures go — provenance table, `script/record-fixtures` regeneration, and
`interop_fixture_*` tests in the decoder crate. It does double duty there: it closes that plan's
owed statsd fixtures *and* backs this one's traffic model. The ADR says the same.

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

**Revised 2026-09-18, from measurement.** The original plan here was for W2 to record one baseline
that W3 and W4 would each diff against later. That does not survive contact with this box: the same
specs, the same commit and the same pins, re-run 90 minutes further into a benchmarking session,
moved CPU µs/event by ~23% and `udp-statsd-small`'s drop rate from 3.1% to 12.4%. It was checked
rather than assumed — re-running the *earlier* commit immediately afterwards reproduced the *later*
numbers — so it is the machine settling into a lower sustained power state, not anything in the
code. A stored baseline from another session is therefore not a control; it is a second variable.

So every workstream takes **its own before/after pair, in one sitting, interleaved**:

1. Check out the parent, run the three scenarios; check out the branch, run them; then **repeat
   that pair at least once more** (parent, branch, parent, branch …). Interleaved rather than
   all-of-one-then-all-of-the-other, because the drift is monotonic within a session: two blocks
   would put all of one side's runs on the cool half of the box and all of the other's on the warm
   half, which is exactly the artifact this is guarding against. Consistency across the pairs is the
   evidence the delta is real.
2. **`script/perf compare <parent.json> <branch.json>`** within that session only. Never against a
   JSON from another day — `compare` warns on a host/CPU-model mismatch, but it cannot see this.
3. `receive_buffer_bytes` and the specs' `rate` stay **unchanged** across a pair, so the comparison
   isolates the code change. `--rate-scale` is how a run reads a different operating point without
   editing a spec; the results file records the effective rate and `compare` warns when two runs
   used different ones.
4. **The box has to be in a fit state**, or the pair is measuring the box. The checklist is in
   [`perf/load/README.md`](../../perf/load/README.md)'s "Box state" section; `logit-perf run` now
   records what it can of it (governor, energy-performance preference, platform profile, AC-online)
   into the results file's preamble and warns loudly on `powersave` or battery.
5. **`docs/design/performance.md`'s tables are labelled by session** — which pair, taken when, on
   what box state — rather than presented as one running series across days.

W2's own recorded numbers are a baseline in this sense: a starting point and a worked example of
the format, not a stored control for W3 to diff against months later.

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

End to end, once the stack lands: each of W2, W3 and W4 takes its own interleaved parent/branch
pair over `udp-statsd{,-small,-packed}` at `--repeat 5 --pin-sender … --pin-child …`, in one
sitting, per the protocol above -- never diffed against a stored JSON from another session. Plus one
full `script/perf run` at W2 proving no pre-existing scenario's numbers moved; `script/perf
attribute --scenario udp-statsd` still works, confirming the telemetry leg stays run-time-only and
never lands in the checked-in YAML. W1's kernel-drop visibility gets the manual cross-check against `/proc/net/udp` described in
its own table row, since no automated test can assert an exact kernel drop count.

## Execution mechanics

Lead plus sub-agent workers in isolated worktrees. Model assignment: W1/W3/W4 touch unsafe code and
queue/cancellation concurrency semantics, so they get the more capable model; W0/W2/W5 are docs,
harness/config plumbing, and closeout, so they get a lighter one. `pr-review --comment` on every PR
touching non-harness code (W1, W3, W4); a lighter, delegated review on W0, W2, W5. Stack-internal
syncs are `merge udp/w<N> into udp/w<M>` merge commits, never rebases, per `AGENTS.md`'s "Workflow"
section. Allocation pins and their `docs/design/memory.md` rows land in the same commit as the code
that changes them, never split across commits.

## Closing assessment

The stack is complete and pushed (W0 #250, W1 #251, W2 #252, W3 #253, W4 #254, W5 this PR), stacked
PRs only, unmerged — Ross directs merging, per the "Settled decisions" section above. What follows is
the retrospective this plan's own W5 row asked for.

**What review caught that tests did not:**

- **The sampler-starvation reordering — found only by measuring, not by reasoning about the code.**
  The ADR's "Sampling cadence" section names the intuitive design as the wrong one: a timer arm
  polled *after* the read arm in `read_loop_sampled`'s `select!` looks like the natural choice
  ("prefer the work, sample while idle"), and it is the one that silences the sampler for the entire
  duration of the overload it exists to report, because `tokio`'s per-poll coop budget hands the read
  arm every unit before a later-polled timer arm ever sees `Pending` for a real reason. No unit test
  written against the intended behavior would have caught this — both orderings pass every test that
  doesn't specifically flood a socket and count which windows see the drop counter move. It surfaced
  from running the measurement W1 needed anyway (eight senders flooding one listener, read-arm-first
  carrying the gauges in 0 of 10 one-second windows against timer-arm-first's 11 of 11), not from
  code review of the `select!` itself.
- **`push_many`'s lost wakeup — found by directing reviewers at a suspected interleaving, not by the
  exhaustive equivalence test.** W3's 3,750-pair batched-vs-expanded-singles model check is genuinely
  exhaustive over its own operation set, and it still didn't find the bug, because the deadlock needs
  a `Block`-policy consumer parked *before* a `push_many` call that admits a prefix and then waits —
  an interleaving the model test's `Block`-exclusion (recorded in its own doc comment) put out of
  reach. It was found in review by asking specifically whether a batch bigger than the queue's free
  room could leave a consumer parked behind an unsent wakeup, then confirmed with a targeted test
  before the fix, not discovered by the broad test suite running.
- **The crate-placement rule for `sockstat`.** The ADR's first draft put `sockstat` in `logit-core`,
  on the strength of "no I/O crate should own another crate's fd". Review measured that argument
  against `logit-core`'s own stated boundary ("no I/O, no pipeline, no protocol codecs live here")
  and `logit-outputs` as the foreseeable second consumer, and moved it to `logit-pipeline` instead —
  recorded in the ADR's own "`sockstat` lives in `logit-pipeline`" section as a decision that changed
  during design, not a build-time drift (see the ADR's "As built" section for the one place a PR
  description still said the old answer after the code had the new one).
- **The `influxdb_out` cross-batch timestamp collision.** The ADR's own account says the `+ i`
  per-datagram offset "was not in this ADR's first draft" — a single `received_at` per read batch
  was the original design, and it was reviewed against `influxdb_out`'s `allocate_timestamp`
  disambiguation specifically (batch-scoped, cleared at the top of every `Encoder::encode`) before
  landing, which is what turned up that a whole batch sharing one instant would routinely produce
  same-series same-timestamp points straddling an output-batch boundary with nothing to disambiguate
  them. No test forced this — `statsd_round_trip.rs` and friends exercise one output batch at a time,
  never two adjacent ones sharing a UDP read batch's tail and head.

**What the harness made possible.** Before W2, no scenario in `crates/logit-perf` touched a real
socket, so every claim about syscall overhead, gauge-lock contention, or kernel drops was an
argument, not a number. `udp-statsd{,-small,-packed}`'s delivered-side denominators, the
interleaved-pair-plus-control-drift protocol, and the calibrated traffic model together are what let
W3 and W4 each report a signal-vs-drift verdict per scenario instead of a single number nobody could
tell was real: `udp-statsd-small`'s result at both W3 and W4 cleared the control-to-control drift by
several times over, and `udp-statsd`/`udp-statsd-packed` staying inside drift at both workstreams is
itself the finding (the decode-bound scenarios have little of this family's cost left to save), not
an absence of one. `docs/design/performance.md` §7 is where that read-out lives.

**Residual debt**, tracked in `docs/known-gaps.md` unless noted otherwise:

- **`docs/design/performance.md` §7's "Open after this workstream" list**: the +7–32% peak-RSS rise
  on `udp-statsd`/`udp-statsd-packed` at W4 has no settled attribution yet (the slab is ruled out; a
  1 s probe's queue-depth reading argues against the working "more bytes in flight" guess as much as
  for it); and every number in this plan's tables is provisional, taken on a throttling laptop on
  battery, pending the lead's re-take on a stable box.
- **"A UDP listener's read and decode loops share one task"** (`docs/known-gaps.md`, new this
  workstream) — W4's report-only, unshipped experiment found real headroom in splitting them
  (`udp-statsd-small` CPU/event −12%, `udp-statsd` kernel drops to zero) at a real cost (+5.6%
  CPU/event on `udp-statsd`, ~3× peak RSS with nothing pacing the reader), and needs a
  join-handle/cancellation redesign of `run_until_shutdown`'s load-bearing two-arm select, a
  `'static` decoder, and a `Fanout`-ownership answer before it's buildable.
- **`SO_REUSEPORT` multi-reader and `UDP_GRO`** stay deliberately deferred — this plan's own premise
  was "measure first, then decide what's still worth building," and the prerequisite measurement
  (whether one reader is still the bottleneck after `recvmmsg`) is now in hand rather than assumed.
  `SO_REUSEPORT` shares the shutdown-cascade and `Fanout`-ownership redesign the entry above needs,
  so the two should be designed together, not separately.
- **UDP sink send-error counting by errno** (`statsd_out`/`syslog_out`/`graphite_out`/`collectd_out`)
  is named and not built — `SockMeminfo`'s send-side fields are read but not emitted as metrics,
  since a UDP send-buffer gauge is ~always zero and the errno at the call site is the real signal.
- **A cloud-VM (or otherwise plugged-in, cooled, dedicated) perf rig is under consideration** for
  this family specifically, not just as general harness hygiene — `udp-statsd*`'s drop rate is
  deliberately the difference between two nearly-equal rates, which is exactly the kind of number a
  laptop on battery under thermal throttle cannot hold still long enough to trust without the
  interleaved-pair discipline this plan had to invent to work around it.
