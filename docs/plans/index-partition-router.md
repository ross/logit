---
created: 2026-10-23
updated: 2026-10-23
---

# Enabling plan: an index-partition router — a routed batch is a view, not a copy

## Context

A router pays for every event it touches. `route_batch` moves each event into a destination buffer
(0.4 µs per event on the perf VM) and clones one a `routes:` list sends to several destinations
(0.75 µs); `route_signals_batch` under `strip: true` splits each event into parts at the same cost.
Sharing the batch behind one `Arc` costs 0.04 to 0.12 µs per event, and that's what a destination
that only reads needs. The worker-count session recorded in
[`docs/design/performance.md`](../design/performance.md)'s "11. Runtime worker count (2026-10-23)"
section (PR #840) measured the ladder: `route-copy-all2` at 0.456 against `route-copy` and `route`,
the only difference being whether the router copies. The four variant scenarios it uses
(`route-copy-all2`, `route-copy-half2`, `route-2t`, `route-signals-nostrip`) are that session's
variants of the shipped `route`, `route-copy`, and `route-signals` scenarios and don't ship under
`perf/scenarios/`. A prototype that sends each destination the shared batch plus an index list
confirmed it on the same VM on 2026-10-23 (prototype branch `cores/e6b-prototype` at `6a306c9b`,
against `main` at `5d5a0429`, both sinking into `null_out`, 5 repeats per cell, free runtime). The
`main` column is that E6 run, so its `route-copy` and `route` figures (1.181 and 0.734) differ
slightly from the section's:

| Scenario | `main` µs/event | Prototype µs/event | CPU | Throughput |
|---|---:|---:|---:|---:|
| `route-copy` | 1.181 | 0.509 | −57% | +54% |
| `route-signals` | 1.093 | 0.379 | −65% | +88% |
| `route-copy-half2` | 1.016 | 0.458 | −55% | +36% |
| `route-2t` | 0.794 | 0.493 | −38% | +4% |
| `route` | 0.734 | 0.514 | −30% | −1% |
| `fanout`, `route-copy-all2`, `route-signals-nostrip` (share-only paths) | | | +1 to 2% | 0% |
| General suite (no router: parse, relay, tail, encode) | | | 0 to 2% | 0 to 2% |

The five router scenarios land at 0.38 to 0.51, at or just above the share floor (`route-copy-all2`
0.456, `route-signals-nostrip` 0.374). `route` and `route-2t` were bound by the generator, so their
gain shows on CPU alone. The raw results and the session's findings live outside the repository, in
`~/lib/logit/tmp/perf/vm-2026-10f/`, as every VM session's do.

Stream key: `cores` (continuing from `cores/w5`, this plan).

Goals:

- A router never touches event bytes: it sends each destination the shared batch, the indices
  of the events that destination gets, and the signals it may see.
- A sink encodes from that view with no copy, and can't read a payload the view doesn't show.
- A consumer that mutates events pays no more than today for the copy it needs, and nothing when
  it's the batch's only holder.
- A stalled destination can't pin unbounded whole batches it would never send.

Non-goals:

- Filters (`has_*`, `drop_*`, `sample`, `has_stale`) emitting narrowed views instead of owning
  their batch. They're index partitions too, and a later stream can make them so.
- Transforms reading a view in place. A transform mutates, so it owns.
- Deduplicating equal index lists across destinations. A list is 4 bytes per event per target
  against the 864 it replaces, and uniform marks send a whole view with no list at all.
- Any change to delivery semantics, the retry path's meaning, the spool format, or the wire. A
  retry resends the same view; a spool record or a hop frame encodes the slice.

## Design

### The view

`BatchView { batch: Arc<EventBatch>, indices: Option<Arc<[u32]>>, signals: Option<u8> }` in
`logit-pipeline` is the one thing a `Delivered` carries. `indices` is the events in view, as
ascending indices into `batch.events`, or `None` for every event; `signals` is the payloads in
view as `signal_index` bits, or `None` for every payload. An event in view always shows its
timestamp and attributes. Today's `Delivered::Owned` and `Delivered::Shared` collapse into a
whole view (`indices: None, signals: None`): one with a single holder is the move, one with
several is the share, and a partial view is what a router sends.

The view's iterator yields a masked event, not `&Event`: a type whose `log()`, `metrics()`, and
`span()` accessors apply `signals`, with the raw payload fields unreachable through it. That's
where `route_signals`'s `strip` contract lives, so no encoder can leak a metric to the logs
target by forgetting a check, and the compiler names every site the encoder sweep has to touch.

### Who copies

The rule is ownership, not reading:

- **Reads without owning, no copy:** a sink and its encoder, which take `&BatchView`; counting,
  which reads `view.len()`.
- **Narrows the view, no copy:** a router. `route` emits one index list per target;
  `route_signals` emits index lists plus a signal mask per target and never strips in place. A
  router fed a partial view composes indices (its lists index the parent's list), so a router
  behind a router costs the same as one.
- **Mutates, so it owns:** every `Transform::process` and `ScriptWorker::process`. They go
  through `unwrap_batch`, which copies only when it must: a whole view with no other holder
  hands the batch back with no copy (`Arc::try_unwrap`, today's move); a whole view with other
  holders clones (today's shared fan-out into a mutating consumer, the same cost); a partial
  view clones the indexed events, under the mask, which is the clone the router paid for it
  before. No transform pays more than today, and a linear pipeline with no router pays nothing
  new.

### Sinks

`Output::send` and `Output::submit` (which `window_round` calls) take `&BatchView`, as do `Encoder`,
`FramedEncoder`, and `SignalEncoder`, `prometheus`'s own pair (exposition and remote-write), and
`datadog`'s `encode_routes`. Each encoder's event loop iterates the view's masked events. Nothing on
the delivery path materializes: the sink queue stores views (`StoreItem` carries one), a retry
resends the same view, `logit_out`'s window round encodes a hop frame from it, and the disk spool
encodes its record from it (both through `native::encode_hop_batch`, which takes a view) and drops
the view once the record is written (a replayed record is an owned batch, as today), so a
`buffer.disk:` sink never pins a shared batch and the backlogged-sink rule below applies to the
memory store alone.

### The backlogged-sink rule

Every target of a routed batch holds the same `Arc`, so a target that gets 1% of the events
holds 100% of the bytes until it drops the view, and under `route_signals` strip the payloads
it will never send too. For a healthy sink that's the batch's lifetime anyway. For a stalled one
it's every batch in its `buffer:`, at the whole batch's size instead of its slice's, which the
buffer's byte bound wouldn't show.

The rule: a sink's queue materializes a partial view on enqueue once its depth passes a
threshold. Below the threshold the sink pins at most that many whole batches; above it, each
new batch is the sink's own slice. The copy runs on the slow sink's task, so a healthy sibling
and the router pay nothing, and the worst case (a sink that's always behind) is the clone the
router pays today. The threshold's unit (a depth in batches or a byte count, as `buffer:`'s
`max_batches` and `max_bytes` are expressed) and its default are W7's to settle. Below the
threshold, the pinned bytes aren't in the sink's own accounting, so operators need the rule
and its bound written where they size buffers (W9).

### `events.copied` becomes `events.multicast`

`logit.component.events.copied` counts deliveries past each event's first. After this change a
router copies nothing, so the name would send an operator looking for a cost that isn't there.
It becomes `logit.component.events.multicast` with the same arithmetic, so the soak ledger's
router balance (`received + multicast − discarded`) and the ADRs' identities hold unchanged.
`duplicated` was rejected because at-least-once delivery already owns that word. No alias: the
project is pre-release.

### Mechanics

Per AGENTS.md "Workflow": each workstream is a PR stacked on the one before (W8a to W8c are siblings
off W7, and W9 and W10 stack on `main` once all three have merged), `pr-review` runs on it before
the fix rounds, every regression test is run once with its fix reverted and the PR body says so, and
the prose sweep (docs, ADRs, module docs, config field docs, `docs/known-gaps/`) precedes each
review. Allocation pins in `crates/logit-bench/tests/allocations.rs` and the matching rows of
[`docs/design/memory.md`](../design/memory.md) move in the PR that moves them, never in a later one.

## Workstreams

| # | Branch | PR title | Size | Depends on | Status |
|---|---|---|---|---|---|
| W5 | `cores/w5` | `cores/w5: plan for the index-partition router` | S | — | in review |
| W6 | `cores/w6` | `cores/w6: ADR index-partition-router` | S | W5 | — |
| W7 | `cores/w7` | `cores/w7: a routed batch is a view` | L | W6 | — |
| W8a–c | `cores/w8a`, `cores/w8b`, `cores/w8c` | `cores/w8<x>: <crate>'s encoders read a view` | M each | W7 | — |
| W9 | `cores/w9` | `cores/w9: operator docs for views and the pinned-batch bound` | S | W8a–c (all merged) | — |
| W10 | `cores/w10` | `cores/w10: router-into-sink scenarios and the VM numbers` | S | W8a–c (all merged) | — |

- **W6, the ADR.** `docs/adr/index-partition-router.md`: the view, the masked event, the ownership
  rule, the backlogged-sink rule, and the rename. It amends [ADR
  `target-components`](../adr/target-components.md), which says an event is moved with one
  destination and shared or cloned with several, and the `strip: true` rules of [ADR
  `route-signals`](../adr/route-signals.md): rule 1 (cleared before routing), rule 4 (attributes
  cloned, payloads moved per part), and rule 7 (no shared path out). It also amends two
  restatements: AGENTS.md's `route`/`target` and `route_signals` bullets, and the module doc of
  `crates/logit-pipeline/src/route_signals.rs`, which moves in W7 with the code. - **W7, the
  pipeline layer.** `BatchView` and the masked event; `Delivered` carrying a view; `route_batch` and
  `route_signals_batch` emitting views (index lists, signal masks, no in-place strip, index
  composition for a router behind a router); `Output::send` and `Output::submit` taking
  `&BatchView`, and `native::encode_hop_batch` (the spool's `DiskQueue::push` and `logit_out`)
  encoding from a view, with `null_out` reading it and every other sink's body opening with one
  helper call that materializes a partial view until its W8 PR removes the call, so the PR is
  shippable on its own (a materializing sink pays what the router paid, a shift, not a loss); the
  sink queue storing views with the backlogged-sink rule and its default; the `multicast` rename
  across the code, `tools/soak/soaklib` (`checks.py`, `selftest.py`), the two `route-*` scenario
  TOML files, [`docs/design/internal-telemetry.md`](../design/internal-telemetry.md), the two ADRs,
  [ADR `telemetry-loss-vocabulary`](../adr/telemetry-loss-vocabulary.md), AGENTS.md,
  [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md), `performance.md`'s Scenarios
  table, `tools/soak/README.md` (scenario table, ledger prose, `R_copied`), the comments in
  `perf/scenarios/route-copy.yaml` and `route-signals.yaml`, the `route` and `route_signals` field
  docs in `crates/logit-config/src/lib.rs` (rerun `script/schema` and commit
  `schema/logit.schema.json`), and the module doc of `crates/logit-pipeline/src/router.rs`. `grep
  -rln 'events\.copied'` finds the set; plans that record history, such as
  `docs/plans/soak-harness.md` and `critical-sections-inventory.md`, stay as written. Also in W7:
  the allocation pins and `memory.md`'s "Routing" rows. The prototype's
  `crates/logit-pipeline/src/view.rs` is the starting point; its materializing default and
  `unwrap_batch` arm are the shapes to keep. - **W8, the encoder sweep.** The three trait signatures
  and every impl, split by crate into sibling PRs off W7: `logit-proto`'s `SplunkEncoder`,
  `OtlpEncoder`, `NativeEncoder`, `InfluxLineEncoder`, `GraphiteEncoder`, and `CollectdEncoder`;
  `logit-outputs`'s `SyslogEncoder`, `StreamEncoder`, `StatsdEncoder`, `KafkaEncoder`, and
  `EventDump`; and `prometheus`'s exposition and remote-write pair with `datadog`'s `encode_routes`
  and `datadog_trace_out`'s `encode_tracer_api_traces` and `encode_client_stats_v06`. Each PR
  removes its sinks' materialize and moves their pins. An encoder with a grouping pass (`datadog`'s
  route split, `prometheus`'s registry) is where to look for a regression under indexed access. -
  **W9, the operator docs.** [`docs/deploying.md`](../deploying.md)'s "Sink delivery buffering" (the
  pinned-batch bound and the threshold), `memory.md`'s "Sharing versus copying" (what a view holds
  and when it's copied), and [`docs/known-gaps/runtime.md`](../known-gaps/runtime.md) (the bytes a
  slow target pins below the threshold aren't in its own accounting). - **W10, the measurement.**
  New `perf/scenarios/` cells that route into a real sink (`route` and `route-signals` into the JSON
  encoder, say), run on the VM with W8's head against `main`, and recorded in `performance.md`. The
  decision didn't need this run; the record of what a real sink gains does, since every number above
  sinks into `null_out`.

## Settled questions

- **Make it a stream: decided by the maintainer, 2026-10-23,** on the E6 numbers and the
  understanding that the win is the router's own work and holds whatever is downstream.
- **The counter is `events.multicast`: decided by the maintainer, 2026-10-23.** Same arithmetic
  as `events.copied`; the soak ledger reads it by name, so the rename is one string per site.
- **A real-sink run comes after the sweep, not before deciding: decided by the maintainer,
  2026-10-23.** A sink that encodes already reads every event it encodes, so a view adds a
  pointer hop per event, not a copy; what a real sink changes is the denominator, not the sign.
- **Ownership, not reading, decides who copies: decided by the maintainer, 2026-10-23.** Sinks
  and counting read; routers narrow; transforms and Lua own, through `unwrap_batch`.
- **The masked event is the strip: decided by the maintainer, 2026-10-23.** One type every
  encoder iterates, so none can get it wrong, over a per-encoder mask check.
- **A backlogged sink owns its events: decided by the maintainer, 2026-10-23.** The cost falls
  on the slow target alone and is bounded by the threshold; the pinned bytes below it are
  documented where operators size buffers.
- **Equal index lists aren't deduplicated: decided by the maintainer, 2026-10-23.** Not worth
  a comparison pass for under a percent in a rare shape.

## Findings

Recorded per workstream as each lands.

- **E6 (the prototype, 2026-10-23).** Only `null_out` took a view natively; every other sink's
  default materialized, so the table above measures the router's saving with no encoder on
  either side. The share-only paths (`fanout`, `route-copy-all2`, `route-signals-nostrip`) cost
  1 to 2% more under the prototype, consistent in sign across all six cells: the extra `Arc` and
  index list per delivery on a path that already shared. Under `route_signals` strip the
  stripped payloads stayed alive in the shared batch until the last target's sink dropped it,
  which is what the backlogged-sink rule bounds. The queue weight of a partial view charged the
  bytes the view shows, as each sink's own batch does today, not the whole shared batch. The
  prototype's `route_batch` took an owned batch, so a router behind a router materialized; W7
  composes indices instead.
