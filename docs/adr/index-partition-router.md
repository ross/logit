---
created: 2026-10-23
updated: 2026-10-23
---

# Index-partition router: a routed batch is a view of one shared batch, not a copy

## Status
Accepted

## Context

A router pays for every event it touches. `route_batch` moves each event into a per-destination
batch and clones an event that a `routes:` list sends to several destinations
([ADR `target-components`](target-components.md)'s 2026-10-16 amendment). `route_signals_batch`
under `strip: true` splits a mixed event into one part per target, cloning its attributes for every
part past the first ([ADR `route-signals`](route-signals.md)). On the perf VM a move costs about
0.4 µs per event and a clone about 0.75 µs, while sharing the incoming batch behind one `Arc` costs
0.04 to 0.12 µs. A sink only reads what it receives, so the move and the clone buy it nothing.

[`docs/design/performance.md`](../design/performance.md)'s "11. Runtime worker count
(2026-10-23)" section measured the gap between a router that copies and one that shares, and a
prototype that sends each destination the shared batch plus an index list closed it. The
[plan](../plans/index-partition-router.md)'s Context has the full table, the scenario variants, and
the run's conditions; every cell sinks into `null_out`. The router rows:

| Scenario | `main` µs/event | Prototype µs/event | CPU |
|---|---:|---:|---:|
| `route-copy` | 1.181 | 0.509 | −57% |
| `route-signals` | 1.093 | 0.379 | −65% |
| `route-copy-half2` | 1.016 | 0.458 | −55% |
| `route-2t` | 0.794 | 0.493 | −38% |
| `route` | 0.734 | 0.514 | −30% |
| Share-only paths (`fanout`, `route-copy-all2`, `route-signals-nostrip`) | | | +1 to 2% |

The five router scenarios land at or near the share floor (`route-copy-all2` at 0.456).

## Decision

**A routed batch is a view: the shared batch, the events a destination gets, and the payloads it
may see. A router narrows views and never touches event bytes; only a consumer that mutates
events copies them.**

### The view

`BatchView { batch: Arc<EventBatch>, indices: Option<Arc<[u32]>>, signals: Option<u8> }` in
`logit-pipeline` is the one payload a `Delivered` carries.

- `indices` lists the events in view as ascending indices into `batch.events`; `None` is every
  event.
- `signals` is the payloads in view as `signal_index` bits; `None` is every payload. An event in
  view always shows its timestamp and attributes.
- `Delivered::Owned` and `Delivered::Shared` collapse into a whole view (`indices: None,
  signals: None`). A whole view with one holder is today's move, and one with several holders is
  today's share. A router is what sends a partial view.

### The masked event

The view's iterator yields a masked event, not `&Event`. Its `log()`, `metrics()`, and `span()`
accessors apply `signals`, and the raw payload fields aren't reachable through it. That type is
where `route_signals`' `strip` contract lives: an encoder can't send a metric to the logs target by
missing a check, because there's no check to miss.

### Who copies

Ownership decides, not reading:

- **Reads, no copy.** A sink and its encoder: `Output::send`, `Output::submit` (which
  `window_round` calls), and the three encoder traits (`Encoder`, `FramedEncoder`,
  `SignalEncoder`), `prometheus`'s own pair, `datadog`'s `encode_routes`, and
  `datadog_trace_out`'s `encode_tracer_api_traces` and `encode_client_stats_v06`, take
  `&BatchView`. Counting reads `view.len()`.
- **Narrows, no copy.** A router. `route` emits one index list per destination. `route_signals`
  emits index lists plus a signal mask per target and never strips in place; it still counts
  `logit.transform.payloads.stripped{signal}` for each payload its masks hide. A router fed a
  partial view composes indices (its lists index the parent's list), so a router behind a router
  costs what one does.
- **Mutates, so owns.** Every `Transform::process` and `ScriptWorker::process` takes its batch
  through `unwrap_batch`. A whole view with no other holder hands back the batch with no copy
  (`Arc::try_unwrap`). A whole view with other holders clones it, as a shared fan-out into a
  mutating consumer does today. A partial view clones the indexed events under the mask, the clone
  the router used to pay on its behalf.

Equal index lists sent to two destinations aren't deduplicated.

### The delivery path never materializes

A sink's memory store holds views, and a retry resends the same view, so the at-least-once
contract of [ADR `delivery-semantics`](delivery-semantics.md) is unchanged. `logit_out`'s send
window and the disk spool ([ADR `disk-backed-sink-buffer`](disk-backed-sink-buffer.md)) encode
their hop frame or record from the view through `native::encode_hop_batch`. The disk store drops
the view once its record is written, and a replayed record decodes to an owned batch as before, so
a `buffer.disk:` sink never pins a shared batch. The spool format and the wire don't change.

### The backlogged-sink rule

Every target of a routed batch holds the same `Arc`. A target that gets 1% of the events holds
all of the batch's bytes until it drops its view, and under `route_signals` the payloads its mask
hides too. For a healthy sink that's the batch's lifetime anyway. A stalled sink would hold every
batch in its `buffer:` at the whole batch's size, which its byte bound doesn't see, because a
partial view's queue weight is the bytes it shows.

So a sink's memory queue materializes a partial view on enqueue once its depth passes a
threshold. Below the threshold the sink pins at most that many whole batches; above it, each new
batch is the sink's own copy of its slice. The copy runs on the slow sink's task, so the router and
the healthy targets pay nothing, and the worst case, a sink that's always behind, costs the clone
the router pays today. The threshold's unit (batches or bytes) and its default are settled in the
plan's W7. The bytes pinned below the threshold aren't in the sink's own accounting, so
`docs/deploying.md`, `docs/design/memory.md`, and `docs/known-gaps/runtime.md` state the rule and
its bound for operators.

### `events.copied` becomes `events.multicast`

A router no longer copies, so `logit.component.events.copied` would send an operator looking for a
cost that isn't there. It's renamed `logit.component.events.multicast`, with the same arithmetic:
deliveries past each event's first. The soak ledger's router identity, `received + multicast −
discarded`, holds unchanged. `duplicated` was rejected because at-least-once delivery already owns
that word. There's no alias, because `logit` is pre-release.

## Alternatives considered

- **Keep moving and cloning in the router, and recommend `runtime.workers: 2`.** That's the
  measured workaround in `performance.md`'s "11. Runtime worker count (2026-10-23)": two workers
  cut `route`, `route-copy`, and `route-signals` to 0.48 to 0.75 µs per event. The copy stays,
  every router config needs the setting, and the view reaches 0.38 to 0.51 on the default
  runtime.
- **A per-encoder mask check instead of a masked type.** Each encoder would test `signals` before
  reading a payload. A missed check leaks a payload to the wrong target silently, and nothing
  names the sites to audit. The masked type makes the compiler name them.
- **Charge every target the whole batch's bytes.** Bounds a stalled sink's pinned bytes through
  its existing `max_bytes`, but a slow sink's buffer fills with bytes it never sends, and a
  1%-slice target trips its byte bound a hundred times early.
- **Deduplicate equal index lists.** A list is 4 bytes per event per target against the 864-byte
  `Event` a copy moves, and a uniform mark sends a whole view with no list. A comparison
  pass isn't worth under a percent in a rare shape.

## Consequences

- **Costs.** A path that already shared pays 1 to 2% more CPU for the extra `Arc` and index
  list. Payloads `route_signals` masks out stay alive until the last target drops the batch,
  bounded by the backlogged-sink rule. A transform behind a router still copies its slice, no worse
  than today.
- **A wide signature change.** `Delivered`, `Output::send` and `Output::submit`, the three encoder
  traits and every impl, `prometheus`'s pair, `datadog`'s `encode_routes`, `datadog_trace_out`'s
  two encoders, and `native::encode_hop_batch` take a view. It lands over the plan's W7 to W10:
  the pipeline layer first, with each sink materializing a partial view until its encoder PR
  removes the call.
- **Pins move with the code.** Allocation pins in `crates/logit-bench/tests/allocations.rs` and the
  matching `docs/design/memory.md` rows change in the PR that changes them.
- **Operator docs.** `docs/deploying.md`, `docs/design/memory.md`, and
  `docs/known-gaps/runtime.md` carry the pinned-batch bound and the threshold.
- **Measured basis.** The decision rests on the router's own saving, measured into `null_out`. The
  plan's W10 records what a real sink gains.
- **Amended.** [ADR `target-components`](target-components.md) (the move-or-share-or-clone rule)
  and [ADR `route-signals`](route-signals.md) (`strip: true` rules 1, 4, and 7), each in a
  2026-10-23 amendment.
