---
created: 2026-09-20
updated: 2026-09-20
---

# `shape`: an observer component that turns each event into measurements of its own shape

## Status

Accepted

## Context

[`docs/design/memory.md`](../design/memory.md) §8 defers two inline-capacity decisions as needing
"a real distribution of attribute/metric counts," and
[`docs/plans/data-shape-survey.md`](../plans/data-shape-survey.md) is the effort to collect one. A
survey needs an instrument, and nothing in the repo could be one: `internal` reports how the
*pipeline* behaves, not what the events flowing through it look like; `logit-perf attribute` folds
`internal`'s own points, keyed on a `component` attribute only those carry; a `lua` stage can read
`#event.attributes` but not where a value landed (event, `Resource`, or `Scope`), not a
`MetricList`'s length as the model holds it, and not a batch boundary.

The same instrument is useful to an operator with no interest in the survey. "How wide are the
events on this leg, and how many distinct key-sets does it carry?" is the question behind sizing a
`keep`, deciding whether a `json` stage is worth its cost, or explaining an interner that keeps
growing — and today the only way to answer it is to write the events somewhere and count by hand.

Two properties of the runtime shape what such an instrument can be.
[`Transform::process`](../../crates/logit-pipeline/src/transform.rs) is `&mut Event -> bool`
([ADR `in-place-transform-process`](in-place-transform-process.md)): one event in, at most that
same event out, never two. And batch boundaries are visible to a transform
(`observe_batch_context` … `end_batch`) but not to anything downstream of a sink — an input's
`BatchAccumulator` merges decode outputs before the first `Delivered` is ever built, and a
`file_out format: native` dump's existing reader flattens frames back into a bare event list.

## Decision

`shape` is a native transform (`crates/logit-transforms/src/shape.rs`), placed on its own branch of
an ordinary fan-out:

```
statsd_in ─┬─> (real pipeline)
           └─> shape ─> aggregate ─> any sink
```

**It rewrites each event in place into a measurement event.** `process` measures the event, then
drops its payload and replaces it with `logit.shape.*` metric records and a small fixed attribute
set, and returns `true`. Because it sits on a fan-out branch, the flow it measures never sees it;
because the fan-out is ordinary, two of them — one straight off an input, one after a transform
chain — measure how much wider an event gets on its way through.

**It emits raw values and never summarizes.** Every distribution-shaped quantity is a
`MetricKind::Samples`, the same raw kind `statsd_in` decodes a timer to and `kv_metrics` derives a
`distributions:` entry to. Whether those become exact retained values or a `DdSketch`, over what
window, keyed how, is an `aggregate` downstream's decision —
[ADR `lossless-transit`](lossless-transit.md)'s "summarization is opt-in and named" applied to
`logit`'s own instrument.

**It emits counts and lengths only.** No attribute key, attribute value, log body, or metric name
from an observed event appears in anything `shape` produces — not in its metrics, not in its tags,
not in its self-logging or telemetry. This is what lets its output leave an environment the traffic
itself can't, and it is the property a reviewer should check first on any change to this component.
Two deliberate edges to it:

- `resource: drop` (the default) substitutes one cached, empty `Resource` through `map_resource`,
  so no resource attribute flows out. `resource: keep` forwards the incoming resource unchanged —
  an operator opting into a per-service breakdown downstream, and into that identity flowing with
  it. `keep` governs the per-event output only: what goes out at `flush` always goes out under the
  empty `Resource`, because a window spans many batches and there is no single resource to keep.
- The batch's `Scope` passes through unchanged. `Transform` has no hook to substitute one, and a
  scope names an instrumentation library, not a payload; adding a trait hook every implementer
  carries, for this one component, is not worth it.

**Per-event measurements** — attribute count, nested-map count and widths, value depth, key and
value byte lengths, a per-type value count, metric count, samples per metric, body length, span
events and links — go out on the rewritten event, tagged `signal` (which of log/metric/span the
observed event carried, `+`-joined in a fixed order, so signal co-occurrence falls out as a tag
value), `source` (the batch provenance's origin component), and `tap` (this component's name, so
two taps into one `aggregate` stay distinct).

**Per-batch measurements** — events in the batch, resource and scope attribute counts, distinct
key-sets within the batch — cannot go out from `process`, which has no second event to put them on.
They accumulate between `observe_batch_context` and `end_batch` and go out at the next `flush` as
`Samples` carrying one value per batch seen in the window. `Resource` and `Scope` are per-batch
facts in the model (`EventBatch` holds both behind an `Arc`), so this is also the only honest place
to report their width: once per batch, next to how many events shared it.

**Stateful measurements** — distinct top-level keys, distinct key-sets, the share of events carried
by the most common one and five key-sets — are gauges emitted at `flush`, cumulative since start. A
key-set's identity is a hash of the event's `Symbol` sequence; `AttrMap` is sorted by `Symbol`, so
that costs no sort and no allocation. Both tables are capped (`max_tracked_keys`,
`max_tracked_keysets`); past a cap, new entries are counted as overflow rather than tracked, and a
`tracking_overflow` gauge says so.

What `shape` calls a batch is whatever its upstream delivered: an input's accumulator flush by
default, the wire's own grouping when that input runs `receive.batch_max_events: 1`, and always the
wire's grouping for `otlp_in` and `prometheus_in`, which bypass the accumulator.

## Alternatives considered

- **An in-line annotator** — a pass-through in the real flow that stamps `shape.*` attributes onto
  each event, leaving `kv_metrics` and `aggregate` to do the rest. The most composable on paper, and
  it would let `route` or `keep` act on width. Rejected: it changes the thing it measures (an event
  one attribute short of spilling now spills), it puts a survey instrument's cost on the real flow,
  and it has nowhere to carry per-batch or stateful measurements at all.
- **A self-aggregating observer** — hold `DdSketch`es internally and emit finished distributions on
  an interval. One component in the config instead of two, and cheaper per event. Rejected: it
  duplicates `aggregate` inside another component and makes a summarization decision (sketch versus
  exact, window, keying) that this project's standing rule gives to a named, operator-chosen stage.
  A survey that wants exact values gets them from `aggregate`'s `distributions: samples`; this
  design would have had to grow the same switch.
- **An offline tool** folding a `file_out format: native` dump, as a `logit-perf` subcommand. No new
  component, and it sees the real `Event` layout. Rejected: batch boundaries at the input are already
  gone by the time a sink writes a frame, a long capture has to fight `file_out`'s rotation, and an
  operator can't point it at live traffic — and it requires writing the payload to disk, which is
  exactly what the counts-only property exists to avoid.
- **A `lua` script.** Zero new Rust. Rejected as the instrument, for the visibility reasons in
  Context; it remains a fine way to ask a one-off question.
- **Emitting per-batch measurements on the first or last event of each batch** instead of at
  `flush`. Rejected: the batch's event count isn't known until `end_batch`, which can't emit, and
  making one event per batch different from the rest makes every downstream aggregate's event count
  subtly wrong.

## Consequences

- A measurement event carries on the order of a dozen metric records, so it always spills
  `MetricList`'s single inline slot, and its `Samples` for key and value lengths spill past
  `SAMPLES_INLINE` on a wide event. That is accepted: this is a tap branch, not the hot path, and
  the allocation pins in `crates/logit-bench/tests/allocations.rs` record what it costs rather than
  leaving it to be discovered. It is also, incidentally, one more multi-metric event shape for the
  `MetricList` question the survey feeds.
- `shape` mutates, so adding one turns a single-consumer edge — which costs nothing at all
  (`Delivered::Owned`, no `Arc`) — into a real fan-out, and the extra branch is a *mutating* one.
  What that costs is shape-dependent, not a flat clone:
  [`docs/design/memory.md`](../design/memory.md) §3's table is the account. Tapping a leg whose
  other branch is a sink is the racy row (one `Arc::new`, plus a full `EventBatch` clone only when
  the mutating side's `unwrap_batch` loses the race — 1 or 4 allocations on the nginx shape, with
  1 the likelier outcome since `drain_inbox` drops the sink's handle on receipt). Tapping a leg
  whose other branch is itself a transform or a Lua stage is the deterministic row: neither side
  can borrow, so one of them always clones. Either way the clone, when it happens, is of the whole
  batch, not of one event at a time. An operator measuring a production flow should expect that
  and remove the tap afterwards; the docs say so.
- The stateful gauges are cumulative since process start, not windowed, and track top-level keys
  only — a nested map's keys are counted in its width, not added to the distinct-key set.
  `docs/known-gaps.md` records both.
- `shape`'s output vocabulary (`logit.shape.*`, the `signal`/`source`/`tap` tags) is now something
  dashboards and the survey's capture harness depend on; renaming a metric is a breaking change to
  them, pre-release or not.
