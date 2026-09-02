# 0008 — `aggregate` transform: tumbling windows, pass-through, and the flush-tick contract

## Status
Accepted

## Context
`BuiltinTransformConfig::Aggregate { interval: Duration }` (`crates/logit-config/src/lib.rs`) has
existed as a config variant since PR #8, rejected at both `logit validate` and `logit run` as "not
implemented yet." It's the last piece of the v0.1 vertical slice
([`tmp/0.1-status.md`](../../tmp/0.1-status.md)), and the first real consumer of two things that
existed only in prose or as unused code before this: `DdSketch::merge`
([`docs/design/data-model.md`](../design/data-model.md)'s mergeable-metric-kinds design) and
`ScriptWorker::flush()` (`crates/logit-script/src/lib.rs`, implemented and tested since the Lua
engine landed, but never invoked by the pipeline).

`docs/design/lua-api.md` fixes some of this transform's shape in advance — a stage-local `interval`
config key, `flush()` runs on that interval and returns events to emit, `aggregate` sits ahead of
user Lua in the chain — but leaves the actual windowing semantics open: tumbling vs. sliding, window
alignment, late/out-of-order data, what an emitted aggregate's timestamp and resource are, and what
happens to events the aggregator can't accumulate. Per `AGENTS.md`'s "a new design decision worth
remembering gets an ADR," those are what this record settles.

## Decision

**Tumbling windows, reset on flush.** Each interval accumulates from empty; a flush drains every
window into one emitted event per series and discards the accumulator. A counter's emitted value is
therefore that window's *sum* (a delta), matching what a raw InfluxDB counter field expects, not a
running total — `docs/design/data-model.md`'s "Counter/Gauge merge trivially (sum / last-write-wins
by timestamp)" describes merging *within* one window, not across window boundaries.

**Per-kind merge, exactly as `data-model.md` specifies, nothing invented:** `Counter` sums; `Gauge`
keeps the value with the latest *source* timestamp (ties favor whichever event is processed second —
arbitrary but deterministic, since processing order is itself deterministic per pipeline);
`Distribution` merges via `DdSketch::merge`. `Set` has no merge implemented here because
`HyperLogLog` (`crates/logit-core/src/metric.rs`) is still a method-less stub — a design gap, not a
decision this ADR is re-litigating. `Histogram`/`Summary` have no merge rule specified anywhere in
this codebase's design docs.

**Pass through, never drop.** Logs, spans, and any metric kind with no defined merge rule here
(`Set`, `Histogram`, `Summary`) are forwarded to the next stage untouched. So is an event whose kind
*conflicts* with a series already accumulating under the same name/unit/attributes (a counter and a
gauge sharing identical tags, say) — there's no correct merge for that either, so it's forwarded
rather than silently corrupting the existing accumulator or being dropped. This means aggregated
output can arrive out of order relative to passed-through events from the same batch (the aggregated
version only appears at the next flush tick) — inherent to windowed aggregation, not a bug.

**Grouped by resource value, not by which pipeline/batch produced it.** Two batches whose
`Arc<Resource>` are different allocations but equal content describe the same origin and aggregate
together; `Resource` is `PartialEq`, not `Hash`, so grouping is a linear scan (one group in practice
today — statsd always uses `Resource::default()`).

**Emitted timestamp is flush wall-clock**, i.e. when the window closed — not a source event's
timestamp, and not the gauge's internally-tracked "latest write" timestamp (used only to pick the
winner, then discarded).

**Windows are wall-clock-driven, not event-time.** A flush fires when the pipeline's clock reaches
the deadline, regardless of what timestamps the accumulated events actually carry. There is no
watermark, no late-data grace period, no reordering buffer: an event that arrives after its window's
flush has already fired lands in the *next* window, full stop. This matches every other timing
decision already made in this codebase (statsd stamps one wall-clock timestamp per datagram) and
keeps the aggregator's state bounded and simple; a real event-time model is a larger design question
for if/when it's actually needed.

**A flush is not exempt from the rest of the chain.** Events an `Aggregate` stage emits at a flush
tick run through every later stage exactly like a normal batch would (`flush_stage`,
`crates/logit-cli/src/pipeline.rs`). A downstream `Aggregate` stage therefore re-accumulates a
flushed event rather than passing it straight through — intentional (a chain of two aggregators is a
two-stage window), not a special case.

**The same flush-tick timer drives Lua's `flush()` too**, via an optional `interval` on the `lua`/
`lua_file` transform config variants (`crates/logit-config/src/lib.rs`). Omitted, the stage never
ticks — the same as a script defining no `flush()` at all, which was already legal and is now
finally reachable in practice. A Lua stage's `flush()` has no batch of its own to take a resource
from (unlike `Aggregate`, which tracks its own per-resource windows); it's stamped with whichever
resource the worker most recently saw on a real batch, or a fresh default if none has arrived yet.
This is a real, narrow gap — documented here rather than left silent — that matters only once a
pipeline has more than one resource feeding it, which nothing in v0.1 does.

## Alternatives considered
- **Sliding windows.** Rejected for v0.1: no consumer needs overlap between windows, and a sliding
  window needs either a ring buffer of sub-windows or re-processing overlapping ranges, real added
  complexity with no concrete requirement driving it yet.
- **Event-time windows with a watermark/grace period.** Rejected for the same reason as sliding
  windows — genuinely more correct for out-of-order data, but no input in this codebase produces
  meaningfully out-of-order data today, and it would need a new "how late is too late" config
  surface with no user asking for it yet.
- **Cumulative (never-reset) counters**, matching OTLP/Prometheus cumulative temporality. Rejected:
  it means state grows unbounded with series cardinality and a process restart resets every series
  to zero with no way to detect that from the emitted stream, whereas tumbling-with-reset makes each
  emitted value self-contained.
- **Drop pass-through-ineligible events instead of forwarding them.** Rejected outright: this is
  exactly the "reports healthy while silently losing telemetry" failure class this project's review
  process has repeatedly flagged and fixed elsewhere (see PR #8's review rounds). An `aggregate`
  stage that silently dropped every log line in a mixed pipeline would be a much worse defect than
  one that merely reorders relative to aggregated metrics.
- **Reject at config time any pipeline where `aggregate` might see a non-metric event.** Not
  implementable today: nothing in `InputConfig` declares which payload types an input kind produces,
  so there's no data to reject on.

## Consequences
- A counter/gauge/distribution series that a misconfigured source sends as two different kinds under
  identical tags is *reported* (`eprintln!`, pending a real diagnostics facility — the same
  known gap as every other stage) and forwarded, not silently merged into a nonsensical value or
  dropped.
- `logit validate`/`logit run` reject a zero flush interval on either an `aggregate` stage or a Lua
  stage's `interval` (`require_implemented_transform`,
  `crates/logit-cli/src/pipeline.rs`) — the hand-rolled humantime codec in `logit-config` accepts
  `0s` structurally, but a zero interval would make the worker's flush schedule perpetually due.
- The published schema (ADR 0003) gets `interval` as an additive, optional field on the Lua
  variants; `BuiltinTransformConfig::Aggregate`'s `interval` was already required.
- No graceful shutdown yet (Ctrl-C still falls through to the OS default), but the worker thread
  now flushes every flush-bearing stage once when its inbound channel closes normally, so a
  pipeline that reaches a clean end doesn't silently lose its last in-flight window.

## Amendment: pass-through is per metric, not per event

[ADR 0012](0012-multi-payload-events.md) replaces `Event`'s one-of `Payload` with independent
`log`/`metrics`/`span` fields, so an event can now carry a log *and* metrics at once (the
`kv_metrics` shape planned in `docs/plans/0002-nginx-integration.md`'s workstream E). This ADR's
"Pass through, never drop" decision above was written against the one-of model, where "pass through"
and "absorb" were mutually exclusive properties of a whole event. They no longer are.

**`Aggregator::process` now absorbs metrics individually, not the event as a whole.** It takes each
metric off `event.metrics` in turn: `Counter`/`Gauge`/`Distribution` are absorbed into window state
exactly as before; `Set`/`Histogram`/`Summary` and any metric whose kind conflicts with an
already-accumulating series under the same name/unit/tags are pushed back onto the event rather than
absorbed — the same set of kinds this ADR always declined to merge, just decided per metric now
instead of once for the whole event. `process` returns `None` only when nothing at all remains on
the event afterward — no unabsorbed metric, no log, no span. An event carrying a log and a clean
counter has its counter absorbed and is forwarded with only the log remaining; before this
amendment, that same event (impossible to construct under the one-of model) would have had no
well-defined behavior at all.

A kind conflict is now reported once per offending *metric* (`eprintln!`, same known diagnostics gap
as before), not once per event — a sibling metric on the same event that merges cleanly is still
absorbed even when another metric on it conflicts.

**This is behavior-preserving for every event shape this ADR could previously describe.** A
metric-only event still behaves exactly as before (absorbed → `None`, or forwarded whole on a
conflict/unmergeable kind → `Some`); a log-only or span-only event still never touches window state
at all (the empty-metrics fast path in `Aggregator::process`). The only newly-defined behavior is for
event shapes the one-of model made impossible to construct in the first place, so nothing this ADR
already committed to changes.

See `crates/logit-transforms/src/aggregate.rs`'s `process` for the implementation, and its test
module for the shapes this amendment adds coverage for (a log absorbing a counter and forwarding the
log; a mixed metric event absorbing what it can and keeping the rest; two same-series metrics on one
event summing together; a kind conflict leaving only the conflicting metric behind).

## Amendment: gauge series carry across the window boundary

[ADR 0024](0024-relative-gauge-adjustments.md) adds `MetricKind::GaugeDelta`, a relative gauge
adjustment (statsd/DogStatsD's leading `+`/`-`) that `aggregate` resolves against a gauge's running
value. But a statsd gauge is sticky by protocol -- the sender transmits only on change and expects
the last value to persist -- so a delta arriving in window *N+1* has to apply against window *N*'s
final absolute value, not against an empty accumulator. `flush`'s unconditional
`self.groups.drain(..)` (the "Tumbling windows, reset on flush" decision above) makes that
impossible: nothing survives a flush to apply a later delta against. This amendment changes that,
for gauge series specifically.

### Why gauges, not counters

This ADR's own "Alternatives considered" rejected cumulative (never-reset) counters, matching
OTLP/Prometheus cumulative temporality, because state grows unbounded with series cardinality and a
process restart resets every series to zero with no way to detect that from the emitted stream. Gauge
retention is the same shape of tradeoff -- state surviving a flush, bounded imperfectly by cardinality
-- so it has to answer the same objection, not quietly reintroduce it through a different metric kind.

The answer is that **a gauge is semantically sticky and a counter is not.** A statsd counter has no
"current value" between windows -- each window's emitted value is that window's own delta, by design
(`Counter`'s merge rule sums; nothing about a counter implies continuity with the window before it).
A statsd gauge, by contrast, *is* a single logical value that a sender updates over time and expects
to persist until the next update -- that persistence is what the wire protocol's relative-adjustment
syntax is *for*. Retention buys correctness for gauges that it would not buy for counters: a retained
counter would just be reinventing the rejected cumulative-counter design with extra steps, while a
retained gauge is preserving a value the protocol itself says should persist.

It is bounded by **two** mechanisms, not one, specifically because a TTL alone bounds only the tail
of the retained set, not its peak: `gauge_retention` (a windows-count TTL) answers "how long does an
idle series linger," but a sustained stream of *C* never-repeating series names per window, at
retention *R*, would hold *C * R* series forever regardless of how short *R* is -- the TTL never
catches up. `max_retained_gauge_series` is the second, independent bound: a hard cap on the total
retained set at any one time, cardinality-guarding exactly the failure mode a TTL alone cannot touch.
Hitting it is not silent -- a later delta against an evicted series resolves against 0.0 and produces
a wrong-looking number, so eviction fires both `logit.transform.series.evicted{reason="cardinality"}`
and a throttled `gauge_retention_full` diagnostic.

### The `at`-reset rule

A retained `Accumulator::Gauge` keeps its `value` but resets `at` to `i64::MIN` the moment it survives
a flush. `at` exists purely as a **within-window** last-write-wins tiebreak (see "Per-kind merge"
above); retention must not be allowed to silently promote it into a **cross-window** ordering
guarantee. Without the reset, an ordinary absolute gauge arriving in window *N+1* with an earlier
source timestamp than window *N*'s winner would fail the `event.timestamp >= at` comparison and be
silently dropped -- a new failure class that grows with retention depth, since a longer
`gauge_retention` would make a stale `at` valid for longer. Resetting `at` on every retain means
window *N+1* starts its own LWW contest from scratch, exactly as if the series were new, while still
keeping the *value* that makes it not actually new.

### `logit.transform.series.active` keeps its existing meaning

Retention gives a resource group a second population of series -- ones carried over, contributing
nothing this window -- alongside the ones that actually received data. `logit.transform.series.active`
already has a documented job: an early-warning signal for cardinality blowup in the *current* window's
absorbed data (see `flush`'s own comment on why it's sampled before anything is touched). Silently
redefining it to include every retained-but-idle series too would break that signal for anyone
watching it, understating how much a single misbehaving window actually cost while overstating the
aggregator's true per-window load. So `.active` stays scoped to series with `updated_this_window ==
true`; a new, separate `logit.transform.series.retained` gauge reports the idle-but-carried
population instead, so both are visible without either question changing meaning underneath an
existing dashboard.

### Two consequences worth stating plainly

**A chained downstream `aggregate` re-retains and emits only when the upstream one emitted.** ADR
0008's own "flush is not exempt from the rest of the chain" decision means a downstream `aggregate`
re-accumulates whatever an upstream one flushes, including its own gauge retention if configured. A
naive reading might expect the downstream stage to emit *every* one of its own windows regardless --
but if the upstream stage retained a gauge series and emitted nothing for it that tick (this
amendment's whole point), the downstream stage never sees that tick at all, so it can't emit anything
for it either. This is consistent (an aggregator only ever reacts to what it's handed), not a bug, but
worth naming since it's a second-order effect of retention that isn't visible from either stage's
config alone.

**A close-time flush emits nothing for a retained-but-idle gauge, and that is correct, not data
loss.** ADR 0013's shutdown-flush guarantee (see "Consequences" above) still fires once when a
listener's inbox closes -- but for a gauge series that was idle at that moment, "flush once more"
means exactly what it means mid-run: no event, because there is nothing new to report. Read in
isolation, a shutdown that produces no final point for a gauge an operator knows is "live" can look
like the last value was lost. It wasn't -- the last value was already emitted at whichever window
actually updated it, and a gauge's whole contract is that its last emitted value stands until
replaced. This is worth saying explicitly because it's the one place retention's silence (correct
mid-run) could plausibly be misread as a bug (at shutdown, when a human is more likely to be watching
closely).

See `crates/logit-transforms/src/aggregate.rs`'s `flush` for the implementation and its test module
(the block following the existing pass-through/multi-payload tests) for the shapes this amendment
adds coverage for: a delta resolving against the previous window's final value; an idle retained
gauge emitting nothing; eviction after `gauge_retention` idle windows followed by an unseeded delta;
`gauge_retention: 0` reproducing today's strictly-tumbling output byte-for-byte; the `at`-reset
regression test; a counter never surviving its window even with retention enabled; a counters-only
resource group disappearing from `groups`; the cardinality cap evicting and firing
`series.evicted{reason="cardinality"}`; contexts never carrying across a flush even for a retained
series; and `flush` never emitting an empty `(resource, events)` pair.
