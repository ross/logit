---
created: 2026-08-29
updated: 2026-09-11
---

# `aggregate` transform: tumbling windows, pass-through, and the flush-tick contract

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
today — `statsd_in` always uses `Resource::default()`, and `internal` always uses its own constant
`service.name = logit` resource, so `windowed`/`self_windowed`-style aggregators downstream of
either still see exactly one distinct resource across the batches they receive).

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
- The published schema (ADR `config-yaml-jsonschema`) gets `interval` as an additive, optional field on the Lua
  variants; `BuiltinTransformConfig::Aggregate`'s `interval` was already required.
- No graceful shutdown yet (Ctrl-C still falls through to the OS default), but the worker thread
  now flushes every flush-bearing stage once when its inbound channel closes normally, so a
  pipeline that reaches a clean end doesn't silently lose its last in-flight window.

## Amendment: pass-through is per metric, not per event

[ADR `multi-payload-events`](multi-payload-events.md) replaces `Event`'s one-of `Payload` with independent
`log`/`metrics`/`span` fields, so an event can now carry a log *and* metrics at once (the
`kv_metrics` shape planned in `docs/plans/nginx-integration.md`'s workstream E). This ADR's
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

[ADR `relative-gauge-adjustments`](relative-gauge-adjustments.md) adds `MetricKind::GaugeDelta`, a relative gauge
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
loss.** ADR `service-lifecycle-and-output-retry`'s shutdown-flush guarantee (see "Consequences" above) still fires once when a
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

## Amendment: raw samples and set members are absorbed

[ADR `metrics-model-v2`](metrics-model-v2.md) added `MetricKind::Samples`/`SetMembers` as the raw
half of `Distribution`/`Set`'s raw-vs-summarized pairs, but left them with no producer and no
merge rule — "`aggregate` and the sinks treat `Samples`/`SetMembers`/`ExponentialHistogram`/a
non-delta-monotonic `Sum` as pass-through or explicitly unsupported, not as a real feature, until
W2/W3/W4" (that ADR's Consequences). [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md)'s
W2 is the workstream that closes the `aggregate` half of that gap. This ADR's original "Decision"
section, above, declared: "`Set` has no merge implemented here because `HyperLogLog`
(`crates/logit-core/src/metric.rs`) is still a method-less stub — a design gap, not a decision this
ADR is re-litigating," and its "Pass through, never drop" rule named "any metric kind with no
defined merge rule here (`Set`, `Histogram`, `Summary`)" as forwarded untouched. `Samples`/
`SetMembers` didn't exist yet when that was written, but inherited the same fate the moment
`metrics-model-v2` added them: no merge rule, pass through.

**`Samples`, `SetMembers`, and `Set` now all have real merge rules — a `HyperLogLog` (wrapping
`cardinality-estimator`) is real state, not a stub, and only `Histogram`/`ExponentialHistogram`/
`Summary`/a cumulative `Sum` remain pass-through.**

### Two modes per raw/summarized pair, and their caps

`ComponentKind::Aggregate` gains two independent mode switches, mirroring each other:
`distributions: sketch | samples` (default `sketch`) for the `Samples`/`Distribution` pair, and
`sets: estimate | members` (default `estimate`) for the `SetMembers`/`Set` pair
(`crates/logit-config/src/lib.rs`). Each mode picks the accumulator a fresh series opens with
(`Accumulator::new_for`), not what merges into it once open — an incoming record's own kind still
drives the merge match in `process` regardless of mode.

- **`distributions: sketch`** (the default): every `Samples` value sketches directly into the
  series' `DdSketch` via `Samples::sketch`'s weighting rule (`add_weighted(v, weight)`, `weight =
  round(1/sample_rate)` clamped to `[1, Samples::MAX_WEIGHT]`) — no raw values ever survive past
  the absorb. `weight == Samples::MAX_WEIGHT` counts
  `logit.transform.samples.weight_clamped` and throttle-warns `sample_rate_clamped`, the same
  diagnostic `statsd_in` reports today (both fire on a `statsd_in -> aggregate` pipeline until W3
  deletes `statsd_in`'s own copy).
- **`distributions: samples`**: a series opens as `Accumulator::Samples`, seeded with the
  *first* record's `sample_rate`, and an incoming `Samples` record concatenates its values
  (`held.values.extend(..)`) — raw retention, bounded by `max_samples_per_series` (default `1000`).
  Two things force an immediate, one-time conversion to `Accumulator::Distribution` (sketching
  everything held plus the incoming record, both weighted) instead of concatenating: the incoming
  record's `sample_rate` disagreeing with the series' first one (`logit.transform.samples.fallback
  {reason="rate_mismatch"}`, diagnostic `samples_rate_mismatch`), or the concatenation growing past
  `max_samples_per_series` (`logit.transform.samples.fallback{reason="cap"}`, diagnostic
  `samples_cap_exceeded`). Either fallback is counted and diagnosed exactly once per triggering
  record — the fallback-and-count rule this amendment applies uniformly. A `samples`-mode series
  that meets an already-summarized incoming `Distribution` (a relay hop past an upstream `aggregate`)
  converts the same way, unconditionally, with no fallback counter of its own — there's nothing to
  fall back *from* raw retention when the incoming record was never raw to begin with.
- **`sets: estimate`** (the default): every `SetMembers` member inserts directly into the series'
  `HyperLogLog` (`insert`, idempotent per member) — no exact membership survives past the absorb.
- **`sets: members`**: a series opens as `Accumulator::SetMembers`, and an incoming `SetMembers`
  record unions in, deduplicated, preserving insertion order (linear `contains` scan — bounded by
  the cap, so this stays cheap), bounded by `max_set_members_per_series` (default `1000`). Growing
  past the cap converts to a fresh `HyperLogLog` (inserting every held-plus-incoming member, so the
  union stays correct across the conversion) and counts
  `logit.transform.set_members.fallback{reason="cap"}`, diagnostic `set_members_cap_exceeded` — the
  same fallback-and-count rule as the `samples` cap case, with no rate-mismatch analog (a set
  member has no sample rate to disagree about). A `members`-mode series meeting an already-
  summarized incoming `Set` converts the same unconditional way `samples`-mode does for an incoming
  `Distribution`.

### Why a mismatched `sample_rate` forces a sketch

`Samples` carries one `sample_rate` for its whole batch of raw values (statsd's own shape: one
`@rate` suffix per line). A `samples`-mode accumulator has to pick *one* rate to report if it never
falls back — its `into_kind` emits a single `MetricKind::Samples { sample_rate, .. }` — and there is
no correct single rate to report for two merged batches sampled at different rates: reporting
either one misrepresents the other's contribution, and averaging the two rates doesn't correspond
to any real extrapolation a consumer could apply uniformly across the concatenated values. Sketching
immediately sidesteps the question entirely: `Samples::sketch`'s per-value weighting already applies
each batch's own rate before the values are folded together, so the *sketch* — unlike a raw
`Samples` accumulator — has no single-rate representation to be wrong about.

### Why `Samples`/`SetMembers`/`Set` series tumble regardless of `gauge_retention`

Gauge retention exists for one reason, named in this ADR's earlier amendment: a gauge is
*semantically sticky* — a single logical value a sender updates over time and expects to persist —
so a relative adjustment arriving in a later window has to resolve against the value retention kept
alive. Nothing about that reasoning applies to `Samples`, `SetMembers`, or `Set`: each window's raw
samples, raw members, or cardinality estimate is that window's own self-contained observation, with
no "current value" a later window's data is relative to (the same "a counter has no current value
between windows" reasoning this ADR's gauge-retention amendment already used to explain why
retention applies to gauges and not counters). `flush` never places a `Samples`/`SetMembers`/`Set`
accumulator into `survivors` — only `is_gauge && self.gauge_retention > 0` does — so every one of
these series drains on every flush exactly like a counter does, even with `gauge_retention` set to a
large value.

### The `(resource, scope)` group key, and `FlushOutput` carrying scope

`ResourceGroup` now keys `group_for` on `(resource value, scope value)`, not resource value alone —
`Aggregator::observe_scope` (a `Transform` trait method, alongside the existing
`observe_batch_context`) records the incoming batch's scope once per batch, the same per-batch-not-
per-event shape `observe_batch_context` already uses, and every metric absorbed from that batch
groups under it. `Transform::flush`'s return type, `FlushOutput`, grew a matching field: it was
`Vec<(Arc<Resource>, Vec<FlushedEvent>)>`, stamping every flushed batch's scope `None` regardless of
what scope actually fed it (`crates/logit-pipeline/src/runtime.rs`'s `run_flush` had nowhere to get
one from); it is now `Vec<(Arc<Resource>, Option<Arc<Scope>>, Vec<FlushedEvent>)>`, one entry per
`(resource, scope)` group, each carrying the scope every series in it shares. This closes the
`otlp_in -> aggregate -> otlp_out` scope-loss gap `docs/plans/lossless-transit.md`'s W2 tracked:
before this, an `aggregate` stage between two OTLP legs silently dropped which instrumentation scope
a metric came from, even though nothing about tumbling-window aggregation requires losing it. Two
batches sharing a resource but carrying different scopes now flush as two distinct groups, each
tagged with its own scope, rather than folding together or losing scope identity to `None`.

### The remaining pass-through set

After this amendment, exactly four metric kinds still have no defined merge rule and pass through
`process` untouched: a cumulative `Sum` (only a *delta* `Sum` merges — the same distinction this
ADR's original "Per-kind merge" rule already drew for `Counter`), `Histogram`,
`ExponentialHistogram`, and `Summary`. `process`'s pass-through `matches!` and
`Accumulator::new_for`'s `unreachable!` arm are kept in sync by comment, deliberately, the same
"kept in sync... a mismatch between the two is a runtime panic, not a compile error" shape this
codebase already uses elsewhere for exactly this kind of paired exhaustiveness.

See `crates/logit-transforms/src/aggregate.rs`'s `process`/`flush`/`Accumulator` for the
implementation, and its test module for the shapes this amendment adds coverage for:
`remaining_pass_through_kinds_survive_process_untouched` (the four kinds above, split out now that
`Samples`/`SetMembers`/`Set` no longer belong in the same test) and its three siblings
`samples_is_not_in_the_pass_through_matches`/`set_members_is_not_in_the_pass_through_matches`/
`set_is_not_in_the_pass_through_matches`; `samples_sketch_mode_merges_weighted_values_and_counts_weight_clamp`,
`samples_mode_concatenates_values_and_into_kind_emits_samples`,
`samples_mode_rate_mismatch_falls_back_to_distribution_and_counts`,
`samples_mode_cap_exceeded_falls_back_to_distribution_and_counts`, and
`samples_accumulator_converts_to_distribution_on_an_incoming_distribution` for the `distributions`
modes and their fallbacks; `set_estimate_mode_merges_hyperloglogs_and_estimates_distinct_members`,
`set_merge_of_two_series_is_a_union`, `set_members_mode_dedups_preserving_insertion_order`,
`set_members_mode_cap_exceeded_falls_back_to_set_and_counts`, and
`set_members_accumulator_converts_to_set_on_an_incoming_set` for the `sets` modes and their
fallback; `a_samples_series_never_survives_a_flush_even_with_series_retention_enabled` and
`a_set_members_series_never_survives_a_flush_even_with_series_retention_enabled` for tumbling
regardless of retention; and `same_resource_different_scope_flush_as_two_groups_carrying_their_scope`
for the `(resource, scope)` group key.

## Amendment: cumulative temporality as an opt-in mode (2026-09-11)

This ADR's original "Alternatives considered" rejected **cumulative (never-reset) counters**, in one
sentence: "it means state grows unbounded with series cardinality and a process restart resets every
series to zero with no way to detect that from the emitted stream, whereas tumbling-with-reset makes
each emitted value self-contained." Both halves of that objection have since been answered by work
done for other reasons, so `aggregate` now offers cumulative accumulation as an explicit,
named, off-by-default mode: `temporality: delta | cumulative` on `ComponentKind::Aggregate`
(`crates/logit-config/src/lib.rs`), default `delta` — byte-for-byte today's behavior.

### The restart half: `start_timestamp` is exactly the missing signal

[ADR `metrics-model-v2`](metrics-model-v2.md) added `MetricRecord::start_timestamp` — "unix
nanoseconds this series started accumulating at; `0` means unknown," OTLP's own
`start_time_unix_nano`. That field *is* the way to detect a restart from the emitted stream, and not
an invention of this amendment: it is the same signal OTLP consumers and Prometheus both use for
reset detection (Prometheus's `_created` series and its staleness/reset handling; OTLP's
`StartTimeUnixNano` on every cumulative data point). A consumer reading two consecutive points of one
series compares their start times: equal means the second point continues the first (so a value that
went *down* is a genuine non-monotonic movement, not a reset), and different means the series
restarted and the counter must be re-based rather than differenced.

So `temporality: cumulative` stamps every flushed `Sum`/`Histogram` with the series' **first-seen
time** — the timestamp of the first event ever absorbed into that series (`SeriesState::first_seen`,
captured from `event.timestamp`, the source's own clock). It never changes while the series lives. It
changes in exactly one circumstance: the series is evicted and a later record re-creates it, which is
precisely the event a consumer needs to be told about. A process restart is the same case by
construction — a fresh `Aggregator` holds no series, so every series' first flush after the restart
carries a new start time. The objection that there was "no way to detect that from the emitted
stream" no longer holds, because there is now a field whose whole job is to carry it.

### The unbounded-state half: the gauge amendment already built the answer

The "gauge series carry across the window boundary" amendment above had to answer the identical
concern for gauges, and did it with **two** bounds rather than one, for the reason argued there at
length: a windows-count TTL bounds only the *tail* of the retained set (how long one idle series
lingers), while a sustained stream of *C* never-repeating series names per window would hold *C × R*
series regardless of how short *R* is, so a second, independent bound on the *peak* is required.
Those two bounds are not gauge-specific in any way — they bound "how many accumulators survive a
flush, and for how long" — so this amendment reuses them verbatim rather than inventing a parallel
mechanism, and **renames them to match their now-general role**: `gauge_retention` →
`series_retention` (still a **count of windows**, never a duration — `interval` alone decides how
long a window is, and a retention expressed in time would silently mean a different number of windows
on every differently-tuned stage), `max_retained_gauge_series` → `max_retained_series`. (Pre-release, so a plain
rename with no serde aliases; the earlier amendments above keep the original spelling as the
historical record of what those fields were called when they were introduced. The throttled
diagnostic renamed with them: `gauge_retention_full` → `series_retention_full`.) Eviction keeps the
counters it already had: `logit.transform.series.evicted{reason="idle"}` for the TTL and
`{reason="cardinality"}` for the cap — `"idle"` rather than a new `"expired"`, because the TTL
eviction path and its counter already existed and renaming a shipped tag value would break the
dashboards `docs/design/internal-telemetry.md` documents for no gain.

Retention is therefore *required* for this mode, not merely advisable: with `series_retention: 0` (or
`max_retained_series: 0`) no accumulator can survive a flush, so every window would emit its own
increment wearing a `Cumulative` label — a silently wrong number for the one kind of consumer the
mode exists for. `temporality: cumulative` therefore requires `series_retention >= 1` (one window,
counted, is the minimum that lets a total cross a boundary at all) and `max_retained_series >= 1`;
`logit validate`/`logit run` reject that combination
(`crates/logit-pipeline/src/graph.rs`, rule 39; `docs/design/pipeline-graph.md`), the same
"an impossible bound is a config error, not a small one" treatment rules 15/18/38 already give.

### What the mode actually changes

- A **delta `Sum`** accumulator survives the flush (`flush`'s `retain` predicate, the same branch a
  retained gauge takes) and keeps summing. Every flush emits the running total as `Sum { temporality:
  Cumulative, monotonic: <as accumulated — the first record's flag, unchanged> }` with
  `start_timestamp` = the series' first-seen time.
- A **delta `Histogram`** gains a merge rule in this mode only: bucket counts add bucket-for-bucket,
  `sum` adds when *both* sides have one (a running sum missing a window's contribution understates
  the series outright, which is worse than reporting no sum — a consumer can tell `None` from a wrong
  number), and `min`/`max` fold across whichever sides have one (unlike a sum, an extreme observed
  over a subset of windows is still a genuine observation). Bucket **bounds must match exactly**
  (compared bitwise, so a `NaN` bound keys with itself): a record whose bounds differ from the
  accumulating series' has no correct merge — adding bucket *i* of one to bucket *i* of the other
  would attribute counts to bounds they were never observed under — so it is passed through
  untouched, the same treatment a kind conflict gets, under its own throttled diagnostic key
  (`histogram_bounds_mismatch`, rather than `kind_conflict`'s, since here the *kind* does match).
- **`delta` mode is unchanged, including for histograms.** A `Histogram` of either temporality stays
  pass-through there, exactly as it has been since this ADR was written: a delta histogram has no
  merge rule in `delta` mode, and giving it one would be a separate decision about what a tumbling
  histogram window means, not a side effect of adding a cumulative mode. `process`'s pass-through
  predicate is therefore mode-dependent for that one kind — it moved from an inline `matches!` to the
  `passes_through` free function precisely so the two places that must agree about it (that check and
  `Accumulator::new_for`'s `unreachable!` arm) can call the same code instead of restating the same
  list twice.
- An **incoming cumulative `Sum`** is still pass-through in *both* modes. `aggregate` re-summing an
  already-running total would double-count it, and nothing about the stage's output mode changes what
  an input record means. The same holds for an incoming cumulative `Histogram`.
- Nothing else changes: `Gauge`/`GaugeDelta` behave exactly as the gauge-retention amendment
  describes in either mode, and a `Distribution`/`Samples`/`Set`/`SetMembers` series still tumbles in
  either mode (each window's summary is self-contained — the reasoning in "Why
  `Samples`/`SetMembers`/`Set` series tumble regardless of `gauge_retention`" above is about the
  data, not about the mode).
- An idle retained cumulative series emits **nothing** that window, exactly like an idle retained
  gauge. Re-emitting an unchanged running total every idle window would multiply this stage's output
  by its retention depth, and a cumulative consumer already treats the last value it saw as standing
  until replaced.

### Who needs it: `prometheus_out`

The concrete consumer is [ADR `prometheus-scrape-and-exposition`](prometheus-scrape-and-exposition.md)'s
`prometheus_out`, which **skips** a delta `Sum`/`Histogram` (counted, with a throttled diagnostic
naming this mode) because the exposition format has no delta temporality to render it as, and
resolving deltas inside a sink would be exactly the unnamed, implicit summarization
[ADR `lossless-transit`](lossless-transit.md) forbids. So the intended pipelines are
`statsd_in -> aggregate(temporality: cumulative) -> prometheus_out` and
`internal -> aggregate(temporality: cumulative) -> prometheus_out`: the summarizing stage is
explicit, named in config, and bounded by its own two documented bounds, rather than hidden in a
sink. `influxdb_out` and `statsd_out` want the `delta` default, which is why it stays the default.

See `crates/logit-transforms/src/aggregate.rs`'s module doc ("Temporality: what a flushed
`Sum`/`Histogram` means"), its `passes_through`/`flush`/`Accumulator` for the implementation, and its
test module for the shapes this amendment adds coverage for:
`cumulative_mode_sums_accumulate_across_flushes_with_a_stable_start_timestamp`,
`cumulative_mode_keeps_the_accumulated_monotonic_flag`,
`an_idle_cumulative_series_emits_nothing_then_resumes_from_its_running_total`,
`cumulative_mode_histograms_accumulate_per_bucket`,
`a_histogram_window_without_a_sum_drops_the_running_sum_but_keeps_min_and_max`,
`a_histogram_with_mismatched_bucket_bounds_is_passed_through`,
`an_evicted_cumulative_series_restarts_with_a_new_start_timestamp`,
`the_cardinality_cap_evicts_cumulative_series_and_fires_series_retention_full`,
`a_cumulative_sum_input_still_passes_through_in_cumulative_mode`,
`delta_mode_sums_tumble_and_emit_delta_temporality_with_no_start_timestamp`,
`a_delta_histogram_still_passes_through_in_delta_mode`, and
`a_distribution_series_still_tumbles_in_cumulative_mode`; plus
`crates/logit-pipeline/src/graph.rs`'s four rule-39 tests and
`crates/logit-bench/tests/allocations.rs`' `aggregate_flush_cumulative_sums` (a retained cumulative
`Sum` costs a flush exactly what a retained gauge does -- 209 allocations for 100 spilled-attribute
series, `docs/design/memory.md`).
