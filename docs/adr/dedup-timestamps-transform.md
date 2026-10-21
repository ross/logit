---
created: 2026-10-21
updated: 2026-10-21
---

# `dedup_timestamps`: collapse readings of one series that share a timestamp, within a batch, before a sink that can't hold both

## Status
Proposed.

## Context
A remote-write series can't hold two samples at one millisecond. `prometheus_out` keeps the later
reading and drops the earlier one, because Prometheus and Mimir answer
`400 duplicate sample for timestamp`, which the sink reads as `Rejected` and which would drop the
whole batch ([ADR `sink-fault-classes`](sink-fault-classes.md)). The model's timestamps are
nanoseconds, so any source that stamps two readings of one series less than a millisecond apart
reaches this: `statsd_in` gauges, `internal`, and a relay with no `aggregate` in the path.

The sink collapses readings in two places, and only one of them counts:

| Case | Where it collapses | What's counted |
|---|---|---|
| Two readings whose nanosecond timestamps differ but truncate to one millisecond | `remote_write::encode_counted`, through `Vec::dedup_by` on each series' samples after the stable sort | `logit.output.metrics.degraded{reason="sub_ms_collapsed"}` per dropped reading. The earlier reading's exemplars are kept. |
| Two readings with the same nanosecond timestamp | `events_to_families`' latest-wins pass over each family's series, since `prometheus_out`'s `partition` puts both events in one timestamp group | Nothing. The whole earlier `Series`, exemplars included, is replaced. |

`crates/logit-proto/src/prometheus/remote_write.rs`'s module doc ("What encode drops, and what it
counts", "Permitted normalizations") states the first row, and `spec/transit/mapping.yaml`'s
`record/same_instant` and `record/same_millisecond` rows state both. A degraded counter is the
wrong word for either: the reading never leaves `logit`, which is loss by
[ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)'s definition. Exposition mode's
registry also replaces a series' value with the later one, uncounted, which is the registry's
purpose: a scrape reads the current value.

The other metric sinks, read from code on 2026-10-21:

- **`datadog_out` sends both.** Its encoder writes one series per `(event, MetricRecord)`, one
  point each, at whole seconds rounded toward negative infinity
  (`crates/logit-proto/src/datadog/mod.rs`'s mapping table: `same_instant` and `same_millisecond`
  are `exact`). Nothing collapses or counts two points of one series in one second. Datadog was
  measured to store a *resent* series point once, the last write winning at its
  `(series, timestamp)` (`crates/logit-outputs/src/datadog.rs`). If two points in one request
  behave the same way, which isn't measured, the destination silently keeps one of them, and for
  a delta `count` that loses the other's increment.
- **`influxdb_out` keeps both.** `InfluxLineEncoder::encode` moves a point that shares a series
  and a timestamp with one already in the batch to the next free nanosecond, counted
  `normalized{reason="next_nanosecond"}`, because InfluxDB overwrites an identical
  `(measurement, tag set, timestamp)`. It needs no remedy.
- **`otlp_out` keeps both**, one data point per record (`crates/logit-proto/src/otlp/metrics.rs`'s
  mapping table). What the backend does with them is the backend's.
- **`statsd_out`** writes one line per record, and statsd lines carry no timestamp.
  **`graphite_out`** sends both; whisper keeps the last write per `(path, second)` at the
  destination, uncounted.

`aggregate` is the stage that already reduces a series to one point, and it doesn't fit a
timestamp-faithful relay ([Alternatives considered](#alternatives-considered)).

## Decision
**A new native transform, `dedup_timestamps`, collapses the metric records in one batch that share a series
and a timestamp bucket of a configured `precision:` into one record, the later reading, with
delta kinds summed into it.** An operator places it in front of a sink whose destination can't
hold two readings of one series at one instant. Separately, `prometheus_out`'s collapse becomes
counted loss.

### Configuration

```yaml
before_remote_write:
  type: dedup_timestamps
  sources: [statsd_in]
  precision: 1ms
```

- **`precision:`** (required) is the bucket width. Two records collide when their series keys are
  equal and `floor(timestamp / precision)` is equal. Use `1ms` in front of remote-write and `1s` in
  front of `datadog_out`, matching each wire's resolution. There's no default, because no width is
  right for every destination, and a wrong one either collapses readings the destination would
  keep or leaves the duplicates the sink drops. `0s` is rejected (graph rule 95, the next free
  number). For a timestamp at or after 1970 the floor is the truncation toward zero that
  `remote_write::encode_counted` applies.

No other field. Selection by metric name, and per-kind overrides, wait for a need, as they did
for `cumulative_to_delta`.

### The series key

A series is the metric name, unit, kind (temporality and monotonicity included), the batch's
resource attributes, its scope (name, version, and attributes), and the event's attributes, all
of them, `_logit.*` carriers included. That's `cumulative_to_delta`'s key with the start time
left out of it, for the same reason: a start time is a property of the reading, not the series.
`aggregate`'s key differs in leaving out resource and scope, which it groups by separately, and
InfluxDB string-field carriers, which it never merges.

The key is at least as fine as any sink's, by design. Two records whose key matches carry the
same attributes, so collapsing them loses no attribute, carrier or otherwise. The cost is that a
pair that differs only where a sink doesn't look (a private carrier, a scope, an `I64` and an
`F64` attribute that render as one label, two keys that sanitize onto one label name) isn't
collapsed here, and the sink still drops one of them, counted. Keying by a sink's own rendering
would close that, but it ties the transform to one sink's label rules.

### Which reading survives

Within a colliding set, the **surviving record is the one on the event with the latest
timestamp**, and the later in batch order on a tie. Its event keeps its own, untruncated
timestamp, and every earlier record in the set is removed from its event. What the surviving
record carries depends on the kind:

| Kind | Rule |
|---|---|
| `Gauge`, any `MetricValue` (a `NaN` gauge included) | last write wins: the later value |
| `Sum`, cumulative, monotonic or not | last write wins |
| `Histogram`, `ExponentialHistogram`, `Summary`, cumulative | last write wins |
| `Sum`, delta, monotonic or not | summed, with `aggregate`'s widening rule for mixed value types; a `NaN` or infinite value, or an integer overflow, passes through |
| `GaugeDelta` | summed: two adjustments at one instant apply as one |
| `Histogram`, delta | merged when the bounds are equal: bucket counts, `count`, and `sum` added, `min` and `max` the lower and higher; otherwise passed through |
| `ExponentialHistogram`, delta | merged when scale and zero threshold are equal, aligned by absolute bucket index; otherwise passed through |
| `Summary`, delta (only `cumulative_to_delta` makes one) | passed through: quantiles don't add |
| a record flagged `NO_RECORDED_VALUE` | takes part in last write wins as a reading; beside a delta kind's value, passed through |
| `Samples`, `SetMembers`, `Distribution`, `Set` | not keyed: these are collections of observations, every surveyed destination adds two of them, and no sink drops one for sharing a timestamp |

Last write wins needs no arithmetic, so it's safe for every kind, `NaN` included. A delta is an
increment, so the later one alone would lose the earlier's; summing is the merge
[ADR `aggregation-window-semantics`](aggregation-window-semantics.md) already defines for the
same kinds, and where no merge is safe the record passes through untouched rather than being
dropped. A passed-through record leaves the duplicate for the sink, which counts it as loss if it
drops one.

On a merged record:

- **Exemplars** are the union, earlier ones first, as `remote_write::encode_counted`'s own
  collapse keeps them: each is an observation of the series in the collapsed span.
- **`start_timestamp`** is the later record's under last write wins, and the earliest nonzero one
  under a sum, since the sum covers both intervals.
- **`description`** is the surviving record's, else the first one present. `flags` are the
  surviving record's.

### Events, not records

An `Event` carries any mix of metrics, a log, and a span, and `dedup_timestamps` acts only on metric records:

- An event whose records all collapse into later events, and that carries no log and no span, is
  removed from the batch and counted `logit.component.events.discarded`.
- An event that keeps another record, a log, or a span is forwarded with only its collapsed
  records removed. Its log and span are never touched.
- Surviving events keep their batch order. No event moves, so a mixed batch reaches the sink in
  the order it arrived, less the emptied events.

### Scope: one batch, and no added latency

`dedup_timestamps` holds no state between batches. That's enough for `prometheus_out`, because the sink
collapses within one batch too: `RemoteWriteOutput::send` encodes one batch into one `POST`, and
nothing in the runtime coalesces batches on the way to a sink. A router or `route_signals`
between `dedup_timestamps` and the sink only splits a batch, so every batch the sink sees is a subset of one
batch `dedup` saw, and its same-millisecond pairs are already gone.

A window across batches was considered and isn't recommended. Once a batch leaves, its reading
has left too, so a later batch's reading of the same instant can only be dropped, which keeps the
wrong one under last write wins and loses an increment under a sum, or held for the window, which
adds the latency and the wall-clock state this component exists to avoid. Two readings in two
requests also never reach the sink's in-request collapse. Whether a destination rejects a
duplicate *across* requests is an [open question](#open-questions), and the answer decides
whether a window is ever needed.

Two placement rules follow, for the operator docs:

- **Place `dedup_timestamps` last before the sink.** A stage after it can create new collisions: a `lua`
  stage, `line_protocol`'s fan-out, or a `set` that makes two keys equal.
- **A `dedup_timestamps` with several `sources:` sees each source's batches separately.** Two sources'
  readings of one series never meet in one batch, so they also never meet in one sink request.

### Mechanism: a batch-level hook on `Transform`

Collapsing into the *later* event needs the whole batch in view: `Transform::process` sees one
event at a time, and the runtime keeps each event it forwarded in the batch's `Vec` with no way
back to it. `Transform` gains one batch-level hook that runs before the per-event loop with the
batch's resource, scope, and `&mut Vec<Event>`, and may edit and remove events in place. The
default is a no-op, so no other transform's hot path changes, which keeps the trait's
"per-event, with per-batch hooks" shape (`crates/logit-pipeline/src/transform.rs`'s module doc).
`process_batch` counts the events the hook removed under `events.discarded`, as it counts a
`false` from `process`.

`dedup_timestamps` makes two passes: one hashes each keyed record's key and bucket into a table cleared per
batch, and only a collision triggers the second, which writes the surviving record and removes the
rest. A batch with no collision pays one hash and one probe per metric record.

### Telemetry

Names follow [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md). Nothing `dedup_timestamps`
does is loss: the operator asked for each outcome, so it emits no `*.dropped`.

| Name | Meaning |
|---|---|
| `logit.transform.metrics.absorbed{reason="superseded"}` | a reading replaced by a later one of its series, under last write wins |
| `logit.transform.metrics.absorbed{reason="merged"}` | a delta reading summed or merged into a later one |
| `logit.transform.metrics.passed_through{reason="non_finite" \| "overflow" \| "layout_mismatch" \| "no_merge_rule" \| "no_recorded_value"}` | a record that shares its series and bucket with another but has no safe merge, forwarded untouched: the sink still sees the duplicate |
| `logit.component.events.discarded` | an event left with no metric, log, or span |

A record with no collision isn't counted. `absorbed` is `aggregate`'s word for a record folded
into a series, and `passed_through` is `aggregate`'s word for a record it forwards unmerged, so an
operator reads both the same way on either component. `*.omitted` doesn't fit: it means a piece
removed from a record that's still delivered, and an absorbed reading isn't delivered. `*.skipped`
doesn't either: something was there to carry.

### The sink-side counter

`prometheus_out` in remote-write mode counts every reading it collapses as
`logit.output.metrics.dropped{reason="duplicate_timestamp"}`, in both places the Context table
names, the same-instant case included. That replaces
`logit.output.metrics.degraded{reason="sub_ms_collapsed"}`. The collapse stays: dropping one
reading is still better than a `400` that drops the batch. The counter change lands in its own
PR, not with this record, and updates the transit spec's `same_instant` and `same_millisecond`
rows for `prometheus_out` with it. Exposition mode's registry replacement stays uncounted.

### Why this isn't the retired kind

[ADR `routing-by-condition-is-lua`](routing-by-condition-is-lua.md) retired a `dedup` kind as an
event-level seen-set: forward an event the first time, discard a repeat. This component isn't
that. It removes no event because it was seen before. It collapses metric *readings* of one
*series* at one *instant*, keeps the later one, and sums deltas, and an event leaves only when
nothing is left on it.

Plain `dedup` stays free for a possible future event-level component, such as one suppressing a log
message that repeats continually.

A `lua` component can keep a table keyed by a series it computes, since a script's globals persist
across calls ([`docs/design/lua-api.md`](../design/lua-api.md)'s "Script contract"). What it can't
do is choose the later reading without adding latency:

- `process(event)` runs once per event, and the script has no end-of-batch hook. It can't know a
  later reading is coming until it arrives.
- An event handle is consumed once `process` returns it, so a script can't remove a record from,
  or add a delta into, an event it already forwarded.
- The only alternative is to hold every metric event and emit it from `flush()`, on the
  component's `interval`. That adds the interval's latency, and `flush()` runs in a root context
  with an empty resource and no scope, so the script would rebuild both.

A script can keep the *first* reading per event, which is the retired seen-set. It can't keep the
last one, or sum two, within a batch at no latency. That's the same kind of ground on which
[ADR `consistent-sampling-component`](consistent-sampling-component.md) brought `sample` back.

## Alternatives considered
- **`aggregate`.** It's the existing stage that leaves one point per series, and it's the wrong
  one for a timestamp-faithful relay. It stamps every emitted point with the flush's wall-clock
  time and runs wall-clock windows
  ([ADR `aggregation-window-semantics`](aggregation-window-semantics.md)'s "Emitted timestamp is
  flush wall-clock"), so it changes the time of every point, folds a replayed backlog into the
  current window, and adds up to a window of latency. It also forwards unmerged every record on
  an event that carries its source's own timestamp, and every kind it has no merge rule for, such
  as an OTLP cumulative `Sum` (that ADR's 2026-10-19 timestamped pass-through amendment), so the
  duplicates it's asked to remove pass through it. `aggregate` stays right for a live delta flow
  that needs a window anyway. `dedup_timestamps` is for a relay that keeps source timestamps.
- **A sink-side option** (`prometheus_out` merging deltas, or a `duplicates:` field). Every sink
  that needs it would carry its own copy of the key, the per-kind rules, and the counters, and an
  encoder would stop being a pure rendering of its batch. The rule that summarization is opt-in
  and named ([ADR `lossless-transit`](lossless-transit.md)) puts it in a component the operator
  places and the graph shows.
- **Do nothing.** The sink keeps collapsing with a degraded counter, the same-instant case stays
  uncounted, and `datadog_out` keeps sending same-second pairs to a destination that probably
  keeps one. Counting the collapse as loss without a remedy leaves an operator with a nonzero
  `*.dropped` and nothing to configure.
- **Keep the first reading.** It's simpler, and a seen-set in Lua already does it. A gauge's later
  reading is the current value, and remote-write's collapse, the exposition registry, and Datadog's
  overwrite all keep the later one, so keeping the first would disagree with every destination.
- **A cross-batch window.** See [Scope](#scope-one-batch-and-no-added-latency).
- **A key built from the sink's rendering** (labels after sanitization, private attributes left
  out). It closes the residual collisions the sink still drops, at the cost of one key per
  destination dialect. Not needed until a counted residue shows up in practice.

## Consequences
- **Operators add `dedup_timestamps` to every timestamp-faithful flow into remote-write or `datadog_out`**
  whose source can stamp two readings of one series inside one bucket, at `precision: 1ms` or
  `1s`. That's almost every such flow from a sub-millisecond source: `statsd_in` gauges,
  `internal`, and any relay with no `aggregate` in it. A flow through `aggregate` doesn't need
  it. After the counter change, a flow without it shows a nonzero
  `logit.output.metrics.dropped{reason="duplicate_timestamp"}`, which is the prompt to add it.
- `Transform` gains a batch-level hook, defaulting to a no-op.
- The graph gets rule 95 (`precision` of `0s`), `logit-config` gets a `ComponentKind` variant, and
  `script/schema` regenerates `schema/logit.schema.json`.
- [`docs/design/memory.md`](../design/memory.md) gets a row for the per-batch table, and
  `crates/logit-bench/tests/allocations.rs` a pin, when the transform lands.
- The transit spec gains flows with `dedup_timestamps` in front of `prometheus_out` and `datadog_out`, and the
  `same_instant` and `same_millisecond` rows change with the sink counter.
- If this record is accepted, these must change, in the PR that builds the component:
  - [ADR `routing-by-condition-is-lua`](routing-by-condition-is-lua.md) gets an amendment beside
    its `sample` one, saying `dedup_timestamps` is back for series-level collapse and
    the event-level seen-set stays retired.
  - [ADR `consistent-sampling-component`](consistent-sampling-component.md)'s two statements that
    `dedup` stays retired.
  - [`docs/known-gaps/transforms.md`](../known-gaps/transforms.md)'s predicate-shaped work entry,
    which lists deduplication as Lua work.
  - [`docs/design/pipeline-graph.md`](../design/pipeline-graph.md)'s comment that `dedup` was
    retired before landing.
  - [`docs/design/lua-api.md`](../design/lua-api.md)'s note that `dedup` was retired rather than
    built.
  - `crates/logit-config/src/lib.rs`'s comment before the `InfluxDbOut` variant and the doc of
    `a_retired_kind_is_a_deserialization_error_not_an_unimplemented_kind`, which both name `dedup`
    as retired.

## Open questions
1. **Cross-request duplicates.** Do Prometheus and Mimir reject a sample whose series and
   millisecond were written by an *earlier* request, and what do they store from a request they
   answer `400` for a duplicate? If they store the rest of the request, the sink's `Rejected`
   reading over-counts loss. Neither has been verified here, and the answer decides whether a
   window is ever needed.
2. **Datadog in one request.** Measure what Datadog stores for two points of one series in one
   second within one request: one, both, or a sum, for a gauge and for a delta `count`. If it keeps
   one, should `datadog_out` count it as `dropped{reason="duplicate_timestamp"}` too?
3. **The counter names.** `absorbed{reason}` reuses `aggregate`'s untagged counter with a new tag,
   and `aggregate`'s identity (`metrics_in == absorbed + passed_through`) doesn't hold here, since
   a record with no collision isn't counted. Is that acceptable, or does `dedup_timestamps` want its own word?
4. **Observation kinds.** Should `Samples`, `SetMembers`, `Distribution`, and `Set` merge too, for
   a destination that turns out to overwrite them? Each has an exact merge.
5. **The exposition registry.** Should its replacement of a series' value get a non-loss counter,
   or stay uncounted as a scrape's own semantics?
