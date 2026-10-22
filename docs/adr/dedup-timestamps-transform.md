---
created: 2026-10-21
updated: 2026-10-21
---

# `dedup_timestamps`: collapse readings of one series that share a timestamp, within a batch, before a sink that can't hold both

## Status
Proposed.

## Context
A remote-write series can't hold two samples at one millisecond. Prometheus and Mimir answer a
request holding two such samples with a `2xx`, keep the first, and drop the second silently. A
later request that repeats the series and millisecond with a different value draws
`400 duplicate sample for timestamp` ([Verified destination behavior](#verified-destination-behavior)).
`prometheus_out` collapses same-millisecond readings in a batch before it sends, keeping the later
reading, so the loss happens inside `logit`, where it can be counted, and the sample sent is the
series' current value. The model's timestamps are nanoseconds, so any source that stamps two
readings of one series less than a millisecond apart reaches this: `statsd_in` gauges,
`internal`, and a relay with no `aggregate` in the path.

The sink collapses readings in two places, and only one of them counts:

| Case | Where it collapses | What's counted |
|---|---|---|
| Two readings whose nanosecond timestamps differ but truncate to one millisecond | `remote_write::encode_counted`, through `Vec::dedup_by` on each series' samples after the stable sort | `logit.output.metrics.degraded{reason="sub_ms_collapsed"}` per dropped reading. The earlier reading's exemplars are kept. |
| Two readings with the same nanosecond timestamp | `events_to_families`' latest-wins pass over each family's series, since `prometheus_out`'s `partition` puts both events in one timestamp group | Nothing. The whole earlier `Series`, exemplars included, is replaced. |

`crates/logit-proto/src/prometheus/remote_write.rs`'s module doc ("What encode drops, and what it
counts", "Permitted normalizations") states the first row, and `spec/transit/mapping.yaml`'s
`record/same_instant` and `record/same_millisecond` rows state both. A degraded counter is the
wrong word for either: the reading never leaves `logit`, which is loss by
[ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)'s definition.

Exposition mode's registry holds one value per series and overwrites it between scrapes. That's
how the pull model works, not loss, and it stays uncounted: an operator who needs every value
between scrapes puts `aggregate` in front of the sink. This record doesn't change exposition; W3
documents the overwrite in the sink.

The other metric sinks, read from code on 2026-10-21:

- **`datadog_out` sends both.** Its encoder writes one series per point a record writes (one, or a
  histogram's or summary's expansion), at whole seconds rounded toward negative infinity
  (`crates/logit-proto/src/datadog/mod.rs`'s mapping table: `same_instant` and `same_millisecond`
  are `exact`). Nothing collapses or counts two points of one series in one second. Datadog keeps
  the higher of two such points in one request, for a gauge and a `count` alike, and the later
  write across requests ([Verified destination behavior](#verified-destination-behavior)). For a
  delta `count`, that loses the lower point's increment, silently.
- **`influxdb_out` keeps both.** `InfluxLineEncoder::encode` moves a point that shares a series
  and a timestamp with one already in the batch to the next free nanosecond, uncounted, because InfluxDB overwrites an identical
  `(measurement, tag set, timestamp)`. `next_nanosecond` is only the transit spec's reason label
  for that `normalized` cell. It needs no remedy.
- **`otlp_out` keeps both**, one data point per record (`crates/logit-proto/src/otlp/metrics.rs`'s
  mapping table). What the backend does with them is the backend's.
- **`statsd_out`** writes one line per record, and statsd lines carry no timestamp.
  **`graphite_out`** sends both; whisper keeps the last write per `(path, second)` at the
  destination, uncounted.

`aggregate` is the stage that already reduces a series to one point, and it doesn't fit a
timestamp-faithful relay ([Alternatives considered](#alternatives-considered)).

## Decision
**A new native transform, `dedup_timestamps`, collapses the metric records in one batch that share a series
and a timestamp bucket of a configured `precision:` into one record, the latest reading that can merge,
with delta kinds and observation kinds merged into it where a merge is sound, and every record
that can't merge passed through untouched.** An operator places it in front of a sink whose destination can't
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

Within a colliding set, a record that can't merge passes through: it stays on its own
event, untouched, and is counted `passed_through` with the reason the table gives. The
**surviving record is the latest-timestamp record among the records that can merge**, and the
later in batch order on a tie. Its event keeps its own, untruncated timestamp, and every other
mergeable record in the set is removed from its event. A `NaN` or infinite delta value, a marker
on a delta kind, a delta `Sum` of `Bool`s, and a kind with no merge rule can't merge on their
own. For the kinds that merge
(delta `Sum`, delta `Histogram`, delta `ExponentialHistogram`, and `Samples`), a layout, a
`sample_rate`, or a value type is compared with the survivor's, so the latest record that can merge
on its own sets them, and a record that differs from it passes through. The last-write-wins kinds
(`Gauge`, the cumulative kinds, and a stale marker on either) take part in last write wins whatever
their value type or layout. The rest fold into the survivor in batch order, and a record whose
addition would overflow the running total passes through. What the surviving record carries depends
on the kind:

| Kind | Rule |
|---|---|
| `Gauge`, any `MetricValue` (a `NaN` gauge included) | last write wins: the later value |
| `Sum`, cumulative, monotonic or not | last write wins |
| `Histogram`, `ExponentialHistogram`, `Summary`, cumulative | last write wins |
| `Sum`, delta, monotonic or not | summed, keeping the value type. A `NaN` or infinite value passes through (`non_finite`), as does a value of any other type from the survivor's, such as `I64` beside `F64` or `U64` beside `I64` (`mixed_type`), and one whose addition would overflow the total (`overflow`) |
| `Sum`, delta, of `Bool`s | passed through (`no_merge_rule`): two booleans sum to a count, an `I64`, so no merge keeps the type |
| `GaugeDelta` | passed through (`no_merge_rule`) |
| `Histogram`, delta | merged when the bucket bounds equal the survivor's: bucket counts added, `sum` added, `min` the lower and `max` the higher, under `aggregate`'s rules below. Other bounds pass through (`layout_mismatch`) |
| `ExponentialHistogram`, delta | merged when scale and zero threshold equal the survivor's: `positive` and `negative` bucket counts added by absolute bucket index, `zero_count` and `count` added, and `sum`, `min`, and `max` under `aggregate`'s rules. Another scale or zero threshold passes through (`layout_mismatch`) |
| `Summary`, delta (only `cumulative_to_delta` makes one) | passed through (`no_merge_rule`): quantiles don't add |
| `Samples` | values concatenated, earlier first, when the `sample_rate` equals the survivor's. Another rate passes through (`layout_mismatch`) |
| `SetMembers` | union: the members of both, each once |
| `Distribution` | `DdSketch::merge`, which re-bins the incoming sketch when the two sketches' mappings differ |
| `Set` | `HyperLogLog::merge`, a union |
| a record flagged `NO_RECORDED_VALUE`, on a `Gauge` or a cumulative kind | last write wins against the set's latest value: see below |
| a record flagged `NO_RECORDED_VALUE`, on a delta `Sum`, `Histogram`, or `ExponentialHistogram` | passed through (`no_recorded_value`); the delta values merge among themselves |

Last write wins needs no arithmetic, so it's safe for every kind, `NaN` included. A delta is an
increment, so the later one alone would lose the earlier's. Summing is the merge
[ADR `aggregation-window-semantics`](aggregation-window-semantics.md) already defines for the
same kinds, and where no merge is safe the record passes through untouched rather than being
dropped.

A passed-through record reaches the sink as a duplicate of the survivor, and a sink that can't
hold both drops it and counts it as loss: in remote-write mode,
`logit.output.metrics.dropped{reason="duplicate_timestamp"}` after
[the sink-side counter](#the-sink-side-counter) change. That's intended. A record passed through
for any reason but `no_recorded_value` or `no_merge_rule` is broken input, such as a `NaN`
increment, two layouts for one series, or a total past the integer range, so its loss is reported
as loss, not as an outcome the config asked for. `no_merge_rule` is the other exception:

- A passed-through `GaugeDelta` never reaches a sink as a duplicate. Every sink drops it, counted
  `dropped{reason="unresolved_gauge_delta"}`, and `aggregate` is what resolves it.
- A delta `Summary` pair, such as `cumulative_to_delta` makes, is legitimate input. `datadog_out`
  sends both points and Datadog keeps the higher. That loss is counted only once `datadog_out`
  gets the `duplicate_timestamp` counter ([The sink-side counter](#the-sink-side-counter)).
- A delta `Sum` of `Bool`s is rare: InfluxDB's boolean fields decode as gauges, so a Lua script
  is the likely source. It passes through because no merge keeps its type, the rule below.

A stale marker beside delta values isn't broken input. It passes through so
that no increment is lost, and neither sink drops the pair as a duplicate: `prometheus_out`'s
remote-write writes a flagged delta `Sum` as a stale marker and drops the unflagged delta,
counted `dropped{reason="unresolved_delta"}`, and `datadog_out` skips the marker, counted
`skipped{reason="no_recorded_value"}`, and sends the delta.

`GaugeDelta` passes through because merging it would reorder it. A statsd gauge's `Gauge` and
`GaugeDelta` share one accumulator in `aggregate`, which applies each delta in arrival order on
top of the current value. The series key includes the kind, so summing the deltas apart from
the absolute moves them past it: `g:+5`, `g:10`, `g:+1` in one datagram would resolve to 16
instead of 11. A `GaugeDelta` exists only upstream of an `aggregate`, which resolves it, so
merging it here gains nothing.

The delta merges never change a value's type. `aggregate` changes it for a mixed-type pair (`I64`
and `F64`, or `U64` and `I64`), an overflowing integer sum, and two `Bool`s, which sum to an `I64`
(`crates/logit-transforms/src/aggregate.rs`'s module doc), and this transform departs from it by
design. `aggregate` summarizes; `dedup_timestamps` keeps the
data as it arrived, and some destinations reject a type change on a series: InfluxDB answers a
field type conflict, and OTLP backends treat an int and a double for one series as a conflict.
So the record that would force a widening passes through instead, and no `widened` counter
exists here.

The histogram merges follow `aggregate`'s, in the module doc of
`crates/logit-transforms/src/aggregate.rs` and its histogram merge:

- **`sum`, `min`, and `max`** each become `None` once a contributing record lacks one, because a
  value over part of the observations is a wrong number a consumer can't detect.
- **`min` and `max`** fold only over records that observed something, through `fold_extreme`: a
  record whose buckets (and, for an `ExponentialHistogram`, `zero_count`) total zero contributes
  neither.
- **Bucket counts, `count`, and `zero_count`** add with an overflow check, not `aggregate`'s
  `saturating_add`. A saturated count changes a value silently, so a record whose addition would
  overflow any of them passes through (`overflow`), as an overflowing integer sum does. An `F64`
  `sum` that would leave the finite range passes through the same way.
- **`ExponentialHistogram` buckets** realign by absolute bucket index: the merged `positive` and
  `negative` ranges each start at the lower of the two offsets and cover both records' buckets.
  `aggregate` has no exponential-histogram merge, so this arithmetic is new.

The four observation kinds merge where `aggregate` merges them, with one difference each:

- **`Samples`** concatenate as `aggregate`'s `distributions: samples` does, and only under one
  rate, because a `Samples` record carries one `sample_rate` for all its values. `aggregate`
  falls back to a sketch on a rate mismatch. `dedup_timestamps` doesn't change a record's kind,
  so a record whose rate differs from the survivor's passes through, counted
  `passed_through{reason="layout_mismatch"}`, and the records at the survivor's rate still
  merge.
- **`SetMembers`** union as `aggregate`'s `sets: members` does, with no cap: both records are in
  the batch already.
- **`Distribution`** merges bin for bin under one mapping. Under two, `DdSketch::merge` re-bins
  the incoming sketch at each bin's representative value, within a bounded error, as it does for
  `aggregate`. Two mappings for one series at one timestamp are rare in a relay. Re-binning loses
  some precision, where last write wins would lose every observation in the earlier sketch.
- **`Set`** always merges: `HyperLogLog::merge` has no precondition.

A record flagged `NO_RECORDED_VALUE`, such as Prometheus's stale marker, carries no reading, so
it never enters arithmetic. What it does in a colliding set depends on the kind.

On a `Gauge` or a cumulative kind, a marker supersedes a value the way a later value would, so it
takes part in last write wins against the latest mergeable value in its set, and the record that
loses is counted `absorbed`. A record that can't merge still passes through, as above.

- **When the marker is the latest**, ties going to the later in batch order, the series ended.
  The marker survives and absorbs every mergeable value in the set.
- **When a value is later**, the value wins. The marker is absorbed.
- **Two markers** resolve by last write wins too.

On a delta `Sum`, `Histogram`, or `ExponentialHistogram`, a value is an increment, which is real
data a marker can't supersede. So every marker in the set passes through on its own event,
counted `passed_through{reason="no_recorded_value"}`, and the delta values merge among themselves
by their kind's rule. No increment is ever absorbed by a marker.

The last-write-wins case departs from `aggregate`, which passes every flagged record through
unmerged: `aggregate` folds values into a window, while here a marker and a gauge or cumulative
value at one instant are two readings of one series, and the later one is the series' state.

On a merged record:

- **Exemplars** are the union, earlier ones first, as `remote_write::encode_counted`'s own
  collapse keeps them: each is an observation of the series in the collapsed span.
- **`start_timestamp`** is the later record's under last write wins, and the earliest nonzero one
  under a sum, since the sum covers both intervals.
- **`description`** is the surviving record's, else the first one present. `flags` are the
  surviving record's.

### Events, not records

An `Event` carries any mix of metrics, a log, and a span, and `dedup_timestamps` acts only on metric records:

- An event whose records all collapse into the survivors' events, and that carries no log and no span, is
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
batch `dedup_timestamps` saw, and its same-millisecond pairs are already gone.

A window across batches is ruled out: it adds latency. Once a batch leaves, its reading has left
too, so a later batch's reading of the same instant can only be dropped, which keeps the wrong one
under last write wins and loses an increment under a sum, or held for the window, which adds the
latency and the wall-clock state this component exists to avoid.

That leaves a hazard this record doesn't close. Two readings in two batches reach the receiver in
two requests. With the same value, Prometheus and Mimir accept the second silently. With a new
value, they answer `400 duplicate sample for timestamp`, the sink reads it as `Rejected`, and the
whole batch drops ([ADR `sink-fault-classes`](sink-fault-classes.md)). What that costs depends on
the receiver:

- **Prometheus under remote-write 1.0**, the sink's default `version:`, stores nothing from the
  request, so every clean sample in the batch is lost, counted as dropped.
- **Prometheus under 2.0, and Mimir under either**, store the request's clean samples, so the
  sink's drop counter overstates the loss.

W3 files this in [`docs/known-gaps/prometheus.md`](../known-gaps/prometheus.md), beside the
out-of-order `400` entry, which has the same shape. Remedies, not decided here, include reading
the `400` body, which names the series, and making 2.0 the default `version:`.

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
does is loss: it removes only readings it merged or superseded, which the operator asked for, so
it emits no `*.dropped`. The loss a passed-through record causes is counted at the sink.

| Name | Meaning |
|---|---|
| `logit.transform.metrics.merged` | a reading summed or otherwise combined into the surviving reading: a delta `Sum`, a delta histogram with a matching layout, and the observation kinds' merges, a re-binned `Distribution` included |
| `logit.transform.metrics.absorbed` | a reading superseded by a later one under last write wins: a `Gauge`, a cumulative kind, or either side of a `NO_RECORDED_VALUE` marker and a value of one of those kinds |
| `logit.transform.metrics.passed_through{reason="non_finite" \| "overflow" \| "mixed_type" \| "layout_mismatch" \| "no_merge_rule" \| "no_recorded_value"}` | a record that shares its series and bucket with another but can't merge, forwarded untouched: the sink still sees the duplicate |
| `logit.component.events.discarded` | an event left with no metric, log, or span |

`merged` and `absorbed` count the readings that are gone, never the survivor. `passed_through`
counts each record forwarded beside the survivor, that is, each duplicate left for the sink: when
no record of the set can merge, every one but the first, so a passed-through pair counts once. A
record with no collision isn't counted. None of the names is in the loss vocabulary's table, and
none ends in a loss word.
`merged` is new under `logit.transform.*`. `passed_through` is `aggregate`'s word for a record
forwarded unmerged, read the same way here.

`absorbed` is narrower here than on `aggregate`. There it counts every record folded into a series,
summed or held, the series' first record included, so `metrics_in == absorbed + passed_through`.
Here it counts only a superseded reading; a combined one is `merged`, and the survivor and every
record with no collision go uncounted, so no such identity holds. In both components an absorbed
record leaves no record of its own downstream. `*.omitted` doesn't fit: it means a piece removed
from a record that's still delivered. `*.skipped` doesn't either: something was there to carry.

### The sink-side counter

`prometheus_out` in remote-write mode counts every reading it collapses as
`logit.output.metrics.dropped{reason="duplicate_timestamp"}`, in both places the Context table
names, the same-instant case included. That replaces
`logit.output.metrics.degraded{reason="sub_ms_collapsed"}`. The collapse stays: without it the
receiver drops one of the pair anyway, silently, and keeps the earlier one. With it, the later
reading survives and the loss is counted. The counter change lands in its own PR, not with this
record, and updates the transit spec's `same_instant` and `same_millisecond` rows for
`prometheus_out` with it. Exposition mode's registry overwrite stays uncounted.

`datadog_out` gets the same counter in a later PR: a series point that shares its series and
second with another in one request counts `logit.output.metrics.dropped{reason="duplicate_timestamp"}`,
since Datadog keeps only the higher. Distribution points don't count, because Datadog keeps both.

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

## Verified destination behavior

Each run sent hand-built requests to a destination, then read back what it stored. Raw logs are
kept outside the repository.

### Prometheus and Mimir

Prometheus v3.15.1 and Grafana Mimir 3.2.2, run 2026-10-21, each over remote-write 1.0 and 2.0,
with out-of-order windows at the default of `0`:

- **A duplicate inside one request**, same series and millisecond, is never an error. Both answer
  `2xx`, keep the first value, and drop the second silently. Mimir counts it in
  `cortex_discarded_samples_total{reason="sample_duplicate_timestamp"}`; Prometheus counts nothing
  the sender can see.
- **A duplicate in a later request with the same value** is accepted silently.
- **A duplicate in a later request with a different value** draws
  `400 duplicate sample for timestamp`. For that `400`, Prometheus under 1.0 stores nothing from
  the request. Prometheus under 2.0, and Mimir under 1.0 and 2.0, store the request's clean
  samples.
- **An out-of-order sample** behaves the same way: dropped silently inside one request, a `400`
  across requests, with the same split in what's stored.
- **2.0's `Samples-Written` response header** counted a silently dropped duplicate as written, so
  the sink can't detect the in-request drop from it.

### Datadog

`/api/v2/series`, `/api/v1/series`, and `/api/v1/distribution_points`, run 2026-10-21:

- **Two points of one series in one second, in one request**, collapse to one point that keeps the
  higher value, whichever comes first (both orders were sent to `/api/v2/series`, one to v1). That held for a gauge and a `count` alike; a `count` isn't
  summed. Each pair was two distinct positive values, so the rule is unverified for negative or
  equal values.
- **Across separate requests**, the later write wins.
- **`/api/v1/distribution_points`** keeps both points.

`dedup_timestamps` at `precision: 1s` ahead of `datadog_out` is the explicit remedy: it sums two
delta counts where Datadog would keep the higher, and keeps the later gauge where Datadog would
keep the higher.

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
  uncounted, and `datadog_out` keeps sending same-second pairs to a destination that keeps the
  higher. Counting the collapse as loss without a remedy leaves an operator with a nonzero
  `*.dropped` and nothing to configure.
- **Keep the first reading.** It's simpler, a seen-set in Lua already does it, and it's what
  Prometheus and Mimir keep inside one request. But a gauge's later reading is the current value,
  and `prometheus_out`'s own collapse, the exposition registry, and Datadog across requests all
  keep the later one. Destinations disagree within a request (first for remote-write, the higher
  for Datadog), so no choice matches all of them, and the later reading is the one with a meaning.
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
- `dedup_timestamps` doesn't stop a cross-batch duplicate with a new value from drawing a remote-write
  `400` that drops a whole batch. W3 records it in `docs/known-gaps/prometheus.md`
  ([Scope](#scope-one-batch-and-no-added-latency)).
- `datadog_out` counts a same-second series pair it sends in one request as
  `logit.output.metrics.dropped{reason="duplicate_timestamp"}`, in a later PR
  ([The sink-side counter](#the-sink-side-counter)).
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
1. **Datadog's rule for negative and equal values.** The run sent only distinct positive pairs.
   Does Datadog keep the higher of two negative points too? It changes nothing in this record,
   because `dedup_timestamps` decides before Datadog does, but `datadog_out`'s counter docs will
   state the rule.
