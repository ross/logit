---
created: 2026-10-20
updated: 2026-10-25
---

# `cumulative_to_delta`: a stateful transform that turns cumulative sums, histograms, and summaries into deltas

## Status
Accepted. Built by [`docs/plans/ddotlp.md`](../plans/ddotlp.md)'s W2, in
`crates/logit-transforms/src/cumulative_to_delta.rs`; the plan's W2 findings record where the
build settled a case this record leaves open.

## Context
Several destinations take only delta temporality for an accumulating metric, and `logit` has no
stage that produces one from a running total:

- **Datadog.** Datadog's OTLP exporter and the Agent's OTLP ingest difference a cumulative
  monotonic `Sum` and a cumulative `Histogram` inside the translator, keeping per-series state
  ([ADR `datadog-otlp-metric-mapping`](datadog-otlp-metric-mapping.md)). `datadog_out` stays
  stateless, so it needs the conversion upstream.
- **Dynatrace and New Relic.** Dynatrace rejects a cumulative monotonic `Sum` and doesn't ingest
  a cumulative `Histogram`, and New Relic's OTLP path drops its cumulative state after 5 minutes
  without data ([`docs/plans/dynatrace-relay.md`](../plans/dynatrace-relay.md)'s "2. One runtime
  dependency", [`docs/plans/newrelic-relay.md`](../plans/newrelic-relay.md)).
- **`statsd_out` and `otlp_out`.** `statsd_out` writes a cumulative `Sum` as a `|g` line, counted
  degraded, and `otlp_out` relays a cumulative point unchanged to a backend that may want delta.

The producers are common: every `prometheus_in` scrape, OTel SDKs on their default cumulative
temporality, and `internal -> aggregate (temporality: cumulative)`. `aggregate` goes the other way
only: its cumulative mode turns deltas into running totals, and it passes an incoming cumulative
record through untouched
([ADR `aggregation-window-semantics`](aggregation-window-semantics.md)).

Two implementations of the conversion exist to copy from, and they disagree in places:

- **The Datadog translator**
  (`datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics`, `ttlcache.go` and
  `default_mapper.go`): one cache keyed by the point's dimensions without its start time, a
  3,600 s TTL refreshed on each accepted point, a point no newer than the cached one dropped, a
  reset read from a changed start time, a start time equal to the timestamp, or a decrease, and
  `sums.initial_cumulative_monotonic_value: auto | keep | drop` for the first value.
- **The Collector's `cumulativetodelta` processor**
  (`opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor`): a key that includes
  the start time, so a changed start time is a new stream; no timestamp ordering check at all;
  every reset point dropped and the baseline replaced; a bucket-layout change dropped as
  `bucket_mismatch`; exponential histograms converted, scale changes included; `max_staleness`
  (1 h) judged on the point's timestamp; `initial_value: auto | keep | drop`; and no bound on the
  number of series.

[Verified behavior](#verified-behavior) has the pinned commits and the code each fact comes from.

## Decision
**A new native transform, `cumulative_to_delta`, keeps the last point of each cumulative series
and replaces each later point with its difference from that one.** An operator places it where a
destination needs deltas. Its rules follow the Datadog translator where the two precedents
differ, except where noted below with the reason.

### What it converts

| Input | Output |
|---|---|
| cumulative monotonic `Sum` | delta monotonic `Sum` of the difference |
| cumulative `Histogram` | delta `Histogram`: each bucket count, the count, and the sum differenced |
| cumulative `ExponentialHistogram` | delta `ExponentialHistogram`, aligned by absolute bucket index, as the Collector processor does |
| `Summary` (cumulative by OTLP's definition) | delta `Summary`: `count` and `sum` differenced, quantiles unchanged |
| every other record: a `Gauge`, any delta record, a cumulative non-monotonic `Sum`, `Samples`, `Distribution`, `SetMembers`, `Set`, `GaugeDelta`, a record flagged `NO_RECORDED_VALUE` | passed through untouched, with no state |

- **A cumulative non-monotonic `Sum` passes through by default.** It's an up-down counter whose
  level is the reading: Datadog's translator sends it as a gauge, Dynatrace and New Relic ingest
  it as one, and the Collector processor skips it. Differencing it would throw the level away.
  The [non-monotonic amendment](#amendment-2026-10-21-non-monotonic-sums-on-request) converts one
  on request.
- **A `Summary` needs a temporality.** `logit_core::Summary` gains a `temporality` field, and every
  decoder sets it to `Cumulative`, so this transform can say its output is a delta.
  [ADR `datadog-otlp-metric-mapping`](datadog-otlp-metric-mapping.md)'s "Summary temporality" has
  the model change and what it costs each sink. The quantiles stay as the producer computed them:
  a quantile isn't additive and can't be differenced. Datadog's translator differences a
  summary's `count` and `sum` the same way and sends the quantiles as they are.
- **The output's `start_timestamp` is the previous point's timestamp**, and its timestamp is
  unchanged, as the Collector processor writes a delta. A first point emitted in full keeps its
  own `start_timestamp`.
- **A delta `Histogram` or `ExponentialHistogram` keeps a `min` or `max` only where it's exact.**
  A cumulative `min` is the lowest value since the series started. When it's lower than the
  previous point's, the new low was observed in this interval, so it's this delta's `min`;
  otherwise this interval's `min` is unknown and the field is cleared. `max` is the mirror image.
  This is the Datadog translator's "min from the last time window" rule
  (`hasMinFromLastTimeWindow`); the Collector processor clears both fields always. A first or
  reset point emitted in full keeps both, since its interval is the series' whole life.
- **A non-finite `sum` leaves the delta with no `sum`, and the baseline keeps the last finite
  one,** so the next finite point differences correctly. The Collector processor writes `0`;
  `Histogram.sum` is an `Option`, so the honest value is absent. A non-finite `Sum` value is
  dropped and counted, and the baseline is unchanged.

### The series key

A series is the metric name, unit, kind and monotonicity, the batch's resource attributes, its
scope (name, version, and attributes), and the record's event attributes. The start time is not
in the key: it's stored with the baseline and compared, as the Datadog translator does.

With the start time in the key, as the Collector processor has it, a producer restart creates a
second series and leaves the old one's state in memory until it goes stale, and nothing ever
reads it as a reset. Keeping the start time in the value replaces the state at the restart and
lets the reset rules below see it.

### First point, resets, and ordering

Each point, in batch order, takes the first rule that applies:

1. **No tracked series:** a first point. `initial_value` decides it:
   - `auto` (the default) emits the full value as the delta when the point has a nonzero
     `start_timestamp` that is at or after the component started and differs from the point's
     timestamp: the series began while this component was running, so its whole total is new.
     Otherwise it records the baseline and emits nothing. Both precedents use this rule.
   - `keep` always emits the full value.
   - `drop` never does.

   The name and values are the Collector processor's `initial_value`; the Datadog exporter calls
   the same setting `sums.initial_cumulative_monotonic_value`.
2. **A timestamp at or before the tracked one:** skipped, and the baseline is unchanged. An equal
   timestamp is a resent point; an earlier one was overtaken by a later total that already
   includes it. Neither carries anything new. This is the Datadog translator's rule for a
   monotonic sum, applied to every kind. The Collector processor has no ordering check, so it
   differences a resent point to zero and an older one into a false reset.
3. **A reset:** evidence that the counter restarted after the previous point. Either the value
   went down (a `Sum`'s value, or a histogram's or summary's count, or any bucket count), or the
   new `start_timestamp` is nonzero, later than the previous point's timestamp, and different
   from the point's own timestamp. The point's full value is emitted as the delta, whatever
   `initial_value` says, and becomes the baseline. The series restarted after a point this
   component saw, so everything in the new total accrued while it watched, and emitting it
   double-counts nothing.
4. **A start change with no reset:** any other new `start_timestamp`, with a value that didn't go
   down. A start computed from uptime jitters by a few milliseconds from point to point, a start
   can move to at or before the previous point, and some producers stamp `start_timestamp`
   equal to the timestamp on every point. None of these says the counter restarted, and the
   value that didn't go down says it didn't. The difference is emitted, the new start is stored
   with the baseline, and `logit.transform.series.start_adjusted` counts it; nothing is lost.
   Read as a reset, each such point would re-send the series' whole total, a large overcount.
5. **A layout change with no reset:** a `Histogram` whose bounds differ from the baseline's, or
   an `ExponentialHistogram` whose scale went up or whose zero threshold went down. Nothing says
   the counts restarted, so the full value could repeat counts already sent, and the difference
   has no meaning. The point is dropped and counted, and becomes the baseline. An
   `ExponentialHistogram` whose scale went down, or whose zero threshold went up, is not a layout
   change: the baseline is downscaled, or its low buckets folded into its zero count, and then
   differenced, as the Collector processor does.
6. **Otherwise:** the difference is emitted, and the point becomes the baseline.

Rules 3 and 4 depart from both precedents, in two ways:

- **What a reset point emits.** The Collector processor drops every reset point, and the Datadog
  translator applies `initial_value` to it, so under `auto` it drops the reset point of a series
  with no start time, which is every Prometheus text-format counter. Both lose the counts between
  the restart and the first point after it, which no later point can recover. A counter restarts
  from zero, so its new value is that increment.
- **What counts as a reset.** The Datadog translator reads any start change, and a start equal to
  the timestamp, as a reset; the Collector processor keys on the start, so any change is a new
  series. Each then applies `initial_value`, so a series that predates the process loses its
  increment at every start change, and one whose jittering start falls after the process started
  re-sends its whole total at every point under `auto`. Rule 3 emits a reset point's full value
  regardless of `initial_value`, so under those readings every jittering or `start == ts` series
  would re-send its total on every point. Rule 3 requires evidence of a restart instead, and rule
  4 differences every other start change. The maintainer accepted rule 3 on 2026-10-20 on the
  condition that it have no such effect elsewhere.

The Datadog translator emits a first or reset value only for the first point of a data-point
slice (`i == 0`, with a source comment asking why). `logit` has no data-point slice: each record
is judged alone.

### Staleness and the series bound

- **`max_staleness`** (default `1h`, the default of both precedents) evicts a series not updated
  for that long, by the wall clock. The Collector processor judges staleness by the point's own
  timestamp, which evicts a replayed backlog's series as fast as it arrives. `0s` is rejected:
  any time passing makes a series idle, so every series is evicted at the next batch and no point
  is ever differenced. Set `max_staleness` above the longest gap between a producer's reports.
- **`max_series`** (default `10000`, `aggregate`'s `max_retained_series` default) caps the table;
  a new series past it evicts the least recently updated one. Neither precedent has a bound, but a
  misconfigured producer that puts a request id in an attribute would otherwise grow the table
  without limit
  ([ADR `deployment-threat-model`](deployment-threat-model.md)). `0` is rejected as an
  impossible bound: the transform would clamp it to `1`, which `0` doesn't say.

An evicted series' next point is a first point. Unless it restarted after its last point, under
`auto` it records a baseline and emits nothing: the counts since its last point are lost. A
nonzero cardinality eviction count means `max_series` is too small for the traffic.

`auto` alone would let such a point through when the series began after the component started,
re-sending a total whose deltas already went out. So under `auto` an eviction keeps a record of
the evicted series, by key hash: its last timestamp, kept only for a series `auto` would pass. A
first point at or before the recorded timestamp is skipped as rule 2 skips one, and the record
stays. A later one removes the record and is emitted in full only on rule 3's restart test
against the recorded timestamp; otherwise it seeds a baseline. The record holds
at most `max_series` entries, oldest out first, and an entry pushed out returns its series to
first-point handling, where `auto` could re-send its total. The component's start time is never
moved, so a series first seen after an eviction is judged as before.

### Telemetry

Every name follows [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md):

| Name | Meaning |
|---|---|
| `logit.transform.metrics.converted{metric_kind}` | records replaced by a delta, a full first or reset value included |
| `logit.transform.metrics.skipped{reason="initial"}` | a first point that only recorded its baseline, as `initial_value` asked |
| `logit.transform.metrics.skipped{reason="duplicate" \| "out_of_order"}` | a point timestamped at, or before, the tracked one |
| `logit.transform.metrics.dropped{reason="layout_changed"}` | rule 5: the counts since the previous point are lost |
| `logit.transform.metrics.dropped{reason="non_finite"}` | a `Sum` whose value is NaN or infinite |
| `logit.transform.series.reset{cause="decrease" \| "restarted"}` | rule 3's resets: the value went down, else the start moved past the previous point |
| `logit.transform.series.start_adjusted` | rule 4: a start change that wasn't a reset, differenced and stored; nothing is lost |
| `logit.transform.series.evicted{reason="idle" \| "cardinality"}` | `max_staleness` and `max_series` evictions, `aggregate`'s names |
| `logit.transform.series.tracked` (gauge) | series in the table |
| `logit.component.events.discarded` | an event left with no metric, log, or span once its skipped and dropped records are removed |

A seeded first point is `skipped`, not `dropped`: the operator chose `initial_value`, and the
default matches both precedents. A record removed from an event that keeps other payloads is
counted only by the reason above.

### Placement and other rules

- **`aggregate`** passes a cumulative record through, so the order of the two matters only for
  what the window does. `cumulative_to_delta` ahead of `aggregate` lets the window fold the
  deltas into one point per window; behind it, it converts what `aggregate`'s
  `temporality: cumulative` produced, which only makes sense when another sink on the same
  `aggregate` needs the running totals.
- **Lossless transit.** The transform discards the running total, the series' start time, and a
  lifetime `min` or `max`. [ADR `lossless-transit`](lossless-transit.md)'s "Summarization is
  opt-in and named" rule permits that from a component whose stated purpose it is, placed by the
  operator and named in the graph. No codec converts temporality, which is why `datadog_out`
  stays stateless.
- **Delivery semantics.** Upstream of the transform, a cumulative point is idempotent: a resent
  point has the same timestamp and rule 2 skips it. Downstream, a delta is not: a resend
  double-counts at a destination that adds what it receives. This transform undoes the remedy
  [ADR `delivery-semantics`](delivery-semantics.md)'s item 5 gives a delta `Sum` (make it
  cumulative upstream), so the destination has to absorb the duplicate itself. A Datadog series `count` point overwrites at its `(series, timestamp)` (measured), so
  `datadog_out`'s count route is safe; a sketch, an OTLP delta backend, and a statsd line aren't
  known to be.
- **State is in memory.** A restart loses it, so each series' first point after the restart is a
  first point: under `auto` a series that started before the restart records a baseline and
  emits nothing, and the counts between the last point before the restart and the first after it
  are lost. Under `keep` the first point after the restart re-sends the series' whole total. The
  Datadog exporter and the Collector processor behave the same way.

## Alternatives considered
- **Convert inside each sink, as Datadog's translator does.** Every delta-only sink would carry
  its own copy of the state, the reset rules, and the bounds, and a sink's encoder would stop
  being a pure function of its batch. One transform serves `datadog_out`, the Dynatrace and New
  Relic sinks, `statsd_out`, and `otlp_out`.
- **Copy the Collector processor whole.** Its start-time key leaks the state of every restarted
  series until it goes stale, it has no ordering check (a resend becomes a false reset), and
  dropping every reset point loses the post-restart counts of a series with no start time.
- **Copy the Datadog translator whole.** Its `i == 0` rule has no meaning without data-point
  slices, it applies `initial_value` to resets, it doesn't convert an exponential histogram, and
  it has no series bound.
- **Convert a cumulative non-monotonic `Sum` too.** Every surveyed destination reads it as a
  gauge, and the difference of a level isn't a useful metric. Rejected as the default only: the
  [non-monotonic amendment](#amendment-2026-10-21-non-monotonic-sums-on-request) adds it as an
  opt-in, for a counter its producer can't mark monotonic.
- **Include and exclude lists by metric name, as the Collector processor has.** `logit` selects
  events with routing and filter components; nothing yet needs selection by metric name inside
  this one.
- **Emit the summary's `count` and `sum` as separate `Sum` records instead of a delta `Summary`.**
  That avoids the model change, but it makes the transform split a metric into a sink's shape,
  which is `datadog_out`'s choice of rendering, not a temporality conversion.

## Consequences
- `logit_core::Summary` gains `temporality`, and every sink that writes a summary has to decide
  what a delta one means; [ADR `datadog-otlp-metric-mapping`](datadog-otlp-metric-mapping.md)
  records each.
- The transform holds a table of up to `max_series` baselines, one histogram's buckets each at
  most; [`docs/design/memory.md`](../design/memory.md) gets its row when it lands.
- Losses on restart, eviction, and a layout change are inherent in the conversion, and are
  counted. Persisting the table across a restart is not decided.
- The Dynatrace and New Relic plans' delta requirement points here.

## Amendment (2026-10-21): non-monotonic sums on request

### Context
collectd's DERIVE is a counter: collectd computes its rate, `(v - v_prev) / dt`, as it does a
COUNTER's, and allows a negative rate only where a type's `types.db` minimum does. Every stock
DERIVE source has a minimum of 0 (`if_octets`, `disk_octets`, `cpu`), so collectd discards a
negative rate as a reset. collectd marks DERIVE non-monotonic only because it can't detect a
reset or a wrap, and `collectd_in` decodes it as a cumulative non-monotonic `Sum`
([ADR `collectd-binary-relay`](collectd-binary-relay.md)). Under the decision above it passes
through, and `datadog_out` writes it as a `gauge` of the running total, a value an operator
almost never wants. The up-down-counter reasoning that rejected the conversion doesn't fit a
counter whose producer can't promise monotonicity. COUNTER, a cumulative monotonic `Sum`,
already converts.

### Decision
**`cumulative_to_delta` takes `non_monotonic: pass | difference | reset_on_decrease`, default
`pass`.** Under either of the two converting values a cumulative non-monotonic `Sum` is a series
of its own, keyed apart from a monotonic `Sum` of the same name, and leaves as a delta
non-monotonic `Sum`. Its monotonic flag stays as the producer set it: the transform converts
temporality, and a destination that reads the flag still learns the producer didn't promise it.
`pass` leaves the decision above unchanged, so no existing config changes behavior.

- **`difference`: a decrease is a negative delta.** For an up-down counter sent to a destination
  that takes only deltas. Rule 3's decrease test doesn't apply, because a decrease is the
  reading, not evidence of a restart; its start-time test does, so a producer that stamps a start
  time still has a reset read. A counter that resets with no start time, such as a collectd
  DERIVE after a host reboot, reads as one large negative delta, the previous total subtracted
  from the new one. That's the cost of the setting's meaning, and the reason it isn't the one
  the collectd recipe uses.
- **`reset_on_decrease`: a decrease is a reset.** The point takes every rule a monotonic `Sum`
  takes, rule 3's decrease test included, so a DERIVE that went down emits its new value. This
  is collectd's own handling of a stock DERIVE, where the minimum of 0 discards the negative
  rate. A DERIVE whose value can go down for real reads each decrease as a restart and
  over-counts by the new value; such a source should use `difference`.
- **A first point** takes rule 1 unchanged. A collectd value list has no start time, so under
  `auto` its first point records a baseline and emits nothing.

The setting is an enum, not a boolean, because "convert" alone doesn't say what a decrease
means, and the two readings are both real: an OpenTelemetry up-down counter needs `difference`,
and collectd's DERIVE needs `reset_on_decrease`. A boolean would pick one for every producer.

`logit.transform.metrics.converted{metric_kind="non_monotonic_sum"}` counts the converted
records, apart from the monotonic ones.

### What reaches each sink
`datadog_out` writes a delta non-monotonic `Sum` as a series `count` of the raw value, as
Datadog's exporter writes any delta sum, counted
`logit.output.metrics.degraded{metric_kind="non_monotonic_sum"}`, so a Datadog rate or count
view reads it as a counter's increment. A negative delta leaves as a negative `count`. The other
sinks take it as they take any delta non-monotonic `Sum`, as `spec/transit/mapping.yaml`'s
`sum/delta_nonmonotonic` row states: `otlp_out` writes it exact, `statsd_out` as a signed `|c`
increment, and `influxdb_out` and `graphite_out` as a bare value. `prometheus_out` drops it, and
`collectd_out` skips it, since collectd has no delta type that can go down; each is counted.

### Consequences
- The opt-in departs from both precedents, which never convert a non-monotonic `Sum`.
- [`docs/datadog.md`](../datadog.md)'s "collectd to Datadog" and
  [fixtures/collectd-to-datadog.yaml](../../fixtures/collectd-to-datadog.yaml) give the
  collectd recipe, `reset_on_decrease`, and
  [`docs/datadog.md`](../datadog.md) states `difference`'s reset cost for operators.

## Verified behavior
Read from source on 2026-10-20; nothing was built or run.

- **Datadog translator:** `DataDog/datadog-agent` at `45132452b8555c0b0df112e3bd48e39284873e03`,
  `pkg/opentelemetry-mapping-go/otlp/metrics/`. `ttlcache.go`'s `putAndGetMonotonic` drops a
  point whose timestamp is at or before the cached one (`cnt.ts >= ts`) and reads a reset from
  `!isNotFirstPoint(startTs, ts, cnt.startTs) || dx < 0`; `isNotFirstPoint` treats a zero start
  as no reset. `metrics_translator.go`'s `shouldConsumeInitialValue` implements `auto` as
  process start before `startTs`, and `startTs != ts`; `default_mapper.go` gates the first value
  on `i == 0`. Its `Diff` for a histogram's count and sum accepts an equal timestamp.
- **Collector processor:** `open-telemetry/opentelemetry-collector-contrib` at
  `314dfba96505e27754f439a92478b274d2ab2a19`, `processor/cumulativetodeltaprocessor/`.
  `internal/tracking/identity.go`'s `Write` puts `StartTimestamp` in the key. `tracker.go`'s
  `Convert` applies `auto` as `metricID.StartTimestamp < t.startTime ||
  metricPoint.ObservedTimestamp == metricID.StartTimestamp` (seed, emit nothing), drops a reset
  point and replaces the baseline, and compares no timestamps; its sweeper evicts states whose
  last point's timestamp is older than `max_staleness`. `processor.go` converts only cumulative
  monotonic sums, histograms, and exponential histograms, clears `min` and `max`, and counts
  `cumulativetodelta_datapoints_dropped{reason="reset" | "initial" | "bucket_mismatch"}`.
  `tracker.go`'s comment on the reason constants says a histogram reset passes the full value
  through; the code drops it, as `processor_telemetry_test.go` expects.
