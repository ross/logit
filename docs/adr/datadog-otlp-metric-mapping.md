---
created: 2026-10-20
updated: 2026-10-20
---

# `datadog_out` maps OTLP metric kinds as Datadog's OTel exporter does, statelessly

## Status
Accepted. Not yet built: [`docs/plans/ddotlp.md`](../plans/ddotlp.md)'s W1 and W2 build it.

## Context
`datadog_out` writes a delta monotonic `Sum`, a `Gauge`, `Samples`, a `Distribution`, and a `Set`,
and skips every other kind an OTLP producer sends: a non-monotonic or cumulative `Sum`, a
`Histogram`, an `ExponentialHistogram`, and a `Summary`
(`crates/logit-proto/src/datadog/mod.rs`'s "Encode" table). An OTel-instrumented application's
metrics reach Datadog only through `otlp_out`, to the Agent's OTLP receiver or Datadog's OTLP
intake; an `otlp_in -> datadog_out` pipeline skips most of them.

Datadog already decided how each OTLP kind should look in Datadog. The Collector's
`datadogexporter` and the Agent's OTLP ingest share one translator,
`datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics`, and a Datadog user's dashboards and
monitors are built on what it produces: a delta histogram as a sketch with its counts spread over
the Agent's bins, a summary as `.quantile` gauges, and so on. Matching it means a metric reaches
the same series whether it went through the Collector, an Agent, or `logit`.

The translator differs from `datadog_out` in one structural way: it keeps per-series state, to
difference a cumulative monotonic `Sum`, a cumulative `Histogram`, and a summary's `count` and
`sum`. `logit` puts that state in an operator-placed transform instead
([ADR `cumulative-to-delta-transform`](cumulative-to-delta-transform.md)), so a codec stays a
function of its batch ([ADR `lossless-transit`](lossless-transit.md)'s "Summarization is opt-in
and named").

[Verified destination behavior](#verified-destination-behavior) has the pinned commits and the
code each fact below comes from.

## Decision
**`datadog_out` renders each OTLP metric kind the way the translator does under the exporter's
default configuration, bin for bin where the result is a sketch, and drops a cumulative kind the
translator would have differenced, naming `cumulative_to_delta` as the remedy.**

### Per kind

| Kind | `datadog_out` writes | Translator |
|---|---|---|
| delta `Sum`, monotonic or not | a series `count` point of the raw value | the same: any delta sum is `COUNT` |
| cumulative non-monotonic `Sum` | a series `gauge` point of the raw value | the same |
| cumulative monotonic `Sum` | nothing: `logit.output.metrics.dropped{reason="unresolved_cumulative"}` | differences it against its cache (`sums.cumulative_monotonic_mode: to_delta`) |
| delta `Histogram` | per `histograms.mode`, below | the same |
| cumulative `Histogram` | nothing: `metrics.dropped{reason="unresolved_cumulative"}` | differences it |
| delta `ExponentialHistogram` | a sketch, under every `histograms.mode`, plus the aggregation series below when asked | the same |
| cumulative `ExponentialHistogram` | nothing: `metrics.dropped{reason="unresolved_cumulative"}` | drops it ("Unknown or unsupported aggregation temporality") |
| `Summary` | `<name>.quantile` gauges, plus `.count` and `.sum` for a delta summary | the same, differencing a cumulative summary's `count` and `sum` |

The `unresolved_cumulative` reason follows `prometheus_out`'s `unresolved_delta`: a missing
upstream stage is loss under [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md),
since a different config would have delivered it. Its throttled diagnostic names
`cumulative_to_delta`.

**Sequencing.** That vocabulary ADR's "Decisions the vocabulary forces" keeps `datadog_out`'s
cumulative `Sum` at `output.metrics.skipped{metric_kind}` until a stage exists that converts it,
because an operator alerted on it would have nothing to change. That holds while W1 ships without
the transform, so W1 counts each cumulative kind above as
`metrics.skipped{metric_kind=cumulative_sum | histogram | exponential_histogram}`, with the same
diagnostic naming the coming transform. W2, which lands `cumulative_to_delta`, switches them to
`metrics.dropped{reason="unresolved_cumulative"}` and amends that bullet to say its condition is
met. The table states the end state. For a cumulative `ExponentialHistogram` the transform makes `logit` a
superset of the translator, which can't send one at all.

Two rows change what the sink does today. A delta non-monotonic `Sum` was skipped as having no
form; it becomes a `count`, as the translator sends it, so a delta up-down counter sums per
interval in Datadog. A cumulative non-monotonic `Sum` was skipped; it becomes a `gauge`.

### Histograms

A `histograms:` block holds the two settings the exporter has under the same names, so an
operator moving a Collector config finds them:

```yaml
histograms:
  mode: distributions          # distributions | counters | nobuckets
  send_aggregation_metrics: false
```

- **`mode: distributions`** (the default) writes one sketch per delta `Histogram` record, built as
  the translator's `getSketchBuckets` builds it, through the Agent's
  `quantile.Agent.InsertInterpolate`:
  - A bucket `(lower, upper]` with both bounds finite spreads its count over the Agent keys from
    `key(lower)` to `key(upper)`. Each deposit goes to the lowest key of a span, sized by the
    span's share of `upper - lower`, truncated, with the fractions carried; whatever is left goes
    to the last key, so the total is exact. It isn't placed at a midpoint.
  - The overflow bucket is a point mass at its finite lower bound, the underflow bucket one at
    its finite upper bound, and a `(0, upper]` bucket one at `upper`.
  - After insertion the sketch's count, sum, and average are the record's own (`avg = sum /
    count`). Its `min` and `max` are the lowest non-empty bucket's lower bound and the highest's
    upper bound where finite, replaced by the record's `min` and `max` when it carries them; if
    the two cross, the record's value wins.
  - A record with more than 4,096 × 65,535 observations isn't sketched, as the translator
    abandons it: `metrics.dropped{reason="oversized_sketch"}`.

  The result is `Mapping::agent` bins, so it uses `datadog_out`'s existing sketch route, and the
  W1 differential corpus pins it against the Go code bin for bin.
- **`mode: counters`** writes one `<name>.bucket` series `count` point per bucket, tagged
  `lower_bound:<f>` and `upper_bound:<f>` with the translator's `formatFloat` (`inf`, `-inf`,
  `0`, and otherwise Go's shortest `'g'` form with `.0` appended to a whole number: `5.0`, `0.5`,
  `1e+06.0`), and with no unit.
- **`mode: nobuckets`** writes no bucket output. It requires `send_aggregation_metrics: true`,
  and config validation rejects it otherwise, as the exporter does. The spelling is the
  exporter's, not `no_buckets`, so a copied value works.
- **`send_aggregation_metrics: true`** adds `<name>.count` (a `count`, no unit) and `<name>.sum`
  (a `count`, the record's unit), and `<name>.min` and `<name>.max` gauges where the record
  carries them. It applies to an `ExponentialHistogram` as well.

A `Histogram` whose `sum` is absent or non-finite gets no `.count` or `.sum` series, and its
sketch keeps the summary the insertion computed. The translator treats a non-finite sum this way
(`histInfo.ok = false`), but reads an absent sum as protobuf's default `0` and sends it, with an
average of `0`; `logit` doesn't write a sum the producer never reported.

### Exponential histograms

A delta `ExponentialHistogram` becomes a sketch as the translator's
`exponentialHistogramToDDSketch` and the Agent's `quantile.ConvertDDSketchIntoSketch` make it:

- The OTLP buckets are read as a `sketches-go` logarithmic DDSketch with γ = 2^(2^−scale) and
  index offset 0, each bucket's index used as is and the zero count in its zero bucket. The
  record's `zero_threshold` is ignored, as the Go code ignores it.
- `ChangeMapping` remaps it onto γ = 1.015625 with index offset `bias + 0.5`, the Agent's key
  space read as a floor. Each source bucket's count is split over every target key it overlaps,
  in proportion to the overlap's linear width, as float counts.
- A record with one observation skips the split: the observation goes to the key of the source
  bucket's representative value (`sketches-go`'s `GetSum`, which is computed from the bins).
- The float counts become integers in ascending signed-key order, rounding the running total and
  carrying the remainder from key to key. Target keys at or below 0 fold into the zero bin, a
  negative bucket becomes a negative key, and a key at or past 32,767 or a non-finite bucket
  boundary drops the record, counted `metrics.dropped{reason="sketch_range"}`.
- A record with more than 4,096 × 65,535 observations, zero count included, is dropped before
  conversion, counted `metrics.dropped{reason="oversized_sketch"}`.
- Count, sum, and average are then the record's own when its sum is finite; `min` and `max` are
  the record's where it carries them (each clamping the other), and the sum for both when the
  count is 1.

**`rebin_to_agent` can't be reused for this.** It moves each source bin's whole count to the one
Agent key of its representative value, rounds each bin on its own, keeps the source's summary,
saturates out-of-range keys instead of failing, and splits an oversize count in the opposite
order. Each of those changes the bins, so W1 ports `ConvertDDSketchIntoSketch` and
`ChangeMapping` as a separate path, on integer indices and float counts, and builds the result
under `Mapping::agent` so the sketch route doesn't rebin it again. The differential corpus pins
it against the Go code, ties at a key's half point and the multiplier form of the index
arithmetic included.

### Summaries

- **Quantiles.** Each `(q, v)` pair becomes a `<name>.quantile` gauge tagged `quantile:<q>`, with
  `q` in `formatFloat`'s form (`quantile:0.5`, `quantile:0.99`, `quantile:1.0`, `quantile:0`). A
  non-finite value is dropped, counted `metrics.dropped{reason="non_finite_value"}` as for any
  gauge. The exporter's `summaries.mode: noquantiles` isn't offered: nothing yet needs it.
- **`.count` and `.sum`.** A delta `Summary` adds `<name>.count` (no unit) and `<name>.sum` as
  `count` points. A cumulative one is a running total, so they aren't written and the quantiles
  still go out. They're lost because a stage is missing, not because the operator asked, so the
  count is loss, as for a cumulative `Sum`: `logit.output.fields.dropped{reason="unresolved_cumulative"}`,
  once per summary record, with the same throttled diagnostic naming `cumulative_to_delta`.
  `*.omitted` would hide it from an operator who alerts only on `*.dropped`. No
  `fields.dropped` counter exists yet; it's new, inside the loss vocabulary's pattern. A
  `cumulative_to_delta` upstream makes them sendable. Sequenced as the cumulative kinds are: W1,
  with no transform to place yet, counts them `logit.output.fields.omitted{reason="unresolved_cumulative"}`,
  and W2 switches the name to `fields.dropped`.

#### Summary temporality

`logit_core::Summary` gains `temporality: Temporality`. Today a summary has none, because OTLP
defines every summary as cumulative and Prometheus's is too, so the transform's delta summary has
nowhere to say what it is. `logit` is pre-release, so the field is a plain addition:

- **Size.** `Summary` grows from 40 to 48 bytes and stays well under `MetricKind`'s 176, which
  `Samples` sets ([`docs/design/memory.md`](../design/memory.md)); no event grows.
- **Decoders** (`otlp_in`, `prometheus_in`, Lua's `Event.new`, `event_json`, the native wire)
  set `Cumulative`, and the native wire and `event_json` carry the field so a delta summary
  survives a hop.
- **Sinks that write a summary assume it's cumulative.** OTLP has no delta summary and
  Prometheus's is a running total, so `otlp_out` and `prometheus_out` can't send a delta one as
  what it is. Each drops it, counted `metrics.dropped{reason="unresolved_delta"}` as
  `prometheus_out` already drops a delta `Sum`. Every other sink that renders a summary
  (`statsd_out`, `graphite_out`, `splunk_hec_out`, and the rest `spec/transit/mapping.yaml`
  lists) gets a `summary/delta` row in W2, decided against how that sink treats a delta `Sum`.

### Everything else the translator does

- **Exemplars** are ignored: the translator has no exemplar code. A record's exemplars are left
  off, uncounted, as for every metric route today.
- **Timestamps** are whole seconds, truncated, as `datadog_out` writes every series and sketch.
- **Interval** is the `_logit.datadog.interval` carrier where present, else `0`. The exporter
  sends `0` unless its alpha `InferIntervalForDeltaMetrics` gate is on.
- **Units.** The exporter's `AddUnits` gate is on by default, so a series carries the record's
  unit, as `datadog_out` already writes it; `.count` and `.bucket` drop it.
- **Names** are the record's own plus the translator's suffixes (`.count`, `.sum`, `.min`,
  `.max`, `.bucket`, `.quantile`). There's no `otel.` prefix or Datadog-name remapping: the
  exporter's `DisableAllMetricRemapping` gate is beta, so on by default, and skips both.
- **`datadog.metric.as_type`** isn't honored. The translator sends a delta `Sum` carrying
  `datadog.metric.as_type: rate` as a `RATE`; the Collector's `datadogreceiver` sets it on a
  Datadog rate. `logit`'s carrier for that is the private `_logit.datadog.type`, which
  `datadog_out` already reads and which `datadog_in` sets on a rate (as a `Gauge`, not a `Sum`). A
  public attribute that changes a wire type is what
  [ADR `private-attribute-namespace`](private-attribute-namespace.md) keeps carriers out of, so the
  attribute is a tag like any other, as the translator also leaves it.
- **`NO_RECORDED_VALUE`** points are skipped and non-finite number points dropped, each counted
  as `datadog_out` counts them today. The translator skips both silently.
- **Tags, host, and resource** come from attributes and the resource as `datadog_out` already
  derives them ([ADR `datadog-agent-and-intake-relay`](datadog-agent-and-intake-relay.md)'s
  decision 10), not from the translator's resource-attribute mapping tables. That divergence
  predates this record and is unchanged.

## Alternatives considered
- **Port the translator's state into `datadog_out`.** It would match the exporter for a cumulative
  input with nothing upstream, but the sink's encoder would carry per-series state, reset rules,
  and a TTL that the Dynatrace and New Relic sinks would each copy. The transform does it once.
- **Place a bucket's count at its midpoint.** Simpler, and what several other Datadog senders
  do, but a different sketch from the one the exporter sends for the same histogram, so a
  percentile in Datadog would differ by which path the metric took.
- **`no_buckets` and `send_counts` spellings.** Closer to `logit`'s config style, but an operator
  comparing a Collector config to a `logit` one would have to map names by hand. The values are
  enum strings either way.
- **Honor `datadog.metric.as_type`.** It would match the translator for a Collector-relayed rate,
  but let any producer's attribute change the wire type.
- **Keep skipping a cumulative kind once the transform exists.** The loss vocabulary reserves
  `skipped` for nothing to carry, an operator's choice, or a format with no form; a cumulative
  sum has a form once a stage upstream converts it. Skipping is right only until that stage
  exists, which is why W1 still skips.

## Consequences
- `datadog_out` gains a sketch builder for explicit buckets, a port of `InsertInterpolate`
  pinned by a differential corpus generated from the Go code (`script/differential`, like
  `prom-text`), and `spec/transit/mapping.yaml`'s `datadog_out` cells change for every row above.
- An OTLP producer's metrics can reach Datadog through `otlp_in -> datadog_out`, with
  `cumulative_to_delta` in between for cumulative producers.
- [`docs/known-gaps/datadog.md`](../known-gaps/datadog.md)'s "`datadog_out` sends no Agent-style
  `h`/`ms` aggregates, and no explicit-bucket `Histogram`" entry narrows to the Agent's five
  `h`/`ms` series.
- A delta `Summary` is a new state for every summary sink, and `otlp_out` and `prometheus_out`
  drop one.
- [ADR `delivery-semantics`](delivery-semantics.md)'s item 5 says `datadog_out`'s series route
  skips a cumulative `Sum`. That stays true through W1; from W2 it drops one, counted, and a
  resent delta `count` point still overwrites at its `(series, timestamp)`.
- [ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)'s `datadog_out` bullet gets an
  amendment in W2, when its condition (a stage that converts a cumulative `Sum`) is met. Its
  non-monotonic half is resolved in W1, which writes both non-monotonic forms.

## Open questions
- **Summary temporality (Ross).** Adding `temporality` to `Summary` makes `otlp_out` and
  `prometheus_out` drop a delta summary they can't express. The alternative is the transform
  emitting a summary's `count` and `sum` as separate delta `Sum` records, which needs no model
  change but has the transform render a sink's shape. This record takes the model change.

## Verified destination behavior
Read from source on 2026-10-20; nothing was built or run.

| Source | Commit | Role |
|---|---|---|
| `DataDog/datadog-agent`, `pkg/opentelemetry-mapping-go/otlp/metrics` and `pkg/util/quantile` | `45132452b8555c0b0df112e3bd48e39284873e03` | The translator and the Agent sketch. The exporter pins `otlp/metrics v0.84.0-devel.0.20260930220258-a8c2777f7050` and `pkg/util/quantile v0.83.0`; the pinned translator commit wasn't fetchable in a shallow clone, so the HEAD three weeks later was read |
| `open-telemetry/opentelemetry-collector-contrib`, `exporter/datadogexporter` and `pkg/datadog/config` | `314dfba96505e27754f439a92478b274d2ab2a19` | The exporter's defaults and feature gates |
| `DataDog/sketches-go` | `36e98e05d756ccb225b94882831c1443fb4ed535` (`v1.4.8`, what `pkg/util/quantile/go.mod` pins) | `DDSketch.ChangeMapping` and `GetSum` |
| `DataDog/opentelemetry-mapping-go` | `df97a19515da669eb403bfc2964ffe1258dc41cd` | The deprecated former home of the translator (`pkg/otlp/metrics/go.mod`: "Deprecated: use github.com/DataDog/datadog-agent/pkg/opentelemetry-mapping-go/otlp/metrics instead"); the exporter no longer imports it |

- **Defaults** (`pkg/datadog/config/config.go`'s `createDefaultConfig`): `histograms.mode:
  distributions`, `histograms.send_aggregation_metrics: false`,
  `sums.cumulative_monotonic_mode: to_delta`, `sums.initial_cumulative_monotonic_value: auto`,
  `summaries.mode: gauges`, `delta_ttl: 3600`. `HistogramConfig.validate` rejects `nobuckets`
  without `send_aggregation_metrics`.
- **Feature gates** (`pkg/datadog/internal/metadata/generated_feature_gates.go`): `AddUnits` and
  `DisableAllMetricRemapping` are beta, on by default; `InferIntervalForDeltaMetrics` is alpha,
  off.
- **Sums** (`metrics_translator.go`'s `mapToDDFormat`): a delta sum maps through
  `MapNumberMetrics(..., Count, ...)` whether monotonic or not; a cumulative non-monotonic one
  through `Gauge`; a cumulative monotonic one through `mapNumberMonotonicMetrics` under
  `to_delta`. An unspecified temporality is dropped.
- **Histograms** (`default_mapper.go`'s `MapHistogramMetrics` and `getSketchBuckets`): the bound
  adjustments (`+Inf` upper to the lower bound, `-Inf` lower to the upper bound, a `0` lower
  bound to the upper bound), `InsertInterpolate` per bucket, the count/sum/average override when
  `histInfo.ok`, the bucket-derived `min`/`max` overridden by the point's, the `MaxCount` cutoff,
  `.count` with `WithoutUnit`, `.min`/`.max` only for a delta point. `getLegacyBuckets` writes
  `.bucket` with `lower_bound`/`upper_bound` tags from `formatFloat`.
- **Exponential histograms** (`mapToDDFormat` maps only `Delta`; `exponential_histograms_translator.go`'s
  `exponentialHistogramToDDSketch` builds a `sketches-go` DDSketch with
  `NewLogarithmicMappingWithGamma(2^(2^-scale), 0)`; `pkg/util/quantile/ddsketch.go`'s
  `ConvertDDSketchIntoSketch` remaps it). `histograms.mode` doesn't apply to them.
- **Summaries** (`default_mapper.go`'s `MapSummaryMetrics`): `.count` through `MonotonicDiff`
  with no unit, `.sum` through `Diff`, `.quantile` gauges tagged by `getQuantileTag`, which is
  `"quantile:" + formatFloat(q)`.
- **`datadog.metric.as_type`** (`lossless_mapper.go`'s `mapNumberMetrics`): `rate` turns a delta
  sum's type into `RATE`; the value is divided by the interval only when the interval is nonzero,
  which needs the alpha gate, and an error is logged once otherwise. The attribute stays among the
  point's tags.
