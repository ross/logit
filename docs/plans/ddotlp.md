---
created: 2026-10-20
updated: 2026-10-21
---

# Enabling plan: OTLP metric kinds into `datadog_out`, and a `cumulative_to_delta` transform

## Context

`datadog_out` skips every OTLP metric kind beyond a delta monotonic `Sum` and a `Gauge`, so an
OTel producer's histograms, exponential histograms, summaries, and up-down counters can't reach
Datadog through it. Two ADRs decide the fix:

- [ADR `datadog-otlp-metric-mapping`](../adr/datadog-otlp-metric-mapping.md): `datadog_out`
  renders each kind as Datadog's OTel exporter does by default, statelessly, bin for bin for a
  sketch.
- [ADR `cumulative-to-delta-transform`](../adr/cumulative-to-delta-transform.md): a new
  `cumulative_to_delta` transform does the per-series differencing the exporter does inside its
  translator, for `datadog_out` and every other delta-only destination.

Stream key: `ddotlp`.

Goals:

- `otlp_in -> datadog_out` sends what the Collector's `datadogexporter` would send for the same
  delta input, and `otlp_in -> cumulative_to_delta -> datadog_out` does for cumulative input.
- Every sketch conversion is pinned against the Go code by a differential corpus, so "bin for
  bin" is a test, not a claim.
- The transit spec states each new cell, and the docs regions it generates follow.

Non-goals:

- The exporter's resource-attribute tag mapping, host resolution, and `otel.`-prefixed remapping
  (off by default in the exporter).
- `summaries.mode: noquantiles`, `sums.cumulative_monotonic_mode: raw_value`, and the exporter's
  alpha interval inference.
- A Dynatrace or New Relic sink; those plans consume the transform.
- Persisting the transform's state across a restart.

## Design

### W1: `datadog_out`'s stateless mappings

- **Sums.** A delta `Sum`, monotonic or not, is a `count`; a cumulative non-monotonic one is a
  `gauge`; a cumulative monotonic one stays `metrics.skipped{metric_kind=cumulative_sum}`, with a
  throttled diagnostic naming the coming `cumulative_to_delta`. It isn't `dropped` yet: no stage
  converts it, so an alerted operator would have nothing to change (the ADR's "Sequencing").
- **Explicit-bucket histograms.** A port of the Agent's `quantile.Agent.InsertInterpolate` and the
  translator's `getSketchBuckets` (bound fixes, summary overrides, the `MaxCount` cutoff),
  building a `Mapping::agent` `DdSketch` directly. The `histograms:` block (`mode:
  distributions | counters | nobuckets`, `send_aggregation_metrics`) in `logit-config`, with its
  validation and `script/schema`. Counters mode's `.bucket` series and the `formatFloat` tag form.
  A cumulative `Histogram` stays `metrics.skipped{metric_kind=histogram}`, and a cumulative
  `ExponentialHistogram` `metrics.skipped{metric_kind=exponential_histogram}`, each with the same
  diagnostic.
- **Exponential histograms.** A port of `exponentialHistogramToDDSketch`,
  `ConvertDDSketchIntoSketch`, and `sketches-go`'s `ChangeMapping` and `LogarithmicMapping`
  arithmetic. `rebin_to_agent` isn't reused; the ADR's "Exponential histograms" section says why.
  The helpers in `logit-core/src/sketch.rs` it needs (`key`, `lower_bound`) become visible to the
  port, or the port lives in `logit-core`.
- **Summaries.** `.quantile` gauges for every summary. A cumulative summary's `.count` and
  `.sum` aren't written, counted `logit.output.fields.omitted{reason="unresolved_cumulative"}`
  (a new counter, with its row in [internal-telemetry.md](../design/internal-telemetry.md)), once
  per summary record, with the diagnostic naming `cumulative_to_delta`; W2 adds the delta case.
- **Differential corpus.** `script/differential datadog-sketch` builds a small Go program against
  `datadog-agent/pkg/util/quantile` and `pkg/opentelemetry-mapping-go/otlp/metrics` at the pinned
  commit, `prom-text`'s pattern: Go modules in a docker volume, the image pinned by tag and
  digest. It writes `testdata/differential/datadog-sketch/`: hand-built histogram and exponential
  histogram cases, each beside the sketch the Go code builds (`k`, `n`, count, sum, average, min,
  max), and a Rust test reads them with no Go installed. Cases cover: open-ended and `(0, B]`
  buckets, a bucket inside one key, a count above 65,535, a negative and a zero-crossing bucket,
  every scale from −10 to 20, a one-observation point, a value on a key's half point, an
  oversize point, a key past 32,767, an empty point, and a cumulative point's rejection.
- **Transit spec.** `spec/transit/mapping.yaml`'s `datadog_out` cells for every changed row, new
  `otlp` cases where the existing ones don't reach a row (a delta up-down counter, each
  histogram mode, a summary), and `script/transit gen`. `crates/logit-proto/src/datadog/mod.rs`'s
  generated table follows.
- **Docs.** [`docs/known-gaps/datadog.md`](../known-gaps/datadog.md)'s "`datadog_out` sends no
  Agent-style `h`/`ms` aggregates, and no explicit-bucket `Histogram`" entry is rewritten to the
  Agent's five `h`/`ms` series, with a new entry for a cumulative kind until W2 lands.
  [`docs/datadog.md`](../datadog.md) gains the OTel-producer topology, and
  [ADR `delivery-semantics`](../adr/delivery-semantics.md)'s item 5 sentence on `datadog_out`'s
  cumulative `Sum` gets its amendment. W1 also amends the non-monotonic half of
  [ADR `telemetry-loss-vocabulary`](../adr/telemetry-loss-vocabulary.md)'s `datadog_out` bullet,
  which W1 makes stale by writing both non-monotonic forms.

### W2: summary temporality and `cumulative_to_delta`

- **`Summary.temporality`.** The field in `logit_core::Summary`, set `Cumulative` by every
  decoder, carried by the native wire, `event_json`, `ndjson.rs`, the human render, Lua's proxy
  and `Event.new`, and every encoder's handling decided. `otlp_out` and `prometheus_out` drop a
  delta summary, counted `metrics.dropped{reason="unresolved_delta"}`. `type_sizes.rs` and
  `docs/design/memory.md` if any size moves.
- **The transform.** The `ComponentKind` variant with `initial_value`, `max_staleness`, and
  `max_series`, its validation, `script/schema`, the `Transform` implementation in
  `crates/logit-transforms/src/cumulative_to_delta.rs`, every telemetry name in the ADR's table,
  and its rows in [internal-telemetry.md](../design/internal-telemetry.md).
  `docs/design/memory.md` gets a row for its per-series state, and `allocations.rs` a pin for one
  converted record.
- **`datadog_out`'s delta summary.** `.count` (no unit) and `.sum` as `count` points.
- **Loss counters flip.** With the transform in place, W1's interim counts become loss:
  `metrics.skipped{metric_kind=cumulative_sum | histogram | exponential_histogram}` becomes
  `metrics.dropped{reason="unresolved_cumulative"}`, and `fields.omitted{reason="unresolved_cumulative"}`
  becomes `fields.dropped{reason="unresolved_cumulative"}`. The transit spec cells and
  `internal-telemetry.md` follow, and
  the vocabulary ADR's `datadog_out` bullet gets an amendment for its cumulative half, saying the
  condition it named is met (W1 amended the non-monotonic half).
- **Transit flows.** `otlp_in -> cumulative_to_delta -> datadog_out` over every cumulative case,
  and `prometheus_in -> cumulative_to_delta -> statsd_out`, which turns a scrape's counters into
  `|c` lines where today they leave as `|g`, degraded. A flow for `otlp_out` joins if a delta-only
  case earns one.
- **Docs.** A `fixtures/` config for the OTel-to-Datadog path, an `examples/` entry if the
  transform's placement needs one, the AGENTS.md transform-table row, `docs/known-gaps/datadog.md`'s
  cumulative entry removed, and the Dynatrace and New Relic plans' pointers checked.

### Mechanics

Per AGENTS.md "Workflow": each workstream is a PR stacked on the one before, `pr-review` runs on
it before the fix rounds, every regression test is run once with its fix reverted and the PR body
says so, and the prose sweep (docs, ADRs, module docs, config field docs, `docs/known-gaps/`)
precedes each review. A sketch test's expected bins come from the differential corpus, never from
the Rust code under test.

## Workstreams

| # | Branch | PR title | Size | Depends on | Status |
|---|---|---|---|---|---|
| W0 | `ddotlp/w0` | `ddotlp/w0: ADRs and plan for OTLP metric kinds into datadog_out` | S | — | in review |
| W1 | `ddotlp/w1` | `ddotlp/w1: datadog_out maps OTLP sums, histograms, and summaries as Datadog's exporter does` | L | W0 | in review |
| W2 | `ddotlp/w2` | `ddotlp/w2: Summary temporality and the cumulative_to_delta transform` | L | W1 | in review |

## Settled questions

- **Summary temporality: decided by the maintainer, 2026-10-20.** `Summary` gains `temporality`
  ([ADR `datadog-otlp-metric-mapping`](../adr/datadog-otlp-metric-mapping.md)'s "Settled
  questions"), and `otlp_out` and `prometheus_out` drop a delta summary.
- **Reset points emit their full value: decided by the maintainer, 2026-10-20,** on the condition
  that it has no negative effect elsewhere.
  [ADR `cumulative-to-delta-transform`](../adr/cumulative-to-delta-transform.md)'s rule 3 meets
  it by requiring evidence of a restart (a value that went down, or a start moved past the
  previous point), and rule 4 differences every other start change, so a jittering or
  `start == ts` start never re-sends a series' whole total. It recovers the post-restart counts of
  a series with no start time, a Prometheus text-format counter included, at the cost of
  matching neither precedent.

## Findings

Recorded per workstream as each lands.

- **W0**: reading the source for the ADRs corrected three readings made while planning. `datadog.metric.as_type: rate` makes a delta sum a `RATE` whatever the interval;
  only the division by the interval needs the alpha gate. `InsertInterpolate`'s `binLow(k)` is
  the key's representative value, not its lower edge. And `ConvertDDSketchIntoSketch`'s
  one-observation case adds `sketches-go`'s bin-derived `GetSum`, not the point's own sum.
- **W1**: building it corrected or added to the ADR in these places.
  - **Units.** The exporter's `AddUnits` gate maps a UCUM unit to Datadog's name (`ms` to
    `millisecond`) and drops one it can't map, where `datadog_out` writes the record's unit
    verbatim; the ADR's "Units" said they match. It's a gap in `docs/known-gaps/datadog.md`.
  - **Degraded counts.** The transit spec needs a counter for a `degraded` cell that names one,
    so `datadog_out` counts `metrics.degraded{metric_kind="non_monotonic_sum" | "histogram" |
    "exponential_histogram"}`. A summary's cell names its `fields.omitted` instead, since a cell
    names one counter.
  - **Drops the ADR didn't name.** A bucket whose upper bound keys below its lower one makes the
    translator's `MapMetrics` fail; `datadog_out` drops the record,
    `metrics.dropped{reason="non_monotonic_bounds"}`. A histogram with the aggregation metrics on
    and no finite sum counts `fields.omitted{reason="no_sum"}`. An exponential histogram past the
    observation cap loses its aggregation series too, as the translator drops the point whole.
  - **Go's `math` is part of the result.** Go's amd64 `Log` and FMA `Exp` round differently from
    libm, which moves counts, so `logit_core::sketch::gomath` ports them; an arm64 Collector can
    differ in the last place. The pinned commit's pseudo-version sorts below the `v0.84.4` tag, so
    `go.mod` pins it with `replace`.
  - **One translator quirk is ported.** A bucketless `(-Inf, +Inf]` point keys every observation
    at `-1`, because Go's `int(+Inf)` is `math.MinInt64` on amd64.
  - **The Agent's `appendSafe` order.** The `Dogsketch` encoder now writes a split count's
    remainder before its full entries, as the Agent does; a `trimLeft` fold's order still
    differs, with equal per-key totals.
- **W2**: building it settled these cases the ADRs leave open.
  - **A start change and a layout change on one point.** The transform ADR lists the start
    change (its rule 4) before the layout change (its rule 5), so read literally a re-bucketed
    histogram whose start also moved would be differenced across two layouts. The transform
    checks the layout first: such a point is dropped as `layout_changed` and isn't counted
    `start_adjusted`.
  - **A summary's unknown sum.** `Summary.sum` is an `f64` with no absent form, so where the
    ADR's rule leaves a delta with no sum (a non-finite point, or a baseline with no finite sum),
    a delta summary carries `NaN`; `datadog_out` drops that `.sum` point as
    `metrics.dropped{reason="non_finite_value"}`.
  - **Validation the ADR didn't name.** Graph rule 94 rejects `max_staleness: 0s`, as the ADR
    says, and also `max_series: 0`, which could convert nothing.
  - **Names the ADR didn't give.** The `initial_value` enum is `logit_config::InitialValue`, and
    the transform's throttled diagnostics are `max_series_full`, `layout_changed`, and
    `non_finite_sum`. `otlp_out` and `prometheus_out` report a dropped delta summary under the
    existing `delta_temporality_unresolved` key, with a message naming `cumulative_to_delta`.
  - **The transit flows run `initial_value: keep`.** The driver runs each case alone, so under
    `auto` every first point would only seed a baseline and the flows would show nothing; the
    driver reads the wall clock through the transform, which is deterministic here because every
    case predates the run and `max_staleness` outlasts it.
  - **`statsd_out` and the other summary sinks.** `statsd_out` writes a delta summary's `.count`
    and `.sum` as `|c` counters, as for a delta histogram; `influxdb_out`, `graphite_out`, and
    `splunk_hec_out` have no temporality on the wire and write a delta summary as a cumulative
    one; `collectd_out` skips every summary, as before.
