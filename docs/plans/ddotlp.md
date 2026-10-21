---
created: 2026-10-20
updated: 2026-10-20
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
  `gauge`; a cumulative monotonic one is `metrics.dropped{reason="unresolved_cumulative"}` with a
  throttled diagnostic naming `cumulative_to_delta`.
- **Explicit-bucket histograms.** A port of the Agent's `quantile.Agent.InsertInterpolate` and the
  translator's `getSketchBuckets` (bound fixes, summary overrides, the `MaxCount` cutoff),
  building a `Mapping::agent` `DdSketch` directly. The `histograms:` block (`mode:
  distributions | counters | nobuckets`, `send_aggregation_metrics`) in `logit-config`, with its
  validation and `script/schema`. Counters mode's `.bucket` series and the `formatFloat` tag form.
  A cumulative `Histogram` is `unresolved_cumulative`.
- **Exponential histograms.** A port of `exponentialHistogramToDDSketch`,
  `ConvertDDSketchIntoSketch`, and `sketches-go`'s `ChangeMapping` and `LogarithmicMapping`
  arithmetic. `rebin_to_agent` isn't reused; the ADR's "Exponential histograms" section says why.
  The helpers in `logit-core/src/sketch.rs` it needs (`key`, `lower_bound`) become visible to the
  port, or the port lives in `logit-core`.
- **Summaries.** `.quantile` gauges for every summary. A cumulative summary's `.count` and
  `.sum` aren't written, counted as loss, `logit.output.fields.dropped{reason="unresolved_cumulative"}`
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
  cumulative `Sum` gets its amendment.

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
| W1 | `ddotlp/w1` | `ddotlp/w1: datadog_out maps OTLP sums, histograms, and summaries as Datadog's exporter does` | L | W0 | not started |
| W2 | `ddotlp/w2` | `ddotlp/w2: Summary temporality and the cumulative_to_delta transform` | L | W1 | not started |

## Open questions

- **Summary temporality.** [ADR `datadog-otlp-metric-mapping`](../adr/datadog-otlp-metric-mapping.md)'s
  open question: a model field that two sinks can't express, against the transform emitting
  separate `Sum` records.
- **Reset points emit their full value.**
  [ADR `cumulative-to-delta-transform`](../adr/cumulative-to-delta-transform.md)'s rule 3 departs
  from both the Collector processor (drops every reset point) and the Datadog translator (applies
  `initial_value` to it). It recovers the post-restart counts of a series with no start time, a
  Prometheus text-format counter included, at the cost of matching neither precedent.

## Findings

Recorded per workstream as each lands.

- **W0**: reading the source for the ADRs corrected three readings made while planning. `datadog.metric.as_type: rate` makes a delta sum a `RATE` whatever the interval;
  only the division by the interval needs the alpha gate. `InsertInterpolate`'s `binLow(k)` is
  the key's representative value, not its lower edge. And `ConvertDDSketchIntoSketch`'s
  one-observation case adds `sketches-go`'s bin-derived `GetSum`, not the point's own sum.
