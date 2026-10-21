---
created: 2026-10-20
updated: 2026-10-20
---

# `influxdb_out` writes histograms and summaries in Telegraf's `prometheus-v1` shape

## Status
Accepted

## Context
Line protocol has no metric type. A point is a measurement, tags, fields, and a timestamp, so a
histogram or a summary reaches InfluxDB only as a naming convention over fields. `influxdb_out`
had its own convention: a histogram as `bucket_<bound>=<n>u` per finite bound, with `sum`, `min`,
`max`, and the overflow bucket dropped uncounted; a summary as `q<quantile>=<v>`, with `count` and
`sum` dropped; and a `Distribution` or `Samples` as `count=<n>u,sum=…,p50=…,p90=…,p99=…`. No
other writer uses those names, so no reader that understands histograms in InfluxDB understands
`logit`'s, and the dropped components were lost with no counter.

The writers an InfluxDB user meets already agree on a convention. This ADR adopts it after reading
the source of three pinned upstream projects (nothing was executed):

| Project | Tag | Commit |
|---|---|---|
| `influxdata/telegraf` | v1.40.2 | `2e0a2ed1de9e091452ace949007cbab7cbfeb570` |
| `influxdata/influxdb-observability` | v0.5.12 | `7bc96fcf948bd65b6d9cc64bd8e4b1417bf1da53` |
| `open-telemetry/opentelemetry-collector-contrib` | v0.163.0 | `df849b5403c041d5e799e7846392a6f18b0b55ae` |

Telegraf's `inputs.opentelemetry` and the Collector's `influxdbexporter` both pin
`influxdb-observability`'s `otel2influx` at v0.5.12, so they write identical shapes. `otel2influx`
offers three schemas (`common/metrics.go`'s `MetricsSchemata`):

- **`telegraf-prometheus-v1`**, the exporter's default (`exporter/influxdbexporter/factory.go`)
  and Telegraf's input's default (`metrics_schema: prometheus-v1`). One point per data point,
  the metric name as the measurement. A histogram (`enqueueHistogram`) writes `count` always,
  `sum`, `min`, and `max` when the point has them, one field per bound named by
  `strconv.FormatFloat(b, 'f', -1, 64)` holding the **cumulative** count up to it, and `+Inf`
  holding the total. A summary (`enqueueSummary`) writes `count`, `sum`, and one field per
  quantile, named the same way. Every value is a `float64`.
- **`telegraf-prometheus-v2`**: measurement `prometheus`, a `<name>_count`/`_sum`/`_min`/`_max`
  point, and a point per bucket or quantile tagged `le` or `quantile` with a
  `<name>_bucket` or `<name>` field.
- **`otel-v1`**: measurement `<name>_<unit>_<type>`, non-cumulative `uint64` buckets with no
  overflow bucket, and no summary at all (an error).

Telegraf's Prometheus parser at its default `metric_version: 1`
(`plugins/parsers/prometheus/metric_v1.go`) writes the `prometheus-v1` shape too, with two
differences: it names a bound with `'g'` formatting (`1e+06` where `otel2influx` writes
`1000000`), and it writes no `min` or `max`, which Prometheus doesn't have.

The read-back side, `influx2otel` v0.5.12 (`influx2otel/metrics.go`), reads a `prometheus-v1`
point as a histogram when it carries `count`, `sum`, or a numeric field name, requires `count`,
`sum`, and every bucket to be a `float64` (an `i` or `u` suffix is an error), and de-cumulates the
buckets. It ignores `min`, `max`, and `start_time_unix_nano`.

No writer has a form for an OTLP exponential histogram. `otel2influx`'s `enqueueMetric` falls to
`unknown metric type` in all three schemas, and `WriteMetrics` returns that as a permanent error
before the batch is written, so the Collector loses the whole request. Telegraf's Prometheus
parser reads no native histogram.

On a non-finite value, Telegraf's influx serializer (`plugins/serializers/influx/influx.go`,
`appendFieldValue`) and `line-protocol/v2`'s `NewValue`, which the exporter uses, both discard the
field and write the rest of the point. `influxd` itself refuses `NaN` and `inf` with a `400` that
fails the whole write ([ADR `influxdb-line-protocol-relay`](influxdb-line-protocol-relay.md)'s
"Verified `influxd` behavior" item 5).

## Decision
`influxdb_out` writes a `Histogram`, a `Summary`, a `Distribution`, and a `Samples` record in
`otel2influx`'s `telegraf-prometheus-v1` shape, on its existing per-record line: the record name
as the measurement, one line per record. There's no option for another schema.

Worked examples, with a tag `host=a` and the field order the encoder writes:

```text
http_req_duration,host=a count=10,sum=4.2,min=0.01,max=2.5,0.1=3,1=8,+Inf=10 1700000000000000000
rpc_latency,host=a count=7,sum=3.5,0.5=0.2,0.99=0.9 1700000000000000000
```

The first is a histogram with bounds `[0.1, 1]`, per-bucket counts `[3, 5, 2]`, a sum of `4.2`, a
min of `0.01`, and a max of `2.5`. The second is a summary with a count of 7, a sum of `3.5`, and
quantiles `0.5 -> 0.2` and `0.99 -> 0.9`. `otel2influx` writes the same fields, in Go map order.

- **Histogram**, delta and cumulative alike: `count` always, then `sum`, `min`, and `max` when the
  record has them, then a field per finite bound holding the cumulative count up to that bound,
  then `+Inf` holding the total. Bounds are sorted ascending before the counts accumulate, because
  the model promises no bound order and `otlp_in` keeps the wire's. `count` and `+Inf` are the sum of every bucket, the overflow
  bucket included, so a histogram with no buckets still writes `count=0,+Inf=0`, and one whose
  last bound is finite still writes `+Inf`. Temporality is dropped, as `otel2influx` drops it and
  as a `Sum`'s already is (normalization 10 of `influxdb_in -> influxdb_out`).
- **Summary**: `count`, `sum`, then a field per quantile, named by the quantile.
- **Distribution and Samples**: the summary shape, with the sketch's count and sum and the `0.5`,
  `0.9`, and `0.99` quantiles that replace `p50`, `p90`, and `p99`, so a reader sees one quantile
  spelling. A `Samples` record is sketched first, each value weighted by its inverse sample rate.
  An empty sketch has no quantile reading, so it writes `count` and `sum` alone.
- **Keys**: a bound or quantile key is the `f64`'s shortest round-trip decimal with no exponent,
  `otel2influx`'s `'f'` spelling (`1000000`, `0.00001`). Rust's `f64` `Display` writes that form.
- **Values**: every value is an unsuffixed float, counts included, because `influx2otel` refuses
  an integer count and Telegraf writes a float. A count above 2^53 loses exactness as a float,
  which every upstream writer accepts too.
- **Non-finite components**: a non-finite `sum`, `min`, `max`, quantile, quantile value, or bound
  other than `+Inf` is left off the line, as both upstream serializers leave it off, and the rest
  of the line is written. The record counts once as
  `logit.output.metrics.degraded{reason="non_finite_component"}`, the existing per-record word for
  a record delivered in a lesser form ([ADR `telemetry-loss-vocabulary`](telemetry-loss-vocabulary.md)).
  Since `count` is written unconditionally, no multi-field kind leaves a line with no fields, and
  the `metrics.dropped{reason="no_fields"}` counter that covered that case is removed.
- **Exponential histogram**: still skipped and counted,
  `logit.output.metrics.skipped{metric_kind="exponential_histogram"}`, because no upstream schema
  has a form for one.
- **Carrier path**: unchanged. A record of one of these kinds on an event that carries
  `_logit.influxdb.measurement` still leaves on its own line, counted `carrier_mismatch`.

`influxdb_in` reads each field back as a `Gauge` named `<name>.<field>` (`<name>.count`,
`<name>.0.5`, `<name>.+Inf`), as it reads any point. `influxdb_in -> influxdb_out` is unaffected,
because `influxdb_in` makes only gauges.

## Alternatives considered
- **`telegraf-prometheus-v2`.** It's a supported, typed-looking shape, with `le` and `quantile`
  tags. It writes every kind under the measurement `prometheus`, so adopting it means replacing
  the per-record line for every kind, gauges and sums included, or writing two conventions side by
  side. A user who needs it can reopen this with a later ADR.
- **A `schema:` config option for v1 and v2.** Rejected for now: one shape that the default
  upstream writers agree on is enough until someone needs the other, and an option is a second
  encoder to test and document.
- **`otel-v1`.** Not selectable in Telegraf's input, has no summary, and writes non-cumulative
  buckets with no overflow count, so it loses data the record has.
- **Telegraf's parser's `'g'` key spelling (`1e+06`).** Rejected for `otel2influx`'s `'f'` form,
  since the OTLP path is the one `logit`'s histograms arrive by, and both readers parse the key
  as a float.
- **Keep the `bucket_`, `q`, and `p50` names.** Rejected: no reader knows them, and they drop
  `count`, `sum`, `min`, `max`, and the overflow bucket uncounted.
- **Write an exponential histogram as explicit buckets.** Rejected: materializing `base^i` bounds
  is the lossy conversion `MetricKind::ExponentialHistogram` exists to avoid, and no upstream
  reader would recognize the result as an exponential histogram anyway.

## Consequences
- A histogram's `count`, `sum`, `min`, `max`, and overflow bucket now reach InfluxDB, and a
  summary's `count` and `sum` do too, in the names an InfluxDB user's queries and dashboards
  for Collector or Telegraf data already use, and `influx2otel` reads them back as a histogram.
  Telegraf's `outputs.prometheus_client` doesn't: a metric read from line protocol arrives
  untyped, so it exposes each field as its own untyped sample.
- A series written under the old names doesn't continue under the new ones. `logit` is
  pre-release, so there's no compatibility path.
- `influx2otel` reads a `prometheus-v1` summary as a histogram, since the two shapes are the same
  on the wire. That's upstream's ambiguity, not one this ADR adds.
- Three upstream fields stay unwritten, each a known gap in
  [`docs/known-gaps/influxdb.md`](../known-gaps/influxdb.md): a start time
  (`otel2influx` writes `start_time_unix_nano=<n>i`), exemplars (`otel2influx` writes them as a
  `<name>_exemplar` measurement), and the `gauge` and `counter` field names `otel2influx` writes
  for a gauge and a sum where `influxdb_out` writes `value`.
- `spec/transit/mapping.yaml`'s `influxdb_out` cells for `histogram`, `summary`, `distribution`,
  `samples`, and `exponential_histogram` state this shape, and the codec's module doc
  (`crates/logit-proto/src/influxdb/mod.rs`) carries the details the cells don't.
