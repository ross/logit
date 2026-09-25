---
created: 2026-09-24
updated: 2026-09-24
---

# Enabling plan: OpenSearch — SS4O documents over `_bulk`

## Context

`logit` has no OpenSearch-specific component. It reaches OpenSearch today only through two
hops nobody has verified end to end: `otlp_out` into Data Prepper, or `otlp_out` into the
OpenTelemetry Collector's `opensearch` exporter. OpenSearch has no OTLP endpoint. Its one write
path is the `_bulk` NDJSON API, and the schema its Observability tooling reads is SS4O (Simple
Schema for Observability), which is OTel-shaped. This plan records what OpenSearch accepts and
emits, how `Event` maps onto SS4O, which way to send data and when, and the workstreams that
build a direct sink.

`Event` fits SS4O well. Nested attributes become JSON objects with no `flatten` stage, resource
and scope land in their own objects, and every OTLP metric kind has a document form. So a direct
`opensearch_out` is one hop and mostly a JSON rendering job.

Goals:

- Send logs, metrics, and traces directly to OpenSearch as SS4O documents over `_bulk`.
- Make leaving Data Prepper or the Collector a configuration change, not an application change.

Non-goals: an `opensearch_in` (§12); SigV4 signing for Amazon OpenSearch Service and
Serverless (§12); index-template management; the Trace Analytics service-map index.

Stream key **`opensearch`**: branches `opensearch/w0`…`opensearch/w7`, stacked as the
workstream table says. PR stack only: nothing is merged by this workstream; Ross directs merging.
The decisions are recorded in ADR `opensearch-ss4o-output` (W1); the design sections below are
the build-out of that record.

Settled with Ross (2026-09-24): `opensearch_out` only, no `opensearch_in`; full SS4O metrics,
every kind; basic auth and TLS in this stack, SigV4 deferred.

The finding that shapes the plan: **SS4O is not one schema in practice.** The catalog's
`.mapping` files, its `.schema` JSON schemas, its sample data, and the Collector exporter
disagree on field names and types (`status.code` `long` vs a string, `SPAN_KIND_SERVER` vs
`Server`, `value@double` vs `value.double`, `exemplars` vs `exemplar`, `eventName` vs
`event.name`). So W1 is a verify-first spike against a real OpenSearch and OpenSearch Dashboards
that fixes the dialect before any encoder is written.

## Coverage by signal

OpenSearch stores all three signals as documents in indexes; nothing distinguishes a metric
store from a log store except the index template. Each cell reads today → after this stack.

| Signal | Direct over `_bulk` | Through Data Prepper | Through the Collector's `opensearch` exporter | Stand-in (receive from OpenSearch clients) |
|---|---|---|---|---|
| Logs | none → `opensearch_out` into `ss4o_logs-*` (W3) | `otlp_out` to Data Prepper's OTel source (unverified) → unchanged | `otlp_out` to the Collector (unverified) → unchanged | none (§12) |
| Metrics | none → `opensearch_out` into `ss4o_metrics-*`, every kind (W4) | `otlp_out` to Data Prepper (unverified) → unchanged | `otlp_out` to the Collector (unverified) → unchanged | none (§12) |
| Traces | none → `opensearch_out` into `ss4o_traces-*` (W3), or `otel-v1-apm-span-*` if W1 requires it (W5) | `otlp_out` to Data Prepper (unverified) → unchanged, and still the path for the service map | `otlp_out` to the Collector (unverified) → unchanged | none (§12) |

## What OpenSearch accepts and emits

Surveyed 2026-09-24 from `docs.opensearch.org`, the
[`opensearch-catalog`](https://github.com/opensearch-project/opensearch-catalog) repository,
[Data Prepper](https://github.com/opensearch-project/data-prepper), the OpenTelemetry Collector
contrib [`opensearchexporter`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/opensearchexporter),
and AWS's OpenSearch Service guide. Items marked UNVERIFIED are settled by W1 against a
running OpenSearch and Dashboards, and this section is updated then.

### `_bulk`, the only write path

Every OpenSearch writer, Data Prepper and the Collector included, is a
[`_bulk` API](https://docs.opensearch.org/latest/api-reference/document-apis/bulk/) client:

| Aspect | Behavior |
|---|---|
| Request | `POST /_bulk`, NDJSON: an action line, then a source line, per document; the body ends in `\n` |
| Operations | `index` (create or replace) and `create` (409 if the `_id` exists); each action line may name its own `_index`, so one body can write many indexes |
| Response | `{"took":…,"errors":bool,"items":[…]}`, one `items[]` entry per action in request order, each with its own `status` and, on failure, an `error` object. A partial failure is HTTP 200 with `errors: true` |
| Ingest pipeline | `?pipeline=<name>` applies an ingest pipeline to every document in the request |
| Size | [`http.max_content_length`](https://docs.opensearch.org/latest/install-and-configure/configuring-opensearch/network-settings/) defaults to 100 MB, and is a hard limit on Amazon OpenSearch Service. 5–15 MB per request is the recommended range |
| Data streams | A [data stream](https://docs.opensearch.org/latest/im-plugin/data-streams/) requires an `@timestamp` field and accepts only `create` |
| Index names | Per [create index](https://docs.opensearch.org/latest/api-reference/index-apis/create-index/): lowercase, no `..`, no leading `.`, and a set of forbidden characters |

### SS4O index templates

[SS4O](https://docs.opensearch.org/latest/observing-your-data/ss4o/) is OpenSearch's
OTel-derived schema for observability data. Its definitions live in `opensearch-catalog` at
version 1.0.1, one directory per signal:
[logs](https://github.com/opensearch-project/opensearch-catalog/tree/main/schema/observability/logs/1.0.1/),
[metrics](https://github.com/opensearch-project/opensearch-catalog/tree/main/schema/observability/metrics/1.0.1/),
and [traces](https://github.com/opensearch-project/opensearch-catalog/tree/main/schema/observability/traces/1.0.1/).
Each holds three files: `*-1.0.1.mapping` (a plain index template), `*-datastream-1.0.1.mapping`
(the data-stream variant), and `*-1.0.1.schema` (a JSON Schema for a document). Indexes are named
`ss4o_{type}-{dataset}-{namespace}`, for example `ss4o_logs-nginx-prod`, and a document carries
the same three parts in `attributes.data_stream.{type,dataset,namespace}`.

The three files and the catalog's sample data don't agree with each other, or with the Collector
exporter:

| Field | Catalog mapping | Catalog schema | Collector exporter |
|---|---|---|---|
| Span `status.code` | `long` | — | a string |
| Span `kind` | — | `SPAN_KIND_*` enum | `Server` form |
| Metric value | `value@int`, `value@double` | `value.double` | — |
| Exemplars | `exemplar` | — | `exemplars` |
| Log event name | `event.name` | — | `eventName` |
| Traces `@timestamp` | absent from the plain 1.0.1 mapping | required | — |
| Metric `kind` | — | enum without `SUMMARY` | `SUMMARY` written |

A `—` is a form this survey didn't record. W1 (item 3 below) fills every cell.

### Third-party SS4O writers

The Collector's `opensearch` exporter
([`README.md`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/opensearchexporter/README.md),
[`config.go`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/opensearchexporter/config.go))
is the reference SS4O writer, at alpha/development stability:

- `mapping.mode`: `ss4o` (default), `ecs`, `flatten_attributes`, `bodymap`, and `otel-v1`;
- defaults: `bulk_action: create`, dataset `default`, namespace `namespace`;
- a log body is written with `AsString()`, so an object body becomes a JSON string, and
  `observedTimestamp` is `now()` at export rather than the record's own
  ([`sso_model.go`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/opensearchexporter/sso_model.go),
  [`encoder.go`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/opensearchexporter/encoder.go));
- metrics go through
  [`metrics_encoder.go`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/opensearchexporter/metrics_encoder.go)
  and
  [`metrics_model.go`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/opensearchexporter/metrics_model.go),
  one document per data point, `kind` as `GAUGE`, `SUM`, `HISTOGRAM`, `EXPONENTIAL_HISTOGRAM`, or
  `SUMMARY`;
- a failed `_bulk` item is retried when its status is 429, 500, 502, 503, or 504
  ([`log_bulk_indexer.go`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/opensearchexporter/log_bulk_indexer.go)).

Data Prepper writes its own layout, not SS4O. Its `OTelProtoOpensearchCodec.java` replaces `.`
in attribute keys with `@`, and nests them under `span.attributes.*` and
`resource.attributes.*`. Spans go to `otel-v1-apm-span-*`, the
[layout Trace Analytics reads](https://github.com/opensearch-project/data-prepper/blob/main/docs/schemas/trace-analytics/otel-v1-apm-span-index-template.md),
with `durationInNanos` and `traceGroupFields` filled from the trace's root span. Its
[`otel_metrics` processor](https://docs.opensearch.org/latest/data-prepper/pipelines/configuration/processors/otel-metrics/)
writes `isMonotonic` where SS4O writes `monotonic`.
[Trace Analytics](https://docs.opensearch.org/latest/observing-your-data/trace/ta-dashboards/)
says a custom span index must follow Data Prepper's mappings.

### What OpenSearch sends out

Nothing is pushed. OpenSearch has no outbound stream, subscription, or forwarding API; reading
data back is `_search`, paged with a point in time (PIT) and `search_after`. For metrics,
OpenSearch steers users the other way: Dashboards
[queries Prometheus through the PPL connector](https://docs.opensearch.org/latest/observing-your-data/prometheusmetrics/)
rather than storing metrics itself.

### Unverified, to be settled by W1

1. Whether `_bulk` accepts a gzip request body (`Content-Encoding: gzip`), and whether
   `http.max_content_length` counts compressed or decompressed bytes.
2. Whether `_bulk` accepts `Content-Type: application/json` as well as
   `application/x-ndjson`.
3. The dialect: `status.code` as an integer or a string; the `kind` form (`SPAN_KIND_SERVER` or
   `Server`); `value@double` or `value.double`; `exemplars` or `exemplar`; `eventName` or
   `event.name`; `traceState` as a string. And whether the exporter's documents index against
   the catalog templates at all.
4. Whether Trace Analytics reads `ss4o_traces-*`.
5. Whether Log Explorer and Discover find `ss4o_logs-*` without the SS4O integration installed.
6. Whether Dashboards shows `ss4o_metrics-*` in any view.
7. `create` with an `_id` on a data stream; `index` without an `_id` on a data stream.
8. Whether a 9-digit RFC 3339 fraction parses into a `date` field.
9. The shape of a 429: per item inside a 200, top-level, or both.
10. The time field the traces index pattern uses, given the plain 1.0.1 traces mapping has no
    `@timestamp` and the schema requires one.

## OpenSearch's data against `Event`

SS4O is OTel-shaped, so most rows are field renames. What degrades is a structured log body
(SS4O's `body` is `text`), the index-side precision of timestamps and histogram numbers (fixed
by the templates, exact in `_source`), and the `logit` metric kinds OTLP doesn't have. Rows
marked UNVERIFIED are settled by W1 (items 3, 8, and 10 above).

| SS4O concept | `Event` representation | Verdict |
|---|---|---|
| Log `@timestamp` (`date`, ms) | `Event::timestamp` as RFC 3339, 9 fraction digits | lossless in `_source`; the index truncates to ms (permitted); 9-digit parse UNVERIFIED |
| Log `observedTimestamp` | `LogRecord.observed_timestamp`; omitted when 0 | lossless (the exporter writes `now()` instead, a divergence W6's fixtures allow) |
| `body` (`text`, required) | `Str` as-is; `Map`/`Array` as a compact JSON string (the exporter's `AsString()`); `Bytes` as lossy UTF-8, counted | degraded for structured bodies: an object in a `text` field is rejected |
| `severity.text`, `severity.number` | `Severity` → `TRACE`…`FATAL`, 1/5/9/13/17/21 | lossless |
| `event.name` (mapping) vs `eventName` (exporter) | `LogRecord.event_name` | field name UNVERIFIED |
| `traceId`, `spanId` (lowercase hex) | `LogRecord.trace` | lossless; `TraceRef.flags` dropped, counted |
| `resource` (dynamic object) | batch `Resource.attributes` as a JSON object, typed | lossless (the exporter stringifies values; `logit` keeps types) |
| `attributes`, with `attributes.data_stream.{type,dataset,namespace}` | `Event.attributes`; `data_stream` filled from index resolution (§4) | lossless modulo key collisions (§9) |
| `instrumentationScope.{name,version,schemaUrl,attributes,droppedAttributesCount}`, `schemaUrl` | `EventBatch.scope`, `Resource.schema_url` | lossless |
| Span `traceId`, `spanId`, `parentSpanId` (`""` for a root), `name`, `traceState` | `SpanRecord`, `SpanExt.trace_state` | lossless |
| Span `kind` | `SpanKind` → `SPAN_KIND_*` (the schema's enum, and Data Prepper's form) | the exporter writes `Server`; form UNVERIFIED |
| `startTime`, `endTime` (`date_nanos`), `@timestamp` (= start) | `Event::timestamp`, `end_timestamp` | lossless; the traces time field UNVERIFIED |
| `durationInNanos` | derived from start and end | `otel-v1` mode only (§8) |
| `status.code` (`long`), `status.message` | `SpanStatus` → 0/1/2; `SpanExt.status_message` | the exporter writes strings into a `long`; integer proposed, UNVERIFIED |
| dropped attribute, event, and link counts | `SpanExt` | lossless |
| `events[]` (nested: `name`, `@timestamp`, `attributes`, `droppedAttributesCount`) | `SpanEvent` | lossless |
| `links[]` (nested: `traceId`, `spanId`, `traceState`, `attributes`, `droppedAttributesCount`) | `SpanLink`; link `flags` dropped, counted | lossless modulo flags |
| `serviceName` (traces, metrics) | a copy of resource `service.name` | lossless |
| Metric `name`, `description`, `unit` | `MetricRecord` | lossless |
| Metric `startTime`, `@timestamp` (`date`) | `start_timestamp` (omitted when 0), `Event::timestamp` | ms in the index, ns in `_source` |
| `kind` | `GAUGE`/`SUM`/`HISTOGRAM`/`EXPONENTIAL_HISTOGRAM`/`SUMMARY` (the exporter's strings; the schema's enum lacks `SUMMARY`) | lossless |
| `aggregationTemporality`, `monotonic` | `AGGREGATION_TEMPORALITY_{DELTA,CUMULATIVE}`, `Sum.monotonic` (Data Prepper: `isMonotonic`) | lossless |
| `value@int` (32-bit), `value@double` | always `value@double`; ±Inf capped to ±`f64::MAX`; NaN drops the document, counted | lossless for finite values; the schema says `value.double`, UNVERIFIED |
| `count`, `sum`, `min`, `max` (`float`) | histogram, exponential-histogram, and summary fields | float32 in the index, exact in `_source` |
| `bucketCountsList`, `explicitBoundsList`, `bucketCount`, `explicitBoundsCount`, `buckets[]{min,max,count}` | `Histogram.buckets` → OTLP bounds and counts, buckets materialized, bounds clamped to ±`f32::MAX` | lossless in `_source` |
| `scale`, `zeroCount`, `positiveOffset`, `negativeOffset`, `positiveBuckets[]`, `negativeBuckets[]` | `ExpHistogram`, buckets materialized | `zero_threshold` dropped, counted |
| `quantiles[]{quantile,value}`, `quantileValuesCount` | `Summary.quantiles` | lossless |
| `exemplars[]{time,value,traceId,spanId,attributes}` (the mapping says `exemplar`) | `MetricRecord.exemplars` | lossless; field name UNVERIFIED |
| `Samples`, `Distribution` | `multi_value: summarize` → a `SUMMARY` document: count, sum, min, max, quantiles 0.5/0.9/0.99 | degraded, counted `degraded{metric_kind}` |
| `Set`, `SetMembers` | `summarize` → a `GAUGE` of the HyperLogLog estimate or the member count | degraded, counted |
| the four kinds above under `multi_value: skip` (default) | dropped | counted `skipped{metric_kind}` |
| a `GaugeDelta` | rejected, as every sink does | not an OpenSearch concept |
| `MetricRecord.flags`, batch provenance | no field | dropped (permitted normalization) |

## Direct, or through Data Prepper or the Collector: trade-offs and best practice

**Direct over `_bulk`.** For: one hop; no JVM to run; per-document accounting, because each
`items[i]` answers one document `logit` wrote. Against: no `traceGroup` fill-in across a trace,
because a stateless relay sees each span alone; no service map, which Data Prepper builds from
state across spans.

**Through Data Prepper.** For: it fills Trace Analytics' `traceGroup` fields and builds the
service map, the two things Trace Analytics needs that a stateless writer can't produce.
Against: a JVM hop per deployment; it writes the `otel-v1` layout, not SS4O, so logs and metrics
land in a schema Observability's SS4O views don't read.

**Through the Collector.** For: the reference SS4O writer, so its output is what SS4O tooling is
tested against. Against: a second hop to reach the same `_bulk` endpoint; alpha/development
stability; the dialect disagreements in the SS4O section above.

**Best practice.** Send logs and metrics directly with `opensearch_out`. Send traces directly
only if W1 shows Trace Analytics reads what `logit` writes (`ss4o_traces-*`, or `otel-v1` under
§8); otherwise send traces with `otlp_out` to Data Prepper. Don't send one signal both ways.

## Design

Each item is a decision the ADR records, and names the workstream that builds it.

### 1. One-way sink, no lossless pair (W1)

`opensearch_out` has no decoder beside it, so it isn't a relay pair and needs no amendment to
[ADR `lossless-transit`](../adr/lossless-transit.md). The codec lives in
`crates/logit-proto/src/opensearch/`, one module whose doc is the mapping table above. W1
writes ADR `opensearch-ss4o-output`.

### 2. Encoder shape (W2)

A [`FramedEncoder`](../adr/framed-encoder.md) with `Meta = { signal, event_idx }`, one message
per document (action line plus source line). Three reasons:

- the sink splits a batch into bodies at document boundaries under `max_body_bytes`;
- `_bulk`'s `items[i]` maps to message *i*, which is what §6's retry subset needs;
- per-message drop accounting is the trait's contract.

**One body for all signals**, with `_index` on every action line: `_bulk` accepts mixed indexes,
and `SignalEncoder`'s per-signal split exists only because OTLP has three RPCs. A multi-payload
event becomes up to three kinds of document, in log, metrics, span order, with one document per
`MetricRecord`.

### 3. The JSON writer is new work (W2)

Nothing in the repo serializes `Value` or `AttrMap` to JSON. The writer is hand-written into a
`Vec<u8>`, escaping strings through `serde_json::to_writer` on `&str`, with a non-allocating
`write_rfc3339_utc` beside `logit_core::time::format_rfc3339_utc`. `Value` rules:

| `Value` | JSON |
|---|---|
| `Timestamp` | RFC 3339 string, 9 fraction digits |
| `Bytes` in attributes | base64, OTLP/JSON's rule |
| `Bytes` in a log body | lossy UTF-8, counted |
| non-finite `F64` in attributes | dropped, counted |
| `U64` | a JSON number |

The allocation count is pinned in `crates/logit-bench/tests/allocations.rs` and
[`docs/design/memory.md`](../design/memory.md)'s table in the same commit.

### 4. Index naming (W2)

`index:` takes one `logit_core::template` string per signal, defaulting to
`ss4o_logs-{dataset}-{namespace}`, `ss4o_metrics-{dataset}-{namespace}`, and
`ss4o_traces-{dataset}-{namespace}`:

- `{dataset}` and `{namespace}` come from resource attributes `data_stream.dataset` and
  `data_stream.namespace`, set upstream with `set` per
  [`operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md), and
  fall back to `default` and `namespace`, the exporter's defaults;
- `{date}` is the UTC `yyyy.MM.dd` of `Event::timestamp`;
- `{resource.<k>}` and `{attr.<k>}` take a `fallback:`.

A resolved name that is empty, starts with `.`, or contains `..` takes the fallback, the
exporter's safety rule. Forbidden characters become `_` and the name is lowercased, counted.
Graph rule 63 validates the templates (62 is the highest rule today).

### 5. Action and document ID (W3)

`bulk_action: create | index`, default `create`: the exporter's default, and the only operation a
data stream accepts. `document_id: none | content_hash`, default `none`. Under `content_hash`,
`_id` is XXH3-128 over the index name and source bytes, base64url-encoded; that needs
`twox-hash`'s `xxhash3_128` feature (only `xxhash64` is enabled today). A 409 on `create` then
counts as delivered, and `duplicate_safe()` is true.

The ADR records the trade-off: two byte-identical documents collapse into one. Amazon OpenSearch
Serverless accepts a client `_id` only on search collections and rejects one on time-series
collections ([Serverless general reference](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-genref.html)),
so `content_hash` isn't available there.

### 6. Reading the `_bulk` response (W3)

`send` stays one attempt, and `write_loop` owns retry, per
[ADR `service-lifecycle-and-output-retry`](../adr/service-lifecycle-and-output-retry.md).

| Response | Outcome |
|---|---|
| top-level 401, 403, 413 | `Fault::Permanent` |
| top-level 429, 5xx | `Fault::Ambiguous` |
| connection error | `classify_reqwest_error` |
| 200, `errors: false` | delivered |
| 200, `errors: true` | walk `items[]`: 400 drops that document, counted `rejected{reason}`; 409 under `content_hash` is delivered; 429 and 5xx are retryable |

If any item is retryable, the sink remembers those message indexes, keyed by the `BatchContext`
from `Output::observe_batch`, and returns `Ambiguous`; `write_loop`'s retry of the same batch
sends only that subset. The sink never resends a document OpenSearch indexed, and never loops
on its own.

### 7. Wire details (W3)

- `Content-Type: application/x-ndjson`.
- `compression: none | gzip`, the default decided by W1 (item 1).
- `max_body_bytes`, default 5 MiB, the low end of the recommended range; a single document over
  it is dropped and counted. No chunked transfer encoding.
- `pipeline:` → `?pipeline=`.
- `username:` with `password: !env OPENSEARCH_PASSWORD`, `tls:` (`TlsClientConfig`), and
  `headers:` through `otlp_out`'s `with_headers`.

### 8. `otel-v1` traces mode (W5, conditional)

Trace Analytics documents that a custom span index must follow Data Prepper's mappings. If W1
shows it can't read `ss4o_traces-*`, W5 adds `schema: ss4o | otel-v1` for traces only:
`durationInNanos`, `traceGroupFields`, and `span.attributes.*`/`resource.attributes.*` keys with
`.` replaced by `@`. A stateless relay fills `traceGroup` only on root spans, and the ADR records
that. `otel-v1` logs and metrics, and the service map, are out (§12).

### 9. Key collisions (W2)

OpenSearch expands a dotted key into an object path, so a scalar `a` beside `a.b` in one map is a
`mapper_parsing_exception` that rejects the whole document. Keys are written verbatim, the
exporter's behavior; within one map, the encoder detects a prefix collision and rewrites only the
colliding keys with Data Prepper's `@` substitution, counted. A type conflict across documents
(a field that is a string in one and an object in the next) can't be fixed at the sink, and gets
a row in [`docs/known-gaps.md`](../known-gaps.md).

### 10. Reuse (W2, W3, W4)

- `crates/logit-outputs/src/http.rs`: `build_client`, `is_retryable_http_status`,
  `classify_reqwest_error`, `read_body_prefix`.
- `otlp_out`'s gzip and `with_headers`.
- `logit_proto::graphite::MultiValue` as the precedent for `multi_value: skip | summarize`, per
  [ADR `graphite-carbon-relay`](../adr/graphite-carbon-relay.md). The variant is `summarize`, not
  `expand`, because the result is one document of another kind, not several series.
- `logit_core::template` for §4.
- `write_loop`'s bounded retry through `Fault`.

### 11. Ordering (all)

W1 comes before any code, because it fixes the dialect every encoder row depends on.

### 12. Not in this stack

- **`opensearch_in`.** OpenSearch has no push or subscribe read path. A `_bulk` receiver
  standing in for OpenSearch in front of Fluent Bit, Logstash, Vector, or Beats is a different
  component from a relay pair, and a possible later stream.
- **SigV4** for `es` and `aoss` ([Serverless clients](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-clients.html)):
  it needs a signing dependency and credential sourcing of its own.
- **Index-template management**: the operator installs the catalog templates.
- **`otel-v1` logs and metrics, and the service map.**
- **A `demo/` leg**: OpenSearch and Dashboards cost gigabytes of heap; W1 and W6's compose
  covers verification.
- **A Prometheus-federation metrics path**: that is `prometheus_out` and the PPL connector,
  and needs nothing new.

## Workstreams

| # | PR | Size | Depends on |
|---|---|---|---|
| W0 | This plan and its index row | S | — |
| W1 | Spike: compose with `opensearchproject/opensearch` and `opensearchproject/opensearch-dashboards` 3.x, plus a 2.19 check; install the catalog templates, plain and data-stream; `curl` a hand-written `_bulk` body per UNVERIFIED item; run the Collector exporter into it; check Log Explorer, Trace Analytics, and the metrics views against `ss4o_*` and `otel-v1-apm-span-*`. Deliverables: ADR `opensearch-ss4o-output` and its index row, the dialect decided, this plan updated | M | W0 |
| W2 | `logit_proto::opensearch`: JSON writer, `write_rfc3339_utc`, log and span documents, action lines, index templates, key-collision rewrite, `allocations.rs` pins | M | W1 |
| W3 | `opensearch_out`: `ComponentKind` variant, registry arm in `crates/logit-cli/src/pipeline.rs`, graph rules 63+ (endpoint, templates, credential pairing), schema regenerated, body packing, gzip, auth, TLS, `pipeline:`, the `_bulk` response parser with the retry subset, `document_id`, `duplicate_safe`; `examples/opensearch.yaml`, with `OPENSEARCH_URL` and `OPENSEARCH_PASSWORD` defaulted in `script/validate` and in `every_shipped_config_loads_and_validates`'s `!env` map | M | W2 |
| W4 | Metrics documents: every kind, `multi_value`, exemplars, float32 clamps, the NaN and Inf policy | M | W3 |
| W5 | `schema: otel-v1` for traces, or a §12 entry if W1 shows it isn't needed | S | W4 |
| W6 | Golden fixtures and a live end-to-end run (see Verification) | M | W5 |
| W7 | `docs/opensearch.md`; `deploying.md`; `known-gaps.md` rows (structured bodies, float32 fields, key collisions, `Samples`/`Set` summaries, dropped `zero_threshold` and flags, no `opensearch_in`, no SigV4); `AGENTS.md` outputs table; [`telemetry-landscape.md`](../design/telemetry-landscape.md) cells | S | W6 |

Landing order: W0 → W1 → … → W7, linear. Each PR is based on and targets its parent's branch and
is brought up to date with `git merge origin/main`, never a rebase.

**Status (2026-09-24):** W0 open.

## Verification

- Per PR: `script/cibuild` green; `script/schema` regenerated and committed by any workstream
  that changes a config type; `script/validate` for any that adds a config.
- W1: every item in "Unverified, to be settled by W1" has a recorded answer, and this plan's
  table and survey are rewritten to match.
- W2 and W4 unit tests: fixtures covering every mapping-table row, including a multi-payload
  event producing three documents, a `Map` body stringified, a key collision rewritten, a NaN
  dropped, and every `MetricKind`. Each document is parsed back with `serde_json` and asserted
  field by field; a body always ends in `\n`.
- W3: a mock HTTP server returns `errors: true` with mixed 201, 400, 429, and 409 items. Assert
  the counters, that the retry carries only the 429 subset, that 409 is success under
  `content_hash`, and that a top-level 413 or 401 is `Permanent`.
- W6 golden fixtures, the `script/record-fixtures` pattern: a producer sends
  `testdata/interop/otlp/*.json` through the Collector's `opensearch` exporter into
  `tools/record-fixtures/raw_capture.py --proto http`, which needs a new reply mode returning a
  `_bulk` success body (today it answers 204). Captures are stored as
  `testdata/interop/opensearch/*.ndjson` with `.headers` sidecars, the Prometheus pattern. A test
  runs the same OTLP through `logit`'s decoder and the `opensearch` encoder and diffs the
  documents as JSON, allowing only the ADR's named divergences.
- W6 live: the W1 compose runs `logit` → `opensearch_out` with the templates installed. Assert
  `_count` per index; every item succeeds except one malformed test document; a data stream
  receives `create`; Dashboards shows logs and traces.
- W0 (this PR) is documentation only: every relative link resolves and `docs/plans/README.md`
  gained a row.
