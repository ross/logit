---
created: 2026-09-24
updated: 2026-09-24
---

# Enabling plan: New Relic — direct APIs and OTLP, shipper-intake stand-in, APM collector stand-in

## Context

`logit` has no New Relic-specific component. Two existing sinks already reach New Relic
unverified: `otlp_out` with `headers: {api-key: …}` against `otlp.nr-data.net`, and
`prometheus_out`'s remote-write sender with an `Authorization: Bearer` header against
`metric-api.newrelic.com/prometheus/v1/write`. This plan records what New Relic accepts and
emits, where the event model and the existing components fall short of it, which way to send data
and when, and the workstreams that close the gaps.

Goals:

- Send logs, metrics, traces, and custom events to New Relic directly, with a best-practice
  recommendation per signal.
- Receive what New Relic-instrumented systems produce, losslessly, in the two places `logit` can
  stand: as the intake that redirected log shippers, Telemetry SDK senders, and the
  Infrastructure agent post to (`newrelic_in`), and as the collector that a fleet of APM agents
  reports to (`newrelic_apm_in`), with a send-back path (`newrelic_apm_out`) so APM data bound
  for New Relic stays APM data there.
- Make migrations to and from New Relic a configuration change, not an application change.

Non-goals: the Trace API's `zipkin` format (OTLP carries strictly more); Infinite Tracing's gRPC
stream (bidirectional streaming, gated on an agent run token; an operator turns it off and the
same spans arrive as `span_event_data`); browser and mobile beacons; the PHP agent's daemon
socket; the Infrastructure agent's inventory and command channel (acknowledged, counted, not
forwarded); a Metric API `summary` as a percentile carrier.

Stream key **`nr`**: branches `nr/w0`…`nr/w9`, stacked as the workstream table says. PR stack
only: nothing is merged by this workstream; Ross directs merging. The decisions are recorded in
an ADR (W1), written once the spike against a real account settles the items marked UNVERIFIED
below; the design sections are the build-out of that record.

Settled with Ross (2026-09-24): the APM collector pair is planned in full, receive and send,
because the point of planning it now is to find any core `logit` blocker while fixing one impacts
nobody, whether or not every workstream is built; the Infrastructure agent is covered for its
events route and identity handshake only; a New Relic account is available and verification
happens first (W1), not last, because every surface here is HTTPS and JSON and a curl settles
most questions in minutes.

## Coverage by signal

New Relic ingests four kinds of data. Each cell reads today → after this stack.

| Signal | Direct, no agent | Intake stand-in (receive from shippers and the Infrastructure agent) | Collector stand-in (receive from APM agents) |
|---|---|---|---|
| Logs | `otlp_out` (unverified) → verified, plus `newrelic_out` Log API (W5) | none → `newrelic_in` `/log/v1` (W4) | none → `newrelic_apm_in` `log_event_data` (W7a) |
| Metrics | `otlp_out` (unverified; `Distribution`/`Samples` arrive as a quantile-less summary), `prometheus_out` remote write (unverified) → both verified, `otlp_out` sends distributions as histograms (W3), plus `newrelic_out` Metric API `count`/`gauge`/`summary` (W5) | none → `newrelic_in` `/metric/v1` and `/metric/v1/infra` (W4); remote write already lands on `prometheus_in` | none → `newrelic_apm_in` `metric_data` timeslices (W7a) |
| Traces | `otlp_out` (unverified) → verified, plus `newrelic_out` Trace API in the `newrelic` format (W5) | none → `newrelic_in` `/trace/v1` (W4) | none → `newrelic_apm_in` `span_event_data` (W7a); to New Relic through `newrelic_apm_out` (W7b) |
| Custom events | none → `newrelic_out` Event API (W5) | none → `newrelic_in` `/v1/accounts/{id}/events` and `/infra/v2/metrics/events/bulk` (W4) | none → `newrelic_apm_in` `analytic_event_data`, `custom_event_data`, `error_event_data` (W7a) |

Two positions Datadog has and New Relic doesn't: there is no local agent that applications send
traces to (every APM agent is in-process and posts to the collector itself), and no data leaves
New Relic except through streaming export to Kinesis Firehose, Azure Event Hubs, or Pub/Sub, so
there is nothing to stand in for behind New Relic. The local surfaces that do exist are ones
`logit` already speaks: `nri-statsd` is statsd over UDP with DogStatsD-style `|#k:v` tags
(`statsd_in`), the New Relic distribution of the OpenTelemetry Collector (NRDOT) is a stock
Collector (`otlp_in`), and the Infrastructure agent's opt-in log listeners take syslog over
TCP, UDP, or a Unix socket (`syslog_in`).

## What New Relic accepts and emits

Surveyed 2026-09-24 from `docs.newrelic.com`, the Telemetry SDK specification
(`github.com/newrelic/newrelic-telemetry-sdk-specs`), the open-source agents (`go-agent`,
`newrelic-python-agent`, `newrelic-ruby-agent`, `node-newrelic`, `newrelic-dotnet-agent`,
`newrelic-php-agent`, `infrastructure-agent`), and `nrdot-collector-releases`. Items marked
UNVERIFIED were not confirmed by a current official page or by source; W1 verifies each against
the account or a real agent and this section is updated then.

### Facts common to every public API

- One license key ("Ingest – License", 40 hex chars) authenticates every ingest API. The
  Insights insert key is legacy. A user key is for NerdGraph only.
- 1 MB per POST, on every API including OTLP. The Go Telemetry SDK measures the compressed body
  (`telemetry/request.go`, `maxCompressedSizeBytes = 1e6`); whether the server measures
  compressed or decompressed bytes is UNVERIFIED.
- Account-wide: attribute names up to 255 chars; values up to 4,096 chars (4,094 on the Log
  API); at most 254 attributes per record; 250 custom event types per account per day.
- Validation is asynchronous. A 2xx means accepted, not stored; content errors surface later as
  `NrIntegrationError` events in the account, keyed by `requestId`. A sink can't see them.
- The SDK specification's client contract, which `newrelic_out` follows: gzip by default; key in
  a header, never the query string; an `x-request-id` (UUIDv4) constant across retries of one
  payload; drop on 400, 401, 403, 404, 405, 409, 410, 411; retry 408; split and retry on 413;
  honor `Retry-After` on 429; backoff `[0, 1, 2, 4, 8, 16, 16, …]` s otherwise; never combine
  telemetry types in one payload.
- Regions: US (`*.newrelic.com`, `otlp.nr-data.net`), EU (`*.eu.newrelic.com`,
  `insights-collector.eu01.nr-data.net`, `otlp.eu01.nr-data.net`), JP (`*.jp.nr-data.net`),
  FedRAMP (`gov-*.newrelic.com`, `gov-otlp.nr-data.net`). Only the Event API carries the
  account id in its path; every other API infers it from the key.

### Public intake APIs (no agent)

| Endpoint | Body and limits | Notes |
|---|---|---|
| `POST metric-api.<region>/metric/v1` | JSON `[{common?: {timestamp, interval.ms, attributes}, metrics: [{name ≤255, type: gauge \| count \| summary, value, timestamp, interval.ms, attributes}]}]`; `summary` value `{count, sum, min, max}`; header `Api-Key`; `Content-Encoding: gzip`; 150 attributes per metric | `count` is a delta over `interval.ms`; `interval.ms` is required for `count` and `summary`. No histogram, distribution, set, or cumulative type on this API: `distribution` and `cumulativeCount` exist in the store but are created only by OTLP, remote write, and events-to-metrics. Points more than 48 h in the past or 24 h in the future are dropped. NaN and ±Inf dropped. Reserved keys: `interval.ms`, `timestamp`, `value`, `common`, `min`, `max`, `count`, `sum`, `metrics`; `entity.guid`/`entity.name`/`entity.type` undefined behavior. 202 `{"requestId"}`; 400, 403, 413, 429 |
| `POST insights-collector.<region>/v1/accounts/<ACCOUNT_ID>/events` | JSON array of flat objects with a required `eventType` (alphanumerics, `_`, `:`; ≤255 chars); values are strings or numbers only, no maps or arrays; 254 attributes; `gzip` or `deflate` | `timestamp` in s or ms within ±24 h of server time, else ingest time. `accountId` dropped, `appId` must be an integer, `eventType` can't be `Metric`, `Entity`, `EntityRelationship`, or start with `Public_`. Whether the Go SDK's account-less `/v1/accounts/events` path is accepted is UNVERIFIED. 200 `{"success": true, "uuid"}` even when individual events are rejected (they become `NrIntegrationError` with `category='EventApiException'`); 400, 403, 408, 413, 415, 429, 503 |
| `POST log-api.<region>/log/v1` | Simplified: one JSON object `{timestamp, message, logtype, …}`. Detailed: `[{common?: {timestamp, attributes}, logs: [{timestamp, message, attributes}]}]`; `Content-Type: application/json` (or `application/gzip`) plus `Content-Encoding: gzip`; header `Api-Key` or query `?Api-Key=` | `log`, `LOG`, `MESSAGE`, `msg` are rewritten to `message`. A `message` that is valid JSON is parsed on ingest and its keys become attributes, nested objects dot-flattened. `timestamp` integer auto-detected as s or ms, string as ISO 8601. A value's first 4,094 chars are indexed, the next 128,000 bytes land in a `newrelic.ext.<name>` blob, the rest is dropped. Arrays stored as strings. Older than 48 h may be dropped; the future bound is UNVERIFIED. `trace.id`/`span.id` correlate. Rate: 300,000 requests and 10 GB per minute, then 429 with `Retry-After`. `X-License-Key` as a header here is UNVERIFIED (the Go SDK sends it) |
| `POST trace-api.<region>/trace/v1` | `Data-Format: newrelic`, `Data-Format-Version: 1`: `[{common?: {attributes}, spans: [{id, trace.id, timestamp ms, attributes: {duration.ms, name, parent.id, service.name, …}}]}]`; or `Data-Format: zipkin`, `Data-Format-Version: 2`: a Zipkin v2 JSON span array | No `kind`, `status`, events, or links fields: any `error.*` attribute marks the span as an error; `http.*` makes it external, `db.*` a datastore span; service boundaries are inferred from `service.name`. `entity.name` is derived from `service.name`; `entity.guid`/`guid`/`entityGuid` are omitted. Zipkin annotations are not stored. 202; 400, 403, 413, 429. Spans per request, attributes per span, and the timestamp window are UNVERIFIED (the docs defer to the account's Limits UI) |
| `POST otlp.<region>` `/v1/{traces,metrics,logs}` (443, 4317, 4318; gRPC and HTTP both on 443) | OTLP/HTTP protobuf ("the default for data sent over the public internet"; OTLP/JSON is undocumented, UNVERIFIED); header `api-key`; `gzip` or `zstd` ("recommended"); TLS 1.2; 1 MB; proto v1.4.0 | Success returns an **empty body**, not an `Export*ServiceResponse`, so there is no `partial_success`; failures carry no `Status.message`. Missing key 401/`UNAUTHENTICATED`, invalid 403/`PERMISSION_DENIED`, over rate 429/`RESOURCE_EXHAUSTED`; 500, 502, 503, 530 and `UNAVAILABLE` are transient. Attribute caps: 64 resource, 8 scope, 128 record, semconv attributes kept first when pruning; strings truncated at 4,095; arrays ≤64 and homogeneous; **maps pruned, on log attributes too**. HTTP/2 negotiation on OTLP/HTTP is UNVERIFIED |
| `POST metric-api.<region>/prometheus/v1/write?prometheus_server=<NAME>` | Remote write 1.0 with `Authorization: Bearer <license key>` (or `?X-License-Key=`, "not recommended"); 2.0 is undocumented, UNVERIFIED | Type from the name suffix: `_bucket`, `_count`, `_total` → `count`, `_sum` → `summary`, else `gauge`; override with a `newrelic_metric_type` label (`counter`, `gauge`, `summary`), stripped on ingest. Counters are stored as `cumulativeCount`, histograms as `distribution`; buckets become metric `<base>_bucket` with attribute `histogram.bucket.le`. Adds `prometheus_server`, `newrelic.source=prometheusAPI`, `instrumentation.*`. 400, 413, 429. Native histograms UNVERIFIED |

OTLP's model mapping, which decides what `otlp_out` should send:

| OTLP | New Relic |
|---|---|
| Gauge | `gauge` |
| Sum, monotonic, delta | `count` |
| Sum, monotonic, cumulative | `cumulativeCount`: the server keeps a delta process per series, treats a decrease as a reset, reorders within a bounded buffer, and **drops state after 5 min without data**, so a reporting interval must stay under that; the fate of the first point is UNVERIFIED |
| Sum, non-monotonic, cumulative | `gauge` |
| Sum, non-monotonic, delta | not supported |
| Histogram, ExponentialHistogram | `distribution` (an internal base-2 exponential histogram); a ±∞ bucket becomes zero-width |
| Summary | `summary` with the quantiles dropped; cumulative summaries "can fail ingest" |
| Exemplars | dropped |
| Span `kind`, `status.code`, `status.message`, `trace_state` | `span.kind`, `otel.status_code`, `otel.status_description`, `w3c.tracestate` attributes; `duration.ms` computed; each span event becomes its own `SpanEvent` record; links "supported", storage UNVERIFIED |
| Log `body`, `severity_*`, `trace_id`/`span_id`, `flags` | `message` (also parsed as JSON), `severity.text`/`severity.number`, `trace.id`/`span.id`, `w3c.flags`; `observed_timestamp` and `event_name` handling UNVERIFIED |

Recommended settings, from New Relic's own best-practice pages: delta temporality for counters
and histograms, cumulative for up-down counters and gauges ("generally a delta metrics system"),
exponential histograms.

Entity synthesis (`github.com/newrelic/entity-definitions`): any record with `service.name` and
no `newrelic.entity.type` synthesizes a service entity, on every API (end-to-end on the Metric
and Log APIs is UNVERIFIED). A host entity is synthesized only from OTLP `system.*`/`process.*`
metrics or OTLP logs carrying `host.id`, never from the JSON APIs. `entity.guid` overrides
synthesis and attaches data to an existing entity, including an APM one; every ingest page
otherwise lists `entity.guid`, `entity.name`, and `entity.type` as undefined behavior.

### Agent outbound surfaces (what New Relic's agents send)

**APM agents** (Java, .NET, Node, Python, Ruby, Go, and the PHP daemon) speak one protocol,
version 17, to `collector.newrelic.com` (region derived from the license key prefix, as
`go-agent`'s `preconnectHost` does). There is no public specification: the agents' source and
`newrelic-dotnet-agent`'s `MockNewRelic` test controller are the spec, and `go-agent`'s comments
cite an internal `agent-specs` repository.

- Transport: `POST https://<host>/agent_listener/invoke_raw_method?method=<M>&protocol_version=17&marshal_format=json&license_key=<KEY>[&run_id=<ID>]`.
  Query-parameter order differs per agent (Java and .NET put `method` first; Go and PHP sort
  alphabetically), so a receiver parses the query string. HTTPS is hard-coded in every agent;
  a private CA goes in `ca_bundle_path` (Java, Python, Ruby), `certificates` (Node), or
  `newrelic.daemon.ssl_ca_bundle` (PHP). `Content-Encoding` is `gzip` (Go always; Python and
  Node above 64 KiB), `deflate` (PHP; Ruby above 2 KiB), or `identity`; `Content-Type` is
  `application/octet-stream` or `application/json`. Every agent adds whatever
  `request_headers_map` the connect reply gave it.
- Redirect: `host`/`NEW_RELIC_HOST` and `port` (Java, .NET, Python, Node, Ruby, Go);
  `newrelic.daemon.collector_host` (PHP). Proxies (`proxy_host` and friends) are separate.
- Handshake: `preconnect` (body `[{"security_policies_token", "high_security"}]`) returns
  `{"return_value": {"redirect_host", "security_policies"}}`, and every later call goes to
  `redirect_host`. `connect` (body: one settings object with `pid`, `language`,
  `agent_version`, `host`, `display_host`, `settings`, `app_name[]`, `high_security`, `labels`,
  `environment`, `identifier`, `utilization`, `metadata`, `event_harvest_config`) returns
  `{"return_value": {agent_run_id, entity_guid, request_headers_map, max_payload_size_in_bytes,
  event_harvest_config: {report_period_ms, harvest_limits: {analytic_event_data,
  custom_event_data, log_event_data, error_event_data, span_event_data}},
  span_event_harvest_config, sampling_target, sampling_target_period_in_seconds, apdex_t,
  web_transactions_apdex, collect_analytics_events, collect_custom_events, collect_traces,
  collect_errors, collect_error_events, collect_span_events, transaction_name_rules,
  url_rules, metric_name_rules, transaction_segment_terms, encoding_key, cross_process_id,
  trusted_account_ids, account_id, trusted_account_key, primary_application_id, agent_config,
  js_agent_loader, beacon, error_beacon, browser_key, application_id, messages[]}}`. Only
  `agent_run_id` is mandatory (Go: "connect reply missing agent run id"); Go's
  `ConnectReplyDefaults` fills the rest (`apdex_t` 0.5, every `collect_*` true, sampling target
  10 per 60 s). Python adds an `agent_settings` call.
- Data methods and bodies:

  | Method | Body |
  |---|---|
  | `metric_data` | `[run_id, start_s, end_s, [[{"name", "scope"}, [count, total, exclusive, min, max, sum_of_squares]], …]]`. Apdex reuses the six slots (satisfied, tolerating, frustrating, 0, apdex_t, apdex_t). Names are New Relic's timeslice vocabulary: `WebTransaction/…`, `Apdex/…`, `Datastore/…`, `External/…`, `Errors/…`, `Supportability/…`; scoped metrics carry the transaction name in `scope` |
  | `analytic_event_data` (transaction events), `custom_event_data`, `error_event_data`, `span_event_data` | `[run_id, {"reservoir_size", "events_seen"}, [[intrinsics, user_attributes, agent_attributes], …]]`; a span's intrinsics include `guid`, `traceId`, `parentId`, `transactionId`, `timestamp`, `duration`, `name`, `category`, `span.kind`, `priority`, `sampled`, `nr.entryPoint` |
  | `log_event_data` | the Log API's detailed shape: `[{"common": {"attributes": {"entity.guid", "entity.name", "hostname", "tags.<k>"}}, "logs": [{"severity", "message", "span_id", "trace_id", "timestamp", "attributes"}]}]` |
  | `error_data` | `[run_id, [[timestamp_ms, transaction_name, message, class, {agentAttributes, userAttributes, intrinsics, stack_trace}, transaction_guid], …]]` |
  | `transaction_sample_data` | `[run_id, [[start_ms, duration_ms, name, request_uri, TRACE, guid, null, force_persist, null, synthetics_resource_id], …]]`; `TRACE` is plain JSON in Go and `base64(zlib(json))` in Python |
  | `sql_trace_data` | `[[[path, uri, id, sql, metric, count, total_ms, min_ms, max_ms, base64(zlib(json(params)))], …]]`, no `run_id` |
  | `get_agent_commands`, `shutdown` | `[run_id]` |
  | `agent_command_results`, `profile_data` | `[run_id, results]` |
  | `update_loaded_modules` | `["Jars", env_info]` |

- Replies are `{"return_value": <value>}`; `{}` is accepted for every data method. Whether the
  legacy `{"exception": {…}}` envelope is still honored is UNVERIFIED.
- Status semantics (Go, Python, Ruby agree; Java and Node UNVERIFIED): 200/202 success;
  401 and 409 force a reconnect for a new run id; 410 shuts the agent down; 408, 429, 500,
  503, and transport errors keep the data for the next harvest; 400, 403, 404, 405, 411, 413,
  414, 415, 417, 431 discard the payload and continue; on `preconnect`/`connect`, anything but
  200/202/410 backs off and retries.
- `max_payload_size_in_bytes` (default 1,000,000, measured compressed in Go) caps each request.
- Infinite Tracing: a separate bidirectional gRPC stream, `com.newrelic.trace.v1.IngestService`
  (`RecordSpan`, `RecordSpanBatch`; `Span {trace_id, intrinsics, user_attributes,
  agent_attributes}`), with `license_key` and `agent_run_token` (the run id) as metadata, to
  `infinite_tracing.trace_observer.host`. Agents with it on send spans only there.
- The Python agent additionally sends dimensional metrics over plain OTLP/HTTP to
  `otlp_host:otlp_port/v1/metrics`, separately from the collector.

**Infrastructure agent** (`github.com/newrelic/infrastructure-agent`), undocumented outbound,
every URL a config key (`public:"false"` in `pkg/config/config.go`):

| Route | Redirect key (default) | Body |
|---|---|---|
| `POST /infra/v2/metrics/events/bulk` | `collector_url` (`https://infra-api.newrelic.com`) | gzip JSON `[{"ExternalKeys": [], "EntityID", "IsAgent", "Events": [{"eventType": "SystemSample" \| "ProcessSample" \| "StorageSample" \| "NetworkSample" \| "InfrastructureEvent" \| …, …}], "ReportingAgentID"}]`; headers `X-License-Key`, `X-NRI-Entity-Key`, `X-NRI-Agent-Entity-Id`; 500 events per batch, flushed every 1 s; honors `Retry-After`, backs off on 429 and 503. Per-sample field lists are UNVERIFIED (defined by the samplers under `internal/plugins/`) |
| `POST /identity/v1/connect` (then `PUT` for updates), `PUT /disconnect`, `/register/batch` | `identity_url` (`https://identity-api.newrelic.com`) | `{"fingerprint", "metadata", "type", "protocol": "v1", "entityId"?}` → `{"identity": {"entityId", "GUID"}}`; header `License` |
| `/inventory/deltas`, `/deltas/bulk` | `collector_url` + `inventory_ingest_endpoint` | reply shape UNVERIFIED |
| `/agent_commands/v1/commands` | `command_channel_url` | polled; method and reply UNVERIFIED |
| `POST /metric/v1/infra` | `metric_url` (`https://metric-api.newrelic.com`) | Metric API JSON with `X-License-Key` and `X-NRI-Agent-Entity-Id` (integration protocol v4 dimensional metrics) |
| Log API | `logging_endpoint` | the embedded Fluent Bit's `out_newrelic` |

The repository's `test/proxy/fakecollector` serves `/identity/v1/connect` and the events route
and is the reference receiver.

**Shippers and SDKs that post the public APIs**, with their redirect setting: Vector's
`new_relic` sink (`api: events | metrics | logs`, region only; an endpoint override is
UNVERIFIED), `newrelic-fluent-bit-output` (`endpoint`), `fluent-plugin-newrelic` (`base_uri`),
`nri-statsd`/gostatsd (Metric API, Event API, or the Infrastructure agent's local `/v1/data`),
the Telemetry SDKs (Go, Node, Rust, .NET, Ruby, and C are archived; Java and Python are not; no
deprecation statement in favor of OTLP was found), and NRDOT (`otlphttp` exporter,
`OTEL_EXPORTER_OTLP_ENDPOINT`, `api-key` header, transform processors that truncate
attributes at 4,095 chars, `cumulative_to_delta` on host metrics).

### Unverified, to be settled by W1

1. Whether the 1 MB cap is measured compressed or decompressed.
2. `X-License-Key` and `X-Insert-Key` as headers on the Metric, Event, and Log APIs.
3. The Event API without an account id in the path.
4. The Log API's future-timestamp bound and its exact success status code.
5. Trace API limits: spans per request, attributes per span, timestamp window.
6. OTLP/JSON, HTTP/2 on OTLP/HTTP, `Retry-After` on an OTLP 429.
7. Storage of OTLP span links; handling of `observed_timestamp` and `event_name`.
8. The first point of a cumulative series.
9. Remote write 2.0 and native histograms.
10. Whether the collector accepts a `connect` from a non-agent sender, re-normalizes metric
    names that never saw its naming rules, and attaches a harvest sent under a new run id to
    the same APM entity as the original agent.
11. Java's and Node's status-code table; the legacy `exception` envelope.
12. Infrastructure agent inventory and command-channel reply shapes; per-sample fields.
13. Entity synthesis from `service.name` on the Metric and Log APIs, end to end.
14. `nri-statsd` support for sets, histograms, and TCP; Vector's endpoint override.

## New Relic's data against `Event`

The event model carries most New Relic fields in a typed field or a `newrelic.*` attribute. Two
gaps are marked; both are settled by W1 with one model addition.

| New Relic concept | `Event` representation | Verdict |
|---|---|---|
| metric `count` with `interval.ms` | `Sum { temporality: Delta, monotonic: true }`; `MetricRecord.start_timestamp` = `timestamp`, `Event::timestamp` = `timestamp + interval.ms` | lossless |
| metric `gauge` | `Gauge` | lossless |
| metric `summary {count, sum, min, max}` | no exact kind: `Summary` has `count`/`sum` and quantiles but no `min`/`max`; `Histogram` has `sum`/`min`/`max` but counts only per bucket | **gap: §1** |
| APM timeslice `[count, total, exclusive, min, max, sum_of_squares]`, `name`, `scope` | no kind carries `exclusive` or `sum_of_squares`; `scope` is an attribute | **gap: §1** |
| custom event (`eventType` plus flat attributes, no message) | a log event with an empty body, `newrelic.event_type` (also `LogRecord.event_name`), attributes verbatim | lossless; `newrelic_out` sends any event carrying `newrelic.event_type` to the Event API and never to the Log API |
| Log API `message`, `timestamp`, attributes, `common` | `LogRecord.message` verbatim, never pre-parsed (New Relic parses a JSON message on ingest, so a relayed message is exact); `common` on the batch `Resource` | lossless |
| Trace API span (`newrelic` format) | `SpanRecord { trace_id, span_id, parent_span_id, name, end_timestamp = timestamp + duration.ms, kind: Internal, status: Error if any error.* }`; `service.name` and every other attribute verbatim | lossless; `kind`, `status: Ok`, events, links, and `trace_state` have no field on this API and are counted on encode (a `known-gaps.md` row) |
| span ids | Trace API ids are free-form strings; APM `guid` is 16 hex chars and `traceId` 32 | lossless for hex ids; a non-hex Trace API id is a decoded `newrelic.span.id`/`newrelic.trace.id` attribute with a derived `TraceRef` (a named normalization) |
| APM event triple `[intrinsics, user, agent]` | intrinsics as bare attributes, `newrelic.user.<k>`, `newrelic.agent.<k>` | lossless; the encoder rebuilds the triple from the prefixes |
| APM `span_event_data` | `SpanRecord` from `guid`, `traceId`, `parentId`, `timestamp`, `duration`; `name`; `span.kind` from `span.kind`/`category`; the rest as attributes | lossless |
| APM `log_event_data` | as the Log API, with `entity.guid`/`entity.name`/`hostname` from `common` on the `Resource` | lossless; `newrelic_apm_out` rewrites `entity.guid` (§2) |
| APM `error_data` | a log event per error: `message`, `severity: Error`, `newrelic.apm.error.class`, `transaction_name`, `transaction_guid`, `stack_trace` (array), the three attribute maps under their prefixes | lossless |
| harvest envelope: `run_id`, `start`/`end`, `reservoir_size`/`events_seen`, method | batch `Resource` under `newrelic.apm.run_id`, `newrelic.apm.method`, `newrelic.apm.reservoir_size`, `newrelic.apm.events_seen`; harvest `start`/`end` as `MetricRecord.start_timestamp`/`Event::timestamp` | lossless; one batch per harvest call is the "batching" normalization |
| agent identity from `connect`: `app_name[]`, `host`, `identifier`, `language`, `agent_version`, `environment`, `utilization`, `labels`, `metadata`, `settings` | `newrelic.apm.*` on the `Resource` of every batch from that run, nested `Value::Map`/`Value::Array` where the payload nests | lossless |
| `transaction_sample_data`, `sql_trace_data`, `profile_data`, `update_loaded_modules` | opaque relay: one log event per method call, `Bytes` body holding the JSON, `newrelic.apm.method`; the per-language `TRACE` encoding travels inside the blob untouched | representable, not modeled |
| Infrastructure `Events[]` with `EntityID`, `ExternalKeys`, `IsAgent`, `ReportingAgentID` | the custom-event representation; the envelope fields as `newrelic.infra.*` on the `Resource` | lossless |
| timestamps: ms on the wire; 48 h/24 h (metrics), 48 h (logs), ±24 h (events) | ns in the model; `newrelic_out` drops and counts `stale` | permitted normalization plus a counter |
| 1 MB per POST; 4,094/4,096-char values; 254/150 attributes | `newrelic_out` splits requests at the cap; truncation is left to New Relic, which reports it through `NrIntegrationError` | permitted normalization |

## Direct or through a stand-in: trade-offs and best practice

**OTLP, direct.** For: New Relic's own recommended path, protobuf, gzip or zstd, the only public
route to `distribution` and `cumulativeCount`, richer than the Trace API (kind, status, events),
and `otlp_out` exists. Against: the 1 MB cap with no request splitting in `otlp_out`; maps in log
attributes pruned; a `Distribution` or `Samples` arrives as a `Summary` whose quantiles New Relic
discards; no custom events.

**The JSON APIs, direct (`newrelic_out`).** For: custom events (nothing else carries them);
`count` with an explicit `interval.ms`; `summary` with min and max; server-side JSON parsing of a
log message; JSON is inspectable. Against: no histogram or distribution type at all; four hosts
and four body shapes; 1 MB per POST.

**Prometheus remote write, direct (`prometheus_out`).** For: already built and cumulative-only,
which is what New Relic's `cumulativeCount` and `distribution` want. Against: type by name
suffix or a label; version 1 only until UNVERIFIED item 9 settles.

**Best practice, to New Relic.** Traces: `otlp_out`. Logs: `otlp_out`, or `newrelic_out` when
the message should be parsed by New Relic rather than by a `json` transform. Metrics: `otlp_out`
with `aggregate`'s default delta temporality, or `prometheus_out` for a Prometheus-shaped
pipeline; `newrelic_out` only for `count`/`gauge`/`summary` where the JSON semantics are wanted.
Custom events: `newrelic_out`. APM-agent data: `newrelic_apm_out`, never `newrelic_out` or
`otlp_out` (§2). Don't send one signal both ways.

**Best practice, from New Relic.**

1. Redirect: point each shipper's `endpoint`, the Infrastructure agent's `collector_url` and
   `identity_url`, and each APM agent's `NEW_RELIC_HOST` at `logit` (`newrelic_in` and
   `newrelic_apm_in`). No application change; reversible per host.
2. Tee: fan `newrelic_in` and `newrelic_apm_in` out to the new backend beside a `newrelic_out`
   or `newrelic_apm_out` leg, and compare.
3. Cut over: drop the New Relic legs.

`nri-statsd` and NRDOT-fronted applications need no new component: `statsd_in` and `otlp_in`
on the same ports.

## Design

Each item is a decision the ADR records, and names the workstream that builds it.

### 1. One model addition: a timeslice metric kind (W1)

`MetricKind::Timeslice { count: u64, total: f64, exclusive: f64, min: f64, max: f64,
sum_of_squares: f64 }`, the APM protocol's six-number aggregate. It is mergeable (sums add, min
and max compose), it fits under `MetricKind`'s 176-byte bound (`Samples` stays the widest arm),
and it is the exact carrier of a Metric API `summary` (`exclusive` and `sum_of_squares` unset).
The ADR decides between optional fields on one kind and a second `Summary`-adjacent kind for the
four-field form. Every encoder gains an arm: OTLP degrades it to a `Histogram` with no buckets
and `sum`/`min`/`max`/`count` (a New Relic `distribution` of one bucket, UNVERIFIED), Prometheus
to a `summary` with `_count`/`_sum`, Graphite and collectd to one datapoint per field under
`multi_value: expand`, the rest skip and count. `type_sizes.rs`, `allocations.rs`,
`docs/design/memory.md`, and the native wire's record encoding change in the same commit.

This is the one core `logit` change the APM pair needs, and the reason W1 comes first.

### 2. The APM pair: a one-way pipeline carrying a request/response protocol (W1, W7)

The collector protocol pushes configuration *to* the agent: harvest limits and periods, sampling
target, naming rules, `agent_config`, `entity_guid`, and commands. `logit`'s pipeline is one-way.
Three shapes were considered:

- **(a) Static answers, per-app sessions on the sink.** `newrelic_apm_in` answers `preconnect`
  with itself, `connect` with a run id it mints, a placeholder `entity_guid`, and a configurable
  `harvest:` block (defaults from Go's `ConnectReplyDefaults` and the current production
  `event_harvest_config`); `get_agent_commands` returns `[]`. `newrelic_apm_out` keeps one
  session per upstream application, keyed on the batch `Resource`'s `newrelic.apm.app_name`,
  `host`, and `identifier`, in a bounded LRU (`max_apps`): on first sight it runs its own
  `preconnect`/`connect` against New Relic with that agent's original connect payload, then
  rewrites `run_id` and every `entity.guid` in the harvest to New Relic's, reconnects on 401 and
  409, and retires the session on 410. Lost: server-side configuration, commands, and the
  collector's naming rules on names the agent already sent (UNVERIFIED whether New Relic
  re-normalizes them).
- **(b) A proxying input.** `newrelic_apm_in` takes an `upstream:` and performs the real
  handshake, relaying the collector's reply to the agent, so configuration keeps flowing.
  `prometheus_in`'s scrape mode is the precedent for an input that is an HTTP client. Rejected
  as the default because it couples the receive side to New Relic's availability and makes the
  stand-in useless once New Relic is gone, which is the migration's end state.
- **(c) A back-channel from a sink to an input.** A new runtime concept for one protocol.
  Rejected.

Decision: (a). (b) is the documented fallback if W1's probe (a fake collector in front of a real
agent, relaying to the account) shows that New Relic rejects un-normalized names or needs the
original entity guid. The runtime needs nothing new for (a): `logit_out` is the precedent for a
sink that holds connection state across `send` calls, and an `Output` may key that state on the
batch resource. What (a) does need, and what W1 checks for, is that an input can answer a
request from configuration alone with no pipeline round trip (it can: `prometheus_in`'s
remote-write receiver already does), that a `Resource` can carry nested values (it can), and that
a batch is one harvest call (the decoder's contract).

### 3. Two new lossless pairs (W2, W6)

`newrelic_in -> newrelic_out` (the four public APIs plus the Infrastructure agent's events
route) and `newrelic_apm_in -> newrelic_apm_out` (the collector protocol, version 17) join the
like-protocol pairs in [ADR `lossless-transit`](../adr/lossless-transit.md) by amendment,
numbered after whichever of the Datadog stack's two pairs have landed first. Both codecs live in
`crates/logit-proto/src/newrelic/`, one module per payload family whose doc is the mapping
table. The amendment lands with the ADR.

### 4. Kinds and config (W4, W5, W7a, W7b)

- `newrelic_in` (W4): `bind:`, `bind_tls:`, and an optional `api_keys:` allowlist (empty =
  accept any) matched against `Api-Key` (header or query), `X-License-Key`, `License`, and
  `Authorization: Bearer`. Routes: `/metric/v1`, `/metric/v1/infra`, `/v1/accounts/{id}/events`,
  `/log/v1` (both body forms), `/trace/v1` (`newrelic` format; `zipkin` answered 415 and counted
  `skipped{format}`), `/infra/v2/metrics/events/bulk`, `/identity/v1/connect` (answered with a
  minted `entityId`/`GUID`); inventory, command channel, and unknown routes acknowledged and
  counted `skipped{route}`. Decompresses gzip and deflate. Per-body cap 1,000,000 bytes
  decompressed, a constant. Replies mirror New Relic's: 202 `{"requestId"}`, 200
  `{"success": true, "uuid"}` on the Event API. A full pipeline answers 429 with `Retry-After`
  or 503, which every sender retries, never 200-and-drop.
- `newrelic_out` (W5): `license_key: !env NEW_RELIC_LICENSE_KEY`, `account_id` (Event API
  only; rule: required iff any event carries `newrelic.event_type`, checked at runtime and
  counted), `region: us | eu | jp | gov` (default `us`), optional per-API `endpoints:` overrides
  so a pair test can point at another `logit`'s `newrelic_in`, `compression: gzip`, `timeout`,
  `tls`. One request per API per batch, split at 1,000,000 compressed bytes, outside the three
  encoder shapes for the same reason `prometheus` is. `x-request-id` per request, constant across
  retries. `duplicate_safe()` is false: New Relic has no dedupe. A 429 `Retry-After` is honored
  by sleeping inside `send`, bounded by the `RetryConfig` budget, unless the ADR promotes it to
  `write_loop`. Host, service, and tags come from attributes and the resource (an upstream
  `set`), not from per-sink fields. Metric kinds: delta monotonic `Sum` → `count` with
  `interval.ms` from `start_timestamp`; `Gauge` → `gauge`; `Timeslice` → `summary`; `Samples`
  → `summary` of its count/sum/min/max (counted `degraded`); `Distribution` → `summary` of its
  summary fields (counted `degraded`); cumulative or non-monotonic `Sum`, `GaugeDelta`,
  `Histogram`, `ExponentialHistogram`, `Summary` (no min/max), `Set`, `SetMembers` skipped and
  counted, the `statsd_out` pattern.
- `newrelic_apm_in` (W7a): `bind:`, `tls:` **required** (agents refuse plaintext; a graph rule
  rejects its absence), `license_keys:` allowlist, `harvest:` (report period, per-type limits,
  sampling target, apdex). Answers every method in the table above; `shutdown` closes the
  session; `/agent_listener/invoke_raw_method` only.
- `newrelic_apm_out` (W7b): `license_key`, `host` (default `collector.newrelic.com`, region
  derived from the key as `go-agent` does), `max_apps` (session LRU bound), `timeout`, `tls`.
  Runs the handshake per session, applies the collector's `max_payload_size_in_bytes`, and maps
  status codes to the agents' own table: 401/409 reconnect, 410 retire, 408/429/500/503 `Clean`
  retry, the rest `Permanent`.
- Graph rules take the next numbers (62 and up), following rules 55 and 56's shape for mode and
  endpoint validation.

### 5. `otlp_out` for New Relic (W3)

- `max_request_bytes:` (default unset): one export becomes N requests per signal, split on
  record boundaries, sized on the compressed body. New Relic sets it to 1,000,000.
- `distributions: summary | histogram` (default `summary`, today's behavior): under `histogram`
  a `Distribution` is encoded as an explicit-bucket `Histogram` from `DdSketch`'s public bins
  (`positive_bins`/`negative_bins`, bucket bounds at the bin edges, `sum`/`min`/`max`/`count`
  from the summary), and `Samples` as the same after `Samples::sketch`. New Relic turns it into
  a `distribution`; a quantile `Summary` it throws away. A named, bounded-error normalization,
  counted `degraded`.
- zstd stays out: `Cargo.toml`'s rationale against the `zstd` crate holds and `ruzstd` decodes
  only. gzip is accepted.
- The empty success body is verified against `otlp_out`'s response handling.

### 6. Reuse (W3–W7)

The HTTP server plumbing behind `otlp_in`/`prometheus_in` (`crates/logit-inputs/src/http.rs`),
with per-listener route dispatch as `otlp_in` does it; `crate::http` in `logit-outputs`
(`build_client`, `read_body_prefix`, `body_snippet`, `is_retryable_http_status`,
`classify_reqwest_error`); `write_loop`'s bounded retry with `Fault` classification;
`TlsClientConfig`/`TlsServerConfig`; `logit_out`'s session-holding sink shape; graph rules 55
and 56.

### 7. Attribute vocabulary (W2, W6)

`newrelic.*` for raw encodings: `newrelic.event_type`, `newrelic.apm.*` (run and agent
identity, method, reservoir fields, `error.*`), `newrelic.infra.*`, `newrelic.user.<k>`,
`newrelic.agent.<k>`. New Relic's own names otherwise: `service.name`, `hostname`, `trace.id`
and `span.id` (decoded into `TraceRef`), `severity`. `newrelic_out` derives the rest:
`timestamp` in ms from `Event::timestamp`, `trace.id`/`span.id` as hex from `TraceRef`,
`message` from the body (a `Map` body serialized as JSON), `service.name` and `hostname` from
the attributes or resource.

### 8. Timestamp windows and stale data (W5)

`newrelic_out` drops and counts `dropped{reason="stale"}` a metric older than 48 h or more than
24 h ahead, a log older than 48 h, and an event outside ±24 h, instead of letting one stale record
fail a payload. The consequence for `buffer.disk:` replay after a long outage is documented with
the component.

### 9. Ordering (all)

W1 first: the spike closes the UNVERIFIED list, the ADR records the model kind and the pair
architecture, and the `Timeslice` kind lands with its tripwires. Then the public-API codecs and
pair (W2, W4, W5), with `otlp_out`'s changes (W3) alongside; then the APM codecs and pair
(W6, W7a, W7b); fixtures and account verification (W8); docs (W9). W3 and W6 depend only on
W1 and can be pulled forward.

### 10. Not in this stack

Infinite Tracing's gRPC stream (bidirectional streaming over the hand-rolled `hyper` gRPC is
new work, and the stream is gated on a run token); browser and mobile beacons (undocumented,
product-bound formats); the PHP daemon's FlatBuffers socket; relaying New Relic's server-side
configuration to an upstream agent if (a) holds; `Retry-After` as a `write_loop` feature if the
ADR keeps it sink-local; an `aggregate` rule for `Timeslice` beyond pass-through.

## Workstreams

| # | PR | Size | Depends on |
|---|---|---|---|
| W0 | This plan and its index row | S | — |
| W1 | Spike against the account: curl each API; `otlp_out` and `prometheus_out` as they are; cumulative first point and reset; the empty OTLP success body; a `Histogram` becoming a `distribution`; `service.name` entity synthesis on each API; a minimal fake collector (`tools/` throwaway) in front of a real Python agent, relaying to the account, to settle UNVERIFIED item 10. ADR `newrelic-api-and-apm-collector-relay`; `lossless-transit` amendment; `MetricKind::Timeslice` with `type_sizes.rs`, `allocations.rs`, `memory.md`, and the native wire; every UNVERIFIED item resolved and this plan updated | M | W0 |
| W2 | `logit_proto::newrelic` codecs: Metric, Event, Log, Trace (`newrelic` v1), Infrastructure events; fixed-point suites | M | W1 |
| W3 | `otlp_out`: `max_request_bytes` splitting, `distributions: histogram`, verified against the account | S | W1 |
| W4 | `newrelic_in`: receiver, key allowlist, identity stub, graph rules, schema | M | W2 |
| W5 | `newrelic_out`: direct API client, region table, request splitting, `Retry-After`, stale filter, graph rules, schema | M | W4 |
| W6 | `logit_proto::newrelic::apm` codecs: the protocol-17 envelope and query, `metric_data`, the event triples, `log_event_data`, `error_data`, opaque methods; fixed-point suites | M | W1 |
| W7a | `newrelic_apm_in`: the collector stand-in (`preconnect`, `connect`, data methods, commands, `shutdown`), TLS required, `harvest:` defaults, schema | M | W6 |
| W7b | `newrelic_apm_out`: per-application sessions, run-id and `entity.guid` rewrite, the status table, bounded sessions | L | W7a |
| W8 | Recorded fixtures via `script/record-fixtures` (Vector's `new_relic` sink, `newrelic-fluent-bit-output`, `nri-statsd`, the Infrastructure agent with `collector_url` at the capture, a Python APM agent with `NEW_RELIC_HOST` at the capture); pair fixed-point tests over the corpus; account end-to-end for `newrelic_out` and `newrelic_apm_out` (spans and transactions under the APM entity, timeslice-driven APM pages) | M | W5, W7b |
| W9 | `docs/newrelic.md` (operator best practices from this plan, including that APM data reaches New Relic only through `newrelic_apm_out`); `deploying.md`; `known-gaps.md` rows; `AGENTS.md` tables; `telemetry-landscape.md` cells; four examples (`newrelic-otlp.yaml`, `newrelic-direct.yaml`, `newrelic-intake-standin.yaml`, `newrelic-apm-standin.yaml`) and `NEW_RELIC_LICENSE_KEY` in `every_shipped_config_loads_and_validates`'s `!env` map (`crates/logit-cli/src/config.rs`) | M | W8 |

Landing order: W0 → W1 → W2 → W3 → W4 → W5 → W6 → W7a → W7b → W8 → W9, linear; W3 and W6
stack in sequence to keep the stack linear even though each depends only on W1. Each PR is based
on and targets its parent's branch and is brought up to date with `git merge origin/main`, never
a rebase.

## Verification

- Per PR: `script/cibuild` green; `script/schema` regenerated and committed by any workstream
  that changes a config type; `script/validate` for any that adds a config.
- W1: each UNVERIFIED item has a recorded answer (the curl, the response, the account's view)
  and the plan text is updated; `type_sizes.rs`, `allocations.rs`, and `memory.md` change in
  the same commit as the kind.
- W2/W6: `decode -> encode -> decode` fixed point over every wire feature listed above, per
  `lossless-transit`'s rule; the APM envelope round-trips each agent's query-parameter order.
- W3: an `otlp_out` export over 1 MB reaches the account in N requests with nothing dropped; a
  `Distribution` appears as a `distribution` with percentiles.
- W4/W5: Vector's `new_relic` sink and the Infrastructure agent deliver to `newrelic_in`; the
  account shows `newrelic_out`'s metrics, events, logs, and spans, with no `NrIntegrationError`
  and a service entity synthesized from `service.name`.
- W7: a real Python and Java agent with `NEW_RELIC_HOST` at `newrelic_apm_in` connect, harvest,
  survive a forced 409 reconnect, and stop on 410; `newrelic_apm_out` relays to the account and
  the APM UI shows the application with transactions, spans, errors, and logs in context.
- W8: both pair tests hold over the recorded corpus.
- W0 (this PR) is documentation only: every relative link resolves and `docs/plans/README.md`
  gained a row.
