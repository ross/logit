---
created: 2026-09-24
updated: 2026-09-24
---

# Enabling plan: Dynatrace — OTLP and the ingest APIs, OneAgent-local stand-in, intake stand-in

## Context

`logit` has no Dynatrace-specific component. Three existing components already touch Dynatrace,
none verified: `otlp_out` can post to `/api/v2/otlp/v1/{traces,metrics,logs}` with
`headers: {Authorization: "Api-Token …"}`; `prometheus_out`'s exposition endpoint is what an
ActiveGate or the Kubernetes annotation scrape pulls from; and `statsd_in` speaks the DogStatsD
grammar OneAgent's own StatsD listener accepts. Nothing sends Dynatrace's metrics line protocol,
its Log API, Davis events, or business events, and nothing receives any of them. This plan
records what Dynatrace accepts and emits, where the event model and the existing components fall
short of it, which way to send data and when, and the workstreams that close the gaps.

Goals:

- Send logs, metrics, traces, Davis events, and business events to Dynatrace, directly to a SaaS
  environment or an ActiveGate and through a local OneAgent, with a best-practice recommendation
  per signal.
- Receive what Dynatrace-instrumented systems produce, losslessly, in the two places `logit` can
  stand: as the **intake** that redirected senders post to (Telegraf, Micrometer, Logstash,
  Fluentd, Fluent Bit, the Dynatrace OpenTelemetry Collector distribution, and applications built
  on `dynatrace-metric-utils`), and as the **OneAgent-local endpoint** on `127.0.0.1:14499` that
  Telegraf, Micrometer, and `dynatrace_ingest` default to.
- Make migrations to and from Dynatrace a configuration change, not an application change.

Non-goals: OneAgent's own channel to an ActiveGate or cluster (`/communication`: protobuf,
encrypted, no published schema, so a relay can't stand in for the cluster from OneAgent's side and
OneAgent-collected data has no interception point); RUM and OpenKit beacons (`/mbeacon`, `/dtmb`);
the Platform generic, SDLC, security, and Smartscape event endpoints under `/platform/ingest/v1/`;
Prometheus remote write (Dynatrace has none; it pulls exposition); OpenPipeline forwarding (the
only egress, gzip NDJSON to S3, Blob, or GCS, so the only thing "behind Dynatrace" is a bucket
drop, not a wire; an object-storage input is out of scope); DynatraceStatsD as a component of its
own (`statsd_in` covers the grammar; W1 verifies the flavor); Extensions 1.0 and 2.0 data sources.

Stream key **`dt`**: branches `dt/w0`…`dt/w7`, stacked as the workstream table says. PR stack
only: nothing is merged by this workstream; Ross directs merging. The decisions are recorded in
an ADR (W1), written once the spike against a real environment settles the items marked
UNVERIFIED below; the design sections are the build-out of that record.

Settled with Ross (2026-09-24): verification happens first (W1), against a Dynatrace SaaS trial
and a OneAgent on a throwaway VM, because every surface here is HTTP with text, JSON, or protobuf
bodies and a curl settles most questions in minutes; cumulative-to-delta conversion, which
Dynatrace requires and `logit` lacks, becomes a `delta` transform with its own ADR, landed outside
this stream because the Datadog and New Relic sinks want it too, and this stream depends on it;
the four-field summary kind (`count`, `sum`, `min`, `max`) that Dynatrace's summary gauge needs
is New Relic's `MetricKind::Timeslice` if `nr/w1` has landed when `dt/w1` starts, else `dt/w1`
lands the four-field form and the New Relic stream adopts it.

## Coverage by signal

Dynatrace ingests five kinds of data. Each cell reads today → after this stack.

| Signal | Direct via OTLP | Direct via the ingest APIs | Through a local OneAgent | Intake stand-in (receive from senders) | OneAgent-local stand-in (`:14499`) |
|---|---|---|---|---|---|
| Logs | `otlp_out` (unverified) → verified (W5) | none → `dynatrace_out` `/api/v2/logs/ingest` (W4) | none → `dynatrace_out` `api: oneagent` `/v2/logs/ingest` (W4) | none → `dynatrace_in` `/api/v2/logs/ingest` and `/api/v2/otlp/v1/logs` (W3) | none → `dynatrace_in` `/v2/logs/ingest` (W3) |
| Metrics | `otlp_out` (unverified; cumulative sums rejected, a `Distribution` arrives as a rejected `Summary`) → verified, delta enforced upstream by `delta`, `distributions: histogram` from `nr/w3` (W5) | none → `dynatrace_out` line protocol `/api/v2/metrics/ingest` (W4) | none → `dynatrace_out` `api: oneagent` `/metrics/ingest`, with OneAgent's host enrichment (W4) | none → `dynatrace_in` line protocol and `/api/v2/otlp/v1/metrics` (W3) | none → `dynatrace_in` `/metrics/ingest` (W3) |
| Traces | `otlp_out` (unverified) → verified (W5); the only trace wire Dynatrace has | — | `otlp_out` to `localhost:14499/otlp/v1/traces` with `compression: none` (W5) | none → `dynatrace_in` `/api/v2/otlp/v1/traces` (W3) | none → `dynatrace_in` `/otlp/v1/traces` (W3) |
| Davis events | — | none → `dynatrace_out` `/api/v2/events/ingest` (W4) | none → `dynatrace_out` `api: oneagent` `/v2/events/ingest` (W4) | none → `dynatrace_in` `/api/v2/events/ingest` (W3) | none → `dynatrace_in` `/v2/events/ingest` (W3) |
| Business events | — | none → `dynatrace_out` `/api/v2/bizevents/ingest` (W4) | no OneAgent path; skipped and counted | none → `dynatrace_in` `/api/v2/bizevents/ingest` (W3) | — |

Two positions Datadog has and Dynatrace doesn't: there is no open agent-to-backend protocol to
stand in for (OneAgent's channel is private), and nothing leaves Dynatrace over a wire (egress is
object storage only). One position New Relic doesn't have and Dynatrace does: a local agent with a
plain-HTTP ingest API on a fixed port that popular libraries default to, so a stand-in on
`127.0.0.1:14499` catches Micrometer and Telegraf traffic with no sender change.

## What Dynatrace accepts and emits

Surveyed 2026-09-24 from `docs.dynatrace.com`, the `dynatrace-metric-utils-go` and
`dynatrace-metric-utils-java` repositories, `dynatrace-otel-collector`, the Telegraf, Micrometer,
Logstash, and Fluentd sender sources, and the OpenTelemetry Collector `resourcedetection`
processor's `dynatrace` detector. Items marked UNVERIFIED were not confirmed by a current official
page or by source; W1 verifies each against the trial environment or a real OneAgent and this
section is updated then.

### Facts common to every public API

- URL forms: SaaS `https://{env}.live.dynatrace.com/api/v2/…`; Managed and Environment
  ActiveGate `https://{ag}:9999/e/{env}/api/v2/…`; a containerized ActiveGate has no `:9999`.
  The "latest Dynatrace" proxy `https://{env}.apps.dynatrace.com/platform/classic/environment-api/v2/…`
  is OAuth only. The FedRAMP host suffix is UNVERIFIED.
- Auth: `Authorization: Api-Token <token>` (a classic token with the scope the endpoint needs:
  `metrics.ingest`, `logs.ingest`, `events.ingest`, `bizevents.ingest`,
  `openTelemetryTrace.ingest`) or `Authorization: Bearer <platform token>` with the matching
  `openpipeline:*:ingest` scope. An ActiveGate takes only classic tokens.
- There is no `/platform/ingest/v1/{logs,metrics,spans,otlp}`: logs, metrics, and spans stay on
  `/api/v2/…`, routed through OpenPipeline. No deprecation notice exists for the v2 ingest
  endpoints. `/platform/ingest/v1/events` and its SDLC, security, and Smartscape siblings are
  separate products (non-goals).
- Rate limiting is an environment-wide thread pool with a queue; when both are full, or a request
  waits more than 10 s in the queue, the answer is 429. No `Retry-After` or `X-RateLimit-*`
  header is documented (UNVERIFIED as a negative).
- Ingest is asynchronous: a success status (202 for metrics and business events, 204 for logs,
  201 for Davis events) means accepted for processing, not stored. The metrics and business-event
  endpoints report per-record validity in the response body; the Log API answers 200 for a
  partial success with no per-record detail.

### Public intake APIs

| Endpoint | Body and limits | Notes |
|---|---|---|
| `POST /api/v2/metrics/ingest` | `text/plain`; lines `key[,dim="v"…] <payload> [ts_ms]`; payloads `gauge,<v>` (a bare `<v>` is `gauge,min=v,max=v,sum=v,count=1`), `gauge,min=<a>,max=<b>,sum=<c>,count=<n>`, `count,delta=<v>`; metadata lines `#key <gauge\|count> dt.meta.displayName="…",dt.meta.description="…",dt.meta.unit="…"`; 1 MB per request; timestamps in UTC ms, optional (absent = server time), accepted from 1 h in the past to 10 min in the future | Metric key 3–255 chars (the Go utils truncate at 250; the OTLP limits page says 2–255): letters, digits, `-`, `_`, `.`-separated sections, first char a letter or `_`; keys starting `dt.` are reserved and dropped. Dimension key ≤100 chars, lowercase `[a-z0-9_:.-]`; dimension value ≤255 (Go: 250), quoted, `\` escapes `"` and `\`; ≤50 dimensions per line (the line is dropped over that); line ≤50,000 chars (utils only, UNVERIFIED for the server). Lines per request: 1,000 in the utils constant and on the OneAgent-local page; the environment-API page says "no limit on the number of metrics" (contradiction, UNVERIFIED). NaN and ±Inf rejected client-side; a summary with `count < 0`, `max < min`, or a mean outside `[min, max]` rejected client-side; negative `count,delta` allowed by every library, server acceptance UNVERIFIED. `Content-Encoding: gzip` UNVERIFIED (no sender compresses). 202 `{"linesOk", "linesInvalid", "error": {"code", "message", "invalidLines": []}, "warnings": {"changedMetricKeys", "message"}}`; 400 = some lines invalid, valid lines accepted; 413 UNVERIFIED. Metadata limits: display name 300, description 65,535 (OTLP page: 1,023), unit 63. Environment limits: 100,000 custom metric keys; 1,000,000 dimension tuples per metric. A summary gauge is not a distinct type: a single value is stored as the degenerate summary, and `count` is the other type. **No histogram, percentile, or sketch payload exists on this wire.** Classic appends `.count`/`.gauge` to keys; Grail doesn't |
| `POST /api/v2/logs/ingest` | `application/json` (one object or an array), the JSONL types (`application/jsonl`, `application/jsonlines`, `application/x-ndjson`, `application/jsonlines+json`, `application/x-jsonlines`), or `text/plain` (**the whole body is one record**, not one per line); UTF-8 with the charset declared; 10 MB per request (413 when the expanded batch exceeds 16 MB), 50,000 records; `content` ≤10 MB (512 kB on the classic pipeline; the older 8,192- and 65,536-char figures are superseded); attribute key ≤100 bytes, value ≤32 kB, ≤500 attributes per record, nesting ≤5 levels, ≤32 values per array attribute; timestamps older than 24 h are discarded, more than 10 min ahead are reset to ingest time | Key lookup is case-insensitive, first match wins. Timestamp keys in order: `timestamp`, `@timestamp`, `_timestamp`, `eventtime`, `date`, `published_date`, `syslog.timestamp`, `time`, `epochSecond`, `startTime`, `datetime`, `ts`, `timeMillis`, `@t`; formats Unix epoch, RFC 3339, RFC 3164; an unparseable value falls back to ingest time. Level keys: `loglevel`, `status`, `severity`, `level`, `syslog.severity`, mapped by prefix (`emerg`/`f` → EMERGENCY, `e` → ERROR, `a` → ALERT, `c` → CRITICAL, `s` → SEVERE, `w` → WARN, `n` → NOTICE, `i` → INFO, `d`/`trace`/`verbose` → DEBUG, else NONE) and folded into `status` (ERROR, WARN, INFO, NONE). Content keys: `content`, `message`, `payload`, `body`, `log` (plus `_raw` in the raw model); a JSON string in `content` is **not** parsed. With OpenPipeline (SaaS, or ActiveGate 1.295+) all JSON types survive; otherwise every value becomes a string. Raw model (default from 1.331): nested objects become JSON strings, arrays are unified to one element type; flattened model (default through 1.330): dotted keys 5 levels deep; selected per request with `?structure=raw\|flattened` or `X-Dynatrace-Options` (SaaS only). Query-parameter and `X-Dynatrace-Attr` attributes override the body; overridden values are kept as `overwritten[N].<key>`. `dt.auth.origin` (the token's public part) is added. Which keys among `host.name`, `dt.entity.*`, `dt.source_entity`, `trace_id`/`span_id`, `event.*` get special handling is UNVERIFIED. 204 success (no body), 200 partial success, 400, 402 (quota), 413, 429 ("retry with exponential backoff"), 501 (log storage not enabled), 502/503/504 retryable. Gzip: the Logstash plugin sends `Content-Encoding: gzip`; the docs are silent (UNVERIFIED) |
| `POST /api/v2/otlp/v1/{traces,metrics,logs}` | OTLP/HTTP, **binary protobuf only** (no OTLP/JSON), **HTTP only** (no gRPC); `Api-Token` or `Bearer`; traces 8 MB (uncompressed and gzip-compressed alike, "to ActiveGate"), metrics 4 MB uncompressed and 15,000 data points (the whole request is dropped over either), logs 10 MB | The model mapping is the next table. Trace end times must fall within 60 min in the past to 10 min in the future. The per-span caps of 128 attributes, events, and links are SDK defaults, "not limited by Dynatrace". Span attribute storage follows the environment's capture preference (allow all minus a blocklist, or block all plus an allowlist; the default for a new environment is UNVERIFIED). Exemplar storage is UNVERIFIED. There is no Platform-API OTLP path |
| `POST /api/v2/events/ingest` (Davis events) | JSON `{eventType, title, startTime?, endTime?, timeout?, entitySelector?, properties?}`; `eventType` one of `AVAILABILITY_EVENT`, `CUSTOM_ALERT`, `CUSTOM_ANNOTATION`, `CUSTOM_CONFIGURATION`, `CUSTOM_DEPLOYMENT`, `CUSTOM_INFO`, `ERROR_EVENT`, `MARKED_FOR_TERMINATION`, `PERFORMANCE_EVENT`, `RESOURCE_CONTENTION_EVENT`, `WARNING`; `title` required; `startTime` UTC ms (default now), `endTime` (default start + timeout), `timeout` minutes (default 15, max 360), `entitySelector` (default the environment), `properties` ≤100 pairs, key ≤100 chars, value ≤4,096 | Windows: 6 h in the past to 5 min in the future for problem-opening events, 30 days to 7 days for info events. 201 `{"reportCount", "eventIngestResults": [{"correlationId", "status": OK \| INVALID_ENTITY_TYPE \| INVALID_METADATA \| INVALID_TIMESTAMPS}]}`. The v1 `/api/v1/events` is deprecated with a published migration guide; its end of life is UNVERIFIED |
| `POST /api/v2/bizevents/ingest` (business events) | `application/json` (one object or an array, no mandatory fields), `application/cloudevent+json` (`specversion`, `id`, `source`, `type` required; `source` → `event.provider`, `type` → `event.type`, `id` → `event.id`, `data` fields lifted to the top level), and a CloudEvents batch type spelled `application/cloudevents-batch+json` on one page and `application/cloudevent-batch+json` on another (contradiction, UNVERIFIED); 5 MB per request | Nested objects are stored as strings; only top-level fields become fields. 202; 400 ("some business events are invalid; valid ones are accepted") with `errors[]` of `{id, index, message, source}`; 413; 429; 503. Reachable through an ActiveGate per community reports only (UNVERIFIED) |

Legacy `POST /api/v1/entity/infrastructure/custom/{id}` (custom devices) is deprecated since
1.263 and its replacement, `POST /api/v2/entities/custom`, carries no data points: the metrics
endpoint above is the replacement.

OTLP's model mapping, which decides what `otlp_out` must send:

| OTLP | Dynatrace |
|---|---|
| Gauge | gauge |
| Sum, monotonic, delta | counter |
| Sum, monotonic, cumulative | **rejected**, `UNSUPPORTED_METRIC_TYPE_MONOTONIC_CUMULATIVE_SUM`; "the Dynatrace backend exclusively works with delta values"; the documented remedy is client-side (`OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=DELTA`, or the Collector's `cumulativetodelta` processor with `max_staleness`); no server-side conversion exists |
| Sum, non-monotonic, delta | counter |
| Sum, non-monotonic, cumulative | gauge |
| Histogram, delta | a histogram with buckets when "Advanced OTLP dimensions" is on; otherwise a counter plus min/max/sum/count. A histogram without `sum`, or with unsorted or NaN/Inf bounds, is an error |
| Histogram, cumulative | not ingested |
| ExponentialHistogram, delta | gauge of min/max/sum/count; the buckets are dropped |
| Summary | **not supported** |
| Exemplars | UNVERIFIED |
| Data-point, scope, and resource attributes | default mode: keys lowercased; a resource or scope attribute becomes a dimension only if it passes the allow list and then the deny list. Advanced mode: every resource, scope, and point attribute becomes a dimension except the deny list, keys keep their case and any printable ASCII, and `otel.scope.name`/`otel.scope.version` are always added. Values must be string, bool, or int; any other type drops that dimension. Metric keys starting `dt.` are dropped |
| Log body | string → `content`; map → a JSON string (raw model) or flattened like attributes; array → stringified; bytes → base64 |
| Log severity, timestamp, trace context | `SeverityText`, then `SeverityNumber`, then the attribute keys above, then NONE; record `Timestamp`, then the attribute keys above; `TraceId`/`SpanId` → `trace_id`/`span_id` in hex; key collisions become `overwrittenN.<key>` |

Recommended settings, from Dynatrace's own pages: delta temporality everywhere, the Collector's
`cumulativetodelta` with a `max_staleness` above the longest reporting gap, gzip, batches of
5,000 spans, 3,000 metric points, or 1,800–2,000 log records with a 60 s flush.

### Agent-side surfaces

**OneAgent's local ingest API**, served by the Extension Execution Controller (EEC) on
`127.0.0.1:14499` (`oneagentctl --set-extensions-ingest-port`), plain HTTP, no auth, loopback only,
available only on Full-Stack and Infrastructure deployments (not in containers, where the docs
point at an ActiveGate). The paths are **not** the cluster's:

| Local path | Cluster equivalent | Differences |
|---|---|---|
| `POST /metrics/ingest` | `/api/v2/metrics/ingest` | OneAgent adds "the host ID and host name" as dimensions (the exact keys are UNVERIFIED); 1,000 lines per request; the documented response is `{"error", "linesValid", "linesInvalid"}`, not the cluster's `linesOk` (contradiction; W1 checks which is real). Needs OneAgent 1.243+ with EEC enabled; the API exists since 1.201 |
| `POST /v2/logs/ingest` | `/api/v2/logs/ingest` | "mimics" the public endpoint under its limits; whether it enriches is UNVERIFIED |
| `POST /v2/events/ingest` | `/api/v2/events/ingest` | POST only; no compression "yet" |
| `POST /otlp/v1/traces` | `/api/v2/otlp/v1/traces` | **traces only** (no metrics or logs), protobuf only, no gRPC, no `Content-Encoding`; disabled by default |

EEC and the local API are switched on under Settings > Preferences > Extension Execution
Controller ("Enable local HTTP Metric, Log and Event Ingest API"), overridable per host or host
group. `dynatrace_ingest` (`/opt/dynatrace/oneagent/agent/tools`, `-p/--port`, default 14499)
takes metric lines on stdin or as arguments and is a client of the same port; whether it uses the
plain `/metrics/ingest` route or a private EEC route is UNVERIFIED.

**DynatraceStatsD**: UDP `127.0.0.1:18125` (`[::1]:18125` without IPv4), inside EEC, OneAgent
1.201+; a remote listener on an ActiveGate at `18126` (1.227+, `statsdenabled=true` in
`extensionsuser.conf`). Types `|c`, `|g`, `|ms`, `|h`, `|d`, and `|s` (1.303+); DogStatsD-form
tags `|#k:v,k:v`; aggregated and sent once per minute; OneAgent adds host ID and host name, the
ActiveGate listener adds nothing. Sample rates, `_e{}` events, `_sc` service checks, and how
`ms`/`h`/`d` become summaries are UNVERIFIED. Not deprecated (the end-of-life page lists Python
Extensions 1.0 and Log Monitoring Classic, not StatsD).

**Metadata enrichment** is done by the sender, not the endpoint, which is why it survives a
stand-in. OneAgent-injected processes see a virtual file
`dt_metadata_e617c525669e072eebe3d0f08212e8f2.{properties,json}` (opened by bare name, holding
one line: the absolute path of the real file); every host has
`/var/lib/dynatrace/enrichment/dt_host_metadata.{properties,json}` (`dt.entity.host`,
`dt.entity.host_group`, `dt.host_group.id`, `host.name`, `dt.smartscape.host`,
`dt.security_context`, cost-center keys, cloud ids, `k8s.cluster.uid`, `k8s.node.name`); the
Operator injects `dt_metadata.{properties,json}` into pods (`k8s.*`; one page also lists
`dt.entity.kubernetes_cluster` and `dt.kubernetes.*`, another only `k8s.*`, a contradiction) and
`endpoint/endpoint.properties` (`DT_METRICS_INGEST_URL`, `DT_METRICS_INGEST_API_TOKEN`). Readers:
`dynatrace-metric-utils-java` (virtual file, then `dt_metadata.properties`; metadata dimensions
override user dimensions), the Go utils (Windows only; Linux needs the OneAgent SDK), and the
Collector's `dynatrace` resource detector (reads only `dt_host_metadata.properties`, keeps only
`dt.entity.host`, `host.name`, `dt.smartscape.host`). Whether
`dt.entity.process_group_instance` appears in the virtual file is UNVERIFIED. What a stand-in on
`:14499` loses is only what OneAgent itself adds on the local API: host ID and host name.

**ActiveGate** (`:9999`, `port-ssl` to change it): serves `/e/{env}/api/v2/metrics/ingest`,
`/e/{env}/api/v2/logs/ingest`, and `/e/{env}/api/v2/otlp/v1/*` on documented pages; events and
bizevents by analogy and community report (UNVERIFIED); StatsD on `18126`. Modules:
`otlp_ingest_enabled`, `log_analytics_collector_enabled`, `metrics_ingest_enabled`,
`restInterface`, `extension_controller_enabled`, `MSGrouter` (routes OneAgent traffic). It batches
logs and answers `503 Usable space limit reached` when its queue (default 300 MB) fills. The
Fluentd and Logstash plugins name their endpoint setting `active_gate_url`. OneAgent →
ActiveGate → cluster traffic (`--set-server=https://host:9999/communication`, authenticated by a
tenant token) is described only as encrypted protobuf; no `.proto`, message schema, or spec is
published, and the OneAgent log module uses the same acknowledged channel. **A relay can't stand in
for an ActiveGate or the cluster from OneAgent's point of view**; the load-balancer guidance allows
only a TLS or TCP pass-through.

**Egress**: OpenPipeline forwarding writes gzip NDJSON (`.json.gz`) to S3, Azure Blob, or GCS
through a Dynatrace connector, about once a minute or at 16 MB, from any pipeline scope, before or
after processing; GA 2026-06-17, billed as data egress. No HTTP, OTLP, syslog, or streaming
destination is documented (UNVERIFIED as a negative). Everything else is pull: the metrics query
API, and DQL at `POST /platform/storage/query/v1/query:execute` plus `query:poll`.

### Senders

| Sender | Wire | Default endpoint | Mapping | Batching and retry |
|---|---|---|---|---|
| Telegraf `outputs.dynatrace` (1.16+) | metrics line | `http://localhost:14499/metrics/ingest` (README: `127.0.0.1`); a token is required for any other URL | name `<measurement>.<field>`; **everything is a gauge** unless listed in `additional_counters`/`additional_counters_patterns` (`count,delta`); bool → 0/1, strings dropped; tags → dimensions plus `default_dimensions` and `dt.metrics.source=telegraf`; `le`/`gt`/`quantile` tags dropped; `User-Agent: telegraf` | 1,000 lines per request; 200/202/400 treated as success; no retry beyond Telegraf's output buffer |
| Micrometer `DynatraceMeterRegistry` v2 | metrics line plus `#` metadata (1.12+) | the URI from the Operator's `endpoint.properties`, else the OneAgent URL; no `Authorization` header for the OneAgent URL | Counter and FunctionCounter → `count,delta`; Gauge → `gauge`; Timer, DistributionSummary, LongTaskTimer, FunctionTimer → `gauge,min,max,sum,count` (**no buckets or percentiles**); NaN/Inf and over-long lines dropped; `dt.metrics.source=micrometer`; `enrichWithDynatraceMetadata`, `useDynatraceSummaryInstruments`, `exportMeterMetadata` default true; `User-Agent: micrometer` | `min(batchSize, 1000)`; expects 202 and parses `linesOk`/`linesInvalid`; **drops on any other status** |
| `dynatrace-metric-utils-{go,java}` | line serializer only (no HTTP client in Go) | `localhost:14499/metrics/ingest` constant | counter delta, gauge, summary; the normalization rules in the metrics row above | 1,000-line constant. The Python, .NET, and JS libraries are archived |
| Logstash `logstash-output-dynatrace` (7.6+) | Log API JSON array | `ingest_endpoint_url` (required) | `event.to_hash` as-is (`message`/`@timestamp` rely on the server's key lists) | byte-bounded batches, `max_payload_size` default 9,500,000; oversized events dropped; gzip opt-in (`http_compression`); retries 429, 500, 502, 503, 504, and connection errors, up to 3 attempts with jittered backoff; 200 logged as partial |
| Fluentd `fluent-plugin-dynatrace` | Log API JSON array | `active_gate_url` (required) | records as-is; optional `@timestamp` in ms | any non-2xx raises and Fluentd retries; recommends `flush_thread_count 1` |
| Fluent Bit | generic `http` output (no dedicated plugin) | `{env}.live.dynatrace.com:443/api/v2/logs/ingest` | `format json`, `json_date_key timestamp`, `json_date_format iso8601`, `Api-Token` header | Fluent Bit's `Retry_Limit`; gzip via `compress gzip` (UNVERIFIED as recommended) |
| Dynatrace OpenTelemetry Collector (v0.56.0, 2026-09-08) | OTLP/HTTP | `${env:DT_ENDPOINT}` (cluster or ActiveGate) | a stock Collector build: receivers `otlp`, `filelog`, `fluentforward`, `hostmetrics`, `jaeger`, `journald`, `netflow`, `prometheus`, `statsd`, `syslog`, `zipkin`, the k8s set, `kafka`; processors including `cumulativetodelta` and `resourcedetection`; exporters `otlp`, `otlphttp`, `loadbalancing`, `kafka`, `debug`; nothing non-standard on the data wire (the `eec://` confmap provider is a config-plane pull "not intended for direct customer use") | `otlphttp`'s `retry_on_failure`/`sending_queue` |
| OpenTelemetry Collector contrib `dynatraceexporter`, and the per-language Dynatrace metric exporters | metrics line | — | removed in contrib v0.99.0 (2024-04-11); unsupported since the end of 2023; the migration path is OTLP | — |
| Vector | **no Dynatrace sink** | — | use its `http` or OTLP sink | — |
| Prometheus | Dynatrace pulls: the Kubernetes annotation scrape (`metrics.dynatrace.com/scrape`, `port`, `path`, `secure`, `filter`; classic limits 1,000 pods, 1,000 metrics per pod, 500,000 points per pod; counter → COUNT delta, gauge → GAUGE, histogram → COUNT bucket/sum/count, summary → quantile gauges plus COUNT) and the Extensions 2.0 Prometheus data source (1.225+, text, OpenMetrics, protobuf; 50 dimensions and 1,000 metric definitions per extension) | | | no remote-write receiver anywhere (UNVERIFIED as an explicit negative) |

### Unverified, to be settled by W1

1. Gzip, 413, and `Retry-After` on the metrics endpoint.
2. The cluster's lines-per-request cap (1,000, or "no limit").
3. Server acceptance of a negative `count,delta`.
4. The 50,000-char line limit; 250 vs 255 for metric keys and dimension values; 2 vs 3 as the
   key minimum.
5. The metrics response schema: `linesOk` (cluster page) vs `linesValid` (OneAgent page).
6. OTLP exemplar storage; the default span attribute-capture preference; the exact success body
   of an OTLP export.
7. The dimension keys OneAgent adds on `/metrics/ingest`, and whether `/v2/logs/ingest`,
   `/v2/events/ingest`, and `/otlp/v1/traces` enrich at all.
8. DynatraceStatsD's type mapping and its support for `|@`, `_e{}`, and `_sc`.
9. `dynatrace_ingest`'s route on `:14499`.
10. Events and bizevents through an ActiveGate; the `/platform/*` paths on an ActiveGate.
11. The CloudEvents batch content type.
12. FedRAMP hosts.
13. Gzip and `Retry-After` on the Log API; which log keys get semantic handling.
14. `prometheus_out` scraped by the annotation scrape end to end, if an ActiveGate is cheap
    to run.
15. Which `Value` types survive under the trial's pipeline (OpenPipeline keeps them all;
    classic stringifies).

## Dynatrace's data against `Event`

The event model carries every Dynatrace field in a typed field or a `dynatrace.*` attribute,
with one gap that a metric kind closes.

| Dynatrace concept | `Event` representation | Verdict |
|---|---|---|
| `count,delta=<v>` | `Sum { temporality: Delta, monotonic: true }` (`monotonic: false` if W1 shows negatives accepted) | lossless |
| `gauge,<v>` | `Gauge` | lossless |
| `gauge,min=,max=,sum=,count=` | the four-field summary kind: `Timeslice` with `exclusive` and `sum_of_squares` unset (`nr/w1`), or the four-field form `dt/w1` lands. No existing kind fits: `Summary` carries quantiles, `count`, and `sum` but no `min`/`max`; `Histogram` carries `sum`/`min`/`max` but counts only per bucket | **gap until the kind lands** (§1) |
| timestamp in ms, or absent | `Event::timestamp` in ns; absent = receipt time, re-encoded with a value | permitted normalization |
| dimensions (always strings; `dt.entity.*`, `dt.metrics.source`, …) | attributes verbatim as `Str`; a typed attribute encodes as its string form | type erasure is a permitted normalization (the DogStatsD precedent) |
| `#` metadata: `unit`, `description`, `displayName` | `MetricRecord.unit` and `description`; `dynatrace.meta.display_name` attribute. The decoder attaches a metadata line to every record of that key in the request; the encoder emits one metadata line per key that carries any of the three | lossless |
| Log API record: `content` and its aliases, timestamp and level aliases, other keys | `LogRecord.message` from the first content key; `Event::timestamp` from the first timestamp key; `Severity` from the first level key, with that key kept verbatim as an attribute; every other key verbatim with its JSON type and nesting (`Value::Map`/`Value::Array`) | lossless; alias keys re-encode under Dynatrace's own names (`content`, `timestamp`), the rename Dynatrace applies on ingest |
| Log `trace_id`/`span_id` (hex) | `TraceRef` (32- and 16-hex ids, which `TraceRef` accepts) | lossless |
| `text/plain` log body | one log event, `Str` body | lossless |
| OTLP logs, metrics, traces | already `otlp_in -> otlp_out` | — |
| Davis event | a log event: `title` as the body, `LogRecord.event_name` = `eventType`, `dynatrace.event.type`, `dynatrace.event.start_time`, `dynatrace.event.end_time`, `dynatrace.event.timeout`, `dynatrace.event.entity_selector`; `properties` verbatim as attributes | lossless; `dynatrace_out` sends any event carrying `dynatrace.event.type` to the events API and never to the Log API |
| business event, JSON form | a log event with an empty body, every field verbatim, `dynatrace.bizevent: true`; `event.type` also as `LogRecord.event_name` | lossless |
| business event, CloudEvents form | as the JSON form after Dynatrace's own mapping (`source` → `event.provider`, `type` → `event.type`, `id` → `event.id`, `data` lifted), plus `dynatrace.cloudevent.specversion` | CloudEvents → plain JSON is a permitted normalization: Dynatrace stores the same record either way |
| ingest responses: `linesInvalid`, partial 200 and 400 | `dynatrace_in` mirrors them; `dynatrace_out` reads `linesInvalid`/`errors[]` and counts `rejected` | — |
| windows: 1 h/10 min (metrics), 24 h/10 min (logs), 6 h/5 min or 30 d/7 d (events) | ns in the model; `dynatrace_out` drops and counts `stale` | permitted normalization plus a counter |
| key and dimension limits, lowercase dimension keys, the `dt.` reserved prefix | sanitizer: lowercase keys, truncate, skip and count a `dt.`-prefixed metric name | permitted normalization |
| OneAgent's host ID and host name dimensions | attributes like any other; nothing in `logit` mints them | a stand-in on `:14499` loses them (§10) |

## Direct, through OneAgent, or a stand-in: trade-offs and best practice

**OTLP, direct (`otlp_out`).** For: the only trace path; Dynatrace's recommended path; the only
public route to a histogram with buckets (under advanced dimensions); protobuf and gzip;
`otlp_out` exists. Against: metrics must be delta, so a cumulative source needs `delta` upstream
(or `aggregate`'s default delta window); a `Distribution` or `Samples` goes out as a `Summary`
today, which Dynatrace rejects outright, so it needs `nr/w3`'s `distributions: histogram`; the
per-signal caps need `nr/w3`'s `max_request_bytes`; resource and scope attributes reach
dimensions only through the allow list in default mode; no events of either kind.

**The ingest APIs, direct (`dynatrace_out`).** For: every line-protocol dimension is stored, no
allow list; `#` metadata lines; four-field summaries as a first-class payload; the Log API is the
same store as OTLP logs; Davis events and business events have no other carrier; text and JSON
are inspectable. Against: no histogram of any shape; 1,000-line batches; a stale point fails
per-line, not per-request, which the sink must read back.

**Through a local OneAgent.** For: host-entity enrichment for free; no token; a fixed loopback
port that libraries default to. Against: loopback only; no compression; not in containers;
traces only on the OTLP path, no OTLP metrics or logs; no business events; OneAgent must stay
installed.

**Best practice, to Dynatrace.** Traces: `otlp_out`, always. Metrics: `otlp_out` after `delta`
when histogram buckets matter and advanced dimensions are on, else `dynatrace_out`. Logs:
either; `dynatrace_out` when the Log API's per-record responses are wanted. Davis and business
events: `dynatrace_out`. On a host with OneAgent, `dynatrace_out` with `api: oneagent` for the
enrichment, and `otlp_out` to `localhost:14499/otlp/v1/traces` with `compression: none`. Don't
send one signal both ways.

**Best practice, from Dynatrace.**

1. Redirect: point each sender's endpoint setting at `dynatrace_in` (Telegraf `url`, Micrometer
   `uri`, Logstash `ingest_endpoint_url`, Fluentd `active_gate_url`, Fluent Bit `host`, the
   Collector's `DT_ENDPOINT`). No application change; reversible per sender. Senders have no
   dual-shipping of their own (the Collector and Telegraf can run two exporters), so the tee
   happens inside `logit`.
2. Tee: fan `dynatrace_in` out to the new backend beside a `dynatrace_out` or `otlp_out` leg to
   Dynatrace, and compare.
3. Cut over: drop the Dynatrace legs.

OneAgent-collected data (its own host, process, and log-module telemetry) has no interception
point and stays with OneAgent until Dynatrace goes. Bind `dynatrace_in` on `127.0.0.1:14499` only
where an application's default endpoint is being kept and OneAgent is gone; the lost host ID and
host name dimensions come back through an upstream `set` (or the host-file reader in §10).

## Design

Each item is a decision the ADR records, and names the workstream that builds it.

### 1. One model dependency: the four-field summary kind (W1)

Dynatrace's `gauge,min,max,sum,count` and New Relic's Metric API `summary` are the same
four-number aggregate, and the New Relic plan lands it as `MetricKind::Timeslice` (six fields,
two of them unset for this form). `dt/w1` takes that kind if `nr/w1` has landed; otherwise
`dt/w1` lands the four-field form with `type_sizes.rs`, `allocations.rs`, `memory.md`, and the
native wire's record encoding in one commit, and the New Relic stream extends it. One kind,
whichever stream is first. Every other Dynatrace field already has a home.

### 2. One runtime dependency: a `delta` transform (outside this stream)

Dynatrace rejects a cumulative monotonic `Sum` and doesn't ingest a cumulative `Histogram`, and
nothing in `logit` converts cumulative to delta: `aggregate`'s `opener_for` lets both kinds
through untouched ([ADR `aggregation-window-semantics`](../adr/aggregation-window-semantics.md)'s
cumulative amendment goes the other way). Any pipeline that relays `prometheus_in`, `otlp_in`
with cumulative producers, or `internal -> aggregate (temporality: cumulative)` into Dynatrace
needs a stateful per-series cumulative-to-delta stage with reset detection and a staleness bound,
the Collector's `cumulativetodelta` processor being the precedent. That transform gets its own
ADR and PR outside the `dt` stream, because `datadog_out` (which skips a cumulative `Sum`) and
New Relic's OTLP path (which drops cumulative state after 5 min) want it as well. This plan records
the requirement and the dependency; W4 and W5 name the transform in their `needs_delta`
diagnostic, and W6's end-to-end runs need it landed.

### 3. One new lossless pair (W2)

`dynatrace_in -> dynatrace_out` over the metrics line protocol, the Log API, Davis events, and
business events joins the like-protocol pairs in
[ADR `lossless-transit`](../adr/lossless-transit.md) by amendment, numbered after whichever of
the Datadog and New Relic pairs land first. The OTLP legs are already `otlp_in -> otlp_out`.
The codecs live in `crates/logit-proto/src/dynatrace/`, one module per payload family whose doc
is the mapping table, as the collectd codec does. The amendment lands with the ADR.

### 4. Kinds and config (W3, W4)

- `dynatrace_in` (W3): `bind:`, `bind_tls:`, and an optional `api_tokens:` allowlist (empty =
  accept any) matched against `Authorization: Api-Token` and `Authorization: Bearer`. Three path
  families on one listener, because senders differ in which they use: the environment form
  (`/api/v2/metrics/ingest`, `/api/v2/logs/ingest`, `/api/v2/events/ingest`,
  `/api/v2/bizevents/ingest`, `/api/v2/otlp/v1/{traces,metrics,logs}`), the ActiveGate and
  Managed form (the same under `/e/{any}/`), and the OneAgent form (`/metrics/ingest`,
  `/v2/logs/ingest`, `/v2/events/ingest`, `/otlp/v1/traces`). The OTLP routes decode with
  `logit_proto::otlp`'s existing decoder (protobuf; a JSON body is accepted too, a documented
  superset of what Dynatrace takes). Replies mirror Dynatrace's: 202 with the `linesOk` JSON
  (or `linesValid` on the OneAgent paths, per W1's answer to item 5), 204 for logs, 201 with
  `eventIngestResults` for events, 202 for business events, an `Export*ServiceResponse` for
  OTLP. Metrics query, entity, and `/platform/*` routes answer 404 and are counted
  `skipped{route}`. Decompresses gzip. Per-route caps are constants, `prometheus_in`'s
  `MAX_REQUEST_BYTES` precedent: 1 MB metrics, 10 MB logs, 8 MB, 4 MB, and 10 MB for the OTLP
  signals, 5 MB business events. A full pipeline answers 429 or 503, which Logstash, Fluentd,
  Fluent Bit, and the Collector retry, never 200-and-drop; Micrometer and Telegraf drop or
  buffer on a non-202 the same way they would against OneAgent.
- `dynatrace_out` (W4): `endpoint:` (a base URL: `https://{env}.live.dynatrace.com`,
  `https://{ag}:9999/e/{env}`, or `http://127.0.0.1:14499`), `api: environment | oneagent`
  (default `environment`) selecting the path table, `api_token: !env DT_API_TOKEN` (required
  under `environment`, rejected under `oneagent`, a graph rule), `compression: gzip | none`
  (default `gzip` for the Log API and events under `environment`, `none` under `oneagent`;
  metrics per W1's answer to item 1), `timeout`, `tls`. One request per API per batch, split at
  1,000 lines and 1 MB for metrics, 50,000 records and 10 MB for logs, 5 MB for business
  events, outside the three encoder shapes for the same reason `prometheus` is: several
  endpoints per signal. Business events under `oneagent` are skipped and counted. A 202 or 400
  with `linesInvalid > 0` is success with rejections, counted `rejected`, not retried; 429 and
  5xx retry through `write_loop`. `duplicate_safe()` is false: nothing dedupes a resent line.
  Host, entity, and source dimensions come from attributes and the resource (an upstream `set`),
  not from per-sink fields. Metric kinds: delta monotonic `Sum` → `count,delta`; `Gauge` →
  `gauge`; the four-field kind → the summary payload; `Samples`, `Distribution`, `Histogram`,
  and `ExponentialHistogram` → a summary of their min/max/sum/count, counted `degraded`;
  cumulative `Sum` and cumulative `Histogram` → skipped, counted `needs_delta`; `GaugeDelta`,
  `Summary` (no min/max), `Set`, `SetMembers` skipped and counted, the `statsd_out` pattern.
  Traces never go through `dynatrace_out`; a batch carrying only a span is skipped and counted
  `unsupported_signal` with a diagnostic naming `otlp_out`.
- Graph rules take the next free numbers (rule 61 is the highest today; the Datadog and New
  Relic streams claim numbers too), following rules 55 and 56's shape for mode and endpoint
  validation.

### 5. `otlp_out` for Dynatrace (W5)

- Verified against the trial: the `Authorization` header through `headers:`, protobuf-only and
  the absence of gRPC, each signal's cap, what the success body is, and the rejection of a
  cumulative `Sum` and of a `Summary`.
- `max_request_bytes:` and `distributions: summary | histogram` are `nr/w3`'s features;
  `dt/w5` stacks on `nr/w3` if it hasn't merged, and sets the recommended values for Dynatrace
  (8 MB, 4 MB, 10 MB by signal, or 15,000 points, whichever W1 shows binds first).
- A `Summary` bound for Dynatrace is skipped and counted, or degraded to a bucketless
  `Histogram`; W1 decides which Dynatrace accepts.
- `compression: none` is the setting for `localhost:14499/otlp/v1/traces`, and `otlp_out`'s
  `paths:` already lets the operator name that path.

### 6. Reuse (W3–W5)

`crates/logit-inputs/src/http.rs` (connection plumbing, idle tracking, bounded body reads) with
per-listener path dispatch the way `otlp_in` does it; `logit-outputs::http` (`build_client`,
`read_body_prefix`, `body_snippet`, `is_retryable_http_status`, `classify_reqwest_error`);
`write_loop`'s bounded retry with `Fault` classification; `TlsClientConfig`/`TlsServerConfig`;
`influxdb.rs`'s `push_float`, `push_i64`, `push_uint`, and `push_escaped` line helpers, promoted
to a shared module for the second line-protocol writer; graph rules 55 and 56.

### 7. Attribute vocabulary (W2)

`dynatrace.*` for raw encodings: `dynatrace.meta.display_name`, `dynatrace.event.*` (type,
start and end time, timeout, entity selector), `dynatrace.bizevent`,
`dynatrace.cloudevent.specversion`. Dynatrace's own names otherwise: `dt.entity.*`,
`dt.metrics.source`, `host.name`, `trace_id` and `span_id` (decoded into `TraceRef`), and the
level key the sender used (`severity`, `status`, `loglevel`, …) kept verbatim beside `Severity`.
`dynatrace_out` derives the rest: `timestamp` in ms from `Event::timestamp`, `content` from the
body (a `Map` body serialized as JSON), `trace_id`/`span_id` as hex from `TraceRef`, dimension
keys lowercased.

### 8. Timestamp windows and stale data (W4)

`dynatrace_out` drops and counts `dropped{reason="stale"}` a metric older than 1 h or more than
10 min ahead, a log older than 24 h, and an event outside its type's window, instead of letting
one stale record fail a request or a line. The consequence for `buffer.disk:` replay after a long
outage (the 1 h metric window is the tightest of any vendor here) is documented with the
component.

### 9. Ordering (all)

W1 first: the spike closes the UNVERIFIED list, the ADR records the pair and the kind
dependency, and the kind lands if `nr/w1` hasn't. Then the codecs and pair (W2, W3, W4), then
`otlp_out` (W5), fixtures and environment verification (W6), and docs (W7). W5 depends only on
W1 and the two cross-stream items and can be pulled forward; the `delta` transform lands outside
the stream whenever it's ready, and W6 is the first workstream that runs without it only by
sourcing delta data.

### 10. Not in this stack

An object-storage input for OpenPipeline forwarding's NDJSON drops; a reader of
`/var/lib/dynatrace/enrichment/dt_host_metadata.properties` that restores OneAgent's host
dimensions on a stand-in (a `set`-shaped source, sketched in `docs/dynatrace.md` as a
follow-up); any change to `statsd_in` for DynatraceStatsD unless W1 finds a grammar difference;
Prometheus annotation-scrape verification beyond W1's one check; the Platform generic-event
endpoints.

## Workstreams

| # | PR | Size | Depends on |
|---|---|---|---|
| W0 | This plan and its index row | S | — |
| W1 | Spike against the trial: curl each API; `otlp_out` as it is (header, caps, success body, cumulative and `Summary` rejection, histogram treatment under both dimension modes); gzip, 413, negative delta, and the line cap on the metrics endpoint; OneAgent on a throwaway VM (`:14499` paths and their responses, enrichment keys, DynatraceStatsD's flavor against `statsd_in`'s decoder, `dynatrace_ingest`'s route); `prometheus_out` under an annotation scrape if an ActiveGate is cheap. ADR `dynatrace-api-and-oneagent-relay`; `lossless-transit` amendment; the four-field kind with tripwires and `memory.md` if `nr/w1` hasn't landed; every UNVERIFIED item resolved and this plan updated | M | W0 |
| W2 | `logit_proto::dynatrace` codecs: metrics line protocol with metadata lines, Log API (JSON, JSONL, text), Davis events, business events (JSON and CloudEvents); fixed-point suites | M | W1 |
| W3 | `dynatrace_in`: receiver, token allowlist, three path families, OTLP routes, graph rules, schema | M | W2 |
| W4 | `dynatrace_out`: `api:` modes, request splitting, stale filter, sanitizer, response accounting, graph rules, schema | M | W3 |
| W5 | `otlp_out` verified for Dynatrace: header, caps, `compression: none` for the OneAgent path, the `Summary` policy; reuse of `nr/w3`'s splitting and histogram export | S | W1, `nr/w3`, `delta` |
| W6 | Recorded fixtures via `script/record-fixtures` (Telegraf, a Micrometer application, Logstash, Fluentd, the Dynatrace Collector distribution with `DT_ENDPOINT` at the capture, `dynatrace_ingest`); pair fixed-point tests over the corpus; trial end-to-end for `dynatrace_out` and `otlp_out` (metrics, summaries, logs, events, spans visible, a rejected line accounted) | M | W4, W5 |
| W7 | `docs/dynatrace.md` (operator best practices from this plan, including that traces reach Dynatrace only through `otlp_out` and that a `:14499` stand-in loses OneAgent's host dimensions); `deploying.md`; `known-gaps.md` rows; `AGENTS.md` tables; `telemetry-landscape.md` cells; four examples (`dynatrace-otlp.yaml`, `dynatrace-direct.yaml`, `dynatrace-via-oneagent.yaml`, `dynatrace-intake-standin.yaml`) and `DT_API_TOKEN` in `every_shipped_config_loads_and_validates`'s `!env` map (`crates/logit-cli/src/config.rs`) | M | W6 |

Landing order: W0 → W1 → W2 → W3 → W4 → W5 → W6 → W7, linear; W5 stacks after W4 to keep
the stack linear even though it depends only on W1. Each PR is based on and targets its parent's
branch and is brought up to date with `git merge origin/main`, never a rebase.

## Verification

- Per PR: `script/cibuild` green; `script/schema` regenerated and committed by any workstream
  that changes a config type; `script/validate` for any that adds a config.
- W1: each UNVERIFIED item has a recorded answer (the curl, the response, the environment's
  view) and the plan text is updated; if the kind lands here, `type_sizes.rs`, `allocations.rs`,
  and `memory.md` change in the same commit.
- W2: `decode -> encode -> decode` fixed point over every wire feature listed above, per
  `lossless-transit`'s rule, including a metadata line, a bare-value gauge, a `text/plain` log,
  and a CloudEvents batch.
- W3/W4: Telegraf, Micrometer, Logstash, and the Dynatrace Collector deliver to `dynatrace_in`
  on both the environment and OneAgent path forms and accept its replies; the trial shows
  `dynatrace_out`'s metrics (with unit and description from metadata lines), summaries, logs,
  Davis events, and business events, and a request with one invalid line is counted `rejected`
  with the rest stored.
- W5: an `otlp_out` export over the cap reaches the environment in N requests with nothing
  dropped; a `Distribution` appears as a histogram with percentiles under advanced dimensions;
  a cumulative `Sum` routed through `delta` is stored as a counter.
- W6: the pair test holds over the recorded corpus.
- W0 (this PR) is documentation only: every relative link resolves and `docs/plans/README.md`
  gained a row.
