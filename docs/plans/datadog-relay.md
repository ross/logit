---
created: 2026-09-23
updated: 2026-09-24
---

# Enabling plan: Datadog — direct API, Agent stand-in, intake stand-in

## Context

`logit` has no Datadog-specific component. The only Datadog wire it speaks is DogStatsD
(`statsd_in`, and `statsd_out` under `format: dogstatsd`), and `otlp_out` can reach Datadog's OTLP
intake, in an Agent or agentless, with a `headers:` block nobody has verified end to end. This
plan records what Datadog accepts and emits, where the event model and the existing components
fall short of it, which way to send data and when, and the workstreams that close the gaps.

Goals:

- Send logs, metrics, traces, events, and service checks to Datadog, directly and through a local
  Datadog Agent, with a best-practice recommendation for each situation.
- Receive what Datadog-instrumented systems produce, losslessly, in both places `logit` can stand:
  in front of the apps, imitating an Agent's listening surfaces (DogStatsD, the APM API on `:8126`,
  OTLP); and behind a fleet of Agents, imitating Datadog's intake (`dd_url` and
  `additional_endpoints` dual-shipping).
- Make migrations to and from Datadog a configuration change, not an application change.

Non-goals: Datadog's Observability Pipelines Worker protocol beyond what Agents already send;
process, orchestrator, and profiling payloads; Datadog's v3 columnar series format (Agents send it
only to Datadog URLs; a relay under any other URL receives v2); Remote Configuration and
telemetry proxying (acknowledged, counted, not forwarded).

Stream key **`dd`**: branches `dd/w0`…`dd/w8`, stacked as the workstream table says. PR stack
only: nothing is merged by this workstream; Ross directs merging. The decisions are recorded in
[ADR `datadog-agent-and-intake-relay`](../adr/datadog-agent-and-intake-relay.md) (W1), written
once the sketch spike settled the one decision the codec depends on; the design sections below
are the build-out of that record.

Settled with Ross (2026-09-23): both stand-in directions are in scope; receive side first, because
the decoders fix the attribute vocabulary the encoders mirror; the Agent-API pair is
`datadog_trace_in`/`datadog_trace_out`; Datadog events and service checks reuse the
`statsd.event.*`/`statsd.service_check.*` attribute names; `datadog_in` decodes zstd with a
pure-Rust decoder; a Datadog trial org is assumed available for W7's end-to-end verification.

## Coverage by signal

Datadog ingests five kinds of data. Each cell reads today → after this stack.

| Signal | Direct API, no Agent | Through a local Agent | Agent stand-in (receive from apps) | Intake stand-in (receive from Agents) |
|---|---|---|---|---|
| Logs | `otlp_out` agentless (unverified) → `datadog_out` `/api/v2/logs` (W5) | `otlp_out` to the Agent's OTLP receiver (its `logs.enabled` defaults to false), or `syslog_out` over TCP to the Agent's `logs` TCP listener, which accepts syslog-formatted lines; `syslog_out` writes a `Str` body verbatim, but whether the Agent then parses a JSON body into attributes is UNVERIFIED | `syslog_in`; apps that write JSON lines to an Agent TCP port have no plain-lines listener (documented gap, §7) | none → `datadog_in` `/api/v2/logs` and legacy `/v1/input` (W3) |
| Metrics | `otlp_out` agentless, delta only → `datadog_out` `/api/v2/series` + `/api/v1/distribution_points` + sketches (W5, §4) | `statsd_out format: dogstatsd`, over UDP or either Agent Unix socket (W4b); `otlp_out` | `statsd_in` over UDP or either Agent Unix socket, `\|e:`/`\|card:` carried (W4b); `otlp_in` | none → `datadog_in` series v1/v2 + sketches (W3) |
| Traces | `otlp_out` agentless, lossy for Datadog-origin spans → `datadog_out` `/api/v0.2/traces` + `/api/v0.2/stats`, the Agent's own protocol (W5, §12); `otlp_out` stays the path for OTel-origin spans | `otlp_out` → also `datadog_trace_out` msgpack `/v0.4/traces` + `/v0.6/stats` (W6) | `otlp_in` (dd-trace OTLP export is Preview) → `datadog_trace_in` on `:8126`, traces and client stats (W4a) | none → `datadog_in` `AgentPayload` + `StatsPayload` (W3) |
| Events | none → `datadog_out` `/api/v1/events` (W5) | `statsd_out` `_e{}` | `statsd_in` `_e{}` | none → `datadog_in` `/intake/` (W3) |
| Service checks | none → `datadog_out` `/api/v1/check_run` (W5) | `statsd_out` `_sc` | `statsd_in` `_sc` | none → `datadog_in` `/api/v1/check_run` (W3) |

## What Datadog accepts and emits

Surveyed 2026-09-23 from Datadog's OpenAPI specs
(`github.com/DataDog/datadog-api-client-go/.generator/schemas/{v1,v2}/openapi.yaml`), the
`datadog-agent` and `agent-payload` sources on `main`, and `docs.datadoghq.com`. Items marked
UNVERIFIED were not confirmed by a current official page; W7 verifies each against the trial org
or a real Agent and this section is updated then.

### Public intake API (no Agent)

| Endpoint | Body and limits | Notes |
|---|---|---|
| `POST api.<site>/api/v2/series` | JSON `{series:[{metric, type, points:[{timestamp s, value}], interval, unit, tags[], resources:[{name,type}], source_type_name, metadata.origin}]}`; 512,000 B compressed, 5,242,880 B decompressed; `Content-Encoding: deflate \| zstd1 \| gzip`; header `DD-API-KEY` | `type`: 0 unspecified, 1 count, 2 rate, 3 gauge. No histogram, set, or distribution type. Points must be no more than 1 h in the past or 10 min in the future |
| `POST /api/v1/distribution_points` | JSON `{series:[{metric, host, tags, type:"distribution", points:[[ts,[v…]]]}]}`; `deflate` only | Raw values; Datadog sketches them server side. Limits undocumented (UNVERIFIED) |
| `POST /api/beta/sketches` | protobuf `SketchPayload` | What Agents send. Not in the public spec; Vector's `datadog_metrics` sink sends it with an API key (UNVERIFIED as supported for third parties) |
| `POST http-intake.logs.<site>/api/v2/logs` | JSON array of `{message, ddsource, ddtags, hostname, service, status, …}`; 1,000 entries, 5 MB decompressed, 1 MB per log (cut, still 2xx); up to 18 h in the past; `gzip`/`deflate`/`identity`; 202 accepted, retry 408/429/500/503 | Other keys are attributes, nested maps included. Trace correlation auto-detects OTel `trace_id`/`span_id` (32-/16-char lowercase hex) and Datadog `dd.trace_id`/`dd.span_id` (decimal) |
| `POST /api/v1/events` | `title`, `text` (≤4,000 chars), `date_happened` (≤18 h old), `priority: normal \| low`, `alert_type`, `aggregation_key` (≤100), `host`, `tags`, `source_type_name` | v2 events (`event-management-intake.<site>`) are a different product surface; v1 is what DogStatsD events map to |
| `POST /api/v1/check_run` | array of `{check, host_name, status 0–3, tags, message?, timestamp?}`; ≤10 min old; message cut at 500 chars, discarded on OK | |
| Traces | **No documented native-span API**, but the Agent's own outbound protocol is open source (`pkg/trace/writer`) and Vector's `datadog_traces` sink is a working third-party sender of it: see the Agent outbound table below | Trace metrics (`trace.*`, service pages) are not derived from `/api/v0.2/traces`; the sender must also send `/api/v0.2/stats` (inferred from `metrics_namespace` docs and the Agent's concentrator; the backend is closed source) |
| `POST otlp.<site>/v1/{traces,metrics,logs}` | OTLP/HTTP protobuf (traces and metrics also JSON); header `dd-api-key`; 512 KiB compressed metrics, 5.1 MiB logs, 15 MiB traces; 413 when over | HTTP only, no gRPC. Metrics must be delta temporality; cumulative is an error. Optional `dd-otel-metric-config` (`histograms.mode`, `summaries.mode`, `resource_attributes_as_tags`) and `dd-otel-span-mapping` headers; `compute_stats=true` to get trace metrics. No Preview banner on any of the three pages, no explicit GA statement either |

Sites: `datadoghq.com`, `datadoghq.eu`, `us3.datadoghq.com`, `us5.datadoghq.com`,
`ap1.datadoghq.com`, `ap2.datadoghq.com`, `uk1.datadoghq.com`, `ddog-gov.com`,
`us2.ddog-gov.com`; every host above is `<prefix>.<site>`.

### Agent listening surfaces (what apps send to an Agent)

- **DogStatsD**: UDP `:8125`, a datagram Unix socket at `dogstatsd_socket`
  (default `/var/run/datadog/dsd.socket`; what Kubernetes clients use), and an optional stream
  Unix socket (`dogstatsd_stream_socket`; a 4-byte little-endian length per packet, UNVERIFIED).
  Buffer 8,192 B. Grammar as
  [`telemetry-landscape.md`](../design/telemetry-landscape.md)'s DogStatsD section, plus two
  suffixes `statsd_in` carries since W4b: `|e:<external data>` (v1.5, Agent 7.57+) and
  `|card:none|low|orchestrator|high` (v1.6, 7.64+). Agent semantics: `c` is time-normalized
  into a rate over the 10 s flush; `s` becomes a gauge of distinct values; `h`/`ms` become
  `.avg`/`.count`/`.median`/`.95percentile`/`.max` per `histogram_aggregates` and
  `histogram_percentiles`; `d` goes to Datadog as a sketch.
- **APM on `:8126`** (`pkg/trace/api/endpoints.go`): `/v0.3/traces` (msgpack or JSON),
  `/v0.4/traces` (msgpack, array of arrays of span maps), `/v0.5/traces` (msgpack, a string table
  plus 12-element span arrays), `/v0.7/traces` (msgpack `TracerPayload`, not protobuf),
  `/v1.0/traces` (msgpack, streaming string table), `/info`, `/v0.6/stats`,
  `/v0.1/pipeline_stats`, `/telemetry/proxy/`, `/v0.7/config`, `/evp_proxy/v1`–`v4`,
  `/profiling/v1/input`, `/debugger/*`, `/tracer_flare/v1`, and an optional
  `receiver_socket` (`/var/run/datadog/apm.socket`). Response `{"rate_by_service": {...}}`.
  Request headers: `X-Datadog-Trace-Count`, `Datadog-Meta-Lang`, `Datadog-Meta-Tracer-Version`,
  `Datadog-Container-ID`, `Datadog-Entity-ID`, `Datadog-Client-Computed-Stats`,
  `Datadog-Client-Computed-Top-Level`, `Datadog-Client-Dropped-P0-{Traces,Spans}`. Span schema
  (`span.proto`): `service`, `name`, `resource`, `type` strings; `trace_id`, `span_id`,
  `parent_id` uint64; `start`, `duration` int64 ns; `error` int32; `meta` string→string;
  `metrics` string→f64; `meta_struct` string→bytes; `span_links`; `span_events`. 128-bit trace
  ids carry their high 64 bits as 16 lowercase hex chars in `meta["_dd.p.tid"]` on the first
  span of a chunk. Limits: `service`/`name` ≤100 chars, `resource` ≤5,000, meta key ≤200, meta
  value ≤25,000.
- **OTLP receiver**: gRPC `:4317`, HTTP `:4318`; `metrics` and `traces` enabled, `logs`
  disabled by default. Metric options: `histograms.mode: distributions | counters | nobuckets`,
  `sums.cumulative_monotonic_mode: to_delta | raw_value`, `summaries.mode: gauges | noquantiles`,
  `resource_attributes_as_tags` (false), `instrumentation_scope_metadata_as_tags` (true). Span
  mapping honors the attributes `operation.name`, `resource.name`, `span.type`, and
  `service.name`; without them it derives operation and resource names from semconv.
- **dd-trace SDKs can export OTLP** (Preview): `DD_TRACE_OTEL_ENABLED=true` with
  `OTEL_TRACES_EXPORTER=otlp` (Java 1.62+, Python 4.8+, Node 5.98+, Go 2.8+, .NET 3.41+) and
  `DD_METRICS_OTEL_ENABLED=true`. Spans keep Datadog semantics.
- **Logs**: a `logs` integration `type: tcp | udp` listener taking raw, JSON, or syslog lines,
  one per `\n`. No HTTP logs endpoint other than OTLP (UNVERIFIED as a negative).

### Agent outbound surfaces (what Agents send to Datadog)

Redirected by `dd_url` (metrics, events, checks, metadata only), `logs_config.logs_dd_url`,
`apm_config.apm_dd_url`, or dual-shipped with `additional_endpoints`,
`logs_config.additional_endpoints`, and `apm_config.additional_endpoints`.

| Route | Body | Notes |
|---|---|---|
| `/api/v2/series` | protobuf `MetricPayload` (`agent-payload` `metrics/agent_payload.proto`); `Content-Type: application/x-protobuf` | **zstd level 1 by default** (`serializer_compressor_kind`), one setting for every endpoint. 512,000 B compressed, 5 MiB decompressed, 10,000 points. v1 JSON only if `use_v2_api.series: false`. v3 columnar (`/api/intake/metrics/v3/series`) only to Datadog URLs (`use_v3_api.series.enabled: datadog_only`) — how the Agent classifies a URL is UNVERIFIED |
| `/api/beta/sketches` | protobuf `SketchPayload`: `{metric, host, tags, dogsketches:[{ts, cnt, min, max, avg, sum, k: sint32[], n: uint32[]}]}` | DDSketch with `eps = 1/128` (gamma 1.015625), `min = 1e-9`, bias, 4,096-bin collapsing, int16 keys (`pkg/util/quantile`). No mapping parameters on the wire: the receiver assumes them |
| `/api/v1/check_run` | JSON | |
| `/intake/` | JSON: events and host metadata in one route | |
| `agent-http-intake.logs.<site>/api/v2/logs` | JSON array, gzip; ≤1 MB and ≤1,000 logs per batch; headers `DD-API-KEY`, `DD-PROTOCOL: agent-json`, `DD-EVP-ORIGIN: agent`, `dd-message-timestamp` | Legacy TCP `agent-intake.logs.<site>:10516`: `<api-key> <json>\n`, or 4-byte length-prefixed protobuf `Log` |
| `trace.agent.<site>/api/v0.2/traces` | protobuf `AgentPayload{hostName, env, tracerPayloads, tags, agentVersion, targetTPS, errorTPS, rareSamplerEnabled}`; headers `DD-Api-Key`, `Content-Type: application/x-protobuf`, `Content-Encoding` (zstd `BestSpeed` by default; gzip in other builds), `User-Agent: Datadog Trace Agent/<ver>/<commit>`, `X-Datadog-Reported-Languages`; 3,200,000 B uncompressed max; flush every 5 s; retries 408, 429, 5xx, and transport errors with full-jitter backoff, 4 retries (`pkg/trace/writer/{trace,sender}.go`) | Vector sends the same payload with `DD-API-KEY` and gzip only, empty `tags`, and its own `hostName`/`env`/`agentVersion` (`src/sinks/datadog/traces/request_builder.rs`) |
| `trace.agent.<site>/api/v0.2/stats` | msgpack + gzip `StatsPayload{agentHostname, agentEnv, agentVersion, clientComputed, stats: [ClientStatsPayload{hostname, env, version, containerID, lang, …, stats: [bucket{start, duration, stats: [group{service, name, resource, type, spanKind, HTTPStatusCode, synthetics, peerTags, hits, errors, duration, topLevelHits, okSummary, errorSummary}]}]}]}`; ≤4,000 groups per payload; 10 s buckets | `okSummary`/`errorSummary` are protobuf DDSketches from `sketches-go` (`LogCollapsingLowestDenseDDSketch(0.01, 2048)`, gamma 1.0202) and carry their mapping on the wire, unlike the metrics sketches. The Agent computes them from every span before sampling (`pkg/trace/stats/concentrator.go`): eligible spans are `_top_level == 1`, `_dd.measured == 1`, or server/client/producer/consumer `span.kind`; weight `1/_sample_rate` |
| `/api/v1/validate`, `/api/v2/host_metadata`, `/api/v1/metadata` | | Answered 200/204, contents dropped and counted |

Vector's `datadog_agent` source (`src/sources/datadog_agent`) implements the same set minus
`/api/v1/check_run` and `/intake/`, and is the cross-check for W2's decoders. Vector drops
`/api/v0.2/stats` on receipt and recomputes stats in its sink, a partial reimplementation of the
Agent's concentrator (no `span.kind`, peer tags, or gRPC/HTTP keys; "beta"; requires Agent
sampling to be off). `logit` relays the stats instead (§12).

What the Agent does to a span between the tracer and the intake (`pkg/trace/agent/agent.go`
`Process`), which an Agent stand-in that sends directly would have to reproduce (§14):
normalization (`service`/`name` ≤100 chars with `unnamed-service`/`unnamed_operation`
fallbacks, empty `resource` → `name`, `env` normalized, invalid `http.status_code` removed);
obfuscation by `span.type` (SQL, Redis, memcached, HTTP URLs, MongoDB, Elasticsearch) unless
the tracer sent `Datadog-Obfuscation-Version`; truncation (`resource` ≤5,000, meta values
≤25,000); `_top_level` marking (root, orphan, or service-boundary spans, or the tracer's
`_dd.top_level` when `Datadog-Client-Computed-Top-Level` is set); sampler tags (`_dd.agent_psr`,
`_dd.rule_psr`, chunk `_dd.p.dm`, `_dd1.sr.rcusr`); chunk `priority` and `origin` lifted from
the root span's `_sampling_priority_v1` and `_dd.origin`; and the stats concentrator. Tracers'
own `/v0.6/stats` client stats are normalized and forwarded under `/api/v0.2/stats`.

## Datadog's data against `Event`

The event model already carries every Datadog field either in a typed field or in a
`datadog.*` attribute, with two exceptions marked as gaps.

| Datadog concept | `Event` representation | Verdict |
|---|---|---|
| metric `count` with `interval` | `Sum { temporality: Delta, monotonic: true }` + `datadog.interval` | lossless |
| metric `rate` | `Gauge` + `datadog.type: rate` + `datadog.interval`. The value is per second; folding it into a `Sum` would multiply and round | lossless through `<proto>.*` attributes; a cross-protocol consumer sees the per-second gauge Datadog itself displays |
| metric `gauge` | `Gauge` | lossless |
| `resources` of type `host` | `host.name` attribute; any other resource type → `datadog.resources` array | lossless |
| `unit`, `source_type_name`, `metadata.origin` | `MetricRecord.unit`; `datadog.source_type_name`; `datadog.origin.*` | lossless |
| `dogsketch` bins | `Distribution(DdSketch)` under `Mapping::agent`, bin-for-bin (§4, landed in W1) | lossless |
| distribution points (raw values) | `Samples` | lossless |
| Agent-side `s`/`h`/`ms` semantics | `aggregate` plus a sink; nothing emits the Agent's `.avg`/`.count`/`.median`/`.95percentile`/`.max` set today | a documented recipe, not a codec gap (§7) |
| log `message`, `status`, `timestamp`, `hostname`, `service`, `ddsource`, `ddtags` | `LogRecord.message`; `severity` plus the raw `status`; `Event::timestamp`; the rest verbatim as attributes, `ddtags` `k:v` pairs expanded the way DogStatsD tags are | lossless |
| log `dd.trace_id`/`dd.span_id` (decimal, 64-bit, or a 128-bit hex trace id) | `TraceRef` via `trace_context` under `format: datadog`: a decimal id in the low 64 bits, high half from an optional `trace_id_high` (`_dd.p.tid`'s form) | lossless (§9, W8a) |
| span ids uint64 + `_dd.p.tid` | `[u8; 16]`/`[u8; 8]`, high bits from `_dd.p.tid`, re-emitted when nonzero | lossless |
| span `service`, `resource`, `type`, `name` | `service.name`, `resource.name`, `span.type` attributes (the names the Agent's own OTLP receiver honors); `SpanRecord.name` | lossless, and an OTLP egress to an Agent reconstructs them for free |
| span `error`, `meta`, `metrics`, `meta_struct`, `_sampling_priority_v1`, `_dd.*` | `status: Error`; attributes verbatim as `Str`/`F64`/`Bytes` | lossless |
| OTel-only span fields (`kind`, `status: Ok`, `trace_state`, typed attributes) | — | Datadog can't carry them; `datadog_trace_out` counts them, a row in `known-gaps.md`'s cross-protocol table |
| chunk `priority`, `origin`, `droppedTrace`, chunk `tags`; `TracerPayload` `languageName`, `languageVersion`, `tracerVersion`, `runtimeID`, `containerID`, `appVersion`, `tags`; `AgentPayload` `hostName`, `env`, `agentVersion`, `targetTPS`, `errorTPS`, `rareSamplerEnabled` | chunk fields as `datadog.chunk.*` attributes on every span of the chunk; tracer fields as `datadog.tracer.*` on the batch `Resource` (one batch per `TracerPayload`); Agent fields as `datadog.agent.*` on the `Resource` | lossless; a batch boundary per `TracerPayload` is the "batching" normalization |
| APM stats (`StatsPayload`, and tracers' `/v0.6/stats`) | one metric event per bucket group: `Sum` `datadog.stats.hits`/`errors`/`top_level_hits` (delta, weighted), `Distribution` `datadog.stats.ok_summary`/`error_summary` decoded from the on-wire DDSketch, group keys (`service`, `name`, `resource`, `span.type`, `span.kind`, `http.status_code`, `synthetics`, peer tags, …) and payload keys (`env`, `version`, `container.id`, `datadog.tracer.lang`, …) as attributes, `Event::timestamp` = bucket start, `datadog.stats.bucket.duration` | lossless: `DdSketch` keeps the wire's own mapping (`Mapping::logarithmic`, §4) |
| OTel-origin span through the native protocol | needs Datadog semantics synthesized: `service`/`resource`/`type`, `_top_level`, priority, and stats | not in this stack (§14); `otlp_out` carries OTel-origin spans |
| events, service checks | `statsd.event.*`, `statsd.service_check.*` (settled) | lossless |
| timestamps: metrics in seconds, 1 h/10 min window; logs 18 h; checks 10 min | ns in the model; `datadog_out` drops and counts `stale` before sending | permitted normalization plus a counter |
| name and tag limits: 200 chars, restricted charset, tags lowercased | sanitizer substitutions | permitted normalization |

## Agent or direct: trade-offs and best practice

**Through a local Agent.** For: host and container metadata, Live Containers, integrations, the
Agent's out-of-the-box tags, APM trace metrics and remote configuration, the Agent's own retry
and buffering, Unix-socket locality, Agent-side `h` aggregation and `d` sketches. Against: one
more process per host; OTLP logs off by default; no HTTP log intake on the Agent (TCP lines or
OTLP only); the Datadog dependency stays on every host for the whole migration.

**Direct.** For: no Agent to run, one hop, works wherever HTTPS does, and `logit` is already the
host collector. Against: the 1 h metric timestamp window, which rejects a disk-buffer replay
after a long outage; no `h` aggregates unless `aggregate` produces them; distributions only as
raw points or through the undocumented sketches route; the API payload limits; for traces,
either the native protocol (Datadog-origin spans, with their stats relayed) or OTLP
(OTel-origin spans, with `compute_stats=true` computing trace metrics from sampled spans
only, and with the losses in §12).

**Best practice, to Datadog.** Send directly (`datadog_out` for metrics, logs, events, checks,
and Datadog-origin traces with their stats; `otlp_out` agentless for OTel-origin traces) when
`logit` is already the collector on the host. Front an Agent when Datadog's host and APM
features are wanted, or when OTel-origin traces need Agent-side sampling and ingestion controls.
Don't send one signal both ways.

**Best practice, from Datadog.**

1. Dual-ship: add `logit`'s `datadog_in` to `additional_endpoints` (and the logs and APM
   equivalents). No app or host change, reversible in one Agent config edit.
2. Tee: fan `datadog_in` out to the new backend beside the Datadog leg, and compare.
3. Cut over: point `dd_url` at `logit` or remove the Agent.

Use the Agent stand-in (`statsd_in` + `datadog_trace_in` + `otlp_in` on the Agent's ports) only
where there's no Agent to redirect (containers without a sidecar, serverless) or in step 3 once
the Agent itself is going away.

## Design

Each item is a decision the ADR records, and names the workstream that builds it.

### 1. Two new lossless pairs (W2)

`datadog_in -> datadog_out` (the intake API) and `datadog_trace_in -> datadog_trace_out` (the
Agent's APM API) join the six pairs in [ADR `lossless-transit`](../adr/lossless-transit.md),
with `statsd_in -> statsd_out` already covering DogStatsD. Both codecs live in
`crates/logit-proto/src/datadog/`, one module per payload family whose doc is the mapping
table, as the collectd codec does. The amendment to `lossless-transit.md` lands with the ADR.

### 2. Kinds and config (W3–W6)

- `datadog_in` (W3): `bind:`, `tls:` (one mode, so `otlp_in`'s field name rather than
  `prometheus_in`'s mode-prefixed `bind_tls:`), `handshake_timeout:`, `idle_timeout:`, and an
  optional `api_keys:` allowlist (empty = accept any). Routes: `/api/v1/series`,
  `/api/v2/series`, `/api/v1/distribution_points`, `/api/beta/sketches`, `/api/v1/check_run`,
  `/api/v2/events`, `/intake/`, `/api/v2/logs`, `/v1/input`, `/api/v0.2/traces`,
  `/api/v0.2/stats` (decoded into stats events, not dropped: Vector drops and recomputes them,
  and that is the "beta" part of its sink); `/api/v1/validate` answers 200; host and inventory
  metadata and the process and orchestrator collectors are acknowledged and counted
  `acknowledged{route}`. An unknown route is a `404`, not an acknowledgement, so an Agent reports
  what the listener doesn't speak. Decompresses gzip, deflate, and zstd.
- `datadog_out` (W5): `api_key: !env DD_API_KEY`, `site` (default `datadoghq.com`), optional
  per-signal `endpoints:` overrides so a pair test can point at another `logit`'s `datadog_in`,
  `compression: gzip`, `timeout`, `tls`. One request per endpoint per batch, outside the three
  encoder shapes for the same reason `prometheus` is: several endpoints per signal.
  `duplicate_safe()` is false until W7 shows the intake dedupes a resent `(series, timestamp)`.
  Host, service, source, and tags come from attributes and the resource (an upstream `set`), not
  from per-sink fields. Metric kinds: delta monotonic `Sum` → `count` with `interval` from
  `datadog.interval` or the batch's `aggregate` window; `Gauge` → `gauge` (or `rate` when
  `datadog.type: rate`); `Samples` → `/api/v1/distribution_points`; `Distribution` → sketches
  (§4); `Set` → `gauge` of the estimate, the Agent's own `s` semantics. Cumulative or
  non-monotonic `Sum`, `GaugeDelta`, `Histogram`, `ExponentialHistogram`, `Summary`, and
  `SetMembers` are skipped and counted `dropped{reason="unsupported_kind"}`, the `statsd_out`
  pattern; `Histogram` as Datadog's `.bucket` counters is a follow-up.
- `datadog_in` request caps are constants sized to what the Agent sends, not config, matching
  `prometheus_in`'s `MAX_REQUEST_BYTES` decision: 5 MiB compressed on every route, and 5,242,880 B
  decompressed on every route but traces, whose cap is 16 MiB (the trace agent's own limit is
  3,200,000 B; the headroom admits a sender that allows more). A full pipeline answers 503 with
  `Retry-After: 1` after a 5 s bounded wait, which the Agent retries with backoff, never
  200-and-drop, so the intake stand-in is at-least-once end to end.
- `datadog_trace_in` (W4a): `bind:` (`:8126`) plus an optional `socket:` Unix path; every
  `/v0.3`–`/v1.0/traces` form; `/v0.6/stats` decoded into the same stats events as
  `datadog_in`'s; `/info`; the tracer's `Datadog-Meta-*`, `Datadog-Container-ID`, and
  `Datadog-Client-Computed-{Stats,Top-Level}` headers kept as `datadog.tracer.*` resource
  attributes; stub answers for telemetry, config, and `evp_proxy`, counted. Responds
  `rate_by_service` with rate 1.0.
- `datadog_trace_out` (W6): msgpack `PUT /v0.4/traces` (or `/v0.7/traces` under `version: v0.7`)
  and `POST /v0.6/stats` to an Agent, with the tracer headers restored from `datadog.tracer.*`;
  `endpoint:` (http or https) or `socket:` (the Agent's `receiver_socket`), `compression: none |
  gzip`, `timeout`, `headers`, `tls`.

### 3. Reuse (W3–W6)

The HTTP server driver behind `otlp_in`/`prometheus_in` (`crates/logit-inputs/src/http.rs`);
`crate::http` in `logit-outputs` (`build_client`, `is_retryable_http_status`,
`classify_reqwest_error`); `write_loop`'s bounded retry with `Fault` classification;
`TlsClientConfig`/`TlsServerConfig`; the `TcpListener` driver for the Unix stream transport;
graph rules 55 and 56 as the precedent for the mode and endpoint validation (new rules 62+).

### 4. Sketch compatibility (W1, settled)

`logit_core::sketch::DdSketch` is hand-rolled and carries its bin mapping
(`crates/logit-core/src/sketch.rs`, ADR §3). The spike found `sketches-ddsketch` a dead end
rather than a near miss: it keys by `floor(log_γ(v))` where the Agent rounds to even and adds a
bias (they differ for every value whose `frac(log_γ)` is at least 0.5, as Datadog's own
`+0.5`-offset shim in `pkg/util/quantile/ddsketch.go` documents), it has no hook to apply that
offset, it exposes no bins, and its `to_java_bytes` is a third format, neither the Agent's
`dogsketch` nor the DDSketch protobuf. So:

- `Mapping::agent` (the default) is the Agent's `Config.Default()`: γ = 1.015625, `bias = 1 -
  floor(log_γ(1e-9))`, magnitudes under `f64(1)` in the zero bin, int16 keys with 32767 as ∞,
  collapse-lowest at 4,096 bins. Its key vectors are ported from the Agent's `config_test.go`
  and its collapse vector from `store_test.go`.
- `Mapping::logarithmic(gamma, index_offset, bin_limit)` is `sketches-go`'s mapping, what a
  decoded APM stats sketch keeps, with the mapping on the wire. Counts are `f64` throughout.
- Bins, the zero count, and the summary are public (`positive_bins`, `negative_bins`,
  `zero_count`, `min`/`max`/`sum`/`stats_exact`), and `from_parts` rebuilds a sketch from decoded
  parts, deriving a summary when the wire carries none; W2's `dogsketch` and DDSketch-protobuf
  codecs are built on these.
- A merge across mappings re-bins by representative value, a bounded-error normalization.
- Quantiles are the bin center at the rank, whose `1 - 1/√γ` (0.78%) bound the Agent documents,
  not the Agent's own `Sketch.Quantile` interpolation (ADR, alternatives).
- `size_of::<DdSketch>()` is 128 (was 176); `MetricKind` stays 176, bounded by `Samples`. The
  native wire's `Distribution` payload is `DdSketch::to_bytes`, a versioned form of the parts
  above. Every allocation tripwire held unchanged (the first bin `Vec` reserves the same 1 KiB
  the old crate's chunk did).

Still UNVERIFIED for W7: whether the intake accepts a stats sketch whose gamma isn't 1.0202 (a
relayed stats sketch keeps its own, so only a locally aggregated one would send another).

### 5. Compression (W3, W5)

`datadog_in` decodes zstd with `ruzstd` (pure Rust, decode-only): Agents compress with zstd by
default and `additional_endpoints` can't vary the compressor per endpoint, so requiring gzip
would change what Datadog receives too. `Cargo.toml`'s rationale against `zstd` stays true for
compressing; W3 records it as compression-only. Senders use gzip.

### 6. msgpack and protobuf (W2b)

A hand-rolled msgpack subset (`nil`, bool, int, float, str, bin, array, map) in
`logit_proto::msgpack`, the pickle precedent, with no `rmp` dependency. `agent-payload`'s
`metrics/agent_payload.proto`, `logs/agent_logs_payload.proto`, and
`trace/{span,tracer_payload,agent_payload}.proto` vendored at a pinned tag under
`crates/logit-proto/proto/datadog/`, generated by `script/protogen`, and committed
([ADR `committed-pregenerated-otlp-protobuf`](../adr/committed-pregenerated-otlp-protobuf.md)) --
the protobuf vendoring landed in W2a; msgpack is W2b's, since only traces and stats use it.

### 7. Documented recipes, not code (W8)

- Agent-equivalent `h`/`ms` output: `aggregate` then a sink that renders quantiles; whether an
  `aggregate` option should emit the Agent's five-metric set is a follow-up decided by W7's
  measurements.
- Logs to an Agent: `otlp_out` with the Agent's `logs.enabled: true`, or `syslog_out` over TCP
  to a `logs` TCP listener (W7 checks whether a JSON body inside the syslog line reaches Datadog
  as attributes; if not, the recipe is OTLP only).
- Apps writing JSON lines to an Agent TCP port: no plain-lines listener today. Recorded as a gap
  with a sketch of a `lines_in` on the `TcpListener` driver; not built in this stack.

### 8. Attribute vocabulary (W2)

`datadog.*` for raw encodings (`datadog.type`, `datadog.interval`, `datadog.resources`,
`datadog.source_type_name`, `datadog.origin.*`, `datadog.source`), Datadog's own OTLP-honored
names for span fields (`service.name`, `resource.name`, `span.type`), `meta`/`metrics` keys
verbatim, and the `statsd.*` names for events and service checks. `datadog_out` reads these
names back, and derives the rest: `hostname` from `host.name`, `service` from `service.name`,
`status` from `severity`, `message` from the body (a `Map` body serialized as JSON), `timestamp`
from `Event::timestamp` in milliseconds, `trace_id`/`span_id` as OTel-form hex from `TraceRef`,
which Datadog auto-detects, unless the log already carries a `trace_id` or `span_id` attribute
(W8a).

### 9. Trace ids (W2b, W8)

Decoders build 16-byte ids from a uint64 and `_dd.p.tid`; encoders emit the low 64 bits and
write `_dd.p.tid` when the high bits are nonzero. `trace_context` gains `format: datadog` (W8a):
`dd.trace_id`/`dd.span_id` by default, a decimal uint64 or 32-hex trace id and a decimal span id,
and an optional `trace_id_high` attribute in `_dd.p.tid`'s form, applied only when the id's high
half is zero. 16 hex isn't a Datadog form, so a 16-digit id is decimal under `datadog` and hex
under the default `otel`, never guessed from the value
([ADR `log-record-trace-context`](../adr/log-record-trace-context.md)'s Datadog amendment; graph
rule 67). The parsers live in `logit_core::trace`, shared with the traces codec.

### 10. `statsd` additions (W4b)

`StatsdTransport` gains `unix` (a `SOCK_DGRAM` socket, the Agent's `dogstatsd_socket`) and
`unix_stream` (a `SOCK_STREAM` socket, `dogstatsd_stream_socket`) on `statsd_in` and `statsd_out`.
Under either, the existing address field (`bind:`, `endpoint:`) holds the socket's absolute path,
as a client's `DD_DOGSTATSD_URL=unix:///…` does, so no new field and no "at least one of" rule is
needed; an operator who wants UDP and the socket at once runs two `statsd_in` components. Graph
rule 64 requires an absolute path and rejects `tls:` under either. The listener's socket file is
mode `0722`, the Agent's own, through path handling shared with `datadog_trace_in`
(`crates/logit-inputs/src/unix.rs`). `unix` runs on the UDP driver (receive queue, `recvmmsg`,
`SO_MEMINFO` sampler); `unix_stream` runs on the TCP driver with each packet (one datagram's worth
of lines) after a 4-byte little-endian length, the framing `uds_stream.go` reads and `datadog-go`
writes, UNVERIFIED until W7. `statsd_out` packs packets as for UDP under both, sends a Unix datagram
with its wait on a full receiver bounded by `connect_timeout`, and connects a stream lazily as for
TCP.

`|e:` → `statsd.external_data` and `|card:` → `statsd.cardinality` (the raw token; the Agent's
four values aren't enforced) on metric, event, and service-check lines, round-tripped. `statsd_out`
writes them after `|c:` and before `|T` (segment order UNVERIFIED until W7) and counts each as a
dropped dialect field under `format: statsd`, so superset requirement 14 holds for the two suffixes
added since it was written.

### 11. Timestamp windows (W5)

`datadog_out` drops and counts `dropped{reason="stale"}` any point older than 1 h, log older
than 18 h, or check older than 10 min, instead of letting one stale point fail a payload. The
consequence for `buffer.disk:` replay after a long outage is documented with the component.

### 12. Traces to Datadog: the Agent's protocol, not OTLP, for Datadog-origin spans (W5, W7)

Both paths were evaluated against the Agent source (`pkg/trace/writer`, `pkg/trace/agent`,
`pkg/trace/api/otlp.go`), Vector's `datadog_traces` sink, and Datadog's OTLP-ingest docs.

| | Native (`/api/v0.2/traces` + `/api/v0.2/stats`) | OTLP (`otlp.<site>/v1/traces`, `compute_stats=true`) |
|---|---|---|
| Documented for third parties | No; the Agent is the spec, Vector the precedent | Yes |
| Lossless for a dd-trace span | Yes: the wire is the Agent's own | No: the Agent's OTLP receiver keeps only the low 64 bits of the trace id (the rest survives as an `otel.trace_id` tag, not `_dd.p.tid`), serializes span events and links into JSON meta strings, has no path to `meta_struct`, adds `otel.*` tags, and rederives `name`/`resource`/`type` unless the `operation.name`/`resource.name`/`span.type` overrides are set. A Datadog consumer can tell the two apart, so this path fails `lossless-transit`'s test |
| Trace metrics | Relayed from the Agent's or tracer's stats, computed from 100% of spans before sampling | Computed by Datadog from the spans that arrive, so after any sampling |
| Datadog-only features | Ingestion controls, `_dd.p.dm` ingestion reasons, App and API Protection | Documented as unavailable for OTel-origin data |
| Payload limits and retries | 3.2 MB uncompressed; 408/429/5xx retried | 15 MiB; 413 when over |
| Cost to `logit` | Protos and msgpack the pairs need anyway; a stats codec (`StatsPayload` + `ClientStatsPayload`) | Nothing: `otlp_out` exists |

Decision: `datadog_out` sends traces natively and relays stats; it never converts a
Datadog-origin span to OTLP. `otlp_out` remains the path for OTel-origin spans (`otlp_in`,
`trace_context`-lifted spans, `logit`'s own internal spans), because sending those natively means
synthesizing Datadog semantics and stats, which is §14, not this stack. `datadog_out` decides
from the data, not the source component: the Agent writes the metric `_top_level = 1` on every
root and service-boundary span (`traceutil.ComputeTopLevel`; a tracer that computes it itself
writes `_dd.top_level`, which the Agent converts), and its stats concentrator and Vector both
key on it, so a chunk whose root span carries `_top_level` has been through an Agent or an
equivalent processor and goes out natively. A chunk without it is raw tracer output, with no
Agent stats and no obfuscation of SQL or URLs unless the tracer did it, and is counted
`records.dropped{reason="needs_agent_processing"}`; a span with no Datadog span fields at all is
`records.dropped{reason="not_datadog_origin"}`. So `datadog_trace_in` must not feed `datadog_out`
directly (W8 documents this): the operator routes it to `datadog_trace_out` and a real Agent, or,
once §14 exists, through that processor, which makes the same data ready by writing the same
marks. The native leg is verified against the trial org in W7 (spans visible, service
pages populated from relayed stats, 128-bit ids correlating with logs); if the intake rejects
third-party `AgentPayload`s, the pair test runs against `datadog_in` and the leg becomes a
tracked gap.

### 13. Ordering (all)

Receive side first: W2's decoders fix the vocabulary and mappings, W3 and W4 consume them, W5
and W6 mirror them. W1 lands before W2 because sketch bin access shapes the sketches codec. W4b
and W8's `trace_context` change are independent and can be pulled forward; the latter was, as
W8a.

### 14. Not in this stack: an Agent-equivalent trace processor

An Agent stand-in that sends OTel- or tracer-origin spans directly to Datadog without a real
Agent in the path has to do what the Agent does between tracer and intake: normalization,
`_top_level` marking, priority and sampler tags, and a stats concentrator (10 s buckets keyed
by service/name/resource/type/kind/status, DDSketch durations, `1/_sample_rate` weights), with
obfuscation as an operator choice. That is a transform of its own (`datadog_apm`, say), placed
between `datadog_trace_in` and `datadog_out`, and its output is ready to send because it writes
the same `_top_level` mark §12 keys on. It is a follow-up plan once W7 shows what the intake
needs. Until then
the tracer-direct topology is "`datadog_trace_in` → `datadog_trace_out` → a real Agent", and
the OTel-direct topology is `otlp_out`.

## Workstreams

| # | PR | Size | Depends on |
|---|---|---|---|
| W0 | This plan and its index row | S | — |
| W1 | **Landed** (`dd/w1`). Hand-rolled `DdSketch` with the Agent and logarithmic mappings, bins exposed, `sketches-ddsketch` removed, tripwires and wire doc updated; ADR `datadog-agent-and-intake-relay`; `lossless-transit` amendment; ADR index row. | M | W0 |
| W2a | **Landed** (`dd/w2a`). Vendored `agent-payload` metrics proto as a third protogen family; `logit_proto::datadog` codecs for series v1/v2 (JSON and protobuf), distribution points, sketches, logs, events (Agent envelope and public v1), and service checks; two fixed-point suites. | L | W1 |
| W2b | **Landed** (`dd/w2b`). Hand-rolled msgpack; the Agent's trace protos and `ddsketch.proto` vendored; traces codecs for v0.4/v0.5/v0.7 and `AgentPayload`; the v0.6 and intake stats codecs with the DDSketch protobuf; three fixed-point suites. | M | W2a |
| W3 | **Landed** (`dd/w3`). `datadog_in` on `otlp_in`'s accept loop: every intake route, `DD-API-KEY` allowlist, gzip/deflate/zstd (`ruzstd`, multi-frame, window-capped), a bounded wait then `503` under backpressure; graph rule 62; schema; `datadog-intake-standin.yaml` and `DD_API_KEY` in the shipped-config `!env` map, pulled forward from W8. | M | W2b |
| W4a | **Landed** (`dd/w4a`). `datadog_trace_in` on TCP and a Unix socket: v0.3/v0.4/v0.5/v0.7 msgpack traces and `/v0.6/stats`, tracer headers as `datadog.tracer.*`, a keep-everything rate reply, `/info`, `404`s and `200` stubs for the rest, a 2 s bounded wait then `503`; `datadog_in`'s request helpers moved into `crate::http`; graph rule 63; schema; `datadog-agent-standin.yaml`, pulled forward from W8. | M | W2b |
| W4b | **Landed** (`dd/w4b`). `statsd_in`/`statsd_out` over `transport: unix`/`unix_stream` on the existing datagram and stream drivers, a shared `unix.rs` bind helper, `\|e:`/`\|card:` carried; graph rule 64; schema; the example's socket component. | S | W0 |
| W5 | **Landed** (`dd/w5`). `datadog_out`: one request per intake route, the stale filter, the `_top_level` trace gate (`logit_proto::datadog::trace_readiness`), a count-then-bisect request splitter, gzip with zlib-deflated distribution points; graph rule 65; schema; `datadog-direct.yaml`, pulled forward from W8; a `datadog_out -> datadog_in` pair test over every route. | M | W3 |
| W6 | **Landed** (`dd/w6`). `datadog_trace_out` over TCP or the Agent's Unix socket: v0.4 or v0.7 traces with the tracer headers restored, `/v0.6/stats`, split by trace under the Agent's 25 MiB limit; `split_encode` shared with `datadog_out`; graph rule 66; schema; `datadog-agent-relay.yaml`; a `datadog_trace_in -> datadog_trace_out` pair test over TCP and the socket. | S | W4a |
| W7 | Recorded fixtures via `script/record-fixtures` (an Agent container with `dd_url` at the capture; a `ddtrace` Python producer; DogStatsD over a Unix socket); trial-org end-to-end for `datadog_out`, including the `/api/v0.2/traces` leg and the stale window; pair fixed-point tests over the corpus; UNVERIFIED items resolved in this plan | M | W5, W6 |
| W8a | **Landed** (`dd/w8a`). `trace_context` `format: datadog`: decimal and 128-bit hex `dd.trace_id`, decimal `dd.span_id`, `trace_id_high`; the Datadog id parsers moved into `logit_core::trace`; `datadog_out` writes a log's `TraceRef` as hex `trace_id`/`span_id` (§8); graph rule 67; schema; the ADR `log-record-trace-context` amendment; `datadog-logs-correlation.yaml`. Split out of W8 and landed ahead of W7, which it doesn't need (§13). | S | W6 |
| W8b | `docs/datadog.md` (operator best practices from this plan, including that `datadog_trace_in` must not feed `datadog_out` directly); `deploying.md`; `known-gaps.md`; `AGENTS.md` tables; `telemetry-landscape.md` cells; four examples (`datadog-direct.yaml`, `datadog-via-agent.yaml`, `datadog-agent-standin.yaml`, `datadog-intake-standin.yaml`) and `DD_API_KEY` in `every_shipped_config_loads_and_validates`'s `!env` map (`crates/logit-cli/src/config.rs:257`) | M | W7 |

Landing order: W0 → W1 → W2a → W2b → W3 → W4a → W5 → W6 → W8a → W7 → W8b, linear; W4b
stacks after W4a to keep the stack linear even though it depends only on W0, and W8a after W6
because it needs nothing from W7. Each PR is based on and
targets its parent's branch and is brought up to date with `git merge origin/main`, never a
rebase.

**Status (2026-09-24):** W0 (#309), W1 (#311), W2a (#318), W2b, W3, W4a, W4b, W5, W6, and W8a
complete on their stacked branches, nothing merged to `main`; W1 targets `dd/w0` and retargets to `main`
once it merges. None of W3's receiver, W4a's, or W4b's Unix sockets has yet been pointed at a real
Agent, tracer, or client; W5's `datadog_out` has sent only to `datadog_in`, never to Datadog; and
W6's `datadog_trace_out` has sent only to `datadog_trace_in`, never to a real Agent. W7 does all
three. W8a's `format: datadog` has read only hand-written log lines, never a real tracer's; W7's
`ddtrace` producer can check it.

## Verification

- Per PR: `script/cibuild` green; `script/schema` regenerated and committed by any workstream
  that changes a config type; `script/validate` for any that adds a config.
- W1: the spike's test vectors show identical bins for the same input under `DdSketch` and the
  Agent's `pkg/util/quantile` (ported as a test oracle); `type_sizes.rs`, `allocations.rs`, and
  `memory.md` updated together.
- W2: `decode -> encode -> decode` fixed point over every wire feature listed above, per
  `lossless-transit`'s rule.
- W3/W4: a real Agent container (`datadog/agent`, any non-empty `DD_API_KEY`) with `dd_url`,
  `logs_config.logs_dd_url`, and `apm_config.apm_dd_url` at `datadog_in` delivers series,
  sketches, checks, events, logs, and traces; a `ddtrace`-instrumented Python app with
  `DD_TRACE_AGENT_URL` at `datadog_trace_in` delivers traces and gets a `rate_by_service` reply
  it accepts; a `datadog` Python client over the Unix socket delivers to `statsd_in`.
- W5/W6: the trial org shows series, distributions, logs, events, checks, and traces sent by
  `datadog_out` and `otlp_out`, with service pages and `trace.*` metrics populated from the
  relayed stats and a stats sketch re-encoded by `DdSketch` accepted; a real Agent accepts
  `datadog_trace_out`'s traces and client stats and the trial org shows the spans.
- W7: both pair tests hold over the recorded corpus; every UNVERIFIED item in this plan is
  resolved and the text updated.
- W0 (this PR) is documentation only: every relative link resolves and `docs/plans/README.md`
  gained a row.
