---
created: 2026-09-24
updated: 2026-09-25
---

# Enabling plan: Splunk — HEC in both directions, and Observability Cloud over OTLP

## Context

`logit` has no Splunk-specific component. `syslog_out` can feed a Splunk network input, which
is how an on-prem Splunk was usually fed a decade ago, and `otlp_out` can reach Splunk
Observability Cloud with a `headers:` block nobody has verified end to end. Neither is what
Splunk's own tooling speaks today. This plan records what Splunk accepts and emits, where the
event model and the existing components fall short of it, which way to send data and when, and
the workstreams that close the gaps.

Goals:

- Send logs and metrics to Splunk Enterprise and Splunk Cloud Platform over the HTTP Event
  Collector (HEC), and spans as HEC events in the shape the OpenTelemetry Collector's
  `splunk_hec` exporter already produces, so a Splunk deployment fed by both sees one schema.
- Receive what HEC clients produce, losslessly, standing where Splunk Connect for Syslog
  (SC4S), the Splunk OpenTelemetry Collector, or a Splunk indexer's HEC port would stand: in
  front of Splunk, taking Docker's `splunk` log driver, Splunk's logging libraries, the OTel
  `splunk_hec` exporter, Vector, and an Edge Processor's HEC destination.
- Reach Splunk Observability Cloud with `otlp_out`, verified, with an example config.
- Make migrations to and from Splunk a configuration change, not an application change.

Non-goals: Splunk-to-Splunk (S2S), the proprietary forwarder protocol on `:9997` and the only
thing a universal forwarder can send (§9); REST search export as an input (§9); a raw-TCP line
listener for a forwarder's `sendCookedData=false` output (§9); Splunk's `/services/collector/mint`
mobile endpoint; SignalFx's pre-OTLP JSON APIs (`/v2/datapoint`, `/v2/event`), which OTLP
supersedes.

Stream key **`splunk`**: branches `splunk/w0`…`splunk/w6`, stacked as the workstream table
says. PR stack only: nothing is merged by this workstream; Ross directs merging. The decisions
are recorded in ADR `splunk-hec-relay` (W1); the design sections below are the build-out of
that record.

Settled with Ross (2026-09-24): `splunk_hec_in` is in scope, not only the sink; the ADR is
written from this survey rather than after a spike against a real Splunk, because a trial costs
more than the facts it would settle (the HEC wire is fully documented); verification against a
real Splunk Enterprise container waits for W5.

## Coverage by signal

Splunk is two products with separate stores. Splunk Enterprise and Splunk Cloud Platform (the
same product, hosted) hold logs in event indexes and metrics in metrics indexes and have no
trace store. Splunk Observability Cloud (the former SignalFx) holds traces and metrics and no
logs: Log Observer Connect reads them from the Platform. Each cell reads today → after this
stack.

| Signal | Platform, direct over HEC | Platform, through SC4S or the Splunk OTel Collector | Observability Cloud | HEC stand-in (receive from HEC clients) |
|---|---|---|---|---|
| Logs | `syslog_out` to a network input (a shape Splunk now steers away from) → `splunk_hec_out` `/services/collector/event` (W3) | `syslog_out` to SC4S; `otlp_out` to the Collector's OTLP receiver, which re-emits HEC | none (no log store) | none → `splunk_hec_in` `/event` and `/raw` (W2) |
| Metrics | none → `splunk_hec_out` multi-metric events (W3) | `otlp_out` to the Collector | `otlp_out` OTLP/HTTP `/v2/datapoint/otlp` (unverified) → example config, unverified (W4) | none → `splunk_hec_in` (W2) |
| Traces | none (no trace store) → `splunk_hec_out` spans as JSON events, the OTel exporter's shape (W3) | `otlp_out` to the Collector | `otlp_out` OTLP/HTTP `/v2/trace/otlp` or OTLP/gRPC (unverified) → example config, unverified (W4) | none → `splunk_hec_in` decodes the OTel span-event shape back to a `SpanRecord` (W2) |

## What Splunk accepts and emits

Surveyed 2026-09-24 from `help.splunk.com`, the OpenTelemetry Collector contrib repository
(`exporter/splunkhecexporter`, `receiver/splunkhecreceiver`, `pkg/translator/splunk`), Vector's
reference docs, and the Splunk OTel Collector's default agent config. Items the survey couldn't
confirm from a current official page were marked UNVERIFIED; W5 checked each against recorded
clients and a Splunk Enterprise 10.4.3 container, and ["Settled by W5"](#settled-by-w5-2026-09-25)
records the outcome.

### HEC: the one open door into the Platform

HEC is an HTTPS listener on every indexer or heavy forwarder (`:8088` on Enterprise; on Splunk Cloud
`https://http-inputs-<stack>.splunkcloud.com:443`, or `http-inputs.<stack>` on GCP and Azure stacks;
a trial stack serves `https://<stack>.splunkcloud.com:8088` instead, per ["Settled by the Cloud
run"](#settled-by-the-cloud-run-2026-09-26)). Every third-party Splunk integration is a HEC client.
The Platform has no native OTLP endpoint: the 2026 announcements of OpenTelemetry log ingestion are
about the Splunk OTel Collector distribution, whose default config sends logs to the Platform
through the `splunk_hec` exporter.

| Endpoint | Body and limits | Notes |
|---|---|---|
| `POST /services/collector/event` (also `/services/collector`, `/event/1.0`) | JSON envelope: `time` (epoch seconds, decimals allowed), `host`, `source`, `sourcetype`, `index`, `event` (any JSON), `fields` (a flat object; nesting is rejected). A batch is concatenated objects or a JSON array, each carrying its own metadata. Header `Authorization: Splunk <token>`. `Content-Encoding: gzip` accepted, and `deflate` answered `415` (Splunk 10.4.3, W5; the OTel exporter gzips every body and Vector offers gzip, zlib, zstd, and snappy) | `index` must be one the token allows. `fields` are indexed fields, searchable without extraction. `limits.conf [http_input] max_content_length` caps a request (1,000,000 bytes on old releases; 838,860,800, 800 MiB, on 10.4.3; which release changed it is still open); over it returns 413 |
| `POST /services/collector/raw` | Raw bytes; metadata as query parameters (`host`, `source`, `sourcetype`, `index`); requires a channel GUID (`X-Splunk-Request-Channel` header or `?channel=`); line breaking follows the sourcetype's `props.conf` | What SC4S and the Docker driver's `raw` format use |
| `POST /services/collector/ack` | `{"acks":[<ackId>…]}` → `{"acks":{"<ackId>":true|false}}` | Only with `useACK=true` on the token, which makes every `/event` and `/raw` POST return `{"text":"Success","code":0,"ackId":N}` (the key is `ackId`; ids count from 0 per channel) and require a channel (code 10 without one). `true` means replicated to the configured replication factor, not fully indexed. Splunk's docs say Splunk Cloud supports HEC acknowledgment only on its Kinesis Firehose path; a Splunk Cloud 10.5.2605.9 trial stack offered and honored it, and answers code 28 without a channel (["Settled by the Cloud run"](#settled-by-the-cloud-run-2026-09-26)) |
| `GET /services/collector/health` | `{"text":"HEC is healthy","code":17}` | Also `/health/1.0`; the OTel exporter probes it at startup and can send heartbeats |
| `POST /services/collector/s2s` | S2S framing over HTTP | What a universal forwarder's `[httpout]` sends. Not HEC JSON, so not something `splunk_hec_in` can accept (§9) |

Responses are `{"text":…,"code":N}`. The codes that matter to a sender: 0 success; 1–4 (401/403)
token disabled, missing, invalid; 5–7, 12–13, 15 (400) malformed data, wrong index, missing
`event`; 6 adds `invalid-event-number`, the index of the first bad object in a batch; 9 (503)
server busy; 10–11 (400) channel missing or invalid; 14 (400) ack disabled; 18–20, 23 (503)
unhealthy or shutting down; 26–27 (429) capacity limits exceeded; 28 (400) channel missing, from
Splunk Cloud.

**Metrics over HEC.** A metric event is `"event": "metric"` with the measurement in `fields`:

- multi-metric form (8.0+): `"metric_name:<name>": <number>`, any number of them per event;
- single-metric form: `"metric_name": "<name>"` and `"_value": <number>`, which Splunk merges
  into the same on-disk shape;
- every other key in `fields` is a dimension; `host`, `source`, and `sourcetype` are added as
  dimensions automatically.

Values are integers or doubles. Names are `[A-Za-z0-9_.:]`, no leading digit or `_`, may not
contain `metric_name`, case-sensitive. There is no native histogram, summary, or set type; the
convention Splunk's own histogram docs and the OTel exporter follow is Prometheus-style
`<name>_bucket` with an `le` dimension (including `+Inf`), `<name>_sum`, and `<name>_count`,
queried with `mstats rate()` and the `histperc` macro. `metric_type` is an ordinary dimension
the OTel exporter writes (`Gauge`, `Sum`), and Splunk gives it no other meaning. A metric event
with 1,000 dimensions is indexed whole on 10.4.3; no cap was reached. An object with no `event`
whose `fields` carry a measurement is indexed as a metric too, and a measurement written as a
numeric string as its number: SC4S sends its own metrics that way.

### Observability Cloud ingest

`https://ingest.<realm>.observability.splunkcloud.com` (the legacy `ingest.<realm>.signalfx.com`
still works), authenticated by an `X-SF-Token` header carrying an org access token:

- traces: OTLP/HTTP protobuf at `/v2/trace/otlp`, or OTLP/gRPC on `:443` with `X-SF-Token` as
  metadata (documented for traces only);
- metrics: OTLP/HTTP protobuf at `/v2/datapoint/otlp`; the docs say the gRPC scheme is not
  supported and mention only explicit-bucket histograms;
- logs: none. Splunk's original Log Observer ingest was retired in January 2024; Log Observer
  Connect queries the Platform.

`otlp_out`'s per-signal `paths:` overrides and `headers:` block cover this without a new kind.

### Third-party HEC conventions

The OTel Collector's `splunk_hec` exporter is the de facto schema for OTel data in Splunk, and
its `splunk_hec` receiver is the inverse. `splunk_hec_out` mirrors it so a Splunk deployment fed
by both tools sees one schema, and `splunk_hec_in` accepts it so the OTel exporter is a
first-class producer.

| Data | Exporter encoding | Notes |
|---|---|---|
| Envelope | resource attributes `com.splunk.source` → `source`, `com.splunk.sourcetype` → `sourcetype`, `com.splunk.index` → `index`, `host.name` → `host` | the receiver maps them back to the same names, and can keep the token as `com.splunk.hec.access_token` |
| Logs | `event` = the body as-is (string or object); `fields` = resource and record attributes, nested maps flattened to dotted keys; severity as `otel.log.severity.text` and `otel.log.severity.number`; the event name as `otel.log.name`; `trace_id` and `span_id` when present; `time` = the record timestamp, falling back to the observed timestamp | gzip on by default; `max_content_length_logs` 2 MiB, `max_event_size` 5 MiB |
| Metrics | gauge and sum → one `metric_name:<n>` field with a `metric_type` dimension (`Gauge`, `Sum`); histogram → `_sum`, `_count`, cumulative `_bucket` with `le`; summary → `_sum`, `_count`, `<n>_<q>` with a `qt` dimension; exponential histogram dropped; ±Inf as the strings `"+Inf"`/`"-Inf"` | `use_multi_metric_format` defaults to off, so one metric per event |
| Traces | `event` = a span object, members in `hecSpan` order: `trace_id`, `span_id`, `parent_span_id`, `name`, `attributes`, `end_time` (raw nanoseconds), `kind` (`SPAN_KIND_*`), `status{message,code}` (`STATUS_CODE_*`), `start_time`, then `events[]` and `links[]`; `time` = start in epoch seconds | the receiver keeps a span object as a log record with a `Map` body (0.161.0, W5) |

Vector's `splunk_hec_logs` sink adds templated `index`/`source`/`sourcetype`, `indexed_fields`,
and automatic ack use with 30 polls at 10 s. Its `splunk_hec_metrics` sink sends counters and
gauges only and always sends a random request channel. Its `splunk_hec` source serves `/event`,
`/raw`, and `/health` with a `valid_tokens` allowlist and produces logs only. Docker's `splunk`
log driver (`splunk-format: inline | json | raw`, optional gzip) and the Splunk Java logging
library (Logback, Log4j 2, JUL appenders) are HEC clients; the .NET library has been dormant
since 2023.

### What Splunk sends out

The Platform pushes to a non-Splunk receiver in few ways, none of them HEC:

- a heavy forwarder's `outputs.conf [syslog]` stanza: RFC 3164 over UDP (default) or TCP, priority
  `<13>` by default, `maxEventSize`, `timestampformat`. Universal forwarders can't;
- any forwarder's `[tcpout]` with `sendCookedData = false`: "raw and untouched" events over TCP,
  each event's `_raw` followed by one LF and nothing else, so an event with an embedded newline
  arrives as two lines (10.4.3, W5);
- Edge Processor (Splunk Cloud, and a Splunk Enterprise 10.x edition) and Ingest Processor: to
  the connected Splunk Cloud, to another Splunk platform over S2S or HEC, to Amazon S3 (Parquet
  or gzip) or Azure Blob, and, for Ingest Processor, to Observability Cloud. Pointing the HEC
  destination at a non-Splunk receiver is still unverified (item 3 below); the docs describe it
  only for Splunk targets and require ack off on the destination token;
- Ingest Actions route to S3 or a file system;
- REST `search/jobs/export` on the management port `:8089` streams results as CSV, JSON, or raw;
  Splunk Cloud needs a support ticket to open it.

Edge Processor's inputs are S2S, HEC (`/event` and, since 2026-06, `/raw`), and syslog over UDP
or TCP; no OTLP input appears in its docs or release notes.

### Settled by W5 (2026-09-25)

These eight were UNVERIFIED in the survey. W5 recorded four real HEC clients
(`testdata/interop/splunk/README.md`) and ran `script/splunk-interop` against Splunk Enterprise
10.4.3 (`tools/splunk-interop/README.md`, "What the run showed"). ADR `splunk-hec-relay`'s W5
amendment records what each changed.

1. **`Content-Encoding: gzip` on `/event` and `/raw`: accepted.** Both answer `200` and index
   the body; `deflate` is `415`. The OTel exporter gzips every body, and the Docker driver with
   `splunk-gzip`.
2. **`max_content_length`: 838,860,800 bytes (800 MiB) on 10.4.3.** Still open: which release
   raised it from 1,000,000. Only one release was run.
3. **An Edge Processor's HEC destination: still unverified.** Edge Processor runs in Splunk Cloud
   (and a Splunk Enterprise 10.x edition the `splunk/splunk` image isn't), so no container here
   has one.
4. **`metric_type`: an ordinary dimension.** `mcatalog values(metric_type)` lists it, `mstats …
   by metric_type` groups by it, and a `Sum`'s value is stored as sent.
5. **Dimension count: no cap reached at 1,000.** A metric event with 200 and one with 1,000
   dimensions were both indexed with every dimension.
6. **The OTel `splunk_hec` receiver doesn't decode span objects.** Posted the recorded exporter
   span body, Collector contrib 0.161.0's receiver made a log record with a `Map` body and no
   trace or span id (`script/record-fixtures splunk-otel` prints it).
7. **`[tcpout] sendCookedData = false`: each event's `_raw` and one LF.** No header, length, or
   metadata; an embedded newline passes through, so a line-oriented receiver splits that event.
   Splunk forwarded its own logs from every index to the one output group too, whatever
   `defaultGroup` and the `forwardedindex` filters said, so no capture is committed.
8. **Code 6: the objects before `invalid-event-number` are indexed, and none from it on.**
   `invalid-event-number` is the 0-based index of the object that failed to parse. That is the
   assumption decision 18 made: `splunk_hec_out` drops object `N` and resends `N+1` onward.
   Codes 12, 13, and 15 carry an `invalid-event-number` with the same prefix-indexed meaning;
   code 7 names the object after the one with the bad index. The sink resends only after code 6.

### Settled by the Cloud run (2026-09-26)

`script/splunk-interop` ran with `SPLUNK_INTEROP_TARGET=cloud` against a Splunk Cloud Platform
trial stack that Splunk Web reports as `Splunk 10.5.2605.9`. A trial has no REST API, so arrival
was confirmed by searching in Splunk Web (`tools/splunk-interop/README.md`, "What the run
showed"). ADR `splunk-hec-relay`'s Cloud amendment records what each changed.

1. **Every leg landed, as on 10.4.3.** Logs with every field indexed, 21 metric series with
   `metric_type` and `histperc` answering 2.8, the span object with every member, the ack leg,
   and the whole recorded corpus relayed through `splunk_hec_in`.
2. **The trial's HEC endpoint is `https://<stack>.splunkcloud.com:8088/services/collector`.**
   The `http-inputs-<stack>.splunkcloud.com` form doesn't resolve for this stack. The certificate
   is Splunk's default self-signed one (`CN=SplunkServerDefaultCert`, issuer `SplunkCommonCA`), so
   a client needs `tls: {insecure_skip_verify: true}`; a `ca_file` with Splunk's CA would still
   fail hostname verification. `/health` answers without a token. Still open: whether a paid
   stack's `http-inputs-` form presents a public CA.
3. **Acknowledgment works on the trial stack.** The token settings offer "Enable indexer
   acknowledgment", and the ack leg counted `acked=78 timeout=0 unsupported=0`. `ackId` counts
   from 0 per channel, a poll answers `true` within about 1.2 s, and another channel sees
   `false`. Splunk's docs say Splunk Cloud supports acknowledgment only
   for its Firehose path; this stack contradicts them, and a customer stack may differ.
4. **A `useACK` token without a channel answers `400` code 28**, text `Data channel is missing.
   If you have multiple indexers, sticky session load balancers must be provisioned and client
   requests must be routed accordingly.` (10.4.3: code 10 `Data channel is missing`). `HecStatus`
   models it, and `splunk_hec_out` treats it as it treats code 10: permanent, counted under its
   code.
5. **No `Set-Cookie` on any reply**, so this stack needs no load-balancer stickiness for ack
   polls.
6. **The body cap is between 5,242,881 and 6,000,000 bytes, and over it the answer is `400` code
   6 naming object 0, not `413`.** Bodies up to 5,242,881 bytes uncompressed, and 2,000,000
   gzipped, were accepted and indexed. `splunk_hec_out`'s 2 MiB `max_body_bytes` default sits
   under the cap. A `max_body_bytes` above it turns the body's first object into an
   `invalid_event` drop and resends the rest, the right outcome only for one huge object.
7. **Everything else matches 10.4.3**: gzip accepted and `deflate` `415`; the prefix semantics
   of codes 6, 7, 12, 13, and 15; the lenient cases; `OPTIONS`; the `404` and `405` bodies;
   `/raw` without a channel; 200 and 1,000 dimensions; both metric forms; `metric_type` a
   dimension.
8. **Edge Processor and Ingest Processor: not provisioned on the trial stack.** Data Management
   shows only a link to request them, which takes a support case or the account team. Ingest
   Processor's destinations are Splunk indexes, S3, and Observability Cloud, so it has nothing
   that reaches `logit`. Item 3 of "Settled by W5" stays open for Edge Processor.

Not settled by this run: Observability Cloud, the paid-stack `http-inputs-` form and its
certificate, which Enterprise release raised `max_content_length`, Vector, and a `useACK` client
against `splunk_hec_in`.

## Splunk's data against `Event`

The HEC envelope and the OTel exporter's log shape fit the model in typed fields and resource
attributes. The metric shape is one number per name, so every multi-number kind degrades on
the way out and needs the switch §4 describes.

| Splunk concept | `Event` representation | Verdict |
|---|---|---|
| `time` (epoch seconds, decimal) | `Event::timestamp` in ns; absent → receipt time, as `syslog_in` does | lossless; the encoder writes seconds with nanosecond decimals, which Splunk truncates to its own precision (permitted number-formatting normalization) |
| `host`, `source`, `sourcetype`, `index` | batch `Resource` attributes `host.name`, `com.splunk.source`, `com.splunk.sourcetype`, `com.splunk.index`; a batch boundary per distinct envelope | lossless; the OTel names, so `otlp_out` to the Splunk Collector and `splunk_hec_out` agree |
| `event` as a string | `LogRecord.message` `Str` | lossless |
| `event` as a JSON object or array | `LogRecord.message` `Map`/`Array`, the shape `json` produces | lossless; Splunk indexes the object's keys itself, so nothing is lifted to attributes |
| `event: "metric"` with `fields` | `MetricRecord`s on the event, one per `metric_name:<n>` (or the single-metric pair), `Gauge` unless `metric_type: Sum`; remaining `fields` as attributes | lossless for gauge and sum |
| `fields` (flat, indexed) | event attributes | lossless; a nested attribute at egress is flattened to dotted keys the way the OTel exporter does, with an operator `flatten` upstream as the alternative |
| `channel`, `ackId` | not modeled | sink-local (§5) and listener-local (§3); a permitted normalization |
| `/raw` body | one `Str` log per line (LF-delimited; CR stripped), envelope from the query string | lossless modulo the line split, which Splunk performs too; `props.conf` multi-line rules are the operator's `regex`/Lua stage |
| OTel span event (`event` = span object) | `SpanRecord`, decoded when the object carries `trace_id`, `span_id`, `start_time`, and `end_time`; otherwise a log with a `Map` body | lossless for the exporter's shape; anything else stays a log |
| OTel log fields (`otel.log.severity.*`, `otel.log.name`, `trace_id`, `span_id`) | `LogRecord.severity`, `event_name`, `trace` | lossless |
| `Histogram`, `Summary` at egress | `_bucket`/`le`, `_sum`, `_count`, `<n>_<q>`/`qt`, the OTel exporter's shape, under `multi_value: expand` (§4) | degraded, counted; a row in `known-gaps.md`'s cross-protocol table |
| `Samples`, `SetMembers`, `Distribution`, `Set`, `ExponentialHistogram` at egress | `expand` renders `Samples` as `_count`/`_sum`/`_min`/`_max`, `Distribution` as `_count`/`_sum`/`_p50`/`_p90`/`_p99` (`influxdb_out`'s summary set), `Set` as the estimate, `SetMembers` as the member count; `ExponentialHistogram` skipped, as the OTel exporter does | degraded, counted |
| cumulative `Sum` | the value as-is with `metric_type: Sum`; Splunk metrics are samples, so `mstats rate()` does the rest | lossless |
| metric name charset | sanitizer: `/`, `-`, and other characters → `_`; a leading digit or `_` prefixed | permitted normalization |
| a `GaugeDelta` | rejected, as every sink does | not a Splunk concept |

## Direct, or through Splunk's own collectors: trade-offs and best practice

**Direct over HEC.** For: one hop, works wherever HTTPS does, the same wire on Enterprise and
Cloud, indexed `fields` without a props/transforms stage, and `logit` is already the host
collector. Against: acknowledgment on Splunk Cloud depends on the stack (Splunk documents it
only for Firehose, a trial stack honored it), so delivery to a stack without it is best-effort
past a 2xx; per-token index allowlists to keep in step; the sourcetype and its `props.conf` still
decide timestamp and line-breaking for `/raw`, so `/event` with an explicit `time` is the path
`splunk_hec_out` takes.

**Through SC4S.** For: Splunk's validated parsers for hundreds of vendor syslog formats, its
recommended path for syslog. Against: one more container; only syslog goes in; a second parse
of what `logit` already parsed.

**Through the Splunk OTel Collector.** For: Splunk's supported path for OpenTelemetry data,
including the Observability Cloud legs and the Platform's log ingest in one agent. Against:
one more process per host; `otlp_out` → Collector → HEC is two hops to reach the same HEC
endpoint `splunk_hec_out` reaches in one.

**Best practice, to Splunk.** Send logs and metrics directly with `splunk_hec_out` when `logit`
is already the collector on the host. Send traces and Observability Cloud metrics with
`otlp_out` and an `X-SF-Token` header. Front SC4S only where its vendor parsers are wanted.
Don't send one signal both ways.

**Best practice, from Splunk.**

1. Tee: point one HEC client (a Docker daemon's `splunk-url`, a Java appender, an OTel
   exporter) at `splunk_hec_in`, and fan out to `splunk_hec_out` beside the new backend. No app
   change, reversible in one URL edit.
2. Compare, then move the rest of the clients.
3. Cut over: remove the `splunk_hec_out` leg.

For a forwarder-fed Splunk, `logit` can't stand in front of the universal forwarders (§9). A
heavy forwarder's `[syslog]` output into `syslog_in` is the tee. Edge Processor's HEC
destination into `splunk_hec_in` would be another, but whether it accepts a non-Splunk target is
still unverified (item 3 of "Settled by W5").

## Design

Each item is a decision the ADR records, and names the workstream that builds it.

### 1. One new lossless pair (W1)

`splunk_hec_in -> splunk_hec_out` joins the six pairs in
[ADR `lossless-transit`](../adr/lossless-transit.md). The codec lives in
`crates/logit-proto/src/splunk/`, one module whose doc is the mapping table above, the collectd
pattern. It is an `Encoder` (one JSON body per batch; `influxdb`'s shape), because a HEC batch
is one request and per-event drop accounting happens in the encoder's own counters. The
amendment to `lossless-transit.md` lands with the ADR.

### 2. Kinds and config (W2, W3)

- `splunk_hec_in` (W2): `bind:`, `tls:`, an optional `tokens:` allowlist (empty = accept
  any, the shape the Datadog and New Relic plans give `api_keys:`), `max_request_bytes`
  (default 5 MiB, the OTel exporter's `max_event_size`, plus the 2 MiB default body), and the
  `TcpListener`-style `handshake_timeout` and `idle_timeout`. Routes:
  `/services/collector`, `/event`, `/event/1.0` (JSON, concatenated or array), `/raw` and
  `/raw/1.0` (lines), `/health` and `/health/1.0` (code 17), `/ack` (every asked id `true`,
  because a 2xx means delivered to the pipeline at-least-once, and a full pipeline answers 503
  code 9, which every HEC client retries). Unknown routes are 404 and counted. Decompresses
  gzip only; a request with another `Content-Encoding` is 415 and counted. Answers Splunk's
  own `{"text","code"}` bodies so a client's error handling reads them as it would Splunk's. The
  token is not kept as an attribute (unlike the OTel receiver's option): a secret has no
  business in the event.
- `splunk_hec_out` (W3): `endpoint:` (the full `/services/collector` URL, so Cloud's port 443
  and a `logit` peer's port are both plain), `token: !env SPLUNK_HEC_TOKEN`,
  `compression: none | gzip` (default gzip, the OTel exporter's default),
  `multi_value: skip | expand` (§4), `ack: false | true` (§5), `timeout`, `tls`, and
  `max_body_bytes` (default 2 MiB, the OTel exporter's), splitting a batch across requests
  when a body would exceed it. `index`, `source`, `sourcetype`, and `host` come from the
  resource attributes above, set upstream with `set`; there are no per-sink fields for them,
  by [`operator-declared-resource-attributes`](../adr/operator-declared-resource-attributes.md).
  `duplicate_safe()` is false: Splunk indexes a resent event twice.
- Error classification: 429 (codes 26, 27) and 503 (9, 18–20, 23) retry through `write_loop`;
  401, 403, and 400 are permanent and counted with the code; a 400 code 6 with
  `invalid-event-number` drops that one event and resends the rest of the body, once, so a
  single malformed event can't poison a batch.

### 3. Reuse (W2, W3)

`crates/logit-inputs/src/http.rs`'s idle-timeout driver and `otlp_in`'s `bind`/TLS/connection
cap/`Content-Type` dispatch for the listener; `crate::http` in `logit-outputs` (`build_client`,
`is_retryable_http_status`, `classify_reqwest_error`, `read_body_prefix`) and `otlp_out`'s
`with_headers` and gzip for the sink; `write_loop`'s bounded retry with `Fault` classification;
`TlsClientConfig`/`TlsServerConfig`; `graphite_out`'s `MultiValue` for §4; graph rules 55 and
56 as the precedent for endpoint validation (new rules 69 and 70).

### 4. Multi-number kinds on a one-number wire (W3)

Splunk has one metric shape, `name = number`. `multi_value: skip` (default) drops `Samples`,
`SetMembers`, `Distribution`, `Set`, `Histogram`, `ExponentialHistogram`, and `Summary`, counted
`logit.output.metrics.skipped{metric_kind}`; `expand` renders each as the series the mapping
table lists, counted `degraded{metric_kind}`, with `Histogram` and `Summary` in the OTel
exporter's exact shape so a Splunk dashboard built for the Collector's output works unchanged.
`ExponentialHistogram` is skipped under both, as the exporter does. This is `graphite_out`'s
switch, not a new mechanism; summarization stays `aggregate`'s job upstream.

### 5. Acknowledgment (W3)

`ack: true` adds a per-sink channel GUID to every request, reads the `ackId`, and polls
`/services/collector/ack` until the id is `true` or `ack_timeout` (default 30 s, the
`batchTimeout` a forwarder uses) elapses, at which point the batch is a `Fault::Ambiguous` for
`write_loop` to retry. Off by default: Splunk documents it on Splunk Cloud only for Firehose
(though the Cloud run's trial stack honored it, item 3 of
["Settled by the Cloud run"](#settled-by-the-cloud-run-2026-09-26)), and a token without
`useACK` returns no id. Both `ack` and the channel header are sink-local; nothing about them
enters the event.

### 6. Attribute vocabulary (W1)

The OTel exporter's names, not a `splunk.*` namespace: `com.splunk.source`,
`com.splunk.sourcetype`, `com.splunk.index`, and `host.name` on the resource;
`otel.log.severity.text`/`.number` and `otel.log.name` decoded into typed fields and re-emitted
from them; `trace_id`/`span_id` into `TraceRef`. A `splunk.*` name appears only for something
the OTel schema has no word for, and this survey found none. The reason: the schema Splunk
users already have dashboards against is the Collector's, and one vocabulary across `otlp_out`
→ Collector → HEC and `splunk_hec_out` is what makes those two paths interchangeable.

### 7. Timestamps (W1, W3)

HEC `time` is seconds with an optional fraction. The decoder keeps every digit the sender gave
into `Event::timestamp` (ns); the encoder writes `secs.nanos` trimmed of trailing zeros. An
event without `time` takes receipt time on the way in, as `syslog_in` does, and `splunk_hec_out`
always writes `time`, so Splunk never applies a sourcetype's timestamp extraction to what
`logit` sends. There is no stale-data window to enforce: Splunk indexes any `time`.

### 8. Ordering (all)

Receive side first: W2's decoder fixes the vocabulary and mappings; W3 mirrors it. W1 (codec
and ADR) precedes both because the pair test needs both halves of the codec.

### 9. Not in this stack

- **S2S.** The universal forwarder speaks only S2S, even over `[httpout]`, which sends S2S
  framing to `/services/collector/s2s`. The protocol has no public specification; Splunk 9.1+
  requires v4 unless `enableOldS2SProtocol = true`; the open implementations are Go and stop at
  v3; Cribl's is proprietary. A `logit` S2S receiver would be a reverse-engineering project of
  its own, recorded in `known-gaps.md` with the workarounds above (heavy forwarder `[syslog]` →
  `syslog_in`; Edge Processor HEC → `splunk_hec_in` if W5 confirms it).
- **REST search export** as an input: a poll-driven source over a management port Splunk
  Cloud opens by ticket; a different component shape from every listener `logit` has.
- **A raw-TCP line listener** for `sendCookedData = false`: not Splunk-specific; the Datadog
  plan's §7 records the same gap for Agent log ports, and a `lines_in` on the `TcpListener`
  driver would serve both.
- **SignalFx JSON APIs** and Observability Cloud's `/v3/event` and `/v1/log` (entity events
  and profiling, not general logs).

## Workstreams

| # | PR | Size | Depends on |
|---|---|---|---|
| W0 | This plan and its index row | S | — |
| W1 | ADR `splunk-hec-relay`; `logit_proto::splunk`: envelope, `/event` JSON (logs, metric events, the OTel span shape), `/raw` lines, the `{"text","code"}` response bodies; encoder with `MultiValue`; fixed-point suite; `lossless-transit` amendment; ADR index row | M | W0 |
| W2 | `splunk_hec_in`: listener, routes, `tokens:`, gzip, graph rules, schema | M | W1 |
| W3 | `splunk_hec_out`: HEC client, body splitting, error classification, `ack:`, graph rules, schema | M | W1 |
| W4 | `fixtures/splunk-observability.yaml`: `otlp_out` to Observability Cloud (traces over OTLP/HTTP, metrics over OTLP/HTTP), marked unverified — no trial org (Ross, 2026-09-25); verification is a follow-up | S | W3 |
| W5 | Recorded fixtures via `script/record-fixtures` (a Splunk Enterprise container with a `useACK` token as the HEC target; producers: the OTel Collector `splunk_hec` exporter with logs, all metric kinds, and traces; Docker's `splunk` driver in each `splunk-format`; SC4S; a Java appender); the pair fixed-point test over the corpus; `splunk_hec_out` end-to-end into that container, including ack and a 400 code 6 split; UNVERIFIED items resolved in this plan | M | W2, W3 |
| W6 | `docs/splunk.md` (operator best practices from this plan); `deploying.md`; `known-gaps.md` (S2S, REST export, raw `tcpout`, the cross-protocol rows); `AGENTS.md` tables; `telemetry-landscape.md` cells; fixtures `splunk-hec-send.yaml`, `splunk-hec-receive.yaml`, `splunk-hec-relay.yaml`; `SPLUNK_HEC_TOKEN` in `every_shipped_config_loads_and_validates`'s `!env` map (`SPLUNK_OBSERVABILITY_TOKEN` landed with W4) | S | W5 |

Landing order: W0 → W1 → W2 → W3 → W4 → W5 → W6, linear. Each PR is based on and targets its
parent's branch and is brought up to date with `git merge origin/main`, never a rebase.

**Status (2026-09-25):** W0 through W6 complete. W0 (#338) is merged to `main`; W1 #363, W2
#368, W3 #373, W4 #376, W5 #378, and W6 (this PR) are open, each stacked on the one before,
nothing else merged. The [closing assessment](#closing-assessment) below records what the
stack closed and what it left open.

## Closing assessment

The new like-protocol pair relays losslessly modulo the permitted normalizations
`crates/logit-proto/src/splunk/mod.rs`'s module doc lists:

- **`splunk_hec_in -> splunk_hec_out`** (Splunk's HTTP Event Collector): `/event` bodies,
  concatenated or as an array, regrouped into one batch per envelope, with `host`, `source`,
  `sourcetype`, and `index` on the resource under the OTel exporter's names; `time` kept to the
  nanosecond without an `f64`; and `/raw` lines with the query-string envelope, which leave
  through `/event`. The normalizations are framing and batching by resource, JSON formatting and
  number spelling, a flattened `fields`, the single-metric form leaving as multi-metric with a
  `metric_type`, span enum spellings, lowercase hex ids, and `/raw` split into one event per line.

Per signal:

- **Logs** relay both ways with the exporter's `otel.log.severity.*`, `otel.log.name`, and
  `trace_id`/`span_id` decoded into `LogRecord`'s typed fields and written back from them (§6); an
  object with no `event` or a blank one is skipped and counted, and every valid object in the body
  delivered. Splunk 10.4.3 skips an object with `fields` and no `event` too, but answers one with
  neither code 12 and a blank `event` code 13, indexing only the objects before it (ADR decision
  16 and its W5 amendment).
- **Metrics**: gauges and sums relay in both metric forms, `metric_type` carrying `Sum` versus
  `Gauge`, and SC4S's `event`-less objects with numeric-string measurements decode as metrics.
  Another protocol's multi-number kinds leave through `multi_value: skip | expand` (§4), with
  histograms and summaries in the exporter's shape.
- **Traces**: the exporter's span object decodes to a `SpanRecord` and leaves in its `hecSpan`
  member order and enum names, so a Splunk index fed by the Collector and by `splunk_hec_out`
  holds one span schema. The Platform itself has no trace store; Observability Cloud's OTLP
  ingest is reached by `otlp_out` with `fixtures/splunk-observability.yaml` (W4), unverified.

Each is proven by a test suite and by real traffic:

- **Codec fixed point**: `crates/logit-proto/tests/splunk_fixed_point.rs` checks
  `decode(encode(decode(b)))` against the normalization list over hand-written bodies and a
  grammar of generated ones, the OTel log and span shapes and both metric forms included.
- **Pair round trips over real sockets**: `crates/logit-cli/tests/splunk_pair_round_trip.rs`
  (`splunk_hec_out -> splunk_hec_in`: gzip, the token check, body splitting, and the channel and
  acknowledgment exchange) and `splunk_hec_in_round_trip.rs` for the listener's routes, answers,
  and backpressure.
- **Recorded interop corpus** (W5): [`testdata/interop/splunk/`](../../testdata/interop/splunk/README.md),
  from the Collector contrib 0.161.0 `splunk_hec` exporter, Docker 29.8.1's `splunk` log driver,
  SC4S 3.40.0, and splunk-library-javalogging 1.11.11, replayed through the codec by
  `crates/logit-proto/tests/splunk_interop.rs` and over a socket through `splunk_hec_in`. Where it
  contradicted the survey, the code changed: `splunk_hec_in` answers `OPTIONS` (Docker's driver
  starts no container without it), `splunk_hec_out` writes the `SPAN_KIND_*`/`STATUS_CODE_*`
  names, and the decoder reads SC4S's metric objects.
- **Splunk Enterprise 10.4.3 run** (W5): `script/splunk-interop` delivered logs, every metric kind
  under `expand` (queried with `mstats` and `histperc`), spans, an `ack: true` leg on a `useACK`
  token, and the whole corpus through `splunk_hec_in` and `splunk_hec_out`, every leg `PASS`. It
  fixed the ack key (`ackId`, not `ackID`, which the sink had misread as "no acknowledgment") and
  settled the survey's UNVERIFIED items, including decision 18's code 6 rule
  (["Settled by W5"](#settled-by-w5-2026-09-25)).
- **Splunk Cloud Platform 10.5.2605.9 run** (2026-09-26): the same legs and probes against a
  trial stack, every leg `PASS` by a search in Splunk Web. It showed acknowledgment working,
  added code 28, and found Cloud's body cap
  (["Settled by the Cloud run"](#settled-by-the-cloud-run-2026-09-26)).
- **Listener fidelity** (2026-09-26): `splunk_hec_in` issues `ackId`s from 0 per channel and
  answers each `true` once on its own channel, as both runs' `useACK` tokens did, within
  `max_ack_channels` and `max_pending_acks` bounds; `/ack` without a channel is code 10; and
  `/health` answers `503` code 18 while posts are answered code 9. `splunk_hec_in_round_trip.rs`
  and `splunk_pair_round_trip.rs` drive both (ADR amendment "faithful listener acks and a busy
  /health").

What's left is tracked in [`docs/known-gaps.md`](../known-gaps.md)'s "Splunk" section, one entry
each:

- No S2S listener, so a universal forwarder can't point at `logit` (§9).
- No REST search export input (§9).
- No listener for a forwarder's `[tcpout] sendCookedData = false` lines, shared with the Datadog
  section's plain-lines gap (§9).
- An Edge Processor's HEC destination pointed at `splunk_hec_in`, still unverified (item 3);
  the Cloud trial stack has no Edge Processor.
- Acknowledgment on Splunk Cloud: honored by the trial stack, documented by Splunk as
  Firehose-only, so `ack: true` stays opt-in.
- HEC codes 21, 22, 24, and 25 unmodeled; code 28 modeled from Splunk Cloud; no code 18 through
  27 seen from a real Splunk.
- Codes 7, 12, 13, and 15 permanent in `splunk_hec_out`, where only code 6 drops one object.
- `splunk_hec_in`'s acknowledgment bounds: an issue window per channel rather than Splunk's
  outstanding-id count, and least-recently-used channel eviction.
- Splunk Cloud's body cap, between 5.2 and 6 MB, answered with code 6 rather than `413`.
- What neither the corpus nor the runs exercised: the exporter's `Summary` and a link's
  `trace_state`, Vector's HEC sinks, a third-party `useACK` client against `splunk_hec_in`, a
  paid Splunk Cloud stack's `http-inputs-` endpoint and its certificate, Splunk Enterprise
  releases other than 10.4.3 (including which one raised `max_content_length`), and
  Observability Cloud.

Cross-protocol egress stays best-effort under ADR `lossless-transit`: the Splunk encode and decode
rows in `known-gaps.md`'s "Cross-protocol semantic gaps" table (non-carrier resource attributes
returning as event attributes, `Sum` temporality, the multi-number kinds, the span and log fields
the exporter's objects have no member for, the `/raw` line split) are counted or documented, not
closed.

[ADR `splunk-hec-relay`](../adr/splunk-hec-relay.md)'s Status, and
[ADR `lossless-transit`](../adr/lossless-transit.md)'s ninth-pair amendment, now record this
closing assessment as the realization of their decisions.

## Verification

- Per PR: `script/cibuild` green; `script/schema` regenerated and committed by any workstream
  that changes a config type; `script/validate` for any that adds a config.
- W1: `decode -> encode -> decode` fixed point over every envelope key, both metric forms, the
  OTel log and span shapes, a `/raw` body, and every response code listed above, per
  `lossless-transit`'s rule; `type_sizes.rs` and `allocations.rs` unchanged (no model change).
- W2: a Docker daemon with `--log-driver splunk --log-opt splunk-url=http://<logit>:8088`
  delivers in each `splunk-format`; the OTel Collector's `splunk_hec` exporter delivers logs,
  metrics, and traces and its health probe succeeds; a client sent a 503 retries.
- W3: a Splunk Enterprise container shows logs with indexed `fields`, metrics queryable by
  `mstats` including an expanded histogram through `histperc`, and span events; a `useACK`
  token round-trips an id; a `400` code 6 batch is split and the rest delivered.
- W4: `script/validate` and `every_shipped_config_loads_and_validates` accept
  `fixtures/splunk-observability.yaml`. No trial org run: the leg stays unverified until a
  follow-up confirms it against a real Observability Cloud org.
- W5: the pair test holds over the recorded corpus; every UNVERIFIED item in this plan is
  resolved and the text updated. Done: `crates/logit-proto/tests/splunk_interop.rs` and
  `splunk_hec_in_round_trip.rs` pass over `testdata/interop/splunk/`, and a
  `script/splunk-interop` run passed every leg. The code 6 split can't be provoked through a real
  Splunk from `splunk_hec_out`, whose bodies always parse; the probe settles what the split
  assumes, and the sink's unit tests cover the split against a stub.
- Cloud run (2026-09-26): `SPLUNK_INTEROP_TARGET=cloud script/splunk-interop` against a Splunk
  Cloud Platform 10.5.2605.9 trial stack sent every leg with nothing dropped, and a search in
  Splunk Web confirmed each leg's data; the probes' answers are in
  ["Settled by the Cloud run"](#settled-by-the-cloud-run-2026-09-26).
- W0 (this PR) is documentation only: every relative link resolves and `docs/plans/README.md`
  gained a row.
