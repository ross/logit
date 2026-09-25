# Splunk HEC interop fixtures

Real HTTP Event Collector (HEC) traffic from four real HEC clients, captured verbatim by
`tools/record-fixtures/raw_capture.py` with no decompression, parsing, or re-encoding: the
OpenTelemetry Collector's `splunk_hec` exporter, Docker's `splunk` log driver, Splunk Connect for
Syslog (SC4S), and Splunk's Java logging library. `logit`'s HEC codec
(`crates/logit-proto/src/splunk/`) and `splunk_hec_in` were written from Splunk's docs and the
exporter's source; these captures check that reading against what the clients put on the wire.
Nothing here came from, or went to, a Splunk: the capture sink stood where HEC would, answering
Splunk's own bodies (`{"text":"Success","code":0}`, and `{"text":"HEC is healthy","code":17}` on
`/health`) on every route alias.

Every producer carried the same dummy token, `00000000-0000-0000-0000-000000000000`, which is the
`Authorization: Splunk …` value in every sidecar. It never authorized anything.

To regenerate, run `script/record-fixtures splunk` (or one of `splunk-otel`, `splunk-docker`,
`splunk-sc4s`, `splunk-java`). The script's header comments say what each producer runs. See
`../README.md` for the corpus-wide rules.

## Fixtures

Each request is two files: the body as sent (`.bin`, gzip where the sidecar's
`content-encoding` says) and a `.headers` sidecar holding the method, the path with its query
string, and every request header, lowercased. A `GET` or `OPTIONS` has an empty `.bin`. Files are
named by producer and route (`raw_capture.py --name-by-path`) and numbered in arrival order per
route.

| Files | Producer | Invocation | Captured | Construct exercised |
|---|---|---|---|---|
| `otel-services-collector-health-000.*` (0 B) | OpenTelemetry Collector contrib 0.161.0 (`otel/opentelemetry-collector-contrib:0.161.0`), fed by telemetrygen v0.161.0 | `tools/record-fixtures/otel-collector-splunk.yaml`: an `otlp` receiver, a `batch` processor, and five `splunk_hec` exporters; `health_check_enabled` on the logs exporter | 2026-09-25 | The exporter's startup `GET /services/collector/health`, with no `Authorization` |
| `otel-services-collector-000.*` (575 B) | Same | `telemetrygen traces --traces=3 --rate=0`, exporter `splunk_hec/traces` | 2026-09-25 | Six span events (three traces, each a `SPAN_KIND_CLIENT` root and a `SPAN_KIND_SERVER` child) in the exporter's `hecSpan` member order: `trace_id`, `span_id`, `parent_span_id` (`""` on a root), `name`, `attributes`, `end_time`, `kind`, `status` (`message`, then `code`), `start_time`; `fields` the resource's `service.name`; envelope `host` `unknown` (telemetrygen sets no `host.name`); gzip |
| `otel-services-collector-001.*` (283 B) | Same | `telemetrygen logs --logs=3 --severity-text=Warn --severity-number=13` with a fixed `--trace-id`/`--span-id` and `--telemetry-attributes=fixture.tags=["a","b"]`, exporter `splunk_hec/logs` | 2026-09-25 | Three logs: `event` the body string, `fields` with `otel.log.severity.number` and `.text`, `trace_id`, `span_id`, and an array-valued field |
| `otel-services-collector-raw-000.*` (38 B) | Same | The same logs, exporter `splunk_hec/raw` (`export_raw: true`) | 2026-09-25 | `/services/collector/raw`, gzip, one body line per log, no query string and no channel |
| `otel-services-collector-00{2..7}.*` (1,746 B) | Same | `telemetrygen metrics --metrics=3` per `--metric-type` (Gauge, Sum, ExponentialHistogram, Histogram), exporters `splunk_hec/metrics` (the default form) and `splunk_hec/metrics_multi` (`use_multi_metric_format: true`); the two exporters' requests for one run arrive in either order | 2026-09-25 | Gauge (`002`, `003`), Sum (`004`, `005`), and Histogram (`006`, `007`) in both forms. The default form is one `metric_name:<n>` field per object, never the `metric_name`/`_value` pair; the multi form merges a histogram's `_count` and `_sum` into one object. `metric_type` is `Gauge`, `Sum`, or `Histogram`; buckets are cumulative `_bucket` objects with an `le` dimension ending `+Inf` |
| `docker-services-collector-event-1-0-00{0,2,4}.*` (0 B) | Docker Engine 29.8.1's `splunk` log driver (`docker:29.8.1-dind`, daemon hostname `splunk-docker-fixture`), `alpine:3.22` containers | One container per `splunk-format`, each echoing a JSON line and a plain line | 2026-09-25 | The driver's connection check before each container starts: `OPTIONS /services/collector/event/1.0`, no `Authorization` |
| `docker-services-collector-event-1-0-001.*` (346 B) | Same | `--log-opt splunk-format=inline` | 2026-09-25 | Concatenated objects, no `Content-Type`; `event` an object `{"line": <the line as text>, "source": "stdout", "tag": <container id>}`; `time` a JSON string with six decimals |
| `docker-services-collector-event-1-0-003.*` (466 B) | Same | `splunk-format=json` plus `splunk-source`, `splunk-sourcetype`, `splunk-index` | 2026-09-25 | The same, with a JSON line's `line` parsed into an object, and all four envelope carriers |
| `docker-services-collector-event-1-0-005.*` (168 B) | Same | `splunk-format=raw` plus `splunk-gzip=true` | 2026-09-25 | `event` a string, `<tag> <line>`; gzip |
| `sc4s-services-collector-event-00{0,1}.*` (144 B) | Splunk Connect for Syslog 3.40.0 (`ghcr.io/splunk/splunk-connect-for-syslog/container3:3.40.0`, syslog-ng 4.22.0) | `SC4S_DEST_SPLUNK_HEC_DEFAULT_URL` at the sink, TLS verification and the disk buffer off | 2026-09-25 | The entrypoint's two `curl` connection tests, `{"event": "HEC TEST EVENT", "sourcetype": "sc4s:probe", "index": "main"}`, `Content-Type: application/x-www-form-urlencoded` (every SC4S request carries it) |
| `sc4s-services-collector-event-00{2,3}.*` (898 B) | Same | Same run | 2026-09-25 | syslog-ng's startup events: sourcetypes `sc4s:events` and `sc4s:events:startup:out`, `fields` of `sc4s_*` keys, numeric and string |
| `sc4s-services-collector-event-00{4..6}.*` (1,726 B) | Same | Three UDP datagrams to port 514 from `splunk-sc4s-lines.txt`: RFC 5424 with structured data, RFC 5424 without, and RFC 3164, all naming host `sc4s-fixture-host` | 2026-09-25 | One request per line: sourcetype `nix:syslog`, index `osnix`, `source` `program:fixture-app`, `time` a string with three decimals, `fields` with `sc4s_syslog_severity` and `sc4s_syslog_facility`; `event` the message after the header |
| `java-services-collector-event-1-0-00{0..5}.*` (1,680 B) | splunk-library-javalogging 1.11.11 (Logback 1.3.16, OkHttp 4.12.0) on Temurin 17.0.20 (`maven:3.9.16-eclipse-temurin-17`) | `tools/record-fixtures/splunk-java/`: INFO (a JSON-object message), WARN (with an MDC value), and ERROR through two `/event` appenders, text and `messageFormat=json`, one request per line, in no fixed order between the two | 2026-09-25 | `Content-Type: application/json; profile="urn:splunk:event:1.0"`; `event` an object `{severity, logger, thread, message, properties}`, the MDC under `properties`; `time` a string with three decimals; the json appender adds `fields` `{"messageFormat":"json"}`. Both appenders sent the JSON-object message as an object |
| `java-services-collector-raw-00{0..2}.*` (209 B) | Same | The `type=raw` appender | 2026-09-25 | `/services/collector/raw?channel=…&host=…&index=…&sourcetype=…&source=…`: the envelope in the query string, `Content-Type: plain/text` |

The whole directory is 17,816 bytes of bodies and sidecars without this README, inside
`../README.md`'s budget.

The run pulls the images above at their pinned tags, and `record_splunk_java` downloads Maven
plugins and the library from Maven Central and Splunk's own repository
(`splunk.jfrog.io/splunk/ext-releases-local`), where `splunk-library-javalogging` is published.

## Privacy

Nothing here describes the machine that recorded it. Every producer ran in a container with a
fixed hostname (`splunk-otel-fixture`, `splunk-docker-fixture`, `splunk-sc4s-fixture`,
`splunk-java-fixture`), and the Docker driver ran in a `docker:dind` daemon rather than this
machine's, so the `host` it stamps is the dind container's. `tools/record-fixtures/check_splunk_capture.py`
runs after every `splunk` recording, decompresses every body, and fails the run if a body or
sidecar holds the recording machine's hostname, `check_datadog_agent_capture.py`'s host markers,
or an IPv4 address outside loopback and Docker's `172.16.0.0/12` (telemetrygen's fixed
`network.peer.address`, `1.2.3.4`, is allowed). The only addresses in the corpus are SC4S's
`sc4s_fromhostip`, a Docker bridge address.

SC4S also posts its own metrics every 30 s, a ~44 KB body per flush. The recorder sends its
syslog lines before the first flush and stops after the seventh request, so none is kept; their
shape is recorded under "What this settled".

## What this settled

Each item was UNVERIFIED in `docs/plans/splunk-relay.md` or ADR `splunk-hec-relay` before these
captures, or is something the captures showed that nobody had written down.

- **The exporter's span object is `hecSpan` in member order, with the protobuf enum names.**
  `otel-services-collector-000` writes `trace_id`, `span_id`, `parent_span_id`, `name`,
  `attributes`, `end_time`, `kind`, `status{message, code}`, `start_time`, the order the codec
  already wrote, but `kind` is `SPAN_KIND_SERVER` and `status.code` is `STATUS_CODE_UNSET`.
  **Changed:** `splunk_hec_out` writes the `SPAN_KIND_*` and `STATUS_CODE_*` names instead of
  `Server` and `Unset`. The decoder already read both spellings. No capture has a span link, so
  the link's `trace_state` member is still read from the exporter's source.
- **The exporter's envelope order is `event`, `fields`, `host`, `source`, `sourcetype`, `index`,
  `time`**, and `time` is the start in seconds as a float (`1790357823.3332317`). The encoder
  writes `time` first with nine decimals; JSON member order and number spelling are permitted
  normalizations.
- **The exporter's default metric form is one `metric_name:<n>` per object, not `metric_name` and
  `_value`.** `use_multi_metric_format` merges a histogram's `_count` and `_sum` into one object and
  leaves buckets one per object. No producer here writes the single-metric pair; Splunk accepts it
  (the `splunk-interop` run).
- **The exporter drops an exponential histogram**: its metrics run sent no request at all, which
  the recipe guarantees by running it before Histogram.
- **The exporter gzips every body, however small**, and sends its health check without a token.
- **The exporter's `export_raw` sends `/raw` with no channel and no query string.**
- **Docker's `splunk` driver checks its endpoint with `OPTIONS /services/collector/event/1.0`**
  (not `GET /health`) and refuses to start a container unless it gets a `200`
  (`docker-…-000`). `splunk_hec_in` answered `405`, so a Docker daemon pointed at it could run no
  container with the driver. **Changed:** `splunk_hec_in` answers `OPTIONS` on every route `200`
  with an empty body and Splunk's `Allow` values, before authentication, as a real Splunk does.
- **The Docker driver sends no `Content-Type` and writes `time` as a string**; `inline` puts the
  line in an `event` object as text, `json` parses a JSON line into that object, and `raw` sends
  `"<tag> <line>"` as a string event.
- **SC4S sends every request `Content-Type: application/x-www-form-urlencoded`**, posts two
  connection tests before syslog-ng starts, and sends one request per syslog line at this rate.
- **SC4S's own metrics have no `event` key, and every measurement is a string**
  (`"metric_name:spl.sc4syslog.dst.written": "0"`, index `_metrics`, sourcetype
  `sc4s:metrics:v2`). The `splunk-interop` run showed Splunk indexes that shape as a metric.
  **Changed:** the decoder reads an object with no `event` whose `fields` carry a measurement as a
  metric event, and a numeric-string measurement as its number; a unit test holds one SC4S object
  to it, since the corpus keeps none.
- **The Java library writes `time` as a string with millisecond decimals, keeps severity inside
  the `event` object** (`severity`, `logger`, `thread`, `message`, `properties`), and its raw
  appender puts the whole envelope in `/raw`'s query string.

## Tests that consume these fixtures

- `crates/logit-proto/tests/splunk_interop.rs` replays every `POST` through `SplunkDecoder` by
  route (`decode_events`, or `decode_raw` with the query-string envelope): nothing rejected,
  skipped, degraded, or diagnosed; every `/event` capture on the codec's fixed point; then the
  facts above (the spans re-encode member for member, both metric forms agree, the log's severity
  and trace reference, the Docker driver's three formats, SC4S's sourcetype and severities, the
  Java appender's severities and query-string envelope).
- `crates/logit-cli/tests/splunk_hec_in_round_trip.rs`'s `every_recorded_request_is_answered_2xx`
  replays every capture over a socket with its recorded method, path, and headers, and expects a
  `2xx`, the `OPTIONS` and `GET` checks included.
- `crates/logit-proto/src/splunk/metrics.rs`'s `sc4s_metrics_with_no_event_and_string_values_decode`
  holds one SC4S metrics object, copied from a recording, to the decoder.
- `script/splunk-interop`'s `hec-relay` leg replays the corpus through `splunk_hec_in` and
  `splunk_hec_out` into a real Splunk (`tools/splunk-interop/README.md`).

## What isn't covered here (yet)

- **Anything a real Splunk decides.** The capture sink answers every request `200`. What Splunk
  Enterprise 10.4.3 accepts, rejects, and indexes is `script/splunk-interop`'s job, recorded in
  `tools/splunk-interop/README.md` and `docs/plans/splunk-relay.md`.
- **Summary and exponential histogram from the exporter.** telemetrygen has no Summary
  generator; its ExponentialHistogram the exporter drops. The codec's Summary shape
  (`<n>_<q>` with `qt`) is still from the exporter's source.
- **Span events and links, and `otel.log.name`.** telemetrygen writes none; the hand-written
  exporter body in `crates/logit-proto/tests/splunk_fixed_point.rs` covers them.
- **Acknowledgment from a client.** None of these clients was configured for `useACK`; the
  `splunk-interop` run covers `splunk_hec_out`'s side against a real Splunk.
- **Vector's `splunk_hec_logs` and `splunk_hec_metrics` sinks, and an Edge Processor.** Not
  recorded; an Edge Processor's HEC destination is Splunk Cloud only.
- **SC4S's metrics body**, too large for the size budget (above).
