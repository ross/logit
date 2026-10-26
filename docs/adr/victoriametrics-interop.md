---
created: 2026-09-24
updated: 2026-10-26
---

# VictoriaMetrics interop: existing components, plus zstd on Prometheus remote-write

## Status
Accepted

## Context

VictoriaMetrics, VictoriaLogs, and VictoriaTraces are standards-first receivers. VictoriaMetrics
ingests Prometheus remote-write 1.0, Prometheus text exposition, InfluxDB line protocol v1 and
v2, Graphite plaintext, OpenTSDB, DataDog and New Relic series, and OTLP metrics over HTTP.
VictoriaLogs adds syslog, Loki push, Elasticsearch bulk, OTLP logs, and JSON lines.
VictoriaTraces takes OTLP traces over HTTP and gRPC and nothing else. Every one of those wires is
one `logit` already speaks, so each of the three products is reachable today with no new
component. [Verified behavior](#verified-behavior) below is the survey of what each product
accepts and emits, checked against the real software.

Two of VictoriaMetrics's wires are its own:

- `/api/v1/import/native` and VictoriaLogs' `/insert/native`, a binary format the docs say "may
  change in incompatible way between releases", meant for `vmctl` and `vlagent`.
- The "VictoriaMetrics remote write protocol": the Prometheus remote-write 1.0 `WriteRequest`
  protobuf with `Content-Encoding: zstd` in place of Snappy. vmagent sends it by default since
  v1.116 and downgrades to Snappy when the receiver answers `415` or `400`.

The data model is a strict subset of `Event`'s: a series is a label set, a millisecond
timestamp, and an `f64`. VictoriaMetrics stores no metric type, unit, description, or
exemplar, accepts remote-write native histograms and converts them to its own `vmrange`
buckets, and doesn't accept remote-write 2.0: it answers `204` and stores nothing. So a
`logit -> VictoriaMetrics` relay loses only what VictoriaMetrics can't hold, and a
`VictoriaMetrics -> logit` relay over `/federate` returns every series untyped.

The workspace keeps zstd out on purpose: the `zstd` crate builds C through `zstd-sys`, which
[ADR `containerized-development`](containerized-development.md) rules out, and
[ADR `native-wire-format-encoding`](native-wire-format-encoding.md) judged the pure-Rust
alternatives uncompetitive when it reserved `Compression::Zstd` in the native frame without
implementing it. `ruzstd` has since gained a real encoder (LZ77 matching plus Huffman and FSE
entropy coding) and a decoder with a bounded window and streaming output, which changes the
receive side of that judgment and part of the send side.

## Decision

**No `victoriametrics_*` kind.** VictoriaMetrics, VictoriaLogs, and VictoriaTraces are served
by the existing standard-protocol components:

| Surface | Component and setting |
|---|---|
| VictoriaMetrics `/api/v1/write` (remote-write 1.0) | `prometheus_out` with `endpoint:` and `version: 1`. `version: 2` gets a `204` with no written count and is stored nowhere, so the sink holds its queue ([ADR `prometheus-remote-write`](prometheus-remote-write.md), "Amendment: a 2.0 write is held to its written count") |
| vmagent scraping `logit` | `prometheus_out` with `bind:` |
| VictoriaMetrics `/api/v2/write` (InfluxDB line protocol) | `influxdb_out` |
| VictoriaMetrics `-graphiteListenAddr` | `graphite_out`, plaintext, `tags: carbon` |
| VictoriaMetrics `/opentelemetry/v1/metrics` | `otlp_out` over HTTP with `endpoint:` at the `/opentelemetry` base |
| VictoriaLogs `/insert/opentelemetry/v1/logs` | `otlp_out` over HTTP with `endpoint:` at the `/insert/opentelemetry` base and the `VL-*` field headers in `headers:` |
| VictoriaLogs syslog listener | `syslog_out` over TCP or TLS |
| VictoriaTraces `/insert/opentelemetry/v1/traces` and its gRPC listener | `otlp_out` over HTTP or gRPC |
| VictoriaMetrics `/federate` | `prometheus_in` with `scrape_targets:` |
| vmagent remote-writing to `logit` | `prometheus_in` with `bind:` |

One `otlp_out` posts every signal it carries to one host, so a pipeline feeding two or three
Victoria products runs one `otlp_out` per product behind a `keep_signals` (or `has_signal`).
A `404` from the wrong product is a permanent fault, not a skip.

**One addition: zstd on Prometheus remote-write, both directions, through `ruzstd`.**

- `prometheus_out`'s send mode gains `compression: snappy | zstd`, default `snappy`. The
  choice is explicit, with no negotiation and no fallback, the same posture
  [ADR `prometheus-remote-write`](prometheus-remote-write.md)'s "The sender's wire version is
  explicit" section gives `version:`. A `415` or `400` from the receiver under `zstd` stays a
  permanent fault whose diagnostic names `compression: snappy` as the remedy.
- `version: 2` with `compression: zstd` is a config-time error under graph rule 56. Remote-write
  2.0 mandates Snappy, and vmagent pairs zstd only with 1.0.
- `prometheus_in`'s receiver accepts `Content-Encoding: zstd` beside `snappy`. Every other
  encoding, and a missing one, stays `415`, because vmagent's downgrade to Snappy keys on
  `415` or `400`, and a receiver that answered anything else would strand a vmagent pointed at
  a `logit` older than this change.
- The decompressed-size cap stays `MAX_REQUEST_BYTES`, checked three ways for zstd: the frame
  header's content size when present, the window size, and a streaming decode that stops one
  byte past the cap. Snappy keeps its `decompress_len` check.
- `ruzstd` is the implementation, scoped to the remote-write codec. Ross chose it on
  2026-09-24 over the C `zstd` crate, accepting the trade-off: `ruzstd`'s encoder reaches
  about libzstd level 1's ratio and no higher, and its decoder runs 1.4 to 3.5 times slower
  than libzstd. The goal is interop with vmagent and VictoriaMetrics on their default wire, not
  bandwidth parity with libzstd. The native frame's `Compression::Zstd` stays reserved and
  rejected.

Compression is a sink-configured transport choice, so under
[ADR `lossless-transit`](lossless-transit.md) it is a permitted normalization: a zstd
`prometheus_out -> prometheus_in` relay is the same fixed point as a Snappy one.

## Alternatives considered

- **A `victoriametrics_out` on `/api/v1/import` JSON lines.** VictoriaMetrics's own line
  format is stable-shaped and simple, but it carries the same label set, millisecond timestamp,
  and value remote-write does, with no metadata, exemplars, or histograms of its own. Rejected:
  nothing over remote-write.
- **A `victoriametrics_out` on `/api/v1/import/native`.** The most efficient wire into
  VictoriaMetrics, and the docs say it may change incompatibly between releases and is meant
  for VictoriaMetrics-to-VictoriaMetrics transfer. Rejected: a relay can't target a format its
  receiver reserves the right to change.
- **A `victoriametrics_in` polling `/api/v1/export`.** Bulk history out of VictoriaMetrics.
  Rejected as niche: `prometheus_in` scraping `/federate` covers live series, and both come
  back untyped.
- **The C `zstd` crate through `zstd-sys`.** Real libzstd ratios and every level. Rejected: it
  reverses [ADR `native-wire-format-encoding`](native-wire-format-encoding.md)'s recorded
  decision and breaks [ADR `containerized-development`](containerized-development.md)'s
  no-C-dependencies property, for a bandwidth gain interop doesn't need.
- **Receive-side zstd only.** A vmagent pointed at `logit` would stay on zstd with no `415`
  round, and `logit -> VictoriaMetrics` would keep sending Snappy, which VictoriaMetrics
  accepts. Rejected: the encoder is the smaller half of the work once the decoder and the
  config seam exist, and a `logit` relay hop in front of VictoriaMetrics should be able to
  send the wire vmagent sends.
- **Auto-negotiating zstd on the sender, downgrading on `415`.** vmagent's own behavior.
  Rejected for the reason [ADR `prometheus-remote-write`](prometheus-remote-write.md) rejects
  version negotiation: the wire format becomes a runtime property the operator can't read from
  the config.

## Consequences

- `ruzstd` joins the workspace as a dependency of `logit-proto`, MIT-licensed and already on
  `deny.toml`'s allow list. Its rationale comment in `Cargo.toml` scopes the earlier "zstd is
  not a dependency" note to the native frame.
- `docs/known-gaps/prometheus.md` gains three rows: an `ExponentialHistogram`
  can't reach VictoriaMetrics's native-histogram ingest over remote-write until `logit` encodes
  native histograms; a `Distribution` is not re-binned onto `vmrange` buckets and goes out as a
  five-quantile summary; and a series scraped back from `/federate` is untyped because
  VictoriaMetrics emits no `# TYPE`.
- `docs/deploying.md`'s "Choosing `version: 1` or `2`" section stops listing VictoriaMetrics
  as a 2.0 receiver.
- A verification harness, `script/victoria-interop` over `tools/victoria-interop/`, runs the
  three products and vmagent in compose and confirms each leg above against real software. It
  is outside `script/cibuild`, like `script/shape-survey`. The recorded vmagent requests it
  captures, on the zstd wire and the Snappy one, join `testdata/interop/prometheus/` and the interop test
  that replays that corpus.
- [Verified behavior](#verified-behavior) records the answers to every unknown, from vmagent's
  header requirements to what VictoriaMetrics does with a delta OTLP sum.

## Verified behavior

Surveyed 2026-09-24 from `docs.victoriametrics.com` and the VictoriaMetrics repository, and
checked against VictoriaMetrics and vmagent v1.152.0, VictoriaLogs v1.52.0, and VictoriaTraces
v0.11.1 by `script/victoria-interop`. "Leg N" is a row in
[`tools/victoria-interop/README.md`](../../tools/victoria-interop/README.md)'s "Recorded run".

### VictoriaMetrics

Single-node listens on `:8428`, vmagent on `:8429`, and a cluster's `vminsert` on `:8480` under
`/insert/<accountID>[:<projectID>]/prometheus/...` (from v1.143.0,
`-enableMultitenancyViaHeaders` takes `AccountID` and `ProjectID` headers instead).

| Ingest | Behavior |
|---|---|
| `POST /api/v1/write` | Remote-write 1.0, Snappy. Remote-write 2.0 isn't supported and isn't refused: v1.152.0 answers it `204` with an empty body and stores nothing, with nothing logged and `vm_http_request_errors_total` unchanged (leg 2). Native histograms are accepted (v1.143.0+) and converted to `vmrange` series |
| The VictoriaMetrics remote write protocol | The 1.0 `WriteRequest` with `Content-Encoding: zstd` and `X-VictoriaMetrics-Remote-Write-Version: 1`. vmagent sends it first and, on a `415` or `400`, repacks the block as Snappy and stays on Snappy for that remote (`-remoteWrite.forceVMProto` disables the downgrade, `-remoteWrite.forcePromProto` the attempt; `app/vmagent/remotewrite/client.go`). VictoriaMetrics requires no version header and ignores `X-Prometheus-Remote-Write-Version` on a zstd body. It doesn't choose a decompressor from `Content-Encoding`: a zstd body labelled `snappy` or unlabelled, and a Snappy body labelled `zstd`, are all stored with a `204` (the probes). So `RESERVED_REMOTE_WRITE_HEADERS` needs no VictoriaMetrics header |
| `POST /api/v1/import` | JSON lines with parallel `values` and `timestamps` arrays, millisecond timestamps, gzip accepted |
| `POST /api/v1/import/native` | VictoriaMetrics's binary format, unstable between releases |
| `POST /api/v1/import/prometheus`, `/api/v1/import/csv` | Prometheus text exposition; CSV columns mapped by `format=` |
| `POST /write`, `POST /api/v2/write` | InfluxDB line protocol. The metric name is `<measurement>_<field>`, tags become labels, and timestamp precision is auto-detected and truncated to ms. `org` and `bucket` become no label (leg 4); `db` is read only from `/write?db=` (`-influxDBLabel`), which `influxdb_out` doesn't send. A bool is stored as 1/0 and a string field as `0`, and an exact duplicate point as a second sample at the same timestamp (leg 11) |
| `-graphiteListenAddr` | Carbon plaintext over TCP and UDP, off by default; `;tag=value` segments become labels (leg 5). No pickle: `-help` has no pickle flag, so `graphite_out` stays `protocol: plaintext` |
| `-opentsdbListenAddr`, `-opentsdbHTTPListenAddr` | OpenTSDB telnet `put` and HTTP `/api/put` |
| `/datadog/api/v1/series`, `/datadog/api/v2/series`, `/datadog/api/beta/sketches` | Datadog series and sketches; no `/datadog/intake` |
| `POST /newrelic/infra/v2/metrics/events/bulk` | New Relic infrastructure events |
| `POST /opentelemetry/v1/metrics` | OTLP metrics over HTTP only, gzip accepted; `-help` has no gRPC flag. A delta `Sum` is stored as its raw points, so `rate()` and `increase()` over it are wrong (leg 6). An `ExponentialHistogram` becomes `_bucket` series with a `vmrange` label (the zero bucket as `-0.000e+00...0.000e+00`) plus `_count` and `_sum`. An empty OTLP scope gains `scope.name="unknown"` and `scope.version="unknown"` labels (`-opentelemetry.promoteScopeMetadata`), and `service.name` becomes a label |

Limits: `-maxLabelsPerTimeseries` defaults to 40 (a series over it is dropped and counted in
`vm_rows_ignored_total`), `-maxLabelValueLen` to 4 KiB, and `-maxLabelNameLen` to 256 bytes
(counted `vm_rows_ignored_total{reason="too_long_label_name"}`). There's no metric-name flag:
the name is the `__name__` label's value.

Data model: a series is a label set, a millisecond timestamp, and a float, with no stored metric
type, so `_bucket`, `_sum`, and `_count` are ordinary series told apart by name. `HELP`, `TYPE`,
and `UNIT` are ignored, and exemplars aren't stored. Stale markers are honored and exported as
JSON `null`. Out-of-order samples and backfill are accepted with no flag. Histograms use a fixed
log-spaced bucket set under `vmrange`, each bucket holding its own count rather than a cumulative
`le` count; MetricsQL's `prometheus_buckets()` bridges the two. Floats lose precision past about
12 significant digits.

| Egress | Behavior |
|---|---|
| `GET /api/v1/export` | The `/api/v1/import` JSON-lines shape; `match[]` required; bounded by `-search.maxExportDuration`. `/api/v1/export/csv` and `/api/v1/export/native` are CSV and the unstable binary format |
| `GET /federate` | Prometheus text exposition for `match[]` (or `match`) with no `# TYPE` or `# HELP`: the last point of each series in the window, with its millisecond timestamp. It also takes `start`, `end`, `max_lookback` (default `5m`, or `step` under `-search.setLookbackToStep`), `extra_label`, `extra_filters[]`, and `timeout`, and returns at most `-search.maxFederateSeries` series (`app/vmselect/prometheus/prometheus.go`'s `FederateHandler`). `prometheus_in` scrapes it as-is, each series a `Gauge` tagged `prometheus.type="untyped"` with its wire timestamp kept (leg 9) |
| `/api/v1/query`, `/api/v1/query_range` | MetricsQL, a PromQL superset with different `rate` and `increase` extrapolation |
| vmagent `-remoteWrite.url` | Remote-write 1.0 to any receiver, zstd first as above; `-streamAggr.config` pre-aggregates by rule |

### VictoriaLogs

Listens on `:9428`. Ingest: `/insert/jsonline` (NDJSON), `/insert/elasticsearch/_bulk`,
`/insert/loki/api/v1/push` (JSON confirmed; protobuf unverified, since no leg sends it),
`/insert/opentelemetry/v1/logs` (HTTP protobuf only; no gRPC flag), syslog through
`-syslog.listenAddr.tcp`, `.udp`, and `.unix` (RFC 3164 and 5424 auto-detected, octet counting
and non-transparent framing both accepted, TLS through `-syslog.tls*`), Datadog logs, journald,
and the unstable `/insert/native`.

A record has `_msg` (required; `-defaultMsgValue` fills an absent one), `_time` (ingest time when
absent), and `_stream`, the label-style rendering of the stream fields. Query parameters or
`VL-*` headers set the field mapping per request, the query string winning: `VL-Msg-Field`,
`VL-Time-Field`, `VL-Stream-Fields`, and `VL-Ignore-Fields`, plus `extra_fields`,
`decolorize_fields`, and `preserve_json_keys`. An OTLP log body maps to `_msg` with no
`VL-Msg-Field` (leg 6), and an empty scope gains `scope.name` and `scope.version` of `unknown`.
Query is `/select/logsql/query` (also the export path) and `/select/logsql/tail`; there's no push
egress.

### VictoriaTraces

Not GA ("currently a work in progress"; first release 2025-07-28). Ingest is OTLP only:
`/insert/opentelemetry/v1/traces` over HTTP on `:10428`, and gRPC on `-otlpGRPCListenAddr`, off
by default with `:4317` recommended. `-otlpGRPC.tls` defaults to `true` and then requires
`-otlpGRPC.tlsCertFile` and `-otlpGRPC.tlsKeyFile`, so plaintext gRPC also needs
`-otlpGRPC.tls=false`. The gRPC listener closes each connection about 5 s after it opens, with a
TCP FIN and no HTTP/2 `GOAWAY` (leg 7). Query is the Jaeger HTTP API under `/select/jaeger/api/`,
plus an experimental Tempo API; there's no push egress.

### `ruzstd` 0.9.0

Its frame-header reader sits in a `pub(crate)` module. What's reachable:
`decoding::FrameDecoder::init(reader)` reads and validates one frame header without decoding a
block, after which `content_size()` returns the declared content size (`0` when none);
`set_max_window_size` caps the window (default 100 MiB), and a frame declaring more fails `init`
with `FrameDecoderError::WindowSizeTooBig`. `StreamingDecoder` decodes one frame per instance,
and `FrameDecoder::decode_all_to_vec` decodes every frame into memory with no bound. The encoder
implements `CompressionLevel::Uncompressed` and `Fastest` only. vmagent's frames set
`Single_Segment_Flag` and declare their content size. `logit_proto::prometheus::compression`'s
three guards rest on these.
