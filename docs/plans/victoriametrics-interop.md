---
created: 2026-09-24
updated: 2026-09-24
---

# Enabling plan: VictoriaMetrics — existing components verified, and zstd on remote-write

## Context

`logit` has no VictoriaMetrics-specific component and needs none. VictoriaMetrics,
VictoriaLogs, and VictoriaTraces ingest standard wires that `prometheus_out`, `influxdb_out`,
`graphite_out`, `otlp_out`, and `syslog_out` already speak, and VictoriaMetrics emits over
`/federate` and remote-write, which `prometheus_in` already receives. None of it is verified
against the real software, and the repo's three mentions of VictoriaMetrics (in
`docs/deploying.md`, `docs/adr/prometheus-remote-write.md`, and
`fixtures/prometheus-remote-write-send.yaml`) list it beside Mimir and Thanos as a remote-write
receiver, with one of them implying it takes remote-write 2.0. It doesn't.

The one VictoriaMetrics wire `logit` can't speak is the "VictoriaMetrics remote write
protocol": remote-write 1.0 with zstd in place of Snappy, which vmagent sends by default. This
plan records what the three products accept and emit, verifies every existing leg against
them, adds zstd to Prometheus remote-write in both directions, and documents the result.

Goals:

- Send metrics, logs, and traces to VictoriaMetrics, VictoriaLogs, and VictoriaTraces with the
  components that exist, each leg proven against a real instance.
- Receive from vmagent on its default wire, and scrape VictoriaMetrics back out.
- Make a migration to or from VictoriaMetrics a configuration change.

Non-goals: `/api/v1/import/native` and VictoriaLogs' `/insert/native` (unstable by
VictoriaMetrics's own statement); a JSON-lines `/api/v1/import` sink (nothing over
remote-write); an `/api/v1/export` polling input (`/federate` covers live series); Prometheus
native-histogram encoding (its own follow-up, tracked in `docs/known-gaps.md`); re-binning a
`Distribution` onto `vmrange` buckets; OpenTSDB, DataDog, and New Relic wires into
VictoriaMetrics (each has or will have its own stream); VictoriaMetrics cluster multitenancy
beyond what a URL path or `headers:` entry expresses.

Stream key **`vm`**: branches `vm/w0` through `vm/w3`, a linear stack. PR stack only: nothing is
merged by this workstream; Ross directs merging. The decisions are recorded in
[ADR `victoriametrics-interop`](../adr/victoriametrics-interop.md).

Settled with Ross (2026-09-24): verification happens first (W1), in a compose stack of the
three products and vmagent, because every leg is an existing component and a query against the
backend settles each one in minutes; zstd lands through `ruzstd`, pure Rust, accepting that its
encoder reaches about libzstd level 1 and its decoder runs 1.4 to 3.5 times slower than
libzstd, because the goal is interop on vmagent's default wire, not bandwidth parity; no
`victoriametrics_*` kind.

## Coverage by signal

Each cell reads today → after this stack.

| Signal | To VictoriaMetrics | To VictoriaLogs | To VictoriaTraces | From the Victoria side |
|---|---|---|---|---|
| Metrics | `prometheus_out` remote-write `version: 1` (unverified) → verified, plus `compression: zstd` (W1, W2); `prometheus_out` `bind:` scraped by vmagent (unverified) → verified (W1); `influxdb_out` `/api/v2/write` (unverified) → verified (W1); `graphite_out` plaintext (unverified) → verified (W1); `otlp_out` HTTP `/opentelemetry` (unverified) → verified (W1) | — | — | `prometheus_in` scraping `/federate` (unverified) → verified, untyped (W1); `prometheus_in` `bind:` from vmagent: works after one `415` downgrade → accepted on zstd directly (W1, W2) |
| Logs | — | `otlp_out` HTTP `/insert/opentelemetry` with `VL-*` headers (unverified) → verified (W1); `syslog_out` TCP (unverified) → verified (W1) | — | none; VictoriaLogs has no push egress (non-goal) |
| Traces | — | — | `otlp_out` HTTP and gRPC (unverified) → verified (W1) | none; VictoriaTraces serves the Jaeger query API only (non-goal) |

## What VictoriaMetrics accepts and emits

Surveyed 2026-09-24 from `docs.victoriametrics.com` and the VictoriaMetrics repository
(`app/vmagent/remotewrite/client.go` for the downgrade). W1 checked the items this survey marked
UNVERIFIED against VictoriaMetrics and vmagent v1.152.0, VictoriaLogs v1.52.0, and VictoriaTraces
v0.11.1 in `script/victoria-interop`'s stack, and each now states what it found and how. The one
item no W1 leg exercises keeps its mark.

### VictoriaMetrics

Single-node listens on `:8428`; vmagent on `:8429`; a cluster's `vminsert` on `:8480` with
paths under `/insert/<accountID>[:<projectID>]/prometheus/...` (from v1.143.0,
`-enableMultitenancyViaHeaders` accepts `AccountID`/`ProjectID` headers instead).

| Ingest | Details |
|---|---|
| `POST /api/v1/write` | Remote-write 1.0: `Content-Type: application/x-protobuf`, `Content-Encoding: snappy`. Remote-write 2.0 is not accepted ("still marked as experimental and is not currently supported"), and not refused either: v1.152.0 answers a 2.0 request `204` with an empty body and stores nothing, with nothing in its log and `vm_http_request_errors_total` unchanged ("Findings", leg 2). Native histograms are accepted (v1.143.0+) and converted to `vmrange` series |
| The VictoriaMetrics remote write protocol | The same 1.0 `WriteRequest`, `Content-Encoding: zstd`, `X-VictoriaMetrics-Remote-Write-Version: 1`, no `X-Prometheus-Remote-Write-Version`. vmagent sends it first and, on a `415` or `400`, repacks the block as Snappy and stays on Snappy for that remote (`-remoteWrite.forceVMProto` disables the downgrade; `-remoteWrite.forcePromProto` disables the attempt). VictoriaMetrics doesn't require the `X-VictoriaMetrics-Remote-Write-Version` header, and ignores `X-Prometheus-Remote-Write-Version` on a zstd body. It doesn't read `Content-Encoding` to choose a decompressor either: it stored the committed `vmagent-zstd-000` capture sent with vmagent's header, with no version header, with `X-Prometheus-Remote-Write-Version: 0.1.0` instead, labelled `snappy`, and with no `Content-Encoding`, and stored a Snappy body labelled `zstd`, each with a `204` (`script/victoria-interop`'s probes) |
| `POST /api/v1/import` | JSON lines: `{"metric":{"__name__":"up","job":"..."},"values":[0,0],"timestamps":[1549891472010,...]}`, parallel arrays, millisecond timestamps, `Content-Encoding: gzip` accepted |
| `POST /api/v1/import/native` | VictoriaMetrics's binary format, "may change in incompatible way between releases" |
| `POST /api/v1/import/prometheus` | Prometheus text exposition |
| `POST /api/v1/import/csv` | Columns mapped by a `format=` parameter |
| `POST /write`, `POST /api/v2/write` | InfluxDB line protocol v1 and v2. Metric name is `<measurement>_<field>` (`-influxMeasurementFieldSeparator`); tags become labels; timestamp precision is auto-detected and truncated to ms. `org` and `bucket` become no label, and `/api/v2/write` carries no `db`: `influxdb_out`'s gauge `vi_influx_gauge` arrived as `vi_influx_gauge_value{leg="influx"}` and nothing else (leg 4). `db` is `/write?db=`'s query parameter, labelled per `-influxDBLabel` |
| `-graphiteListenAddr` | Carbon plaintext over TCP and UDP, off by default; `;tag=value` segments become labels (leg 5). No pickle: v1.152.0's `-help` describes `-graphiteListenAddr` as "Graphite plaintext data" and has no pickle flag |
| `-opentsdbListenAddr`, `-opentsdbHTTPListenAddr` | OpenTSDB telnet `put` and HTTP `/api/put` |
| `/datadog/api/v1/series`, `/datadog/api/v2/series`, `/datadog/api/beta/sketches` | DataDog series and sketches; `/datadog/intake` is not supported |
| `POST /newrelic/infra/v2/metrics/events/bulk` | New Relic infrastructure events |
| `POST /opentelemetry/v1/metrics` | OTLP metrics over HTTP, protobuf, `Content-Encoding: gzip` accepted. No OTLP/gRPC: v1.152.0's `-help` has no gRPC listener flag. A delta `Sum` is stored as its raw points, not a running total: leg 6's `vi_otlp_delta_sum` holds `1` at every timestamp, so `rate()` and `increase()` over it are wrong. An `ExponentialHistogram` becomes `_bucket` series with a `vmrange` label (the zero bucket as `-0.000e+00...0.000e+00`) plus `_count` and `_sum`. A point with an empty OTLP scope gains `scope.name="unknown"` and `scope.version="unknown"` labels (`-opentelemetry.promoteScopeMetadata`), and `service.name` becomes a label |

Limits: `-maxLabelsPerTimeseries` default 40 (a series over it is dropped and counted in
`vm_rows_ignored_total`); `-maxLabelValueLen` default 4 KiB; `-maxLabelNameLen` default 256
bytes, a longer name counted `vm_rows_ignored_total{reason="too_long_label_name"}`
(v1.152.0's `-help`). `-help` has no metric-name flag: the name is the `__name__` label's value.

Data model: a series is a label set, a millisecond timestamp, and a float. There is no stored
metric type: `_bucket`, `_sum`, and `_count` are ordinary series told apart only by name.
`HELP`, `TYPE`, and `UNIT` are ignored. Exemplars are not stored (`/api/v1/query_exemplars`
is a stub). Stale markers are honored and exported as JSON `null`. Out-of-order samples and
backfill are accepted with no flag. VictoriaMetrics histograms use a fixed log-spaced bucket
set under a `vmrange` label, storing each bucket's own count rather than a cumulative `le`
count; `prometheus_buckets()` bridges the two in MetricsQL. Floats lose precision past about 12
significant digits.

| Egress | Details |
|---|---|
| `GET /api/v1/export` | The `/api/v1/import` JSON-lines shape; `match[]` required, `start`/`end`, `max_rows_per_line`; bounded by `-search.maxExportDuration` |
| `GET /api/v1/export/csv`, `GET /api/v1/export/native` | CSV by `format=`; the binary format, unstable |
| `GET /federate` | Prometheus text exposition for `match[]` (or `match`), with no `# TYPE` or `# HELP` lines: the last point of each series in the window, with its millisecond timestamp (`vi_rw1_gauge{leg="rw1"} 42 1790280068767`). It also takes `start`, `end`, `max_lookback` (default `5m`; `step` under `-search.setLookbackToStep`), `extra_label`, `extra_filters[]`, and `timeout`, and returns at most `-search.maxFederateSeries` series (`app/vmselect/prometheus/prometheus.go`'s `FederateHandler` and `getCommonParams`; the docs' "Federation" section). `prometheus_in` scrapes it as-is (leg 9) |
| `/api/v1/query`, `/api/v1/query_range` | MetricsQL, a PromQL superset with different `rate`/`increase` extrapolation |
| vmagent `-remoteWrite.url` | Remote-write 1.0 to any receiver, zstd first as above; optional `-streamAggr.config` pre-aggregation by rule |

### VictoriaLogs

Listens on `:9428`. Ingest paths: `/insert/jsonline` (NDJSON, `Content-Type:
application/stream+json`), `/insert/elasticsearch/_bulk`, `/insert/loki/api/v1/push` (JSON
confirmed; protobuf UNVERIFIED, since no W1 leg sends it), `/insert/opentelemetry/v1/logs`
(HTTP protobuf only: v1.52.0's `-help` has no gRPC flag), syslog via `-syslog.listenAddr.tcp`/`.udp`/`.unix` (RFC 3164 and 5424
auto-detected, octet-counting and non-transparent framing both accepted, TLS via
`-syslog.tls*`), DataDog logs, journald, and `/insert/native` (for `vlagent`, unstable).

Record model: `_msg` is the message (required; `-defaultMsgValue` fills an absent one), `_time`
the timestamp (ingest time when absent), `_stream` the label-style rendering of the fields
named as stream fields. The field mapping is set per request by query parameters or headers
(the query string wins): `_msg_field`/`VL-Msg-Field`, `_time_field`/`VL-Time-Field`,
`_stream_fields`/`VL-Stream-Fields`, `ignore_fields`/`VL-Ignore-Fields`, plus
`extra_fields`, `decolorize_fields`, and `preserve_json_keys`. An OTLP log body maps to `_msg`
with no `VL-Msg-Field`: leg 6 sends the same logs through two sinks, one with
`VL-Msg-Field: body` and one without, and both store the body as `_msg`. Like VictoriaMetrics,
VictoriaLogs adds `scope.name` and `scope.version` fields of `unknown` for an empty scope.

Query: `/select/logsql/query` streams JSON lines and doubles as export (`format=csv`
available); `/select/logsql/tail` for live tailing. No push egress.

### VictoriaTraces

Not GA: the repository README says "currently a work in progress", first release 2025-07-28,
latest v0.11.1 (2026-09-16). Ingest is OTLP only: `/insert/opentelemetry/v1/traces` over HTTP
on `:10428`, and gRPC on `-otlpGRPCListenAddr`, which is off by default ("The recommended port is
':4317'"). `-otlpGRPC.tls` defaults to `true` and then requires `-otlpGRPC.tlsCertFile` and
`-otlpGRPC.tlsKeyFile`, so a plaintext gRPC listener also needs `-otlpGRPC.tls=false` (v0.11.1's
`-help`). The gRPC listener closes each connection about five seconds after it opens, with a TCP
FIN and no HTTP/2 `GOAWAY` ("Findings", leg 7). Query is the Jaeger HTTP API under
`/select/jaeger/api/`, plus an experimental Tempo API. No push egress.

### Unverified, settled by W1

Each item as W1 found it against the versions above; "leg N" is a row in "Findings".

1. **VictoriaMetrics requires no version header from a zstd sender, and ignores
   `X-Prometheus-Remote-Write-Version` on one.** `script/victoria-interop` replays the committed
   `vmagent-zstd-000` capture with vmagent's `X-VictoriaMetrics-Remote-Write-Version: 1`, with no
   version header, and with `X-Prometheus-Remote-Write-Version: 0.1.0` instead: all three answer
   `204` and are stored. VictoriaMetrics doesn't choose a decompressor from `Content-Encoding` at
   all: the zstd body labelled `snappy` or sent with no `Content-Encoding`, and a Snappy body
   labelled `zstd`, are stored too. So `RESERVED_REMOTE_WRITE_HEADERS` gains nothing in W2
   (Design §3).
2. **`204` with an empty body, and nothing stored.** VictoriaMetrics v1.152.0 doesn't refuse a
   2.0 request: `prometheus_out` `version: 2` saw only successes (leg 2), and the committed
   `prometheus-v2-000` capture replayed directly gets `204` and no series. Nothing is logged and
   `vm_http_request_errors_total{path="/api/v1/write"}` stays `0`. The data is lost silently.
3. **None of them.** `influxdb_out`'s `/api/v2/write?org=vi-org&bucket=vi-bucket` stores
   `vi_influx_gauge_value{leg="influx"}` and no other label (leg 4). `db` is only read from
   `/write`'s `?db=` (`-influxDBLabel`, default `db`), which `influxdb_out` doesn't send.
4. **A delta `Sum` is stored as its raw points; an `ExponentialHistogram` becomes `vmrange`
   buckets.** Leg 6's `vi_otlp_delta_sum` holds `1` at every timestamp, beside a cumulative
   `vi_otlp_cumulative_sum` that climbs: VictoriaMetrics keeps no temporality, so a delta series
   reads as a gauge of per-interval increments. `vi_otlp_exphist` arrives as `_bucket` series
   with one `vmrange` label per populated bucket, the zero bucket as `-0.000e+00...0.000e+00`,
   plus `_count` and `_sum`.
5. **Neither.** Neither VictoriaMetrics v1.152.0's nor VictoriaLogs v1.52.0's `-help` lists a
   gRPC listener or flag; both take OTLP over HTTP only. VictoriaTraces is the one product with
   an OTLP gRPC listener.
6. **`-otlpGRPCListenAddr`, off by default, recommended `:4317`, with TLS on by default.**
   v0.11.1's `-help`: "Defaults to empty, which means it is disabled. The recommended port is
   ':4317'", and `-otlpGRPC.tls` "is set to true by default, and -otlpGRPC.tlsCertFile and
   -otlpGRPC.tlsKeyFile must be set", so plaintext needs `-otlpGRPC.tls=false`.
7. **Not needed.** Leg 6's two VictoriaLogs sinks differ only in `VL-Msg-Field: body`, and both
   store the OTLP body as `_msg`.
8. **No pickle.** VictoriaMetrics v1.152.0's `-help` describes `-graphiteListenAddr` as the
   address "to listen for Graphite plaintext data" and has no pickle flag. `graphite_out` must
   stay `protocol: plaintext` against it.
9. **`/federate` also takes `start`, `end`, `max_lookback`, `extra_label`, `extra_filters[]`, and
   `timeout`, and `prometheus_in` scrapes its untyped output as-is.** From
   `app/vmselect/prometheus/prometheus.go`'s `FederateHandler` and `getCommonParams` and the
   docs' "Federation" section: `match[]` (or `match`), `start`/`end`, `max_lookback` (default
   `5m`, or `step` under `-search.setLookbackToStep`), `extra_label`, `extra_filters[]`, and
   `timeout`; output is the last point per series with its millisecond timestamp and no `# TYPE`
   or `# HELP`, capped at `-search.maxFederateSeries`. Leg 9 scrapes it every 5 s with no error
   and no skip: each series comes back a `Gauge` tagged `prometheus.type="untyped"`, with its
   wire timestamp kept (`prometheus.timestamp=true`).
10. **`ruzstd` 0.9.0 exposes the content size but not the window size.** Its frame-header reader
    (`decoding::frame::read_frame_header`, `FrameHeader::frame_content_size`,
    `FrameHeader::window_size`) sits in a `pub(crate) mod frame` and isn't reachable. What is:
    `decoding::FrameDecoder::init(reader)` reads and validates one frame header without decoding
    a block, then `FrameDecoder::content_size()` returns the declared content size;
    `set_max_window_size` caps the window (default `DEFAULT_MAX_WINDOW_SIZE`, 100 MiB), and a
    frame declaring more fails `init` with `FrameDecoderError::WindowSizeTooBig`.
    `StreamingDecoder` decodes one frame per instance (concatenated frames need a new decoder per
    frame); `FrameDecoder::decode_all_to_vec` loops over frames but decodes into memory with no
    bound. The encoder implements `CompressionLevel::Uncompressed` and `Fastest` only. (Sources:
    docs.rs/ruzstd/0.9.0; `ruzstd/src/decoding/mod.rs` and `frame_decoder.rs` in
    KillingSpark/zstd-rs.) For W2's three guards (Design §4) that means: the content-size check
    via `init` plus `content_size()`, the window cap via `set_max_window_size`, and the
    `Read::take(max + 1)` streaming bound around a per-frame loop. vmagent's frames set
    `Single_Segment_Flag` and declare their content size, so against vmagent the first guard
    always has a number to check.

## VictoriaMetrics's data against `Event`

| VictoriaMetrics concept | `Event` representation | Verdict |
|---|---|---|
| A series: labels, ms timestamp, float | metric name, attributes, `Event::timestamp` in ns, `Gauge` or `Sum` | ms truncation is the permitted normalization remote-write already has |
| `Sum` cumulative, `Gauge`, classic `Histogram`, `Summary` | plain series, as at any Prometheus receiver | lossless from VictoriaMetrics's point of view |
| `Sum` delta, `GaugeDelta` | skipped and counted by `prometheus_out`; VictoriaMetrics has no temporality | an `aggregate` with `temporality: cumulative` upstream, as today |
| `ExponentialHistogram` | VictoriaMetrics accepts a remote-write native histogram and converts it to `vmrange` | blocked by `logit`'s own native-histogram gap (`docs/known-gaps.md`, Prometheus) |
| `Distribution` | today a five-quantile summary; `vmrange` is also log-bucketed | re-binning not attempted; a known gap, not a loss VictoriaMetrics imposes |
| `unit`, `description`, exemplars, `flags` | dropped by VictoriaMetrics | VictoriaMetrics's limitation, nothing to do |
| `Resource`, `Scope` | flattened to labels by `prometheus_out` | as today |
| A series scraped back from `/federate` | `Gauge`, untyped | VictoriaMetrics emits no `# TYPE` |
| A VictoriaLogs record: `_msg`, `_time`, `_stream`, fields | `LogRecord` body, `Event::timestamp`, the resource and attributes `VL-Stream-Fields` names | lossless; VictoriaLogs has no egress to check the other direction |
| A VictoriaTraces span | OTLP, already `otlp_in -> otlp_out` | — |

## Design

Each item is a decision the ADR records, and names the workstream that builds it.

### 1. Verification is a compose harness, and the only committed artifacts are captures (W1)

`script/victoria-interop` drives `tools/victoria-interop/compose.yaml` (pinned
`victoriametrics/victoria-metrics`, `victoria-logs`, `victoria-traces`, `vmagent`, and `logit`
from the release image) on the host with `$DOCKER`, the way `script/shape-survey` and
`script/record-fixtures` do, and confirms each leg by querying the backend: VictoriaMetrics
`/api/v1/series` and `/api/v1/export`, VictoriaLogs `/select/logsql/query`, VictoriaTraces'
Jaeger API. It prints one pass or fail row per leg. It is not in `script/cibuild`, and no test
depends on it running.

What W1 commits: the harness, one `logit` config per leg under `tools/victoria-interop/`
(added to `script/validate` and `every_shipped_config_loads_and_validates`), the vmagent scrape
config, a `vmagent` producer in `script/record-fixtures` that captures vmagent's zstd and Snappy
wires, one sample request and one metadata request each, into `testdata/interop/prometheus/`
with provenance rows, and the
"Findings" table below, filled in.

### 2. The config type (W2)

`prometheus_out` gains `compression: snappy | zstd`, default `snappy`, a
`RemoteWriteCompression` enum in `logit_config` (the native frame's enum is already named
`Compression`). The field name matches `otlp_out`, `logit_out`, and `stdio_out`. The operator
doc says: what each value is, that `zstd` is accepted by VictoriaMetrics, vmagent, and `logit`'s
own receiver and rejected by Prometheus and Mimir, that there is no negotiation, and that it
needs `version: 1` and send mode.

### 3. Graph rule 56 grows two checks (W2)

Bind mode rejects a non-default `compression`, beside its existing `version`, `timeout`,
`headers`, and `endpoint_tls` checks; send mode rejects `version: 2` with `compression: zstd`.
No new rule number. `RESERVED_REMOTE_WRITE_HEADERS` already covers `content-encoding`;
`X-VictoriaMetrics-Remote-Write-Version` joins it only if W1's item 1 says VictoriaMetrics
needs it.

### 4. A shared compression seam with three bomb guards (W2)

`logit_proto::prometheus::compression` holds `Encoding { Snappy, Zstd }`, `from_header`,
`compress`, and `decompress_bounded(encoding, body, max)` returning `Malformed` or
`TooLarge { declared }`. The Snappy arm keeps the `decompress_len` check that runs before a byte
is expanded. The zstd arm guards three ways, because a zstd frame's content size is optional
and a header can claim any window: the frame header's content size when present, a cap on the
window size, and a streaming decode through `Read::take(max + 1)` that counts across
concatenated frames (the `inflate` pattern `otlp_in` already uses for gzip). `ruzstd` is a
dependency of `logit-proto` only, with a `Cargo.toml` rationale comment that scopes the earlier
"zstd is not a dependency" note to the native frame.

### 5. Receiver and sender (W2)

`prometheus_in`'s `write_response` gates on `Encoding::from_header`; anything else stays `415`
with a message naming both encodings. `decompress_bounded` replaces the inline Snappy block,
mapping `TooLarge` to `413` and `Malformed` to `400`. `logit.input.writes{class="ok"}` gains an
`encoding` tag. `prometheus_out`'s send mode sets the matching `Content-Encoding` and
compresses through the seam; a `415` or `400` under zstd adds "receiver may not accept zstd;
set `compression: snappy`" to the `remote_write_rejected` diagnostic and stays
`Fault::Permanent`.

### 6. Reuse (W1, W2)

`tools/shape-survey/lib.sh`'s namespaced compose plumbing; `tools/record-fixtures/raw_capture.py
--proto http` as the vmagent capture target; `crates/logit-proto/tests/prometheus_remote_write_interop.rs`'s
`Capture` and `all_captures`, made encoding-aware from the `.headers` sidecar; the canned server
in `crates/logit-outputs/src/prometheus.rs`'s tests; `otlp_in`'s bounded `inflate`; graph rule
56's mode checks.

### 7. Not in this stack

Native-histogram encoding on `prometheus_out` (which would let an `ExponentialHistogram` reach
`vmrange`); `Distribution` to `vmrange` re-binning; zstd in the native frame; an
`/api/v1/export` input; VictoriaMetrics cluster tenancy beyond a path or header.

## Workstreams

| # | Branch | PR title | Size | Depends on |
|---|---|---|---|---|
| W0 | `vm/w0` | `vm/w0: ADR and plan for VictoriaMetrics interop` | S | — |
| W1 | `vm/w1` | `vm/w1: verify logit against VictoriaMetrics, VictoriaLogs, VictoriaTraces, and vmagent` | M | W0 |
| W2 | `vm/w2` | `vm/w2: zstd on Prometheus remote-write, both directions` | M | W1 |
| W3 | `vm/w3` | `vm/w3: VictoriaMetrics deploying guide, examples, known gaps` | S | W2 |

Landing order: W0 → W1 → W2 → W3, linear. Each PR is based on and targets its parent's branch
and is brought up to date with `git merge origin/main`, never a rebase. `script/vm` is the
perf-VM tool, so nothing in this stream is named `script/vm-*`.

### W1's legs

Each becomes a row in "Findings": worked, fixed (with the commit), or gap (with the
`docs/known-gaps.md` row W3 adds).

1. `prometheus_out` `version: 1` → VictoriaMetrics `/api/v1/write`.
2. `prometheus_out` `version: 2` → VictoriaMetrics, expecting a rejection; record the status
   and body.
3. `prometheus_out` `bind:` scraped by vmagent.
4. `influxdb_out` → VictoriaMetrics `/api/v2/write`; record which of `org`, `bucket`, and `db`
   become labels.
5. `graphite_out` plaintext, `tags: carbon` → VictoriaMetrics `:2003`.
6. `otlp_out` over HTTP, one sink per product behind `keep_signals`: metrics →
   VictoriaMetrics `/opentelemetry` (a delta `Sum` beside a cumulative one; an
   `ExponentialHistogram`); logs → VictoriaLogs `/insert/opentelemetry` with
   `VL-Stream-Fields` and `VL-Msg-Field` in `headers:`; traces → VictoriaTraces
   `/insert/opentelemetry`.
7. `otlp_out` over gRPC → VictoriaTraces.
8. `syslog_out` over TCP → VictoriaLogs syslog; confirm octet-counting is detected.
9. `prometheus_in` scraping VictoriaMetrics `/federate?match[]=...`; the series come back
   untyped.
10. vmagent → `prometheus_in` `bind:` before W2: one `logit.input.writes{class="unsupported"}`
    (the `415`), then Snappy with `class="ok"`. This proves the downgrade against the receiver
    as it is.

## Findings

From `script/victoria-interop` on 2026-09-24, at `73ca4931`, with `logit` built from that tree.
Image tags: `victoriametrics/victoria-metrics:v1.152.0`, `victoriametrics/vmagent:v1.152.0`,
`victoriametrics/victoria-logs:v1.52.0`, `victoriametrics/victoria-traces:v0.11.1`. The harness
prints `PASS`, `GAP`, or `FAIL` per leg; the result column here is the plan's worked, fixed, or
gap. Legs 1z and 10 were rerun on 2026-09-24 at `cacf17bf` (W2 landed), after adding a permanent
zstd leg (1z) and extending leg 10's check for the codec change; their rows and the verbatim block
below reflect that rerun, the rest reflect the original run.

| Leg | Result | Detail |
|---|---|---|
| 1 | Worked | `vi_rw1_gauge` and `vi_rw1_requests_total` (a `Sum` through `aggregate` `temporality: cumulative`) stored with their `leg` label and nothing added |
| 1z | Worked | `prometheus_out` `compression: zstd` (W2): `vi_rw1z_gauge` and `vi_rw1z_requests_total` stored via a zstd-compressed remote-write body, same as leg 1 otherwise |
| 2 | Gap | VictoriaMetrics answers a remote-write 2.0 request `204` with an empty body and stores nothing, with nothing logged, so `prometheus_out` `version: 2` reports every batch delivered while all of it is lost. Replaying the committed `prometheus-v2-000` capture gives the same `204` |
| 3 | Worked | vmagent scrapes `prometheus_out`'s `bind:` and remote-writes `vi_expose_gauge` and `vi_expose_requests_total` to VictoriaMetrics with `job="logit-expose"` and `instance` added |
| 4 | Worked | `vi_influx_gauge` arrives as `vi_influx_gauge_value{leg="influx"}` (`<measurement>_<field>`); none of `org`, `bucket`, and `db` becomes a label |
| 5 | Worked | `vi_graphite_gauge{leg="graphite"}`: carbon's `;leg=graphite` segment is a label |
| 6 | Worked | Metrics, logs, and traces each reach their product through one `otlp_out` behind a `keep_signals`. VictoriaMetrics stores the delta `Sum` as raw per-interval points and the `ExponentialHistogram` as `vmrange` buckets plus `_count`/`_sum`; VictoriaLogs stores the OTLP body as `_msg` with or without `VL-Msg-Field` and `service.name` as the stream; VictoriaTraces serves the spans under `victoria-interop-otlp-http` on its Jaeger API. Both VictoriaMetrics and VictoriaLogs add `scope.name="unknown"` and `scope.version="unknown"` to a record with an empty OTLP scope |
| 7 | Gap, plus a fix | Spans arrive, but VictoriaTraces's gRPC listener closes every connection about 5 s after it opens with a TCP FIN and no HTTP/2 `GOAWAY` (packet capture: the FIN lands 5.0 s after connect, whatever is in flight). A request in flight at that moment fails as ambiguous and `otlp_out` drops the batch. At 1 batch/s the first stack, before the fix below, logged a `send_failed` warning every 6 s; an isolated 20 s rerun after the fix saw 3 closes and 2 dropped batches. The harness run recorded here reported `PASS` because no request raced a close inside its window. The same capture showed every export stalled 40 ms between its HEADERS and DATA frames, Nagle against delayed ACK; fixed in `73ca4931` (`fix(outputs): set TCP_NODELAY on otlp_out's gRPC connections`) |
| 8 | Worked | 53 records under `app_name:vi-syslog`, `_msg` the message with no length prefix, so VictoriaLogs detects `syslog_out`'s octet counting; `format=rfc5424`, stream `{app_name, hostname, proc_id}` |
| 9 | Worked | `prometheus_in` scrapes `/federate?match[]=vi_rw1_gauge` every 5 s: `vi_rw1_gauge` comes back a `Gauge` with `prometheus.type="untyped"` and its wire timestamp kept |
| 10 | Fixed, plus W2 | Before W2: vmagent's first zstd request got `415` (`logit.input.writes{class="unsupported"}` = 1), vmagent logged "Downgrading protocol from VictoriaMetrics to Prometheus remote write for all future requests", and every later request was Snappy and `class="ok"`. Fixed in `9bce48fd` (`fix(proto): sort a remote-write label set instead of skipping it`) for the label-sorting bug the original leg-10 row described. After W2 (`prometheus_in` accepts zstd), vmagent's first request already succeeds: `class="ok",encoding="zstd"` for every write (22 in the window), `class="unsupported"` stays 0, and vmagent's log never shows the downgrade; both `vi_expose_*` series arrive with nothing skipped |

The run's own table, verbatim (legs 1z and 10 from the `cacf17bf` rerun, the rest from the
original `73ca4931` run):

```
| 1 | prometheus_out version: 1 -> VictoriaMetrics /api/v1/write | PASS | vi_rw1_gauge, vi_rw1_requests_total stored (62 points), labels ['leg'] |
| 1z | prometheus_out version: 1, compression: zstd -> VictoriaMetrics /api/v1/write | PASS | vi_rw1z_gauge, vi_rw1z_requests_total stored (63 points) via compression: zstd, labels ['leg'] |
| 2 | prometheus_out version: 2 -> VictoriaMetrics | GAP | VictoriaMetrics answered every 2.0 request 2xx and stored nothing; prometheus_out logged no rejection (see the probe row for the status) |
| 3 | prometheus_out bind: <- vmagent scrape -> VictoriaMetrics | PASS | vi_expose_gauge, vi_expose_requests_total scraped by vmagent, labels ['instance', 'job', 'leg'] |
| 4 | influxdb_out -> VictoriaMetrics /api/v2/write | PASS | vi_influx_gauge_value, labels ['leg']; of org/bucket/db, none became labels |
| 5 | graphite_out plaintext, tags: carbon -> VictoriaMetrics :2003 | PASS | vi_graphite_gauge, labels ['leg'] (the ;leg= tag is a label) |
| 6 | otlp_out HTTP -> VictoriaMetrics, VictoriaLogs, VictoriaTraces | PASS | metrics ['vi_otlp_cumulative_sum', 'vi_otlp_delta_sum', 'vi_otlp_exphist_bucket', 'vi_otlp_exphist_count', 'vi_otlp_exphist_sum']; delta sum stored as values [1]; exphist as 3 vmrange buckets; logs[default] _msg='victoria-interop otlp-http log 54' _stream={service.name="victoria-interop-otlp-http"}; logs[msg-field] _msg='victoria-interop otlp-http log 54' _stream={service.name="victoria-interop-otlp-http"}; traces: 20 with vi-otlp-http-span |
| 7 | otlp_out gRPC -> VictoriaTraces | PASS | 20 traces with vi-otlp-grpc-span in VictoriaTraces |
| 8 | syslog_out TCP -> VictoriaLogs syslog | PASS | 53 records, _msg='victoria-interop syslog line 52' (no length prefix: octet counting detected), format=rfc5424, _stream={app_name="vi-syslog",hostname="vi-syslog-host",proc_id="-"} |
| 9 | prometheus_in scrape <- VictoriaMetrics /federate | PASS | 10 scrapes of vi_rw1_gauge, rendered `vi_rw1_gauge gauge=42`, prometheus.type=untyped, wire timestamp kept |
| 10 | vmagent remote-write -> prometheus_in bind: | PASS | writes class=ok,encoding=zstd 22 (class=ok total 22), class=unsupported 0; vmagent log does not show the downgrade; received ['vi_expose_gauge', 'vi_expose_requests_total'] vi_expose_* series; 0 series skipped |
```

### Gaps for W3

Each becomes a `docs/known-gaps.md` row or `docs/deploying.md` guidance in W3, unless noted.

- **VictoriaMetrics silently discards remote-write 2.0** (leg 2). `prometheus_out` can't tell a
  `204` that stored nothing from one that stored everything, so the guide says `version: 1`
  for VictoriaMetrics, and `docs/deploying.md`'s "Choosing `version: 1` or `2`" says why.
- **VictoriaTraces's gRPC listener drops the connection under an in-flight request** (leg 7), and
  `otlp_out` loses that batch as an ambiguous failure. Until VictoriaTraces sends a `GOAWAY` (an
  upstream report), the guide recommends OTLP over HTTP for VictoriaTraces. Whether `otlp_out`
  should retry a gRPC request that got no response frame before the connection closed is a
  larger question than this stream, since without a `GOAWAY` the request may have been
  processed. `check.py`'s leg-7 row can pass a run in which no request raced a close; it
  counts `send_failed` lines but can't force the race.
- **A delta `Sum` sent over OTLP is stored as raw points** (leg 6, item 4). The guide puts an
  `aggregate` with `temporality: cumulative` ahead of `otlp_out` to VictoriaMetrics, as it
  already must be ahead of `prometheus_out`.
- **Empty OTLP scopes cost two labels per series.** VictoriaMetrics and VictoriaLogs add
  `scope.name="unknown"` and `scope.version="unknown"` when `otlp_out` sends no scope; the guide
  names VictoriaMetrics's `-opentelemetry.promoteScopeMetadata=false`, or a `lua` stage that
  sets `scope`.

## Verification

- Per PR: `script/cibuild` green; `script/schema` regenerated and committed by W2;
  `script/validate` for W1 and W3, which add configs.
- W0 (this PR) is documentation only: every relative link resolves, and `docs/adr/README.md`
  and `docs/plans/README.md` each gained a row.
- W1: `script/victoria-interop` prints a row for all ten legs; each is transcribed into
  "Findings" with the image tags; `script/record-fixtures vmagent` yields two captures whose
  sidecars read `content-encoding: zstd` and `content-encoding: snappy`; vmagent's own log
  shows the downgrade against the unchanged receiver; every item in "Unverified, settled by W1"
  has a recorded answer and the text above is updated.
- W2: unit tests for a zstd body that declares a content size over the cap (`413` before
  decoding), one with no declared size that inflates past it (`413`), a corrupt body (`400`),
  and `Content-Encoding: gzip` (still `415`); rule 56 tests for a bind-mode `compression:` and
  for `version: 2` with `zstd`; the interop test decodes both vmagent captures with nothing
  skipped; a zstd `prometheus_out -> prometheus_in` round trip is a fixed point; W1's legs 1
  and 10 re-run, with the zstd sender accepted by VictoriaMetrics and vmagent staying on zstd
  against `prometheus_in`, counted `logit.input.writes{class="ok",encoding="zstd"}`.
- W3: the shipped-config test and `script/validate` cover `fixtures/victoriametrics-*.yaml`;
  each `docs/known-gaps.md` row links to the "Findings" row it comes from;
  `docs/deploying.md`'s "Choosing `version: 1` or `2`" no longer lists VictoriaMetrics as a
  2.0 receiver.
