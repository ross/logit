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
`examples/prometheus-remote-write-send.yaml`) list it beside Mimir and Thanos as a remote-write
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
(`app/vmagent/remotewrite/client.go` for the downgrade). Items marked UNVERIFIED were not
confirmed by a current official page or by source; W1 confirms each against the compose stack
and this section is updated then.

### VictoriaMetrics

Single-node listens on `:8428`; vmagent on `:8429`; a cluster's `vminsert` on `:8480` with
paths under `/insert/<accountID>[:<projectID>]/prometheus/...` (from v1.143.0,
`-enableMultitenancyViaHeaders` accepts `AccountID`/`ProjectID` headers instead).

| Ingest | Details |
|---|---|
| `POST /api/v1/write` | Remote-write 1.0: `Content-Type: application/x-protobuf`, `Content-Encoding: snappy`. Remote-write 2.0 is not accepted ("still marked as experimental and is not currently supported"). Native histograms are accepted (v1.143.0+) and converted to `vmrange` series |
| The VictoriaMetrics remote write protocol | The same 1.0 `WriteRequest`, `Content-Encoding: zstd`, `X-VictoriaMetrics-Remote-Write-Version: 1`, no `X-Prometheus-Remote-Write-Version`. vmagent sends it first and, on a `415` or `400`, repacks the block as Snappy and stays on Snappy for that remote (`-remoteWrite.forceVMProto` disables the downgrade; `-remoteWrite.forcePromProto` disables the attempt). Whether VictoriaMetrics itself requires the `X-VictoriaMetrics-Remote-Write-Version` header from a zstd sender is UNVERIFIED |
| `POST /api/v1/import` | JSON lines: `{"metric":{"__name__":"up","job":"..."},"values":[0,0],"timestamps":[1549891472010,...]}`, parallel arrays, millisecond timestamps, `Content-Encoding: gzip` accepted |
| `POST /api/v1/import/native` | VictoriaMetrics's binary format, "may change in incompatible way between releases" |
| `POST /api/v1/import/prometheus` | Prometheus text exposition |
| `POST /api/v1/import/csv` | Columns mapped by a `format=` parameter |
| `POST /write`, `POST /api/v2/write` | InfluxDB line protocol v1 and v2. Metric name is `<measurement>_<field>` (`-influxMeasurementFieldSeparator`); tags become labels; timestamp precision is auto-detected and truncated to ms. What VictoriaMetrics does with `org`, `bucket`, and `db` is UNVERIFIED |
| `-graphiteListenAddr` | Carbon plaintext over TCP and UDP, off by default; `;tag=value` segments become labels. Pickle support is UNVERIFIED |
| `-opentsdbListenAddr`, `-opentsdbHTTPListenAddr` | OpenTSDB telnet `put` and HTTP `/api/put` |
| `/datadog/api/v1/series`, `/datadog/api/v2/series`, `/datadog/api/beta/sketches` | DataDog series and sketches; `/datadog/intake` is not supported |
| `POST /newrelic/infra/v2/metrics/events/bulk` | New Relic infrastructure events |
| `POST /opentelemetry/v1/metrics` | OTLP metrics over HTTP, protobuf, `Content-Encoding: gzip` accepted. OTLP/gRPC for metrics is UNVERIFIED. What VictoriaMetrics does with a delta `Sum` and with an `ExponentialHistogram` over OTLP is UNVERIFIED |

Limits: `-maxLabelsPerTimeseries` default 40 (a series over it is dropped and counted in
`vm_rows_ignored_total`); `-maxLabelValueLen` default 4 KiB; `-maxLabelNameLen` default 256
bytes (secondary source, UNVERIFIED); no metric-name-length cap found (UNVERIFIED).

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
| `GET /federate` | Prometheus text exposition for `match[]`, with no `# TYPE` lines. Parameters beyond `match[]` are UNVERIFIED |
| `/api/v1/query`, `/api/v1/query_range` | MetricsQL, a PromQL superset with different `rate`/`increase` extrapolation |
| vmagent `-remoteWrite.url` | Remote-write 1.0 to any receiver, zstd first as above; optional `-streamAggr.config` pre-aggregation by rule |

### VictoriaLogs

Listens on `:9428`. Ingest paths: `/insert/jsonline` (NDJSON, `Content-Type:
application/stream+json`), `/insert/elasticsearch/_bulk`, `/insert/loki/api/v1/push` (JSON
confirmed; protobuf UNVERIFIED), `/insert/opentelemetry/v1/logs` (HTTP protobuf; gRPC
UNVERIFIED), syslog via `-syslog.listenAddr.tcp`/`.udp`/`.unix` (RFC 3164 and 5424
auto-detected, octet-counting and non-transparent framing both accepted, TLS via
`-syslog.tls*`), DataDog logs, journald, and `/insert/native` (for `vlagent`, unstable).

Record model: `_msg` is the message (required; `-defaultMsgValue` fills an absent one), `_time`
the timestamp (ingest time when absent), `_stream` the label-style rendering of the fields
named as stream fields. The field mapping is set per request by query parameters or headers
(the query string wins): `_msg_field`/`VL-Msg-Field`, `_time_field`/`VL-Time-Field`,
`_stream_fields`/`VL-Stream-Fields`, `ignore_fields`/`VL-Ignore-Fields`, plus
`extra_fields`, `decolorize_fields`, and `preserve_json_keys`. Whether an OTLP log body maps to
`_msg` without `VL-Msg-Field` is UNVERIFIED.

Query: `/select/logsql/query` streams JSON lines and doubles as export (`format=csv`
available); `/select/logsql/tail` for live tailing. No push egress.

### VictoriaTraces

Not GA: the repository README says "currently a work in progress", first release 2025-07-28,
latest v0.11.1 (2026-09-16). Ingest is OTLP only: `/insert/opentelemetry/v1/traces` over HTTP,
and gRPC on a separate listener whose flag name and default port are UNVERIFIED. Query is the
Jaeger HTTP API under `/select/jaeger/api/`, plus an experimental Tempo API. No push egress.

### Unverified, to be settled by W1

1. Whether VictoriaMetrics requires `X-VictoriaMetrics-Remote-Write-Version` from a zstd
   sender, and what it does with `X-Prometheus-Remote-Write-Version` on one.
2. The status and body VictoriaMetrics returns to a remote-write 2.0 request.
3. Which of `org`, `bucket`, and `db` become labels on `/api/v2/write`.
4. VictoriaMetrics's handling of a delta OTLP `Sum` and of an `ExponentialHistogram` over OTLP.
5. OTLP/gRPC support for VictoriaMetrics metrics and VictoriaLogs logs.
6. VictoriaTraces' OTLP gRPC flag and default port.
7. Whether VictoriaLogs needs `VL-Msg-Field` for OTLP logs.
8. Graphite pickle support in VictoriaMetrics.
9. `/federate`'s parameter set beyond `match[]`, and its handling of a scrape from
   `prometheus_in` with no `# TYPE` lines.
10. `ruzstd`'s public accessor for a frame's content size and window size before decoding.

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
config, a `vmagent` producer in `script/record-fixtures` that captures one zstd and one Snappy
remote-write request into `testdata/interop/prometheus/` with provenance rows, and the
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

Filled by W1. Image tags used: (W1 records them here).

| Leg | Result | Detail |
|---|---|---|
| 1 | | |
| 2 | | |
| 3 | | |
| 4 | | |
| 5 | | |
| 6 | | |
| 7 | | |
| 8 | | |
| 9 | | |
| 10 | | |

## Verification

- Per PR: `script/cibuild` green; `script/schema` regenerated and committed by W2;
  `script/validate` for W1 and W3, which add configs.
- W0 (this PR) is documentation only: every relative link resolves, and `docs/adr/README.md`
  and `docs/plans/README.md` each gained a row.
- W1: `script/victoria-interop` prints a row for all ten legs; each is transcribed into
  "Findings" with the image tags; `script/record-fixtures vmagent` yields two captures whose
  sidecars read `content-encoding: zstd` and `content-encoding: snappy`; vmagent's own log
  shows the downgrade against the unchanged receiver; every item in "Unverified, to be settled
  by W1" has a recorded answer and the text above is updated.
- W2: unit tests for a zstd body that declares a content size over the cap (`413` before
  decoding), one with no declared size that inflates past it (`413`), a corrupt body (`400`),
  and `Content-Encoding: gzip` (still `415`); rule 56 tests for a bind-mode `compression:` and
  for `version: 2` with `zstd`; the interop test decodes both vmagent captures with nothing
  skipped; a zstd `prometheus_out -> prometheus_in` round trip is a fixed point; W1's legs 1
  and 10 re-run, with the zstd sender accepted by VictoriaMetrics and vmagent staying on zstd
  against `prometheus_in`, counted `logit.input.writes{class="ok",encoding="zstd"}`.
- W3: the shipped-config test and `script/validate` cover `examples/victoriametrics-*.yaml`;
  each `docs/known-gaps.md` row links to the "Findings" row it comes from;
  `docs/deploying.md`'s "Choosing `version: 1` or `2`" no longer lists VictoriaMetrics as a
  2.0 receiver.
