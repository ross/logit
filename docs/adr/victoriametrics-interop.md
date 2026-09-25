---
created: 2026-09-24
updated: 2026-09-24
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
component. [`docs/plans/victoriametrics-interop.md`](../plans/victoriametrics-interop.md)'s
"What VictoriaMetrics accepts and emits" section is the survey; its "Coverage by signal" table
maps every surface to the component that serves it.

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
| VictoriaMetrics `/api/v1/write` (remote-write 1.0) | `prometheus_out` with `endpoint:` and `version: 1`. `version: 2` gets a `204` and is stored nowhere |
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
- `docs/known-gaps.md`'s Prometheus section gains three rows: an `ExponentialHistogram` can't
  reach VictoriaMetrics's native-histogram ingest over remote-write until `logit` encodes native
  histograms; a `Distribution` is not re-binned onto `vmrange` buckets and goes out as a
  five-quantile summary; and a series scraped back from `/federate` is untyped because
  VictoriaMetrics emits no `# TYPE`.
- `docs/deploying.md`'s "Choosing `version: 1` or `2`" section stops listing VictoriaMetrics
  as a 2.0 receiver.
- A verification harness, `script/victoria-interop` over `tools/victoria-interop/`, runs the
  three products and vmagent in compose and confirms each leg above against real software. It
  is outside `script/cibuild`, like `script/shape-survey`. The recorded vmagent requests it
  captures, on the zstd wire and the Snappy one, join `testdata/interop/prometheus/` and the interop test
  that replays that corpus.
- Every remaining unknown, from vmagent's header requirements to what VictoriaMetrics does with
  a delta OTLP sum, is listed in the plan's "Unverified, settled by W1" section and
  answered there, not here.
