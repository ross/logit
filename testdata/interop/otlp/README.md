# OTLP interop fixtures

Newline-delimited OTLP/JSON, one line per export batch, exactly as written by the OpenTelemetry
Collector's own `file` exporter (`tools/record-fixtures/otel-collector-config.yaml`) -- **not** a
raw byte capture like `../syslog/`'s fixtures. That's deliberate, not an inconsistency: the `file`
exporter's default marshaler *is* pdata's JSON marshaler, the normative implementation of
OTLP/JSON -- there's no more "real" OTLP/JSON to capture underneath it, and recording it this way
means the fixture is valid regardless of whether the original sender used gRPC or HTTP, protobuf or
JSON, on the wire to the Collector. See `docs/plans/recorded-interop-fixtures.md`'s "How captures
are recorded" section for the full reasoning.

Producer: [`telemetrygen`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen),
the OpenTelemetry project's own load-generation tool -- a real OTel SDK-backed exporter, not a
hand-built payload -- sending real OTLP/gRPC to a real `otel/opentelemetry-collector-contrib:latest`
(v0.160.0 as of this capture) configured with just an `otlp` receiver and three `file` exporters
(one per signal). Regenerate with `script/record-fixtures otlp` (see `../README.md` and
`script/record-fixtures`'s own header comment).

| File | Invocation | Captured | Construct exercised |
|---|---|---|---|
| `traces.json` | `telemetrygen traces --otlp-endpoint otelcol:4317 --otlp-insecure --traces=3` | 2026-09-10 | 3 export batches (one per trace, telemetrygen's own batching choice, not ours), each a `resourceSpans` array with one parent (`kind: SERVER`) and one child (`kind: CLIENT`) span sharing a `traceId`, real `network.peer.address`/`service.peer.name` span attributes, non-nil `parentSpanId` |
| `logs.json` | `telemetrygen logs --otlp-endpoint otelcol:4317 --otlp-insecure --logs=3` | 2026-09-10 | One export batch, `resourceLogs` with 3 `logRecords`, real `severityNumber`/`severityText`, a `droppedAttributesCount` field (present because telemetrygen's default log attribute count exceeds pdata's default limit -- a real batch-marshaling detail a hand-written test literal wouldn't think to include) |
| `metrics.json` | `telemetrygen metrics --otlp-endpoint otelcol:4317 --otlp-insecure --metrics=3` | 2026-09-10 | One export batch, `resourceMetrics` with 3 separate `scopeMetrics` entries, each one `gauge` data point (telemetrygen's default `--metric-type`) |

A fixed `--traces=3`/`--logs=3`/`--metrics=3` (not `--duration`) keeps each re-record the same
*shape*, even though the actual trace/span ids, timestamps, and container-local resource
attributes will differ between runs -- see `../README.md`'s "Consuming these fixtures" section for
why that's fine.

## What isn't covered here (yet)

- **OTLP/protobuf fixtures** -- every fixture here is JSON (by construction, see above); a
  protobuf-shaped fixture would mean capturing `otel-collector-config.yaml`'s receiver traffic
  directly rather than through the `file` exporter, which is a genuinely different (raw-byte)
  capture shape from the rest of this directory. Not attempted yet -- `crates/logit-proto/src/otlp/mod.rs`'s
  existing `OTLP_TRACE_REQUEST` hand-built literal already covers the protobuf decode path; see the
  plan doc's follow-on list.
- **HTTP-transport OTLP** -- `telemetrygen`'s default exporter here is gRPC; `--otlp-http` is a
  one-flag change to `record_otlp`'s invocations if an HTTP-specific fixture is ever needed (OTLP's
  wire *content* is identical either way for what `file` exporter re-emits, so this is a low-value
  addition, not a gap in what's exercised).
