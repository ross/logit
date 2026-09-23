# OTLP interop fixtures

Newline-delimited OTLP/JSON, one line per export batch, exactly as written by the OpenTelemetry
Collector's own `file` exporter (`tools/record-fixtures/otel-collector-config.yaml`). Unlike
`../syslog/`'s fixtures, these are **not** a raw byte capture, and that's deliberate: the `file`
exporter's default marshaler *is* pdata's JSON marshaler, the normative implementation of
OTLP/JSON. There's no more "real" OTLP/JSON to capture underneath it, and recording it this way
means the fixture is valid whether the original sender used gRPC or HTTP, protobuf or JSON, on the
wire to the Collector. `docs/plans/recorded-interop-fixtures.md`'s "How captures are recorded"
section has the full reasoning.

To regenerate, run `script/record-fixtures otlp`. See `../README.md` and the header comment in
`script/record-fixtures`.

## Fixtures

The producer is
[`telemetrygen`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen),
the OpenTelemetry project's own load-generation tool. It's a real OTel SDK-backed exporter, not a
hand-built payload, and it sends real OTLP/gRPC to a real
`otel/opentelemetry-collector-contrib:latest` (v0.160.0 as of this capture). The Collector is
configured with only an `otlp` receiver and three `file` exporters, one per signal.

| File | Invocation | Captured | Construct exercised |
|---|---|---|---|
| `traces.json` | `telemetrygen traces --otlp-endpoint otelcol:4317 --otlp-insecure --traces=3` | 2026-09-10 | 3 export batches, one per trace (telemetrygen's own batching choice, not ours). Each is a `resourceSpans` array with one parent span (`kind: CLIENT`, `lets-go`) and one child span (`kind: SERVER`, `okey-dokey-0`) sharing a `traceId`, with real `network.peer.address`/`service.peer.name` span attributes and a non-nil `parentSpanId` on the child |
| `logs.json` | `telemetrygen logs --otlp-endpoint otelcol:4317 --otlp-insecure --logs=3` | 2026-09-10 | One export batch: `resourceLogs` with 3 `logRecords`, real `severityNumber`/`severityText`, and a `droppedAttributesCount` field. That field is present because telemetrygen's default log attribute count exceeds pdata's default limit, a real batch-marshaling detail that a hand-written test literal wouldn't think to include |
| `metrics.json` | `telemetrygen metrics --otlp-endpoint otelcol:4317 --otlp-insecure --metrics=3` | 2026-09-10 | One export batch: `resourceMetrics` with 3 separate `scopeMetrics` entries, each holding one `gauge` data point (telemetrygen's default `--metric-type`) |

A fixed `--traces=3`/`--logs=3`/`--metrics=3`, not `--duration`, keeps each re-record the same
*shape*. The actual trace and span IDs, timestamps, and container-local resource attributes still
differ between runs. `../README.md`'s "Consuming these fixtures" section explains why that's fine.

## Tests that consume these fixtures

None yet. The OTLP/JSON decoder these fixtures are meant to back-fill now exists
(`crates/logit-proto/src/otlp/json/`), but no `interop_fixture_*`-style test reads these files.
`docs/plans/recorded-interop-fixtures.md`'s "Explicitly deferred" list tracks that test.

## What isn't covered here (yet)

- **OTLP/protobuf fixtures.** Every fixture here is JSON, by construction (see above). A
  protobuf-shaped fixture means capturing `otel-collector-config.yaml`'s receiver traffic directly
  rather than through the `file` exporter, which is a genuinely different, raw-byte capture shape
  from the rest of this directory. It isn't attempted yet: `crates/logit-proto/src/otlp/mod.rs`'s
  existing hand-built `OTLP_TRACE_REQUEST` literal already covers the protobuf decode path. See the
  plan doc's follow-on list.
- **HTTP-transport OTLP.** `telemetrygen`'s default exporter here is gRPC. If an HTTP-specific
  fixture is ever needed, `--otlp-http` is a one-flag change to `record_otlp`'s invocations. OTLP's
  wire *content* is identical either way for what the `file` exporter re-emits, so this is a
  low-value addition, not a gap in what's exercised.
