# Vendored OTLP `.proto` sources

Fetched verbatim (no local edits) from
[`open-telemetry/opentelemetry-proto`](https://github.com/open-telemetry/opentelemetry-proto) at:

- **Tag:** `v1.11.0`
- **Commit:** `790608c4d51e6ffc12210b541e8514cbed9e91a4`

Files vendored under `opentelemetry/proto/`:

```
common/v1/common.proto
resource/v1/resource.proto
logs/v1/logs.proto
metrics/v1/metrics.proto
trace/v1/trace.proto
collector/logs/v1/logs_service.proto
collector/metrics/v1/metrics_service.proto
collector/trace/v1/trace_service.proto
```

The `collector/*_service.proto` files are vendored for provenance and for PR3 (`feat/otlp-components`,
which hand-rolls the unary gRPC/HTTP transport rather than generating service stubs) -- this PR
(`feat/otlp-codec`) generates Rust types only from the five non-collector files above.
`ExportTraceServiceRequest`/`ExportLogsServiceRequest`/`ExportMetricsServiceRequest` are wire-identical
to `TracesData`/`LogsData`/`MetricsData` (both are exactly `{ repeated Resource*Signal* = 1; }` on the
wire), so `crates/logit-proto/src/otlp` encodes/decodes against the latter and needs no generated
code from the collector protos at all.

## Regenerating

`script/protogen` runs `prost-build` (with `protoc`, inside a throwaway image --
`tools/protogen/Dockerfile` -- never the dev image, per
[ADR `committed-pregenerated-otlp-protobuf`](../../../docs/adr/committed-pregenerated-otlp-protobuf.md)) against these files and
overwrites `crates/logit-proto/src/otlp/generated/*.v1.rs`. Review the diff and commit it by hand;
this is a deliberate, reviewed act, not a CI check (`script/cibuild` never touches `protoc`).

To bump the vendored version: update the tag/commit above, re-fetch each file from
`https://raw.githubusercontent.com/open-telemetry/opentelemetry-proto/<tag>/opentelemetry/proto/...`,
run `script/protogen`, and review both diffs (`.proto` and generated `.rs`) together.
