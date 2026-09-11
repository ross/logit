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

**`crates/logit-proto/src/otlp/json/` is hand-written against these definitions, not generated
from them, and `script/protogen` does not touch it.** It's a dialect-parsing layer -- OTLP/JSON's
camelCase-or-snake_case keys, hex-vs-base64 ids, string-or-number 64-bit fields, enum name-or-
number -- over the same message shapes `generated/*.v1.rs` declares (see
[ADR `otlp-json-decoding`](../../../docs/adr/otlp-json-decoding.md) for why it isn't generated
too: `pbjson`, the natural choice, implements proto3 JSON's bytes-as-base64 rule faithfully, and
OTLP's hex trace/span ids are exactly where OTLP deviates from that rule). **Bumping the vendored
version is consequently a two-step review, not one**: after `script/protogen` regenerates
`generated/*.v1.rs`, check whether any new/renamed/retyped field the diff introduces needs a
matching hand-written accessor in `otlp/json/` -- a field prost's struct gains for free, the JSON
path does not pick up automatically.
