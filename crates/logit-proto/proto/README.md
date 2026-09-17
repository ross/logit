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

## Vendored Prometheus prompb `.proto` sources

Fetched verbatim (no local edits) from
[`prometheus/prometheus`](https://github.com/prometheus/prometheus) at:

- **Tag:** `v3.14.0`
- **Commit:** `d7598b7141418fa35be2b5ec5d0fefb634199610`

Files vendored under `prometheus/prompb/`:

```
remote.proto
types.proto
io/prometheus/write/v2/types.proto
```

`remote.proto` (package `prometheus`, the 1.0 `WriteRequest`/`ReadRequest` messages) `import
"types.proto"`s its sibling in the same directory, and both `import "gogoproto/gogo.proto"`; the
2.0 file (package `io.prometheus.write.v2`, self-contained -- it references no `prometheus`-package
type) also imports `gogoproto/gogo.proto` only. Neither prompb file imports the other, so
`tools/protogen`'s Prometheus family passes two include roots: this `prometheus/prompb/` directory
(so `types.proto` and the nested v2 file resolve) and this `proto/` directory itself (so
`gogoproto/gogo.proto` resolves). `gogoproto/gogo.proto`'s own `import
"google/protobuf/descriptor.proto"` resolves against `protoc`'s bundled well-known-types include
path, which needs no vendoring here.

Also vendored, under `gogoproto/`:

```
gogo.proto
```

from [`gogo/protobuf`](https://github.com/gogo/protobuf) at commit
`f67b8970b736e53dbd7d0a27146c8f1ac52f74e5` (the tip of `master` at fetch time; gogo/protobuf cuts
no recent tags). The only gogoproto extension the vendored prompb files use is
`(gogoproto.nullable) = false`, applied only to `repeated` fields -- a no-op for `prost`, which
never wraps a `repeated` field in `Option` regardless of this option, so no prost-build
customization is needed to honor it correctly.

Regenerating this family works the same way as OTLP's above: `script/protogen` overwrites
`crates/logit-proto/src/prometheus/generated/{prometheus.rs,io.prometheus.write.v2.rs}`; review the
diff and commit it by hand. To bump the vendored version: update the tag/commit above, re-fetch each
file from `https://raw.githubusercontent.com/prometheus/prometheus/<tag>/prompb/...` (and
`gogoproto/gogo.proto` from `https://raw.githubusercontent.com/gogo/protobuf/<commit>/gogoproto/gogo.proto`
if it has moved), run `script/protogen`, and review both diffs together.
