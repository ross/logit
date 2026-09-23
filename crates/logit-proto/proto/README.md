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

Rust types are generated only from the five non-collector files. The `collector/*_service.proto`
files are vendored for provenance: `otlp_in` and `otlp_out` hand-roll their unary gRPC and HTTP
transport rather than generating service stubs, and they need no generated code from these files.
`ExportTraceServiceRequest`/`ExportLogsServiceRequest`/`ExportMetricsServiceRequest` are
wire-identical to `TracesData`/`LogsData`/`MetricsData` (both are exactly
`{ repeated Resource*Signal* = 1; }` on the wire), so `crates/logit-proto/src/otlp` encodes and
decodes against the latter.

## Regenerating

`script/protogen` runs `prost-build` against these files and overwrites
`crates/logit-proto/src/otlp/generated/*.v1.rs`. It runs `protoc` inside a throwaway image
(`tools/protogen/Dockerfile`), never the dev image, per
[ADR `committed-pregenerated-otlp-protobuf`](../../../docs/adr/committed-pregenerated-otlp-protobuf.md).
Review the diff and commit it by hand. Regeneration is a deliberate, reviewed act, not a CI check:
`script/cibuild` never touches `protoc`.

To bump the vendored version:

1. Update the tag and commit above.
2. Re-fetch each file from
   `https://raw.githubusercontent.com/open-telemetry/opentelemetry-proto/<tag>/opentelemetry/proto/...`.
3. Run `script/protogen`.
4. Review both diffs (`.proto` and generated `.rs`) together.
5. Check the hand-written JSON path, described next.

**`crates/logit-proto/src/otlp/json/` is hand-written against these definitions, not generated
from them, and `script/protogen` doesn't touch it.** It's a dialect-parsing layer over the same
message shapes `generated/*.v1.rs` declares: OTLP/JSON's camelCase-or-snake_case keys, hex-vs-base64
ids, string-or-number 64-bit fields, and enum name-or-number. It isn't generated because `pbjson`,
the natural choice, implements proto3 JSON's bytes-as-base64 rule faithfully, and OTLP's hex
trace/span ids are exactly where OTLP deviates from that rule (see
[ADR `otlp-json-decoding`](../../../docs/adr/otlp-json-decoding.md)). **Bumping the vendored
version is consequently a two-step review, not one**: after `script/protogen` regenerates
`generated/*.v1.rs`, check whether any new, renamed, or retyped field in the diff needs a matching
hand-written accessor in `otlp/json/`. prost's struct gains a field for free; the JSON path
doesn't pick it up automatically.

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

The imports these files make determine the include roots `protoc` needs:

- `remote.proto` (package `prometheus`, the 1.0 `WriteRequest`/`ReadRequest` messages) imports
  its sibling (`import "types.proto"`). Both carry `import "gogoproto/gogo.proto"`.
- The 2.0 file (package `io.prometheus.write.v2`) is self-contained: it references no
  `prometheus`-package type and imports only `gogoproto/gogo.proto`. The 1.0 and 2.0 files never
  import each other.

So `tools/protogen`'s Prometheus family passes **three** include roots
(`tools/protogen/src/main.rs`):

- This `prometheus/prompb/` directory, so `types.proto` and the nested v2 file resolve.
- This `proto/` directory itself, so `gogoproto/gogo.proto` resolves.
- `/usr/include`, so `gogoproto/gogo.proto`'s own `import "google/protobuf/descriptor.proto"`
  resolves.

Debian's `protobuf-compiler` package doesn't ship `/usr/include/google/protobuf/*.proto` on its
own. `tools/protogen/Dockerfile` installs `libprotobuf-dev` specifically to put those
well-known-type sources on disk there. Without it, `protoc` fails with
`google/protobuf/descriptor.proto: File not found`. A re-vendor that drops the third include root
(or the `libprotobuf-dev` install) hits that failure again.

Also vendored, under `gogoproto/`:

```
gogo.proto
```

from [`gogo/protobuf`](https://github.com/gogo/protobuf) at commit
`f67b8970b736e53dbd7d0a27146c8f1ac52f74e5` (the tip of `master` at fetch time; gogo/protobuf cuts
no recent tags). The only gogoproto extension the vendored prompb files use is
`(gogoproto.nullable) = false`, almost always on `repeated` fields. That's a no-op for `prost`,
which never wraps a `repeated` field in `Option` regardless of this option, so prost-build needs no
customization to honor it.

**One exception:** `io.prometheus.write.v2.TimeSeries.metadata`
(`prompb/io/prometheus/write/v2/types.proto:80`,
`Metadata metadata = 5 [(gogoproto.nullable) = false];`) is a **singular** message field, not a
`repeated` one. gogo renders it as a by-value struct whose marshaller always emits field 5, but
`prost` can't honor `nullable = false` on a singular message field and generates
`pub metadata: Option<Metadata>` regardless (`generated/io.prometheus.write.v2.rs:58-59`).
Remote-write 2.0 requires per-series metadata, so **the remote-write codec
(`crates/logit-proto/src/prometheus/remote_write.rs`) must always populate `Some(Metadata { .. })`
on encode**, and treat `None` on decode as "unspecified" rather than as evidence the field can
legitimately be absent on the wire.

To regenerate this family, follow the OTLP steps above: `script/protogen` overwrites
`crates/logit-proto/src/prometheus/generated/{prometheus.rs,io.prometheus.write.v2.rs}`; review the
diff and commit it by hand. To bump the vendored version, update the tag and commit above, re-fetch
each file from `https://raw.githubusercontent.com/prometheus/prometheus/<tag>/prompb/...` (and
`gogoproto/gogo.proto` from `https://raw.githubusercontent.com/gogo/protobuf/<commit>/gogoproto/gogo.proto`
if it has moved), run `script/protogen`, and review both diffs together.
