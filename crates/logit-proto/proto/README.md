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
`tools/protogen`'s Prometheus family passes **three** include roots (`tools/protogen/src/main.rs`):
this `prometheus/prompb/` directory (so `types.proto` and the nested v2 file resolve), this
`proto/` directory itself (so `gogoproto/gogo.proto` resolves), and `/usr/include` (so
`gogoproto/gogo.proto`'s own `import "google/protobuf/descriptor.proto"` resolves).
`/usr/include/google/protobuf/*.proto` is *not* something Debian's `protobuf-compiler` package
ships on its own -- `tools/protogen/Dockerfile` installs `libprotobuf-dev` specifically to put
those well-known-type sources on disk there; without it `protoc` fails with
`google/protobuf/descriptor.proto: File not found`. A re-vendor that drops the third include root
(or the `libprotobuf-dev` install) will hit that failure again.

Also vendored, under `gogoproto/`:

```
gogo.proto
```

from [`gogo/protobuf`](https://github.com/gogo/protobuf) at commit
`f67b8970b736e53dbd7d0a27146c8f1ac52f74e5` (the tip of `master` at fetch time; gogo/protobuf cuts
no recent tags). The only gogoproto extension the vendored prompb files use is
`(gogoproto.nullable) = false`, almost always applied to `repeated` fields -- a no-op for `prost`,
which never wraps a `repeated` field in `Option` regardless of this option, so no prost-build
customization is needed to honor it correctly there. **One exception:**
`io.prometheus.write.v2.TimeSeries.metadata` (`prompb/io/prometheus/write/v2/types.proto:80`,
`Metadata metadata = 5 [(gogoproto.nullable) = false];`) is a **singular** message field, not a
`repeated` one -- gogo renders it as a by-value struct whose marshaller always emits field 5, but
`prost` has no way to honor `nullable = false` on a singular message field and generates
`pub metadata: Option<Metadata>` regardless (`generated/io.prometheus.write.v2.rs:58-59`). Remote-write
2.0 requires per-series metadata, so **the W2 codec must always populate `Some(Metadata { .. })` on
encode**, and treat `None` on decode as "unspecified" rather than as evidence the field can
legitimately be absent on the wire.

Regenerating this family works the same way as OTLP's above: `script/protogen` overwrites
`crates/logit-proto/src/prometheus/generated/{prometheus.rs,io.prometheus.write.v2.rs}`; review the
diff and commit it by hand. To bump the vendored version: update the tag/commit above, re-fetch each
file from `https://raw.githubusercontent.com/prometheus/prometheus/<tag>/prompb/...` (and
`gogoproto/gogo.proto` from `https://raw.githubusercontent.com/gogo/protobuf/<commit>/gogoproto/gogo.proto`
if it has moved), run `script/protogen`, and review both diffs together.

## Vendored Datadog agent-payload `.proto` sources

Fetched verbatim (no local edits) from
[`DataDog/agent-payload`](https://github.com/DataDog/agent-payload) at:

- **Tag:** `v5.0.211`
- **Commit:** `9584637d1527d2e12d4678372e11a4082f9980af`

File vendored under `datadog/agent-payload/proto/`:

```
metrics/agent_payload.proto
```

`agent_payload.proto` (package `datadog.agentpayload`) `import
"github.com/gogo/protobuf/gogoproto/gogo.proto"`s using a Go-style import path rather than a
relative one. `tools/protogen`'s Datadog family resolves that against a second include root,
`crates/logit-proto/proto/datadog/include/`, which holds nothing but a relative symlink --
`datadog/include/github.com/gogo/protobuf/gogoproto/gogo.proto ->
../../../../../../gogoproto/gogo.proto` -- pointing back at the one vendored `gogoproto/gogo.proto`
Prometheus's family already vendors (see above), so there is exactly one copy of that file in the
repo regardless of how many families import it. A symlink is preferred over a second copy on
purpose; if a future `protoc`/environment refuses to follow it, fall back to a real copy under that
same path and note it here. `/usr/include` is still needed as a third include root, same reason as
Prometheus's: `gogo.proto`'s own `import "google/protobuf/descriptor.proto"`. The only gogoproto
extension the vendored file uses is `(gogoproto.nullable) = false` (on `SketchPayload`'s `sketches`/
`metadata` fields and `Sketch`'s `distributions`/`dogsketches` fields) -- a no-op for `prost`, same
as the prompb files above.

Regenerating this family works the same way as OTLP's and Prometheus's above: `script/protogen`
overwrites `crates/logit-proto/src/datadog/generated/datadog.agentpayload.rs`; review the diff and
commit it by hand. To bump the vendored version: update the tag/commit above, re-fetch
`metrics/agent_payload.proto` from
`https://raw.githubusercontent.com/DataDog/agent-payload/<tag>/proto/metrics/agent_payload.proto`,
run `script/protogen`, and review both diffs together.

## Vendored Datadog Agent trace/stats and sketches-go DDSketch `.proto` sources

Fetched verbatim (no local edits) from
[`DataDog/datadog-agent`](https://github.com/DataDog/datadog-agent) at:

- **Tag:** `7.83.3`
- **Commit:** `8c639c92581e6f5da73f90f1886b21b9a2441bca`

Files vendored under `datadog/datadog-agent/pkg/proto/datadog/trace/`:

```
span.proto              9eb62328fd74b0eb8f3684abc4745b048da316e654977e44351ac2d828e55a09
tracer_payload.proto    390e813a112bff307f9afdf5a4795ee56f616600abf2b33864953a521d84d3fb
agent_payload.proto     8f84d14d2bcb8bd5d8ab204b9d0e521d20a790da23bb10c8f2b422193180865c
stats.proto             bbd27b1eebec01d30c324c82267a0aac30c149352592ae18e33feb361c7189c9
idx/span.proto              275fc4f4ec5f5bcdf9276737152c0112c5e5a11ea03969b13d5112bdef2fb546
idx/tracer_payload.proto    cffba85298181887f11bc0fdbb31d299e0eb7f71da34aeed170bc46ba4764ddb
```

(sha256, in the same order as the file list; `idx/` paths are relative to the same `trace/`
directory.)

All four non-`idx` files declare `package datadog.trace;` and merge into one generated output file
(`datadog.trace.rs`), the same rule as Prometheus's `remote.proto`/`types.proto`. `tracer_payload.proto`
imports its sibling `span.proto`; `agent_payload.proto` (this one, the trace-agent's own, not the
agent-payload family's `metrics/agent_payload.proto` above -- same upstream basename, different
package, different repo) imports both `tracer_payload.proto` and `idx/tracer_payload.proto`.

**`idx/span.proto` and `idx/tracer_payload.proto` (package `datadog.trace.idx`) are vendored only
because `agent_payload.proto` imports `idx/tracer_payload.proto`** for its `idxTracerPayloads`
field -- the string-table-indexed v1.0 wire form the `idx` package describes isn't implemented by
this crate; nothing here constructs an `idx::TracerPayload`. They're vendored (not stubbed or
elided) because `protoc` needs the real import target to resolve, and because a real, complete
upstream file is easier to keep honest across a version bump than a hand-trimmed one.

`tools/protogen`'s Datadog family resolves `import "datadog/trace/..."` against a third include
root, `crates/logit-proto/proto/datadog/datadog-agent/pkg/proto/` (matching upstream's own
`pkg/proto/` layout, so the vendored `import` lines need no rewriting).

`idx/tracer_payload.proto`'s `AgentPayload.idx_tracer_payloads` field (in `datadog.trace.rs`) types
as the bare, unqualified `idx::TracerPayload` -- `datadog.trace.idx` is a genuine **child** package
of `datadog.trace`, not a sibling, so prost-build emits no `super::` at all. See
`crates/logit-proto/src/datadog/generated/mod.rs`'s module doc for how that's satisfied: rather
than a hand-nested level in that file (which can't inject an item into a `#[path] mod trace;`
file-module's fixed content from outside), `tools/protogen` itself appends `idx`'s own `#[path =
"datadog.trace.idx.rs"] pub mod idx;` declaration straight into the generated `datadog.trace.rs`.

Also vendored, verbatim, from [`DataDog/sketches-go`](https://github.com/DataDog/sketches-go) at:

- **Tag:** `v1.4.8`
- **Commit:** `36e98e05d756ccb225b94882831c1443fb4ed535`

File vendored under `datadog/sketches-go/ddsketch/pb/`:

```
ddsketch.proto    8cf53bf5f29a750b015be6e4caac032a2b2856fb4d50797967a861549f006734
```

`ddsketch.proto` has no imports of its own; its root (`crates/logit-proto/proto/datadog/sketches-go/`)
is still a fourth include root because `protoc` requires every input file to sit under some
declared include directory. It declares **`package test;`** -- upstream's own placeholder package
name, never renamed there -- which `tools/protogen`'s `rename_datadog` rewrites on disk from
prost-build's package-derived `test.rs` to `ddsketch.rs` (a plain filename rewrite; the package
name itself, and so the generated code's own internal references, are untouched). It has no
cross-package references, so (like `datadog.agentpayload`) it's one flat module with nothing to
nest.

Regenerating this pair works the same way as the rest of the Datadog family above:
`script/protogen` overwrites `crates/logit-proto/src/datadog/generated/{datadog.trace.rs,
datadog.trace.idx.rs,ddsketch.rs}`; review the diff and commit it by hand. To bump either vendored
version: update the relevant tag/commit above, re-fetch the changed files from
`https://raw.githubusercontent.com/DataDog/datadog-agent/<tag>/pkg/proto/datadog/trace/...` and/or
`https://raw.githubusercontent.com/DataDog/sketches-go/<tag>/ddsketch/pb/ddsketch.proto`, run
`script/protogen`, and review both diffs together.
