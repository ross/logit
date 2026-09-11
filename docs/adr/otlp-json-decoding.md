---
created: 2026-09-10
updated: 2026-09-10
---

# OTLP/JSON decoding: hand-written against `serde_json::Value`, not generated

## Status
Accepted

## Context

`otlp_in` (`crates/logit-inputs/src/otlp.rs`) has shipped protobuf-only since it landed, rejecting
`Content-Type: application/json` outright with a 415 and an explicit message
(`docs/known-gaps.md`'s now-closed entry). That was a deliberate scope cut, not an oversight — but
it is now a real blocker for one concrete consumer: a real OpenTelemetry-JS browser SDK, which
speaks only OTLP/JSON (`@opentelemetry/exporter-trace-otlp-proto` is Node-only;
protobuf-in-the-browser has been an open upstream request since 2022,
[open-telemetry/opentelemetry-js#3118](https://github.com/open-telemetry/opentelemetry-js/issues/3118)).
More generally, OTLP/JSON is what browsers and a fair number of polyglot SDKs send by default, so
closing this gap is worth having independent of any one demo.

`crates/logit-proto/src/otlp/{common,logs,metrics,traces}.rs` decode/encode against generated
`prost` types committed under `crates/logit-proto/src/otlp/generated/` — see
[ADR `committed-pregenerated-otlp-protobuf`](committed-pregenerated-otlp-protobuf.md), which
settles that those files are `prost-build`'s direct, never-hand-edited output. Any approach to
OTLP/JSON has to either work within that constraint or argue for lifting it.

The OTLP/JSON dialect is not plain proto3 JSON. From the
[OTLP spec](https://opentelemetry.io/docs/specs/otlp/):

- **`traceId`/`spanId`/`parentSpanId` are case-insensitive hex strings, not base64** — OTLP's own
  documented deviation from proto3 JSON's normal bytes-as-base64 rule for `bytes` fields
  ([spec issue #786](https://github.com/open-telemetry/opentelemetry-specification/issues/786) is
  the history). Every other `bytes` field (`AnyValue.bytesValue`) is unmodified proto3 JSON and
  *is* base64.
- 64-bit integers may be a JSON number or a decimal string.
- Enum fields "MUST be encoded as integer values ... the enum name strings MUST NOT be used" —
  stricter than plain proto3 JSON, not more lenient.
- Field names are lowerCamelCase; "Original field names are not valid" (i.e. snake_case is
  non-conformant on the wire, even though most implementations accept it on receipt).

Three shapes were evaluated for producing `EventBatch`es from an OTLP/JSON request body:

1. `pbjson`/`pbjson-build`, generating `serde::Deserialize` impls for the vendored proto types the
   same way `prost-build` generates the types themselves.
2. A hand-written `serde_json`-based decoder producing `EventBatch`es directly, bypassing the
   generated `prost` types entirely.
3. A hand-written `serde_json`-based parser producing the *same* generated `prost` structs the
   protobuf path already decodes into, then reusing `otlp::{decode_resource_logs,
   decode_resource_spans, OtlpDecoder::decode_resource_metrics}` unchanged.

## Decision

**Option 3.** `crates/logit-proto/src/otlp/json/{mod,logs,traces,metrics}.rs` walks a parsed
`serde_json::Value` tree by hand into the existing `LogsData`/`TracesData`/`MetricsData` prost
structs, then feeds them through the exact same decode functions the protobuf path uses. The new
code is purely a dialect-parsing layer — camelCase-or-snake_case keys, hex-vs-base64 bytes,
string-or-number 64-bit ints, enum name-or-number, "absent-or-null means default" — with no signal
semantics of its own.

Exposed as an inherent method, `OtlpDecoder::decode_signal_json`, not a `SignalDecoder` trait
method: `OtlpDecoder` is the trait's only implementor and nothing in the crate is generic over
it, so a trait method would either need a default body (silently giving any future implementor
"JSON unsupported" with no compile error) or force every implementor to answer a question that's
only meaningful for OTLP. See `crate::SignalDecoder`'s own doc comment for the fuller version of
this reasoning.

**Dependencies: `serde_json` and `base64`, both already in `Cargo.lock` at these exact versions**
(`serde_json` transitively through several existing workspace members; `base64 0.22.1` via
`reqwest`/`rustls`) — promoted to `[workspace.dependencies]`, adding zero new crate versions to the
graph. **No `serde` derive** — the walk is plain functions over `serde_json::Value`, not
`#[derive(Deserialize)]` on anything, generated or hand-written.

`otlp_in`'s change (`crates/logit-inputs/src/otlp.rs`) is comparatively small: accept
`application/json` alongside `application/x-protobuf`/`application/protobuf` (matched
case-insensitively — HTTP media types are case-insensitive, and the exact-match check this
replaces was a latent bug), dispatch to `decode_signal_json`, and answer with the same
`Content-Type` the request carried (the spec: "The server MUST use the same Content-Type in the
response as it received in the request") — `application/json` with a body of `{}`, matching what
an all-default `ExportTraceServiceResponse` serializes to in proto3 JSON, and never a zero-length
body, since `opentelemetry-js`'s exporter parses the success body and `JSON.parse("")` throws.
gRPC is unchanged: OTLP/gRPC is defined only over protobuf framing, and no OTel SDK speaks
`application/grpc+json`.

All three signals accept JSON, not traces alone: a per-signal content-type matrix is *more*
branching in `handle_http`, not less, and the shared `AnyValue`/`KeyValue`/`Resource`/scope layer
is most of the cost regardless of how many signals use it. `exemplars` is parsed nowhere in the
JSON layer and always decodes empty — `otlp::metrics::decode_metric` never reads a data point's
`exemplars` field on the *protobuf* path either, so preserving them in JSON only would be
asymmetric effort spent on a field nothing downstream looks at.

## Alternatives considered

- **`pbjson`/`pbjson-build` — rejected on correctness, not preference.** Its `Builder` exposes
  `out_dir`, `register_descriptors`/`register_file_descriptor`, `exclude`, `extern_path`,
  `ignore_unknown_fields`, `ignore_unknown_enum_variants`, `use_integers_for_enums`,
  `emit_fields`, `preserve_proto_field_names`, `retain_enum_prefix`, and `btree_map` — **no
  per-field serializer hook and no alternative encoding for `bytes`**. Generated code routes every
  `bytes` field through `pbjson::private::base64` unconditionally, because `pbjson` implements
  proto3 JSON faithfully, and proto3 JSON says bytes are base64. OTLP's hex trace/span ids are
  exactly where OTLP deviates from that rule. A real 32-character hex `traceId` — what any
  conforming SDK sends — would base64-decode to 24 bytes instead of the 16 hex-decoding produces,
  which `otlp::traces`'s `ids::trace_id` rejects outright ("trace_id must be 16 bytes, got 24");
  every real browser span would fail to decode. A base64-encoded id nobody sends would be silently
  *accepted*. The only workaround is post-editing `generated/*.v1.rs`'s bytes handling by hand,
  which `committed-pregenerated-otlp-protobuf` forbids in as many words ("prost-build's direct
  output (never hand-edited)") and whose Consequences section tells reviewers to expect
  "whole-file rewrites after a deliberate `script/protogen` run, never a hand-edited diff" — an
  `extern_path`-based split (hand-write `Span`, generate the rest) would fracture the generated
  tree along a line the generator doesn't understand. Secondarily: it would add `pbjson` + `serde`
  + `base64` as runtime dependencies to `logit-proto` (this ADR's decision needs only
  `serde_json`+`base64`, no `serde`), require `prost_build::Config::file_descriptor_set_path`
  plumbing in `tools/protogen`, and multiply `generated/`'s ~1,850 lines several-fold with
  lint-and-format-exempt `Visitor` impls for ~30 message types.
- **JSON straight to `EventBatch`, bypassing the generated types entirely.** Rejected: it
  duplicates the mapping tables that are the actual intellectual content of this codec —
  `otlp::metrics` alone is over a thousand lines of exponential-bucket bridging, the
  cumulative-`Sum`→`Gauge` rule, and skip-and-count telemetry. A second copy reachable only via
  JSON would drift silently from the protobuf path, and every existing test proving those rules
  would cover only half the input surface a request can now arrive in.
- **`#[derive(Deserialize)]` directly on the generated prost types.** Rejected for the same reason
  as `pbjson`: it requires hand-editing generated code (or wrapper newtypes defeating the point),
  and derives can't express "hex here, base64 there" or "number or string" without per-field
  `#[serde(with = "...")]` modules that amount to writing this ADR's chosen approach anyway, just
  spread across attribute annotations instead of plain functions.

## Consequences

- **The JSON dialect layer must be updated by hand whenever `generated/` is regenerated from a
  newer OTLP release.** This is the real cost of rejecting a generator for this piece: `prost-build`
  regenerating `generated/*.v1.rs` (a deliberate, reviewed `script/protogen` run) does not
  regenerate `otlp/json/*.rs` alongside it. A new field on a message needs a corresponding hand-
  written accessor before it's reachable from JSON, even though the protobuf path picks it up for
  free the moment prost's struct gains the field. `crates/logit-proto/proto/README.md` should
  carry this warning for whoever runs `script/protogen` next.
- OTLP/JSON requests decode through a `serde_json::Value` tree before ever reaching the generated
  structs, which costs more peak memory per byte than protobuf's direct `prost::Message::decode`
  under the same request-size cap (`MAX_REQUEST_BYTES` in `crates/logit-inputs/src/otlp.rs`) —
  tracked in `docs/known-gaps.md`.
- `otlp_in`'s 4xx/5xx error bodies stay `text/plain` on both encodings. The spec wants a
  protobuf-encoded `google.rpc.Status` message on every error response regardless of request
  encoding; `otlp_in` doesn't build one on the protobuf path today either, so this is a pre-existing
  deviation, not a regression, and building a `Status` encoder is orthogonal to decoding — tracked
  separately in `docs/known-gaps.md` rather than folded into this change.
- Cross-origin browser access to `otlp_in` is still not possible after this change: `handle_http`
  answers any non-POST method, `OPTIONS` included, with a 404, so there is no CORS preflight
  support. A same-origin reverse proxy (what `docs/plans/browser-tracing.md` describes) remains the
  supported path for a browser client; a dedicated `cors:` config surface is a separate feature,
  tracked in `docs/known-gaps.md`.
- `docs/adr/committed-pregenerated-otlp-protobuf.md`'s line "the only new *runtime* dependency this
  PR adds is `prost`" is superseded by this ADR for the OTLP/JSON surface specifically — that
  sentence is left as-is there (it was true of the PR it described) rather than edited after the
  fact, per this repo's convention that an ADR's text doesn't get rewritten once landed.
