---
created: 2026-09-02
updated: 2026-09-02
---

# Hand-rolled unary gRPC over `hyper`, not `tonic`

## Status
Accepted

## Context

`otlp_in`/`otlp_out` (`crates/logit-inputs/src/otlp.rs`, `crates/logit-outputs/src/otlp.rs`) need
to speak both OTLP transports a real deployment expects: OTLP/HTTP (a plain protobuf-body POST,
already well served by `reqwest` on the client side and a plain HTTP server on the listener side)
and OTLP/gRPC (`:4317` in the demo, and every OTel SDK's default exporter transport). Only the gRPC
half needs new infrastructure — HTTP/2 framing with trailers, and gRPC's own status-code vocabulary.

Unary gRPC over HTTP/2 is small and fully specified: `POST <method>`,
`content-type: application/grpc+proto`, `te: trailers`, a body framed as
`[compressed:u8][len:u32 BE][protobuf]`, and a response with the identical framing plus
`grpc-status`/`grpc-message` trailers. `otlp_in`/`otlp_out` only ever need the three unary `Export`
RPCs OTLP defines — no streaming, no reflection, no health checking, no load balancing, no
interceptors.

## Decision

**Hand-roll the gRPC client and server directly against `hyper` 1.x** (`hyper::client::conn::http2`
for the client half, `hyper::server::conn::http2` for the server half), plus `hyper-util`
(`TokioExecutor`/`TokioIo`, and `hyper_util::server::conn::auto` for `otlp_in`'s HTTP transport,
which has to handle both HTTP/1.1 and h2c). `http`/`http-body-util` round out the direct
dependencies — all four already resolve in `Cargo.lock` at the exact versions `reqwest` (an
existing dependency) itself depends on transitively, so this adds no new version of any of them to
the dependency graph; `script/audit`'s `[bans] multiple-versions` check confirmed this rather than
assumed it. `reqwest` stays the OTLP/HTTP client (`otlp_out`'s HTTP transport) — nothing about this
decision touches that path.

The framing (`grpc_frame`/`grpc_unframe`) and the tiny hand-rolled `Export*ServiceResponse`
parse/encode (`parse_partial_success`/`export_response`) are each under 40 lines, duplicated once
per crate rather than shared — consistent with this codebase's existing precedent for small,
protocol-specific pieces (hand-rolled statsd, hand-rolled syslog UDP framing, hand-rolled varint
handling wherever it's needed) rather than a shared "otlp transport" crate for two ~150-200 line
files. The one non-trivial piece of hand-rolled infrastructure is `GrpcBody`
(`crates/logit-inputs/src/otlp.rs`): a `hyper::body::Body` impl that yields exactly one data frame
then one trailers frame, since `http_body_util::Full` has no trailers concept at all and a unary
gRPC response needs exactly one.

## Alternatives considered

- **`tonic`.** `tonic` 0.14's server is built on an `axum` router, which pulls in `axum` + `h2` +
  `tower` + `tower-http` + `tonic-prost` — real weight against a workspace that has none of `axum`/
  `tower`/`tower-http` today and already pins `hyper`/`http` through `reqwest`, for ~95% unused
  surface (streaming, reflection, health checking, interceptors, load balancing) that OTLP's three
  unary `Export` methods never touch. Rejected — the dependency cost buys nothing this PR needs.
- **Raw `h2` (the crate underneath both `hyper` and `tonic`'s HTTP/2 support).** Would mean
  reimplementing connection lifecycle management (accept loop, h1/h2c protocol sniffing for
  `otlp_in`'s HTTP transport, keep-alive) that `hyper`/`hyper-util` already provide as tested,
  general infrastructure. Rejected — strictly more hand-rolled surface than `hyper` for no benefit,
  since `hyper` already sits on top of `h2` for exactly this purpose.
- **HTTP-only (skip gRPC entirely for this PR).** Rejected: `:4317` is the default OTLP transport
  every OTel SDK and the OTel Collector's own exporter reach for first, and the demo (PR4) pushes to
  Tempo's gRPC receiver specifically, to exercise the transport a smaller HTTP-first split would
  otherwise have let this series skip a PR longer. Shipping only OTLP/HTTP would leave the more
  commonly-used transport unimplemented.

## Consequences

- `otlp_in`'s gRPC server is the largest single risk item in this PR (`docs/plans/
  otlp-end-to-end.md` budgets it as roughly half the PR's effort) — HTTP/2 trailers,
  per-method routing, and gRPC status-code mapping are all things `tonic` would have supplied for
  free. The round-trip tests (`otlp_output_to_otlp_input_round_trips_a_batch_through_http`/
  `_through_grpc`, `crates/logit-cli/tests/otlp_round_trip.rs`) are the confidence check this buys
  back: they exercise the hand-rolled gRPC client and server against each other, with no external
  service, and are the strongest evidence either half is wire-correct.
- **Compression is not supported.** Real gRPC/OTLP-HTTP peers commonly default to `gzip` — the OTel
  Collector's own default exporter does — so `otlp_in` rejects a `grpc-encoding`/`Content-Encoding`
  request outright (`grpc-status: 12`/`415`) rather than silently mishandling it, and `otlp_out`
  never requests it. This is a known, documented gap (`docs/known-gaps.md`), not a bug: adding
  `flate2` and per-frame decompression is real, security-relevant surface (a compression bomb
  against untrusted input) that a from-scratch gRPC server shouldn't take on speculatively.
- Every new HTTP/gRPC connection this PR's server side accepts gets its own `tokio::spawn`ed
  handler — a slow or hostile client's connection can't block another's, and `otlp_in` is now the
  first listener in this codebase with real backpressure to its source (a blocked `sink.send`
  blocks the handler, which stalls that connection) rather than a UDP listener's silent
  kernel-level drop; see `docs/design/pipeline-graph.md`'s backpressure section.
- No new transitive dependency version enters the graph (`h2`, pulled in by enabling `hyper`'s
  `http2` feature, resolves to the same version `reqwest` already pins) — confirmed via
  `script/audit`, not assumed. `deny.toml` needed no edit.
