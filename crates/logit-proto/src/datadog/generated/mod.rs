//! Hand-written module tree wrapping four `prost-build` output files -- never regenerated itself,
//! `script/protogen` only ever (re)writes `datadog.agentpayload.rs`, `datadog.trace.rs`,
//! `datadog.trace.idx.rs`, and `ddsketch.rs`. See
//! [ADR `committed-pregenerated-otlp-protobuf`](../../../../../docs/adr/committed-pregenerated-otlp-protobuf.md)
//! (this crate's OTLP family is where that ADR is written up; this third family follows the same
//! scheme) and `crates/logit-proto/proto/README.md`'s "Vendored Datadog" section for the pinned
//! tags/commits:
//!
//! - `datadog.agentpayload.rs` -- `DataDog/agent-payload` `v5.0.211`, one file, one package
//!   (`datadog.agentpayload`).
//! - `datadog.trace.rs` -- `DataDog/datadog-agent` `7.83.3`'s `pkg/proto/datadog/trace/{span,
//!   tracer_payload,agent_payload,stats}.proto`, all four declaring `package datadog.trace;`, so
//!   they merge into this one output file (same rule as Prometheus's `remote.proto`/`types.proto`).
//! - `datadog.trace.idx.rs` -- the same tag's `pkg/proto/datadog/trace/idx/{span,
//!   tracer_payload}.proto` (package `datadog.trace.idx`), vendored *only* because
//!   `agent_payload.proto` (the trace one) imports `idx/tracer_payload.proto` for its
//!   `idxTracerPayloads` field -- the v1.0 (string-table-indexed) wire form itself isn't
//!   implemented, so nothing in this crate constructs an `idx::TracerPayload` today.
//! - `ddsketch.rs` -- `DataDog/sketches-go` `v1.4.8`'s `ddsketch/pb/ddsketch.proto`, renamed from
//!   prost-build's package-derived `test.rs` (upstream's own placeholder package name) by
//!   `tools/protogen`'s `rename_datadog`.
//!
//! # Why `trace`/`idx` nest, unlike `agentpayload`/`ddsketch`
//!
//! `datadog.trace.idx` is a genuine **child** package of `datadog.trace`, not a sibling --
//! `AgentPayload.idx_tracer_payloads` (in `datadog.trace.rs`) is typed the bare, unqualified
//! `idx::TracerPayload`, no `super::` (checked against the actual generated output, the same way
//! OTLP's own `super::super::common::v1::KeyValue` cross-references were). That only resolves if
//! `idx` is a real child item of whatever module holds `datadog.trace.rs`'s own top-level items --
//! which a hand-written nesting level in *this* file can't provide: a `#[path] mod trace;`
//! file-module's content is fixed to exactly its file, and nothing outside it (this `mod.rs`
//! included) can inject an extra `pub mod idx` inside that scope. So `tools/protogen`'s `nest`
//! mechanism appends `idx`'s own `#[path = "datadog.trace.idx.rs"] pub mod idx;` declaration
//! straight into the written `datadog.trace.rs` (an ordinary extra item -- order doesn't matter),
//! rather than nesting it here. Below, `trace` is consequently a single flat `#[path] pub mod
//! trace;` file-module whose *content* already declares its own `idx` child; unlike OTLP's five
//! sibling packages, there's no multi-level `#[path = "."]` reset trick to write here, because
//! there's only one real nesting edge (`trace` -> `idx`) and it lives inside the generated file,
//! not in this tree.
//!
//! `agentpayload` and `ddsketch` are each one file declaring one package with no cross-package
//! references at all, so (as `datadog.agentpayload`'s original doc noted) each is a single flat
//! module -- no nesting, no `super::`-relative path to preserve.
//!
//! `#[path = ...] mod ...;` (a real file-module), not `mod ... { include!(...); }` -- inner
//! attributes (`#![allow(clippy::all)]` etc., at the top of every generated file) are only valid
//! at the true start of a file module; `include!`'s textual splice doesn't count as one, and rustc
//! rejects them there. This is exactly why `trace`'s own `idx` child is appended as generated
//! content rather than hand-nested here: `mod trace { include!("datadog.trace.rs"); pub mod idx {
//! include!("datadog.trace.idx.rs"); } }` would put both files' header attributes in
//! non-first position and fail to compile.
//!
//! `#[rustfmt::skip]` on each file's module declaration -- not `#![rustfmt::skip]` inside the
//! generated files themselves -- is what keeps `script/format`/`script/format --check` off this
//! generated code: rustfmt honors `#[rustfmt::skip]` on a module *declaration* by skipping the
//! file it names entirely, and the outer form is stable (the inner `#![rustfmt::skip]` form each
//! generated file would otherwise want at its own top is nightly-only, rust-lang/rust#54726).
//! Skipping `trace` this way also skips its nested `idx` declaration and `datadog.trace.idx.rs`
//! itself, since rustfmt only discovers a nested file module by first formatting the file that
//! declares it.

/// The Agent metrics-intake payload types (`MetricPayload`/`metric_payload::MetricSeries`,
/// `SketchPayload`/`sketch_payload::Sketch`, `EventsPayload`, `CommonMetadata`, `Metadata`,
/// `Origin`, ...), from `agent_payload.proto` (the agent-payload one).
#[rustfmt::skip]
#[path = "datadog.agentpayload.rs"]
pub mod agentpayload;

/// The trace-agent payload types (`Span`, `SpanLink`, `SpanEvent`, `AttributeAnyValue`,
/// `TraceChunk`, `TracerPayload`, `ContainerDebug`, `AgentPayload`, `StatsPayload`,
/// `ClientStatsPayload`, `ClientStatsBucket`, `ClientGroupedStats`, `Trilean`, ...), from
/// `span.proto`/`tracer_payload.proto`/`agent_payload.proto`/`stats.proto` (all four `package
/// datadog.trace;`). `trace::idx` (`TracerPayload`, `Span`, ...) is this module's own child --
/// see the module doc above for why it's declared inside the generated file rather than nested
/// here.
#[rustfmt::skip]
#[path = "datadog.trace.rs"]
pub mod trace;

/// `DDSketch`/`IndexMapping`/`Store`, from sketches-go's `ddsketch.proto` (package `test`,
/// renamed on disk by `tools/protogen`).
#[rustfmt::skip]
#[path = "ddsketch.rs"]
pub mod ddsketch;
