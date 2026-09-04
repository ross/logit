---
created: 2026-09-03
updated: 2026-09-04
---

# `LogRecord` gains a native application trace/span reference

## Status
Accepted. Partially superseded on 2026-09-04 by
[ADR `trace-context-span-lifting`](trace-context-span-lifting.md): this ADR's "splitting a
`traceparent` string is a script's job" stance (`trace_context.rs`'s original doc comment, quoted
below) is revisited there — `trace_context` now parses a `traceparent` header natively for its
trace id and flags. Everything else here — `TraceRef`'s shape, the lenient-decode/strict-span-id
asymmetry, the `trace` global vs. `event.log.trace_id` distinction — stands unchanged.

## Context

OTLP's `LogRecord` carries `trace_id` (16 bytes), `span_id` (8 bytes), and `flags` (the low 8
bits of a `fixed32`, W3C trace flags — bit 0 is `SAMPLED`) so a backend can correlate a log line
to the trace or span it was emitted under. `logit_core::LogRecord` had no field for any of this —
`crates/logit-proto/src/otlp/logs.rs` always emitted empty ids on encode and silently dropped
inbound ones on decode, documented as deliberate: "a log and its span correlate today only by
sharing one `Event`." That's true as far as it goes, but it only covers the case where `logit`
itself decoded both signals from one OTLP request into one `Event`. It says nothing about a log
that arrives with its own trace reference already on it (a real OTLP producer's `LogRecord`), or
about an application that puts a `trace_id` in its log body as plain JSON (`{"trace_id":
"4bf92f...", ...}`), which is common practice for anything not emitting OTLP directly. Both cases
made every OTLP log sink — Loki included, per `docs/plans/otlp-logs-and-resource-identity.md`'s
workstream D — permanently unable to get native trace correlation from `logit`, regardless of what
the source actually knew.

`docs/design/data-model.md` states the model has to be "a strict superset of what OTLP can
express: anything OTLP can carry that `Event` can't represent makes the OTLP codec lossy." A
native `trace_id`/`span_id`/`flags` on `LogRecord` closes exactly that gap.

## Decision

`LogRecord` gains `pub trace: Option<TraceRef>`, where `TraceRef` (new, `crates/logit-core/src/
trace.rs`) bundles `trace_id: [u8; 16]`, `span_id: Option<[u8; 8]>`, and `flags: u8` into one
type — not two independent `Option`s on `LogRecord` — because OTLP's own contract is "if `SpanId`
is present, `TraceId` SHOULD be also present": a `TraceRef` makes "span without trace"
unrepresentable instead of merely undocumented.

**Sensible by default, `logit`'s code never invents one.** `otlp_in` round-trips whatever
`trace_id`/`span_id`/`flags` a real OTLP producer sent. When *encoding* a log with no trace
context of its own but whose same `Event` carries a `span` — `logit`'s own existing correlation
mechanism — the encoder falls back to that span's ids, `flags: 0`. Nothing else ever sets `trace`
on `logit`'s own initiative; every other path is an operator's explicit choice:

- The `trace_context` native transform (`crates/logit-transforms/src/trace_context.rs`,
  `ComponentKind::TraceContext`) lifts `trace_id`/`span_id`/`flags` off named attributes — the
  common "my JSON log body already has a `trace_id` field" case — without writing Lua.
- Lua's `event.log` proxy (`crates/logit-script/src/proxy.rs`'s `LogProxy`) gives a script
  `trace_id`/`span_id`/`trace_flags` read+write for anything `trace_context`'s attribute-lifting
  shape doesn't fit.

This is the same posture PR #60/#61/#62 already settled for the batch *resource*
(`docs/adr/operator-declared-resource-attributes.md`): `logit`'s code may not guess an identity
for data it didn't produce, but an operator configuring the pipeline may declare one. A log's
trace context is exactly that kind of declaration, one level down from the resource.

**Decode is lenient, deliberately unlike a `Span`'s ids.** `crates/logit-proto/src/otlp/
traces.rs`'s `mod ids` rejects a malformed `Span.trace_id` outright — a span's identity is
required, OTLP treats it as such. A log's trace correlation is optional metadata: `logs.proto`'s
own doc comment says receivers "SHOULD assume the log record is not associated with a trace" if
the id is absent or invalid, so `TraceRef::from_bytes` (the parse/validate entry point both the
codec and the Lua proxy's hex parsers route through) degrades a wrong-length or all-zero id to
`None` rather than failing the whole record. `flags` on an otherwise-invalid `trace_id` is dropped
along with it — there's no "flags with no trace" case worth keeping.

**The span fallback is not a round-trip fixed point, and that's fine.** `encode_signals` splits a
log+span `Event` into a separate `LogRecord` and `Span` payload; decode makes one `Event` per
record. So `Event { log: { trace: None }, span: Some(s) }` encodes and decodes back as a *log*
event carrying `trace: Some(s's ids)`, plus a separate span event — an enrichment, not a mirror.
Documented in `logs.rs`'s module doc rather than treated as a bug.

**Lua's `message`/`severity`/`body_format` stay read-only.** `event.log`'s trace fields are
read+write from day one; the other three record fields are read-only, deferred to a later design
pass once a concrete need for writing them shows up — the same "don't guess ahead of a real
consumer" posture `docs/design/lua-api.md` already had for typed record access in general.

**`trace` (the pipeline global) and `event.log.trace_id` (this) are different things, and `logit`
never conflates them.** `trace` is `logit`'s own internal `TraceContext` — which node-visit
processed a batch, propagated on `Delivered` (`docs/adr/trace-context-propagation-on-delivered.md`).
`event.log.trace_id` is the *application's* trace, decoded off the wire or set by config/script. A
script copying one onto the other (`event.log.trace_id = trace.trace_id`, stamping a log with
"which `logit` run handled this line") is a real, useful, and entirely deliberate thing to write —
`logit` just never does it without being told to.

## Alternatives considered

- **Two flat `Option`s on `LogRecord`** (`trace_id: Option<[u8;16]>`, `span_id: Option<[u8;8]>`)
  instead of a bundled `TraceRef`. Rejected: it can represent "span without trace," a state OTLP's
  own spec says shouldn't exist, and every call site would need to re-derive the "span_id only
  means something with a trace_id" invariant by hand instead of it being structural.
- **Reject a malformed log trace id like `traces.rs` does for spans.** Rejected: `logs.proto`
  explicitly tells receivers to degrade gracefully, and a `Result`-returning `decode_log_record`
  would ripple into `decode_resource_logs` (`otlp/mod.rs`, currently infallible) for a case the
  spec says shouldn't fail the record at all.
- **A pipeline-`TraceContext`-stamping mode on `trace_context`** (an opt-in flag that copies
  `logit`'s own batch trace onto a log, the native equivalent of the Lua pattern above). Deferred,
  not rejected — out of scope for this change; revisit once a concrete use case needs it (tracked
  in `docs/known-gaps.md`).
- **Demo-stack wiring** (a second Grafana `derivedFields` entry keyed on Loki's `trace_id`
  structured metadata instead of the existing body regex). Deferred to the demo app's own tracing
  rework, a separate, already-planned piece of work — this ADR only lands the model/codec/Lua/
  transform pieces.

## Consequences

- `LogRecord`: 48 → 72 bytes; `Event`: 776 → 800 bytes. Measured, not estimated —
  `crates/logit-core/tests/type_sizes.rs` pins both, and `Option<TraceRef>`/`Option<LogRecord>`
  stay niche-free (26 and 72 respectively) confirmed the same way.
  `docs/design/memory.md`/`data-model.md` updated in the same commit as the model change, per
  `AGENTS.md`'s rule for exactly this kind of size-affecting change.
- `syslog_out`/`syslog_in`/`influxdb_out` are untouched by this change. `syslog_out` has no
  STRUCTURED-DATA emission at all today (`docs/known-gaps.md`), so a log's trace context has
  nowhere to go over that wire yet — filed as the new residual gap, not silently dropped without a
  trace. `influxdb_out` ignores a log's fields entirely already (it only ever writes metrics), so
  nothing changes there.
- Every `LogRecord { .. }` construction site in the tree (28 of them, 3 production) gained
  `trace: None` explicitly — no `Default` impl, no constructor, matching the existing
  `SpanRecord`-literal style everywhere else in this codebase.
