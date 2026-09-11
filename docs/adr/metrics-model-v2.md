---
created: 2026-09-11
updated: 2026-09-11
---

# Metrics model v2: `Sum` replaces `Counter`, raw/summarized pairs, boxed span fidelity, batch-level `Scope`

## Status
Accepted

## Context

[ADR `lossless-transit`](lossless-transit.md) commits `logit` to a lossless relay for every
like-protocol pair and names the concrete gap: `MetricKind`, `MetricRecord`, `LogRecord`,
`SpanRecord`, and `EventBatch` can't represent fields OTLP, statsd, and syslog all carry today —
aggregation temporality, monotonicity, raw (unsummarized) statsd samples and set members, a metric's
start time/description/exemplars, a log's `event_name`/`observed_timestamp`, a span's `flags`/
`trace_state`/status message, and the batch-level OTLP instrumentation scope. `docs/plans/lossless-transit.md`
(W0) surveyed the gap field by field and proposed a target model; this ADR is workstream W1 landing
that model plus the native wire codec reshape it forces, and it records the concrete shape decisions
— each one specific enough that "was this followed?" has a yes/no answer, per this project's own ADR
convention.

W1's scope boundary (decided in the plan, restated here since it shapes every decision below): this
is the *model + codec + keep-everything-compiling* PR. It does not implement `aggregate` sketching
`Samples`/`SetMembers`, statsd producing them, or OTLP mapping the bulk of the new fields — those are
later workstreams. Three narrow OTLP exceptions are pulled forward because leaving them out would
mean writing *more* transitional code, not less: `Sum`/`Histogram`/`ExponentialHistogram`
temporality rides the new fields (`otel.temporality` retired); OTLP `ExponentialHistogram` maps 1:1
in both directions; `Histogram.sum/min/max` and `Summary.count/sum` are carried through since the
fields now exist.

## Decision

**A single `Sum { value, temporality, monotonic }` replaces the old `Counter(f64)` variant.**
`MetricKind::counter(v)` is a convenience constructor for the common delta-monotonic case
(`Sum { value: v, temporality: Delta, monotonic: true }`) — there is no separate `Counter` variant
alongside it. `Temporality` is a new two-variant enum (`Delta`/`Cumulative`), shared by `Sum`,
`Histogram`, and `ExponentialHistogram`.

**Raw-vs-summarized pairs: `Samples`/`Distribution` and `SetMembers`/`Set`.** `Samples { values:
SmallVec<[f64; SAMPLES_INLINE]>, sample_rate: f64 }` carries raw observations exactly as statsd's
`ms`/`h`/`d` hand them over; `SetMembers(Vec<bytes::Bytes>)` carries raw set members exactly as
statsd's `s` hands them over. Only `aggregate` produces the summarized half of each pair
(`Distribution`'s `DdSketch`, `Set`'s `HyperLogLog`) — this is [ADR `lossless-transit`](lossless-transit.md)'s
"summarization is opt-in and named" rule made concrete: a decoder never pre-summarizes what an
explicit `aggregate` stage should decide about. Neither raw variant has a producer yet in this PR
(statsd still decodes straight to `Distribution`, unchanged until W3) — the variants exist so the
model and codec are ready for W3 to fill in without a second reshape.

**`ExponentialHistogram` is kept as its own variant, not materialized into `Histogram`'s explicit
buckets.** This is what makes `otlp_in -> otlp_out` a fixed point for OTLP's own base-2 exponential
histogram shape: decoding into explicit bounds and re-encoding would be spec-legal but not the same
wire bytes, and would have meant carrying `decode_exponential_buckets`/`MAX_DERIVED_BUCKETS` forever
as a lossy fallback path with no reason to exist once the model can just hold the shape directly.
`aggregate` may still choose to convert one to `Distribution` when explicitly summarizing (later
workstream); the codec itself never does.

**`Histogram`/`Summary` carry the fields their OTLP counterparts do.** `Histogram` gains
`temporality: Temporality`, `sum: Option<f64>`, `min: Option<f64>`, `max: Option<f64>` alongside its
existing per-bucket counts. `Summary` gains `count: u64`, `sum: f64` alongside its quantiles.
`ExpHistogram` carries the full `ExponentialHistogramDataPoint` shape: `scale`, `zero_count`,
`zero_threshold`, `positive`/`negative` as `(offset, bucket_counts)` pairs, `temporality`, `count`,
and the same `sum`/`min`/`max` triple.

**`MetricRecord` gains `description: Option<Symbol>`, `start_timestamp: i64`, and `exemplars:
Vec<Exemplar>`.** `start_timestamp` follows OTLP's own convention of `0` meaning "unknown" rather
than an `Option<i64>` — consistent with every other "unset means the type's own zero" field this
model already uses (`LogRecord::observed_timestamp`, `SpanRecord::end_timestamp`'s existing
precedent). `exemplars` is an empty `Vec` on the common path, so it allocates nothing until a
producer actually attaches one. `MetricRecord::new(name, kind)` is a convenience constructor filling
`unit`/`description` `None`, `start_timestamp` `0`, `exemplars` empty — what most producers and
nearly every test want, since only `kind` and `name` vary in practice today.

**`LogRecord` gains `event_name: Option<Symbol>`, `observed_timestamp: i64` (`0` = unset), and
`dropped_attributes_count: u32`** — OTLP's `LogRecord.event_name`/`observed_time_unix_nano`/
`dropped_attributes_count`, with no producer for the first two until W4 (nothing decodes them off
OTLP yet; the fields exist so `otlp_in`/`otlp_out` don't need a second model reshape when W4 lands).

**Span fidelity fields are boxed into a new `SpanExt` struct, not inlined onto `SpanRecord`
directly.** `SpanRecord` gains `flags: u32` (W3C trace flags, OTLP's own `Span.flags`, inline since
it's a plain `u32` every span pays the same cost for) and `ext: Option<Box<SpanExt>>`. `SpanExt`
holds `status_message: Option<Bytes>`, `trace_state: Option<Bytes>`, and three `dropped_*_count`
fields — the overwhelmingly common span (no error status message, no `tracestate`, nothing dropped)
pays 8 bytes for `Option<Box<SpanExt>>` rather than `SpanExt`'s own 80 inline. `SpanLink` gains
`flags: u32`, `trace_state: Option<Bytes>`, `dropped_attributes_count: u32` directly (not boxed —
a link is already a `Vec` element, so there's no "common case pays for the rare one" cost to avoid
the way there is on `SpanRecord` itself); `SpanEvent` gains `dropped_attributes_count: u32`. Every
one of these record types derives `PartialEq` now, `SpanExt` also `Default`.

**Batch-level `Scope`, not `otel.scope.*` attributes.** `EventBatch` gains `scope: Option<Arc<Scope>>`,
`Arc`-shared across the batch the same way `resource` is. `Scope { name: Bytes, version: Bytes,
attributes: AttrMap, dropped_attributes_count: u32, schema_url: Option<Bytes> }` replaces the
`otel.scope.name`/`otel.scope.version` event-attribute convention `otlp_in`/`otlp_out` used before —
the OTLP codec still stamps those attributes on decode/encode until W4 rewires it to group by
`(resource, scope)` and read/write the new field instead; this PR only adds the field and its wire
representation. `Resource` gains `dropped_attributes_count: u32` and `schema_url: Option<Bytes>`,
kept `Default`.

**`PartialEq` on every record type, with `DdSketch` compared via `to_java_bytes`.**
`sketches_ddsketch::DDSketch` exposes no bin iteration and has no `PartialEq` of its own; its
canonical serialized form (`to_java_bytes`, already how the native codec carries a `Distribution`
losslessly) is the only lossless view the wrapped crate offers, so it's also the only faithful
equality check available. `HyperLogLog` derives `PartialEq` trivially (it's still a unit-payload
stub). This is what makes `Event`/`EventBatch` themselves `PartialEq`-derivable, which the native
codec's round-trip tests now rely on (`assert_eq!` on the whole value, not a field-by-field
comparison) — see Consequences.

**Native codec: every record type is TLV-framed, only non-default fields are written, and `Scope`
is a mandatory presence-prefixed batch section, never an optional trailing one.**
`crates/logit-proto/src/native/record.rs` replaces `MetricRecord`/`LogRecord`/`SpanRecord`/
`SpanLink`/`SpanEvent`'s previous positional layouts with the same `tag(u8) + len(uvarint) +
payload` framing `Event`'s own fields already used, via one shared `write_field`/`for_each_field`
helper pair (plus `write_scalar_field` for a payload whose length is known before writing, avoiding
a temporary buffer for every fixed-size scalar). `Resource` becomes a length-prefixed TLV section
in the batch payload; `Scope` gets its own section immediately after `Resource` — a presence byte
(`0`/`1`) followed by a length-prefixed TLV body when present. This is placed where it is, and
framed the way it is, specifically because an *optional trailing* section would let a payload
truncated exactly at that boundary decode successfully as "no scope" instead of failing — breaking
`crates/logit-proto/tests/robustness.rs`'s "no proper prefix of a valid encoding is itself valid"
invariant, the same reasoning `CODEC_NATIVE_V2`'s mandatory provenance-trailer length prefix already
follows (`docs/design/wire-protocol.md`). See that document's "Record layout" section for the full
field-tag and metric-kind-tag tables this decision produced.

**`SAMPLES_INLINE = 19`, measured, not guessed.** `size_of::<DdSketch>()` is 176 bytes (a
`sketches_ddsketch::DDSketch` inlined directly). `MetricKind::Distribution(DdSketch)` fits in
exactly 176 bytes with no separate discriminant byte — rustc niche-fills the outer enum's tag into
spare bit patterns already present inside `DDSketch`'s own layout. That trick is specific to
`DDSketch`'s layout and isn't available to `Samples`: a `Samples` sized to exactly 176 bytes too
(measured at `SAMPLES_INLINE = 20`) forces a real discriminant on top, growing `MetricKind` to 184.
`SAMPLES_INLINE = 19` is the largest value that avoids this — `size_of::<Samples>() == 168`, leaving
just enough room inside the existing 176-byte envelope for `MetricKind`'s discriminant, so
`MetricKind` stays exactly 176. Both are asserted exactly in `crates/logit-core/tests/type_sizes.rs`.

## Alternatives considered

- **Keep `Counter` as a separate variant alongside a new `Sum`.** Rejected: `Counter(f64)` is
  exactly `Sum { temporality: Delta, monotonic: true }` with no wire concept `Sum` can't already
  express, so keeping both would mean two representations of the same series shape and every
  exhaustive match having to decide which one a given piece of code should ever produce. A single
  variant with a convenience constructor for the common case gives the same ergonomics without the
  duplicate representation.
- **`Gauge { relative: bool }`-style flags instead of separate variants**, generalized from the
  precedent [ADR `relative-gauge-adjustments`](relative-gauge-adjustments.md) already rejected for
  `GaugeDelta` specifically. Rejected for the same reason here: a flag field lets an exhaustive match
  silently treat a raw/unresolved value as a summarized one by pattern-matching only on the outer
  variant, which is exactly the bug class distinct variants exist to make a compile-time decision
  instead of a runtime one. `Samples`/`Distribution` and `SetMembers`/`Set` follow the same
  established pattern as `Gauge`/`GaugeDelta`, not a new one.
- **Box the `DdSketch` inside `MetricKind::Distribution`, instead of sizing `Samples` to fit under
  it.** Considered again and rejected again, for the reason [`docs/design/memory.md`](../design/memory.md)
  already recorded when this was first measured: boxing trades `MetricKind`'s size for an allocation
  on every distribution metric actually constructed or cloned, and distributions are a shipping,
  commonly-populated feature (`kv_metrics`, statsd's `ms`/`h`/`d`), not a rare one — this project has
  a standing priority for that exact conflict ([ADR `minimize-allocations-over-event-size`](minimize-allocations-over-event-size.md)).
  Sizing `Samples` to sit at or under `DdSketch`'s existing 176-byte footprint gets the new variant
  in for free instead of re-opening a settled tradeoff.
- **Scope as event attributes (status quo), left alone.** Rejected: `otel.scope.name`/
  `otel.scope.version` riding as ordinary event attributes means every event on a batch repeats the
  same two strings, and — more importantly — decoding scope into attributes and re-encoding a
  hardcoded `{name: "logit", version: CARGO_PKG_VERSION}` scope on every `otlp_out` batch is a real
  identity loss, not a permitted regroup (`docs/plans/lossless-transit.md`'s assessment section
  named this explicitly). A batch-level field is the only representation that survives a decode/
  re-encode round trip without inventing a scope that was never there.
- **Positional wire field additions instead of TLV framing**, i.e. keep appending new fixed-offset
  fields to each record's existing layout. Rejected: a positional layout has no way to skip a field
  it doesn't recognize, so growing any one of `MetricRecord`/`LogRecord`/`SpanRecord`/`SpanLink`/
  `SpanEvent` again later would force yet another special-cased reshape of that one record's reader.
  TLV framing (the same shape `Event`'s own fields already used) pays the skip-unknown cost once, on
  every record type uniformly, for cheap torn-write hygiene even though `logit`'s pre-release status
  means nothing is actually reading an old frame against a new writer today.

## Consequences

- Sizes moved — everything below measured and asserted in `crates/logit-core/tests/type_sizes.rs`,
  written into [`docs/design/memory.md`](../design/memory.md)'s §1 table:

  | Type | Before | After |
  |---|---:|---:|
  | `MetricKind` | 176 | 176 (unchanged — the point of `SAMPLES_INLINE`'s sizing) |
  | `MetricRecord` | 184 | 224 |
  | `MetricList` | 192 | 232 |
  | `LogRecord` | 72 | 88 |
  | `SpanRecord` | 136 | 144 |
  | `SpanExt` (new) | — | 80 |
  | `Option<Box<SpanExt>>` (new) | — | 8 |
  | `Resource` | 392 (== `AttrMap`) | 432 |
  | `Scope` (new) | — | 496 |
  | `DdSketch` (new assertion) | 176 | 176 |
  | `Samples` (new) | — | 168 |
  | `Event` | 800 | 864 |

- **Every exhaustive `MetricKind` match in the codebase gained four arms**
  (`Sum`/`Samples`/`SetMembers`/`ExponentialHistogram` — `GaugeDelta`/`Gauge`/`Distribution`/`Set`/
  `Histogram`/`Summary` were already there): `event::metric_record_heap_bytes`,
  `native/record.rs`'s encode and decode, `otlp/metrics.rs`, `outputs/{influxdb,stdio,statsd}.rs`,
  `transforms/aggregate.rs`, and `bench/bakeoff/wire_mirror.rs`. This is the same "a future variant
  is a compile error, not a panic" property [ADR `relative-gauge-adjustments`](relative-gauge-adjustments.md)
  established for `GaugeDelta`, now exercised four times over in one PR — every one of those call
  sites had to make an explicit decision (pass-through, degrade-and-count, or a real encoding) for
  each new variant, not a silent default.
- **The allocation counts this PR's model changes touch move too** — tracked and re-measured in
  `crates/logit-bench/tests/allocations.rs` and [`docs/design/memory.md`](../design/memory.md) §2 by
  the phase of this workstream owning that crate; this ADR doesn't restate those numbers.
- **`aggregate` and the sinks treat `Samples`/`SetMembers`/`ExponentialHistogram`/a non-delta-
  monotonic `Sum` as pass-through or explicitly unsupported, not as a real feature, until W2/W3/W4.**
  No behavior beyond "compiles and doesn't panic" is promised by this PR for any of the four new
  variants; `Samples`/`SetMembers` have no producer at all yet (statsd still decodes straight to
  `Distribution`/errors on `s`, unchanged until W3).
- **OTLP stays lossy on every field this PR didn't pull forward.** `description`, `start_timestamp`,
  `exemplars`, `event_name`, `observed_timestamp`, dropped-attribute counts, span `trace_state`/
  status message, and scope grouping all decode to their defaults and are ignored on encode until
  W4 — the fields exist in the model now specifically so W4 is a codec change, not another model
  reshape.
- **`otel.temporality` is retired now** — `Sum`/`Histogram`/`ExponentialHistogram` temporality rides
  the real field on both sides of the OTLP codec, and this attribute is never stamped or read again.
  **`otel.scope.*` and `otel.status_message` are retired in W4**, not this PR — `otlp_in`/`otlp_out`
  still stamp/read those attributes today, since `EventBatch.scope`/`SpanExt.status_message` don't
  have real producers on the OTLP side yet.
- **Follow-on workstreams, per [`docs/plans/lossless-transit.md`](../plans/lossless-transit.md):**
  W2 (`aggregate` sketches `Samples`→`Distribution`/unions `SetMembers`→`Set`, wiring a real
  `HyperLogLog`), W3 (statsd produces `Samples`/`SetMembers`, `|c:`/`|T`, sample-rate retention on
  timers), W4 (the rest of the OTLP mapping this PR deferred), W7 (expose the new fields through the
  Lua proxy — otherwise the model is lossless but the scripting surface can't see any of it).
