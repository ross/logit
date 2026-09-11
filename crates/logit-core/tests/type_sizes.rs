//! Byte-size assertions for the event model.
//!
//! `Event` is moved by value on every hop between pipeline nodes and deep-cloned once per extra
//! fan-out consumer (`logit-pipeline`'s `Fanout`), so its size is a throughput property, not a
//! curiosity -- see `docs/design/memory.md` for the full accounting and for what each of these
//! numbers is made of.
//!
//! These are exact-equality assertions on purpose. A `<=` bound would silently absorb the thing
//! this test exists to catch: a field added to `Event` (or to any type it inlines) quietly adding
//! hundreds of bytes to every event in flight. When one of these fails, that's the test working --
//! decide whether the growth is worth it, update the number, and update `docs/design/memory.md`'s
//! table in the same commit.
//!
//! Sizes are architecture-dependent (`Bytes`, `Vec`, and `SmallVec` are all pointer-sized), so
//! every assertion is gated on a 64-bit target rather than asserting something false on a 32-bit
//! one.

#![cfg(target_pointer_width = "64")]

use logit_core::{
    AttrMap, DdSketch, Event, LogRecord, MetricKind, MetricList, MetricRecord, Provenance,
    Resource, Samples, Scope, SpanExt, SpanRecord, Symbol, TraceRef, Value, SAMPLES_INLINE,
};
use std::mem::{size_of, size_of_val};

/// The interned-key type. `lasso::Spur` is a `NonZeroU32`, which is what makes `Option<Symbol>`
/// (on `MetricRecord::unit`) free rather than a padded 8 bytes.
#[test]
fn symbol_is_a_niche_optimized_u32() {
    assert_eq!(size_of::<Symbol>(), 4);
    assert_eq!(size_of::<Option<Symbol>>(), 4, "Spur's NonZero niche should absorb the None case");
}

/// `Provenance` (`origin`/`previous`, both `Option<Symbol>`) -- the batch-level graph identity
/// carried alongside `logit-pipeline`'s `TraceContext` on every `Delivered`
/// (`docs/adr/batch-provenance-on-delivered.md`). Both fields niche-optimize per
/// `symbol_is_a_niche_optimized_u32` above, so this is two 4-byte fields with no padding.
#[test]
fn provenance_is_two_niche_optimized_option_symbols() {
    assert_eq!(size_of::<Provenance>(), 8);
}

/// `Value`'s size is set by its largest variant, `Bytes` (4 words: ptr, len, data, vtable), plus a
/// discriminant rounded up to `Bytes`'s 8-byte alignment. `Map` is boxed specifically to keep it
/// from being the largest variant (`value.rs`), and `Array`'s `Vec` is 3 words.
#[test]
fn value_is_bytes_plus_a_discriminant_word() {
    assert_eq!(size_of::<bytes::Bytes>(), 32);
    assert_eq!(size_of::<Value>(), 40);
}

/// The dominant term in `Event`. `AttrMap` is a `SmallVec<[(Symbol, Value); 8]>`, and a `SmallVec`
/// occupies its inline footprint **whether or not it has spilled to the heap** -- the inline array
/// and the heap `(ptr, cap)` share one union-or-enum slot sized by the larger of the two. So an
/// event with 13 attributes pays both a heap allocation *and* this full inline footprint.
///
/// `(Symbol, Value)` is 48 bytes, not 44: `Value` is 8-byte aligned, so the 4-byte `Symbol` is
/// followed by 4 bytes of padding.
#[test]
fn attr_map_pays_its_inline_capacity_whether_or_not_it_spills() {
    assert_eq!(size_of::<(Symbol, Value)>(), 48);
    assert_eq!(
        size_of::<AttrMap>(),
        392,
        "8 * 48 inline + 8 of smallvec overhead, now that the `union` feature (workspace \
         Cargo.toml) shares the discriminant with the inline/heap union instead of paying for \
         it separately"
    );

    // Not a size assertion, but the claim the comment above rests on: spilling doesn't shrink it.
    let mut spilled = AttrMap::new();
    for i in 0..32 {
        spilled.insert(&format!("k{i}"), Value::I64(i));
    }
    assert_eq!(size_of_val(&spilled), size_of::<AttrMap>());
}

/// `MetricKind` inlines either a whole `sketches_ddsketch::DDSketch` (`Distribution`) or a
/// `SmallVec<[f64; SAMPLES_INLINE]>` (`Samples`) -- the two largest variants, deliberately sized to
/// match. `size_of::<DdSketch>()` is 176 bytes (two `Store`s, each a `Vec` plus bookkeeping, and a
/// `Config`), and `MetricKind::Distribution(DdSketch)` fits in exactly that many bytes with no
/// separate discriminant byte: rustc niche-fills the outer enum's tag into spare bit patterns
/// already present inside `DDSketch`'s own layout. That trick is specific to `DDSketch`'s layout,
/// not available to `Samples`'s -- a `Samples` variant sized to exactly 176 bytes too would force a
/// real discriminant on top, growing `MetricKind` to 184 (measured directly while choosing
/// `SAMPLES_INLINE`, see `metric.rs`'s own doc comment on the constant). `SAMPLES_INLINE = 19`
/// keeps `Samples` at 168 bytes, leaving exactly enough room for that discriminant to land inside
/// the existing 176-byte envelope instead of growing it.
#[test]
fn metric_kind_is_sized_by_its_two_largest_variants() {
    assert_eq!(
        size_of::<DdSketch>(),
        176,
        "sketches_ddsketch::DDSketch inlined directly (no Box): two Stores (a Vec plus \
         bookkeeping each) and a Config"
    );
    assert_eq!(
        size_of::<Samples>(),
        168,
        "SmallVec<[f64; SAMPLES_INLINE]> (max(24, SAMPLES_INLINE * 8 + 8) under the union \
         feature) plus an 8-byte sample_rate: f64"
    );
    assert_eq!(
        size_of::<MetricKind>(),
        176,
        "sized by the larger of its two big variants (Distribution's inlined DDSketch, at 176) \
         plus room for a real discriminant that Samples's own 168-byte payload leaves inside that \
         envelope -- every other variant (Sum/Gauge/GaugeDelta/SetMembers/Set/Histogram/\
         ExponentialHistogram/Summary) is far smaller and pays the same 176 regardless"
    );
    assert_eq!(
        size_of::<MetricRecord>(),
        224,
        "MetricKind (176) + name: Symbol (4) + unit: Option<Symbol> (4) + description: \
         Option<Symbol> (4, 4 bytes padding to the next i64-aligned field) + start_timestamp: i64 \
         (8) + exemplars: Vec<Exemplar> (24)"
    );
    assert_eq!(
        size_of::<MetricList>(),
        232,
        "SmallVec<[MetricRecord; 1]>: the inline record (224), plus 8 bytes of capacity-and-\
         discriminant overhead (smallvec's `union` feature, enabled workspace-wide in \
         Cargo.toml)"
    );
}

/// `TraceRef` -- `LogRecord`'s optional application-trace reference (`docs/adr/
/// log-record-trace-context.md`). `span_id: Option<[u8;8]>` has no niche of its own ([u8;8]'s
/// value space is fully used), so it costs a discriminant byte: 16 (trace_id) + 9 (span_id) + 1
/// (flags) = 26.
#[test]
fn trace_ref_is_sized_by_its_two_id_arrays_plus_a_span_discriminant() {
    assert_eq!(size_of::<TraceRef>(), 26);
    // `Option<TraceRef>` is free, somewhat surprisingly: `span_id`'s inner `Option<[u8;8]>`
    // discriminant byte only uses 2 of its 256 possible values, and rustc's niche-filling finds
    // and reuses one of the other 254 for the outer `Option`'s `None` -- confirmed here, not
    // assumed, since it's a compiler optimization with no language guarantee behind it.
    assert_eq!(size_of::<Option<TraceRef>>(), 26, "niche-filled through Option<[u8;8]>'s tag");
}

/// `SpanExt` is boxed on `SpanRecord` specifically so the overwhelmingly common span (no status
/// message, no `tracestate`, nothing dropped) doesn't pay for it inline -- confirm both halves of
/// that trade: the box itself is pointer-sized and niche-free (`None` needs no separate
/// discriminant), and `SpanExt` on its own is worth boxing at all.
#[test]
fn span_ext_is_boxed_to_a_niche_free_pointer() {
    assert_eq!(
        size_of::<SpanExt>(),
        80,
        "status_message: Option<Bytes> (32) + trace_state: Option<Bytes> (32) + three u32 \
         dropped counts (12, padded to 16 for Bytes's 8-byte alignment)"
    );
    assert_eq!(
        size_of::<Option<Box<SpanExt>>>(),
        8,
        "Box's non-null pointer niche absorbs the None case"
    );
}

/// `Scope` -- the batch-level OTLP instrumentation scope (`docs/adr/lossless-transit.md`). Two
/// `Bytes` (32 each) for `name`/`version`, the `AttrMap` (392), a `u32` `dropped_attributes_count`,
/// and an `Option<Bytes>` `schema_url` (32, no niche: `Bytes` carries no spare bit pattern to fill
/// with `None`, so this costs a real discriminant, padded to `Bytes`'s 8-byte alignment).
#[test]
fn scope_size() {
    assert_eq!(size_of::<Scope>(), 496);
}

#[test]
fn record_types() {
    assert_eq!(
        size_of::<LogRecord>(),
        88,
        "message: Value (40) + severity: Option<Severity> (1, niche-free) + body_format: \
         BodyFormat (1) + trace: Option<TraceRef> (26) + event_name: Option<Symbol> (4) + \
         observed_timestamp: i64 (8) + dropped_attributes_count: u32 (4), plus alignment padding"
    );
    assert_eq!(
        size_of::<SpanRecord>(),
        144,
        "the original 136-byte record plus flags: u32 (4, padded to 8) and ext: \
         Option<Box<SpanExt>> (8, niche-free per span_ext_is_boxed_to_a_niche_free_pointer)"
    );
    assert_eq!(
        size_of::<Resource>(),
        432,
        "AttrMap (392) + dropped_attributes_count: u32 (4, \
        padded) + schema_url: Option<Bytes> (32, no niche)"
    );

    // Both `Option`s are free: `Severity` and `SpanKind` are small field-less enums, so their
    // spare discriminants absorb the `None` case. Worth asserting rather than assuming -- adding
    // a 256-variant enum to either record would silently cost `Event` another 8 bytes.
    assert_eq!(size_of::<Option<LogRecord>>(), size_of::<LogRecord>());
    assert_eq!(size_of::<Option<SpanRecord>>(), size_of::<SpanRecord>());
}

/// The number that matters: what one event costs to move between two pipeline nodes, and to deep-
/// clone for each extra fan-out consumer. `docs/design/memory.md` breaks this down term by term
/// and lists what could be reclaimed.
#[test]
fn event_size() {
    assert_eq!(
        size_of::<Event>(),
        864,
        "800 (pre-metrics-model-v2 baseline) + 16 (LogRecord 72 -> 88) + 40 (MetricList 192 -> \
         232, via MetricRecord 184 -> 224) + 8 (SpanRecord 136 -> 144); the two records stay \
         niche-free through their enclosing Option -- see docs/adr/metrics-model-v2.md and \
         record_types/metric_kind_is_sized_by_the_inlined_ddsketch above"
    );

    // The breakdown, asserted so it can't drift out of sync with the total above.
    let sum = size_of::<i64>()
        + size_of::<AttrMap>()
        + size_of::<Option<LogRecord>>()
        + size_of::<MetricList>()
        + size_of::<Option<SpanRecord>>();
    assert_eq!(sum, size_of::<Event>(), "Event should have no padding beyond its fields");
}

/// `SAMPLES_INLINE` is a measured constant, not an arbitrary one -- pin its value directly so a
/// future change to it (or to `Samples`'s other field) is a deliberate, reviewed edit here, not a
/// silent drift.
#[test]
fn samples_inline_is_the_measured_constant() {
    assert_eq!(SAMPLES_INLINE, 19);
}
