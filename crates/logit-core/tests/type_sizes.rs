//! Byte-size assertions for the event model.
//!
//! `Event` moves by value on every hop and is deep-cloned once per extra fan-out consumer, so its
//! size is a throughput property. `docs/design/memory.md` has the full accounting.
//!
//! These are exact-equality assertions. Never relax one to `<=`: that would silently absorb what
//! this test exists to catch, a field added to `Event` (or a type it inlines) adding bytes to every
//! event in flight. When one fails, that's the test working: decide whether the growth is worth
//! it, then update the constant and `docs/design/memory.md`'s table in the same commit.
//!
//! Gated on 64-bit targets: `Bytes`, `Vec`, and `SmallVec` sizes are pointer-dependent.

#![cfg(target_pointer_width = "64")]

use logit_core::{
    AttrMap, DdSketch, Event, LogRecord, MetricKind, MetricList, MetricRecord, Provenance,
    Resource, Samples, Scope, SpanExt, SpanRecord, Symbol, TraceRef, Value, SAMPLES_INLINE,
};
use std::mem::{size_of, size_of_val};

/// `lasso::Spur` is a `NonZeroU32`, so `Option<Symbol>` costs nothing extra.
#[test]
fn symbol_is_a_niche_optimized_u32() {
    assert_eq!(size_of::<Symbol>(), 4);
    assert_eq!(size_of::<Option<Symbol>>(), 4, "Spur's NonZero niche should absorb the None case");
}

/// Two niche-optimized `Option<Symbol>`s, carried on every `Delivered`.
#[test]
fn provenance_is_two_niche_optimized_option_symbols() {
    assert_eq!(size_of::<Provenance>(), 8);
}

/// `Value` is its largest variant, `Bytes` (4 words), plus an aligned discriminant. `Map` is boxed
/// so it isn't the largest.
#[test]
fn value_is_bytes_plus_a_discriminant_word() {
    assert_eq!(size_of::<bytes::Bytes>(), 32);
    assert_eq!(size_of::<Value>(), 40);
}

/// The dominant term in `Event`. A `SmallVec` occupies its inline footprint **whether or not it
/// has spilled**, so a 13-attribute event pays a heap allocation and the full inline footprint.
/// `(Symbol, Value)` is 48 bytes, not 44: `Value`'s alignment pads the `Symbol`.
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

    // Spilling doesn't shrink it.
    let mut spilled = AttrMap::new();
    for i in 0..32 {
        spilled.insert(&format!("k{i}"), Value::I64(i));
    }
    assert_eq!(size_of_val(&spilled), size_of::<AttrMap>());
}

/// `MetricKind`'s two largest variants, sized to match: `Distribution` inlines a `DdSketch`, now
/// 128 bytes hand-rolled, down from a wrapped `sketches_ddsketch::DDSketch`'s 176, and
/// `SAMPLES_INLINE = 19` keeps `Samples` at 168 -- still needing a real discriminant, since it
/// doesn't niche the way `DdSketch` used to -- so `Samples` plus its tag is what now sizes
/// `MetricKind` at 176 (`SAMPLES_INLINE`'s doc in `metric.rs`).
#[test]
fn metric_kind_is_sized_by_its_two_largest_variants() {
    assert_eq!(
        size_of::<DdSketch>(),
        128,
        "a Mapping (kind, gamma, gamma_ln, offset, bin_limit), two bin Vecs, and the f64 \
         summary (zero_count, count, min, max, sum) plus the exact-stats flag"
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
        "sized by its largest variant, Samples at 168, plus a real discriminant -- every other \
         variant (Distribution's 128-byte DdSketch included, and Sum/Gauge/GaugeDelta/SetMembers/\
         Set/Histogram/ExponentialHistogram/Summary) is smaller and pays the same 176 regardless"
    );
    assert_eq!(
        size_of::<MetricRecord>(),
        224,
        "MetricKind (176) + name: Symbol (4) + unit: Option<Symbol> (4) + description: \
         Option<Symbol> (4) + flags: u32 (4, fills what used to be padding to the next \
         i64-aligned field -- docs/adr/metrics-model-v2.md's W4 amendment) + start_timestamp: i64 \
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

/// `span_id: Option<[u8; 8]>` has no niche, so it costs a tag byte: 16 + 9 + 1 (flags) = 26.
#[test]
fn trace_ref_is_sized_by_its_two_id_arrays_plus_a_span_discriminant() {
    assert_eq!(size_of::<TraceRef>(), 26);
    // The outer `Option` reuses a spare value of `span_id`'s tag byte: a compiler optimization
    // with no language guarantee, hence asserted.
    assert_eq!(size_of::<Option<TraceRef>>(), 26, "niche-filled through Option<[u8;8]>'s tag");
}

/// `SpanExt` is boxed so the common span doesn't pay for it inline: it's big enough to box, and
/// `Option<Box<_>>` is one pointer.
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

/// Two `Bytes` (32 each), the `AttrMap` (392), a `u32`, and an `Option<Bytes>` `schema_url`.
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

    // Both `Option`s are free via a small enum's spare discriminants; a 256-variant enum on either
    // record would cost `Event` another 8 bytes.
    assert_eq!(size_of::<Option<LogRecord>>(), size_of::<LogRecord>());
    assert_eq!(size_of::<Option<SpanRecord>>(), size_of::<SpanRecord>());
}

/// What one event costs to move between nodes; `docs/design/memory.md` breaks it down.
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

    // The breakdown, so it can't drift from the total.
    let sum = size_of::<i64>()
        + size_of::<AttrMap>()
        + size_of::<Option<LogRecord>>()
        + size_of::<MetricList>()
        + size_of::<Option<SpanRecord>>();
    assert_eq!(sum, size_of::<Event>(), "Event should have no padding beyond its fields");
}

/// `SAMPLES_INLINE` is measured; pinning it makes a change a reviewed edit here.
#[test]
fn samples_inline_is_the_measured_constant() {
    assert_eq!(SAMPLES_INLINE, 19);
}
