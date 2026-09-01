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
    AttrMap, Event, LogRecord, MetricKind, MetricList, MetricRecord, Resource, SpanRecord, Symbol,
    Value,
};
use std::mem::{size_of, size_of_val};

/// The interned-key type. `lasso::Spur` is a `NonZeroU32`, which is what makes `Option<Symbol>`
/// (on `MetricRecord::unit`) free rather than a padded 8 bytes.
#[test]
fn symbol_is_a_niche_optimized_u32() {
    assert_eq!(size_of::<Symbol>(), 4);
    assert_eq!(size_of::<Option<Symbol>>(), 4, "Spur's NonZero niche should absorb the None case");
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

/// `MetricKind` inlines a whole `sketches_ddsketch::DDSketch` in its `Distribution` variant (two
/// `Store`s, each a `Vec` plus bookkeeping), which makes every `MetricRecord` pay for a sketch it
/// almost never holds. Boxing that one variant is the single cheapest size win available; see
/// `docs/design/memory.md`.
/// `MetricKind::Distribution` now boxes its `DdSketch` (`docs/design/memory.md` §8 item 10), so
/// `MetricKind`'s size is set by its largest *remaining* inline variant instead --
/// `Histogram`/`Summary`'s `Vec` (24 bytes) plus an 8-byte discriminant, not the roughly 8 bytes
/// `Counter`/`Gauge`/`Distribution(Box<_>)` alone would need. Boxing the sketch therefore saves
/// 144 bytes here (176 -> 32), more than `docs/design/memory.md`'s original "~168 B" estimate,
/// which assumed the next-largest variant was negligible rather than a 24-byte `Vec`.
#[test]
fn metric_kind_is_sized_by_the_inlined_ddsketch() {
    assert_eq!(
        size_of::<MetricKind>(),
        32,
        "Histogram/Summary's Vec<(f64, _)> (24 bytes) plus discriminant is now the largest \
         variant, since Distribution holds only a Box<DdSketch> (8 bytes)"
    );
    assert_eq!(
        size_of::<MetricRecord>(),
        40,
        "MetricKind + a Symbol + a niche-free Option<Symbol>"
    );
    assert_eq!(
        size_of::<MetricList>(),
        48,
        "SmallVec<[MetricRecord; 1]>: the inline record, plus 8 bytes of capacity-and-\
         discriminant overhead (smallvec's `union` feature, enabled workspace-wide in \
         Cargo.toml, saved the other 8)"
    );
}

#[test]
fn record_types() {
    assert_eq!(size_of::<LogRecord>(), 48);
    assert_eq!(size_of::<SpanRecord>(), 136);
    assert_eq!(size_of::<Resource>(), size_of::<AttrMap>());

    // `LogRecord` is free: `Severity` is a small field-less enum, so its spare discriminant
    // absorbs the `None` case. Worth asserting rather than assuming -- adding a 256-variant enum
    // would silently cost `Event` another 8 bytes.
    assert_eq!(size_of::<Option<LogRecord>>(), size_of::<LogRecord>());

    // `SpanRecord` itself is unchanged (136 bytes) -- what changed (`docs/design/memory.md` §8
    // item 9) is that `Event` no longer inlines it directly. `Box<SpanRecord>`'s pointer niche
    // absorbs `None` the same way `Symbol`'s and `Severity`'s do, so `Option<Box<SpanRecord>>` is
    // exactly a pointer, not a pointer plus a discriminant.
    assert_eq!(size_of::<Box<SpanRecord>>(), 8);
    assert_eq!(
        size_of::<Option<Box<SpanRecord>>>(),
        8,
        "Box's NonNull niche should absorb the None case"
    );
}

/// The number that matters: what one event costs to move between two pipeline nodes, and to deep-
/// clone for each extra fan-out consumer. `docs/design/memory.md` breaks this down term by term
/// and lists what could be reclaimed.
///
/// Note what this means for the cheap cases: a bare log line with two attributes now costs 504
/// bytes to move (down from 792 before this pass' three changes), because `AttrMap`'s inline
/// capacity is still paid unconditionally regardless of whether the event carries a span or a
/// distribution -- but boxing `SpanRecord` (item 9) and `DdSketch` (item 10) mean it no longer
/// pays 136 bytes for a span it doesn't have or 144 bytes of `MetricList` slack for a sketch
/// variant it isn't using either.
#[test]
fn event_size() {
    assert_eq!(
        size_of::<Event>(),
        504,
        "648 minus the 144 bytes boxing DdSketch in MetricKind::Distribution saves (MetricList \
         192 -> 48, `docs/design/memory.md` §8 item 10)"
    );

    // The breakdown, asserted so it can't drift out of sync with the total above.
    let sum = size_of::<i64>()
        + size_of::<AttrMap>()
        + size_of::<Option<LogRecord>>()
        + size_of::<MetricList>()
        + size_of::<Option<Box<SpanRecord>>>();
    assert_eq!(sum, size_of::<Event>(), "Event should have no padding beyond its fields");
}
