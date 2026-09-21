//! The equivalence gate and the allocation counts for the attribute-sizing bake-off's bench-only
//! arms (`docs/plans/event-sizing.md`'s **W3b**, [`logit_bench::bakeoff::attr_arms`]).
//!
//! Two jobs, in the order they matter:
//!
//! 1. **Equivalence.** Every arm must produce the same sorted `(Symbol, Value)` sequence the
//!    shipped `AttrMap` produces from the same input -- **including a repeated key, where the last
//!    write wins**. A faster map that quietly keeps a different duplicate, or iterates in a
//!    different order, is not a candidate: `attrs::merged`, `SeriesKey`, `keep`, the native
//!    encoder's dictionary and Lua's `AttrsProxy` all depend on both properties. This is the same
//!    discipline `tests/wire_format_bakeoff.rs` applies to the wire arms -- fidelity before
//!    timing.
//! 2. **Allocation counts and bytes**, through [`logit_bench::alloc::CountingAlloc`] wrapping
//!    `System`. Counts are allocator-independent, so they belong here rather than in
//!    `benches/attr_arms.rs`, which runs real jemalloc and reports no allocation column
//!    (`docs/design/memory.md` §7).
//!
//! The counts below are **not** pinned the way `tests/allocations.rs` pins the shipped pipeline's:
//! these are mirrors of types nothing ships yet, so an exact constant would be pinning a
//! bench-only decision. What is asserted is the *relationships* the arms are being judged on --
//! that today's clone asks for more bytes than it needs, that a thin nested map allocates less
//! than a boxed one, that a key-set cache hit allocates once. Every measurement is printed as
//! well, so `cargo nextest run -p logit-bench --no-capture -E 'test(attr_arms)'` produces the
//! table W4 re-takes on the perf VM.

use logit_bench::alloc::{measure, CountingAlloc, Stats};
use logit_bench::bakeoff::attr_arms::clone_arms::{is_scalar, MirrorMap, PodFlagMap};
use logit_bench::bakeoff::attr_arms::keyset::{KeySetCache, KeySetMap, TransitionCache};
use logit_bench::bakeoff::attr_arms::shapes::{self, Gateway, Mix, GATEWAY_SETS};
use logit_bench::bakeoff::attr_arms::thin::{self, ThinMap, ThinValue};
use logit_core::interner::{intern, Symbol};
use logit_core::{AttrMap, Value};

/// Installed for this test binary only -- no other crate's tests pay the counting overhead.
#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new(std::alloc::System);

/// Prints one measurement in the same column shape `tests/allocations.rs` uses.
fn report(label: &str, stats: Stats) {
    println!(
        "{label:<46} allocs={:<4} reallocs={:<4} bytes={:<7} peak_live={}",
        stats.allocs, stats.reallocs, stats.bytes, stats.peak_live_bytes
    );
}

/// The sorted sequence an arm produced, resolved to comparable pairs.
fn pairs<'a>(iter: impl Iterator<Item = (Symbol, &'a Value)>) -> Vec<(Symbol, Value)> {
    iter.map(|(k, v)| (k, v.clone())).collect()
}

fn attr_pairs(map: &AttrMap) -> Vec<(Symbol, Value)> {
    pairs(map.iter())
}

// ---------------------------------------------------------------------------------------------
// Equivalence: every arm agrees with `AttrMap`
// ---------------------------------------------------------------------------------------------

/// The gate. Over every survey width and every value mix, each arm's sorted sequence must equal
/// the shipped `AttrMap`'s, built from the identical arrival-order scratch.
#[test]
fn every_arm_matches_attrmap_on_every_survey_shape() {
    for width in shapes::WIDTHS {
        for mix in shapes::MIXES {
            let scratch = shapes::scratch("eq", width, mix);
            let expected = attr_pairs(&shapes::attr_map(&scratch));
            assert_eq!(expected.len(), width, "the scratch keys should be distinct");

            assert_eq!(shapes::bulk_build(&scratch), expected, "bulk build at {width}/{mix:?}");
            assert_eq!(
                pairs(MirrorMap::from_scratch(&scratch).iter()),
                expected,
                "mirror map at {width}/{mix:?}"
            );
            assert_eq!(
                pairs(PodFlagMap::from_scratch(&scratch).iter()),
                expected,
                "pod-flag map at {width}/{mix:?}"
            );
            assert_eq!(
                pairs(KeySetMap::build_uncached(scratch.clone()).iter()),
                expected,
                "key-set map at {width}/{mix:?}"
            );
            let mut cache = KeySetCache::new(64);
            // Once to learn the shape, once to take the hit path -- both must agree.
            assert_eq!(pairs(cache.build(scratch.clone()).iter()), expected, "key-set miss path");
            assert_eq!(pairs(cache.build(scratch.clone()).iter()), expected, "key-set hit path");
            assert_eq!(cache.hits, 1, "the second build should hit");
        }
    }
}

/// A repeated key is the case that separates a correct bulk build from a fast one.
/// `AttrMap::insert` is last-write-wins, so an arm that sorts unstably, or dedups keeping the
/// first entry, produces a *different event* -- and real parsers do repeat keys (`statsd_in`'s
/// tags, `syslog_in`'s SD params).
#[test]
fn a_repeated_key_keeps_the_last_value_in_every_arm() {
    let a = intern("w3b.dup.a");
    let b = intern("w3b.dup.b");
    let scratch = vec![
        (b, Value::I64(1)),
        (a, Value::I64(2)),
        (b, Value::I64(3)),
        (a, Value::str("last-a")),
        (b, Value::str("last-b")),
    ];

    let mut expected_map = AttrMap::new();
    for (key, value) in &scratch {
        expected_map.insert_sym(*key, value.clone());
    }
    let expected = attr_pairs(&expected_map);
    assert_eq!(expected.len(), 2, "two distinct keys");
    assert_eq!(expected_map.get_sym(a), Some(&Value::str("last-a")));
    assert_eq!(expected_map.get_sym(b), Some(&Value::str("last-b")));

    assert_eq!(shapes::bulk_build(&scratch), expected, "bulk build must be last-write-wins");
    assert_eq!(pairs(MirrorMap::from_scratch(&scratch).iter()), expected, "mirror map");
    assert_eq!(pairs(PodFlagMap::from_scratch(&scratch).iter()), expected, "pod-flag map");
    // Arm K's slow path: a repeated key makes the slot permutation not a permutation, so
    // `place` falls back to a sorted build. That fallback must still be last-write-wins.
    assert_eq!(pairs(KeySetMap::build_uncached(scratch.clone()).iter()), expected, "key-set map");
    let mut cache = KeySetCache::new(64);
    assert_eq!(pairs(cache.build(scratch.clone()).iter()), expected, "key-set miss path");
    assert_eq!(pairs(cache.build(scratch.clone()).iter()), expected, "key-set hit path");
}

/// Every arm-C candidate must produce a map equal to the one the shipped clone produces, on every
/// mix -- the bitwise-copy paths especially, since those are the ones that could silently duplicate
/// a `Bytes` without bumping its refcount.
#[test]
fn every_clone_candidate_equals_the_baseline_clone() {
    for width in shapes::WIDTHS {
        for mix in shapes::MIXES {
            let map = MirrorMap::from_scratch(&shapes::scratch("cc", width, mix));
            let baseline = map.clone_baseline();
            assert_eq!(map.clone_exact_loop(), baseline, "exact_loop at {width}/{mix:?}");
            assert_eq!(map.clone_scalar_branch(), baseline, "scalar_branch at {width}/{mix:?}");
            assert_eq!(map.clone_detect_pod(), baseline, "detect_pod at {width}/{mix:?}");

            let flagged = PodFlagMap::from_scratch(&shapes::scratch("cc", width, mix));
            assert_eq!(flagged.clone_flagged(), flagged, "flagged clone at {width}/{mix:?}");
            assert_eq!(
                flagged.all_scalar(),
                mix == Mix::Scalar || width == 0,
                "the flag should track the mix at {width}/{mix:?}"
            );
        }
    }
}

/// A cloned `Value::Str` must share its buffer with the original, not copy it -- the property
/// every bitwise-copy candidate has to preserve, and the one a bug in [`is_scalar`] would break.
#[test]
fn a_cloned_string_value_still_shares_its_buffer() {
    let map = MirrorMap::from_scratch(&shapes::scratch("share", 4, Mix::AllStr));
    for candidate in [
        MirrorMap::clone_baseline as fn(&MirrorMap) -> MirrorMap,
        MirrorMap::clone_exact_loop,
        MirrorMap::clone_scalar_branch,
        MirrorMap::clone_detect_pod,
    ] {
        let cloned = candidate(&map);
        for ((_, original), (_, copy)) in map.iter().zip(cloned.iter()) {
            match (original, copy) {
                (Value::Str(a), Value::Str(b)) => {
                    assert_eq!(a.as_ptr(), b.as_ptr(), "a string clone must share, not copy");
                }
                _ => unreachable!("this fixture is all strings"),
            }
        }
    }
}

/// `is_scalar` decides whether a bitwise copy is sound, so it is checked against every variant
/// rather than trusted. A new `Value` variant makes this test fail to compile, which is the point.
#[test]
fn is_scalar_classifies_every_value_variant() {
    let cases = [
        (Value::Null, true),
        (Value::Bool(true), true),
        (Value::I64(1), true),
        (Value::U64(1), true),
        (Value::F64(1.0), true),
        (Value::Timestamp(1), true),
        (Value::Bytes(bytes::Bytes::from_static(b"x")), false),
        (Value::str("x"), false),
        (Value::Array(vec![Value::I64(1)]), false),
        (Value::Map(Box::new(AttrMap::new())), false),
    ];
    for (value, expected) in &cases {
        assert_eq!(is_scalar(value), *expected, "is_scalar({value:?})");
    }
    assert_eq!(cases.len(), 10, "every `Value` variant is covered");
}

/// Arm E's thin map must behave like an `AttrMap`: sorted iteration, binary-search lookup,
/// last-write-wins insert, removal.
#[test]
fn the_thin_map_matches_attrmap_semantics() {
    let keys = shapes::keys("thin-sem", 5);
    let mut thin_map = ThinMap::new();
    let mut attr_map = AttrMap::new();
    for (i, key) in shapes::shuffled(&keys).iter().enumerate() {
        thin_map.insert_sym(*key, ThinValue::I64(i as i64));
        attr_map.insert_sym(*key, Value::I64(i as i64));
    }
    // Last write wins, on both.
    thin_map.insert_sym(keys[2], ThinValue::I64(99));
    attr_map.insert_sym(keys[2], Value::I64(99));

    let thin_keys: Vec<Symbol> = thin_map.iter().map(|(k, _)| k).collect();
    let attr_keys: Vec<Symbol> = attr_map.iter().map(|(k, _)| k).collect();
    assert_eq!(thin_keys, attr_keys, "both iterate in sorted-symbol order");
    assert_eq!(thin_map.get_sym(keys[2]), Some(&ThinValue::I64(99)));
    assert_eq!(thin_map.remove_sym(keys[2]), Some(ThinValue::I64(99)));
    assert_eq!(thin_map.get_sym(keys[2]), None);
    assert_eq!(thin_map.len(), attr_map.len() - 1);
}

/// The nested fixtures must reproduce `docs/design/data-shapes.md` §5.3's measured pino-http
/// shape -- 10 top-level attributes, four maps at median width 3, depth 2 -- or arm E's numbers
/// describe some other record.
#[test]
fn the_nested_fixtures_match_the_measured_pino_shape() {
    let today = thin::nested_today(&thin::pino_keys(), Mix::Mostly);
    assert_eq!(today.len(), 10, "ten top-level attributes");
    let mut maps = 0;
    let mut widths = Vec::new();
    for (_, value) in today.iter() {
        if let Value::Map(inner) = value {
            maps += 1;
            widths.push(inner.len());
            for (_, nested) in inner.iter() {
                if let Value::Map(headers) = nested {
                    maps += 1;
                    widths.push(headers.len());
                }
            }
        }
    }
    assert_eq!(maps, 4, "four maps");
    assert_eq!(widths, vec![3, 3, 3, 3], "median width 3");

    let thin_map = thin::nested_thin(&thin::pino_keys(), Mix::Mostly);
    assert_eq!(thin_map.len(), today.len(), "the thin fixture is the same shape");
    let today_keys: Vec<Symbol> = today.iter().map(|(k, _)| k).collect();
    let thin_keys: Vec<Symbol> = thin_map.iter().map(|(k, _)| k).collect();
    assert_eq!(thin_keys, today_keys, "same keys, same order");
}

/// Arm K's shape transitions, cached and not, must land in the same place -- and the cached one
/// must actually reuse a key-set rather than rebuilding one that merely compares equal.
#[test]
fn a_cached_shape_transition_reuses_the_key_set() {
    let scratch = shapes::scratch("trans", 12, Mix::Mostly);
    let added = intern("w3b.trans.added");

    let mut uncached = KeySetMap::build_uncached(scratch.clone());
    uncached.insert_one(added, Value::I64(7));

    let mut cache = TransitionCache::new();
    let mut first = KeySetMap::build_uncached(scratch.clone());
    first.insert_one_cached(added, Value::I64(7), &mut cache);
    let mut second = KeySetMap::build_uncached(scratch.clone());
    second.insert_one_cached(added, Value::I64(7), &mut cache);

    assert_eq!(pairs(first.iter()), pairs(uncached.iter()), "cached transition agrees");
    assert_eq!(pairs(second.iter()), pairs(uncached.iter()), "and again on the hit");
    assert_eq!(cache.len(), 1, "one learned transition");
    assert!(
        std::sync::Arc::ptr_eq(first.keys(), second.keys()),
        "the second transition should share the first's key-set, not rebuild it"
    );
    assert_eq!(first.get_sym(added), Some(&Value::I64(7)));

    let mut removed = first.clone();
    assert_eq!(removed.remove_one(added), Some(Value::I64(7)));
    assert_eq!(pairs(removed.iter()), pairs(KeySetMap::build_uncached(scratch).iter()));
}

// ---------------------------------------------------------------------------------------------
// Layout
// ---------------------------------------------------------------------------------------------

/// The mirrors must have the layout they claim to, or every number taken from them is about some
/// other type. Relationships, not absolute constants: `crates/logit-core/tests/type_sizes.rs` owns
/// the absolute pins for the shipped types.
#[test]
fn the_mirrors_have_the_layout_they_claim() {
    use std::mem::size_of;

    println!("size_of::<Value>()       = {}", size_of::<Value>());
    println!("size_of::<ThinValue>()   = {}", size_of::<ThinValue>());
    println!("size_of::<AttrMap>()     = {}", size_of::<AttrMap>());
    println!("size_of::<MirrorMap>()   = {}", size_of::<MirrorMap>());
    println!("size_of::<PodFlagMap>()  = {}", size_of::<PodFlagMap>());
    println!("size_of::<ThinMap>()     = {}", size_of::<ThinMap>());
    println!("size_of::<KeySetMap>()   = {}", size_of::<KeySetMap>());

    assert_eq!(size_of::<MirrorMap>(), size_of::<AttrMap>(), "arm C's mirror is layout-identical");
    assert_eq!(
        size_of::<ThinValue>(),
        size_of::<Value>(),
        "a `Vec`-backed map fits in `Value`'s existing footprint -- the premise of arm E's first case"
    );
    assert_eq!(size_of::<ThinMap>(), 24, "three words, no inline capacity");
    assert_eq!(
        size_of::<KeySetMap>(),
        size_of::<Arc<[Symbol]>>() + size_of::<Vec<Value>>(),
        "a fat pointer plus a vector"
    );
    assert!(
        size_of::<PodFlagMap>() <= size_of::<AttrMap>() + size_of::<usize>(),
        "the all-scalar flag costs at most one word of footprint"
    );
}

use std::sync::Arc;

// ---------------------------------------------------------------------------------------------
// Allocation counts and bytes
// ---------------------------------------------------------------------------------------------

/// What each arm allocates to **build** a map, at every survey width. The scratch buffer is built
/// and warmed outside the measured region, so what is counted is the map alone.
#[test]
fn build_allocations_by_arm() {
    for width in shapes::WIDTHS {
        let scratch = shapes::scratch("ba", width, Mix::Mostly);
        // Warm: the interner, and `Bytes`' first-clone promotion (see `shapes::shared_str`).
        drop(shapes::attr_map(&scratch));
        drop(shapes::bulk_build(&scratch));
        drop(KeySetMap::build_uncached(scratch.clone()));

        let (map, attr) = measure(|| shapes::attr_map(&scratch));
        report(&format!("build attrmap w={width}"), attr);
        let (bulk, bulk_stats) = measure(|| shapes::bulk_build(&scratch));
        report(&format!("build bulk-sort w={width}"), bulk_stats);
        // The scratch clone is hoisted out of every measured region: arm K consumes its input,
        // so leaving the clone inside would charge it one `Vec` the other arms never pay.
        let input = scratch.clone();
        let (uncached, miss) = measure(|| KeySetMap::build_uncached(input));
        report(&format!("build keyset-miss w={width}"), miss);

        let mut cache = KeySetCache::new(64);
        drop(cache.build(scratch.clone()));
        let input = scratch.clone();
        let (hit_map, hit) = measure(|| cache.build(input));
        report(&format!("build keyset-hit w={width}"), hit);

        assert_eq!(map.len(), width);
        assert_eq!(bulk.len(), width);
        assert_eq!(uncached.len(), width);
        assert_eq!(hit_map.len(), width);

        // The one relationship worth asserting: today's build allocates nothing inside the inline
        // capacity and exactly once past it, while both bulk arms allocate whatever their width
        // needs at any width above zero. That asymmetry is arm S's whole subject.
        if width <= 8 {
            assert_eq!(attr.allocs, 0, "an inline `AttrMap` build allocates nothing at w={width}");
        } else {
            assert_eq!(attr.allocs, 1, "one spill at w={width}");
        }
        // A cache hit costs the values vector and nothing else -- no key-set, no permutation.
        assert_eq!(hit.allocs, 1, "a key-set cache hit allocates once at w={width}");
    }
}

/// What each arm allocates to **clone** one, and -- the finding this test exists for -- how many
/// bytes it asks for.
///
/// smallvec's `reserve` rounds `len + additional` up to the next power of two, and `Clone` goes
/// through it, so cloning a 9- or 12-entry `AttrMap` asks for **16 × 48 = 768 bytes** and a 17- or
/// 30-entry one for **32 × 48 = 1536**. `SmallVec::with_capacity` (which arm C's `clone_exact_loop`
/// uses) goes through `reserve_exact` and asks for exactly what it needs.
#[test]
fn clone_allocations_and_bytes_by_arm() {
    for width in shapes::WIDTHS {
        let scratch = shapes::scratch("ca", width, Mix::Mostly);
        let attr = shapes::attr_map(&scratch);
        let mirror = MirrorMap::from_scratch(&scratch);
        let keyset = KeySetMap::build_uncached(scratch.clone());
        drop((attr.clone(), mirror.clone_exact_loop(), keyset.clone()));

        let (_, baseline) = measure(|| attr.clone());
        report(&format!("clone attrmap w={width}"), baseline);
        let (_, exact) = measure(|| mirror.clone_exact_loop());
        report(&format!("clone exact-loop w={width}"), exact);
        let (_, detect) = measure(|| mirror.clone_detect_pod());
        report(&format!("clone detect-pod w={width}"), detect);
        let (_, ks) = measure(|| keyset.clone());
        report(&format!("clone keyset w={width}"), ks);

        if width > 8 {
            assert!(
                exact.bytes < baseline.bytes,
                "at w={width} an exactly-sized clone should ask for fewer bytes than smallvec's \
                 next-power-of-two reserve ({} vs {})",
                exact.bytes,
                baseline.bytes
            );
            assert_eq!(exact.bytes, width as u64 * 48, "exactly `width` entries");
        }
        // Arm K clones an `Arc` (free) plus a values vector (one allocation at any non-zero
        // width) -- where today's clone allocates only past the inline capacity.
        assert_eq!(ks.allocs, u64::from(width > 0), "one values vector at w={width}");
    }
}

/// Arm E, case 1: a nested map as `Value::Map(Box<AttrMap>)` against an inline, exactly-sized one.
/// Four boxed maps of three entries each is W1's costliest survey shape.
#[test]
fn nested_map_allocations_today_versus_thin() {
    for shape in ["pino", "1map", "4map"] {
        let keys = match shape {
            "pino" => thin::pino_keys(),
            "1map" => thin::synthetic_keys(1),
            _ => thin::synthetic_keys(4),
        };
        let today = thin::nested_today(&keys, Mix::Mostly);
        let thin_map = thin::nested_thin(&keys, Mix::Mostly);
        drop((today.clone(), thin_map.clone()));

        let (_, today_clone) = measure(|| today.clone());
        report(&format!("clone nested today {shape}"), today_clone);
        let (_, thin_clone) = measure(|| thin_map.clone());
        report(&format!("clone nested thin  {shape}"), thin_clone);

        assert_eq!(
            today_clone.allocs, thin_clone.allocs,
            "{shape}: the same number of allocations either way -- one per nested map, plus the \
             spill; what changes is how many bytes each asks for"
        );
        assert!(
            thin_clone.bytes < today_clone.bytes,
            "{shape}: a three-entry map should not cost a full-size `AttrMap` ({} vs {})",
            thin_clone.bytes,
            today_clone.bytes
        );
    }
}

/// Arm E, case 2: `Scope` and `Resource` widths. A `Scope`'s median is 0 attributes, where today's
/// embedded `SmallVec` still costs its full inline footprint and a `Vec`-backed one costs three
/// words and allocates nothing.
#[test]
fn embedding_allocations_by_width() {
    for width in [0usize, 5, 17, 29] {
        let scratch = shapes::scratch("eb", width, Mix::Mostly);
        drop(shapes::attr_map(&scratch));
        drop(thin::thin_map(&scratch));

        let (attr, attr_stats) = measure(|| shapes::attr_map(&scratch));
        report(&format!("build embed attrmap w={width}"), attr_stats);
        let (thin_map, thin_stats) = measure(|| thin::thin_map(&scratch));
        report(&format!("build embed thin    w={width}"), thin_stats);

        let (_, attr_clone) = measure(|| attr.clone());
        report(&format!("clone embed attrmap w={width}"), attr_clone);
        let (_, thin_clone) = measure(|| thin_map.clone());
        report(&format!("clone embed thin    w={width}"), thin_clone);

        assert_eq!(attr.len(), width);
        assert_eq!(thin_map.len(), width);
        if width == 0 {
            assert_eq!(attr_clone.allocs, 0, "an empty inline map clones for free");
            assert_eq!(thin_clone.allocs, 0, "and so does an empty `Vec`");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The gateway distribution and its cache
// ---------------------------------------------------------------------------------------------

/// The synthesized gateway must reproduce the two numbers `docs/design/data-shapes.md` §4 reports
/// -- 196 distinct key-sets, top-1 9%, top-5 36% -- or arm K's adversarial case is adversarial in
/// some other way. See `shapes::Gateway` for why it is a two-component distribution and not a Zipf.
#[test]
fn the_gateway_distribution_matches_the_surveys_numbers() {
    let gateway = Gateway::new(20_000);
    assert_eq!(gateway.sets.len(), GATEWAY_SETS);

    let mut counts = vec![0usize; GATEWAY_SETS];
    for &set in &gateway.stream {
        counts[set] += 1;
    }
    assert!(counts.iter().all(|&c| c > 0), "every key-set appears at least once");

    let total: usize = counts.iter().sum();
    let mut sorted = counts.clone();
    sorted.sort_unstable_by(|a, b| b.cmp(a));
    let top1 = sorted[0] as f64 / total as f64;
    let top5: f64 = sorted[..5].iter().sum::<usize>() as f64 / total as f64;
    println!("gateway: events={total} sets={GATEWAY_SETS} top1={top1:.3} top5={top5:.3}");

    assert!((top1 - 0.09).abs() < 0.01, "top-1 should be ~9%, got {top1:.3}");
    assert!((top5 - 0.36).abs() < 0.02, "top-5 should be ~36%, got {top5:.3}");
}

/// What a bounded cache actually achieves on that distribution -- the number every gateway timing
/// has to be read beside. 64 entries against 196 shapes is the survey's own comparison
/// (`logit_core::interner::KeyCache` is 64 entries).
#[test]
fn the_bounded_key_set_cache_misses_on_the_gateway_tail() {
    let gateway = Gateway::new(2000);
    let stream: Vec<Vec<(Symbol, Value)>> =
        gateway.stream.iter().map(|&set| gateway.event_scratch(set, Mix::Mostly)).collect();

    for capacity in [64usize, GATEWAY_SETS * 2] {
        let mut cache = KeySetCache::new(capacity);
        for scratch in &stream {
            drop(cache.build(scratch.clone()));
        }
        println!(
            "gateway cache capacity={capacity:<4} events={} hits={} misses={} evictions={} \
             miss_rate={:.3}",
            stream.len(),
            cache.hits,
            cache.misses,
            cache.evictions,
            cache.miss_rate()
        );
        if capacity >= GATEWAY_SETS {
            assert_eq!(
                cache.misses as usize, GATEWAY_SETS,
                "with room for every shape, each one is learned exactly once"
            );
            assert_eq!(cache.evictions, 0);
        } else {
            assert!(
                cache.misses as usize > GATEWAY_SETS,
                "a 64-entry cache against 196 shapes must re-learn evicted ones"
            );
        }
    }
}
