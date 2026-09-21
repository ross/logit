//! Timings for the attribute-sizing bake-off's bench-only arms -- `docs/plans/event-sizing.md`'s
//! **W3b**, over [`logit_bench::bakeoff::attr_arms`]. Arm **C** (the clone path), arm **E**
//! (per-embedding capacity) and arm **K** (a shared key-set) each have a module below; the arms
//! themselves, and every simplification their mirrors make, are documented where they are defined.
//!
//! **Allocator: real jemalloc, no counting wrapper**, for `benches/size_vs_alloc.rs`'s reasons --
//! production runs jemalloc ([ADR `jemalloc-global-allocator`](../../../docs/adr/jemalloc-global-allocator.md)),
//! and a counter increment inside the timed region distorts exactly the operation in question. So
//! there is no allocation column here; the counts for these same arms and shapes live in
//! `tests/attr_arms.rs`.
//!
//! **Everything is measured in the "consumed" shape**, and that is not incidental. divan stores
//! each iteration's return value in a pre-allocated slot when the output needs dropping
//! (`divan-0.1.21`'s `benchmark/mod.rs`), so a bench that *returns* a ~400-byte
//! `SmallVec<[(Symbol, Value); 8]>` writes 400 bytes into a fresh slot of a large buffer every
//! iteration -- memory traffic the benched code never performs. Every comparison below therefore
//! builds or clones, `black_box`es a *reference*, and returns `()`, which puts the drop inside the
//! timed region and the output write nowhere. [`clone_c::artifact`] measures the same clone both
//! ways so the size of that distortion is on the record, and [`clone_c::drop_only`] isolates the
//! drop so "clone + drop" can be split.
//!
//! **Run it pinned**, on the perf VM (`docs/design/performance.md` §0):
//!
//! ```sh
//! taskset -c 2 cargo bench -p logit-bench --bench attr_arms
//! ```
//!
//! **None of these numbers belong in a repository document.**

use divan::counter::ItemsCount;
use divan::{black_box, Bencher};
use logit_bench::bakeoff::attr_arms::clone_arms::{MirrorMap, PodFlagMap};
use logit_bench::bakeoff::attr_arms::keyset::{KeySetCache, KeySetMap, TransitionCache};
use logit_bench::bakeoff::attr_arms::shapes::{self, Mix};
use logit_bench::bakeoff::attr_arms::thin;
use logit_bench::fixtures;
use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Value};
use logit_pipeline::Transform;

/// The real allocator, unwrapped -- see this file's module doc.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    divan::main();
}

/// Consumes a value **inside** the timed region: makes it opaque to the optimizer, then drops it.
///
/// This is the "consumed" shape this file's module doc describes, and every comparison below uses
/// it. Returning `()` -- a zero-sized output that needs no drop -- is what keeps divan on its
/// cheap sample loop instead of writing each iteration's result into a defer slot.
#[inline]
fn consume<T>(value: T) {
    black_box(&value);
}

/// `docs/design/data-shapes.md`'s widths: 2 (a metric event), 8 (the last inline width), 9 (the
/// first spilled one), 12 (the commonest JSON log), 17 (the span p90), 30 (an access log).
const WIDTHS: [usize; 6] = shapes::WIDTHS;

/// The commonest measured log width, where the by-mix and kill-criterion comparisons are taken.
const LOG_WIDTH: usize = 12;

/// Value mixes by label, since divan names an argument by its `ToString`.
const MIXES: [&str; 3] = ["scalar", "str75", "str100"];

fn mix(label: &str) -> Mix {
    match label {
        "scalar" => Mix::Scalar,
        "str75" => Mix::Mostly,
        "str100" => Mix::AllStr,
        other => unreachable!("unknown mix {other}"),
    }
}

/// **Arm C -- the clone path.**
mod clone_c {
    use super::*;

    fn mirror(width: usize, mix: Mix) -> MirrorMap {
        MirrorMap::from_scratch(&shapes::scratch("c", width, mix))
    }

    fn flagged(width: usize, mix: Mix) -> PodFlagMap {
        PodFlagMap::from_scratch(&shapes::scratch("c", width, mix))
    }

    /// Every candidate across the survey's widths, at the middle of the measured string share
    /// (75%). The shipped path is `baseline`; everything else is a candidate replacement.
    mod by_width {
        use super::*;

        #[divan::bench(args = WIDTHS)]
        fn baseline(bencher: Bencher, width: usize) {
            let map = mirror(width, Mix::Mostly);
            bencher.bench_local(|| consume(black_box(&map).clone_baseline()));
        }

        #[divan::bench(args = WIDTHS)]
        fn exact_loop(bencher: Bencher, width: usize) {
            let map = mirror(width, Mix::Mostly);
            bencher.bench_local(|| consume(black_box(&map).clone_exact_loop()));
        }

        #[divan::bench(args = WIDTHS)]
        fn scalar_branch(bencher: Bencher, width: usize) {
            let map = mirror(width, Mix::Mostly);
            bencher.bench_local(|| consume(black_box(&map).clone_scalar_branch()));
        }

        #[divan::bench(args = WIDTHS)]
        fn detect_pod(bencher: Bencher, width: usize) {
            let map = mirror(width, Mix::Mostly);
            bencher.bench_local(|| consume(black_box(&map).clone_detect_pod()));
        }

        #[divan::bench(args = WIDTHS)]
        fn pod_flag(bencher: Bencher, width: usize) {
            let map = flagged(width, Mix::Mostly);
            bencher.bench_local(|| consume(black_box(&map).clone_flagged()));
        }
    }

    /// The same candidates at the commonest log width, across the value mixes -- all scalars (the
    /// only mix a bitwise copy is legal on, and the one W2 measured), 75% strings (the middle of
    /// the survey's band), and all strings (an atomic increment per entry).
    mod by_mix {
        use super::*;

        #[divan::bench(args = MIXES)]
        fn baseline(bencher: Bencher, label: &str) {
            let map = mirror(LOG_WIDTH, mix(label));
            bencher.bench_local(|| consume(black_box(&map).clone_baseline()));
        }

        #[divan::bench(args = MIXES)]
        fn exact_loop(bencher: Bencher, label: &str) {
            let map = mirror(LOG_WIDTH, mix(label));
            bencher.bench_local(|| consume(black_box(&map).clone_exact_loop()));
        }

        #[divan::bench(args = MIXES)]
        fn scalar_branch(bencher: Bencher, label: &str) {
            let map = mirror(LOG_WIDTH, mix(label));
            bencher.bench_local(|| consume(black_box(&map).clone_scalar_branch()));
        }

        #[divan::bench(args = MIXES)]
        fn detect_pod(bencher: Bencher, label: &str) {
            let map = mirror(LOG_WIDTH, mix(label));
            bencher.bench_local(|| consume(black_box(&map).clone_detect_pod()));
        }

        #[divan::bench(args = MIXES)]
        fn pod_flag(bencher: Bencher, label: &str) {
            let map = flagged(LOG_WIDTH, mix(label));
            bencher.bench_local(|| consume(black_box(&map).clone_flagged()));
        }
    }

    /// The shipped `AttrMap::clone` itself, as a control on the mirror: if these two disagree past
    /// noise, the mirror is not measuring what it claims to.
    #[divan::bench(args = WIDTHS)]
    fn real_attrmap(bencher: Bencher, width: usize) {
        let map = shapes::attr_map(&shapes::scratch("c", width, Mix::Mostly));
        bencher.bench_local(|| consume(black_box(&map).clone()));
    }

    /// Dropping a clone, with the clone itself generated outside the timed region -- subtract this
    /// from any bench above to get the clone alone.
    #[divan::bench(args = WIDTHS)]
    fn drop_only(bencher: Bencher, width: usize) {
        let map = mirror(width, Mix::Mostly);
        bencher.with_inputs(|| map.clone_baseline()).bench_local_values(drop);
    }

    /// **The measurement artifact, on the record.** The identical clone, consumed in place versus
    /// returned into divan's defer slot. The gap is the per-iteration write of a ~400-byte output
    /// into a multi-megabyte buffer, and it is the reason W2's §6 read ~190 ns for what a 392-byte
    /// copy should cost single-digit nanoseconds -- and the reason its §3 build comparison, where
    /// one arm returns an `AttrMap` and the other a 24-byte `Vec`, is not like-for-like.
    mod artifact {
        use super::*;

        #[divan::bench(args = [8usize, 12])]
        fn consumed(bencher: Bencher, width: usize) {
            let map = mirror(width, Mix::Scalar);
            bencher.bench_local(|| consume(black_box(&map).clone_baseline()));
        }

        #[divan::bench(args = [8usize, 12])]
        fn returned(bencher: Bencher, width: usize) {
            let map = mirror(width, Mix::Scalar);
            bencher.bench_local(|| black_box(&map).clone_baseline())
        }
    }

    /// `Event::clone` on W1's six survey shapes -- the whole-event cost a fan-out really pays, of
    /// which the attribute map is one part. Nothing here is a mirror: these are the shipped types.
    mod event_clone {
        use super::*;

        fn parsed(mut event: Event) -> Event {
            let mut json = fixtures::json_parser();
            let resource = fixtures::resource();
            assert!(json.process(&resource, &mut event), "json always forwards");
            event
        }

        /// 12 flat attributes, the commonest measured log shape.
        #[divan::bench]
        fn flat_json_log(bencher: Bencher) {
            let event = parsed(fixtures::flat_json_log_event());
            bencher.bench_local(|| consume(black_box(&event).clone()));
        }

        /// 10 attributes with four boxed nested maps -- W1's costliest shape, five allocations to
        /// clone where the 30-attribute log takes one.
        #[divan::bench]
        fn pino_http_nested(bencher: Bencher) {
            let event = parsed(fixtures::pino_http_log_event());
            bencher.bench_local(|| consume(black_box(&event).clone()));
        }

        /// 30 attributes, one spilled allocation.
        #[divan::bench]
        fn access_log(bencher: Bencher) {
            let event = parsed(fixtures::access_log_event());
            bencher.bench_local(|| consume(black_box(&event).clone()));
        }

        /// 17 attributes, no span events and no links (the measured shape).
        #[divan::bench]
        fn wide_server_span(bencher: Bencher) {
            let event = fixtures::wide_server_span_event();
            bencher.bench_local(|| consume(black_box(&event).clone()));
        }

        /// 6 attributes (inline) and three metric records (spilled) -- the inverse shape.
        #[divan::bench]
        fn collectd_three_record(bencher: Bencher) {
            let event = fixtures::collectd_three_record_event();
            bencher.bench_local(|| consume(black_box(&event).clone()));
        }

        /// Five 9-attribute events sharing a 17-attribute `Arc`-shared resource: the copy-on-write
        /// `EventBatch::clone` a contended fan-out pays, per event.
        #[divan::bench]
        fn enriched_resource_batch(bencher: Bencher) {
            let batch = fixtures::enriched_resource_batch();
            bencher
                .counter(ItemsCount::new(batch.events.len()))
                .bench_local(|| consume(black_box(&batch).clone()));
        }
    }
}

/// **Arm E -- per-embedding capacity.**
mod embed_e {
    use super::*;

    /// The nested-map shapes: the pino-http record (four maps, the measured one) and the 1-map and
    /// 4-map synthetics either side of it.
    const NESTED: [&str; 3] = ["pino", "1map", "4map"];

    /// Interned once, outside every timed region -- see `thin::NestedKeys`.
    fn nested_keys(shape: &str) -> thin::NestedKeys {
        match shape {
            "pino" => thin::pino_keys(),
            "1map" => thin::synthetic_keys(1),
            "4map" => thin::synthetic_keys(4),
            other => unreachable!("unknown nested shape {other}"),
        }
    }

    /// `Value::Map(Box<AttrMap>)` against an inline, exactly-sized map.
    mod nested {
        use super::*;

        #[divan::bench(args = NESTED)]
        fn build_today(bencher: Bencher, shape: &str) {
            let keys = nested_keys(shape);
            bencher.bench_local(|| consume(thin::nested_today(black_box(&keys), Mix::Mostly)));
        }

        #[divan::bench(args = NESTED)]
        fn build_thin(bencher: Bencher, shape: &str) {
            let keys = nested_keys(shape);
            bencher.bench_local(|| consume(thin::nested_thin(black_box(&keys), Mix::Mostly)));
        }

        #[divan::bench(args = NESTED)]
        fn clone_today(bencher: Bencher, shape: &str) {
            let map = thin::nested_today(&nested_keys(shape), Mix::Mostly);
            bencher.bench_local(|| consume(black_box(&map).clone()));
        }

        #[divan::bench(args = NESTED)]
        fn clone_thin(bencher: Bencher, shape: &str) {
            let map = thin::nested_thin(&nested_keys(shape), Mix::Mostly);
            bencher.bench_local(|| consume(black_box(&map).clone()));
        }

        #[divan::bench(args = NESTED)]
        fn drop_today(bencher: Bencher, shape: &str) {
            let map = thin::nested_today(&nested_keys(shape), Mix::Mostly);
            bencher.with_inputs(|| map.clone()).bench_local_values(drop);
        }

        #[divan::bench(args = NESTED)]
        fn drop_thin(bencher: Bencher, shape: &str) {
            let map = thin::nested_thin(&nested_keys(shape), Mix::Mostly);
            bencher.with_inputs(|| map.clone()).bench_local_values(drop);
        }

        /// One top-level lookup that lands on a nested map, then one lookup inside it -- the
        /// `Box` deref today against a `Vec` deref.
        #[divan::bench(args = NESTED)]
        fn lookup_today(bencher: Bencher, shape: &str) {
            let map = thin::nested_today(&nested_keys(shape), Mix::Mostly);
            let (outer, inner) = lookup_pair(shape);
            bencher.bench_local(|| {
                let found = black_box(&map).get_sym(black_box(outer));
                match found {
                    Some(Value::Map(inner_map)) => black_box(inner_map.get_sym(inner)).is_some(),
                    _ => unreachable!("the nested map is always present"),
                }
            });
        }

        #[divan::bench(args = NESTED)]
        fn lookup_thin(bencher: Bencher, shape: &str) {
            let map = thin::nested_thin(&nested_keys(shape), Mix::Mostly);
            let (outer, inner) = lookup_pair(shape);
            bencher.bench_local(|| {
                let found = black_box(&map).get_sym(black_box(outer));
                match found {
                    Some(thin::ThinValue::Map(inner_map)) => {
                        black_box(inner_map.get_sym(inner)).is_some()
                    }
                    _ => unreachable!("the nested map is always present"),
                }
            });
        }

        /// The outer key of a nested map and one key inside it, for the lookup benches.
        fn lookup_pair(shape: &str) -> (Symbol, Symbol) {
            match shape {
                "pino" => (intern("w3b.pino.req"), intern("w3b.pino.req.00")),
                _ => (intern("w3b.syn.map.0"), intern("w3b.syn.m0.00")),
            }
        }
    }

    /// `Scope` and `Resource` widths: 0 (a `Scope`'s median), 5 (a `Resource` outside a
    /// collector), 17 (the measured median behind one), 29 (its measured maximum). A `Resource` is
    /// `Arc`-shared across a batch, so its build is paid once per batch and its clone almost never;
    /// a `Scope` is embedded and pays its footprint per batch whatever it holds.
    mod embedding {
        use super::*;

        const EMBED_WIDTHS: [usize; 4] = [0, 5, 17, 29];

        #[divan::bench(args = EMBED_WIDTHS)]
        fn build_attrmap(bencher: Bencher, width: usize) {
            let scratch = shapes::scratch("e", width, Mix::Mostly);
            bencher.bench_local(|| consume(shapes::attr_map(black_box(&scratch))));
        }

        #[divan::bench(args = EMBED_WIDTHS)]
        fn build_thin(bencher: Bencher, width: usize) {
            let scratch = shapes::scratch("e", width, Mix::Mostly);
            bencher.bench_local(|| consume(thin::thin_map(black_box(&scratch))));
        }

        #[divan::bench(args = EMBED_WIDTHS)]
        fn clone_attrmap(bencher: Bencher, width: usize) {
            let map = shapes::attr_map(&shapes::scratch("e", width, Mix::Mostly));
            bencher.bench_local(|| consume(black_box(&map).clone()));
        }

        #[divan::bench(args = EMBED_WIDTHS)]
        fn clone_thin(bencher: Bencher, width: usize) {
            let map = thin::thin_map(&shapes::scratch("e", width, Mix::Mostly));
            bencher.bench_local(|| consume(black_box(&map).clone()));
        }
    }
}

/// **Arm K -- a shared key-set plus a values vector.**
mod keyset_k {
    use super::*;

    /// The three shapes arm K is judged on: the commonest log (12 flat attributes), the access log
    /// (30), and the nested record's **top-level** width (10) -- arm K changes only the top level,
    /// so a nested map rides along as an ordinary `Value` either way.
    const SHAPES: [&str; 3] = ["log12", "access30", "nested10"];

    fn width(shape: &str) -> usize {
        match shape {
            "log12" => 12,
            "access30" => 30,
            "nested10" => 10,
            other => unreachable!("unknown shape {other}"),
        }
    }

    fn scratch(shape: &str) -> Vec<(Symbol, Value)> {
        shapes::scratch(shape, width(shape), Mix::Mostly)
    }

    mod build {
        use super::*;

        /// Today: `k` sorted `insert_sym` calls.
        #[divan::bench(args = SHAPES)]
        fn sorted_insert(bencher: Bencher, shape: &str) {
            let scratch = scratch(shape);
            bencher
                .with_inputs(|| scratch.clone())
                .bench_local_values(|s| consume(shapes::attr_map(&s)));
        }

        /// Arm P: append, stable sort, dedup-last.
        #[divan::bench(args = SHAPES)]
        fn bulk_sort(bencher: Bencher, shape: &str) {
            let scratch = scratch(shape);
            bencher
                .with_inputs(|| scratch.clone())
                .bench_local_values(|s| consume(shapes::bulk_build(&s)));
        }

        /// Arm K, cache hit: hash the key sequence, `Arc`-bump the key-set, place the values.
        #[divan::bench(args = SHAPES)]
        fn keyset_hit(bencher: Bencher, shape: &str) {
            let scratch = scratch(shape);
            let mut cache = KeySetCache::new(64);
            // Warm the entry so every measured call is a hit.
            drop(cache.build(scratch.clone()));
            bencher.with_inputs(|| scratch.clone()).bench_local_values(|s| consume(cache.build(s)));
        }

        /// Arm K, cache miss: sort, build the permutation, allocate a fresh key-set.
        #[divan::bench(args = SHAPES)]
        fn keyset_miss(bencher: Bencher, shape: &str) {
            let scratch = scratch(shape);
            bencher
                .with_inputs(|| scratch.clone())
                .bench_local_values(|s| consume(KeySetMap::build_uncached(s)));
        }
    }

    mod clone {
        use super::*;

        #[divan::bench(args = SHAPES)]
        fn attrmap(bencher: Bencher, shape: &str) {
            let map = shapes::attr_map(&scratch(shape));
            bencher.bench_local(|| consume(black_box(&map).clone()));
        }

        #[divan::bench(args = SHAPES)]
        fn keyset(bencher: Bencher, shape: &str) {
            let map = KeySetMap::build_uncached(scratch(shape));
            bencher.bench_local(|| consume(black_box(&map).clone()));
        }
    }

    mod lookup {
        use super::*;

        #[divan::bench(args = SHAPES)]
        fn attrmap_hit(bencher: Bencher, shape: &str) {
            let map = shapes::attr_map(&scratch(shape));
            let key = shapes::keys(shape, width(shape))[width(shape) / 2];
            bencher.bench_local(|| black_box(black_box(&map).get_sym(black_box(key))).is_some());
        }

        #[divan::bench(args = SHAPES)]
        fn keyset_hit(bencher: Bencher, shape: &str) {
            let map = KeySetMap::build_uncached(scratch(shape));
            let key = shapes::keys(shape, width(shape))[width(shape) / 2];
            bencher.bench_local(|| black_box(black_box(&map).get_sym(black_box(key))).is_some());
        }

        #[divan::bench(args = SHAPES)]
        fn attrmap_miss(bencher: Bencher, shape: &str) {
            let map = shapes::attr_map(&scratch(shape));
            let key = intern("w3b.absent.probe");
            bencher.bench_local(|| black_box(black_box(&map).get_sym(black_box(key))).is_none());
        }

        #[divan::bench(args = SHAPES)]
        fn keyset_miss(bencher: Bencher, shape: &str) {
            let map = KeySetMap::build_uncached(scratch(shape));
            let key = intern("w3b.absent.probe");
            bencher.bench_local(|| black_box(black_box(&map).get_sym(black_box(key))).is_none());
        }
    }

    /// The shape transition: adding and removing one attribute, which is what a `set`/`remove`
    /// transform does to every event it sees.
    mod mutate {
        use super::*;

        #[divan::bench]
        fn attrmap_insert_one(bencher: Bencher) {
            let map = shapes::attr_map(&scratch("log12"));
            let key = intern("w3b.added.key");
            bencher.with_inputs(|| map.clone()).bench_local_values(|mut m| {
                m.insert_sym(key, Value::I64(1));
                black_box(&m);
            });
        }

        #[divan::bench]
        fn attrmap_remove_one(bencher: Bencher) {
            let map = shapes::attr_map(&scratch("log12"));
            let key = shapes::keys("log12", 12)[6];
            bencher.with_inputs(|| map.clone()).bench_local_values(|mut m| {
                black_box(m.remove_sym(key));
                black_box(&m);
            });
        }

        /// The miss path: every transition rebuilds and reallocates the key-set.
        #[divan::bench]
        fn keyset_insert_one_uncached(bencher: Bencher) {
            let map = KeySetMap::build_uncached(scratch("log12"));
            let key = intern("w3b.added.key");
            bencher.with_inputs(|| map.clone()).bench_local_values(|mut m| {
                m.insert_one(key, Value::I64(1));
                black_box(&m);
            });
        }

        /// The same transition through a memo of `(key-set, added key) -> key-set`.
        #[divan::bench]
        fn keyset_insert_one_cached(bencher: Bencher) {
            let map = KeySetMap::build_uncached(scratch("log12"));
            let key = intern("w3b.added.key");
            let mut cache = TransitionCache::new();
            // Warm the transition so every measured call takes the hit path.
            let mut warm = map.clone();
            warm.insert_one_cached(key, Value::I64(1), &mut cache);
            bencher.with_inputs(|| map.clone()).bench_local_values(|mut m| {
                m.insert_one_cached(key, Value::I64(1), &mut cache);
                black_box(&m);
            });
        }

        #[divan::bench]
        fn keyset_remove_one(bencher: Bencher) {
            let map = KeySetMap::build_uncached(scratch("log12"));
            let key = shapes::keys("log12", 12)[6];
            bencher.with_inputs(|| map.clone()).bench_local_values(|mut m| {
                black_box(m.remove_one(key));
                black_box(&m);
            });
        }
    }

    /// Iterating in sorted-`Symbol` order -- what every encoder and `attrs::merged` does per event.
    mod iterate {
        use super::*;

        #[divan::bench(args = SHAPES)]
        fn attrmap(bencher: Bencher, shape: &str) {
            let map = shapes::attr_map(&scratch(shape));
            let probe = intern("w3b.absent.probe");
            bencher.counter(ItemsCount::new(map.len())).bench_local(|| {
                let mut acc = 0usize;
                for (key, value) in black_box(&map).iter() {
                    acc = acc.wrapping_add((key == probe) as usize);
                    acc = acc.wrapping_add(matches!(value, Value::Str(_)) as usize);
                }
                acc
            });
        }

        #[divan::bench(args = SHAPES)]
        fn keyset(bencher: Bencher, shape: &str) {
            let map = KeySetMap::build_uncached(scratch(shape));
            let probe = intern("w3b.absent.probe");
            bencher.counter(ItemsCount::new(map.len())).bench_local(|| {
                let mut acc = 0usize;
                for (key, value) in black_box(&map).iter() {
                    acc = acc.wrapping_add((key == probe) as usize);
                    acc = acc.wrapping_add(matches!(value, Value::Str(_)) as usize);
                }
                acc
            });
        }
    }

    /// The adversarial case: a mixed OTLP gateway with 196 distinct key-sets, top-1 9%, top-5 36%
    /// (`docs/design/data-shapes.md` §4). Inputs are generated outside the timed region, so what is
    /// measured is one event's build -- including, for the cached arms, the key-sequence hash and
    /// whatever the cache does about a miss. `tests/attr_arms.rs` reports the miss *rates* these
    /// timings go with; a number here without one beside it means nothing.
    mod gateway {
        use super::*;

        /// How many events the synthesized stream covers. Enough that every one of the 196 sets
        /// appears and the tail's eviction behaviour is exercised.
        const EVENTS: usize = 2000;

        fn stream() -> Vec<Vec<(Symbol, Value)>> {
            let gateway = shapes::Gateway::new(EVENTS);
            gateway.stream.iter().map(|&set| gateway.event_scratch(set, Mix::Mostly)).collect()
        }

        #[divan::bench]
        fn bulk_sort(bencher: Bencher) {
            let stream = stream();
            let mut next = 0usize;
            bencher
                .with_inputs(|| {
                    let scratch = stream[next % stream.len()].clone();
                    next += 1;
                    scratch
                })
                .bench_local_values(|s| consume(shapes::bulk_build(&s)));
        }

        /// 64 entries against 196 shapes -- the bounded cache, with eviction.
        #[divan::bench]
        fn keyset_cache_64(bencher: Bencher) {
            let stream = stream();
            let mut cache = KeySetCache::new(64);
            let mut next = 0usize;
            bencher
                .with_inputs(|| {
                    let scratch = stream[next % stream.len()].clone();
                    next += 1;
                    scratch
                })
                .bench_local_values(|s| consume(cache.build(s)));
        }

        /// The same stream with room for every shape -- the ceiling a bounded cache is measured
        /// against.
        #[divan::bench]
        fn keyset_cache_unbounded(bencher: Bencher) {
            let stream = stream();
            let mut cache = KeySetCache::new(shapes::GATEWAY_SETS * 2);
            let mut next = 0usize;
            bencher
                .with_inputs(|| {
                    let scratch = stream[next % stream.len()].clone();
                    next += 1;
                    scratch
                })
                .bench_local_values(|s| consume(cache.build(s)));
        }
    }
}

/// **Arm K's pre-registered kill criterion**, on its own so it cannot be misread off a table:
/// build + clone of the 12-attribute log, arm K against arm P's bulk build. K must win by **≥10%**
/// or it is dropped (`docs/plans/event-sizing.md`'s arm K).
///
/// Both arms take the same pre-built scratch, build a map from it, clone that map once, and drop
/// both inside the timed region. "Build + clone" is one number because that is how the criterion is
/// written: the representation is supposed to pay for its build cost with a cheaper fan-out.
mod kill_criterion {
    use super::*;

    fn scratch() -> Vec<(Symbol, Value)> {
        shapes::scratch("kill", LOG_WIDTH, Mix::Mostly)
    }

    #[divan::bench]
    fn bulk_build_and_clone(bencher: Bencher) {
        let scratch = scratch();
        bencher.with_inputs(|| scratch.clone()).bench_local_values(|s| {
            let built = shapes::bulk_build(&s);
            let cloned = built.clone();
            black_box(&built);
            black_box(&cloned);
        });
    }

    #[divan::bench]
    fn sorted_insert_and_clone(bencher: Bencher) {
        let scratch = scratch();
        bencher.with_inputs(|| scratch.clone()).bench_local_values(|s| {
            let built = shapes::attr_map(&s);
            let cloned = built.clone();
            black_box(&built);
            black_box(&cloned);
        });
    }

    #[divan::bench]
    fn keyset_build_and_clone(bencher: Bencher) {
        let scratch = scratch();
        let mut cache = KeySetCache::new(64);
        drop(cache.build(scratch.clone()));
        bencher.with_inputs(|| scratch.clone()).bench_local_values(|s| {
            let built = cache.build(s);
            let cloned = built.clone();
            black_box(&built);
            black_box(&cloned);
        });
    }
}
