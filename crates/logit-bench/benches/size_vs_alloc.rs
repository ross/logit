//! What one jemalloc alloc/free pair costs, against what moving, scanning, and cloning the bytes an
//! inline `AttrMap` slot buys costs: the ratio
//! [ADR `minimize-allocations-over-event-size`](../../../docs/adr/minimize-allocations-over-event-size.md)
//! assumes. ADR `event-sizing-and-allocation-strategy` records what these benches found;
//! `docs/design/performance.md` §8(e) has the numbers.
//!
//! **Allocator: real jemalloc, no counting wrapper.** This file installs
//! `tikv_jemallocator::Jemalloc` as its `#[global_allocator]`, the allocator `logit-cli` ships
//! ([ADR `jemalloc-global-allocator`](../../../docs/adr/jemalloc-global-allocator.md)), not
//! `benches/pipeline.rs`'s `divan::AllocProfiler`. `AllocProfiler` wraps the **system** allocator
//! and adds a counter increment inside the timed region, and the allocator call is what is being
//! timed. So these benches report no allocation column; counts for the same shapes are in
//! `tests/allocations.rs` (`docs/design/memory.md` §7: counts are allocator-independent, timings
//! are not).
//!
//! **What each group isolates, and what it can't tell you:**
//!
//! - [`alloc_free`]: a raw `alloc`/`dealloc` pair at the sizes `AttrMap`'s growth ladder asks for
//!   (`memory.md` §1), same-thread (jemalloc's tcache fast path) and **cross-thread** (allocated
//!   on one thread, freed on another, in steady state). Cross-thread is `logit`'s real lifecycle: a
//!   spilled buffer is built on a listener or transform task and dropped on a sink task. It
//!   excludes the first-touch page fault and the work that made the allocation necessary, and it
//!   is uncontended: one producer, one consumer, on an otherwise idle machine.
//! - [`realloc_chain`]: the growth ladder against one exactly-sized allocation. This is what arm
//!   **P** (pre-sizing) saves and nothing more; it says nothing about how often a real workload
//!   takes the second step.
//! - [`build_shape`]: the sorted `insert` into a real `AttrMap` (O(k²) bytes moved) against an
//!   append-then-sort build through a local `Vec<(Symbol, Value)>`, at the widths
//!   `docs/design/data-shapes.md` measured. Values are `Value::I64`, so this is the move and
//!   compare cost alone; a real parser's `Value::Str(Bytes)` adds a refcount bump per moved entry.
//! - [`move_value`]: one `Event`-sized move at every candidate `size_of::<Event>()`. A padded
//!   struct, not a real `Event`, so `Drop` glue and `Bytes` refcounts stay out of a bytes-moved
//!   measurement.
//! - [`scan`]: cache density, the one place `size_of::<Event>()` is paid on every event whether or
//!   not it uses the space. 1000 elements per batch (the `receive.batch_max_events` default), 8
//!   batches rotated so the 4-13 MB working set can't sit in L2. It reads the first 64 bytes of
//!   each element (one cache line, the shape of "look up one attribute per event"), so it measures
//!   *stride*, not attribute lookup. The `drop`/`clone` benches beside it cover the 0.5-1.6 MB
//!   batch buffer itself, a jemalloc **large** allocation with a different cost structure from the
//!   small ones above.
//! - [`attr_clone`]: cloning a real `AttrMap` inline (8 entries) against spilled (9 and 12), the
//!   fan-out cost `data-shapes.md` §6 says the sizing arms should be judged on.
//!
//! **Arm letters** (P, S, R) are `docs/plans/event-sizing.md`'s "Bake-off arms".
//!
//! **divan's time column is per iteration.** A bench with an `ItemsCount` counter still reports
//! the whole iteration's time (64 blocks, or 1000 elements); divide by the batch size by hand.
//! `docs/design/performance.md` §8(e) lists each bench's divisor.
//!
//! **Run pinned** (`taskset -c 2`) on the perf VM. Unpinned runs on heterogeneous cores (Zen 5 and
//! 5c) are bimodal by about 2x (`docs/design/performance.md` §0). Record numbers in
//! `performance.md` §8 from VM runs only.

use divan::counter::ItemsCount;
use divan::{black_box, Bencher};
use logit_core::{AttrMap, Symbol, Value};
use std::alloc::{alloc, dealloc, realloc, Layout};

/// The shipped allocator, unwrapped. The module doc says why neither `divan::AllocProfiler` nor
/// `logit_bench::alloc::CountingAlloc` is used here.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    divan::main();
}

/// The byte sizes an `AttrMap` asks jemalloc for:
///
/// | Bytes | Where it comes from |
/// |---:|---|
/// | 432 | 9 entries exactly: arm **P**'s request where the spill asks for 768 |
/// | 576 | 12 entries exactly (the commonest measured log width) |
/// | 768 | the spill: smallvec doubles the inline capacity, 16 × 48 |
/// | 1440 | 30 entries exactly (the access-log shape) |
/// | 1536 | the first growth step, 32 × 48 |
/// | 3072 | the second, 64 × 48 |
///
/// They round up to jemalloc's 448, 640, 768, 1536, 1536, and 3072-byte small size classes
/// (`lg_quantum`=4), so 1440 and 1536 share a class.
const BLOCK_SIZES: [usize; 6] = [432, 576, 768, 1440, 1536, 3072];

/// Candidate `size_of::<Event>()` values for arm **S**'s static sweep, N ∈ {0, 4, 8, 12, 16, 24}.
/// N ≥ 4 is `48·N + 480`, 864 (N=8) is the shipped size, and N=0 is 496, not 480.
const EVENT_SIZES: [usize; 6] = [496, 672, 864, 1056, 1248, 1632];

/// How many blocks one chunked alloc/free iteration handles. Big enough that the chunk `Vec` and
/// the channel hand-off amortize away, small enough that the live set stays inside the tcache.
const CHUNK: usize = 64;

/// One heap block, owned, and freed by `Drop` on whichever thread drops it.
/// [`alloc_free::pair_cross_thread`] depends on that: a `Vec<Block>` sent down a channel frees
/// every block on the *receiving* thread, as a sink frees a spilled `AttrMap` a transform built.
struct Block {
    ptr: *mut u8,
    layout: Layout,
}

// SAFETY: `Block` owns its allocation exclusively -- there is no interior sharing, and jemalloc
// (like every `GlobalAlloc`) permits a block to be freed on a thread other than the one that
// allocated it. Sending one is therefore an ordinary ownership transfer; the raw pointer is what
// makes the auto-impl not apply, not any thread affinity.
unsafe impl Send for Block {}

impl Block {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 8).expect("a valid layout");
        // SAFETY: `size` is never zero (every `BLOCK_SIZES` entry is positive), which is
        // `alloc`'s only precondition beyond a valid layout.
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "allocation of {size} bytes failed");
        Self { ptr, layout }
    }

    /// Writes every byte, so a later `realloc`'s internal copy moves pages this process has
    /// faulted in. Reallocating never-touched memory measures a copy of fresh zero pages, which is
    /// not what growing a half-built `AttrMap` does.
    fn touch(self) -> Self {
        // SAFETY: `self.ptr` is a live allocation of exactly `self.layout.size()` bytes.
        unsafe { std::ptr::write_bytes(self.ptr, 0xA5, self.layout.size()) };
        self
    }

    /// One growth step of the ladder, as `realloc`: what smallvec does when a spilled `AttrMap`
    /// doubles.
    fn grow(mut self, new_size: usize) -> Self {
        // SAFETY: `self.ptr` came from `alloc` with `self.layout`, and `new_size` is non-zero and
        // rounds to no more than `isize::MAX`.
        let ptr = unsafe { realloc(self.ptr, self.layout, new_size) };
        assert!(!ptr.is_null(), "reallocation to {new_size} bytes failed");
        self.ptr = ptr;
        self.layout = Layout::from_size_align(new_size, self.layout.align()).expect("valid");
        self
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`layout` are the pair this block was allocated (or last reallocated) with,
        // and `Drop` runs exactly once.
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

/// `N` bytes at `Event`'s 8-byte alignment, with no `Drop` glue, refcounts, or niches: a stand-in
/// that measures bytes moved and nothing else.
#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Padded<const N: usize>([u8; N]);

impl<const N: usize> Padded<N> {
    fn new(seed: u8) -> Self {
        Self([seed; N])
    }

    /// Reads the first 64 bytes: one cache line, what a per-event attribute probe touches before
    /// the stride decides whether the next element is already resident.
    #[inline]
    fn head_sum(&self) -> u64 {
        let words = self.0.as_ptr().cast::<u64>();
        let mut acc = 0u64;
        for i in 0..8 {
            // SAFETY: `N >= 64` for every size benched here, the array is 8-byte aligned by
            // `repr(align(8))`, and `i < 8` keeps the read inside it.
            acc = acc.wrapping_add(unsafe { words.add(i).read() });
        }
        acc
    }
}

/// jemalloc's small-allocation path at the six sizes `AttrMap` asks for, freed on the allocating
/// thread and on another.
mod alloc_free {
    use super::*;

    /// `alloc` immediately followed by `dealloc`, one block live at a time: jemalloc's best case,
    /// and the one the ADR's "tens of nanoseconds" premise describes.
    #[divan::bench(args = BLOCK_SIZES)]
    fn immediate(bencher: Bencher, size: usize) {
        bencher.bench_local(|| drop(Block::new(black_box(size))));
    }

    /// [`immediate`] chunked: 64 blocks allocated into a `Vec`, then all 64 freed, so it compares
    /// directly with [`pair_cross_thread`], which can only be chunked. The per-iteration
    /// `Vec::with_capacity(64)` is one extra 1 KiB allocation per 64 blocks, paid identically by
    /// both.
    #[divan::bench(args = BLOCK_SIZES)]
    fn pair_same_thread(bencher: Bencher, size: usize) {
        bencher.counter(ItemsCount::new(CHUNK)).bench_local(|| {
            let mut chunk = Vec::with_capacity(CHUNK);
            for _ in 0..CHUNK {
                chunk.push(Block::new(black_box(size)));
            }
            chunk
        });
    }

    /// The real lifecycle: every block is allocated here and freed on another thread, in steady
    /// state. The bounded channel (capacity 2) keeps it steady: the consumer keeps up, so the
    /// producer always asks jemalloc for memory whose previous incarnation went into *another*
    /// thread's cache. That is the case `docs/adr/jemalloc-global-allocator.md` chose jemalloc for,
    /// and the one glibc's per-thread arenas handle badly.
    ///
    /// The channel send is inside the timed region, once per 64 blocks, and this bench can't
    /// separate it from the allocator's cost; [`pair_same_thread`] pays the same `Vec` and loop
    /// with no send.
    #[divan::bench(args = BLOCK_SIZES)]
    fn pair_cross_thread(bencher: Bencher, size: usize) {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<Block>>(2);
        let worker = std::thread::spawn(move || {
            // The cross-thread free: what this bench measures.
            for chunk in rx {
                drop(chunk);
            }
        });

        bencher.counter(ItemsCount::new(CHUNK)).bench_local(|| {
            let mut chunk = Vec::with_capacity(CHUNK);
            for _ in 0..CHUNK {
                chunk.push(Block::new(black_box(size)));
            }
            tx.send(chunk).expect("the freeing thread outlives the bench");
        });

        drop(tx);
        worker.join().expect("the freeing thread should not panic");
    }
}

/// What smallvec's doubling ladder costs against the one exactly-sized allocation arm **P** would
/// make instead.
mod realloc_chain {
    use super::*;

    /// The first growth step alone: a spilled 16-entry buffer becoming a 32-entry one.
    #[divan::bench]
    fn step_768_to_1536(bencher: Bencher) {
        bencher
            .with_inputs(|| Block::new(768).touch())
            .bench_local_values(|block| block.grow(black_box(1536)));
    }

    /// The second: 32 entries becoming 64. A 30-field access log stops after the first; a 33-field
    /// one pays both.
    #[divan::bench]
    fn step_1536_to_3072(bencher: Bencher) {
        bencher
            .with_inputs(|| Block::new(1536).touch())
            .bench_local_values(|block| block.grow(black_box(3072)));
    }

    /// The whole ladder for a >32-entry map: spill to 768, grow to 1536, grow to 3072. Every block
    /// is touched before it grows, so the reallocs copy resident pages.
    #[divan::bench]
    fn ladder_768_1536_3072(bencher: Bencher) {
        bencher.bench_local(|| {
            let block = Block::new(black_box(768)).touch();
            let block = block.grow(black_box(1536)).touch();
            block.grow(black_box(3072)).touch()
        });
    }

    /// The control: one allocation of the size the ladder ends at, touched once. The difference
    /// between this and [`ladder_768_1536_3072`] is what a `reserve` on the decode path buys, with
    /// nothing else changed.
    #[divan::bench]
    fn exact_3072(bencher: Bencher) {
        bencher.bench_local(|| Block::new(black_box(3072)).touch());
    }

    /// The same comparison one rung down, for the commoner 9-to-16-entry case: the 768-byte spill
    /// against the 432 bytes nine entries need.
    #[divan::bench(args = [432usize, 768])]
    fn spill_width(bencher: Bencher, size: usize) {
        bencher.bench_local(|| Block::new(black_box(size)).touch());
    }
}

/// `AttrMap`'s sorted positional `insert` against append-then-sort, at the widths
/// `docs/design/data-shapes.md` measured.
mod build_shape {
    use super::*;

    /// 9 (one past inline), 12 (the commonest JSON-log width), 17 (the span p90), 30 (the
    /// access-log shape). `docs/design/data-shapes.md` §2-§5.
    const WIDTHS: [usize; 4] = [9, 12, 17, 30];

    /// Interned keys in a fixed, unsorted order.
    ///
    /// `AttrMap::insert_sym` is a `binary_search` plus a positional `SmallVec::insert`, so
    /// ascending symbol order is its best case (every insert lands at the end, nothing moves) and
    /// this shuffle is a worse one. Most real sources deliver ascending order, because keys arrive
    /// in the interner's first-seen order (ADR `event-sizing-and-allocation-strategy`,
    /// "Consequences"), so this group overstates the sorted build's cost for them. The native
    /// decoder is the exception: `read_attr_map_at` (`crates/logit-proto/src/native/value.rs`)
    /// inserts in the *writer's* symbol order, which dictionary remapping can leave unsorted for
    /// the reader.
    ///
    /// The keys are interned once, outside every timed region, so no bench below measures the
    /// interner.
    fn keys(width: usize) -> Vec<Symbol> {
        let symbols: Vec<Symbol> =
            (0..width).map(|i| logit_core::interner::intern(&format!("w2.key.{i:02}"))).collect();
        // A fixed, reproducible shuffle: step by a stride coprime with the width, which visits
        // every index exactly once without pulling in a random-number generator.
        let stride = if width.is_multiple_of(7) { 5 } else { 7 };
        (0..width).map(|i| symbols[(i * stride) % width]).collect()
    }

    /// The shipped path: `k` sorted `insert_sym` calls into a fresh `AttrMap`. Past 8 entries this
    /// also pays the spill and, past 16, the growth ladder [`realloc_chain`] isolates. The costs
    /// aren't separated here because no caller can separate them.
    #[divan::bench(args = WIDTHS)]
    fn sorted_insert(bencher: Bencher, width: usize) {
        let keys = keys(width);
        bencher.bench_local(|| {
            let mut map = AttrMap::new();
            for (i, key) in black_box(&keys).iter().enumerate() {
                map.insert_sym(*key, Value::I64(i as i64));
            }
            map
        });
    }

    /// Arm **P**'s build, mirrored locally: reserve exactly, push in arrival order, sort once.
    /// `sort_unstable_by_key` on `Symbol` is the comparison `insert_sym`'s binary search makes,
    /// done O(k log k) times over entries that move at most once.
    ///
    /// Not equivalent to `AttrMap` on a repeated key (an unstable sort, no dedup);
    /// `logit_bench::bakeoff::attr_arms::shapes::bulk_build` is the equivalent build.
    #[divan::bench(args = WIDTHS)]
    fn append_then_sort(bencher: Bencher, width: usize) {
        let keys = keys(width);
        bencher.bench_local(|| {
            let keys = black_box(&keys);
            let mut entries: Vec<(Symbol, Value)> = Vec::with_capacity(keys.len());
            for (i, key) in keys.iter().enumerate() {
                entries.push((*key, Value::I64(i as i64)));
            }
            entries.sort_unstable_by_key(|(k, _)| *k);
            entries
        });
    }

    /// [`append_then_sort`] without the `with_capacity`: the growth ladder a `Vec` climbs from
    /// nothing (4, 8, 16, 32 entries) against the one exact allocation above, which separates the
    /// reserve's effect from the sort's.
    #[divan::bench(args = WIDTHS)]
    fn append_then_sort_unreserved(bencher: Bencher, width: usize) {
        let keys = keys(width);
        bencher.bench_local(|| {
            let mut entries: Vec<(Symbol, Value)> = Vec::new();
            for (i, key) in black_box(&keys).iter().enumerate() {
                entries.push((*key, Value::I64(i as i64)));
            }
            entries.sort_unstable_by_key(|(k, _)| *k);
            entries
        });
    }
}

/// One `Event`-sized move, at every candidate `size_of::<Event>()`.
mod move_value {
    use super::*;

    /// 64 individual element moves: `route_batch`'s partition pass, `Vec::retain_mut`'s
    /// compaction, and every other place an `Event` moves one at a time. Not a bulk
    /// `copy_from_slice`: one large `memmove` is faster per byte than 64 separate ones, and nothing
    /// in the pipeline moves events that way.
    #[divan::bench(consts = EVENT_SIZES)]
    fn ptr_move<const N: usize>(bencher: Bencher) {
        assert_eq!(std::mem::size_of::<Padded<N>>(), N, "the padding should be exact");
        let src: Vec<Padded<N>> = (0..CHUNK).map(|i| Padded::new(i as u8)).collect();
        let mut dst: Vec<Padded<N>> = (0..CHUNK).map(|_| Padded::new(0)).collect();
        bencher.counter(ItemsCount::new(CHUNK)).bench_local(|| {
            for i in 0..CHUNK {
                let i = black_box(i);
                // SAFETY: both vectors have exactly `CHUNK` elements, `i < CHUNK`, and they are
                // distinct allocations so the regions cannot overlap.
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr().add(i), dst.as_mut_ptr().add(i), 1);
                }
            }
            black_box(&dst);
        });
    }

    /// The same move through safe `Vec` pushes and pops, which is what assembling and draining a
    /// batch executes. The gap to [`ptr_move`] is the length bookkeeping, not the copy.
    #[divan::bench(consts = EVENT_SIZES)]
    fn vec_push_pop<const N: usize>(bencher: Bencher) {
        let src: Vec<Padded<N>> = (0..CHUNK).map(|i| Padded::new(i as u8)).collect();
        let mut dst: Vec<Padded<N>> = Vec::with_capacity(CHUNK);
        bencher.counter(ItemsCount::new(CHUNK * 2)).bench_local(|| {
            for value in black_box(&src) {
                dst.push(*value);
            }
            while let Some(value) = dst.pop() {
                black_box(value);
            }
        });
    }
}

/// Cache density: the cost `size_of::<Event>()` imposes on every batch scan whether or not the
/// event uses the space.
mod scan {
    use super::*;

    /// `receive.batch_max_events`' default.
    const BATCH: usize = 1000;
    /// Enough batches that the working set (4 MB at N=496, 13 MB at N=1632) is far past any L2, so
    /// the scan pays for the stride rather than re-reading a resident buffer. Rotated per iteration
    /// so no single batch stays hot.
    const BATCHES: usize = 8;

    fn batches<const N: usize>() -> Vec<Vec<Padded<N>>> {
        (0..BATCHES).map(|b| (0..BATCH).map(|i| Padded::new((b + i) as u8)).collect()).collect()
    }

    /// One pass over 1000 elements, reading the first cache line of each. The time column is per
    /// pass; divide by 1000 for a per-event cost.
    #[divan::bench(consts = EVENT_SIZES)]
    fn touch_head<const N: usize>(bencher: Bencher) {
        let batches = batches::<N>();
        let mut which = 0usize;
        bencher.counter(ItemsCount::new(BATCH)).bench_local(|| {
            which = (which + 1) % BATCHES;
            let batch = &black_box(&batches)[which];
            let mut acc = 0u64;
            for element in batch {
                acc = acc.wrapping_add(element.head_sum());
            }
            acc
        });
    }

    /// Freeing the batch buffer itself: 0.5 MB at N=496, 1.6 MB at N=1632. That is a jemalloc
    /// **large** allocation (past the 16 KiB small/large boundary), so it doesn't come from a
    /// thread cache: it is an extent operation, possibly a `madvise`. Arm **R**'s target.
    #[divan::bench(consts = EVENT_SIZES)]
    fn drop_batch<const N: usize>(bencher: Bencher) {
        bencher
            .counter(ItemsCount::new(BATCH))
            .with_inputs(|| (0..BATCH).map(|i| Padded::<N>::new(i as u8)).collect::<Vec<_>>())
            .bench_local_values(drop);
    }

    /// Cloning it: one large allocation plus a `BATCH * N`-byte copy. This is the copy-on-write
    /// `EventBatch::clone` a two-mutating-consumer fan-out pays (`docs/design/memory.md` §3), with
    /// every per-`Event` cost stripped out so only the stride is left.
    #[divan::bench(consts = EVENT_SIZES)]
    fn clone_batch<const N: usize>(bencher: Bencher) {
        let batch: Vec<Padded<N>> = (0..BATCH).map(|i| Padded::new(i as u8)).collect();
        bencher.counter(ItemsCount::new(BATCH)).bench_local(|| black_box(&batch).clone());
    }
}

/// Cloning a real `AttrMap`, inline against spilled.
mod attr_clone {
    use super::*;

    /// 8 is the last inline width; 9 is the first spilled one (and the enriched-resource batch's
    /// own shape); 12 is the commonest measured JSON-log width. The step from 8 to 9 costs an
    /// allocation; every width past it is the same one allocation, wider.
    const WIDTHS: [usize; 3] = [8, 9, 12];

    fn map(width: usize) -> AttrMap {
        let mut map = AttrMap::new();
        for i in 0..width {
            map.insert(&format!("w2.clone.{i:02}"), Value::I64(i as i64));
        }
        map
    }

    /// `Value::I64` throughout, so this is the map's own copy and (past 8) its one allocation, with
    /// no `Bytes` refcount traffic folded in. A real 12-attribute log record's clone pays an atomic
    /// increment per string value on top of what this reports.
    #[divan::bench(args = WIDTHS)]
    fn clone(bencher: Bencher, width: usize) {
        let map = map(width);
        assert_eq!(map.len(), width, "every key should be distinct");
        bencher.bench_local(|| black_box(&map).clone());
    }
}
