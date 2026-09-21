//! The ratio [ADR `minimize-allocations-over-event-size`](../../../docs/adr/minimize-allocations-over-event-size.md)
//! asserts and nobody here has ever measured: what one jemalloc alloc/free pair costs, against
//! what moving, scanning, and cloning the extra bytes an inline `AttrMap` slot buys costs.
//! `docs/plans/event-sizing.md`'s **W2**.
//!
//! That ADR's premise is "an allocation is commonly tens of nanoseconds; a few hundred bytes of
//! copy is single-digit nanoseconds, often absorbed by cache effects already happening." Both
//! halves are plausible and neither has a number behind it in this repo. Every bench below exists
//! to put one there, so W3's arms and W5's ADR argue from measurement.
//!
//! **Allocator: real jemalloc, no counting wrapper.** This file installs
//! `tikv_jemallocator::Jemalloc` as its `#[global_allocator]` -- the same allocator
//! `logit-cli` ships with ([ADR `jemalloc-global-allocator`](../../../docs/adr/jemalloc-global-allocator.md)),
//! and deliberately *not* `benches/pipeline.rs`'s `divan::AllocProfiler`, which wraps the
//! **system** allocator and counts every request from inside the timed region. Those are the two
//! things this file cannot tolerate: measuring glibc's malloc when production runs jemalloc, and
//! adding a counter increment to the very operation whose cost is the question. The price is that
//! these benches report no allocation column at all -- allocation *counts* for the same shapes
//! live in `tests/allocations.rs`, which is where they belong (`docs/design/memory.md` §7's
//! "allocation counts are allocator-independent, timings are not").
//!
//! **What each group isolates, and what it can't tell you:**
//!
//! - [`alloc_free`] -- a raw `alloc`/`dealloc` pair at the sizes `AttrMap`'s growth ladder
//!   actually asks for (`memory.md` §1), same-thread (jemalloc tcache fast path) and
//!   **cross-thread** (allocated on one thread, freed on another, in steady state), because
//!   cross-thread is `logit`'s real lifecycle: a spilled buffer is built on a listener or
//!   transform task and dropped on a sink task. It does *not* include the first-touch page fault
//!   on fresh memory, nor any of the work that made the allocation necessary; it is the allocator
//!   call and nothing else. Nor is it contended -- one producer, one consumer, on an otherwise
//!   idle machine, which is the friendliest case jemalloc ever sees.
//! - [`realloc_chain`] -- what the growth ladder costs against one exactly-sized allocation. This
//!   is precisely what arm **P** (`AttrMap::with_capacity`) saves and nothing more; it says
//!   nothing about how often a real workload takes the second step.
//! - [`build_shape`] -- today's O(k²)-bytes sorted `insert` into a real `AttrMap` against an
//!   append-then-sort build through a local `Vec<(Symbol, Value)>` mirror, at the widths
//!   `docs/design/data-shapes.md` measured. The values are `Value::I64`, so this is the *move* and
//!   *compare* cost of the build with no per-value work folded in -- a real parser's
//!   `Value::Str(Bytes)` adds a refcount bump per moved entry that this understates.
//! - [`move_value`] -- one `Event`-sized move, at every candidate `size_of::<Event>()`. A padded
//!   struct, never a real `Event`: the question is bytes moved, and a real `Event` would fold in
//!   `Drop` glue, `Bytes` refcounts, and whatever its variants happen to hold.
//! - [`scan`] -- the cache-density measurement, the one place `size_of::<Event>()` is paid on
//!   every event whether or not it uses the space: 1000 elements per batch (the
//!   `receive.batch_max_events` default), 8 batches rotated so the working set is 4-13 MB and
//!   cannot sit in L2. It reads the first 64 bytes of each element -- one cache line, the shape of
//!   "look up one attribute per event" -- so it measures *stride*, not attribute lookup. The
//!   `drop`/`clone` benches beside it cover the 0.5-1.6 MB batch buffer itself, a jemalloc **large**
//!   allocation with a different cost structure from the small ones above.
//! - [`attr_clone`] -- cloning a real `AttrMap` inline (8 entries) versus spilled (9 and 12), the
//!   fan-out cost `data-shapes.md` §6 says the sizing arms should actually be judged on.
//!
//! **None of these numbers belong in a document.** This workstation has heterogeneous cores and
//! unpinned runs are bimodal by about 2x (`docs/design/memory.md`'s preamble,
//! `docs/design/performance.md` §0); recorded figures come from the perf VM, in W4.
//! `docs/plans/event-sizing.md`'s W2 section names the commands.

use divan::counter::ItemsCount;
use divan::{black_box, Bencher};
use logit_core::{AttrMap, Symbol, Value};
use std::alloc::{alloc, dealloc, realloc, Layout};

/// The real allocator, unwrapped -- see this file's module doc for why neither
/// `divan::AllocProfiler` nor `logit_bench::alloc::CountingAlloc` is used here.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    divan::main();
}

/// The byte sizes an `AttrMap` actually asks jemalloc for, plus the two neighbouring
/// `Resource`/`Event`-adjacent ones:
///
/// | Bytes | Where it comes from |
/// |---:|---|
/// | 432 | 9 entries exactly -- what arm **P** would request where today's spill asks for 768 |
/// | 576 | 12 entries exactly (the commonest measured log width) |
/// | 768 | today's spill: smallvec doubles the inline capacity, 16 × 48 (verified in W1) |
/// | 1440 | 30 entries exactly (the access-log shape) |
/// | 1536 | the first growth step, 32 × 48 |
/// | 3072 | the second, 64 × 48 |
///
/// All six land in a different jemalloc size class (448, 640, 768, 1536, 1536, 3072 of the
/// `lg_quantum`=4 small classes), which is itself worth seeing in the numbers.
const BLOCK_SIZES: [usize; 6] = [432, 576, 768, 1440, 1536, 3072];

/// Candidate `size_of::<Event>()` values: `48·N + 480` for N ∈ {0, 4, 8, 12, 16, 24} -- arm **S**'s
/// static sweep, with 864 (today) in the middle.
const EVENT_SIZES: [usize; 6] = [496, 672, 864, 1056, 1248, 1632];

/// How many blocks one chunked alloc/free iteration handles. Big enough that the chunk `Vec` and
/// the channel hand-off amortize away, small enough that the live set stays inside the tcache.
const CHUNK: usize = 64;

/// One heap block, owned. Deallocated by `Drop`, wherever the drop happens -- which is the whole
/// point of [`alloc_free::pair_cross_thread`]: a `Vec<Block>` sent down a channel frees every
/// block on the *receiving* thread, exactly as a spilled `AttrMap` built by a transform is freed
/// by a sink.
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
    /// actually faulted in -- reallocating never-touched memory measures a copy of fresh zero
    /// pages, which is not what growing a half-built `AttrMap` does.
    fn touch(self) -> Self {
        // SAFETY: `self.ptr` is a live allocation of exactly `self.layout.size()` bytes.
        unsafe { std::ptr::write_bytes(self.ptr, 0xA5, self.layout.size()) };
        self
    }

    /// One growth step of the ladder, as `realloc` -- the operation smallvec performs when a
    /// spilled `AttrMap` doubles.
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

/// A value the size of an `Event`, and nothing else: no `Drop` glue, no refcounts, no niches --
/// just `N` bytes at `Event`'s own 8-byte alignment. Exactly what "does +384 bytes per event cost
/// more than the allocation it saves?" needs and no more.
#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Padded<const N: usize>([u8; N]);

impl<const N: usize> Padded<N> {
    fn new(seed: u8) -> Self {
        Self([seed; N])
    }

    /// Reads the first 64 bytes -- one cache line, the amount a per-event attribute probe touches
    /// before the stride decides whether the next element is already resident.
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

/// jemalloc's small-allocation fast path, at the six sizes `AttrMap` asks for, freed where it was
/// allocated and freed somewhere else.
mod alloc_free {
    use super::*;

    /// `alloc` immediately followed by `dealloc`, one block live at a time: the friendliest
    /// possible case, and the one the ADR's "tens of nanoseconds" claim is really about.
    #[divan::bench(args = BLOCK_SIZES)]
    fn immediate(bencher: Bencher, size: usize) {
        bencher.bench_local(|| drop(Block::new(black_box(size))));
    }

    /// [`immediate`]'s chunked shape -- 64 blocks allocated into a `Vec`, then all 64 freed --
    /// so it is directly comparable with [`pair_cross_thread`], which can only be chunked. The
    /// per-iteration `Vec::with_capacity(64)` is one extra 1 KiB allocation per 64 blocks, present
    /// identically in both.
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
    /// state. A bounded channel (capacity 2) is what makes it steady -- the consumer keeps up, so
    /// the producer is always asking jemalloc for memory whose previous incarnation went into
    /// *another* thread's cache, which is the case `docs/adr/jemalloc-global-allocator.md` chose
    /// jemalloc for and the case glibc's per-thread arenas handle badly.
    ///
    /// The channel send is inside the timed region and the `ItemsCount` divides it by 64. What
    /// this cannot separate is the send's own cost from the allocator's; compare against
    /// [`pair_same_thread`], which pays the identical `Vec` and loop and no send.
    #[divan::bench(args = BLOCK_SIZES)]
    fn pair_cross_thread(bencher: Bencher, size: usize) {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<Block>>(2);
        let worker = std::thread::spawn(move || {
            // Dropping the chunk here is the measurement's entire point.
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

    /// The same comparison one rung down, for the far commoner 9-to-16-entry case: today's
    /// 768-byte spill against the 432 bytes nine entries actually need.
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

    /// Interned keys in a fixed, deliberately unsorted order.
    ///
    /// Insertion order matters and is not incidental: `AttrMap::insert_sym` is a
    /// `binary_search` plus a positional `SmallVec::insert`, so inserting in ascending symbol
    /// order is the best case (every insert lands at the end, nothing moves) and a shuffled order
    /// is the realistic one. The native decoder is the worst offender here -- it rebuilds the map
    /// in the *writer's* symbol order, which dictionary remapping has already made unsorted for
    /// the reader (`crates/logit-proto/src/native/value.rs`, and `docs/plans/event-sizing.md`).
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

    /// Today's path: `k` sorted `insert_sym` calls into a fresh `AttrMap`. Past 8 entries this
    /// also pays the spill and, past 16, the growth ladder [`realloc_chain`] isolates -- the two
    /// costs are deliberately *not* separated here, because no caller can separate them today
    /// either.
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

    /// Arm **P**'s build, mirrored locally (no production change lands in this PR): reserve
    /// exactly, push in arrival order, sort once. `sort_unstable_by_key` on `Symbol` is the same
    /// comparison `insert_sym`'s binary search makes, done O(k log k) times over entries that
    /// never move more than once.
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

    /// [`append_then_sort`] without the `with_capacity` -- the growth ladder a `Vec` climbs from
    /// nothing (4, 8, 16, 32 entries) against the one exact allocation above. This is the
    /// bench-only way to see the reserve on its own, separate from the sort.
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

    /// 64 individual element moves -- `route_batch`'s partition pass, `Vec::retain_mut`'s
    /// compaction, and every other place an `Event` is moved one at a time. Deliberately not a
    /// bulk `copy_from_slice`: a single large `memmove` is faster per byte than 64 separate ones,
    /// and nothing in the pipeline moves events that way.
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

    /// The same move through safe `Vec` pushes and pops, which is what a batch assembled and then
    /// drained actually executes. Slower than [`ptr_move`] by whatever the length bookkeeping
    /// costs; the gap between them is that bookkeeping, not the copy.
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
    /// Enough batches that the working set (4 MB at N=496, 13 MB at N=1632) is far past any L2 and
    /// the scan is really paying for the stride rather than re-reading a resident buffer. Rotated
    /// per iteration so no single batch stays hot.
    const BATCHES: usize = 8;

    fn batches<const N: usize>() -> Vec<Vec<Padded<N>>> {
        (0..BATCHES).map(|b| (0..BATCH).map(|i| Padded::new((b + i) as u8)).collect()).collect()
    }

    /// One pass over 1000 elements, reading the first cache line of each. `ItemsCount` makes the
    /// headline number ns/element, which is what a per-event cost is denominated in.
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

    /// Freeing the batch buffer itself: 0.5 MB at N=496, 1.6 MB at N=1632, which is a jemalloc
    /// **large** allocation (past the 16 KiB small/large boundary) and so does not come from a
    /// thread cache at all -- it is an extent operation, possibly a `madvise`. Arm **R**'s target.
    #[divan::bench(consts = EVENT_SIZES)]
    fn drop_batch<const N: usize>(bencher: Bencher) {
        bencher
            .counter(ItemsCount::new(BATCH))
            .with_inputs(|| (0..BATCH).map(|i| Padded::<N>::new(i as u8)).collect::<Vec<_>>())
            .bench_local_values(drop);
    }

    /// Cloning it: one large allocation plus a `BATCH * N`-byte copy -- the copy-on-write
    /// `EventBatch::clone` a two-mutating-consumer fan-out pays (`docs/design/memory.md` §3),
    /// with every per-`Event` cost stripped out so only the stride is left.
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
    /// own shape); 12 is the commonest measured JSON-log width. The step from 8 to 9 is the one
    /// that costs an allocation -- everything past it is the same one allocation, wider.
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
