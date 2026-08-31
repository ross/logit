//! [`CountingAlloc`]: a `GlobalAlloc` wrapper that counts what the thread it runs on allocates.
//!
//! This exists so "how many allocations does decoding one nginx access-log line cost?" can be an
//! ordinary `#[test]` with an exact answer, rather than a number someone reads off a profiler once
//! and never checks again. See `docs/design/memory.md`.
//!
//! **Counters are thread-local, not global**, for two reasons. Correctness: a global counter would
//! fold in whatever the test harness, a tokio worker, or a background reaper happened to do while
//! [`measure`] was running, making results depend on timing. Cost: a thread-local `Cell` increment
//! is a couple of instructions with no atomics, so wrapping every allocation in the process stays
//! cheap enough that the benches measuring *time* aren't distorted by it.
//!
//! The thread-locals are declared with `const` initializers and hold `Cell<u64>`, which has no
//! destructor. Both details are load-bearing: a lazily-initialized or destructor-carrying
//! thread-local allocates on first access, and allocating from inside the allocator is an infinite
//! recursion. `try_with` covers the remaining case -- an allocation arriving during thread
//! teardown, after the local is already gone -- by dropping the count rather than panicking.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
    static REALLOCS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
    /// Signed: [`measure`] zeroes this at the start of the region, so freeing something allocated
    /// *before* the region legitimately drives it negative.
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

/// What one measured region allocated. Counts are per-thread and cover only the region
/// [`measure`] wrapped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Fresh allocations (`alloc` + `alloc_zeroed`). This is the headline number: it's what
    /// allocator pressure actually scales with.
    pub allocs: u64,
    /// Reallocations, counted separately rather than folded into `allocs` because they mean
    /// something different -- a `Vec`/`String` that was grown, i.e. a missing `with_capacity`,
    /// not a new object.
    pub reallocs: u64,
    /// Total bytes requested across every `alloc` and every `realloc` *growth*. Not a memory
    /// footprint: it counts a buffer that was allocated and freed inside the region too.
    pub bytes: u64,
    /// The high-water mark of bytes live at once, relative to the region's start. This is the
    /// footprint number -- what the region needed resident simultaneously.
    pub peak_live_bytes: u64,
}

/// Runs `f` with the thread's allocation counters zeroed, and reports what it allocated.
///
/// The value `f` returns is handed back rather than dropped inside the measured region, so
/// whatever it owns still counts as allocated -- which is the intent: measuring "decode this
/// datagram" should include the events the decode produced, not net them out against themselves.
///
/// **Warm up before measuring.** Plenty of things in this codebase allocate exactly once, on
/// first use -- the `OnceLock` interner (`logit_core::interner`), a `HashMap`'s first table, a
/// `thread_local`'s backing store. Measuring a cold call attributes all of that to the first
/// iteration and reports a number that never reproduces. Every test in this crate calls the thing
/// it's measuring at least once before the `measure` that counts.
pub fn measure<T>(f: impl FnOnce() -> T) -> (T, Stats) {
    ALLOCS.with(|c| c.set(0));
    REALLOCS.with(|c| c.set(0));
    BYTES.with(|c| c.set(0));
    LIVE.with(|c| c.set(0));
    PEAK.with(|c| c.set(0));

    let value = f();

    let stats = Stats {
        allocs: ALLOCS.with(Cell::get),
        reallocs: REALLOCS.with(Cell::get),
        bytes: BYTES.with(Cell::get),
        peak_live_bytes: PEAK.with(Cell::get).max(0) as u64,
    };
    (value, stats)
}

fn record_alloc(size: usize) {
    let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
    let _ = BYTES.try_with(|c| c.set(c.get() + size as u64));
    bump_live(size as i64);
}

fn record_dealloc(size: usize) {
    bump_live(-(size as i64));
}

fn record_realloc(old_size: usize, new_size: usize) {
    let _ = REALLOCS.try_with(|c| c.set(c.get() + 1));
    if new_size > old_size {
        let _ = BYTES.try_with(|c| c.set(c.get() + (new_size - old_size) as u64));
    }
    bump_live(new_size as i64 - old_size as i64);
}

fn bump_live(delta: i64) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + delta;
        live.set(now);
        let _ = PEAK.try_with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

/// Wraps another allocator, counting every request that passes through it.
///
/// Generic over the inner allocator so a benchmark can measure against whatever allocator
/// production actually uses (`docs/adr/0015-jemalloc-global-allocator.md`) rather than always
/// against `System` -- allocation *counts* are allocator-independent, but the time those counts
/// cost is not.
pub struct CountingAlloc<A = System> {
    inner: A,
}

impl<A> CountingAlloc<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

impl Default for CountingAlloc<System> {
    fn default() -> Self {
        Self::new(System)
    }
}

// SAFETY: every method forwards to `inner`, which upholds `GlobalAlloc`'s contract; the counting
// around each call touches only thread-local `Cell`s and never allocates (see the module comment
// on why the `const`-initialized, destructor-free declaration matters).
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_alloc(layout.size());
        self.inner.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record_dealloc(layout.size());
        self.inner.dealloc(ptr, layout)
    }

    // Overridden rather than left to the default (which would route through `Self::alloc` and be
    // counted correctly anyway) so the inner allocator's zeroing fast path isn't lost.
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_alloc(layout.size());
        self.inner.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record_realloc(layout.size(), new_size);
        self.inner.realloc(ptr, layout, new_size)
    }
}
