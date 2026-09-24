//! [`CountingAlloc`]: a `GlobalAlloc` wrapper that counts what the thread it runs on allocates.
//!
//! It lets "how many allocations does decoding one nginx access-log line cost?" be an ordinary
//! `#[test]` with an exact answer (`docs/design/memory.md` §7, "Instrumentation").
//!
//! **Counters are thread-local, not global.** A global counter would fold in whatever the test
//! harness, a tokio worker, or a background reaper did while [`measure`] ran, so results would
//! depend on timing. A thread-local `Cell` increment is also a couple of instructions with no
//! atomics, cheap enough not to distort the benches that measure *time*.
//!
//! The thread-locals have `const` initializers and hold destructor-free `Cell`s. Both are required:
//! a lazily-initialized or destructor-carrying thread-local allocates on first access, and
//! allocating from inside the allocator recurses forever. `try_with` covers an allocation during
//! thread teardown, after the local is gone, by dropping the count rather than panicking.

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
    /// Fresh allocations (`alloc` + `alloc_zeroed`). The headline number: allocator pressure
    /// scales with it.
    pub allocs: u64,
    /// Reallocations, kept out of `allocs` because they mean a grown `Vec`/`String` (a missing
    /// `with_capacity`), not a new object.
    pub reallocs: u64,
    /// Total bytes requested across every `alloc` and every `realloc` *growth*. Not a memory
    /// footprint: it counts a buffer that was allocated and freed inside the region too.
    pub bytes: u64,
    /// The high-water mark of bytes live at once, relative to the region's start: the footprint
    /// number.
    pub peak_live_bytes: u64,
}

/// Runs `f` with the thread's allocation counters zeroed, and reports what it allocated.
///
/// `f`'s return value is handed back, not dropped inside the region, so whatever it owns still
/// counts: "decode this datagram" includes the events the decode produced.
///
/// **Warm up before measuring.** Plenty of things allocate once, on first use: the `OnceLock`
/// interner (`logit_core::interner`), a `HashMap`'s first table, a `thread_local`'s backing store.
/// A cold measurement charges all of that to the first call and never reproduces. Every test in
/// this crate calls the thing it measures at least once before the `measure` that counts.
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
/// Generic over the inner allocator so a benchmark can run on production's allocator
/// (`docs/adr/jemalloc-global-allocator.md`): allocation *counts* don't depend on the allocator,
/// but their cost in time does.
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
