//! Mutation testing over the two sketch decoders that read peer bytes: `DdSketch::from_bytes` and
//! `HyperLogLog::from_bytes`. Both run on every native `Distribution`/`Set` a `logit_in` peer or
//! the disk spool hands `logit_proto::native`, and both feed `aggregate`'s accumulators.
//!
//! For each decoder: every single-byte truncation of a valid blob, thousands of seeded bit flips,
//! counts inflated past the blob, and the peak allocation of a valid decode. A decode never
//! panics, never sizes an allocation from a declared count before bounding it, and whatever it
//! accepts survives the operations `aggregate` and the encoders run on it (merge, quantile or
//! estimate, `to_bytes`, drop).
//!
//! The harness (`CountingAlloc`, `peak_live_bytes`, `Lcg`) is a copy of
//! `crates/logit-proto/tests/robustness.rs`'s: `logit-proto` depends on `logit-core`, so this
//! crate can't reuse it.

use logit_core::{Bin, DdSketch, HyperLogLog, Mapping, SketchDecodeError, SketchStats};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::panic::AssertUnwindSafe;

// -- a tiny, self-contained peak-allocation counter -----------------------------------------

struct CountingAlloc;

thread_local! {
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as i64));
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size as i64 - layout.size() as i64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn record(delta: i64) {
    LIVE.with(|live| {
        let now = live.get() + delta;
        live.set(now);
        PEAK.with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Runs `f`, returning its peak live bytes. Zeroes the counters first so an earlier call can't
/// inflate the peak.
fn peak_live_bytes(f: impl FnOnce()) -> i64 {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    f();
    PEAK.with(|peak| peak.get())
}

// -- seeded LCG -------------------------------------------------------------------------------

/// A linear congruential generator with Numerical Recipes' constants, reproducible from its seed.
struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

// -- fixtures -----------------------------------------------------------------------------------

/// Valid sketch blobs: an Agent sketch with both stores and the zero bin, a logarithmic one with
/// derived stats, and a wide Agent one whose store sits at the bin limit.
fn sketch_blobs() -> Vec<Vec<u8>> {
    let mut agent = DdSketch::new();
    for v in [0.25, 1.0, 7.5, 100.0, -2.0, -1e6, 0.0, 1e300] {
        agent.add(v);
    }
    let log = DdSketch::from_parts(
        Mapping::logarithmic(1.0202, 0.5, 2048),
        vec![Bin { key: -3, count: 1.5 }, Bin { key: 40, count: 2.0 }],
        vec![Bin { key: 7, count: 1.0 }],
        2.0,
        None,
    );
    let mut wide = DdSketch::new();
    for i in 0..6000 {
        wide.add(1.001f64.powi(i * 3));
    }
    vec![agent.to_bytes(), log.to_bytes(), wide.to_bytes()]
}

fn hll_of(n: u32) -> HyperLogLog {
    let mut hll = HyperLogLog::new();
    for i in 0..n {
        hll.insert(&i.to_le_bytes());
    }
    hll
}

/// Valid HLL blobs in each representation: small (0 and 2 members), array (10), HLL (5000).
fn hll_blobs() -> Vec<Vec<u8>> {
    [0, 2, 10, 5000].into_iter().map(|n| hll_of(n).to_bytes()).collect()
}

/// Runs what `aggregate` and the encoders do with an accepted sketch: merges into a fresh and a
/// populated accumulator, every quantile, a byte round trip.
fn exercise_sketch(sketch: &DdSketch) {
    for mut acc in [DdSketch::new(), {
        let mut s = DdSketch::new();
        s.add(3.0);
        s
    }] {
        acc.merge(sketch);
        for q in [0.0, 0.01, 0.5, 0.99, 1.0] {
            let _ = acc.quantile(q);
        }
    }
    let _ = DdSketch::from_bytes(&sketch.to_bytes());
}

/// What `aggregate` and the encoders do with an accepted HLL, merging into a clone of each of
/// `accumulators` (built once by the caller: one per representation).
fn exercise_hll(hll: &HyperLogLog, accumulators: &[HyperLogLog]) {
    let _ = hll.estimate();
    for acc in accumulators {
        let mut acc = acc.clone();
        acc.merge(hll);
        let _ = acc.estimate();
    }
    let _ = HyperLogLog::from_bytes(&hll.to_bytes());
}

// -- truncation -------------------------------------------------------------------------------

/// No proper prefix of a valid blob decodes: both formats end in length-checked content.
#[test]
fn every_truncation_of_a_sketch_blob_fails_cleanly() {
    for blob in sketch_blobs() {
        for len in 0..blob.len() {
            let result = std::panic::catch_unwind(|| DdSketch::from_bytes(&blob[..len]));
            match result {
                Ok(Err(_)) => {}
                Ok(Ok(_)) => panic!("a {len}-byte prefix of a {}-byte blob decoded", blob.len()),
                Err(_) => panic!("decoding a {len}-byte prefix panicked"),
            }
        }
    }
}

#[test]
fn every_truncation_of_an_hll_blob_fails_cleanly() {
    for blob in hll_blobs() {
        for len in 0..blob.len() {
            let result =
                std::panic::catch_unwind(|| HyperLogLog::from_bytes(&blob[..len]).is_err());
            assert_eq!(
                result.ok(),
                Some(true),
                "a {len}-byte prefix of a {}-byte blob",
                blob.len()
            );
        }
    }
}

// -- seeded bit flips -------------------------------------------------------------------------

#[test]
fn seeded_bit_flips_in_a_sketch_blob_never_panic() {
    let mut rng = Lcg(0x5eed_d053);
    for blob in sketch_blobs() {
        for _ in 0..1000 {
            let mut flipped = blob.clone();
            let bit = rng.next_usize(flipped.len() * 8);
            flipped[bit / 8] ^= 1 << (bit % 8);
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                if let Ok(sketch) = DdSketch::from_bytes(&flipped) {
                    exercise_sketch(&sketch);
                }
            }));
            assert!(outcome.is_ok(), "bit {bit} of a {}-byte blob", blob.len());
        }
    }
}

#[test]
fn seeded_bit_flips_in_an_hll_blob_never_panic() {
    let mut rng = Lcg(0x5eed_d054);
    let accumulators = [HyperLogLog::new(), hll_of(10), hll_of(5000)];
    for blob in hll_blobs() {
        for _ in 0..1000 {
            let mut flipped = blob.clone();
            let bit = rng.next_usize(flipped.len() * 8);
            flipped[bit / 8] ^= 1 << (bit % 8);
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                if let Ok(hll) = HyperLogLog::from_bytes(&flipped) {
                    exercise_hll(&hll, &accumulators);
                }
            }));
            assert!(outcome.is_ok(), "bit {bit} of a {}-byte blob", blob.len());
        }
    }
}

// -- hostile counts ---------------------------------------------------------------------------

/// An Agent sketch header with an empty summary, then `count` as the positive store's bin count
/// and nothing after it: a blob of a few dozen bytes claiming up to `u64::MAX` bins.
fn sketch_claiming(count: u64) -> Vec<u8> {
    let mut blob = DdSketch::new().to_bytes();
    // The empty sketch ends in two zero bin counts; replace them with the hostile one.
    blob.truncate(blob.len() - 2);
    let mut v = count;
    while v >= 0x80 {
        blob.push((v as u8) | 0x80);
        v >>= 7;
    }
    blob.push(v as u8);
    blob
}

#[test]
fn a_hostile_bin_count_is_malformed_without_a_proportional_allocation() {
    for count in [1u64 << 20, u64::from(u32::MAX), u64::MAX] {
        let blob = sketch_claiming(count);
        let mut result = None;
        let peak = peak_live_bytes(|| result = Some(DdSketch::from_bytes(&blob)));
        assert_eq!(result, Some(Err(SketchDecodeError::Malformed)), "count {count}");
        assert!(peak < 1024, "count {count}: peak {peak} bytes for a {}-byte blob", blob.len());
    }
}

/// A `data` word with representation tag `tag`, then a members list claiming `count` entries and
/// carrying none.
fn hll_claiming(tag: u64, count: u32) -> Vec<u8> {
    let mut blob = tag.to_le_bytes().to_vec();
    blob.push(1);
    blob.extend_from_slice(&count.to_le_bytes());
    blob
}

#[test]
fn a_hostile_members_count_is_rejected_without_a_proportional_allocation() {
    for (tag, count) in [(1, 129), (1, u32::MAX), (3, 772), (3, u32::MAX), (0, u32::MAX)] {
        let blob = hll_claiming(tag, count);
        let mut rejected = false;
        let peak = peak_live_bytes(|| rejected = HyperLogLog::from_bytes(&blob).is_err());
        assert!(rejected, "tag {tag}, count {count}");
        assert!(peak < 1024, "tag {tag}, count {count}: peak {peak} bytes");
    }
}

// -- peak allocation of a valid decode --------------------------------------------------------

/// The most a valid decode may allocate per blob byte. A sketch bin is at least 9 wire bytes and
/// 16 heap bytes, held twice while `from_parts` normalizes it; an HLL blob's members list is its
/// own size on the heap.
const MAX_DECODE_BYTES_PER_BLOB_BYTE: i64 = 4;

#[test]
fn decoding_a_sketch_blob_allocates_at_most_four_times_its_length() {
    let bins: Vec<Bin> = (1..=4096).map(|key| Bin { key, count: 1.0 }).collect();
    let full = DdSketch::from_parts(
        Mapping::agent(),
        bins.clone(),
        bins,
        0.0,
        Some(SketchStats { count: 8192.0, min: -1.0, max: 1.0, sum: 0.0 }),
    );
    for blob in sketch_blobs().into_iter().chain([full.to_bytes()]) {
        let peak = peak_live_bytes(|| drop(DdSketch::from_bytes(&blob).expect("valid blob")));
        assert!(
            peak <= MAX_DECODE_BYTES_PER_BLOB_BYTE * blob.len() as i64,
            "peak {peak} bytes for a {}-byte blob",
            blob.len()
        );
    }
}

#[test]
fn decoding_an_hll_blob_allocates_at_most_four_times_its_length() {
    for blob in hll_blobs() {
        let peak = peak_live_bytes(|| drop(HyperLogLog::from_bytes(&blob).expect("valid blob")));
        assert!(
            peak <= MAX_DECODE_BYTES_PER_BLOB_BYTE * blob.len() as i64,
            "peak {peak} bytes for a {}-byte blob",
            blob.len()
        );
    }
}
