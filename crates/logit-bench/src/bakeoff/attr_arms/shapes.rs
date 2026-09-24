//! The inputs every attribute arm is measured on: widths, value mixes, arrival orders, and the
//! mixed-gateway key-set distribution.
//!
//! Nothing here is timed. Every helper builds its data *outside* the measured region, for the
//! reason `docs/design/memory.md`'s "Fixtures" section gives: a `bytes::Bytes` promotes to its
//! shared, atomically-refcounted representation on its *first* clone, so a value built fresh
//! inside a timed loop would measure that one-time promotion instead of the refcount bump a real
//! clone pays. [`shared_str`] memoizes and pre-promotes, so every `Value::Str` a bench gets is
//! already shared.
//!
//! **Widths** are `docs/design/data-shapes.md`'s own: 2 (a statsd/collectd metric event), 8 (the
//! last inline width, and the OTLP span p50), 9 (the first spilled width, and the median OTLP log
//! record), 12 (the commonest measured JSON-log width), 17 (the span p90), 30 (a PostgreSQL
//! `jsonlog` access-log line).
//!
//! **Value mixes** come from the same survey: §2 puts string values at 50-88% of a parsed log
//! record's attributes, the rest integers, floats and booleans. [`Mix::Mostly`] is 75% strings,
//! the middle of that band, and the two extremes bracket it. The mix matters most to arm **C**: a
//! `Value::Str` clone is an atomic increment, a `Value::I64` clone is a register move, and a
//! "bitwise copy the whole slice" fast path is only available to the latter.

use bytes::Bytes;
use logit_core::interner::{intern, Symbol};
use logit_core::{AttrMap, Value};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// The widths every arm is measured at. `docs/design/data-shapes.md` §2-§5.
pub const WIDTHS: [usize; 6] = [2, 8, 9, 12, 17, 30];

/// How the values of a synthetic map are distributed across `Value` variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mix {
    /// Every value a scalar (`I64`/`F64`/`Bool`/`Timestamp`): no heap, no refcount, and the only
    /// mix where a bitwise copy of the whole entry slice is a legal clone. `size_vs_alloc.rs`'s
    /// `attr_clone` group measures only this mix.
    Scalar,
    /// 75% `Value::Str`, the middle of the survey's 50-88% band; the rest scalars.
    Mostly,
    /// Every value a `Value::Str`. The upper end of the band, and the worst case for any fast path
    /// that depends on scalars.
    AllStr,
}

impl Mix {
    /// The name this mix appears under in divan's output.
    pub fn label(self) -> &'static str {
        match self {
            Mix::Scalar => "scalar",
            Mix::Mostly => "str75",
            Mix::AllStr => "str100",
        }
    }

    /// Whether entry `i` of a `width`-wide map is a string under this mix. Deterministic, and
    /// spread across the map rather than clustered at one end, so a fast path that gives up at the
    /// first non-scalar gives up in the same place every run.
    fn is_str(self, i: usize) -> bool {
        match self {
            Mix::Scalar => false,
            // Every slot but every fourth one: 3 in 4, wherever the map is cut.
            Mix::Mostly => !i.is_multiple_of(4),
            Mix::AllStr => true,
        }
    }
}

/// Every mix, for a `divan` `args` list.
pub const MIXES: [Mix; 3] = [Mix::Scalar, Mix::Mostly, Mix::AllStr];

/// A `Value::Str` over an already-shared, already-promoted `Bytes`, memoized per string. The module
/// doc says why the memoization matters.
pub fn shared_str(s: &str) -> Value {
    static CACHE: OnceLock<Mutex<HashMap<String, Bytes>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache.lock().expect("no bench panics while holding this");
    let bytes = cache.entry(s.to_string()).or_insert_with(|| {
        let bytes = Bytes::from(s.to_string());
        // The promotion itself: clone once here, in setup, so no measured clone is the first.
        let _promoted = bytes.clone();
        bytes
    });
    Value::Str(bytes.clone())
}

/// `width` interned keys under `prefix`, in **ascending `Symbol` order**.
///
/// Interning is monotonic and process-global, so the symbols a prefix produces are assigned in
/// first-call order and are stable for the life of the process. Keys are interned once, here,
/// outside every timed region: no bench below measures the interner.
pub fn keys(prefix: &str, width: usize) -> Vec<Symbol> {
    let mut keys: Vec<Symbol> =
        (0..width).map(|i| intern(&format!("w3b.{prefix}.{i:02}"))).collect();
    keys.sort_unstable();
    keys
}

/// The same keys in a fixed, *unsorted* arrival order.
///
/// `AttrMap::insert_sym` is a binary search plus a positional `SmallVec::insert`, so ascending
/// order is its best case (nothing moves) and this shuffle a worse one. Most real sources deliver
/// ascending order, because keys arrive in the interner's first-seen order (ADR
/// `event-sizing-and-allocation-strategy`, "Consequences"). The stride is coprime with the width,
/// so it visits every index once without a random-number generator: the same construction as
/// `benches/size_vs_alloc.rs`'s `build_shape::keys`, so the two files' build numbers compare.
pub fn shuffled(keys: &[Symbol]) -> Vec<Symbol> {
    let width = keys.len();
    if width == 0 {
        return Vec::new();
    }
    let stride = if width.is_multiple_of(7) { 5 } else { 7 };
    (0..width).map(|i| keys[(i * stride) % width]).collect()
}

/// One value for slot `i` of a `width`-wide map under `mix`. String lengths follow
/// `docs/design/data-shapes.md` §2's measured band (median 10-16 bytes, p90 26-37).
pub fn value(mix: Mix, i: usize) -> Value {
    if mix.is_str(i) {
        // 14 bytes at the median, 33 in the tail: inside §2's measured bands.
        if i.is_multiple_of(5) {
            shared_str("Mozilla/5.0 (Macintosh; Intel M")
        } else {
            shared_str("198.51.100.23 ")
        }
    } else {
        match i % 4 {
            0 => Value::I64(i as i64 * 7),
            1 => Value::F64(i as f64 * 1.5),
            2 => Value::Bool(i.is_multiple_of(2)),
            _ => Value::Timestamp(1_725_091_200_000_000_000 + i as i64),
        }
    }
}

/// A parser's scratch buffer: `width` `(Symbol, Value)` pairs in arrival (unsorted) order, which is
/// the shape `json`/`logfmt`/`kv` and both count-knowing decoders have in hand at the merge.
pub fn scratch(prefix: &str, width: usize, mix: Mix) -> Vec<(Symbol, Value)> {
    let keys = shuffled(&keys(prefix, width));
    keys.iter().enumerate().map(|(i, k)| (*k, value(mix, i))).collect()
}

/// The shipped build, for the arms to be measured against: `insert_sym` per entry, in arrival
/// order.
pub fn attr_map(scratch: &[(Symbol, Value)]) -> AttrMap {
    let mut map = AttrMap::new();
    for (k, v) in scratch {
        map.insert_sym(*k, v.clone());
    }
    map
}

/// Arm **P**'s build, mirrored: append in arrival order, then sort once.
///
/// **Not `sort_unstable`, and not `sort` alone.** `AttrMap::insert` is last-write-wins on a
/// repeated key (`insert_sym`'s `Ok(i) => self.0[i].1 = value`), so a bulk build is only equivalent
/// if it (a) sorts *stably*, preserving arrival order within a key, and (b) collapses each run to
/// its **last** entry. `benches/size_vs_alloc.rs`'s `append_then_sort` does neither, so it times a
/// build that would keep an arbitrary duplicate. The difference isn't free (a stable sort allocates
/// a scratch buffer past 20 elements, and the dedup is another pass), so an arm-P number from that
/// bench is optimistic on inputs with repeated keys. `tests/attr_arms.rs`'s equivalence tests pin
/// the semantics this function implements.
pub fn bulk_build(scratch: &[(Symbol, Value)]) -> Vec<(Symbol, Value)> {
    let mut entries: Vec<(Symbol, Value)> = Vec::with_capacity(scratch.len());
    entries.extend_from_slice(scratch);
    entries.sort_by_key(|(k, _)| *k);
    dedup_last(&mut entries);
    entries
}

/// Collapses each run of equal keys to its last entry, in place: the last-write-wins half of
/// [`bulk_build`]'s equivalence with repeated `AttrMap::insert` calls.
pub fn dedup_last(entries: &mut Vec<(Symbol, Value)>) {
    let mut write = 0usize;
    let mut read = 0usize;
    while read < entries.len() {
        let key = entries[read].0;
        let mut last = read;
        while last + 1 < entries.len() && entries[last + 1].0 == key {
            last += 1;
        }
        entries.swap(write, last);
        write += 1;
        read = last + 1;
    }
    entries.truncate(write);
}

// -- the mixed-gateway key-set distribution ------------------------------------------------------

/// How many distinct key-sets the survey's mixed OTLP gateway carried
/// (`docs/design/data-shapes.md` §5.4).
pub const GATEWAY_SETS: usize = 196;

/// A synthesized stream of key-sets matching the two numbers the survey reports for that gateway,
/// rounded: **top-1 = 9%** of events and **top-5 = 36%** (§5.4 measured 9.5% and 36.1%).
///
/// **Two components, not a plain Zipf.** No single Zipf exponent over 196 sets reaches both
/// numbers: matching top-5 = 36% requires `s ≈ 0.245`, which puts the head at 1.4%, and matching
/// top-1 = 9% requires `s ≈ 1.0`, which puts the top five at 45%. So the head is five sets carrying
/// 9/8/7/6/6% (36% in total, top-1 9%), and the tail is a Zipf (`s = 1`) over the remaining 191
/// sets, scaled to the leftover 64%. Both reported numbers hold by construction; the tail's
/// *shape*, which the survey doesn't report, is the assumption. A flatter tail would make a bounded
/// cache look worse and a steeper one better, so any cache-hit-rate number from this fixture
/// carries that assumption.
///
/// Widths are drawn deterministically from 8-17, the OTLP span band §4 measures (p50 8, p90 17).
pub struct Gateway {
    /// Each distinct key-set, sorted, most frequent first.
    pub sets: Vec<Vec<Symbol>>,
    /// One event's worth of key-set index per element, interleaved so no set arrives in a run.
    pub stream: Vec<usize>,
}

impl Gateway {
    /// Builds the distribution over a stream of `events` events.
    pub fn new(events: usize) -> Self {
        let sets: Vec<Vec<Symbol>> = (0..GATEWAY_SETS)
            .map(|s| {
                let width = 8 + (s * 3) % 10;
                keys(&format!("gw{s:03}"), width)
            })
            .collect();

        // Shares, in per-mille so the arithmetic is exact: the five-set head, then Zipf s=1 over
        // the tail, scaled to the remaining 640.
        let head = [90u32, 80, 70, 60, 60];
        // The tail's ranks continue the head's rather than restarting at 1: a Zipf restarted at
        // rank 1 would give its own top set ~11% of the stream, more than the head's 9%, and the
        // "top-1" the test then measures would be a tail set. Continuing the ranks puts the tail's
        // largest share at ~3%, comfortably under the head's smallest.
        let tail_harmonic: f64 =
            (1..=(GATEWAY_SETS - head.len())).map(|r| 1.0 / (r + head.len()) as f64).sum();
        let mut counts: Vec<usize> = Vec::with_capacity(GATEWAY_SETS);
        for share in head {
            counts.push(events * share as usize / 1000);
        }
        for rank in 1..=(GATEWAY_SETS - head.len()) {
            let share = 0.640 / ((rank + head.len()) as f64 * tail_harmonic);
            // Every tail set appears at least once, so the cache sees all 196 distinct sets.
            counts.push(((events as f64 * share).round() as usize).max(1));
        }

        // Interleave: emit one event per set per pass, cycling, so the arrival order is mixed the
        // way a gateway's is rather than 196 contiguous runs (which any cache would ace).
        let mut stream = Vec::with_capacity(counts.iter().sum());
        let mut remaining = counts;
        while remaining.iter().any(|&c| c > 0) {
            for (set, count) in remaining.iter_mut().enumerate() {
                if *count > 0 {
                    *count -= 1;
                    stream.push(set);
                }
            }
        }
        Self { sets, stream }
    }

    /// The scratch buffer one event of key-set `set` would hand a builder: arrival order shuffled,
    /// values under `mix`.
    pub fn event_scratch(&self, set: usize, mix: Mix) -> Vec<(Symbol, Value)> {
        let keys = shuffled(&self.sets[set]);
        keys.iter().enumerate().map(|(i, k)| (*k, value(mix, i))).collect()
    }
}
