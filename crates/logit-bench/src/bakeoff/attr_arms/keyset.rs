//! **Arm K -- a shared key-set plus a values vector.** A map becomes `Arc<[Symbol]>` (the sorted
//! key-set, shared by every event of the same shape) and a `Vec<Value>` in the same order. Looking
//! the key-set up once per event at the merge is affordable because a parser has the whole
//! key sequence in hand by then, which is what makes this different from a general object model
//! where every insert is a potential shape transition.
//!
//! `docs/plans/event-sizing.md` states the case against before the case for: interned 4-byte
//! symbols already banked most of what a key-set saves, and the clone -- the operation this is
//! supposed to help -- barely moves, since `Vec<Value>` still clones every value. Hence the
//! **pre-registered kill criterion**: K must beat the append-then-sort bulk build by **≥10% on the
//! 12-attribute log's build + clone combined**, and hold up on the 196-key-set gateway, or it is
//! dropped. `benches/attr_arms.rs`'s `kill_criterion` module is that one comparison, run on its
//! own so it cannot be read off a table by accident.
//!
//! **What is mirrored and what is simplified.**
//!
//! - The values are real `logit_core::Value`s, so clone cost is the true one (an atomic increment
//!   per `Value::Str`).
//! - [`KeySetCache`] is a `HashMap<u64, CacheEntry>` with **LRU eviction by linear scan** over its
//!   64 entries. A production cache would keep an intrusive LRU list; the scan only runs on a miss,
//!   and the gateway bench reports miss *rate* alongside the timing so the two can be separated.
//! - The key-sequence hash is an FxHash-shaped multiply-rotate over the symbols
//!   ([`SeqHasher`]), not SipHash: a cache on this path must be cheaper than the sort it replaces,
//!   and `DefaultHasher` would decide the question by itself.
//! - **Duplicate keys take a slow path.** The fast placement writes each parsed value straight into
//!   its slot through a raw pointer, which requires the arrival sequence's keys to be distinct
//!   (otherwise two sources target one slot and another is left uninitialized). A sequence with a
//!   repeat falls back to a sorted build. Real parsers do produce repeats (`statsd_in`'s tags,
//!   `syslog_in`'s SD params), so this is a real cost of the representation, not a bench shortcut;
//!   `tests/attr_arms.rs` pins that the slow path still yields last-write-wins.
//! - Not modelled at all: what a shared key-set would do to `attrs::merged`, `SeriesKey`, `keep`,
//!   the native encoder's dictionary, and Lua's `AttrsProxy`, all of which hand out `&Value` and
//!   iterate in sorted-`Symbol` order. [`KeySetMap`] preserves both properties, which is the
//!   precondition for any of them to keep working -- it does not prove they would.

use logit_core::interner::Symbol;
use logit_core::Value;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// An FxHash-shaped hasher over `Symbol`s: one multiply and one rotate per key.
#[derive(Default)]
pub struct SeqHasher(u64);

impl Hasher for SeqHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.write_u32(*byte as u32);
        }
    }

    fn write_u32(&mut self, value: u32) {
        self.0 = (self.0 ^ value as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
        self.0 = self.0.rotate_left(26);
    }
}

/// Hashes a key sequence **in arrival order** -- the cache maps a sequence, not a set, so a
/// producer that emits its fields in a stable order (which is what makes this arm worth trying at
/// all) gets a hit without any canonicalization first.
pub fn hash_sequence(keys: impl IntoIterator<Item = Symbol>) -> u64 {
    let mut hasher = SeqHasher::default();
    for key in keys {
        key.hash(&mut hasher);
    }
    hasher.finish()
}

/// A map as a shared sorted key-set plus a positional values vector.
#[derive(Debug, Clone, PartialEq)]
pub struct KeySetMap {
    keys: Arc<[Symbol]>,
    values: Vec<Value>,
}

impl KeySetMap {
    /// Builds directly from an arrival-order scratch with no cache at all -- sort, dedup, allocate
    /// a fresh key-set. This is both the cache-miss path and the honest baseline for "what if the
    /// shape never repeats".
    pub fn build_uncached(scratch: Vec<(Symbol, Value)>) -> Self {
        let (keys, perm, distinct) = key_set_of(&scratch);
        Self::place(Arc::from(keys), &perm, scratch, distinct)
    }

    /// The sorted key-set, shared.
    pub fn keys(&self) -> &Arc<[Symbol]> {
        &self.keys
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Binary search over the key-set, then one index into the values -- the same two steps
    /// `AttrMap::get_sym` takes, over two allocations instead of one.
    pub fn get_sym(&self, key: Symbol) -> Option<&Value> {
        self.keys.binary_search(&key).ok().map(|i| &self.values[i])
    }

    /// Sorted-`Symbol` order, like `AttrMap::iter` -- the property every consumer of an `AttrMap`
    /// depends on.
    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &Value)> {
        self.keys.iter().copied().zip(self.values.iter())
    }

    /// A shape transition with no help: the key-set is rebuilt and reallocated.
    pub fn insert_one(&mut self, key: Symbol, value: Value) {
        match self.keys.binary_search(&key) {
            Ok(i) => self.values[i] = value,
            Err(i) => {
                let mut keys: Vec<Symbol> = self.keys.to_vec();
                keys.insert(i, key);
                self.keys = Arc::from(keys);
                self.values.insert(i, value);
            }
        }
    }

    /// The same transition with a memo of `(key-set identity, added key) -> key-set`, so a repeated
    /// enrichment (a `set` transform stamping the same attribute on every event, say) pays the
    /// `Arc` bump and the `Vec::insert` but not the key-set rebuild.
    pub fn insert_one_cached(&mut self, key: Symbol, value: Value, cache: &mut TransitionCache) {
        match self.keys.binary_search(&key) {
            Ok(i) => self.values[i] = value,
            Err(i) => {
                let from = Arc::as_ptr(&self.keys) as *const () as usize;
                let keys = match cache.map.get(&(from, key)) {
                    Some(keys) => Arc::clone(keys),
                    None => {
                        let mut keys: Vec<Symbol> = self.keys.to_vec();
                        keys.insert(i, key);
                        let keys: Arc<[Symbol]> = Arc::from(keys);
                        cache.map.insert((from, key), Arc::clone(&keys));
                        keys
                    }
                };
                self.keys = keys;
                self.values.insert(i, value);
            }
        }
    }

    /// Removing a key is the same transition in reverse, and has no cached form here: the memo is
    /// keyed by the *source* key-set's identity, and a removal's source is whatever the last
    /// transition produced.
    pub fn remove_one(&mut self, key: Symbol) -> Option<Value> {
        let i = self.keys.binary_search(&key).ok()?;
        let mut keys: Vec<Symbol> = self.keys.to_vec();
        keys.remove(i);
        self.keys = Arc::from(keys);
        Some(self.values.remove(i))
    }

    /// Moves each scratch value into the slot its key occupies in the key-set.
    fn place(
        keys: Arc<[Symbol]>,
        perm: &[u32],
        scratch: Vec<(Symbol, Value)>,
        distinct: bool,
    ) -> Self {
        if !distinct {
            // The slow path: repeated keys mean `perm` is not a permutation, so fall back to a
            // sorted build with last-write-wins (see this module's doc).
            let mut entries: Vec<(Symbol, Value)> = scratch;
            entries.sort_by_key(|(k, _)| *k);
            super::shapes::dedup_last(&mut entries);
            let values = entries.into_iter().map(|(_, v)| v).collect();
            return Self { keys, values };
        }
        let len = scratch.len();
        let mut values: Vec<Value> = Vec::with_capacity(len);
        let dst = values.as_mut_ptr();
        for (i, (_, value)) in scratch.into_iter().enumerate() {
            // SAFETY: `perm` is a permutation of `0..len` when `distinct` (every key occupies
            // exactly one key-set slot), `values` has capacity `len`, and each slot is written
            // exactly once from an uninitialized state. The length is set only after the loop, so
            // a panic mid-loop leaks the values written so far rather than dropping uninitialized
            // memory.
            unsafe { dst.add(perm[i] as usize).write(value) };
        }
        // SAFETY: every slot in `0..len` was written exactly once by the loop above.
        unsafe { values.set_len(len) };
        Self { keys, values }
    }
}

/// The sorted key-set of an arrival sequence, each source position's destination slot, and whether
/// the sequence's keys were distinct.
fn key_set_of(scratch: &[(Symbol, Value)]) -> (Vec<Symbol>, Vec<u32>, bool) {
    let len = scratch.len();
    let mut order: Vec<u32> = (0..len as u32).collect();
    order.sort_by_key(|&i| scratch[i as usize].0);
    let mut keys: Vec<Symbol> = Vec::with_capacity(len);
    let mut perm = vec![0u32; len];
    let mut distinct = true;
    for &source in &order {
        let key = scratch[source as usize].0;
        if keys.last() == Some(&key) {
            distinct = false;
        } else {
            keys.push(key);
        }
        perm[source as usize] = (keys.len() - 1) as u32;
    }
    (keys, perm, distinct)
}

/// A learned, bounded memo of `key sequence -> (key-set, slot permutation)`.
pub struct KeySetCache {
    entries: HashMap<u64, CacheEntry>,
    capacity: usize,
    clock: u64,
    /// Sequences served from the memo.
    pub hits: u64,
    /// Sequences that had to be sorted (a first sighting, a hash collision, or an eviction victim
    /// coming back).
    pub misses: u64,
    /// Entries thrown out to stay inside `capacity`.
    pub evictions: u64,
}

struct CacheEntry {
    sequence: Vec<Symbol>,
    keys: Arc<[Symbol]>,
    perm: Vec<u32>,
    distinct: bool,
    last_used: u64,
}

impl KeySetCache {
    /// `capacity` distinct key sequences. 64 is `logit_core::interner::KeyCache`'s own size and the
    /// number `docs/design/data-shapes.md` §4 measures the 196-set gateway against.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            capacity,
            clock: 0,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    /// Builds a map from a parser's scratch, learning the shape on a miss.
    pub fn build(&mut self, scratch: Vec<(Symbol, Value)>) -> KeySetMap {
        self.clock += 1;
        let hash = hash_sequence(scratch.iter().map(|(k, _)| *k));
        if let Some(entry) = self.entries.get_mut(&hash) {
            // Verified, not assumed: a 64-bit collision would otherwise mis-key an event's values.
            if entry.sequence.len() == scratch.len()
                && entry.sequence.iter().zip(scratch.iter()).all(|(a, (b, _))| a == b)
            {
                entry.last_used = self.clock;
                self.hits += 1;
                let keys = Arc::clone(&entry.keys);
                let distinct = entry.distinct;
                let perm = entry.perm.clone();
                return KeySetMap::place(keys, &perm, scratch, distinct);
            }
        }
        self.misses += 1;
        let (keys, perm, distinct) = key_set_of(&scratch);
        let keys: Arc<[Symbol]> = Arc::from(keys);
        if self.entries.len() >= self.capacity && !self.entries.contains_key(&hash) {
            self.evict_one();
        }
        self.entries.insert(
            hash,
            CacheEntry {
                sequence: scratch.iter().map(|(k, _)| *k).collect(),
                keys: Arc::clone(&keys),
                perm: perm.clone(),
                distinct,
                last_used: self.clock,
            },
        );
        KeySetMap::place(keys, &perm, scratch, distinct)
    }

    /// Least-recently-used, by linear scan -- see this module's doc for why that is acceptable
    /// here and would not be in production.
    fn evict_one(&mut self) {
        if let Some((&victim, _)) = self.entries.iter().min_by_key(|(_, entry)| entry.last_used) {
            self.entries.remove(&victim);
            self.evictions += 1;
        }
    }

    pub fn miss_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.misses as f64 / total as f64
        }
    }
}

/// The memo [`KeySetMap::insert_one_cached`] consults: `(key-set identity, added key) -> key-set`.
///
/// Unbounded, deliberately: a transform's set of added keys is configuration, not input, so the
/// number of distinct transitions is bounded by the config -- unlike [`KeySetCache`], whose keys
/// come off the wire.
#[derive(Default)]
pub struct TransitionCache {
    map: HashMap<(usize, Symbol), Arc<[Symbol]>>,
}

impl TransitionCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}
