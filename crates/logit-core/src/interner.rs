//! Process-wide interning for attribute/metric keys.
//!
//! Keys repeat enormously across telemetry (`host`, `env`, `service.name`, ...). Interning them
//! once means `AttrMap` compares and hashes `u32`s instead of repeated string allocations, and the
//! same table backs the wire format's dictionary encoding (see `docs/design/wire-protocol.md`).
//!
//! The table is process-global and concurrent, so even a hit costs a hash and a shard lock. A
//! component whose keys come from its *input* rather than its config -- and so can't intern them
//! once at construction -- fronts it with a [`KeyCache`], a small single-owner memo that makes a
//! repeat key one `memcmp`.

use lasso::{Spur, ThreadedRodeo};
use std::sync::OnceLock;

/// An interned string. Cheap to copy, compare, and hash.
pub type Symbol = Spur;

static INTERNER: OnceLock<ThreadedRodeo> = OnceLock::new();

fn interner() -> &'static ThreadedRodeo {
    INTERNER.get_or_init(ThreadedRodeo::new)
}

/// Intern a key/value string, returning its `Symbol`. Safe to call concurrently from any worker.
pub fn intern(s: &str) -> Symbol {
    interner().get_or_intern(s)
}

/// Resolve a `Symbol` back to its string. Panics if the symbol was not produced by [`intern`] --
/// symbols are only ever created by this module, so this indicates a bug, not bad input.
pub fn resolve(sym: Symbol) -> &'static str {
    interner().resolve(&sym)
}

/// Look up a string's `Symbol` *without* interning it. Returns `None` if the string was never
/// interned, and -- unlike [`intern`] -- a miss never adds it to the table. Since interning is
/// monotonic and process-global, `None` here means the string cannot possibly be the key of
/// anything already built from a `Symbol` (an `AttrMap` entry, a `SeriesKey`, ...), so a caller
/// that only wants to test membership can skip `intern` entirely. Use this instead of `intern`
/// whenever the string might not exist and creating it on a miss would be wasted work -- e.g.
/// `AttrMap::get`/`remove` on a key the map turns out not to have.
pub fn lookup(s: &str) -> Option<Symbol> {
    interner().get(s)
}

/// Count of distinct strings interned so far, process-wide. Never decreases -- `ThreadedRodeo`
/// never evicts (see `docs/design/memory.md` §4) -- so this is an observability hook for that
/// growth, not a live size. Cheap enough to expose now even with nothing wired up to read it yet.
pub fn len() -> usize {
    interner().len()
}

/// A per-component memo of `&str -> Symbol` for keys a parser sees again and again -- a pure
/// fast path in front of [`intern`], never a substitute for it.
///
/// **Why it exists.** `intern` on a string the table already holds allocates nothing, but it is
/// still a hash of the bytes plus a `DashMap` shard lock (an atomic on a cache line every task
/// in the process shares) plus a table probe. A parser that produces keys from its *input* --
/// `json`'s object keys, unlike the config-time keys `set`/`csv`/`kv_metrics` intern once at
/// construction -- pays that once per key per event, and on the `json-parse` load-test
/// scenario that was the single largest cost left in the parse (`docs/design/performance.md`).
/// The key set of a real log stream is small and repeats in the same order on every line, so a
/// cache owned by the one task that runs the parser turns each of those into one `memcmp`.
///
/// **Shape.** Not a hash map: entries are kept in first-seen (document) order with a cursor,
/// so on the steady path the next key is the entry at the cursor -- a length compare and a
/// `memcmp`, no hashing at all. A key that isn't there (an optional field absent this event, a
/// producer that reorders) is found by scanning forward from the cursor and wrapping, which
/// resynchronises within a few compares; a name repeated at two nesting depths (`id` and
/// `user.id`) is one entry either way. Only a key seen for the first time reaches `intern`.
///
/// **Bounds.** Capped at [`KeyCache::MAX_ENTRIES`] distinct keys, and keys longer than
/// [`KeyCache::MAX_KEY_LEN`] are never cached: the process-wide interner already retains every
/// distinct key forever (`docs/design/memory.md` §4 accepts that on the "keys are schema-shaped"
/// premise), and this is a *second* copy per node, so it must not also grow without bound when
/// a producer puts data in key position. Past the cap a miss still returns the right `Symbol`
/// -- it just pays today's `intern` after a bounded scan of mostly single-instruction length
/// rejections. Never evicts: an entry, once cached, is as eternal as its `Symbol`.
///
/// **Contract.** `cache.get_or_intern(s) == intern(s)` for every `s`, always; the cache holds no
/// `Symbol` the global table doesn't. Single-owner by design (`&mut self`): a `Transform` is
/// owned by exactly one task, so there is nothing to synchronise.
#[derive(Debug, Default)]
pub struct KeyCache {
    entries: Vec<(Box<str>, Symbol)>,
    cursor: usize,
}

impl KeyCache {
    /// Most distinct keys one cache will hold. 64 covers a wide flat log object (pino's default
    /// shape is 28 keys) plus the keys of any nested objects with room to spare; see the type
    /// docs for why it is capped at all.
    pub const MAX_ENTRIES: usize = 64;
    /// Longest key worth caching. A key past this is data in key position, not schema, and
    /// caching it would only spend the cap on something that won't repeat.
    pub const MAX_KEY_LEN: usize = 128;

    pub fn new() -> Self {
        Self { entries: Vec::with_capacity(Self::MAX_ENTRIES), cursor: 0 }
    }

    /// The `Symbol` for `s`, exactly as [`intern`] would return it -- from the cache when `s` has
    /// been seen by this cache before, from the interner (and then cached, if there is room)
    /// otherwise.
    #[inline]
    pub fn get_or_intern(&mut self, s: &str) -> Symbol {
        let len = self.entries.len();
        if len > 0 {
            // Steady state: the same keys in the same order as last time, so the one we want is
            // at the cursor. Wrap so the first key of the next event follows the last key of
            // this one without a scan.
            let start = if self.cursor < len { self.cursor } else { 0 };
            if self.entries[start].0.as_ref() == s {
                self.cursor = start + 1;
                return self.entries[start].1;
            }
            // Resync: scan forward from the cursor, wrapping, so an absent optional key or a
            // reordered one is found in a few compares rather than a full pass.
            for offset in 1..len {
                let i = (start + offset) % len;
                if self.entries[i].0.as_ref() == s {
                    self.cursor = i + 1;
                    return self.entries[i].1;
                }
            }
        }
        let sym = intern(s);
        if len < Self::MAX_ENTRIES && s.len() <= Self::MAX_KEY_LEN {
            self.entries.push((Box::from(s), sym));
            self.cursor = len + 1;
        }
        sym
    }

    /// Distinct keys cached so far -- never more than [`KeyCache::MAX_ENTRIES`].
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cache_always_agrees_with_the_interner() {
        let mut cache = KeyCache::new();
        for key in ["host", "status", "request_time", "host", "status", "brand_new_key_xyzzy"] {
            assert_eq!(cache.get_or_intern(key), intern(key), "{key}");
        }
        assert_eq!(cache.len(), 4);
    }

    #[test]
    fn keys_in_the_same_order_are_hits_and_never_touch_the_interner() {
        let mut cache = KeyCache::new();
        let keys = ["keycache_seq_a", "keycache_seq_b", "keycache_seq_c"];
        let expected: Vec<Symbol> = keys.iter().map(|k| cache.get_or_intern(k)).collect();

        // `nextest` runs each test in its own process, so `len()` reflects only this test.
        let before = len();
        for _ in 0..3 {
            let again: Vec<Symbol> = keys.iter().map(|k| cache.get_or_intern(k)).collect();
            assert_eq!(again, expected);
        }
        assert_eq!(len(), before, "a repeat pass must not intern anything");
        assert_eq!(cache.len(), keys.len(), "and must not add entries");
    }

    #[test]
    fn a_skipped_or_reordered_key_is_found_by_the_wrapping_scan() {
        let mut cache = KeyCache::new();
        let a = cache.get_or_intern("keycache_wrap_a");
        let b = cache.get_or_intern("keycache_wrap_b");
        let c = cache.get_or_intern("keycache_wrap_c");

        let before = len();
        // `b` absent this event: `c` is one step past the cursor.
        assert_eq!(cache.get_or_intern("keycache_wrap_a"), a);
        assert_eq!(cache.get_or_intern("keycache_wrap_c"), c);
        // Fully reversed: every key is found by scanning, none re-interned.
        assert_eq!(cache.get_or_intern("keycache_wrap_c"), c);
        assert_eq!(cache.get_or_intern("keycache_wrap_b"), b);
        assert_eq!(cache.get_or_intern("keycache_wrap_a"), a);
        assert_eq!(len(), before);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn the_cache_stops_growing_at_its_cap_but_still_answers_correctly() {
        let mut cache = KeyCache::new();
        let keys: Vec<String> =
            (0..KeyCache::MAX_ENTRIES + 8).map(|i| format!("keycache_cap_{i}")).collect();
        for key in &keys {
            assert_eq!(cache.get_or_intern(key), intern(key));
        }
        assert_eq!(cache.len(), KeyCache::MAX_ENTRIES);
        // The uncached tail still resolves, and re-asking doesn't grow the cache either.
        for key in &keys[KeyCache::MAX_ENTRIES..] {
            assert_eq!(cache.get_or_intern(key), intern(key));
        }
        assert_eq!(cache.len(), KeyCache::MAX_ENTRIES);
    }

    #[test]
    fn an_over_long_key_is_interned_but_not_cached() {
        let mut cache = KeyCache::new();
        let long = "k".repeat(KeyCache::MAX_KEY_LEN + 1);
        assert_eq!(cache.get_or_intern(&long), intern(&long));
        assert!(cache.is_empty());
        let short = "k".repeat(KeyCache::MAX_KEY_LEN);
        assert_eq!(cache.get_or_intern(&short), intern(&short));
        assert_eq!(cache.len(), 1);
    }
}
