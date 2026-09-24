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

/// Resolve a `Symbol` back to its string. Panics if the symbol didn't come from [`intern`], which
/// is a bug, not bad input: only this module creates symbols.
pub fn resolve(sym: Symbol) -> &'static str {
    interner().resolve(&sym)
}

/// Look up a string's `Symbol` without interning it; a miss adds nothing to the table.
///
/// Interning is monotonic and process-global, so `None` means the string can't be a key of
/// anything already built (an `AttrMap` entry, a `SeriesKey`). Use it where a miss is likely and
/// interning would be wasted, e.g. `AttrMap::get`/`remove`.
pub fn lookup(s: &str) -> Option<Symbol> {
    interner().get(s)
}

/// Count of distinct strings interned so far, process-wide. Never decreases (`ThreadedRodeo`
/// never evicts, `docs/design/memory.md` §4); `internal` reports it as
/// `logit.process.interner.strings`.
pub fn len() -> usize {
    interner().len()
}

/// A per-component `&str -> Symbol` memo in front of [`intern`], for keys a parser takes from its
/// input rather than its config.
///
/// **Why.** A hit on [`intern`] allocates nothing but still costs a hash, a `DashMap` shard lock
/// (an atomic on a cache line every task shares), and a probe. A parser like `json` pays that per
/// key per event; it was the largest remaining parse cost in the `json-parse` load test
/// (`docs/design/performance.md`). A real log stream's keys are few and repeat in the same order
/// every line, so a cache owned by the parser's task makes each one a `memcmp`.
///
/// **Shape.** Entries are in first-seen order with a cursor, not a hash map: on the steady path
/// the next key is the entry at the cursor, a length compare and a `memcmp`. An absent optional
/// field or a reordering producer is found by scanning forward from the cursor with wrap-around.
/// A name at two nesting depths (`id`, `user.id`) is one entry. Only a first sighting reaches
/// [`intern`].
///
/// **Bounds.** At most [`KeyCache::MAX_ENTRIES`] keys, none longer than
/// [`KeyCache::MAX_KEY_LEN`]: this is a second copy per node of keys the interner already keeps
/// forever, so data in key position mustn't grow it without bound. Past the cap a miss still
/// returns the right `Symbol`, via [`intern`] after a bounded scan. Never evicts.
///
/// **Contract.** `cache.get_or_intern(s) == intern(s)` for every `s`; the cache holds no `Symbol`
/// the global table doesn't. Single-owner (`&mut self`), so nothing to synchronize. `Clone`
/// because decoders are cloned per accepted connection; a clone starts warm, which is harmless.
#[derive(Debug, Default, Clone)]
pub struct KeyCache {
    entries: Vec<(Box<str>, Symbol)>,
    cursor: usize,
}

impl KeyCache {
    /// Most distinct keys one cache holds: a wide flat log object (pino's default is 28 keys)
    /// plus nested objects' keys, with room to spare.
    pub const MAX_ENTRIES: usize = 64;
    /// Longest key worth caching; a longer one is data in key position and won't repeat.
    pub const MAX_KEY_LEN: usize = 128;

    pub fn new() -> Self {
        Self { entries: Vec::with_capacity(Self::MAX_ENTRIES), cursor: 0 }
    }

    /// The `Symbol` [`intern`] would return for `s`, from the cache when seen before, otherwise
    /// interned and cached if there's room.
    #[inline]
    pub fn get_or_intern(&mut self, s: &str) -> Symbol {
        let len = self.entries.len();
        if len > 0 {
            // Steady state: the key is at the cursor. Wrapping lets the next event's first key
            // follow this event's last without a scan.
            let start = if self.cursor < len { self.cursor } else { 0 };
            if self.entries[start].0.as_ref() == s {
                self.cursor = start + 1;
                return self.entries[start].1;
            }
            // Resync: scan forward from the cursor, wrapping.
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

    /// Distinct keys cached, at most [`KeyCache::MAX_ENTRIES`].
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
