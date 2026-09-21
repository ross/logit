//! `AttrMap`: the small, sorted, interned-key map that backs event attributes, plus the
//! resource-attributes-overridden-by-event-attributes merge-join ([`merged`]) every codec and sink
//! that renders both onto one wire representation shares.
//!
//! Most events carry well under a dozen attributes, so a sorted `SmallVec` beats a `HashMap` on
//! both lookup and iteration at this size, and gives deterministic ordering for free -- which the
//! wire format's dictionary encoding and reproducible tests both depend on. See
//! `docs/design/data-model.md`.

use crate::interner::{intern, lookup, Symbol};
use crate::value::Value;
use crate::{Event, Resource};
use lasso::Key;
use smallvec::SmallVec;
use std::cmp::Ordering;

const INLINE_CAPACITY: usize = 8;

#[derive(Debug, Default, PartialEq)]
pub struct AttrMap(SmallVec<[(Symbol, Value); INLINE_CAPACITY]>);

/// Hand-written rather than derived, for one reason: a derived `Clone` is `SmallVec::clone`, which
/// collects through `extend`, which sizes a spilled copy with `reserve` -- and smallvec's `reserve`
/// rounds up to the next power of two. So cloning a 12-attribute map asked for 16 slots (768 B
/// where 576 hold it) and a 17-attribute one for 32 (1536 B for 816), on every fan-out branch that
/// copies an event. `reserve_exact` first makes `extend`'s own `reserve` a no-op: the same single
/// allocation, sized to `len`. A map that fits inline reserves nothing either way. See
/// `docs/design/memory.md` §1 and `docs/plans/event-sizing.md`.
impl Clone for AttrMap {
    fn clone(&self) -> Self {
        let mut entries = SmallVec::new();
        entries.reserve_exact(self.0.len());
        entries.extend(self.0.iter().cloned());
        Self(entries)
    }
}

impl AttrMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty map with room for `capacity` entries and no further allocation. `capacity` at or
    /// under the inline capacity allocates nothing at all
    /// (`docs/plans/event-sizing.md`'s invariant I1); above it, exactly one buffer of exactly
    /// `capacity` entries -- see [`AttrMap::reserve_exact`] for why exactly.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(SmallVec::with_capacity(capacity))
    }

    /// Entries this map can hold before it has to allocate again. Reported for the inline case
    /// too, where it is the inline capacity and no heap buffer exists.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Room for `additional` more entries, **rounded up to the next power of two** by smallvec's
    /// amortized growth policy. Only for a caller that will keep inserting past `additional`
    /// afterwards and wants the growth chain amortized; every producer that knows its final width
    /// wants [`AttrMap::reserve_exact`] instead, and nothing in the tree calls this today.
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Room for exactly `additional` more entries, in exactly one allocation of exactly that size.
    ///
    /// **This is the default, and `reserve` is the exception.** `docs/plans/event-sizing.md`'s
    /// invariant I2 asks for "at most one, exactly-sized attribute allocation per building stage",
    /// and smallvec's `reserve` rounds to the next power of two: a 12-attribute log would take a
    /// 16-entry, 768-byte buffer where 12 entries is 576. Rounding is only worth its bytes when
    /// more inserts are coming, and a producer reaching for a bulk build has by definition just
    /// counted everything it is about to insert. (jemalloc's size classes blunt but do not erase
    /// the difference -- 576 lands in the 640 class against 768's own, still 128 bytes per event.)
    pub fn reserve_exact(&mut self, additional: usize) {
        self.0.reserve_exact(additional);
    }

    /// Inserts every pair in `pairs`, in **at most one** allocation and **one** sort, with exactly
    /// [`AttrMap::insert_sym`]'s semantics: the map ends up sorted by [`Symbol`], and on a repeated
    /// key the last write wins -- whether the earlier write is another entry of `pairs` or an entry
    /// the map already held.
    ///
    /// This is the bulk build every decoder and parser that knows its width should use. A
    /// `k`-attribute map built one [`AttrMap::insert_sym`] at a time moves O(k²) bytes, because
    /// each insert shifts the entries after its sorted position; appending and sorting once moves
    /// O(k log k) and, when the heap is needed at all, takes exactly one exactly-sized allocation
    /// instead of a growth chain. `pairs` need not be sorted, and generally is not: [`Symbol`]
    /// order is *local interning* order, so nothing off a wire or out of a parser scratch arrives
    /// sorted here.
    ///
    /// **A map that ends up within the inline capacity still allocates nothing**, however long
    /// `pairs` claims to be -- the reservation happens at the moment of a genuine spill, not up
    /// front. `pairs` is an upper bound on the final width, not a count of new keys: entries that
    /// overwrite (each other, or what the map already held) cost no capacity at all.
    ///
    /// `pairs` must know its length ([`ExactSizeIterator`]) -- that length is what gets reserved
    /// if a spill happens. A producer that cannot hand over an iterator (a decoder reading
    /// fallibly from a byte stream, say) uses [`AttrMap::bulk_insert`] directly, which is what
    /// this is written over.
    pub fn extend_unsorted<I>(&mut self, pairs: I)
    where
        I: IntoIterator<Item = (Symbol, Value)>,
        I::IntoIter: ExactSizeIterator,
    {
        let pairs = pairs.into_iter();
        let mut bulk = self.bulk_insert(pairs.len());
        for (key, value) in pairs {
            bulk.push(key, value);
        }
    }

    /// Opens a bulk build of up to `additional` more entries: takes them in any order through
    /// [`BulkInsert::push`], reserves once if and when one of those pushes actually needs the
    /// heap, and restores the sorted invariant when the returned guard is dropped.
    /// [`AttrMap::extend_unsorted`] is the one-call form and the one to prefer; this exists for a
    /// producer whose push loop can fail partway (`?` out of a decode) or whose pairs don't come
    /// from a single iterator.
    ///
    /// `additional` is an upper bound, not a promise, and nothing is reserved on the strength of
    /// it until a push finds the map full: pushing fewer, or pushing keys that overwrite rather
    /// than append, costs nothing, and pushing more falls back to ordinary amortized growth. The
    /// guard borrows the map, so no caller can observe the unsorted intermediate state, and it
    /// finishes on `Drop`, so an early return leaves a valid map behind.
    pub fn bulk_insert(&mut self, additional: usize) -> BulkInsert<'_> {
        let base = self.0.len();
        BulkInsert { map: self, base, seen: 0, remaining: additional }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        // `lookup`, not `intern`: a key that was never interned can't be in this map either
        // (interning is monotonic and global), so a miss here returns `None` without growing the
        // process-wide interner table for an attribute this event doesn't carry. See
        // `docs/design/memory.md` §4.
        let key = lookup(key)?;
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| &self.0[i].1)
    }

    /// Same as [`AttrMap::get`], but for a caller that already holds an interned [`Symbol`] --
    /// skips both the `lookup` hash probe and the `resolve` such a caller would otherwise need to
    /// reconstruct the `&str`. The [`AttrMap::insert_sym`] reasoning applied to the read side:
    /// a matcher that interns its configured keys once, at construction, then probes those same
    /// `Symbol`s on every event pays a plain `binary_search_by_key` instead of a hash + resolve
    /// round trip. The interner-growth guarantee `get` documents is trivially preserved here --
    /// the `Symbol` already exists, so there is nothing left to intern.
    pub fn get_sym(&self, key: Symbol) -> Option<&Value> {
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| &self.0[i].1)
    }

    pub fn insert(&mut self, key: &str, value: impl Into<Value>) {
        let key = intern(key);
        self.insert_sym(key, value);
    }

    /// Same as [`AttrMap::insert`], but for a caller that already holds an interned [`Symbol`] --
    /// skips the interner lookup `insert` would otherwise redo on every call. `logit-transforms`'
    /// `set` transform is the first caller: it interns its configured keys once, at construction,
    /// then inserts the same `Symbol`s into every event's/resource's map on the per-event hot
    /// path.
    pub fn insert_sym(&mut self, key: Symbol, value: impl Into<Value>) {
        match self.0.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => self.0[i].1 = value.into(),
            Err(i) => self.0.insert(i, (key, value.into())),
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        // Same reasoning as `get`: a key never interned was never inserted, so it can't be present.
        let key = lookup(key)?;
        self.remove_sym(key)
    }

    /// Same as [`AttrMap::remove`], but for a caller that already holds the [`Symbol`] -- the
    /// [`AttrMap::get_sym`] reasoning applied to removal. The first callers are the `remove` ->
    /// `insert` merge steps for a repeated key (`statsd_in`'s tags, `syslog_in`'s SD params),
    /// which resolve the key once through a `KeyCache` and then do both halves by `Symbol`.
    pub fn remove_sym(&mut self, key: Symbol) -> Option<Value> {
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| self.0.remove(i).1)
    }

    /// Empties the map, **keeping** whatever backing storage it already holds -- so a caller that
    /// clears and refills one map per event pays no allocation for the refill once the map has
    /// spilled. `logit-transforms`' `shape` is the first caller: it rewrites an event in place
    /// into a measurement event, replacing the observed attributes with its own small tag set
    /// (`docs/adr/shape-observer-component.md`), and reusing the map it is about to overwrite is
    /// the natural way to do that.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates in sorted-symbol order -- stable and deterministic, not insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &Value)> {
        self.0.iter().map(|(k, v)| (*k, v))
    }
}

/// An open bulk build of an [`AttrMap`], from [`AttrMap::bulk_insert`]. Pushed entries land in
/// push order at the tail of the map; the sort (and the merge with whatever the map already held)
/// happens once, on `Drop`.
///
/// See [`AttrMap::extend_unsorted`] for the contract this implements and why it beats a loop of
/// [`AttrMap::insert_sym`].
pub struct BulkInsert<'a> {
    map: &'a mut AttrMap,
    /// Length of the map when the build opened: entries below this are the already-sorted ones
    /// the incoming entries have to merge with (and can overwrite).
    base: usize,
    /// A 128-bit membership filter over the keys pushed into the **tail** so far,
    /// `1 << (symbol % 128)`.
    ///
    /// Duplicate keys have to be resolved **before** the sort, because the sort is
    /// `sort_unstable_by_key` and so tells us nothing about which of two equal keys was pushed
    /// first -- and the sort has to be unstable because `slice::sort` allocates a scratch buffer
    /// above roughly 20 elements, which a 30-field access log would silently pay for on every
    /// event (`crates/logit-bench/tests/allocations.rs`'s `attr_map_bulk_build_is_one_exact_allocation`
    /// pins that it doesn't). Resolving at push time means a membership test per push; a set that
    /// allocates would defeat the point, and a linear scan of the tail per push would reinstate
    /// the O(k²) this exists to remove. So: one `u128`, checked in a couple of instructions,
    /// with a false positive costing a scan that finds nothing and a true positive costing a scan
    /// that overwrites in place. Duplicate keys are rare in real telemetry (a repeated field in
    /// one JSON object or logfmt line), so the scan is rare, and the filter is exact for any map
    /// whose keys happen to fall in distinct residues.
    ///
    /// It covers only the tail because a key the *head* already holds never reaches the tail --
    /// [`BulkInsert::push`] overwrites it where it sits.
    seen: u128,
    /// Entries the caller still says are coming, counting the one being pushed: `additional` at
    /// open, decremented by every `push` whatever that push does. `len() + remaining` is therefore
    /// a non-increasing **upper bound** on the map's final length, which is exactly the number
    /// [`BulkInsert::push`] reserves at the moment it first needs the heap.
    remaining: usize,
}

impl BulkInsert<'_> {
    /// Adds one entry. Order doesn't matter; a key already pushed in this build, or already in the
    /// map before it opened, is overwritten -- last write wins, exactly as with repeated
    /// [`AttrMap::insert_sym`] calls.
    ///
    /// Three things happen here rather than in `Drop`, and all three are load-bearing.
    ///
    /// **A key the map already held is overwritten in place**, before the tail is consulted. That
    /// is what keeps the tail free of keys the head has: without it, an incoming run that merely
    /// *re-states* attributes an event already carries would grow the map to `base + k` entries
    /// and only collapse back at the end, spilling on the way for a result that fits inline. A
    /// re-parse onto an already-parsed event is a real shape, not a contrived one.
    ///
    /// **The reservation is lazy.** `bulk_insert` reserves nothing; the heap is asked for only
    /// when an append actually finds the map full, and then for `remaining` entries -- the whole
    /// rest of the declared run in one `reserve_exact`, sized to `len() + remaining`. So a build
    /// that ends up inside the inline capacity allocates nothing no matter what the caller
    /// declared (`docs/plans/event-sizing.md`'s invariant I1), and one that genuinely spills still
    /// takes a single exactly-sized buffer (I2). An eager `reserve_exact(additional)` broke I1 for
    /// every caller whose `additional` is an upper bound rather than a count -- `regex`'s named
    /// groups, a run that mostly overwrites -- which is what a review of the first version of this
    /// found.
    ///
    /// **A hint that was too low just falls back to smallvec's own growth.** Once `remaining` hits
    /// zero the reservation is skipped entirely and `push` grows the map the ordinary amortized
    /// way, because the alternative -- `reserve_exact(1)` per entry -- is a `realloc` per entry.
    /// `bulk_insert(0)` (Lua, which cannot get a table's hash-part length out of the VM) is the
    /// deliberate case: it takes the sort and none of the sizing, which is exactly today's growth
    /// behaviour.
    pub fn push(&mut self, key: Symbol, value: impl Into<Value>) {
        let value = value.into();
        // Counting this entry: `map.len() + declared` is the run's upper-bound final length.
        let declared = self.remaining;
        self.remaining = declared.saturating_sub(1);

        // Already in the map before this build opened: overwrite where it sits. The head is
        // sorted and, being the map's own entries, holds each key at most once.
        if self.base > 0 {
            if let Ok(at) = self.map.0[..self.base].binary_search_by_key(&key, |(k, _)| *k) {
                self.map.0[at].1 = value;
                return;
            }
        }

        // Already pushed into the tail by this build: same rule, one filter test away.
        let bit = 1u128 << (key.into_usize() & 127);
        if self.seen & bit != 0 {
            if let Some(slot) = self.map.0[self.base..].iter_mut().find(|(k, _)| *k == key) {
                slot.1 = value;
                return;
            }
        }
        self.seen |= bit;

        if declared > 0 && self.map.0.len() == self.map.0.capacity() {
            // Exactly the rest of the declared run, this entry included: one buffer of
            // `len() + declared` entries, and no second one unless the hint was too low.
            self.map.0.reserve_exact(declared);
        }
        self.map.0.push((key, value));
    }
}

impl Drop for BulkInsert<'_> {
    fn drop(&mut self) {
        if self.map.0.len() == self.base {
            return; // nothing appended -- either nothing was pushed, or it all overwrote
        }
        // Every key is distinct: the head held each of its own at most once, [`BulkInsert::push`]
        // kept the tail free of both head keys and tail repeats, so there are no equal elements
        // for an unstable sort to reorder and its result is fully determined. It allocates
        // nothing, which `slice::sort` would not promise at these widths.
        self.map.0.sort_unstable_by_key(|(k, _)| *k);
    }
}

impl FromIterator<(&'static str, Value)> for AttrMap {
    fn from_iter<T: IntoIterator<Item = (&'static str, Value)>>(iter: T) -> Self {
        let mut map = AttrMap::new();
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

/// Iterates `resource.attributes` merged with `event.attributes`, in sorted-[`Symbol`] order, the
/// event's value winning on an equal key. Both maps already iterate in that order
/// ([`AttrMap::iter`]), so walking them in lockstep and preferring the event's value on a tie
/// produces exactly the same sequence a clone-and-insert would -- without copying an `AttrMap` per
/// event, and without the `resolve` -> `intern` round trip re-inserting every key would cost.
///
/// Lives here rather than in one sink: `influxdb_out`'s tags, `statsd_out`'s tags
/// (`crates/logit-outputs/src/attrs.rs` re-exports this) and the Prometheus codec's labels
/// (`crates/logit-proto/src/prometheus/`) all need the identical merge with a different emission
/// format on top, and `logit-proto` cannot reach into `logit-outputs` (the dependency runs the other
/// way).
pub fn merged<'a>(
    resource: &'a Resource,
    event: &'a Event,
) -> impl Iterator<Item = (Symbol, &'a Value)> {
    let mut resource_attrs = resource.attributes.iter().peekable();
    let mut event_attrs = event.attributes.iter().peekable();
    std::iter::from_fn(move || {
        match (resource_attrs.peek().map(|(k, _)| *k), event_attrs.peek().map(|(k, _)| *k)) {
            (Some(r), Some(e)) => match r.cmp(&e) {
                Ordering::Less => resource_attrs.next(),
                Ordering::Greater => event_attrs.next(),
                // Same key on both: the event's value wins, and the resource's is discarded.
                Ordering::Equal => {
                    resource_attrs.next();
                    event_attrs.next()
                }
            },
            (Some(_), None) => resource_attrs.next(),
            (None, Some(_)) => event_attrs.next(),
            (None, None) => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interner;

    /// Distinct symbols to build maps out of, interned once. Deliberately *not* in alphabetical
    /// order of key string: `Symbol` order is interning order, and the whole point of the bulk
    /// build is that it can't assume its input arrives sorted.
    fn probe_symbols(n: usize) -> Vec<Symbol> {
        (0..n).map(|i| intern(&format!("attrmap.bulk.probe.k{i:03}"))).collect()
    }

    /// The reference implementation the bulk build has to match, entry for entry.
    fn by_repeated_insert(base: &[(Symbol, i64)], pairs: &[(Symbol, i64)]) -> AttrMap {
        let mut map = AttrMap::new();
        for (k, v) in base.iter().chain(pairs) {
            map.insert_sym(*k, Value::I64(*v));
        }
        map
    }

    fn by_bulk(base: &[(Symbol, i64)], pairs: &[(Symbol, i64)]) -> AttrMap {
        let mut map = AttrMap::new();
        for (k, v) in base {
            map.insert_sym(*k, Value::I64(*v));
        }
        map.extend_unsorted(pairs.iter().map(|(k, v)| (*k, Value::I64(*v))));
        map
    }

    #[test]
    fn a_clone_is_sized_to_its_length_not_the_next_power_of_two() {
        for k in [1, INLINE_CAPACITY, 9, 12, 17, 30] {
            let syms = probe_symbols(k);
            let mut map = AttrMap::new();
            map.extend_unsorted(syms.iter().enumerate().map(|(i, s)| (*s, Value::I64(i as i64))));
            let cloned = map.clone();
            assert_eq!(cloned, map);
            assert_eq!(cloned.capacity(), k.max(INLINE_CAPACITY), "clone of {k} entries");
        }
    }

    #[test]
    fn with_capacity_within_the_inline_capacity_allocates_nothing() {
        // Not an allocation count -- that lives in `crates/logit-bench/tests/allocations.rs`. What
        // is checkable here is that the capacity is still the inline one, which is the same fact.
        assert_eq!(AttrMap::with_capacity(0).capacity(), INLINE_CAPACITY);
        assert_eq!(AttrMap::with_capacity(INLINE_CAPACITY).capacity(), INLINE_CAPACITY);
    }

    #[test]
    fn reserve_exact_takes_exactly_what_was_asked_for() {
        let mut map = AttrMap::new();
        map.reserve_exact(12);
        assert_eq!(map.capacity(), 12, "`reserve_exact` must not round up to 16");

        let mut rounded = AttrMap::new();
        rounded.reserve(12);
        assert_eq!(rounded.capacity(), 16, "`reserve` rounds to the next power of two");
    }

    /// The regression a review of the first version of this found: an eager
    /// `reserve_exact(additional)` spilled a map that, built a key at a time, would have stayed
    /// inline -- because `additional` is an upper bound for several callers (`regex`'s named
    /// capture groups, any run that overwrites rather than appends), not a count of new keys.
    #[test]
    fn a_bulk_build_that_stays_inline_never_reserves() {
        let syms = probe_symbols(10);

        // A generous hint, one actual push, six entries already there: `regex` with a three-group
        // pattern of which one participated.
        let mut map = AttrMap::new();
        for (i, s) in syms.iter().take(6).enumerate() {
            map.insert_sym(*s, Value::I64(i as i64));
        }
        {
            let mut bulk = map.bulk_insert(3);
            bulk.push(syms[7], Value::I64(70));
        }
        assert_eq!(map.len(), 7);
        assert_eq!(map.capacity(), INLINE_CAPACITY, "an unused hint must not spill the map");

        // Every incoming key overwrites one already there, so the map does not grow at all.
        let mut map = AttrMap::new();
        for (i, s) in syms.iter().take(6).enumerate() {
            map.insert_sym(*s, Value::I64(i as i64));
        }
        map.extend_unsorted(vec![
            (syms[0], Value::I64(100)),
            (syms[3], Value::I64(103)),
            (syms[5], Value::I64(105)),
        ]);
        assert_eq!(map.len(), 6, "three overwrites, no new entries");
        assert_eq!(map.capacity(), INLINE_CAPACITY, "overwrites must not spill the map");
        assert_eq!(map.get_sym(syms[3]), Some(&Value::I64(103)));

        // Three genuinely new keys onto the same six: one exactly-sized buffer of nine.
        let mut map = AttrMap::new();
        for (i, s) in syms.iter().take(6).enumerate() {
            map.insert_sym(*s, Value::I64(i as i64));
        }
        map.extend_unsorted(vec![
            (syms[6], Value::I64(106)),
            (syms[7], Value::I64(107)),
            (syms[8], Value::I64(108)),
        ]);
        assert_eq!(map.len(), 9);
        assert_eq!(map.capacity(), 9, "one buffer, sized to the run");
    }

    /// A hint below the number of pushes is legal and just gives up the sizing: the map grows the
    /// ordinary amortized way from the point the hint runs out, rather than taking a
    /// `reserve_exact(1)` -- and so a `realloc` -- per entry. `bulk_insert(0)` is the deliberate
    /// case (Lua, which cannot get a table's hash-part length out of the VM).
    #[test]
    fn a_hint_that_was_too_low_falls_back_to_ordinary_growth() {
        let syms = probe_symbols(20);

        let mut map = AttrMap::new();
        {
            let mut bulk = map.bulk_insert(0);
            for (i, s) in syms.iter().enumerate() {
                bulk.push(*s, Value::I64(i as i64));
            }
        }
        assert_eq!(map.len(), 20);
        assert_eq!(map.capacity(), 32, "smallvec's own doubling, 8 -> 16 -> 32");

        // A hint that covers part of the run: the reservation it does make is still exact, and
        // the rest doubles from there.
        let mut map = AttrMap::new();
        {
            let mut bulk = map.bulk_insert(10);
            for (i, s) in syms.iter().enumerate() {
                bulk.push(*s, Value::I64(i as i64));
            }
        }
        assert_eq!(map.len(), 20);
        assert_eq!(
            map.capacity(),
            32,
            "reserved exactly 10 at the spill, then smallvec's own next-power-of-two growth"
        );
    }

    #[test]
    fn a_bulk_build_reserves_exactly_once() {
        let syms = probe_symbols(30);
        let mut map = AttrMap::new();
        map.extend_unsorted(syms.iter().enumerate().map(|(i, s)| (*s, Value::I64(i as i64))));
        assert_eq!(map.len(), 30);
        assert_eq!(map.capacity(), 30, "one exactly-sized buffer, no growth chain");
    }

    #[test]
    fn a_bulk_build_orders_by_symbol() {
        let syms = probe_symbols(5);
        let mut map = AttrMap::new();
        // Pushed back to front, so push order and sorted order disagree at every position.
        map.extend_unsorted(syms.iter().rev().enumerate().map(|(i, s)| (*s, Value::I64(i as i64))));
        let keys: Vec<Symbol> = map.iter().map(|(k, _)| k).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn a_bulk_build_takes_the_last_write_of_a_repeated_key() {
        let syms = probe_symbols(3);
        let mut map = AttrMap::new();
        map.extend_unsorted(vec![
            (syms[1], Value::I64(1)),
            (syms[0], Value::I64(2)),
            (syms[1], Value::I64(3)),
            (syms[2], Value::I64(4)),
            (syms[1], Value::I64(5)),
        ]);
        assert_eq!(map.len(), 3);
        assert_eq!(map.get_sym(syms[1]), Some(&Value::I64(5)));
    }

    #[test]
    fn a_bulk_build_overwrites_what_the_map_already_held() {
        let syms = probe_symbols(4);
        let mut map = AttrMap::new();
        map.insert_sym(syms[0], Value::I64(10));
        map.insert_sym(syms[3], Value::I64(13));

        map.extend_unsorted(vec![(syms[3], Value::I64(99)), (syms[1], Value::I64(11))]);

        assert_eq!(map.len(), 3);
        assert_eq!(map.get_sym(syms[0]), Some(&Value::I64(10)), "untouched key survives");
        assert_eq!(map.get_sym(syms[1]), Some(&Value::I64(11)));
        assert_eq!(map.get_sym(syms[3]), Some(&Value::I64(99)), "incoming wins");
    }

    #[test]
    fn a_bulk_build_of_nothing_leaves_the_map_alone() {
        let syms = probe_symbols(2);
        let mut map = AttrMap::new();
        map.insert_sym(syms[0], Value::I64(1));
        map.extend_unsorted(Vec::new());
        assert_eq!(map.len(), 1);
        assert_eq!(map.get_sym(syms[0]), Some(&Value::I64(1)));
    }

    /// The `bulk_insert` guard finishes on `Drop`, so a producer that gives up partway through its
    /// push loop (a decode that hits a truncated frame, say) still leaves a sorted, valid map.
    #[test]
    fn an_abandoned_bulk_insert_still_leaves_a_sorted_map() {
        let syms = probe_symbols(6);
        let mut map = AttrMap::new();
        map.insert_sym(syms[5], Value::I64(50));
        {
            let mut bulk = map.bulk_insert(4);
            bulk.push(syms[3], Value::I64(3));
            bulk.push(syms[1], Value::I64(1));
            // ... and then the producer stops early, dropping the guard.
        }
        assert_eq!(map.len(), 3);
        let keys: Vec<Symbol> = map.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![syms[1], syms[3], syms[5]]);
    }

    proptest::proptest! {
        /// The contract, stated once: a bulk build is indistinguishable from the sequence of
        /// `insert_sym` calls it replaces -- same entries, same values after duplicates resolve,
        /// same sorted-`Symbol` iteration order -- whatever the widths, the key collisions, or the
        /// overlap with what the map already held.
        #[test]
        fn a_bulk_build_matches_a_sequence_of_inserts(
            base_idx in proptest::collection::vec(0usize..40, 0..12),
            pair_idx in proptest::collection::vec(0usize..40, 0..70),
        ) {
            let syms = probe_symbols(40);
            let base: Vec<(Symbol, i64)> =
                base_idx.iter().enumerate().map(|(v, i)| (syms[*i], v as i64)).collect();
            let pairs: Vec<(Symbol, i64)> = pair_idx
                .iter()
                .enumerate()
                .map(|(v, i)| (syms[*i], 1000 + v as i64))
                .collect();

            let expected = by_repeated_insert(&base, &pairs);
            let actual = by_bulk(&base, &pairs);

            let expected_entries: Vec<(Symbol, &Value)> = expected.iter().collect();
            let actual_entries: Vec<(Symbol, &Value)> = actual.iter().collect();
            proptest::prop_assert_eq!(actual_entries, expected_entries);
        }

        /// The same equivalence through `bulk_insert` with an **arbitrary** hint -- far too low
        /// (including 0), far too high, or right. The hint is a sizing input and nothing else: it
        /// may not change a single entry or their order, at any width.
        #[test]
        fn a_bulk_build_matches_a_sequence_of_inserts_for_any_hint(
            base_idx in proptest::collection::vec(0usize..40, 0..12),
            pair_idx in proptest::collection::vec(0usize..40, 0..70),
            hint in 0usize..120,
        ) {
            let syms = probe_symbols(40);
            let base: Vec<(Symbol, i64)> =
                base_idx.iter().enumerate().map(|(v, i)| (syms[*i], v as i64)).collect();
            let pairs: Vec<(Symbol, i64)> = pair_idx
                .iter()
                .enumerate()
                .map(|(v, i)| (syms[*i], 1000 + v as i64))
                .collect();

            let expected = by_repeated_insert(&base, &pairs);

            let mut actual = AttrMap::new();
            for (k, v) in &base {
                actual.insert_sym(*k, Value::I64(*v));
            }
            {
                let mut bulk = actual.bulk_insert(hint);
                for (k, v) in &pairs {
                    bulk.push(*k, Value::I64(*v));
                }
            }

            let expected_entries: Vec<(Symbol, &Value)> = expected.iter().collect();
            let actual_entries: Vec<(Symbol, &Value)> = actual.iter().collect();
            proptest::prop_assert_eq!(actual_entries, expected_entries);
            // Whatever the hint, a map that fits inline must not have reached the heap.
            if actual.len() <= INLINE_CAPACITY {
                proptest::prop_assert_eq!(actual.capacity(), INLINE_CAPACITY);
            }
        }
    }

    #[test]
    fn get_present_key_returns_the_value() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        assert_eq!(map.get("host"), Some(&Value::from("web-1")));
    }

    #[test]
    fn get_absent_key_returns_none() {
        let map = AttrMap::new();
        assert_eq!(map.get("does-not-exist"), None);
    }

    #[test]
    fn remove_present_key_returns_the_value_and_removes_it() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        assert_eq!(map.remove("host"), Some(Value::from("web-1")));
        assert_eq!(map.get("host"), None);
    }

    #[test]
    fn remove_absent_key_returns_none() {
        let mut map = AttrMap::new();
        assert_eq!(map.remove("does-not-exist"), None);
    }

    /// Pins the fix this module exists for: `get`/`remove` on a key that was never interned must
    /// not intern it just to find out it's absent. `nextest` runs each test in its own process
    /// (see `docs/design/memory.md` §7), so `interner::len()` here reflects only this test.
    #[test]
    fn getting_an_absent_key_does_not_grow_the_interner() {
        let map = AttrMap::new();
        let never_interned_elsewhere = "attrmap_absent_key_probe_xyzzy";

        let before = interner::len();
        assert_eq!(map.get(never_interned_elsewhere), None);
        assert_eq!(interner::len(), before, "a missed `get` must not intern the key");

        let mut map = map;
        assert_eq!(map.remove(never_interned_elsewhere), None);
        assert_eq!(interner::len(), before, "a missed `remove` must not intern the key");
    }

    #[test]
    fn get_sym_present_key_returns_the_value() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        let sym = intern("host");
        assert_eq!(map.get_sym(sym), Some(&Value::from("web-1")));
    }

    #[test]
    fn get_sym_absent_key_returns_none() {
        let map = AttrMap::new();
        let sym = intern("attrmap_get_sym_absent_probe");
        assert_eq!(map.get_sym(sym), None);
    }

    #[test]
    fn get_sym_agrees_with_get_for_every_key() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        map.insert("env", "prod");
        map.insert("retries", 3_i64);

        for key in ["host", "env", "retries", "does-not-exist"] {
            let sym = intern(key);
            assert_eq!(map.get_sym(sym), map.get(key), "mismatch for key {key:?}");
        }
    }

    /// `get_sym` takes an already-interned `Symbol`, so there is nothing left for it to intern --
    /// interning the probe key happens explicitly, before the snapshot, so this only pins
    /// `get_sym` itself against growing the interner.
    #[test]
    fn get_sym_never_touches_the_interner() {
        let map = AttrMap::new();
        let sym = intern("attrmap_get_sym_no_growth_probe_xyzzy");

        let before = interner::len();
        assert_eq!(map.get_sym(sym), None);
        assert_eq!(interner::len(), before, "`get_sym` must not intern anything");
    }

    fn resource_with(attrs: &[(&str, &str)]) -> Resource {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Resource { attributes, ..Default::default() }
    }

    fn event_with(attrs: &[(&str, &str)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Event::empty(0, attributes)
    }

    fn collect(resource: &Resource, event: &Event) -> Vec<(String, String)> {
        merged(resource, event)
            .map(|(k, v)| (interner::resolve(k).to_string(), v.as_str().unwrap().to_string()))
            .collect()
    }

    #[test]
    fn an_event_attribute_overrides_a_resource_attribute_of_the_same_name() {
        let resource = resource_with(&[("env", "staging")]);
        let event = event_with(&[("env", "prod")]);
        assert_eq!(collect(&resource, &event), vec![("env".to_string(), "prod".to_string())]);
    }

    #[test]
    fn a_resource_attribute_with_no_event_counterpart_still_appears() {
        let resource = resource_with(&[("region", "us-east")]);
        let event = event_with(&[]);
        assert_eq!(collect(&resource, &event), vec![("region".to_string(), "us-east".to_string())]);
    }

    /// [`AttrMap::iter`] orders by `Symbol` (interning order), not alphabetically -- so the property
    /// worth pinning is that the merge-join matches what a single combined map would produce, not
    /// any particular string ordering.
    #[test]
    fn merged_order_matches_a_single_combined_attrmap() {
        let resource = resource_with(&[("zzz", "resource-only"), ("shared", "from-resource")]);
        let event = event_with(&[("aaa", "event-only"), ("shared", "from-event")]);

        let mut combined = AttrMap::new();
        combined.insert("zzz", "resource-only");
        combined.insert("shared", "from-event"); // event wins
        combined.insert("aaa", "event-only");

        let expected: Vec<(String, String)> = combined
            .iter()
            .map(|(k, v)| (interner::resolve(k).to_string(), v.as_str().unwrap().to_string()))
            .collect();
        assert_eq!(collect(&resource, &event), expected);
    }
}
