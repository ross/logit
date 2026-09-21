//! **Arm C -- the clone path.** Why `AttrMap::clone` costs what W2 measured, and what a production
//! fix would look like.
//!
//! W2 put `AttrMap::clone` at ~190 ns for eight *inline* `Value::I64` entries -- 392 bytes, no
//! allocation -- and at ~16 ns per entry past that. A 392-byte copy is single-digit nanoseconds
//! (`benches/size_vs_alloc.rs`'s `move_value` group measures 0.022 ns/byte), so something other
//! than the copy is being paid. This module is the search for it, with four candidate replacements
//! measured against the shipped one.
//!
//! **What the code actually does.** `AttrMap` derives `Clone`, so it is `SmallVec::clone`, which in
//! smallvec 1.16 is `SmallVec::from(self.as_slice())` -> `slice.iter().cloned().collect()` ->
//! `SmallVec::new()` + `Extend::extend`. Two consequences follow from reading that, both
//! measurable here:
//!
//! 1. **The copy is an element-at-a-time loop, not a `memcpy`.** `extend` writes through
//!    `SetLenOnDrop`, which stores the new length to memory on every iteration; each element goes
//!    through `Value`'s derived `Clone`, a ten-variant match that LLVM outlines rather than
//!    inlines (four of the variants -- `Bytes`, `Str`, `Array`, `Map` -- carry real work). The
//!    per-element store to the length field and the outlined call together stop the loop
//!    vectorizing. smallvec's `Copy` specialization, which would `memcpy`, is behind the nightly
//!    `specialization` feature and is not enabled -- and `(Symbol, Value)` is not `Copy` anyway.
//! 2. **A spilled clone over-allocates.** `extend` calls `reserve(9)`, and smallvec's `reserve`
//!    rounds `len + additional` up to the **next power of two** (`try_reserve`, verified against
//!    smallvec 1.16's source). So cloning a 9- or 12-entry map allocates 16 × 48 = **768 bytes**
//!    where 432 or 576 would do, and a 17- or 30-entry map allocates 32 × 48 = **1536**.
//!    `SmallVec::with_capacity` goes through `reserve_exact` and does not.
//!
//! **The candidates**, each a free function over the same mirror type so the shipped `AttrMap` is
//! never changed by this PR:
//!
//! - [`MirrorMap::clone_baseline`] -- what ships today, through the same `SmallVec` path.
//! - [`MirrorMap::clone_exact_loop`] -- `with_capacity(len)` (so: exactly sized) plus a tight write
//!   loop through a raw pointer that sets the length once, at the end. This is candidate (i).
//! - [`MirrorMap::clone_scalar_branch`] -- the same loop, with `Value`'s clone reduced to **one**
//!   branch for the scalar variants and a call for the rest. Candidate (iii).
//! - [`MirrorMap::clone_detect_pod`] -- scans the entry slice for a non-scalar value and, finding
//!   none, copies the whole slice with one `copy_nonoverlapping`. Candidate (ii), in its
//!   no-extra-state form: the question it answers is whether the detection is cheap enough to pay
//!   for itself on a mix that fails it.
//! - [`PodFlagMap`] -- candidate (ii) in its cheap-detection form: a per-map `all_scalar` flag
//!   maintained by `insert`, so the clone branches once on a bool it already has. The cost moves to
//!   the build, which is why it is a separate type rather than another method.
//!
//! **The measurement shape matters as much as the candidates, and W2's numbers are distorted by
//! it.** divan stores each iteration's return value into a pre-allocated `DeferSlot` when the
//! output needs dropping (`divan-0.1.21`'s `benchmark/mod.rs`: the `size_of::<O>() == 0 ||
//! !needs_drop::<O>()` branch is the cheap one, everything else goes through `defer_store`). A
//! `SmallVec<[(Symbol, Value); 8]>` is ~400 bytes and needs dropping, so a bench that *returns* one
//! writes 400 bytes into a fresh slot of a multi-megabyte buffer on every iteration -- streaming
//! stores the benched code never performs. `benches/attr_arms.rs` measures every candidate in the
//! **consumed** shape (clone, `black_box` the reference, drop inside the timed region, return `()`)
//! and keeps one *returned* bench per candidate purely to show the gap. The same distortion sits
//! under W2's §3 build comparison, where today's arm returns a 400-byte `AttrMap` and the arm-P
//! mirror returns a 24-byte `Vec` -- that comparison is not like-for-like.
//!
//! **Simplifications to hold against these numbers.** The mirror uses the *real*
//! `logit_core::Value`, so no value-side layout drift is possible; only the map wrapper is local,
//! and it has the same `SmallVec<[(Symbol, Value); 8]>` backing, the same sorted invariant and the
//! same `insert_sym` body as `AttrMap`. What it does not mirror is `AttrMap`'s visibility from
//! `logit-core`: these clones are all in the same codegen unit as their callers, so LLVM may inline
//! them where a cross-crate `AttrMap::clone` (not `#[inline]`, no LTO in `cargo bench`'s profile)
//! would not. That flatters every candidate equally, including the baseline, which is what keeps
//! the *ratios* usable and the absolute numbers merely indicative.

use logit_core::interner::Symbol;
use logit_core::Value;
use smallvec::SmallVec;

/// The same backing store `AttrMap` uses: eight inline `(Symbol, Value)` entries, 48 bytes each.
pub type Entries = SmallVec<[(Symbol, Value); 8]>;

/// Whether `value` is one of the variants that owns nothing on the heap -- so a bitwise copy of it
/// is a complete, independent clone and dropping either copy is a no-op.
///
/// Exhaustive on purpose (no `_` arm): a new `Value` variant must be classified here deliberately,
/// because getting this wrong makes [`clone_scalar_value`] unsound rather than slow.
#[inline]
pub fn is_scalar(value: &Value) -> bool {
    match value {
        Value::Null
        | Value::Bool(_)
        | Value::I64(_)
        | Value::U64(_)
        | Value::F64(_)
        | Value::Timestamp(_) => true,
        Value::Bytes(_) | Value::Str(_) | Value::Array(_) | Value::Map(_) => false,
    }
}

/// Candidate (iii): one branch for every scalar variant, a call for the four that own something.
#[inline]
pub fn clone_scalar_value(value: &Value) -> Value {
    if is_scalar(value) {
        // SAFETY: `is_scalar` is exhaustive over `Value` and true only for variants that own no
        // heap allocation and have no `Drop` glue, so the bits of `*value` are a complete,
        // independent value: the copy and the original can both be used and both dropped.
        unsafe { std::ptr::read(value) }
    } else {
        value.clone()
    }
}

/// A local stand-in for `AttrMap` -- same backing store, same sorted-by-`Symbol` invariant, same
/// `insert` semantics (last write wins on a repeated key) -- so the clone strategies below can be
/// compared without touching `logit-core`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MirrorMap(Entries);

impl MirrorMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// `AttrMap::insert_sym`'s body, verbatim.
    pub fn insert_sym(&mut self, key: Symbol, value: Value) {
        match self.0.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => self.0[i].1 = value,
            Err(i) => self.0.insert(i, (key, value)),
        }
    }

    /// Builds from a parser's arrival-order scratch the way every producer does today.
    pub fn from_scratch(scratch: &[(Symbol, Value)]) -> Self {
        let mut map = Self::new();
        for (k, v) in scratch {
            map.insert_sym(*k, v.clone());
        }
        map
    }

    /// Builds from entries already sorted and deduplicated -- arm **P**'s bulk build, for the arms
    /// that want a map without paying the O(k²) build first.
    pub fn from_sorted(entries: Vec<(Symbol, Value)>) -> Self {
        debug_assert!(entries.windows(2).all(|w| w[0].0 < w[1].0), "sorted and deduplicated");
        Self(SmallVec::from_vec(entries))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Capacity, for the tests that pin how many bytes a clone asks for.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn as_slice(&self) -> &[(Symbol, Value)] {
        &self.0
    }

    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &Value)> {
        self.0.iter().map(|(k, v)| (*k, v))
    }

    /// **What shipped until `sizing/w3c`**: `SmallVec`'s own clone, which is
    /// `iter().cloned().collect()` and sizes a spilled copy to the next power of two. The real
    /// `AttrMap` now reserves exactly first; this mirror keeps the derived behaviour measurable.
    pub fn clone_baseline(&self) -> Self {
        Self(self.0.clone())
    }

    /// **Candidate (i)**: exactly-sized, length set once.
    pub fn clone_exact_loop(&self) -> Self {
        let mut out: Entries = SmallVec::with_capacity(self.0.len());
        let dst = out.as_mut_ptr();
        for (i, entry) in self.0.iter().enumerate() {
            // SAFETY: `out` has capacity for `self.0.len()` entries and `i` is in range; the slot
            // is uninitialized, so `write` (not an assignment) is correct and nothing is dropped.
            // The length is still 0, so a panic in `Value::clone` leaks the entries written so far
            // rather than dropping uninitialized memory -- a leak, never unsoundness.
            unsafe { dst.add(i).write((entry.0, entry.1.clone())) };
        }
        // SAFETY: every one of the `len` slots was written by the loop above.
        unsafe { out.set_len(self.0.len()) };
        Self(out)
    }

    /// **Candidate (iii)**: candidate (i)'s loop with [`clone_scalar_value`] in place of `Value`'s
    /// derived clone.
    pub fn clone_scalar_branch(&self) -> Self {
        let mut out: Entries = SmallVec::with_capacity(self.0.len());
        let dst = out.as_mut_ptr();
        for (i, entry) in self.0.iter().enumerate() {
            // SAFETY: as `clone_exact_loop`.
            unsafe { dst.add(i).write((entry.0, clone_scalar_value(&entry.1))) };
        }
        // SAFETY: every slot was written.
        unsafe { out.set_len(self.0.len()) };
        Self(out)
    }

    /// **Candidate (ii), detected**: if every value is a scalar, one `copy_nonoverlapping` over the
    /// whole entry slice; otherwise [`MirrorMap::clone_scalar_branch`]. The scan is `len` branches
    /// with no memory traffic beyond the discriminants the copy would read anyway.
    pub fn clone_detect_pod(&self) -> Self {
        if !self.0.iter().all(|(_, v)| is_scalar(v)) {
            return self.clone_scalar_branch();
        }
        let len = self.0.len();
        let mut out: Entries = SmallVec::with_capacity(len);
        // SAFETY: every value is scalar (just checked), so the entry slice owns nothing on the
        // heap and a bitwise copy of it is a complete, independent set of entries; `out` has
        // capacity for `len` of them, and the two allocations are distinct so they cannot overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(self.0.as_ptr(), out.as_mut_ptr(), len);
            out.set_len(len);
        }
        Self(out)
    }
}

/// **Candidate (ii), flagged**: the same map carrying a bit that says whether every value is a
/// scalar, maintained by `insert` so the clone never has to look.
///
/// The bit costs one branch per inserted value on the build path and one `bool` of footprint
/// (which `AttrMap`'s `SmallVec` has spare padding for -- `size_of` is pinned in
/// `tests/attr_arms.rs`). It is only ever *cleared*, never re-established: removing the last
/// non-scalar value leaves the map falsely marked mixed, which costs a slower clone and nothing
/// else. That asymmetry is deliberate and is what keeps `remove` free of a rescan.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PodFlagMap {
    entries: Entries,
    all_scalar: bool,
}

impl PodFlagMap {
    pub fn new() -> Self {
        Self { entries: Entries::new(), all_scalar: true }
    }

    pub fn insert_sym(&mut self, key: Symbol, value: Value) {
        self.all_scalar &= is_scalar(&value);
        match self.entries.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => self.entries[i].1 = value,
            Err(i) => self.entries.insert(i, (key, value)),
        }
    }

    pub fn from_scratch(scratch: &[(Symbol, Value)]) -> Self {
        let mut map = Self::new();
        for (k, v) in scratch {
            map.insert_sym(*k, v.clone());
        }
        map
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn all_scalar(&self) -> bool {
        self.all_scalar
    }

    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &Value)> {
        self.entries.iter().map(|(k, v)| (*k, v))
    }

    /// One branch on a flag the map already holds, then either a `memcpy` or the per-element loop.
    pub fn clone_flagged(&self) -> Self {
        if !self.all_scalar {
            let mut out: Entries = SmallVec::with_capacity(self.entries.len());
            let dst = out.as_mut_ptr();
            for (i, entry) in self.entries.iter().enumerate() {
                // SAFETY: as `MirrorMap::clone_exact_loop`.
                unsafe { dst.add(i).write((entry.0, clone_scalar_value(&entry.1))) };
            }
            // SAFETY: every slot was written.
            unsafe { out.set_len(self.entries.len()) };
            return Self { entries: out, all_scalar: false };
        }
        let len = self.entries.len();
        let mut out: Entries = SmallVec::with_capacity(len);
        // SAFETY: `all_scalar` is set by `insert_sym` for every value the map holds and is only
        // ever cleared, so it being true means no entry owns heap memory and the slice can be
        // copied bitwise; `out` has capacity for `len` entries in a distinct allocation.
        unsafe {
            std::ptr::copy_nonoverlapping(self.entries.as_ptr(), out.as_mut_ptr(), len);
            out.set_len(len);
        }
        Self { entries: out, all_scalar: true }
    }
}
