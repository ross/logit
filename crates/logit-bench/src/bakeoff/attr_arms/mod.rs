//! The attribute-sizing bake-off's **bench-only arms**: `docs/plans/event-sizing.md`'s W3b.
//!
//! Three arms, none of which changes a production type -- each is a local mirror measured against
//! the shipped `logit_core::AttrMap`, following the pattern
//! [`bakeoff::wire_mirror`](super::wire_mirror) set for the native-wire-format bake-off: a
//! plain-data local type stands in for the shipped one so an arm can be measured before anything is
//! committed to, and every simplification the mirror makes is stated in its own module doc rather
//! than discovered later from a number that doesn't reproduce.
//!
//! - [`clone_arms`] -- **arm C**, the clone path. Not one of the plan's original arms: W2 measured
//!   `AttrMap::clone` at ~190 ns for eight inline `Value::I64` entries, an order of magnitude more
//!   than a 392-byte copy costs, and W1 found the clone (not the build) is what a fan-out actually
//!   pays. Four candidate replacements, plus the demonstration that part of the shipped number is a
//!   measurement artifact.
//! - [`thin`] -- **arm E**, per-embedding capacity: a heap-only, exactly-sized map for
//!   `Value::Map`, `Scope` and `Resource`, against today's `Box<AttrMap>` and embedded
//!   `SmallVec<[_; 8]>`.
//! - [`keyset`] -- **arm K**, a shared `Arc<[Symbol]>` key-set plus a positional values vector,
//!   with a learned per-source cache, against arm P's bulk build. Carries a pre-registered kill
//!   criterion.
//!
//! [`shapes`] holds the inputs all three are measured on -- the survey's widths, its value mixes,
//! and a synthesized mixed-gateway key-set distribution.
//!
//! The timings live in `benches/attr_arms.rs` (real jemalloc, no counting wrapper); the allocation
//! counts and the equivalence gates -- every arm must produce the same sorted `(Symbol, Value)`
//! sequence the shipped `AttrMap` does, duplicate keys included -- live in `tests/attr_arms.rs`.
//! **No number from either belongs in a repository document**: this workstation's cores are
//! heterogeneous and `docs/design/performance.md` §0 says recorded figures come from the perf VM.

pub mod clone_arms;
pub mod keyset;
pub mod shapes;
pub mod thin;
