//! The attribute-sizing bake-off's **bench-only arms**: the evidence behind the key-set, thin
//! `Value::Map`, and clone-path entry in ADR `event-sizing-and-allocation-strategy`'s
//! "Alternatives considered". Arm letters are `docs/plans/event-sizing.md`'s "Bake-off arms",
//! except arm C: added during the bake-off, once the clone, not the build, proved to be what a
//! fan-out pays.
//!
//! No arm changes a production type. Each is a local mirror measured against the shipped
//! `logit_core::AttrMap`, the pattern [`bakeoff::wire_mirror`](super::wire_mirror) set for the
//! wire-format bake-off: a plain-data local type stands in for the shipped one so an arm can be
//! measured before anything is committed to, and each mirror's module doc states every
//! simplification it makes.
//!
//! - [`clone_arms`]: **arm C**, the clone path. Why `AttrMap::clone` costs far more than copying
//!   its bytes, four candidate replacements, and the divan measurement artifact that inflates the
//!   shipped number.
//! - [`thin`]: **arm E**, per-embedding capacity. A heap-only, exactly-sized map for `Value::Map`,
//!   `Scope`, and `Resource`, against the shipped `Box<AttrMap>` and embedded
//!   `SmallVec<[_; 8]>`.
//! - [`keyset`]: **arm K**, a shared `Arc<[Symbol]>` key-set plus a positional values vector,
//!   with a learned per-source cache, against arm P's bulk build. Carries a pre-registered kill
//!   criterion.
//!
//! [`shapes`] holds the inputs all three are measured on: the survey's widths, its value mixes, and
//! a synthesized mixed-gateway key-set distribution.
//!
//! Timings are in `benches/attr_arms.rs` (real jemalloc, no counting wrapper). Allocation counts
//! and the equivalence gate are in `tests/attr_arms.rs`: every arm must produce the same sorted
//! `(Symbol, Value)` sequence the shipped `AttrMap` does, repeated keys included. Recorded numbers
//! come from the perf VM only (`docs/design/performance.md` §0 and §8), never a workstation.

pub mod clone_arms;
pub mod keyset;
pub mod shapes;
pub mod thin;
