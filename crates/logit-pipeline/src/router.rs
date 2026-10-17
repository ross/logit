//! The [`Router`] trait: a node that directs each event to one or more destinations without
//! changing it ([ADR `target-components`](../../../docs/adr/target-components.md)).
//! `logit-transforms::Route` and a `lua`/`lua_file` component with `targets:` implement it.
//!
//! **A separate trait, not a wider `Transform`.** `Transform::process`'s `bool` says "keep" or
//! "absorbed" and has no place to say where; widening it would put routing on every transform
//! (the ADR's "Alternatives considered"). The `observe_*`/`map_resource` hooks match
//! `Transform`'s, with the same per-batch reasoning.
//!
//! **No drop.** Dropping is a transform's job: place a `keep`/`has_*`/`lua` transform before the
//! router. A [`Destinations`] set is never empty once [`crate::runtime::route_batch`] has checked
//! it, so every event leaves at least once, once to each destination its mark names. An event
//! marked for one destination is moved; one marked for several is shared or copied, and those
//! extra deliveries are what `logit.component.events.copied` counts.
//!
//! **No flush hooks.** Routing is a pure function of the event and the per-batch state the
//! `observe_*` hooks supply, so a router has nothing to accumulate or emit between batches.
//! `crate::runtime::run_router` is `run_transform` minus the flush-deadline race.

use crate::fanout::TraceContext;
use logit_core::{Event, Provenance, Resource, Scope};
use std::sync::Arc;

/// The set of destinations one event goes to: a bit per slot, slot 0 being `Forward` (the
/// router's own outbound edge, whoever lists it in `sources:`, the else-branch) and slot `n + 1`
/// being target `n`. A target index is a slot in [`crate::graph::targets_of`] order (the order
/// `ResolvedComponent::targets` records and the node runtime builds its `Vec<Fanout>` in), never
/// a component id, so the hot path never compares or hashes a string.
///
/// `Copy` and 8 bytes, so a per-event mark costs what the single-destination enum it replaced
/// did. A `u64` caps a router at [`Destinations::MAX_TARGETS`] targets; graph rule 51 enforces
/// that on `route`. A Lua router marks with `event:to(..)`'s `Option<u16>` and never builds one.
///
/// Graph rule 50 rejects a router with no ordinary consumer, so a `Forward` event always has
/// somewhere to go; an operator who wants those events gone lists the router in a `null_out`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Destinations(u64);

impl Destinations {
    /// The most targets one mark can name: 64 bits, less slot 0 (`Forward`).
    pub const MAX_TARGETS: usize = 63;

    /// The router's own outbound edge alone.
    pub const FORWARD: Self = Self(1);

    /// No destination. Never a valid final mark: `route_batch` turns it into `FORWARD`. A router
    /// builds a mark up from it with [`Destinations::with`].
    pub const NONE: Self = Self(0);

    /// Target `n` alone. An `n` at or past [`Destinations::MAX_TARGETS`] has no bit and yields
    /// [`Destinations::NONE`], which `route_batch` treats as the bug it is.
    pub const fn target(n: u16) -> Self {
        match 1u64.checked_shl(n as u32 + 1) {
            Some(bit) => Self(bit),
            None => Self::NONE,
        }
    }

    /// The union of `self` and `other`.
    #[must_use]
    pub const fn with(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Whether the set names no destination.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// How many destinations the set names.
    pub const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    /// Whether the set names slot `slot` (0 is `Forward`, `n + 1` is target `n`).
    pub const fn contains_slot(self, slot: usize) -> bool {
        slot < 64 && self.0 & (1 << slot) != 0
    }

    /// The slots in the set, ascending.
    pub fn slots(self) -> impl Iterator<Item = usize> {
        let mut bits = self.0;
        std::iter::from_fn(move || {
            if bits == 0 {
                return None;
            }
            let slot = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            Some(slot)
        })
    }

    /// The highest slot in the set, or `None` for an empty one.
    pub(crate) const fn highest_slot(self) -> Option<usize> {
        match self.0 {
            0 => None,
            bits => Some(63 - bits.leading_zeros() as usize),
        }
    }

    /// The set less slot `slot`.
    pub(crate) const fn without_slot(self, slot: usize) -> Self {
        Self(self.0 & !(1 << slot))
    }

    /// The set restricted to the first `slots` slots, and whether anything was outside them.
    pub(crate) const fn within(self, slots: usize) -> (Self, bool) {
        let valid = match slots {
            0 => 0,
            s if s >= 64 => u64::MAX,
            s => (1u64 << s) - 1,
        };
        (Self(self.0 & valid), self.0 & !valid != 0)
    }
}

impl std::ops::BitOr for Destinations {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        self.with(other)
    }
}

impl std::ops::BitOrAssign for Destinations {
    fn bitor_assign(&mut self, other: Self) {
        *self = self.with(other);
    }
}

/// A node that directs each event to one or more destinations and never mutates or absorbs it.
pub trait Router: Send {
    /// Returns the event's destinations. Borrows the event: `Event` is large
    /// (`crates/logit-core/tests/type_sizes.rs`), and a by-value `(Destinations, Event)` return
    /// would memcpy every event through a tuple. [`crate::runtime::route_batch`] moves the event
    /// from the incoming batch into its destination's batch, and shares or copies it only when
    /// the set names more than one destination.
    fn route(&mut self, resource: &Arc<Resource>, event: &Event) -> Destinations;

    /// Called once per incoming batch, before any of its events reach [`Router::route`]; the
    /// mirror of [`crate::Transform::observe_batch_context`]. Default no-op: no shipped router
    /// reads the trace context.
    fn observe_batch_context(&mut self, ctx: TraceContext) {
        let _ = ctx;
    }

    /// Called once per incoming batch, alongside [`Router::observe_batch_context`]. `route`'s
    /// `by: {provenance: ..}` forms cache it on `self` and read their key from it. Default no-op.
    fn observe_provenance(&mut self, provenance: Provenance) {
        let _ = provenance;
    }

    /// Called once per incoming batch, alongside the two hooks above; the mirror of
    /// [`crate::Transform::observe_scope`]. The scope rides through onto every outgoing partition
    /// unchanged. Default no-op.
    fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        let _ = scope;
    }

    /// Called once per incoming batch, after the `observe_*` hooks and before any event reaches
    /// [`Router::route`]; the mirror of [`crate::Transform::map_resource`]. `None` (the default)
    /// keeps the resource the batch arrived with on every outgoing partition, its `Arc` cloned
    /// once per used destination, never per event.
    fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> {
        let _ = resource;
        None
    }
}

/// One router node's reusable partition buffers, owned by [`crate::runtime::run_router`] and
/// handed to [`crate::runtime::route_batch`] on every batch. The partition allocates once per
/// destination that receives events, and nothing per event it moves:
///
/// - `marks` and `counts` are cleared and refilled in place, never reallocated once grown.
/// - Each `dests` entry is handed out by `std::mem::take`, leaving a capacity-0 `Vec` behind, so
///   the next batch's `reserve_exact(count)` allocates the needed capacity once, and nothing for
///   a destination that received no events.
pub struct RouterScratch {
    /// One [`Destinations`] set per event in the batch being routed, in event order: filled by
    /// pass 1 (which borrows each event) and read by pass 4 (which moves it).
    pub(crate) marks: Vec<Destinations>,
    /// Per-destination event counts for the batch being routed; `counts[0]` is `Forward`.
    pub(crate) counts: Vec<usize>,
    /// Per-destination event buffers, `dests[0]` being `Forward` and `dests[n + 1]` being
    /// target `n`. Always left empty (and capacity-0) when `route_batch` returns.
    pub(crate) dests: Vec<Vec<Event>>,
}

impl RouterScratch {
    /// `targets` is the number of `target` components this router directs at (the length of the
    /// runtime's slot-ordered `Vec<Fanout>`). Slot 0 is [`Destinations::FORWARD`]; slot `n + 1`
    /// is [`Destinations::target`]`(n)`.
    pub fn new(targets: usize) -> Self {
        Self {
            marks: Vec::new(),
            counts: vec![0; targets + 1],
            dests: (0..targets + 1).map(|_| Vec::new()).collect(),
        }
    }

    /// How many destinations this scratch can address, `Forward` included: `targets + 1`.
    pub fn destinations(&self) -> usize {
        self.dests.len()
    }
}
