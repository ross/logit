//! The [`Router`] trait: a node that directs each event to one destination without changing it
//! ([ADR `target-components`](../../../docs/adr/target-components.md)).
//! `logit-transforms::Route` and a `lua`/`lua_file` component with `targets:` implement it.
//!
//! **A separate trait, not a wider `Transform`.** `Transform::process`'s `bool` says "keep" or
//! "absorbed" and has no place to say where; widening it would put routing on every transform
//! (the ADR's "Alternatives considered"). The `observe_*`/`map_resource` hooks match
//! `Transform`'s, with the same per-batch reasoning.
//!
//! **No `Destination::Drop`.** Dropping is a transform's job: place a `keep`/`has_*`/`lua`
//! transform before the router. A router moves every event it is given into exactly one outgoing
//! batch, which is what makes [`crate::runtime::route_batch`]'s partition total: every event in,
//! every event out.
//!
//! **No flush hooks.** Routing is a pure function of the event and the per-batch state the
//! `observe_*` hooks supply, so a router has nothing to accumulate or emit between batches.
//! `crate::runtime::run_router` is `run_transform` minus the flush-deadline race.

use crate::fanout::TraceContext;
use logit_core::{Event, Provenance, Resource, Scope};
use std::sync::Arc;

/// Which destination one event belongs to. `To`'s index is a slot in
/// [`crate::graph::targets_of`] order (the order `ResolvedComponent::targets` records and the
/// node runtime builds its `Vec<Fanout>` in), never a component id, so the hot path never
/// compares or hashes a string.
///
/// `Forward` is the router's own outbound edge (whoever lists it in `sources:`): the else-branch.
/// A router with targets and no ordinary consumers is a legal config; its `Forward` events are
/// dropped and counted `logit.component.events.dropped{reason="unrouted"}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    Forward,
    To(u16),
}

/// A node that directs each event to one destination and never mutates or absorbs it.
pub trait Router: Send {
    /// Returns the event's destination. Borrows the event: `Event` is large
    /// (`crates/logit-core/tests/type_sizes.rs`), and a by-value `(Destination, Event)` return
    /// would memcpy every event through an enum. [`crate::runtime::route_batch`] moves the event
    /// from the incoming batch into its destination's batch, so no event is copied.
    fn route(&mut self, resource: &Arc<Resource>, event: &Event) -> Destination;

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
/// destination that receives events, and nothing per event:
///
/// - `marks` and `counts` are cleared and refilled in place, never reallocated once grown.
/// - Each `dests` entry is handed out by `std::mem::take`, leaving a capacity-0 `Vec` behind, so
///   the next batch's `reserve_exact(count)` allocates the needed capacity once, and nothing for
///   a destination that received no events.
pub struct RouterScratch {
    /// One [`Destination`] per event in the batch being routed, in event order: filled by pass 1
    /// (which borrows each event) and read by pass 4 (which moves it).
    pub(crate) marks: Vec<Destination>,
    /// Per-destination event counts for the batch being routed; `counts[0]` is `Forward`.
    pub(crate) counts: Vec<usize>,
    /// Per-destination event buffers, `dests[0]` being `Forward` and `dests[n + 1]` being
    /// `Destination::To(n)`. Always left empty (and capacity-0) when `route_batch` returns.
    pub(crate) dests: Vec<Vec<Event>>,
}

impl RouterScratch {
    /// `targets` is the number of `target` components this router directs at (the length of the
    /// runtime's slot-ordered `Vec<Fanout>`). Slot 0 is [`Destination::Forward`]; slot `n + 1`
    /// is `Destination::To(n)`.
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
