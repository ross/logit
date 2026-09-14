//! The [`Router`] trait: a node that *directs* each event to one destination without changing it
//! -- the runtime seam [ADR `target-components`](../../../docs/adr/target-components.md) adds
//! beside [`crate::Transform`]. `logit-transforms::Route` (W4) and a `lua`/`lua_file` component
//! with `targets:` (W5) are its two implementers; like every other trait in this crate, they live
//! in the crates *above* it (`docs/design/pipeline-graph.md`'s "Crate layout").
//!
//! **Why a separate trait rather than widening `Transform`.** `Transform::process` is
//! one-in/zero-or-one-out, and the one bit it returns (`Some`/`None`) means "keep" or "absorbed"
//! -- it has no place to say *where*. Widening it to return a destination would put a routing
//! concern on every transform in the workspace, for the two kinds that actually route; a separate
//! trait costs one small trait and one node kind, and the ADR's "Alternatives considered" records
//! that trade directly. The `observe_*`/`map_resource` hooks are deliberately the same shape
//! `Transform` already has, with the same per-batch-not-per-event reasoning (see
//! `crate::transform::Transform`'s own doc comments) -- `route`'s `by: {provenance: ..}` form
//! reads its key straight off `observe_provenance`'s cached value, exactly as
//! `has_provenance`/`drop_provenance` do.
//!
//! **Why there is no `Destination::Drop`.** Absorbing an event is `Transform`'s job, and a router
//! that could also drop would be two components' worth of behaviour in one: put a `keep`/`has_*`/
//! `lua` transform *before* the router and the dropped events never reach it at all. A router
//! only ever moves an event it was given into exactly one outgoing batch, which is what makes the
//! partition in [`crate::runtime::route_batch`] total -- every event in, every event out.
//!
//! **Why there are no flush hooks.** No router flushes. `Transform` carries
//! `flush_interval`/`flush` because `aggregate` genuinely emits on a timer independent of inbound
//! traffic; routing is a pure per-event function of the event and the per-batch state the
//! `observe_*` hooks supply, so there is nothing for a router to accumulate and nothing to emit
//! between batches. This is stated here rather than carried as an unused default-no-op pair, per
//! `AGENTS.md`'s "stub code says so" convention: an unimplemented hook that nobody calls is worse
//! than a sentence saying the contract doesn't have one. `crate::runtime::run_router` is
//! `run_transform` minus the flush-deadline race, for exactly this reason.

use crate::fanout::TraceContext;
use logit_core::{Event, Provenance, Resource, Scope};
use std::sync::Arc;

/// Which destination one event belongs to. `To`'s index is a *slot*, in
/// [`crate::graph::targets_of`] order -- the same order `ResolvedComponent::targets` records and
/// the node runtime builds its `Vec<Fanout>` in -- never a component id, so nothing on the hot
/// path ever compares or hashes a string.
///
/// `Forward` is the router's *own* outbound edge (whoever lists it in `sources:`) -- the
/// else-branch, spelled as an ordinary graph edge rather than as a chain of complementary filters
/// (`docs/adr/target-components.md`). A router with targets and no ordinary consumers is a legal
/// config: its `Forward` events are dropped and counted
/// `logit.component.events.dropped{reason="unrouted"}`, never silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Destination {
    Forward,
    To(u16),
}

/// A node that directs each event to one destination and never mutates or absorbs it.
pub trait Router: Send {
    /// **Borrows the event on purpose.** `Event` is large (`crates/logit-core/tests/type_sizes.rs`
    /// pins the exact `size_of`), and a by-value `(Destination, Event)`-shaped return would memcpy
    /// every event through an enum on the hot path for nothing -- the routing verdict is a
    /// two-byte answer to a question about the event, not a transformation of it. The node moves
    /// the event straight from the incoming batch into its destination's batch instead
    /// ([`crate::runtime::route_batch`]'s four passes), so no event is ever copied.
    fn route(&mut self, resource: &Arc<Resource>, event: &Event) -> Destination;

    /// Called once per incoming batch, before any of that batch's events reach [`Router::route`]
    /// -- the same hook, with the same signature and the same reasoning, as
    /// [`crate::Transform::observe_batch_context`]. Default no-op: no shipped router reads the
    /// trace context (nothing links a routing decision to an upstream batch), but the hook is
    /// here so the two node traits' per-batch surfaces stay identical rather than gratuitously
    /// different.
    fn observe_batch_context(&mut self, ctx: TraceContext) {
        let _ = ctx;
    }

    /// Called once per incoming batch, alongside [`Router::observe_batch_context`] -- what
    /// `route`'s `by: {provenance: origin}`/`{provenance: previous}` forms read their key from
    /// (W4), cached on `self` exactly as `has_provenance`/`drop_provenance` already cache it
    /// (`crates/logit-transforms/src/provenance.rs`). Per-*batch*, not per-event: provenance is a
    /// property of the batch, and widening `route`'s per-event signature would cost every
    /// implementer for the ones that need it. Default no-op.
    fn observe_provenance(&mut self, provenance: Provenance) {
        let _ = provenance;
    }

    /// Called once per incoming batch, alongside the two hooks above -- the mirror of
    /// [`crate::Transform::observe_scope`]. Default no-op: no shipped router routes on scope,
    /// but the batch's scope is read out once per batch anyway (it rides through onto every
    /// outgoing partition unchanged), so handing it over costs nothing.
    fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        let _ = scope;
    }

    /// Called once per incoming batch, after the `observe_*` hooks and before any event reaches
    /// [`Router::route`] -- the mirror of [`crate::Transform::map_resource`]. `None` (the default,
    /// and what both shipped routers return) means "every outgoing partition keeps the resource
    /// the batch arrived with," with the incoming `Arc` moved straight through and cloned once per
    /// *used destination*, never per event.
    fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> {
        let _ = resource;
        None
    }
}

/// One router node's reusable partition buffers -- owned by [`crate::runtime::run_router`] for the
/// life of the node and handed to [`crate::runtime::route_batch`] on every batch, so the
/// partition allocates **exactly once per destination that actually receives events**, and
/// nothing at all per event.
///
/// The three buffers are all amortized-zero after the first few batches:
///
/// - `marks` and `counts` are `clear()`ed/refilled in place, never reallocated once they have
///   grown to the largest batch/destination count this node has seen.
/// - `dests` is the one that matters: each entry is handed out by `std::mem::take`, which leaves
///   an *empty `Vec` with capacity 0* behind. The next batch's `reserve_exact(count)` therefore
///   allocates exactly the needed capacity, once -- no growth doubling, no over-reservation, and
///   no allocation at all for a destination that received nothing.
pub struct RouterScratch {
    /// One [`Destination`] per event in the batch being routed, in event order -- filled by pass
    /// 1 (which only *borrows* each event) and read back by pass 4 (which moves it).
    pub(crate) marks: Vec<Destination>,
    /// Per-destination event counts for the batch being routed; `counts[0]` is `Forward`.
    pub(crate) counts: Vec<usize>,
    /// Per-destination event buffers, `dests[0]` being `Forward` and `dests[n + 1]` being
    /// `Destination::To(n)`. Always left empty (and capacity-0) when `route_batch` returns.
    pub(crate) dests: Vec<Vec<Event>>,
}

impl RouterScratch {
    /// `targets` is the number of `target` components this router directs at -- the length of the
    /// slot-ordered `Vec<Fanout>` the runtime hands it. Every buffer is sized `targets + 1`: slot
    /// 0 is the router's own outbound edge ([`Destination::Forward`]), slot `n + 1` is
    /// `Destination::To(n)`.
    pub fn new(targets: usize) -> Self {
        Self {
            marks: Vec::new(),
            counts: vec![0; targets + 1],
            dests: (0..targets + 1).map(|_| Vec::new()).collect(),
        }
    }

    /// How many destinations this scratch can address, `Forward` included -- `targets + 1`.
    pub fn destinations(&self) -> usize {
        self.dests.len()
    }
}
