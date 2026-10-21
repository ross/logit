//! The `Transform` trait: native (`Send`), non-Lua transform components. A native transform
//! runs as an ordinary tokio task in the node runtime, unlike a Lua component, which needs its own
//! OS thread (`docs/design/pipeline-graph.md`'s "Node kinds and the transform trait question").
//!
//! `process` is per-event on purpose: it matches `aggregate`'s accumulation contract (one event
//! in, absorbed or forwarded), and `process_into` lets one event also emit others into the same
//! batch (`docs/adr/transform-fan-out.md`). Everything per-batch goes through the `observe_*`/`map_resource`/
//! `begin_batch`/`end_batch` hooks, so the per-event hot path never widens for one implementer's
//! needs.

use crate::fanout::TraceContext;
use logit_core::{Event, Provenance, Resource, Scope, SpanLink};
use std::sync::Arc;
use std::time::Duration;

/// One flushed event, paired with the bounded, best-effort set of `TraceContext`s that
/// contributed to it (see [`Transform::flush`]).
pub type FlushedEvent = (Event, Vec<SpanLink>);

/// [`Transform::flush`]'s return type: one entry per `(resource, scope)` group, each holding
/// every series flushed for that group. `run_flush` stamps each group's scope on its outgoing
/// batch, which is what keeps `otlp_in -> aggregate -> otlp_out` from losing scope. Group by
/// value, not `Arc` identity: two batches that build equal-content `Arc<Scope>`s describe the
/// same instrumentation scope and aggregate together.
pub type FlushOutput = Vec<(Arc<Resource>, Option<Arc<Scope>>, Vec<FlushedEvent>)>;

pub trait Transform: Send {
    /// Transforms one event **in place**. `true` means "forward this downstream" (possibly
    /// unchanged, possibly mutated through the `&mut`); `false` means the transform absorbed the
    /// event into internal state (e.g. an aggregator accumulating a mergeable metric kind) or
    /// dropped it, and the caller discards the event rather than forwarding it.
    ///
    /// Borrows the event, as [`crate::Router::route`] does: `Event` is 864 bytes
    /// (`crates/logit-core/tests/type_sizes.rs`), and an owned `Event -> Option<Event>` shape
    /// would memcpy one per event per node hop. [`crate::runtime::process_batch`] drives this,
    /// through [`Transform::process_into`], with `Vec::retain_mut`, so a forwarded event is never
    /// moved.
    ///
    /// An absorbing transform moves the payload it wants out with `std::mem::take` (`aggregate`
    /// takes `event.metrics` this way) and leaves the drained shell for the caller to drop.
    fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool;

    /// [`Transform::process`] with an emit path, and the only one of the two the runtime calls.
    /// The return value means what `process`'s does: `true` forwards `event`, `false` discards
    /// it. Every event pushed onto `emitted` is forwarded whatever the bool says, after the
    /// batch's surviving events, in emission order across the batch, with the batch's resource
    /// and scope. A native transform has no `targets:`, so a pushed event carries no route mark
    /// and goes to the node's ordinary edge (`docs/adr/transform-fan-out.md`).
    ///
    /// `emitted` is the node's scratch, empty on entry to each batch. Push onto it; don't
    /// reorder or remove what an earlier call in the same batch pushed. The default forwards to
    /// `process` and pushes nothing, which allocates nothing.
    fn process_into(
        &mut self,
        resource: &Arc<Resource>,
        event: &mut Event,
        emitted: &mut Vec<Event>,
    ) -> bool {
        let _ = emitted;
        self.process(resource, event)
    }

    /// Called once per incoming batch, before any of its events reach `process`. A transform
    /// whose emission spans several batches (`aggregate`) records which batch contributed to what
    /// it absorbs, for `docs/adr/trace-context-propagation-on-delivered.md`'s flush-side linking.
    /// Default no-op, for a transform that never flushes.
    fn observe_batch_context(&mut self, ctx: TraceContext) {
        let _ = ctx;
    }

    /// Called once per incoming batch, alongside `observe_batch_context`, with the batch's
    /// provenance (`origin`/`previous`, `docs/adr/batch-provenance-on-delivered.md`). An
    /// implementer (`HasProvenance`/`DropProvenance`) caches it on `self` for `process` to read.
    /// Default no-op.
    fn observe_provenance(&mut self, provenance: Provenance) {
        let _ = provenance;
    }

    /// Called once per incoming batch, alongside `observe_batch_context`/`observe_provenance`,
    /// with the batch's scope. `aggregate` records it so `flush` can stamp its emission with it
    /// (see [`FlushOutput`]); `shape` measures it. Default no-op.
    fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        let _ = scope;
    }

    /// Called once per incoming batch, after `observe_batch_context` and before any of its events
    /// reach `process`, to substitute the batch's resource (`set`,
    /// `docs/adr/operator-declared-resource-attributes.md`). `None` (the default) forwards the
    /// batch under the resource it arrived with; `process_batch` moves that `Arc` through without
    /// cloning it. `Some` replaces it for both `process`'s `resource` argument and the outgoing
    /// batch.
    fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> {
        let _ = resource;
        None
    }

    /// Called once per incoming batch, after `map_resource` and before any of its events reach
    /// `process`, with the batch's resource, scope, and events. It may edit events in place and
    /// remove them, but not add one: a new event goes through `process_into`'s `emitted`.
    /// [`crate::runtime::process_batch`] counts a removed event under
    /// `logit.component.events.discarded`, as it counts a `false` from `process`. For a transform
    /// whose decision about one event depends on a later one in the same batch
    /// (`dedup_timestamps`, `docs/adr/dedup-timestamps-transform.md`). Default no-op.
    fn begin_batch(
        &mut self,
        resource: &Arc<Resource>,
        scope: Option<&Arc<Scope>>,
        events: &mut Vec<Event>,
    ) {
        let _ = (resource, scope, events);
    }

    /// Called once per incoming batch, after its last event has been through `process`. A place
    /// for per-batch bookkeeping kept out of `process`: `kv_metrics` tallies counts in plain
    /// integers per event and emits them as telemetry here, instead of paying
    /// `Telemetry::count`'s mutex and hash-map upsert per event. Default no-op.
    fn end_batch(&mut self) {}

    /// `Some(interval)` if this transform has a flush contract -- a timer-driven emission
    /// independent of inbound traffic, like `aggregate`'s tumbling windows
    /// (`docs/adr/aggregation-window-semantics.md`). `None` (the default) means this
    /// transform never flushes.
    fn flush_interval(&self) -> Option<Duration> {
        None
    }

    /// Flushes accumulated state. Called only for a transform whose `flush_interval` returned
    /// `Some`; the default returns nothing rather than panicking.
    ///
    /// Each emitted `Event` is paired with the bounded, best-effort set of `TraceContext`s that
    /// contributed to it (from `observe_batch_context`; empty if the transform never records
    /// any). `run_flush` unions every group's links onto the one flush span it records for this
    /// call, capped at `MAX_LINKS_PER_SPAN`
    /// (`docs/adr/internal-span-emission-and-deterministic-sampling.md`).
    fn flush(&mut self, now: i64) -> FlushOutput {
        let _ = now;
        Vec::new()
    }
}
