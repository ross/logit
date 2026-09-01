//! The `Transform` trait: native (`Send`), non-Lua transform components --
//! `logit-transforms::Aggregator` is the first implementer. Runs as an ordinary tokio task in the
//! node runtime, unlike a Lua component, which needs its own OS thread
//! (`docs/design/pipeline-graph.md`'s "Node kinds and the transform trait question").
//!
//! Per-event, not per-batch, on purpose: it matches `Aggregator`'s actual accumulation contract
//! (one event in, an absorbed-or-passed-through event out) exactly, so `impl Transform for
//! Aggregator` needs no reshaping of its existing methods.

use crate::fanout::TraceContext;
use logit_core::{Event, Resource, SpanLink};
use std::sync::Arc;
use std::time::Duration;

/// One flushed event, paired with the bounded, best-effort set of `TraceContext`s that
/// contributed to it -- see [`Transform::flush`]'s doc comment.
pub type FlushedEvent = (Event, Vec<SpanLink>);

/// [`Transform::flush`]'s return type: one entry per resource group, each holding every series
/// flushed for that resource.
pub type FlushOutput = Vec<(Arc<Resource>, Vec<FlushedEvent>)>;

pub trait Transform: Send {
    /// Consumes or transforms one event. `Some` means "forward this downstream" (possibly
    /// unchanged, possibly mutated); `None` means the transform absorbed it into internal state
    /// (e.g. an aggregator accumulating a mergeable metric kind).
    fn process(&mut self, resource: &Arc<Resource>, event: Event) -> Option<Event>;

    /// Called once per incoming batch, before any of that batch's events reach `process` -- gives
    /// a transform whose emission spans several batches (only `Aggregator` today) a chance to
    /// record which batch contributed to whatever it's about to absorb, for
    /// `docs/adr/0020-trace-context-propagation-on-delivered.md`'s flush-side linking
    /// (`crates/logit-transforms/src/aggregate.rs`). Default no-op: `Json`/`KvMetrics`/`Keep`
    /// never flush, so they have nothing to attribute across batches and never override this.
    /// Deliberately not a `process` parameter -- the context is per-*batch*, and widening the
    /// per-event hot path (this trait's own doc comment: "per-event, not per-batch, on purpose")
    /// would cost every implementer for the one that actually needs it.
    fn observe_batch_context(&mut self, ctx: TraceContext) {
        let _ = ctx;
    }

    /// `Some(interval)` if this transform has a flush contract -- a timer-driven emission
    /// independent of inbound traffic, like `aggregate`'s tumbling windows
    /// (`docs/adr/0008-aggregation-window-semantics.md`). `None` (the default) means this
    /// transform never flushes.
    fn flush_interval(&self) -> Option<Duration> {
        None
    }

    /// Flushes accumulated state. Only ever called for a transform whose `flush_interval`
    /// returned `Some`; the default is unreachable in practice but returns nothing rather than
    /// panicking, matching this project's stance on not failing loudly over an internal
    /// invariant a caller is expected to uphold.
    ///
    /// Each emitted `Event` is paired with its own `Vec<SpanLink>` -- the bounded,
    /// best-effort set of `TraceContext`s that contributed to it, per `observe_batch_context`
    /// above (empty for a transform, like the default here, that never calls it). Paired rather
    /// than a parallel same-length array so there's no index correspondence to get wrong. Nothing
    /// consumes these yet -- `run_flush` (`crates/logit-pipeline/src/runtime.rs`) discards them
    /// on the way out -- `docs/known-gaps.md`'s internal-spans entry (item 2) tracks the still-open
    /// question of what turns a link set into a real `SpanRecord`.
    fn flush(&mut self, now: i64) -> FlushOutput {
        let _ = now;
        Vec::new()
    }
}
