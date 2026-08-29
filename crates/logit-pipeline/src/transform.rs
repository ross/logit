//! The `Transform` trait: native (`Send`), non-Lua transform components --
//! `logit-transforms::Aggregator` is the first implementer. Runs as an ordinary tokio task in the
//! node runtime, unlike a Lua component, which needs its own OS thread
//! (`docs/design/pipeline-graph.md`'s "Node kinds and the transform trait question").
//!
//! Per-event, not per-batch, on purpose: it matches `Aggregator`'s actual accumulation contract
//! (one event in, an absorbed-or-passed-through event out) exactly, so `impl Transform for
//! Aggregator` needs no reshaping of its existing methods.

use logit_core::{Event, Resource};
use std::sync::Arc;
use std::time::Duration;

pub trait Transform: Send {
    /// Consumes or transforms one event. `Some` means "forward this downstream" (possibly
    /// unchanged, possibly mutated); `None` means the transform absorbed it into internal state
    /// (e.g. an aggregator accumulating a mergeable metric kind).
    fn process(&mut self, resource: &Arc<Resource>, event: Event) -> Option<Event>;

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
    fn flush(&mut self, now: i64) -> Vec<(Arc<Resource>, Vec<Event>)> {
        let _ = now;
        Vec::new()
    }
}
