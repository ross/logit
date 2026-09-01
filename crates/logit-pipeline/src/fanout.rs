//! [`Fanout`]: the outbound side of a graph node. Every non-sink component (listener, transform,
//! Lua stage) sends what it produces through one of these -- one `mpsc::Sender` per consumer,
//! resolved from the inverted `sources` relation at graph-build time
//! (`docs/design/pipeline-graph.md`'s "Runtime model").
//!
//! The channel payload is [`Delivered`], not a bare `EventBatch`
//! (`docs/adr/0016-arc-eventbatch-copy-on-write.md`). `send`/`send_blocking` still take an owned
//! `EventBatch` -- callers construct one exactly as before -- but an edge with exactly one
//! consumer (the common case: a linear chain, and every shipped listener's first hop) moves it
//! through as `Delivered::Owned`, with no `Arc` involved at all. Only a real fan-out (more than one
//! consumer) wraps the batch in an `Arc` and hands out `Delivered::Shared` clones -- a refcount
//! bump, not a deep clone. The consuming side handles either variant per node kind: `run_output`
//! (`runtime.rs`) borrows `&EventBatch` straight out of either variant -- `Output::send` takes a
//! reference, so this is where the fan-out saving actually lands -- while `run_transform`/`run_lua`
//! still call `unwrap_batch` to get an owned `EventBatch`, since `Transform::process`/
//! `ScriptWorker::process` need to mutate or consume an owned `Event`. (A listener's own inbox is
//! never fed at all -- arity rules out a `sources` entry pointing at one -- so `Input` never
//! receives a `Delivered` either way.)

use logit_core::EventBatch;
use std::sync::Arc;
use tokio::sync::mpsc;

/// What travels one graph edge. `Fanout::send`/`send_blocking` pick the variant per send based on
/// how many consumers that `Fanout` has -- a property of the edge, not of the batch itself.
pub enum Delivered {
    /// This edge's `Fanout` had exactly one consumer: the batch moved through with no `Arc`
    /// allocated at all. The common case -- every listener's first hop, and every interior edge of
    /// a linear chain (the v0.1 reference config's `statsd_in -> aggregate -> lua -> influxdb_out`
    /// among them).
    Owned(EventBatch),
    /// This edge's `Fanout` had more than one consumer: every one of them holds a handle to the
    /// same `Arc`-wrapped batch. Which handle (if any) gets to reclaim the batch without cloning is
    /// decided at the consuming end, by which one happens to be dropped last at runtime -- there is
    /// no privileged branch, and under concurrent consumption more than one can end up cloning; see
    /// `runtime.rs`'s `unwrap_batch`.
    Shared(Arc<EventBatch>),
}

/// A node's outbound edges. Fan-in (multiple sources feeding one component) is free -- it's just
/// N cloned `Sender`s feeding the same inbox on the consumer's side, nothing this type needs to
/// know about. Fan-out (one component feeding several consumers) is what this type exists to make
/// cheap to get right: a single-consumer edge moves the batch through for free, and only a real
/// fan-out pays for an `Arc`.
#[derive(Clone, Default)]
pub struct Fanout {
    consumers: Vec<mpsc::Sender<Delivered>>,
}

impl Fanout {
    pub fn new(consumers: Vec<mpsc::Sender<Delivered>>) -> Self {
        Self { consumers }
    }

    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    /// Sends `batch` to every consumer. A closed consumer is silently skipped -- see
    /// `docs/design/pipeline-graph.md`'s backpressure section: propagating a closed downstream as
    /// a real shutdown signal is a named open question, not solved here.
    ///
    /// Exactly one consumer: `batch` moves through as [`Delivered::Owned`], no `Arc` involved at
    /// all -- this is what keeps a linear chain (no fan-out anywhere on it) free of this change's
    /// cost entirely. More than one consumer: wraps `batch` in an `Arc` once, then clones the `Arc`
    /// (a refcount bump, not a deep clone) for every consumer but the last, which gets it moved --
    /// saving one atomic increment/decrement pair, not a structural privilege (see
    /// [`Delivered::Shared`]'s doc comment).
    pub async fn send(&self, batch: EventBatch) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        if rest.is_empty() {
            let _ = last.send(Delivered::Owned(batch)).await;
            return;
        }
        let batch = Arc::new(batch);
        for tx in rest {
            let _ = tx.send(Delivered::Shared(batch.clone())).await;
        }
        let _ = last.send(Delivered::Shared(batch)).await;
    }

    /// The `blocking_send` equivalent of [`Fanout::send`], for a node running on a plain OS
    /// thread rather than as a tokio task (a Lua node -- see
    /// `docs/design/pipeline-graph.md`'s "Thread model" section).
    pub fn send_blocking(&self, batch: EventBatch) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        if rest.is_empty() {
            let _ = last.blocking_send(Delivered::Owned(batch));
            return;
        }
        let batch = Arc::new(batch);
        for tx in rest {
            let _ = tx.blocking_send(Delivered::Shared(batch.clone()));
        }
        let _ = last.blocking_send(Delivered::Shared(batch));
    }
}
