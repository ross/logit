//! [`Fanout`]: the outbound side of a graph node. Every non-sink component (listener, transform,
//! Lua stage) sends what it produces through one of these -- one `mpsc::Sender` per consumer,
//! resolved from the inverted `sources` relation at graph-build time
//! (`docs/design/pipeline-graph.md`'s "Runtime model").

use logit_core::EventBatch;
use tokio::sync::mpsc;

/// A node's outbound edges. Fan-in (multiple sources feeding one component) is free -- it's just
/// N cloned `Sender`s feeding the same inbox on the consumer's side, nothing this type needs to
/// know about. Fan-out (one component feeding several consumers) is what this type exists to
/// make cheap to get right: clone the batch for every consumer but the last, so the final send
/// can move it instead of cloning one time too many.
#[derive(Clone, Default)]
pub struct Fanout {
    consumers: Vec<mpsc::Sender<EventBatch>>,
}

impl Fanout {
    pub fn new(consumers: Vec<mpsc::Sender<EventBatch>>) -> Self {
        Self { consumers }
    }

    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    /// Sends `batch` to every consumer. A closed consumer is silently skipped -- see
    /// `docs/design/pipeline-graph.md`'s backpressure section: propagating a closed downstream as
    /// a real shutdown signal is a named open question, not solved here.
    pub async fn send(&self, batch: EventBatch) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        for tx in rest {
            let _ = tx.send(batch.clone()).await;
        }
        let _ = last.send(batch).await;
    }

    /// The `blocking_send` equivalent of [`Fanout::send`], for a node running on a plain OS
    /// thread rather than as a tokio task (a Lua node -- see
    /// `docs/design/pipeline-graph.md`'s "Thread model" section).
    pub fn send_blocking(&self, batch: EventBatch) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        for tx in rest {
            let _ = tx.blocking_send(batch.clone());
        }
        let _ = last.blocking_send(batch);
    }
}
