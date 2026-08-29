//! Input trait plus per-protocol implementations. The v0.1 vertical slice target is `statsd`
//! (`docs/OVERVIEW.md`); other protocols implement the same trait incrementally.

pub mod statsd;

use logit_core::EventBatch;

/// An input listens for data and produces batches. What "listens" means varies (a UDP socket, a
/// TCP accept loop, a file-tail watcher) but every input converges on this.
#[async_trait::async_trait]
pub trait Input {
    async fn run(&mut self, sink: tokio::sync::mpsc::Sender<EventBatch>) -> anyhow::Result<()>;
}
