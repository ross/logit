//! The `Output` trait -- moved here from `logit-outputs`, same reasoning as [`crate::input`].

use logit_core::EventBatch;

/// A sink component: takes batches and delivers them somewhere. Buffering between the pipeline
/// and delivery is the output's responsibility, via `logit_proto::buffer::Buffer`
/// (`docs/design/wire-protocol.md`). A sink has at least one source and is never itself a source
/// of anything else (`docs/design/pipeline-graph.md`'s arity table).
#[async_trait::async_trait]
pub trait Output {
    async fn send(&mut self, batch: EventBatch) -> anyhow::Result<()>;
}
