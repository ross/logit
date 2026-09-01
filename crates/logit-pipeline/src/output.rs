//! The `Output` trait -- moved here from `logit-outputs`, same reasoning as [`crate::input`].

use logit_core::EventBatch;

/// A sink component: takes batches and delivers them somewhere. Buffering between the pipeline
/// and delivery is the output's responsibility, via `logit_proto::buffer::Buffer`
/// (`docs/design/wire-protocol.md`). A sink has at least one source and is never itself a source
/// of anything else (`docs/design/pipeline-graph.md`'s arity table).
///
/// Takes `&EventBatch`, not an owned one -- a sink only ever reads a batch to encode/write it, and
/// this is the half of `docs/adr/0016-arc-eventbatch-copy-on-write.md`'s copy-on-write design that
/// actually realizes the fan-out saving: `run_output` (`runtime.rs`) can hand a `Delivered::Shared`
/// branch straight through as a reference, with no `Arc::try_unwrap`/clone ever needed for a
/// read-only `Output` consumer, regardless of how many sibling branches still hold their own
/// handle to the same batch.
#[async_trait::async_trait]
pub trait Output {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()>;
}
