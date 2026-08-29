//! Output trait plus per-protocol sinks. The v0.1 vertical slice target is `influxdb`
//! (`docs/OVERVIEW.md`); other protocols implement the same trait incrementally.

pub mod influxdb;

use logit_core::EventBatch;

/// An output takes batches and delivers them somewhere. Buffering between the pipeline and
/// delivery is the output's responsibility, via `logit_proto::buffer::Buffer`
/// (`docs/design/wire-protocol.md`).
#[async_trait::async_trait]
pub trait Output {
    async fn send(&mut self, batch: EventBatch) -> anyhow::Result<()>;
}
