//! statsd / DogStatsD-tagged metrics over UDP -- the input side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`: statsd -> transform -> InfluxDB).

use crate::Input;
use logit_core::EventBatch;

pub struct StatsdInput {
    pub bind: String,
}

#[async_trait::async_trait]
impl Input for StatsdInput {
    async fn run(&mut self, _sink: tokio::sync::mpsc::Sender<EventBatch>) -> anyhow::Result<()> {
        // TODO: UDP listener decoding `name:value|type|#tag:val,...` (DogStatsD tag extension)
        // into `MetricRecord`s via `logit_proto::Decoder`. Left unimplemented for the design/
        // scaffolding pass -- this is the first thing to build in the v0.1 slice.
        todo!("bind {} and decode statsd lines into EventBatches", self.bind)
    }
}
