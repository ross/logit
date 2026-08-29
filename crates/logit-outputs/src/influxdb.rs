//! InfluxDB 2.x line-protocol output -- the output side of the v0.1 vertical slice
//! (`docs/OVERVIEW.md`), writing to `/api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml` for local testing.

use crate::Output;
use logit_core::EventBatch;

pub struct InfluxDbOutput {
    pub url: String,
    pub org: String,
    pub bucket: String,
    pub token: String,
}

#[async_trait::async_trait]
impl Output for InfluxDbOutput {
    async fn send(&mut self, _batch: EventBatch) -> anyhow::Result<()> {
        // TODO: encode MetricRecords as line protocol and POST to
        // `{url}/api/v2/write?org={org}&bucket={bucket}` with `Authorization: Token {token}`.
        // Left unimplemented for the design/scaffolding pass -- this is the second thing to build
        // in the v0.1 slice, after the statsd input.
        todo!("encode {} metrics as line protocol and write to InfluxDB", self.bucket)
    }
}
