//! `null_out`: a sink that discards everything. It's the explicit discard an operator names in the
//! graph, such as the consumer of a router's unrouted events, which graph rule 50 requires
//! ([ADR `target-components`](../../../../docs/adr/target-components.md)). Paired with
//! `generate_in`, it measures everything upstream of a sink with no encoder, socket, or filesystem
//! in the number ([ADR `load-test-harness`](../../../../docs/adr/load-test-harness.md)). Also
//! useful for running a config's listener and transform chain against a real process with nothing
//! downstream.
//!
//! ## Config
//!
//! ```yaml
//! drop:
//!   type: null_out
//!   sources: [in]
//!   buffer:
//!     disk:
//!       path: /var/lib/logit/spool
//! ```
//!
//! No fields of its own. `buffer:`, including `buffer.disk`, works as on any sink, so a scenario
//! can measure a disk spool with no destination behind it.
//!
//! ## Telemetry
//!
//! Layer 2 only: the write loop's `logit.component.batches.received`,
//! `logit.component.events.received`, and `logit.component.send.duration` still count every batch.
//! This sink emits nothing of its own.
//!
//! ## Faults
//!
//! [`NullOutput::send`] never fails, so its delivery posture, the default, never comes into play.

use logit_core::EventBatch;
use logit_pipeline::{BatchView, Output};

/// `logit_pipeline::Output` for `null_out`. Stateless, so no builder.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullOutput;

#[async_trait::async_trait]
impl Output for NullOutput {
    /// Drops `batch` and returns `Ok`.
    async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
        Ok(())
    }

    /// Drops `view` without reading or materializing it.
    async fn send_view(&mut self, _view: &BatchView) -> anyhow::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Event, Resource};
    use std::sync::Arc;

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    #[tokio::test]
    async fn send_accepts_any_batch_and_returns_ok() {
        let mut output = NullOutput;
        assert!(output.send(&batch(vec![Event::empty(0, AttrMap::new())])).await.is_ok());
        assert!(output.send(&batch(vec![])).await.is_ok());
    }
}
