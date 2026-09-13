//! A sink that drops everything, as cheaply as the runtime allows -- the sink end of the perf
//! harness (`docs/plans/load-test-harness.md`). Pairs with a `generate_in` listener to measure
//! everything *upstream* of a sink with no real encoder, socket, or filesystem in the number.
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
//! No fields of its own. `buffer:` (including `buffer.disk`) works exactly as it does on every
//! other sink -- `build_spec` gives this the same `queue_config`/`write_config` treatment
//! (`crates/logit-cli/src/pipeline.rs`), so a scenario can put a real disk-backed queue in front
//! of this sink to measure the spool without a real destination behind it.
//!
//! ## Faults
//!
//! [`NullOutput::send`] never fails, so it attaches no [`logit_pipeline::Fault`] context --
//! `logit_pipeline::classify`'s conservative "no marker found" default is never reached because
//! there is never an `Err` to classify in the first place.
//!
//! ## Telemetry
//!
//! **Layer 2 only** -- `logit.component.batches.received`/`logit.component.events.received`/
//! `logit.component.send.duration` come from the generic write loop
//! (`crates/logit-pipeline/src/runtime.rs`) that wraps every sink's `send` call. A dedicated
//! counter here would just duplicate `events.received` for a sink that does no work of its own to
//! report on.
//!
//! ## Duplicate safety
//!
//! [`NullOutput::duplicate_safe`] is `true`: `send` has no side effect and no destination to
//! double-write to -- dropping a batch twice is still just dropping it.
//!
//! Two intended uses: a load-test scenario's sink (`perf/scenarios/*.yaml`,
//! `docs/plans/load-test-harness.md`), and validating the front half of a config -- a listener,
//! its parsing/transform chain -- against a real running process with nothing on the other end.

use logit_core::EventBatch;
use logit_pipeline::Output;

/// `logit_pipeline::Output` for `null_out`. Unit-like and `Copy` -- there is no state to hold and
/// no builder needed, unlike every other sink in this crate.
#[derive(Debug, Default, Clone, Copy)]
pub struct NullOutput;

#[async_trait::async_trait]
impl Output for NullOutput {
    /// Drops `batch` and returns immediately. No encoding, no I/O -- see the module doc for why
    /// there is nothing else here, including no early return for an empty batch: an empty batch
    /// costs exactly as little as a non-empty one already.
    async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
        Ok(())
    }

    /// See the module doc's "Duplicate safety" section.
    fn duplicate_safe(&self) -> bool {
        true
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

    #[test]
    fn null_out_is_duplicate_safe() {
        assert!(NullOutput.duplicate_safe());
    }
}
