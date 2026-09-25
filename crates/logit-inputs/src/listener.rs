//! Per-connection bookkeeping every connection-oriented listener shares: the
//! `logit.input.connections` gauge (`docs/design/internal-telemetry.md`).
//!
//! The gauge is a drop guard ([`LiveConnection`]) rather than an increment and a decrement around
//! a connection's serving future, so every way a connection task ends, a panic included, brings
//! it back down. tokio runs a task's `poll` under `catch_unwind` and drops the task's future on a
//! panic, which runs the guard's `Drop`; no build profile sets `panic = "abort"`.

use logit_core::Telemetry;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// One listener's count of connections holding a permit, and the handle it publishes through.
/// Cloning shares the count, so a listener with more than one accept loop
/// (`datadog_trace_in`'s TCP and Unix sockets) reports one gauge.
#[derive(Clone)]
pub(crate) struct LiveConnections {
    count: Arc<AtomicI64>,
    telemetry: Telemetry,
}

impl LiveConnections {
    pub(crate) fn new(telemetry: Telemetry) -> Self {
        Self { count: Arc::new(AtomicI64::new(0)), telemetry }
    }

    /// Counts one connection in and publishes the new value. The count comes back down when the
    /// returned guard drops.
    pub(crate) fn enter(&self) -> LiveConnection {
        let live = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        self.publish(live);
        LiveConnection(self.clone())
    }

    /// The current count, for a test that holds the handle rather than a `Registry`.
    #[cfg(test)]
    pub(crate) fn count(&self) -> i64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Publishes from the read-modify-write's return value, never a separate `load`:
    /// `Telemetry::gauge` is last-write-wins per key, so two tasks interleaving a change and a load
    /// would leave the stale value published until the next transition.
    fn publish(&self, live: i64) {
        self.telemetry.gauge("logit.input.connections", live as f64, &[]);
    }
}

/// One live connection, held for as long as its task runs. See [`LiveConnections::enter`].
pub(crate) struct LiveConnection(LiveConnections);

impl Drop for LiveConnection {
    fn drop(&mut self) {
        let live = self.0.count.fetch_sub(1, Ordering::Relaxed) - 1;
        self.0.publish(live);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{MetricKind, Registry};

    fn gauge(registry: &Registry) -> Option<f64> {
        registry.drain(0).iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match m.kind {
                MetricKind::Gauge(v)
                    if logit_core::interner::resolve(m.name) == "logit.input.connections" =>
                {
                    Some(v)
                }
                _ => None,
            })
        })
    }

    #[tokio::test]
    async fn a_panicking_connection_task_still_returns_the_gauge_to_zero() {
        let registry = Registry::new();
        let live = LiveConnections::new(registry.telemetry_for("in", "otlp_in", "listener"));

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn({
            let live = live.clone();
            async move {
                let _connection = live.enter();
                entered_tx.send(()).unwrap();
                tokio::task::yield_now().await;
                panic!("a connection task panicking mid-serve");
            }
        });
        entered_rx.await.unwrap();
        assert_eq!(gauge(&registry), Some(1.0), "counted in while the task runs");

        assert!(handle.await.unwrap_err().is_panic(), "the task ended in a panic");
        assert_eq!(gauge(&registry), Some(0.0), "the unwind dropped the guard");
    }

    #[test]
    fn clones_share_one_count() {
        let registry = Registry::new();
        let live = LiveConnections::new(registry.telemetry_for("in", "otlp_in", "listener"));
        let first = live.enter();
        let second = live.clone().enter();
        assert_eq!(gauge(&registry), Some(2.0));
        drop(first);
        drop(second);
        assert_eq!(gauge(&registry), Some(0.0));
    }
}
