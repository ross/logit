//! [`Fanout`]: the outbound side of a graph node. Every non-sink component (listener, transform,
//! Lua stage) sends what it produces through one of these -- one `mpsc::Sender` per consumer,
//! resolved from the inverted `sources` relation at graph-build time
//! (`docs/design/pipeline-graph.md`'s "Runtime model").
//!
//! The channel payload is [`Delivered`], not a bare `EventBatch`
//! (`docs/adr/0016-arc-eventbatch-copy-on-write.md`). `send`/`send_blocking` still take an owned
//! `EventBatch` -- callers construct one exactly as before -- but an edge with exactly one
//! consumer (the common case: a linear chain, and every shipped listener's first hop) moves it
//! through as `Delivered::Owned`, with no `Arc` involved at all. Only a real fan-out (more than one
//! consumer) wraps the batch in an `Arc` and hands out `Delivered::Shared` clones -- a refcount
//! bump, not a deep clone. The consuming side handles either variant per node kind: `run_output`
//! (`runtime.rs`) borrows `&EventBatch` straight out of either variant -- `Output::send` takes a
//! reference, so this is where the fan-out saving actually lands -- while `run_transform`/`run_lua`
//! still call `unwrap_batch` to get an owned `EventBatch`, since `Transform::process`/
//! `ScriptWorker::process` need to mutate or consume an owned `Event`. (A listener's own inbox is
//! never fed at all -- arity rules out a `sources` entry pointing at one -- so `Input` never
//! receives a `Delivered` either way.)

use logit_core::{EventBatch, Telemetry};
use std::sync::Arc;
use tokio::sync::mpsc;

/// What travels one graph edge. `Fanout::send`/`send_blocking` pick the variant per send based on
/// how many consumers that `Fanout` has -- a property of the edge, not of the batch itself.
pub enum Delivered {
    /// This edge's `Fanout` had exactly one consumer: the batch moved through with no `Arc`
    /// allocated at all. The common case -- every listener's first hop, and every interior edge of
    /// a linear chain (the v0.1 reference config's `statsd_in -> aggregate -> lua -> influxdb_out`
    /// among them).
    Owned(EventBatch),
    /// This edge's `Fanout` had more than one consumer: every one of them holds a handle to the
    /// same `Arc`-wrapped batch. Which handle (if any) gets to reclaim the batch without cloning is
    /// decided at the consuming end, by which one happens to be dropped last at runtime -- there is
    /// no privileged branch, and under concurrent consumption more than one can end up cloning; see
    /// `runtime.rs`'s `unwrap_batch`.
    Shared(Arc<EventBatch>),
}

/// A node's outbound edges. Fan-in (multiple sources feeding one component) is free -- it's just
/// N cloned `Sender`s feeding the same inbox on the consumer's side, nothing this type needs to
/// know about. Fan-out (one component feeding several consumers) is what this type exists to make
/// cheap to get right: a single-consumer edge moves the batch through for free, and only a real
/// fan-out pays for an `Arc`.
///
/// This is also the one choke point every producer node -- a listener, a `Transform`, a Lua
/// component -- sends through, regardless of kind, which is what makes it the natural place to
/// record the uniform "how much did this component produce, and how long did sending it take"
/// telemetry (`docs/design/internal-telemetry.md`) for every one of them without adding any code
/// to `run_input`/`run_transform`/`run_lua` (`crates/logit-pipeline/src/runtime.rs`) individually.
/// [`Fanout::with_telemetry`] attaches the producing component's own handle;
/// [`Fanout::default`]/[`Fanout::new`] leave it [`Telemetry::default`] (disabled, zero-cost).
#[derive(Clone, Default)]
pub struct Fanout {
    consumers: Vec<mpsc::Sender<Delivered>>,
    telemetry: Telemetry,
}

impl Fanout {
    pub fn new(consumers: Vec<mpsc::Sender<Delivered>>) -> Self {
        Self { consumers, telemetry: Telemetry::default() }
    }

    /// Attaches the producing component's telemetry handle -- see this type's doc comment.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    /// Sends `batch` to every consumer. A closed consumer is silently skipped -- see
    /// `docs/design/pipeline-graph.md`'s backpressure section: propagating a closed downstream as
    /// a real shutdown signal is a named open question, not solved here -- but it's no longer
    /// silent to telemetry: a closed-consumer send counts toward
    /// `logit.component.events.dropped{reason="closed_consumer"}`.
    ///
    /// Exactly one consumer: `batch` moves through as [`Delivered::Owned`], no `Arc` involved at
    /// all -- this is what keeps a linear chain (no fan-out anywhere on it) free of this change's
    /// cost entirely. More than one consumer: wraps `batch` in an `Arc` once, then clones the `Arc`
    /// (a refcount bump, not a deep clone) for every consumer but the last, which gets it moved --
    /// saving one atomic increment/decrement pair, not a structural privilege (see
    /// [`Delivered::Shared`]'s doc comment).
    pub async fn send(&self, batch: EventBatch) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        let n = batch.events.len();
        self.record_send(n);
        let timer = self.telemetry.timer("logit.component.send.blocked.duration");
        if rest.is_empty() {
            if last.send(Delivered::Owned(batch)).await.is_err() {
                self.record_dropped_on_close(n);
            }
            return;
        }
        let batch = Arc::new(batch);
        for tx in rest {
            if tx.send(Delivered::Shared(batch.clone())).await.is_err() {
                self.record_dropped_on_close(n);
            }
        }
        if last.send(Delivered::Shared(batch)).await.is_err() {
            self.record_dropped_on_close(n);
        }
        drop(timer);
    }

    /// The `blocking_send` equivalent of [`Fanout::send`], for a node running on a plain OS
    /// thread rather than as a tokio task (a Lua node -- see
    /// `docs/design/pipeline-graph.md`'s "Thread model" section).
    pub fn send_blocking(&self, batch: EventBatch) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        let n = batch.events.len();
        self.record_send(n);
        let timer = self.telemetry.timer("logit.component.send.blocked.duration");
        if rest.is_empty() {
            if last.blocking_send(Delivered::Owned(batch)).is_err() {
                self.record_dropped_on_close(n);
            }
            return;
        }
        let batch = Arc::new(batch);
        for tx in rest {
            if tx.blocking_send(Delivered::Shared(batch.clone())).is_err() {
                self.record_dropped_on_close(n);
            }
        }
        if last.blocking_send(Delivered::Shared(batch)).is_err() {
            self.record_dropped_on_close(n);
        }
        drop(timer);
    }

    /// One batch, `n` events, about to be offered to every consumer -- counted once here rather
    /// than once per consumer, since fan-out to several consumers still represents one batch this
    /// component produced, not several.
    fn record_send(&self, n: usize) {
        self.telemetry.count("logit.component.batches.sent", 1.0, &[]);
        self.telemetry.count("logit.component.events.sent", n as f64, &[]);
    }

    /// `n` events that this one consumer's copy of the batch never delivered, because its channel
    /// was already closed.
    fn record_dropped_on_close(&self, n: usize) {
        self.telemetry.count(
            "logit.component.events.dropped",
            n as f64,
            &[("reason", "closed_consumer")],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, MetricKind, Registry, Resource};

    /// `Delivered` had no size assertion at all before the internal-spans costing exercise
    /// (`docs/known-gaps.md`'s internal-spans entry) went looking for one -- added regardless of
    /// that exercise's outcome (a prototype `TraceContext` field was measured and reverted; see
    /// `docs/design/memory.md`'s "Costing internal spans" section for the numbers), because it's
    /// the guard that should have existed either way. `Owned`'s `EventBatch` (32 bytes: an
    /// `Arc<Resource>` pointer plus a `Vec<Event>`) is the larger variant, and it fits with no
    /// separate discriminant byte -- the `Vec`'s non-null pointer gives the compiler a niche to
    /// fold the tag into for free, the same trick that makes `Option<SpanRecord>` cost nothing
    /// over `SpanRecord` (`crates/logit-core/tests/type_sizes.rs`).
    #[test]
    fn delivered_is_32_bytes_no_wider_than_its_larger_variant() {
        assert_eq!(std::mem::size_of::<Delivered>(), 32);
    }

    fn batch(n: usize) -> EventBatch {
        EventBatch {
            resource: Arc::new(Resource::default()),
            events: (0..n).map(|_| logit_core::Event::empty(0, AttrMap::new())).collect(),
        }
    }

    fn counter_value(events: &[logit_core::Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v) if logit_core::interner::resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    #[tokio::test]
    async fn a_disabled_fanout_records_nothing() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        fanout.send(batch(3)).await;
        assert!(rx.recv().await.is_some());
        // Nothing to drain -- no `Registry` was ever attached, so `Telemetry::default()` inside
        // `Fanout` recorded nothing. Nothing to assert directly beyond "this doesn't panic and
        // the batch still arrives" -- covered above.
    }

    #[tokio::test]
    async fn a_single_consumer_send_counts_one_batch_and_its_events() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("in", "statsd_in", "listener");
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry);

        fanout.send(batch(3)).await;
        rx.recv().await.expect("should receive");

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.component.batches.sent"), Some(1.0));
        assert_eq!(counter_value(&events, "logit.component.events.sent"), Some(3.0));
    }

    #[tokio::test]
    async fn a_fan_out_to_two_consumers_still_counts_one_batch() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("in", "statsd_in", "listener");
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx_a, tx_b]).with_telemetry(telemetry);

        fanout.send(batch(2)).await;
        rx_a.recv().await.expect("a should receive");
        rx_b.recv().await.expect("b should receive");

        let events = registry.drain(0);
        assert_eq!(
            counter_value(&events, "logit.component.batches.sent"),
            Some(1.0),
            "one batch fanning out to two consumers is still one batch produced"
        );
        assert_eq!(counter_value(&events, "logit.component.events.sent"), Some(2.0));
    }

    #[tokio::test]
    async fn sending_into_a_closed_consumer_counts_as_dropped_not_silent() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("in", "statsd_in", "listener");
        let (tx, rx) = mpsc::channel(1);
        drop(rx); // closed before the send below
        let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry);

        fanout.send(batch(4)).await;

        let events = registry.drain(0);
        assert_eq!(
            counter_value(&events, "logit.component.events.dropped"),
            Some(4.0),
            "every event in the batch should count as dropped, not just the batch"
        );
    }
}
