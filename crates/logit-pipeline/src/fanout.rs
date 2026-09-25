//! [`Fanout`]: the outbound side of a graph node. Every non-sink component (listener, transform,
//! Lua stage) sends what it produces through one of these -- one `mpsc::Sender` per consumer,
//! resolved from the inverted `sources` relation at graph-build time
//! (`docs/design/pipeline-graph.md`'s "Runtime model").
//!
//! The channel payload is [`Delivered`], not a bare `EventBatch`
//! (`docs/adr/arc-eventbatch-copy-on-write.md`). The move-vs-clone rule: an edge with one
//! consumer (a linear chain, every listener's first hop) moves the batch through as
//! `Delivered::Owned` with no `Arc`; only a real fan-out wraps it in one `Arc` and hands out
//! `Delivered::Shared` refcount bumps, never a deep clone. `run_output` borrows `&EventBatch` out
//! of either variant, which is where the fan-out saving lands; `run_transform`/`run_lua` call
//! `unwrap_batch` for an owned batch, which deep-clones only a contended `Shared`
//! (`docs/design/memory.md` pins those allocation counts). A listener's inbox is never fed, so
//! `Input` never receives a `Delivered`.
//!
//! Every `Delivered` also carries a [`BatchContext`]: a [`TraceContext`]
//! (`docs/adr/trace-context-propagation-on-delivered.md`) and a [`Provenance`], which node created
//! the batch and which last handed it off (`docs/adr/batch-provenance-on-delivered.md`).
//! `Fanout::send`/`send_blocking`/`send_with_deadline`/`send_reserved` record a listener's span
//! around the send (`docs/adr/internal-span-emission-and-deterministic-sampling.md`).
//!
//! **Which send a listener calls.** `send` delivers consumer by consumer, so a send dropped while
//! parked on a full consumer leaves the ones before it holding the batch and the rest without it.
//! A listener whose send no peer can cancel calls `send` (the UDP, TCP, and Unix drivers,
//! `logit_in`'s `send_relayed`, `generate_in`, `internal`, `prometheus_in`'s scrape tick). A
//! listener whose send runs inside an HTTP handler has it dropped by its peer: a client that closes
//! mid-request drops hyper's service future on h1, and an h2 `RST_STREAM` cancels the stream's
//! task. Those reserve a slot on every consumer before delivering to any, so a dropped send
//! reaches every consumer or none and a client's retry never duplicates on one branch:
//! `send_reserved` for `otlp_in` and `prometheus_in`'s receiver, `send_with_deadline` for the two
//! Datadog listeners, which also answer `503` when the wait runs out. See
//! `docs/design/pipeline-graph.md`'s "Trace context propagation" section for which node kinds
//! propagate a parent and which mint a root, and its "Provenance propagation" section for the
//! stamping rule ([`Fanout::stamp`]/[`Fanout::stamp_relayed`]).

use logit_core::interner::intern;
use logit_core::random_id_bytes;
use logit_core::{EventBatch, Provenance, SpanKind, Symbol, Telemetry};
use std::sync::Arc;
use tokio::sync::mpsc;

/// One batch's place in a trace: which trace it belongs to, and which span produced it. `Copy`,
/// 24 bytes, carried on every [`Delivered`] whether or not anything turns it into a span.
///
/// `trace_id` is set once, at a trace's origin, and never changes. [`TraceContext::child`] keeps
/// it and mints a fresh `span_id` at every hop, so a hop's `span_id` is the next hop's span's
/// `parent_span_id`.
///
/// Only a node with one incoming batch per emission (`Transform::process`/
/// `ScriptWorker::process`) produces a `child`. A flush (`Transform::flush`, Lua's timer-driven
/// `flush()`) is built from however many batches arrived since the last tick, has no single
/// parent, and mints a [`TraceContext::new_root`]
/// (`docs/adr/trace-context-propagation-on-delivered.md`). `docs/known-gaps.md`'s internal-spans
/// entry names what is still open.
///
/// `Default` is the all-zero context, for tests and benches that build a `Delivered` directly;
/// `Fanout` never uses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
}

impl TraceContext {
    /// A fresh, unrelated context. Used at a trace's origin (a listener's batches) and at every
    /// flush-driven emission.
    pub fn new_root() -> Self {
        TraceContext { trace_id: random_id_bytes(), span_id: random_id_bytes() }
    }

    /// A context for what a node emits from processing one incoming batch carrying `self`: same
    /// `trace_id`, fresh `span_id`.
    pub fn child(&self) -> Self {
        TraceContext { trace_id: self.trace_id, span_id: random_id_bytes() }
    }
}

/// What travels with a batch but not in it: its trace/span ([`TraceContext`]) and which
/// components created and last handled it ([`Provenance`],
/// `docs/adr/batch-provenance-on-delivered.md`). The two travel through the same places
/// (`Delivered`, `SinkQueue`, the per-batch hooks, the disk-queue record), so they are one struct.
///
/// `Provenance` is defined in `logit-core`, not here, so `logit-proto` and `logit-script`, which
/// can't depend on this crate, can name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BatchContext {
    pub trace: TraceContext,
    pub provenance: Provenance,
}

impl From<TraceContext> for BatchContext {
    /// Pairs a trace context with empty provenance.
    fn from(trace: TraceContext) -> Self {
        BatchContext { trace, provenance: Provenance::default() }
    }
}

/// What travels one graph edge. The variant is picked per send from how many consumers the
/// `Fanout` has: a property of the edge, not the batch.
pub enum Delivered {
    /// The `Fanout` had one consumer: the batch moved through with no `Arc`. The common case:
    /// every listener's first hop and every edge of a linear chain.
    Owned(EventBatch, BatchContext),
    /// The `Fanout` had more than one consumer, each holding a handle to the same `Arc`. No
    /// branch is privileged: whichever handle is unwrapped last reclaims the batch without
    /// cloning, and under concurrent consumption more than one can clone (see `unwrap_batch`).
    Shared(Arc<EventBatch>, BatchContext),
}

impl Delivered {
    /// This batch's `TraceContext`. Read it before `unwrap_batch` consumes the `Delivered`, to
    /// use as the parent for what the consuming node emits.
    pub fn context(&self) -> TraceContext {
        self.batch_context().trace
    }

    /// This batch's [`Provenance`]: which component created it, and which this node received it
    /// from. Read it before `unwrap_batch`, as with [`Delivered::context`].
    pub fn provenance(&self) -> Provenance {
        self.batch_context().provenance
    }

    /// The full [`BatchContext`], for a caller that threads it along (`run_transform` passing it
    /// to `Fanout::send_with_own_context`).
    pub fn batch_context(&self) -> BatchContext {
        match self {
            Delivered::Owned(_, ctx) => *ctx,
            Delivered::Shared(_, ctx) => *ctx,
        }
    }
}

/// [`Fanout::send_with_deadline`]'s deadline passed before every consumer had room for the
/// batch, so no consumer received it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendTimeout;

impl std::fmt::Display for SendTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("not every consumer accepted the batch before the deadline; none received it")
    }
}

impl std::error::Error for SendTimeout {}

/// A node's outbound edges. Fan-in is N cloned `Sender`s feeding one inbox and needs nothing
/// here. Fan-out moves the batch through a single-consumer edge and pays for an `Arc` only on a
/// real fan-out.
///
/// Every producer node (listener, `Transform`, Lua component) sends through this type, so it
/// records the uniform sent/blocked telemetry (`docs/design/internal-telemetry.md`) for all of
/// them. [`Fanout::with_telemetry`] attaches the producing component's handle;
/// [`Fanout::default`]/[`Fanout::new`] leave it [`Telemetry::default`] (disabled).
#[derive(Clone, Default)]
pub struct Fanout {
    consumers: Vec<mpsc::Sender<Delivered>>,
    telemetry: Telemetry,
    /// This node's id, interned once at graph-build time so stamping never interns on the hot
    /// path. `None` (a `Fanout` built without `with_component`, as tests do) makes stamping inert:
    /// provenance passes through unchanged.
    component: Option<Symbol>,
}

impl Fanout {
    pub fn new(consumers: Vec<mpsc::Sender<Delivered>>) -> Self {
        Self { consumers, telemetry: Telemetry::default(), component: None }
    }

    /// Attaches the producing component's telemetry handle.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Attaches this node's id, which every send stamps into the outgoing batch's
    /// [`Provenance`]. Interns `id` once, here.
    pub fn with_component(mut self, id: &str) -> Self {
        self.component = Some(intern(id));
        self
    }

    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    /// The stamping rule for every non-relaying send: `origin` is set once and never overwritten,
    /// `previous` is always rewritten to this node's id. See [`Fanout::stamp_relayed`] for
    /// `logit_in`'s rule.
    fn stamp(&self, mut ctx: BatchContext) -> BatchContext {
        if let Some(me) = self.component {
            ctx.provenance.origin.get_or_insert(me);
            ctx.provenance.previous = Some(me);
        }
        ctx
    }

    /// `logit_in`'s rule: back-fill only what the wire didn't carry (a v1 peer, or a v2 peer with
    /// none), so `logit_out -> logit_in` preserves a peer's `origin`/`previous` and never leaves
    /// either empty.
    fn stamp_relayed(&self, mut ctx: BatchContext) -> BatchContext {
        if let Some(me) = self.component {
            ctx.provenance.origin.get_or_insert(me);
            ctx.provenance.previous.get_or_insert(me);
        }
        ctx
    }

    /// Sends `batch` as a new trace root: the call for a listener, which has no incoming batch to
    /// inherit a parent from. See [`Fanout::send_with_own_context`] for delivery mechanics.
    ///
    /// This is where a listener's `SpanKind::Producer` span is recorded
    /// (`docs/adr/internal-span-emission-and-deterministic-sampling.md`'s per-node-kind table).
    /// Its window is this call only: `Input::run` is a free-form loop, so the time spent building
    /// `batch` is unknowable here. Only listeners call this, so one `send` is one listener
    /// emission.
    pub async fn send(&self, batch: EventBatch) {
        let ctx = TraceContext::new_root();
        let mut span =
            self.telemetry.span("send", SpanKind::Producer, ctx.trace_id, ctx.span_id, None);
        span.events(batch.events.len() as u64);
        self.send_with_own_context(batch, ctx.into()).await;
    }

    /// Sends `batch` to every consumer as a [`TraceContext::child`] of `parent`, recording no span
    /// and carrying empty incoming provenance. A caller with provenance to propagate, or a span
    /// of its own around the send, calls [`Fanout::send_with_own_context`] instead.
    pub async fn send_with_context(&self, batch: EventBatch, parent: TraceContext) {
        self.send_with_own_context(batch, parent.child().into()).await
    }

    /// Sends `batch` to every consumer under `ctx` as the caller minted it; every other `send*`
    /// is built from this. A node recording its own span around the send calls this so the span's
    /// `span_id` and the outgoing `Delivered`'s are the same id.
    ///
    /// Stamps `ctx.provenance` via [`Fanout::stamp`]; `Fanout` is the only writer of
    /// `origin`/`previous` (`docs/adr/batch-provenance-on-delivered.md`).
    ///
    /// A closed consumer is skipped and its events counted as
    /// `logit.component.events.dropped{reason="closed_consumer"}`. Propagating a closed downstream
    /// as a shutdown signal is an open question (`docs/design/pipeline-graph.md`'s backpressure
    /// section).
    ///
    /// One consumer gets the batch moved as [`Delivered::Owned`]. With more, the batch is wrapped
    /// in one `Arc`, cloned for every consumer but the last, which gets it moved (saving one
    /// refcount pair, not a privilege). Every consumer gets the same context: one fan-out is one
    /// emission.
    pub async fn send_with_own_context(&self, batch: EventBatch, ctx: BatchContext) {
        let ctx = self.stamp(ctx);
        self.deliver(batch, ctx).await;
    }

    /// [`Fanout::send`] with all-edges-or-nothing delivery under `deadline`: the call for a
    /// listener that answers "busy" rather than blocking its client (`datadog_in`).
    ///
    /// Mints the root context, the Producer span, and the provenance stamp exactly as `send` does.
    /// Then, before sending anything, it reserves a slot on every consumer's channel, all held at
    /// once. If `deadline` passes first, it returns [`SendTimeout`] with nothing sent: the held
    /// slots are released, no consumer receives the batch, `batches.sent`/`events.sent` are not
    /// counted, and neither the span nor the `send.blocked.duration` sample is recorded. Once every
    /// slot is held, the batch goes to every consumer (moved to one, shared as in `deliver` to
    /// several), then counts as sent and finishes the span.
    ///
    /// A closed consumer is skipped and counted `events.dropped{reason="closed_consumer"}`, as in
    /// `deliver`, but only once the batch actually goes out.
    pub async fn send_with_deadline(
        &self,
        batch: EventBatch,
        deadline: tokio::time::Instant,
    ) -> Result<(), SendTimeout> {
        self.send_all_or_nothing(batch, Some(deadline)).await
    }

    /// [`Fanout::send`] with [`Fanout::send_with_deadline`]'s all-edges-or-nothing delivery and no
    /// deadline: the call for a listener whose handler can be cancelled mid-send (`otlp_in`,
    /// `prometheus_in`'s receiver). Dropping the future before every slot is held sends nothing
    /// and records nothing, where `send` would leave the consumers it already reached holding the
    /// batch and the rest without it.
    pub async fn send_reserved(&self, batch: EventBatch) {
        // Without a deadline the reservation only ends by completing or by being dropped.
        let _always_ok = self.send_all_or_nothing(batch, None).await;
    }

    /// The reserve-every-slot-then-deliver path behind [`Fanout::send_with_deadline`] and
    /// [`Fanout::send_reserved`]. `Err` only when `deadline` is `Some` and passes.
    async fn send_all_or_nothing(
        &self,
        batch: EventBatch,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<(), SendTimeout> {
        let trace = TraceContext::new_root();
        let mut span =
            self.telemetry.span("send", SpanKind::Producer, trace.trace_id, trace.span_id, None);
        span.events(batch.events.len() as u64);
        let ctx = self.stamp(trace.into());
        if self.consumers.is_empty() {
            return Ok(());
        }
        let timer = self.telemetry.timer("logit.component.send.blocked.duration");
        // Until every slot is held, a timeout or a dropped future discards both unrecorded.
        let mut unsent = Unsent(Some((span, timer)));
        let reserve_all = async {
            let mut permits = Vec::with_capacity(self.consumers.len());
            for tx in &self.consumers {
                // `Err` is a closed consumer: nothing to wait for, counted once the batch goes out.
                permits.push(tx.reserve().await.ok());
            }
            permits
        };
        let permits = match deadline {
            // Dropping the partial `permits` inside the cancelled future releases every held slot.
            Some(deadline) => match tokio::time::timeout_at(deadline, reserve_all).await {
                Ok(permits) => permits,
                Err(_elapsed) => return Err(SendTimeout),
            },
            None => reserve_all.await,
        };
        let (span, timer) = unsent.0.take().expect("taken only here, once every slot is held");
        let n = batch.events.len();
        if permits.len() == 1 {
            match permits.into_iter().next().flatten() {
                Some(permit) => permit.send(Delivered::Owned(batch, ctx)),
                None => self.record_dropped_on_close(n),
            }
        } else {
            let batch = Arc::new(batch);
            for permit in permits {
                match permit {
                    Some(permit) => permit.send(Delivered::Shared(batch.clone(), ctx)),
                    None => self.record_dropped_on_close(n),
                }
            }
        }
        self.record_send(n);
        drop(timer);
        span.finish();
        Ok(())
    }

    /// `logit_in`'s send: [`Fanout::send`] (a fresh root and a listener span), stamped with
    /// [`Fanout::stamp_relayed`]'s back-fill rule so a peer's relayed `origin`/`previous` survive.
    /// See `docs/design/wire-protocol.md`'s `logit_out`/`logit_in` section.
    pub async fn send_relayed(&self, batch: EventBatch, provenance: Provenance) {
        let trace = TraceContext::new_root();
        let mut span =
            self.telemetry.span("send", SpanKind::Producer, trace.trace_id, trace.span_id, None);
        span.events(batch.events.len() as u64);
        let ctx = self.stamp_relayed(BatchContext { trace, provenance });
        self.deliver(batch, ctx).await;
    }

    /// The per-consumer delivery loop, with `ctx` already stamped.
    async fn deliver(&self, batch: EventBatch, ctx: BatchContext) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        let n = batch.events.len();
        self.record_send(n);
        let timer = self.telemetry.timer("logit.component.send.blocked.duration");
        if rest.is_empty() {
            if last.send(Delivered::Owned(batch, ctx)).await.is_err() {
                self.record_dropped_on_close(n);
            }
            return;
        }
        let batch = Arc::new(batch);
        for tx in rest {
            if tx.send(Delivered::Shared(batch.clone(), ctx)).await.is_err() {
                self.record_dropped_on_close(n);
            }
        }
        if last.send(Delivered::Shared(batch, ctx)).await.is_err() {
            self.record_dropped_on_close(n);
        }
        drop(timer);
    }

    /// The `blocking_send` equivalent of [`Fanout::send`], for a node on a plain OS thread (a Lua
    /// node; `docs/design/pipeline-graph.md`'s "Thread model" section).
    pub fn send_blocking(&self, batch: EventBatch) {
        let ctx = TraceContext::new_root();
        let mut span =
            self.telemetry.span("send", SpanKind::Producer, ctx.trace_id, ctx.span_id, None);
        span.events(batch.events.len() as u64);
        self.send_blocking_with_own_context(batch, ctx.into());
    }

    /// The `blocking_send` equivalent of [`Fanout::send_with_context`].
    pub fn send_blocking_with_context(&self, batch: EventBatch, parent: TraceContext) {
        self.send_blocking_with_own_context(batch, parent.child().into())
    }

    /// The `blocking_send` equivalent of [`Fanout::send_with_own_context`], stamping included.
    pub fn send_blocking_with_own_context(&self, batch: EventBatch, ctx: BatchContext) {
        let ctx = self.stamp(ctx);
        self.deliver_blocking(batch, ctx);
    }

    /// The `blocking_send` equivalent of [`Fanout::send_relayed`]. No shipped component calls it
    /// (`logit_in` is a tokio task); it completes the blocking/async pairing.
    pub fn send_relayed_blocking(&self, batch: EventBatch, provenance: Provenance) {
        let trace = TraceContext::new_root();
        let mut span =
            self.telemetry.span("send", SpanKind::Producer, trace.trace_id, trace.span_id, None);
        span.events(batch.events.len() as u64);
        let ctx = self.stamp_relayed(BatchContext { trace, provenance });
        self.deliver_blocking(batch, ctx);
    }

    /// The blocking twin of [`Fanout::deliver`].
    fn deliver_blocking(&self, batch: EventBatch, ctx: BatchContext) {
        let Some((last, rest)) = self.consumers.split_last() else { return };
        let n = batch.events.len();
        self.record_send(n);
        let timer = self.telemetry.timer("logit.component.send.blocked.duration");
        if rest.is_empty() {
            if last.blocking_send(Delivered::Owned(batch, ctx)).is_err() {
                self.record_dropped_on_close(n);
            }
            return;
        }
        let batch = Arc::new(batch);
        for tx in rest {
            if tx.blocking_send(Delivered::Shared(batch.clone(), ctx)).is_err() {
                self.record_dropped_on_close(n);
            }
        }
        if last.blocking_send(Delivered::Shared(batch, ctx)).is_err() {
            self.record_dropped_on_close(n);
        }
        drop(timer);
    }

    /// Counts one batch of `n` events, once rather than per consumer: a fan-out is still one
    /// batch produced.
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

/// The span and blocked-duration timer of an all-or-nothing send that has not sent yet. Dropped
/// while still holding them (a deadline passed, or the send's future was dropped), it cancels
/// both, so a send that never happened records nothing.
struct Unsent(Option<(logit_core::SpanGuard, logit_core::telemetry::Timer)>);

impl Drop for Unsent {
    fn drop(&mut self) {
        if let Some((span, timer)) = self.0.take() {
            timer.cancel();
            span.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, MetricKind, Registry, Resource};

    /// Pins `Delivered`'s size exactly, as `crates/logit-core/tests/type_sizes.rs` does for
    /// `Event`: `Owned`'s `EventBatch` (`Arc<Resource>` 8 + `Option<Arc<Scope>>` 8 + `Vec<Event>`
    /// 24) plus `BatchContext`'s 32 is 72, and the `Vec`'s non-null pointer gives a niche for the
    /// tag, so there is no discriminant byte. `docs/design/memory.md`'s "Costing internal spans"
    /// section has the breakdown.
    #[test]
    fn delivered_is_72_bytes_no_wider_than_its_larger_variant() {
        assert_eq!(std::mem::size_of::<Delivered>(), 72);
        // The niche is still available one level up.
        assert_eq!(std::mem::size_of::<Option<Delivered>>(), 72);
    }

    /// `BatchContext` adds no padding to `TraceContext` + `Provenance` (pinned in `logit-core`).
    #[test]
    fn batch_context_is_trace_context_plus_provenance_with_no_padding() {
        assert_eq!(std::mem::size_of::<BatchContext>(), 32);
    }

    /// `TraceContext::child` keeps `trace_id` and mints a fresh `span_id`.
    #[test]
    fn child_context_keeps_the_trace_id_and_mints_a_fresh_span_id() {
        let root = TraceContext::new_root();
        let child = root.child();
        assert_eq!(child.trace_id, root.trace_id);
        assert_ne!(child.span_id, root.span_id);
    }

    /// Smoke test that `random_id_bytes` isn't returning a constant.
    #[test]
    fn two_roots_are_not_the_same_context() {
        let a = TraceContext::new_root();
        let b = TraceContext::new_root();
        assert_ne!(a.trace_id, b.trace_id);
        assert_ne!(a.span_id, b.span_id);
    }

    /// Catches a `const` thread-local seed, which makes every thread's first id collide; the
    /// single-threaded test above can't see that.
    #[test]
    fn a_fresh_threads_first_root_differs_from_this_threads_first_root() {
        let here = TraceContext::new_root();
        let there =
            std::thread::spawn(TraceContext::new_root).join().expect("thread shouldn't panic");
        assert_ne!(
            here.trace_id, there.trace_id,
            "two threads' first-ever ids must not collide just because they're both first"
        );
    }

    fn batch(n: usize) -> EventBatch {
        EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: (0..n).map(|_| logit_core::Event::empty(0, AttrMap::new())).collect(),
        }
    }

    fn counter_value(events: &[logit_core::Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(s) if logit_core::interner::resolve(m.name) == name => {
                    Some(s.value)
                }
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
        // No `Registry` attached: the check is that the batch still arrives without a panic.
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

    fn timing_count(events: &[logit_core::Event], name: &str) -> usize {
        events
            .iter()
            .flat_map(|e| e.metrics.iter())
            .filter_map(|m| match &m.kind {
                MetricKind::Distribution(d) if logit_core::interner::resolve(m.name) == name => {
                    Some(d.count())
                }
                _ => None,
            })
            .sum()
    }

    fn in_ms(ms: u64) -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_millis(ms)
    }

    /// Asserts `events` hold no sent counts, no send span, and no `send.blocked.duration` sample:
    /// what a timed-out `send_with_deadline` must leave behind.
    fn assert_nothing_recorded_as_sent(events: &[logit_core::Event]) {
        assert_eq!(counter_value(events, "logit.component.batches.sent"), None);
        assert_eq!(counter_value(events, "logit.component.events.sent"), None);
        assert!(events.iter().all(|e| e.span.is_none()), "no span for a send that didn't happen");
        assert_eq!(timing_count(events, "logit.component.send.blocked.duration"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn send_with_deadline_to_one_consumer_with_room_delivers_and_counts_once() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("in", "datadog_in", "listener");
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry).with_component("dd_in");

        fanout.send_with_deadline(batch(3), in_ms(100)).await.expect("room: should send");

        let received = rx.recv().await.expect("should receive");
        assert!(matches!(received, Delivered::Owned(..)), "one consumer gets the batch moved");
        assert_eq!(received.provenance().origin_str(), Some("dd_in"));
        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.component.batches.sent"), Some(1.0));
        assert_eq!(counter_value(&events, "logit.component.events.sent"), Some(3.0));
        let span = events.iter().find_map(|e| e.span.as_ref()).expect("a send span");
        assert_eq!(span.span_id, received.context().span_id);
        assert_eq!(timing_count(&events, "logit.component.send.blocked.duration"), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn send_with_deadline_to_a_full_consumer_times_out_sending_and_counting_nothing() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("in", "datadog_in", "listener");
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Delivered::Owned(batch(1), BatchContext::default())).expect("prefill");
        let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry);

        let result = fanout.send_with_deadline(batch(3), in_ms(100)).await;

        assert_eq!(result, Err(SendTimeout));
        let prefilled = rx.recv().await.expect("the prefilled batch");
        assert_eq!(prefilled.batch_context(), BatchContext::default(), "only the prefill arrived");
        assert!(rx.try_recv().is_err(), "the timed-out batch must never be enqueued");
        assert_nothing_recorded_as_sent(&registry.drain(0));
    }

    /// The all-or-nothing case: the first consumer had room, the second didn't, so the first's
    /// reserved slot is released unused and neither receives the batch.
    #[tokio::test(start_paused = true)]
    async fn send_with_deadline_with_one_of_two_consumers_full_delivers_to_neither() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("in", "datadog_in", "listener");
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        tx_b.try_send(Delivered::Owned(batch(1), BatchContext::default())).expect("prefill b");
        let probe_a = tx_a.clone();
        let fanout = Fanout::new(vec![tx_a, tx_b]).with_telemetry(telemetry);

        let result = fanout.send_with_deadline(batch(2), in_ms(100)).await;

        assert_eq!(result, Err(SendTimeout));
        assert!(rx_a.try_recv().is_err(), "a must not hold a batch the request was refused for");
        assert_eq!(probe_a.capacity(), 1, "a's reserved slot must be released, not leaked");
        rx_b.recv().await.expect("b's prefill");
        assert!(rx_b.try_recv().is_err(), "b must not receive the timed-out batch either");
        assert_nothing_recorded_as_sent(&registry.drain(0));
    }

    #[tokio::test(start_paused = true)]
    async fn send_with_deadline_to_two_consumers_with_room_shares_one_batch_and_counts_once() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("in", "datadog_in", "listener");
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx_a, tx_b]).with_telemetry(telemetry);

        fanout.send_with_deadline(batch(2), in_ms(100)).await.expect("room: should send");

        let a = rx_a.recv().await.expect("a should receive");
        let b = rx_b.recv().await.expect("b should receive");
        assert!(matches!(a, Delivered::Shared(..)) && matches!(b, Delivered::Shared(..)));
        assert_eq!(a.batch_context(), b.batch_context(), "one fan-out is one emission");
        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.component.batches.sent"), Some(1.0));
        assert_eq!(counter_value(&events, "logit.component.events.sent"), Some(2.0));
    }

    #[tokio::test(start_paused = true)]
    async fn send_with_deadline_into_a_closed_consumer_counts_as_dropped_not_silent() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("in", "datadog_in", "listener");
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry);

        fanout.send_with_deadline(batch(4), in_ms(100)).await.expect("closed isn't a timeout");

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.component.events.dropped"), Some(4.0));
    }

    /// A send dropped while it waits on a full second consumer (an HTTP handler cancelled by its
    /// client going away) leaves neither consumer with the batch: the first's slot was only
    /// reserved, and dropping the future releases it.
    #[tokio::test(start_paused = true)]
    async fn a_send_cancelled_after_the_first_consumer_accepted_leaves_no_consumer_with_the_batch()
    {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("in", "otlp_in", "listener");
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        tx_b.try_send(Delivered::Owned(batch(1), BatchContext::default())).expect("prefill b");
        let probe_a = tx_a.clone();
        let fanout = Fanout::new(vec![tx_a, tx_b]).with_telemetry(telemetry);

        let mut send = Box::pin(fanout.send_reserved(batch(2)));
        tokio::select! {
            () = &mut send => panic!("b is full, so the send cannot complete"),
            () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
        drop(send);

        let a = rx_a.try_recv().is_ok();
        rx_b.recv().await.expect("b's prefill");
        let b = rx_b.try_recv().is_ok();
        assert_eq!((a, b), (false, false), "every consumer or none has the batch (a, b)");
        assert_eq!(probe_a.capacity(), 1, "a's reserved slot must be released, not leaked");
        assert_nothing_recorded_as_sent(&registry.drain(0));
    }

    /// `send` mints a fresh root every call.
    #[tokio::test]
    async fn send_mints_a_new_root_every_call() {
        let (tx, mut rx) = mpsc::channel(2);
        let fanout = Fanout::new(vec![tx]);

        fanout.send(batch(1)).await;
        fanout.send(batch(1)).await;

        let first = rx.recv().await.expect("should receive").context();
        let second = rx.recv().await.expect("should receive").context();
        assert_ne!(first.trace_id, second.trace_id, "unrelated sends should get unrelated traces");
    }

    /// `send_with_context` keeps the parent's `trace_id` and mints a fresh `span_id`.
    #[tokio::test]
    async fn send_with_context_propagates_the_trace_id_as_a_child_of_the_parent() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let parent = TraceContext::new_root();

        fanout.send_with_context(batch(1), parent).await;

        let received = rx.recv().await.expect("should receive").context();
        assert_eq!(received.trace_id, parent.trace_id);
        assert_ne!(received.span_id, parent.span_id, "each hop mints its own span id");
    }

    /// Every branch of a fan-out sees the same child context.
    #[tokio::test]
    async fn send_with_context_gives_every_fan_out_branch_the_same_child_context() {
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx_a, tx_b]);
        let parent = TraceContext::new_root();

        fanout.send_with_context(batch(1), parent).await;

        let a = rx_a.recv().await.expect("a should receive").context();
        let b = rx_b.recv().await.expect("b should receive").context();
        assert_eq!(a, b, "both branches of one fan-out should carry the identical child context");
        assert_eq!(a.trace_id, parent.trace_id);
    }

    /// `send`'s `SpanKind::Producer` span has the same `span_id` the batch went out under.
    #[tokio::test]
    async fn send_records_a_root_span_whose_span_id_is_the_context_it_sent_under() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("in", "statsd_in", "listener");
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry);

        fanout.send(batch(2)).await;
        let sent_ctx = rx.recv().await.expect("should receive").context();

        let events = registry.drain(0);
        let span_event = events.iter().find(|e| e.span.is_some()).expect("a span event");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(record.span_id, sent_ctx.span_id);
        assert_eq!(record.trace_id, sent_ctx.trace_id);
        assert_eq!(record.parent_span_id, None, "a listener span has no parent");
        assert_eq!(record.kind, logit_core::SpanKind::Producer);
    }

    /// A listener's `send` stamps both `origin` and `previous` with its own id.
    #[tokio::test]
    async fn a_listeners_first_send_stamps_both_origin_and_previous() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]).with_component("nginx_in");

        fanout.send(batch(1)).await;

        let provenance = rx.recv().await.expect("should receive").provenance();
        assert_eq!(provenance.origin_str(), Some("nginx_in"));
        assert_eq!(provenance.previous_str(), Some("nginx_in"));
    }

    /// A later hop keeps `origin` and rewrites `previous`.
    #[tokio::test]
    async fn an_interior_hop_rewrites_previous_and_keeps_origin() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]).with_component("enrich");
        let incoming = BatchContext {
            trace: TraceContext::new_root(),
            provenance: logit_core::Provenance {
                origin: Some(logit_core::interner::intern("nginx_in")),
                previous: Some(logit_core::interner::intern("nginx_in")),
            },
        };

        fanout.send_with_own_context(batch(1), incoming).await;

        let provenance = rx.recv().await.expect("should receive").provenance();
        assert_eq!(provenance.origin_str(), Some("nginx_in"), "origin must not change downstream");
        assert_eq!(provenance.previous_str(), Some("enrich"), "previous names the last hop");
    }

    /// A `Fanout` built without `with_component` passes provenance through unchanged.
    #[tokio::test]
    async fn a_fanout_with_no_component_passes_provenance_through_unchanged() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]); // no with_component
        let incoming = BatchContext {
            trace: TraceContext::new_root(),
            provenance: logit_core::Provenance {
                origin: Some(logit_core::interner::intern("nginx_in")),
                previous: Some(logit_core::interner::intern("enrich")),
            },
        };

        fanout.send_with_own_context(batch(1), incoming).await;

        let provenance = rx.recv().await.expect("should receive").provenance();
        assert_eq!(provenance, incoming.provenance);
    }

    /// Every branch of a fan-out gets the same provenance.
    #[tokio::test]
    async fn a_fan_out_gives_every_branch_the_same_provenance() {
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx_a, tx_b]).with_component("split");

        fanout.send(batch(1)).await;

        let a = rx_a.recv().await.expect("a should receive").provenance();
        let b = rx_b.recv().await.expect("b should receive").provenance();
        assert_eq!(a, b);
    }

    /// `send_relayed` leaves a v2 peer's full provenance untouched.
    #[tokio::test]
    async fn send_relayed_passes_through_full_provenance_from_a_v2_peer_untouched() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]).with_component("logit_in");
        let from_wire = logit_core::Provenance {
            origin: Some(logit_core::interner::intern("remote_listener")),
            previous: Some(logit_core::interner::intern("remote_enrich")),
        };

        fanout.send_relayed(batch(1), from_wire).await;

        let provenance = rx.recv().await.expect("should receive").provenance();
        assert_eq!(provenance, from_wire);
    }

    /// `send_relayed` back-fills this listener's id into empty provenance.
    #[tokio::test]
    async fn send_relayed_backfills_this_listeners_id_when_the_wire_carried_none() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]).with_component("logit_in");

        fanout.send_relayed(batch(1), logit_core::Provenance::default()).await;

        let provenance = rx.recv().await.expect("should receive").provenance();
        assert_eq!(provenance.origin_str(), Some("logit_in"));
        assert_eq!(provenance.previous_str(), Some("logit_in"));
    }
}
