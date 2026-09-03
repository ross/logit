//! [`Fanout`]: the outbound side of a graph node. Every non-sink component (listener, transform,
//! Lua stage) sends what it produces through one of these -- one `mpsc::Sender` per consumer,
//! resolved from the inverted `sources` relation at graph-build time
//! (`docs/design/pipeline-graph.md`'s "Runtime model").
//!
//! The channel payload is [`Delivered`], not a bare `EventBatch`
//! (`docs/adr/arc-eventbatch-copy-on-write.md`). `send`/`send_blocking` still take an owned
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
//!
//! Every `Delivered` also carries a [`TraceContext`] -- the substrate for internal spans, built
//! per `docs/adr/trace-context-propagation-on-delivered.md` on the measured evidence of a
//! costing exercise that came before it (`docs/known-gaps.md`). Real span emission -- turning
//! that context into an actual `SpanRecord`-carrying `Event` -- landed in
//! `docs/adr/internal-span-emission-and-deterministic-sampling.md`: `Fanout::send`/
//! `send_blocking` (below) record this node's own listener span around the send. See
//! [`TraceContext`]'s own doc comment for the propagation model, and
//! `docs/design/pipeline-graph.md`'s "Trace context propagation" section for the account of which
//! node kinds propagate a real parent today and which still mint a root.

use logit_core::{EventBatch, SpanKind, Telemetry};
use std::cell::Cell;
use std::sync::Arc;
use tokio::sync::mpsc;

/// One batch's place in a trace: which trace it belongs to, and which span produced it. Copy, no
/// allocation -- 24 bytes, carried on every [`Delivered`] regardless of whether anything downstream
/// ever turns it into a real span.
///
/// **Propagation model:** `trace_id` is set once, at a trace's true origin, and never changes
/// again as a batch moves through the graph -- every hop's emitted batch keeps its parent's
/// `trace_id`. `span_id` changes at *every* hop: [`TraceContext::child`] keeps `trace_id` and mints
/// a fresh `span_id`, so a hop's own `span_id` is what the *next* hop's span records as its own
/// `parent_span_id` -- the actual `SpanRecord` this builds into is real now
/// (`docs/adr/internal-span-emission-and-deterministic-sampling.md`; see the module doc above).
///
/// **Not every node can produce a `child`.** A node with exactly one incoming batch per emission (a
/// listener producing its first batch, `Transform::process`/`ScriptWorker::process`'s per-batch
/// loop) has one unambiguous parent, and does. A node whose emission is built from however many
/// upstream batches contributed since the last tick (`Transform::flush`, Lua's timer-driven
/// `flush()`) has no single correct parent -- an *n*-to-1 relationship, not 1-to-1 -- and mints a
/// fresh [`TraceContext::new_root`] instead, deliberately, rather than picking one arbitrarily; ADR
/// 0022 records this as the settled design, not something still to be built. `docs/known-gaps.md`'s
/// internal-spans entry names the narrower residuals that *are* still open (the listener span's
/// window, Lua `flush()`'s link-less root).
/// `Default` is the all-zero context -- a placeholder for tests/benches that construct a
/// `Delivered` directly and don't care what it carries, never used by `Fanout` itself (which
/// always calls [`TraceContext::new_root`] or [`TraceContext::child`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
}

impl TraceContext {
    /// A fresh, unrelated context: both `trace_id` and `span_id` newly minted. Used at a trace's
    /// true origin (a listener's own batches -- `Input::run` never receives a `Delivered`, so it
    /// has no parent to inherit) and, for now, at every flush-driven emission (see this type's own
    /// doc comment for why that's a deliberate, tracked gap rather than an oversight).
    pub fn new_root() -> Self {
        TraceContext { trace_id: next_id_bytes(), span_id: next_id_bytes() }
    }

    /// A context for whatever this node emits as a direct, unambiguous result of processing one
    /// incoming batch carrying `self` -- same `trace_id`, a fresh `span_id`. See this type's own
    /// doc comment for which node kinds can call this today.
    pub fn child(&self) -> Self {
        TraceContext { trace_id: self.trace_id, span_id: next_id_bytes() }
    }
}

/// A per-thread SplitMix64, good enough to mint distinct trace/span ids without a new `rand`
/// dependency or `tracing::span::Id` (a `Registry` recycles those after a span closes, so they're
/// not a safe source of identity here -- two spans minutes apart could share one). Not
/// security-relevant: `logit`'s listeners are private by deployment shape
/// (`docs/OVERVIEW.md`), the same premise `docs/known-gaps.md`'s interner entry leans on, and a
/// trace id is not a capability.
fn next_id_bytes<const N: usize>() -> [u8; N] {
    thread_local! {
        // Seeded once, lazily, on this thread's first call -- not a compile-time constant.
        // Caught in review: a `const` seed here is identical on every thread and every process
        // run, so the *first* `next_id_bytes()` call on any two fresh threads returned the same
        // bytes, deterministically merging unrelated traces. `initial_seed` below is real
        // per-run (OS-random) and per-thread entropy instead.
        static STATE: Cell<u64> = Cell::new(initial_seed());
    }
    let mut out = [0u8; N];
    let mut filled = 0;
    while filled < N {
        let mut z = STATE.with(|c| {
            let z = c.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
            c.set(z);
            z
        });
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        for b in z.to_le_bytes() {
            if filled >= N {
                break;
            }
            out[filled] = b;
            filled += 1;
        }
    }
    out
}

/// This thread's starting seed: real entropy, not a shared constant. `RandomState::new()` is
/// keyed from OS randomness at process start and refreshed by an internal per-call counter, so it
/// already differs call to call within one process; mixing in this thread's `ThreadId` makes two
/// threads calling this at nearly the same instant diverge too, rather than relying on
/// `RandomState`'s own per-call drift alone. Not security-relevant, same as `next_id_bytes`'s own
/// doc comment above -- this only needs to not repeat, not resist prediction.
fn initial_seed() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(std::thread::current().id())
}

/// What travels one graph edge. `Fanout::send`/`send_blocking` pick the variant per send based on
/// how many consumers that `Fanout` has -- a property of the edge, not of the batch itself.
pub enum Delivered {
    /// This edge's `Fanout` had exactly one consumer: the batch moved through with no `Arc`
    /// allocated at all. The common case -- every listener's first hop, and every interior edge of
    /// a linear chain (the v0.1 reference config's `statsd_in -> aggregate -> lua -> influxdb_out`
    /// among them).
    Owned(EventBatch, TraceContext),
    /// This edge's `Fanout` had more than one consumer: every one of them holds a handle to the
    /// same `Arc`-wrapped batch. Which handle (if any) gets to reclaim the batch without cloning is
    /// decided at the consuming end, by which one happens to be dropped last at runtime -- there is
    /// no privileged branch, and under concurrent consumption more than one can end up cloning; see
    /// `runtime.rs`'s `unwrap_batch`.
    Shared(Arc<EventBatch>, TraceContext),
}

impl Delivered {
    /// This batch's `TraceContext`, borrowed -- read this *before* `unwrap_batch` consumes the
    /// `Delivered`, to use as the parent for whatever the consuming node emits
    /// (`TraceContext::child`). Deliberately not part of `unwrap_batch`'s own return type: that
    /// would force every existing caller (most of which don't propagate anything, and never will
    /// -- a sink, a flush) to thread a value through it doesn't use.
    pub fn context(&self) -> TraceContext {
        match self {
            Delivered::Owned(_, ctx) => *ctx,
            Delivered::Shared(_, ctx) => *ctx,
        }
    }
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

    /// Sends `batch` as a new trace root, recording this node's own listener span around the
    /// send -- the right call for a node with no single incoming batch to inherit a parent
    /// context from (every listener; `Input::run` never receives a `Delivered`, so has no parent
    /// to inherit). See [`Fanout::send_with_own_context`] for everything about delivery mechanics.
    ///
    /// **This is the one place a listener's own `SpanKind::Producer` span is recorded** --
    /// `docs/adr/internal-span-emission-and-deterministic-sampling.md`'s per-node-kind table.
    /// Its window is deliberately just this call, not "however long the listener spent building
    /// `batch`": `Fanout::send` has no visibility into that (`Input::run` is a free-form loop), so
    /// this doesn't fabricate a start time it can't actually know. Once `run_flush`/`run_lua`'s
    /// flush path minted its own root and called [`Fanout::send_with_own_context`] directly
    /// (this PR), a genuine listener is the *only* remaining caller of this method -- so "one
    /// call to `send`" and "one listener emission" are now the same event.
    pub async fn send(&self, batch: EventBatch) {
        let ctx = TraceContext::new_root();
        let mut span =
            self.telemetry.span("send", SpanKind::Producer, ctx.trace_id, ctx.span_id, None);
        span.events(batch.events.len() as u64);
        self.send_with_own_context(batch, ctx).await;
    }

    /// Sends `batch` to every consumer, as a [`TraceContext::child`] of `parent` -- the right call
    /// for a node that has exactly one incoming batch to attribute this emission to, but that
    /// doesn't itself record a span for the send (`run_transform`/`run_lua`'s non-flush paths
    /// record their own `SpanKind::Internal` span around `process` *and* the send, so the context
    /// this mints has to be knowable *before* the send call -- see [`Fanout::send_with_own_context`],
    /// which this is now defined in terms of).
    pub async fn send_with_context(&self, batch: EventBatch, parent: TraceContext) {
        self.send_with_own_context(batch, parent.child()).await
    }

    /// Sends `batch` to every consumer under `ctx`, exactly as minted by the caller -- the
    /// primitive every other `send*` variant on this type is built from. The right call for a
    /// node that already minted its own `TraceContext` for a span it's recording around this send
    /// (or around a wider window that includes it): the span's `span_id` and the outgoing
    /// `Delivered`'s `span_id` must be the *same* id, which only holds if nothing mints a second,
    /// unrelated context here. `send_with_context(b, parent)` is exactly
    /// `send_with_own_context(b, parent.child())` -- the two aren't independent behaviors, just
    /// two ways of arriving at the context this one actually sends under.
    ///
    /// A closed consumer is silently skipped -- see `docs/design/pipeline-graph.md`'s backpressure
    /// section: propagating a closed downstream as a real shutdown signal is a named open
    /// question, not solved here -- but it's no longer silent to telemetry: a closed-consumer send
    /// counts toward `logit.component.events.dropped{reason="closed_consumer"}`.
    ///
    /// Exactly one consumer: `batch` moves through as [`Delivered::Owned`], no `Arc` involved at
    /// all -- this is what keeps a linear chain (no fan-out anywhere on it) free of this change's
    /// cost entirely. More than one consumer: wraps `batch` in an `Arc` once, then clones the `Arc`
    /// (a refcount bump, not a deep clone) for every consumer but the last, which gets it moved --
    /// saving one atomic increment/decrement pair, not a structural privilege (see
    /// [`Delivered::Shared`]'s doc comment). Every consumer gets the *same* context -- one batch
    /// forking into several downstream branches is still one emission, not several, so it records
    /// (at most, at the caller's own discretion) exactly one span here, never one per branch.
    pub async fn send_with_own_context(&self, batch: EventBatch, ctx: TraceContext) {
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

    /// The `blocking_send` equivalent of [`Fanout::send`], for a node running on a plain OS
    /// thread rather than as a tokio task (a Lua node -- see
    /// `docs/design/pipeline-graph.md`'s "Thread model" section). Same listener-span reasoning as
    /// `send`'s own doc comment.
    pub fn send_blocking(&self, batch: EventBatch) {
        let ctx = TraceContext::new_root();
        let mut span =
            self.telemetry.span("send", SpanKind::Producer, ctx.trace_id, ctx.span_id, None);
        span.events(batch.events.len() as u64);
        self.send_blocking_with_own_context(batch, ctx);
    }

    /// The `blocking_send` equivalent of [`Fanout::send_with_context`] -- see that method for the
    /// propagation contract.
    pub fn send_blocking_with_context(&self, batch: EventBatch, parent: TraceContext) {
        self.send_blocking_with_own_context(batch, parent.child())
    }

    /// The `blocking_send` equivalent of [`Fanout::send_with_own_context`] -- see that method for
    /// the propagation contract.
    pub fn send_blocking_with_own_context(&self, batch: EventBatch, ctx: TraceContext) {
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

    /// `Delivered`'s size, pinned exactly, the same reasoning `crates/logit-core/tests/type_sizes.rs`
    /// applies to `Event`: a `<=` bound would absorb exactly what this exists to catch. 56, not 32
    /// -- `TraceContext` (24 bytes: `trace_id` + `span_id`) is now a real, permanent field on every
    /// variant, not the measured-then-reverted prototype `docs/known-gaps.md`'s internal-spans
    /// entry originally costed it as (`docs/design/memory.md`'s "Costing internal spans" section
    /// has that history). `Owned`'s `EventBatch` (32 bytes: an `Arc<Resource>` pointer plus a
    /// `Vec<Event>`) plus `TraceContext` (24) is the larger variant at 56, and it still fits with
    /// no separate discriminant byte -- the `Vec`'s non-null pointer gives the compiler a niche to
    /// fold the tag into for free, the same trick that makes `Option<SpanRecord>` cost nothing
    /// over `SpanRecord` (`crates/logit-core/tests/type_sizes.rs`).
    #[test]
    fn delivered_is_56_bytes_no_wider_than_its_larger_variant() {
        assert_eq!(std::mem::size_of::<Delivered>(), 56);
    }

    /// `TraceContext::child` keeps `trace_id`, mints a fresh `span_id` -- the propagation contract
    /// every `send_with_context`/`send_blocking_with_context` call relies on.
    #[test]
    fn child_context_keeps_the_trace_id_and_mints_a_fresh_span_id() {
        let root = TraceContext::new_root();
        let child = root.child();
        assert_eq!(child.trace_id, root.trace_id);
        assert_ne!(child.span_id, root.span_id);
    }

    /// Two independently-minted roots should (overwhelmingly likely) differ in both halves --
    /// not a proof of uniqueness, just a smoke test that `next_id_bytes` isn't returning a
    /// constant.
    #[test]
    fn two_roots_are_not_the_same_context() {
        let a = TraceContext::new_root();
        let b = TraceContext::new_root();
        assert_ne!(a.trace_id, b.trace_id);
        assert_ne!(a.span_id, b.span_id);
    }

    /// The bug the test above can't catch: a `const` thread-local seed is identical on every
    /// thread, so *this* thread's first call and a *fresh* thread's first call would collide --
    /// invisible to `two_roots_are_not_the_same_context`, which only ever calls from one thread.
    /// Spawns a real new thread and takes its very first `new_root()`, matching the actual failure
    /// shape (every worker thread's first trace, not some later call after the seed has already
    /// advanced).
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

    /// `send` (no explicit parent) mints a fresh root every call -- the behavior every listener
    /// and every flush-driven emission relies on.
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

    /// `send_with_context` keeps the parent's `trace_id` and mints a fresh `span_id` -- the
    /// propagation contract `run_transform`/`run_lua`'s non-flush paths rely on
    /// (`crates/logit-pipeline/src/runtime.rs`).
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

    /// A real fan-out: every branch should see the *same* child context -- one batch forking into
    /// several downstream consumers is still one emission, not several unrelated ones.
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

    /// `send` mints a root and records exactly one `SpanKind::Producer` span for it -- the
    /// drained span's own `span_id` must be the same id the delivered batch actually went out
    /// under, not some unrelated id minted separately
    /// (`docs/adr/internal-span-emission-and-deterministic-sampling.md`'s "the span's `span_id`
    /// and the outgoing `Delivered`'s `span_id` must be the same id").
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
}
