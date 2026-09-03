//! [`BoundedQueue`]: the async wrapper around `logit_proto::buffer::Buffer` that decouples a
//! node's own I/O from whatever's downstream of it -- see
//! `docs/adr/0021-buffered-sink-delivery.md` (the sink side, `SinkQueue`) and
//! `docs/adr/0026-decoupled-listener-io.md` (the listener side, `ReceiveQueue`). `Buffer` itself
//! is sync (no `.await` in a critical section), so this type owns the one thing a sync trait
//! can't express: `Block`, which awaits room rather than dropping.
//!
//! Generalized from a `SinkQueue` that hardcoded `Arc<EventBatch>` -- the [`Queued`] trait and
//! [`QueueMetrics`] are what let one implementation serve both a sink's delivery queue and a UDP
//! listener's receive queue with no behavior change on the sink side: `SinkQueue` is now a type
//! alias (over `(Arc<EventBatch>, TraceContext)`, not bare `Arc<EventBatch>` -- see that alias's
//! own doc comment for why), and every sink-side metric name, default, and test is unchanged.

use crate::fanout::TraceContext;
use logit_core::{EventBatch, Telemetry};
use logit_proto::buffer::{Buffer, InMemoryBuffer, OverflowPolicy as DropPolicy, PushOutcome};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// What a [`BoundedQueue`] needs to know about the thing it holds, read once per item at push
/// time and cached alongside it (`weight`, because `Buffer::push` already takes a precomputed
/// weight) or read once per drop (`units`, to count a drop in the unit an operator actually
/// reasons about -- events for a batch, bytes for a datagram).
pub trait Queued: Send + Sync + 'static {
    /// Admission-control weight in bytes -- an estimate, not an allocator figure
    /// (`docs/design/memory.md` §5).
    fn weight(&self) -> u64;
    /// How many countable things this item represents. Drives the `*_dropped` companion counter
    /// alongside the per-item `*_dropped` count.
    fn units(&self) -> u64;
}

impl Queued for Arc<EventBatch> {
    fn weight(&self) -> u64 {
        self.estimated_heap_bytes()
    }
    fn units(&self) -> u64 {
        self.events.len() as u64
    }
}

/// [`SinkQueue`]'s actual item type: a batch alongside the [`TraceContext`] it arrived with.
/// `TraceContext` is `Copy`, 24 bytes, so this rides inline in the existing `(item, weight)` slot
/// `InMemoryBuffer` already stores -- no new allocation, and weight/units are unaffected, since
/// both are computed from the batch alone. See `docs/adr/0025-internal-span-emission-and-
/// deterministic-sampling.md` for why this exists: `write_loop`'s sink span (the only span that
/// can carry `SpanStatus::Error` and a retry count) needs the context that arrived with this
/// batch, and `drain_inbox`/`peek` were the last place it was still being discarded.
impl Queued for (Arc<EventBatch>, TraceContext) {
    fn weight(&self) -> u64 {
        self.0.weight()
    }
    fn units(&self) -> u64 {
        self.0.units()
    }
}

/// Every metric name one [`BoundedQueue`] emits, resolved once at construction and never
/// formatted -- `docs/design/internal-telemetry.md`'s cardinality convention requires every name
/// to be a compile-time constant, and a name built at runtime (`format!("logit.{kind}...")`)
/// would be exactly the mistake that convention exists to prevent. [`SINK_QUEUE_METRICS`] is the
/// one instance today; `logit-inputs`' receive queue (`docs/adr/0026-decoupled-listener-io.md`)
/// adds a second.
pub struct QueueMetrics {
    /// Gauge: items currently queued.
    pub depth: &'static str,
    /// Gauge: weight currently queued.
    pub bytes: &'static str,
    /// Gauge: `max(depth ratio, bytes ratio)` against the two configured bounds.
    pub utilization: &'static str,
    /// Timing: how long a `Block`-policy push waited for room; only recorded when a push
    /// actually had to wait.
    pub push_blocked: &'static str,
    /// Count, tagged `reason`: one per dropped item.
    pub items_dropped: &'static str,
    /// Count, tagged `reason`: the dropped item's own `units()`.
    pub units_dropped: &'static str,
}

pub static SINK_QUEUE_METRICS: QueueMetrics = QueueMetrics {
    depth: "logit.component.buffer.batches",
    bytes: "logit.component.buffer.bytes",
    utilization: "logit.component.buffer.utilization",
    push_blocked: "logit.component.buffer.push.blocked.duration",
    items_dropped: "logit.component.batches.dropped",
    units_dropped: "logit.component.events.dropped",
};

/// What to do when the queue is full. `Block` isn't part of `logit_proto::buffer::OverflowPolicy`
/// -- it's this type's own addition, layered on top of the two dropping policies that trait can
/// express synchronously (see `logit_proto::buffer::OverflowPolicy`'s doc comment for why). Shared
/// between the sink and receive sides: both need the same three-way choice, only the *default*
/// differs (`docs/adr/0026-decoupled-listener-io.md`'s core argument for why a UDP listener's
/// default must not be `Block`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    Block,
    DropOldest,
    DropNewest,
}

/// Bounds and overflow behavior for one [`BoundedQueue`], in the queue's own generic terms
/// (items/weight rather than a domain-specific unit). [`SinkQueueConfig`]/`ReceiveQueueConfig`
/// (`logit-inputs`, `docs/adr/0026-decoupled-listener-io.md`) each convert into this rather than
/// being this directly -- a sink operator reasons in batches, a listener operator in datagrams,
/// and each config type's own field names and doc comments should say so.
#[derive(Debug, Clone, Copy)]
pub struct QueueConfig {
    pub max_items: usize,
    pub max_weight: u64,
    pub overflow: OverflowPolicy,
}

/// Bounds and overflow behavior for one sink's [`SinkQueue`]. `max_batches`/`max_bytes` are both
/// enforced -- whichever trips first -- exactly like `InMemoryBuffer`'s own two bounds
/// (`crates/logit-proto/src/buffer.rs`); `max_bytes` is checked against
/// `EventBatch::estimated_heap_bytes`, computed once per batch at push time.
#[derive(Debug, Clone, Copy)]
pub struct SinkQueueConfig {
    pub max_batches: usize,
    pub max_bytes: u64,
    pub overflow: OverflowPolicy,
}

/// What a production sink gets when its component omits a `buffer:` block entirely
/// (`logit_config::BufferConfig::default()` mirrors these same values, and `logit-cli::pipeline`
/// builds this `SinkQueueConfig` from whatever the config actually resolved to -- see
/// `queue_config` there). 1024 batches / 64 MiB is deep enough to ride out a real destination
/// hiccup without being a meaningfully unbounded queue; `Block` is the default overflow behavior
/// because losing data silently should be an explicit per-sink opt-in (`DropOldest`/`DropNewest`),
/// not the out-of-the-box posture.
impl Default for SinkQueueConfig {
    fn default() -> Self {
        Self { max_batches: 1024, max_bytes: 64 * 1024 * 1024, overflow: OverflowPolicy::Block }
    }
}

impl From<SinkQueueConfig> for QueueConfig {
    fn from(c: SinkQueueConfig) -> Self {
        Self { max_items: c.max_batches, max_weight: c.max_bytes, overflow: c.overflow }
    }
}

/// The async wrapper around `logit_proto::buffer::InMemoryBuffer<T>` that sits between a node's
/// own I/O and whatever it's decoupled from, letting the two proceed independently -- a sink's
/// inbox drain and its writer (`docs/adr/0021-buffered-sink-delivery.md`, `SinkQueue`), or a UDP
/// listener's socket read and its decode loop (`docs/adr/0026-decoupled-listener-io.md`,
/// `logit-inputs`' `ReceiveQueue`). Not `Clone` -- exactly one value exists per node, wrapped in
/// `Arc` by its two callers, each holding their own `Arc::clone`.
///
/// `std::sync::Mutex`, not tokio's -- every critical section here is a `VecDeque` push/pop with
/// no `.await` inside it, so the sync mutex is both simpler and cheaper.
pub struct BoundedQueue<T: Queued> {
    inner: Mutex<InMemoryBuffer<T>>,
    /// Woken by `push`/`commit`/`pop` progress -- signals "there might be something to read now"
    /// to `peek`/`pop`.
    not_empty: Notify,
    /// Woken by `commit`/`pop` (room freed) and by `close` -- signals "there might be room now,
    /// or the queue is closing" to a blocked `push`.
    not_full: Notify,
    closed: AtomicBool,
    /// Whether `push` should await room rather than let a push through to the underlying
    /// buffer's dropping policy. `true` iff the configured [`OverflowPolicy`] is `Block`.
    block_when_full: bool,
    max_items: usize,
    max_weight: u64,
    metrics: &'static QueueMetrics,
    telemetry: Telemetry,
}

impl<T: Queued> BoundedQueue<T> {
    /// `OverflowPolicy::Block` has no equivalent in `logit_proto::buffer::OverflowPolicy` --
    /// that trait only knows the two dropping policies (by design; a sync trait can't block
    /// usefully). The resolution: the underlying `InMemoryBuffer` is always built with a
    /// concrete dropping policy (`DropOldest` standing in for `Block`), but under `Block`,
    /// `push` (below) always awaits room *before* it ever calls the underlying `Buffer::push` --
    /// so in ordinary single-writer operation the underlying buffer's dropping fallback is never
    /// actually exercised. It's `DropOldest`, not `DropNewest`, specifically so the one case
    /// where `push` *does* fall through without blocking -- a batch that could never fit even
    /// against an empty queue, see `push`'s "impossible to ever fit" check below -- degrades to
    /// "evict what little can be evicted, then accept anyway" rather than silently rejecting an
    /// item `Block`'s whole contract says should never be dropped. This is also the fallback for
    /// the push-races-`close()` case documented on [`BoundedQueue::close`].
    ///
    /// `InMemoryBuffer::new` preallocates its `VecDeque` to `max_items.min(4096)` -- see its own
    /// doc comment; negligible for a sink (1024 items × 16B), worth it for a receive queue an
    /// order of magnitude deeper, where the warm-up reallocations an empty-start deque would
    /// otherwise pay land in the hot path.
    pub fn with_metrics(
        config: QueueConfig,
        metrics: &'static QueueMetrics,
        telemetry: Telemetry,
    ) -> Self {
        let block_when_full = config.overflow == OverflowPolicy::Block;
        let underlying = match config.overflow {
            OverflowPolicy::Block | OverflowPolicy::DropOldest => DropPolicy::DropOldest,
            OverflowPolicy::DropNewest => DropPolicy::DropNewest,
        };
        Self {
            inner: Mutex::new(InMemoryBuffer::new(config.max_items, config.max_weight, underlying)),
            not_empty: Notify::new(),
            not_full: Notify::new(),
            closed: AtomicBool::new(false),
            block_when_full,
            max_items: config.max_items,
            max_weight: config.max_weight,
            metrics,
            telemetry,
        }
    }

    /// Pushes `item`, weighing it by [`Queued::weight`] -- computed exactly once here, not per
    /// retry attempt below.
    ///
    /// Under `Block`: waits for room (re-checked under the lock on every wakeup -- the standard
    /// `Notify` condvar pattern, race-free because the `Notified` future is constructed *before*
    /// the state check it's guarding, so a `commit()`/`pop()`/`close()` landing anywhere after
    /// that point is never missed) before ever attempting the underlying push, so the underlying
    /// buffer's dropping fallback is never reached in ordinary operation. Under
    /// `DropOldest`/`DropNewest`: one lock acquisition, one push attempt, no waiting.
    ///
    /// **Never blocks on an item that could never fit even against an empty queue** (`weight`
    /// alone exceeds `max_weight`, or `max_items` is configured as `0`) -- no amount of waiting
    /// would ever free enough room, since there's nothing productive for a concurrent `commit()`/
    /// `pop()` to do about it. Such an item instead falls straight through to the underlying
    /// `DropOldest` fallback (see `with_metrics`), which evicts what little it safely can and
    /// accepts the item anyway rather than wedging this node, and everything upstream of it,
    /// forever.
    ///
    /// Every accepted push notifies `not_empty` once. A push that actually had to wait times
    /// `metrics.push_blocked` around the whole wait -- a push that never had to wait records no
    /// sample at all, so the metric isn't muddied by a stream of ~0 durations from the common
    /// case.
    pub async fn push(&self, item: T) {
        let weight = item.weight();
        let impossible_to_ever_fit = weight > self.max_weight || self.max_items == 0;
        let mut waited = false;
        let mut blocked_timer: Option<logit_core::telemetry::Timer> = None;
        // `Option`, not a bare `T` reused across iterations, specifically so this method needs no
        // `T: Clone` bound: `push` retries (the `None` arm below) without ever consuming `item`,
        // and the one arm that does consume it (`take()`) always breaks the loop immediately
        // afterward, so `take()` is never called a second time.
        let mut item = Some(item);

        let (len, total_weight, evicted, rejected) = loop {
            // Registered before the state check below -- see this method's doc comment on why
            // that ordering is what makes this race-free.
            let notified = self.not_full.notified();

            let attempt = {
                let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let full = self.would_overflow(&inner, weight);
                if self.block_when_full
                    && full
                    && !impossible_to_ever_fit
                    && !self.closed.load(Ordering::Acquire)
                {
                    None
                } else {
                    let taken = item.take().expect(
                        "item is only ever taken on the one path that immediately breaks the loop",
                    );
                    let outcome = inner.push(taken, weight);
                    Some((outcome, inner.len(), inner.weight()))
                }
            };

            match attempt {
                None => {
                    if !waited {
                        waited = true;
                        blocked_timer = Some(self.telemetry.timer(self.metrics.push_blocked));
                    }
                    notified.await;
                }
                Some((outcome, len, total_weight)) => {
                    let (evicted, rejected) = match outcome {
                        PushOutcome::Accepted => (Vec::new(), None),
                        PushOutcome::Evicted(evicted) => (evicted, None),
                        PushOutcome::Rejected(rejected) => (Vec::new(), Some(rejected)),
                    };
                    break (len, total_weight, evicted, rejected);
                }
            }
        };

        for evicted_item in &evicted {
            self.count_dropped("overflow_oldest", evicted_item);
        }
        if let Some(rejected_item) = &rejected {
            self.count_dropped("overflow_newest", rejected_item);
        }

        // Only records a sample if `blocked_timer` is `Some` -- i.e. only if this push actually
        // waited at least once above.
        drop(blocked_timer);
        self.not_empty.notify_one();
        self.update_gauges(len, total_weight);
    }

    /// Removes and returns the head (a no-op returning `None` on an empty queue), notifies any
    /// blocked `push` that room may now be available, and refreshes the depth/utilization
    /// gauges. For [`SinkQueue`], this returns the `TraceContext` the head was pushed with
    /// alongside its batch -- nothing downstream of a commit (counting a delivered/dropped batch)
    /// needs it; a caller that does should have already read it from the matching
    /// [`BoundedQueue::peek`] first.
    pub fn commit(&self) -> Option<T> {
        let (item, len, weight) = {
            let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let item = inner.commit();
            (item, inner.len(), inner.weight())
        };
        self.not_full.notify_one();
        self.update_gauges(len, weight);
        item
    }

    /// Awaits and removes the head in one step -- the remove-on-read counterpart to
    /// `peek`/`commit` for a consumer with no retry (a UDP listener's decode loop: a datagram
    /// that fails to decode is diagnosed and dropped, never re-attempted). `peek().await` then
    /// `commit()` at the call site is equivalent when it runs to completion, but is
    /// **cancellation-unsafe**: `peek` reserves the head against `drop_oldest` eviction, and if
    /// the awaiting task is dropped between `peek` and `commit` -- exactly what a shutdown-grace
    /// cancellation does -- that reservation never clears, permanently exempting the head from
    /// eviction and letting the queue grow past its bound. `pop` never awaits between reserving
    /// and removing, so a consumer cancelled mid-call can never leave one dangling.
    pub async fn pop(&self) -> Option<T> {
        loop {
            let notified = self.not_empty.notified();
            let (item, len, weight) = {
                let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                // `peek` (reserve) immediately followed by `commit` (remove), both under the same
                // lock acquisition and with no `.await` between them -- this is what makes the
                // whole method cancellation-safe: there is no suspend point at which a reservation
                // could be left standing.
                if inner.peek().is_some() {
                    let item = inner.commit();
                    (item, inner.len(), inner.weight())
                } else if self.closed.load(Ordering::Acquire) {
                    return None;
                } else {
                    (None, inner.len(), inner.weight())
                }
            };
            if let Some(item) = item {
                self.not_full.notify_one();
                self.update_gauges(len, weight);
                return Some(item);
            }
            notified.await;
        }
    }

    /// Marks the queue closed: no more items will ever arrive, so once it's also empty,
    /// `peek()`/`pop()` should stop waiting and return `None`.
    ///
    /// Wakes every waiter on both `Notify`s -- `not_empty`, so a `peek()`/`pop()` parked on an
    /// empty queue observes the close instead of waiting forever, and `not_full`, so a `push()`
    /// blocked on a full queue under `Block` also wakes rather than hanging against a queue that
    /// will never drain further once its consumer sees it close.
    ///
    /// **Decision on a push racing a concurrent close:** a blocked push that wakes because of
    /// this call re-checks state (per `push`'s loop) and, seeing `closed == true`, falls through
    /// to one best-effort attempt against the underlying buffer's `DropOldest` fallback (see
    /// `with_metrics`) rather than waiting again -- it may then evict (never the reserved head, if
    /// any) or accept over-bound instead of blocking forever. In practice the drain/read side only
    /// calls `close()` once it has stopped producing, so no further `push()` calls happen at all;
    /// this only matters for a hypothetical caller that pushes concurrently with closing, and the
    /// contract for that case is simply: never panic, never hang.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }

    fn would_overflow(&self, inner: &InMemoryBuffer<T>, weight: u64) -> bool {
        inner.len() >= self.max_items || inner.weight() + weight > self.max_weight
    }

    fn count_dropped(&self, reason: &'static str, item: &T) {
        self.telemetry.count(self.metrics.items_dropped, 1.0, &[("reason", reason)]);
        self.telemetry.count(
            self.metrics.units_dropped,
            item.units() as f64,
            &[("reason", reason)],
        );
    }

    /// `metrics.utilization` is `max(items ratio, bytes ratio)` -- whichever bound is closer to
    /// tripping is what actually predicts blocking/dropping next, so reporting only one of the
    /// two bounds would under-report risk whenever the other is the tighter one for a given
    /// workload. Guards both denominators against zero (a config with either bound set to 0 is
    /// degenerate, but this must not panic or produce NaN/inf against it).
    fn update_gauges(&self, len: usize, weight: u64) {
        self.telemetry.gauge(self.metrics.depth, len as f64, &[]);
        self.telemetry.gauge(self.metrics.bytes, weight as f64, &[]);
        let items_ratio =
            if self.max_items == 0 { 0.0 } else { len as f64 / self.max_items as f64 };
        let bytes_ratio =
            if self.max_weight == 0 { 0.0 } else { weight as f64 / self.max_weight as f64 };
        self.telemetry.gauge(self.metrics.utilization, items_ratio.max(bytes_ratio), &[]);
    }
}

impl<T: Queued + Clone> BoundedQueue<T> {
    /// The head, without removing it -- a clone the caller can act on and only remove (via
    /// [`BoundedQueue::commit`]) once that action succeeds. Requires `T: Clone` (a cheap
    /// refcount bump for `Arc<EventBatch>`, or for [`SinkQueue`]'s `(Arc<EventBatch>,
    /// TraceContext)`, a refcount bump plus a `Copy`); a consumer with no such retry contract
    /// should use [`BoundedQueue::pop`] instead, which needs no `Clone` bound and is
    /// cancellation-safe. Awaits `not_empty` while the queue is empty and open; returns `None`
    /// once the queue is both closed and empty, checked together under one lock acquisition so a
    /// concurrent `close()` can never be observed racing a concurrent `push()` -- either the push
    /// landed before this check took the lock (and is seen), or it didn't (and `closed` becoming
    /// true afterward is this call's problem on its *next* iteration, not this one).
    pub async fn peek(&self) -> Option<T> {
        loop {
            let notified = self.not_empty.notified();
            {
                let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(item) = inner.peek() {
                    return Some(item.clone());
                }
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
            }
            notified.await;
        }
    }
}

/// A sink's delivery queue: `Arc<EventBatch>` paired with the [`TraceContext`] it arrived with,
/// not bare `Arc<EventBatch>` -- `write_loop`'s sink span needs the context that produced each
/// batch, and `peek`/`commit` are the only place it can still be read back
/// (`docs/adr/0025-internal-span-emission-and-deterministic-sampling.md`).
pub type SinkQueue = BoundedQueue<(Arc<EventBatch>, TraceContext)>;

impl SinkQueue {
    pub fn new(config: SinkQueueConfig, telemetry: Telemetry) -> Self {
        Self::with_metrics(config.into(), &SINK_QUEUE_METRICS, telemetry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Event, Resource, Value};
    use std::time::Duration;

    /// A batch carrying roughly `extra_bytes` of attribute payload beyond the fixed cost of one
    /// empty event -- enough to give tests a way to force the byte bound, not just the count
    /// bound, without depending on the exact `estimated_heap_bytes` formula.
    fn batch(extra_bytes: usize) -> Arc<EventBatch> {
        let mut attrs = AttrMap::new();
        if extra_bytes > 0 {
            attrs.insert("payload", Value::str("x".repeat(extra_bytes)));
        }
        Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![Event::empty(0, attrs)],
        })
    }

    fn tiny_batch() -> Arc<EventBatch> {
        batch(0)
    }

    fn queue(max_batches: usize, max_bytes: u64, overflow: OverflowPolicy) -> SinkQueue {
        SinkQueue::new(SinkQueueConfig { max_batches, max_bytes, overflow }, Telemetry::default())
    }

    /// Every test in this module pushes under a placeholder context -- none of them exercise
    /// `TraceContext` propagation itself (`fanout.rs`/`runtime.rs`'s tests do that); this queue
    /// only needs to carry whatever it was given back out again unchanged, which
    /// `push_then_peek_then_commit_round_trips_one_batch` below proves directly with a real,
    /// non-default one.
    fn ctx() -> TraceContext {
        TraceContext::default()
    }

    #[tokio::test]
    async fn push_then_peek_then_commit_round_trips_one_batch() {
        let q = queue(10, u64::MAX, OverflowPolicy::Block);
        let sent = tiny_batch();
        let sent_ctx = TraceContext::new_root();
        q.push((Arc::clone(&sent), sent_ctx)).await;

        let (peeked, peeked_ctx) = q.peek().await.expect("should peek the pushed batch");
        assert!(Arc::ptr_eq(&peeked, &sent));
        assert_eq!(
            peeked_ctx, sent_ctx,
            "the context pushed with a batch should come back unchanged"
        );

        let (committed, _) = q.commit().expect("should commit the pushed batch");
        assert!(Arc::ptr_eq(&committed, &sent));
        assert!(q.commit().is_none(), "nothing left to commit");
    }

    #[tokio::test]
    async fn peek_without_commit_called_twice_returns_the_same_batch_both_times() {
        let q = queue(10, u64::MAX, OverflowPolicy::Block);
        let sent = tiny_batch();
        q.push((Arc::clone(&sent), ctx())).await;

        let (first, _) = q.peek().await.expect("should peek");
        let (second, _) = q.peek().await.expect("should peek again");
        assert!(Arc::ptr_eq(&first, &sent));
        assert!(Arc::ptr_eq(&second, &sent));
    }

    #[tokio::test(start_paused = true)]
    async fn under_block_a_push_that_must_wait_for_room_completes_once_a_concurrent_commit_frees_space(
    ) {
        let q = Arc::new(queue(1, u64::MAX, OverflowPolicy::Block));
        q.push((tiny_batch(), ctx())).await; // fills the one slot

        let q2 = Arc::clone(&q);
        let blocked = tokio::spawn(async move {
            q2.push((tiny_batch(), ctx())).await;
        });

        // Give the spawned push a chance to run and park on `not_full`.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked.is_finished(), "push should still be blocked with the queue full");

        q.commit().expect("should commit the original batch, freeing room");

        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("the blocked push should resolve once room is freed")
            .expect("the spawned task should not panic");
    }

    #[tokio::test]
    async fn under_drop_oldest_pushing_into_a_full_queue_evicts_and_is_reflected_in_commit_order() {
        let q = queue(2, u64::MAX, OverflowPolicy::DropOldest);
        let a = tiny_batch();
        let b = tiny_batch();
        let c = tiny_batch();
        q.push((Arc::clone(&a), ctx())).await;
        q.push((Arc::clone(&b), ctx())).await;
        q.push((Arc::clone(&c), ctx())).await; // evicts `a`

        let (first, _) = q.commit().expect("should commit");
        assert!(Arc::ptr_eq(&first, &b), "the oldest batch (a) should never appear");
        let (second, _) = q.commit().expect("should commit");
        assert!(Arc::ptr_eq(&second, &c));
        assert!(q.commit().is_none());
    }

    /// The ack invariant a review finding named directly: with `[A, B]` at capacity, `peek()`ing
    /// `A` (as `write_loop` does before attempting delivery) must protect it from a concurrent
    /// `DropOldest` push evicting it -- otherwise `commit()` after a successful send of `A` would
    /// remove whatever is now at the front (`B`, never actually sent) instead of `A`, silently
    /// losing `B` and falsely counting `A` as an overflow drop.
    #[tokio::test]
    async fn drop_oldest_never_evicts_a_batch_currently_peeked_and_commit_still_returns_it() {
        let q = queue(2, u64::MAX, OverflowPolicy::DropOldest);
        let a = tiny_batch();
        let b = tiny_batch();
        let c = tiny_batch();
        q.push((Arc::clone(&a), ctx())).await;
        q.push((Arc::clone(&b), ctx())).await;

        let (peeked, _) = q.peek().await.expect("should peek a"); // reserves `a`
        assert!(Arc::ptr_eq(&peeked, &a));

        q.push((Arc::clone(&c), ctx())).await; // must evict `b`, never the reserved `a`

        let (committed, _) =
            q.commit().expect("should commit the batch that was actually peeked/sent");
        assert!(
            Arc::ptr_eq(&committed, &a),
            "commit must return the exact batch that was peeked, not whatever is now at the front"
        );
        let (next, _) = q.commit().expect("should commit");
        assert!(Arc::ptr_eq(&next, &c), "b should have been the one evicted, not delivered");
        assert!(q.commit().is_none());
    }

    /// The other review finding: under `Block`, a batch whose own weight exceeds `max_bytes`
    /// must not wait forever even against an empty queue -- there's nothing a concurrent
    /// `commit()` could ever do to make room for it, since nothing else is queued.
    #[tokio::test]
    async fn under_block_a_batch_too_large_to_ever_fit_is_accepted_immediately_not_blocked_forever()
    {
        let oversized = batch(1000);
        let weight = oversized.estimated_heap_bytes();
        let q = queue(1000, weight - 1, OverflowPolicy::Block); // one byte too small, always

        tokio::time::timeout(Duration::from_secs(5), q.push((oversized, ctx())))
            .await
            .expect("a batch that can never fit must be accepted immediately, not block forever");

        assert_eq!(
            q.commit().map(|_| ()),
            Some(()),
            "the oversized batch should still be queued and deliverable"
        );
    }

    /// The degenerate-config variant of the same finding: `max_batches: 0` makes every push
    /// overflow unconditionally, even against an empty queue -- must not hang either.
    #[tokio::test]
    async fn under_block_a_zero_max_batches_config_does_not_hang_a_push() {
        let q = queue(0, u64::MAX, OverflowPolicy::Block);
        tokio::time::timeout(Duration::from_secs(5), q.push((tiny_batch(), ctx())))
            .await
            .expect("max_batches: 0 must not permanently block every push");
    }

    #[tokio::test]
    async fn under_drop_newest_pushing_into_a_full_queue_is_a_no_op_on_queue_contents() {
        let q = queue(2, u64::MAX, OverflowPolicy::DropNewest);
        let a = tiny_batch();
        let b = tiny_batch();
        let c = tiny_batch();
        q.push((Arc::clone(&a), ctx())).await;
        q.push((Arc::clone(&b), ctx())).await;
        q.push((c, ctx())).await; // rejected -- queue contents unchanged

        let (first, _) = q.commit().expect("should commit");
        assert!(Arc::ptr_eq(&first, &a));
        let (second, _) = q.commit().expect("should commit");
        assert!(Arc::ptr_eq(&second, &b));
        assert!(q.commit().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn peek_on_an_empty_unclosed_queue_awaits_until_a_concurrent_push_wakes_it() {
        let q = Arc::new(queue(10, u64::MAX, OverflowPolicy::Block));
        let q2 = Arc::clone(&q);
        let sent = tiny_batch();
        let sent2 = Arc::clone(&sent);

        let peeking = tokio::spawn(async move { q2.peek().await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!peeking.is_finished(), "peek should still be waiting on an empty queue");

        q.push((sent2, ctx())).await;

        let (peeked, _) = tokio::time::timeout(Duration::from_secs(1), peeking)
            .await
            .expect("peek should resolve once a batch is pushed")
            .expect("the spawned task should not panic")
            .expect("peek should return the pushed batch");
        assert!(Arc::ptr_eq(&peeked, &sent));
    }

    #[tokio::test]
    async fn peek_on_an_empty_closed_queue_returns_none_immediately() {
        let q = queue(10, u64::MAX, OverflowPolicy::Block);
        q.close();
        assert!(q.peek().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn close_while_a_peek_is_already_waiting_on_an_empty_queue_wakes_it_with_none() {
        let q = Arc::new(queue(10, u64::MAX, OverflowPolicy::Block));
        let q2 = Arc::clone(&q);
        let peeking = tokio::spawn(async move { q2.peek().await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!peeking.is_finished(), "peek should still be waiting on an empty queue");

        q.close();

        let peeked = tokio::time::timeout(Duration::from_secs(1), peeking)
            .await
            .expect("peek should resolve once the queue closes")
            .expect("the spawned task should not panic");
        assert!(peeked.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn the_max_batches_bound_independently_gates_blocks_wait() {
        let q = Arc::new(queue(1, u64::MAX, OverflowPolicy::Block));
        q.push((tiny_batch(), ctx())).await;

        let q2 = Arc::clone(&q);
        let blocked = tokio::spawn(async move { q2.push((tiny_batch(), ctx())).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked.is_finished(), "a full batch count alone should be enough to block");

        q.commit();
        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("should resolve once room is freed")
            .expect("should not panic");
    }

    #[tokio::test(start_paused = true)]
    async fn the_max_bytes_bound_independently_gates_blocks_wait() {
        // A huge batch count bound, but a byte bound too small for even one nonzero-weight batch
        // to fit alongside another -- proves the byte bound alone can trigger `Block`, not just
        // the batch-count bound.
        let first = batch(64);
        let weight = first.estimated_heap_bytes();
        let q = Arc::new(queue(1000, weight, OverflowPolicy::Block));
        q.push((first, ctx())).await;

        let q2 = Arc::clone(&q);
        let blocked = tokio::spawn(async move { q2.push((batch(64), ctx())).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked.is_finished(), "the byte bound alone should be enough to block");

        q.commit();
        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("should resolve once room is freed")
            .expect("should not panic");
    }

    // -- Generalization coverage: a non-`Arc<EventBatch>` `Queued` type, and `pop()`. --

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestItem {
        weight: u64,
        units: u64,
    }

    impl Queued for TestItem {
        fn weight(&self) -> u64 {
            self.weight
        }
        fn units(&self) -> u64 {
            self.units
        }
    }

    static TEST_METRICS: QueueMetrics = QueueMetrics {
        depth: "test.queue.depth",
        bytes: "test.queue.bytes",
        utilization: "test.queue.utilization",
        push_blocked: "test.queue.push.blocked.duration",
        items_dropped: "test.queue.items.dropped",
        units_dropped: "test.queue.units.dropped",
    };

    fn test_queue(
        max_items: usize,
        max_weight: u64,
        overflow: OverflowPolicy,
    ) -> BoundedQueue<TestItem> {
        BoundedQueue::with_metrics(
            QueueConfig { max_items, max_weight, overflow },
            &TEST_METRICS,
            Telemetry::default(),
        )
    }

    #[tokio::test]
    async fn a_generic_queue_bounds_the_item_count_and_the_weight_independently() {
        // Weight bound alone: two items of weight 1 fit under max_weight=2, a third doesn't.
        let q = test_queue(1000, 2, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 5 }).await;
        q.push(TestItem { weight: 1, units: 5 }).await;
        q.push(TestItem { weight: 1, units: 5 }).await; // evicts the first
        assert_eq!(q.commit().expect("should commit").units, 5);
        assert_eq!(q.commit().expect("should commit").units, 5);
        assert!(q.commit().is_none());

        // Item-count bound alone: two zero-weight items fit under max_items=2, a third doesn't.
        let q2 = test_queue(2, u64::MAX, OverflowPolicy::DropOldest);
        q2.push(TestItem { weight: 0, units: 1 }).await;
        q2.push(TestItem { weight: 0, units: 1 }).await;
        q2.push(TestItem { weight: 0, units: 1 }).await; // evicts the first
        assert_eq!(q2.commit().expect("should commit").units, 1);
        assert_eq!(q2.commit().expect("should commit").units, 1);
        assert!(q2.commit().is_none());
    }

    #[tokio::test]
    async fn a_dropped_items_units_not_its_item_count_drives_the_units_dropped_metric() {
        // Not asserting on telemetry output directly (Telemetry::default() is the no-op sink),
        // but proving the evicted item retains its own `units()` value through eviction --
        // `count_dropped` reads it off exactly this item, so if this holds, the metric is
        // reporting the right number.
        let q = test_queue(1, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 40 }).await;
        q.push(TestItem { weight: 1, units: 7 }).await; // evicts the units=40 item
        let remaining = q.commit().expect("should commit");
        assert_eq!(remaining.units, 7, "the surviving item, not the evicted one, should remain");
    }

    #[tokio::test]
    async fn pop_is_fifo_and_returns_none_once_closed_and_empty() {
        let q = test_queue(10, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 1 }).await;
        q.push(TestItem { weight: 1, units: 2 }).await;

        assert_eq!(q.pop().await.expect("should pop").units, 1);
        assert_eq!(q.pop().await.expect("should pop").units, 2);

        q.close();
        assert!(q.pop().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn pop_awaits_on_an_empty_open_queue_and_resolves_once_a_push_lands() {
        let q = Arc::new(test_queue(10, u64::MAX, OverflowPolicy::DropOldest));
        let q2 = Arc::clone(&q);
        let popping = tokio::spawn(async move { q2.pop().await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!popping.is_finished(), "pop should still be waiting on an empty queue");

        q.push(TestItem { weight: 1, units: 9 }).await;

        let popped = tokio::time::timeout(Duration::from_secs(1), popping)
            .await
            .expect("pop should resolve once a push lands")
            .expect("should not panic")
            .expect("should return the pushed item");
        assert_eq!(popped.units, 9);
    }

    /// The cancellation-safety property `pop()` exists for: dropping a `pop()` future mid-wait
    /// (the shutdown-grace cancellation shape) must never leave a head reservation standing --
    /// otherwise a subsequent `drop_oldest` push at capacity could no longer evict at all, since
    /// `evict_to_fit` never touches a reserved head (`crates/logit-proto/src/buffer.rs`).
    #[tokio::test(start_paused = true)]
    async fn a_pop_dropped_mid_wait_leaves_no_reservation_a_later_push_can_still_evict() {
        let q = Arc::new(test_queue(2, u64::MAX, OverflowPolicy::DropOldest));

        {
            // Start a pop on an *empty* queue -- this must park inside `notified.await`, strictly
            // before it ever reserves anything (there is nothing to reserve yet), so dropping it
            // here exercises "cancelled while waiting", the shape a shutdown-grace timeout hits.
            let q2 = Arc::clone(&q);
            let mut popping = Box::pin(async move { q2.pop().await });
            tokio::time::timeout(Duration::from_millis(1), &mut popping)
                .await
                .expect_err("nothing pushed yet -- pop must still be waiting");
            // `popping` (and the `notified` future inside it) is dropped here.
        }

        // At capacity (2 items); a third push only evicts if nothing is reserved -- proving the
        // dropped `pop()` above left no dangling reservation behind.
        q.push(TestItem { weight: 1, units: 1 }).await;
        q.push(TestItem { weight: 1, units: 2 }).await;
        q.push(TestItem { weight: 1, units: 3 }).await; // must evict units=1

        assert_eq!(q.commit().expect("should commit").units, 2, "units=1 should have been evicted");
        assert_eq!(q.commit().expect("should commit").units, 3);
        assert!(q.commit().is_none());
    }
}
