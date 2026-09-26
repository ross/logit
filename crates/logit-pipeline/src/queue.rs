//! [`BoundedQueue`]: the async wrapper around `logit_proto::buffer::Buffer` that decouples a
//! node's I/O from whatever is downstream of it: a sink's delivery queue ([`SinkQueue`],
//! `docs/adr/buffered-sink-delivery.md`) and a UDP listener's receive queue (`ReceiveQueue` in
//! `logit-inputs`, `docs/adr/decoupled-listener-io.md`). `Buffer` is sync, so this type owns what
//! a sync trait can't express: `Block`, which awaits room rather than dropping. [`Queued`] and
//! [`QueueMetrics`] are what let one implementation serve both sides.

use crate::fanout::BatchContext;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_proto::buffer::{Buffer, InMemoryBuffer, OverflowPolicy as DropPolicy, PushOutcome};
use std::iter::Peekable;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::vec;
use tokio::sync::Notify;

/// What a [`BoundedQueue`] needs to know about an item: `weight`, read once at push time and
/// stored with it, and `units`, read once per drop to count it in the unit an operator reasons
/// about (events for a batch, bytes for a datagram).
pub trait Queued: Send + Sync + 'static {
    /// Admission-control weight in bytes: an estimate, not an allocator figure
    /// (`docs/design/memory.md` §5).
    fn weight(&self) -> u64;
    /// How many countable things this item represents, for the `units_dropped` counter.
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

/// [`SinkQueue`]'s item type: a batch and the [`BatchContext`] it arrived with. `BatchContext`
/// is `Copy` and 32 bytes, so it rides inline with no allocation; weight and units come from the
/// batch alone.
impl Queued for (Arc<EventBatch>, BatchContext) {
    fn weight(&self) -> u64 {
        self.0.weight()
    }
    fn units(&self) -> u64 {
        self.0.units()
    }
}

/// Every metric name one [`BoundedQueue`] emits. Each is a compile-time constant, never
/// formatted at runtime, per `docs/design/internal-telemetry.md`'s cardinality convention.
/// [`SINK_QUEUE_METRICS`] is the sink side's; `logit-inputs`' receive queue defines its own.
pub struct QueueMetrics {
    /// Gauge: items currently queued.
    pub depth: &'static str,
    /// Gauge: weight currently queued.
    pub bytes: &'static str,
    /// Gauge: `max(depth ratio, bytes ratio)` against the two configured bounds.
    pub utilization: &'static str,
    /// Timing: how long a `Block`-policy push waited for room; recorded only when it waited.
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

/// What to do when the queue is full. `Block` is layered here on top of the two dropping
/// policies `logit_proto::buffer::OverflowPolicy` can express synchronously. The sink and receive
/// sides differ only in the default: a UDP listener's must not be `Block`
/// (`docs/adr/decoupled-listener-io.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    Block,
    DropOldest,
    DropNewest,
}

/// Bounds and overflow behavior for one [`BoundedQueue`], in generic items/weight terms.
/// [`SinkQueueConfig`] (batches) and `logit-inputs`' `ReceiveQueueConfig` (datagrams) convert
/// into it, so each keeps field names in its operator's unit.
#[derive(Debug, Clone, Copy)]
pub struct QueueConfig {
    pub max_items: usize,
    pub max_weight: u64,
    pub overflow: OverflowPolicy,
}

/// Bounds and overflow behavior for one sink's [`SinkQueue`]. Both bounds are enforced,
/// whichever trips first; `max_bytes` is checked against `EventBatch::estimated_heap_bytes`,
/// computed once per batch at push time.
#[derive(Debug, Clone, Copy)]
pub struct SinkQueueConfig {
    pub max_batches: usize,
    pub max_bytes: u64,
    pub overflow: OverflowPolicy,
}

/// The values a sink gets with no `buffer:` block; keep them in step with
/// `logit_config::BufferConfig::default()`, which `logit-cli::pipeline::queue_config` resolves
/// from. 1024 batches / 64 MiB rides out a destination hiccup while staying bounded. `Block` is
/// the default because dropping data should be a per-sink opt-in.
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

/// A `Vec`'s drain that, when dropped, counts every item it never yielded as dropped under
/// `reason`: one `metrics.items_dropped` per item and its [`Queued::units`] as
/// `metrics.units_dropped`. An item is either yielded, and the caller accounts for it, or still in
/// the drain when it drops, so nothing is counted twice. Exhausting it disarms it: the drop then
/// makes no telemetry call and allocates nothing.
///
/// `reason` is `"shutdown"` at every call site: the shutdown race and the grace backstop are the
/// only production cancellers of a future that holds one.
///
/// The drop calls only `Telemetry::count`, never the queue lock. `Telemetry::count` takes its own
/// buffer's lock, which is never held across an await or while a `CountedDrain` drops
/// (`docs/adr/shutdown-accounting-and-cancellation-safety.md`, "Alternatives considered").
pub struct CountedDrain<'a, T: Queued> {
    drain: Peekable<vec::Drain<'a, T>>,
    telemetry: &'a Telemetry,
    metrics: &'static QueueMetrics,
    reason: &'static str,
}

impl<'a, T: Queued> CountedDrain<'a, T> {
    /// Drains all of `items`. The `Vec` is empty, with its capacity intact, once this drops.
    pub fn new(
        items: &'a mut Vec<T>,
        telemetry: &'a Telemetry,
        metrics: &'static QueueMetrics,
        reason: &'static str,
    ) -> Self {
        Self { drain: items.drain(..).peekable(), telemetry, metrics, reason }
    }

    /// The next item without yielding it. A peeked item that is never yielded is counted.
    pub fn peek(&mut self) -> Option<&T> {
        self.drain.peek()
    }
}

impl<T: Queued> Iterator for CountedDrain<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.drain.next()
    }
}

impl<T: Queued> Drop for CountedDrain<'_, T> {
    fn drop(&mut self) {
        // Iterates the `Peekable` itself: an item `peek` returned sits in its peeked slot, not in
        // the inner `Drain`.
        let mut items = 0u64;
        let mut units = 0u64;
        for item in self.drain.by_ref() {
            items += 1;
            units = units.saturating_add(item.units());
        }
        if items > 0 {
            let tags = [("reason", self.reason)];
            self.telemetry.count(self.metrics.items_dropped, items as f64, &tags);
            self.telemetry.count(self.metrics.units_dropped, units as f64, &tags);
        }
    }
}

/// The async wrapper around `logit_proto::buffer::InMemoryBuffer<T>` between a node's I/O and
/// what it's decoupled from: a sink's inbox drain and its writer, or a UDP listener's socket read
/// and its decode loop. Not `Clone`: one per node, shared by its two sides through an `Arc`.
///
/// `std::sync::Mutex`, not tokio's: no critical section here contains an `.await`.
pub struct BoundedQueue<T: Queued> {
    inner: Mutex<InMemoryBuffer<T>>,
    /// Woken when an item may be readable, for `peek`/`pop`/`pop_many`.
    not_empty: Notify,
    /// Woken when room may be free, or the queue is closing, for a blocked `push`/`push_many`.
    not_full: Notify,
    closed: AtomicBool,
    /// `true` iff the configured [`OverflowPolicy`] is `Block`: `push` awaits room rather than
    /// reaching the underlying buffer's dropping policy.
    block_when_full: bool,
    max_items: usize,
    max_weight: u64,
    metrics: &'static QueueMetrics,
    telemetry: Telemetry,
    /// How many times `update_gauges` has run. [`BoundedQueue::push_many`]/
    /// [`BoundedQueue::pop_many`] exist to make it once per call, and a drained `Registry` can't
    /// show that: `Telemetry::gauge` is last-write-wins per series, so one write and sixty-four
    /// drain as the same point (which is why batching them is safe). Test builds only.
    #[cfg(test)]
    gauge_updates: std::sync::atomic::AtomicUsize,
}

impl<T: Queued> BoundedQueue<T> {
    /// Builds a queue that emits under `metrics`.
    ///
    /// Under `Block`, the underlying `InMemoryBuffer` is built with `DropOldest`: `push` awaits
    /// room before it calls `Buffer::push`, so the dropping policy is reached only when `push`
    /// falls through without waiting (an item that can never fit, or a push racing
    /// [`BoundedQueue::close`]). `DropOldest`, not `DropNewest`, so that fallback evicts what it
    /// can and accepts the item rather than dropping one `Block` promised to keep.
    ///
    /// `InMemoryBuffer::new` preallocates `max_items.min(4096)` slots, so a deep receive queue
    /// pays no warm-up reallocations on the hot path.
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
            #[cfg(test)]
            gauge_updates: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many `update_gauges` calls this queue has made (see the field).
    #[cfg(test)]
    fn gauge_updates(&self) -> usize {
        self.gauge_updates.load(Ordering::Relaxed)
    }

    /// Pushes `item`, weighing it once by [`Queued::weight`].
    ///
    /// Under `Block`, waits for room, re-checked under the lock on every wakeup. Race-free because
    /// the `Notified` future is constructed before the state check it guards, so a
    /// `commit()`/`pop()`/`close()` landing after that point is never missed. Under
    /// `DropOldest`/`DropNewest`: one lock acquisition, one push attempt, no waiting.
    ///
    /// Never blocks on an item that could never fit even an empty queue (`weight` alone exceeds
    /// `max_weight`, or `max_items` is `0`): no wait could free enough room. It falls through to
    /// the `DropOldest` fallback (see `with_metrics`), which evicts what it can and accepts the
    /// item rather than wedging this node and everything upstream.
    ///
    /// Every accepted push notifies `not_empty` once. Only a push that waited records a
    /// `metrics.push_blocked` sample, spanning the whole wait.
    pub async fn push(&self, item: T) {
        let weight = item.weight();
        let impossible_to_ever_fit = weight > self.max_weight || self.max_items == 0;
        let mut waited = false;
        let mut blocked_timer: Option<logit_core::telemetry::Timer> = None;
        // `Option` so this needs no `T: Clone`: a retry never consumes `item`, and the one arm
        // that `take()`s it breaks the loop immediately.
        let mut item = Some(item);

        let (len, total_weight, evicted, rejected) = loop {
            // Registered before the state check: see the doc comment.
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

        // Records a sample only if this push waited.
        drop(blocked_timer);
        self.not_empty.notify_one();
        self.update_gauges(len, total_weight);
    }

    /// [`BoundedQueue::push`] over a whole batch. Each item gets the same admission control
    /// `push` applies: its own weight, the never-fits fallback, `Block` waiting, `DropOldest`
    /// eviction (never the reserved head) or `DropNewest` rejection, and a drop count with its own
    /// [`Queued::units`]. Only the bookkeeping is per call: one `update_gauges` for the whole
    /// invocation, and one lock acquisition per contiguous run of items that fit
    /// (`docs/adr/udp-intake-batching-and-socket-visibility.md`, "`push_many`/`pop_many` live on
    /// `BoundedQueue` itself").
    ///
    /// `items` is left empty with its capacity intact once the future has been polled, including
    /// on cancellation (dropping the [`CountedDrain`] removes what it had not yielded). A future
    /// dropped without ever being polled never touches `items`. An empty `items` is a no-op: no
    /// lock, no notification, no gauge update.
    ///
    /// At most one `push_blocked` sample per call, spanning the first wait to the last, so one
    /// 5 ms stall stays distinguishable from 64 stalls of 78 µs.
    ///
    /// **Notification: one `not_empty.notify_one()` at the end, plus one before every wait; never
    /// `notify_waiters()`.** The end-of-call notification alone is a lost wakeup under `Block`: a
    /// batch bigger than the free room admits a prefix and parks, and a consumer that parked on
    /// `not_empty` first would wait for a permit sent only after this call returns, while this
    /// call waits for that consumer to free room. Notifying before the suspend point also covers
    /// a call cancelled mid-wait. `notify_one` stores a permit, so a consumer that decided the
    /// queue was empty but hasn't registered its `Notified` yet still wakes; `notify_waiters()`
    /// stores nothing and would lose that race. Repeated permits collapse into one, so they are
    /// not extra wakeups.
    ///
    /// **Cancellation counts the remainder.** A future dropped mid-call (the listener's shutdown
    /// race, or the grace backstop) leaves the accepted prefix queued, accounted for, and already
    /// announced. The [`CountedDrain`] counts the unreached remainder as
    /// `items_dropped`/`units_dropped{reason="shutdown"}` as it drops, without the queue lock
    /// (`docs/adr/shutdown-accounting-and-cancellation-safety.md`, decision 4). A future dropped
    /// before its first poll has taken nothing out of `items`, so the caller still holds every
    /// item and is the one that must count them.
    pub async fn push_many(&self, items: &mut Vec<T>) {
        if items.is_empty() {
            return;
        }
        let mut blocked_timer: Option<logit_core::telemetry::Timer> = None;
        // What one lock acquisition evicted or rejected, counted and freed right after the lock
        // is released. Deferring to the end of the call would make the counts lag a long `Block`
        // wait and keep evicted datagrams alive for it.
        let mut dropped: Vec<(&'static str, T)> = Vec::new();
        let (len, total_weight) = {
            let mut drain = CountedDrain::new(items, &self.telemetry, self.metrics, "shutdown");
            // The head item's weight, computed once however many times the wait loop re-examines
            // it.
            let mut head_weight: Option<u64> = None;
            loop {
                // Registered before the state check, as in `push`.
                let notified = self.not_full.notified();

                let (must_wait, len, total_weight) = {
                    let mut inner =
                        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    let must_wait = loop {
                        let Some(next) = drain.peek() else { break false };
                        let weight = *head_weight.get_or_insert_with(|| next.weight());
                        let impossible_to_ever_fit =
                            weight > self.max_weight || self.max_items == 0;
                        if self.block_when_full
                            && self.would_overflow(&inner, weight)
                            && !impossible_to_ever_fit
                            && !self.closed.load(Ordering::Acquire)
                        {
                            break true;
                        }
                        let item = drain.next().expect("just peeked");
                        head_weight = None;
                        match inner.push(item, weight) {
                            PushOutcome::Accepted => {}
                            PushOutcome::Evicted(evicted) => dropped
                                .extend(evicted.into_iter().map(|item| ("overflow_oldest", item))),
                            PushOutcome::Rejected(rejected) => {
                                dropped.push(("overflow_newest", rejected))
                            }
                        }
                    };
                    (must_wait, inner.len(), inner.weight())
                };

                for (reason, item) in dropped.drain(..) {
                    self.count_dropped(reason, &item);
                }

                if !must_wait {
                    break (len, total_weight);
                }
                // Announce what this call has admitted before suspending, or a consumer parked on
                // `not_empty` and this call wait on each other forever (`max_items: 2`, a batch of
                // 5, `Block`); see the doc comment.
                //
                // Never spurious: `must_wait` implies `would_overflow`, and
                // `!impossible_to_ever_fit` rules out degenerate configs, so either
                // `len >= max_items >= 1` or `weight() + w > max_weight` with `w <= max_weight`,
                // which needs `weight() > 0`. The queue is non-empty.
                self.not_empty.notify_one();
                if blocked_timer.is_none() {
                    blocked_timer = Some(self.telemetry.timer(self.metrics.push_blocked));
                }
                notified.await;
            }
        };

        // Records a sample only if this call waited.
        drop(blocked_timer);
        self.not_empty.notify_one();
        self.update_gauges(len, total_weight);
    }

    /// Removes and returns the head (`None` on an empty queue), clearing any reservation from
    /// [`BoundedQueue::peek`], wakes a blocked `push`, and refreshes the gauges. After a `peek`,
    /// this removes the peeked item only under `peek`'s one-consumer contract.
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

    /// Awaits and removes the head in one step, for a consumer with no retry (a decode loop).
    /// `None` once the queue is closed and empty. Cancellation-safe, unlike `peek().await` then
    /// `commit()`: `peek` reserves the head against `drop_oldest` eviction, and a task dropped
    /// between the two (a shutdown-grace cancellation) leaves the reservation standing forever,
    /// letting the queue grow past its bound. `pop` never awaits between reserving and removing.
    pub async fn pop(&self) -> Option<T> {
        loop {
            let notified = self.not_empty.notified();
            let (item, len, weight) = {
                let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                // Reserve and remove under one lock, with no suspend point between them.
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

    /// [`BoundedQueue::pop`] over a batch: awaits at least one item, removes up to `max` under one
    /// lock acquisition, appends them to `out` in FIFO order, and returns how many. `0` means
    /// closed and empty, and never happens on a live queue. `out` is appended to, never cleared,
    /// and untouched on a `0` return.
    ///
    /// One `update_gauges` call per invocation (see [`BoundedQueue::push_many`]).
    ///
    /// **`max == 0` is a caller bug**: it would wait for an item and remove none, parking forever
    /// against a non-empty queue. A `debug_assert!` catches it; release builds clamp it to 1.
    ///
    /// Cancellation-safe, as `pop` is: no `.await` between locking and removing, `Buffer::commit`
    /// clears any head reservation, and the call awaits only on an iteration that removed nothing,
    /// so no item is lost into a half-filled `out`.
    pub async fn pop_many(&self, out: &mut Vec<T>, max: usize) -> usize {
        debug_assert!(max > 0, "pop_many(max = 0) would wait for an item and then remove none");
        let max = max.max(1);
        loop {
            let notified = self.not_empty.notified();
            let (popped, len, weight) = {
                let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let mut popped = 0usize;
                while popped < max {
                    // `commit` without `peek`: a reservation protects a head across a gap
                    // between calls, and there is none under this one lock.
                    let Some(item) = inner.commit() else { break };
                    out.push(item);
                    popped += 1;
                }
                if popped == 0 && self.closed.load(Ordering::Acquire) {
                    return 0;
                }
                (popped, inner.len(), inner.weight())
            };
            if popped > 0 {
                // One `notify_one` per item removed, as `popped` sequential `pop`s would: one
                // call wakes one pusher, and this may have freed room for several. `notify_one`,
                // not `notify_waiters()`, for the permit (see `push_many`). With 0 or 1 waiters
                // the extra calls collapse into one permit.
                for _ in 0..popped {
                    self.not_full.notify_one();
                }
                self.update_gauges(len, weight);
                return popped;
            }
            notified.await;
        }
    }

    /// Marks the queue closed: once it is also empty, `peek`/`pop`/`pop_many` return `None`/`0`.
    /// Wakes every waiter on both `Notify`s, so no consumer or blocked `push` hangs.
    ///
    /// **`notify_waiters()` stores no permit, and needs none.** `Notify::notified()` snapshots the
    /// `notify_waiters` call counter when the future is constructed, and the first poll compares
    /// it before anything else (tokio 1.53.1, `sync/notify.rs`, `poll_notified`'s `State::Init`
    /// arm). Every wait loop here constructs its `Notified` before its state check, so a `close()`
    /// after construction is seen at the first poll and one before is seen by the check.
    /// `a_notify_waiters_call_between_constructing_a_notified_and_polling_it_is_never_lost` pins
    /// that tokio behavior.
    ///
    /// A push racing a close re-checks state, sees `closed`, and makes one best-effort attempt
    /// against the `DropOldest` fallback (see `with_metrics`): it may evict (never the reserved
    /// head) or accept over-bound, but never panics or hangs. Callers close only after they stop
    /// producing, so this does not arise in practice.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }

    /// Saturating, as `InMemoryBuffer`'s own check is: `Queued::weight` is generic, and an
    /// overflowing add would panic under the lock in debug builds or wrap to "fits" in release.
    fn would_overflow(&self, inner: &InMemoryBuffer<T>, weight: u64) -> bool {
        inner.len() >= self.max_items || inner.weight().saturating_add(weight) > self.max_weight
    }

    fn count_dropped(&self, reason: &'static str, item: &T) {
        self.telemetry.count(self.metrics.items_dropped, 1.0, &[("reason", reason)]);
        self.telemetry.count(
            self.metrics.units_dropped,
            item.units() as f64,
            &[("reason", reason)],
        );
    }

    /// `metrics.utilization` is `max(items ratio, bytes ratio)`: whichever bound is closer to
    /// tripping predicts the next block or drop. A zero bound reports a ratio of 0 rather than
    /// NaN or infinity.
    ///
    /// Runs after the lock is released, so under callers on parallel threads the last write can
    /// carry an older state than the queue's. Benign in production: each queue's producer and
    /// consumer halves run in one task, so their calls never overlap.
    fn update_gauges(&self, len: usize, weight: u64) {
        #[cfg(test)]
        self.gauge_updates.fetch_add(1, Ordering::Relaxed);
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
    /// A clone of the head, without removing it, for a caller that removes it with
    /// [`BoundedQueue::commit`] only once its action succeeds. Reserves the head against
    /// `DropOldest` eviction until that `commit`, so a delivered batch is the one committed. A
    /// consumer with no retry should use the cancellation-safe [`BoundedQueue::pop`] instead.
    ///
    /// Awaits while the queue is empty and open; returns `None` once it is closed and empty,
    /// both checked under one lock so a racing `push` is either seen or left for the next
    /// iteration.
    ///
    /// **One consumer.** `peek` then `commit` removes the peeked item only if nothing else
    /// removed items in between. Two `peek`/`commit` consumers, or one beside a `pop`/`pop_many`
    /// consumer, deliver one item twice and lose the next: A peeks X, B's `pop_many` removes X,
    /// and A's `commit` removes Y, which nobody delivered. Every production queue has one
    /// consumer: a sink's `write_loop`, or a listener's decode loop.
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

/// A sink's delivery queue. Each batch keeps the [`BatchContext`] it arrived with: `write_loop`'s
/// sink span needs its trace context
/// (`docs/adr/internal-span-emission-and-deterministic-sampling.md`), and
/// `Output::observe_batch` its provenance (`docs/adr/batch-provenance-on-delivered.md`).
pub type SinkQueue = BoundedQueue<(Arc<EventBatch>, BatchContext)>;

impl SinkQueue {
    pub fn new(config: SinkQueueConfig, telemetry: Telemetry) -> Self {
        Self::with_metrics(config.into(), &SINK_QUEUE_METRICS, telemetry)
    }
}

/// A sink's queue, chosen per component from its `buffer:` block: `Memory` ([`SinkQueue`]) or
/// `Disk` (`crate::disk_queue::DiskQueue`, `docs/adr/disk-backed-sink-buffer.md`). Enum dispatch
/// over the two known implementations. `DiskQueue` does not implement
/// `logit_proto::buffer::Buffer<T>`, whose sync `&mut self` shape can't express an async,
/// file-backed queue.
pub enum SinkStore {
    Memory(SinkQueue),
    // Boxed so the common `Memory` variant isn't padded to `DiskQueue`'s size
    // (`clippy::large_enum_variant`).
    Disk(Box<crate::disk_queue::DiskQueue>),
}

/// [`SinkStore`] at the config stage: `logit-cli::pipeline::queue_config` builds it from a
/// component's `logit_config::BufferConfig`, and `run_output` opens it with [`SinkStore::open`].
pub enum SinkStoreConfig {
    Memory(SinkQueueConfig),
    Disk(crate::disk_queue::DiskQueueConfig),
}

impl SinkStore {
    /// Builds the store a `SinkStoreConfig` describes. Infallible for `Memory`. `Disk` opens or
    /// recovers the spool directory, which fails on a bad path, a permissions error, or another
    /// process holding the lock; that is a startup error for the component.
    pub fn open(
        config: SinkStoreConfig,
        telemetry: Telemetry,
        diag: Diagnostics,
    ) -> anyhow::Result<Self> {
        match config {
            SinkStoreConfig::Memory(cfg) => Ok(SinkStore::Memory(SinkQueue::new(cfg, telemetry))),
            SinkStoreConfig::Disk(cfg) => Ok(SinkStore::Disk(Box::new(
                crate::disk_queue::DiskQueue::open(cfg, telemetry, diag)?,
            ))),
        }
    }

    pub async fn push(&self, item: (Arc<EventBatch>, BatchContext)) {
        match self {
            SinkStore::Memory(q) => q.push(item).await,
            SinkStore::Disk(q) => q.push(item).await,
        }
    }

    pub async fn peek(&self) -> Option<(Arc<EventBatch>, BatchContext)> {
        match self {
            SinkStore::Memory(q) => q.peek().await,
            SinkStore::Disk(q) => q.peek().await,
        }
    }

    /// Advances past the head, returning it. Production callers ignore the value; tests read it.
    pub fn commit(&self) -> Option<(Arc<EventBatch>, BatchContext)> {
        match self {
            SinkStore::Memory(q) => q.commit(),
            SinkStore::Disk(q) => q.commit(),
        }
    }

    pub fn close(&self) {
        match self {
            SinkStore::Memory(q) => q.close(),
            SinkStore::Disk(q) => q.close(),
        }
    }

    /// Shutdown-time finalization, called from `crate::runtime::finish_and_flush` once nothing
    /// can push into this store. Returns `(dropped_batches, dropped_events)`.
    ///
    /// `Memory` commits everything still held and counts it dropped: this is the last chance to
    /// count it.
    ///
    /// `Disk` drops nothing and returns `(0, 0)`: it persists and `fsync`s the read cursor, the
    /// active segment, and the directory, so what is queued delivers after the next
    /// `DiskQueue::open`. The shutdown grace bounds delivery attempts, never what a disk-backed
    /// sink holds.
    pub async fn finish(&self) -> (u64, u64) {
        match self {
            SinkStore::Memory(q) => {
                let mut dropped_batches = 0u64;
                let mut dropped_events = 0u64;
                while let Some((batch, _ctx)) = q.commit() {
                    dropped_batches += 1;
                    dropped_events += batch.events.len() as u64;
                }
                (dropped_batches, dropped_events)
            }
            SinkStore::Disk(q) => {
                q.finish().await;
                (0, 0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::TraceContext;
    use logit_core::{AttrMap, Event, Resource, Value};
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    /// A batch carrying about `extra_bytes` of attribute payload, to force the byte bound without
    /// depending on the `estimated_heap_bytes` formula.
    fn batch(extra_bytes: usize) -> Arc<EventBatch> {
        let mut attrs = AttrMap::new();
        if extra_bytes > 0 {
            attrs.insert("payload", Value::str("x".repeat(extra_bytes)));
        }
        Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::empty(0, attrs)],
        })
    }

    fn tiny_batch() -> Arc<EventBatch> {
        batch(0)
    }

    fn queue(max_batches: usize, max_bytes: u64, overflow: OverflowPolicy) -> SinkQueue {
        SinkQueue::new(SinkQueueConfig { max_batches, max_bytes, overflow }, Telemetry::default())
    }

    /// A placeholder context. `push_then_peek_then_commit_round_trips_one_batch` checks a real
    /// one round-trips.
    fn ctx() -> BatchContext {
        BatchContext::default()
    }

    #[tokio::test]
    async fn push_then_peek_then_commit_round_trips_one_batch() {
        let q = queue(10, u64::MAX, OverflowPolicy::Block);
        let sent = tiny_batch();
        let sent_ctx = BatchContext {
            trace: TraceContext::new_root(),
            provenance: logit_core::Provenance {
                origin: Some(logit_core::interner::intern("nginx_in")),
                previous: Some(logit_core::interner::intern("logit_out")),
            },
        };
        q.push((Arc::clone(&sent), sent_ctx)).await;

        let (peeked, peeked_ctx) = q.peek().await.expect("should peek the pushed batch");
        assert!(Arc::ptr_eq(&peeked, &sent));
        assert_eq!(
            peeked_ctx, sent_ctx,
            "the context (trace and provenance alike) pushed with a batch should come back \
             unchanged"
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

        // Let the spawned push park on `not_full`.
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

    /// A peeked head survives a `DropOldest` eviction, so `commit()` after delivering it removes
    /// that head, not the never-sent batch behind it.
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

    /// Under `Block`, a batch heavier than `max_bytes` is accepted rather than waiting forever.
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

    /// Under `Block`, `max_batches: 0` does not hang a push.
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
        // Room for one batch by weight, a thousand by count.
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
        // `count_dropped` reads `units()` off the evicted item; this checks which item that is.
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

    /// A `pop()` dropped mid-wait leaves no head reservation, so a later `drop_oldest` push can
    /// still evict (`evict_to_fit` never touches a reserved head).
    #[tokio::test(start_paused = true)]
    async fn a_pop_dropped_mid_wait_leaves_no_reservation_a_later_push_can_still_evict() {
        let q = Arc::new(test_queue(2, u64::MAX, OverflowPolicy::DropOldest));

        {
            // Park a pop on an empty queue, then drop it mid-wait.
            let q2 = Arc::clone(&q);
            let mut popping = Box::pin(async move { q2.pop().await });
            tokio::time::timeout(Duration::from_millis(1), &mut popping)
                .await
                .expect_err("nothing pushed yet -- pop must still be waiting");
            // `popping` (and the `notified` future inside it) is dropped here.
        }

        // At capacity, a third push evicts only if nothing is reserved.
        q.push(TestItem { weight: 1, units: 1 }).await;
        q.push(TestItem { weight: 1, units: 2 }).await;
        q.push(TestItem { weight: 1, units: 3 }).await; // must evict units=1

        assert_eq!(q.commit().expect("should commit").units, 2, "units=1 should have been evicted");
        assert_eq!(q.commit().expect("should commit").units, 3);
        assert!(q.commit().is_none());
    }

    // -- `push_many` / `pop_many`: one lock and one gauge update per batch, per-item admission. --
    // Tests with a single-item twin above word their assertions the same way.

    /// A queue whose telemetry records, so a test can read back drop counts.
    fn recording_queue(
        max_items: usize,
        max_weight: u64,
        overflow: OverflowPolicy,
    ) -> (Arc<logit_core::Registry>, BoundedQueue<TestItem>) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("test", "input", "source");
        let q = BoundedQueue::with_metrics(
            QueueConfig { max_items, max_weight, overflow },
            &TEST_METRICS,
            telemetry,
        );
        (registry, q)
    }

    /// Sums every recorded point named `name` (only those carrying `tag`, if given) in a drained
    /// `Registry`.
    fn metric_sum(events: &[Event], name: &str, tag: Option<(&str, &str)>) -> f64 {
        let name_sym = logit_core::interner::intern(name);
        events
            .iter()
            .filter(|e| match tag {
                Some((k, v)) => e.attributes.get(k).and_then(Value::as_str) == Some(v),
                None => true,
            })
            .flat_map(|e| e.metrics.iter())
            .filter(|m| m.name == name_sym)
            .map(|m| match &m.kind {
                logit_core::MetricKind::Sum(s) => s.value,
                logit_core::MetricKind::Gauge(v) => *v,
                _ => 0.0,
            })
            .sum()
    }

    fn recorded_point(events: &[Event], name: &str) -> bool {
        let name_sym = logit_core::interner::intern(name);
        events.iter().flat_map(|e| e.metrics.iter()).any(|m| m.name == name_sym)
    }

    /// `[(weight, units), ...]` as a batch ready to hand to `push_many`.
    fn batch_of(specs: &[(u64, u64)]) -> Vec<TestItem> {
        specs.iter().map(|&(weight, units)| TestItem { weight, units }).collect()
    }

    fn drain_units(q: &BoundedQueue<TestItem>) -> Vec<u64> {
        let mut out = Vec::new();
        while let Some(item) = q.commit() {
            out.push(item.units);
        }
        out
    }

    #[tokio::test]
    async fn push_many_drains_the_vec_leaving_it_empty_with_its_capacity_intact() {
        let q = test_queue(10, u64::MAX, OverflowPolicy::DropOldest);
        let mut items = batch_of(&[(1, 1), (1, 2), (1, 3)]);
        let capacity = items.capacity();

        q.push_many(&mut items).await;

        assert!(items.is_empty(), "push_many must drain its input");
        assert_eq!(
            items.capacity(),
            capacity,
            "draining (not replacing) the Vec is what lets a caller reuse one buffer for free"
        );
        assert_eq!(drain_units(&q), vec![1, 2, 3], "and in order");
    }

    #[tokio::test]
    async fn push_many_with_nothing_to_push_is_a_complete_no_op() {
        let q = test_queue(10, u64::MAX, OverflowPolicy::DropOldest);
        let mut items: Vec<TestItem> = Vec::new();
        q.push_many(&mut items).await;
        assert_eq!(q.gauge_updates(), 0, "an empty batch should not even touch the gauges");
    }

    /// Batched `DropOldest` eviction counts each evicted item with its own `units()`.
    #[tokio::test]
    async fn push_many_under_drop_oldest_evicts_and_counts_each_evicted_item_with_its_own_units() {
        let (registry, q) = recording_queue(2, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 40 }).await;
        q.push(TestItem { weight: 1, units: 7 }).await;

        // Evicts units=40, units=7, then units=100, as three sequential pushes would.
        let mut items = batch_of(&[(1, 100), (1, 200), (1, 300)]);
        q.push_many(&mut items).await;

        assert_eq!(drain_units(&q), vec![200, 300]);
        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, TEST_METRICS.items_dropped, Some(("reason", "overflow_oldest"))),
            3.0,
            "one drop counted per evicted item, never one per push_many call"
        );
        assert_eq!(
            metric_sum(&events, TEST_METRICS.units_dropped, Some(("reason", "overflow_oldest"))),
            40.0 + 7.0 + 100.0,
            "each evicted item contributes its own units(), not a per-item 1"
        );
    }

    /// Batched `DropNewest` rejects the overflow and counts each rejected item.
    #[tokio::test]
    async fn push_many_under_drop_newest_rejects_the_overflow_and_counts_each_rejected_item() {
        let (registry, q) = recording_queue(2, u64::MAX, OverflowPolicy::DropNewest);

        let mut items = batch_of(&[(1, 1), (1, 2), (1, 30), (1, 40)]);
        q.push_many(&mut items).await;

        assert_eq!(drain_units(&q), vec![1, 2], "the first two fit; the rest are rejected");
        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, TEST_METRICS.items_dropped, Some(("reason", "overflow_newest"))),
            2.0
        );
        assert_eq!(
            metric_sum(&events, TEST_METRICS.units_dropped, Some(("reason", "overflow_newest"))),
            30.0 + 40.0
        );
    }

    /// Under `Block`, the prefix that fits is accepted and the rest waits rather than dropping.
    #[tokio::test(start_paused = true)]
    async fn push_many_under_block_accepts_the_prefix_that_fits_and_waits_for_room_for_the_rest() {
        let q = Arc::new(test_queue(2, u64::MAX, OverflowPolicy::Block));
        let q2 = Arc::clone(&q);
        let pushing = tokio::spawn(async move {
            let mut items = batch_of(&[(1, 1), (1, 2), (1, 3)]);
            q2.push_many(&mut items).await;
            items.len()
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!pushing.is_finished(), "the third item should still be waiting for room");

        let mut popped = Vec::new();
        assert_eq!(q.pop_many(&mut popped, 2).await, 2, "frees both slots at once");

        let left = tokio::time::timeout(Duration::from_secs(1), pushing)
            .await
            .expect("the blocked push_many should resolve once room is freed")
            .expect("the spawned task should not panic");
        assert_eq!(left, 0, "the Vec is drained even on the path that had to wait");
        assert_eq!(popped.iter().map(|i| i.units).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(drain_units(&q), vec![3]);
    }

    /// The weight bound alone can admit a batch partially.
    #[tokio::test(start_paused = true)]
    async fn push_many_under_block_waits_on_the_byte_bound_alone_and_completes_when_a_pop_frees_it()
    {
        let q = Arc::new(test_queue(1000, 10, OverflowPolicy::Block));
        let q2 = Arc::clone(&q);
        let pushing = tokio::spawn(async move {
            let mut items = batch_of(&[(6, 1), (4, 2), (5, 3)]);
            q2.push_many(&mut items).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!pushing.is_finished(), "weight 6+4 fills the 10-byte bound; the 5 must wait");

        let mut popped = Vec::new();
        assert_eq!(
            q.pop_many(&mut popped, 1).await,
            1,
            "frees 6 bytes -- enough for the last item"
        );

        tokio::time::timeout(Duration::from_secs(1), pushing)
            .await
            .expect("the blocked push_many should resolve once room is freed")
            .expect("the spawned task should not panic");
        assert_eq!(drain_units(&q), vec![2, 3]);
    }

    /// The never-fits check is per item, as in `push`, so such an item doesn't park the batch.
    #[tokio::test]
    async fn push_many_with_an_item_that_can_never_fit_takes_the_same_fallback_as_push() {
        let q = test_queue(1000, 10, OverflowPolicy::Block);
        let mut items = batch_of(&[(1, 1), (99, 2)]);

        tokio::time::timeout(Duration::from_secs(5), q.push_many(&mut items))
            .await
            .expect("an item that can never fit must be accepted, not block the batch forever");

        // The fallback evicts units=1 and accepts the heavy item over-bound, as `push` does.
        assert!(items.is_empty());
        assert_eq!(drain_units(&q), vec![2]);

        let single = test_queue(1000, 10, OverflowPolicy::Block);
        single.push(TestItem { weight: 1, units: 1 }).await;
        single.push(TestItem { weight: 99, units: 2 }).await;
        assert_eq!(drain_units(&single), vec![2], "the same outcome as push, item for item");
    }

    /// `max_items: 0` does not hang a `push_many`.
    #[tokio::test]
    async fn push_many_against_a_zero_max_items_config_does_not_hang() {
        let q = test_queue(0, u64::MAX, OverflowPolicy::Block);
        let mut items = batch_of(&[(1, 1), (1, 2)]);
        tokio::time::timeout(Duration::from_secs(5), q.push_many(&mut items))
            .await
            .expect("max_items: 0 must not permanently block a batch either");
        assert!(items.is_empty());
    }

    /// A close wakes a waiting `push_many`, which takes `push`'s `DropOldest` fallback.
    #[tokio::test(start_paused = true)]
    async fn push_many_on_a_queue_that_closes_mid_wait_behaves_like_push_on_a_closed_queue() {
        let q = Arc::new(test_queue(1, u64::MAX, OverflowPolicy::Block));
        let q2 = Arc::clone(&q);
        let pushing = tokio::spawn(async move {
            let mut items = batch_of(&[(1, 1), (1, 2)]);
            q2.push_many(&mut items).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!pushing.is_finished(), "the second item is waiting for room in a 1-slot queue");

        q.close();

        tokio::time::timeout(Duration::from_secs(1), pushing)
            .await
            .expect("a close must wake the batch rather than leave it parked forever")
            .expect("the spawned task should not panic");
        assert_eq!(drain_units(&q), vec![2], "units=1 was evicted by the fallback, as in `push`");

        // And a batch that starts against an already-closed queue takes the same path.
        let q3 = test_queue(1, u64::MAX, OverflowPolicy::Block);
        q3.close();
        let mut items = batch_of(&[(1, 10), (1, 20)]);
        tokio::time::timeout(Duration::from_secs(1), q3.push_many(&mut items))
            .await
            .expect("a closed queue must never park a push_many");
        assert_eq!(drain_units(&q3), vec![20]);
    }

    /// A cancelled `push_many` keeps its accepted prefix, counts the remainder as `shutdown` drops
    /// with each item's own units, empties the caller's `Vec`, and leaves the queue's accounting
    /// exact.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_push_many_keeps_its_prefix_and_counts_its_remainder_as_shutdown_drops() {
        let (registry, q) = recording_queue(2, u64::MAX, OverflowPolicy::Block);
        let mut items = batch_of(&[(1, 1), (1, 2), (1, 3), (1, 4)]);

        {
            let mut pushing = Box::pin(q.push_many(&mut items));
            tokio::time::timeout(Duration::from_millis(1), &mut pushing)
                .await
                .expect_err("the queue holds 2 of 4 -- push_many must still be waiting");
            // Dropping `pushing` drops the `CountedDrain` holding items 3 and 4.
        }

        assert!(items.is_empty(), "the caller's Vec is emptied even by a cancellation");
        assert_eq!(drain_units(&q), vec![1, 2], "the accepted prefix stayed accepted");

        let events = registry.drain(0);
        let shutdown = Some(("reason", "shutdown"));
        assert_eq!(metric_sum(&events, TEST_METRICS.items_dropped, shutdown), 2.0);
        assert_eq!(
            metric_sum(&events, TEST_METRICS.units_dropped, shutdown),
            3.0 + 4.0,
            "each unreached item counts its own units()"
        );
        assert_eq!(
            metric_sum(&events, TEST_METRICS.items_dropped, None),
            2.0,
            "nothing else is counted: the prefix was admitted, not dropped"
        );

        // Exact accounting: once drained, the queue takes a full batch without evicting.
        let (registry2, q2) = recording_queue(2, u64::MAX, OverflowPolicy::Block);
        let mut items2 = batch_of(&[(1, 1), (1, 2), (1, 3)]);
        {
            let mut pushing = Box::pin(q2.push_many(&mut items2));
            tokio::time::timeout(Duration::from_millis(1), &mut pushing).await.expect_err("waits");
        }
        let mut popped = Vec::new();
        assert_eq!(q2.pop_many(&mut popped, 8).await, 2);
        let mut fresh = batch_of(&[(1, 9), (1, 8)]);
        q2.push_many(&mut fresh).await;
        assert_eq!(drain_units(&q2), vec![9, 8]);
        let events2 = registry2.drain(0);
        assert_eq!(
            metric_sum(&events2, TEST_METRICS.items_dropped, Some(("reason", "overflow_oldest"))),
            0.0,
            "a queue whose accounting leaked would have evicted to make room for these two"
        );
        assert_eq!(metric_sum(&events2, TEST_METRICS.items_dropped, shutdown), 1.0);
    }

    /// A `push_many` future dropped before its first poll has taken nothing: every item is still in
    /// the caller's `Vec`, and nothing is counted. Counting what it holds is the caller's job.
    #[tokio::test]
    async fn a_push_many_never_polled_before_shutdown_leaves_every_item_in_the_callers_vec_uncounted(
    ) {
        let (registry, q) = recording_queue(2, u64::MAX, OverflowPolicy::Block);
        let mut items = batch_of(&[(1, 1), (1, 2), (1, 3), (1, 4)]);

        let pushing = q.push_many(&mut items);
        drop(pushing);

        assert_eq!(
            items.iter().map(|i| i.units).collect::<Vec<_>>(),
            vec![1, 2, 3, 4],
            "an unpolled push_many leaves the caller's Vec intact"
        );
        assert!(q.commit().is_none(), "and queues nothing");
        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, TEST_METRICS.items_dropped, None),
            0.0,
            "and counts nothing: the caller still holds the items"
        );
        assert_eq!(q.gauge_updates(), 1, "only the commit above refreshed the gauges");
    }

    /// Weights summing past `u64::MAX` saturate, so committing them never underflows, and an empty
    /// queue weighs 0: a `Block` push against it completes rather than waiting forever.
    #[tokio::test]
    async fn commit_after_saturated_weights_never_underflows_and_an_empty_queue_has_zero_weight() {
        let q = test_queue(10, u64::MAX, OverflowPolicy::Block);
        q.push(TestItem { weight: u64::MAX, units: 1 }).await;
        q.push(TestItem { weight: 5, units: 2 }).await;
        assert_eq!(q.commit().expect("should commit").units, 1);
        assert_eq!(q.commit().expect("should commit").units, 2);
        {
            let inner = q.inner.lock().unwrap_or_else(|p| p.into_inner());
            assert_eq!(inner.len(), 0);
            assert_eq!(inner.weight(), 0, "len == 0 must mean weight == 0");
        }
        tokio::time::timeout(Duration::from_secs(5), q.push(TestItem { weight: 1, units: 3 }))
            .await
            .expect("a Block push against an empty queue must complete");
        assert_eq!(drain_units(&q), vec![3]);
    }

    /// Near `u64::MAX`, `would_overflow` reports a full queue rather than wrapping to "fits".
    #[tokio::test]
    async fn would_overflow_saturates_rather_than_wrapping_near_u64_max() {
        let (registry, q) = recording_queue(10, u64::MAX - 1, OverflowPolicy::DropNewest);
        q.push(TestItem { weight: u64::MAX - 1, units: 1 }).await;
        {
            let inner = q.inner.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                q.would_overflow(&inner, 2),
                "(u64::MAX - 1) + 2 wraps to 0 unchecked, which would read as room to spare"
            );
            assert!(!q.would_overflow(&inner, 0), "a weightless item still fits");
        }
        q.push(TestItem { weight: 2, units: 2 }).await;
        assert_eq!(
            drain_units(&q),
            vec![1],
            "the heavy item stays; the overflowing one is rejected"
        );
        assert_eq!(
            metric_sum(
                &registry.drain(0),
                TEST_METRICS.items_dropped,
                Some(("reason", "overflow_newest"))
            ),
            1.0
        );
    }

    /// A `CountedDrain` counts what it never yielded, a peeked item included, and nothing for what
    /// it yielded; an exhausted one records no point at all.
    #[test]
    fn counted_drain_counts_only_what_it_never_yielded() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("test", "input", "source");
        let mut items = batch_of(&[(1, 1), (1, 2), (1, 3), (1, 4), (1, 5)]);
        let capacity = items.capacity();
        {
            let mut drain = CountedDrain::new(&mut items, &telemetry, &TEST_METRICS, "shutdown");
            assert_eq!(drain.next().map(|i| i.units), Some(1));
            assert_eq!(drain.next().map(|i| i.units), Some(2));
            assert_eq!(drain.peek().map(|i| i.units), Some(3), "peeked, not yielded");
        }
        assert!(items.is_empty());
        assert_eq!(items.capacity(), capacity);
        let events = registry.drain(0);
        let shutdown = Some(("reason", "shutdown"));
        assert_eq!(metric_sum(&events, TEST_METRICS.items_dropped, shutdown), 3.0);
        assert_eq!(metric_sum(&events, TEST_METRICS.units_dropped, shutdown), 3.0 + 4.0 + 5.0);

        let mut all = batch_of(&[(1, 1), (1, 2)]);
        let yielded: Vec<u64> = CountedDrain::new(&mut all, &telemetry, &TEST_METRICS, "shutdown")
            .map(|i| i.units)
            .collect();
        assert_eq!(yielded, vec![1, 2]);
        assert!(
            !recorded_point(&registry.drain(0), TEST_METRICS.items_dropped),
            "an exhausted drain makes no telemetry call"
        );
    }

    #[tokio::test]
    async fn pop_many_is_fifo_across_push_and_push_many_interleavings_and_never_exceeds_max() {
        let q = test_queue(100, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 1 }).await;
        let mut items = batch_of(&[(1, 2), (1, 3)]);
        q.push_many(&mut items).await;
        q.push(TestItem { weight: 1, units: 4 }).await;
        let mut more = batch_of(&[(1, 5)]);
        q.push_many(&mut more).await;

        let mut out = Vec::new();
        assert_eq!(q.pop_many(&mut out, 2).await, 2, "never more than max");
        assert_eq!(q.pop_many(&mut out, 2).await, 2);
        assert_eq!(q.pop_many(&mut out, 2).await, 1, "and never more than what is queued");
        assert_eq!(
            out.iter().map(|i| i.units).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5],
            "arrival order across both push shapes, appended to the caller's Vec in order"
        );
    }

    #[tokio::test]
    async fn pop_many_returns_zero_only_once_closed_and_empty() {
        let q = test_queue(10, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 1 }).await;
        q.close();

        let mut out = Vec::new();
        assert_eq!(q.pop_many(&mut out, 4).await, 1, "a closed queue still drains what it holds");
        assert_eq!(q.pop_many(&mut out, 4).await, 0, "closed and empty");
        assert_eq!(out.len(), 1, "the zero return leaves the caller's Vec untouched");
    }

    #[tokio::test(start_paused = true)]
    async fn pop_many_awaits_on_an_empty_open_queue_and_resolves_once_a_push_many_lands() {
        let q = Arc::new(test_queue(10, u64::MAX, OverflowPolicy::DropOldest));
        let q2 = Arc::clone(&q);
        let popping = tokio::spawn(async move {
            let mut out = Vec::new();
            let n = q2.pop_many(&mut out, 8).await;
            (n, out.iter().map(|i| i.units).collect::<Vec<_>>())
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!popping.is_finished(), "pop_many should still be waiting on an empty queue");

        // One permit for the whole batch must be enough to see all three items.
        let mut items = batch_of(&[(1, 7), (1, 8), (1, 9)]);
        q.push_many(&mut items).await;

        let (n, units) = tokio::time::timeout(Duration::from_secs(1), popping)
            .await
            .expect("pop_many should resolve once a batch lands")
            .expect("the spawned task should not panic");
        assert_eq!(n, 3);
        assert_eq!(units, vec![7, 8, 9]);
    }

    /// `a_pop_dropped_mid_wait_leaves_no_reservation_a_later_push_can_still_evict`, for `pop_many`.
    #[tokio::test(start_paused = true)]
    async fn a_pop_many_dropped_mid_wait_leaves_no_reservation_a_later_push_can_still_evict() {
        let q = Arc::new(test_queue(2, u64::MAX, OverflowPolicy::DropOldest));

        {
            let q2 = Arc::clone(&q);
            let mut popping = Box::pin(async move {
                let mut out = Vec::new();
                q2.pop_many(&mut out, 8).await
            });
            tokio::time::timeout(Duration::from_millis(1), &mut popping)
                .await
                .expect_err("nothing pushed yet -- pop_many must still be waiting");
        }

        let mut items = batch_of(&[(1, 1), (1, 2), (1, 3)]);
        q.push_many(&mut items).await; // must evict units=1

        assert_eq!(drain_units(&q), vec![2, 3], "units=1 should have been evicted");
    }

    /// Pins the tokio behavior [`BoundedQueue::close`] relies on: a `notify_waiters()` between
    /// constructing a `Notified` and its first poll still wakes it. Without it, a `close()`
    /// racing a waiter would hang shutdown intermittently.
    #[tokio::test]
    async fn a_notify_waiters_call_between_constructing_a_notified_and_polling_it_is_never_lost() {
        let notify = Notify::new();
        // Construct, broadcast, then poll: a `close()` racing a waiter's state check.
        let notified = notify.notified();
        notify.notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), notified).await.expect(
            "a notify_waiters() landing after a Notified is constructed but before it is first \
             polled must still wake it -- BoundedQueue::close's contract depends on this",
        );
    }

    /// Pins the tokio behavior every losing `select!` arm and `decode_loop`'s
    /// `timeout(wait, pop_many)` rely on: a `notify_one` delivered to a registered `Notified` that
    /// is then dropped without being polled again passes to the next waiter, or is stored as a
    /// permit when there is none. A `Notified` that has already returned `Ready` has consumed its
    /// permit, and dropping it passes nothing on.
    #[test]
    fn a_notify_one_delivered_to_a_registered_notified_that_is_dropped_unpolled_is_passed_on() {
        let notify = Notify::new();
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut first = std::pin::pin!(notify.notified());
            assert!(first.as_mut().poll(&mut cx).is_pending(), "registers as a waiter");
            notify.notify_one(); // delivered to `first`
        } // dropped without another poll
        let mut second = std::pin::pin!(notify.notified());
        assert!(
            second.as_mut().poll(&mut cx).is_ready(),
            "the permit `first` was handed must reach the next Notified, or a losing select! arm \
             swallows a wakeup"
        );
    }

    /// Pins that `notify_one` with no waiter stores one permit, not a count: two calls wake one
    /// later `Notified`, not two. The queues rely on repeated permits collapsing (see
    /// `BoundedQueue::push_many`'s notification paragraph).
    #[test]
    fn a_notify_one_with_no_waiter_stores_one_permit_not_two() {
        let notify = Notify::new();
        notify.notify_one();
        notify.notify_one();
        let mut cx = Context::from_waker(Waker::noop());
        let mut first = std::pin::pin!(notify.notified());
        assert!(first.as_mut().poll(&mut cx).is_ready(), "one permit is stored");
        let mut second = std::pin::pin!(notify.notified());
        assert!(second.as_mut().poll(&mut cx).is_pending(), "and only one");
    }

    // -- The lost wakeup: a consumer parked before a blocking `push_many`. --

    /// Which consumer a test parks: every method that waits on `not_empty` is covered.
    #[derive(Clone, Copy, Debug)]
    enum Consumer {
        PopMany,
        Pop,
        Peek,
    }

    /// Spawns a consumer that collects exactly `want` items' units, in the order it receives them,
    /// and stops early only if the queue closes.
    fn spawn_consumer(
        q: Arc<BoundedQueue<TestItem>>,
        kind: Consumer,
        want: usize,
    ) -> tokio::task::JoinHandle<Vec<u64>> {
        tokio::spawn(async move {
            let mut got = Vec::new();
            while got.len() < want {
                match kind {
                    Consumer::PopMany => {
                        let mut out = Vec::new();
                        if q.pop_many(&mut out, 8).await == 0 {
                            break;
                        }
                        got.extend(out.into_iter().map(|item| item.units));
                    }
                    Consumer::Pop => match q.pop().await {
                        Some(item) => got.push(item.units),
                        None => break,
                    },
                    Consumer::Peek => match q.peek().await {
                        Some(item) => {
                            got.push(item.units);
                            q.commit();
                        }
                        None => break,
                    },
                }
            }
            got
        })
    }

    /// A consumer parked on an empty queue before a `push_many` bigger than the whole queue is
    /// woken by the admitted prefix, under either bound and for every consumer kind.
    #[tokio::test(start_paused = true)]
    async fn a_consumer_parked_before_a_blocking_push_many_is_woken_by_the_prefix_it_admits() {
        // (max_items, max_weight, per-item weight): each bound leaves room for two of five items.
        for (max_items, max_weight, weight) in [(2usize, u64::MAX, 1u64), (1000, 2, 1)] {
            for kind in [Consumer::PopMany, Consumer::Pop, Consumer::Peek] {
                let q = Arc::new(test_queue(max_items, max_weight, OverflowPolicy::Block));
                let consumer = spawn_consumer(Arc::clone(&q), kind, 5);

                tokio::time::sleep(Duration::from_millis(10)).await;
                assert!(
                    !consumer.is_finished(),
                    "{kind:?}: the consumer should be parked on the empty queue before the push"
                );

                let mut items =
                    batch_of(&[(weight, 1), (weight, 2), (weight, 3), (weight, 4), (weight, 5)]);
                tokio::time::timeout(Duration::from_secs(5), q.push_many(&mut items))
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "{kind:?} (max_items {max_items}, max_weight {max_weight}): push_many \
                             deadlocked against a consumer that parked before it"
                        )
                    });
                assert!(items.is_empty());

                let got = tokio::time::timeout(Duration::from_secs(5), consumer)
                    .await
                    .unwrap_or_else(|_| panic!("{kind:?}: the consumer never woke"))
                    .expect("the consumer task should not panic");
                assert_eq!(
                    got,
                    vec![1, 2, 3, 4, 5],
                    "{kind:?}: every item should arrive, in order"
                );
            }
        }
    }

    /// A `push_many` dropped mid-wait has already announced its prefix to a parked consumer.
    #[tokio::test(start_paused = true)]
    async fn a_push_many_cancelled_mid_wait_still_wakes_a_consumer_parked_behind_its_prefix() {
        let q = Arc::new(test_queue(2, u64::MAX, OverflowPolicy::Block));
        // Takes two items and stops, so the push is parked again when it is cancelled.
        let consumer = spawn_consumer(Arc::clone(&q), Consumer::PopMany, 2);

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!consumer.is_finished(), "the consumer should be parked on the empty queue");

        let mut items = batch_of(&[(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6)]);
        {
            let mut pushing = Box::pin(q.push_many(&mut items));
            tokio::time::timeout(Duration::from_millis(1), &mut pushing)
                .await
                .expect_err("the batch is bigger than the queue and the consumer has stopped");
            // `pushing` is dropped here, mid-wait.
        }
        assert!(items.is_empty(), "the caller's Vec is emptied even by a cancellation");

        let got = tokio::time::timeout(Duration::from_secs(5), consumer)
            .await
            .expect("the cancelled push_many must still have woken the parked consumer")
            .expect("the consumer task should not panic");
        assert_eq!(got, vec![1, 2], "the consumer receives the prefix that was admitted for it");
        assert_eq!(drain_units(&q), vec![3, 4], "and the rest of the prefix stays queued");
    }

    /// Under `Block` with a running consumer, batched and single pushes deliver the same stream,
    /// whichever side parks first. The exhaustive test below can't cover `Block`, which needs a
    /// concurrent consumer to terminate.
    #[tokio::test(start_paused = true)]
    async fn under_block_with_a_running_consumer_batched_and_single_pushes_agree() {
        async fn drive(
            max_items: usize,
            max_weight: u64,
            weight: u64,
            batches: &[usize],
            consumer_first: bool,
            batched: bool,
        ) -> Vec<u64> {
            let q = Arc::new(test_queue(max_items, max_weight, OverflowPolicy::Block));
            let total: usize = batches.iter().sum();
            let spawn_producer = |q: Arc<BoundedQueue<TestItem>>, batches: Vec<usize>| {
                tokio::spawn(async move {
                    let mut next = 1u64;
                    for n in batches {
                        let mut items: Vec<TestItem> = (0..n)
                            .map(|_| {
                                let item = TestItem { weight, units: next };
                                next += 1;
                                item
                            })
                            .collect();
                        if batched {
                            q.push_many(&mut items).await;
                        } else {
                            for item in items.drain(..) {
                                q.push(item).await;
                            }
                        }
                        assert!(items.is_empty());
                    }
                })
            };

            // Let whichever side starts first park before the other appears.
            let (consumer, producer) = if consumer_first {
                let consumer = spawn_consumer(Arc::clone(&q), Consumer::PopMany, total);
                tokio::time::sleep(Duration::from_millis(10)).await;
                let producer = spawn_producer(Arc::clone(&q), batches.to_vec());
                (consumer, producer)
            } else {
                let producer = spawn_producer(Arc::clone(&q), batches.to_vec());
                tokio::time::sleep(Duration::from_millis(10)).await;
                let consumer = spawn_consumer(Arc::clone(&q), Consumer::PopMany, total);
                (consumer, producer)
            };

            tokio::time::timeout(Duration::from_secs(5), producer)
                .await
                .expect("the producer must not deadlock against the consumer")
                .expect("the producer task should not panic");
            tokio::time::timeout(Duration::from_secs(5), consumer)
                .await
                .expect("the consumer must not be left parked")
                .expect("the consumer task should not panic")
        }

        for (max_items, max_weight, weight) in [
            (1usize, u64::MAX, 1u64),
            (2, u64::MAX, 1),
            (3, u64::MAX, 1),
            (1000, 2, 1),
            (1000, 6, 2),
        ] {
            for batches in [&[5usize][..], &[2, 3][..], &[3, 1, 4][..], &[4, 4][..]] {
                for consumer_first in [true, false] {
                    let batched =
                        drive(max_items, max_weight, weight, batches, consumer_first, true).await;
                    let single =
                        drive(max_items, max_weight, weight, batches, consumer_first, false).await;
                    let expected: Vec<u64> = (1..=batches.iter().sum::<usize>() as u64).collect();
                    assert_eq!(
                        batched, expected,
                        "batched: {batches:?} at (max_items {max_items}, max_weight {max_weight}, \
                         weight {weight}), consumer_first={consumer_first}"
                    );
                    assert_eq!(
                        batched, single,
                        "batched and single-call runs must deliver the same stream: {batches:?} at \
                         (max_items {max_items}, max_weight {max_weight}, weight {weight}), \
                         consumer_first={consumer_first}"
                    );
                }
            }
        }
    }

    /// One gauge update per `push_many`/`pop_many` call (counted by the test-only counter, since a
    /// drained `Registry` can't show it), carrying the end state, and none mid-call.
    #[tokio::test(start_paused = true)]
    async fn push_many_and_pop_many_each_update_the_gauges_exactly_once_per_call() {
        let (registry, q) = recording_queue(100, u64::MAX, OverflowPolicy::DropOldest);
        let q = Arc::new(q);

        let mut items = batch_of(&[(3, 1), (3, 2), (3, 3), (3, 4), (3, 5)]);
        q.push_many(&mut items).await;
        assert_eq!(q.gauge_updates(), 1, "one update for a five-item push_many");

        let mut out = Vec::new();
        assert_eq!(q.pop_many(&mut out, 4).await, 4);
        assert_eq!(q.gauge_updates(), 2, "one more for the pop_many, not one per item");

        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, TEST_METRICS.depth, None),
            1.0,
            "the one update carries the end state (5 pushed, 4 popped), not an intermediate one"
        );
        assert_eq!(metric_sum(&events, TEST_METRICS.bytes, None), 3.0);

        // A `Block` push_many parked mid-batch has updated nothing yet.
        let (registry2, q2) = recording_queue(1, u64::MAX, OverflowPolicy::Block);
        let q2 = Arc::new(q2);
        let q3 = Arc::clone(&q2);
        let pushing = tokio::spawn(async move {
            let mut items = batch_of(&[(1, 1), (1, 2)]);
            q3.push_many(&mut items).await;
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!pushing.is_finished());
        assert_eq!(q2.gauge_updates(), 0, "nothing gauged while the batch is still in flight");
        assert!(!recorded_point(&registry2.drain(0), TEST_METRICS.depth));
        pushing.abort();
    }

    /// `push_blocked` records one sample per call, however many items waited.
    #[tokio::test(start_paused = true)]
    async fn a_push_many_that_waits_records_exactly_one_push_blocked_sample() {
        let (registry, q) = recording_queue(1, u64::MAX, OverflowPolicy::Block);
        let q = Arc::new(q);
        let q2 = Arc::clone(&q);
        let pushing = tokio::spawn(async move {
            let mut items = batch_of(&[(1, 1), (1, 2), (1, 3)]);
            q2.push_many(&mut items).await;
        });

        // Two separate waits: one for item 2, one for item 3.
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_millis(10), q.pop_many(&mut out, 1)).await;
        }
        tokio::time::timeout(Duration::from_secs(1), pushing)
            .await
            .expect("push_many should finish once room keeps being freed")
            .expect("the spawned task should not panic");

        let events = registry.drain(0);
        let samples: usize = events
            .iter()
            .flat_map(|e| e.metrics.iter())
            .filter(|m| m.name == logit_core::interner::intern(TEST_METRICS.push_blocked))
            .map(|m| match &m.kind {
                logit_core::MetricKind::Distribution(sketch) => sketch.count(),
                _ => 0,
            })
            .sum();
        assert_eq!(samples, 1, "one sample spanning the whole call, not one per waiting item");
    }

    /// `max: 0` trips a `debug_assert!` in debug builds.
    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "pop_many(max = 0)")]
    async fn pop_many_with_max_zero_trips_a_debug_assert() {
        let q = test_queue(10, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 1 }).await;
        let mut out = Vec::new();
        let _ = q.pop_many(&mut out, 0).await;
    }

    #[cfg(not(debug_assertions))]
    #[tokio::test]
    async fn pop_many_with_max_zero_is_clamped_to_one_rather_than_hanging() {
        let q = test_queue(10, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 1 }).await;
        q.push(TestItem { weight: 1, units: 2 }).await;
        let mut out = Vec::new();
        assert_eq!(q.pop_many(&mut out, 0).await, 1);
    }

    /// Every four-operation sequence of `push`/`push_many`/`pop`/`pop_many` leaves the same queue
    /// contents and drop counts as the same sequence expanded into single-item calls, across
    /// three bound shapes under each dropping policy. Exhaustive, so no `proptest`.
    ///
    /// Both queues are closed first, so a pop against an empty queue returns instead of parking;
    /// `closed` is consulted only on `Block`'s waiting path, which neither policy takes.
    #[tokio::test]
    async fn any_interleaving_of_batched_and_single_calls_agrees_with_the_single_call_sequence() {
        #[derive(Clone, Copy, Debug)]
        enum Op {
            Push,
            PushMany2,
            PushMany3,
            Pop,
            PopMany2,
        }
        const OPS: [Op; 5] = [Op::Push, Op::PushMany2, Op::PushMany3, Op::Pop, Op::PopMany2];

        /// Runs one sequence, returning `(popped units, remaining units)` in order. `batched`
        /// picks `push_many`/`pop_many` or their single-item expansion.
        async fn run(
            q: &BoundedQueue<TestItem>,
            seq: &[Op],
            weight: u64,
            batched: bool,
        ) -> (Vec<u64>, Vec<u64>) {
            q.close();
            let mut next_unit = 1u64;
            let mut popped = Vec::new();
            for op in seq {
                let count = match op {
                    Op::Push => 1,
                    Op::PushMany2 | Op::PopMany2 => 2,
                    Op::PushMany3 => 3,
                    Op::Pop => 1,
                };
                match op {
                    Op::Push | Op::PushMany2 | Op::PushMany3 => {
                        let mut items: Vec<TestItem> = (0..count)
                            .map(|_| {
                                let item = TestItem { weight, units: next_unit };
                                next_unit += 1;
                                item
                            })
                            .collect();
                        if batched && count > 1 {
                            q.push_many(&mut items).await;
                        } else {
                            for item in items.drain(..) {
                                q.push(item).await;
                            }
                        }
                    }
                    Op::Pop | Op::PopMany2 => {
                        if batched && count > 1 {
                            let mut out = Vec::new();
                            q.pop_many(&mut out, count).await;
                            popped.extend(out.into_iter().map(|i| i.units));
                        } else {
                            for _ in 0..count {
                                match q.pop().await {
                                    Some(item) => popped.push(item.units),
                                    None => break,
                                }
                            }
                        }
                    }
                }
            }
            (popped, drain_units(q))
        }

        for (max_items, max_weight, weight) in [(2usize, u64::MAX, 1u64), (100, 4, 1), (3, 6, 2)] {
            for overflow in [OverflowPolicy::DropOldest, OverflowPolicy::DropNewest] {
                for a in OPS {
                    for b in OPS {
                        for c in OPS {
                            for d in OPS {
                                let seq = [a, b, c, d];
                                let (r1, q1) = recording_queue(max_items, max_weight, overflow);
                                let batched = run(&q1, &seq, weight, true).await;
                                let (r2, q2) = recording_queue(max_items, max_weight, overflow);
                                let single = run(&q2, &seq, weight, false).await;
                                assert_eq!(
                                    batched, single,
                                    "{seq:?} under {overflow:?} (max_items {max_items}, \
                                     max_weight {max_weight}, weight {weight}) must pop and \
                                     retain exactly what the single-call sequence does"
                                );
                                let (e1, e2) = (r1.drain(0), r2.drain(0));
                                for reason in ["overflow_oldest", "overflow_newest"] {
                                    let tag = Some(("reason", reason));
                                    assert_eq!(
                                        metric_sum(&e1, TEST_METRICS.items_dropped, tag),
                                        metric_sum(&e2, TEST_METRICS.items_dropped, tag),
                                        "{seq:?} under {overflow:?}: {reason} item drops must match"
                                    );
                                    assert_eq!(
                                        metric_sum(&e1, TEST_METRICS.units_dropped, tag),
                                        metric_sum(&e2, TEST_METRICS.units_dropped, tag),
                                        "{seq:?} under {overflow:?}: {reason} unit drops must match"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// `any_sequence_with_close_and_cancelled_calls_agrees_between_batched_and_single_calls`: the
    /// exhaustive test above, extended with `Block`, `close()`, and cancellation. Every call is
    /// polled once with a no-op waker and dropped if `Pending`, so where a call is cut off is
    /// exact, or dropped before its first poll.
    mod batched_vs_single {
        use super::*;
        use proptest::prelude::*;

        #[derive(Clone, Copy, Debug)]
        enum Polling {
            /// Polled once, then dropped if `Pending`.
            Once,
            /// Built and dropped without a poll.
            Never,
        }

        #[derive(Clone, Debug)]
        enum Op {
            Push(u64, Polling),
            PushMany(Vec<u64>, Polling),
            Pop(Polling),
            PopMany(usize, Polling),
            Close,
        }

        fn poll_once<F: Future>(fut: F) -> Poll<F::Output> {
            let mut fut = std::pin::pin!(fut);
            fut.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        }

        /// What one run observed, every item named by its unique `units`.
        #[derive(Debug, PartialEq, Default)]
        struct Run {
            popped: Vec<u64>,
            remaining: Vec<u64>,
            /// Items a never-polled `push_many` left in the caller's `Vec`.
            held_by_caller: Vec<u64>,
            /// Single `push` calls that were never admitted.
            cancelled_pushes: Vec<u64>,
        }

        /// Runs `ops`. `batched` picks `push_many`/`pop_many` or their single-item expansion. The
        /// expansion of a `push_many` stops at the first `push` left `Pending`, and returns what it
        /// never reached as `(items, units)`: the remainder a cancelled `push_many` counts itself.
        fn run(q: &BoundedQueue<TestItem>, ops: &[Op], batched: bool) -> (Run, (f64, f64)) {
            let mut next_unit = 1u64;
            let mut item = |weight: u64| {
                let item = TestItem { weight, units: next_unit };
                next_unit += 1;
                item
            };
            let mut out = Run::default();
            let mut unreached = (0.0, 0.0);
            for op in ops {
                match op {
                    Op::Push(weight, polling) => {
                        let it = item(*weight);
                        let units = it.units;
                        let admitted = match polling {
                            Polling::Once => poll_once(q.push(it)).is_ready(),
                            Polling::Never => {
                                drop(q.push(it));
                                false
                            }
                        };
                        if !admitted {
                            out.cancelled_pushes.push(units);
                        }
                    }
                    Op::PushMany(weights, polling) => {
                        let mut items: Vec<TestItem> = weights.iter().map(|&w| item(w)).collect();
                        match polling {
                            Polling::Never => {
                                drop(q.push_many(&mut items));
                                out.held_by_caller.extend(items.iter().map(|i| i.units));
                            }
                            Polling::Once if batched => {
                                let _ = poll_once(q.push_many(&mut items));
                                assert!(items.is_empty(), "a polled push_many drains its Vec");
                            }
                            Polling::Once => {
                                let mut rest = items.into_iter();
                                for it in rest.by_ref() {
                                    let units = it.units;
                                    if poll_once(q.push(it)).is_pending() {
                                        unreached.0 += 1.0;
                                        unreached.1 += units as f64;
                                        break;
                                    }
                                }
                                for it in rest {
                                    unreached.0 += 1.0;
                                    unreached.1 += it.units as f64;
                                }
                            }
                        }
                    }
                    Op::Pop(Polling::Once) => {
                        if let Poll::Ready(Some(it)) = poll_once(q.pop()) {
                            out.popped.push(it.units);
                        }
                    }
                    Op::Pop(Polling::Never) | Op::PopMany(_, Polling::Never) => {}
                    Op::PopMany(max, Polling::Once) if batched => {
                        let mut got = Vec::new();
                        match poll_once(q.pop_many(&mut got, *max)) {
                            Poll::Ready(n) => assert_eq!(n, got.len()),
                            Poll::Pending => assert!(got.is_empty(), "Pending removes nothing"),
                        }
                        out.popped.extend(got.into_iter().map(|i| i.units));
                    }
                    Op::PopMany(max, Polling::Once) => {
                        for _ in 0..*max {
                            match poll_once(q.pop()) {
                                Poll::Ready(Some(it)) => out.popped.push(it.units),
                                _ => break,
                            }
                        }
                    }
                    Op::Close => q.close(),
                }
            }
            out.remaining = drain_units(q);
            (out, unreached)
        }

        fn polling() -> impl Strategy<Value = Polling> {
            prop_oneof![4 => Just(Polling::Once), 1 => Just(Polling::Never)]
        }

        fn op() -> impl Strategy<Value = Op> {
            prop_oneof![
                4 => (0u64..=7, polling()).prop_map(|(w, p)| Op::Push(w, p)),
                4 => (prop::collection::vec(0u64..=7, 2..=5), polling())
                    .prop_map(|(ws, p)| Op::PushMany(ws, p)),
                3 => polling().prop_map(Op::Pop),
                3 => (2usize..=4, polling()).prop_map(|(k, p)| Op::PopMany(k, p)),
                1 => Just(Op::Close),
            ]
        }

        const POLICIES: [OverflowPolicy; 3] =
            [OverflowPolicy::Block, OverflowPolicy::DropOldest, OverflowPolicy::DropNewest];
        /// `(max_items, max_weight)`. Weights run 0..=7, so the last two shapes also see items
        /// that can never fit.
        const SHAPES: [(usize, u64); 3] = [(2, u64::MAX), (100, 4), (3, 6)];

        proptest! {
            #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

            #[test]
            fn any_sequence_with_close_and_cancelled_calls_agrees_between_batched_and_single_calls(
                ops in prop::collection::vec(op(), 1..=24),
                policy in 0usize..3,
                shape in 0usize..3,
            ) {
                let overflow = POLICIES[policy];
                let (max_items, max_weight) = SHAPES[shape];
                let (r1, q1) = recording_queue(max_items, max_weight, overflow);
                let (batched, batched_unreached) = run(&q1, &ops, true);
                let (r2, q2) = recording_queue(max_items, max_weight, overflow);
                let (single, single_unreached) = run(&q2, &ops, false);

                prop_assert_eq!(&batched, &single, "{:?} under {:?}", ops, overflow);
                prop_assert_eq!(batched_unreached, (0.0, 0.0));
                let (e1, e2) = (r1.drain(0), r2.drain(0));
                for reason in ["overflow_oldest", "overflow_newest"] {
                    let tag = Some(("reason", reason));
                    prop_assert_eq!(
                        metric_sum(&e1, TEST_METRICS.items_dropped, tag),
                        metric_sum(&e2, TEST_METRICS.items_dropped, tag),
                        "{} item drops", reason
                    );
                    prop_assert_eq!(
                        metric_sum(&e1, TEST_METRICS.units_dropped, tag),
                        metric_sum(&e2, TEST_METRICS.units_dropped, tag),
                        "{} unit drops", reason
                    );
                }
                let shutdown = Some(("reason", "shutdown"));
                prop_assert_eq!(
                    (
                        metric_sum(&e1, TEST_METRICS.items_dropped, shutdown),
                        metric_sum(&e1, TEST_METRICS.units_dropped, shutdown),
                    ),
                    single_unreached,
                    "a cancelled push_many counts what the single calls never reached"
                );
                prop_assert_eq!(metric_sum(&e2, TEST_METRICS.items_dropped, shutdown), 0.0);
            }
        }
    }
}
