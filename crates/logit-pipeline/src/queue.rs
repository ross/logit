//! [`BoundedQueue`]: the async wrapper around `logit_proto::buffer::Buffer` that decouples a
//! node's own I/O from whatever's downstream of it -- see
//! `docs/adr/buffered-sink-delivery.md` (the sink side, `SinkQueue`) and
//! `docs/adr/decoupled-listener-io.md` (the listener side, `ReceiveQueue`). `Buffer` itself
//! is sync (no `.await` in a critical section), so this type owns the one thing a sync trait
//! can't express: `Block`, which awaits room rather than dropping.
//!
//! Generalized from a `SinkQueue` that hardcoded `Arc<EventBatch>` -- the [`Queued`] trait and
//! [`QueueMetrics`] are what let one implementation serve both a sink's delivery queue and a UDP
//! listener's receive queue with no behavior change on the sink side: `SinkQueue` is now a type
//! alias (over `(Arc<EventBatch>, BatchContext)`, not bare `Arc<EventBatch>` -- see that alias's
//! own doc comment for why), and every sink-side metric name, default, and test is unchanged.

use crate::fanout::BatchContext;
use logit_core::{Diagnostics, EventBatch, Telemetry};
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

/// [`SinkQueue`]'s actual item type: a batch alongside the [`BatchContext`] it arrived with.
/// `BatchContext` is `Copy`, 32 bytes, so this rides inline in the existing `(item, weight)` slot
/// `InMemoryBuffer` already stores -- no new allocation, and weight/units are unaffected, since
/// both are computed from the batch alone. See
/// `docs/adr/internal-span-emission-and-deterministic-sampling.md` for why the trace half of this
/// exists (`write_loop`'s sink span needs the context that arrived with this batch, and
/// `drain_inbox`/`peek` were the last place it was still being discarded) and
/// `docs/adr/batch-provenance-on-delivered.md` for the provenance half (`logit_out`'s
/// `Output::observe_batch` reads it from here too).
impl Queued for (Arc<EventBatch>, BatchContext) {
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
/// one instance today; `logit-inputs`' receive queue (`docs/adr/decoupled-listener-io.md`)
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
/// differs (`docs/adr/decoupled-listener-io.md`'s core argument for why a UDP listener's
/// default must not be `Block`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    Block,
    DropOldest,
    DropNewest,
}

/// Bounds and overflow behavior for one [`BoundedQueue`], in the queue's own generic terms
/// (items/weight rather than a domain-specific unit). [`SinkQueueConfig`]/`ReceiveQueueConfig`
/// (`logit-inputs`, `docs/adr/decoupled-listener-io.md`) each convert into this rather than
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
/// inbox drain and its writer (`docs/adr/buffered-sink-delivery.md`, `SinkQueue`), or a UDP
/// listener's socket read and its decode loop (`docs/adr/decoupled-listener-io.md`,
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
    /// How many times `update_gauges` has run on this queue -- the one property
    /// [`BoundedQueue::push_many`]/[`BoundedQueue::pop_many`] exist for that a drained `Registry`
    /// cannot show. `Telemetry::gauge` is last-write-wins per `(name, tags)` in
    /// `ComponentBuffer`'s pending map (`crates/logit-core/src/telemetry.rs`), so one gauge write
    /// and sixty-four gauge writes of the same series drain as the same single point -- which is
    /// precisely why batching them is safe, and precisely why "exactly one per call" is
    /// unobservable from the outside. A test-only counter is the honest way to pin it; production
    /// builds carry neither the field nor the increment.
    #[cfg(test)]
    gauge_updates: std::sync::atomic::AtomicUsize,
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
            #[cfg(test)]
            gauge_updates: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// How many `update_gauges` calls this queue has made -- see the field's own doc comment for
    /// why this is test-only state rather than something a test reads back off a `Registry`.
    #[cfg(test)]
    fn gauge_updates(&self) -> usize {
        self.gauge_updates.load(Ordering::Relaxed)
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

    /// [`BoundedQueue::push`] over a whole batch: drains `items` into the queue, applying exactly
    /// the same admission control to each item that `push` applies to its one -- same per-item
    /// [`Queued::weight`], same "impossible to ever fit" fallback, same `Block` waiting, same
    /// `DropOldest` eviction (never the reserved head) and `DropNewest` rejection, same
    /// `count_dropped` call per evicted/rejected item with that item's own [`Queued::units`]. What
    /// is *not* per-item is the bookkeeping around it, which is the entire point
    /// (`docs/adr/udp-intake-batching-and-socket-visibility.md`, "`push_many`/`pop_many` live on
    /// `BoundedQueue` itself"): **one `update_gauges` call covers the whole invocation** rather
    /// than one per item, so a receive queue's two hottest loops stop contending on
    /// `ComponentBuffer`'s mutex once per datagram. One lock acquisition covers each contiguous run
    /// of items that currently fit, so an uncontended batch takes exactly one.
    ///
    /// `items` is **always left empty** (its capacity intact, so a caller reusing one `Vec` across
    /// calls pays no allocation) -- on the ordinary path because the `Drain` ran to the end, and on
    /// the cancellation path below because dropping a `Drain` removes whatever it had not yet
    /// yielded. An empty `items` is a complete no-op: no lock, no notification, no gauge update.
    ///
    /// **At most one `push_blocked` sample per call**, timing the whole span from the first wait to
    /// the last, however many of the batch's items had to wait individually -- the metric answers
    /// "how long did the producer stall", and a call that stalled once for 5 ms should not be
    /// indistinguishable from one that stalled 64 times for 78 µs each.
    ///
    /// **One `not_empty.notify_one()` per call, deliberately not `notify_waiters()`.**
    /// `notify_one` stores at most one permit, so N calls before a waiter next polls are
    /// indistinguishable from one call (the ADR records this so nobody credits the batched call
    /// with a wakeup win it does not have) -- but the choice between the two primitives is real.
    /// There is exactly one consumer per queue today (`decode_loop` on the receive side,
    /// `write_loop`/`drain_inbox` on the sink side), so one permit is one wakeup for the one task
    /// that wants it. Even with several consumers it stays correct: the woken one re-checks state
    /// under the lock before it ever parks again (`pop`/`pop_many`'s loop), so it keeps draining
    /// while items remain, and a consumer cannot be left parked next to a non-empty queue.
    /// `notify_waiters()` would wake *all* current waiters, but stores **no** permit -- a consumer
    /// that had already decided the queue was empty and had not yet registered its `Notified` would
    /// miss the wakeup entirely and park against a queue that just got items. That is the race the
    /// permit exists to close, so `notify_one` is the primitive, here and in `push`.
    ///
    /// **Cancellation: the remainder is dropped uncounted.** The `Peekable<Drain>` is held across
    /// every `.await` in this method, so a caller whose future is dropped mid-call (`read_loop`'s
    /// shutdown race, the same shape `crates/logit-inputs/src/udp.rs`'s single-datagram
    /// cancellation already has) leaves: everything already accepted still in the queue and fully
    /// accounted for, the not-yet-reached remainder dropped along with the `Drain` **without being
    /// counted as a drop**, and the caller's `Vec` empty. This widens the "one datagram in flight at
    /// shutdown, uncounted" loss ADR `decoupled-listener-io` already accepted to at most one batch's
    /// worth, still shutdown-path-only -- ordinary operation never cancels a `push_many`. It cannot
    /// be counted: counting a drop needs `Telemetry`, and the only code that runs on cancellation is
    /// `Drop`, so the count would have to be emitted from a `Drop` impl holding a handle to this
    /// queue -- lock-acquisition-from-`Drop` machinery, with its own poisoning and ordering hazards,
    /// to move an already-bounded, shutdown-only loss from "uncounted" to "counted".
    pub async fn push_many(&self, items: &mut Vec<T>) {
        if items.is_empty() {
            return;
        }
        let mut blocked_timer: Option<logit_core::telemetry::Timer> = None;
        // Reused across every iteration of the wait loop: whatever the underlying buffer evicted or
        // rejected under one lock acquisition, tagged with the reason to count it under, drained
        // and counted immediately after that lock is released. Counting them at the *end* of the
        // call instead would be simpler and wrong twice over -- the drop counts would lag behind an
        // arbitrarily long `Block` wait, and the evicted items themselves (whole datagrams) would
        // be held alive for the duration rather than freed as soon as they left the queue.
        let mut dropped: Vec<(&'static str, T)> = Vec::new();
        let (len, total_weight) = {
            let mut drain = items.drain(..).peekable();
            // The head item's weight, computed exactly once per item however many times the wait
            // loop re-examines it -- `push` computes `weight` once per call for the same reason.
            let mut head_weight: Option<u64> = None;
            loop {
                // Registered before the state check below, exactly as `push` does it -- see that
                // method's doc comment on why that ordering is what makes this race-free.
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
                if blocked_timer.is_none() {
                    blocked_timer = Some(self.telemetry.timer(self.metrics.push_blocked));
                }
                notified.await;
            }
        };

        // Only records a sample if this call actually waited at least once above.
        drop(blocked_timer);
        self.not_empty.notify_one();
        self.update_gauges(len, total_weight);
    }

    /// Removes and returns the head (a no-op returning `None` on an empty queue), notifies any
    /// blocked `push` that room may now be available, and refreshes the depth/utilization
    /// gauges. For [`SinkQueue`], this returns the `BatchContext` the head was pushed with
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

    /// [`BoundedQueue::pop`] over a whole batch, and [`BoundedQueue::push_many`]'s mirror: awaits
    /// at least one item, then removes up to `max` of them under **one** lock acquisition, appends
    /// them to `out` in FIFO order, and returns how many it appended. `0` means exactly what
    /// `pop`'s `None` means -- closed *and* empty, nothing will ever arrive again -- and is the only
    /// value that means it: a return of `0` never happens on a live queue, because this call waits.
    /// `out` is appended to, never cleared, and a caller reusing one `Vec` across calls keeps its
    /// capacity; `out` is left untouched on the `0` return.
    ///
    /// One `update_gauges` call per invocation, on the path that actually removed something -- the
    /// whole reason this method exists, see [`BoundedQueue::push_many`].
    ///
    /// **`max == 0` is a caller bug.** It would otherwise mean "wait for an item, then remove none
    /// of it", i.e. park forever against a non-empty queue. A `debug_assert!` catches it in
    /// development; in release it's clamped to 1, because degrading a wrong constant into a slow
    /// consumer is better than hanging a listener's decode loop outright.
    ///
    /// **Cancellation-safe, by the same argument as [`BoundedQueue::pop`]**: there is no `.await`
    /// between taking the lock and removing items, and `Buffer::commit` clears any head
    /// reservation, so a future dropped mid-call can never leave a reservation standing (the thing
    /// `a_pop_dropped_mid_wait_leaves_no_reservation_a_later_push_can_still_evict` pins) and can
    /// never lose an item into a half-filled `out`: this call only ever awaits on an iteration that
    /// removed nothing at all, and returns immediately on any iteration that removed something.
    pub async fn pop_many(&self, out: &mut Vec<T>, max: usize) -> usize {
        debug_assert!(max > 0, "pop_many(max = 0) would wait for an item and then remove none");
        let max = max.max(1);
        loop {
            let notified = self.not_empty.notified();
            let (popped, len, weight) = {
                let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                let mut popped = 0usize;
                while popped < max {
                    // `commit` (remove) directly rather than `peek` (reserve) then `commit`: a
                    // reservation only exists to protect a head some *other* caller is acting on
                    // between two calls, and there is no such gap here -- everything below happens
                    // under this one lock acquisition with no suspend point in it, which is what
                    // makes this method cancellation-safe exactly the way `pop` is. `commit` also
                    // clears any reservation it finds, so this cannot inherit a stale one either.
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
                // One `notify_one` per unit of room freed, not one per call: `notify_one` wakes one
                // waiter (or stores one permit if there is none), so a single call would wake a
                // single blocked pusher even when this pop freed room for many. That is exactly
                // what `popped` sequential `pop`s would have done, and it matters as soon as more
                // than one pusher can exist on a queue -- nothing in this type restricts that, and
                // a permit-storing `notify_one` is what closes the race a `notify_waiters()` sweep
                // would leave open for a pusher that has decided it is full but not yet registered
                // its `Notified` (see `push_many`'s doc comment). When there are 0 or 1 waiters the
                // extra calls collapse into the same one permit, so this costs a handful of
                // uncontended atomics in the common case.
                for _ in 0..popped {
                    self.not_full.notify_one();
                }
                self.update_gauges(len, weight);
                return popped;
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
    /// The head, without removing it -- a clone the caller can act on and only remove (via
    /// [`BoundedQueue::commit`]) once that action succeeds. Requires `T: Clone` (a cheap
    /// refcount bump for `Arc<EventBatch>`, or for [`SinkQueue`]'s `(Arc<EventBatch>,
    /// BatchContext)`, a refcount bump plus a `Copy`); a consumer with no such retry contract
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

/// A sink's delivery queue: `Arc<EventBatch>` paired with the [`BatchContext`] it arrived with,
/// not bare `Arc<EventBatch>` -- `write_loop`'s sink span needs the trace context that produced
/// each batch (`docs/adr/internal-span-emission-and-deterministic-sampling.md`), and `logit_out`'s
/// `Output::observe_batch` needs the provenance that arrived with it
/// (`docs/adr/batch-provenance-on-delivered.md`); `peek`/`commit` are the only place either can
/// still be read back.
pub type SinkQueue = BoundedQueue<(Arc<EventBatch>, BatchContext)>;

impl SinkQueue {
    pub fn new(config: SinkQueueConfig, telemetry: Telemetry) -> Self {
        Self::with_metrics(config.into(), &SINK_QUEUE_METRICS, telemetry)
    }
}

/// What a sink's queue actually is, chosen per component by `logit-cli::pipeline::build_spec`
/// from that component's `buffer:` block -- `Memory` (today's `SinkQueue`, unchanged) or `Disk`
/// (`crate::disk_queue::DiskQueue`, `docs/adr/disk-backed-sink-buffer.md`). Enum dispatch, not
/// `dyn Trait`: there are exactly two implementations, both known at compile time, and
/// `disk_queue::DiskQueue` deliberately does *not* implement `logit_proto::buffer::Buffer<T>` --
/// that trait is sync/`&mut self`/generic, the wrong seam for a concrete, async, file-backed
/// queue (see `disk_queue`'s own module doc).
pub enum SinkStore {
    Memory(SinkQueue),
    // `Box`ed: `DiskQueue` is far larger than `SinkQueue` (it owns a `PathBuf`, open file
    // handles, and a lock file), and `clippy::large_enum_variant` is right that leaving it
    // unboxed would pad every `SinkStore::Memory` (the common case) out to `DiskQueue`'s size.
    Disk(Box<crate::disk_queue::DiskQueue>),
}

/// Mirrors [`SinkStore`] one level up, at the config stage -- `logit-cli::pipeline::queue_config`
/// builds one of these from a component's `logit_config::BufferConfig`, and `run_output`
/// (`crate::runtime`) turns it into the live [`SinkStore`] via [`SinkStore::open`].
pub enum SinkStoreConfig {
    Memory(SinkQueueConfig),
    Disk(crate::disk_queue::DiskQueueConfig),
}

impl SinkStore {
    /// Builds the store a `SinkStoreConfig` describes. Infallible for `Memory` (`SinkQueue::new`
    /// never fails); `Disk` opens (or recovers) the spool directory, which can fail -- a bad path,
    /// a permissions error, another process already holding the lock -- and that failure is a
    /// startup error for the component, not something `run_output` degrades from.
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

    /// Advances past the head, returning it -- mirrors `SinkQueue::commit`/`DiskQueue::commit`
    /// exactly. `write_loop`/`drain_inbox` never need the value back, but a test driving
    /// `write_loop` directly does (to confirm what it left behind on a shutdown-grace exit,
    /// say), so this doesn't discard it the way `SinkStore::finish`'s internal drain loop does.
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
    /// can push into this store any more. Returns `(dropped_batches, dropped_events)`.
    ///
    /// `Memory` drains whatever the queue still holds by repeatedly committing (exactly
    /// `finish_and_flush`'s old inline loop) -- an in-memory queue's contents don't survive
    /// process exit regardless, so this is the last chance to count them as dropped.
    ///
    /// `Disk` drops **nothing**: it persists the read cursor, `fsync`s it, the active segment,
    /// and the directory, and returns `(0, 0)` unconditionally -- whatever's still queued survives
    /// this process exit and delivers on the next `DiskQueue::open`. The shutdown grace only
    /// bounds how long `write_loop` keeps attempting delivery, never what a disk-backed sink is
    /// still holding.
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

    /// Every test in this module pushes under a placeholder context -- none of them exercise
    /// `BatchContext` propagation itself (`fanout.rs`/`runtime.rs`'s tests do that); this queue
    /// only needs to carry whatever it was given back out again unchanged, which
    /// `push_then_peek_then_commit_round_trips_one_batch` below proves directly with a real,
    /// non-default one.
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

    // -- `push_many` / `pop_many`: one lock and one gauge update per batch, per-item admission. --
    //
    // (`docs/adr/udp-intake-batching-and-socket-visibility.md`, "`push_many`/`pop_many` live on
    // `BoundedQueue` itself". Every test below mirrors an existing single-item test above; where a
    // property has a single-item twin, the assertion is deliberately worded the same way.)

    /// A queue whose telemetry actually records, so a test can read back the drop counts
    /// `count_dropped` emitted -- the `TestItem` tests above use `Telemetry::default()`, which is
    /// the no-op sink, and infer drop accounting from what survived in the queue instead.
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

    /// Sums every recorded point named `name` (optionally only those carrying `tag`) out of a
    /// drained `Registry` -- the same shape `disk_queue`'s tests use to read their own counters.
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

    /// `under_drop_oldest_pushing_into_a_full_queue_evicts_and_is_reflected_in_commit_order`'s
    /// batched twin, plus the accounting that test could not make: each evicted item is counted
    /// individually, and with its *own* `units()`.
    #[tokio::test]
    async fn push_many_under_drop_oldest_evicts_and_counts_each_evicted_item_with_its_own_units() {
        let (registry, q) = recording_queue(2, u64::MAX, OverflowPolicy::DropOldest);
        q.push(TestItem { weight: 1, units: 40 }).await;
        q.push(TestItem { weight: 1, units: 7 }).await;

        // Three more into a queue of two: evicts units=40, then units=7, then the first of this
        // very batch (units=100) -- exactly what three sequential pushes would have evicted.
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

    /// `under_drop_newest_pushing_into_a_full_queue_is_a_no_op_on_queue_contents`'s batched twin.
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

    /// The batched twin of
    /// `under_block_a_push_that_must_wait_for_room_completes_once_a_concurrent_commit_frees_space`:
    /// the prefix that fits is accepted immediately, and the rest waits rather than dropping.
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

    /// The byte-bound half of the same property -- `the_max_bytes_bound_independently_gates_blocks_wait`
    /// for a batch: a batch can be admitted partially because of the *weight* bound alone, with the
    /// item count nowhere near its own.
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

    /// `under_block_a_batch_too_large_to_ever_fit_is_accepted_immediately_not_blocked_forever`, for
    /// an item inside a batch: the "impossible to ever fit" check is per item, exactly as in
    /// `push`, so such an item is accepted immediately rather than parking the whole batch behind
    /// a wait no `pop` could ever satisfy.
    #[tokio::test]
    async fn push_many_with_an_item_that_can_never_fit_takes_the_same_fallback_as_push() {
        let q = test_queue(1000, 10, OverflowPolicy::Block);
        let mut items = batch_of(&[(1, 1), (99, 2)]);

        tokio::time::timeout(Duration::from_secs(5), q.push_many(&mut items))
            .await
            .expect("an item that can never fit must be accepted, not block the batch forever");

        // Exactly what two sequential pushes do: the impossible item falls through to the
        // underlying `DropOldest` fallback, which evicts what little it can (units=1) and accepts
        // the item over-bound rather than wedging the producer forever.
        assert!(items.is_empty());
        assert_eq!(drain_units(&q), vec![2]);

        let single = test_queue(1000, 10, OverflowPolicy::Block);
        single.push(TestItem { weight: 1, units: 1 }).await;
        single.push(TestItem { weight: 99, units: 2 }).await;
        assert_eq!(drain_units(&single), vec![2], "the same outcome as push, item for item");
    }

    /// The batched twin of `under_block_a_zero_max_batches_config_does_not_hang_a_push`.
    #[tokio::test]
    async fn push_many_against_a_zero_max_items_config_does_not_hang() {
        let q = test_queue(0, u64::MAX, OverflowPolicy::Block);
        let mut items = batch_of(&[(1, 1), (1, 2)]);
        tokio::time::timeout(Duration::from_secs(5), q.push_many(&mut items))
            .await
            .expect("max_items: 0 must not permanently block a batch either");
        assert!(items.is_empty());
    }

    /// `close()`'s documented contract for a push racing a close -- "never panic, never hang" --
    /// holds for a whole batch, and for a batch whose wait is *already* in progress when the close
    /// lands: it falls through to the same best-effort `DropOldest` fallback `push` does.
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

    /// The cancellation contract `push_many`'s doc comment states, and the widening ADR
    /// `udp-intake-batching-and-socket-visibility` names: the accepted prefix stays, the remainder
    /// goes with the `Drain` (uncounted), the caller's `Vec` is left empty, and the queue's own
    /// accounting is exactly what the accepted prefix implies -- nothing half-charged.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_push_many_leaves_the_prefix_queued_the_vec_empty_and_accounting_exact() {
        let (registry, q) = recording_queue(2, u64::MAX, OverflowPolicy::Block);
        let mut items = batch_of(&[(1, 1), (1, 2), (1, 3), (1, 4)]);

        {
            let mut pushing = Box::pin(q.push_many(&mut items));
            tokio::time::timeout(Duration::from_millis(1), &mut pushing)
                .await
                .expect_err("the queue holds 2 of 4 -- push_many must still be waiting");
            // `pushing`, and with it the `Drain` holding items 3 and 4, is dropped here.
        }

        assert!(items.is_empty(), "the caller's Vec is emptied even by a cancellation");
        assert_eq!(drain_units(&q), vec![1, 2], "the accepted prefix stayed accepted");

        let events = registry.drain(0);
        assert_eq!(
            metric_sum(&events, TEST_METRICS.items_dropped, None),
            0.0,
            "the cancelled remainder is dropped uncounted -- deliberately, see the doc comment"
        );

        // Accounting is exact, not merely plausible: the queue believes it is empty (len and
        // weight both back to zero), so it takes a fresh full batch without evicting anything.
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
        assert_eq!(
            metric_sum(&registry2.drain(0), TEST_METRICS.items_dropped, None),
            0.0,
            "a queue whose accounting leaked would have evicted to make room for these two"
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

        // One `not_empty` notification for the whole batch -- if one permit were not enough for a
        // waiter to see all three items, this would come back with fewer than three.
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

    /// The property both methods exist for. It cannot be read back off a drained `Registry` --
    /// `Telemetry::gauge` is last-write-wins per series, so one write and sixty-four writes of the
    /// same gauge drain as one identical point (which is exactly why batching them is safe) -- so
    /// the count comes from the queue's own test-only counter, and the `Registry` is asserted on
    /// for the half it *can* answer: that the single update carries the end state, and that nothing
    /// at all is emitted mid-call.
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

        // And a `Block` push_many that is still parked mid-batch has emitted nothing at all: the
        // single update happens when the call completes, not once per admitted item.
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

    /// The `push_blocked` timing is per *call*, not per item that had to wait: a `DdSketch` records
    /// one sample however many of the batch's items waited individually.
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

    /// `max: 0` is a caller bug -- it asks to wait for an item and then take none of it. Caught by
    /// a `debug_assert!` in development; clamped to 1 in release rather than parking a decode loop
    /// forever against a queue with items in it.
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

    /// The equivalence the whole design rests on, checked exhaustively rather than by example:
    /// **any** interleaving of `push`/`push_many`/`pop`/`pop_many` must leave the same queue
    /// contents and the same drop counts as the equivalent sequence of single-item calls. Every
    /// sequence of four operations over the alphabet below is run twice -- once using the batched
    /// methods, once with each batched operation expanded into its singles -- against two different
    /// bound shapes under each dropping policy, and the two must agree exactly.
    ///
    /// **Both queues are closed before the sequence runs**, which is what makes an exhaustive
    /// enumeration possible at all: a `pop`/`pop_many` against an open, empty queue would park
    /// forever, and `closed` changes nothing else here (it is consulted only on `Block`'s waiting
    /// path, and neither policy under test blocks). `Block` itself has no place in this test for
    /// the same reason -- a batch that must wait has no terminating single-call equivalent without
    /// a concurrent consumer, and the tests above cover it directly.
    ///
    /// This is deliberately not a `proptest`: the space is small enough to enumerate completely,
    /// and `logit-pipeline` does not depend on `proptest` today (only `logit-proto` and
    /// `logit-outputs` do). An exhaustive check is both stronger and deterministic here.
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

        /// Runs one sequence, returning `(popped units in order, remaining units in order)`.
        /// `batched` picks whether the many-shaped ops use `push_many`/`pop_many` or are expanded
        /// into the single-item calls they are supposed to be equivalent to.
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
}
