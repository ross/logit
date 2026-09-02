//! [`SinkQueue`]: the async wrapper around `logit_proto::buffer::Buffer` that a sink's writer
//! drains from and its drain loop pushes into -- see `docs/adr/0021-buffered-sink-delivery.md`.
//! `Buffer` itself is sync (no `.await` in a critical section), so this type owns the one thing a
//! sync trait can't express: `Block`, which awaits room rather than dropping.

use logit_core::{EventBatch, Telemetry};
use logit_proto::buffer::{Buffer, InMemoryBuffer, OverflowPolicy as DropPolicy, PushOutcome};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// What to do when the queue is full. `Block` isn't part of `logit_proto::buffer::OverflowPolicy`
/// -- it's this type's own addition, layered on top of the two dropping policies that trait can
/// express synchronously (see `logit_proto::buffer::OverflowPolicy`'s doc comment for why).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    Block,
    DropOldest,
    DropNewest,
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

/// The async wrapper around `logit_proto::buffer::InMemoryBuffer<Arc<EventBatch>>` that sits
/// between a sink's inbox drain and its writer, letting the two proceed independently
/// (`docs/adr/0021-buffered-sink-delivery.md`). Not `Clone` -- exactly one `SinkQueue` value
/// exists per sink, wrapped in `Arc` by its two callers (`drain_inbox`/`write_loop` in
/// `runtime.rs`, each holding their own `Arc::clone`).
///
/// `std::sync::Mutex`, not tokio's -- every critical section here is a `VecDeque` push/pop with
/// no `.await` inside it, so the sync mutex is both simpler and cheaper.
pub struct SinkQueue {
    inner: Mutex<InMemoryBuffer<Arc<EventBatch>>>,
    /// Woken by `push`/`commit` progress -- signals "there might be something to read now" to
    /// `peek`.
    not_empty: Notify,
    /// Woken by `commit` (room freed) and by `close` -- signals "there might be room now, or the
    /// queue is closing" to a blocked `push`.
    not_full: Notify,
    closed: AtomicBool,
    /// Whether `push` should await room rather than let a push through to the underlying
    /// buffer's dropping policy. `true` iff the configured [`OverflowPolicy`] is `Block`.
    block_when_full: bool,
    max_batches: usize,
    max_bytes: u64,
    telemetry: Telemetry,
}

impl SinkQueue {
    /// `OverflowPolicy::Block` has no equivalent in `logit_proto::buffer::OverflowPolicy` --
    /// that trait only knows the two dropping policies (by design; a sync trait can't block
    /// usefully). The resolution: the underlying `InMemoryBuffer` is always built with a
    /// concrete dropping policy (`DropOldest` standing in for `Block`), but under `Block`,
    /// `push` (below) always awaits room *before* it ever calls the underlying `Buffer::push` --
    /// so in ordinary single-writer operation the underlying buffer's dropping fallback is never
    /// actually exercised. It's `DropOldest`, not `DropNewest`, specifically so the one case
    /// where `push` *does* fall through without blocking -- a batch that could never fit even
    /// against an empty queue, see `push`'s "impossible to ever fit" check below -- degrades to
    /// "evict what little can be evicted, then accept anyway" rather than silently rejecting a
    /// batch `Block`'s whole contract says should never be dropped. This is also the fallback for
    /// the push-races-`close()` case documented on [`SinkQueue::close`].
    pub fn new(config: SinkQueueConfig, telemetry: Telemetry) -> Self {
        let block_when_full = config.overflow == OverflowPolicy::Block;
        let underlying = match config.overflow {
            OverflowPolicy::Block | OverflowPolicy::DropOldest => DropPolicy::DropOldest,
            OverflowPolicy::DropNewest => DropPolicy::DropNewest,
        };
        Self {
            inner: Mutex::new(InMemoryBuffer::new(
                config.max_batches,
                config.max_bytes,
                underlying,
            )),
            not_empty: Notify::new(),
            not_full: Notify::new(),
            closed: AtomicBool::new(false),
            block_when_full,
            max_batches: config.max_batches,
            max_bytes: config.max_bytes,
            telemetry,
        }
    }

    /// Pushes `batch`, weighing it by `EventBatch::estimated_heap_bytes` -- computed exactly
    /// once here, not per retry attempt below.
    ///
    /// Under `Block`: waits for room (re-checked under the lock on every wakeup -- the standard
    /// `Notify` condvar pattern, race-free because the `Notified` future is constructed *before*
    /// the state check it's guarding, so a `commit()`/`close()` landing anywhere after that point
    /// is never missed) before ever attempting the underlying push, so the underlying buffer's
    /// dropping fallback is never reached in ordinary operation. Under `DropOldest`/`DropNewest`:
    /// one lock acquisition, one push attempt, no waiting.
    ///
    /// **Never blocks on a batch that could never fit even against an empty queue** (`weight`
    /// alone exceeds `max_bytes`, or `max_batches` is configured as `0`) -- no amount of waiting
    /// would ever free enough room, since there's nothing productive for a concurrent `commit()`
    /// to do about it. Such a batch instead falls straight through to the underlying `DropOldest`
    /// fallback (see `new`), which evicts what little it safely can and accepts the batch anyway
    /// rather than wedging this sink, and everything upstream of it, forever.
    ///
    /// Every accepted push notifies `not_empty` once. A push that actually had to wait times
    /// `logit.component.buffer.push.blocked.duration` around the whole wait -- a push that never
    /// had to wait records no sample at all, so the metric isn't muddied by a stream of ~0
    /// durations from the common case.
    pub async fn push(&self, batch: Arc<EventBatch>) {
        let weight = batch.estimated_heap_bytes();
        let impossible_to_ever_fit = weight > self.max_bytes || self.max_batches == 0;
        let mut waited = false;
        let mut blocked_timer: Option<logit_core::telemetry::Timer> = None;

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
                    let outcome = inner.push(Arc::clone(&batch), weight);
                    Some((outcome, inner.len(), inner.weight()))
                }
            };

            match attempt {
                None => {
                    if !waited {
                        waited = true;
                        blocked_timer = Some(
                            self.telemetry.timer("logit.component.buffer.push.blocked.duration"),
                        );
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

        for evicted_batch in &evicted {
            self.count_dropped("overflow_oldest", evicted_batch);
        }
        if let Some(rejected_batch) = &rejected {
            self.count_dropped("overflow_newest", rejected_batch);
        }

        // Only records a sample if `blocked_timer` is `Some` -- i.e. only if this push actually
        // waited at least once above.
        drop(blocked_timer);
        self.not_empty.notify_one();
        self.update_gauges(len, total_weight);
    }

    /// The head, without removing it -- a cloned `Arc` (a cheap refcount bump), so the caller
    /// can act on it and only remove it (via [`SinkQueue::commit`]) once that action succeeds.
    /// Awaits `not_empty` while the queue is empty and open; returns `None` once the queue is
    /// both closed and empty, checked together under one lock acquisition so a concurrent
    /// `close()` can never be observed racing a concurrent `push()` -- either the push landed
    /// before this check took the lock (and is seen), or it didn't (and `closed` becoming true
    /// afterward is this call's problem on its *next* iteration, not this one).
    pub async fn peek(&self) -> Option<Arc<EventBatch>> {
        loop {
            let notified = self.not_empty.notified();
            {
                let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(item) = inner.peek() {
                    return Some(Arc::clone(item));
                }
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
            }
            notified.await;
        }
    }

    /// Removes and returns the head (a no-op returning `None` on an empty queue), notifies any
    /// blocked `push` that room may now be available, and refreshes the depth/utilization
    /// gauges.
    pub fn commit(&self) -> Option<Arc<EventBatch>> {
        let (item, len, weight) = {
            let mut inner = self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let item = inner.commit();
            (item, inner.len(), inner.weight())
        };
        self.not_full.notify_one();
        self.update_gauges(len, weight);
        item
    }

    /// Marks the queue closed: no more items will ever arrive, so once it's also empty,
    /// `peek()` should stop waiting and return `None`.
    ///
    /// Wakes every waiter on both `Notify`s -- `not_empty`, so a `peek()` parked on an empty
    /// queue observes the close instead of waiting forever, and `not_full`, so a `push()`
    /// blocked on a full queue under `Block` also wakes rather than hanging against a queue that
    /// will never drain further once its writer sees it close.
    ///
    /// **Decision on a push racing a concurrent close:** a blocked push that wakes because of
    /// this call re-checks state (per `push`'s loop) and, seeing `closed == true`, falls through
    /// to one best-effort attempt against the underlying buffer's `DropOldest` fallback (see
    /// `new`) rather than waiting again -- it may then evict (never the reserved head, if any) or
    /// accept over-bound instead of blocking forever. In practice `drain_inbox` only calls
    /// `close()` once its own inbox is fully drained, so no further `push()` calls happen at all;
    /// this only matters for a hypothetical caller that pushes concurrently with closing, and the
    /// contract for that case is simply: never panic, never hang.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.not_empty.notify_waiters();
        self.not_full.notify_waiters();
    }

    fn would_overflow(&self, inner: &InMemoryBuffer<Arc<EventBatch>>, weight: u64) -> bool {
        inner.len() >= self.max_batches || inner.weight() + weight > self.max_bytes
    }

    fn count_dropped(&self, reason: &'static str, batch: &Arc<EventBatch>) {
        self.telemetry.count("logit.component.batches.dropped", 1.0, &[("reason", reason)]);
        self.telemetry.count(
            "logit.component.events.dropped",
            batch.events.len() as f64,
            &[("reason", reason)],
        );
    }

    /// `logit.component.buffer.utilization` is `max(batches ratio, bytes ratio)` -- whichever
    /// bound is closer to tripping is what actually predicts blocking/dropping next, so
    /// reporting only one of the two bounds would under-report risk whenever the other is the
    /// tighter one for a given workload. Guards both denominators against zero (a config with
    /// either bound set to 0 is degenerate, but this must not panic or produce NaN/inf against
    /// it).
    fn update_gauges(&self, len: usize, weight: u64) {
        self.telemetry.gauge("logit.component.buffer.batches", len as f64, &[]);
        self.telemetry.gauge("logit.component.buffer.bytes", weight as f64, &[]);
        let batches_ratio =
            if self.max_batches == 0 { 0.0 } else { len as f64 / self.max_batches as f64 };
        let bytes_ratio =
            if self.max_bytes == 0 { 0.0 } else { weight as f64 / self.max_bytes as f64 };
        self.telemetry.gauge(
            "logit.component.buffer.utilization",
            batches_ratio.max(bytes_ratio),
            &[],
        );
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

    #[tokio::test]
    async fn push_then_peek_then_commit_round_trips_one_batch() {
        let q = queue(10, u64::MAX, OverflowPolicy::Block);
        let sent = tiny_batch();
        q.push(Arc::clone(&sent)).await;

        let peeked = q.peek().await.expect("should peek the pushed batch");
        assert!(Arc::ptr_eq(&peeked, &sent));

        let committed = q.commit().expect("should commit the pushed batch");
        assert!(Arc::ptr_eq(&committed, &sent));
        assert!(q.commit().is_none(), "nothing left to commit");
    }

    #[tokio::test]
    async fn peek_without_commit_called_twice_returns_the_same_batch_both_times() {
        let q = queue(10, u64::MAX, OverflowPolicy::Block);
        let sent = tiny_batch();
        q.push(Arc::clone(&sent)).await;

        let first = q.peek().await.expect("should peek");
        let second = q.peek().await.expect("should peek again");
        assert!(Arc::ptr_eq(&first, &sent));
        assert!(Arc::ptr_eq(&second, &sent));
    }

    #[tokio::test(start_paused = true)]
    async fn under_block_a_push_that_must_wait_for_room_completes_once_a_concurrent_commit_frees_space(
    ) {
        let q = Arc::new(queue(1, u64::MAX, OverflowPolicy::Block));
        q.push(tiny_batch()).await; // fills the one slot

        let q2 = Arc::clone(&q);
        let blocked = tokio::spawn(async move {
            q2.push(tiny_batch()).await;
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
        q.push(Arc::clone(&a)).await;
        q.push(Arc::clone(&b)).await;
        q.push(Arc::clone(&c)).await; // evicts `a`

        let first = q.commit().expect("should commit");
        assert!(Arc::ptr_eq(&first, &b), "the oldest batch (a) should never appear");
        let second = q.commit().expect("should commit");
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
        q.push(Arc::clone(&a)).await;
        q.push(Arc::clone(&b)).await;

        let peeked = q.peek().await.expect("should peek a"); // reserves `a`
        assert!(Arc::ptr_eq(&peeked, &a));

        q.push(Arc::clone(&c)).await; // must evict `b`, never the reserved `a`

        let committed = q.commit().expect("should commit the batch that was actually peeked/sent");
        assert!(
            Arc::ptr_eq(&committed, &a),
            "commit must return the exact batch that was peeked, not whatever is now at the front"
        );
        let next = q.commit().expect("should commit");
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

        tokio::time::timeout(Duration::from_secs(5), q.push(oversized))
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
        tokio::time::timeout(Duration::from_secs(5), q.push(tiny_batch()))
            .await
            .expect("max_batches: 0 must not permanently block every push");
    }

    #[tokio::test]
    async fn under_drop_newest_pushing_into_a_full_queue_is_a_no_op_on_queue_contents() {
        let q = queue(2, u64::MAX, OverflowPolicy::DropNewest);
        let a = tiny_batch();
        let b = tiny_batch();
        let c = tiny_batch();
        q.push(Arc::clone(&a)).await;
        q.push(Arc::clone(&b)).await;
        q.push(c).await; // rejected -- queue contents unchanged

        let first = q.commit().expect("should commit");
        assert!(Arc::ptr_eq(&first, &a));
        let second = q.commit().expect("should commit");
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

        q.push(sent2).await;

        let peeked = tokio::time::timeout(Duration::from_secs(1), peeking)
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
        q.push(tiny_batch()).await;

        let q2 = Arc::clone(&q);
        let blocked = tokio::spawn(async move { q2.push(tiny_batch()).await });
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
        q.push(first).await;

        let q2 = Arc::clone(&q);
        let blocked = tokio::spawn(async move { q2.push(batch(64)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!blocked.is_finished(), "the byte bound alone should be enough to block");

        q.commit();
        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .expect("should resolve once room is freed")
            .expect("should not panic");
    }
}
