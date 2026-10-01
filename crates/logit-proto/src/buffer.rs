//! The in-process buffering trait and its one implementation, [`InMemoryBuffer`], which
//! `logit_pipeline::queue::BoundedQueue` wraps. The disk spool (`DiskQueue`) doesn't implement
//! [`Buffer`]: a sync, `&mut self` trait is the wrong seam for file I/O
//! (`docs/design/wire-protocol.md`'s "Buffering" section). ADR `buffered-sink-delivery` says why
//! the ack shape is `peek`/`commit` rather than `pop`, and why `Block` isn't an [`OverflowPolicy`].

use std::collections::VecDeque;

/// What a push did against a bounded buffer.
#[derive(Debug)]
#[must_use]
pub enum PushOutcome<T> {
    /// Accepted with room to spare.
    Accepted,
    /// Accepted after evicting these items, oldest first (`OverflowPolicy::DropOldest`), so the
    /// caller can count every one. Can be empty: when only reserved items (see
    /// [`Buffer::peek_at`]) are left, the new item is accepted over the bound rather than evicting
    /// a reservation.
    Evicted(Vec<T>),
    /// Not accepted; the item is handed back unchanged (`OverflowPolicy::DropNewest`).
    Rejected(T),
}

/// What to do when a bounded buffer is full and another item arrives.
///
/// No `Block`: a synchronous trait can't block usefully, so the async
/// `logit_pipeline::queue::BoundedQueue` layers it on top.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    DropOldest,
    DropNewest,
}

/// A bounded, in-process queue whose consumer acknowledges (`peek`, then `commit`) instead of
/// popping, so a failed delivery retries the same item.
///
/// The consumer may read ahead with `peek_at`, which reserves a prefix of the queue: every item
/// from the head through the one it returned. `commit` removes the head and shrinks the prefix by
/// one, so a consumer with several items in flight commits them in order.
pub trait Buffer<T> {
    /// Pushes `item` weighing `weight` bytes (e.g. `EventBatch::estimated_heap_bytes`).
    ///
    /// Under `DropOldest`, an overflowing push evicts the oldest unreserved items until the item
    /// fits, never a reserved one (see `peek_at`). If nothing evictable is left, the item is
    /// accepted over the bound anyway.
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T>;
    /// The head, without removing it; `None` iff empty.
    ///
    /// **Reserves the head against `DropOldest` eviction** until `commit()`. Without that, a
    /// `push()` between `peek()` and `commit()` could evict the item in flight, and `commit()`
    /// would remove a different one. Call `commit()` only once delivery succeeded.
    fn peek(&mut self) -> Option<&T> {
        self.peek_at(0)
    }
    /// The `n`-th item from the head (`0` is the head), without removing it; `None` when
    /// `n >= len()`, reserving nothing new.
    ///
    /// Reserves items `0..=n` against `DropOldest` eviction, so a consumer with `n + 1` items in
    /// flight never has one evicted under it. A reservation is released only by `commit()`,
    /// which releases one.
    fn peek_at(&mut self, n: usize) -> Option<&T>;
    /// Removes and returns the head, releasing one reservation, even when empty.
    fn commit(&mut self) -> Option<T>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// The sum of every held item's `push`-time weight.
    fn weight(&self) -> u64;
}

/// A `VecDeque` bounded by item count and total weight, whichever trips first.
pub struct InMemoryBuffer<T> {
    /// Each item with its push-time weight, so `weight()` never recomputes.
    items: VecDeque<(T, u64)>,
    max_len: usize,
    max_weight: u64,
    /// The held items' weights, summed saturating and reduced saturating: exact whenever the true
    /// sum fits in a `u64`, otherwise at most the true sum, so `len == 0` always means
    /// `weight == 0`. Unchecked arithmetic would panic under the caller's lock in debug builds, or
    /// wrap in release and leave a `Block` push waiting forever on an empty queue.
    weight: u64,
    overflow: OverflowPolicy,
    /// How many items from the head `peek_at` has reserved; `items[..reserved]` is never
    /// evicted. Raised by `peek_at`, lowered by one per `commit`; never above `items.len()`.
    reserved: usize,
}

impl<T> InMemoryBuffer<T> {
    /// Preallocates `max_len.min(4096)` slots: a deep UDP receive queue otherwise pays ~14
    /// warm-up reallocations in the hot path, and the cap stops a huge `max_len` from
    /// preallocating gigabytes.
    pub fn new(max_len: usize, max_weight: u64, overflow: OverflowPolicy) -> Self {
        Self {
            items: VecDeque::with_capacity(max_len.min(4096)),
            max_len,
            max_weight,
            weight: 0,
            overflow,
            reserved: 0,
        }
    }

    /// Whether one more item of `weight` bytes would trip either bound.
    fn would_overflow(&self, weight: u64) -> bool {
        self.items.len() >= self.max_len || self.weight.saturating_add(weight) > self.max_weight
    }

    /// Evicts the oldest unreserved items until `weight` fits or nothing but reserved items is
    /// left. Returns the evicted items, oldest first.
    fn evict_to_fit(&mut self, weight: u64) -> Vec<T> {
        let mut evicted = Vec::new();
        while self.would_overflow(weight) {
            // The reserved prefix is `items[..reserved]`, so evict the first item past it.
            let evict_at = self.reserved;
            if evict_at >= self.items.len() {
                break;
            }
            let Some((item, item_weight)) = self.items.remove(evict_at) else {
                break; // unreachable given the length check above; defensive, not a real path
            };
            self.weight = self.weight.saturating_sub(item_weight);
            evicted.push(item);
        }
        evicted
    }
}

impl<T> Buffer<T> for InMemoryBuffer<T> {
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T> {
        if !self.would_overflow(weight) {
            self.items.push_back((item, weight));
            self.weight = self.weight.saturating_add(weight);
            return PushOutcome::Accepted;
        }
        match self.overflow {
            OverflowPolicy::DropOldest => {
                let evicted = self.evict_to_fit(weight);
                self.items.push_back((item, weight));
                self.weight = self.weight.saturating_add(weight);
                PushOutcome::Evicted(evicted)
            }
            OverflowPolicy::DropNewest => PushOutcome::Rejected(item),
        }
    }

    fn peek_at(&mut self, n: usize) -> Option<&T> {
        let (item, _) = self.items.get(n)?;
        self.reserved = self.reserved.max(n + 1);
        Some(item)
    }

    fn commit(&mut self) -> Option<T> {
        self.reserved = self.reserved.saturating_sub(1);
        self.items.pop_front().map(|(item, weight)| {
            self.weight = self.weight.saturating_sub(weight);
            item
        })
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn weight(&self) -> u64 {
        self.weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unbounded_by_weight(
        max_len: usize,
        overflow: OverflowPolicy,
    ) -> InMemoryBuffer<&'static str> {
        InMemoryBuffer::new(max_len, u64::MAX, overflow)
    }

    /// Push, asserting `Accepted`: in setup, an eviction means a broken fixture.
    fn push_accepted<T: std::fmt::Debug>(buf: &mut impl Buffer<T>, item: T, weight: u64) {
        match buf.push(item, weight) {
            PushOutcome::Accepted => {}
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    #[test]
    fn push_peek_commit_is_fifo() {
        let mut buf = unbounded_by_weight(10, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        push_accepted(&mut buf, "b", 1);
        push_accepted(&mut buf, "c", 1);

        assert_eq!(buf.commit(), Some("a"));
        assert_eq!(buf.commit(), Some("b"));
        assert_eq!(buf.commit(), Some("c"));
        assert_eq!(buf.commit(), None);
    }

    #[test]
    fn peek_does_not_remove() {
        let mut buf = unbounded_by_weight(10, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        push_accepted(&mut buf, "b", 1);

        assert_eq!(buf.peek(), Some(&"a"));
        assert_eq!(buf.peek(), Some(&"a"));
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn commit_on_empty_buffer_is_a_no_op() {
        let mut buf: InMemoryBuffer<&str> = unbounded_by_weight(10, OverflowPolicy::DropOldest);
        assert_eq!(buf.commit(), None);
        assert_eq!(buf.commit(), None);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn drop_oldest_evicts_the_head_and_admits_the_new_item() {
        let mut buf = unbounded_by_weight(2, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        push_accepted(&mut buf, "b", 1);

        match buf.push("c", 1) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, vec!["a"]),
            other => panic!("expected Evicted, got {other:?}"),
        }

        assert_eq!(buf.len(), 2);
        assert_eq!(buf.peek(), Some(&"b"));
        assert_eq!(buf.commit(), Some("b"));
        assert_eq!(buf.commit(), Some("c"));
    }

    #[test]
    fn drop_oldest_evicts_everything_needed_to_actually_fit_in_one_push() {
        // 100 one-weight items at the weight bound, then one 90-weight push: one eviction would
        // leave weight at 189, still over the bound.
        let mut buf = InMemoryBuffer::new(1000, 100, OverflowPolicy::DropOldest);
        for i in 0..100 {
            push_accepted(&mut buf, i, 1);
        }
        assert_eq!(buf.weight(), 100);

        // (100-k)+90 <= 100 needs k >= 90 evictions, landing at weight 100.
        match buf.push(999, 90) {
            PushOutcome::Evicted(evicted) => {
                assert_eq!(
                    evicted.len(),
                    90,
                    "must evict 90 one-weight items to fit a 90-weight push at a 100-weight bound"
                );
                assert_eq!(evicted, (0..90).collect::<Vec<_>>(), "oldest evicted first");
            }
            other => panic!("expected Evicted, got {other:?}"),
        }
        assert!(
            buf.weight() <= 100,
            "weight bound must actually hold after eviction, got {}",
            buf.weight()
        );
    }

    #[test]
    fn peek_reserves_the_head_and_drop_oldest_evicts_around_it_instead() {
        let mut buf = unbounded_by_weight(2, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        push_accepted(&mut buf, "b", 1);

        assert_eq!(buf.peek(), Some(&"a")); // reserves "a"

        match buf.push("c", 1) {
            PushOutcome::Evicted(evicted) => {
                assert_eq!(evicted, vec!["b"], "must evict b, never the reserved head a")
            }
            other => panic!("expected Evicted, got {other:?}"),
        }

        assert_eq!(buf.commit(), Some("a"));
        assert_eq!(buf.commit(), Some("c"));
    }

    #[test]
    fn a_reserved_solo_item_with_nothing_else_evictable_is_never_evicted_new_item_still_accepted() {
        let mut buf = unbounded_by_weight(1, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        assert_eq!(buf.peek(), Some(&"a")); // reserves "a"; buffer is now at its length bound

        match buf.push("b", 1) {
            PushOutcome::Evicted(evicted) => {
                assert!(evicted.is_empty(), "nothing evictable besides the reserved head -- must evict nothing, not the reservation")
            }
            other => panic!("expected Evicted([]), got {other:?}"),
        }

        // The buffer now holds both, past its length bound.
        assert_eq!(buf.commit(), Some("a"));
        assert_eq!(buf.commit(), Some("b"));
    }

    #[test]
    fn commit_releases_the_reservation_so_a_later_push_can_evict_normally() {
        let mut buf = unbounded_by_weight(1, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        assert_eq!(buf.peek(), Some(&"a"));
        assert_eq!(buf.commit(), Some("a")); // releases the reservation

        push_accepted(&mut buf, "b", 1);
        match buf.push("c", 1) {
            PushOutcome::Evicted(evicted) => {
                assert_eq!(evicted, vec!["b"], "no reservation active -- ordinary eviction")
            }
            other => panic!("expected Evicted, got {other:?}"),
        }
    }

    #[test]
    fn peek_at_reserves_every_item_up_to_n_and_drop_oldest_evicts_past_them() {
        let mut buf = unbounded_by_weight(4, OverflowPolicy::DropOldest);
        for item in ["a", "b", "c", "d"] {
            push_accepted(&mut buf, item, 1);
        }

        assert_eq!(buf.peek_at(2), Some(&"c")); // reserves a, b, c

        match buf.push("e", 1) {
            PushOutcome::Evicted(evicted) => {
                assert_eq!(evicted, vec!["d"], "must evict d, never a reserved item")
            }
            other => panic!("expected Evicted, got {other:?}"),
        }
        assert_eq!(buf.commit(), Some("a"));
        assert_eq!(buf.commit(), Some("b"));
        assert_eq!(buf.commit(), Some("c"));
        assert_eq!(buf.commit(), Some("e"));
    }

    #[test]
    fn peek_at_past_the_end_returns_none_and_reserves_nothing_new() {
        let mut buf = unbounded_by_weight(2, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        push_accepted(&mut buf, "b", 1);

        assert_eq!(buf.peek_at(0), Some(&"a"));
        assert_eq!(buf.peek_at(2), None);
        assert_eq!(buf.peek_at(usize::MAX), None);

        // Only "a" is reserved, so an overflowing push evicts "b".
        match buf.push("c", 1) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, vec!["b"]),
            other => panic!("expected Evicted, got {other:?}"),
        }
    }

    #[test]
    fn commit_releases_one_reservation_and_leaves_the_rest() {
        let mut buf = unbounded_by_weight(3, OverflowPolicy::DropOldest);
        for item in ["a", "b", "c"] {
            push_accepted(&mut buf, item, 1);
        }
        assert_eq!(buf.peek_at(1), Some(&"b")); // reserves a, b
        assert_eq!(buf.commit(), Some("a")); // b stays reserved

        push_accepted(&mut buf, "d", 1);
        match buf.push("e", 1) {
            PushOutcome::Evicted(evicted) => {
                assert_eq!(evicted, vec!["c"], "b is still reserved; c is the oldest unreserved")
            }
            other => panic!("expected Evicted, got {other:?}"),
        }
        assert_eq!(buf.commit(), Some("b"));

        // Nothing is reserved now: the oldest item goes first.
        push_accepted(&mut buf, "f", 1);
        match buf.push("g", 1) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, vec!["d"]),
            other => panic!("expected Evicted, got {other:?}"),
        }
    }

    #[test]
    fn with_every_item_reserved_drop_oldest_accepts_one_over_the_bound() {
        let mut buf = unbounded_by_weight(2, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 1);
        push_accepted(&mut buf, "b", 1);
        assert_eq!(buf.peek_at(1), Some(&"b")); // reserves both

        match buf.push("c", 1) {
            PushOutcome::Evicted(evicted) => assert!(evicted.is_empty()),
            other => panic!("expected Evicted([]), got {other:?}"),
        }
        assert_eq!(buf.len(), 3, "accepted one over the length bound");
        assert_eq!(buf.commit(), Some("a"));
        assert_eq!(buf.commit(), Some("b"));
        assert_eq!(buf.commit(), Some("c"));
    }

    #[test]
    fn drop_newest_rejects_and_leaves_the_buffer_unchanged() {
        let mut buf = unbounded_by_weight(2, OverflowPolicy::DropNewest);
        push_accepted(&mut buf, "a", 1);
        push_accepted(&mut buf, "b", 1);

        match buf.push("c", 1) {
            PushOutcome::Rejected(rejected) => assert_eq!(rejected, "c"),
            other => panic!("expected Rejected, got {other:?}"),
        }

        assert_eq!(buf.len(), 2);
        assert_eq!(buf.peek(), Some(&"a"));
        assert_eq!(buf.commit(), Some("a"));
        assert_eq!(buf.commit(), Some("b"));
    }

    #[test]
    fn length_bound_trips_independently_of_weight_bound() {
        let mut buf = InMemoryBuffer::new(2, u64::MAX, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 0);
        push_accepted(&mut buf, "b", 0);
        match buf.push("c", 0) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, vec!["a"]),
            other => panic!("expected Evicted, got {other:?}"),
        }
    }

    #[test]
    fn weight_bound_trips_independently_of_length_bound() {
        let mut buf = InMemoryBuffer::new(100, 10, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 6);
        push_accepted(&mut buf, "b", 4);
        assert_eq!(buf.len(), 2);

        match buf.push("c", 1) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, vec!["a"]),
            other => panic!("expected Evicted, got {other:?}"),
        }
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn weight_stays_exact_across_a_push_evict_commit_sequence() {
        let mut buf = InMemoryBuffer::new(100, 100, OverflowPolicy::DropOldest);
        assert_eq!(buf.weight(), 0);

        push_accepted(&mut buf, "a", 10);
        assert_eq!(buf.weight(), 10);
        push_accepted(&mut buf, "b", 20);
        assert_eq!(buf.weight(), 30);
        push_accepted(&mut buf, "c", 5);
        assert_eq!(buf.weight(), 35);

        match buf.push("d", 70) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, vec!["a"]),
            other => panic!("expected Evicted, got {other:?}"),
        }
        // 35 - 10 (evicted "a") + 70 (pushed "d") = 95.
        assert_eq!(buf.weight(), 95);

        assert_eq!(buf.commit(), Some("b"));
        assert_eq!(buf.weight(), 75);
        assert_eq!(buf.commit(), Some("c"));
        assert_eq!(buf.weight(), 70);
        assert_eq!(buf.commit(), Some("d"));
        assert_eq!(buf.weight(), 0);
        assert_eq!(buf.commit(), None);
        assert_eq!(buf.weight(), 0);
    }

    /// A weight sum past `u64::MAX` saturates on the way in and never underflows on the way out,
    /// through `commit` or eviction, so an empty buffer always weighs 0.
    #[test]
    fn saturated_weights_never_underflow_and_an_empty_buffer_weighs_nothing() {
        let mut buf = InMemoryBuffer::new(10, u64::MAX, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "huge", u64::MAX);
        push_accepted(&mut buf, "small", 5);
        assert_eq!(buf.weight(), u64::MAX, "the sum saturates rather than wrapping");
        assert_eq!(buf.commit(), Some("huge"));
        assert_eq!(buf.commit(), Some("small"));
        assert_eq!(buf.weight(), 0, "len == 0 must mean weight == 0");

        // The same through eviction: two items at a two-item bound, then a third evicts one.
        let mut buf = InMemoryBuffer::new(2, u64::MAX, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "huge", u64::MAX);
        push_accepted(&mut buf, "small", 5);
        match buf.push("next", 1) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, vec!["huge"]),
            other => panic!("expected Evicted, got {other:?}"),
        }
        assert_eq!(buf.commit(), Some("small"));
        assert_eq!(buf.commit(), Some("next"));
        assert_eq!(buf.weight(), 0);
    }
}
