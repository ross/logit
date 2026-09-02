//! The output buffering trait. See `docs/design/wire-protocol.md`: this boundary is cheap to add
//! now and expensive to retrofit onto call sites that assumed an in-memory queue, so it's defined
//! even though only an in-memory implementation ships initially. See
//! `docs/adr/0020-buffered-sink-delivery.md` for why the ack shape is `peek`/`commit` rather than
//! `push`/`pop`, and why `Block` is not a variant of [`OverflowPolicy`].

use std::collections::VecDeque;

/// What a push did against a bounded buffer.
#[derive(Debug)]
#[must_use]
pub enum PushOutcome<T> {
    /// Accepted with room to spare.
    Accepted,
    /// Accepted; the listed items (oldest evicted first) were evicted to make room
    /// (`OverflowPolicy::DropOldest`), so the caller can count/log every one of them -- never
    /// silently. Can be empty: if the head is reserved (see [`Buffer::peek`]) and nothing else is
    /// evictable, the new item is still accepted (over-bound) rather than evicting the reserved
    /// item or blocking forever on a batch that can never fit.
    Evicted(Vec<T>),
    /// Not accepted; the item is handed back unchanged (`OverflowPolicy::DropNewest`).
    Rejected(T),
}

/// What to do when a bounded buffer is full and another item arrives. `Block` is deliberately
/// NOT a variant here -- a synchronous trait can't block usefully, so `Block` is a concern of the
/// async wrapper built on top of this (`logit_pipeline::SinkQueue`, not yet built), not of
/// `Buffer` or its impls. This trait implements only the two dropping policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    DropOldest,
    DropNewest,
}

/// A bounded, in-process queue between a producer and a slower/intermittent consumer, with an
/// ack-based removal so a consumer can retry a not-yet-confirmed delivery without losing it
/// (`peek`/`commit`, not `pop`) -- see `docs/adr/0020-buffered-sink-delivery.md`.
pub trait Buffer<T> {
    /// Push `item`, weighing `weight` bytes for the buffer's byte-aware bound (see
    /// `EventBatch::estimated_heap_bytes`, `logit-core`; impls that don't bound by weight can
    /// ignore it). Under `DropOldest`, an overflowing push evicts from the head, in a loop, until
    /// it fits or nothing more is evictable -- never the reserved head (see `peek`), so `weight()`
    /// stays within `max_weight` after this call unless nothing evictable was left (a reserved
    /// solo item, or the buffer already empty), in which case the new item is accepted anyway
    /// rather than evicting what's reserved or leaving the caller with nothing accepted at all.
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T>;
    /// The head, without removing it, and **reserves it against `DropOldest` eviction** until
    /// `commit()` releases the reservation -- the ack invariant depends on this: a caller that
    /// peeks an item, starts acting on it, and only later calls `commit()` must never have that
    /// exact item silently evicted out from under it by a concurrent `push()` in between, which
    /// would make `commit()` remove a *different* item than the one the caller actually acted on.
    /// `None` iff empty (reservation state unchanged). Call this, then `commit()` only after
    /// whatever the caller does with the peeked item has actually succeeded.
    fn peek(&mut self) -> Option<&T>;
    /// Removes and returns the head, releasing any reservation `peek()` established (even if this
    /// call finds the buffer empty). A no-op returning `None` when empty.
    fn commit(&mut self) -> Option<T>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Total weight of everything currently held (sum of each held item's `push`-time weight).
    fn weight(&self) -> u64;
}

/// The one shipping `Buffer` implementation: a `VecDeque` bounded by item count and total weight,
/// whichever trips first, evicting or rejecting per `overflow`.
pub struct InMemoryBuffer<T> {
    /// Each item alongside its push-time weight, so `weight()` never recomputes anything.
    items: VecDeque<(T, u64)>,
    max_len: usize,
    max_weight: u64,
    weight: u64,
    overflow: OverflowPolicy,
    /// Set by `peek`, cleared by `commit` -- while true, the item at `items[0]` is off-limits to
    /// eviction (see `peek`'s doc comment on the trait).
    head_reserved: bool,
}

impl<T> InMemoryBuffer<T> {
    pub fn new(max_len: usize, max_weight: u64, overflow: OverflowPolicy) -> Self {
        Self {
            items: VecDeque::new(),
            max_len,
            max_weight,
            weight: 0,
            overflow,
            head_reserved: false,
        }
    }

    /// Whether accepting one more item of `weight` bytes (on top of what's already held) would
    /// trip either bound.
    fn would_overflow(&self, weight: u64) -> bool {
        self.items.len() >= self.max_len || self.weight + weight > self.max_weight
    }

    /// Evicts from the front, in a loop, until `weight` would fit or nothing more is evictable --
    /// never touching a reserved head. Returns every evicted item, oldest first; empty if nothing
    /// was evictable (the buffer was already empty, or the only item present is the reserved
    /// head).
    fn evict_to_fit(&mut self, weight: u64) -> Vec<T> {
        let mut evicted = Vec::new();
        while self.would_overflow(weight) {
            // The reserved head, if any, is always at index 0 -- evict index 1 instead so it's
            // never touched. If reserved and nothing follows it, there's genuinely nothing left
            // this call may evict.
            let evict_at = usize::from(self.head_reserved);
            if evict_at >= self.items.len() {
                break;
            }
            let Some((item, item_weight)) = self.items.remove(evict_at) else {
                break; // unreachable given the length check above; defensive, not a real path
            };
            self.weight -= item_weight;
            evicted.push(item);
        }
        evicted
    }
}

impl<T> Buffer<T> for InMemoryBuffer<T> {
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T> {
        if !self.would_overflow(weight) {
            self.items.push_back((item, weight));
            self.weight += weight;
            return PushOutcome::Accepted;
        }
        match self.overflow {
            OverflowPolicy::DropOldest => {
                let evicted = self.evict_to_fit(weight);
                self.items.push_back((item, weight));
                self.weight += weight;
                PushOutcome::Evicted(evicted)
            }
            OverflowPolicy::DropNewest => PushOutcome::Rejected(item),
        }
    }

    fn peek(&mut self) -> Option<&T> {
        if !self.items.is_empty() {
            self.head_reserved = true;
        }
        self.items.front().map(|(item, _)| item)
    }

    fn commit(&mut self) -> Option<T> {
        self.head_reserved = false;
        self.items.pop_front().map(|(item, weight)| {
            self.weight -= weight;
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

    /// Push, asserting the push was accepted outright -- for test setup where an eviction or
    /// rejection would indicate a broken fixture, not the behavior under test.
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
        // 100 one-weight items at the weight bound, then one 90-weight push -- a single eviction
        // (the old, buggy shape) would leave weight at 100-1+90=189, still over the bound. Loop
        // eviction must keep evicting until it actually fits.
        let mut buf = InMemoryBuffer::new(1000, 100, OverflowPolicy::DropOldest);
        for i in 0..100 {
            push_accepted(&mut buf, i, 1);
        }
        assert_eq!(buf.weight(), 100);

        // Evicting k one-weight items leaves weight = 100-k; the push fits once
        // (100-k)+90 <= 100, i.e. k >= 90 -- so exactly 90 evictions, leaving weight at
        // 10, then +90 for the push = 100, right at the bound.
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

        // "a" is still exactly what commit() returns -- the ack invariant this reservation
        // exists to protect: a caller that peeked "a" and is mid-delivery must get "a" back.
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

        // "a" (the reserved item) must still be exactly what commit() returns -- it was never
        // evicted despite the buffer now holding both "a" and "b" past its nominal length bound.
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
        // Under the weight bound (each item weighs nothing) but at the length bound: still
        // evicts.
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
        // Under the length bound but over the weight bound: still evicts.
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

        // Pushing something too heavy to fit alongside the rest trips the weight bound and
        // evicts "a" (weight 10).
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
}
