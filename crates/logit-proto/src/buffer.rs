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
    /// Accepted, but the head was evicted to make room (`OverflowPolicy::DropOldest`). The
    /// evicted item is returned so the caller can count/log what was lost -- never silently.
    Evicted(T),
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
    /// ignore it). Under `DropOldest`, an overflowing push evicts only the current head, once --
    /// not a loop down to the bound -- so `weight()` can briefly exceed `max_weight` by up to the
    /// pushed item's own size when it's larger than what got evicted. Self-correcting on the next
    /// push (the check re-runs against the true current weight), and the same shape of soft,
    /// bounded-not-exact cap `docs/design/memory.md` §5 already accepts for `CHANNEL_CAPACITY`.
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T>;
    /// The head, without removing it. `None` iff empty. Call this, then `commit()` only after
    /// whatever the caller does with the peeked item has actually succeeded.
    fn peek(&self) -> Option<&T>;
    /// Removes and returns the head. A no-op returning `None` when empty.
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
}

impl<T> InMemoryBuffer<T> {
    pub fn new(max_len: usize, max_weight: u64, overflow: OverflowPolicy) -> Self {
        Self { items: VecDeque::new(), max_len, max_weight, weight: 0, overflow }
    }

    /// Whether accepting one more item of `weight` bytes (on top of what's already held) would
    /// trip either bound.
    fn would_overflow(&self, weight: u64) -> bool {
        self.items.len() >= self.max_len || self.weight + weight > self.max_weight
    }
}

impl<T> Buffer<T> for InMemoryBuffer<T> {
    fn push(&mut self, item: T, weight: u64) -> PushOutcome<T> {
        if self.would_overflow(weight) {
            match self.overflow {
                OverflowPolicy::DropOldest => {
                    let evicted = self.items.pop_front().map(|(evicted_item, evicted_weight)| {
                        self.weight -= evicted_weight;
                        evicted_item
                    });
                    self.items.push_back((item, weight));
                    self.weight += weight;
                    match evicted {
                        Some(evicted) => PushOutcome::Evicted(evicted),
                        // An empty buffer can only overflow via `max_weight` being smaller than
                        // `weight` itself; there's nothing to evict, so just accept it.
                        None => PushOutcome::Accepted,
                    }
                }
                OverflowPolicy::DropNewest => PushOutcome::Rejected(item),
            }
        } else {
            self.items.push_back((item, weight));
            self.weight += weight;
            PushOutcome::Accepted
        }
    }

    fn peek(&self) -> Option<&T> {
        self.items.front().map(|(item, _)| item)
    }

    fn commit(&mut self) -> Option<T> {
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
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, "a"),
            other => panic!("expected Evicted, got {other:?}"),
        }

        assert_eq!(buf.len(), 2);
        assert_eq!(buf.peek(), Some(&"b"));
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
        // Under the weight bound (each item weighs nothing) but at the length bound: still
        // evicts.
        let mut buf = InMemoryBuffer::new(2, u64::MAX, OverflowPolicy::DropOldest);
        push_accepted(&mut buf, "a", 0);
        push_accepted(&mut buf, "b", 0);
        match buf.push("c", 0) {
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, "a"),
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
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, "a"),
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
            PushOutcome::Evicted(evicted) => assert_eq!(evicted, "a"),
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
