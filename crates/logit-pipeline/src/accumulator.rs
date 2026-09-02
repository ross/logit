//! [`BatchAccumulator`]: amortizes many small decoded batches into fewer, larger ones before a
//! [`crate::Fanout::send`] -- the "datagram->batch assembly" half of
//! `docs/adr/0022-decoupled-listener-io.md`. Transport-agnostic and socket-free by design: a UDP
//! listener's decode loop (`logit-inputs`) is the only caller today, but nothing here mentions a
//! socket, a datagram, or any concrete [`logit_proto::Decoder`].

use logit_core::{Event, EventBatch, Resource};
use std::sync::Arc;
use std::time::Duration;

/// Why an accumulated batch was emitted -- a `&'static str` reason tag on
/// `logit.component.receive.flushed` (`docs/design/internal-telemetry.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    MaxEvents,
    MaxBytes,
    Interval,
    ResourceChange,
    Shutdown,
}

impl FlushReason {
    pub fn as_str(self) -> &'static str {
        match self {
            FlushReason::MaxEvents => "max_events",
            FlushReason::MaxBytes => "max_bytes",
            FlushReason::Interval => "interval",
            FlushReason::ResourceChange => "resource_change",
            FlushReason::Shutdown => "shutdown",
        }
    }
}

/// Accumulates decoded events until one of three bounds is reached, then hands back everything
/// held as one merged batch. Owns no clock and no timer of its own -- the caller races its own
/// deadline (see [`BatchAccumulator::next_deadline`]) and calls [`BatchAccumulator::take`] when it
/// fires; this type only tracks the held events and their resource.
///
/// No `weight` field cached incrementally -- see [`BatchAccumulator::absorb`]'s doc comment for
/// why, and why that's the right trade rather than an oversight.
pub struct BatchAccumulator {
    resource: Option<Arc<Resource>>,
    events: Vec<Event>,
    max_events: usize,
    max_bytes: u64,
}

impl BatchAccumulator {
    pub fn new(max_events: usize, max_bytes: u64) -> Self {
        Self { resource: None, events: Vec::new(), max_events, max_bytes }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Absorbs `events` under `resource`, appending via [`Vec::append`] rather than taking `events`
    /// by value. This is what makes the whole exercise pay off: `events` comes straight from a
    /// [`logit_proto::Decoder::decode_into`] call against a buffer the caller reuses across
    /// datagrams, and `Vec::append` drains `events` into this accumulator's own buffer while
    /// leaving `events` empty **with its allocated capacity intact** -- unlike `std::mem::take`,
    /// which would replace it with a fresh, capacity-0 `Vec` and silently undo the whole point of
    /// reusing a scratch buffer across calls (`docs/design/memory.md` §2; see also
    /// `docs/adr/0022-decoupled-listener-io.md`'s allocation accounting).
    ///
    /// Returns `Some` once a bound is *reached or exceeded* -- never splits a decoded batch, which
    /// is what makes `batch_max_events: 1` mean "one send per datagram": every non-empty decode
    /// immediately reaches the bound, so a single datagram decoding to 40 events still emits one
    /// batch of 40, not 40 batches -- the bound governs when to stop accumulating, never how to
    /// subdivide one decode's output.
    ///
    /// **The resource rule.** An accumulated batch carries one `Arc<Resource>`. If `resource` is
    /// not `Arc::ptr_eq` to whatever this accumulator already holds, whatever was held is flushed
    /// first (`FlushReason::ResourceChange`) and `events` starts a fresh accumulation -- merging
    /// across distinct resources would silently relabel events onto the wrong one, a correctness
    /// bug no test would catch since the output stays well-formed. Every decoder shipped today
    /// (`StatsdDecoder`, `SyslogDecoder`) constructs one `Arc::new(Resource::default())` per
    /// decoder instance and stamps every decoded batch with it, so in practice this comparison
    /// never trips -- it exists to make that assumption load-bearing rather than latent, the same
    /// *n*-to-1 hazard `docs/adr/0008-aggregation-window-semantics.md` already documents for a Lua
    /// component's `flush()`.
    ///
    /// **Why weight isn't tracked incrementally.** The byte bound needs
    /// `EventBatch::estimated_heap_bytes`, which operates on a real `&EventBatch`, not a resource
    /// and an event slice separately -- so this recomputes it each call via a zero-copy
    /// swap-in/swap-out of this accumulator's own fields into a throwaway `EventBatch` (see
    /// [`BatchAccumulator::current_weight`]), rather than duplicating that formula here to track a
    /// running total incrementally. `estimated_heap_bytes` is already documented as "a deliberately
    /// approximate O(events) walk" (`docs/design/memory.md` §5), so recomputing it once per absorbed
    /// datagram is the same asymptotic cost class the design already accepts, not a new one --
    /// bounded by `max_events` regardless, so it never grows past one flush cycle's worth of work.
    ///
    /// An empty `events` (a datagram that decoded to nothing) is absorbed as a no-op: it never
    /// changes the held resource and never triggers a flush on its own.
    #[must_use]
    pub fn absorb(
        &mut self,
        resource: Arc<Resource>,
        events: &mut Vec<Event>,
    ) -> Option<(EventBatch, FlushReason)> {
        if events.is_empty() {
            return None;
        }

        let resource_changed = match &self.resource {
            Some(held) => !Arc::ptr_eq(held, &resource),
            None => false,
        };

        if resource_changed {
            // Flush whatever was held under the old resource, then start a fresh accumulation
            // with the incoming events -- they are NOT dropped, only deferred to a later
            // `take()`/`absorb()`. Not also bound-checked against `max_events`/`max_bytes` here:
            // this call already reports one flush (the resource change); a lone incoming batch
            // that happens to also exceed a bound on its own gets flushed on the very next
            // `absorb` or by the caller's interval timer, whichever comes first -- accepted
            // staleness of at most one absorb, not a correctness gap (nothing is ever dropped).
            let flushed =
                self.take().expect("resource_changed is only true when self.resource is Some");
            self.resource = Some(resource);
            self.events.append(events);
            return Some((flushed, FlushReason::ResourceChange));
        }

        self.resource = Some(resource);
        self.events.append(events);

        if self.events.len() >= self.max_events {
            return self.take().map(|flushed| (flushed, FlushReason::MaxEvents));
        }
        if self.current_weight() >= self.max_bytes {
            return self.take().map(|flushed| (flushed, FlushReason::MaxBytes));
        }
        None
    }

    /// Everything held, if anything -- the interval and shutdown paths, which don't go through
    /// `absorb`'s bound checks. `None` when nothing has been absorbed since the last `take`.
    pub fn take(&mut self) -> Option<EventBatch> {
        let resource = self.resource.take()?;
        let events = std::mem::take(&mut self.events);
        Some(EventBatch { resource, events })
    }

    /// See `absorb`'s doc comment. Zero-copy: swaps this accumulator's own `resource`/`events`
    /// into a throwaway `EventBatch` just long enough to call the one authoritative estimator,
    /// then swaps them back -- no clone of the event data, only an `Arc` refcount bump for
    /// `resource`.
    fn current_weight(&mut self) -> u64 {
        let resource = self.resource.clone().expect("only called once self.resource is Some");
        let events = std::mem::take(&mut self.events);
        let probe = EventBatch { resource, events };
        let weight = probe.estimated_heap_bytes();
        self.events = probe.events;
        weight
    }

    /// The next point on `deadline`'s interval cadence, reusing `run_transform`'s own
    /// constant-time cadence math (`crate::runtime::advance_flush_deadline`) rather than a second
    /// copy of it. `logit-inputs`' decode loop races this against its queue read, exactly the
    /// shape `run_transform` already uses for a stateful transform's flush timer.
    pub fn next_deadline(
        deadline: tokio::time::Instant,
        now: tokio::time::Instant,
        interval: Duration,
    ) -> tokio::time::Instant {
        crate::runtime::advance_flush_deadline(deadline, now, interval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Event, Value};

    fn resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn events(count: usize) -> Vec<Event> {
        (0..count).map(|_| Event::empty(0, AttrMap::new())).collect()
    }

    fn heavy_events(extra_bytes: usize) -> Vec<Event> {
        let mut attrs = AttrMap::new();
        attrs.insert("payload", Value::str("x".repeat(extra_bytes)));
        vec![Event::empty(0, attrs)]
    }

    #[test]
    fn max_events_1_emits_once_per_absorbed_batch_and_never_splits_a_multi_event_decode() {
        let mut acc = BatchAccumulator::new(1, u64::MAX);
        let r = resource();

        // One datagram decoding to 40 events must emit exactly one batch of 40, not 40 batches --
        // the bound governs *when* to stop accumulating, never how to subdivide one absorb call.
        let (flushed, reason) =
            acc.absorb(Arc::clone(&r), &mut events(40)).expect("should flush immediately");
        assert_eq!(flushed.events.len(), 40);
        assert_eq!(reason, FlushReason::MaxEvents);
        assert!(acc.is_empty());
    }

    #[test]
    fn reaching_the_bound_exactly_and_exceeding_it_both_emit() {
        let r = resource();

        let mut exact = BatchAccumulator::new(3, u64::MAX);
        assert!(exact.absorb(Arc::clone(&r), &mut events(2)).is_none());
        let (flushed, reason) =
            exact.absorb(Arc::clone(&r), &mut events(1)).expect("reaching 3 should flush");
        assert_eq!(flushed.events.len(), 3);
        assert_eq!(reason, FlushReason::MaxEvents);

        let mut exceeding = BatchAccumulator::new(3, u64::MAX);
        assert!(exceeding.absorb(Arc::clone(&r), &mut events(2)).is_none());
        let (flushed, reason) =
            exceeding.absorb(Arc::clone(&r), &mut events(5)).expect("exceeding 3 should flush");
        assert_eq!(flushed.events.len(), 7);
        assert_eq!(reason, FlushReason::MaxEvents);
    }

    #[test]
    fn the_byte_bound_trips_independently_of_the_count_bound() {
        let mut acc = BatchAccumulator::new(1_000_000, 1);
        let r = resource();
        let (flushed, reason) = acc
            .absorb(Arc::clone(&r), &mut heavy_events(64))
            .expect("a nonzero-weight batch should flush");
        assert_eq!(reason, FlushReason::MaxBytes);
        assert_eq!(flushed.events.len(), 1);
    }

    #[test]
    fn take_on_an_empty_accumulator_is_none() {
        let mut acc = BatchAccumulator::new(10, u64::MAX);
        assert!(acc.take().is_none());
    }

    #[test]
    fn an_empty_incoming_batch_is_a_no_op() {
        let mut acc = BatchAccumulator::new(1, u64::MAX);
        let r = resource();
        assert!(acc.absorb(r, &mut Vec::new()).is_none());
        assert!(acc.is_empty());
    }

    #[test]
    fn a_resource_ptr_eq_mismatch_flushes_the_old_batch_before_starting_a_new_accumulation() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let r1 = resource();
        let r2 = resource(); // a distinct Arc, not ptr_eq to r1 even if `Resource` derives Eq

        assert!(acc.absorb(Arc::clone(&r1), &mut events(1)).is_none());
        let (flushed, reason) = acc
            .absorb(Arc::clone(&r2), &mut events(1))
            .expect("a resource change should flush the old batch");
        assert_eq!(reason, FlushReason::ResourceChange);
        assert!(
            Arc::ptr_eq(&flushed.resource, &r1),
            "the flushed batch must carry the OLD resource"
        );
        assert_eq!(flushed.events.len(), 1);

        // The accumulator now holds the new resource's batch, not yet flushed.
        assert!(!acc.is_empty());
        let remaining = acc.take().expect("should hold the new accumulation");
        assert!(Arc::ptr_eq(&remaining.resource, &r2));
        assert_eq!(remaining.events.len(), 1);
    }

    #[test]
    fn two_batches_sharing_one_arc_resource_merge_into_one_batch_holding_that_same_arc() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let r = resource();
        assert!(acc.absorb(Arc::clone(&r), &mut events(1)).is_none());
        assert!(acc.absorb(Arc::clone(&r), &mut events(1)).is_none());
        let merged = acc.take().expect("should hold both");
        assert_eq!(merged.events.len(), 2);
        assert!(Arc::ptr_eq(&merged.resource, &r), "must be the exact same Arc, not an equal one");
    }

    /// The property the whole `&mut Vec<Event>` signature exists for: `absorb` must leave the
    /// caller's buffer empty but with its capacity intact (via `Vec::append`, not
    /// `std::mem::take`), so a caller reusing one scratch buffer across many `decode_into` calls
    /// actually gets to reuse it.
    #[test]
    fn absorb_drains_the_callers_buffer_via_append_leaving_its_capacity_intact() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let mut scratch = Vec::with_capacity(64);
        let warm_capacity = scratch.capacity();
        scratch.extend(events(1));

        assert!(acc.absorb(resource(), &mut scratch).is_none());

        assert!(scratch.is_empty(), "the caller's buffer must be drained");
        assert_eq!(
            scratch.capacity(),
            warm_capacity,
            "the caller's buffer must keep its allocated capacity, not be replaced with a fresh Vec"
        );
    }
}
