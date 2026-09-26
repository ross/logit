//! [`BatchAccumulator`]: amortizes many small decoded batches into fewer, larger ones before a
//! [`crate::Fanout::send`], the "datagram->batch assembly" half of
//! `docs/adr/decoupled-listener-io.md`. Transport-agnostic: the UDP, TCP, and tail drivers in
//! `logit-inputs` all use it, and nothing here knows a socket or a concrete
//! [`logit_proto::Decoder`].

use logit_core::{Event, EventBatch, Resource, Scope};
use std::sync::Arc;
use std::time::Duration;

/// Why an accumulated batch was emitted: the `reason` tag on `logit.component.receive.flushed`
/// (`docs/design/internal-telemetry.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    MaxEvents,
    MaxBytes,
    Interval,
    ResourceChange,
    /// The held [`logit_core::Scope`] changed: [`FlushReason::ResourceChange`] for the key's other
    /// term. A distinct tag lets an operator tell why OTLP batches come out small.
    ScopeChange,
    Shutdown,
    /// One tracked file or connection is ending and its accumulator flushes on the way out: a
    /// tailed file rotated away, removed, or drained past EOF (`logit_inputs::tail`), or one TCP
    /// connection (`logit_inputs::tcp`) reaching EOF, reset by its client, failing framing, or
    /// closed idle (`docs/adr/idle-connection-timeout.md`). Distinct from
    /// [`FlushReason::Shutdown`], so a healthy listener doesn't report `reason="shutdown"` every
    /// time a client hangs up.
    Closed,
}

impl FlushReason {
    pub fn as_str(self) -> &'static str {
        match self {
            FlushReason::MaxEvents => "max_events",
            FlushReason::MaxBytes => "max_bytes",
            FlushReason::Interval => "interval",
            FlushReason::ResourceChange => "resource_change",
            FlushReason::ScopeChange => "scope_change",
            FlushReason::Shutdown => "shutdown",
            FlushReason::Closed => "closed",
        }
    }
}

/// Accumulates decoded events until a bound is reached, then hands back everything held as one
/// merged batch. Owns no timer: the caller races its own deadline (see
/// [`BatchAccumulator::next_deadline`]) and calls [`BatchAccumulator::take`] when it fires.
///
/// The `*_weight` fields cache [`EventBatch::estimated_heap_bytes`]'s non-capacity terms
/// incrementally; see [`BatchAccumulator::absorb`] for why the total is exact.
pub struct BatchAccumulator {
    resource: Option<Arc<Resource>>,
    /// The held [`Scope`], part of the accumulation key alongside `resource` (see
    /// [`BatchAccumulator::absorb`]). Carried onto the emitted [`EventBatch`].
    scope: Option<Arc<Scope>>,
    events: Vec<Event>,
    /// [`Resource::estimated_heap_bytes`] of the held resource, recomputed only when it changes.
    resource_weight: u64,
    /// [`Scope::estimated_heap_bytes`] of the held scope, recomputed only when it changes. Zero
    /// when no scope is held.
    scope_weight: u64,
    /// Running sum of [`Event::estimated_heap_bytes`] over every held event; each `absorb` adds
    /// only the incoming slice's contribution.
    events_weight: u64,
    max_events: usize,
    max_bytes: u64,
}

impl BatchAccumulator {
    pub fn new(max_events: usize, max_bytes: u64) -> Self {
        Self {
            resource: None,
            scope: None,
            events: Vec::new(),
            resource_weight: 0,
            scope_weight: 0,
            events_weight: 0,
            max_events,
            max_bytes,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Absorbs `events` under `resource` and `scope`, returning the held batch and why if this
    /// call flushed it.
    ///
    /// Drains `events` with [`Vec::append`], leaving it empty with its capacity intact, so a
    /// caller reusing one scratch buffer across [`logit_proto::Decoder::decode_into`] calls keeps
    /// its allocation. `std::mem::take` would leave a capacity-0 `Vec` (`docs/design/memory.md`
    /// §2).
    ///
    /// Returns `Some` once a bound is reached or exceeded, and never splits a decoded batch:
    /// under `batch_max_events: 1` a datagram decoding to 40 events emits one batch of 40.
    ///
    /// **The resource/scope rule.** The accumulation key is `(resource, scope)`, compared by
    /// `Arc::ptr_eq` (`None`/`Some` is a change; two `None`s are equal). On a change, whatever was
    /// held is flushed first (`ResourceChange` wins over `ScopeChange` if both changed) and
    /// `events` starts a fresh accumulation. Merging across keys would relabel events onto the
    /// wrong resource or scope with well-formed output no test would catch. A decoder therefore
    /// stamps one shared `Arc<Resource>` per stream, or every batch would flush on arrival.
    ///
    /// **Weight tracking is exact.** `EventBatch::estimated_heap_bytes` is the sum of four terms:
    /// resource, scope, `events.capacity() * size_of::<Event>()`, and each event's own. The
    /// resource and scope terms change only with the key, the per-event term is a running sum,
    /// and [`BatchAccumulator::current_weight`] reads capacity live, so the total equals a full
    /// recompute at O(incoming events) per call.
    ///
    /// An empty `events` (a datagram that decoded to nothing) is a no-op: it never changes the
    /// held key and never triggers a flush.
    #[must_use]
    pub fn absorb(
        &mut self,
        resource: Arc<Resource>,
        scope: Option<Arc<Scope>>,
        events: &mut Vec<Event>,
    ) -> Option<(EventBatch, FlushReason)> {
        if events.is_empty() {
            return None;
        }

        let holding_something = self.resource.is_some();
        let resource_changed = match &self.resource {
            Some(held) => !Arc::ptr_eq(held, &resource),
            None => false,
        };
        // With nothing held, the first absorb is never a change, even though `self.scope`
        // starts as `None`.
        let scope_changed = holding_something && !scope_eq(&self.scope, &scope);

        let incoming_weight: u64 = events.iter().map(Event::estimated_heap_bytes).sum();

        if resource_changed || scope_changed {
            // Flush the old key's batch and start a fresh accumulation with the incoming events.
            // They are not bound-checked here, since this call already reports one flush: an
            // incoming batch that alone exceeds a bound flushes on the next `absorb` or the
            // caller's interval, whichever comes first. Nothing is dropped.
            let flushed = self
                .take()
                .expect("resource_changed/scope_changed are only true when something is held");
            self.resource_weight = resource.estimated_heap_bytes();
            self.scope_weight = scope.as_ref().map(|s| s.estimated_heap_bytes()).unwrap_or(0);
            self.resource = Some(resource);
            self.scope = scope;
            self.events.append(events);
            self.events_weight = incoming_weight;
            let reason = if resource_changed {
                FlushReason::ResourceChange
            } else {
                FlushReason::ScopeChange
            };
            return Some((flushed, reason));
        }

        if self.resource.is_none() {
            self.resource_weight = resource.estimated_heap_bytes();
            self.scope_weight = scope.as_ref().map(|s| s.estimated_heap_bytes()).unwrap_or(0);
        }
        self.resource = Some(resource);
        self.scope = scope;
        self.events.append(events);
        self.events_weight += incoming_weight;

        if self.events.len() >= self.max_events {
            return self.take().map(|flushed| (flushed, FlushReason::MaxEvents));
        }
        if self.current_weight() >= self.max_bytes {
            return self.take().map(|flushed| (flushed, FlushReason::MaxBytes));
        }
        None
    }

    /// Takes everything held, for the interval and shutdown paths. `None` when nothing has been
    /// absorbed since the last `take`.
    pub fn take(&mut self) -> Option<EventBatch> {
        let resource = self.resource.take()?;
        let scope = self.scope.take();
        let events = std::mem::take(&mut self.events);
        self.resource_weight = 0;
        self.scope_weight = 0;
        self.events_weight = 0;
        Some(EventBatch { resource, scope, events })
    }

    /// `EventBatch::estimated_heap_bytes` of what is held (see `absorb`): the three cached terms
    /// plus the capacity term, read live because it changes on every `append`.
    fn current_weight(&self) -> u64 {
        self.resource_weight
            + self.scope_weight
            + (self.events.capacity() * std::mem::size_of::<Event>()) as u64
            + self.events_weight
    }

    /// The next point on `deadline`'s interval cadence, using `run_transform`'s flush-timer math
    /// (`crate::runtime::advance_flush_deadline`). A decode loop races it against its queue read.
    pub fn next_deadline(
        deadline: tokio::time::Instant,
        now: tokio::time::Instant,
        interval: Duration,
    ) -> tokio::time::Instant {
        crate::runtime::advance_flush_deadline(deadline, now, interval)
    }
}

/// Two `None`s are equal, `None`/`Some` differ, and two `Some`s compare by `Arc::ptr_eq`, like
/// the resource.
fn scope_eq(a: &Option<Arc<Scope>>, b: &Option<Arc<Scope>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Event, Value};

    fn resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn scope() -> Arc<Scope> {
        Arc::new(Scope::default())
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

        // The bound decides when to stop accumulating, never how to split one absorb call.
        let (flushed, reason) =
            acc.absorb(Arc::clone(&r), None, &mut events(40)).expect("should flush immediately");
        assert_eq!(flushed.events.len(), 40);
        assert_eq!(reason, FlushReason::MaxEvents);
        assert!(acc.is_empty());
    }

    #[test]
    fn reaching_the_bound_exactly_and_exceeding_it_both_emit() {
        let r = resource();

        let mut exact = BatchAccumulator::new(3, u64::MAX);
        assert!(exact.absorb(Arc::clone(&r), None, &mut events(2)).is_none());
        let (flushed, reason) =
            exact.absorb(Arc::clone(&r), None, &mut events(1)).expect("reaching 3 should flush");
        assert_eq!(flushed.events.len(), 3);
        assert_eq!(reason, FlushReason::MaxEvents);

        let mut exceeding = BatchAccumulator::new(3, u64::MAX);
        assert!(exceeding.absorb(Arc::clone(&r), None, &mut events(2)).is_none());
        let (flushed, reason) = exceeding
            .absorb(Arc::clone(&r), None, &mut events(5))
            .expect("exceeding 3 should flush");
        assert_eq!(flushed.events.len(), 7);
        assert_eq!(reason, FlushReason::MaxEvents);
    }

    #[test]
    fn the_byte_bound_trips_independently_of_the_count_bound() {
        let mut acc = BatchAccumulator::new(1_000_000, 1);
        let r = resource();
        let (flushed, reason) = acc
            .absorb(Arc::clone(&r), None, &mut heavy_events(64))
            .expect("a nonzero-weight batch should flush");
        assert_eq!(reason, FlushReason::MaxBytes);
        assert_eq!(flushed.events.len(), 1);
    }

    #[test]
    fn incremental_weight_matches_a_full_recompute_after_many_absorbs() {
        let mut acc = BatchAccumulator::new(usize::MAX, u64::MAX);
        let r = resource();

        // Many absorbs under one resource: a per-absorb resource term would count it repeatedly.
        for i in 0..25 {
            assert!(acc.absorb(Arc::clone(&r), None, &mut heavy_events(i * 7)).is_none());
        }

        let incremental = acc.current_weight();

        // Move the same `Vec` into an `EventBatch`, not a clone: a clone's capacity would differ.
        let resource = acc.resource.clone().expect("absorbed at least one non-empty batch");
        let events = std::mem::take(&mut acc.events);
        let probe = EventBatch { resource, scope: None, events };
        let authoritative = probe.estimated_heap_bytes();
        acc.events = probe.events;

        assert_eq!(incremental, authoritative);
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
        assert!(acc.absorb(r, None, &mut Vec::new()).is_none());
        assert!(acc.is_empty());
    }

    #[test]
    fn a_resource_ptr_eq_mismatch_flushes_the_old_batch_before_starting_a_new_accumulation() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let r1 = resource();
        let r2 = resource(); // a distinct Arc, not ptr_eq to r1 even if `Resource` derives Eq

        assert!(acc.absorb(Arc::clone(&r1), None, &mut events(1)).is_none());
        let (flushed, reason) = acc
            .absorb(Arc::clone(&r2), None, &mut events(1))
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
        assert!(acc.absorb(Arc::clone(&r), None, &mut events(1)).is_none());
        assert!(acc.absorb(Arc::clone(&r), None, &mut events(1)).is_none());
        let merged = acc.take().expect("should hold both");
        assert_eq!(merged.events.len(), 2);
        assert!(Arc::ptr_eq(&merged.resource, &r), "must be the exact same Arc, not an equal one");
    }

    /// `absorb` leaves the caller's scratch buffer empty with its capacity intact.
    #[test]
    fn absorb_drains_the_callers_buffer_via_append_leaving_its_capacity_intact() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let mut scratch = Vec::with_capacity(64);
        let warm_capacity = scratch.capacity();
        scratch.extend(events(1));

        assert!(acc.absorb(resource(), None, &mut scratch).is_none());

        assert!(scratch.is_empty(), "the caller's buffer must be drained");
        assert_eq!(
            scratch.capacity(),
            warm_capacity,
            "the caller's buffer must keep its allocated capacity, not be replaced with a fresh Vec"
        );
    }

    #[test]
    fn a_scope_ptr_eq_mismatch_flushes_the_old_batch_before_starting_a_new_accumulation() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let r = resource();
        let s1 = scope();
        let s2 = scope(); // a distinct Arc, not ptr_eq to s1 even if `Scope` derives Eq

        assert!(acc.absorb(Arc::clone(&r), Some(Arc::clone(&s1)), &mut events(1)).is_none());
        let (flushed, reason) = acc
            .absorb(Arc::clone(&r), Some(Arc::clone(&s2)), &mut events(1))
            .expect("a scope change should flush the old batch");
        assert_eq!(reason, FlushReason::ScopeChange);
        assert!(
            Arc::ptr_eq(flushed.scope.as_ref().expect("old batch carried a scope"), &s1),
            "the flushed batch must carry the OLD scope"
        );
        assert_eq!(flushed.events.len(), 1);

        // The accumulator now holds the new scope's batch, not yet flushed.
        assert!(!acc.is_empty());
        let remaining = acc.take().expect("should hold the new accumulation");
        assert!(Arc::ptr_eq(remaining.scope.as_ref().expect("new batch carries a scope"), &s2));
        assert_eq!(remaining.events.len(), 1);
    }

    /// `None` -> `Some` is also a change, same as `Some` -> a different `Some`.
    #[test]
    fn a_scope_going_from_none_to_some_also_flushes() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let r = resource();
        let s = scope();

        assert!(acc.absorb(Arc::clone(&r), None, &mut events(1)).is_none());
        let (flushed, reason) = acc
            .absorb(Arc::clone(&r), Some(Arc::clone(&s)), &mut events(1))
            .expect("a scope appearing should flush the old batch");
        assert_eq!(reason, FlushReason::ScopeChange);
        assert!(flushed.scope.is_none(), "the flushed batch must carry the OLD (absent) scope");
    }

    #[test]
    fn two_batches_sharing_one_arc_scope_merge_without_flushing_and_carry_that_same_arc() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let r = resource();
        let s = scope();
        assert!(acc.absorb(Arc::clone(&r), Some(Arc::clone(&s)), &mut events(1)).is_none());
        assert!(acc.absorb(Arc::clone(&r), Some(Arc::clone(&s)), &mut events(1)).is_none());
        let merged = acc.take().expect("should hold both");
        assert_eq!(merged.events.len(), 2);
        assert!(
            Arc::ptr_eq(merged.scope.as_ref().expect("carries the shared scope"), &s),
            "must be the exact same Arc, not an equal one"
        );
    }

    /// Two `None`s never count as a scope change.
    #[test]
    fn two_batches_with_no_scope_merge_without_flushing() {
        let mut acc = BatchAccumulator::new(1_000, u64::MAX);
        let r = resource();
        assert!(acc.absorb(Arc::clone(&r), None, &mut events(1)).is_none());
        assert!(acc.absorb(Arc::clone(&r), None, &mut events(1)).is_none());
        let merged = acc.take().expect("should hold both");
        assert_eq!(merged.events.len(), 2);
        assert!(merged.scope.is_none());
    }

    #[test]
    fn current_weight_includes_the_scope_term() {
        let r = resource();
        let s = Arc::new(Scope {
            name: bytes::Bytes::from_static(b"a reasonably long scope name for the estimator"),
            ..Default::default()
        });

        let mut with_scope = BatchAccumulator::new(1_000, u64::MAX);
        assert!(with_scope.absorb(Arc::clone(&r), Some(s), &mut events(1)).is_none());

        let mut without_scope = BatchAccumulator::new(1_000, u64::MAX);
        assert!(without_scope.absorb(Arc::clone(&r), None, &mut events(1)).is_none());

        assert!(
            with_scope.current_weight() > without_scope.current_weight(),
            "a populated scope must add weight"
        );
    }
}
