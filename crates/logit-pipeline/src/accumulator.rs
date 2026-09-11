//! [`BatchAccumulator`]: amortizes many small decoded batches into fewer, larger ones before a
//! [`crate::Fanout::send`] -- the "datagram->batch assembly" half of
//! `docs/adr/decoupled-listener-io.md`. Transport-agnostic and socket-free by design: a UDP
//! listener's decode loop (`logit-inputs`) is the only caller today, but nothing here mentions a
//! socket, a datagram, or any concrete [`logit_proto::Decoder`].

use logit_core::{Event, EventBatch, Resource, Scope};
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
    /// The held [`logit_core::Scope`] changed -- same trigger as [`FlushReason::ResourceChange`],
    /// just for the batch's other identity term (`BatchAccumulator` keys on `(resource, scope)`).
    /// Kept distinct rather than folded into `ResourceChange` since the enum is cheap to extend and
    /// a distinct `logit.component.receive.flushed` reason tag is more useful for operators
    /// diagnosing unexpectedly small OTLP batches than an undifferentiated one.
    ScopeChange,
    Shutdown,
    /// A tailed file is being closed (rotated away, removed, or drained past EOF) --
    /// `logit_inputs::tail`'s only caller. Distinct from `Shutdown`: this fires while the
    /// listener keeps running, for one file among several it may be tracking, not for the whole
    /// component's own shutdown.
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

/// Accumulates decoded events until one of three bounds is reached, then hands back everything
/// held as one merged batch. Owns no clock and no timer of its own -- the caller races its own
/// deadline (see [`BatchAccumulator::next_deadline`]) and calls [`BatchAccumulator::take`] when it
/// fires; this type only tracks the held events and their resource.
///
/// `resource_weight`/`events_weight` cache [`EventBatch::estimated_heap_bytes`]'s two per-batch,
/// non-capacity terms incrementally -- see [`BatchAccumulator::absorb`]'s doc comment for why this
/// is exact, not approximate, and costs O(incoming events) per call rather than O(everything held
/// so far).
pub struct BatchAccumulator {
    resource: Option<Arc<Resource>>,
    /// The held [`Scope`], if any -- part of this accumulator's key alongside `resource` (see
    /// [`BatchAccumulator::absorb`]'s doc comment). Carried onto the emitted [`EventBatch`].
    scope: Option<Arc<Scope>>,
    events: Vec<Event>,
    /// [`Resource::estimated_heap_bytes`] of the held resource -- recomputed only when the
    /// resource changes (rare, and O(that resource's own attributes) regardless), not per absorb.
    resource_weight: u64,
    /// [`Scope::estimated_heap_bytes`] of the held scope -- recomputed only when the scope
    /// changes, mirroring `resource_weight`. Zero when no scope is held.
    scope_weight: u64,
    /// The running sum of [`Event::estimated_heap_bytes`] over every event currently held --
    /// updated by adding just the incoming slice's contribution each `absorb`, never by re-walking
    /// events already accounted for.
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

    /// Absorbs `events` under `resource`, appending via [`Vec::append`] rather than taking `events`
    /// by value. This is what makes the whole exercise pay off: `events` comes straight from a
    /// [`logit_proto::Decoder::decode_into`] call against a buffer the caller reuses across
    /// datagrams, and `Vec::append` drains `events` into this accumulator's own buffer while
    /// leaving `events` empty **with its allocated capacity intact** -- unlike `std::mem::take`,
    /// which would replace it with a fresh, capacity-0 `Vec` and silently undo the whole point of
    /// reusing a scratch buffer across calls (`docs/design/memory.md` §2; see also
    /// `docs/adr/decoupled-listener-io.md`'s allocation accounting).
    ///
    /// Returns `Some` once a bound is *reached or exceeded* -- never splits a decoded batch, which
    /// is what makes `batch_max_events: 1` mean "one send per datagram": every non-empty decode
    /// immediately reaches the bound, so a single datagram decoding to 40 events still emits one
    /// batch of 40, not 40 batches -- the bound governs when to stop accumulating, never how to
    /// subdivide one decode's output.
    ///
    /// **The resource/scope rule.** An accumulated batch carries one `Arc<Resource>` and at most
    /// one `Arc<Scope>` -- the accumulator's key is the pair `(resource, scope)`. If `resource` is
    /// not `Arc::ptr_eq` to whatever this accumulator already holds, or `scope` is not equivalent
    /// to whatever is already held (`Arc::ptr_eq` when both are `Some`; `None`/`Some` always
    /// counts as a change; two `None`s are equal), whatever was held is flushed first
    /// (`FlushReason::ResourceChange` or `FlushReason::ScopeChange` respectively -- resource wins
    /// if both changed at once) and `events` starts a fresh accumulation -- merging across
    /// distinct resources or scopes would silently relabel events onto the wrong one, a
    /// correctness bug no test would catch since the output stays well-formed. Every decoder
    /// shipped today (`StatsdDecoder`, `SyslogDecoder`) constructs one `Arc::new(Resource::
    /// default())` per decoder instance and stamps every decoded batch with it, and none produce a
    /// `Scope`, so in practice neither comparison trips -- they exist to make that assumption
    /// load-bearing rather than latent, the same *n*-to-1 hazard
    /// `docs/adr/aggregation-window-semantics.md` already documents for a Lua component's
    /// `flush()`.
    ///
    /// **Why weight tracking here is exact, not approximate, despite being incremental.**
    /// `EventBatch::estimated_heap_bytes` is `resource.estimated_heap_bytes() +
    /// scope.estimated_heap_bytes() + events.capacity() * size_of::<Event>() +
    /// events.iter().map(Event::estimated_heap_bytes).sum()` -- four terms, each cheap to
    /// reproduce without re-walking events already accounted for: the resource term only changes
    /// when the resource does (`resource_weight`, updated on the rare `ResourceChange` path
    /// below), the scope term only when the scope does (`scope_weight`, same pattern); the
    /// per-event term is a plain running sum, so adding just the incoming slice's contribution
    /// (`events_weight`) reproduces the same total a full walk would; and the capacity term is
    /// read live off `self.events.capacity()` in [`BatchAccumulator::current_weight`] --
    /// `Vec::capacity` is O(1), so nothing needs to track it. The four added together equal
    /// `estimated_heap_bytes` exactly, by construction, not approximately -- this isn't trading
    /// accuracy for speed, the original per-call recomputation was simply doing O(everything
    /// held) of work to answer a question four O(1)/O(incoming) updates already answer.
    ///
    /// An empty `events` (a datagram that decoded to nothing) is absorbed as a no-op: it never
    /// changes the held resource/scope and never triggers a flush on its own.
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
        // Only a real change once something is actually held -- the very first absorb ever (no
        // resource held yet) must never count as a "change" just because `self.scope` starts as
        // `None`, the same reasoning `resource_changed` already gets for free from matching on
        // `self.resource` above.
        let scope_changed = holding_something && !scope_eq(&self.scope, &scope);

        let incoming_weight: u64 = events.iter().map(Event::estimated_heap_bytes).sum();

        if resource_changed || scope_changed {
            // Flush whatever was held under the old resource/scope, then start a fresh
            // accumulation with the incoming events -- they are NOT dropped, only deferred to a
            // later `take()`/`absorb()`. Not also bound-checked against `max_events`/`max_bytes`
            // here: this call already reports one flush; a lone incoming batch that happens to
            // also exceed a bound on its own gets flushed on the very next `absorb` or by the
            // caller's interval timer, whichever comes first -- accepted staleness of at most one
            // absorb, not a correctness gap (nothing is ever dropped).
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

    /// Everything held, if anything -- the interval and shutdown paths, which don't go through
    /// `absorb`'s bound checks. `None` when nothing has been absorbed since the last `take`.
    pub fn take(&mut self) -> Option<EventBatch> {
        let resource = self.resource.take()?;
        let scope = self.scope.take();
        let events = std::mem::take(&mut self.events);
        self.resource_weight = 0;
        self.scope_weight = 0;
        self.events_weight = 0;
        Some(EventBatch { resource, scope, events })
    }

    /// See `absorb`'s doc comment: `resource_weight`, `scope_weight`, and `events_weight` are the
    /// three non-capacity terms of `EventBatch::estimated_heap_bytes`, maintained incrementally;
    /// only the capacity term is read live here, since `Vec::capacity` is O(1) and changes with
    /// every `append` in a way not worth shadowing in a separate field.
    fn current_weight(&self) -> u64 {
        self.resource_weight
            + self.scope_weight
            + (self.events.capacity() * std::mem::size_of::<Event>()) as u64
            + self.events_weight
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

/// `None`/`Some` always counts as different; two `Some`s compare by `Arc::ptr_eq` (not by value --
/// the same reasoning as the resource comparison above); two `None`s are equal.
fn scope_eq(a: &Option<Arc<Scope>>, b: &Option<Arc<Scope>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

#[cfg(test)]
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

        // One datagram decoding to 40 events must emit exactly one batch of 40, not 40 batches --
        // the bound governs *when* to stop accumulating, never how to subdivide one absorb call.
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

        // Absorb varying-weight slices across many calls under one shared resource -- the shape
        // a real decode loop produces -- to prove `current_weight`'s incrementally-tracked total
        // never drifts from a full, from-scratch recompute of the merged batch. This is exactly
        // the case a naive per-call recomputation of the resource's own contribution would get
        // wrong (double-, triple-, ...-counting it once per absorb instead of once per batch).
        for i in 0..25 {
            assert!(acc.absorb(Arc::clone(&r), None, &mut heavy_events(i * 7)).is_none());
        }

        let incremental = acc.current_weight();

        // Recompute authoritatively: swap the accumulator's own resource/events into a real
        // `EventBatch` -- the same `Vec`, not a clone (which would reset capacity to length and
        // invalidate the comparison's capacity-driven term) -- and ask the one formula both are
        // supposed to agree with, then swap them back so `acc` is left unchanged.
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
