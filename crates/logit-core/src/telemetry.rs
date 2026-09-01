//! Component-level self-telemetry: what a component (and the runtime, on its behalf) records
//! about its own behavior. See `docs/design/internal-telemetry.md` and
//! `docs/adr/0018-internal-telemetry-as-pipeline-events.md`.
//!
//! Mirrors [`crate::diag`]'s shape deliberately -- [`Telemetry::default`] is a disabled, no-op
//! handle, so a component that never receives a live one keeps working with zero added cost: no
//! allocation, no clock read, one predictable branch per call. A handle only does anything once a
//! config's `internal` component asks for a live [`Registry`] (`crates/logit-inputs/src/
//! internal.rs`) and every component is built with a handle to it.
//!
//! **Not a scrape target, and not a second aggregation model.** A point is coalesced with any
//! later point sharing its `(name, tags)` only to avoid flooding the pipeline with one-point
//! events between drains -- [`Registry::drain`] emits whatever accumulated since the last call,
//! using exactly the merges `logit-transforms::Aggregator` already performs on real events (sum
//! for counts, last-write-wins for gauges, sketch merge for timings). That's what lets a real
//! `aggregate` component attached downstream extend this to any actual time window *correctly*,
//! because the merges compose -- see the module doc on why this can't take statsd clients'
//! "batch raw samples, let the server aggregate" option for timings: `logit`'s `MetricKind` has no
//! raw-sample representation, only mergeable ones.

use crate::interner::intern;
use crate::{AttrMap, DdSketch, Event, MetricKind, MetricRecord};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A tag on a point. Both halves are `&'static str` by convention, not by type-system force --
/// documented here, followed by every shipped component: `("class", "5xx")`, never a raw path or
/// peer address. This matters more here than it would elsewhere: the process-wide attribute
/// interner never evicts (`docs/known-gaps.md`), so a runtime-derived tag *value* would leak for
/// the life of the process. Cardinality is the tag author's responsibility, same as any other
/// interned key in this codebase.
pub type Tag = (&'static str, &'static str);

/// Caps the number of distinct `(name, tags)` keys one component's buffer will hold between
/// drains. Not a volume limit under normal traffic -- repeat points at the same key coalesce, so
/// volume is bounded by distinct keys, not by how often a component calls in -- this exists only
/// to bound a component that ignores the tag-cardinality convention above. Beyond the cap, a new
/// key is dropped and counted (`ComponentBuffer::drain`'s `logit.internal.points.dropped`), never
/// silently grown: the same "bound and count the drop" shape every mature statsd client uses for
/// its own overflow.
const MAX_KEYS_PER_COMPONENT: usize = 1024;

#[derive(Clone, Debug)]
enum Pending {
    Count(f64),
    Gauge(f64),
    Timing(DdSketch),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PointKey {
    name: &'static str,
    /// Sorted so a caller's tag order never creates a spurious second key for what's really the
    /// same point.
    tags: SmallVec<[Tag; 4]>,
}

impl PointKey {
    fn new(name: &'static str, tags: &[Tag]) -> Self {
        let mut tags: SmallVec<[Tag; 4]> = tags.iter().copied().collect();
        tags.sort_unstable();
        Self { name, tags }
    }
}

/// One component's telemetry handle. `Clone` is cheap (an `Arc` bump). [`Telemetry::default`] is
/// the disabled handle every component starts with -- every method on it is a no-op that touches
/// nothing, not even the clock ([`Telemetry::timer`]'s doc comment).
#[derive(Clone, Debug, Default)]
pub struct Telemetry(Option<Arc<ComponentBuffer>>);

impl Telemetry {
    /// Adds `n` to a counter, sum-coalesced with any pending point at the same `(name, tags)`
    /// until the next drain, then emitted as `MetricKind::Counter`.
    pub fn count(&self, name: &'static str, n: f64, tags: &[Tag]) {
        let Some(buf) = &self.0 else { return };
        buf.upsert(
            name,
            tags,
            || Pending::Count(n),
            |p| match p {
                Pending::Count(v) => *v += n,
                other => *other = Pending::Count(n),
            },
        );
    }

    /// Sets a gauge, last-write-wins against any pending point at the same `(name, tags)` until
    /// the next drain, then emitted as `MetricKind::Gauge`.
    pub fn gauge(&self, name: &'static str, v: f64, tags: &[Tag]) {
        let Some(buf) = &self.0 else { return };
        buf.upsert(name, tags, || Pending::Gauge(v), |p| *p = Pending::Gauge(v));
    }

    /// Records one duration sample, merged into one `DdSketch` with any pending point at the same
    /// `(name, tags)` until the next drain, then emitted as `MetricKind::Distribution`.
    pub fn timing(&self, name: &'static str, d: Duration, tags: &[Tag]) {
        let Some(buf) = &self.0 else { return };
        let secs = d.as_secs_f64();
        buf.upsert(
            name,
            tags,
            || {
                let mut sketch = DdSketch::new();
                sketch.add(secs);
                Pending::Timing(sketch)
            },
            |p| match p {
                Pending::Timing(sketch) => sketch.add(secs),
                other => {
                    let mut sketch = DdSketch::new();
                    sketch.add(secs);
                    *other = Pending::Timing(sketch);
                }
            },
        );
    }

    /// A guard that records one `timing` sample for `name` when dropped (or via
    /// [`Timer::stop`]). Reads the clock only when this handle is live -- a disabled handle's
    /// timer never calls `Instant::now()` at all, so timing a hot path costs nothing when nobody
    /// asked for telemetry.
    pub fn timer(&self, name: &'static str) -> Timer {
        Timer { telemetry: self.clone(), name, start: self.0.as_ref().map(|_| Instant::now()) }
    }

    /// Whether this handle is live. Lets a caller skip building tags/values for a call it would
    /// otherwise make unconditionally, when that work isn't already free.
    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }
}

/// See [`Telemetry::timer`].
#[must_use = "a Timer records nothing until it is dropped or stopped"]
pub struct Timer {
    telemetry: Telemetry,
    name: &'static str,
    start: Option<Instant>,
}

impl Timer {
    /// Records the elapsed time now, under `tags`, instead of waiting for `Drop` (which always
    /// records under no tags -- use this when the tags aren't known until the timed work
    /// finishes, e.g. an HTTP response's status class).
    pub fn stop(mut self, tags: &[Tag]) {
        if let Some(start) = self.start.take() {
            self.telemetry.timing(self.name, start.elapsed(), tags);
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            self.telemetry.timing(self.name, start.elapsed(), &[]);
        }
    }
}

/// One component's buffer: every point it has recorded since the last [`Registry::drain`],
/// keyed and coalesced by `(name, tags)`. Not exported -- reached only through [`Telemetry`]
/// (write side) and [`Registry`] (drain side).
#[derive(Debug)]
pub struct ComponentBuffer {
    id: String,
    kind: &'static str,
    role: &'static str,
    points: Mutex<HashMap<PointKey, Pending>>,
    /// Distinct keys rejected by the [`MAX_KEYS_PER_COMPONENT`] cap since the last drain.
    dropped: AtomicU64,
}

impl ComponentBuffer {
    fn new(id: String, kind: &'static str, role: &'static str) -> Self {
        Self { id, kind, role, points: Mutex::new(HashMap::new()), dropped: AtomicU64::new(0) }
    }

    fn upsert(
        &self,
        name: &'static str,
        tags: &[Tag],
        initial: impl FnOnce() -> Pending,
        update: impl FnOnce(&mut Pending),
    ) {
        let key = PointKey::new(name, tags);
        // Never held across an `.await` -- every call site is a synchronous emit, not a
        // long-lived guard, so a `std::sync::Mutex` (no extra dependency) is the right tool here.
        let mut points = self.points.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = points.get_mut(&key) {
            update(existing);
            return;
        }
        if points.len() >= MAX_KEYS_PER_COMPONENT {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        points.insert(key, initial());
    }

    fn base_attrs(&self) -> AttrMap {
        let mut attrs = AttrMap::new();
        attrs.insert("component", self.id.as_str());
        attrs.insert("kind", self.kind);
        attrs.insert("role", self.role);
        attrs
    }

    /// Takes every point buffered since the last call, emitting one [`Event`] per `(name, tags)`
    /// key, stamped `now`, plus a `logit.internal.points.dropped` counter naming this component
    /// (`reason = "cardinality"`) if the cap above rejected any new key meanwhile -- self-
    /// telemetry reporting its own losses, the same convention every mature statsd client follows
    /// for its own send failures.
    fn drain(&self, now: i64) -> Vec<Event> {
        let points = {
            let mut points = self.points.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *points)
        };
        let dropped = self.dropped.swap(0, Ordering::Relaxed);

        let mut events = Vec::with_capacity(points.len() + usize::from(dropped > 0));
        for (key, pending) in points {
            let mut attrs = self.base_attrs();
            for (k, v) in &key.tags {
                attrs.insert(k, *v);
            }
            let kind = match pending {
                Pending::Count(v) => MetricKind::Counter(v),
                Pending::Gauge(v) => MetricKind::Gauge(v),
                Pending::Timing(sketch) => MetricKind::Distribution(sketch),
            };
            events.push(Event::metric(
                now,
                attrs,
                MetricRecord { name: intern(key.name), kind, unit: None },
            ));
        }
        if dropped > 0 {
            let mut attrs = self.base_attrs();
            attrs.insert("reason", "cardinality");
            events.push(Event::metric(
                now,
                attrs,
                MetricRecord {
                    name: intern("logit.internal.points.dropped"),
                    kind: MetricKind::Counter(dropped as f64),
                    unit: None,
                },
            ));
        }
        events
    }
}

/// The process-wide set of every component's [`ComponentBuffer`]. Built once, per run, only when
/// a config's `internal` component exists (`crates/logit-cli/src/pipeline.rs`); every component is
/// then handed a [`Telemetry`] from [`Registry::telemetry_for`], and the `internal` component
/// itself drains it on its own configured interval. No config with an `internal` component means
/// no `Registry` is ever built, and every handle stays [`Telemetry::default`] -- see this crate's
/// `telemetry` module doc for what that guarantees.
#[derive(Default)]
pub struct Registry {
    buffers: Mutex<Vec<Arc<ComponentBuffer>>>,
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Registers a new buffer for component `id` and returns a live handle to it. `kind`/`role`
    /// are stamped onto every point this handle ever records (`component`/`kind`/`role`
    /// attributes) -- pass the config `type` tag and the arity role, both known once at
    /// construction, never per call.
    pub fn telemetry_for(&self, id: &str, kind: &'static str, role: &'static str) -> Telemetry {
        let buf = Arc::new(ComponentBuffer::new(id.to_string(), kind, role));
        self.buffers.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(buf.clone());
        Telemetry(Some(buf))
    }

    /// Drains every registered component's buffer, in registration order, into one flat list of
    /// events. Registration order is deterministic (`crates/logit-cli/src/pipeline.rs` builds
    /// components in sorted-id order), so drain output is reproducible across runs, which matters
    /// for tests more than for production use.
    pub fn drain(&self, now: i64) -> Vec<Event> {
        // Cloning the `Vec<Arc<_>>` (cheap: one refcount bump per component) rather than holding
        // the registry lock while draining each buffer -- draining calls into each buffer's own
        // lock, and nothing here needs the registry's own lock held that long.
        let buffers = self.buffers.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        buffers.iter().flat_map(|buf| buf.drain(now)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[Tag]) -> Vec<Tag> {
        pairs.to_vec()
    }

    #[test]
    fn a_disabled_handle_records_nothing_and_never_reads_the_clock() {
        let telemetry = Telemetry::default();
        assert!(!telemetry.is_enabled());
        telemetry.count("x", 1.0, &[]);
        telemetry.gauge("x", 1.0, &[]);
        telemetry.timing("x", Duration::from_secs(1), &[]);
        // `timer()` on a disabled handle must not call `Instant::now()` -- there is nothing to
        // assert about a clock read directly, so this asserts the observable consequence: the
        // guard's `start` is `None`, proven by dropping it recording nothing (no panic, and
        // nothing to drain, since a disabled handle has no buffer at all).
        drop(telemetry.timer("y"));
    }

    #[test]
    fn counts_at_the_same_key_sum_between_drains() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        telemetry.count("logit.input.datagrams", 1.0, &[]);
        telemetry.count("logit.input.datagrams", 2.0, &[]);

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Counter(v) => assert_eq!(*v, 3.0),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn gauges_at_the_same_key_are_last_write_wins() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("internal", "internal", "listener");
        telemetry.gauge("logit.process.interner.strings", 10.0, &[]);
        telemetry.gauge("logit.process.interner.strings", 42.0, &[]);

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Gauge(v) => assert_eq!(*v, 42.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn timings_at_the_same_key_merge_into_one_sketch() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("influxdb_out", "influxdb_out", "sink");
        telemetry.timing("logit.output.request.duration", Duration::from_millis(10), &[]);
        telemetry.timing("logit.output.request.duration", Duration::from_millis(20), &[]);

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Distribution(sketch) => assert_eq!(sketch.count(), 2),
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn distinct_tag_sets_are_distinct_points() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("influxdb_out", "influxdb_out", "sink");
        telemetry.count("logit.output.requests", 1.0, &tags(&[("class", "2xx")]));
        telemetry.count("logit.output.requests", 1.0, &tags(&[("class", "5xx")]));

        let events = registry.drain(0);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn tag_order_does_not_create_a_second_key() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("x", "x", "transform");
        telemetry.count("m", 1.0, &[("a", "1"), ("b", "2")]);
        telemetry.count("m", 1.0, &[("b", "2"), ("a", "1")]);

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Counter(v) => assert_eq!(*v, 2.0),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn a_drain_takes_every_point_leaving_the_buffer_empty() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("x", "x", "transform");
        telemetry.count("m", 1.0, &[]);

        assert_eq!(registry.drain(0).len(), 1);
        assert_eq!(registry.drain(0).len(), 0, "a second drain with nothing new should be empty");
    }

    #[test]
    fn every_point_is_stamped_with_component_kind_and_role() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("my_id", "statsd_in", "listener");
        telemetry.count("m", 1.0, &[]);

        let events = registry.drain(0);
        let attrs = &events[0].attributes;
        assert_eq!(attrs.get("component").and_then(|v| v.as_str()), Some("my_id"));
        assert_eq!(attrs.get("kind").and_then(|v| v.as_str()), Some("statsd_in"));
        assert_eq!(attrs.get("role").and_then(|v| v.as_str()), Some("listener"));
    }

    #[test]
    fn a_key_beyond_the_cardinality_cap_is_dropped_and_counted_not_grown() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("noisy", "lua", "transform");
        for i in 0..MAX_KEYS_PER_COMPONENT + 5 {
            // Leak the formatted tag value as `'static` for this test only -- production tags are
            // always genuinely `'static` (compile-time constants); this is the cheapest way to
            // manufacture that many distinct *test* keys without changing the API's shape.
            let value: &'static str = Box::leak(i.to_string().into_boxed_str());
            telemetry.count("m", 1.0, &[("i", value)]);
        }

        let events = registry.drain(0);
        assert_eq!(events.len(), MAX_KEYS_PER_COMPONENT + 1, "the cap plus one drop counter");
        let dropped = events
            .iter()
            .find(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("cardinality"))
            .expect("a cardinality-drop counter event should be present");
        match &dropped.metrics[0].kind {
            MetricKind::Counter(v) => assert_eq!(*v, 5.0),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn a_timer_records_on_drop() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("x", "x", "transform");
        {
            let _timer = telemetry.timer("logit.component.process.duration");
            std::thread::sleep(Duration::from_millis(1));
        }
        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Distribution(sketch) => assert!(sketch.count() >= 1),
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn a_timer_stopped_early_with_tags_records_those_tags() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("x", "influxdb_out", "sink");
        let timer = telemetry.timer("logit.output.request.duration");
        timer.stop(&[("class", "2xx")]);

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attributes.get("class").and_then(|v| v.as_str()), Some("2xx"));
    }

    #[test]
    fn registering_two_components_and_draining_returns_both() {
        let registry = Registry::new();
        let a = registry.telemetry_for("a", "statsd_in", "listener");
        let b = registry.telemetry_for("b", "influxdb_out", "sink");
        a.count("m", 1.0, &[]);
        b.count("m", 1.0, &[]);

        assert_eq!(registry.drain(0).len(), 2);
    }
}
