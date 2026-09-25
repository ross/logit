//! Component-level self-telemetry: what a component (and the runtime, on its behalf) records
//! about its own behavior. See `docs/design/internal-telemetry.md` and
//! `docs/adr/internal-telemetry-as-pipeline-events.md`.
//!
//! Like [`crate::diag`], [`Telemetry::default`] is a disabled handle: no allocation, no clock
//! read, one predictable branch per call. A handle is live only when a config has an `internal`
//! component, which gets a [`Registry`] that hands every component its handle.
//!
//! **Not a scrape target, and not a second aggregation model.** Points sharing a `(name, tags)`
//! coalesce between drains only to avoid flooding the pipeline, using the merges
//! `logit-transforms::Aggregator` performs (sum for counts, last write for gauges, sketch merge for
//! timings). Those merges compose, so a downstream `aggregate` extends them to any window
//! correctly. Timings are sketched eagerly rather than kept as [`crate::MetricKind::Samples`]
//! because no `aggregate` is guaranteed to run over internal telemetry.

use crate::interner::intern;
use crate::{
    AttrMap, BodyFormat, DdSketch, Event, LogRecord, MetricKind, MetricRecord, Severity, SpanKind,
    SpanLink, SpanRecord, SpanStatus, Value,
};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A tag on a point. Both halves are `&'static str` by convention, not by type:
/// `("class", "5xx")`, never a raw path or peer address. The process-wide interner never evicts
/// (`docs/known-gaps.md`), so a runtime-derived tag value would leak for the life of the process.
pub type Tag = (&'static str, &'static str);

/// Caps distinct `(name, tags)` keys per component buffer between drains.
///
/// Repeat points coalesce, so this bounds only a component that ignores the [`Tag`] convention.
/// A new key past the cap is dropped and counted as
/// `logit.internal.points.dropped{reason="cardinality"}`.
const MAX_KEYS_PER_COMPONENT: usize = 1024;

/// Tag keys reserved for a point's component identity (`ComponentBuffer::base_attrs`), filtered
/// out of a point's key in [`PointKey::new`].
///
/// Overwriting identity at drain time alone would stop spoofing, but `("kind", "a")` and
/// `("kind", "b")` would still occupy two slots and drain as two indistinguishable points.
/// Filtering before the key is built is what keeps them coalescing.
const RESERVED_TAG_KEYS: [&str; 3] = ["component", "kind", "role"];

/// Whether `key` is reserved for a point's identity (see [`RESERVED_TAG_KEYS`]). Public so the Lua
/// binding (`crates/logit-script/src/telemetry.rs`) can reject one with a clear error instead of
/// having it filtered silently.
pub fn is_reserved_tag_key(key: &str) -> bool {
    RESERVED_TAG_KEYS.contains(&key)
}

/// Caps spans per component buffer between drains: a volume bound, since spans never coalesce.
/// A span past the cap is dropped and counted as
/// `logit.internal.spans.dropped{reason="buffer_full"}`.
const MAX_SPANS_PER_COMPONENT: usize = 512;

/// Caps logs per component buffer between drains: a volume bound, since logs never coalesce. A log
/// past the cap is dropped and counted as `logit.internal.logs.dropped{reason="buffer_full"}`.
const MAX_LOGS_PER_COMPONENT: usize = 256;

/// Caps [`SpanLink`]s per span, so a flush absorbing many batches can't grow one span without
/// limit (the same reason as `aggregate`'s `MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`). A link past the
/// cap is counted immediately as `logit.internal.span.links.dropped{reason="cardinality"}`.
const MAX_LINKS_PER_SPAN: usize = 32;

/// The `span_sample_rate` an `internal` component gets when it doesn't set one.
///
/// Below `1.0` because spans don't coalesce: one per node visit per batch would multiply internal
/// telemetry's volume in a way points never do. See
/// `docs/adr/internal-span-emission-and-deterministic-sampling.md`.
pub const DEFAULT_SPAN_SAMPLE_RATE: f64 = 0.1;

/// Whether to keep `trace_id`'s spans at `rate`, deterministically, like OTel's
/// `TraceIdRatioBased`.
///
/// Every node, and every `logit` process in a split-collection topology, reaches the same verdict
/// with nothing propagated. The low 8 bytes, big-endian, go straight into
/// [`crate::sampling::keep`] with no hash: these are `logit`'s own random pipeline trace ids,
/// never an application's. The `sample` transform hashes its key instead, so the two reach
/// different verdicts for the same 16 bytes (`docs/adr/consistent-sampling-component.md`).
pub fn trace_is_sampled(trace_id: &[u8; 16], rate: f64) -> bool {
    let x = u64::from_be_bytes(trace_id[8..16].try_into().expect("8 bytes"));
    crate::sampling::keep(x, rate)
}

#[derive(Clone, Debug)]
enum Pending {
    Count(f64),
    Gauge(f64),
    Timing(DdSketch),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PointKey {
    name: &'static str,
    /// Sorted, so tag order never makes a second key. Never holds a [`RESERVED_TAG_KEYS`] entry.
    tags: SmallVec<[Tag; 4]>,
}

impl PointKey {
    fn new(name: &'static str, tags: &[Tag]) -> Self {
        let mut tags: SmallVec<[Tag; 4]> =
            tags.iter().copied().filter(|(k, _)| !is_reserved_tag_key(k)).collect();
        tags.sort_unstable();
        Self { name, tags }
    }
}

/// One component's telemetry handle; `Clone` is an `Arc` bump. [`Telemetry::default`] is the
/// disabled handle, on which every method is a no-op that doesn't even read the clock.
#[derive(Clone, Debug, Default)]
pub struct Telemetry(Option<Arc<ComponentBuffer>>);

impl Telemetry {
    /// Adds `n` to a counter, summed per `(name, tags)` until the next drain and emitted as
    /// [`MetricKind::counter`].
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

    /// Sets a gauge, last write wins per `(name, tags)` until the next drain.
    pub fn gauge(&self, name: &'static str, v: f64, tags: &[Tag]) {
        let Some(buf) = &self.0 else { return };
        buf.upsert(name, tags, || Pending::Gauge(v), |p| *p = Pending::Gauge(v));
    }

    /// Records one duration in seconds, sketched per `(name, tags)` until the next drain and
    /// emitted as `MetricKind::Distribution`.
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

    /// A guard that records one `timing` sample for `name` when dropped (or via [`Timer::stop`]).
    /// A disabled handle's timer never reads the clock, so timing a hot path is free.
    pub fn timer(&self, name: &'static str) -> Timer {
        Timer { telemetry: self.clone(), name, start: self.0.as_ref().map(|_| Instant::now()) }
    }

    /// Whether this handle is live, so a caller can skip building tags or values that aren't free.
    pub fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    /// Opens a span for this component's visit to one unit of work (per node kind, see
    /// `docs/adr/internal-span-emission-and-deterministic-sampling.md`).
    ///
    /// Sampling ([`trace_is_sampled`]) is decided here from `trace_id` alone. An unsampled or
    /// disabled span gets a stateless guard whose methods are no-ops: no allocation, no clock
    /// read.
    ///
    /// The caller supplies `span_id`/`parent_span_id` from the `TraceContext` it already minted
    /// and sends downstream; minting another id here would desynchronize the two.
    pub fn span(
        &self,
        op: &'static str,
        kind: SpanKind,
        trace_id: [u8; 16],
        span_id: [u8; 8],
        parent_span_id: Option<[u8; 8]>,
    ) -> SpanGuard {
        let Some(buf) = &self.0 else { return SpanGuard::disabled() };
        if !trace_is_sampled(&trace_id, buf.span_sample_rate) {
            return SpanGuard::disabled();
        }
        SpanGuard {
            telemetry: Telemetry(Some(buf.clone())),
            span: Some(PendingSpan {
                start: now_unix_nanos(),
                started_at: Instant::now(),
                end: 0,
                trace_id,
                span_id,
                parent_span_id,
                op,
                kind,
                status: SpanStatus::Ok,
                events: 0,
                links: Vec::new(),
                tags: SmallVec::new(),
            }),
        }
    }
}

/// Wall-clock Unix nanoseconds. A span reads it once, at start, never at finish (see
/// [`PendingSpan::started_at`]).
///
/// The `#[cfg(test)]` override lets a test jump the wall clock mid-span without sleeping; it's
/// test-only so production spans don't pay a thread-local read.
fn now_unix_nanos() -> i64 {
    #[cfg(test)]
    {
        if let Some(overridden) = tests::CLOCK_OVERRIDE.with(|cell| cell.get()) {
            return overridden;
        }
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// See [`Telemetry::timer`].
///
/// **A cancelled `.await` still records a sample.** If the future holding a `Timer` is dropped
/// mid-wait (e.g. `run_input`'s shutdown `select!` winning mid-`Fanout::send`), the sample is
/// time-to-cancellation. Accepted: it's inherent to record-on-`Drop`, it only happens in a
/// shutdown race, and a truncated sample still answers "is it stuck".
#[must_use = "a Timer records nothing until it is dropped or stopped"]
pub struct Timer {
    telemetry: Telemetry,
    name: &'static str,
    start: Option<Instant>,
}

impl Timer {
    /// Records the elapsed time now under `tags`; `Drop` records under none. For tags known only
    /// when the work finishes, e.g. an HTTP status class.
    pub fn stop(mut self, tags: &[Tag]) {
        if let Some(start) = self.start.take() {
            self.telemetry.timing(self.name, start.elapsed(), tags);
        }
    }

    /// Discards this timer without recording a sample, for a wait that turned out not to be the
    /// thing it measures (`Fanout::send_with_deadline` giving up before anything was sent).
    pub fn cancel(mut self) {
        self.start = None;
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            self.telemetry.timing(self.name, start.elapsed(), &[]);
        }
    }
}

/// A span still being built; becomes an `Event` carrying a `SpanRecord` only at drain time.
#[derive(Debug)]
struct PendingSpan {
    /// Unix nanoseconds; the drained `Event::timestamp`, not the drain time.
    start: i64,
    /// Monotonic reading taken with `start`; `end` is `start + started_at.elapsed()`.
    ///
    /// A second wall-clock read at finish could land before `start` if the system clock steps
    /// backward mid-span (NTP, an admin `date`), plausible over a long retrying send. `Instant`
    /// never goes backward, so `end >= start` by construction.
    started_at: Instant,
    /// Unix nanoseconds, set at finish; `0` until then, but a span is only pushed once it's set.
    end: i64,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    /// `"process"|"flush"|"send"|"deliver"`; drains as the span name after the component's
    /// `kind` (`"aggregate process"`).
    op: &'static str,
    kind: SpanKind,
    status: SpanStatus,
    /// Events this visit emitted; `0` for a transform that absorbed everything.
    events: u64,
    links: Vec<SpanLink>,
    /// Extra attributes from [`SpanGuard::tag`]. Not filtered against [`RESERVED_TAG_KEYS`]: only
    /// this project's own Rust code records spans, never script input.
    tags: SmallVec<[Tag; 2]>,
}

/// One `tracing` event captured by [`TelemetryLayer`]; becomes an `Event` carrying a
/// [`LogRecord`] only at drain time.
#[derive(Debug)]
struct PendingLog {
    /// Unix nanoseconds at capture; the drained `Event::timestamp`, not the drain time.
    ts: i64,
    level: Severity,
    /// The event's `key` field, or a placeholder (see `TelemetryLayer::on_event`).
    key: String,
    message: String,
}

/// A guard opened by [`Telemetry::span`] that records one span when it finishes or drops. When
/// disabled or unsampled it holds no state and every method returns immediately.
#[must_use = "a SpanGuard records nothing until it is dropped or finished"]
pub struct SpanGuard {
    /// A live handle to the buffer this span drains into, so [`SpanGuard::link`] can count an
    /// over-cap drop through `Telemetry::count`.
    telemetry: Telemetry,
    span: Option<PendingSpan>,
}

impl SpanGuard {
    fn disabled() -> Self {
        SpanGuard { telemetry: Telemetry::default(), span: None }
    }

    /// Sets the emitted-events count, drained as the `events` attribute. Overwrites rather than
    /// adds: call it once with the final count.
    pub fn events(&mut self, n: u64) {
        if let Some(span) = &mut self.span {
            span.events = n;
        }
    }

    /// Attaches one contributing-context link, dropped and counted
    /// (`logit.internal.span.links.dropped{reason="cardinality"}`) past [`MAX_LINKS_PER_SPAN`].
    pub fn link(&mut self, link: SpanLink) {
        let Some(span) = &mut self.span else { return };
        if span.links.len() >= MAX_LINKS_PER_SPAN {
            self.telemetry.count(
                "logit.internal.span.links.dropped",
                1.0,
                &[("reason", "cardinality")],
            );
            return;
        }
        span.links.push(link);
    }

    /// [`SpanGuard::link`] for each of `links`; each counts against the cap individually.
    pub fn links(&mut self, links: impl IntoIterator<Item = SpanLink>) {
        for link in links {
            self.link(link);
        }
    }

    /// Attaches an extra attribute, e.g. `("fault", "ambiguous")` on a failed delivery.
    pub fn tag(&mut self, k: &'static str, v: &'static str) {
        if let Some(span) = &mut self.span {
            span.tags.push((k, v));
        }
    }

    /// Marks this span's status `Error`, e.g. a sink's `deliver_with_retry` giving up. A span that
    /// never calls this drains as `Ok`, not `Unset`: completing without an error is a success.
    pub fn error(&mut self) {
        if let Some(span) = &mut self.span {
            span.status = SpanStatus::Error;
        }
    }

    /// Marks this span's status `Ok`, the default (see [`SpanGuard::error`]).
    pub fn ok(&mut self) {
        if let Some(span) = &mut self.span {
            span.status = SpanStatus::Ok;
        }
    }

    /// Finishes this span now rather than at `Drop`.
    pub fn finish(mut self) {
        self.finish_inner();
    }

    /// Discards this span without recording it, for a visit that turned out not to happen
    /// (`Fanout::send_with_deadline` giving up before anything was sent).
    pub fn cancel(mut self) {
        self.span = None;
    }

    fn finish_inner(&mut self) {
        let Some(mut span) = self.span.take() else { return };
        let Some(buf) = self.telemetry.0.as_ref() else { return };
        // Never a second wall-clock read (see `PendingSpan::started_at`). Saturates rather than
        // wrapping negative past ~292 years.
        let elapsed_nanos = span.started_at.elapsed().as_nanos().min(i64::MAX as u128) as i64;
        span.end = span.start.saturating_add(elapsed_nanos);
        buf.push_span(span);
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        self.finish_inner();
    }
}

/// One component's points (coalesced by `(name, tags)`), spans, and logs since the last
/// [`Registry::drain`]. Reached only through [`Telemetry`] (write side) and [`Registry`] (drain
/// side).
#[derive(Debug)]
pub struct ComponentBuffer {
    id: String,
    kind: &'static str,
    role: &'static str,
    points: Mutex<HashMap<PointKey, Pending>>,
    /// Distinct keys rejected by the [`MAX_KEYS_PER_COMPONENT`] cap since the last drain.
    dropped: AtomicU64,
    /// Finished spans since the last drain; unkeyed, since spans never coalesce.
    spans: Mutex<Vec<PendingSpan>>,
    /// Spans rejected by the [`MAX_SPANS_PER_COMPONENT`] cap since the last drain.
    spans_dropped: AtomicU64,
    /// Logs [`TelemetryLayer`] captured for this component since the last drain; unkeyed.
    logs: Mutex<Vec<PendingLog>>,
    /// Logs rejected by the [`MAX_LOGS_PER_COMPONENT`] cap since the last drain.
    logs_dropped: AtomicU64,
    /// Copied from [`Registry`] at construction so [`Telemetry::span`] takes no registry lock.
    span_sample_rate: f64,
}

impl ComponentBuffer {
    fn new(id: String, kind: &'static str, role: &'static str, span_sample_rate: f64) -> Self {
        Self {
            id,
            kind,
            role,
            points: Mutex::new(HashMap::new()),
            dropped: AtomicU64::new(0),
            spans: Mutex::new(Vec::new()),
            spans_dropped: AtomicU64::new(0),
            logs: Mutex::new(Vec::new()),
            logs_dropped: AtomicU64::new(0),
            span_sample_rate,
        }
    }

    /// Pushes `span`, or counts it dropped past [`MAX_SPANS_PER_COMPONENT`].
    fn push_span(&self, span: PendingSpan) {
        let mut spans = self.spans.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if spans.len() >= MAX_SPANS_PER_COMPONENT {
            drop(spans);
            self.spans_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        spans.push(span);
    }

    /// Pushes `log`, or counts it dropped past [`MAX_LOGS_PER_COMPONENT`].
    fn push_log(&self, log: PendingLog) {
        let mut logs = self.logs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if logs.len() >= MAX_LOGS_PER_COMPONENT {
            drop(logs);
            self.logs_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        logs.push(log);
    }

    /// Turns one finished [`PendingSpan`] into its drained `Event`. The name `String` is built
    /// here, off the hot path, only for a sampled span that reached a drain.
    fn span_event(&self, span: PendingSpan) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in &span.tags {
            attrs.insert(k, *v);
        }
        attrs.insert("logit.node.op", span.op);
        attrs.insert("events", span.events as i64);
        attrs.insert("component", self.id.as_str());
        attrs.insert("kind", self.kind);
        attrs.insert("role", self.role);
        let record = SpanRecord {
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            name: Value::str(format!("{} {}", self.kind, span.op)),
            kind: span.kind,
            status: span.status,
            events: Vec::new(),
            links: span.links,
            end_timestamp: span.end,
            flags: 0,
            ext: None,
        };
        Event::span(span.start, attrs, record)
    }

    fn upsert(
        &self,
        name: &'static str,
        tags: &[Tag],
        initial: impl FnOnce() -> Pending,
        update: impl FnOnce(&mut Pending),
    ) {
        let key = PointKey::new(name, tags);
        // Never held across an `.await`, so a `std::sync::Mutex` suffices.
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

    /// Takes everything buffered since the last call: one [`Event`] per point key, span, and log,
    /// plus a drop counter for each cap that rejected anything.
    ///
    /// Points and counters are stamped `now`. **A span is stamped with its own `start` and a log
    /// with its capture time, never `now`**: `Event::timestamp` is a span's start
    /// (`SpanRecord`'s doc), and the drain time would make both look later by however long they
    /// sat here.
    fn drain(&self, now: i64) -> Vec<Event> {
        let points = {
            let mut points = self.points.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *points)
        };
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        let spans = {
            let mut spans = self.spans.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *spans)
        };
        let spans_dropped = self.spans_dropped.swap(0, Ordering::Relaxed);
        let logs = {
            let mut logs = self.logs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *logs)
        };
        let logs_dropped = self.logs_dropped.swap(0, Ordering::Relaxed);

        let mut events = Vec::with_capacity(
            points.len()
                + spans.len()
                + logs.len()
                + usize::from(dropped > 0)
                + usize::from(spans_dropped > 0)
                + usize::from(logs_dropped > 0),
        );
        for (key, pending) in points {
            // `PointKey::new` already filtered reserved keys; inserting identity last is defense
            // in depth, since `AttrMap::insert` overwrites on collision.
            let mut attrs = AttrMap::new();
            for (k, v) in &key.tags {
                attrs.insert(k, *v);
            }
            attrs.insert("component", self.id.as_str());
            attrs.insert("kind", self.kind);
            attrs.insert("role", self.role);
            let kind = match pending {
                Pending::Count(v) => MetricKind::counter(v),
                Pending::Gauge(v) => MetricKind::Gauge(v),
                Pending::Timing(sketch) => MetricKind::Distribution(sketch),
            };
            events.push(Event::metric(now, attrs, MetricRecord::new(intern(key.name), kind)));
        }
        if dropped > 0 {
            let mut attrs = self.base_attrs();
            attrs.insert("reason", "cardinality");
            events.push(Event::metric(
                now,
                attrs,
                MetricRecord::new(
                    intern("logit.internal.points.dropped"),
                    MetricKind::counter(dropped as f64),
                ),
            ));
        }
        for span in spans {
            events.push(self.span_event(span));
        }
        if spans_dropped > 0 {
            let mut attrs = self.base_attrs();
            attrs.insert("reason", "buffer_full");
            events.push(Event::metric(
                now,
                attrs,
                MetricRecord::new(
                    intern("logit.internal.spans.dropped"),
                    MetricKind::counter(spans_dropped as f64),
                ),
            ));
        }
        for log in logs {
            let mut attrs = self.base_attrs();
            attrs.insert("key", log.key.as_str());
            events.push(Event::log(
                log.ts,
                attrs,
                LogRecord {
                    message: Value::str(log.message),
                    severity: Some(log.level),
                    body_format: BodyFormat::Raw,
                    trace: None,
                    event_name: None,
                    observed_timestamp: 0,
                    dropped_attributes_count: 0,
                },
            ));
        }
        if logs_dropped > 0 {
            let mut attrs = self.base_attrs();
            attrs.insert("reason", "buffer_full");
            events.push(Event::metric(
                now,
                attrs,
                MetricRecord::new(
                    intern("logit.internal.logs.dropped"),
                    MetricKind::counter(logs_dropped as f64),
                ),
            ));
        }
        events
    }
}

/// The process-wide set of component buffers, built once per run only when the config has an
/// `internal` component. Every component gets its handle from [`Registry::telemetry_for`], and the
/// `internal` component drains it on its interval.
pub struct Registry {
    buffers: Mutex<Vec<Arc<ComponentBuffer>>>,
    /// Stamped onto every [`ComponentBuffer`] this registry creates.
    span_sample_rate: f64,
}

impl Registry {
    /// [`Registry::with_span_sampling`] at [`DEFAULT_SPAN_SAMPLE_RATE`].
    pub fn new() -> Arc<Self> {
        Self::with_span_sampling(DEFAULT_SPAN_SAMPLE_RATE)
    }

    /// A registry whose buffers sample spans at `rate` (`0.0..=1.0`, [`trace_is_sampled`]).
    /// Process-wide: graph rule 13 allows at most one `internal` component, so there's one rate.
    pub fn with_span_sampling(rate: f64) -> Arc<Self> {
        Arc::new(Self { buffers: Mutex::new(Vec::new()), span_sample_rate: rate })
    }

    /// Registers a buffer for component `id` and returns a live handle to it. `kind` (the config
    /// `type`) and `role` are stamped onto everything the handle records.
    ///
    /// A repeat `id` gets the same buffer, keeping its first `kind`/`role`: two buffers with one
    /// `component` attribute would drain as two racing copies of what looks like one component.
    pub fn telemetry_for(&self, id: &str, kind: &'static str, role: &'static str) -> Telemetry {
        let mut buffers = self.buffers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = buffers.iter().find(|buf| buf.id == id) {
            return Telemetry(Some(existing.clone()));
        }
        let buf = Arc::new(ComponentBuffer::new(id.to_string(), kind, role, self.span_sample_rate));
        buffers.push(buf.clone());
        Telemetry(Some(buf))
    }

    /// Pushes `log` into `component_id`'s buffer, for `TelemetryLayer::on_event`. A no-op for an
    /// unregistered id, which shouldn't happen: every component, `internal` included, registers
    /// at startup.
    fn push_log(&self, component_id: &str, log: PendingLog) {
        let buffers = self.buffers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(buf) = buffers.iter().find(|buf| buf.id == component_id) {
            buf.push_log(log);
        }
    }

    /// Drains every buffer, in registration order (sorted by id, so reproducible), into one list.
    pub fn drain(&self, now: i64) -> Vec<Event> {
        // Clone the `Arc`s rather than hold the registry lock across each buffer's drain.
        let buffers = self.buffers.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        buffers.iter().flat_map(|buf| buf.drain(now)).collect()
    }
}

/// A `tracing_subscriber::Layer` that captures `logit`-targeted events at or above its threshold
/// into the component buffers as log events. See `docs/design/internal-telemetry.md`'s "Logs"
/// section.
///
/// Starts **inactive**: every event is a no-op until [`TelemetryLayer::activate`].
/// `logit-cli::main` installs it in the global subscriber before the config loads (so a bad
/// `--log-level` fails fast and `starting` logs even for a config that won't resolve), and there's
/// no stable API to add a layer after `.init()`. A config with no `internal` component, or
/// `logs: off`, never activates it, which costs nothing, like [`Telemetry::default`].
#[derive(Clone, Default)]
pub struct TelemetryLayer(Arc<std::sync::RwLock<Option<ActiveTelemetryLayer>>>);

struct ActiveTelemetryLayer {
    registry: Arc<Registry>,
    threshold: Severity,
    /// The `internal` component's id (not its kind; `self` in `demo/logit.yaml`), where an event
    /// with no `component` field lands.
    internal_id: String,
}

impl TelemetryLayer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts capturing into `registry` at `threshold`, once the config's `internal` component and
    /// its `logs:` setting are known. Events before this call are not captured.
    pub fn activate(
        &self,
        registry: Arc<Registry>,
        threshold: Severity,
        internal_id: impl Into<String>,
    ) {
        let mut inner = self.0.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        *inner =
            Some(ActiveTelemetryLayer { registry, threshold, internal_id: internal_id.into() });
    }

    /// The per-layer filter this layer must be installed with (`Layer::with_filter`), with the
    /// `--log-level`/`LOGIT_LOG` `EnvFilter` scoped the same way onto the stderr layer, so stderr
    /// verbosity and capture stay independent.
    ///
    /// As a global filter, `--log-level error` would return `Interest::never()` for every `warn`
    /// callsite, which `tracing-core` caches for the life of the process: `on_event` would never
    /// run, `logs: warn` would silently become `error`, and no drop counter could see it.
    ///
    /// `WARN` is a static cap, not the configured threshold, because the layer exists before the
    /// config loads. That's sound while `logit_config::InternalLogs` offers only
    /// `warn`/`error`/`off`; `on_event`'s threshold check is the real gate. A level below `warn`
    /// must lower this cap. It's a cap rather than no filter because an unfiltered layer reports
    /// no `max_level_hint`, raising the static max level to `TRACE` for every dependency.
    pub fn capture_filter() -> tracing_subscriber::filter::Targets {
        tracing_subscriber::filter::Targets::new()
            .with_target("logit", tracing_subscriber::filter::LevelFilter::WARN)
    }
}

fn severity_from_level(level: tracing::Level) -> Severity {
    match level {
        tracing::Level::TRACE => Severity::Trace,
        tracing::Level::DEBUG => Severity::Debug,
        tracing::Level::INFO => Severity::Info,
        tracing::Level::WARN => Severity::Warn,
        tracing::Level::ERROR => Severity::Error,
    }
}

impl<S> tracing_subscriber::Layer<S> for TelemetryLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let inner = self.0.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(inner) = &*inner else { return }; // not yet activated, or never will be
        if event.metadata().target() != "logit" {
            // Not `logit`'s own diagnostics (which set this target explicitly): a dependency's.
            return;
        }
        let level = severity_from_level(*event.metadata().level());
        if level < inner.threshold {
            return;
        }

        // String fields and the message can arrive through `record_debug`, quoted; hence the
        // `trim_matches('"')`.
        struct Visitor {
            component: Option<String>,
            key: Option<String>,
            message: String,
        }
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.record_str(field, &format!("{value:?}"));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                let value = value.trim_matches('"');
                match field.name() {
                    "component" => self.component = Some(value.to_string()),
                    "key" => self.key = Some(value.to_string()),
                    "message" => self.message = value.to_string(),
                    _ => {}
                }
            }
        }
        let mut visitor = Visitor { component: None, key: None, message: String::new() };
        event.record(&mut visitor);

        // No `component` (a lifecycle event like `ready`): the `internal` component, key
        // `"process"`. A `component` with no `key` (`Diagnostics::warn`): key `"log"`.
        let (target_id, key) = match visitor.component {
            Some(component) => (component, visitor.key.unwrap_or_else(|| "log".to_string())),
            None => (inner.internal_id.clone(), "process".to_string()),
        };
        inner.registry.push_log(
            &target_id,
            PendingLog { ts: now_unix_nanos(), level, key, message: visitor.message },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    // `now_unix_nanos`'s wall-clock override.
    thread_local! {
        pub(super) static CLOCK_OVERRIDE: Cell<Option<i64>> = const { Cell::new(None) };
    }

    /// Sets what `now_unix_nanos()` reports on this thread until [`clear_test_clock`].
    fn set_test_clock(nanos: i64) {
        CLOCK_OVERRIDE.with(|cell| cell.set(Some(nanos)));
    }

    fn clear_test_clock() {
        CLOCK_OVERRIDE.with(|cell| cell.set(None));
    }

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
            MetricKind::Sum(s) => assert_eq!(s.value, 3.0),
            other => panic!("expected Sum, got {other:?}"),
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
            MetricKind::Sum(s) => assert_eq!(s.value, 2.0),
            other => panic!("expected Sum, got {other:?}"),
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

    /// A caller-supplied tag can't relabel which component a point is attributed to.
    #[test]
    fn a_tag_named_component_kind_or_role_cannot_override_the_real_identity() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("real_id", "lua", "transform");
        telemetry.count(
            "m",
            1.0,
            &[("component", "spoofed"), ("kind", "spoofed"), ("role", "spoofed")],
        );

        let events = registry.drain(0);
        let attrs = &events[0].attributes;
        assert_eq!(attrs.get("component").and_then(|v| v.as_str()), Some("real_id"));
        assert_eq!(attrs.get("kind").and_then(|v| v.as_str()), Some("lua"));
        assert_eq!(attrs.get("role").and_then(|v| v.as_str()), Some("transform"));
    }

    /// Different values under a reserved tag key coalesce into one point, not two slots.
    #[test]
    fn reserved_tags_with_different_values_still_coalesce_into_one_point() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("real_id", "lua", "transform");
        telemetry.count("m", 1.0, &[("kind", "a")]);
        telemetry.count("m", 2.0, &[("kind", "b")]);

        let events = registry.drain(0);
        assert_eq!(
            events.len(),
            1,
            "both calls should coalesce into one point, not fragment into two"
        );
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => assert_eq!(s.value, 3.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_key_beyond_the_cardinality_cap_is_dropped_and_counted_not_grown() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("noisy", "lua", "transform");
        for i in 0..MAX_KEYS_PER_COMPONENT + 5 {
            // Leaked to satisfy `Tag`'s `'static`; production tags are constants.
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
            MetricKind::Sum(s) => assert_eq!(s.value, 5.0),
            other => panic!("expected Sum, got {other:?}"),
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

    #[test]
    fn a_second_telemetry_for_call_with_the_same_id_reuses_the_first_buffer_not_a_second_one() {
        let registry = Registry::new();
        let first = registry.telemetry_for("dup", "statsd_in", "listener");
        let second = registry.telemetry_for("dup", "influxdb_out", "sink");

        first.count("m", 1.0, &[]);
        second.count("m", 1.0, &[]);

        let events = registry.drain(0);
        assert_eq!(events.len(), 1, "both handles should coalesce into one buffer, not race two");
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => assert_eq!(s.value, 2.0),
            other => panic!("expected Sum, got {other:?}"),
        }
        // The first registration's kind/role wins.
        assert_eq!(events[0].attributes.get("kind").and_then(|v| v.as_str()), Some("statsd_in"));
    }

    /// A `Timer` dropped early (as by a cancelled `.await`) records one sample, per `Timer`'s doc.
    #[test]
    fn a_timer_dropped_early_still_records_exactly_one_sample() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("x", "x", "transform");
        let timer = telemetry.timer("logit.component.send.blocked.duration");
        drop(timer); // simulates a cancelled `.await` dropping its in-flight Timer

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Distribution(sketch) => assert_eq!(sketch.count(), 1),
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn a_cancelled_timer_records_nothing() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("x", "x", "transform");
        telemetry.timer("logit.component.send.blocked.duration").cancel();

        let events = registry.drain(0);
        assert!(events.is_empty(), "a cancelled timer should record no sample, got: {events:?}");
    }

    // -------------------------------------------------------------------------------------------
    // Spans
    // -------------------------------------------------------------------------------------------

    fn trace_id(seed: u8) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[15] = seed;
        id
    }

    fn span_id(seed: u8) -> [u8; 8] {
        let mut id = [0u8; 8];
        id[7] = seed;
        id
    }

    /// Plausible random ids from `RandomState`'s entropy, good enough for a distributional test.
    fn random_trace_ids(n: usize) -> Vec<[u8; 16]> {
        use std::hash::{BuildHasher, Hasher};
        let state = std::collections::hash_map::RandomState::new();
        (0..n)
            .map(|i| {
                let mut high = state.build_hasher();
                high.write_usize(i);
                let mut low = state.build_hasher();
                low.write_usize(i ^ 0xD1B5_4A32_D192_ED03);
                let mut id = [0u8; 16];
                id[..8].copy_from_slice(&high.finish().to_be_bytes());
                id[8..].copy_from_slice(&low.finish().to_be_bytes());
                id
            })
            .collect()
    }

    fn find_span_event(events: &[Event]) -> Option<&Event> {
        events.iter().find(|e| e.span.is_some())
    }

    #[test]
    fn a_disabled_handle_opens_a_span_that_records_nothing_and_never_reads_the_clock() {
        let telemetry = Telemetry::default();
        let mut span = telemetry.span("send", SpanKind::Producer, trace_id(1), span_id(1), None);
        span.events(3);
        span.tag("k", "v");
        span.error();
        drop(span);
    }

    #[test]
    fn a_cancelled_span_records_nothing() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("x", "x", "listener");
        let mut span = telemetry.span("send", SpanKind::Producer, trace_id(1), span_id(1), None);
        span.events(2);
        span.cancel();

        let events = registry.drain(0);
        assert!(
            find_span_event(&events).is_none(),
            "a cancelled span should never be pushed, got: {events:?}"
        );
    }

    #[test]
    fn an_unsampled_trace_builds_no_span_record_at_all() {
        let registry = Registry::with_span_sampling(0.0);
        let telemetry = registry.telemetry_for("x", "x", "transform");
        let mut span = telemetry.span("process", SpanKind::Internal, trace_id(1), span_id(1), None);
        span.events(1);
        drop(span);

        let events = registry.drain(0);
        assert!(
            find_span_event(&events).is_none(),
            "a rate-0.0 registry should never build a span record, got: {events:?}"
        );
    }

    #[test]
    fn the_sampler_gives_the_same_answer_for_the_same_trace_id_every_time() {
        let id = trace_id(7);
        let first = trace_is_sampled(&id, 0.37);
        for _ in 0..100 {
            assert_eq!(trace_is_sampled(&id, 0.37), first);
        }
    }

    /// Pins exact verdicts for fixed ids, so a change in which ids are kept fails here.
    #[test]
    fn the_sampler_reaches_pinned_verdicts_for_fixed_trace_ids() {
        fn id(high: u64, low: u64) -> [u8; 16] {
            let mut id = [0u8; 16];
            id[..8].copy_from_slice(&high.to_be_bytes());
            id[8..].copy_from_slice(&low.to_be_bytes());
            id
        }
        // The rate-0.5 threshold is exactly 2^63 in the low 8 bytes; the high 8 never matter.
        assert!(trace_is_sampled(&id(0, 0x7FFF_FFFF_FFFF_FFFF), 0.5));
        assert!(trace_is_sampled(&id(u64::MAX, 0x7FFF_FFFF_FFFF_FFFF), 0.5));
        assert!(!trace_is_sampled(&id(0, 0x8000_0000_0000_0000), 0.5));
        assert!(!trace_is_sampled(&id(u64::MAX, 0x8000_0000_0000_0000), 0.5));
        // W3C's own example trace id: low 8 bytes 0xa3ce929d0e0e4736, ~0.6399 of the range.
        let w3c = id(0x4bf9_2f35_77b3_4da6, 0xa3ce_929d_0e0e_4736);
        assert!(trace_is_sampled(&w3c, 0.65));
        assert!(!trace_is_sampled(&w3c, 0.63));
        // The low 11 bits are discarded: an id of only those bits sits at 0, kept at the smallest
        // representable threshold (2^-53) and dropped below it, where `rate * 2^53` truncates to 0.
        assert!(trace_is_sampled(&id(0, 0x7FF), 2f64.powi(-53)));
        assert!(!trace_is_sampled(&id(0, 0x7FF), 2f64.powi(-54)));
        // NaN and >= 1 keep; <= 0 drops, even for the all-zero id.
        assert!(trace_is_sampled(&id(0, u64::MAX), f64::NAN));
        assert!(trace_is_sampled(&id(0, u64::MAX), 1.5));
        assert!(!trace_is_sampled(&id(0, 0), 0.0));
        assert!(!trace_is_sampled(&id(0, 0), -1.0));
    }

    #[test]
    fn a_rate_of_one_keeps_every_trace_and_a_rate_of_zero_keeps_none() {
        for id in random_trace_ids(200) {
            assert!(trace_is_sampled(&id, 1.0), "rate 1.0 should keep every trace");
            assert!(!trace_is_sampled(&id, 0.0), "rate 0.0 should keep no trace");
        }
    }

    #[test]
    fn the_sampler_keeps_roughly_the_configured_fraction_of_ten_thousand_random_trace_ids() {
        let ids = random_trace_ids(10_000);
        let rate = 0.25;
        let kept = ids.iter().filter(|id| trace_is_sampled(id, rate)).count();
        let fraction = kept as f64 / ids.len() as f64;
        assert!(
            (fraction - rate).abs() <= 0.03,
            "expected roughly {rate} of 10,000 ids sampled, got {fraction} ({kept} kept)"
        );
    }

    #[test]
    fn a_span_beyond_the_per_component_capacity_is_dropped_and_counted_not_grown() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("noisy", "lua", "transform");
        for i in 0..MAX_SPANS_PER_COMPONENT + 5 {
            let seed = (i % 256) as u8;
            drop(telemetry.span(
                "process",
                SpanKind::Internal,
                trace_id(seed),
                span_id(seed),
                None,
            ));
        }

        let events = registry.drain(0);
        let span_count = events.iter().filter(|e| e.span.is_some()).count();
        assert_eq!(span_count, MAX_SPANS_PER_COMPONENT, "the cap, not one more");
        let dropped = events
            .iter()
            .find(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("buffer_full"))
            .expect("a buffer_full drop counter event should be present");
        match &dropped.metrics[0].kind {
            MetricKind::Sum(s) => assert_eq!(s.value, 5.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_drained_span_event_carries_the_spans_own_start_timestamp_not_the_drain_time() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("x", "x", "transform");
        let before = now_unix_nanos();
        drop(telemetry.span("process", SpanKind::Internal, trace_id(1), span_id(1), None));
        let after = now_unix_nanos();

        let events = registry.drain(1);
        let span_event = find_span_event(&events).expect("a span event should be present");
        assert!(
            span_event.timestamp >= before && span_event.timestamp <= after,
            "expected the span's own start ({before}..={after}), got {}",
            span_event.timestamp
        );
        assert_ne!(span_event.timestamp, 1, "must not be stamped with the drain time");
    }

    /// A backward wall-clock jump mid-span can't make `end_timestamp` precede the start.
    #[test]
    fn a_wall_clock_moving_backward_between_start_and_finish_cannot_make_end_precede_start() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("x", "x", "transform");

        set_test_clock(1_000_000_000); // T0: an arbitrary wall-clock start
        let span = telemetry.span("deliver", SpanKind::Client, trace_id(1), span_id(1), None);
        // Back a full second, far more than the test's own elapsed time.
        set_test_clock(0);
        drop(span);
        clear_test_clock();

        let events = registry.drain(0);
        let span_event = find_span_event(&events).expect("a span event should be present");
        let record = span_event.span.as_ref().expect("span record");
        assert!(
            record.end_timestamp >= span_event.timestamp,
            "end_timestamp ({}) must never precede the span's own start ({}), even across a \
             backward wall-clock jump",
            record.end_timestamp,
            span_event.timestamp
        );
    }

    #[test]
    fn a_drained_span_event_is_stamped_with_component_kind_and_role_like_every_point() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("my_id", "aggregate", "transform");
        drop(telemetry.span("flush", SpanKind::Internal, trace_id(1), span_id(1), None));

        let events = registry.drain(0);
        let span_event = find_span_event(&events).expect("a span event should be present");
        let attrs = &span_event.attributes;
        assert_eq!(attrs.get("component").and_then(|v| v.as_str()), Some("my_id"));
        assert_eq!(attrs.get("kind").and_then(|v| v.as_str()), Some("aggregate"));
        assert_eq!(attrs.get("role").and_then(|v| v.as_str()), Some("transform"));
        assert_eq!(attrs.get("logit.node.op").and_then(|v| v.as_str()), Some("flush"));
    }

    #[test]
    fn links_beyond_the_per_span_cap_are_dropped_and_counted() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("x", "aggregate", "transform");
        let mut span = telemetry.span("flush", SpanKind::Internal, trace_id(1), span_id(1), None);
        for i in 0..MAX_LINKS_PER_SPAN + 3 {
            let seed = (i % 256) as u8;
            span.link(SpanLink {
                trace_id: trace_id(seed),
                span_id: span_id(seed),
                attributes: AttrMap::new(),
                flags: 0,
                trace_state: None,
                dropped_attributes_count: 0,
            });
        }
        drop(span);

        let events = registry.drain(0);
        let span_event = find_span_event(&events).expect("a span event should be present");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(record.links.len(), MAX_LINKS_PER_SPAN, "the cap, not one more");

        let dropped_count: f64 = events
            .iter()
            .filter_map(|e| {
                e.metrics.iter().find_map(|m| {
                    (crate::interner::resolve(m.name) == "logit.internal.span.links.dropped")
                        .then_some(match &m.kind {
                            MetricKind::Sum(s) => s.value,
                            _ => 0.0,
                        })
                })
            })
            .sum();
        assert_eq!(dropped_count, 3.0, "3 links beyond the cap should be dropped and counted");
    }

    #[test]
    fn spans_and_points_drain_together_in_one_call() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("x", "aggregate", "transform");
        telemetry.count("m", 1.0, &[]);
        drop(telemetry.span("process", SpanKind::Internal, trace_id(1), span_id(1), None));

        let events = registry.drain(0);
        assert!(events.iter().any(|e| !e.metrics.is_empty() && e.span.is_none()), "a metric event");
        assert!(find_span_event(&events).is_some(), "a span event");
    }

    // -- `TelemetryLayer` --

    use tracing_subscriber::layer::Layer as _;
    use tracing_subscriber::layer::SubscriberExt;

    fn find_log_event(events: &[Event]) -> Option<&Event> {
        events.iter().find(|e| e.log.is_some())
    }

    #[test]
    fn a_warn_event_with_component_lands_in_that_components_buffer_as_a_log() {
        let registry = Registry::new();
        registry.telemetry_for("x", "json", "transform");
        let layer = TelemetryLayer::new();
        layer.activate(registry.clone(), Severity::Warn, "self");

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target: "logit", component = "x", key = "bad_frame", "malformed input");
        });

        let events = registry.drain(0);
        let event = find_log_event(&events).expect("a log event should be present");
        assert_eq!(event.attributes.get("component").and_then(|v| v.as_str()), Some("x"));
        assert_eq!(event.attributes.get("kind").and_then(|v| v.as_str()), Some("json"));
        assert_eq!(event.attributes.get("role").and_then(|v| v.as_str()), Some("transform"));
        assert_eq!(event.attributes.get("key").and_then(|v| v.as_str()), Some("bad_frame"));
        let record = event.log.as_ref().unwrap();
        assert_eq!(record.severity, Some(Severity::Warn));
        assert_eq!(record.message.as_str(), Some("malformed input"));
        assert_eq!(record.body_format, BodyFormat::Raw);
        assert!(record.trace.is_none());
    }

    #[test]
    fn an_info_event_is_not_captured_below_the_warn_threshold() {
        let registry = Registry::new();
        registry.telemetry_for("x", "json", "transform");
        let layer = TelemetryLayer::new();
        layer.activate(registry.clone(), Severity::Warn, "self");

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "logit", component = "x", "just fyi");
        });

        let events = registry.drain(0);
        assert!(
            find_log_event(&events).is_none(),
            "an info event should not pass a warn threshold"
        );
    }

    #[test]
    fn an_unattributed_error_lands_under_the_internal_component_with_key_process() {
        let registry = Registry::new();
        registry.telemetry_for("self", "internal", "listener");
        let layer = TelemetryLayer::new();
        layer.activate(registry.clone(), Severity::Warn, "self");

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(target: "logit", "something went wrong with no component");
        });

        let events = registry.drain(0);
        let event = find_log_event(&events).expect("a log event should be present");
        assert_eq!(event.attributes.get("component").and_then(|v| v.as_str()), Some("self"));
        assert_eq!(event.attributes.get("key").and_then(|v| v.as_str()), Some("process"));
        assert_eq!(event.log.as_ref().unwrap().severity, Some(Severity::Error));
    }

    #[test]
    fn an_inactive_layer_captures_nothing() {
        let registry = Registry::new();
        registry.telemetry_for("x", "json", "transform");
        let layer = TelemetryLayer::new();

        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(target: "logit", component = "x", "should go nowhere");
        });

        let events = registry.drain(0);
        assert!(find_log_event(&events).is_none(), "an inactive layer must capture nothing");
    }

    #[test]
    fn a_strict_env_filter_does_not_suppress_capture() {
        // A global `EnvFilter` would cache `Interest::never()` for `warn` under `error` (see
        // `capture_filter`); this builds `init_logging`'s per-layer shape.
        let registry = Registry::new();
        registry.telemetry_for("x", "json", "transform");
        let layer = TelemetryLayer::new();
        layer.activate(registry.clone(), Severity::Warn, "self");

        let subscriber = tracing_subscriber::registry()
            .with(layer.with_filter(TelemetryLayer::capture_filter()))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(tracing_subscriber::EnvFilter::new("error")),
            );
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target: "logit", component = "x", key = "bad_frame", "malformed input");
        });

        let events = registry.drain(0);
        let event = find_log_event(&events)
            .expect("a warn must still reach the pipeline under --log-level error");
        let record = event.log.as_ref().unwrap();
        assert_eq!(record.severity, Some(Severity::Warn));
    }

    #[test]
    fn a_log_beyond_the_per_component_capacity_is_dropped_and_counted_not_grown() {
        let registry = Registry::new();
        registry.telemetry_for("noisy", "lua", "transform");
        for i in 0..MAX_LOGS_PER_COMPONENT + 5 {
            registry.push_log(
                "noisy",
                PendingLog {
                    ts: i as i64,
                    level: Severity::Warn,
                    key: "k".to_string(),
                    message: "m".to_string(),
                },
            );
        }

        let events = registry.drain(0);
        let log_count = events.iter().filter(|e| e.log.is_some()).count();
        assert_eq!(log_count, MAX_LOGS_PER_COMPONENT, "the cap, not one more");
        let dropped = events
            .iter()
            .find(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("buffer_full"))
            .expect("a buffer_full drop counter event should be present");
        match &dropped.metrics[0].kind {
            MetricKind::Sum(s) => assert_eq!(s.value, 5.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }
}
