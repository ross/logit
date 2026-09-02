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
use crate::{
    AttrMap, DdSketch, Event, MetricKind, MetricRecord, SpanKind, SpanLink, SpanRecord, SpanStatus,
    Value,
};
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

/// Tag keys reserved for a point's own component identity (`ComponentBuffer::base_attrs`) --
/// never allowed to become part of a point's cardinality key. Without this, two calls tagged
/// `("kind", "a")` and `("kind", "b")` would occupy two distinct keys here (correctly, from
/// `PointKey`'s point of view: they really are different tag sets) but both drain with the *same*
/// real `kind` -- overwriting a caller-supplied `kind` at drain time (below) stops one from
/// spoofing the other's identity, but does nothing about the two of them wasting a slot in the
/// bounded key space each and emitting two externally indistinguishable points instead of one
/// coalesced count. Filtering here, before a key is ever constructed, is what actually restores
/// coalescing -- drain-time overwriting alone only fixes the label, not the accounting.
const RESERVED_TAG_KEYS: [&str; 3] = ["component", "kind", "role"];

/// Whether `key` is reserved for a point's own component identity -- see [`RESERVED_TAG_KEYS`].
/// Public so a caller-facing binding (`crates/logit-script/src/telemetry.rs`) can reject a
/// reserved key with a clear error at the point a script actually used it, rather than only
/// discovering the same filter silently applied once a point reaches this buffer.
pub fn is_reserved_tag_key(key: &str) -> bool {
    RESERVED_TAG_KEYS.contains(&key)
}

/// Caps the number of spans one component's buffer will hold between drains -- a volume bound,
/// not a cardinality one: unlike a point, a span never coalesces with another one (two visits to
/// the same node are two distinct spans, always), so nothing else bounds this except drain
/// interval × sample rate. Beyond the cap, a new span is dropped and counted
/// (`ComponentBuffer::drain`'s `logit.internal.spans.dropped{reason="buffer_full"}`), the same
/// bound-and-count-the-drop shape [`MAX_KEYS_PER_COMPONENT`] uses for points.
const MAX_SPANS_PER_COMPONENT: usize = 512;

/// Caps the number of [`SpanLink`]s one span will carry -- the same reasoning
/// `logit-transforms::Aggregator`'s own `MAX_CONTRIBUTING_CONTEXTS_PER_SERIES` bound has (a
/// flush absorbing an unbounded number of contributing batches shouldn't let one span's own size
/// grow without limit). Beyond the cap, a link is dropped and counted on the guard itself, as
/// `logit.internal.span.links.dropped{reason="cardinality"}` -- immediately, not batched to drain
/// time, since [`SpanGuard`] already holds a live handle back into this same buffer.
const MAX_LINKS_PER_SPAN: usize = 32;

/// The default `span_sample_rate` (`logit_config::ComponentKind::Internal`) when a config's
/// `internal` component doesn't set one. Below `1.0` deliberately: span volume is a different
/// shape than metric volume -- one span per node-visit per batch, where a metric point coalesces
/// between drains -- so keeping everything by default would multiply internal telemetry's own
/// volume in a way metrics never do. See `docs/adr/0022-internal-span-emission-and-deterministic-sampling.md`.
pub const DEFAULT_SPAN_SAMPLE_RATE: f64 = 0.1;

/// Deterministic on `trace_id`, so every node -- and every `logit` process in a split-collection
/// topology (`docs/OVERVIEW.md`) -- reaches the same keep/drop verdict independently, with no
/// propagation and no extra bytes on `TraceContext`/`Delivered`: a kept trace is kept at every
/// hop, a dropped one dropped at every hop, without any node ever telling another its answer.
/// Same shape as OTel's `TraceIdRatioBased` sampler.
///
/// The top 53 bits of the low 8 `trace_id` bytes, not all 64: `rate * 2f64.powi(64)` loses
/// precision near 1.0, which would reject traces it should keep. 53 bits is `f64`'s
/// exact-integer range, so the comparison below is exact, not an approximation of one.
pub fn trace_is_sampled(trace_id: &[u8; 16], rate: f64) -> bool {
    // `!(rate < 1.0)` rather than `rate >= 1.0` -- also catches NaN (every comparison against NaN
    // is false, so `rate < 1.0` is false and this branch is taken): keep everything rather than
    // silently drop everything on a malformed rate. Graph validation (rule 16,
    // `crates/logit-pipeline/src/graph.rs`) is what actually rejects a NaN/out-of-range config
    // value before this is ever called with one in practice; the negated comparison is what makes
    // this fn's own behavior correct even if that guarantee is ever bypassed (a direct caller, a
    // future one), so it's kept as-is rather than rewritten to a `partial_cmp` form that would
    // lose the "NaN falls through to `true`" property clippy's lint can't see is deliberate here.
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    if !(rate < 1.0) {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    let x = u64::from_be_bytes(trace_id[8..16].try_into().expect("8 bytes"));
    (x >> 11) < (rate * (1u64 << 53) as f64) as u64
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
    /// Sorted so a caller's tag order never creates a spurious second key for what's really the
    /// same point. Never contains a [`RESERVED_TAG_KEYS`] entry -- filtered out in [`PointKey::new`]
    /// before this is built, not just overwritten cosmetically later.
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

    /// Opens a span for this component's one visit to one unit of work -- see
    /// `docs/adr/0022-internal-span-emission-and-deterministic-sampling.md` for what "one unit of
    /// work" means per node kind. The sample decision (`trace_is_sampled`) is made here, from
    /// `trace_id` alone, before any span-shaped state exists at all: an unsampled trace gets the
    /// same `SpanGuard::disabled()` a disabled handle's [`Telemetry::timer`] returns, so every
    /// method on it is an immediate no-op and nothing about this call allocates or reads the
    /// clock beyond the one comparison `trace_is_sampled` itself does.
    ///
    /// `span_id`/`parent_span_id` are supplied, not minted here -- the caller (`Fanout::send`,
    /// `run_transform`, ...) already minted the `TraceContext` this span's identity comes from,
    /// because that same context is also what gets sent downstream (`Fanout::send_with_own_context`).
    /// Minting a second, unrelated id here would desynchronize the two.
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

/// The wall-clock Unix-nanosecond "now" -- read exactly once per span, at
/// [`Telemetry::span`]'s own call, never again at finish (see [`PendingSpan::started_at`]'s doc
/// comment for why: a second independent read is what this whole split avoids).
///
/// The `#[cfg(test)]` override below exists purely so a test can simulate a wall clock that jumps
/// (an NTP correction, an admin `date` call) *between* a span's start and its finish, without an
/// actual multi-second sleep -- see `a_wall_clock_moving_backward_between_start_and_finish_cannot_make_end_precede_start`.
/// Never compiled into a non-test binary: the thread-local's own overhead (a branch and a
/// thread-local read) would otherwise be paid on every single span, sampled or not, for a facility
/// only tests use.
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
/// **Recording on `Drop` means a cancelled `.await` still records a sample** -- if the future
/// holding this `Timer` is dropped mid-wait (e.g. `run_input`'s shutdown-racing `tokio::select!`
/// in `crates/logit-pipeline/src/runtime.rs` winning while a listener is mid-`Fanout::send`), the
/// elapsed time reflects time-to-cancellation, not a completed operation. Accepted rather than
/// worked around: it's inherent to the record-on-`Drop` idiom every call site here relies on, the
/// effect is confined to one metric's statistical shape, and it only fires during a shutdown race
/// -- exactly the window an operator is more likely to be watching this for "is it stuck," where a
/// short, truncated sample is a reasonable enough signal anyway.
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

/// One span, still being built -- everything [`Telemetry::span`] captured plus whatever
/// [`SpanGuard`]'s own methods add before it is finished. Turned into a real `Event` carrying a
/// `SpanRecord` only at drain time (`ComponentBuffer::drain`'s span pass), same as a `Pending`
/// point is only turned into a `MetricRecord` there -- this type never leaves this module.
#[derive(Debug)]
struct PendingSpan {
    /// Unix nanoseconds -- becomes the drained `Event::timestamp`, *not* the drain time
    /// (`ComponentBuffer::drain`'s doc comment).
    start: i64,
    /// Captured alongside `start`, from the same [`Telemetry::span`] call -- a monotonic clock
    /// reading `end` is derived from at finish time (`started_at.elapsed()`), instead of a second
    /// independent `SystemTime::now()` read. Two independent wall-clock reads would let the
    /// *system* clock moving backward between them (an NTP correction, an admin `date` call --
    /// plausible over a long span, e.g. a retrying sink send) produce `end < start`, an invalid
    /// duration downstream; `Instant` is guaranteed monotonically non-decreasing on every platform
    /// this project ships to, so `start + elapsed` can never precede `start`, structurally, by
    /// construction -- not merely "usually doesn't."
    started_at: Instant,
    /// Unix nanoseconds, set when the guard finishes (`SpanGuard::finish`/`Drop`) -- becomes
    /// `SpanRecord::end_timestamp`. `0` until then; never observed in that state, since nothing
    /// reads a `PendingSpan` before it's pushed to the buffer, which only ever happens once `end`
    /// has been set.
    end: i64,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    /// `"process"|"flush"|"send"|"deliver"` -- half of the drained span's name (the other half is
    /// this component's own `kind`, joined at drain time: `"aggregate process"`).
    op: &'static str,
    kind: SpanKind,
    status: SpanStatus,
    /// How many events this node's emission carried -- `0` for a transform visit that absorbed
    /// everything. Drained as a non-interned `Value::I64` attribute (`events`), never a tag: a
    /// per-span count has no cardinality to bound.
    events: u64,
    links: Vec<SpanLink>,
    /// Extra `(key, value)` attributes a call site chose to attach (`SpanGuard::tag`, e.g.
    /// `write_loop`'s `("fault", "ambiguous")` on a failed delivery) -- unlike a point's tags,
    /// never filtered against [`RESERVED_TAG_KEYS`]: every call site recording a span is this
    /// project's own Rust code, not untrusted script input, so there's no adversarial caller to
    /// defend a span's identity attributes against the way point tags must be.
    tags: SmallVec<[Tag; 2]>,
}

/// A guard opened by [`Telemetry::span`], recording one [`SpanRecord`]-carrying `Event` when it
/// finishes -- mirrors [`Timer`]'s shape exactly, including the "disabled/unsampled holds no
/// state" trick that makes an unsampled span free: every method below is an immediate return
/// when `span` is `None`.
#[must_use = "a SpanGuard records nothing until it is dropped or finished"]
pub struct SpanGuard {
    /// `Telemetry(Some(_))` back into the same buffer this span will drain into -- reused (rather
    /// than a bare `Arc<ComponentBuffer>`) so [`SpanGuard::link`] can count an over-cap drop via
    /// the ordinary `Telemetry::count` path with no second field.
    telemetry: Telemetry,
    span: Option<PendingSpan>,
}

impl SpanGuard {
    fn disabled() -> Self {
        SpanGuard { telemetry: Telemetry::default(), span: None }
    }

    /// Sets the emitted-events count -- see [`PendingSpan::events`]'s doc comment for what this
    /// means per node kind. Overwrites, rather than adds: every call site calls this at most once,
    /// with the final count for the one emission this span records.
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

    /// [`SpanGuard::link`], for every link in `links` -- each still counted individually against
    /// the per-span cap, not as one all-or-nothing batch.
    pub fn links(&mut self, links: impl IntoIterator<Item = SpanLink>) {
        for link in links {
            self.link(link);
        }
    }

    /// Attaches an extra `(key, value)` attribute, alongside this span's `logit.node.op`/
    /// `events`/identity attributes at drain time -- e.g. `write_loop`'s `("fault", "ambiguous")`
    /// on a failed delivery.
    pub fn tag(&mut self, k: &'static str, v: &'static str) {
        if let Some(span) = &mut self.span {
            span.tags.push((k, v));
        }
    }

    /// Marks this span's status `Error` -- e.g. a sink's `deliver_with_retry` giving up. A span
    /// that never calls this or [`SpanGuard::ok`] drains as `Ok` (`PendingSpan`'s status starts
    /// `Ok`, not `Unset`): a node visit that completes without an explicit error is a success,
    /// the same default every shipped call site relies on.
    pub fn error(&mut self) {
        if let Some(span) = &mut self.span {
            span.status = SpanStatus::Error;
        }
    }

    /// Explicitly marks this span's status `Ok` -- rarely needed (see [`SpanGuard::error`]'s doc
    /// comment on the default), but available so a call site that computes success/failure from a
    /// branch can say so directly rather than relying on "didn't call `error`."
    pub fn ok(&mut self) {
        if let Some(span) = &mut self.span {
            span.status = SpanStatus::Ok;
        }
    }

    /// Finishes this span now, pushing it to its component's buffer -- explicit alternative to
    /// letting [`Drop`] do the same at the end of this guard's scope, for a call site that wants
    /// the finish to happen at a precise point rather than implicitly.
    pub fn finish(mut self) {
        self.finish_inner();
    }

    fn finish_inner(&mut self) {
        let Some(mut span) = self.span.take() else { return };
        let Some(buf) = self.telemetry.0.as_ref() else { return };
        // `start + elapsed`, never a second `now_unix_nanos()` read -- see `PendingSpan::started_at`'s
        // doc comment. `as i64` after `.min(i64::MAX as u128)` is a saturating conversion: a span
        // living longer than ~292 years is never real, but this must not panic or wrap negative on
        // the (impossible in practice) day it happens.
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
    /// Every span recorded (`SpanGuard::finish`/`Drop`) since the last drain -- a plain `Vec`, not
    /// a keyed map like `points`: spans are unique by construction (no two node-visits share a
    /// `span_id`), so there is nothing here to coalesce.
    spans: Mutex<Vec<PendingSpan>>,
    /// Spans rejected by the [`MAX_SPANS_PER_COMPONENT`] cap since the last drain.
    spans_dropped: AtomicU64,
    /// Copied from [`Registry`] at construction (never changes after) so [`Telemetry::span`]
    /// never needs a second lock beyond whichever one this buffer's own state already takes --
    /// process-wide, set once, per graph validation rule 16 guaranteeing at most one `internal`
    /// component (`crates/logit-pipeline/src/graph.rs`).
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
            span_sample_rate,
        }
    }

    /// Pushes `span`, dropping and counting it (`logit.internal.spans.dropped{reason=
    /// "buffer_full"}`, drained alongside the points-side `logit.internal.points.dropped`) past
    /// [`MAX_SPANS_PER_COMPONENT`] rather than growing this buffer without bound.
    fn push_span(&self, span: PendingSpan) {
        let mut spans = self.spans.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if spans.len() >= MAX_SPANS_PER_COMPONENT {
            drop(spans);
            self.spans_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        spans.push(span);
    }

    /// Turns one finished [`PendingSpan`] into its drained `Event`. `name` is built here, not at
    /// `SpanGuard` construction -- the one place this touches a `String`, off the hot path, since
    /// it only happens for a span that both got sampled and survived to a drain.
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

    /// Takes every point and span buffered since the last call, emitting one [`Event`] per
    /// `(name, tags)` point key plus one per finished span, stamped `now` -- **with one
    /// exception: a span event's `Event::timestamp` is the span's own `start`, never `now`.**
    /// `now` is the drain time, not when the work the span records actually happened, and
    /// `SpanRecord`'s own doc comment already makes `Event::timestamp` the span's start; stamping
    /// it with the drain time instead would make every span drift later than reality by however
    /// long it sat in this buffer. Also emits `logit.internal.points.dropped{reason=
    /// "cardinality"}` and `logit.internal.spans.dropped{reason="buffer_full"}` if either cap
    /// rejected anything meanwhile -- self-telemetry reporting its own losses, the same
    /// convention every mature statsd client follows for its own send failures.
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

        let mut events = Vec::with_capacity(
            points.len() + spans.len() + usize::from(dropped > 0) + usize::from(spans_dropped > 0),
        );
        for (key, pending) in points {
            // `key.tags` never holds a reserved key at all (filtered out in `PointKey::new`, so a
            // caller-supplied `kind` tag couldn't fragment cardinality against the real one even
            // before reaching here). Identity is still inserted last, as defense in depth: if that
            // filter were ever bypassed, `AttrMap::insert`'s overwrite-on-collision behavior means
            // identity would still win, so a caller-supplied tag still could not relabel which
            // component a point is attributed to.
            let mut attrs = AttrMap::new();
            for (k, v) in &key.tags {
                attrs.insert(k, *v);
            }
            attrs.insert("component", self.id.as_str());
            attrs.insert("kind", self.kind);
            attrs.insert("role", self.role);
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
        for span in spans {
            events.push(self.span_event(span));
        }
        if spans_dropped > 0 {
            let mut attrs = self.base_attrs();
            attrs.insert("reason", "buffer_full");
            events.push(Event::metric(
                now,
                attrs,
                MetricRecord {
                    name: intern("logit.internal.spans.dropped"),
                    kind: MetricKind::Counter(spans_dropped as f64),
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
pub struct Registry {
    buffers: Mutex<Vec<Arc<ComponentBuffer>>>,
    /// The `span_sample_rate` every [`ComponentBuffer`] this registry creates is stamped with --
    /// see [`Registry::with_span_sampling`].
    span_sample_rate: f64,
}

impl Registry {
    /// Same as [`Registry::with_span_sampling`] at [`DEFAULT_SPAN_SAMPLE_RATE`] -- the rate a
    /// config's `internal` component gets when it doesn't set `span_sample_rate` explicitly.
    pub fn new() -> Arc<Self> {
        Self::with_span_sampling(DEFAULT_SPAN_SAMPLE_RATE)
    }

    /// Builds a registry whose every component buffer samples spans at `rate` (`0.0..=1.0`,
    /// [`trace_is_sampled`]) -- process-wide, since graph validation rule 13
    /// (`crates/logit-pipeline/src/graph.rs`) already guarantees at most one `internal` component
    /// per config, so there is only ever one rate to set. `crates/logit-cli/src/pipeline.rs::prepare`
    /// reads this off the config's `internal` component (`ComponentKind::Internal::span_sample_rate`)
    /// and calls this instead of [`Registry::new`] whenever one exists.
    pub fn with_span_sampling(rate: f64) -> Arc<Self> {
        Arc::new(Self { buffers: Mutex::new(Vec::new()), span_sample_rate: rate })
    }

    /// Registers a new buffer for component `id` and returns a live handle to it. `kind`/`role`
    /// are stamped onto every point this handle ever records (`component`/`kind`/`role`
    /// attributes) -- pass the config `type` tag and the arity role, both known once at
    /// construction, never per call.
    ///
    /// A second call for an `id` already registered returns a handle to the *same* buffer rather
    /// than creating a second, independent one -- every call site today (`logit-cli::pipeline::
    /// prepare`) registers each id exactly once, so this only matters for a caller this crate
    /// doesn't have yet (a hot-reload/reconfiguration path, say); without it, two buffers stamped
    /// with the same `component` attribute would coalesce and drain independently, racing each
    /// other under what looks downstream like one component. `kind`/`role` are taken from
    /// whichever call registered first; a later call's values are ignored, same as they would be
    /// if that caller had simply kept its first handle around instead of asking again.
    pub fn telemetry_for(&self, id: &str, kind: &'static str, role: &'static str) -> Telemetry {
        let mut buffers = self.buffers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = buffers.iter().find(|buf| buf.id == id) {
            return Telemetry(Some(existing.clone()));
        }
        let buf = Arc::new(ComponentBuffer::new(id.to_string(), kind, role, self.span_sample_rate));
        buffers.push(buf.clone());
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
    use std::cell::Cell;

    // `now_unix_nanos`'s test-only wall-clock override -- see that fn's own doc comment.
    // `pub(super)` so `now_unix_nanos` (defined in the parent module) can read it; nothing outside
    // this crate ever sees it, `#[cfg(test)]` on both this module and the read site keeps it out
    // of a non-test binary entirely.
    thread_local! {
        pub(super) static CLOCK_OVERRIDE: Cell<Option<i64>> = const { Cell::new(None) };
    }

    /// Sets the wall clock `now_unix_nanos()` will report on this thread until
    /// [`clear_test_clock`] is called -- thread-local, and `cargo nextest` runs each test in its
    /// own process, so this can't leak into a sibling test.
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

    /// A caller-supplied tag can never relabel which component a point is attributed to -- a real
    /// risk once a tag key is chosen by something less constrained than this codebase's own
    /// `&'static str` call sites (a Lua script, `crates/logit-script/src/telemetry.rs`). Proven
    /// directly against `Telemetry::count`, not just against the Lua binding, since the guarantee
    /// belongs to `ComponentBuffer::drain` regardless of who supplied the tag.
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

    /// The gap overwriting identity at drain time alone left open: two counts tagged with
    /// *different* values under a reserved key must still coalesce into one point, not occupy two
    /// separate (and, once identity is overwritten, externally indistinguishable) cardinality
    /// slots. Proven by checking there is exactly one drained event summing both counts, not just
    /// that the identity attributes come out right.
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
            MetricKind::Counter(v) => assert_eq!(*v, 3.0),
            other => panic!("expected Counter, got {other:?}"),
        }
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
            MetricKind::Counter(v) => assert_eq!(*v, 2.0),
            other => panic!("expected Counter, got {other:?}"),
        }
        // The first registration's kind/role wins -- a later call didn't silently overwrite it.
        assert_eq!(events[0].attributes.get("kind").and_then(|v| v.as_str()), Some("statsd_in"));
    }

    /// Pins the caveat documented on `Timer`: recording on `Drop` means a `Timer` dropped without
    /// ever observing the "real" elapsed time (e.g. because the future holding it was cancelled)
    /// still records *something*, deliberately -- there is no way to distinguish "completed" from
    /// "cancelled" from inside `Drop` alone, and that's accepted, not a bug this test exists to
    /// catch. What it does pin: a `Timer` explicitly dropped early still records exactly one
    /// sample, never zero and never a panic.
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

    /// Test-only trace id generation -- `next_id_bytes` (`crates/logit-pipeline/src/fanout.rs`) is
    /// a sibling crate's private helper, not reachable here, so this reimplements "many plausible,
    /// non-degenerate ids" using nothing more exotic than `RandomState`'s own per-call entropy.
    /// Fine for a distributional test; not a claim about production id quality.
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
        // Every method is callable and a no-op -- nothing to assert about a clock read directly
        // (same reasoning as `Timer`'s own disabled test), so this asserts the observable
        // consequence instead: dropping the guard records nothing (no panic, and this handle has
        // no buffer to drain in the first place).
        span.events(3);
        span.tag("k", "v");
        span.error();
        drop(span);
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
            MetricKind::Counter(v) => assert_eq!(*v, 5.0),
            other => panic!("expected Counter, got {other:?}"),
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

    /// The clock-safety guarantee `PendingSpan::started_at`'s doc comment describes, proven
    /// directly rather than merely asserted after one lucky run: simulates a wall clock that jumps
    /// backward between a span's start and its finish (an NTP correction, an admin `date` call) via
    /// `now_unix_nanos`'s test-only override, and shows `end_timestamp` still can't precede
    /// `Event::timestamp` (the span's own start) -- because `finish_inner` never reads the wall
    /// clock a second time at all, only `started_at.elapsed()` (`Instant`, monotonic on every
    /// platform this project ships to). Against a two-independent-`SystemTime::now()`-reads
    /// version, the exact backward jump below would produce `end < start` every time, not
    /// occasionally -- this is a structural guarantee, not a timing-dependent one.
    #[test]
    fn a_wall_clock_moving_backward_between_start_and_finish_cannot_make_end_precede_start() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("x", "x", "transform");

        set_test_clock(1_000_000_000); // T0: an arbitrary wall-clock start
        let span = telemetry.span("deliver", SpanKind::Client, trace_id(1), span_id(1), None);
        // The wall clock jumps backward by a full second while the span is still open -- far
        // larger than any real elapsed time this test's own execution could add on its own.
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
                        .then_some(match m.kind {
                            MetricKind::Counter(v) => v,
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
}
