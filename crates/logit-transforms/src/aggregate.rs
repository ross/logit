//! The built-in `aggregate` transform: a stateful, tumbling-window metric aggregator.
//!
//! Windowing/merge semantics are recorded in `docs/adr/aggregation-window-semantics.md` and
//! come from `docs/design/data-model.md`'s mergeable-metric-kinds design: a delta `Sum` sums
//! (`docs/adr/metrics-model-v2.md` -- what `Counter` used to mean, now `Sum { temporality: Delta,
//! .. }`), `Gauge` keeps the value with the latest source timestamp, `Distribution` merges via
//! `DdSketch::merge` (this is that method's first real caller anywhere in the codebase). `Samples`
//! absorbs into a `Distribution` (sketched, the default) or a raw `Samples` accumulator
//! (`distributions: samples`, see `ComponentKind::Aggregate`'s doc comment); `SetMembers` absorbs
//! into a `Set` (`HyperLogLog`, the default) or a raw `SetMembers` accumulator (`sets: members`) --
//! both raw accumulators fall back to their summarized counterpart on overflow or (`Samples` only)
//! a sample-rate mismatch, see `process`'s merge match and `docs/adr/aggregation-window-semantics.md`'s
//! amendment for the full design. A cumulative `Sum`, `ExponentialHistogram`, and `Summary` have no
//! defined merge rule here and pass through untouched rather than being dropped --
//! this project's consistent stance on data it doesn't know how to handle correctly. Since an event
//! can now carry a log and/or a span alongside its metrics (docs/adr/multi-payload-events.md),
//! pass-through is per *metric*, not per *event*: this stage absorbs every mergeable metric off an
//! event and forwards whatever's left -- the unmergeable metrics, plus any log/span -- rather than
//! treating "can't merge one metric" as a reason to forward the whole event untouched.
//!
//! # Temporality: what a flushed `Sum`/`Histogram` means
//!
//! `temporality: delta` ([`AggregateTemporality::Delta`], the default) is strictly tumbling: every
//! window's emitted `Sum` is that window's own increment, the accumulator resets at flush, and a
//! `Histogram` -- of *either* temporality -- is pass-through, exactly as it always has been. That
//! last point is a deliberate non-change: a delta `Histogram` has no merge rule in `delta` mode, so
//! it stays on the event, unchanged from before this mode existed
//! (`docs/adr/aggregation-window-semantics.md`'s cumulative amendment says why widening it would be
//! a separate decision).
//!
//! `temporality: cumulative` instead keeps a delta `Sum`'s and a delta `Histogram`'s accumulator
//! alive across the flush -- the same retention machinery, the same two bounds
//! (`series_retention`/`max_retained_series`) a retained gauge already uses -- and emits the running
//! total every window as `Sum { temporality: Cumulative, .. }` / `Histogram { temporality:
//! Cumulative, .. }` stamped with `MetricRecord::start_timestamp` = the series' first-seen event
//! timestamp. A histogram's per-bucket counts add with `saturating_add`, not `+`: they are
//! wire-supplied `u64`s, so a hostile or broken producer sending `u64::MAX` twice on one series
//! pins the affected bucket at `u64::MAX` -- visibly wrong and still monotonic -- rather than
//! panicking the transform task or wrapping the running total backwards while `start_timestamp`
//! still claims the series never restarted. (`Sum`'s `f64` needs no such guard; it saturates to
//! `inf`.) That stamp is the restart/reset signal OTLP and Prometheus consumers detect a counter
//! reset with: it never changes while the series lives, and a series that is evicted (TTL or
//! cardinality cap) and later re-created gets a new one. An incoming *cumulative* `Sum` is still
//! pass-through in both modes -- `aggregate` re-summing an already-running total would double-count
//! it. See `docs/adr/aggregation-window-semantics.md`'s "cumulative temporality as an opt-in mode"
//! amendment, and `docs/adr/prometheus-scrape-and-exposition.md` for the consumer that needs it
//! (`prometheus_out` skips delta records).

use bytes::Bytes;
use logit_core::interner::Symbol;
use logit_core::{
    AttrMap, Diagnostics, Event, MetricKind, MetricRecord, Resource, Samples, Scope, SpanLink, Sum,
    Telemetry, Temporality, Value,
};
use logit_pipeline::{FlushOutput, TraceContext, Transform};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// This transform's own copy of `logit_config::Distributions` -- `logit-transforms` deliberately
/// doesn't depend on `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so every
/// config enum this transform is configured by gets a local twin here, mapped at
/// `crates/logit-cli/src/pipeline.rs`'s wiring boundary (the `MatchMode`/`to_match_mode`
/// precedent). See [`Aggregator::with_distributions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Distributions {
    /// Sketch every `Samples` value on absorb (`Samples::sketch`) -- no raw samples survive past
    /// the window. The right default: bounded memory regardless of how many samples a series
    /// sees.
    #[default]
    Sketch,
    /// Keep raw values for the whole window (bounded by `max_samples_per_series`), only sketching
    /// on overflow or a sample-rate mismatch.
    Samples,
}

/// This transform's own copy of `logit_config::AggregateTemporality` -- see [`Distributions`]'s doc
/// comment for why a local twin exists at all. Distinct from `logit_core::Temporality`, which is
/// the *per-record* field on the wire: this is the stage's configured mode, which decides what a
/// *flushed* record's `temporality` is set to and whether a series' accumulator survives the flush.
/// See [`Aggregator::with_temporality`] and this module's own "Temporality" doc section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AggregateTemporality {
    /// Tumbling: each window's emitted `Sum` is that window's own increment, and a `Histogram` is
    /// pass-through. Byte-for-byte the behavior every config had before this mode existed.
    #[default]
    Delta,
    /// A delta `Sum`/`Histogram` series' accumulator survives the flush and keeps summing; every
    /// window emits the running total as `Cumulative`, stamped with the series' first-seen time.
    Cumulative,
}

/// This transform's own copy of `logit_config::Sets` -- see [`Distributions`]'s doc comment for
/// why a local twin exists at all. See [`Aggregator::with_sets`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Sets {
    /// Insert every `SetMembers` member into a `HyperLogLog` on absorb -- no exact member set
    /// survives past the window. The right default: bounded memory regardless of how many
    /// distinct members a series sees.
    #[default]
    Estimate,
    /// Keep the exact, deduplicated member set for the whole window (bounded by
    /// `max_set_members_per_series`), only falling back to an estimate on overflow.
    Members,
}

/// A small, bounded set of the distinct `TraceContext`s that have contributed to one series since
/// the last flush -- reasonable/best-effort, not exhaustive. `SpanLink` (`crates/logit-core/src/span.rs`)
/// already exists in the data model for exactly this shape (OTel's answer to "this span was
/// influenced by several others, not descended from one"), per
/// `docs/adr/trace-context-propagation-on-delivered.md`. Same "cap gates insertion only,
/// already-seen is always free to re-observe, drop-and-count past it" shape
/// `ComponentBuffer::upsert` already uses (`crates/logit-core/src/telemetry.rs`) -- the one
/// precedent in this codebase for a bounded, drop-and-counted set.
///
/// Per-series, not shared across a whole resource group or the whole `Aggregator`: a link belongs
/// to the specific series whose flush would become a span, and attributing it to unrelated series
/// in the same window would be exactly the "silently wrong" shape ADR `trace-context-propagation-on-delivered` rejected when it
/// considered (and rejected) picking an arbitrary parent for a flush.
#[derive(Default)]
struct ContributingContexts {
    /// Inline capacity 1: most series see exactly one contributing source, so this stays free
    /// (`docs/adr/minimize-allocations-over-event-size.md`'s policy) until a genuinely
    /// fanned-in series needs more.
    seen: SmallVec<[TraceContext; 1]>,
    dropped: u64,
}

/// Caps how many distinct contexts one series tracks between flushes. A fixed constant, not
/// configurable, matching `docs/known-gaps.md`'s stance on `MAX_KEYS_PER_COMPONENT` -- revisit if
/// a legitimate series ever needs more than this many distinct sources tracked at once.
const MAX_CONTRIBUTING_CONTEXTS_PER_SERIES: usize = 8;

impl ContributingContexts {
    /// Records `ctx` as a contributor, unless it's already tracked (free to re-observe) or the cap
    /// is already full (dropped and counted, never silently grown).
    fn observe(&mut self, ctx: TraceContext) {
        if self.seen.contains(&ctx) {
            return;
        }
        if self.seen.len() >= MAX_CONTRIBUTING_CONTEXTS_PER_SERIES {
            self.dropped += 1;
            return;
        }
        self.seen.push(ctx);
    }

    /// Consumes the tracked set into the `SpanLink`s a flush would attach to whatever span
    /// eventually represents this series (not built yet -- `docs/known-gaps.md`'s internal-spans
    /// entry, item 2), plus how many distinct contexts the cap rejected.
    fn into_links(self) -> (Vec<SpanLink>, u64) {
        let links = self
            .seen
            .into_iter()
            .map(|ctx| SpanLink {
                trace_id: ctx.trace_id,
                span_id: ctx.span_id,
                attributes: AttrMap::new(),
                flags: 0,
                trace_state: None,
                dropped_attributes_count: 0,
            })
            .collect();
        (links, self.dropped)
    }
}

/// One tumbling-window aggregator, owned by one pipeline stage. `process` accumulates what it can
/// and passes everything else straight through; `flush` drains every non-retainable accumulator and
/// every retained series past its retention window, resetting each to empty -- state does not carry
/// across flushes for those. **A gauge series is the one exception under `temporality: delta`**, per
/// `series_retention`: see `docs/adr/aggregation-window-semantics.md`'s "gauge series carry across
/// the window boundary" amendment for the full design and why gauges specifically (not counters) get
/// this. Under `temporality: cumulative` a `Sum`/`Histogram` series is retained the same way, by the
/// same two bounds -- see that ADR's "cumulative temporality as an opt-in mode" amendment and this
/// module's own "Temporality" doc section.
pub struct Aggregator {
    interval: Duration,
    groups: Vec<ResourceGroup>,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// The most recent batch's `TraceContext`, per `observe_batch_context` -- rolling state, not
    /// reset by `flush` (it isn't part of any one window). Mirrors `run_lua`'s `last_resource`
    /// precedent (`crates/logit-pipeline/src/runtime.rs`): default until the first batch arrives.
    current_batch_context: TraceContext,
    /// How many consecutive *idle* windows (no update at all) a retainable series is kept past its
    /// last update, so a gauge delta in window N+1 can still resolve against window N's final
    /// absolute value -- and, under [`AggregateTemporality::Cumulative`], so a `Sum`/`Histogram`'s
    /// running total survives the window boundary at all. `0` (the default, matching
    /// `Aggregator::new`) reproduces today's strictly-tumbling behavior exactly -- no series ever
    /// survives a flush. Set via [`Aggregator::with_series_retention`]. See the ADR
    /// `aggregation-window-semantics` amendments.
    series_retention: u32,
    /// Hard cap on how many series may be retained across all resource groups at once -- a
    /// DoS/cardinality guard, not a tuning knob. `series_retention` alone bounds only the *tail*
    /// (how long one series survives); this bounds the *peak* (how many can exist retained at
    /// once), which a sustained stream of never-repeating series names would otherwise blow past
    /// regardless of how short the retention window is. Least-recently-updated series are evicted
    /// first once this is exceeded. Meaningless while `series_retention` is `0`.
    max_retained_series: usize,
    /// Whether a flushed `Sum`/`Histogram` is this window's increment (the default) or a running
    /// total that survives the flush. Set via [`Aggregator::with_temporality`]; see
    /// [`AggregateTemporality`] and this module's "Temporality" doc section.
    temporality: AggregateTemporality,
    /// Whether an absorbed `Samples` series sketches on arrival (the default) or retains raw
    /// values for the window. Set via [`Aggregator::with_distributions`].
    distributions: Distributions,
    /// Bounds a raw `Samples` accumulator (`distributions: samples`) before it falls back to
    /// sketching what it holds. Meaningless while `distributions` is `Sketch`. Set via
    /// [`Aggregator::with_distributions`].
    max_samples_per_series: usize,
    /// Whether an absorbed `SetMembers` series estimates on arrival (the default) or retains its
    /// exact member set for the window. Set via [`Aggregator::with_sets`].
    sets: Sets,
    /// Bounds a raw `SetMembers` accumulator (`sets: members`) before it falls back to a
    /// `HyperLogLog` estimate of what it holds. Meaningless while `sets` is `Estimate`. Set via
    /// [`Aggregator::with_sets`].
    max_set_members_per_series: usize,
    /// The most recent batch's `Scope`, per `observe_scope` -- rolling state, not reset by
    /// `flush`, mirroring `current_batch_context`'s own field doc comment exactly:
    /// `Transform::observe_scope` (mirroring `observe_batch_context`) fires once per incoming
    /// batch, before any of that batch's events reach `process`, so this is read (and cloned into
    /// any newly-opened `ResourceGroup`) rather than threaded through `process`'s own parameter
    /// list the way `resource` is -- scope, like the trace context, is a per-*batch* fact, not a
    /// per-event one, and `Transform::process`'s per-event hot path shouldn't pay for a value
    /// that never changes within one batch (`Transform::observe_batch_context`'s own doc comment
    /// makes the identical argument). `None` until the first batch arrives, same as
    /// `current_batch_context` defaults to a zero `TraceContext`.
    current_scope: Option<Arc<Scope>>,
}

/// Keyed by `(resource value, scope value)`, not identity -- the same reasoning `group_for`'s own
/// doc comment gives for resource alone, extended to scope: two batches that happen to build their
/// own `Arc::new(Scope { .. })` with equal contents describe the same instrumentation scope and
/// should aggregate together.
struct ResourceGroup {
    resource: Arc<Resource>,
    scope: Option<Arc<Scope>>,
    series: HashMap<SeriesKey, SeriesState>,
}

/// One series' accumulated value, paired with which batches contributed to it since the last
/// flush.
struct SeriesState {
    accumulator: Accumulator,
    contexts: ContributingContexts,
    /// Consecutive flushes this series has survived with **no** update at all -- reset to 0 the
    /// moment any event touches it again. Only ever incremented for a retained series (a gauge, or
    /// a cumulative-mode `Sum`/`Histogram`, with `series_retention > 0`); a series that can't be
    /// retained never survives a flush to have this matter. Compared against
    /// `Aggregator::series_retention` at flush to decide eviction.
    idle_windows: u32,
    /// The timestamp of the first event ever absorbed into this series, in unix nanos -- emitted as
    /// `MetricRecord::start_timestamp` on every cumulative-mode `Sum`/`Histogram` flush, unchanged
    /// for as long as the series lives, which is exactly what makes it the reset signal an OTLP or
    /// Prometheus consumer detects a counter restart with. Captured from `event.timestamp` (the
    /// source's own clock, the only unix-nanos value `process` has and the one an operator can
    /// reason about) rather than a `SystemTime::now()` read, which would put a syscall on the
    /// open-a-series path and make this untestable. A series evicted and later re-created gets a
    /// fresh `SeriesState`, hence a fresh value here -- the restart signal, by construction.
    /// Recorded for every series, not just cumulative ones: it costs 8 bytes on an already
    /// heap-allocated struct, and branching on the mode to decide whether to fill it in would make
    /// the field mean two different things depending on config.
    first_seen: i64,
    /// Whether any event touched this series since the last flush. An explicit field, not derived
    /// from `contexts.seen` being non-empty -- that happens to correlate (`observe` fires on
    /// exactly the successful merges that also flip this), but coupling this to a set built for a
    /// different purpose (span linking) is fragile: a future change to `ContributingContexts`
    /// that stops recording on some merge path would silently break retention's idea of "was this
    /// updated" too. A `bool` costs nothing extra on an already heap-allocated struct.
    updated_this_window: bool,
}

enum Accumulator {
    /// A delta `Sum` merges the way `Counter` used to: the accumulator sums `value` and carries
    /// the *first* record's `monotonic` flag (later merges don't overwrite it, even if a
    /// well-behaved producer would never send mismatched flags under one series identity). Only a
    /// *delta* `Sum` ever reaches an accumulator -- an incoming cumulative one passes through
    /// (`process`'s pass-through predicate) -- so the emitted `temporality` is decided by the
    /// stage's own mode, not by the records that fed it: `Delta` per window under
    /// [`AggregateTemporality::Delta`], `Cumulative` (a running total, retained across flushes)
    /// under [`AggregateTemporality::Cumulative`]. See `into_kind`.
    Sum {
        total: f64,
        monotonic: bool,
    },
    /// Per-bucket running totals for a fixed-bucket histogram -- **only reachable under
    /// [`AggregateTemporality::Cumulative`]**, where a delta `Histogram` gains a merge rule
    /// (`new_for`); in `delta` mode a `Histogram` of either temporality is still pass-through, so
    /// this variant is never constructed. Held as a whole `logit_core::Histogram` (already stamped
    /// `Cumulative`, since that's the only shape it is ever emitted as) so `into_kind` is a move
    /// rather than a rebuild. `buckets` carries the *bounds* of the first record that opened the
    /// series: a later record whose bounds differ has no correct merge and is passed through, see
    /// `process`'s `Histogram` merge arm.
    Histogram(logit_core::Histogram),
    /// `at` is the source event's timestamp, used to pick the last-write-wins value -- not the
    /// window's timestamp, which doesn't exist until flush.
    Gauge {
        value: f64,
        at: i64,
    },
    Distribution(logit_core::DdSketch),
    /// Raw retained samples -- only reachable when `distributions: samples`
    /// ([`Aggregator::with_distributions`]). `sample_rate` is the *first* record's; an incoming
    /// record with a different rate, or growth past `max_samples_per_series`, converts this to
    /// `Distribution` instead of merging (see `process`'s merge match).
    Samples(Samples),
    /// A real cardinality estimate -- `sets: estimate`, the default.
    Set(logit_core::HyperLogLog),
    /// Raw retained set members -- only reachable when `sets: members`
    /// ([`Aggregator::with_sets`]). Exact, deduplicated, insertion-ordered union bounded by
    /// `max_set_members_per_series`; overflow converts this to `Set` instead of merging.
    SetMembers(Vec<Bytes>),
}

/// Whether `kind` has no defined merge rule in this stage and must be forwarded untouched -- a free
/// function, not an inline `matches!`, because the answer now depends on the configured
/// `temporality` and the same question is asked in two places that must agree exactly (`process`,
/// and `Accumulator::new_for`'s `unreachable!` arm -- see both).
///
/// Mode-dependent for one kind only: a `Histogram` whose own temporality is `Delta` merges under
/// [`AggregateTemporality::Cumulative`] (that mode's whole point) and passes through under
/// `Delta`, where it always has. An already-`Cumulative` `Histogram` passes through in both modes,
/// for the same reason a cumulative `Sum` does -- re-accumulating a running total double-counts it.
fn passes_through(kind: &MetricKind, temporality: AggregateTemporality) -> bool {
    match kind {
        MetricKind::Sum(Sum { temporality: Temporality::Cumulative, .. })
        | MetricKind::ExponentialHistogram(_)
        | MetricKind::Summary(_) => true,
        MetricKind::Histogram(h) => {
            h.temporality == Temporality::Cumulative || temporality == AggregateTemporality::Delta
        }
        _ => false,
    }
}

/// Whether two histograms describe the same bucket layout, and so can be added bucket-by-bucket.
/// Compared bitwise (`to_bits`), the same rule `SeriesKey`'s own `PartialEq` uses for an `f64`
/// attribute value and for the same reason: a bound is an *identity* here, not a measurement, so
/// `NaN` (which a `+Inf`-adjacent producer can emit) must compare equal to itself rather than
/// forcing a spurious mismatch on every single record.
fn bucket_bounds_match(held: &[(f64, u64)], incoming: &[(f64, u64)]) -> bool {
    held.len() == incoming.len()
        && held.iter().zip(incoming).all(|((a, _), (b, _))| a.to_bits() == b.to_bits())
}

/// Folds an optional `min`/`max` across a merge: whichever side has a value wins when only one
/// does, `pick` decides when both do. See the `Histogram` merge arm for why this is deliberately
/// more forgiving than the `sum` rule beside it.
fn fold_extreme(
    held: Option<f64>,
    incoming: Option<f64>,
    pick: fn(f64, f64) -> f64,
) -> Option<f64> {
    match (held, incoming) {
        (Some(held), Some(incoming)) => Some(pick(held, incoming)),
        (Some(only), None) | (None, Some(only)) => Some(only),
        (None, None) => None,
    }
}

impl Aggregator {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            groups: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            current_batch_context: TraceContext::default(),
            series_retention: 0,
            max_retained_series: 0,
            temporality: AggregateTemporality::default(),
            distributions: Distributions::default(),
            max_samples_per_series: 1000,
            sets: Sets::default(),
            max_set_members_per_series: 1000,
            current_scope: None,
        }
    }

    /// Enables cross-flush series retention -- see the `series_retention`/`max_retained_series`
    /// field doc comments and the ADR `aggregation-window-semantics` amendments. Matches the existing
    /// `with_diagnostics`/`with_telemetry` builder shape, so `Aggregator::new(interval)`'s
    /// signature stays unchanged and every existing caller (including `crates/logit-bench`'s
    /// fixture) compiles unchanged, defaulting to `0` -- strictly tumbling, exactly today's
    /// behavior.
    pub fn with_series_retention(mut self, retention: u32, max_retained: usize) -> Self {
        self.series_retention = retention;
        self.max_retained_series = max_retained;
        self
    }

    /// Selects what a flushed `Sum`/`Histogram` means -- see [`AggregateTemporality`] and this
    /// module's "Temporality" doc section. Same builder shape as [`Self::with_series_retention`],
    /// defaulting to `Delta`: byte-for-byte today's behavior for every caller that doesn't set it.
    ///
    /// `Cumulative` only does anything useful alongside `with_series_retention(r, m)` with both
    /// bounds non-zero -- a running total that can't survive a flush is just this window's delta
    /// wearing a `Cumulative` label -- which is why `logit_config`'s pair of fields is rejected in
    /// that combination at graph-validation time (`crates/logit-pipeline/src/graph.rs`, rule 39)
    /// rather than being silently accepted here.
    pub fn with_temporality(mut self, temporality: AggregateTemporality) -> Self {
        self.temporality = temporality;
        self
    }

    /// Configures how an absorbed `Samples` series is retained -- see [`Distributions`]'s own doc
    /// comment. Same builder shape as `with_series_retention`: `Aggregator::new`'s signature stays
    /// unchanged, defaulting to `Distributions::Sketch` with a `1000`-value cap (mirroring
    /// `logit_config`'s own defaults, though this crate doesn't depend on that one to read them
    /// directly -- see [`Distributions`]'s doc comment).
    pub fn with_distributions(
        mut self,
        mode: Distributions,
        max_samples_per_series: usize,
    ) -> Self {
        self.distributions = mode;
        self.max_samples_per_series = max_samples_per_series;
        self
    }

    /// Configures how an absorbed `SetMembers` series is retained -- see [`Sets`]'s own doc
    /// comment. Same builder shape as `with_distributions`.
    pub fn with_sets(mut self, mode: Sets, max_set_members_per_series: usize) -> Self {
        self.sets = mode;
        self.max_set_members_per_series = max_set_members_per_series;
        self
    }

    /// Records `ctx` as the context of the batch about to be `process`ed -- see
    /// `Transform::observe_batch_context`'s doc comment for why this is per-batch, not per-event.
    pub fn observe_batch_context(&mut self, ctx: TraceContext) {
        self.current_batch_context = ctx;
    }

    /// Records `scope` as the scope of the batch about to be `process`ed -- the scope analog of
    /// `observe_batch_context` above; see `current_scope`'s own field doc comment for why this is
    /// a batch-level hook rather than a `process` parameter.
    pub fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        self.current_scope = scope;
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Attaches a telemetry handle -- see `flush`'s `logit.transform.series.active`/
    /// `.resource.groups` gauges.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Absorbs every mergeable metric off `event` into this aggregator's window state, and
    /// forwards whatever's left -- unmergeable metric kinds (a cumulative `Sum`, `Histogram`,
    /// `ExponentialHistogram`, `Summary`), a kind conflict with an already-accumulating series,
    /// and/or a log or span, if the event carries any (docs/adr/multi-payload-events.md). `None`
    /// only when nothing at all remains on the event; a pure log/span event (no metrics at all)
    /// never touches window state, matching the zero-cost pass-through this had before an event
    /// could carry more than one payload.
    ///
    /// Grouped by `(resource, scope)` *value*, not `Arc` identity: two inputs that each build
    /// their own `Arc::new(Resource::default())` describe the same (empty) origin and should
    /// aggregate together, not be split into separate windows because they happen to be
    /// different allocations. One input's batches do share one `Arc` in practice (see
    /// `crates/logit-inputs/src/statsd.rs`), so the common case is a single linear-scan group.
    pub fn process(&mut self, resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        if event.metrics.is_empty() {
            return Some(event);
        }

        // Read once, not once per metric -- `observe_batch_context`/`observe_scope` each fire
        // once per incoming batch, so every metric on every event of that batch shares the same
        // values. `distributions`/`max_samples_per_series`/`sets`/`max_set_members_per_series`
        // are copied out for the same reason `ctx`/`scope` are (and so the merge match below can
        // read them without holding a second borrow of `self` alongside `state`).
        let ctx = self.current_batch_context;
        let scope = self.current_scope.clone();
        let temporality = self.temporality;
        let distributions = self.distributions;
        let max_samples_per_series = self.max_samples_per_series;
        let sets = self.sets;
        let max_set_members_per_series = self.max_set_members_per_series;

        // Taken as an owned list, not filtered in place with `retain`: a `retain` closure would
        // need `&event.attributes`/`&event.timestamp` at the same time `self.group_for(resource,
        // &scope)` needs `&mut self`, and those two borrows can't coexist. Owning the metrics up
        // front makes them independent of `event` for the rest of this loop; anything not
        // absorbed is pushed back at the end, in its original relative order.
        let metrics = std::mem::take(&mut event.metrics);
        for record in metrics {
            // An OTLP `NO_RECORDED_VALUE`-flagged record has no genuine reading to fold into a
            // series -- pass it through unmerged, the same shape as the kind-conflict pass-through
            // below, rather than silently folding its default numeric payload in as though it were
            // a real sample (`docs/adr/lossless-transit.md`, `crates/logit-core/src/metric.rs`'s
            // `flags` doc, `docs/known-gaps.md`'s cross-protocol table).
            if record.is_no_recorded_value() {
                self.telemetry.count(
                    "logit.transform.metrics.passed_through",
                    1.0,
                    &[("reason", "no_recorded_value")],
                );
                event.metrics.push(record);
                continue;
            }

            // No merge rule defined for these (docs/design/data-model.md) -- leave them on the
            // event rather than absorbing or dropping them. `GaugeDelta`/`Samples`/`SetMembers`/
            // `Set` are *not* here -- they all have a real resolution below
            // (docs/adr/relative-gauge-adjustments.md, docs/adr/aggregation-window-semantics.md's
            // amendment). A cumulative `Sum` is also pass-through in *both* modes -- only a *delta*
            // `Sum` merges (below), the same way `Counter` used to; re-summing a running total
            // would double-count it.
            //
            // Kept in sync with `Accumulator::new_for`'s `unreachable!` arm *deliberately* -- a
            // kind listed as pass-through here must also be listed there, and vice versa; a
            // mismatch between the two is a runtime panic, not a compile error. Both are
            // mode-dependent for `Histogram` alone, and in the same direction.
            if passes_through(&record.kind, temporality) {
                event.metrics.push(record);
                continue;
            }

            let key = SeriesKey {
                name: record.name,
                unit: record.unit,
                attributes: event.attributes.clone(),
            };
            let group = self.group_for(resource, &scope);
            let entry = group.series.entry(key);
            // Whether this metric opened a brand-new series -- not derivable from the
            // accumulator's `at`/`value` afterward, since a genuine `Gauge(0.0)` at `at:
            // i64::MIN` looks identical to an unseeded delta's result. Only meaningful for
            // `GaugeDelta` below; a fresh `Sum`/`Distribution` series has no equivalent
            // "resolved against a placeholder" hazard, since there's no prior value to have
            // wanted.
            let was_vacant = matches!(entry, std::collections::hash_map::Entry::Vacant(_));
            let state = entry.or_insert_with(|| SeriesState {
                accumulator: Accumulator::new_for(&record.kind, distributions, sets, temporality),
                contexts: ContributingContexts::default(),
                idle_windows: 0,
                // This record's own timestamp: the series starts accumulating where its first
                // observation says it does -- see the field's own doc comment.
                first_seen: event.timestamp,
                updated_this_window: false,
            });

            // Set inside the merge match below, read afterward alongside `self.telemetry`/
            // `self.diag` -- kept out of the match itself so the match only ever needs `state`
            // (already mutably borrowed) and local values, never `self` (see this method's own
            // borrow-shape comment above); mirrors `was_vacant`'s existing "compute during the
            // match, report after" shape for `gauge_delta_unseeded` below.
            let mut samples_weight_clamped = false;
            let mut samples_fallback_reason: Option<&'static str> = None;
            let mut set_members_fallback = false;
            let mut histogram_bounds_mismatch = false;

            let accumulated = match &record.kind {
                MetricKind::Sum(Sum { temporality: Temporality::Delta, value, .. }) => {
                    match &mut state.accumulator {
                        Accumulator::Sum { total, .. } => {
                            *total += value;
                            true
                        }
                        _ => false,
                    }
                }
                // Only reachable under `temporality: cumulative` with a *delta* incoming histogram
                // (`passes_through` filters every other shape out above): add per bucket into the
                // running total, exactly the way the `Sum` arm adds a scalar.
                MetricKind::Histogram(incoming) => match &mut state.accumulator {
                    Accumulator::Histogram(held) => {
                        if !bucket_bounds_match(&held.buckets, &incoming.buckets) {
                            // Two histograms of the same series with different bucket layouts have
                            // no correct merge -- adding bucket *i* of one to bucket *i* of the
                            // other would silently attribute counts to bounds they were never
                            // observed under. Same treatment as a kind conflict: this one record
                            // stays on the event, the accumulator is untouched (its own
                            // diagnostic key below, since the *kind* does match here).
                            histogram_bounds_mismatch = true;
                            false
                        } else {
                            for (held_bucket, incoming_bucket) in
                                held.buckets.iter_mut().zip(incoming.buckets.iter())
                            {
                                // `saturating_add`, not `+`: these counts arrive from the wire
                                // (`otlp_in` copies `bucket_counts` verbatim, no clamp) and this
                                // is the only place this stage sums *integers* rather than
                                // `f64`s, which saturate to `inf` on their own. Unchecked, two
                                // delta points carrying `u64::MAX` on one series would panic the
                                // transform task under `overflow-checks` and wrap the running
                                // total *backwards* without them -- while `start_timestamp` stays
                                // pinned, i.e. the one thing the reset protocol promises a
                                // consumer cannot happen. Saturating matches this crate's posture
                                // on untrusted numbers elsewhere (`trace_context.rs`'s
                                // `checked_mul`/`checked_add`, `scale.rs`'s overflow guard): a
                                // pinned `u64::MAX` is visibly wrong and monotonic, where a
                                // wrapped total is invisibly wrong.
                                held_bucket.1 = held_bucket.1.saturating_add(incoming_bucket.1);
                            }
                            // `sum` adds only when *both* sides have one: a running total missing
                            // one window's contribution understates the series outright, which is
                            // worse than reporting no sum at all (a consumer can tell `None` from
                            // a wrong number). `min`/`max` fold across whichever sides have one --
                            // unlike a sum, an extreme observed over a subset of the windows is
                            // still a genuine observation, just possibly not the true extreme.
                            held.sum = match (held.sum, incoming.sum) {
                                (Some(held_sum), Some(incoming_sum)) => {
                                    Some(held_sum + incoming_sum)
                                }
                                _ => None,
                            };
                            held.min = fold_extreme(held.min, incoming.min, f64::min);
                            held.max = fold_extreme(held.max, incoming.max, f64::max);
                            true
                        }
                    }
                    _ => false,
                },
                MetricKind::Gauge(v) => match &mut state.accumulator {
                    Accumulator::Gauge { value, at } => {
                        // Last-write-wins by timestamp (docs/design/data-model.md): a
                        // later-or-equal source timestamp replaces the held value. Equal
                        // timestamps favor whichever arrives second -- arbitrary but
                        // deterministic given actual processing order. Two gauges of the same
                        // series inside one event share `event.timestamp`, so this tiebreak is
                        // what decides between them too.
                        if event.timestamp >= *at {
                            *value = *v;
                            *at = event.timestamp;
                        }
                        true
                    }
                    _ => false,
                },
                MetricKind::GaugeDelta(d) => match &mut state.accumulator {
                    Accumulator::Gauge { value, .. } => {
                        // Asymmetric on purpose (docs/adr/relative-gauge-adjustments.md, verbatim
                        // there): a delta applies to the running value in *arrival* order and
                        // never advances `at` -- note the `..`. Mixing "deltas in arrival order"
                        // with "absolutes by last-write-wins" is undefined the moment they
                        // interleave unless one of the two rules is pinned independently of the
                        // other's tiebreak; leaving `at` untouched here is what keeps an
                        // absolute's LWW rule meaningful regardless of how many deltas land
                        // between two absolutes.
                        *value += d;
                        true
                    }
                    _ => false,
                },
                MetricKind::Distribution(incoming) => match &mut state.accumulator {
                    Accumulator::Distribution(sketch) => {
                        sketch.merge(incoming);
                        true
                    }
                    // A `samples`-mode series that already fell back to a sketch (or one that
                    // simply never has before now) meets an already-summarized `Distribution` --
                    // e.g. a relay hop where an upstream `aggregate` already sketched. Convert
                    // what's held and merge, same fallback shape `process`'s `Samples` arm below
                    // uses. `held`'s borrow ends at `std::mem::take` (its last use), which is
                    // what makes the `state.accumulator = ..` reassignment two lines down legal.
                    Accumulator::Samples(held) => {
                        let held_owned = std::mem::take(held);
                        let mut sketch = held_owned.sketch();
                        sketch.merge(incoming);
                        state.accumulator = Accumulator::Distribution(sketch);
                        true
                    }
                    // A series already accumulating under one kind (e.g. it started as a delta
                    // sum) just saw a metric of a different kind under the same name/unit/tags
                    // (e.g. a gauge). No correct merge exists for that -- leave this one metric
                    // on the event rather than silently dropping it or corrupting the existing
                    // accumulator with a type-punned value. Per-metric now, not per-event: a
                    // sibling metric on the same event that *does* merge cleanly is still
                    // absorbed. A `GaugeDelta` against a `Sum`/`Distribution` series lands here
                    // too, same as `Gauge` always has.
                    _ => false,
                },
                MetricKind::Samples(incoming) => match &mut state.accumulator {
                    // `distributions: sketch` (the default), or a `samples`-mode series that has
                    // already fallen back -- sketch every value, weighted by `Samples::weight`.
                    // Per-value `add_weighted`, not `sketch.merge(&incoming.sketch())`: the
                    // latter would allocate a whole temporary `DdSketch` (its own bin `Vec`) just
                    // to immediately fold it into this one, real, avoidable allocation work this
                    // loop doesn't pay (`docs/adr/minimize-allocations-over-event-size.md`).
                    Accumulator::Distribution(sketch) => {
                        let weight = incoming.weight();
                        for v in &incoming.values {
                            sketch.add_weighted(*v, weight);
                        }
                        if weight == Samples::MAX_WEIGHT {
                            samples_weight_clamped = true;
                        }
                        true
                    }
                    // `distributions: samples`: concatenate while the rate agrees and the cap
                    // isn't exceeded; otherwise fall back to a sketch (`held` plus `incoming`,
                    // both weighted) and count/diagnose why. `held`'s borrow ends at
                    // `std::mem::take` in both fallback arms, same reasoning as the
                    // `Distribution`/`Samples` arm above.
                    Accumulator::Samples(held) => {
                        if held.sample_rate != incoming.sample_rate {
                            let held_owned = std::mem::take(held);
                            let mut sketch = held_owned.sketch();
                            let weight = incoming.weight();
                            for v in &incoming.values {
                                sketch.add_weighted(*v, weight);
                            }
                            if weight == Samples::MAX_WEIGHT {
                                samples_weight_clamped = true;
                            }
                            state.accumulator = Accumulator::Distribution(sketch);
                            samples_fallback_reason = Some("rate_mismatch");
                            true
                        } else if held.values.len() + incoming.values.len() > max_samples_per_series
                        {
                            let held_owned = std::mem::take(held);
                            let mut sketch = held_owned.sketch();
                            let weight = incoming.weight();
                            for v in &incoming.values {
                                sketch.add_weighted(*v, weight);
                            }
                            if weight == Samples::MAX_WEIGHT {
                                samples_weight_clamped = true;
                            }
                            state.accumulator = Accumulator::Distribution(sketch);
                            samples_fallback_reason = Some("cap");
                            true
                        } else {
                            held.values.extend(incoming.values.iter().copied());
                            true
                        }
                    }
                    _ => false,
                },
                MetricKind::Set(incoming) => match &mut state.accumulator {
                    Accumulator::Set(hll) => {
                        hll.merge(incoming);
                        true
                    }
                    // A `members`-mode series meets an already-summarized `Set` (a relay hop, as
                    // above) -- convert what's held into a fresh estimator and union `incoming`
                    // into it. `held`'s borrow (`std::mem::take`) ends before the reassignment,
                    // same shape as every other conversion arm here.
                    Accumulator::SetMembers(held) => {
                        let held_owned = std::mem::take(held);
                        let mut hll = logit_core::HyperLogLog::new();
                        for m in &held_owned {
                            hll.insert(m);
                        }
                        hll.merge(incoming);
                        state.accumulator = Accumulator::Set(hll);
                        true
                    }
                    _ => false,
                },
                MetricKind::SetMembers(incoming) => match &mut state.accumulator {
                    // `sets: estimate` (the default), or a `members`-mode series that has already
                    // fallen back -- insert every member directly.
                    Accumulator::Set(hll) => {
                        for m in incoming {
                            hll.insert(m);
                        }
                        true
                    }
                    // `sets: members`: union in, deduped (linear scan -- fine at the cap size,
                    // `docs/adr/aggregation-window-semantics.md`'s amendment), preserving
                    // insertion order; falling back to a `HyperLogLog` estimate (inserting every
                    // held-plus-incoming member, so the union stays correct across the
                    // conversion) when that would exceed the cap.
                    Accumulator::SetMembers(held) => {
                        let mut merged = std::mem::take(held);
                        for m in incoming {
                            if !merged.contains(m) {
                                merged.push(m.clone());
                            }
                        }
                        if merged.len() > max_set_members_per_series {
                            let mut hll = logit_core::HyperLogLog::new();
                            for m in &merged {
                                hll.insert(m);
                            }
                            state.accumulator = Accumulator::Set(hll);
                            set_members_fallback = true;
                            true
                        } else {
                            *held = merged;
                            true
                        }
                    }
                    _ => false,
                },
                _ => false,
            };
            if accumulated {
                // Only on an actual merge -- a kind-conflicted metric didn't touch this series'
                // accumulator, so it shouldn't be recorded as one of its contributors either.
                state.contexts.observe(ctx);
                // An explicit flag, not derived from `contexts.seen` -- see `SeriesState`'s field
                // doc comment for why. `flush` resets this to `false` for every series it retains.
                state.updated_this_window = true;
                if was_vacant && matches!(record.kind, MetricKind::GaugeDelta(_)) {
                    // A delta that opened a brand-new series resolved against 0.0 (statsd's own
                    // rule for an unseeded gauge) -- correct per spec, but indistinguishable from
                    // a real 0.0 in the emitted number, so this is counted and reported rather
                    // than left silent.
                    //
                    // In this workstream (B) every flush still drains the whole series map --
                    // gauge retention across the window boundary is workstream C, not yet landed
                    // -- so a series fed *only* by deltas (a client relying on statsd's sticky-
                    // gauge semantics, never sending an absolute) opens a brand-new, empty
                    // accumulator every single window and this fires every time, not once at
                    // startup. That is expected, current-workstream behavior, not a leak or a
                    // bug: once C lands, a gauge series survives an idle window and this only
                    // fires for a genuinely new series or one that's aged out of retention. See
                    // `docs/adr/relative-gauge-adjustments.md`'s Consequences.
                    self.telemetry.count("logit.transform.gauge.delta.unseeded", 1.0, &[]);
                    self.diag.warn_throttled(
                        "gauge_delta_unseeded",
                        format_args!(
                            "gauge delta for '{}' opened a new series and resolved against 0.0                              -- no prior absolute value seen for this series",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
                if samples_weight_clamped {
                    // Moved from `statsd_in`'s own copy of this diagnostic (`docs/plans/
                    // lossless-transit.md`'s W2) -- `statsd_in` keeps its copy until W3 deletes
                    // it, so a `statsd_in -> aggregate` pipeline reports this twice today
                    // (input-side and here); that's expected during this workstream, not a bug.
                    self.telemetry.count("logit.transform.samples.weight_clamped", 1.0, &[]);
                    self.diag.warn_throttled(
                        "sample_rate_clamped",
                        format_args!(
                            "sample_rate for '{}' implies a weight beyond Samples::MAX_WEIGHT                              ({}); clamping",
                            logit_core::interner::resolve(record.name),
                            Samples::MAX_WEIGHT
                        ),
                    );
                }
                if let Some(reason) = samples_fallback_reason {
                    self.telemetry.count(
                        "logit.transform.samples.fallback",
                        1.0,
                        &[("reason", reason)],
                    );
                    let (key, why) = if reason == "rate_mismatch" {
                        (
                            "samples_rate_mismatch",
                            "an incoming record's sample_rate disagreed with this series' first                              record",
                        )
                    } else {
                        ("samples_cap_exceeded", "max_samples_per_series was exceeded")
                    };
                    self.diag.warn_throttled(
                        key,
                        format_args!(
                            "raw samples for '{}' fell back to a sketch: {why}",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
                if set_members_fallback {
                    self.telemetry.count(
                        "logit.transform.set_members.fallback",
                        1.0,
                        &[("reason", "cap")],
                    );
                    self.diag.warn_throttled(
                        "set_members_cap_exceeded",
                        format_args!(
                            "raw set members for '{}' fell back to a HyperLogLog estimate:                              max_set_members_per_series was exceeded",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
            }
            if !accumulated {
                if histogram_bounds_mismatch {
                    // Its own key, not `kind_conflict`'s: the *kind* matches here, only the bucket
                    // layout differs, and an operator chasing this needs to know it's the bounds
                    // (a producer that re-bucketed mid-run) rather than two kinds colliding.
                    self.diag.warn_throttled(
                        "histogram_bounds_mismatch",
                        format_args!(
                            "histogram '{}' arrived with bucket bounds that differ from the \
                             already-accumulating series under the same name/unit/tags -- \
                             forwarding it untouched",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                } else {
                    self.diag.warn_throttled(
                        "kind_conflict",
                        format_args!(
                            "metric '{}' has a kind that conflicts with an already-accumulating                          series under the same name/unit/tags -- forwarding it untouched",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
                event.metrics.push(record);
            }
        }

        if event.metrics.is_empty() && event.log.is_none() && event.span.is_none() {
            None
        } else {
            Some(event)
        }
    }

    fn group_for(
        &mut self,
        resource: &Arc<Resource>,
        scope: &Option<Arc<Scope>>,
    ) -> &mut ResourceGroup {
        if let Some(i) = self
            .groups
            .iter()
            .position(|g| g.resource.as_ref() == resource.as_ref() && scope_key_eq(&g.scope, scope))
        {
            &mut self.groups[i]
        } else {
            self.groups.push(ResourceGroup {
                resource: resource.clone(),
                scope: scope.clone(),
                series: HashMap::new(),
            });
            self.groups.last_mut().expect("just pushed")
        }
    }

    /// One window's worth of series becomes one emitted event each, stamped with `now` and paired
    /// with the `SpanLink`s `ContributingContexts::into_links` built for it. **Every
    /// non-retainable accumulator is still removed unconditionally** -- tumbling, exactly as before
    /// this method gained retention. A *retainable* series with `series_retention > 0` instead
    /// survives into the next window, subject to `max_retained_series`: a gauge in either mode, and
    /// a `Sum`/`Histogram` under `temporality: cumulative`. See the `series_retention`/
    /// `max_retained_series` field doc comments and `docs/adr/aggregation-window-semantics.md`'s
    /// two retention amendments for the full design. `current_batch_context` is *not* reset here --
    /// it isn't part of any one window (see its own field doc comment).
    pub fn flush(&mut self, now: i64) -> FlushOutput {
        // Sampled before any series is touched below -- the peak-of-window value, at the one
        // point this aggregator already visits every series it holds. `aggregate`'s own
        // `SeriesKey` includes an event's whole attribute set (this module's doc comment), so an
        // un-pruned high-cardinality attribute reaching it shows up here first -- see
        // `docs/design/internal-telemetry.md` and `crate::keep`'s own module doc, which already
        // warns about exactly this failure mode.
        //
        // `.active` keeps its documented meaning -- series that received data *this* window --
        // rather than silently growing to include every retained-but-idle series too, which would
        // break its existing high-cardinality early-warning use. `.retained` is the new, separate
        // count for those.
        let mut active_series: usize = 0;
        let mut retained_series: usize = 0;
        for group in &self.groups {
            for state in group.series.values() {
                if state.updated_this_window {
                    active_series += 1;
                } else {
                    retained_series += 1;
                }
            }
        }
        self.telemetry.gauge("logit.transform.series.active", active_series as f64, &[]);
        self.telemetry.gauge("logit.transform.series.retained", retained_series as f64, &[]);
        self.telemetry.gauge("logit.transform.resource.groups", self.groups.len() as f64, &[]);

        // Every series this flush decided to keep, not yet placed back into its group --
        // the cardinality cap below needs to see the *global* candidate set (across every
        // resource group) before any of them are final, since the cap is a whole-`Aggregator`
        // bound, not a per-group one. Each group's own `series` map is emptied via `mem::take`
        // below (not the whole `self.groups` Vec) specifically so surviving series can be
        // re-inserted straight back into their original group afterward, with no need to also
        // rebuild a parallel `resources`/`events_per_group` Vec pair just to remember which
        // group each one came from -- that would cost three extra allocations on the always-taken
        // default (`series_retention: 0`) path for no benefit, since nothing in that path ever
        // populates `survivors` at all.
        let mut survivors: Vec<(usize, SeriesKey, SeriesState)> = Vec::new();
        let mut total_dropped_links: u64 = 0;
        let mut evicted_idle: u64 = 0;
        let mut result = Vec::new();

        for (gi, group) in self.groups.iter_mut().enumerate() {
            let series = std::mem::take(&mut group.series);
            // `series.len()` upper-bounds `events.len()` -- every series either emits (tumbling,
            // or a freshly-retained one) or doesn't (a still-idle retained one), never more
            // than one event each -- so this preallocates for the always-taken default
            // (`series_retention: 0`) path exactly as the pre-retention code did, avoiding the
            // Vec's own amortized-growth reallocations that pushing into an unsized `Vec::new()`
            // would otherwise pay on every flush.
            let mut events = Vec::with_capacity(series.len());
            for (key, mut state) in series {
                // Which series survive this flush: a gauge (its value is sticky by protocol, the
                // original retention amendment) in either mode, and a `Sum`/`Histogram` under
                // `temporality: cumulative`, where the running total *is* what the mode emits. A
                // `Distribution`/`Samples`/`Set`/`SetMembers` series never survives, in either
                // mode -- each window's summary is self-contained, see the ADR.
                let retain = self.series_retention > 0
                    && match &state.accumulator {
                        Accumulator::Gauge { .. } => true,
                        Accumulator::Sum { .. } | Accumulator::Histogram(_) => {
                            self.temporality == AggregateTemporality::Cumulative
                        }
                        _ => false,
                    };
                if state.updated_this_window {
                    let (links, dropped) = std::mem::take(&mut state.contexts).into_links();
                    total_dropped_links += dropped;

                    if retain {
                        // Retained: read the current value without consuming the accumulator
                        // (free for `Gauge`/`Sum`, whose fields are plain `Copy` types; one
                        // bucket-`Vec` clone for a cumulative `Histogram`) rather than
                        // `into_kind()`, which would require cloning the whole accumulator just
                        // to keep a copy of it around afterward. `key.attributes` is cloned here
                        // -- and *only* here, not on the tumbling path below -- because `key`
                        // itself has to survive to become this series' map key again.
                        let mut record = MetricRecord::new(
                            key.name,
                            state.accumulator.kind_for_retained(self.temporality),
                        );
                        record.unit = key.unit;
                        // The series' first-seen time, unchanged for as long as it lives -- the
                        // reset signal a cumulative consumer needs (`SeriesState::first_seen`).
                        // Only for the kinds that carry a running total: a `Gauge` has no start
                        // time to report, so it keeps `MetricRecord::new`'s `0` ("unknown", OTLP's
                        // own convention) exactly as it did before this mode existed.
                        if matches!(record.kind, MetricKind::Sum(_) | MetricKind::Histogram(_)) {
                            record.start_timestamp = state.first_seen;
                        }
                        // Explicit, not just `MetricRecord::new`'s implicit `0` default: an
                        // accumulated value is by construction never a `NO_RECORDED_VALUE` point
                        // (a flagged record short-circuits into `process`'s pass-through instead
                        // of ever reaching an accumulator) -- see `crates/logit-core/src/
                        // metric.rs`'s `flags` doc.
                        record.flags = 0;
                        events.push((Event::metric(now, key.attributes.clone(), record), links));
                        // A retained gauge keeps `value` but resets `at` to `i64::MIN`: LWW is a
                        // within-window tiebreak, and retention must not promote it to a
                        // cross-window ordering guarantee -- an ordinary absolute gauge in the
                        // next window with an earlier source timestamp than this window's winner
                        // must still be accepted, not silently dropped by a stale `at`. Nothing
                        // equivalent applies to a retained `Sum`/`Histogram`: they accumulate in
                        // arrival order and have no timestamp tiebreak to stale out.
                        if let Accumulator::Gauge { at, .. } = &mut state.accumulator {
                            *at = i64::MIN;
                        }
                        state.updated_this_window = false;
                        state.idle_windows = 0;
                        survivors.push((gi, key, state));
                    } else {
                        // Not retained: consume the accumulator directly, exactly as before
                        // retention existed -- zero-cost for `Distribution` (the sketch's backing
                        // `Vec`s move rather than being cloned).
                        let kind = state.accumulator.into_kind(self.temporality);
                        let mut record = MetricRecord::new(key.name, kind);
                        record.unit = key.unit;
                        // See the retained arm above: an accumulated value is never a
                        // `NO_RECORDED_VALUE` point, made explicit rather than relying on
                        // `MetricRecord::new`'s implicit `0` default.
                        record.flags = 0;
                        events.push((Event::metric(now, key.attributes, record), links));
                    }
                } else {
                    // A previously-retained, still-idle series (only reachable when
                    // `series_retention > 0` -- nothing else survives to see an unupdated flush).
                    // Emits nothing this window -- not a repeat of last window's value, not a
                    // zero -- which is the whole point of retention: silence, not noise, for a
                    // series nobody touched. A cumulative `Sum`/`Histogram` is deliberately no
                    // exception: re-emitting an unchanged running total every idle window would
                    // multiply this stage's output by its retention depth, and a cumulative
                    // consumer already treats the last value it saw as standing until replaced.
                    state.contexts = ContributingContexts::default(); // never carried, even empty
                    state.idle_windows += 1;
                    if state.idle_windows < self.series_retention {
                        survivors.push((gi, key, state));
                    } else {
                        evicted_idle += 1;
                    }
                }
            }
            // Guarded on `events.is_empty()` *after* building, not on whether the group's series
            // were empty *before*: a group can now hold only retained-but-idle gauges (zero
            // events this window) without its series map being empty, and the old "skip an
            // empty-series group" guard would have let that same shape through as an empty batch.
            if !events.is_empty() {
                result.push((group.resource.clone(), group.scope.clone(), events));
            }
        }

        // Cardinality cap: a hard bound on *all* retained series at once, evicting the
        // least-recently-updated (highest `idle_windows`) first once exceeded. `series_retention`
        // alone bounds only how long one series survives; without this, a stream of C
        // never-repeating series names per window would hold C * series_retention series forever,
        // regardless of how short the retention window is.
        let mut evicted_cardinality: u64 = 0;
        if survivors.len() > self.max_retained_series {
            let excess = survivors.len() - self.max_retained_series;
            // Stable sort: ties (e.g. several series retained fresh this same flush, all at
            // `idle_windows == 0`) keep their relative order rather than picking an eviction
            // victim nondeterministically among equally-idle series.
            survivors.sort_by_key(|(_, _, state)| std::cmp::Reverse(state.idle_windows));
            survivors.drain(0..excess);
            evicted_cardinality = excess as u64;
        }

        // Re-insert the survivors into their original group's now-empty `series` map, then drop
        // any group left with none: every non-retainable series was always removed above; every
        // retainable one was either not retained, idle-evicted, or just cardinality-evicted -- a
        // delta-mode counters-only resource group disappears from `self.groups` exactly as it
        // always has, tumbling or not.
        for (gi, key, state) in survivors {
            self.groups[gi].series.insert(key, state);
        }
        self.groups.retain(|g| !g.series.is_empty());

        if total_dropped_links > 0 {
            self.telemetry.count(
                "logit.transform.links.dropped",
                total_dropped_links as f64,
                &[("reason", "cardinality")],
            );
        }
        if evicted_idle > 0 {
            self.telemetry.count(
                "logit.transform.series.evicted",
                evicted_idle as f64,
                &[("reason", "idle")],
            );
        }
        if evicted_cardinality > 0 {
            self.telemetry.count(
                "logit.transform.series.evicted",
                evicted_cardinality as f64,
                &[("reason", "cardinality")],
            );
            // Never silent: hitting the cap means a later gauge delta against an evicted series
            // resolves against 0.0 and produces a wrong-looking number, and a cumulative series
            // restarts from zero with a new `start_timestamp` -- correct per the reset protocol,
            // but not something an operator should have to infer.
            self.diag.warn_throttled(
                "series_retention_full",
                format_args!(
                    "series retention cap ({}) exceeded; evicted {evicted_cardinality} least-\
                     recently-updated series -- a later gauge delta against an evicted series \
                     will resolve against 0.0, and an evicted cumulative series restarts from \
                     zero",
                    self.max_retained_series
                ),
            );
        }

        result
    }
}

/// `Aggregator`'s existing inherent methods already match `Transform`'s contract exactly (a
/// deliberate match, not a coincidence -- see `crate::Transform`'s doc comment): this impl is
/// pure delegation, no reshaping needed.
impl Transform for Aggregator {
    fn process(&mut self, resource: &Arc<Resource>, event: Event) -> Option<Event> {
        Aggregator::process(self, resource, event)
    }

    fn observe_batch_context(&mut self, ctx: TraceContext) {
        Aggregator::observe_batch_context(self, ctx)
    }

    fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        Aggregator::observe_scope(self, scope)
    }

    fn flush_interval(&self) -> Option<Duration> {
        Some(self.interval())
    }

    fn flush(&mut self, now: i64) -> FlushOutput {
        Aggregator::flush(self, now)
    }
}

impl Accumulator {
    /// Kept in sync with [`passes_through`] *deliberately* -- see that function's own doc comment.
    /// A kind reaching this function that `passes_through` should have already filtered out is a
    /// runtime panic here, not a compile error.
    ///
    /// `distributions`/`sets` decide the *opened* shape for `Samples`/`SetMembers` --
    /// `Distribution`/`Set` regardless of mode (nothing raw to retain -- there's nothing mode-
    /// dependent about an already-summarized incoming kind), `Samples`/`SetMembers` only when the
    /// matching mode asks to retain raw data. Either way, the actual values/members from *this*
    /// record still land in the accumulator via `process`'s merge match, which runs
    /// unconditionally right after -- `new_for` only decides the empty starting shape, never
    /// contains data itself (the same "create empty, `process` immediately merges into it"
    /// pattern `Distribution`'s own arm already follows).
    fn new_for(
        kind: &MetricKind,
        distributions: Distributions,
        sets: Sets,
        temporality: AggregateTemporality,
    ) -> Self {
        match kind {
            MetricKind::Sum(Sum { temporality: Temporality::Delta, monotonic, .. }) => {
                Accumulator::Sum { total: 0.0, monotonic: *monotonic }
            }
            // `Gauge` and `GaugeDelta` share one accumulator -- they're not a kind conflict, just
            // two different ways to update the same running value (`docs/adr/
            // relative-gauge-adjustments.md`). This makes `new_for` non-injective on
            // purpose: two different `MetricKind`s map to the same `Accumulator` variant, which
            // would otherwise be easy to miss given the `unreachable!()` arm below makes the rest
            // of this mapping look total-and-one-to-one.
            MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => {
                Accumulator::Gauge { value: 0.0, at: i64::MIN }
            }
            MetricKind::Distribution(_) => Accumulator::Distribution(logit_core::DdSketch::new()),
            // `sample_rate` is seeded from *this* record -- the only field `new_for` ever reads
            // off the record it's creating an accumulator for, and only because a freshly-created
            // `Samples` accumulator has no other record to have inherited a rate from yet. Every
            // other accumulator variant starts genuinely empty/identity (`0.0`, `DdSketch::new()`,
            // `HyperLogLog::new()`, an empty `Vec`); this is the one exception, and it's what
            // keeps the very first record `process` merges into this series from spuriously
            // looking like a `rate_mismatch` against a made-up default rate.
            MetricKind::Samples(s) => match distributions {
                Distributions::Sketch => Accumulator::Distribution(logit_core::DdSketch::new()),
                Distributions::Samples => Accumulator::Samples(Samples {
                    values: SmallVec::new(),
                    sample_rate: s.sample_rate,
                }),
            },
            MetricKind::Set(_) => Accumulator::Set(logit_core::HyperLogLog::new()),
            MetricKind::SetMembers(_) => match sets {
                Sets::Estimate => Accumulator::Set(logit_core::HyperLogLog::new()),
                Sets::Members => Accumulator::SetMembers(Vec::new()),
            },
            // A delta `Histogram` only reaches here under `temporality: cumulative`
            // ([`passes_through`]). The opened shape is this record's own bucket *bounds* with zero
            // counts -- the identity element for this series' layout, which `process`'s merge arm
            // immediately adds the record's real counts into, the same "create empty, merge right
            // after" pattern every other arm here follows. `sum` is seeded `Some(0.0)` exactly when
            // this first record has one, so the strict "adds only when both sides have a sum" merge
            // rule doesn't discard a perfectly good sum on the very first merge; `min`/`max` start
            // `None` and fold in. Already stamped `Cumulative`: a retained histogram is only ever
            // emitted as a running total (see `kind_for_retained`/`into_kind`).
            MetricKind::Histogram(h) if temporality == AggregateTemporality::Cumulative => {
                Accumulator::Histogram(logit_core::Histogram {
                    buckets: h.buckets.iter().map(|(bound, _)| (*bound, 0)).collect(),
                    temporality: Temporality::Cumulative,
                    sum: h.sum.map(|_| 0.0),
                    min: None,
                    max: None,
                })
            }
            MetricKind::Sum(Sum { temporality: Temporality::Cumulative, .. })
            | MetricKind::Histogram(_)
            | MetricKind::ExponentialHistogram(_)
            | MetricKind::Summary(_) => {
                unreachable!("process() never creates an accumulator for a pass-through kind")
            }
        }
    }

    /// Consumes this accumulator into the `MetricKind` a *tumbling* flush emits. `temporality`
    /// decides what a `Sum` is labelled: the stage's configured mode, not the records that fed it
    /// (only delta records ever reach an accumulator at all -- see [`passes_through`]).
    fn into_kind(self, temporality: AggregateTemporality) -> MetricKind {
        match self {
            Accumulator::Sum { total, monotonic } => MetricKind::Sum(Sum {
                value: total,
                temporality: record_temporality(temporality),
                monotonic,
            }),
            Accumulator::Gauge { value, .. } => MetricKind::Gauge(value),
            Accumulator::Histogram(histogram) => MetricKind::Histogram(histogram),
            Accumulator::Distribution(sketch) => MetricKind::Distribution(sketch),
            Accumulator::Samples(samples) => MetricKind::Samples(samples),
            Accumulator::Set(hll) => MetricKind::Set(hll),
            Accumulator::SetMembers(members) => MetricKind::SetMembers(members),
        }
    }

    /// [`Accumulator::into_kind`]'s non-consuming twin, for a series that must survive this flush:
    /// the emitted record is a *copy* of the running value, the accumulator keeps accumulating.
    /// Free for `Gauge`/`Sum` (`Copy` fields); one bucket-`Vec` clone for a cumulative `Histogram`,
    /// the unavoidable cost of emitting a snapshot of state that has to persist.
    fn kind_for_retained(&self, temporality: AggregateTemporality) -> MetricKind {
        match self {
            Accumulator::Gauge { value, .. } => MetricKind::Gauge(*value),
            Accumulator::Sum { total, monotonic } => MetricKind::Sum(Sum {
                value: *total,
                temporality: record_temporality(temporality),
                monotonic: *monotonic,
            }),
            Accumulator::Histogram(histogram) => MetricKind::Histogram(histogram.clone()),
            // Kept in sync with `flush`'s `retain` predicate *deliberately*, the same paired-
            // exhaustiveness shape `new_for`/`passes_through` use: nothing else is ever retained.
            Accumulator::Distribution(_)
            | Accumulator::Samples(_)
            | Accumulator::Set(_)
            | Accumulator::SetMembers(_) => {
                unreachable!("flush() only ever retains a Gauge, or a cumulative Sum/Histogram")
            }
        }
    }
}

/// The `logit_core::Temporality` a flushed record carries under a given stage mode -- the one place
/// the config-level mode becomes a per-record wire field, so `into_kind` and `kind_for_retained`
/// can't disagree about it.
fn record_temporality(temporality: AggregateTemporality) -> Temporality {
    match temporality {
        AggregateTemporality::Delta => Temporality::Delta,
        AggregateTemporality::Cumulative => Temporality::Cumulative,
    }
}

/// A metric series' identity: name, unit, and attribute set. Used as a `HashMap` key, which
/// `AttrMap`/`Value` can't be directly -- neither implements `Eq`/`Hash` (`Value::F64` has no
/// total order). Projects `f64` through `to_bits()` for both comparison and hashing instead, so
/// `NaN` keys consistently with itself (`Eq`'s reflexivity requires `a == a`) rather than the
/// IEEE-754 "NaN != NaN" that would otherwise make an aggregation key grow without bound.
struct SeriesKey {
    name: Symbol,
    unit: Option<Symbol>,
    attributes: AttrMap,
}

impl PartialEq for SeriesKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.unit == other.unit
            && self.attributes.len() == other.attributes.len()
            && self
                .attributes
                .iter()
                .zip(other.attributes.iter())
                .all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
    }
}

impl Eq for SeriesKey {}

impl Hash for SeriesKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.unit.hash(state);
        // `AttrMap::iter()` yields sorted-by-`Symbol` order (see its doc comment), so this is
        // stable regardless of insertion order -- two events with the same tags added in a
        // different order still hash and key identically.
        for (k, v) in self.attributes.iter() {
            k.hash(state);
            hash_value(v, state);
        }
    }
}

/// Field-wise equality for a `group_for` scope key, treating `Value::F64` bitwise (via
/// `value_key_eq`) rather than through `Scope`'s derived `PartialEq` -- which recurses into
/// `Value::F64` through `AttrMap` and so, like the IEEE-754 float it ultimately compares, judges a
/// `NaN` scope attribute unequal even to itself. Left as derived `PartialEq`, that would make
/// `group_for` open a fresh `ResourceGroup` for every single event carrying such a scope (two
/// `Arc<Scope>`s that are `==` by every field but never `==` to themselves aren't `==` to each other
/// either), rather than folding them into one -- the same failure mode `SeriesKey`'s own `PartialEq`
/// exists to avoid for series attributes, via the same fix.
///
/// `Arc::ptr_eq` is checked first as a fast path: every event decoded from one batch shares the same
/// `Arc<Scope>` (`EventBatch::scope`), so same-batch events -- the overwhelmingly common case here --
/// never need the field walk at all.
fn scope_key_eq(a: &Option<Arc<Scope>>, b: &Option<Arc<Scope>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            Arc::ptr_eq(a, b)
                || (a.name == b.name
                    && a.version == b.version
                    && a.schema_url == b.schema_url
                    && a.dropped_attributes_count == b.dropped_attributes_count
                    && attr_map_key_eq(&a.attributes, &b.attributes))
        }
        (None, Some(_)) | (Some(_), None) => false,
    }
}

/// `AttrMap` equality via `value_key_eq` (bitwise-float) instead of comparing `Value`s with `==`
/// directly -- factored out of [`SeriesKey`]'s own `PartialEq` (see its doc comment) so
/// [`scope_key_eq`] can give a scope's attributes the identical treatment without duplicating it.
/// `AttrMap::iter()` yields sorted-by-`Symbol` order (see `SeriesKey::hash`'s own comment), so a
/// length check plus a zipped walk is a valid equality test regardless of insertion order.
fn attr_map_key_eq(a: &AttrMap, b: &AttrMap) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
}

fn value_key_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::I64(a), Value::I64(b)) => a == b,
        (Value::U64(a), Value::U64(b)) => a == b,
        (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| value_key_eq(a, b))
        }
        (Value::Map(a), Value::Map(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
        }
        _ => false,
    }
}

fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    // Hash the variant first (by discriminant-ish tag) so e.g. an empty `Array` and an empty
    // `Map` don't collide.
    match v {
        Value::Null => 0u8.hash(state),
        Value::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        Value::I64(n) => {
            2u8.hash(state);
            n.hash(state);
        }
        Value::U64(n) => {
            3u8.hash(state);
            n.hash(state);
        }
        Value::F64(n) => {
            4u8.hash(state);
            n.to_bits().hash(state);
        }
        Value::Bytes(b) => {
            5u8.hash(state);
            b.hash(state);
        }
        Value::Str(s) => {
            6u8.hash(state);
            s.hash(state);
        }
        Value::Timestamp(t) => {
            7u8.hash(state);
            t.hash(state);
        }
        Value::Array(items) => {
            8u8.hash(state);
            items.len().hash(state);
            for item in items {
                hash_value(item, state);
            }
        }
        Value::Map(map) => {
            9u8.hash(state);
            map.len().hash(state);
            for (k, v) in map.iter() {
                k.hash(state);
                hash_value(v, state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;

    fn metric_event(name: &str, kind: MetricKind, timestamp: i64) -> Event {
        Event::metric(timestamp, AttrMap::new(), MetricRecord::new(intern(name), kind))
    }

    fn metric_event_with_tags(
        name: &str,
        kind: MetricKind,
        timestamp: i64,
        tags: &[(&str, &str)],
    ) -> Event {
        let mut event = metric_event(name, kind, timestamp);
        for (k, v) in tags {
            event.attributes.insert(k, *v);
        }
        event
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    /// Most existing assertions below don't care about the per-event `SpanLink` set `flush` now
    /// returns alongside each `Event` (`Transform::flush`'s doc comment) -- this flattens it away
    /// so those assertions keep the same shape they had before flush-side linking landed. Tests
    /// that *do* care about links call `agg.flush(...)` directly instead (see the
    /// `contributing_context`-prefixed tests below).
    fn flush_events(agg: &mut Aggregator, now: i64) -> Vec<(Arc<Resource>, Vec<Event>)> {
        agg.flush(now)
            .into_iter()
            .map(|(resource, _scope, events)| {
                (resource, events.into_iter().map(|(event, _links)| event).collect())
            })
            .collect()
    }

    fn kind_of(event: &Event) -> &MetricKind {
        &event
            .metrics
            .first()
            .unwrap_or_else(|| panic!("expected a metric on event {event:?}"))
            .kind
    }

    fn counter_value(kind: &MetricKind) -> f64 {
        match kind {
            MetricKind::Sum(sum) => sum.value,
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn counters_sum_within_a_window() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::counter(1.0), 0))
            .is_none());
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::counter(2.0), 1))
            .is_none());
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::counter(3.0), 2))
            .is_none());

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 6.0);
        assert_eq!(events[0].timestamp, 100);
    }

    #[test]
    fn gauge_keeps_the_value_with_the_latest_source_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        // Deliberately out of arrival order: the later-timestamped value (5) arrives first.
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(5.0), 50));
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(1.0), 10));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "should keep the value stamped at t=50"),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn distributions_merge_via_ddsketch() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            let mut sketch = logit_core::DdSketch::new();
            sketch.add(v);
            agg.process(&resource, metric_event("latency", MetricKind::Distribution(sketch), 0));
        }

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Distribution(sketch) => {
                assert_eq!(sketch.count(), 5);
                let median = sketch.quantile(0.5).expect("merged sketch has a median");
                assert!((median - 30.0).abs() < 5.0, "median should be near 30, got {median}");
            }
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn a_second_flush_after_the_first_emits_nothing() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        assert_eq!(agg.flush(100).len(), 1, "first flush should emit the window");
        assert!(agg.flush(200).is_empty(), "tumbling: state resets, second flush is empty");
    }

    #[test]
    fn logs_and_spans_pass_through_untouched() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let log = Event::log(
            0,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let passed = agg.process(&resource, log);
        assert!(passed.is_some(), "a log event should pass through, not be absorbed");
        assert!(agg.flush(100).is_empty(), "nothing should have been accumulated");
    }

    /// The four kinds still left with no defined merge rule after this workstream absorbed
    /// `Samples`/`SetMembers`/`Set` (`process`'s pass-through `matches!`) survive `process`
    /// untouched -- a cumulative `Sum` included, since only a *delta* `Sum` merges. Split from the
    /// single combined test this used to be (`docs/plans/lossless-transit.md`'s W2) now that
    /// `Samples`/`SetMembers`/`Set` are no longer pass-through -- see
    /// `samples_is_not_in_the_pass_through_matches` and its two siblings just below for that half.
    #[test]
    fn remaining_pass_through_kinds_survive_process_untouched() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for kind in [
            MetricKind::Sum(Sum {
                value: 1.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
            MetricKind::Histogram(logit_core::Histogram {
                buckets: vec![(10.0, 1)],
                temporality: Temporality::Cumulative,
                sum: None,
                min: None,
                max: None,
            }),
            MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: (0, vec![1]),
                negative: (0, vec![]),
                temporality: Temporality::Cumulative,
                count: 1,
                sum: None,
                min: None,
                max: None,
            }),
            MetricKind::Summary(logit_core::Summary {
                quantiles: vec![(0.5, 1.0)],
                count: 1,
                sum: 1.0,
            }),
        ] {
            let event = metric_event("m", kind, 0);
            assert!(
                agg.process(&resource, event).is_some(),
                "a kind with no defined merge rule should pass through"
            );
        }
        assert!(agg.flush(100).is_empty());
    }

    /// Guards `process`'s pass-through `matches!` directly, not just by implication -- a `Samples`
    /// series must be absorbed (sketched, the default `distributions: sketch` mode), not
    /// forwarded, now that this workstream gives it a real merge rule. Same shape as
    /// `gauge_delta_is_not_in_the_pass_through_matches`.
    #[test]
    fn samples_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let event =
            metric_event("latency", MetricKind::Samples(logit_core::Samples::new([1.0])), 0);
        assert!(agg.process(&resource, event).is_none(), "a Samples-only event should absorb");
    }

    /// Same as `samples_is_not_in_the_pass_through_matches`, for `SetMembers` (`sets: estimate`,
    /// the default).
    #[test]
    fn set_members_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let event = metric_event(
            "unique.users",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a")]),
            0,
        );
        assert!(agg.process(&resource, event).is_none(), "a SetMembers-only event should absorb");
    }

    /// Same as `samples_is_not_in_the_pass_through_matches`, for an already-summarized `Set` --
    /// e.g. a relayed batch from an upstream `aggregate`. Absorbed by merging into a fresh
    /// `HyperLogLog` (`Accumulator::new_for` + the `(Set, MetricKind::Set)` merge arm), same as
    /// every other first-record-opens-a-series case.
    #[test]
    fn set_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut hll = logit_core::HyperLogLog::default();
        hll.insert(b"member");
        let event = metric_event("unique.users", MetricKind::Set(hll), 0);
        assert!(agg.process(&resource, event).is_none(), "a Set-only event should absorb");
    }

    /// The other half of `set_histogram_and_summary_pass_through_untouched`'s coverage: a
    /// cumulative `Sum` must not merge into an already-accumulating *delta* `Sum` series either --
    /// it's a kind conflict from that series' point of view, same as `Gauge` vs. a delta `Sum`
    /// always has been. Pins the plan's explicit `(Sum{Delta}, Sum{Delta})` merges /
    /// `(Sum{Delta}, Sum{Cumulative})` doesn't distinction.
    #[test]
    fn a_cumulative_sum_never_merges_into_an_existing_delta_sum_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());

        let cumulative = metric_event(
            "m",
            MetricKind::Sum(Sum {
                value: 5.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
            0,
        );
        let passed = agg.process(&resource, cumulative);
        assert!(
            passed.is_some(),
            "a cumulative sum must pass through untouched, never merge with a delta series"
        );
        assert!(matches!(
            passed.unwrap().metrics[0].kind,
            MetricKind::Sum(Sum { temporality: Temporality::Cumulative, .. })
        ));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(counter_value(kind_of(&events[0])), 1.0, "the delta series should be untouched");
    }

    /// The accumulator carries the *first* record's `monotonic` flag, not the last -- a later
    /// merge into the same series doesn't overwrite it, even though a well-behaved producer would
    /// never send mismatched flags under one series identity.
    #[test]
    fn sum_merge_carries_the_first_records_monotonic_flag() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event(
                "m",
                MetricKind::Sum(Sum {
                    value: 1.0,
                    temporality: Temporality::Delta,
                    monotonic: false,
                }),
                0,
            ),
        );
        agg.process(
            &resource,
            metric_event(
                "m",
                MetricKind::Sum(Sum {
                    value: 2.0,
                    temporality: Temporality::Delta,
                    monotonic: true,
                }),
                0,
            ),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Sum(sum) => {
                assert_eq!(sum.value, 3.0);
                assert_eq!(sum.temporality, Temporality::Delta);
                assert!(!sum.monotonic, "should carry the first record's monotonic flag (false)");
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    /// Guards `process`'s pass-through `matches!` directly, not just by implication
    /// (`docs/adr/relative-gauge-adjustments.md`): a `GaugeDelta` must be absorbed, not
    /// forwarded, now that workstream B gives it a real resolution -- the opposite of what
    /// workstream A's version of this test pinned. A comment alone on the `matches!` wouldn't
    /// catch a future change that silently defeated the whole feature by adding `GaugeDelta`
    /// back to that list; a test does.
    #[test]
    fn gauge_delta_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let event = metric_event("temp", MetricKind::GaugeDelta(5.0), 0);
        assert!(agg.process(&resource, event).is_none(), "a GaugeDelta-only event should absorb");
    }

    /// A delta opening a brand-new series resolves against 0.0 (statsd's own rule for an
    /// unseeded gauge) and fires the `gauge_delta_unseeded` counter -- correct per spec, but
    /// indistinguishable from a real 0.0 in the emitted number unless reported.
    #[test]
    fn a_delta_into_an_empty_window_resolves_against_zero_and_fires_the_unseeded_counter() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        assert!(agg
            .process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0))
            .is_none());

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "unseeded delta resolves against 0.0"),
            other => panic!("expected Gauge, got {other:?}"),
        }

        let drained = registry.drain(0);
        let unseeded = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.gauge.delta.unseeded" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(unseeded, Some(1.0), "the unseeded delta should be counted");
    }

    #[test]
    fn absolute_then_delta_adds_to_the_absolute() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 15.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// The delta is subsumed by the later absolute, not added underneath it -- an absolute always
    /// *replaces* the running value, never adds to it.
    #[test]
    fn delta_then_absolute_is_subsumed_by_the_absolute() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0));
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 10.0, "the absolute should win outright"),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// Pins the `..` on `at`: a delta must never advance the last-write-wins timestamp. Sequence:
    /// an absolute at t=50 sets `at` to 50; a delta at t=60 must apply (arrival order) but must
    /// leave `at` at 50; a second absolute, also stamped t=50, then arrives -- it only wins the
    /// `event.timestamp >= *at` tiebreak if `at` is still 50. If the delta had incorrectly
    /// advanced `at` to 60, this final absolute (50 >= 60 is false) would be silently dropped and
    /// the flushed value would stay 15, not become 99 -- so asserting 99 is what actually pins
    /// this, not just an assertion that *some* value came out.
    #[test]
    fn a_delta_never_advances_the_last_write_wins_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 50));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 60));
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(99.0), 50));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 99.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn two_deltas_accumulate() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 1));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(-3.0), 2));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 12.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A `GaugeDelta` against an already-accumulating `Counter` series is a real kind conflict --
    /// the same `_ => false` / `kind_conflict` path a `Gauge` vs. `Counter` conflict always took.
    #[test]
    fn gauge_delta_against_a_counter_series_is_a_kind_conflict_and_is_forwarded() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());

        let conflicting = metric_event("m", MetricKind::GaugeDelta(5.0), 0);
        let passed = agg.process(&resource, conflicting);
        assert!(passed.is_some(), "the conflicting delta should be forwarded, not absorbed");
        assert!(matches!(passed.unwrap().metrics[0].kind, MetricKind::GaugeDelta(v) if v == 5.0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(counter_value(kind_of(&events[0])), 1.0, "the counter should be untouched");
    }

    /// `into_kind` always emits `MetricKind::Gauge`, never `MetricKind::GaugeDelta` -- a delta
    /// never survives `aggregate`, however it entered.
    #[test]
    fn a_gauge_delta_never_survives_aggregate() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert!(
            !matches!(kind_of(&events[0]), MetricKind::GaugeDelta(_)),
            "GaugeDelta must never be the kind of a flushed event"
        );
        assert!(matches!(kind_of(&events[0]), MetricKind::Gauge(_)));
    }

    #[test]
    fn distinct_tag_sets_stay_distinct_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event_with_tags("hits", MetricKind::counter(1.0), 0, &[("host", "a")]),
        );
        agg.process(
            &resource,
            metric_event_with_tags("hits", MetricKind::counter(1.0), 0, &[("host", "b")]),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 2, "different tag values should be different series");
    }

    #[test]
    fn same_tags_in_different_insertion_order_collide_into_one_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event_with_tags(
                "hits",
                MetricKind::counter(1.0),
                0,
                &[("host", "a"), ("env", "prod")],
            ),
        );
        agg.process(
            &resource,
            metric_event_with_tags(
                "hits",
                MetricKind::counter(1.0),
                0,
                &[("env", "prod"), ("host", "a")],
            ),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "AttrMap keeps sorted order regardless of insertion order");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    #[test]
    fn different_resources_do_not_fold_together() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let mut resource_a = Resource::default();
        resource_a.attributes.insert("host", "a");
        let mut resource_b = Resource::default();
        resource_b.attributes.insert("host", "b");

        agg.process(&Arc::new(resource_a), metric_event("hits", MetricKind::counter(1.0), 0));
        agg.process(&Arc::new(resource_b), metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(100);
        assert_eq!(flushed.len(), 2, "distinct resources should produce distinct batches");
    }

    #[test]
    fn nan_attribute_value_keys_stably_across_events() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut e1 = metric_event("hits", MetricKind::counter(1.0), 0);
        e1.attributes.insert("score", f64::NAN);
        let mut e2 = metric_event("hits", MetricKind::counter(1.0), 0);
        e2.attributes.insert("score", f64::NAN);

        agg.process(&resource, e1);
        agg.process(&resource, e2);

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "two NaN-tagged events should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    /// A `NO_RECORDED_VALUE`-flagged record has no genuine reading -- `process` must leave it on
    /// the event unmerged (pass-through, the same shape as a kind conflict) rather than fold its
    /// default `0.0` in as a real sample, and count it. Fix 3 in PR #123's review
    /// (`docs/adr/lossless-transit.md`, `docs/known-gaps.md`'s cross-protocol table).
    #[test]
    fn a_no_recorded_value_record_is_passed_through_unmerged_and_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        let mut flagged = metric_event("conns", MetricKind::Gauge(0.0), 0);
        flagged.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let passed = agg.process(&resource, flagged);
        assert!(passed.is_some(), "a flagged record must be forwarded, not absorbed");
        let passed = passed.unwrap();
        assert_eq!(passed.metrics.len(), 1);
        assert_eq!(passed.metrics[0].flags, MetricRecord::FLAG_NO_RECORDED_VALUE);

        // Nothing was absorbed into a series -- a flush produces no event for it.
        let flushed = flush_events(&mut agg, 100);
        assert!(flushed.is_empty() || flushed.iter().all(|(_, events)| events.is_empty()));

        let drained = registry.drain(0);
        let passed_through = drained.iter().find_map(|e| {
            if e.attributes.get("reason").and_then(|v| v.as_str()) != Some("no_recorded_value") {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.metrics.passed_through" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(passed_through, Some(1.0), "the flagged record should be counted");
    }

    #[test]
    fn a_kind_conflict_on_one_series_is_forwarded_not_dropped_or_a_panic() {
        // Same name, same (empty) tags, different kinds -- e.g. a misconfigured statsd source
        // sending both `foo:1|c` and `foo:1|g`. There's no correct merge, so this must forward
        // the conflicting event rather than panic (this exact shape used to hit an `unreachable!`)
        // or silently corrupt the counter accumulator already in progress.
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());
        let conflicting = metric_event("m", MetricKind::Gauge(5.0), 0);
        let passed = agg.process(&resource, conflicting);
        assert!(passed.is_some(), "the conflicting event should be forwarded, not absorbed");

        // The counter accumulator should be untouched by the conflicting event.
        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    /// The headline test for the multi-payload model (docs/adr/multi-payload-events.md): an
    /// event carrying both a log and a counter has the counter absorbed into window state while
    /// the log is still forwarded -- pass-through is per metric now, not per event.
    #[test]
    fn a_log_event_carrying_a_counter_has_the_counter_absorbed_and_the_log_forwarded() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = Event::log(
            0,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        event.metrics.push(MetricRecord::new(intern("hits"), MetricKind::counter(1.0)));

        let passed = agg.process(&resource, event).expect("the log half should be forwarded");
        assert!(passed.metrics.is_empty(), "the counter should have been absorbed");
        assert_eq!(
            passed.log.as_ref().expect("the log should still be present").message,
            Value::str("hello")
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    #[test]
    fn a_mixed_metric_event_absorbs_the_mergeable_ones_and_keeps_the_rest() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("hits", MetricKind::counter(1.0), 0);
        event.metrics.push(MetricRecord::new(
            intern("sizes"),
            MetricKind::Histogram(logit_core::Histogram {
                buckets: vec![(10.0, 1)],
                temporality: Temporality::Cumulative,
                sum: None,
                min: None,
                max: None,
            }),
        ));

        let passed =
            agg.process(&resource, event).expect("the histogram should survive as the remainder");
        assert_eq!(passed.metrics.len(), 1, "only the unmergeable histogram should remain");
        assert!(matches!(passed.metrics[0].kind, MetricKind::Histogram(_)));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    #[test]
    fn a_metric_only_event_fully_absorbed_returns_none() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("a", MetricKind::counter(1.0), 0);
        event.metrics.push(MetricRecord::new(intern("b"), MetricKind::counter(2.0)));

        assert!(
            agg.process(&resource, event).is_none(),
            "an event with nothing left to forward should still return None"
        );
    }

    #[test]
    fn two_metrics_of_the_same_series_on_one_event_sum_into_one_flushed_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("hits", MetricKind::counter(1.0), 0);
        event.metrics.push(MetricRecord::new(intern("hits"), MetricKind::counter(2.0)));

        assert!(agg.process(&resource, event).is_none());

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "both metrics should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 3.0);
    }

    #[test]
    fn a_kind_conflict_leaves_only_the_conflicting_metric_on_the_event() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());

        let mut event = metric_event("m", MetricKind::counter(1.0), 0);
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::Gauge(5.0)));

        let passed =
            agg.process(&resource, event).expect("the conflicting gauge should be forwarded");
        assert_eq!(passed.metrics.len(), 1, "the absorbed counter should not also be forwarded");
        assert!(matches!(passed.metrics[0].kind, MetricKind::Gauge(v) if v == 5.0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 2.0, "both counters should have merged");
    }

    /// Two batches, two different `TraceContext`s, both contributing to the same series --
    /// `flush` should link both, per `ContributingContexts`' whole point.
    #[test]
    fn flush_links_every_distinct_context_that_contributed_to_a_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();

        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        let (_, links) = &events[0];
        assert_eq!(links.len(), 2, "both contributing traces should be linked");
        let trace_ids: Vec<[u8; 16]> = links.iter().map(|l| l.trace_id).collect();
        assert!(trace_ids.contains(&ctx_a.trace_id));
        assert!(trace_ids.contains(&ctx_b.trace_id));
    }

    /// The same context observed twice (e.g. two events from the same batch touching the same
    /// series) shouldn't produce two links -- `ContributingContexts::observe`'s `contains` check.
    #[test]
    fn repeat_events_under_the_same_context_dont_duplicate_a_link() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.observe_batch_context(TraceContext::new_root());
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "the same context observed twice should still be one link");
    }

    /// Nine distinct contributing contexts on one series: the cap (`MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`,
    /// 8) admits the first 8, the 9th is dropped and counted -- the same "bound and count the
    /// drop, never silently grow" shape `ComponentBuffer::upsert`'s own cardinality cap uses.
    #[test]
    fn a_series_fed_by_more_than_the_cap_drops_and_counts_the_rest() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        for _ in 0..9 {
            agg.observe_batch_context(TraceContext::new_root());
            agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));
        }

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 8, "capped at MAX_CONTRIBUTING_CONTEXTS_PER_SERIES");

        let drained = registry.drain(0);
        let dropped = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == "logit.transform.links.dropped" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(dropped, Some(1.0), "the 9th distinct context should be dropped and counted");
    }

    /// Tumbling, not sliding (ADR `aggregation-window-semantics`): a series' contributing-context set resets with the rest
    /// of its accumulator state at flush, exactly like `a_second_flush_after_the_first_emits_nothing`
    /// already pins for the accumulated value itself.
    #[test]
    fn contributing_contexts_reset_after_a_flush() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));
        agg.flush(100); // first window's links discarded along with its accumulator

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(200);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "tumbling: the first window's context shouldn't carry over");
        assert_eq!(
            links[0].trace_id, ctx_b.trace_id,
            "only the second window's context should be linked"
        );
    }

    // Takes already-drained `events`, not a `&Registry` -- `Registry::drain` is consuming (it
    // empties every buffer via `mem::take`), so calling it once per assertion in the same test
    // would make every assertion after the first see an already-emptied registry.
    fn gauge_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    /// `logit.transform.series.evicted{reason}`'s value out of already-drained telemetry events --
    /// the shape three eviction tests below each need, factored out rather than re-inlined.
    fn evicted_count(events: &[Event], reason: &str) -> Option<f64> {
        counter_with_tag(events, "logit.transform.series.evicted", "reason", reason)
    }

    /// `logit.component.diagnostics{key}`'s value -- how a `warn_throttled` call is asserted
    /// (`crates/logit-core/src/diag.rs` counts every occurrence, throttled log or not).
    fn diagnostic_count(events: &[Event], key: &str) -> Option<f64> {
        counter_with_tag(events, "logit.component.diagnostics", "key", key)
    }

    fn counter_with_tag(events: &[Event], name: &str, tag: &str, tag_value: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            if e.attributes.get(tag).and_then(|v| v.as_str()) != Some(tag_value) {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if logit_core::interner::resolve(m.name) == name => {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    #[test]
    fn flush_records_active_series_and_resource_group_counts() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        agg.process(&resource, metric_event("a", MetricKind::counter(1.0), 0));
        agg.process(&resource, metric_event("b", MetricKind::counter(1.0), 0));
        agg.flush(100);

        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.series.active"), Some(2.0));
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(1.0));
    }

    #[test]
    fn flush_with_nothing_accumulated_records_zero_series_and_zero_groups() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);

        agg.flush(100);

        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.series.active"), Some(0.0));
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(0.0));
    }

    #[test]
    fn flush_sums_series_across_multiple_resource_groups() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);

        let mut resource_a = logit_core::AttrMap::new();
        resource_a.insert("host", "a");
        let resource_a = Arc::new(Resource { attributes: resource_a, ..Default::default() });
        let mut resource_b = logit_core::AttrMap::new();
        resource_b.insert("host", "b");
        let resource_b = Arc::new(Resource { attributes: resource_b, ..Default::default() });

        agg.process(&resource_a, metric_event("a", MetricKind::counter(1.0), 0));
        agg.process(&resource_b, metric_event("b", MetricKind::counter(1.0), 0));
        agg.process(&resource_b, metric_event("c", MetricKind::counter(1.0), 0));
        agg.flush(100);

        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.series.active"), Some(3.0));
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(2.0));
    }

    // -----------------------------------------------------------------------------------------
    // Workstream C: gauge series retention across the window boundary
    // (docs/adr/aggregation-window-semantics.md's amendment)
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_delta_in_the_next_window_resolves_against_the_previous_windows_final_value() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        let flushed = flush_events(&mut agg, 100);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 10.0),
            other => panic!("expected Gauge, got {other:?}"),
        }

        // Window 2 sees only a delta, no absolute -- it must resolve against window 1's value.
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 150));
        let flushed = flush_events(&mut agg, 200);
        assert_eq!(flushed.len(), 1);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(*v, 15.0, "the delta should resolve against window 1's final value")
            }
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn a_retained_idle_gauge_emits_nothing_that_window() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        assert_eq!(flush_events(&mut agg, 100).len(), 1, "window 1 emits the gauge");

        // Window 2: nothing touches "conns" at all -- not a repeat of 10.0, not a 0.0, nothing.
        let flushed = agg.flush(200);
        assert!(flushed.is_empty(), "an idle retained gauge must emit nothing that window");
    }

    #[test]
    fn an_idle_gauge_is_evicted_after_series_retention_windows_and_a_later_delta_resolves_against_zero(
    ) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(2, 100)
            .with_telemetry(telemetry);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        assert_eq!(agg.flush(100).len(), 1, "window 1: emits, idle_windows resets to 0");
        assert!(agg.flush(200).is_empty(), "window 2: idle_windows -> 1, still under retention 2");
        assert!(agg.flush(300).is_empty(), "window 3: idle_windows -> 2, now evicted");

        // A delta after eviction opens a brand-new series, resolving against 0.0.
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 350));
        let flushed = flush_events(&mut agg, 400);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "should resolve against 0.0 post-eviction"),
            other => panic!("expected Gauge, got {other:?}"),
        }

        let drained = registry.drain(0);
        let unseeded = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.gauge.delta.unseeded" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(unseeded, Some(1.0), "the post-eviction delta should count as unseeded");
    }

    /// `series_retention: 0` must reproduce today's exact tumbling behavior byte-for-byte -- the
    /// migration story for anyone not opting into retention (which is every existing config,
    /// since it's the field's default).
    #[test]
    fn series_retention_zero_reproduces_the_strictly_tumbling_output() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(0, 0);
        let resource = default_resource();
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(5.0), 50));
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(1.0), 10));

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "should keep the value stamped at t=50"),
            other => panic!("expected Gauge, got {other:?}"),
        }

        assert!(
            agg.flush(200).is_empty(),
            "series_retention: 0 must not retain anything across flushes, exactly like today"
        );
    }

    /// The regression test that matters most (per the plan): retaining `Gauge { value, at }`
    /// verbatim would retain `at` too, so an ordinary absolute gauge in window 2 with an earlier
    /// source timestamp than window 1's winner would be silently dropped by the last-write-wins
    /// rule (`event.timestamp` compared against `at`) -- a new failure class that grows with
    /// retention depth. A retained gauge must reset `at` to `i64::MIN`: LWW is a within-window
    /// tiebreak, and retention must not promote it to a cross-window ordering guarantee.
    #[test]
    fn an_absolute_gauge_in_the_next_window_with_an_earlier_timestamp_is_still_accepted() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        // Window 1's winner is stamped at t=500.
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 500));
        flush_events(&mut agg, 1000);

        // Window 2: an absolute gauge stamped at t=1 -- far earlier than window 1's `at` (500).
        // If retention had carried `at` across the boundary, this would fail `>= at` and be
        // silently dropped.
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(99.0), 1));
        let flushed = flush_events(&mut agg, 2000);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(
                *v, 99.0,
                "an earlier-timestamped absolute must still win against a reset `at`"
            ),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// Retention alone never keeps a counter alive: only `temporality: cumulative` does, and this
    /// aggregator is in the default `delta` mode (see
    /// `cumulative_mode_sums_accumulate_across_flushes_with_a_stable_start_timestamp` for the
    /// other half).
    #[test]
    fn a_delta_mode_counter_series_does_not_survive_its_window_even_with_series_retention() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));
        assert_eq!(flush_events(&mut agg, 100).len(), 1);
        assert!(
            agg.flush(200).is_empty(),
            "a delta-mode counter series must never survive a flush, retention enabled or not"
        );
    }

    #[test]
    fn a_counters_only_resource_group_disappears_from_groups() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(5, 100)
            .with_telemetry(telemetry);
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));
        agg.flush(100);
        // A second flush's own `resource.groups` sample reflects state as of right before it --
        // i.e. right after the first flush pruned the now-empty counters-only group.
        agg.flush(200);

        let events = registry.drain(0);
        assert_eq!(
            gauge_value(&events, "logit.transform.resource.groups"),
            Some(0.0),
            "a counters-only resource group should disappear from `groups` after its flush"
        );
    }

    #[test]
    fn the_cardinality_cap_evicts_and_fires_series_evicted_cardinality() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(5, 2)
            .with_telemetry(telemetry);
        let resource = default_resource();
        for i in 0..3 {
            agg.process(&resource, metric_event(&format!("g{i}"), MetricKind::Gauge(i as f64), 0));
        }
        // 3 fresh gauge series, all wanting retention, but the cap is 2 -- one must be evicted.
        agg.flush(100);

        let events = registry.drain(0);
        let evicted_cardinality = events.iter().find_map(|e| {
            if e.attributes.get("reason").and_then(|v| v.as_str()) != Some("cardinality") {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.series.evicted" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(evicted_cardinality, Some(1.0), "exactly one series should exceed the cap");
    }

    /// `ContributingContexts`' doc comment scopes it to "since the last flush" -- a carried-over
    /// context on a retained gauge would produce the silently-wrong `SpanLink` parent ADR `trace-context-propagation-on-delivered`
    /// rejected. `mem::take`n every flush unconditionally, retained or not.
    #[test]
    fn contexts_are_never_carried_across_a_flush_even_for_a_retained_gauge() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "window 1 links its one contributing context");

        // Window 2: the series is retained-idle, contributing nothing -- if its context leaked
        // forward, a *later* series sharing the same key would incorrectly inherit ctx_a's link.
        // Touch it again with a different context and confirm only the new one is linked.
        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(1.0), 150));
        let flushed = agg.flush(200);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(
            links.len(),
            1,
            "only ctx_b should be linked -- ctx_a must not have carried over"
        );
        assert_eq!(links[0].trace_id, ctx_b.trace_id);
    }

    /// A group holding only retained-but-idle gauges emits zero events; `flush` must not send an
    /// empty `(resource, events)` batch downstream for it.
    #[test]
    fn flush_emits_no_empty_resource_events_group() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        agg.flush(100); // retains "conns", idle from here on

        // Window 2: "conns" is retained-idle (no event); nothing else touches this resource.
        let flushed = agg.flush(200);
        assert!(
            flushed.iter().all(|(_, _, events)| !events.is_empty()),
            "flush must never emit a (resource, events) pair with an empty events list"
        );
        assert!(flushed.is_empty(), "the only group present here has nothing to emit at all");
    }

    // -- Samples/SetMembers/Set absorb (docs/plans/lossless-transit.md's W2) -------------------

    /// `distributions: sketch` (the default): every `Samples` record sketches its values,
    /// weighted by `Samples::weight()`. A rate implying a weight past `Samples::MAX_WEIGHT` is
    /// clamped, not exploded, and counted.
    #[test]
    fn samples_sketch_mode_merges_weighted_values_and_counts_weight_clamp() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        // Unsampled: weight 1, contributes 2 raw observations to the sketch.
        let mut unsampled = Samples::new([10.0, 20.0]);
        unsampled.sample_rate = 1.0;
        agg.process(&resource, metric_event("latency", MetricKind::Samples(unsampled), 0));

        // Sampled at a rate that implies a weight past MAX_WEIGHT -- clamped, not exploded.
        let mut clamped = Samples::new([30.0]);
        clamped.sample_rate = 0.0001;
        agg.process(&resource, metric_event("latency", MetricKind::Samples(clamped), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Distribution(sketch) => {
                assert_eq!(
                    sketch.count(),
                    2 + Samples::MAX_WEIGHT as usize,
                    "2 unweighted + MAX_WEIGHT (clamped) observations"
                );
            }
            other => panic!("expected Distribution, got {other:?}"),
        }

        let drained = registry.drain(0);
        let clamped_count = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.samples.weight_clamped" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(clamped_count, Some(1.0), "the clamped record should be counted once");
    }

    /// `distributions: samples`: raw values concatenate in arrival order, the accumulator keeps
    /// the first record's `sample_rate`, and `into_kind` emits `MetricKind::Samples`.
    #[test]
    fn samples_mode_concatenates_values_and_into_kind_emits_samples() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_distributions(Distributions::Samples, 1000);
        let resource = default_resource();
        let mut a = Samples::new([1.0, 2.0]);
        a.sample_rate = 0.5;
        agg.process(&resource, metric_event("latency", MetricKind::Samples(a), 0));
        let mut b = Samples::new([3.0]);
        b.sample_rate = 0.5;
        agg.process(&resource, metric_event("latency", MetricKind::Samples(b), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Samples(s) => {
                assert_eq!(
                    s.values.as_slice(),
                    &[1.0, 2.0, 3.0],
                    "values concatenate in arrival order"
                );
                assert_eq!(s.sample_rate, 0.5, "the first record's rate is kept");
            }
            other => panic!("expected Samples, got {other:?}"),
        }
    }

    /// A `samples`-mode series meeting a record with a different `sample_rate` falls back to a
    /// sketch (held + incoming, both weighted) and counts/diagnoses why.
    #[test]
    fn samples_mode_rate_mismatch_falls_back_to_distribution_and_counts() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_distributions(Distributions::Samples, 1000)
            .with_telemetry(telemetry);
        let resource = default_resource();

        let mut a = Samples::new([1.0, 2.0]);
        a.sample_rate = 1.0;
        agg.process(&resource, metric_event("latency", MetricKind::Samples(a), 0));

        let mut b = Samples::new([3.0]);
        b.sample_rate = 0.5; // weight 2 -- different rate, triggers fallback
        agg.process(&resource, metric_event("latency", MetricKind::Samples(b), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Distribution(sketch) => {
                assert_eq!(
                    sketch.count(),
                    4,
                    "2 held values at weight 1 + 1 incoming value at weight 2"
                );
            }
            other => panic!("expected Distribution, got {other:?}"),
        }

        let drained = registry.drain(0);
        let fallback = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.samples.fallback"
                        && e.attributes.get("reason").and_then(|v| v.as_str())
                            == Some("rate_mismatch") =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(fallback, Some(1.0), "the rate-mismatch fallback should be counted once");
    }

    /// A `samples`-mode series growing past `max_samples_per_series` falls back to a sketch and
    /// counts/diagnoses why -- the cap-triggered sibling of the rate-mismatch fallback above.
    #[test]
    fn samples_mode_cap_exceeded_falls_back_to_distribution_and_counts() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_distributions(Distributions::Samples, 2)
            .with_telemetry(telemetry);
        let resource = default_resource();

        agg.process(
            &resource,
            metric_event("latency", MetricKind::Samples(Samples::new([1.0, 2.0])), 0),
        );
        // Pushes held (2) + incoming (2) past the cap of 2.
        agg.process(
            &resource,
            metric_event("latency", MetricKind::Samples(Samples::new([3.0, 4.0])), 0),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Distribution(sketch) => assert_eq!(sketch.count(), 4),
            other => panic!("expected Distribution, got {other:?}"),
        }

        let drained = registry.drain(0);
        let fallback = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.samples.fallback"
                        && e.attributes.get("reason").and_then(|v| v.as_str()) == Some("cap") =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(fallback, Some(1.0), "the cap fallback should be counted once");
    }

    /// `(Accumulator::Samples, MetricKind::Distribution)`: a `samples`-mode series meeting an
    /// already-summarized `Distribution` (e.g. a relay hop) converts what it holds and merges.
    #[test]
    fn samples_accumulator_converts_to_distribution_on_an_incoming_distribution() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_distributions(Distributions::Samples, 1000);
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event("latency", MetricKind::Samples(Samples::new([1.0, 2.0])), 0),
        );
        let mut incoming_sketch = logit_core::DdSketch::new();
        incoming_sketch.add(3.0);
        agg.process(
            &resource,
            metric_event("latency", MetricKind::Distribution(incoming_sketch), 0),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Distribution(sketch) => {
                assert_eq!(
                    sketch.count(),
                    3,
                    "2 held (weight 1) + 1 from the incoming Distribution"
                )
            }
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    /// `sets: estimate` (the default): every `SetMembers` record inserts into a `HyperLogLog`,
    /// re-observing an already-seen member doesn't inflate the estimate, and merging two `Set`
    /// series is a real union.
    #[test]
    fn set_estimate_mode_merges_hyperloglogs_and_estimates_distinct_members() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for i in 0..50 {
            agg.process(
                &resource,
                metric_event(
                    "unique.users",
                    MetricKind::SetMembers(vec![Bytes::from(format!("user-{i}"))]),
                    0,
                ),
            );
        }
        // Re-observe a few already-seen members -- must not inflate the estimate.
        for i in 0..10 {
            agg.process(
                &resource,
                metric_event(
                    "unique.users",
                    MetricKind::SetMembers(vec![Bytes::from(format!("user-{i}"))]),
                    0,
                ),
            );
        }

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Set(hll) => {
                let estimate = hll.estimate();
                assert!(
                    (40..=60).contains(&estimate),
                    "estimate should be close to the true 50 distinct members, got {estimate}"
                );
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    /// `(Set, MetricKind::Set)`: merging two already-summarized `Set` series is a real union, not
    /// a sum -- a shared member must not be double-counted.
    #[test]
    fn set_merge_of_two_series_is_a_union() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut a = logit_core::HyperLogLog::new();
        a.insert(b"x");
        a.insert(b"y");
        agg.process(&resource, metric_event("unique.users", MetricKind::Set(a), 0));
        let mut b = logit_core::HyperLogLog::new();
        b.insert(b"y");
        b.insert(b"z");
        agg.process(&resource, metric_event("unique.users", MetricKind::Set(b), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Set(hll) => {
                assert_eq!(hll.estimate(), 3, "x, y, z -- y shared, not double-counted")
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    /// `sets: members`: an exact, deduplicated union preserving insertion order.
    #[test]
    fn set_members_mode_dedups_preserving_insertion_order() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_sets(Sets::Members, 1000);
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event(
                "unique.users",
                MetricKind::SetMembers(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]),
                0,
            ),
        );
        agg.process(
            &resource,
            metric_event(
                "unique.users",
                MetricKind::SetMembers(vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")]),
                0,
            ),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::SetMembers(members) => {
                assert_eq!(
                    members,
                    &vec![
                        Bytes::from_static(b"a"),
                        Bytes::from_static(b"b"),
                        Bytes::from_static(b"c")
                    ],
                    "exact deduped union in insertion order"
                );
            }
            other => panic!("expected SetMembers, got {other:?}"),
        }
    }

    /// A `members`-mode series growing past `max_set_members_per_series` falls back to a
    /// `HyperLogLog` estimate (inserting every held-plus-incoming member) and counts/diagnoses
    /// why.
    #[test]
    fn set_members_mode_cap_exceeded_falls_back_to_set_and_counts() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_sets(Sets::Members, 2)
            .with_telemetry(telemetry);
        let resource = default_resource();

        agg.process(
            &resource,
            metric_event(
                "unique.users",
                MetricKind::SetMembers(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]),
                0,
            ),
        );
        agg.process(
            &resource,
            metric_event("unique.users", MetricKind::SetMembers(vec![Bytes::from_static(b"c")]), 0),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Set(hll) => assert_eq!(hll.estimate(), 3),
            other => panic!("expected Set (cap fallback), got {other:?}"),
        }

        let drained = registry.drain(0);
        let fallback = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.set_members.fallback"
                        && e.attributes.get("reason").and_then(|v| v.as_str()) == Some("cap") =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        });
        assert_eq!(fallback, Some(1.0), "the cap fallback should be counted once");
    }

    /// `(Accumulator::SetMembers, MetricKind::Set)`: a `members`-mode series meeting an
    /// already-summarized `Set` (e.g. a relay hop) converts what it holds and merges.
    #[test]
    fn set_members_accumulator_converts_to_set_on_an_incoming_set() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_sets(Sets::Members, 1000);
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event("unique.users", MetricKind::SetMembers(vec![Bytes::from_static(b"a")]), 0),
        );
        let mut incoming = logit_core::HyperLogLog::new();
        incoming.insert(b"b");
        agg.process(&resource, metric_event("unique.users", MetricKind::Set(incoming), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Set(hll) => assert_eq!(hll.estimate(), 2, "held 'a' plus incoming 'b'"),
            other => panic!("expected Set, got {other:?}"),
        }
    }

    /// A `Samples` series must tumble every window regardless of `series_retention` -- it's not a
    /// `Gauge`, so nothing about retention applies to it.
    #[test]
    fn a_samples_series_never_survives_a_flush_even_with_series_retention_enabled() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(5, 100)
            .with_distributions(Distributions::Samples, 1000);
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event("latency", MetricKind::Samples(Samples::new([1.0])), 0),
        );
        assert_eq!(agg.flush(100).len(), 1, "first flush emits the series");
        assert!(agg.flush(200).is_empty(), "a Samples series must tumble, never retain");
    }

    /// Same as `a_samples_series_never_survives_a_flush_even_with_series_retention_enabled`, for a
    /// `SetMembers` series.
    #[test]
    fn a_set_members_series_never_survives_a_flush_even_with_series_retention_enabled() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(5, 100)
            .with_sets(Sets::Members, 1000);
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event("unique.users", MetricKind::SetMembers(vec![Bytes::from_static(b"a")]), 0),
        );
        assert_eq!(agg.flush(100).len(), 1, "first flush emits the series");
        assert!(agg.flush(200).is_empty(), "a SetMembers series must tumble, never retain");
    }

    // -- Scope-keyed groups (`FlushOutput` carries scope, docs/plans/lossless-transit.md's W2) --

    /// Two batches sharing a resource but carrying different scopes flush as two distinct groups,
    /// each carrying its own scope -- `ResourceGroup` keys on `(resource, scope)` value, not
    /// resource alone.
    #[test]
    fn same_resource_different_scope_flush_as_two_groups_carrying_their_scope() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let scope_a = Arc::new(Scope { name: Bytes::from_static(b"scope-a"), ..Scope::default() });
        let scope_b = Arc::new(Scope { name: Bytes::from_static(b"scope-b"), ..Scope::default() });

        agg.observe_scope(Some(scope_a.clone()));
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        agg.observe_scope(Some(scope_b.clone()));
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(100);
        assert_eq!(
            flushed.len(),
            2,
            "distinct scopes under the same resource should be distinct groups"
        );
        let scope_names: std::collections::HashSet<Option<Bytes>> =
            flushed.iter().map(|(_, scope, _)| scope.as_ref().map(|s| s.name.clone())).collect();
        assert!(scope_names.contains(&Some(Bytes::from_static(b"scope-a"))));
        assert!(scope_names.contains(&Some(Bytes::from_static(b"scope-b"))));
    }

    /// P2's pinning test: two distinct `Arc<Scope>`s with identical contents -- including a `NaN`
    /// attribute -- must still fold into one `ResourceGroup`. Left to `Scope`'s derived `PartialEq`
    /// (which recurses into `Value::F64` via `AttrMap`), a `NaN` attribute is unequal even to
    /// itself, so `group_for` would treat every event as a new scope and never accumulate them --
    /// two `counter(1.0)` events would flush as two separate `Sum`s of `1.0` instead of one `Sum` of
    /// `2.0`. `scope_key_eq`'s `Arc::ptr_eq` fast path is deliberately not exercised here (`scope_a`/
    /// `scope_b` are separate `Arc`s), so this only passes once the field-wise, bitwise-float
    /// fallback is correct.
    #[test]
    fn scopes_with_a_nan_attribute_but_distinct_arcs_fold_into_one_group() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();

        let make_scope = || {
            let mut attrs = AttrMap::new();
            attrs.insert("temp", f64::NAN);
            Arc::new(Scope {
                name: Bytes::from_static(b"nan-scope"),
                attributes: attrs,
                ..Scope::default()
            })
        };
        let scope_a = make_scope();
        let scope_b = make_scope();
        assert!(!Arc::ptr_eq(&scope_a, &scope_b), "test setup: must be distinct Arcs");

        agg.observe_scope(Some(scope_a));
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 0));

        agg.observe_scope(Some(scope_b));
        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 1));

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(
            flushed.len(),
            1,
            "two Arcs of an equal, NaN-bearing scope must be one ResourceGroup, not two"
        );
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    // -----------------------------------------------------------------------------------------
    // `temporality: cumulative` (docs/adr/aggregation-window-semantics.md's "cumulative
    // temporality as an opt-in mode" amendment)
    // -----------------------------------------------------------------------------------------

    /// An aggregator in `cumulative` mode with both retention bounds set -- the only combination
    /// `logit_config` allows for that mode (graph rule 39), so every test below uses it.
    fn cumulative_agg() -> Aggregator {
        Aggregator::new(Duration::from_secs(10))
            .with_temporality(AggregateTemporality::Cumulative)
            .with_series_retention(5, 100)
    }

    fn sum_of(event: &Event) -> &Sum {
        match kind_of(event) {
            MetricKind::Sum(sum) => sum,
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    fn start_timestamp_of(event: &Event) -> i64 {
        event.metrics.first().expect("a metric on the event").start_timestamp
    }

    fn histogram_of(event: &Event) -> &logit_core::Histogram {
        match kind_of(event) {
            MetricKind::Histogram(h) => h,
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    /// A delta histogram event, the shape a scrape/OTLP delta producer hands over: per-bucket
    /// counts, an optional `sum`, and optional `min`/`max`.
    fn delta_histogram_event(
        name: &str,
        buckets: &[(f64, u64)],
        sum: Option<f64>,
        min: Option<f64>,
        max: Option<f64>,
        timestamp: i64,
    ) -> Event {
        metric_event(
            name,
            MetricKind::Histogram(logit_core::Histogram {
                buckets: buckets.to_vec(),
                temporality: Temporality::Delta,
                sum,
                min,
                max,
            }),
            timestamp,
        )
    }

    /// The headline test for this mode: a delta `Sum` series' accumulator survives every flush,
    /// each window emits the *running total* labelled `Cumulative`, and `start_timestamp` is the
    /// series' first-seen event timestamp -- identical across flushes, which is exactly what makes
    /// it a reset signal rather than a per-window timestamp.
    #[test]
    fn cumulative_mode_sums_accumulate_across_flushes_with_a_stable_start_timestamp() {
        let mut agg = cumulative_agg();
        let resource = default_resource();

        agg.process(&resource, metric_event("hits", MetricKind::counter(2.0), 1_000));
        agg.process(&resource, metric_event("hits", MetricKind::counter(3.0), 1_500));
        let flushed = flush_events(&mut agg, 100_000);
        let first = &flushed[0].1[0];
        assert_eq!(sum_of(first).value, 5.0, "window 1's own increments");
        assert_eq!(sum_of(first).temporality, Temporality::Cumulative);
        assert!(sum_of(first).monotonic, "MetricKind::counter is monotonic, and that carries");
        assert_eq!(
            start_timestamp_of(first),
            1_000,
            "the first event's timestamp opened this series"
        );

        // Window 2: a further increment adds to the running total rather than starting over.
        agg.process(&resource, metric_event("hits", MetricKind::counter(4.0), 110_000));
        let flushed = flush_events(&mut agg, 200_000);
        let second = &flushed[0].1[0];
        assert_eq!(sum_of(second).value, 9.0, "5 carried forward plus 4 this window");
        assert_eq!(sum_of(second).temporality, Temporality::Cumulative);
        assert_eq!(
            start_timestamp_of(second),
            1_000,
            "start_timestamp must not move while the series lives"
        );
    }

    /// A non-monotonic delta `Sum` (an OTLP up-down counter) keeps its flag through cumulative
    /// accumulation -- the emitted temporality comes from the stage's mode, the `monotonic` flag
    /// from the data, and the two are independent.
    #[test]
    fn cumulative_mode_keeps_the_accumulated_monotonic_flag() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let up_down = |v: f64| {
            MetricKind::Sum(Sum { value: v, temporality: Temporality::Delta, monotonic: false })
        };
        agg.process(&resource, metric_event("queue.depth", up_down(5.0), 10));
        agg.process(&resource, metric_event("queue.depth", up_down(-2.0), 20));

        let flushed = flush_events(&mut agg, 100);
        let emitted = sum_of(&flushed[0].1[0]);
        assert_eq!(emitted.value, 3.0);
        assert_eq!(emitted.temporality, Temporality::Cumulative);
        assert!(!emitted.monotonic, "a non-monotonic sum stays non-monotonic");
    }

    /// A retained cumulative series that goes idle emits nothing that window (the same silence a
    /// retained gauge keeps) and then resumes from its running total, not from zero.
    #[test]
    fn an_idle_cumulative_series_emits_nothing_then_resumes_from_its_running_total() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::counter(7.0), 100));
        assert_eq!(flush_events(&mut agg, 1_000).len(), 1, "window 1 emits the total");

        assert!(agg.flush(2_000).is_empty(), "an idle cumulative series emits nothing");

        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 2_500));
        let flushed = flush_events(&mut agg, 3_000);
        assert_eq!(sum_of(&flushed[0].1[0]).value, 8.0, "the idle window didn't reset the total");
        assert_eq!(start_timestamp_of(&flushed[0].1[0]), 100, "still the original start");
    }

    /// A delta `Histogram` merges per bucket in `cumulative` mode: bucket counts add, `sum` adds
    /// (both sides have one), `min`/`max` fold, and every flush emits the running total as
    /// `Cumulative` with the series' `start_timestamp`.
    #[test]
    fn cumulative_mode_histograms_accumulate_per_bucket() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let buckets = [(1.0, 1u64), (5.0, 2), (f64::INFINITY, 3)];
        assert!(
            agg.process(
                &resource,
                delta_histogram_event("sizes", &buckets, Some(30.0), Some(0.5), Some(9.0), 500)
            )
            .is_none(),
            "a delta histogram must be absorbed in cumulative mode, not forwarded"
        );

        let flushed = flush_events(&mut agg, 10_000);
        let first = &flushed[0].1[0];
        assert_eq!(histogram_of(first).buckets, buckets.to_vec());
        assert_eq!(histogram_of(first).temporality, Temporality::Cumulative);
        assert_eq!(histogram_of(first).sum, Some(30.0));
        assert_eq!(start_timestamp_of(first), 500);

        // Window 2: another delta histogram over the same bounds, with a lower min and higher max.
        agg.process(
            &resource,
            delta_histogram_event(
                "sizes",
                &[(1.0, 10), (5.0, 20), (f64::INFINITY, 30)],
                Some(4.0),
                Some(0.1),
                Some(11.0),
                11_000,
            ),
        );
        let flushed = flush_events(&mut agg, 20_000);
        let second = &flushed[0].1[0];
        assert_eq!(
            histogram_of(second).buckets,
            vec![(1.0, 11), (5.0, 22), (f64::INFINITY, 33)],
            "bucket counts add bucket-for-bucket across windows"
        );
        assert_eq!(histogram_of(second).sum, Some(34.0), "sums add when both sides have one");
        assert_eq!(histogram_of(second).min, Some(0.1), "min folds to the lower of the two");
        assert_eq!(histogram_of(second).max, Some(11.0), "max folds to the higher of the two");
        assert_eq!(start_timestamp_of(second), 500, "start_timestamp is still the first-seen time");
    }

    /// `sum` is strict where `min`/`max` are forgiving: a window whose histogram reports no `sum`
    /// makes the running total's `sum` `None` (a partial total would understate the series
    /// outright), while its absent `min`/`max` leave the folded extremes standing.
    #[test]
    fn a_histogram_window_without_a_sum_drops_the_running_sum_but_keeps_min_and_max() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let bounds = [(1.0, 1u64), (f64::INFINITY, 1)];
        agg.process(
            &resource,
            delta_histogram_event("sizes", &bounds, Some(3.0), Some(0.5), Some(2.0), 0),
        );
        agg.process(&resource, delta_histogram_event("sizes", &bounds, None, None, None, 1));

        let flushed = flush_events(&mut agg, 100);
        let emitted = histogram_of(&flushed[0].1[0]);
        assert_eq!(emitted.buckets, vec![(1.0, 2), (f64::INFINITY, 2)]);
        assert_eq!(emitted.sum, None, "a sum missing one window's contribution is no sum at all");
        assert_eq!(emitted.min, Some(0.5), "the extremes observed so far still stand");
        assert_eq!(emitted.max, Some(2.0));
    }

    /// Bucket counts are wire-supplied `u64`s (`otlp_in` copies `bucket_counts` verbatim, with no
    /// clamp), and this is the only place the stage sums integers rather than `f64`s. Two delta
    /// points carrying `u64::MAX` on one series must saturate: unchecked, this panics the transform
    /// task under `overflow-checks` and wraps the running total backwards without them -- while
    /// `start_timestamp` stays pinned, which is exactly the "the series did not restart" promise a
    /// cumulative consumer relies on. Asserting `u64::MAX` (not just "no panic") is what pins the
    /// saturation rather than any other recovery.
    #[test]
    fn cumulative_histogram_bucket_counts_saturate_instead_of_overflowing() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let maxed = [(1.0, u64::MAX), (f64::INFINITY, u64::MAX)];
        assert!(agg
            .process(&resource, delta_histogram_event("sizes", &maxed, None, None, None, 0))
            .is_none());
        assert!(agg
            .process(&resource, delta_histogram_event("sizes", &maxed, None, None, None, 1))
            .is_none());

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(
            histogram_of(&flushed[0].1[0]).buckets,
            vec![(1.0, u64::MAX), (f64::INFINITY, u64::MAX)],
            "a saturated bucket pins at u64::MAX -- never wraps back around"
        );
        assert_eq!(
            start_timestamp_of(&flushed[0].1[0]),
            0,
            "and the series is still the same series, which is why wrapping would be so wrong"
        );
    }

    /// Two histograms of one series with different bucket *bounds* have no correct merge -- adding
    /// bucket i of one to bucket i of the other would attribute counts to bounds they were never
    /// observed under. Same treatment as a kind conflict: the offending record stays on the event,
    /// the accumulator is untouched, and its own diagnostic fires.
    #[test]
    fn a_histogram_with_mismatched_bucket_bounds_is_passed_through() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = cumulative_agg()
            .with_diagnostics(Diagnostics::default().with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        let resource = default_resource();

        agg.process(
            &resource,
            delta_histogram_event("sizes", &[(1.0, 1), (f64::INFINITY, 1)], None, None, None, 0),
        );
        let mismatched = delta_histogram_event(
            "sizes",
            &[(2.0, 5), (f64::INFINITY, 5)], // different bound: 2.0, not 1.0
            None,
            None,
            None,
            1,
        );
        let passed = agg.process(&resource, mismatched);
        let passed = passed.expect("the mismatched histogram must be forwarded, not absorbed");
        assert_eq!(passed.metrics.len(), 1);
        assert!(matches!(passed.metrics[0].kind, MetricKind::Histogram(_)));

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(
            histogram_of(&flushed[0].1[0]).buckets,
            vec![(1.0, 1), (f64::INFINITY, 1)],
            "the accumulating series must be untouched by the mismatched record"
        );

        let drained = registry.drain(0);
        assert_eq!(
            diagnostic_count(&drained, "histogram_bounds_mismatch"),
            Some(1.0),
            "the bounds mismatch should be diagnosed under its own key"
        );
    }

    /// An idle cumulative series ages out after `series_retention` windows, fires
    /// `series.evicted{reason="idle"}`, and a later increment opens a **new** series -- restarting
    /// the total from that increment alone with a fresh `start_timestamp`. That new start time is
    /// precisely the restart signal a cumulative consumer detects a reset with.
    #[test]
    fn an_evicted_cumulative_series_restarts_with_a_new_start_timestamp() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_temporality(AggregateTemporality::Cumulative)
            .with_series_retention(2, 100)
            .with_telemetry(telemetry);
        let resource = default_resource();

        agg.process(&resource, metric_event("hits", MetricKind::counter(5.0), 100));
        assert_eq!(agg.flush(1_000).len(), 1, "window 1: emits 5, idle_windows resets to 0");
        assert!(agg.flush(2_000).is_empty(), "window 2: idle_windows -> 1, still under retention");
        assert!(agg.flush(3_000).is_empty(), "window 3: idle_windows -> 2, now evicted");

        agg.process(&resource, metric_event("hits", MetricKind::counter(1.0), 3_500));
        let flushed = flush_events(&mut agg, 4_000);
        let restarted = &flushed[0].1[0];
        assert_eq!(sum_of(restarted).value, 1.0, "an evicted series restarts from zero");
        assert_eq!(
            start_timestamp_of(restarted),
            3_500,
            "the re-created series carries a new start_timestamp -- the reset signal"
        );

        let drained = registry.drain(0);
        assert_eq!(
            evicted_count(&drained, "idle"),
            Some(1.0),
            "TTL eviction of a cumulative series fires series.evicted with reason=idle"
        );
    }

    /// The cardinality cap bounds cumulative series exactly as it bounds retained gauges: the
    /// least-recently-updated survivors are evicted, counted under `reason="cardinality"`, and the
    /// `series_retention_full` diagnostic fires -- never silent, because an evicted cumulative
    /// series restarts from zero.
    #[test]
    fn the_cardinality_cap_evicts_cumulative_series_and_fires_series_retention_full() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_temporality(AggregateTemporality::Cumulative)
            .with_series_retention(5, 2)
            .with_diagnostics(Diagnostics::default().with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        let resource = default_resource();
        for i in 0..3 {
            agg.process(&resource, metric_event(&format!("c{i}"), MetricKind::counter(1.0), 0));
        }
        agg.flush(100);

        let drained = registry.drain(0);
        assert_eq!(
            evicted_count(&drained, "cardinality"),
            Some(1.0),
            "exactly one of the three cumulative series should exceed the cap of 2"
        );
        assert_eq!(
            diagnostic_count(&drained, "series_retention_full"),
            Some(1.0),
            "hitting the cap must not be silent"
        );
    }

    /// An incoming *cumulative* `Sum` is pass-through in `cumulative` mode too -- `aggregate` must
    /// never re-sum an already-running total, which would double-count it. The complement of
    /// `a_cumulative_sum_never_merges_into_an_existing_delta_sum_series`, which pins the same rule
    /// in `delta` mode.
    #[test]
    fn a_cumulative_sum_input_still_passes_through_in_cumulative_mode() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let incoming = metric_event(
            "m",
            MetricKind::Sum(Sum {
                value: 42.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
            0,
        );
        let passed = agg.process(&resource, incoming);
        assert!(passed.is_some(), "an already-cumulative Sum must pass through untouched");
        assert!(agg.flush(100).is_empty(), "and must not have opened a series");
    }

    /// `delta` mode is unchanged by this amendment, stated directly: a delta `Sum` tumbles, and its
    /// emitted record is labelled `Delta` with no `start_timestamp`.
    #[test]
    fn delta_mode_sums_tumble_and_emit_delta_temporality_with_no_start_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::counter(4.0), 1_000));

        let flushed = flush_events(&mut agg, 10_000);
        let emitted = &flushed[0].1[0];
        assert_eq!(sum_of(emitted).value, 4.0);
        assert_eq!(sum_of(emitted).temporality, Temporality::Delta);
        assert_eq!(start_timestamp_of(emitted), 0, "delta records carry no start time");
        assert!(agg.flush(20_000).is_empty(), "and the series tumbles, retention or not");
    }

    /// The mode-dependent half of [`passes_through`], pinned directly: a delta `Histogram` is
    /// pass-through in `delta` mode, exactly as it was before this amendment existed. Widening that
    /// is a separate decision, not a side effect of adding a cumulative mode.
    #[test]
    fn a_delta_histogram_still_passes_through_in_delta_mode() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        let event = delta_histogram_event("sizes", &[(1.0, 1)], Some(1.0), None, None, 0);
        let passed = agg.process(&resource, event);
        assert!(passed.is_some(), "a delta histogram must still pass through in delta mode");
        assert!(agg.flush(100).is_empty(), "and must not have opened a series");
    }

    /// A `Distribution` series tumbles in `cumulative` mode too: only `Sum`/`Histogram` gained a
    /// running total, and a sketch of one window's observations is self-contained (the same
    /// reasoning `a_samples_series_never_survives_a_flush_even_with_series_retention_enabled`
    /// pins for retention).
    #[test]
    fn a_distribution_series_still_tumbles_in_cumulative_mode() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let mut sketch = logit_core::DdSketch::new();
        sketch.add(1.0);
        agg.process(&resource, metric_event("latency", MetricKind::Distribution(sketch), 0));
        assert_eq!(agg.flush(100).len(), 1, "the first flush emits the sketch");
        assert!(agg.flush(200).is_empty(), "a Distribution series must tumble in either mode");
    }
}
