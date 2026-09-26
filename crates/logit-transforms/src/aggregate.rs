//! The built-in `aggregate` transform: a stateful, tumbling-window metric aggregator.
//!
//! Windowing and merge semantics: `docs/adr/aggregation-window-semantics.md`. A delta `Sum` sums,
//! `Gauge` keeps the value with the latest source timestamp, `Distribution` merges via
//! `DdSketch::merge`. `Samples` absorbs into a `Distribution` (the default) or a raw `Samples`
//! accumulator (`distributions: samples`); `SetMembers` absorbs into a `Set` (`HyperLogLog`, the
//! default) or a raw `SetMembers` accumulator (`sets: members`). Each raw accumulator falls back to
//! its summarized counterpart on overflow, and `Samples` also on a sample-rate mismatch (see
//! `process`'s merge match).
//!
//! A cumulative `Sum`, `ExponentialHistogram`, and `Summary` have no merge rule here and pass
//! through untouched rather than being dropped. Pass-through is per *metric*, not per *event*: this
//! stage absorbs every mergeable metric off an event and forwards what's left (the unmergeable
//! metrics, plus any log or span). Two functions decide by kind, each alone: [`opener_for`] whether
//! a record merges and what a new series opens with, and [`Accumulator::retained_kind`] whether a
//! series survives a flush. A delta `Sum` whose value is `NaN` or infinite passes through too, so a
//! cumulative total stays finite. Every record is counted once, as
//! `logit.transform.metrics.absorbed` or `logit.transform.metrics.passed_through{reason}` (see
//! [`Tally`]).
//!
//! # Temporality: what a flushed `Sum`/`Histogram` means
//!
//! `temporality: delta` ([`AggregateTemporality::Delta`], the default) is strictly tumbling: each
//! window's emitted `Sum` is that window's own increment, the accumulator resets at flush, and a
//! `Histogram` of either temporality passes through (the ADR's "Amendment: cumulative temporality
//! as an opt-in mode" section says why merging delta histograms here is a separate decision).
//!
//! `temporality: cumulative` keeps a delta `Sum`'s and a delta `Histogram`'s accumulator alive
//! across the flush, under the same two bounds a retained gauge uses
//! (`series_retention`/`max_retained_series`), and emits the running total every window as
//! `Cumulative`, with `MetricRecord::start_timestamp` set to the series' first-seen event
//! timestamp. That stamp is the reset signal OTLP and Prometheus consumers detect a counter restart
//! with: it never changes while the series lives, and a series evicted (TTL or cardinality cap) and
//! later re-created gets a new one.
//!
//! A histogram's `sum`, `min`, and `max` each become `None` once a contributing record lacks one,
//! because a value over part of the observations is a wrong number a consumer can't detect. For
//! `min`/`max`, a record whose buckets total zero observed nothing and doesn't count.
//!
//! A histogram's bucket counts add with `saturating_add`, not `+`: they are wire-supplied `u64`s,
//! so a producer sending `u64::MAX` twice pins the bucket at `u64::MAX` (wrong but still monotonic)
//! instead of panicking or wrapping the total backwards under an unchanged `start_timestamp`.
//! `Sum`'s `f64` saturates to `inf` on its own.
//!
//! An incoming *cumulative* `Sum` passes through in both modes: re-summing a running total would
//! double-count it. `docs/adr/prometheus-scrape-and-exposition.md` covers the consumer that needs
//! this mode (`prometheus_out` skips delta records).

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

/// How an absorbed `Samples` series is held. Mirrors `logit_config::Distributions`; `logit-cli`
/// converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Distributions {
    /// Sketch every `Samples` value on absorb (`Samples::sketch`): bounded memory however many
    /// samples a series sees.
    #[default]
    Sketch,
    /// Keep raw values for the window (bounded by `max_samples_per_series`), sketching only on
    /// overflow or a sample-rate mismatch.
    Samples,
}

/// What a flushed `Sum`/`Histogram` means. Mirrors `logit_config::AggregateTemporality`.
///
/// Not `logit_core::Temporality`, the per-record wire field: this is the stage's mode, which sets a
/// *flushed* record's `temporality` and decides whether a series' accumulator survives the flush.
/// See this module's "Temporality" section.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AggregateTemporality {
    /// Tumbling: each window's emitted `Sum` is that window's increment; a `Histogram` passes
    /// through.
    #[default]
    Delta,
    /// A delta `Sum`/`Histogram` series' accumulator survives the flush and keeps summing; every
    /// window emits the running total as `Cumulative`, stamped with the series' first-seen time.
    Cumulative,
}

/// How an absorbed `SetMembers` series is held. Mirrors `logit_config::Sets`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Sets {
    /// Insert every `SetMembers` member into a `HyperLogLog` on absorb: bounded memory however
    /// many distinct members a series sees.
    #[default]
    Estimate,
    /// Keep the exact, deduplicated member set for the window (bounded by
    /// `max_set_members_per_series`), falling back to an estimate on overflow.
    Members,
}

/// A bounded, best-effort set of the distinct `TraceContext`s that contributed to one series since
/// the last flush, destined to become `SpanLink`s
/// (`docs/adr/trace-context-propagation-on-delivered.md`). The cap gates insertion only: an
/// already-seen context is free to re-observe, and one past the cap is dropped and counted, as in
/// `ComponentBuffer::upsert`.
///
/// Per series, not per resource group or `Aggregator`: a link belongs to the series whose flush
/// would become a span. Attributing it to unrelated series in the same window is the wrong-parent
/// shape that ADR rejects.
#[derive(Default)]
struct ContributingContexts {
    /// Inline capacity 1: most series see one contributing source, so this doesn't allocate
    /// (`docs/adr/minimize-allocations-over-event-size.md`) until a fanned-in series needs more.
    seen: SmallVec<[TraceContext; 1]>,
    dropped: u64,
}

/// Caps how many distinct contexts one series tracks between flushes. Fixed, not configurable, the
/// same stance `docs/known-gaps.md` takes on `MAX_KEYS_PER_COMPONENT`.
const MAX_CONTRIBUTING_CONTEXTS_PER_SERIES: usize = 8;

impl ContributingContexts {
    /// Records `ctx` as a contributor, unless it's already tracked or the cap is full (dropped and
    /// counted).
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

    /// Consumes the tracked set into the `SpanLink`s `flush` pairs with this series' emitted
    /// event, plus how many distinct contexts the cap rejected.
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

/// One tumbling-window aggregator, owned by one pipeline stage.
///
/// `process` accumulates what it can and passes the rest through; `flush` drains every
/// non-retainable accumulator and every retained series past its retention window. The retained
/// exceptions are a gauge series (either mode) and, under `temporality: cumulative`, a
/// `Sum`/`Histogram` series, both bounded by `series_retention`/`max_retained_series`. See
/// `docs/adr/aggregation-window-semantics.md`'s "Amendment: gauge series carry across the window
/// boundary" section for why gauges and not delta counters.
pub struct Aggregator {
    interval: Duration,
    groups: Vec<ResourceGroup>,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// The most recent batch's `TraceContext`, per `observe_batch_context`. Not reset by `flush`:
    /// it belongs to no one window. Default until the first batch arrives.
    current_batch_context: TraceContext,
    /// How many consecutive idle windows a retainable series is kept past its last update, so a
    /// gauge delta in window N+1 can resolve against window N's final value and a cumulative
    /// `Sum`/`Histogram` survives the flush at all. `0` (the default) means strictly tumbling: no
    /// series survives a flush.
    series_retention: u32,
    /// Cap on retained series across all resource groups: a cardinality guard, not a tuning knob.
    /// `series_retention` bounds how long one series survives; this bounds how many can be retained
    /// at once, against a stream of never-repeating series names. Evicts least-recently-updated
    /// first. Meaningless while `series_retention` is `0`.
    max_retained_series: usize,
    /// Whether a flushed `Sum`/`Histogram` is this window's increment (the default) or a running
    /// total that survives the flush.
    temporality: AggregateTemporality,
    distributions: Distributions,
    /// Bounds a raw `Samples` accumulator before it falls back to sketching what it holds.
    /// Meaningless while `distributions` is `Sketch`.
    max_samples_per_series: usize,
    sets: Sets,
    /// Bounds a raw `SetMembers` accumulator before it falls back to a `HyperLogLog` of what it
    /// holds. Meaningless while `sets` is `Estimate`.
    max_set_members_per_series: usize,
    /// The most recent batch's `Scope`, per `observe_scope`; not reset by `flush`, like
    /// `current_batch_context`. Read (and cloned into any newly opened `ResourceGroup`) rather than
    /// passed to `process`: scope is a per-batch fact, and the per-event hot path shouldn't pay for
    /// a value that never changes within one batch (`Transform::observe_batch_context`'s doc makes
    /// the same argument). `None` until the first batch arrives.
    current_scope: Option<Arc<Scope>>,
}

/// Keyed by `(resource, scope)` value, not `Arc` identity (as `group_for` does for resource): two
/// batches that each build an equal `Scope` describe the same instrumentation scope and aggregate
/// together.
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
    /// Consecutive flushes this series survived with no update; reset to 0 on any update. Only
    /// incremented for a retained series; compared against `Aggregator::series_retention` at flush
    /// to decide eviction.
    idle_windows: u32,
    /// Unix-nanos timestamp of the first event absorbed into this series, emitted as
    /// `MetricRecord::start_timestamp` on every cumulative-mode `Sum`/`Histogram` flush. A series
    /// evicted and re-created gets a fresh `SeriesState` and so a fresh value: that's the restart
    /// signal. Taken from `event.timestamp`, not `SystemTime::now()`, which would put a syscall on
    /// the open-a-series path and make this untestable. Recorded in both modes so the field has one
    /// meaning regardless of config.
    first_seen: i64,
    /// Whether any event touched this series since the last flush. Not derived from `contexts.seen`
    /// being non-empty: that correlates today, but ties retention to a set built for span linking.
    updated_this_window: bool,
    /// The opening record's `description`, emitted on every flush of this series. A later record's
    /// is ignored. Exemplars aren't carried: a summarized window has no single observation to
    /// attach one to.
    description: Option<Symbol>,
}

enum Accumulator {
    /// A delta `Sum`: sums `value` and keeps the *first* record's `monotonic` flag. Only a delta
    /// `Sum` reaches an accumulator, so the emitted `temporality` comes from the stage's mode, not
    /// the input records (see `into_kind`).
    Sum {
        total: f64,
        monotonic: bool,
    },
    /// Per-bucket running totals, constructed only under [`AggregateTemporality::Cumulative`].
    /// Held as a whole `logit_core::Histogram`, already stamped `Cumulative`, so `into_kind` is a
    /// move. `buckets` keeps the bounds of the record that opened the series; a later record with
    /// different bounds passes through (see `process`'s `Histogram` arm).
    Histogram(logit_core::Histogram),
    /// `at` is the source event's timestamp, which picks the last-write-wins value; the window has
    /// no timestamp until flush.
    Gauge {
        value: f64,
        at: i64,
    },
    Distribution(logit_core::DdSketch),
    /// Raw samples, only under `distributions: samples`. `sample_rate` is the first record's; a
    /// record with a different rate, or growth past `max_samples_per_series`, converts this to
    /// `Distribution` instead of merging.
    Samples(Samples),
    /// `sets: estimate`, the default.
    Set(logit_core::HyperLogLog),
    /// Raw members, only under `sets: members`: an exact, deduplicated, insertion-ordered union
    /// bounded by `max_set_members_per_series`. Overflow converts this to `Set`.
    SetMembers(Vec<Bytes>),
}

/// The accumulator a new series opens with, decided from the record that opens it without building
/// anything: [`opener_for`] runs on every merged record, and only a vacant series calls
/// [`Opener::open`]. Borrows a `Histogram` record so the decision doesn't clone its bucket `Vec`.
#[derive(Clone, Copy)]
enum Opener<'k> {
    Sum {
        monotonic: bool,
    },
    Gauge,
    Distribution,
    /// `sample_rate` comes from the opening record, or the first merge would look like a
    /// `rate_mismatch` against a made-up default.
    RawSamples {
        sample_rate: f64,
    },
    Set,
    RawSetMembers,
    Histogram(&'k logit_core::Histogram),
}

/// How a record of `kind` opens a series under this stage's modes, or `None` when the kind has no
/// merge rule here and the record is forwarded untouched. The one place pass-through is decided.
///
/// A cumulative `Sum` passes through in both modes: re-accumulating a running total double-counts
/// it. `Histogram` is mode-dependent: a delta one merges under
/// [`AggregateTemporality::Cumulative`] and passes through under `Delta`, and a cumulative one
/// passes through in both, as a cumulative `Sum` does. `distributions`/`sets` pick the opened shape
/// for `Samples`/`SetMembers`: raw only when the mode asks for it. An already-summarized
/// `Distribution`/`Set` opens summarized in any mode.
fn opener_for(
    kind: &MetricKind,
    distributions: Distributions,
    sets: Sets,
    temporality: AggregateTemporality,
) -> Option<Opener<'_>> {
    match kind {
        MetricKind::Sum(Sum { temporality: Temporality::Delta, monotonic, .. }) => {
            Some(Opener::Sum { monotonic: *monotonic })
        }
        MetricKind::Sum(Sum { temporality: Temporality::Cumulative, .. }) => None,
        // `Gauge` and `GaugeDelta` share one accumulator: two ways to update one running value,
        // not a kind conflict (`docs/adr/relative-gauge-adjustments.md`).
        MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => Some(Opener::Gauge),
        MetricKind::Distribution(_) => Some(Opener::Distribution),
        MetricKind::Samples(s) => Some(match distributions {
            Distributions::Sketch => Opener::Distribution,
            Distributions::Samples => Opener::RawSamples { sample_rate: s.sample_rate },
        }),
        MetricKind::Set(_) => Some(Opener::Set),
        MetricKind::SetMembers(_) => Some(match sets {
            Sets::Estimate => Opener::Set,
            Sets::Members => Opener::RawSetMembers,
        }),
        MetricKind::Histogram(h) => (h.temporality == Temporality::Delta
            && temporality == AggregateTemporality::Cumulative)
            .then_some(Opener::Histogram(h)),
        MetricKind::ExponentialHistogram(_) | MetricKind::Summary(_) => None,
    }
}

impl Opener<'_> {
    /// The empty accumulator; `process` merges the opening record into it right after.
    fn open(self) -> Accumulator {
        match self {
            Opener::Sum { monotonic } => Accumulator::Sum { total: 0.0, monotonic },
            Opener::Gauge => Accumulator::Gauge { value: 0.0, at: i64::MIN },
            Opener::Distribution => Accumulator::Distribution(logit_core::DdSketch::new()),
            Opener::RawSamples { sample_rate } => {
                Accumulator::Samples(Samples { values: SmallVec::new(), sample_rate })
            }
            Opener::Set => Accumulator::Set(logit_core::HyperLogLog::new()),
            Opener::RawSetMembers => Accumulator::SetMembers(Vec::new()),
            // This record's bounds with zero counts. `sum` starts `Some(0.0)` when the record has
            // one, so the both-sides-or-`None` merge rule keeps it on the first merge. Stamped
            // `Cumulative`: that's the only shape it's emitted as.
            Opener::Histogram(h) => Accumulator::Histogram(logit_core::Histogram {
                buckets: h.buckets.iter().map(|(bound, _)| (*bound, 0)).collect(),
                temporality: Temporality::Cumulative,
                sum: h.sum.map(|_| 0.0),
                min: None,
                max: None,
            }),
        }
    }
}

/// Per-`process`-call totals behind `logit.transform.metrics.absorbed` and
/// `logit.transform.metrics.passed_through{reason}`: every record `process` receives lands in one
/// field. Reported once per non-zero field after the loop, because `Telemetry::count` locks and
/// hashes on every call.
#[derive(Default)]
struct Tally {
    absorbed: u32,
    no_recorded_value: u32,
    no_merge_rule: u32,
    kind_conflict: u32,
    histogram_bounds_mismatch: u32,
    non_finite: u32,
}

impl Tally {
    fn report(&self, telemetry: &Telemetry) {
        if self.absorbed > 0 {
            telemetry.count("logit.transform.metrics.absorbed", f64::from(self.absorbed), &[]);
        }
        for (reason, n) in [
            ("no_recorded_value", self.no_recorded_value),
            ("no_merge_rule", self.no_merge_rule),
            ("kind_conflict", self.kind_conflict),
            ("histogram_bounds_mismatch", self.histogram_bounds_mismatch),
            ("non_finite", self.non_finite),
        ] {
            if n > 0 {
                telemetry.count(
                    "logit.transform.metrics.passed_through",
                    f64::from(n),
                    &[("reason", reason)],
                );
            }
        }
    }
}

/// Whether two histograms have the same bucket layout, and so can be added bucket by bucket.
/// Compared bitwise, like `SeriesKey`'s `f64` attributes: a bound is an identity, not a
/// measurement, so a `NaN` bound must equal itself rather than mismatch on every record.
fn bucket_bounds_match(held: &[(f64, u64)], incoming: &[(f64, u64)]) -> bool {
    held.len() == incoming.len()
        && held.iter().zip(incoming).all(|((a, _), (b, _))| a.to_bits() == b.to_bits())
}

/// Adds `samples` to `sketch` value by value at [`Samples::weight`], the fold [`Samples::sketch`]
/// does, without building a temporary `DdSketch`. Returns how many values were non-finite:
/// `DdSketch::add_count` drops those, since a `NaN` or infinite observation has no bin.
fn sketch_samples(sketch: &mut logit_core::DdSketch, samples: &Samples) -> u32 {
    let weight = samples.weight();
    let mut non_finite = 0;
    for v in &samples.values {
        if v.is_finite() {
            sketch.add_weighted(*v, weight);
        } else {
            non_finite += 1;
        }
    }
    non_finite
}

/// Whether a histogram's buckets hold any observation. A record that observed nothing has no
/// `min`/`max` to contribute, so [`fold_extreme`] skips it.
fn observed(buckets: &[(f64, u64)]) -> bool {
    buckets.iter().any(|(_, count)| *count > 0)
}

/// Folds a histogram's `min`/`max` across a merge under the `sum` rule: once both sides observed
/// something, a side missing the value makes the result `None`, because an extreme taken over
/// part of the observations can pair one window's `min` with another's `max` and give
/// `min > max`. A side that observed nothing is ignored, so the empty accumulator a series opens
/// with takes the first observed record's value. `pick` decides when both sides have one.
fn fold_extreme(
    held: (bool, Option<f64>),
    incoming: (bool, Option<f64>),
    pick: fn(f64, f64) -> f64,
) -> Option<f64> {
    match (held, incoming) {
        (held, (false, _)) => held.1,
        ((false, _), incoming) => incoming.1,
        ((true, Some(held)), (true, Some(incoming))) => Some(pick(held, incoming)),
        _ => None,
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

    /// Enables cross-flush series retention; see the `series_retention`/`max_retained_series`
    /// fields. Without it, retention is `0`: strictly tumbling.
    pub fn with_series_retention(mut self, retention: u32, max_retained: usize) -> Self {
        self.series_retention = retention;
        self.max_retained_series = max_retained;
        self
    }

    /// Selects what a flushed `Sum`/`Histogram` means (see this module's "Temporality" section).
    /// Defaults to `Delta`.
    ///
    /// `Cumulative` is only useful with both `with_series_retention` bounds non-zero: a running
    /// total that can't survive a flush is this window's delta labeled `Cumulative`. Graph
    /// validation rejects that combination (`crates/logit-pipeline/src/graph.rs`, rule 39); this
    /// builder doesn't check it.
    pub fn with_temporality(mut self, temporality: AggregateTemporality) -> Self {
        self.temporality = temporality;
        self
    }

    /// Configures how an absorbed `Samples` series is retained. Defaults to
    /// `Distributions::Sketch` with a `1000`-value cap, matching `logit_config`'s defaults.
    pub fn with_distributions(
        mut self,
        mode: Distributions,
        max_samples_per_series: usize,
    ) -> Self {
        self.distributions = mode;
        self.max_samples_per_series = max_samples_per_series;
        self
    }

    /// Configures how an absorbed `SetMembers` series is retained. Defaults to `Sets::Estimate`
    /// with a `1000`-member cap.
    pub fn with_sets(mut self, mode: Sets, max_set_members_per_series: usize) -> Self {
        self.sets = mode;
        self.max_set_members_per_series = max_set_members_per_series;
        self
    }

    /// Records `ctx` as the context of the batch about to be `process`ed. Per batch, not per
    /// event: a batch shares one context, and the per-event hot path shouldn't carry a value that
    /// never changes within a batch (`Transform::observe_batch_context`'s doc).
    pub fn observe_batch_context(&mut self, ctx: TraceContext) {
        self.current_batch_context = ctx;
    }

    /// Records `scope` as the scope of the batch about to be `process`ed; the scope analog of
    /// `observe_batch_context`.
    pub fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        self.current_scope = scope;
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Attaches a telemetry handle, for `flush`'s `logit.transform.series.*` and
    /// `.resource.groups` gauges.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Absorbs every mergeable metric off `event` into window state and leaves the rest on it:
    /// pass-through kinds, a metric whose kind conflicts with its series, and any log or span.
    /// Returns `false` only when nothing remains on the event. An event with no metrics never
    /// touches window state.
    ///
    /// Grouped by `(resource, scope)` value, not `Arc` identity: two inputs that each build an
    /// empty `Resource` describe the same origin and aggregate together. The lookup is a linear
    /// scan over groups. `statsd_in` holds one `Arc<Resource>` per listener, so its matching group
    /// costs one [`resource_key_eq`] `Arc::ptr_eq`. `otlp_in` builds one `Arc` per
    /// `ResourceMetrics`, `logit_in` one per frame, and a Lua resource write one per batch, so each
    /// of those pays a full field compare per metric.
    pub fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
        if event.metrics.is_empty() {
            return true;
        }

        // Copied out once: they're per-batch, and the merge match below can't borrow `self`
        // alongside `state`.
        let ctx = self.current_batch_context;
        let scope = self.current_scope.clone();
        let temporality = self.temporality;
        let distributions = self.distributions;
        let max_samples_per_series = self.max_samples_per_series;
        let sets = self.sets;
        let max_set_members_per_series = self.max_set_members_per_series;

        // Taken, not filtered with `retain`: a `retain` closure would borrow `event` while
        // `self.group_for` needs `&mut self`. Anything not absorbed is pushed back in its original
        // relative order.
        let metrics = std::mem::take(&mut event.metrics);
        let mut tally = Tally::default();
        for record in metrics {
            // An OTLP `NO_RECORDED_VALUE` record has no reading to fold in; its default numeric
            // payload would count as a real sample (`MetricRecord::flags`'s doc).
            if record.is_no_recorded_value() {
                tally.no_recorded_value += 1;
                event.metrics.push(record);
                continue;
            }

            let Some(opener) = opener_for(&record.kind, distributions, sets, temporality) else {
                tally.no_merge_rule += 1;
                event.metrics.push(record);
                continue;
            };

            // Checked before the series key, so no series opens for it. A non-finite increment
            // would pin a cumulative total at `NaN` or infinity for the series' whole life.
            // Only a delta `Sum` gets here: `opener_for` passed a cumulative one through.
            let non_finite =
                matches!(record.kind, MetricKind::Sum(Sum { value, .. }) if !value.is_finite());
            if non_finite {
                tally.non_finite += 1;
                self.diag.warn_throttled(
                    "sum_non_finite",
                    format_args!(
                        "sum '{}' has a non-finite value -- forwarding it untouched",
                        logit_core::interner::resolve(record.name)
                    ),
                );
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
            // Whether this metric opened a new series. Not derivable afterward: a real
            // `Gauge(0.0)` at `at: i64::MIN` looks identical to an unseeded delta's result. Only
            // `GaugeDelta` uses it.
            let was_vacant = matches!(entry, std::collections::hash_map::Entry::Vacant(_));
            let state = entry.or_insert_with(|| SeriesState {
                accumulator: opener.open(),
                contexts: ContributingContexts::default(),
                idle_windows: 0,
                first_seen: event.timestamp,
                updated_this_window: false,
                description: record.description,
            });

            // Set inside the merge match, reported after it: the match can't borrow `self`.
            // Records whose sample rate `Samples::is_clamped`, held or incoming, and values a
            // sketch dropped as non-finite.
            let mut samples_clamped: u32 = 0;
            let mut samples_non_finite: u32 = 0;
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
                // Only a delta histogram under `temporality: cumulative` reaches here.
                MetricKind::Histogram(incoming) => match &mut state.accumulator {
                    Accumulator::Histogram(held) => {
                        if !bucket_bounds_match(&held.buckets, &incoming.buckets) {
                            // Different layouts have no correct merge: adding bucket i to bucket
                            // i would attribute counts to bounds they weren't observed under. The
                            // record stays on the event, with its own diagnostic key.
                            histogram_bounds_mismatch = true;
                            false
                        } else {
                            let held_observed = observed(&held.buckets);
                            let incoming_observed = observed(&incoming.buckets);
                            for (held_bucket, incoming_bucket) in
                                held.buckets.iter_mut().zip(incoming.buckets.iter())
                            {
                                // Wire-supplied counts (`otlp_in` copies them unclamped): see the
                                // module doc's "Temporality" section for why this saturates.
                                held_bucket.1 = held_bucket.1.saturating_add(incoming_bucket.1);
                            }
                            // `sum` adds only when both sides have one: a total missing a window's
                            // contribution is a wrong number, and a consumer can tell `None` from
                            // that. `min`/`max` follow the same rule over the records that
                            // observed something (`fold_extreme`).
                            held.sum = match (held.sum, incoming.sum) {
                                (Some(held_sum), Some(incoming_sum)) => {
                                    Some(held_sum + incoming_sum)
                                }
                                _ => None,
                            };
                            held.min = fold_extreme(
                                (held_observed, held.min),
                                (incoming_observed, incoming.min),
                                f64::min,
                            );
                            held.max = fold_extreme(
                                (held_observed, held.max),
                                (incoming_observed, incoming.max),
                                f64::max,
                            );
                            true
                        }
                    }
                    _ => false,
                },
                MetricKind::Gauge(v) => match &mut state.accumulator {
                    Accumulator::Gauge { value, at } => {
                        // Last-write-wins by source timestamp; a tie goes to the later arrival.
                        // Two gauges of one series in one event share `event.timestamp`, so the
                        // tiebreak decides between them too.
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
                        // A delta applies in arrival order and never advances `at` (the `..`),
                        // which keeps an absolute's last-write-wins rule meaningful however many
                        // deltas land between two absolutes
                        // (`docs/adr/relative-gauge-adjustments.md`).
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
                    // A raw `samples` series meets an already-sketched `Distribution` (say, from
                    // an upstream `aggregate`): sketch what's held and merge.
                    Accumulator::Samples(held) => {
                        let held_owned = std::mem::take(held);
                        let mut sketch = logit_core::DdSketch::new();
                        samples_non_finite += sketch_samples(&mut sketch, &held_owned);
                        samples_clamped += u32::from(held_owned.is_clamped());
                        sketch.merge(incoming);
                        state.accumulator = Accumulator::Distribution(sketch);
                        true
                    }
                    // Kind conflict: the series accumulates another kind under the same
                    // name/unit/tags. No correct merge exists, so this one metric stays on the
                    // event; a sibling metric that does merge is still absorbed.
                    _ => false,
                },
                MetricKind::Samples(incoming) => match &mut state.accumulator {
                    // `distributions: sketch`, or a `samples` series that already fell back.
                    // Per-value `add_weighted`, not `sketch.merge(&incoming.sketch())`, which
                    // would allocate a temporary `DdSketch` only to fold it in.
                    Accumulator::Distribution(sketch) => {
                        samples_non_finite += sketch_samples(sketch, incoming);
                        samples_clamped += u32::from(incoming.is_clamped());
                        true
                    }
                    // `distributions: samples`: concatenate while the rate agrees and the cap
                    // holds; otherwise sketch `held` plus `incoming` and record why. A clamp on
                    // the held records is reported here, when their weight is first applied.
                    Accumulator::Samples(held) => {
                        // Bitwise, like a series key's `f64`: a `NaN` rate must match itself.
                        let fallback = if held.sample_rate.to_bits()
                            != incoming.sample_rate.to_bits()
                        {
                            Some("rate_mismatch")
                        } else if held.values.len() + incoming.values.len() > max_samples_per_series
                        {
                            Some("cap")
                        } else {
                            None
                        };
                        match fallback {
                            None => held.values.extend(incoming.values.iter().copied()),
                            Some(reason) => {
                                let held_owned = std::mem::take(held);
                                let mut sketch = logit_core::DdSketch::new();
                                for side in [&held_owned, incoming] {
                                    samples_non_finite += sketch_samples(&mut sketch, side);
                                    samples_clamped += u32::from(side.is_clamped());
                                }
                                state.accumulator = Accumulator::Distribution(sketch);
                                samples_fallback_reason = Some(reason);
                            }
                        }
                        true
                    }
                    _ => false,
                },
                MetricKind::Set(incoming) => match &mut state.accumulator {
                    Accumulator::Set(hll) => {
                        hll.merge(incoming);
                        true
                    }
                    // A raw `members` series meets an already-estimated `Set`: estimate what's
                    // held and union `incoming` into it.
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
                    // `sets: estimate`, or a `members` series that already fell back.
                    Accumulator::Set(hll) => {
                        for m in incoming {
                            hll.insert(m);
                        }
                        true
                    }
                    // `sets: members`: an insertion-ordered union, deduplicated by linear scan,
                    // which the cap keeps affordable. The scan stops once the union passes the
                    // cap: every held member and the rest of the record go into a
                    // `HyperLogLog`, so the union survives the conversion, and one oversized
                    // record costs O(cap²) compares and cap-bounded memory, not its own size
                    // squared.
                    Accumulator::SetMembers(held) => {
                        let mut rest = incoming.iter();
                        let mut overflowed = false;
                        for m in rest.by_ref() {
                            if !held.contains(m) {
                                held.push(m.clone());
                                if held.len() > max_set_members_per_series {
                                    overflowed = true;
                                    break;
                                }
                            }
                        }
                        if overflowed {
                            let mut hll = logit_core::HyperLogLog::new();
                            for m in held.iter().chain(rest) {
                                hll.insert(m);
                            }
                            state.accumulator = Accumulator::Set(hll);
                            set_members_fallback = true;
                        }
                        true
                    }
                    _ => false,
                },
                _ => false,
            };
            if accumulated {
                tally.absorbed += 1;
                // Only on a real merge: a kind-conflicted metric isn't a contributor.
                state.contexts.observe(ctx);
                state.updated_this_window = true;
                if was_vacant && matches!(record.kind, MetricKind::GaugeDelta(_)) {
                    // A delta that opened a new series resolved against 0.0 (statsd's rule for an
                    // unseeded gauge): correct, but indistinguishable from a real 0.0, so it's
                    // counted. With `series_retention: 0` a delta-only series reopens every
                    // window and this fires every window; with retention, only for a new or
                    // aged-out series.
                    self.telemetry.count("logit.transform.gauge.delta.unseeded", 1.0, &[]);
                    self.diag.warn_throttled(
                        "gauge_delta_unseeded",
                        format_args!(
                            "gauge delta for '{}' opened a new series and resolved against 0.0 \
                             -- no prior absolute value seen for this series",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
                if samples_non_finite > 0 {
                    self.telemetry.count(
                        "logit.transform.samples.non_finite_dropped",
                        f64::from(samples_non_finite),
                        &[],
                    );
                }
                if samples_clamped > 0 {
                    // The one place a sample rate implying more than `Samples::MAX_WEIGHT`
                    // observations per value is noticed: `statsd_in` doesn't sketch or clamp.
                    self.telemetry.count(
                        "logit.transform.samples.weight_clamped",
                        f64::from(samples_clamped),
                        &[],
                    );
                    self.diag.warn_throttled(
                        "sample_rate_clamped",
                        format_args!(
                            "sample_rate for '{}' implies a weight beyond Samples::MAX_WEIGHT \
                             ({}); clamping",
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
                            "an incoming record's sample_rate disagreed with this series' first \
                             record",
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
                            "raw set members for '{}' fell back to a HyperLogLog estimate: \
                             max_set_members_per_series was exceeded",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
            }
            if !accumulated {
                if histogram_bounds_mismatch {
                    tally.histogram_bounds_mismatch += 1;
                    // Its own key, not `kind_conflict`: the kind matches and only the bounds differ
                    // (a producer that re-bucketed mid-run).
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
                    tally.kind_conflict += 1;
                    self.diag.warn_throttled(
                        "kind_conflict",
                        format_args!(
                            "metric '{}' has a kind that conflicts with an already-accumulating \
                             series under the same name/unit/tags -- forwarding it untouched",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
                event.metrics.push(record);
            }
        }
        tally.report(&self.telemetry);

        !(event.metrics.is_empty() && event.log.is_none() && event.span.is_none())
    }

    fn group_for(
        &mut self,
        resource: &Arc<Resource>,
        scope: &Option<Arc<Scope>>,
    ) -> &mut ResourceGroup {
        if let Some(i) = self
            .groups
            .iter()
            .position(|g| resource_key_eq(&g.resource, resource) && scope_key_eq(&g.scope, scope))
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

    /// Emits one event per series updated this window, stamped with `now` and paired with its
    /// `SpanLink`s.
    ///
    /// Every non-retainable accumulator is removed. A retainable series (a gauge in either mode, a
    /// `Sum`/`Histogram` under `temporality: cumulative`) survives into the next window when
    /// `series_retention > 0`, subject to `max_retained_series`.
    pub fn flush(&mut self, now: i64) -> FlushOutput {
        // Sampled before any series is touched: the peak-of-window value. `SeriesKey` includes the
        // event's whole attribute set, so an unpruned high-cardinality attribute shows up here
        // first (`crate::keep`'s module doc warns about this).
        //
        // `.active` counts only series that received data this window, so it stays a
        // high-cardinality early warning; `.retained` counts retained-but-idle series.
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

        // Series this flush keeps, not yet placed back: the cap is whole-`Aggregator`, so it must
        // see every group's candidates first. Each group's `series` map is taken (not
        // `self.groups` itself) so survivors go back into their group by index, with no parallel
        // Vecs allocated on the default `series_retention: 0` path, which never fills this.
        let mut survivors: Vec<(usize, SeriesKey, SeriesState)> = Vec::new();
        let mut total_dropped_links: u64 = 0;
        let mut evicted_idle: u64 = 0;
        let mut result = Vec::new();

        for (gi, group) in self.groups.iter_mut().enumerate() {
            let series = std::mem::take(&mut group.series);
            // At most one event per series, and exactly one each on the default path.
            let mut events = Vec::with_capacity(series.len());
            for (key, mut state) in series {
                if state.updated_this_window {
                    let (links, dropped) = std::mem::take(&mut state.contexts).into_links();
                    total_dropped_links += dropped;

                    // Asked only of an updated series: an idle one is here because it was
                    // retained, and asking would clone a `Histogram`'s buckets for nothing.
                    let retained = if self.series_retention > 0 {
                        state.accumulator.retained_kind(self.temporality)
                    } else {
                        None
                    };
                    if let Some(kind) = retained {
                        // `key.attributes` is cloned only on this path because `key` goes back
                        // into the map.
                        let mut record = MetricRecord::new(key.name, kind);
                        record.unit = key.unit;
                        record.description = state.description;
                        // The reset signal (`SeriesState::first_seen`). A `Gauge` has no start
                        // time and keeps `0`, OTLP's "unknown".
                        if matches!(record.kind, MetricKind::Sum(_) | MetricKind::Histogram(_)) {
                            record.start_timestamp = state.first_seen;
                        }
                        // An accumulated value is never a `NO_RECORDED_VALUE` point: `process`
                        // passes flagged records through.
                        record.flags = 0;
                        events.push((Event::metric(now, key.attributes.clone(), record), links));
                        // Last-write-wins is a within-window tiebreak: reset `at` so an absolute
                        // gauge next window with an earlier source timestamp is still accepted.
                        // A `Sum`/`Histogram` accumulates in arrival order and has no `at`.
                        if let Accumulator::Gauge { at, .. } = &mut state.accumulator {
                            *at = i64::MIN;
                        }
                        state.updated_this_window = false;
                        state.idle_windows = 0;
                        survivors.push((gi, key, state));
                    } else {
                        // Consumed: a sketch's backing `Vec`s move rather than clone.
                        let kind = state.accumulator.into_kind(self.temporality);
                        let mut record = MetricRecord::new(key.name, kind);
                        record.unit = key.unit;
                        record.description = state.description;
                        record.flags = 0;
                        events.push((Event::metric(now, key.attributes, record), links));
                    }
                } else {
                    // A retained, idle series emits nothing: not a repeat, not a zero. That holds
                    // for a cumulative `Sum`/`Histogram` too: re-emitting an unchanged total
                    // would multiply output by the retention depth, and a cumulative consumer
                    // treats the last value as standing until replaced.
                    state.contexts = ContributingContexts::default(); // never carried, even empty
                    state.idle_windows += 1;
                    if state.idle_windows < self.series_retention {
                        survivors.push((gi, key, state));
                    } else {
                        evicted_idle += 1;
                    }
                }
            }
            // Checked after building: a group holding only idle retained series has a non-empty
            // map but no events.
            if !events.is_empty() {
                result.push((group.resource.clone(), group.scope.clone(), events));
            }
        }

        // Cardinality cap, evicting the most idle first. Without it, C never-repeating series
        // names per window would hold C * series_retention series indefinitely.
        let mut evicted_cardinality: u64 = 0;
        if survivors.len() > self.max_retained_series {
            let excess = survivors.len() - self.max_retained_series;
            // Stable, so ties among equally idle series evict deterministically.
            survivors.sort_by_key(|(_, _, state)| std::cmp::Reverse(state.idle_windows));
            survivors.drain(0..excess);
            evicted_cardinality = excess as u64;
        }

        // Survivors go back to their group; a group left empty is dropped.
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
            // Warned: a later gauge delta against an evicted series resolves against 0.0, and an
            // evicted cumulative series restarts from zero with a new `start_timestamp`.
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

/// Pure delegation: the inherent methods already match `Transform`'s contract.
impl Transform for Aggregator {
    fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
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
    /// Consumes this accumulator into the `MetricKind` a tumbling flush emits. A `Sum` is labeled
    /// with the stage's mode, not the (always delta) records that fed it.
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

    /// What a series holding this accumulator emits when it survives the flush, or `None` when it
    /// doesn't survive (the one place retention by kind is decided). `flush` asks only when
    /// `series_retention > 0`.
    ///
    /// A gauge's value is sticky by protocol, and a cumulative `Sum`/`Histogram`'s running total
    /// is what that mode emits. `Distribution`/`Samples`/`Set`/`SetMembers` never survive: each
    /// window's summary is self-contained. Reads without consuming: one bucket-`Vec` clone for a
    /// `Histogram`, free otherwise.
    fn retained_kind(&self, temporality: AggregateTemporality) -> Option<MetricKind> {
        let cumulative = temporality == AggregateTemporality::Cumulative;
        match self {
            Accumulator::Gauge { value, .. } => Some(MetricKind::Gauge(*value)),
            Accumulator::Sum { total, monotonic } if cumulative => Some(MetricKind::Sum(Sum {
                value: *total,
                temporality: record_temporality(temporality),
                monotonic: *monotonic,
            })),
            Accumulator::Histogram(histogram) if cumulative => {
                Some(MetricKind::Histogram(histogram.clone()))
            }
            Accumulator::Sum { .. }
            | Accumulator::Histogram(_)
            | Accumulator::Distribution(_)
            | Accumulator::Samples(_)
            | Accumulator::Set(_)
            | Accumulator::SetMembers(_) => None,
        }
    }
}

/// The `logit_core::Temporality` a flushed record carries under a stage mode; one mapping so
/// `into_kind` and `retained_kind` can't disagree.
fn record_temporality(temporality: AggregateTemporality) -> Temporality {
    match temporality {
        AggregateTemporality::Delta => Temporality::Delta,
        AggregateTemporality::Cumulative => Temporality::Cumulative,
    }
}

/// A metric series' identity: name, unit, and attribute set.
///
/// `AttrMap`/`Value` implement neither `Eq` nor `Hash`, so this compares and hashes `f64` by
/// `to_bits()`: a `NaN` attribute then equals itself, instead of opening a new series per event.
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
        // `AttrMap::iter()` yields `Symbol` order, so insertion order doesn't change the hash.
        for (k, v) in self.attributes.iter() {
            k.hash(state);
            hash_value(v, state);
        }
    }
}

/// Field-wise equality for a `group_for` resource key, comparing `Value::F64` bitwise, the rule
/// `SeriesKey` and [`scope_key_eq`] follow. `Resource`'s derived `PartialEq` judges a `NaN`
/// attribute unequal to itself, even through one `Arc`, which would open a new `ResourceGroup` per
/// metric carrying it; and it judges `-0.0` equal to `0.0`, which series identity keeps apart.
///
/// `Arc::ptr_eq` first: an input that reuses one `Arc<Resource>` skips the field walk.
fn resource_key_eq(a: &Arc<Resource>, b: &Arc<Resource>) -> bool {
    Arc::ptr_eq(a, b)
        || (a.schema_url == b.schema_url
            && a.dropped_attributes_count == b.dropped_attributes_count
            && attr_map_key_eq(&a.attributes, &b.attributes))
}

/// Field-wise equality for a `group_for` scope key, comparing `Value::F64` bitwise. `Scope`'s
/// derived `PartialEq` judges a `NaN` attribute unequal to itself, which would open a new
/// `ResourceGroup` per event carrying it (the failure `SeriesKey` avoids the same way).
///
/// `Arc::ptr_eq` first: events from one batch share one `Arc<Scope>`, so the common case skips
/// the field walk.
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

/// `AttrMap` equality with bitwise floats, for [`resource_key_eq`] and [`scope_key_eq`]. `AttrMap::iter()` yields `Symbol`
/// order, so a length check plus a zipped walk is order-independent.
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
    // A variant tag first, so an empty `Array` and an empty `Map` don't collide.
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

    /// Runs `process` on an owned event: `Some` is what was forwarded, `None` means absorbed.
    fn feed(agg: &mut Aggregator, resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        agg.process(resource, &mut event).then_some(event)
    }

    /// `flush` with scopes and `SpanLink`s dropped; link tests call `agg.flush` directly.
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
        assert!(
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0)).is_none()
        );
        assert!(
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(2.0), 1)).is_none()
        );
        assert!(
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(3.0), 2)).is_none()
        );

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
        feed(&mut agg, &resource, metric_event("temp", MetricKind::Gauge(5.0), 50));
        feed(&mut agg, &resource, metric_event("temp", MetricKind::Gauge(1.0), 10));

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
            feed(&mut agg, &resource, metric_event("latency", MetricKind::Distribution(sketch), 0));
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
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

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
        let passed = feed(&mut agg, &resource, log);
        assert!(passed.is_some(), "a log event should pass through, not be absorbed");
        assert!(agg.flush(100).is_empty(), "nothing should have been accumulated");
    }

    /// The four kinds with no merge rule, cumulative `Sum` included, pass through `process`.
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
                feed(&mut agg, &resource, event).is_some(),
                "a kind with no defined merge rule should pass through"
            );
        }
        assert!(agg.flush(100).is_empty());
    }

    /// One record of every `MetricKind` variant, in both temporalities where the kind carries one.
    fn every_kind() -> Vec<MetricKind> {
        let histogram = |temporality| {
            MetricKind::Histogram(logit_core::Histogram {
                buckets: vec![(1.0, 1), (f64::INFINITY, 0)],
                temporality,
                sum: Some(0.5),
                min: Some(0.5),
                max: Some(0.5),
            })
        };
        let exp_histogram = |temporality| {
            MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                scale: 0,
                zero_count: 0,
                zero_threshold: 0.0,
                positive: (0, vec![1]),
                negative: (0, vec![]),
                temporality,
                count: 1,
                sum: None,
                min: None,
                max: None,
            })
        };
        let mut hll = logit_core::HyperLogLog::new();
        hll.insert(b"a");
        let sum = |temporality| MetricKind::Sum(Sum { value: 1.0, temporality, monotonic: true });
        vec![
            sum(Temporality::Delta),
            sum(Temporality::Cumulative),
            MetricKind::Gauge(1.0),
            MetricKind::GaugeDelta(1.0),
            MetricKind::Samples(Samples::new([1.0])),
            MetricKind::Distribution(Samples::new([1.0]).sketch()),
            MetricKind::SetMembers(vec![Bytes::from_static(b"a")]),
            MetricKind::Set(hll),
            histogram(Temporality::Delta),
            histogram(Temporality::Cumulative),
            exp_histogram(Temporality::Delta),
            exp_histogram(Temporality::Cumulative),
            MetricKind::Summary(logit_core::Summary {
                quantiles: vec![(0.5, 1.0)],
                count: 1,
                sum: 1.0,
            }),
        ]
    }

    fn every_mode() -> Vec<(AggregateTemporality, Distributions, Sets)> {
        let mut modes = Vec::new();
        for t in [AggregateTemporality::Delta, AggregateTemporality::Cumulative] {
            for d in [Distributions::Sketch, Distributions::Samples] {
                for s in [Sets::Estimate, Sets::Members] {
                    modes.push((t, d, s));
                }
            }
        }
        modes
    }

    #[test]
    fn opener_for_is_none_iff_process_forwards_the_record() {
        let resource = default_resource();
        for kind in every_kind() {
            for (t, d, s) in every_mode() {
                let mut agg = Aggregator::new(Duration::from_secs(10))
                    .with_temporality(t)
                    .with_distributions(d, 1000)
                    .with_sets(s, 1000);
                let passes = opener_for(&kind, d, s, t).is_none();
                let forwarded = feed(&mut agg, &resource, metric_event("m", kind.clone(), 0));
                assert_eq!(forwarded.is_some(), passes, "{kind:?} under {t:?}/{d:?}/{s:?}");
                assert_eq!(agg.groups.len(), usize::from(!passes), "{kind:?} under {t:?}");
            }
        }
    }

    /// One accumulator of every `Accumulator` variant.
    fn accumulator(variant: usize) -> Accumulator {
        match variant {
            0 => Accumulator::Sum { total: 1.0, monotonic: true },
            1 => Accumulator::Histogram(logit_core::Histogram {
                buckets: vec![(1.0, 1)],
                temporality: Temporality::Cumulative,
                sum: None,
                min: None,
                max: None,
            }),
            2 => Accumulator::Gauge { value: 1.0, at: 0 },
            3 => Accumulator::Distribution(Samples::new([1.0]).sketch()),
            4 => Accumulator::Samples(Samples::new([1.0])),
            5 => Accumulator::Set(logit_core::HyperLogLog::new()),
            _ => Accumulator::SetMembers(vec![Bytes::from_static(b"a")]),
        }
    }

    #[test]
    fn retained_kind_is_some_iff_the_series_survives_a_flush() {
        for variant in 0..7 {
            for (t, _, _) in every_mode() {
                for retention in [0, 1, 3] {
                    let mut agg = Aggregator::new(Duration::from_secs(10))
                        .with_temporality(t)
                        .with_series_retention(retention, 10);
                    let retained = accumulator(variant).retained_kind(t).is_some();
                    let key =
                        SeriesKey { name: intern("m"), unit: None, attributes: AttrMap::new() };
                    let state = SeriesState {
                        accumulator: accumulator(variant),
                        contexts: ContributingContexts::default(),
                        idle_windows: 0,
                        first_seen: 0,
                        updated_this_window: true,
                        description: None,
                    };
                    agg.groups.push(ResourceGroup {
                        resource: default_resource(),
                        scope: None,
                        series: HashMap::from([(key, state)]),
                    });

                    let emitted = flush_events(&mut agg, 10);
                    assert_eq!(emitted.len(), 1, "an updated series emits once");
                    let survived = agg.groups.iter().map(|g| g.series.len()).sum::<usize>();
                    assert_eq!(
                        survived,
                        usize::from(retention > 0 && retained),
                        "variant {variant} under {t:?}, retention {retention}"
                    );
                }
            }
        }
    }

    /// A `Samples` metric is absorbed, not passed through.
    #[test]
    fn samples_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let event =
            metric_event("latency", MetricKind::Samples(logit_core::Samples::new([1.0])), 0);
        assert!(feed(&mut agg, &resource, event).is_none(), "a Samples-only event should absorb");
    }

    /// A `SetMembers` metric is absorbed, not passed through.
    #[test]
    fn set_members_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let event = metric_event(
            "unique.users",
            MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a")]),
            0,
        );
        assert!(
            feed(&mut agg, &resource, event).is_none(),
            "a SetMembers-only event should absorb"
        );
    }

    /// An already-summarized `Set` is absorbed, not passed through.
    #[test]
    fn set_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut hll = logit_core::HyperLogLog::default();
        hll.insert(b"member");
        let event = metric_event("unique.users", MetricKind::Set(hll), 0);
        assert!(feed(&mut agg, &resource, event).is_none(), "a Set-only event should absorb");
    }

    /// A cumulative `Sum` passes through even when a delta `Sum` series of the same name exists.
    #[test]
    fn a_cumulative_sum_never_merges_into_an_existing_delta_sum_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(feed(&mut agg, &resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());

        let cumulative = metric_event(
            "m",
            MetricKind::Sum(Sum {
                value: 5.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
            0,
        );
        let passed = feed(&mut agg, &resource, cumulative);
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

    /// A merged `Sum` keeps the first record's `monotonic` flag.
    #[test]
    fn sum_merge_carries_the_first_records_monotonic_flag() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(
            &mut agg,
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
        feed(
            &mut agg,
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

    /// A `GaugeDelta` metric is absorbed, not passed through.
    #[test]
    fn gauge_delta_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let event = metric_event("temp", MetricKind::GaugeDelta(5.0), 0);
        assert!(
            feed(&mut agg, &resource, event).is_none(),
            "a GaugeDelta-only event should absorb"
        );
    }

    /// A delta opening a new series resolves against 0.0 and is counted as unseeded.
    #[test]
    fn a_delta_into_an_empty_window_resolves_against_zero_and_fires_the_unseeded_counter() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        assert!(feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0))
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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 15.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A later absolute replaces the running value rather than adding to it.
    #[test]
    fn delta_then_absolute_is_subsumed_by_the_absolute() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0));
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 10.0, "the absolute should win outright"),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A delta never advances `at`: the t=50 absolute after a t=60 delta still wins (99, not 15).
    #[test]
    fn a_delta_never_advances_the_last_write_wins_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 50));
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 60));
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(99.0), 50));

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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 1));
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(-3.0), 2));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 12.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A `GaugeDelta` against a `Sum` series is a kind conflict and is forwarded.
    #[test]
    fn gauge_delta_against_a_counter_series_is_a_kind_conflict_and_is_forwarded() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(feed(&mut agg, &resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());

        let conflicting = metric_event("m", MetricKind::GaugeDelta(5.0), 0);
        let passed = feed(&mut agg, &resource, conflicting);
        assert!(passed.is_some(), "the conflicting delta should be forwarded, not absorbed");
        assert!(matches!(passed.unwrap().metrics[0].kind, MetricKind::GaugeDelta(v) if v == 5.0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(counter_value(kind_of(&events[0])), 1.0, "the counter should be untouched");
    }

    /// A gauge series always flushes as `Gauge`, never `GaugeDelta`.
    #[test]
    fn a_gauge_delta_never_survives_aggregate() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0));

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
        feed(
            &mut agg,
            &resource,
            metric_event_with_tags("hits", MetricKind::counter(1.0), 0, &[("host", "a")]),
        );
        feed(
            &mut agg,
            &resource,
            metric_event_with_tags("hits", MetricKind::counter(1.0), 0, &[("host", "b")]),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 2, "different tag values should be different series");
    }

    /// Identical `Value::Array` tags (a repeated DogStatsD tag key) fold into one series.
    #[test]
    fn identical_multi_valued_array_tags_fold_into_one_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let team = Value::Array(vec![Value::str("a"), Value::str("b")]);
        let mut first = metric_event("hits", MetricKind::counter(1.0), 0);
        first.attributes.insert("team", team.clone());
        feed(&mut agg, &resource, first);
        let mut second = metric_event("hits", MetricKind::counter(1.0), 0);
        second.attributes.insert("team", team);
        feed(&mut agg, &resource, second);

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "the identical array tag should be one series");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0, "the two counters should have merged");
    }

    /// Array element order is part of series identity: wire order is meaningful.
    #[test]
    fn multi_valued_array_tags_differing_only_in_element_order_stay_two_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut first = metric_event("hits", MetricKind::counter(1.0), 0);
        first.attributes.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
        feed(&mut agg, &resource, first);
        let mut second = metric_event("hits", MetricKind::counter(1.0), 0);
        second.attributes.insert("team", Value::Array(vec![Value::str("b"), Value::str("a")]));
        feed(&mut agg, &resource, second);

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 2, "array element order should be part of the series identity");
    }

    #[test]
    fn same_tags_in_different_insertion_order_collide_into_one_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event_with_tags(
                "hits",
                MetricKind::counter(1.0),
                0,
                &[("host", "a"), ("env", "prod")],
            ),
        );
        feed(
            &mut agg,
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

        feed(&mut agg, &Arc::new(resource_a), metric_event("hits", MetricKind::counter(1.0), 0));
        feed(&mut agg, &Arc::new(resource_b), metric_event("hits", MetricKind::counter(1.0), 0));

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

        feed(&mut agg, &resource, e1);
        feed(&mut agg, &resource, e2);

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "two NaN-tagged events should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    /// A `NO_RECORDED_VALUE` record passes through unmerged and counted, never folded in as `0.0`.
    #[test]
    fn a_no_recorded_value_record_is_passed_through_unmerged_and_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        let mut flagged = metric_event("conns", MetricKind::Gauge(0.0), 0);
        flagged.metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let passed = feed(&mut agg, &resource, flagged);
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
        // A statsd source sending both `foo:1|c` and `foo:1|g`.
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(feed(&mut agg, &resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());
        let conflicting = metric_event("m", MetricKind::Gauge(5.0), 0);
        let passed = feed(&mut agg, &resource, conflicting);
        assert!(passed.is_some(), "the conflicting event should be forwarded, not absorbed");

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    /// On a log-plus-counter event the counter is absorbed and the log still forwarded.
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

        let passed = feed(&mut agg, &resource, event).expect("the log half should be forwarded");
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

        let passed = feed(&mut agg, &resource, event)
            .expect("the histogram should survive as the remainder");
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
            feed(&mut agg, &resource, event).is_none(),
            "an event with nothing left to forward should still return None"
        );
    }

    #[test]
    fn two_metrics_of_the_same_series_on_one_event_sum_into_one_flushed_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("hits", MetricKind::counter(1.0), 0);
        event.metrics.push(MetricRecord::new(intern("hits"), MetricKind::counter(2.0)));

        assert!(feed(&mut agg, &resource, event).is_none());

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "both metrics should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 3.0);
    }

    #[test]
    fn a_kind_conflict_leaves_only_the_conflicting_metric_on_the_event() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(feed(&mut agg, &resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());

        let mut event = metric_event("m", MetricKind::counter(1.0), 0);
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::Gauge(5.0)));

        let passed =
            feed(&mut agg, &resource, event).expect("the conflicting gauge should be forwarded");
        assert_eq!(passed.metrics.len(), 1, "the absorbed counter should not also be forwarded");
        assert!(matches!(passed.metrics[0].kind, MetricKind::Gauge(v) if v == 5.0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 2.0, "both counters should have merged");
    }

    /// Two batches' contexts contributing to one series are both linked at flush.
    #[test]
    fn flush_links_every_distinct_context_that_contributed_to_a_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();

        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        let (_, links) = &events[0];
        assert_eq!(links.len(), 2, "both contributing traces should be linked");
        let trace_ids: Vec<[u8; 16]> = links.iter().map(|l| l.trace_id).collect();
        assert!(trace_ids.contains(&ctx_a.trace_id));
        assert!(trace_ids.contains(&ctx_b.trace_id));
    }

    /// One context observed twice on a series yields one link.
    #[test]
    fn repeat_events_under_the_same_context_dont_duplicate_a_link() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.observe_batch_context(TraceContext::new_root());
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "the same context observed twice should still be one link");
    }

    /// Past `MAX_CONTRIBUTING_CONTEXTS_PER_SERIES` (8), a ninth context is dropped and counted.
    #[test]
    fn a_series_fed_by_more_than_the_cap_drops_and_counts_the_rest() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        for _ in 0..9 {
            agg.observe_batch_context(TraceContext::new_root());
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
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

    /// A series' contributing contexts reset at flush along with its value.
    #[test]
    fn contributing_contexts_reset_after_a_flush() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
        agg.flush(100); // first window's links discarded along with its accumulator

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

        let flushed = agg.flush(200);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "tumbling: the first window's context shouldn't carry over");
        assert_eq!(
            links[0].trace_id, ctx_b.trace_id,
            "only the second window's context should be linked"
        );
    }

    // Takes drained `events`, not a `&Registry`: `Registry::drain` empties every buffer, so one
    // drain per assertion would leave later assertions nothing to see.
    fn gauge_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    /// `logit.transform.series.evicted{reason}`'s value in drained telemetry.
    fn evicted_count(events: &[Event], reason: &str) -> Option<f64> {
        counter_with_tag(events, "logit.transform.series.evicted", "reason", reason)
    }

    /// `logit.component.diagnostics{key}`'s value: `warn_throttled` counts every occurrence, even
    /// a throttled one.
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

        feed(&mut agg, &resource, metric_event("a", MetricKind::counter(1.0), 0));
        feed(&mut agg, &resource, metric_event("b", MetricKind::counter(1.0), 0));
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

        feed(&mut agg, &resource_a, metric_event("a", MetricKind::counter(1.0), 0));
        feed(&mut agg, &resource_b, metric_event("b", MetricKind::counter(1.0), 0));
        feed(&mut agg, &resource_b, metric_event("c", MetricKind::counter(1.0), 0));
        agg.flush(100);

        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.series.active"), Some(3.0));
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(2.0));
    }

    // -- Series retention across the window boundary -------------------------------------------

    #[test]
    fn a_delta_in_the_next_window_resolves_against_the_previous_windows_final_value() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        let flushed = flush_events(&mut agg, 100);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 10.0),
            other => panic!("expected Gauge, got {other:?}"),
        }

        // Window 2: only a delta.
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 150));
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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        assert_eq!(flush_events(&mut agg, 100).len(), 1, "window 1 emits the gauge");

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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        assert_eq!(agg.flush(100).len(), 1, "window 1: emits, idle_windows resets to 0");
        assert!(agg.flush(200).is_empty(), "window 2: idle_windows -> 1, still under retention 2");
        assert!(agg.flush(300).is_empty(), "window 3: idle_windows -> 2, now evicted");

        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 350));
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

    /// `series_retention: 0` retains nothing across flushes.
    #[test]
    fn series_retention_zero_reproduces_the_strictly_tumbling_output() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(0, 0);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("temp", MetricKind::Gauge(5.0), 50));
        feed(&mut agg, &resource, metric_event("temp", MetricKind::Gauge(1.0), 10));

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

    /// A retained gauge resets `at`, so next window's absolute with an earlier timestamp wins.
    #[test]
    fn an_absolute_gauge_in_the_next_window_with_an_earlier_timestamp_is_still_accepted() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 500));
        flush_events(&mut agg, 1000);

        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(99.0), 1));
        let flushed = flush_events(&mut agg, 2000);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(
                *v, 99.0,
                "an earlier-timestamped absolute must still win against a reset `at`"
            ),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// In `delta` mode, retention never keeps a counter series alive.
    #[test]
    fn a_delta_mode_counter_series_does_not_survive_its_window_even_with_series_retention() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
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
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
        agg.flush(100);
        // This flush's `resource.groups` sample shows the state the first flush left.
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
            feed(
                &mut agg,
                &resource,
                metric_event(&format!("g{i}"), MetricKind::Gauge(i as f64), 0),
            );
        }
        // Three retainable series against a cap of 2.
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

    /// A retained gauge's contributing contexts still reset at every flush.
    #[test]
    fn contexts_are_never_carried_across_a_flush_even_for_a_retained_gauge() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 0));

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "window 1 links its one contributing context");

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        feed(&mut agg, &resource, metric_event("conns", MetricKind::GaugeDelta(1.0), 150));
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

    /// A group holding only idle retained series emits no empty batch.
    #[test]
    fn flush_emits_no_empty_resource_events_group() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        agg.flush(100); // retains "conns", idle from here on

        let flushed = agg.flush(200);
        assert!(
            flushed.iter().all(|(_, _, events)| !events.is_empty()),
            "flush must never emit a (resource, events) pair with an empty events list"
        );
        assert!(flushed.is_empty(), "the only group present here has nothing to emit at all");
    }

    // -- Samples/SetMembers/Set absorb ---------------------------------------------------------

    /// `distributions: sketch` sketches weighted values and clamps and counts an oversized weight.
    #[test]
    fn samples_sketch_mode_merges_weighted_values_and_counts_weight_clamp() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        // Unsampled: weight 1, contributes 2 raw observations to the sketch.
        let mut unsampled = Samples::new([10.0, 20.0]);
        unsampled.sample_rate = 1.0;
        feed(&mut agg, &resource, metric_event("latency", MetricKind::Samples(unsampled), 0));

        // A rate implying a weight past MAX_WEIGHT.
        let mut clamped = Samples::new([30.0]);
        clamped.sample_rate = 0.0001;
        feed(&mut agg, &resource, metric_event("latency", MetricKind::Samples(clamped), 0));

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

    /// `distributions: samples` concatenates in arrival order and keeps the first `sample_rate`.
    #[test]
    fn samples_mode_concatenates_values_and_into_kind_emits_samples() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_distributions(Distributions::Samples, 1000);
        let resource = default_resource();
        let mut a = Samples::new([1.0, 2.0]);
        a.sample_rate = 0.5;
        feed(&mut agg, &resource, metric_event("latency", MetricKind::Samples(a), 0));
        let mut b = Samples::new([3.0]);
        b.sample_rate = 0.5;
        feed(&mut agg, &resource, metric_event("latency", MetricKind::Samples(b), 0));

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

    /// A `sample_rate` mismatch falls a raw series back to a sketch, counted and diagnosed.
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
        feed(&mut agg, &resource, metric_event("latency", MetricKind::Samples(a), 0));

        let mut b = Samples::new([3.0]);
        b.sample_rate = 0.5; // weight 2 -- different rate, triggers fallback
        feed(&mut agg, &resource, metric_event("latency", MetricKind::Samples(b), 0));

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

    /// Growing past `max_samples_per_series` falls a raw series back to a sketch, counted.
    #[test]
    fn samples_mode_cap_exceeded_falls_back_to_distribution_and_counts() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_distributions(Distributions::Samples, 2)
            .with_telemetry(telemetry);
        let resource = default_resource();

        feed(
            &mut agg,
            &resource,
            metric_event("latency", MetricKind::Samples(Samples::new([1.0, 2.0])), 0),
        );
        // Pushes held (2) + incoming (2) past the cap of 2.
        feed(
            &mut agg,
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

    /// A raw `Samples` series meeting a `Distribution` sketches what it holds and merges.
    #[test]
    fn samples_accumulator_converts_to_distribution_on_an_incoming_distribution() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_distributions(Distributions::Samples, 1000);
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event("latency", MetricKind::Samples(Samples::new([1.0, 2.0])), 0),
        );
        let mut incoming_sketch = logit_core::DdSketch::new();
        incoming_sketch.add(3.0);
        feed(
            &mut agg,
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

    /// `sets: estimate`: a re-observed member doesn't inflate the `HyperLogLog` estimate.
    #[test]
    fn set_estimate_mode_merges_hyperloglogs_and_estimates_distinct_members() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for i in 0..50 {
            feed(
                &mut agg,
                &resource,
                metric_event(
                    "unique.users",
                    MetricKind::SetMembers(vec![Bytes::from(format!("user-{i}"))]),
                    0,
                ),
            );
        }
        for i in 0..10 {
            feed(
                &mut agg,
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

    /// Merging two `Set`s is a union: a shared member isn't double-counted.
    #[test]
    fn set_merge_of_two_series_is_a_union() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut a = logit_core::HyperLogLog::new();
        a.insert(b"x");
        a.insert(b"y");
        feed(&mut agg, &resource, metric_event("unique.users", MetricKind::Set(a), 0));
        let mut b = logit_core::HyperLogLog::new();
        b.insert(b"y");
        b.insert(b"z");
        feed(&mut agg, &resource, metric_event("unique.users", MetricKind::Set(b), 0));

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
        feed(
            &mut agg,
            &resource,
            metric_event(
                "unique.users",
                MetricKind::SetMembers(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]),
                0,
            ),
        );
        feed(
            &mut agg,
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

    /// Growing past `max_set_members_per_series` falls back to a `HyperLogLog`, counted.
    #[test]
    fn set_members_mode_cap_exceeded_falls_back_to_set_and_counts() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_sets(Sets::Members, 2)
            .with_telemetry(telemetry);
        let resource = default_resource();

        feed(
            &mut agg,
            &resource,
            metric_event(
                "unique.users",
                MetricKind::SetMembers(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]),
                0,
            ),
        );
        feed(
            &mut agg,
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

    /// A raw `SetMembers` series meeting a `Set` estimates what it holds and merges.
    #[test]
    fn set_members_accumulator_converts_to_set_on_an_incoming_set() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_sets(Sets::Members, 1000);
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event("unique.users", MetricKind::SetMembers(vec![Bytes::from_static(b"a")]), 0),
        );
        let mut incoming = logit_core::HyperLogLog::new();
        incoming.insert(b"b");
        feed(&mut agg, &resource, metric_event("unique.users", MetricKind::Set(incoming), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Set(hll) => assert_eq!(hll.estimate(), 2, "held 'a' plus incoming 'b'"),
            other => panic!("expected Set, got {other:?}"),
        }
    }

    /// A `Samples` series tumbles even with `series_retention` set.
    #[test]
    fn a_samples_series_never_survives_a_flush_even_with_series_retention_enabled() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(5, 100)
            .with_distributions(Distributions::Samples, 1000);
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event("latency", MetricKind::Samples(Samples::new([1.0])), 0),
        );
        assert_eq!(agg.flush(100).len(), 1, "first flush emits the series");
        assert!(agg.flush(200).is_empty(), "a Samples series must tumble, never retain");
    }

    /// A `SetMembers` series tumbles even with `series_retention` set.
    #[test]
    fn a_set_members_series_never_survives_a_flush_even_with_series_retention_enabled() {
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(5, 100)
            .with_sets(Sets::Members, 1000);
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event("unique.users", MetricKind::SetMembers(vec![Bytes::from_static(b"a")]), 0),
        );
        assert_eq!(agg.flush(100).len(), 1, "first flush emits the series");
        assert!(agg.flush(200).is_empty(), "a SetMembers series must tumble, never retain");
    }

    // -- Scope-keyed groups ---------------------------------------------------------------------

    /// One resource with two scopes flushes as two groups, each carrying its scope.
    #[test]
    fn same_resource_different_scope_flush_as_two_groups_carrying_their_scope() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let scope_a = Arc::new(Scope { name: Bytes::from_static(b"scope-a"), ..Scope::default() });
        let scope_b = Arc::new(Scope { name: Bytes::from_static(b"scope-b"), ..Scope::default() });

        agg.observe_scope(Some(scope_a.clone()));
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

        agg.observe_scope(Some(scope_b.clone()));
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

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

    /// Two separate but equal `Arc<Scope>`s with a `NaN` attribute fold into one group.
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
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));

        agg.observe_scope(Some(scope_b));
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 1));

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

    fn resource_with_attr(key: &str, value: Value) -> Arc<Resource> {
        let mut attributes = AttrMap::new();
        attributes.insert(key, value);
        Arc::new(Resource { attributes, ..Resource::default() })
    }

    /// A `NaN` resource attribute through one `Arc` is one group across records and windows, so a
    /// cumulative `Sum` keeps its running total instead of restarting per record.
    #[test]
    fn a_nan_resource_attribute_through_one_arc_is_one_group_across_windows() {
        let mut agg = cumulative_agg();
        let resource = resource_with_attr("temp", Value::F64(f64::NAN));

        for ts in 0..10 {
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), ts));
        }
        assert_eq!(agg.groups.len(), 1, "ten records under one NaN-bearing Arc");
        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].1.len(), 1);
        assert_eq!(sum_of(&flushed[0].1[0]).value, 10.0);

        for ts in 100..105 {
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), ts));
        }
        assert_eq!(agg.groups.len(), 1, "the retained series' group is found again");
        let flushed = flush_events(&mut agg, 200);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].1.len(), 1);
        let second = &flushed[0].1[0];
        assert_eq!(sum_of(second).value, 15.0, "the running total carries across the window");
        assert_eq!(start_timestamp_of(second), 0);
    }

    /// Two `Arc`s with equal content, a `NaN` attribute included, are one group.
    #[test]
    fn resources_with_a_nan_attribute_but_distinct_arcs_fold_into_one_group() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let a = resource_with_attr("temp", Value::F64(f64::NAN));
        let b = resource_with_attr("temp", Value::F64(f64::NAN));
        assert!(!Arc::ptr_eq(&a, &b), "test setup: must be distinct Arcs");

        feed(&mut agg, &a, metric_event("hits", MetricKind::counter(1.0), 0));
        feed(&mut agg, &b, metric_event("hits", MetricKind::counter(2.0), 1));
        assert_eq!(agg.groups.len(), 1);

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        assert_eq!(counter_value(kind_of(&flushed[0].1[0])), 3.0);
    }

    /// `-0.0` and `0.0` are distinct resources, as they are distinct series attributes.
    #[test]
    fn resources_differing_only_in_the_sign_of_zero_are_two_groups() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let negative = resource_with_attr("host", Value::F64(-0.0));
        let positive = resource_with_attr("host", Value::F64(0.0));

        feed(&mut agg, &negative, metric_event("hits", MetricKind::counter(1.0), 0));
        feed(&mut agg, &positive, metric_event("hits", MetricKind::counter(1.0), 1));
        assert_eq!(agg.groups.len(), 2);
        assert_eq!(flush_events(&mut agg, 100).len(), 2);
    }

    /// `schema_url` and `dropped_attributes_count` are part of a resource's identity.
    #[test]
    fn resources_differing_only_in_schema_url_or_dropped_count_are_distinct_groups() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let plain = Arc::new(Resource::default());
        let with_schema = Arc::new(Resource {
            schema_url: Some(Bytes::from_static(b"https://opentelemetry.io/schemas/1.26.0")),
            ..Resource::default()
        });
        let with_dropped =
            Arc::new(Resource { dropped_attributes_count: 1, ..Resource::default() });

        for (ts, resource) in [&plain, &with_schema, &with_dropped].into_iter().enumerate() {
            feed(&mut agg, resource, metric_event("hits", MetricKind::counter(1.0), ts as i64));
        }
        assert_eq!(agg.groups.len(), 3);
        assert_eq!(flush_events(&mut agg, 100).len(), 3);
    }

    // -- `temporality: cumulative` -------------------------------------------------------------

    /// `cumulative` mode with both retention bounds set, the only combination rule 39 allows.
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

    /// A metric event carrying one delta `Histogram`.
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

    /// Each window emits the running total as `Cumulative` under an unchanging `start_timestamp`.
    #[test]
    fn cumulative_mode_sums_accumulate_across_flushes_with_a_stable_start_timestamp() {
        let mut agg = cumulative_agg();
        let resource = default_resource();

        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(2.0), 1_000));
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(3.0), 1_500));
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

        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(4.0), 110_000));
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

    /// A non-monotonic delta `Sum` (an up-down counter) stays non-monotonic when cumulative.
    #[test]
    fn cumulative_mode_keeps_the_accumulated_monotonic_flag() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let up_down = |v: f64| {
            MetricKind::Sum(Sum { value: v, temporality: Temporality::Delta, monotonic: false })
        };
        feed(&mut agg, &resource, metric_event("queue.depth", up_down(5.0), 10));
        feed(&mut agg, &resource, metric_event("queue.depth", up_down(-2.0), 20));

        let flushed = flush_events(&mut agg, 100);
        let emitted = sum_of(&flushed[0].1[0]);
        assert_eq!(emitted.value, 3.0);
        assert_eq!(emitted.temporality, Temporality::Cumulative);
        assert!(!emitted.monotonic, "a non-monotonic sum stays non-monotonic");
    }

    /// An idle cumulative series emits nothing, then resumes from its running total.
    #[test]
    fn an_idle_cumulative_series_emits_nothing_then_resumes_from_its_running_total() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(7.0), 100));
        assert_eq!(flush_events(&mut agg, 1_000).len(), 1, "window 1 emits the total");

        assert!(agg.flush(2_000).is_empty(), "an idle cumulative series emits nothing");

        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 2_500));
        let flushed = flush_events(&mut agg, 3_000);
        assert_eq!(sum_of(&flushed[0].1[0]).value, 8.0, "the idle window didn't reset the total");
        assert_eq!(start_timestamp_of(&flushed[0].1[0]), 100, "still the original start");
    }

    /// Delta histograms accumulate per bucket, with `sum` adding and `min`/`max` folding.
    #[test]
    fn cumulative_mode_histograms_accumulate_per_bucket() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let buckets = [(1.0, 1u64), (5.0, 2), (f64::INFINITY, 3)];
        assert!(
            feed(
                &mut agg,
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

        // Same bounds, lower min, higher max.
        feed(
            &mut agg,
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

    /// A window with observations but no `sum`, `min`, or `max` makes each running value `None`.
    #[test]
    fn a_histogram_window_without_sum_min_or_max_drops_all_three() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let bounds = [(1.0, 1u64), (f64::INFINITY, 1)];
        feed(
            &mut agg,
            &resource,
            delta_histogram_event("sizes", &bounds, Some(3.0), Some(0.5), Some(2.0), 0),
        );
        feed(&mut agg, &resource, delta_histogram_event("sizes", &bounds, None, None, None, 1));

        let flushed = flush_events(&mut agg, 100);
        let emitted = histogram_of(&flushed[0].1[0]);
        assert_eq!(emitted.buckets, vec![(1.0, 2), (f64::INFINITY, 2)]);
        assert_eq!(emitted.sum, None, "a sum missing one window's contribution is no sum at all");
        assert_eq!(emitted.min, None, "a min missing one window's observations is no min");
        assert_eq!(emitted.max, None);
    }

    /// Two `u64::MAX` bucket counts saturate at `u64::MAX` rather than panic or wrap.
    #[test]
    fn cumulative_histogram_bucket_counts_saturate_instead_of_overflowing() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let maxed = [(1.0, u64::MAX), (f64::INFINITY, u64::MAX)];
        assert!(feed(
            &mut agg,
            &resource,
            delta_histogram_event("sizes", &maxed, None, None, None, 0)
        )
        .is_none());
        assert!(feed(
            &mut agg,
            &resource,
            delta_histogram_event("sizes", &maxed, None, None, None, 1)
        )
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

    /// A histogram with different bucket bounds is forwarded, leaving the series untouched.
    #[test]
    fn a_histogram_with_mismatched_bucket_bounds_is_passed_through() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = cumulative_agg()
            .with_diagnostics(Diagnostics::default().with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        let resource = default_resource();

        feed(
            &mut agg,
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
        let passed = feed(&mut agg, &resource, mismatched);
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

    /// An idle-evicted cumulative series restarts from zero with a new `start_timestamp`.
    #[test]
    fn an_evicted_cumulative_series_restarts_with_a_new_start_timestamp() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_temporality(AggregateTemporality::Cumulative)
            .with_series_retention(2, 100)
            .with_telemetry(telemetry);
        let resource = default_resource();

        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(5.0), 100));
        assert_eq!(agg.flush(1_000).len(), 1, "window 1: emits 5, idle_windows resets to 0");
        assert!(agg.flush(2_000).is_empty(), "window 2: idle_windows -> 1, still under retention");
        assert!(agg.flush(3_000).is_empty(), "window 3: idle_windows -> 2, now evicted");

        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 3_500));
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

    /// The cardinality cap evicts cumulative series too, counted and diagnosed.
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
            feed(&mut agg, &resource, metric_event(&format!("c{i}"), MetricKind::counter(1.0), 0));
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

    /// An incoming cumulative `Sum` passes through in `cumulative` mode too.
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
        let passed = feed(&mut agg, &resource, incoming);
        assert!(passed.is_some(), "an already-cumulative Sum must pass through untouched");
        assert!(agg.flush(100).is_empty(), "and must not have opened a series");
    }

    /// In `delta` mode a `Sum` tumbles and is emitted as `Delta` with no `start_timestamp`.
    #[test]
    fn delta_mode_sums_tumble_and_emit_delta_temporality_with_no_start_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(4.0), 1_000));

        let flushed = flush_events(&mut agg, 10_000);
        let emitted = &flushed[0].1[0];
        assert_eq!(sum_of(emitted).value, 4.0);
        assert_eq!(sum_of(emitted).temporality, Temporality::Delta);
        assert_eq!(start_timestamp_of(emitted), 0, "delta records carry no start time");
        assert!(agg.flush(20_000).is_empty(), "and the series tumbles, retention or not");
    }

    /// A delta `Histogram` passes through in `delta` mode.
    #[test]
    fn a_delta_histogram_still_passes_through_in_delta_mode() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        let event = delta_histogram_event("sizes", &[(1.0, 1)], Some(1.0), None, None, 0);
        let passed = feed(&mut agg, &resource, event);
        assert!(passed.is_some(), "a delta histogram must still pass through in delta mode");
        assert!(agg.flush(100).is_empty(), "and must not have opened a series");
    }

    /// A `Distribution` series tumbles in `cumulative` mode too.
    #[test]
    fn a_distribution_series_still_tumbles_in_cumulative_mode() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let mut sketch = logit_core::DdSketch::new();
        sketch.add(1.0);
        feed(&mut agg, &resource, metric_event("latency", MetricKind::Distribution(sketch), 0));
        assert_eq!(agg.flush(100).len(), 1, "the first flush emits the sketch");
        assert!(agg.flush(200).is_empty(), "a Distribution series must tumble in either mode");
    }

    /// Every `warn` message a `tracing` subscriber sees, by its `message` field.
    #[derive(Clone, Default)]
    struct CapturedWarnings(Arc<std::sync::Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedWarnings {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Message<'a>(&'a mut Vec<String>);
            impl tracing::field::Visit for Message<'_> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0.push(format!("{value:?}"));
                    }
                }
            }
            let mut messages = self.0.lock().unwrap_or_else(|p| p.into_inner());
            event.record(&mut Message(&mut messages));
        }
    }

    /// Each diagnostic `process` and `flush` report reads as one sentence: a `\` lost from a
    /// wrapped string literal leaves a run of the source's indentation inside the message.
    #[test]
    fn no_diagnostic_message_carries_a_run_of_spaces() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let captured = CapturedWarnings::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        tracing::subscriber::with_default(subscriber, || {
            let resource = default_resource();
            let samples = |rate: f64, values: &[f64]| {
                MetricKind::Samples(Samples {
                    values: values.iter().copied().collect(),
                    sample_rate: rate,
                })
            };
            let members = |m: &[&'static [u8]]| {
                MetricKind::SetMembers(m.iter().map(|m| Bytes::from_static(m)).collect())
            };

            // gauge_delta_unseeded, kind_conflict, sample_rate_clamped, sum_non_finite.
            let mut agg = Aggregator::new(Duration::from_secs(10));
            feed(&mut agg, &resource, metric_event("n", MetricKind::counter(f64::NAN), 0));
            feed(&mut agg, &resource, metric_event("g", MetricKind::GaugeDelta(1.0), 0));
            feed(&mut agg, &resource, metric_event("g", MetricKind::counter(1.0), 0));
            feed(&mut agg, &resource, metric_event("s", samples(0.0001, &[1.0]), 0));

            // samples_rate_mismatch and samples_cap_exceeded.
            let mut agg = Aggregator::new(Duration::from_secs(10))
                .with_distributions(Distributions::Samples, 2);
            feed(&mut agg, &resource, metric_event("r", samples(1.0, &[1.0]), 0));
            feed(&mut agg, &resource, metric_event("r", samples(0.5, &[1.0]), 0));
            feed(&mut agg, &resource, metric_event("c", samples(1.0, &[1.0, 2.0]), 0));
            feed(&mut agg, &resource, metric_event("c", samples(1.0, &[3.0]), 0));

            // set_members_cap_exceeded.
            let mut agg = Aggregator::new(Duration::from_secs(10)).with_sets(Sets::Members, 1);
            feed(&mut agg, &resource, metric_event("u", members(&[b"a", b"b"]), 0));

            // histogram_bounds_mismatch, and series_retention_full from the flush.
            let mut agg = cumulative_agg().with_series_retention(5, 1);
            let h = |bounds: &[(f64, u64)]| delta_histogram_event("h", bounds, None, None, None, 0);
            feed(&mut agg, &resource, h(&[(1.0, 1)]));
            feed(&mut agg, &resource, h(&[(2.0, 1)]));
            feed(&mut agg, &resource, metric_event("other", MetricKind::counter(1.0), 0));
            agg.flush(100);
        });

        let messages = captured.0.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(messages.len(), 9, "one report per diagnostic key: {messages:#?}");
        for message in &messages {
            assert!(!message.contains("  "), "a run of spaces in {message:?}");
        }
    }

    fn passed_through(events: &[Event], reason: &str) -> f64 {
        counter_with_tag(events, "logit.transform.metrics.passed_through", "reason", reason)
            .unwrap_or(0.0)
    }

    /// Every `name` counter in drained telemetry, whatever its tags, summed.
    fn counter_total(events: &[Event], name: &str) -> f64 {
        events
            .iter()
            .flat_map(|e| &e.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == name)
            .map(|m| counter_value(&m.kind))
            .sum()
    }

    fn absorbed(events: &[Event]) -> f64 {
        counter_total(events, "logit.transform.metrics.absorbed")
    }

    /// Every record `process` receives is absorbed or passed through under one reason, and the
    /// counters total per call.
    #[test]
    fn every_record_is_counted_absorbed_or_passed_through_once() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = cumulative_agg().with_telemetry(telemetry);
        let resource = default_resource();

        let mut no_value = MetricRecord::new(intern("nv"), MetricKind::Gauge(0.0));
        no_value.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let histogram = |bound: f64| {
            MetricKind::Histogram(logit_core::Histogram {
                buckets: vec![(bound, 1)],
                temporality: Temporality::Delta,
                sum: None,
                min: None,
                max: None,
            })
        };
        let mut records = vec![
            no_value,
            MetricRecord::new(
                intern("cum"),
                MetricKind::Sum(Sum {
                    value: 1.0,
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
            MetricRecord::new(
                intern("q"),
                MetricKind::Summary(logit_core::Summary { quantiles: vec![], count: 0, sum: 0.0 }),
            ),
            MetricRecord::new(intern("g"), MetricKind::Gauge(1.0)),
            MetricRecord::new(intern("g"), MetricKind::counter(1.0)),
            MetricRecord::new(intern("h"), histogram(1.0)),
            MetricRecord::new(intern("h"), histogram(2.0)),
            MetricRecord::new(intern("h"), histogram(1.0)),
            MetricRecord::new(intern("n"), MetricKind::counter(f64::NAN)),
            MetricRecord::new(intern("n"), MetricKind::counter(f64::NEG_INFINITY)),
            MetricRecord::new(intern("n"), MetricKind::counter(2.0)),
        ];
        let metrics_in = records.len() as f64;
        let first = records.remove(0);
        let mut event = Event::metric(0, AttrMap::new(), first);
        event.metrics.extend(records);
        assert!(agg.process(&resource, &mut event));
        assert_eq!(event.metrics.len(), 7, "the passed-through records stay on the event");

        let events = registry.drain(0);
        assert_eq!(absorbed(&events), 4.0);
        assert_eq!(passed_through(&events, "no_recorded_value"), 1.0);
        assert_eq!(passed_through(&events, "no_merge_rule"), 2.0);
        assert_eq!(passed_through(&events, "kind_conflict"), 1.0);
        assert_eq!(passed_through(&events, "histogram_bounds_mismatch"), 1.0);
        assert_eq!(passed_through(&events, "non_finite"), 2.0);
        let reasons = [
            "no_recorded_value",
            "no_merge_rule",
            "kind_conflict",
            "histogram_bounds_mismatch",
            "non_finite",
        ];
        let passed: f64 = reasons.iter().map(|r| passed_through(&events, r)).sum();
        assert_eq!(metrics_in, absorbed(&events) + passed);
    }

    /// A statsd `1e308|c|@0.5` extrapolates to infinity. It passes through instead of merging, so
    /// a cumulative total stays finite for the rest of the series' life.
    #[test]
    fn a_non_finite_delta_sum_passes_through_and_leaves_the_total_finite() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
        for bad in [f64::INFINITY, f64::NAN] {
            let forwarded =
                feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(bad), 0));
            assert!(forwarded.is_some(), "a non-finite delta sum is forwarded");
        }
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(2.0), 0));
        assert_eq!(sum_of(&flush_events(&mut agg, 10)[0].1[0]).value, 3.0);
    }

    /// `min` and `max` follow the `sum` rule: a record with observations but no `min` (or `max`)
    /// makes the accumulated one `None`, so a series never pairs one window's `min` with another's
    /// `max`.
    #[test]
    fn a_histogram_min_or_max_missing_from_an_observed_window_becomes_none() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        let bounds = [(1.0, 1u64), (f64::INFINITY, 1)];
        feed(&mut agg, &resource, delta_histogram_event("h", &bounds, None, Some(5.0), None, 0));
        feed(&mut agg, &resource, delta_histogram_event("h", &bounds, None, None, Some(1.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let emitted = histogram_of(&flushed[0].1[0]);
        assert_eq!((emitted.min, emitted.max), (None, None), "never min 5 above max 1");
    }

    /// A record whose buckets total zero observed nothing, so its missing `min`/`max` doesn't
    /// erase the series' extremes, in either arrival order.
    #[test]
    fn a_zero_count_histogram_record_is_ignored_for_min_and_max() {
        let observed = [(1.0, 1u64), (f64::INFINITY, 1)];
        let empty = [(1.0, 0u64), (f64::INFINITY, 0)];
        for empty_first in [false, true] {
            let mut agg = cumulative_agg();
            let resource = default_resource();
            let a = delta_histogram_event("h", &observed, Some(3.0), Some(0.5), Some(2.0), 0);
            let z = delta_histogram_event("h", &empty, Some(0.0), None, None, 0);
            let (first, second) = if empty_first { (z, a) } else { (a, z) };
            feed(&mut agg, &resource, first);
            feed(&mut agg, &resource, second);

            let flushed = flush_events(&mut agg, 100);
            let emitted = histogram_of(&flushed[0].1[0]);
            assert_eq!(emitted.min, Some(0.5), "empty first: {empty_first}");
            assert_eq!(emitted.max, Some(2.0), "empty first: {empty_first}");
            assert_eq!(emitted.sum, Some(3.0));
        }
    }

    fn samples_at(rate: f64, values: &[f64]) -> MetricKind {
        MetricKind::Samples(Samples { values: values.iter().copied().collect(), sample_rate: rate })
    }

    fn with_registry(agg: Aggregator) -> (Aggregator, Arc<logit_core::Registry>) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let diag = Diagnostics::default().with_telemetry(telemetry.clone());
        (agg.with_diagnostics(diag).with_telemetry(telemetry), registry)
    }

    fn weight_clamped(events: &[Event]) -> f64 {
        counter_total(events, "logit.transform.samples.weight_clamped")
    }

    /// `@0.001` is weight 1000, the largest unclamped one; `@0.00099` rounds to 1010 and clamps;
    /// a record with no values under-weights nothing.
    #[test]
    fn weight_clamped_fires_only_past_max_weight_and_with_values() {
        let resource = default_resource();
        for (rate, values, expected) in
            [(0.001, &[1.0][..], 0.0), (0.00099, &[1.0][..], 1.0), (0.0001, &[][..], 0.0)]
        {
            let (mut agg, registry) = with_registry(Aggregator::new(Duration::from_secs(10)));
            feed(&mut agg, &resource, metric_event("t", samples_at(rate, values), 0));
            let events = registry.drain(0);
            assert_eq!(weight_clamped(&events), expected, "rate {rate}, values {values:?}");
        }
    }

    /// A clamped record held raw under `distributions: samples` is reported when the series falls
    /// back and its weight is first applied.
    #[test]
    fn a_held_clamped_record_is_reported_at_fallback() {
        let resource = default_resource();
        let (agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_distributions(Distributions::Samples, 100),
        );
        let mut agg = agg;
        feed(&mut agg, &resource, metric_event("t", samples_at(0.0001, &[1.0]), 0));
        feed(&mut agg, &resource, metric_event("t", samples_at(0.5, &[1.0]), 0));
        let events = registry.drain(0);
        assert_eq!(weight_clamped(&events), 1.0, "the held @0.0001 record");
        match kind_of(&flush_events(&mut agg, 10)[0].1[0]) {
            MetricKind::Distribution(sketch) => assert_eq!(sketch.count(), 1000 + 2),
            other => panic!("expected a Distribution, got {other:?}"),
        }
    }

    /// A non-finite sample has no bin; the sketch drops it and `aggregate` counts it.
    #[test]
    fn non_finite_samples_are_dropped_from_the_sketch_and_counted() {
        let resource = default_resource();
        let (mut agg, registry) = with_registry(Aggregator::new(Duration::from_secs(10)));
        let values = [1.0, f64::NAN, f64::INFINITY];
        feed(&mut agg, &resource, metric_event("t", samples_at(1.0, &values), 0));
        let events = registry.drain(0);
        assert_eq!(counter_total(&events, "logit.transform.samples.non_finite_dropped"), 2.0);
        match kind_of(&flush_events(&mut agg, 10)[0].1[0]) {
            MetricKind::Distribution(sketch) => assert_eq!(sketch.count(), 1),
            other => panic!("expected a Distribution, got {other:?}"),
        }
    }

    /// Sample rates compare by bit pattern, so a `NaN` rate (which only the native decoder
    /// produces) matches itself and the series stays raw.
    #[test]
    fn a_nan_sample_rate_matches_itself_under_distributions_samples() {
        let resource = default_resource();
        let (agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_distributions(Distributions::Samples, 100),
        );
        let mut agg = agg;
        feed(&mut agg, &resource, metric_event("t", samples_at(f64::NAN, &[1.0]), 0));
        feed(&mut agg, &resource, metric_event("t", samples_at(f64::NAN, &[2.0]), 0));
        let events = registry.drain(0);
        assert_eq!(counter_total(&events, "logit.transform.samples.fallback"), 0.0);
        match kind_of(&flush_events(&mut agg, 10)[0].1[0]) {
            MetricKind::Samples(s) => {
                assert_eq!(&s.values[..], &[1.0, 2.0]);
                assert!(s.sample_rate.is_nan());
            }
            other => panic!("expected raw Samples, got {other:?}"),
        }
    }

    /// One record far past `max_set_members_per_series` stops the deduplicating scan at the cap
    /// and streams the rest into the `HyperLogLog`, instead of unioning the whole record first
    /// (quadratic in the record's own size).
    #[test]
    fn a_set_members_record_past_the_cap_falls_back_without_a_quadratic_union() {
        const CAP: usize = 1000;
        const MEMBERS: usize = 20 * CAP;
        let resource = default_resource();
        let (agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_sets(Sets::Members, CAP));
        let mut agg = agg;
        let members: Vec<Bytes> = (0..MEMBERS).map(|i| Bytes::from(i.to_string())).collect();

        let started = std::time::Instant::now();
        feed(&mut agg, &resource, metric_event("u", MetricKind::SetMembers(members), 0));
        let elapsed = started.elapsed();

        let events = registry.drain(0);
        assert_eq!(
            counter_with_tag(&events, "logit.transform.set_members.fallback", "reason", "cap"),
            Some(1.0)
        );
        match kind_of(&flush_events(&mut agg, 10)[0].1[0]) {
            MetricKind::Set(hll) => {
                let estimate = hll.estimate() as f64;
                let error = (estimate - MEMBERS as f64).abs() / MEMBERS as f64;
                assert!(error < 0.05, "estimate {estimate} for {MEMBERS} members");
            }
            other => panic!("expected a Set, got {other:?}"),
        }
        // A union of the whole record is about MEMBERS² / 2 = 2e8 compares, about 3 s in a debug
        // build; stopping at the cap is about CAP² / 2 = 5e5.
        assert!(elapsed < Duration::from_millis(1500), "absorb took {elapsed:?}");
    }
}

#[cfg(test)]
mod verification;
