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
//! series survives a flush. A delta `Sum`, `Gauge`, or `GaugeDelta` whose value is `NaN` or
//! infinite passes through too, so a total or a held gauge stays finite.
//!
//! Every record on an event flagged `_logit.metrics.closed` passes through too, the event whole: its
//! source already closed the time span it summarizes, which a window would merge with other spans
//! and restamp with the flush time. The flag, its values, and who sets it are ADR `metric-flags`'s
//! (`docs/adr/metric-flags.md`, "The flags"); a decoder sets it beside its protocol carriers (a
//! DogStatsD `|T` line, a Prometheus sample with its own timestamp, a Datadog APM stats group), and
//! this stage reads the flag, never a carrier ([`Flags`]). An event carrying a carrier and no flag
//! is windowed. Every record is counted once, as `logit.transform.metrics.absorbed`,
//! `logit.transform.metrics.passed_through{reason}`, or `logit.transform.metrics.dropped{reason}`
//! (see [`Tally`]).
//!
//! # The series cap
//!
//! `max_series` bounds every series the stage holds at once, active and retained, across all
//! groups ([`SeriesBudget`]). It's checked only where a record would open a series: a record for a
//! held series is always absorbed, and a pass-through record never reaches the check. At a full
//! cap, one retained series not updated this window makes room for each new one, in the flush
//! cap's order ([`EvictionRank`]: most idle, then newest, an overflow series last), counted
//! `logit.transform.series.evicted{reason="series_limit"}`. A series updated this window is never
//! evicted to make room: with none left, the record is dropped and counted
//! `logit.transform.metrics.dropped{reason="series_limit"}`. A refused record creates no group. An
//! emptied group stays until the flush, so a window holds at most `max_series` groups plus one per
//! eviction, at most twice `max_series`. Admission is first come, first served within a window;
//! `flush` frees the slot of every series it drains or evicts.
//!
//! # The per-name limit
//!
//! `max_series_per_metric`, off by default, counts the series held per metric name across every
//! group ([`NameCount`]). Past it, a record that would open a series folds into its name's
//! overflow series instead: the record's name, unit, and group, with the attributes cut to
//! `logit.overflow: true`, the private `_logit.*` keys, and `host.name` ([`OverflowKey`]). An
//! overflow series is known by that key alone, so one from an upstream stage is one here too. It
//! doesn't count against the per-name limit, but it's admitted under `max_series` like any series,
//! and it's evicted last by both the flush cap and an admission eviction. A record whose kind
//! conflicts with the overflow series passes through as `kind_conflict`. A folded record counts as
//! `absorbed` and `logit.transform.metrics.overflowed`, never `*.dropped`; `flush` reports how
//! many names had a record past their limit that window as `logit.transform.metrics.overflowing`, and only the
//! throttled `series_per_metric_overflow` diagnostic names a metric. A record absorbed into an
//! upstream overflow series isn't a fold here and counts toward neither. With the limit off, no
//! name is counted and nothing on the hot path changes.
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
//! `Cumulative`, with `MetricRecord::start_timestamp` set to the series' start time (its first
//! event's timestamp, clamped into the window it opened in; see `SeriesState::first_seen`). That
//! stamp is the reset signal OTLP and Prometheus consumers detect a counter restart with: it never
//! changes while the series lives, and a series evicted (TTL or cardinality cap) and later
//! re-created gets a new one.
//!
//! A histogram's `sum`, `min`, and `max` each become `None` once a contributing record lacks one,
//! because a value over part of the observations is a wrong number a consumer can't detect. For
//! `min`/`max`, a record whose buckets total zero observed nothing and doesn't count.
//!
//! A histogram's bucket counts add with `saturating_add`, not `+`: they are wire-supplied `u64`s,
//! so a producer sending `u64::MAX` twice pins the bucket at `u64::MAX` (wrong but still monotonic)
//! instead of panicking or wrapping the total backwards under an unchanged `start_timestamp`.
//! A `Sum` or gauge value is a typed `MetricValue`: the first record of a series seeds it with its
//! own type, and every later one folds in with `MetricValue::checked_add`, which keeps an integer
//! an integer and widens on a mixed-type pair or an overflow, counted as
//! `logit.transform.metrics.normalized{reason="widened"}` (`docs/adr/typed-metric-scalar.md`'s
//! "Arithmetic widens, counted"). An `F64` total saturates to `inf` on its own.
//!
//! An incoming *cumulative* `Sum` passes through in both modes: re-summing a running total would
//! double-count it. `docs/adr/prometheus-scrape-and-exposition.md` covers the consumer that needs
//! this mode (`prometheus_out` drops delta records).
//!
//! # Series identity
//!
//! A series is its record's name and unit and its event's attributes. On an event flagged
//! `_logit.metrics.fields`, the attributes under the field namespace (`_logit.field.*`,
//! [`KeyClasses`]) and the flag itself are left out, and a flushed series doesn't carry them
//! either. On an unflagged event, no key is scanned for and a `_logit.field.*` key is identity like
//! any other. Every other private attribute is identity and stays. A record that passes through
//! keeps its event's attributes whole. The ADR's "Amendment: InfluxDB string fields aren't series
//! identity (2026-10-19)" has the reasoning, and ADR `metric-flags` the flag.

use bytes::Bytes;
use logit_core::attrs::{
    CLOSED_BUCKETED, CLOSED_FLAGGED, CLOSED_TIMESTAMPED, HOST_NAME, METRICS_CLOSED, METRICS_FIELDS,
    OVERFLOW,
};
use logit_core::interner::Symbol;
use logit_core::{
    AttrMap, Diagnostics, Event, MetricKind, MetricRecord, MetricValue, Resource, Samples, Scope,
    SpanLink, Sum, Telemetry, Temporality, Value,
};
use logit_pipeline::{FlushOutput, TraceContext, Transform};
use smallvec::SmallVec;
use std::collections::hash_map::DefaultHasher;
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
/// same stance `docs/known-gaps/telemetry.md` takes on `MAX_KEYS_PER_COMPONENT`.
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
    /// event, plus how many observations the cap rejected: one per observation of a context the
    /// full set doesn't link, so a context seen in two batches counts twice.
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
    /// The last group `find_group` found or `open_group` opened, keyed by the `Arc`s it was asked
    /// with. It holds the incoming `Arc`s, not the group's stored ones: `otlp_in`, `logit_in`, a
    /// Lua resource write, and `set` hand every batch a fresh `Arc<Resource>` equal by value to an
    /// earlier one, so only the incoming pointer repeats, across the events of one batch. Holding
    /// the clones keeps those addresses from being reused by a different resource while memoized.
    /// Cleared at `flush`, whose `retain` shifts or removes indices, which also releases a
    /// finished batch's `Arc`.
    last_group: Option<GroupMemo>,
    diag: Diagnostics,
    /// How many times `report_overflow` built its report, for the test that pins the throttle.
    #[cfg(test)]
    overflow_reports_built: u32,
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
    /// at once, against a stream of never-repeating series names. Evicts in [`EvictionRank`]'s
    /// order: the most idle first, then the newest among equally idle series, an overflow series
    /// last. Meaningless while `series_retention` is `0`.
    max_retained_series: usize,
    /// How many series are held and how many may be; see this module's "The series cap".
    budget: SeriesBudget,
    /// The series a full `max_series` evicts this window, last to evict first, with each one's
    /// group index; `Aggregator::evict_one`'s. Empty until a window's first eviction, and cleared
    /// at `flush`.
    eviction_order: Vec<(EvictionRank, usize, SeriesKey)>,
    /// Whether this window's `eviction_order` is built; it's never rebuilt before `flush`.
    eviction_order_built: bool,
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
    /// The previous flush's `now`: the start of the window being filled, and the earliest start
    /// time a series opened in it takes. `None` before the first flush.
    window_start: Option<i64>,
    /// The `SeriesState::open_seq` the next opened series gets. Never reused, so it orders every
    /// series this `Aggregator` has held, across groups and flushes.
    next_open_seq: u64,
    /// The two flags `process` reads, interned once ([`Flags`]).
    flags: Flags,
    /// The current batch's resource half of the flags, read once in `begin_batch`. An event's own
    /// value wins over it.
    batch_flags: BatchFlags,
    key_classes: KeyClasses,
    overflow: OverflowKey,
}

/// The flag keys of ADR `metric-flags` (`docs/adr/metric-flags.md`, "The flags"), interned once so
/// `process` pays one binary search per flag per event and resolves no key. A decoder sets them
/// beside its protocol carriers; this stage reads no carrier.
struct Flags {
    /// `_logit.metrics.closed`: forward the event whole, unmerged.
    closed: Symbol,
    /// `_logit.metrics.fields`: the event carries `_logit.field.*` keys to leave out of identity.
    fields: Symbol,
}

/// The batch resource's value of each flag, which applies to every event of the batch that
/// doesn't carry its own.
#[derive(Default, Clone, Copy)]
struct BatchFlags {
    closed: Option<Closed>,
    fields: bool,
}

/// Why an event's records pass through whole under `_logit.metrics.closed`, one per
/// `passed_through` reason the flag can produce.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Closed {
    Bucketed,
    Timestamped,
    /// A value outside the closed set, or `Bool(true)`: counted under one fixed reason so a
    /// script can't put free text into a telemetry tag.
    Flagged,
}

impl Closed {
    /// The verdict a `_logit.metrics.closed` value gives, or `None` when it doesn't close the
    /// event (`Bool(false)`, or any non-`Str` other than `Bool(true)`).
    fn of(value: &Value) -> Option<Closed> {
        match value {
            Value::Str(_) => Some(match value.as_str() {
                Some(CLOSED_BUCKETED) => Closed::Bucketed,
                Some(CLOSED_TIMESTAMPED) => Closed::Timestamped,
                _ => Closed::Flagged,
            }),
            Value::Bool(true) => Some(Closed::Flagged),
            _ => None,
        }
    }
}

/// `Aggregator::last_group`'s entry: the `Arc`s `find_group` was asked with, and the index of the
/// group it answered.
struct GroupMemo {
    resource: Arc<Resource>,
    scope: Option<Arc<Scope>>,
    index: usize,
}

impl GroupMemo {
    fn matches(&self, resource: &Arc<Resource>, scope: &Option<Arc<Scope>>) -> bool {
        Arc::ptr_eq(&self.resource, resource)
            && match (&self.scope, scope) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
    }
}

/// Keyed by `(resource, scope)` value, not `Arc` identity (as `find_group` does for resource): two
/// batches that each build an equal `Scope` describe the same instrumentation scope and aggregate
/// together.
struct ResourceGroup {
    resource: Arc<Resource>,
    scope: Option<Arc<Scope>>,
    /// [`group_hash`] of `resource` and `scope`, which `find_group` compares before the full field
    /// compare. It can't go stale: the group holds both `Arc`s, and the values behind them are
    /// immutable.
    hash: u64,
    series: HashMap<SeriesKey, SeriesState>,
}

impl ResourceGroup {
    #[cfg(test)]
    fn new(resource: Arc<Resource>, scope: Option<Arc<Scope>>) -> Self {
        let hash = group_hash(&resource, &scope);
        Self { resource, scope, hash, series: HashMap::new() }
    }
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
    /// This series' place in the order the `Aggregator` opened series, from
    /// `Aggregator::next_open_seq`. The cardinality cap evicts the highest first among equally idle
    /// series, so a series updated every window outlives one-off series opened after it.
    open_seq: u64,
    /// This series' start time in Unix nanos, emitted as `MetricRecord::start_timestamp` on every
    /// cumulative-mode `Sum`/`Histogram` flush. A series evicted and re-created gets a fresh
    /// `SeriesState` and so a fresh value: that's the restart signal.
    ///
    /// Opened as the opening event's timestamp, raised to `Aggregator::window_start`, and lowered
    /// to the flush clock at the first flush that emits it, so it lies between the previous
    /// incarnation's last point and this one's first. A source timestamp alone can precede the
    /// point it replaces, run ahead of the flush, or be `0`. Both bounds are clocks the stage
    /// already has, so opening a series costs no `SystemTime::now()` syscall. Recorded in both
    /// modes so the field has one meaning regardless of config.
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
        total: MetricValue,
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
        value: MetricValue,
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
    /// The empty accumulator; `process` merges the opening record into it right after. A `Sum`'s
    /// or gauge's placeholder value is never read: the opening record replaces it, so the series
    /// takes that record's type rather than widening against an `F64` zero.
    fn open(self) -> Accumulator {
        match self {
            Opener::Sum { monotonic } => {
                Accumulator::Sum { total: MetricValue::F64(0.0), monotonic }
            }
            Opener::Gauge => Accumulator::Gauge { value: MetricValue::F64(0.0), at: i64::MIN },
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

/// Per-`process`-call totals behind `logit.transform.metrics.absorbed`,
/// `logit.transform.metrics.passed_through{reason}`, and `logit.transform.metrics.dropped{reason}`:
/// every record `process` receives lands in one field, except `widened` and
/// `series_limit_evicted`, which count folds and series, and `overflowed`, a subset of
/// `absorbed`.
/// Reported once per non-zero field after the loop, because `Telemetry::count` locks and
/// hashes on every call.
#[derive(Default)]
struct Tally {
    absorbed: u32,
    bucketed: u32,
    timestamped: u32,
    flagged: u32,
    no_recorded_value: u32,
    no_merge_rule: u32,
    kind_conflict: u32,
    histogram_bounds_mismatch: u32,
    non_finite: u32,
    /// Records dropped because opening their series would pass `max_series`.
    series_limit: u32,
    /// Series evicted to make room under `max_series`; not a record count.
    series_limit_evicted: u32,
    /// Absorbed records that folded into an overflow series under `max_series_per_metric`.
    overflowed: u32,
    /// Folds whose `MetricValue::checked_add` changed the type; not a pass-through.
    widened: u32,
}

impl Tally {
    /// A tally of `n` records passed through under `closed`.
    fn closed(closed: Closed, n: u32) -> Tally {
        match closed {
            Closed::Bucketed => Tally { bucketed: n, ..Tally::default() },
            Closed::Timestamped => Tally { timestamped: n, ..Tally::default() },
            Closed::Flagged => Tally { flagged: n, ..Tally::default() },
        }
    }

    fn report(&self, telemetry: &Telemetry) {
        if self.absorbed > 0 {
            telemetry.count("logit.transform.metrics.absorbed", f64::from(self.absorbed), &[]);
        }
        for (reason, n) in [
            (CLOSED_BUCKETED, self.bucketed),
            (CLOSED_TIMESTAMPED, self.timestamped),
            (CLOSED_FLAGGED, self.flagged),
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
        if self.series_limit > 0 {
            telemetry.count(
                "logit.transform.metrics.dropped",
                f64::from(self.series_limit),
                &[("reason", "series_limit")],
            );
        }
        if self.series_limit_evicted > 0 {
            telemetry.count(
                "logit.transform.series.evicted",
                f64::from(self.series_limit_evicted),
                &[("reason", "series_limit")],
            );
        }
        if self.overflowed > 0 {
            telemetry.count("logit.transform.metrics.overflowed", f64::from(self.overflowed), &[]);
        }
        if self.widened > 0 {
            telemetry.count(
                "logit.transform.metrics.normalized",
                f64::from(self.widened),
                &[("reason", "widened")],
            );
        }
    }
}

/// The `max_series` bound and the `max_series_per_metric` limit: how many series the stage
/// holds, active and retained across every group, against how many it may, and, with the limit
/// set, how many each name holds. Kept as counts, so a full cap with nothing to evict answers
/// without a scan of the groups: `admit`, `process`, and `Aggregator::evict_one` move them, and
/// `flush` resets them from the series it keeps.
struct SeriesBudget {
    held: usize,
    /// The held series not updated this window: retained across the last flush, and the
    /// candidates a full cap evicts.
    idle: usize,
    max: usize,
    /// `max_series_per_metric`; `None` leaves `names` empty and untouched.
    per_name: Option<usize>,
    /// The held series per metric name, overflow series excluded, under `per_name` only. A name
    /// enters when one of its series is admitted, never when it's refused, so the map holds at
    /// most `max` names plus that window's admission evictions. `flush` rebuilds it from its survivors, keeping the allocation.
    names: HashMap<Symbol, NameCount>,
}

/// One metric name's entry in [`SeriesBudget::names`].
#[derive(Default, Clone, Copy)]
struct NameCount {
    /// Its held series, overflow series excluded: what `max_series_per_metric` limits.
    series: usize,
    /// Records past the limit since the last flush, whether or not the fold settled: what
    /// `logit.transform.metrics.overflowing` counts names by, and the diagnostic's ranking. A
    /// record absorbed into an overflow series that arrived from upstream never reaches the limit
    /// here, so each process reports only its own folding.
    folded: u32,
}

/// [`SeriesBudget::admit`]'s answer for a record that would open a series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// Open the series; its slot is already counted.
    Open,
    /// Evict one series not updated this window (`Aggregator::evict_one`), then open the series.
    /// Its slot is already counted, one over `max` until the eviction.
    EvictIdle,
    /// Drop the record: the cap is full and every held series was updated this window.
    Refused,
    /// Fold the record into its name's overflow series: the name holds `max_series_per_metric`
    /// series. Nothing is counted yet; the overflow series is admitted on its own if it's new.
    Overflow,
}

impl SeriesBudget {
    /// Decides whether a record may open the series `key`, and counts the slot when it may. The
    /// one place a new series is admitted: `process` asks only where a record would open a
    /// series, so a held series never pays for it. `is_overflow` is asked only under the per-name
    /// limit.
    fn admit(
        &mut self,
        key: &SeriesKey,
        is_overflow: impl FnOnce(&SeriesKey) -> bool,
    ) -> Admission {
        let Some(limit) = self.per_name else {
            return self.admit_slot();
        };
        let is_overflow = is_overflow(key);
        if !is_overflow {
            if let Some(count) = self.names.get_mut(&key.name) {
                if count.series >= limit {
                    count.folded = count.folded.saturating_add(1);
                    return Admission::Overflow;
                }
            }
        }
        let admission = self.admit_slot();
        if admission != Admission::Refused && !is_overflow {
            self.names.entry(key.name).or_default().series += 1;
        }
        admission
    }

    /// The `max_series` half of [`SeriesBudget::admit`].
    fn admit_slot(&mut self) -> Admission {
        if self.held < self.max {
            self.held += 1;
            Admission::Open
        } else if self.idle > 0 {
            self.held += 1;
            Admission::EvictIdle
        } else {
            Admission::Refused
        }
    }

    /// Frees the slot of an idle series evicted to make room.
    fn release(&mut self, name: Symbol, is_overflow: bool) {
        self.held -= 1;
        self.idle -= 1;
        if !is_overflow {
            if let Some(count) = self.names.get_mut(&name) {
                count.series -= 1;
            }
        }
    }

    /// Resets every count to the series `flush` keeps, none updated yet in the window it opens.
    /// `is_overflow` is asked of each survivor only under the per-name limit.
    fn reset<'k>(
        &mut self,
        survivors: impl ExactSizeIterator<Item = &'k SeriesKey>,
        mut is_overflow: impl FnMut(&SeriesKey) -> bool,
    ) {
        self.held = survivors.len();
        self.idle = self.held;
        if self.per_name.is_none() {
            return;
        }
        self.names.clear();
        for key in survivors.filter(|key| !is_overflow(key)) {
            self.names.entry(key.name).or_default().series += 1;
        }
    }

    /// How many names folded a record since the last flush ([`NameCount::folded`]).
    fn overflowing(&self) -> usize {
        self.names.values().filter(|count| count.folded > 0).count()
    }
}

/// The order an eviction takes held series in, ascending from the first evicted: an ordinary
/// series before an overflow series, then the most idle, then the newest. Shared by the flush cap
/// and `Aggregator::evict_one`. An overflow series goes last because it's opened after its name's
/// other series, so it's always the newest, and evicting it first would restart a cumulative
/// overflow total every window. `open_seq` is unique, so the order is total.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EvictionRank(bool, std::cmp::Reverse<u32>, std::cmp::Reverse<u64>);

impl EvictionRank {
    fn of(state: &SeriesState, is_overflow: bool) -> Self {
        EvictionRank(
            is_overflow,
            std::cmp::Reverse(state.idle_windows),
            std::cmp::Reverse(state.open_seq),
        )
    }

    fn is_overflow(&self) -> bool {
        self.0
    }
}

/// [`Aggregator::settle_admission`]'s answer for an admitted record: the series key to open or
/// absorb into, and whether the record folded into an overflow series to get it.
struct Settled {
    key: SeriesKey,
    folded: bool,
}

/// Whether two histograms have the same bucket layout, and so can be added bucket by bucket.
/// Compared bitwise, like `SeriesKey`'s `f64` attributes: a bound is an identity, not a
/// measurement, so a `NaN` bound must equal itself rather than mismatch on every record.
pub(crate) fn bucket_bounds_match(held: &[(f64, u64)], incoming: &[(f64, u64)]) -> bool {
    held.len() == incoming.len()
        && held.iter().zip(incoming).all(|((a, _), (b, _))| a.to_bits() == b.to_bits())
}

/// Unions `incoming` into the raw members `held` under `sets: members`: insertion-ordered and
/// deduplicated by linear scan, which the cap keeps affordable. Once `held` passes `cap`, the scan
/// stops and every held member plus the rest of `incoming` goes into the returned `HyperLogLog`,
/// so the union survives the conversion. The scan makes at most `cap` compares per member, so
/// one oversized record costs O(n·cap) compares and cap-bounded memory, not its own size squared.
///
/// Also returns how many member compares the scan made, so a test can bound the cost without a
/// clock.
fn union_members(
    held: &mut Vec<Bytes>,
    incoming: &[Bytes],
    cap: usize,
) -> (Option<logit_core::HyperLogLog>, usize) {
    let mut compares = 0;
    let mut rest = incoming.iter();
    for m in rest.by_ref() {
        match held.iter().position(|h| h == m) {
            Some(i) => compares += i + 1,
            None => {
                compares += held.len();
                held.push(m.clone());
                if held.len() > cap {
                    let mut hll = logit_core::HyperLogLog::new();
                    for m in held.iter().chain(rest) {
                        hll.insert(m);
                    }
                    return (Some(hll), compares);
                }
            }
        }
    }
    (None, compares)
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
pub(crate) fn observed(buckets: &[(f64, u64)]) -> bool {
    buckets.iter().any(|(_, count)| *count > 0)
}

/// Folds a histogram's `min`/`max` across a merge under the `sum` rule: once both sides observed
/// something, a side missing the value makes the result `None`, because an extreme taken over
/// part of the observations can pair one window's `min` with another's `max` and give
/// `min > max`. A side that observed nothing is ignored, so the empty accumulator a series opens
/// with takes the first observed record's value. `pick` decides when both sides have one.
pub(crate) fn fold_extreme(
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
            last_group: None,
            diag: Diagnostics::default(),
            #[cfg(test)]
            overflow_reports_built: 0,
            telemetry: Telemetry::default(),
            current_batch_context: TraceContext::default(),
            series_retention: 0,
            max_retained_series: 0,
            budget: SeriesBudget {
                held: 0,
                idle: 0,
                max: 100_000,
                per_name: None,
                names: HashMap::new(),
            },
            eviction_order: Vec::new(),
            eviction_order_built: false,
            temporality: AggregateTemporality::default(),
            distributions: Distributions::default(),
            max_samples_per_series: 1000,
            sets: Sets::default(),
            max_set_members_per_series: 1000,
            current_scope: None,
            window_start: None,
            next_open_seq: 0,
            flags: Flags {
                closed: logit_core::interner::intern(METRICS_CLOSED),
                fields: logit_core::interner::intern(METRICS_FIELDS),
            },
            batch_flags: BatchFlags::default(),
            key_classes: KeyClasses::default(),
            overflow: OverflowKey {
                marker: logit_core::interner::intern(OVERFLOW),
                host_name: logit_core::interner::intern(HOST_NAME),
            },
        }
    }

    /// Enables cross-flush series retention; see the `series_retention`/`max_retained_series`
    /// fields. Without it, retention is `0`: strictly tumbling.
    pub fn with_series_retention(mut self, retention: u32, max_retained: usize) -> Self {
        self.series_retention = retention;
        self.max_retained_series = max_retained;
        self
    }

    /// Caps how many series the stage holds at once (this module's "The series cap"). Defaults to
    /// `100_000`, matching `logit_config`'s default. Graph validation rejects `0`; this builder
    /// doesn't check it.
    pub fn with_max_series(mut self, max_series: usize) -> Self {
        self.budget.max = max_series;
        self
    }

    /// Limits how many series each metric name holds, folding the rest into an overflow series
    /// (this module's "The per-name limit"). `None`, the default, turns the limit off. Graph
    /// validation rejects `Some(0)`; this builder doesn't check it.
    pub fn with_max_series_per_metric(mut self, limit: Option<usize>) -> Self {
        self.budget.per_name = limit;
        self
    }

    /// Selects what a flushed `Sum`/`Histogram` means (see this module's "Temporality" section).
    /// Defaults to `Delta`.
    ///
    /// `Cumulative` needs both `with_series_retention` bounds non-zero: a running total that can't
    /// survive a flush is this window's delta labeled `Cumulative`. Graph validation rejects that
    /// combination (`crates/logit-pipeline/src/graph.rs`, rule 39); this builder doesn't check it,
    /// and `flush` asserts it in debug builds.
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

    /// Reads the resource half of the flags for the batch about to be `process`ed: once per batch,
    /// not once per event, since every event of a batch shares its resource. A caller of
    /// `process` that skips this loses a flag set on the resource, and keeps the previous batch's.
    pub fn begin_batch(&mut self, resource: &Resource) {
        let attributes = &resource.attributes;
        self.batch_flags = BatchFlags {
            closed: attributes.get_sym(self.flags.closed).and_then(Closed::of),
            fields: matches!(attributes.get_sym(self.flags.fields), Some(Value::Bool(true))),
        };
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
    /// pass-through kinds, a metric whose kind conflicts with its series, and any log or span. A
    /// metric that would open a series past `max_series` is dropped (this module's "The series
    /// cap"). Returns `false` only when nothing remains on the event. An event with no metrics
    /// never touches window state.
    ///
    /// An event flagged `_logit.metrics.closed` is forwarded whole, and `_logit.metrics.fields`
    /// decides whether its `_logit.field.*` keys leave the series key (this module's doc). Each
    /// flag is the event's value, else the resource's that `begin_batch` read, so call
    /// `begin_batch` once per batch first.
    ///
    /// Grouped by `(resource, scope)` value, not `Arc` identity: two inputs that each build an
    /// empty `Resource` describe the same origin and aggregate together. The group is resolved
    /// once per call, on the first absorbable record, so a multi-record event pays one lookup
    /// (again after a record `max_series` refused before its group existed).
    /// `find_group` answers from `last_group` when the incoming `Arc`s are the ones it last saw,
    /// which holds for every event of a batch after the first. `statsd_in` holds one
    /// `Arc<Resource>` per listener, so its memo hits across batches too, until a flush clears it.
    /// `otlp_in` builds one `Arc` per `ResourceMetrics`, `logit_in` one per frame, and a Lua
    /// resource write one per batch, so each batch's first event misses the memo. That lookup
    /// hashes the pair once and scans the groups comparing each stored hash, taking the full field
    /// compare only where the hashes match.
    pub fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
        if event.metrics.is_empty() {
            return true;
        }

        // A closed event keeps every record, unmerged: the series key has no timestamp, so two
        // closed spans of one series would merge, and a flush would restamp them with its own
        // time. The event's value wins over the resource's, as for every private carrier.
        let closed = match event.attributes.get_sym(self.flags.closed) {
            Some(value) => Closed::of(value),
            None => self.batch_flags.closed,
        };
        if let Some(closed) = closed {
            let n = u32::try_from(event.metrics.len()).unwrap_or(u32::MAX);
            Tally::closed(closed, n).report(&self.telemetry);
            return true;
        }
        let scan_fields = match event.attributes.get_sym(self.flags.fields) {
            Some(value) => matches!(value, Value::Bool(true)),
            None => self.batch_flags.fields,
        };

        // Copied out once: they're per-batch, and the merge match below can't borrow `self`
        // alongside `state`.
        let ctx = self.current_batch_context;
        let scope = self.current_scope.clone();
        let temporality = self.temporality;
        let distributions = self.distributions;
        let max_samples_per_series = self.max_samples_per_series;
        let sets = self.sets;
        let max_set_members_per_series = self.max_set_members_per_series;
        let window_start = self.window_start;

        // Taken, not filtered with `retain`: a `retain` closure would borrow `event` while
        // `self.group_for` needs `&mut self`. Anything not absorbed is pushed back in its original
        // relative order.
        let metrics = std::mem::take(&mut event.metrics);
        let mut tally = Tally::default();
        // An index, not a `&mut ResourceGroup`, so the loop's other `self` uses don't conflict.
        // Only `open_group` pushes to `groups` and nothing in the loop removes one (an eviction
        // leaves an emptied group for `flush`), so it stays valid for the whole call.
        let mut group_index: Option<usize> = None;
        // Found on the event's first keyed record, and only under `_logit.metrics.fields`: an
        // unflagged event's attributes are its series identity whole.
        let mut string_fields: Option<SmallVec<[Symbol; 4]>> = None;
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
            // would pin a cumulative total at `NaN` or infinity for the series' whole life, and
            // a non-finite gauge would win last-write-wins or poison every later delta, then
            // persist through retention. Only a delta `Sum` gets here: `opener_for` passed a
            // cumulative one through.
            let non_finite = match record.kind {
                MetricKind::Sum(Sum { value, .. }) if !value.is_finite() => {
                    Some(("sum_non_finite", "sum"))
                }
                MetricKind::Gauge(value) | MetricKind::GaugeDelta(value) if !value.is_finite() => {
                    Some(("gauge_non_finite", "gauge"))
                }
                _ => None,
            };
            if let Some((key, noun)) = non_finite {
                tally.non_finite += 1;
                self.diag.warn_throttled(
                    key,
                    format_args!(
                        "{noun} '{}' has a non-finite value -- forwarding it untouched",
                        logit_core::interner::resolve(record.name)
                    ),
                );
                event.metrics.push(record);
                continue;
            }

            let string_fields = string_fields.get_or_insert_with(|| {
                if scan_fields {
                    self.key_classes.string_fields(&event.attributes, self.flags.fields)
                } else {
                    SmallVec::new()
                }
            });
            // Cloned into the key, then pruned: a local map moved in afterward copies it again per record.
            let mut key = SeriesKey {
                name: record.name,
                unit: record.unit,
                attributes: event.attributes.clone(),
            };
            for &field in string_fields.iter() {
                key.attributes.remove_sym(field);
            }
            let open_seq = self.next_open_seq;
            // A record whose group doesn't exist opens a series by definition, so it's admitted
            // before the group is: a refusal creates and memoizes nothing.
            let (gi, key, admitted, mut folded) =
                match group_index.or_else(|| self.find_group(resource, &scope)) {
                    Some(gi) => (gi, key, false, false),
                    None => {
                        let (overflow, classes) = (&self.overflow, &mut self.key_classes);
                        let admission = self.budget.admit(&key, |k| overflow.is(k, classes));
                        let Some(settled) = self.settle_admission(None, admission, key, &mut tally)
                        else {
                            continue;
                        };
                        (self.open_group(resource, &scope), settled.key, true, settled.folded)
                    }
                };
            group_index = Some(gi);
            let entry = self.groups[gi].series.entry(key);
            // Whether this metric opened a new series: the overflow series' entry when it folded.
            // Not derivable afterward: a real `Gauge(0.0)` at `at: i64::MIN` looks identical to an
            // unseeded delta's result. The `Sum` and `GaugeDelta` arms seed the series' value from
            // it, and `GaugeDelta`'s unseeded count reads it.
            let mut was_vacant = matches!(entry, std::collections::hash_map::Entry::Vacant(_));
            // `admit` is asked in place, beside the entry: only an answer other than `Open` gives
            // the entry up, so only an admission eviction or a fold pays a second hash.
            let entry = match entry {
                std::collections::hash_map::Entry::Vacant(vacant) if !admitted => {
                    let (overflow, classes) = (&self.overflow, &mut self.key_classes);
                    match self.budget.admit(vacant.key(), |k| overflow.is(k, classes)) {
                        Admission::Open => std::collections::hash_map::Entry::Vacant(vacant),
                        admission => {
                            let key = vacant.into_key();
                            let Some(settled) =
                                self.settle_admission(Some(gi), admission, key, &mut tally)
                            else {
                                continue;
                            };
                            folded = settled.folded;
                            let entry = self.groups[gi].series.entry(settled.key);
                            was_vacant =
                                matches!(entry, std::collections::hash_map::Entry::Vacant(_));
                            entry
                        }
                    }
                }
                entry => entry,
            };
            let state = entry.or_insert_with(|| SeriesState {
                accumulator: opener.open(),
                contexts: ContributingContexts::default(),
                idle_windows: 0,
                open_seq,
                first_seen: window_start
                    .map_or(event.timestamp, |start| event.timestamp.max(start)),
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
                            if was_vacant {
                                *total = *value;
                            } else {
                                let (sum, widened) = total.checked_add(*value);
                                *total = sum;
                                tally.widened += u32::from(widened);
                            }
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
                        // (`docs/adr/relative-gauge-adjustments.md`). A delta opening the series
                        // resolves against zero, which keeps its own type.
                        if was_vacant {
                            *value = *d;
                        } else {
                            let (sum, widened) = value.checked_add(*d);
                            *value = sum;
                            tally.widened += u32::from(widened);
                        }
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
                    // `sets: members`: see `union_members`.
                    Accumulator::SetMembers(held) => {
                        let (overflow, _) =
                            union_members(held, incoming, max_set_members_per_series);
                        if let Some(hll) = overflow {
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
                tally.overflowed += u32::from(folded);
                // Only on a real merge: a kind-conflicted metric isn't a contributor.
                state.contexts.observe(ctx);
                if !was_vacant && !state.updated_this_window {
                    self.budget.idle -= 1; // no longer an eviction candidate this window
                }
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
                        "logit.transform.samples.omitted",
                        f64::from(samples_non_finite),
                        &[("reason", "non_finite")],
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
            if was_vacant {
                self.next_open_seq += 1;
            }
        }
        tally.report(&self.telemetry);

        !(event.metrics.is_empty() && event.log.is_none() && event.span.is_none())
    }

    /// The index in `groups` of the `(resource, scope)` group, memoized, or `None` when no group
    /// holds it. Opens nothing: a record with no group opens a series, which `max_series` may
    /// refuse ([`Aggregator::open_group`] opens one).
    fn find_group(
        &mut self,
        resource: &Arc<Resource>,
        scope: &Option<Arc<Scope>>,
    ) -> Option<usize> {
        if let Some(memo) = self.last_group.as_ref().filter(|m| m.matches(resource, scope)) {
            return Some(memo.index);
        }
        // The hash only rules a group out; a match still takes the full compare, so a collision
        // can't merge two groups.
        let hash = group_hash(resource, scope);
        let index = self.groups.iter().position(|g| {
            g.hash == hash
                && resource_key_eq(&g.resource, resource)
                && scope_key_eq(&g.scope, scope)
        })?;
        self.last_group =
            Some(GroupMemo { resource: resource.clone(), scope: scope.clone(), index });
        Some(index)
    }

    /// Opens the `(resource, scope)` group, which `find_group` didn't find, and memoizes it.
    fn open_group(&mut self, resource: &Arc<Resource>, scope: &Option<Arc<Scope>>) -> usize {
        self.groups.push(ResourceGroup {
            resource: resource.clone(),
            scope: scope.clone(),
            hash: group_hash(resource, scope),
            series: HashMap::new(),
        });
        let index = self.groups.len() - 1;
        self.last_group =
            Some(GroupMemo { resource: resource.clone(), scope: scope.clone(), index });
        index
    }

    /// Acts on [`SeriesBudget::admit`]'s answer for a record that would open the series `key`,
    /// at both places `process` asks: before the record's group exists (`gi` is `None`), and on a
    /// vacant entry in group `gi`. Returns the key to open or absorb into, or `None` for a
    /// refused record, counted and diagnosed. A fold swaps in the overflow key and, when that
    /// series isn't held yet, asks `admit` again for it, which never answers `Overflow`.
    fn settle_admission(
        &mut self,
        gi: Option<usize>,
        admission: Admission,
        key: SeriesKey,
        tally: &mut Tally,
    ) -> Option<Settled> {
        match admission {
            Admission::Open => Some(Settled { key, folded: false }),
            Admission::EvictIdle => {
                self.evict_one(tally);
                Some(Settled { key, folded: false })
            }
            Admission::Refused => {
                tally.series_limit += 1;
                self.refuse(key.name);
                None
            }
            Admission::Overflow => {
                self.report_overflow();
                let key = self.overflow.of(key, &mut self.key_classes);
                if gi.is_some_and(|gi| self.groups[gi].series.contains_key(&key)) {
                    return Some(Settled { key, folded: true });
                }
                let (overflow, classes) = (&self.overflow, &mut self.key_classes);
                let admission = self.budget.admit(&key, |k| overflow.is(k, classes));
                debug_assert_ne!(admission, Admission::Overflow, "an overflow key never folds");
                let settled = self.settle_admission(gi, admission, key, tally)?;
                Some(Settled { key: settled.key, folded: true })
            }
        }
    }

    /// Makes room under a full `max_series` by evicting one held series not updated this window,
    /// in the flush cap's order ([`EvictionRank`]). The order is built on the window's first
    /// eviction, the one scan and sort this path pays, and each later eviction pops it, skipping
    /// a series that absorbed since. A group this empties stays until `flush` removes it, so
    /// `group_index`, the memo, and the order's group indices stay valid; the groups a window
    /// holds are then at most `max_series` plus its evictions.
    fn evict_one(&mut self, tally: &mut Tally) {
        // Built once per window: no series becomes a candidate between flushes, so an exhausted
        // order means none is left, which `budget.idle` already says.
        if !self.eviction_order_built {
            self.eviction_order_built = true;
            for (gi, group) in self.groups.iter().enumerate() {
                for (key, state) in &group.series {
                    if !state.updated_this_window {
                        let rank =
                            EvictionRank::of(state, self.overflow.is(key, &mut self.key_classes));
                        self.eviction_order.push((rank, gi, key.clone()));
                    }
                }
            }
            // Descending, so `pop` takes the rank that evicts first.
            self.eviction_order.sort_unstable_by(|(a, _, _), (b, _, _)| b.cmp(a));
        }
        while let Some((rank, gi, key)) = self.eviction_order.pop() {
            let series = &mut self.groups[gi].series;
            if series.get(&key).is_none_or(|state| state.updated_this_window) {
                continue; // absorbed since the order was built
            }
            series.remove(&key);
            self.budget.release(key.name, rank.is_overflow());
            tally.series_limit_evicted += 1;
            // Warned like the flush cap's eviction, for the same two consequences.
            self.diag.warn_throttled(
                "series_limit_evicted",
                format_args!(
                    "max_series ({}) is full; evicted a series not updated this window to open \
                     a new one -- a later gauge delta against an evicted series will resolve \
                     against 0.0, and an evicted cumulative series restarts from zero",
                    self.budget.max
                ),
            );
            return;
        }
        debug_assert!(false, "budget.idle > 0 with no series left in the eviction order");
    }

    /// The throttled diagnostic for a record that reached `max_series_per_metric`, the one place
    /// a metric name over the limit is named. The report scans every held series of the worst
    /// name, so it's built only when the throttle will let it out.
    fn report_overflow(&mut self) {
        const KEY: &str = "series_per_metric_overflow";
        let report = self.diag.would_report(KEY).then(|| {
            #[cfg(test)]
            {
                self.overflow_reports_built += 1;
            }
            OverflowReport::of(&self.budget, &self.groups, self.overflow.marker).to_string()
        });
        self.diag.warn_throttled(KEY, format_args!("{}", report.as_deref().unwrap_or_default()));
    }

    /// The diagnostic for a record `max_series` refused; `settle_admission` counts it.
    fn refuse(&mut self, name: Symbol) {
        self.diag.warn_throttled(
            "series_limit",
            format_args!(
                "max_series ({}) is full and no series held is idle; dropping metric '{}', which \
                 would open a new series -- bound attribute cardinality upstream with keep \
                 or keep_values, or raise max_series",
                self.budget.max,
                logit_core::interner::resolve(name)
            ),
        );
    }

    /// Emits one event per series updated this window, stamped with `now` and paired with its
    /// `SpanLink`s.
    ///
    /// Every non-retainable accumulator is removed. A retainable series (a gauge in either mode, a
    /// `Sum`/`Histogram` under `temporality: cumulative`) survives into the next window when
    /// `series_retention > 0`, subject to `max_retained_series`.
    pub fn flush(&mut self, now: i64) -> FlushOutput {
        debug_assert!(
            self.temporality == AggregateTemporality::Delta
                || (self.series_retention > 0 && self.max_retained_series > 0),
            "temporality: cumulative without both retention bounds, which graph rule 39 rejects"
        );
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
        if self.budget.per_name.is_some() {
            let overflowing = self.budget.overflowing();
            self.telemetry.gauge("logit.transform.metrics.overflowing", overflowing as f64, &[]);
        }

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
                        // The reset signal (`SeriesState::first_seen`), kept once lowered so
                        // it never moves while the series lives. A `Gauge` has no start time and
                        // keeps `0`, OTLP's "unknown".
                        if matches!(record.kind, MetricKind::Sum(_) | MetricKind::Histogram(_)) {
                            state.first_seen = state.first_seen.min(now);
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

        // Cardinality cap, in `EvictionRank`'s order: the most idle first and the newest among
        // equally idle, an overflow series last. Without it, C never-repeating series names per
        // window would hold C * series_retention series indefinitely. Newest first keeps a series
        // updated every window ahead of the one-off series opened after it. The rank is total, so
        // an unstable sort gives one order.
        let mut evicted_active: u64 = 0;
        let mut evicted_cardinality_idle: u64 = 0;
        if survivors.len() > self.max_retained_series {
            let excess = survivors.len() - self.max_retained_series;
            let (overflow, classes) = (&self.overflow, &mut self.key_classes);
            survivors.sort_unstable_by_key(|(_, key, state)| {
                EvictionRank::of(state, overflow.is(key, classes))
            });
            for (_, _, state) in survivors.drain(0..excess) {
                // An updated survivor left the loop above with `idle_windows == 0`.
                if state.idle_windows == 0 {
                    evicted_active += 1;
                } else {
                    evicted_cardinality_idle += 1;
                }
            }
        }
        let evicted_cardinality = evicted_active + evicted_cardinality_idle;

        // Survivors go back to their group; a group left empty is dropped. Every other series left
        // the stage above, so the survivors are what the budget holds, none updated yet in the
        // window this flush opens. The eviction order's group indices end here.
        let (overflow, classes) = (&self.overflow, &mut self.key_classes);
        self.budget.reset(survivors.iter().map(|(_, key, _)| key), |k| overflow.is(k, classes));
        self.eviction_order.clear();
        self.eviction_order_built = false;
        for (gi, key, state) in survivors {
            self.groups[gi].series.insert(key, state);
        }
        self.groups.retain(|g| !g.series.is_empty());
        self.last_group = None;
        self.window_start = Some(now);

        if total_dropped_links > 0 {
            self.telemetry.count(
                "logit.transform.links.omitted",
                total_dropped_links as f64,
                &[("reason", "contexts")],
            );
        }
        if evicted_idle > 0 {
            self.telemetry.count(
                "logit.transform.series.evicted",
                evicted_idle as f64,
                &[("reason", "idle")],
            );
        }
        // Split by whether the series was updated this window: an active one was emitted this
        // flush and is then lost, the loss worth alerting on.
        for (state, n) in [("active", evicted_active), ("idle", evicted_cardinality_idle)] {
            if n > 0 {
                self.telemetry.count(
                    "logit.transform.series.evicted",
                    n as f64,
                    &[("reason", "cardinality"), ("state", state)],
                );
            }
        }
        if evicted_cardinality > 0 {
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

/// Delegation: the inherent methods match `Transform`'s contract, `begin_batch` taking only the
/// resource.
impl Transform for Aggregator {
    fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
        Aggregator::process(self, resource, event)
    }

    fn begin_batch(
        &mut self,
        resource: &Arc<Resource>,
        _scope: Option<&Arc<Scope>>,
        _events: &mut Vec<Event>,
    ) {
        Aggregator::begin_batch(self, resource)
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

/// Which attribute keys are private (`_logit.*`), and which of those are in the field namespace
/// (`_logit.field.*`), which a series key leaves out on a flagged event: such a key is data about
/// a point, not identity of its series, and an aggregate summarizes numbers. Keyed on them, two
/// readings of one series that differ only in a string such as Telegraf's `uptime_format` would
/// flush as two series that every protocol sink but `influxdb_out` renders with the same labels.
/// Every other private key stays in the key: `_logit.influxdb.measurement` and other protocols'
/// carriers are identity that a sink reads after this stage, and an overflow series keeps them
/// ([`OverflowKey`]).
///
/// Memoized by the key's interner index, so only a key's first sighting resolves it: `resolve`
/// takes a shard lock and a hash. One byte per index up to the highest key seen, which the
/// process-wide interner bounds.
#[derive(Default)]
struct KeyClasses {
    /// Per interner index: [`UNSEEN`], [`STRING_FIELD`], [`PRIVATE_KEY`], or [`OTHER_KEY`].
    seen: Vec<u8>,
}

const UNSEEN: u8 = 0;
const STRING_FIELD: u8 = 1;
const PRIVATE_KEY: u8 = 2;
const OTHER_KEY: u8 = 3;

impl KeyClasses {
    /// The field-namespace keys among `attributes`, plus `fields_flag` when present: the flag
    /// announces those keys, so it leaves the series key with them, and a series whose readings
    /// differ only in whether they carried a string field stays one series.
    fn string_fields(
        &mut self,
        attributes: &AttrMap,
        fields_flag: Symbol,
    ) -> SmallVec<[Symbol; 4]> {
        attributes
            .iter()
            .map(|(key, _)| key)
            .filter(|&key| key == fields_flag || self.class(key) == STRING_FIELD)
            .collect()
    }

    /// Whether `key` is in the private namespace, field keys included.
    fn is_private(&mut self, key: Symbol) -> bool {
        matches!(self.class(key), STRING_FIELD | PRIVATE_KEY)
    }

    fn class(&mut self, key: Symbol) -> u8 {
        let i = logit_core::interner::index(key);
        if i >= self.seen.len() {
            self.seen.resize(i + 1, UNSEEN);
        }
        if self.seen[i] == UNSEEN {
            let name = logit_core::interner::resolve(key);
            self.seen[i] = if logit_core::attrs::is_field(name) {
                STRING_FIELD
            } else if logit_core::attrs::is_private(name) {
                PRIVATE_KEY
            } else {
                OTHER_KEY
            };
        }
        self.seen[i]
    }
}

/// The overflow series' attribute set under `max_series_per_metric`: `logit.overflow: true`, the
/// record's private `_logit.*` keys (protocol carriers and metric flags a sink in its family
/// reads), and `host.name` (the host every sink writes). Everything else is the detail the fold
/// gives up. A series is an overflow series when its key has that shape, never by a flag on its
/// state, so an upstream stage's overflow series is one here too.
struct OverflowKey {
    /// `logit.overflow`.
    marker: Symbol,
    /// `host.name`.
    host_name: Symbol,
}

impl OverflowKey {
    /// `key`'s overflow series: its name and unit, and the attributes it keeps.
    fn of(&self, key: SeriesKey, classes: &mut KeyClasses) -> SeriesKey {
        let mut attributes = AttrMap::new();
        for (k, v) in key.attributes.into_pairs() {
            if k == self.host_name || classes.is_private(k) {
                attributes.insert_sym(k, v);
            }
        }
        attributes.insert_sym(self.marker, Value::Bool(true));
        SeriesKey { name: key.name, unit: key.unit, attributes }
    }

    /// Whether `key` is an overflow series' key: `logit.overflow: true` and no attribute
    /// [`OverflowKey::of`] would remove. A key without the marker answers after one lookup.
    fn is(&self, key: &SeriesKey, classes: &mut KeyClasses) -> bool {
        matches!(key.attributes.get_sym(self.marker), Some(Value::Bool(true)))
            && key
                .attributes
                .iter()
                .all(|(k, _)| k == self.marker || k == self.host_name || classes.is_private(k))
    }
}

/// The text of the `series_per_metric_overflow` diagnostic: the overflowing name with the most
/// records folded since the last flush (every overflowing name holds the limit's count of series,
/// so the series count can't rank them), the attribute key whose values differ most among that
/// name's held series, and how many other names are overflowing. Names and keys only: an
/// attribute value never appears.
struct OverflowReport {
    limit: usize,
    name: Symbol,
    folded: u32,
    /// The key with the most distinct values, and how many.
    widest: Option<(Symbol, usize)>,
    others: usize,
}

impl OverflowReport {
    fn of(budget: &SeriesBudget, groups: &[ResourceGroup], marker: Symbol) -> Self {
        let overflowing = budget.names.iter().filter(|(_, count)| count.folded > 0);
        // Ties go to the earlier-interned name, so the report is deterministic.
        let (name, folded) = overflowing
            .clone()
            .map(|(name, count)| (*name, count.folded))
            .max_by_key(|&(name, folded)| {
                (folded, std::cmp::Reverse(logit_core::interner::index(name)))
            })
            .expect("a record reached the limit, so its name is overflowing");
        let others = overflowing.count() - 1;
        // Distinct values per key, as value hashes: `Value` has no `Hash`, and two values that
        // collide only undercount a key.
        let mut values: HashMap<Symbol, std::collections::HashSet<u64>> = HashMap::new();
        for key in groups.iter().flat_map(|g| g.series.keys()) {
            if key.name != name || key.attributes.get_sym(marker).is_some() {
                continue;
            }
            for (k, v) in key.attributes.iter() {
                let mut hasher = DefaultHasher::new();
                hash_value(v, &mut hasher);
                values.entry(k).or_default().insert(hasher.finish());
            }
        }
        let widest = values
            .into_iter()
            .map(|(k, distinct)| (k, distinct.len()))
            .max_by_key(|&(k, n)| (n, std::cmp::Reverse(logit_core::interner::index(k))));
        OverflowReport { limit: budget.per_name.unwrap_or(0), name, folded, widest, others }
    }
}

impl std::fmt::Display for OverflowReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "max_series_per_metric ({}) reached; folding new series of metric '{}' into its \
             logit.overflow series ({} records this window)",
            self.limit,
            logit_core::interner::resolve(self.name),
            self.folded
        )?;
        if let Some((key, n)) = self.widest {
            write!(
                f,
                ", whose attribute '{}' has the most distinct values ({n})",
                logit_core::interner::resolve(key)
            )?;
        }
        write!(
            f,
            "; {} other metric names are over the limit -- bound attribute cardinality upstream \
             with keep or keep_values, or raise max_series_per_metric",
            self.others
        )
    }
}

/// A metric series' identity: name, unit, and attribute set.
///
/// `AttrMap`/`Value` implement neither `Eq` nor `Hash`, so this compares and hashes `f64` by
/// `to_bits()`: a `NaN` attribute then equals itself, instead of opening a new series per event.
#[derive(Clone)]
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

/// Field-wise equality for a `find_group` resource key, comparing `Value::F64` bitwise, the rule
/// `SeriesKey` and [`scope_key_eq`] follow. `Resource`'s derived `PartialEq` judges a `NaN`
/// attribute unequal to itself, even through one `Arc`, which would open a new `ResourceGroup` per
/// metric carrying it; and it judges `-0.0` equal to `0.0`, which series identity keeps apart.
///
/// `Arc::ptr_eq` first: an input that reuses one `Arc<Resource>` skips the field walk.
pub(crate) fn resource_key_eq(a: &Arc<Resource>, b: &Arc<Resource>) -> bool {
    Arc::ptr_eq(a, b)
        || (a.schema_url == b.schema_url
            && a.dropped_attributes_count == b.dropped_attributes_count
            && attr_map_key_eq(&a.attributes, &b.attributes))
}

/// Field-wise equality for a `find_group` scope key, comparing `Value::F64` bitwise. `Scope`'s
/// derived `PartialEq` judges a `NaN` attribute unequal to itself, which would open a new
/// `ResourceGroup` per event carrying it (the failure `SeriesKey` avoids the same way).
///
/// `Arc::ptr_eq` first: events from one batch share one `Arc<Scope>`, so the common case skips
/// the field walk.
pub(crate) fn scope_key_eq(a: &Option<Arc<Scope>>, b: &Option<Arc<Scope>>) -> bool {
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

/// A `(resource, scope)` group's hash, over the fields [`resource_key_eq`] and [`scope_key_eq`]
/// compare, with floats by bit pattern, so key-equal pairs hash equal. A fresh
/// `DefaultHasher::new()` has fixed keys, so a stored hash and a lookup hash agree within the
/// process.
pub(crate) fn group_hash(resource: &Resource, scope: &Option<Arc<Scope>>) -> u64 {
    let mut state = DefaultHasher::new();
    resource.schema_url.hash(&mut state);
    resource.dropped_attributes_count.hash(&mut state);
    hash_attr_map(&resource.attributes, &mut state);
    match scope {
        None => 0u8.hash(&mut state),
        Some(scope) => {
            1u8.hash(&mut state);
            scope.name.hash(&mut state);
            scope.version.hash(&mut state);
            scope.schema_url.hash(&mut state);
            scope.dropped_attributes_count.hash(&mut state);
            hash_attr_map(&scope.attributes, &mut state);
        }
    }
    state.finish()
}

/// The length first, so the resource's attributes can't run into the scope fields after them.
/// `AttrMap::iter()` yields `Symbol` order, so insertion order doesn't change the hash.
pub(crate) fn hash_attr_map<H: Hasher>(map: &AttrMap, state: &mut H) {
    map.len().hash(state);
    for (k, v) in map.iter() {
        k.hash(state);
        hash_value(v, state);
    }
}

/// `AttrMap` equality with bitwise floats, for [`resource_key_eq`] and [`scope_key_eq`].
/// `AttrMap::iter()` yields `Symbol` order, so a length check plus a zipped walk is
/// order-independent.
pub(crate) fn attr_map_key_eq(a: &AttrMap, b: &AttrMap) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
}

pub(crate) fn value_key_eq(a: &Value, b: &Value) -> bool {
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

pub(crate) fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
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

    /// Runs `begin_batch` and `process` on an owned event, as a one-event batch: `Some` is what
    /// was forwarded, `None` means absorbed.
    fn feed(agg: &mut Aggregator, resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        agg.begin_batch(resource);
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
            MetricKind::Sum(sum) => sum.value.as_f64(),
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

    /// `datadog_in` gives every series batch one resource, whether or not a payload carries a
    /// unit, so one series from two payloads aggregates in one group.
    #[test]
    fn datadog_series_from_payloads_with_and_without_units_share_a_group() {
        let mut decoder = logit_proto::datadog::DatadogDecoder::new();
        let with_unit = br#"{"series":[{"metric":"hits","points":[[1700000000,1]],"type":"count","host":"h"},{"metric":"bytes","points":[[1700000000,5]],"type":"count","host":"h","unit":"byte"}]}"#;
        let without = br#"{"series":[{"metric":"hits","points":[[1700000001,2]],"type":"count","host":"h"}]}"#;
        let mut agg = Aggregator::new(Duration::from_secs(10));
        for body in [&with_unit[..], &without[..]] {
            let batch = decoder.decode_series_v1(body, 0).unwrap();
            for event in batch.events {
                assert!(feed(&mut agg, &batch.resource, event).is_none());
            }
        }
        let flushed = flush_events(&mut agg, 100);
        let hits: Vec<f64> = flushed
            .iter()
            .flat_map(|(_, events)| events)
            .filter(|e| logit_core::interner::resolve(e.metrics[0].name) == "hits")
            .map(|e| counter_value(kind_of(e)))
            .collect();
        assert_eq!(hits, [3.0], "one `hits` series, summed across both payloads");
        assert_eq!(flushed.len(), 1, "one resource group");
    }

    #[test]
    fn gauge_keeps_the_value_with_the_latest_source_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        // Deliberately out of arrival order: the later-timestamped value (5) arrives first.
        feed(&mut agg, &resource, metric_event("temp", MetricKind::gauge(5.0), 50));
        feed(&mut agg, &resource, metric_event("temp", MetricKind::gauge(1.0), 10));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(*v, MetricValue::F64(5.0), "should keep the value stamped at t=50")
            }
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
                value: MetricValue::F64(1.0),
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
                temporality: Temporality::Cumulative,
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
        let sum = |temporality| {
            MetricKind::Sum(Sum { value: MetricValue::F64(1.0), temporality, monotonic: true })
        };
        vec![
            sum(Temporality::Delta),
            sum(Temporality::Cumulative),
            MetricKind::gauge(1.0),
            MetricKind::GaugeDelta(MetricValue::F64(1.0)),
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
                temporality: Temporality::Cumulative,
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
            0 => Accumulator::Sum { total: MetricValue::F64(1.0), monotonic: true },
            1 => Accumulator::Histogram(logit_core::Histogram {
                buckets: vec![(1.0, 1)],
                temporality: Temporality::Cumulative,
                sum: None,
                min: None,
                max: None,
            }),
            2 => Accumulator::Gauge { value: MetricValue::F64(1.0), at: 0 },
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
                    if retention == 0 && t == AggregateTemporality::Cumulative {
                        continue; // rule 39 rejects it, and `flush` asserts so
                    }
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
                        open_seq: 0,
                        first_seen: 0,
                        updated_this_window: true,
                        description: None,
                    };
                    let mut group = ResourceGroup::new(default_resource(), None);
                    group.series.insert(key, state);
                    agg.groups.push(group);

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
                value: MetricValue::F64(5.0),
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
                    value: MetricValue::F64(1.0),
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
                    value: MetricValue::F64(2.0),
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
                assert_eq!(sum.value, MetricValue::F64(3.0));
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
        let event = metric_event("temp", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 0);
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

        assert!(feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 0)
        )
        .is_none());

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(*v, MetricValue::F64(5.0), "unseeded delta resolves against 0.0")
            }
            other => panic!("expected Gauge, got {other:?}"),
        }

        let drained = registry.drain(0);
        let unseeded = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.gauge.delta.unseeded" =>
                {
                    Some(sum.value.as_f64())
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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 1),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, MetricValue::F64(15.0)),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A later absolute replaces the running value rather than adding to it.
    #[test]
    fn delta_then_absolute_is_subsumed_by_the_absolute() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 0),
        );
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(*v, MetricValue::F64(10.0), "the absolute should win outright")
            }
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A delta never advances `at`: the t=50 absolute after a t=60 delta still wins (99, not 15).
    #[test]
    fn a_delta_never_advances_the_last_write_wins_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 50));
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 60),
        );
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(99.0), 50));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, MetricValue::F64(99.0)),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn two_deltas_accumulate() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 1),
        );
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(-3.0)), 2),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, MetricValue::F64(12.0)),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A `GaugeDelta` against a `Sum` series is a kind conflict and is forwarded.
    #[test]
    fn gauge_delta_against_a_counter_series_is_a_kind_conflict_and_is_forwarded() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(feed(&mut agg, &resource, metric_event("m", MetricKind::counter(1.0), 0)).is_none());

        let conflicting = metric_event("m", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 0);
        let passed = feed(&mut agg, &resource, conflicting);
        assert!(passed.is_some(), "the conflicting delta should be forwarded, not absorbed");
        assert!(
            matches!(passed.unwrap().metrics[0].kind, MetricKind::GaugeDelta(v) if v == MetricValue::F64(5.0))
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(counter_value(kind_of(&events[0])), 1.0, "the counter should be untouched");
    }

    /// A gauge series always flushes as `Gauge`, never `GaugeDelta`.
    #[test]
    fn a_gauge_delta_never_survives_aggregate() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 0),
        );

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

    /// Two readings of one InfluxDB series that differ only in a string field are one series
    /// under `_logit.metrics.fields`: the string field and the flag are left out of the key and
    /// the output, and every other private key stays. A reading with no string field, and so no
    /// flag, joins the same series.
    #[test]
    fn an_influxdb_string_field_is_not_series_identity() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for (value, uptime, at) in
            [(1.0, Some("1 day"), 10), (2.0, Some("2 days"), 20), (3.0, None, 30)]
        {
            let mut event = metric_event("system.uptime", MetricKind::gauge(value), at);
            event.attributes.insert("host", "a");
            event.attributes.insert("_logit.influxdb.measurement", "system");
            if let Some(uptime) = uptime {
                event.attributes.insert("_logit.field.uptime_format", uptime);
                event.attributes.insert(METRICS_FIELDS, true);
            }
            feed(&mut agg, &resource, event);
        }

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "a string field shouldn't split the series");
        assert_eq!(*kind_of(&events[0]), MetricKind::gauge(3.0), "the latest reading wins");
        let attributes = &events[0].attributes;
        assert_eq!(attributes.get("_logit.field.uptime_format"), None);
        assert_eq!(attributes.get(METRICS_FIELDS), None, "the flag leaves with its keys");
        assert_eq!(attributes.get("host").and_then(Value::as_str), Some("a"));
        assert_eq!(
            attributes.get("_logit.influxdb.measurement").and_then(Value::as_str),
            Some("system"),
            "the measurement carrier is identity and stays"
        );
    }

    /// Without `_logit.metrics.fields`, nothing is scanned for: a `_logit.field.*` key is identity
    /// like any other, so two values split the series and each flushed series keeps its key. The
    /// flag on the resource enables the scan for the whole batch.
    #[test]
    fn the_fields_flag_gates_the_field_scan() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for (value, note) in [(1.0, "a"), (2.0, "b")] {
            let mut event = metric_event("m.x", MetricKind::gauge(value), 10);
            event.attributes.insert("_logit.field.note", note);
            feed(&mut agg, &resource, event);
        }
        let flushed = flush_events(&mut agg, 100);
        let mut notes: Vec<&str> = flushed[0]
            .1
            .iter()
            .map(|e| e.attributes.get("_logit.field.note").and_then(Value::as_str).unwrap())
            .collect();
        notes.sort_unstable();
        assert_eq!(notes, ["a", "b"], "unflagged, the field key is identity");

        let mut flagged = Resource::default();
        flagged.attributes.insert(METRICS_FIELDS, true);
        let flagged = Arc::new(flagged);
        for (value, note) in [(1.0, "a"), (2.0, "b")] {
            let mut event = metric_event("m.x", MetricKind::gauge(value), 10);
            event.attributes.insert("_logit.field.note", note);
            feed(&mut agg, &flagged, event);
        }
        let flushed = flush_events(&mut agg, 200);
        let events: Vec<&Event> = flushed.iter().flat_map(|(_, events)| events).collect();
        assert_eq!(events.len(), 1, "flagged on the resource, the field key isn't identity");
        assert_eq!(events[0].attributes.get("_logit.field.note"), None);
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

        let mut flagged = metric_event("conns", MetricKind::gauge(0.0), 0);
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
                    Some(sum.value.as_f64())
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
        let conflicting = metric_event("m", MetricKind::gauge(5.0), 0);
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
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::gauge(5.0)));

        let passed =
            feed(&mut agg, &resource, event).expect("the conflicting gauge should be forwarded");
        assert_eq!(passed.metrics.len(), 1, "the absorbed counter should not also be forwarded");
        assert!(
            matches!(passed.metrics[0].kind, MetricKind::Gauge(v) if v == MetricValue::F64(5.0))
        );

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
        assert_eq!(
            counter_with_tag(&drained, "logit.transform.links.omitted", "reason", "contexts"),
            Some(1.0),
            "the 9th distinct context should be omitted and counted under reason=contexts"
        );
        assert_eq!(
            counter_with_tag(&drained, "logit.transform.links.dropped", "reason", "contexts"),
            None
        );
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
                MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => {
                    Some(v.as_f64())
                }
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

    /// The total over every point carrying `tag_value` under `tag`, which may also carry other
    /// tags (`series.evicted{reason="cardinality"}` splits by `state`), or `None` without one.
    fn counter_with_tag(events: &[Event], name: &str, tag: &str, tag_value: &str) -> Option<f64> {
        events
            .iter()
            .filter(|e| e.attributes.get(tag).and_then(|v| v.as_str()) == Some(tag_value))
            .flat_map(|e| &e.metrics)
            .filter_map(|m| match &m.kind {
                MetricKind::Sum(sum) if logit_core::interner::resolve(m.name) == name => {
                    Some(sum.value.as_f64())
                }
                _ => None,
            })
            .reduce(|a, b| a + b)
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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));
        let flushed = flush_events(&mut agg, 100);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, MetricValue::F64(10.0)),
            other => panic!("expected Gauge, got {other:?}"),
        }

        // Window 2: only a delta.
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 150),
        );
        let flushed = flush_events(&mut agg, 200);
        assert_eq!(flushed.len(), 1);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(
                    *v,
                    MetricValue::F64(15.0),
                    "the delta should resolve against window 1's final value"
                )
            }
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn a_retained_idle_gauge_emits_nothing_that_window() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));
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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));
        assert_eq!(agg.flush(100).len(), 1, "flush 1: emits, idle_windows resets to 0");
        assert!(agg.flush(200).is_empty(), "flush 2: idle_windows -> 1, still under retention 2");
        assert!(agg.flush(300).is_empty(), "flush 3: idle_windows -> 2, now evicted");

        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(5.0)), 350),
        );
        let flushed = flush_events(&mut agg, 400);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(*v, MetricValue::F64(5.0), "should resolve against 0.0 post-eviction")
            }
            other => panic!("expected Gauge, got {other:?}"),
        }

        let drained = registry.drain(0);
        let unseeded = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.gauge.delta.unseeded" =>
                {
                    Some(sum.value.as_f64())
                }
                _ => None,
            })
        });
        assert_eq!(unseeded, Some(1.0), "the post-eviction delta should count as unseeded");
    }

    /// With `series_retention: N`, a series survives N idle flushes after the one that emitted it
    /// and is evicted, counted `idle`, at the flush that closes the Nth.
    #[test]
    fn a_series_survives_series_retention_idle_flushes() {
        for retention in 1..=3u32 {
            let registry = logit_core::Registry::new();
            let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
            let mut agg = Aggregator::new(Duration::from_secs(10))
                .with_series_retention(retention, 100)
                .with_telemetry(telemetry);
            let resource = default_resource();
            feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));
            assert_eq!(agg.flush(100).len(), 1);
            for idle in 1..=retention {
                assert!(agg.flush(100 + i64::from(idle) * 100).is_empty());
                let held = series_count(&agg);
                let evicted = evicted_count(&registry.drain(0), "idle");
                if idle < retention {
                    assert_eq!((held, evicted), (1, None), "retention {retention}, idle {idle}");
                } else {
                    assert_eq!(
                        (held, evicted),
                        (0, Some(1.0)),
                        "retention {retention}, idle {idle}"
                    );
                }
            }
        }
    }

    /// `series_retention: 0` retains nothing across flushes.
    #[test]
    fn series_retention_zero_reproduces_the_strictly_tumbling_output() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(0, 0);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("temp", MetricKind::gauge(5.0), 50));
        feed(&mut agg, &resource, metric_event("temp", MetricKind::gauge(1.0), 10));

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(*v, MetricValue::F64(5.0), "should keep the value stamped at t=50")
            }
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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 500));
        flush_events(&mut agg, 1000);

        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(99.0), 1));
        let flushed = flush_events(&mut agg, 2000);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(
                *v,
                MetricValue::F64(99.0),
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
                metric_event(&format!("g{i}"), MetricKind::gauge(i as f64), 0),
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
                    Some(sum.value.as_f64())
                }
                _ => None,
            })
        });
        assert_eq!(evicted_cardinality, Some(1.0), "exactly one series should exceed the cap");
    }

    /// `logit.transform.series.evicted{reason="cardinality", state}`'s value in drained telemetry,
    /// or 0.
    fn evicted_cardinality(events: &[Event], state: &str) -> f64 {
        events
            .iter()
            .filter(|e| {
                e.attributes.get("reason").and_then(|v| v.as_str()) == Some("cardinality")
                    && e.attributes.get("state").and_then(|v| v.as_str()) == Some(state)
            })
            .flat_map(|e| &e.metrics)
            .filter_map(|m| match &m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.series.evicted" =>
                {
                    Some(sum.value.as_f64())
                }
                _ => None,
            })
            .sum()
    }

    fn series_count(agg: &Aggregator) -> usize {
        agg.groups.iter().map(|g| g.series.len()).sum()
    }

    /// Among equally idle series the newest goes first, and an idle series before an active one.
    #[test]
    fn the_cardinality_cap_evicts_the_most_idle_then_the_newest() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(5, 2)
            .with_telemetry(telemetry);
        let resource = default_resource();
        let names = |agg: &Aggregator| {
            let mut names: Vec<&str> = agg
                .groups
                .iter()
                .flat_map(|g| g.series.keys())
                .map(|k| logit_core::interner::resolve(k.name))
                .collect();
            names.sort_unstable();
            names
        };

        feed(&mut agg, &resource, metric_event("a", MetricKind::gauge(1.0), 0));
        feed(&mut agg, &resource, metric_event("b", MetricKind::gauge(1.0), 0));
        agg.flush(100);
        registry.drain(0);

        // `a` and `b` are idle for one flush and `c` is active: one of the idle pair goes, the
        // newer.
        feed(&mut agg, &resource, metric_event("c", MetricKind::gauge(1.0), 150));
        agg.flush(200);
        assert_eq!(names(&agg), ["a", "c"]);
        let drained = registry.drain(0);
        assert_eq!(evicted_cardinality(&drained, "idle"), 1.0);
        assert_eq!(evicted_cardinality(&drained, "active"), 0.0);

        // All three active: the newest goes.
        for name in ["d", "c", "a"] {
            feed(&mut agg, &resource, metric_event(name, MetricKind::gauge(1.0), 250));
        }
        agg.flush(300);
        assert_eq!(names(&agg), ["a", "c"]);
        let drained = registry.drain(0);
        assert_eq!(evicted_cardinality(&drained, "idle"), 0.0);
        assert_eq!(evicted_cardinality(&drained, "active"), 1.0);
    }

    /// One flush of [`cap_soak`].
    #[derive(Debug, PartialEq)]
    struct SoakFlush {
        stable_kept: usize,
        evicted_active: f64,
        evicted_idle: f64,
    }

    const SOAK_STABLE: usize = 20;
    const SOAK_FRESH: usize = 1000;
    const SOAK_CAP: usize = 100;

    /// Runs 20 windows of 20 stable series, updated every window, and 1000 one-off series against
    /// `series_retention: 3` and `max_retained_series: 100`, and returns what each flush evicted.
    /// A warm-up window opens the stable series first; with `churn_first` it opens one one-off
    /// series before them, so `churn`'s group comes first in `groups`, and every later window feeds
    /// the one-off series before the stable ones.
    ///
    /// The warm-up is what lets the stable series survive: newest first protects a series that has
    /// survived one flush. Without it, stable series fed after the churn are the newest in every
    /// window (`a_stable_series_arriving_after_the_churn_is_evicted_every_flush`).
    ///
    /// Checks at every flush: at most 100 series held, every eviction counted, one group per
    /// resource, and under `Cumulative` an unchanged `start_timestamp` on every stable series.
    fn cap_soak(
        kind: fn(usize) -> MetricKind,
        temporality: AggregateTemporality,
        stable: &Arc<Resource>,
        churn: &Arc<Resource>,
        churn_first: bool,
    ) -> Vec<SoakFlush> {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("soak", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_temporality(temporality)
            .with_series_retention(3, SOAK_CAP)
            .with_telemetry(telemetry);
        let groups = if Arc::ptr_eq(stable, churn) { 1 } else { 2 };
        let feed_stable = |agg: &mut Aggregator, ts: i64| {
            for i in 0..SOAK_STABLE {
                let id = i.to_string();
                let event = metric_event_with_tags("stable", kind(i), ts, &[("id", &id)]);
                assert!(feed(agg, stable, event).is_none());
            }
        };
        let feed_churn = |agg: &mut Aggregator, w: usize, n: usize, ts: i64| {
            for j in 0..n {
                let id = format!("{w}-{j}");
                let event = metric_event_with_tags("fresh", kind(j), ts, &[("id", &id)]);
                assert!(feed(agg, churn, event).is_none());
            }
        };

        if churn_first {
            feed_churn(&mut agg, 0, 1, 1);
        }
        feed_stable(&mut agg, 1);
        agg.flush(1_000);
        registry.drain(0);

        let mut flushes = Vec::new();
        for w in 1..=20 {
            let ts = w as i64 * 1_000 + 1;
            if churn_first {
                feed_churn(&mut agg, w, SOAK_FRESH, ts);
                feed_stable(&mut agg, ts);
            } else {
                feed_stable(&mut agg, ts);
                feed_churn(&mut agg, w, SOAK_FRESH, ts);
            }
            let before = series_count(&agg);
            let flushed = agg.flush(ts + 999);
            let after = series_count(&agg);
            let drained = registry.drain(0);

            assert!(after <= SOAK_CAP, "flush {w} holds {after} series");
            let evicted_active = evicted_cardinality(&drained, "active");
            let evicted_idle = evicted_cardinality(&drained, "idle");
            let evicted_total = evicted_count(&drained, "cardinality").unwrap_or(0.0);
            let evicted_ttl = evicted_count(&drained, "idle").unwrap_or(0.0);
            // Every series here is retainable, so the only exits are the two evictions.
            assert_eq!(
                evicted_total + evicted_ttl,
                (before - after) as f64,
                "flush {w}: every series that left is counted"
            );
            assert_eq!(agg.groups.len(), groups, "flush {w}");
            if temporality == AggregateTemporality::Cumulative {
                for (_, _, events) in &flushed {
                    for (event, _) in events {
                        if logit_core::interner::resolve(event.metrics[0].name) == "stable" {
                            assert_eq!(start_timestamp_of(event), 1, "flush {w}: {event:?}");
                        }
                    }
                }
            }
            let stable_kept = agg
                .groups
                .iter()
                .flat_map(|g| g.series.keys())
                .filter(|k| logit_core::interner::resolve(k.name) == "stable")
                .count();
            flushes.push(SoakFlush { stable_kept, evicted_active, evicted_idle });
        }
        flushes
    }

    /// What every soak flush evicts once the tie-break holds: the one-off series left idle by the
    /// previous flush (80, or the warm-up's one), then the newest active ones down to the cap.
    fn soak_expected(churn_first: bool) -> Vec<SoakFlush> {
        let kept_fresh = (SOAK_CAP - SOAK_STABLE) as f64;
        (1..=20)
            .map(|w| SoakFlush {
                stable_kept: SOAK_STABLE,
                evicted_active: (SOAK_STABLE + SOAK_FRESH - SOAK_CAP) as f64,
                evicted_idle: match w {
                    1 if churn_first => 1.0,
                    1 => 0.0,
                    _ => kept_fresh,
                },
            })
            .collect()
    }

    /// The tie-break's limit: with no warm-up and the stable series fed after the churn in every
    /// window, each flush re-creates them newest and evicts them all, counted `state="active"`.
    #[test]
    fn a_stable_series_arriving_after_the_churn_is_evicted_every_flush() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("soak", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(3, SOAK_CAP)
            .with_telemetry(telemetry);
        let resource = default_resource();
        for w in 0..20 {
            let ts = w as i64 * 1_000 + 1;
            for j in 0..SOAK_FRESH {
                let id = format!("{w}-{j}");
                let event =
                    metric_event_with_tags("fresh", MetricKind::gauge(1.0), ts, &[("id", &id)]);
                feed(&mut agg, &resource, event);
            }
            for i in 0..SOAK_STABLE {
                let id = i.to_string();
                let event =
                    metric_event_with_tags("stable", MetricKind::gauge(1.0), ts, &[("id", &id)]);
                feed(&mut agg, &resource, event);
            }
            agg.flush(ts + 999);
            let stable_kept = agg
                .groups
                .iter()
                .flat_map(|g| g.series.keys())
                .filter(|k| logit_core::interner::resolve(k.name) == "stable")
                .count();
            assert_eq!(stable_kept, 0, "flush {w}");
            let drained = registry.drain(0);
            assert_eq!(
                evicted_cardinality(&drained, "active"),
                (SOAK_STABLE + SOAK_FRESH - SOAK_CAP) as f64,
                "flush {w}"
            );
        }
    }

    fn soak_gauge(i: usize) -> MetricKind {
        MetricKind::gauge(i as f64)
    }

    fn soak_counter(_: usize) -> MetricKind {
        MetricKind::counter(1.0)
    }

    /// A series updated every window survives a cap that one-off series overflow every window.
    #[test]
    fn stable_series_survive_a_cardinality_cap_soak() {
        let resource = default_resource();
        let flushes =
            cap_soak(soak_gauge, AggregateTemporality::Delta, &resource, &resource, false);
        assert_eq!(flushes, soak_expected(false));
    }

    /// The same soak under `temporality: cumulative`: stable counters keep their start time.
    #[test]
    fn stable_cumulative_series_keep_their_start_time_through_a_cap_soak() {
        let resource = default_resource();
        let flushes =
            cap_soak(soak_counter, AggregateTemporality::Cumulative, &resource, &resource, false);
        assert_eq!(flushes, soak_expected(false));
    }

    /// Many one-off resources in one window open as many groups, and the flush drops every group
    /// with nothing retained.
    #[test]
    fn a_flush_drops_every_group_without_a_retained_series() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_series_retention(3, 100)
            .with_telemetry(telemetry);
        for i in 0..2000 {
            let mut attributes = AttrMap::new();
            attributes.insert("host", i.to_string().as_str());
            let resource = Arc::new(Resource { attributes, ..Default::default() });
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
        }
        assert_eq!(agg.groups.len(), 2000);
        assert_eq!(agg.flush(1_000).len(), 2000);
        assert!(agg.groups.is_empty(), "a delta counter is never retained");
        let drained = registry.drain(0);
        assert_eq!(gauge_value(&drained, "logit.transform.resource.groups"), Some(2000.0));
    }

    /// The cap is global across groups: stable series in one group survive churn in another,
    /// whichever group `groups` holds first.
    #[test]
    fn stable_series_in_one_group_survive_churn_in_another() {
        let resource = |host: &str| {
            let mut attributes = AttrMap::new();
            attributes.insert("host", host);
            Arc::new(Resource { attributes, ..Default::default() })
        };
        let (a, b) = (resource("a"), resource("b"));
        for churn_first in [false, true] {
            let flushes = cap_soak(soak_gauge, AggregateTemporality::Delta, &a, &b, churn_first);
            assert_eq!(flushes, soak_expected(churn_first), "churn_first: {churn_first}");
        }
    }

    /// A retained gauge's contributing contexts still reset at every flush.
    #[test]
    fn contexts_are_never_carried_across_a_flush_even_for_a_retained_gauge() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(5, 100);
        let resource = default_resource();
        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));

        let flushed = agg.flush(100);
        let (_, _, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "window 1 links its one contributing context");

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::GaugeDelta(MetricValue::F64(1.0)), 150),
        );
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
        feed(&mut agg, &resource, metric_event("conns", MetricKind::gauge(10.0), 0));
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
                    Some(sum.value.as_f64())
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
                    Some(sum.value.as_f64())
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
                    Some(sum.value.as_f64())
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
                    Some(sum.value.as_f64())
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
        assert_eq!(sum_of(&flushed[0].1[0]).value, MetricValue::F64(10.0));

        for ts in 100..105 {
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), ts));
        }
        assert_eq!(agg.groups.len(), 1, "the retained series' group is found again");
        let flushed = flush_events(&mut agg, 200);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].1.len(), 1);
        let second = &flushed[0].1[0];
        assert_eq!(
            sum_of(second).value,
            MetricValue::F64(15.0),
            "the running total carries across the window"
        );
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

    // -- `find_group`'s memo --------------------------------------------------------------------

    /// A flush that removes the memoized group shifts the groups after it down; the memo must not
    /// send the next absorb under that group's `Arc` to whichever group took its index.
    #[test]
    fn a_memoized_group_removed_by_flush_is_reopened_not_confused_with_its_successor() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_series_retention(1, 10);
        let b = resource_with_attr("host", Value::from("b"));
        let a = resource_with_attr("host", Value::from("a"));

        // `b` opens first, at index 0, and is memoized last; `a` holds a retained gauge.
        feed(&mut agg, &b, metric_event("hits", MetricKind::counter(1.0), 0));
        feed(&mut agg, &a, metric_event("temp", MetricKind::gauge(20.0), 0));
        feed(&mut agg, &b, metric_event("hits", MetricKind::counter(1.0), 1));
        assert_eq!(agg.groups.len(), 2);

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 2);
        assert_eq!(agg.groups.len(), 1, "`b`'s delta counter tumbles, so its group is dropped");
        assert!(Arc::ptr_eq(&agg.groups[0].resource, &a), "`a` now holds index 0");

        feed(&mut agg, &b, metric_event("hits", MetricKind::counter(5.0), 101));
        assert_eq!(agg.groups.len(), 2, "`b` reopens its own group");
        let a_group = agg.groups.iter().find(|g| Arc::ptr_eq(&g.resource, &a)).unwrap();
        assert_eq!(a_group.series.len(), 1, "only the retained gauge is under `a`");

        let flushed = flush_events(&mut agg, 200);
        let b_events = &flushed.iter().find(|(r, _)| Arc::ptr_eq(r, &b)).unwrap().1;
        assert_eq!(b_events.len(), 1);
        assert_eq!(counter_value(kind_of(&b_events[0])), 5.0);
    }

    /// Batches that each carry a fresh `Arc` of an equal resource miss the memo on their first
    /// event and still land in the one group.
    #[test]
    fn equal_resources_behind_fresh_arcs_per_batch_share_one_group() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);

        for batch in 0..3 {
            let resource = resource_with_attr("host", Value::from("a"));
            for ts in 0..4 {
                feed(
                    &mut agg,
                    &resource,
                    metric_event("hits", MetricKind::counter(1.0), batch + ts),
                );
            }
            assert_eq!(agg.groups.len(), 1, "batch {batch}");
        }
        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        assert_eq!(counter_value(kind_of(&flushed[0].1[0])), 12.0);
        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(1.0));
    }

    /// The memo keys on the scope `Arc` as well as the resource `Arc`: a scope change between
    /// `process` calls under one resource `Arc` resolves the group afresh.
    #[test]
    fn a_scope_change_under_one_resource_arc_opens_a_distinct_group() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let scope = Arc::new(Scope { name: Bytes::from_static(b"scope"), ..Scope::default() });

        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 0));
        agg.observe_scope(Some(scope.clone()));
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(2.0), 1));
        assert_eq!(agg.groups.len(), 2, "`None` and `Some` scopes are distinct groups");
        agg.observe_scope(None);
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(4.0), 2));
        assert_eq!(agg.groups.len(), 2, "back to the unscoped group");

        let flushed = agg.flush(100);
        let total = |want: Option<&Arc<Scope>>| {
            let (_, _, events) = flushed
                .iter()
                .find(|(_, s, _)| {
                    s.as_ref().map(|s| s.name.clone()) == want.map(|s| s.name.clone())
                })
                .unwrap();
            counter_value(kind_of(&events[0].0))
        };
        assert_eq!(total(None), 5.0);
        assert_eq!(total(Some(&scope)), 2.0);
    }

    /// An absent scope and an all-default scope are distinct groups, and their hashes differ.
    #[test]
    fn a_default_scope_and_no_scope_hash_differently() {
        let resource = Resource::default();
        assert_ne!(
            group_hash(&resource, &None),
            group_hash(&resource, &Some(Arc::new(Scope::default())))
        );
    }

    /// A stored hash that collides with a different resource's doesn't merge the two groups.
    #[test]
    fn a_group_hash_collision_opens_a_distinct_group() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let a = resource_with_attr("host", Value::from("a"));
        let b = resource_with_attr("host", Value::from("b"));
        let mut group = ResourceGroup::new(a.clone(), None);
        group.hash = group_hash(&b, &None);
        agg.groups.push(group);

        feed(&mut agg, &b, metric_event("hits", MetricKind::counter(1.0), 0));
        assert_eq!(agg.groups.len(), 2);
        assert!(agg.groups[0].series.is_empty(), "nothing lands under `a`");
        assert!(Arc::ptr_eq(&agg.groups[1].resource, &b));
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
        assert_eq!(sum_of(first).value, MetricValue::F64(5.0), "window 1's own increments");
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
        assert_eq!(
            sum_of(second).value,
            MetricValue::F64(9.0),
            "5 carried forward plus 4 this window"
        );
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
            MetricKind::Sum(Sum {
                value: MetricValue::F64(v),
                temporality: Temporality::Delta,
                monotonic: false,
            })
        };
        feed(&mut agg, &resource, metric_event("queue.depth", up_down(5.0), 10));
        feed(&mut agg, &resource, metric_event("queue.depth", up_down(-2.0), 20));

        let flushed = flush_events(&mut agg, 100);
        let emitted = sum_of(&flushed[0].1[0]);
        assert_eq!(emitted.value, MetricValue::F64(3.0));
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
        assert_eq!(
            sum_of(&flushed[0].1[0]).value,
            MetricValue::F64(8.0),
            "the idle window didn't reset the total"
        );
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
        assert_eq!(agg.flush(1_000).len(), 1, "flush 1: emits 5, idle_windows resets to 0");
        assert!(agg.flush(2_000).is_empty(), "flush 2: idle_windows -> 1, still under retention");
        assert!(agg.flush(3_000).is_empty(), "flush 3: idle_windows -> 2, now evicted");

        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 3_500));
        let flushed = flush_events(&mut agg, 4_000);
        let restarted = &flushed[0].1[0];
        assert_eq!(
            sum_of(restarted).value,
            MetricValue::F64(1.0),
            "an evicted series restarts from zero"
        );
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

    /// A series the cap evicts while active and a later event re-creates starts inside the window
    /// it reopened in, whatever the reopening event's source timestamp: never before the point it
    /// replaces, never after its own first point, never `0`.
    #[test]
    fn a_re_created_cumulative_series_starts_inside_its_window() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_temporality(AggregateTemporality::Cumulative)
            .with_series_retention(5, 1)
            .with_telemetry(telemetry);
        let resource = default_resource();
        let hits_of = |flushed: Vec<(Arc<Resource>, Vec<Event>)>| {
            flushed
                .into_iter()
                .flat_map(|(_, events)| events)
                .find(|e| logit_core::interner::resolve(e.metrics[0].name) == "hits")
                .expect("hits is emitted before the cap evicts it")
        };

        // Before any flush there's no window start: the event timestamp stands. `anchor` opens
        // first, so the cap keeps it and evicts the newer `hits` at every flush.
        feed(&mut agg, &resource, metric_event("anchor", MetricKind::counter(1.0), 100));
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 100));
        let first = hits_of(flush_events(&mut agg, 1_000));
        assert_eq!(start_timestamp_of(&first), 100);
        let drained = registry.drain(0);
        assert_eq!(evicted_cardinality(&drained, "active"), 1.0);

        // Source timestamps before the previous flush, after this one, and 0.
        let mut previous_point = first.timestamp;
        for (ts, now, start) in [(500, 2_000, 1_000), (9_999, 3_000, 3_000), (0, 4_000, 3_000)] {
            feed(&mut agg, &resource, metric_event("anchor", MetricKind::counter(1.0), ts));
            feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), ts));
            let hits = hits_of(flush_events(&mut agg, now));
            assert_eq!(sum_of(&hits).value, MetricValue::F64(1.0), "re-created at ts {ts}");
            let got = start_timestamp_of(&hits);
            assert!(
                previous_point <= got && got <= hits.timestamp,
                "ts {ts}: start {got} outside [{previous_point}, {}]",
                hits.timestamp
            );
            assert_eq!(got, start, "ts {ts}");
            previous_point = hits.timestamp;
        }
    }

    /// In the first window a source timestamp past the flush clock is capped at the flush.
    #[test]
    fn a_first_window_start_time_is_at_most_the_flush_clock() {
        let mut agg = cumulative_agg();
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 5_000));
        let flushed = flush_events(&mut agg, 1_000);
        assert_eq!(start_timestamp_of(&flushed[0].1[0]), 1_000);
        // The series keeps the capped value: a start time that moved would read as a reset.
        feed(&mut agg, &resource, metric_event("hits", MetricKind::counter(1.0), 5_000));
        let flushed = flush_events(&mut agg, 2_000);
        assert_eq!(start_timestamp_of(&flushed[0].1[0]), 1_000);
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
                value: MetricValue::F64(42.0),
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
        assert_eq!(sum_of(emitted).value, MetricValue::F64(4.0));
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

            // gauge_delta_unseeded, kind_conflict, sample_rate_clamped, sum_non_finite,
            // gauge_non_finite.
            let mut agg = Aggregator::new(Duration::from_secs(10));
            feed(&mut agg, &resource, metric_event("n", MetricKind::counter(f64::NAN), 0));
            feed(&mut agg, &resource, metric_event("ng", MetricKind::gauge(f64::NAN), 0));
            feed(
                &mut agg,
                &resource,
                metric_event("g", MetricKind::GaugeDelta(MetricValue::F64(1.0)), 0),
            );
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

            // series_limit, then series_limit_evicted.
            let mut agg = Aggregator::new(Duration::from_secs(10))
                .with_series_retention(5, 100)
                .with_max_series(1);
            feed(&mut agg, &resource, metric_event("a", MetricKind::gauge(1.0), 0));
            feed(&mut agg, &resource, metric_event("b", MetricKind::gauge(1.0), 0));
            agg.flush(100);
            agg.flush(200);
            feed(&mut agg, &resource, metric_event("b", MetricKind::gauge(1.0), 250));

            // series_per_metric_overflow.
            let mut agg =
                Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1));
            for path in ["/a", "/b"] {
                let event =
                    metric_event_with_tags("p", MetricKind::counter(1.0), 0, &[("path", path)]);
                feed(&mut agg, &resource, event);
            }

            // histogram_bounds_mismatch, and series_retention_full from the flush.
            let mut agg = cumulative_agg().with_series_retention(5, 1);
            let h = |bounds: &[(f64, u64)]| delta_histogram_event("h", bounds, None, None, None, 0);
            feed(&mut agg, &resource, h(&[(1.0, 1)]));
            feed(&mut agg, &resource, h(&[(2.0, 1)]));
            feed(&mut agg, &resource, metric_event("other", MetricKind::counter(1.0), 0));
            agg.flush(100);
        });

        let messages = captured.0.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(messages.len(), 13, "one report per diagnostic key: {messages:#?}");
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

        let mut no_value = MetricRecord::new(intern("nv"), MetricKind::gauge(0.0));
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
                    value: MetricValue::F64(1.0),
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
            MetricRecord::new(
                intern("q"),
                MetricKind::Summary(logit_core::Summary {
                    quantiles: vec![],
                    count: 0,
                    sum: 0.0,
                    temporality: Temporality::Cumulative,
                }),
            ),
            MetricRecord::new(intern("g"), MetricKind::gauge(1.0)),
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
        agg.begin_batch(&resource);
        assert!(agg.process(&resource, &mut event));
        assert_eq!(event.metrics.len(), 7, "the passed-through records stay on the event");

        // A closed event's records count its reason whatever else they'd count.
        let mut stamped = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern("t"), MetricKind::gauge(1.0)),
        );
        stamped.attributes.insert(METRICS_CLOSED, CLOSED_TIMESTAMPED);
        let mut flagged = MetricRecord::new(intern("t"), MetricKind::gauge(0.0));
        flagged.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        stamped.metrics.push(flagged);
        assert!(agg.process(&resource, &mut stamped));
        let metrics_in = metrics_in + 2.0;

        let events = registry.drain(0);
        assert_eq!(passed_through(&events, "timestamped"), 2.0);
        assert_eq!(absorbed(&events), 4.0);
        assert_eq!(passed_through(&events, "no_recorded_value"), 1.0);
        assert_eq!(passed_through(&events, "no_merge_rule"), 2.0);
        assert_eq!(passed_through(&events, "kind_conflict"), 1.0);
        assert_eq!(passed_through(&events, "histogram_bounds_mismatch"), 1.0);
        assert_eq!(passed_through(&events, "non_finite"), 2.0);
        let reasons = [
            "bucketed",
            "timestamped",
            "flagged",
            "no_recorded_value",
            "no_merge_rule",
            "kind_conflict",
            "histogram_bounds_mismatch",
            "non_finite",
        ];
        let passed: f64 = reasons.iter().map(|r| passed_through(&events, r)).sum();
        assert_eq!(metrics_in, absorbed(&events) + passed);
    }

    /// A non-finite gauge would win last-write-wins and a non-finite gauge delta would poison every
    /// later one, so each passes through, counted and diagnosed like a non-finite delta `Sum`.
    #[test]
    fn a_non_finite_gauge_or_gauge_delta_passes_through_and_leaves_the_gauge_finite() {
        let (mut agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_series_retention(2, 10));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("g", MetricKind::gauge(1.0), 0));
        for (bad, at) in [
            (MetricKind::gauge(f64::NAN), 5),
            (MetricKind::GaugeDelta(MetricValue::F64(f64::INFINITY)), 5),
            (MetricKind::GaugeDelta(MetricValue::F64(f64::NEG_INFINITY)), 5),
        ] {
            let forwarded = feed(&mut agg, &resource, metric_event("g", bad.clone(), at))
                .expect("a non-finite gauge is forwarded");
            assert_eq!(forwarded.metrics.len(), 1);
            // Debug, since `NaN != NaN`.
            assert_eq!(format!("{:?}", forwarded.metrics[0].kind), format!("{bad:?}"));
        }
        feed(
            &mut agg,
            &resource,
            metric_event("g", MetricKind::GaugeDelta(MetricValue::F64(2.0)), 6),
        );
        let flushed = flush_events(&mut agg, 10);
        assert_eq!(*kind_of(&flushed[0].1[0]), MetricKind::gauge(3.0));

        let events = registry.drain(0);
        assert_eq!(passed_through(&events, "non_finite"), 3.0);
        assert_eq!(diagnostic_count(&events, "gauge_non_finite"), Some(3.0));
        assert_eq!(diagnostic_count(&events, "sum_non_finite"), None);
    }

    /// An event flagged `_logit.metrics.closed` keeps every record, its timestamp, and its
    /// attributes, opens no series, and counts each record under the flag's reason: its own value
    /// for one in the closed set, `flagged` for any other `Str` or `Bool(true)`. A value that
    /// doesn't close the event lets it merge into a series.
    #[test]
    fn a_closed_event_passes_through_untouched_counted_by_its_reason() {
        let resource = default_resource();
        for (value, reason) in [
            (Value::str(CLOSED_TIMESTAMPED), "timestamped"),
            (Value::str(CLOSED_BUCKETED), "bucketed"),
            (Value::str("anything else"), "flagged"),
            (Value::Bool(true), "flagged"),
        ] {
            let (mut agg, registry) = with_registry(Aggregator::new(Duration::from_secs(10)));
            let mut event = metric_event("g", MetricKind::gauge(7.0), 1_799_999_970_000_000_000);
            event.attributes.insert(METRICS_CLOSED, value.clone());
            event.attributes.insert("room", "lab");
            event.metrics.push(MetricRecord::new(intern("c"), MetricKind::counter(1.0)));
            let original = event.clone();

            let forwarded = feed(&mut agg, &resource, event).expect("nothing is absorbed");
            assert_eq!(forwarded, original, "{value:?}: forwarded untouched");
            assert!(flush_events(&mut agg, 1_800_000_000_000_000_000).is_empty(), "{value:?}");
            let events = registry.drain(0);
            assert_eq!(passed_through(&events, reason), 2.0, "{value:?}");
            assert_eq!(absorbed(&events), 0.0, "{value:?}");
        }

        for value in [Value::Bool(false), Value::U64(1)] {
            let (mut agg, registry) = with_registry(Aggregator::new(Duration::from_secs(10)));
            let mut event = metric_event("g", MetricKind::gauge(7.0), 0);
            event.attributes.insert(METRICS_CLOSED, value.clone());
            assert!(feed(&mut agg, &resource, event).is_none(), "{value:?}: absorbed");
            let events = registry.drain(0);
            for reason in ["timestamped", "bucketed", "flagged"] {
                assert_eq!(passed_through(&events, reason), 0.0, "{value:?}");
            }
        }
    }

    /// The regression the flags exist for: `aggregate` reads no protocol carrier. An event that
    /// carries every carrier the decoders stamp beside a flag, and no flag, is absorbed and
    /// windowed, and its `_logit.field.*` key is series identity that the flushed series keeps.
    #[test]
    fn an_event_with_every_protocol_carrier_and_no_flag_is_windowed() {
        let (mut agg, registry) = with_registry(Aggregator::new(Duration::from_secs(10)));
        let resource = default_resource();
        let mut event = metric_event("g", MetricKind::gauge(7.0), 1_799_999_970_000_000_000);
        event.attributes.insert("_logit.datadog.stats.name", "web.request");
        event.attributes.insert("_logit.field.x", "note");

        assert!(feed(&mut agg, &resource, event).is_none(), "absorbed, not passed through");
        let flushed = flush_events(&mut agg, 1_800_000_000_000_000_000);
        assert_eq!(flushed.len(), 1);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "one windowed series");
        assert_eq!(events[0].timestamp, 1_800_000_000_000_000_000, "stamped with the flush time");
        assert_eq!(
            events[0].attributes.get("_logit.field.x").and_then(Value::as_str),
            Some("note"),
            "with no fields flag, a field key is identity"
        );
        let telemetry = registry.drain(0);
        assert_eq!(absorbed(&telemetry), 1.0);
        for reason in ["timestamped", "bucketed", "flagged"] {
            assert_eq!(passed_through(&telemetry, reason), 0.0, "{reason}");
        }
    }

    /// `_logit.metrics.closed` on the resource closes every event of the batch, read once in
    /// `begin_batch`; an event's own value wins over it, and the next batch's resource replaces
    /// it.
    #[test]
    fn a_closed_flag_on_the_resource_passes_every_event_of_the_batch_through() {
        let (mut agg, registry) = with_registry(Aggregator::new(Duration::from_secs(10)));
        let mut flagged = Resource::default();
        flagged.attributes.insert(METRICS_CLOSED, CLOSED_TIMESTAMPED);
        let flagged = Arc::new(flagged);

        agg.begin_batch(&flagged);
        for at in [1, 2, 3] {
            let mut event = metric_event("hits", MetricKind::counter(1.0), at);
            assert!(agg.process(&flagged, &mut event), "event {at} is forwarded");
        }
        // The event's own value wins: `Bool(false)` doesn't close it.
        let mut opted_out = metric_event("hits", MetricKind::counter(1.0), 4);
        opted_out.attributes.insert(METRICS_CLOSED, false);
        assert!(!agg.process(&flagged, &mut opted_out), "the event's own value wins");

        // A batch whose resource has no flag closes nothing.
        let plain = default_resource();
        agg.begin_batch(&plain);
        let mut event = metric_event("hits", MetricKind::counter(1.0), 5);
        assert!(!agg.process(&plain, &mut event), "the previous batch's flag doesn't carry over");

        let events = registry.drain(0);
        assert_eq!(passed_through(&events, "timestamped"), 3.0);
        assert_eq!(absorbed(&events), 2.0);
    }

    /// A Datadog APM stats group, flagged `bucketed` on the event or the resource, keeps its
    /// bucket start, its records, and its carriers, opens no series, and counts each record
    /// `bucketed`. Two buckets of one group would otherwise share a series key and merge.
    #[test]
    fn a_datadog_apm_stats_group_passes_through_untouched() {
        let count = |v: u64| {
            MetricKind::Sum(Sum {
                value: MetricValue::U64(v),
                temporality: Temporality::Delta,
                monotonic: true,
            })
        };
        for on_resource in [false, true] {
            let mut resource = Resource::default();
            let mut attrs = AttrMap::new();
            attrs.insert("_logit.datadog.stats.bucket.duration", Value::U64(10_000_000_000));
            attrs.insert("_logit.datadog.stats.name", "web.request");
            if on_resource {
                resource.attributes.insert(METRICS_CLOSED, CLOSED_BUCKETED);
            } else {
                attrs.insert(METRICS_CLOSED, CLOSED_BUCKETED);
            }
            let resource = Arc::new(resource);
            let (mut agg, registry) = with_registry(cumulative_agg());
            for start in [1_700_000_000_000_000_000, 1_700_000_010_000_000_000] {
                let mut event = Event::metric(
                    start,
                    attrs.clone(),
                    MetricRecord::new(intern("datadog.stats.hits"), count(3)),
                );
                event.metrics.push(MetricRecord::new(intern("datadog.stats.errors"), count(1)));
                let original = event.clone();
                let forwarded = feed(&mut agg, &resource, event).expect("nothing is absorbed");
                assert_eq!(forwarded, original, "on the resource: {on_resource}");
            }
            assert!(flush_events(&mut agg, 1_800_000_000_000_000_000).is_empty());
            let events = registry.drain(0);
            assert_eq!(passed_through(&events, "bucketed"), 4.0, "on the resource: {on_resource}");
            assert_eq!(absorbed(&events), 0.0);
        }
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
        assert_eq!(sum_of(&flush_events(&mut agg, 10)[0].1[0]).value, MetricValue::F64(3.0));
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
        assert_eq!(
            counter_with_tag(&events, "logit.transform.samples.omitted", "reason", "non_finite"),
            Some(2.0)
        );
        assert_eq!(counter_total(&events, "logit.transform.samples.non_finite_dropped"), 0.0);
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

    /// The union stops scanning at member `cap + 1`: one record of 20 times the cap costs the
    /// compares of filling the cap once, `(cap + 1)(cap + 2) / 2` at most, not the record's own
    /// size squared.
    #[test]
    fn union_members_converts_at_cap_plus_one_with_bounded_compares() {
        const CAP: usize = 1000;
        let members: Vec<Bytes> = (0..20 * CAP).map(|i| Bytes::from(i.to_string())).collect();

        let mut held = Vec::new();
        let (overflow, compares) = union_members(&mut held, &members[..CAP], CAP);
        assert!(overflow.is_none(), "cap members fit");
        assert_eq!(held.len(), CAP);

        let mut held = Vec::new();
        let (overflow, compares_past_cap) = union_members(&mut held, &members, CAP);
        let hll = overflow.expect("the record passes the cap");
        assert!(compares_past_cap <= (CAP + 1) * (CAP + 2) / 2, "{compares_past_cap} compares");
        assert_eq!(held.len(), CAP + 1, "the scan stops at member cap + 1");
        assert!(compares <= compares_past_cap);
        let error = (hll.estimate() as f64 - members.len() as f64).abs() / members.len() as f64;
        assert!(error < 0.05, "estimate {} for {} members", hll.estimate(), members.len());
    }

    /// One record far past `max_set_members_per_series` falls back to a `HyperLogLog` holding
    /// every member, counted once.
    #[test]
    fn a_set_members_record_past_the_cap_falls_back_with_every_member() {
        const CAP: usize = 1000;
        const MEMBERS: usize = 20 * CAP;
        let resource = default_resource();
        let (agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_sets(Sets::Members, CAP));
        let mut agg = agg;
        let members: Vec<Bytes> = (0..MEMBERS).map(|i| Bytes::from(i.to_string())).collect();

        feed(&mut agg, &resource, metric_event("u", MetricKind::SetMembers(members), 0));

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
    }

    // -- Typed values (`docs/adr/typed-metric-scalar.md`) ----------------------------------------

    fn typed_sum(value: MetricValue) -> MetricKind {
        MetricKind::Sum(Sum { value, temporality: Temporality::Delta, monotonic: true })
    }

    /// Feeds each value as a delta `Sum` of one series into a telemetry-attached `agg`, flushes
    /// at `now`, and returns the emitted value and the `widened` count.
    fn fold_sums(
        agg: Aggregator,
        values: &[MetricValue],
        now: i64,
    ) -> (Aggregator, MetricValue, Option<f64>) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = agg.with_telemetry(telemetry);
        let resource = default_resource();
        for (ts, v) in values.iter().enumerate() {
            feed(&mut agg, &resource, metric_event("hits", typed_sum(*v), now - 10 + ts as i64));
        }
        let flushed = flush_events(&mut agg, now);
        let value = sum_of(&flushed[0].1[0]).value;
        let widened = counter_with_tag(
            &registry.drain(0),
            "logit.transform.metrics.normalized",
            "reason",
            "widened",
        );
        (agg, value, widened)
    }

    #[test]
    fn i64_sums_stay_i64() {
        let agg = Aggregator::new(Duration::from_secs(10));
        let (_, value, widened) =
            fold_sums(agg, &[MetricValue::I64(3), MetricValue::I64(-1), MetricValue::I64(4)], 100);
        assert_eq!(value, MetricValue::I64(6));
        assert_eq!(widened, None, "a same-type fold isn't widened");
    }

    #[test]
    fn u64_sums_stay_u64_and_exact_above_2_pow_53() {
        let agg = Aggregator::new(Duration::from_secs(10));
        let big = 1_u64 << 53;
        let (_, value, widened) =
            fold_sums(agg, &[MetricValue::U64(big), MetricValue::U64(1)], 100);
        assert_eq!(value, MetricValue::U64(big + 1), "an f64 can't hold 2^53 + 1");
        assert_eq!(widened, None);
    }

    #[test]
    fn an_i64_overflow_widens_to_f64_and_is_counted() {
        let agg = Aggregator::new(Duration::from_secs(10));
        let (_, value, widened) =
            fold_sums(agg, &[MetricValue::I64(i64::MAX), MetricValue::I64(1)], 100);
        assert_eq!(value, MetricValue::F64(i64::MAX as f64 + 1.0));
        assert_eq!(widened, Some(1.0));
    }

    #[test]
    fn an_i64_and_an_f64_sum_widens_to_f64_and_is_counted() {
        let agg = Aggregator::new(Duration::from_secs(10));
        let (_, value, widened) =
            fold_sums(agg, &[MetricValue::I64(2), MetricValue::F64(0.5)], 100);
        assert_eq!(value, MetricValue::F64(2.5));
        assert_eq!(widened, Some(1.0));
    }

    /// The first record seeds the series with its own type: a lone `Bool` isn't widened against
    /// an `F64` zero.
    #[test]
    fn the_first_record_seeds_the_series_with_its_own_type() {
        let agg = Aggregator::new(Duration::from_secs(10));
        let (_, value, widened) = fold_sums(agg, &[MetricValue::Bool(true)], 100);
        assert_eq!(value, MetricValue::Bool(true));
        assert_eq!(widened, None);
    }

    #[test]
    fn a_cumulative_i64_counter_stays_i64_across_two_flushes() {
        let (mut agg, first, _) =
            fold_sums(cumulative_agg(), &[MetricValue::I64(2), MetricValue::I64(3)], 100);
        assert_eq!(first, MetricValue::I64(5));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("hits", typed_sum(MetricValue::I64(4)), 150));
        let flushed = flush_events(&mut agg, 200);
        let second = sum_of(&flushed[0].1[0]);
        assert_eq!(second.value, MetricValue::I64(9), "the carried total keeps its type");
        assert_eq!(second.temporality, Temporality::Cumulative);
    }

    #[test]
    fn a_gauge_delta_on_an_i64_gauge_stays_i64() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();
        let gauge = MetricKind::Gauge(MetricValue::I64(10));
        feed(&mut agg, &resource, metric_event("conns", gauge, 0));
        let delta = MetricKind::GaugeDelta(MetricValue::I64(-3));
        feed(&mut agg, &resource, metric_event("conns", delta, 1));
        let flushed = flush_events(&mut agg, 100);
        assert_eq!(kind_of(&flushed[0].1[0]), &MetricKind::Gauge(MetricValue::I64(7)));
        let events = registry.drain(0);
        let widened =
            counter_with_tag(&events, "logit.transform.metrics.normalized", "reason", "widened");
        assert_eq!(widened, None);
    }

    #[test]
    fn an_f64_gauge_delta_on_an_i64_gauge_widens_and_is_counted() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();
        feed(
            &mut agg,
            &resource,
            metric_event("conns", MetricKind::Gauge(MetricValue::I64(10)), 0),
        );
        let delta = MetricKind::GaugeDelta(MetricValue::F64(0.5));
        feed(&mut agg, &resource, metric_event("conns", delta, 1));
        let flushed = flush_events(&mut agg, 100);
        assert_eq!(kind_of(&flushed[0].1[0]), &MetricKind::Gauge(MetricValue::F64(10.5)));
        let events = registry.drain(0);
        let widened =
            counter_with_tag(&events, "logit.transform.metrics.normalized", "reason", "widened");
        assert_eq!(widened, Some(1.0));
    }

    // -- `max_series` ---------------------------------------------------------------------------

    fn series_limit_dropped(events: &[Event]) -> Option<f64> {
        counter_with_tag(events, "logit.transform.metrics.dropped", "reason", "series_limit")
    }

    /// `series.evicted{reason="series_limit"}`: series a full `max_series` evicted to make room.
    fn evicted_for_series_limit(events: &[Event]) -> Option<f64> {
        counter_with_tag(events, "logit.transform.series.evicted", "reason", "series_limit")
    }

    /// The names of every series held, sorted.
    fn held_names(agg: &Aggregator) -> Vec<String> {
        let mut names: Vec<String> = agg
            .groups
            .iter()
            .flat_map(|g| g.series.keys())
            .map(|k| logit_core::interner::resolve(k.name).to_string())
            .collect();
        names.sort();
        names
    }

    /// The budget's counts against a scan of every group.
    fn assert_budget_matches_the_groups(agg: &Aggregator) {
        let states: Vec<&SeriesState> = agg.groups.iter().flat_map(|g| g.series.values()).collect();
        let idle = states.iter().filter(|s| !s.updated_this_window).count();
        assert_eq!(agg.budget.held, states.len(), "held");
        assert_eq!(agg.budget.idle, idle, "idle");
    }

    /// A gauge `aggregate` with retention and the given `max_series`, holding `old1` and `old2`
    /// idle for two flushes and `young` for one, opened in that order.
    fn three_retained_gauges(max_series: usize) -> (Aggregator, Arc<logit_core::Registry>) {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(5, 100)
                .with_max_series(max_series),
        );
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("old1", MetricKind::gauge(1.0), 0));
        feed(&mut agg, &resource, metric_event("old2", MetricKind::gauge(1.0), 0));
        agg.flush(100);
        feed(&mut agg, &resource, metric_event("young", MetricKind::gauge(1.0), 150));
        agg.flush(200); // old1 and old2 idle for 1, young for 0
        agg.flush(300); // old1 and old2 idle for 2, young for 1
        registry.drain(0);
        (agg, registry)
    }

    /// Past `max_series`, a record that would open a series is dropped, counted, and diagnosed,
    /// and the series already held flush as usual.
    #[test]
    fn past_max_series_a_new_series_is_dropped_and_counted() {
        let (mut agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_max_series(2));
        let resource = default_resource();
        for name in ["a", "b", "c", "d"] {
            let forwarded =
                feed(&mut agg, &resource, metric_event(name, MetricKind::counter(1.0), 0));
            assert!(forwarded.is_none());
        }
        let flushed = flush_events(&mut agg, 100);
        let mut names: Vec<_> = flushed[0]
            .1
            .iter()
            .map(|e| logit_core::interner::resolve(e.metrics[0].name).to_string())
            .collect();
        names.sort();
        assert_eq!(names, ["a", "b"], "the first two series opened");
        let events = registry.drain(0);
        assert_eq!(series_limit_dropped(&events), Some(2.0));
        assert_eq!(diagnostic_count(&events, "series_limit"), Some(2.0));
        assert_eq!(absorbed(&events), 2.0);
    }

    /// A full cap refuses only a new series: a held one keeps absorbing.
    #[test]
    fn at_max_series_a_held_series_keeps_absorbing() {
        let (mut agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_max_series(1));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("a", MetricKind::counter(1.0), 0));
        feed(&mut agg, &resource, metric_event("b", MetricKind::counter(1.0), 0));
        feed(&mut agg, &resource, metric_event("a", MetricKind::counter(2.0), 0));
        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed[0].1.len(), 1);
        assert_eq!(sum_of(&flushed[0].1[0]).value, MetricValue::F64(3.0));
        let events = registry.drain(0);
        assert_eq!(series_limit_dropped(&events), Some(1.0));
        assert_eq!(absorbed(&events), 2.0);
    }

    /// A series retained across a flush holds its slot. At a full cap, a retained series not
    /// updated this window makes room for a new one, the newest first among equally idle, and
    /// one that absorbed this window doesn't.
    #[test]
    fn retained_series_count_toward_max_series_and_make_room() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(5, 100)
                .with_max_series(2),
        );
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("g1", MetricKind::gauge(1.0), 0));
        feed(&mut agg, &resource, metric_event("g2", MetricKind::gauge(1.0), 0));
        agg.flush(100);
        registry.drain(0);
        assert_budget_matches_the_groups(&agg);

        // Both are idle so far this window, at `idle_windows == 0`.
        feed(&mut agg, &resource, metric_event("g1", MetricKind::gauge(2.0), 150));
        feed(&mut agg, &resource, metric_event("g3", MetricKind::gauge(1.0), 150));
        feed(&mut agg, &resource, metric_event("g4", MetricKind::gauge(1.0), 150));
        let events = registry.drain(0);
        assert_eq!(evicted_for_series_limit(&events), Some(1.0), "g2 makes room for g3");
        assert_eq!(series_limit_dropped(&events), Some(1.0), "g4 finds both slots active");
        assert_eq!(absorbed(&events), 2.0);
        assert_eq!(held_names(&agg), ["g1", "g3"]);
        assert_budget_matches_the_groups(&agg);
    }

    /// At a full cap, each new series evicts one held series, in the flush cap's order: the
    /// most idle first, then the newest. Once every held series was updated this window, the
    /// next new one is dropped.
    #[test]
    fn at_max_series_each_new_series_evicts_one_in_the_flush_caps_order() {
        let (mut agg, registry) = three_retained_gauges(3);
        let resource = default_resource();
        assert_budget_matches_the_groups(&agg);

        let mut evicted_after = Vec::new();
        for (i, name) in ["new1", "new2", "new3", "new4"].into_iter().enumerate() {
            feed(&mut agg, &resource, metric_event(name, MetricKind::gauge(1.0), 350 + i as i64));
            evicted_after.push(held_names(&agg));
            assert_budget_matches_the_groups(&agg);
        }
        assert_eq!(evicted_after[0], ["new1", "old1", "young"], "old2 is the newer of idle 2");
        assert_eq!(evicted_after[1], ["new1", "new2", "young"]);
        assert_eq!(evicted_after[2], ["new1", "new2", "new3"]);
        assert_eq!(evicted_after[3], ["new1", "new2", "new3"], "nothing left to evict");

        let events = registry.drain(0);
        assert_eq!(evicted_for_series_limit(&events), Some(3.0));
        assert_eq!(diagnostic_count(&events, "series_limit_evicted"), Some(3.0));
        assert_eq!(series_limit_dropped(&events), Some(1.0));
        assert_eq!(
            counter_with_tag(&events, "logit.transform.series.evicted", "reason", "cardinality"),
            None
        );
    }

    /// A retained series that absorbs this window is never evicted to make room, even after the
    /// window's eviction order listed it.
    #[test]
    fn a_retained_series_absorbing_this_window_is_never_evicted() {
        let (mut agg, registry) = three_retained_gauges(3);
        let resource = default_resource();
        // Builds the order (old2, old1, young) and evicts old2.
        feed(&mut agg, &resource, metric_event("new1", MetricKind::gauge(1.0), 350));
        feed(&mut agg, &resource, metric_event("old1", MetricKind::gauge(2.0), 350));
        assert_budget_matches_the_groups(&agg);
        feed(&mut agg, &resource, metric_event("new2", MetricKind::gauge(1.0), 350));
        assert_eq!(held_names(&agg), ["new1", "new2", "old1"], "old1 skipped, young evicted");
        feed(&mut agg, &resource, metric_event("new3", MetricKind::gauge(1.0), 350));
        assert_eq!(held_names(&agg), ["new1", "new2", "old1"]);
        assert_budget_matches_the_groups(&agg);

        let events = registry.drain(0);
        assert_eq!(evicted_for_series_limit(&events), Some(2.0));
        assert_eq!(series_limit_dropped(&events), Some(1.0), "new3: every series held is active");
        let flushed = flush_events(&mut agg, 400);
        let old1 = flushed[0]
            .1
            .iter()
            .find(|e| logit_core::interner::resolve(e.metrics[0].name) == "old1")
            .expect("old1 survived");
        assert_eq!(kind_of(old1), &MetricKind::gauge(2.0));
    }

    /// An admission eviction happens between flushes, so the flush's series identity
    /// (`series_at_flush_start == emitted_and_removed + kept + evicted{idle} +
    /// evicted{cardinality}`) still holds with it counted under its own reason.
    #[test]
    fn the_flush_series_identity_holds_after_admission_evictions() {
        let (mut agg, registry) = three_retained_gauges(3);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("new1", MetricKind::gauge(1.0), 350));
        feed(&mut agg, &resource, metric_event("new2", MetricKind::gauge(1.0), 350));
        registry.drain(0);

        agg.flush(400);
        let events = registry.drain(0);
        let at_start = gauge_value(&events, "logit.transform.series.active").unwrap()
            + gauge_value(&events, "logit.transform.series.retained").unwrap();
        assert_eq!(at_start, 3.0, "the evicted two aren't held at the flush");
        // Gauges are retained, so nothing is emitted and removed.
        let kept = agg.groups.iter().map(|g| g.series.len()).sum::<usize>() as f64;
        let idle = evicted_count(&events, "idle").unwrap_or(0.0);
        let cardinality = evicted_count(&events, "cardinality").unwrap_or(0.0);
        assert_eq!(at_start, kept + idle + cardinality);
        assert_eq!(evicted_for_series_limit(&events), None, "counted when it happened, not here");
        assert_budget_matches_the_groups(&agg);
    }

    /// The eviction order lasts one window: the next window's first eviction builds it again
    /// from the series that window holds.
    #[test]
    fn each_window_builds_its_own_eviction_order() {
        let (mut agg, registry) = three_retained_gauges(3);
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("new1", MetricKind::gauge(1.0), 350));
        assert_eq!(held_names(&agg), ["new1", "old1", "young"]);
        agg.flush(400);
        feed(&mut agg, &resource, metric_event("new2", MetricKind::gauge(1.0), 450));
        assert_eq!(held_names(&agg), ["new1", "new2", "young"], "old1 is the most idle");
        assert_budget_matches_the_groups(&agg);
        let events = registry.drain(0);
        assert_eq!(evicted_for_series_limit(&events), Some(2.0));
        assert_eq!(series_limit_dropped(&events), None);
    }

    /// A record whose group doesn't exist yet still makes room at a full cap: the held series
    /// under another resource is evicted, and its emptied group stays until the flush.
    #[test]
    fn a_record_with_no_group_evicts_to_make_room() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(5, 100)
                .with_max_series(1),
        );
        let a = default_resource();
        let b = Arc::new(Resource { dropped_attributes_count: 1, ..Resource::default() });
        feed(&mut agg, &a, metric_event("old", MetricKind::gauge(1.0), 0));
        agg.flush(100);
        registry.drain(0);

        feed(&mut agg, &b, metric_event("new", MetricKind::gauge(1.0), 150));
        let events = registry.drain(0);
        assert_eq!(evicted_for_series_limit(&events), Some(1.0));
        assert_eq!(series_limit_dropped(&events), None);
        assert_eq!(held_names(&agg), ["new"]);
        assert_eq!(agg.groups.len(), 2, "a's emptied group stays until the flush");
        assert_budget_matches_the_groups(&agg);
        agg.flush(200);
        assert_eq!(agg.groups.len(), 1);
        assert!(Arc::ptr_eq(&agg.groups[0].resource, &b));
    }

    /// A flush discards the window's eviction order, so the next window's order holds only the
    /// series that window holds, indexed by the groups the flush left. Here the flush removes the
    /// first group, shifting the second group's index.
    #[test]
    fn a_flush_discards_the_eviction_order() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(1, 100)
                .with_max_series(2),
        );
        let a = default_resource();
        let b = Arc::new(Resource { dropped_attributes_count: 1, ..Resource::default() });
        feed(&mut agg, &a, metric_event("a1", MetricKind::gauge(1.0), 0));
        feed(&mut agg, &b, metric_event("b1", MetricKind::gauge(1.0), 0));
        agg.flush(100);
        // Builds the order (b1, a1) under groups (a, b) and evicts b1, the newer.
        feed(&mut agg, &b, metric_event("n1", MetricKind::gauge(1.0), 150));
        assert_eq!(held_names(&agg), ["a1", "n1"]);
        assert!(!agg.eviction_order.is_empty(), "a1 is still listed");

        agg.flush(200); // a1 idle past `series_retention: 1`: evicted, and group a with it
        assert!(agg.eviction_order.is_empty());
        assert_eq!(agg.groups.len(), 1);
        feed(&mut agg, &b, metric_event("n2", MetricKind::gauge(1.0), 250));
        feed(&mut agg, &b, metric_event("n3", MetricKind::gauge(1.0), 250));
        assert_eq!(held_names(&agg), ["n2", "n3"], "n1 makes room for n3");
        assert_budget_matches_the_groups(&agg);
        let events = registry.drain(0);
        assert_eq!(evicted_for_series_limit(&events), Some(2.0));
        assert_eq!(series_limit_dropped(&events), None);
    }

    /// A series updated this window, retained or new, is never evicted to make room.
    #[test]
    fn an_idle_series_updated_this_window_is_not_evicted_to_make_room() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(5, 100)
                .with_max_series(1),
        );
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("g", MetricKind::gauge(1.0), 0));
        agg.flush(100);
        agg.flush(200); // idle for 1
        registry.drain(0);
        feed(&mut agg, &resource, metric_event("g", MetricKind::gauge(2.0), 250));
        assert_budget_matches_the_groups(&agg);
        feed(&mut agg, &resource, metric_event("h", MetricKind::gauge(1.0), 250));
        let events = registry.drain(0);
        assert_eq!(series_limit_dropped(&events), Some(1.0));
        assert_eq!(evicted_for_series_limit(&events), None);
        assert_budget_matches_the_groups(&agg);
    }

    /// A flush frees the slot of every series it drains, and of a retained series it evicts
    /// for idleness.
    #[test]
    fn a_flush_frees_the_slots_of_the_series_it_removes() {
        let (mut agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_max_series(1));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("a", MetricKind::counter(1.0), 0));
        agg.flush(100);
        feed(&mut agg, &resource, metric_event("b", MetricKind::counter(1.0), 150));
        let flushed = flush_events(&mut agg, 200);
        assert_eq!(logit_core::interner::resolve(flushed[0].1[0].metrics[0].name), "b");

        let (mut agg, registry_retained) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(1, 100)
                .with_max_series(1),
        );
        feed(&mut agg, &resource, metric_event("g1", MetricKind::gauge(1.0), 0));
        agg.flush(100); // retained
        agg.flush(200); // idle past `series_retention: 1`, evicted
        feed(&mut agg, &resource, metric_event("g2", MetricKind::gauge(1.0), 250));
        let flushed = flush_events(&mut agg, 300);
        assert_eq!(logit_core::interner::resolve(flushed[0].1[0].metrics[0].name), "g2");

        for events in [registry.drain(0), registry_retained.drain(0)] {
            assert_eq!(series_limit_dropped(&events), None);
        }
    }

    /// A refused record opens no group: the cap bounds groups as well as series, and a held
    /// group keeps absorbing after the refusal.
    #[test]
    fn a_refused_record_leaves_no_empty_group() {
        let (mut agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_max_series(1));
        let held = default_resource();
        let other = Arc::new(Resource { dropped_attributes_count: 1, ..Resource::default() });
        feed(&mut agg, &held, metric_event("a", MetricKind::counter(1.0), 0));
        let mut two = metric_event("b", MetricKind::counter(1.0), 0);
        two.metrics.push(MetricRecord::new(intern("c"), MetricKind::counter(1.0)));
        assert!(feed(&mut agg, &other, two).is_none(), "both records dropped");
        assert_eq!(agg.groups.len(), 1, "no group left behind for the refused resource");
        assert!(agg.last_group.as_ref().is_none_or(|m| Arc::ptr_eq(&m.resource, &held)));
        assert_budget_matches_the_groups(&agg);
        feed(&mut agg, &held, metric_event("a", MetricKind::counter(1.0), 0));
        assert_eq!(agg.flush(100).len(), 1);
        let events = registry.drain(0);
        assert_eq!(series_limit_dropped(&events), Some(2.0));
        assert_eq!(absorbed(&events), 2.0);
    }

    /// At a full cap, every record still lands in one counter: absorbed, passed through, or
    /// dropped, and a record that passes through is never refused. An event left with only its
    /// pass-through records and its log is still forwarded.
    #[test]
    fn at_max_series_every_record_is_absorbed_passed_through_or_dropped_once() {
        let (mut agg, registry) =
            with_registry(Aggregator::new(Duration::from_secs(10)).with_max_series(1));
        let resource = default_resource();
        feed(&mut agg, &resource, metric_event("held", MetricKind::counter(1.0), 0));

        let mut event = metric_event("new", MetricKind::counter(1.0), 0);
        event.metrics.extend([
            MetricRecord::new(intern("held"), MetricKind::counter(1.0)),
            MetricRecord::new(intern("n"), MetricKind::counter(f64::NAN)),
            MetricRecord::new(
                intern("cum"),
                MetricKind::Sum(Sum {
                    value: MetricValue::F64(1.0),
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
            MetricRecord::new(intern("other"), MetricKind::gauge(1.0)),
        ]);
        event.log = Some(logit_core::LogRecord {
            message: Value::from("line"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        let metrics_in = 1.0 + event.metrics.len() as f64;
        let forwarded = feed(&mut agg, &resource, event).expect("the log and pass-throughs stay");
        assert_eq!(forwarded.metrics.len(), 2, "only the two pass-through records stay");
        assert!(forwarded.log.is_some());

        let events = registry.drain(0);
        assert_eq!(absorbed(&events), 2.0);
        assert_eq!(passed_through(&events, "non_finite"), 1.0);
        assert_eq!(passed_through(&events, "no_merge_rule"), 1.0);
        assert_eq!(series_limit_dropped(&events), Some(2.0));
        let dropped = counter_total(&events, "logit.transform.metrics.dropped");
        let passed = counter_total(&events, "logit.transform.metrics.passed_through");
        assert_eq!(metrics_in, absorbed(&events) + passed + dropped);
    }

    // -- `max_series_per_metric` ----------------------------------------------------------------

    fn overflowed(events: &[Event]) -> f64 {
        counter_total(events, "logit.transform.metrics.overflowed")
    }

    /// Whether a flushed or held attribute set is an overflow series': the marker set.
    fn is_marked(attributes: &AttrMap) -> bool {
        matches!(attributes.get(OVERFLOW), Some(Value::Bool(true)))
    }

    /// The flushed events of `name`, with their attributes.
    fn flushed_named<'a>(
        flushed: &'a [(Arc<Resource>, Vec<Event>)],
        name: &'a str,
    ) -> impl Iterator<Item = &'a Event> {
        flushed
            .iter()
            .flat_map(|(_, events)| events)
            .filter(move |e| logit_core::interner::resolve(e.metrics[0].name) == name)
    }

    /// The budget's per-name counts against a scan of every group, overflow series excluded.
    fn assert_names_match_the_groups(agg: &mut Aggregator) {
        assert_budget_matches_the_groups(agg);
        let mut expected: HashMap<Symbol, usize> = HashMap::new();
        for key in agg.groups.iter().flat_map(|g| g.series.keys()) {
            if !agg.overflow.is(key, &mut agg.key_classes) {
                *expected.entry(key.name).or_default() += 1;
            }
        }
        for (name, count) in &agg.budget.names {
            assert_eq!(count.series, expected.remove(name).unwrap_or_default(), "{name:?}");
        }
        assert!(expected.is_empty(), "held names the budget doesn't count: {expected:?}");
    }

    fn tagged_counter(name: &str, value: f64, tags: &[(&str, &str)]) -> Event {
        metric_event_with_tags(name, MetricKind::counter(value), 0, tags)
    }

    /// Past the limit, a new series of the name folds into one overflow series: the name's
    /// total stays correct, the fold is counted as absorbed and overflowed, and nothing drops.
    #[test]
    fn past_max_series_per_metric_a_new_series_folds_into_the_overflow_series() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(2)),
        );
        let resource = default_resource();
        for (i, path) in ["/a", "/b", "/c", "/d", "/e"].into_iter().enumerate() {
            let event = tagged_counter("req", (i + 1) as f64, &[("path", path)]);
            assert!(feed(&mut agg, &resource, event).is_none());
        }
        // Another name keeps its own count.
        feed(&mut agg, &resource, tagged_counter("other", 1.0, &[("path", "/z")]));
        assert_names_match_the_groups(&mut agg);

        let flushed = flush_events(&mut agg, 100);
        let req: Vec<&Event> = flushed_named(&flushed, "req").collect();
        assert_eq!(req.len(), 3, "two admitted series and one overflow series");
        let overflow: Vec<&&Event> = req.iter().filter(|e| is_marked(&e.attributes)).collect();
        assert_eq!(overflow.len(), 1);
        assert_eq!(overflow[0].attributes.len(), 1, "the marker alone: no host or private key");
        assert_eq!(counter_value(kind_of(overflow[0])), 3.0 + 4.0 + 5.0);
        let total: f64 = req.iter().map(|e| counter_value(kind_of(e))).sum();
        assert_eq!(total, 15.0, "every record of the name accounted for");

        let events = registry.drain(0);
        assert_eq!(absorbed(&events), 6.0);
        assert_eq!(overflowed(&events), 3.0);
        assert_eq!(counter_total(&events, "logit.transform.metrics.dropped"), 0.0);
        assert_eq!(gauge_value(&events, "logit.transform.metrics.overflowing"), Some(1.0));
    }

    /// The overflow series keeps the record's private keys (after the field pruning) and its
    /// `host.name`, so one name splits into an overflow series per host.
    #[test]
    fn the_overflow_series_keeps_private_keys_and_the_host() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1));
        let resource = default_resource();
        let mut event = |path: &str, host: &str| {
            let mut event = tagged_counter("req", 1.0, &[("path", path), (HOST_NAME, host)]);
            event.attributes.insert("_logit.statsd.type", "c");
            event.attributes.insert(METRICS_FIELDS, true);
            event.attributes.insert("_logit.field.note", "free text");
            feed(&mut agg, &resource, event);
        };
        event("/a", "h1");
        event("/b", "h1");
        event("/c", "h1");
        event("/d", "h2");
        let flushed = flush_events(&mut agg, 100);
        let mut overflow: Vec<AttrMap> = flushed_named(&flushed, "req")
            .filter(|e| is_marked(&e.attributes))
            .map(|e| e.attributes.clone())
            .collect();
        overflow.sort_by_key(|a| a.get(HOST_NAME).and_then(Value::as_str).map(str::to_owned));
        assert_eq!(overflow.len(), 2, "one overflow series per host");
        for (attributes, host) in overflow.iter().zip(["h1", "h2"]) {
            let mut expected = AttrMap::new();
            expected.insert(OVERFLOW, true);
            expected.insert(HOST_NAME, host);
            expected.insert("_logit.statsd.type", "c");
            assert_eq!(attributes, &expected, "no path, no field key, no fields flag");
        }
    }

    /// The limit counts a name across every group, and a fold lands in the record's own group.
    #[test]
    fn the_per_metric_limit_counts_a_name_across_groups() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1));
        let a = default_resource();
        let b = Arc::new(Resource { dropped_attributes_count: 1, ..Resource::default() });
        feed(&mut agg, &a, tagged_counter("req", 1.0, &[("path", "/a")]));
        feed(&mut agg, &b, tagged_counter("req", 1.0, &[("path", "/b")]));
        assert_names_match_the_groups(&mut agg);
        let flushed = flush_events(&mut agg, 100);
        let in_b = flushed.iter().find(|(r, _)| Arc::ptr_eq(r, &b)).expect("b's group opened");
        assert_eq!(in_b.1.len(), 1);
        assert!(is_marked(&in_b.1[0].attributes), "b's record folded, in b's group");
    }

    /// The overflow series doesn't count against the per-name limit, but it does count against
    /// `max_series`, which drops a fold that would open it at a full cap.
    #[test]
    fn the_overflow_series_counts_against_max_series_only() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_max_series(2)
                .with_max_series_per_metric(Some(1)),
        );
        let resource = default_resource();
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/a")]));
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/b")])); // opens overflow
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/c")])); // folds into it
        feed(&mut agg, &resource, tagged_counter("other", 1.0, &[])); // cap full: dropped
        assert_names_match_the_groups(&mut agg);
        let events = registry.drain(0);
        assert_eq!(overflowed(&events), 2.0);
        assert_eq!(series_limit_dropped(&events), Some(1.0));

        // A full `max_series` drops a fold whose overflow series isn't open yet.
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_max_series(1)
                .with_max_series_per_metric(Some(1)),
        );
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/a")]));
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/b")]));
        assert_names_match_the_groups(&mut agg);
        let events = registry.drain(0);
        assert_eq!(overflowed(&events), 0.0, "a dropped record isn't an absorbed one");
        assert_eq!(series_limit_dropped(&events), Some(1.0));
        assert_eq!(absorbed(&events), 1.0);
    }

    /// A flush frees the slot of every series it drains: the next window admits new series of
    /// the name again before it folds.
    #[test]
    fn a_flush_frees_a_names_slots() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1));
        let resource = default_resource();
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/a")]));
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/b")]));
        agg.flush(100);
        assert!(agg.budget.names.is_empty(), "rebuilt from no survivors");
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/b")]));
        let flushed = flush_events(&mut agg, 200);
        let req: Vec<&Event> = flushed_named(&flushed, "req").collect();
        assert_eq!(req.len(), 1);
        assert!(!is_marked(&req[0].attributes), "/b is admitted as itself");
    }

    /// Retained series hold their name's slots across a flush, and an admission eviction frees
    /// one. The name counts stay equal to a scan of the groups throughout.
    #[test]
    fn retained_series_hold_a_names_slots_and_an_eviction_frees_one() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(5, 100)
                .with_max_series(2)
                .with_max_series_per_metric(Some(1)),
        );
        let resource = default_resource();
        let gauge = |path: &str| {
            metric_event_with_tags("g", MetricKind::gauge(1.0), 150, &[("path", path)])
        };
        feed(&mut agg, &resource, gauge("/a"));
        feed(&mut agg, &resource, gauge("/b")); // opens the overflow series
        agg.flush(100);
        assert_names_match_the_groups(&mut agg);
        assert_eq!(agg.budget.names[&intern("g")].series, 1);

        // A new name at the full cap evicts /a, the ordinary series, though the idle overflow
        // series is newer, and frees g's slot.
        feed(&mut agg, &resource, metric_event("h", MetricKind::gauge(1.0), 150));
        assert_names_match_the_groups(&mut agg);
        assert_eq!(agg.budget.names[&intern("g")].series, 0);
        assert!(agg.groups.iter().flat_map(|g| g.series.keys()).any(|k| is_marked(&k.attributes)));
        // So /c opens as itself, evicting the overflow series, the last idle one.
        feed(&mut agg, &resource, gauge("/c"));
        assert_names_match_the_groups(&mut agg);
        assert_eq!(held_names(&agg), ["g", "h"]);
        assert!(!agg.groups.iter().flat_map(|g| g.series.keys()).any(|k| is_marked(&k.attributes)));
        let events = registry.drain(0);
        assert_eq!(evicted_for_series_limit(&events), Some(2.0));
        assert_eq!(overflowed(&events), 1.0, "window one's /b");
    }

    /// The flush cap evicts an overflow series last, so a cumulative overflow total survives
    /// the one-off series opened before it.
    #[test]
    fn the_flush_cap_evicts_an_overflow_series_last() {
        let mut agg =
            cumulative_agg().with_series_retention(5, 2).with_max_series_per_metric(Some(2));
        let resource = default_resource();
        for path in ["/a", "/b", "/c"] {
            feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", path)]));
        }
        // Three survivors over a cap of two; the overflow series opened last, so it's the
        // newest and would go first by `open_seq` alone.
        agg.flush(100);
        let held: Vec<&SeriesKey> = agg.groups.iter().flat_map(|g| g.series.keys()).collect();
        assert_eq!(held.len(), 2);
        assert!(held.iter().any(|k| is_marked(&k.attributes)), "the overflow series survived");
        assert_names_match_the_groups(&mut agg);
    }

    /// Under `temporality: cumulative` the overflow total stays monotonic while retained, and a
    /// series admitted once a slot frees starts at zero: nothing counts twice.
    #[test]
    fn a_cumulative_overflow_total_is_monotonic_and_a_later_series_starts_at_zero() {
        let mut agg =
            cumulative_agg().with_series_retention(1, 100).with_max_series_per_metric(Some(1));
        let resource = default_resource();
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/a")]));
        feed(&mut agg, &resource, tagged_counter("req", 2.0, &[("path", "/b")]));
        agg.flush(100);
        // /a goes idle and is evicted at the next flush; the overflow series keeps absorbing.
        feed(&mut agg, &resource, tagged_counter("req", 3.0, &[("path", "/b")]));
        let flushed = flush_events(&mut agg, 200);
        let overflow = flushed_named(&flushed, "req").find(|e| is_marked(&e.attributes)).unwrap();
        assert_eq!(counter_value(kind_of(overflow)), 5.0, "2, then 3 more");
        // /a's slot is free, so /c opens as itself, from zero.
        feed(&mut agg, &resource, tagged_counter("req", 4.0, &[("path", "/c")]));
        let flushed = flush_events(&mut agg, 300);
        let c = flushed_named(&flushed, "req")
            .find(|e| e.attributes.get("path").and_then(Value::as_str) == Some("/c"))
            .expect("/c admitted");
        assert_eq!(counter_value(kind_of(c)), 4.0);
    }

    /// A record whose kind conflicts with its name's overflow series passes through as a
    /// `kind_conflict`, with its own attributes, and isn't counted as overflowed.
    #[test]
    fn a_kind_conflict_with_the_overflow_series_passes_through() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1)),
        );
        let resource = default_resource();
        feed(&mut agg, &resource, tagged_counter("m", 1.0, &[("path", "/a")]));
        feed(&mut agg, &resource, tagged_counter("m", 1.0, &[("path", "/b")]));
        let gauge = metric_event_with_tags("m", MetricKind::gauge(1.0), 0, &[("path", "/c")]);
        let forwarded = feed(&mut agg, &resource, gauge).expect("the gauge passes through");
        assert_eq!(forwarded.attributes.get("path").and_then(Value::as_str), Some("/c"));
        let events = registry.drain(0);
        assert_eq!(passed_through(&events, "kind_conflict"), 1.0);
        assert_eq!(overflowed(&events), 1.0, "only /b folded");
        assert_names_match_the_groups(&mut agg);
    }

    /// An upstream stage's overflow series is an overflow series here too: known by its key, so
    /// it never counts against the name's limit, and a local fold merges into it.
    #[test]
    fn an_upstream_overflow_series_is_one_here_too() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1)),
        );
        let resource = default_resource();
        let mut upstream = tagged_counter("req", 10.0, &[]);
        upstream.attributes.insert(OVERFLOW, true);
        feed(&mut agg, &resource, upstream);
        feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", "/a")]));
        feed(&mut agg, &resource, tagged_counter("req", 2.0, &[("path", "/b")]));
        assert_names_match_the_groups(&mut agg);
        let flushed = flush_events(&mut agg, 100);
        let req: Vec<&Event> = flushed_named(&flushed, "req").collect();
        assert_eq!(req.len(), 2, "/a admitted, /b folded into the upstream overflow series");
        let overflow = req.iter().find(|e| is_marked(&e.attributes)).unwrap();
        assert_eq!(counter_value(kind_of(overflow)), 12.0);
        assert_eq!(overflowed(&registry.drain(0)), 1.0);

        // A marker beside an attribute the fold removes isn't an overflow key: it counts.
        let mut classes = KeyClasses::default();
        let mut attributes = AttrMap::new();
        attributes.insert(OVERFLOW, true);
        attributes.insert("path", "/a");
        let key = SeriesKey { name: intern("req"), unit: None, attributes };
        assert!(!agg.overflow.is(&key, &mut classes));
        assert!(agg.overflow.is(&agg.overflow.of(key, &mut classes), &mut classes));
    }

    /// With the limit off, no name is counted, the gauge isn't reported, and every series opens.
    #[test]
    fn with_the_limit_off_no_name_is_counted() {
        let (mut agg, registry) = with_registry(Aggregator::new(Duration::from_secs(10)));
        let resource = default_resource();
        for path in ["/a", "/b", "/c"] {
            feed(&mut agg, &resource, tagged_counter("req", 1.0, &[("path", path)]));
        }
        assert!(agg.budget.names.is_empty());
        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed_named(&flushed, "req").count(), 3);
        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.metrics.overflowing"), None);
        assert_eq!(overflowed(&events), 0.0);
    }

    /// `overflowing` counts the names that folded a record this window: a name that only reached
    /// its limit didn't fold.
    #[test]
    fn overflowing_counts_the_names_over_the_limit() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1)),
        );
        let resource = default_resource();
        for name in ["a", "b", "c"] {
            feed(&mut agg, &resource, tagged_counter(name, 1.0, &[("path", "/1")]));
        }
        for name in ["a", "b"] {
            feed(&mut agg, &resource, tagged_counter(name, 1.0, &[("path", "/2")]));
        }
        agg.flush(100);
        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.metrics.overflowing"), Some(2.0));
        agg.flush(200);
        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.metrics.overflowing"), Some(0.0));
    }

    /// A retained overflow series that absorbed nothing this window doesn't keep its name in
    /// `overflowing`, and a record absorbed into an upstream overflow series isn't a fold here, so
    /// it counts toward neither `overflowing` nor `overflowed`.
    #[test]
    fn overflowing_counts_only_this_windows_folds() {
        let (mut agg, registry) = with_registry(
            Aggregator::new(Duration::from_secs(10))
                .with_series_retention(5, 100)
                .with_max_series_per_metric(Some(1)),
        );
        let resource = default_resource();
        let gauge = |path: &str| {
            metric_event_with_tags("g", MetricKind::gauge(1.0), 150, &[("path", path)])
        };
        feed(&mut agg, &resource, gauge("/a"));
        feed(&mut agg, &resource, gauge("/b"));
        agg.flush(100);
        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.metrics.overflowing"), Some(1.0));
        assert!(agg.groups.iter().flat_map(|g| g.series.keys()).any(|k| is_marked(&k.attributes)));

        // The overflow series is retained but idle.
        agg.flush(200);
        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.metrics.overflowing"), Some(0.0));

        // Records for another name's upstream overflow series absorb without folding.
        for value in [1.0, 2.0] {
            let mut upstream = tagged_counter("req", value, &[]);
            upstream.attributes.insert(OVERFLOW, true);
            feed(&mut agg, &resource, upstream);
        }
        agg.flush(300);
        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.metrics.overflowing"), Some(0.0));
        assert_eq!(overflowed(&events), 0.0);
    }

    /// The overflow report's scan runs only when the throttle will let the warning out: at the
    /// first, second, and fourth fold, and never while the diagnostics are muted.
    #[test]
    fn the_overflow_report_is_built_only_when_it_will_be_reported() {
        let five_folds = |agg: &mut Aggregator| {
            let resource = default_resource();
            for path in ["/a", "/b", "/c", "/d", "/e", "/f"] {
                feed(agg, &resource, tagged_counter("req", 1.0, &[("path", path)]));
            }
        };
        let (mut agg, _registry) = with_registry(
            Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(1)),
        );
        five_folds(&mut agg);
        assert_eq!(agg.overflow_reports_built, 3, "folds 1, 2, and 4 of 5");

        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let gate = logit_core::CountGate::new();
        gate.set_muted(true);
        let diag = Diagnostics::default().with_telemetry(telemetry.clone()).gated(&gate);
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_max_series_per_metric(Some(1))
            .with_diagnostics(diag)
            .with_telemetry(telemetry);
        five_folds(&mut agg);
        assert_eq!(agg.overflow_reports_built, 0, "muted");
    }

    /// The diagnostic names the name with the most folds and the attribute key whose values
    /// differ most among its series, never a value, and counts the other overflowing names.
    #[test]
    fn the_overflow_diagnostic_names_the_worst_metric_and_its_widest_key() {
        use tracing_subscriber::layer::SubscriberExt as _;

        let captured = CapturedWarnings::default();
        let subscriber = tracing_subscriber::registry().with(captured.clone());
        tracing::subscriber::with_default(subscriber, || {
            let mut agg =
                Aggregator::new(Duration::from_secs(10)).with_max_series_per_metric(Some(2));
            let resource = default_resource();
            let feed_req = |agg: &mut Aggregator, name: &str, id: &str| {
                let event = tagged_counter(name, 1.0, &[("region", "eu"), ("request_id", id)]);
                feed(agg, &resource, event);
            };
            feed_req(&mut agg, "quiet", "q1");
            feed_req(&mut agg, "quiet", "q2");
            feed_req(&mut agg, "quiet", "q3"); // the 1st fold reports
            feed_req(&mut agg, "loud", "secret-1");
            feed_req(&mut agg, "loud", "secret-2");
            feed_req(&mut agg, "loud", "secret-3"); // the 2nd reports
            feed_req(&mut agg, "loud", "secret-4"); // the 3rd is suppressed
            feed_req(&mut agg, "loud", "secret-5"); // the 4th reports
        });
        let messages = captured.0.lock().unwrap_or_else(|p| p.into_inner()).clone();
        assert_eq!(messages.len(), 3, "{messages:#?}");
        assert!(messages[0].contains("metric 'quiet'") && messages[0].contains("0 other"));
        let last = &messages[2];
        assert!(last.contains("metric 'loud'"), "{last}");
        assert!(last.contains("3 records this window"), "{last}");
        assert!(last.contains("attribute 'request_id' has the most distinct values (2)"), "{last}");
        assert!(last.contains("1 other metric names"), "{last}");
        for message in &messages {
            assert!(!message.contains("secret") && !message.contains("eu"), "{message}");
        }
    }

    /// An unseeded delta opens the series with its own type, as a `Sum`'s first record does.
    #[test]
    fn an_unseeded_i64_delta_resolves_to_an_i64_gauge() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let delta = MetricKind::GaugeDelta(MetricValue::I64(5));
        feed(&mut agg, &resource, metric_event("conns", delta, 0));
        let flushed = flush_events(&mut agg, 100);
        assert_eq!(kind_of(&flushed[0].1[0]), &MetricKind::Gauge(MetricValue::I64(5)));
    }
}

#[cfg(test)]
mod verification;
