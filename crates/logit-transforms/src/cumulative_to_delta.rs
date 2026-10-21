//! `cumulative_to_delta`: keeps the last point of each cumulative series and replaces each later
//! point with its difference from that one, for a destination that takes only deltas
//! (`docs/adr/cumulative-to-delta-transform.md`, the canonical statement of every rule below).
//!
//! **What converts.** A cumulative monotonic `Sum`, a cumulative `Histogram` and
//! `ExponentialHistogram`, and a cumulative `Summary` (its `count` and `sum`; the quantiles pass
//! as they are). Every other record, a flagged `NO_RECORDED_VALUE` one included, passes through
//! untouched and keeps no state.
//!
//! **The series key** is the record's name, unit, and [`Kind`], the batch's resource and scope,
//! and the event's attributes. The start time is stored with the baseline, not keyed.
//!
//! **Each point takes the first rule that applies**, the ADR's "First point, resets, and
//! ordering":
//!
//! 1. no tracked series: `initial_value` decides whether the full value is emitted; either way
//!    the point becomes the baseline;
//! 2. a timestamp at or before the tracked one: skipped, baseline unchanged;
//! 3. a value that went down, or a nonzero start later than the previous point's timestamp and
//!    unequal to the point's own: a reset, emitted in full, and the new baseline;
//! 4. a layout change (a `Histogram`'s bounds, an `ExponentialHistogram`'s scale going up or its
//!    zero threshold going down): dropped, and the new baseline;
//! 5. any other start change: differenced, the new start stored, counted `start_adjusted`;
//! 6. otherwise: differenced.
//!
//! The ADR lists the start change (its rule 4) before the layout change (its rule 5); a point
//! that is both can't be differenced, so the layout rule is checked first and the point counts
//! only as a layout change.
//!
//! A differenced point's `start_timestamp` is the previous point's timestamp. A point emitted in
//! full keeps its own, and its `min` and `max`; a differenced histogram keeps a `min` or `max`
//! only where the lifetime extreme moved, the one case where it is this interval's.
//!
//! **Bounds.** A series no accepted point has touched for longer than `max_staleness`, by the
//! wall clock, is evicted at the next batch; a new series past `max_series` evicts the least
//! recently touched one. Both orders come from one update-ordered queue with lazily discarded
//! entries, so a steady-state point allocates nothing.
//!
//! **Telemetry** is the ADR's "Telemetry" table, tallied per batch and emitted in `end_batch`
//! when nonzero. The runtime counts `logit.component.events.discarded` for an event this left
//! with nothing.

use crate::aggregate::{attr_map_key_eq, group_hash, hash_attr_map, resource_key_eq, scope_key_eq};
use logit_core::interner::Symbol;
use logit_core::{
    AttrMap, Diagnostics, Event, MetricKind, MetricRecord, MetricValue, Resource, Scope, Telemetry,
    Temporality,
};
use logit_pipeline::Transform;
use smallvec::SmallVec;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type Clock = Box<dyn Fn() -> i64 + Send + Sync>;

fn wall_clock_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// What a series' first point emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InitialValue {
    /// The full value when the point's start is nonzero, at or after this component started,
    /// and unequal to its timestamp; otherwise nothing.
    #[default]
    Auto,
    /// Always the full value.
    Keep,
    /// Never anything.
    Drop,
}

/// The converted kind a series holds, part of its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
}

impl Kind {
    fn of(record: &MetricRecord) -> Option<Kind> {
        if record.is_no_recorded_value() {
            return None;
        }
        let cumulative = |t: Temporality| t == Temporality::Cumulative;
        match &record.kind {
            MetricKind::Sum(s) if cumulative(s.temporality) && s.monotonic => Some(Kind::Sum),
            MetricKind::Histogram(h) if cumulative(h.temporality) => Some(Kind::Histogram),
            MetricKind::ExponentialHistogram(e) if cumulative(e.temporality) => {
                Some(Kind::ExponentialHistogram)
            }
            MetricKind::Summary(s) if cumulative(s.temporality) => Some(Kind::Summary),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Kind::Sum => "sum",
            Kind::Histogram => "histogram",
            Kind::ExponentialHistogram => "exponential_histogram",
            Kind::Summary => "summary",
        }
    }
}

struct Key {
    resource: Arc<Resource>,
    scope: Option<Arc<Scope>>,
    name: Symbol,
    unit: Option<Symbol>,
    kind: Kind,
    attributes: AttrMap,
}

/// One side of an exponential histogram: `(offset, counts)`, `counts[i]` at index `offset + i`.
type ExpSide = (i32, Vec<u64>);

/// The previous point, as far as the next difference needs it. A `sum` here is the last finite
/// one, so a non-finite point doesn't break the next difference.
enum Baseline {
    Sum(MetricValue),
    Histogram {
        buckets: Vec<(f64, u64)>,
        sum: Option<f64>,
        min: Option<f64>,
        max: Option<f64>,
    },
    Exponential {
        scale: i32,
        zero_count: u64,
        zero_threshold: f64,
        positive: ExpSide,
        negative: ExpSide,
        count: u64,
        sum: Option<f64>,
        min: Option<f64>,
        max: Option<f64>,
    },
    Summary {
        count: u64,
        sum: Option<f64>,
    },
}

fn finite(v: Option<f64>) -> Option<f64> {
    v.filter(|v| v.is_finite())
}

impl Baseline {
    fn of(kind: &MetricKind) -> Baseline {
        match kind {
            MetricKind::Sum(s) => Baseline::Sum(s.value),
            MetricKind::Histogram(h) => Baseline::Histogram {
                buckets: h.buckets.clone(),
                sum: finite(h.sum),
                min: h.min,
                max: h.max,
            },
            MetricKind::ExponentialHistogram(e) => Baseline::Exponential {
                scale: e.scale,
                zero_count: e.zero_count,
                zero_threshold: e.zero_threshold,
                positive: e.positive.clone(),
                negative: e.negative.clone(),
                count: e.count,
                sum: finite(e.sum),
                min: e.min,
                max: e.max,
            },
            MetricKind::Summary(s) => {
                Baseline::Summary { count: s.count, sum: finite(Some(s.sum)) }
            }
            _ => unreachable!("only a converted kind has a baseline"),
        }
    }

    /// Replaces this baseline with `kind`'s point, keeping a non-finite sum's last finite one.
    fn rebase(&mut self, kind: &MetricKind) {
        let keep_sum = |old: Option<f64>, new: Option<f64>| finite(new).or(old);
        match (self, kind) {
            (Baseline::Sum(v), MetricKind::Sum(s)) => *v = s.value,
            (Baseline::Histogram { buckets, sum, min, max }, MetricKind::Histogram(h)) => {
                buckets.clear();
                buckets.extend_from_slice(&h.buckets);
                *sum = keep_sum(*sum, h.sum);
                *min = h.min;
                *max = h.max;
            }
            (
                Baseline::Exponential {
                    scale,
                    zero_count,
                    zero_threshold,
                    positive,
                    negative,
                    count,
                    sum,
                    min,
                    max,
                },
                MetricKind::ExponentialHistogram(e),
            ) => {
                *scale = e.scale;
                *zero_count = e.zero_count;
                *zero_threshold = e.zero_threshold;
                for (side, from) in [(positive, &e.positive), (negative, &e.negative)] {
                    side.0 = from.0;
                    side.1.clear();
                    side.1.extend_from_slice(&from.1);
                }
                *count = e.count;
                *sum = keep_sum(*sum, e.sum);
                *min = e.min;
                *max = e.max;
            }
            (Baseline::Summary { count, sum }, MetricKind::Summary(s)) => {
                *count = s.count;
                *sum = keep_sum(*sum, Some(s.sum));
            }
            (base, kind) => *base = Baseline::of(kind),
        }
    }
}

struct Series {
    key: Key,
    hash: u64,
    /// The last accepted point's `start_timestamp`.
    start: i64,
    /// The last accepted point's timestamp.
    ts: i64,
    base: Baseline,
    /// The update sequence of this series' live entry in [`CumulativeToDelta::queue`].
    seq: u64,
    /// Wall-clock nanoseconds of the last accepted point.
    touched: i64,
}

/// One batch's tallies, emitted by `end_batch`.
#[derive(Default)]
struct Tally {
    converted: [u64; 4],
    skipped_initial: u64,
    skipped_duplicate: u64,
    skipped_out_of_order: u64,
    dropped_layout: u64,
    dropped_non_finite: u64,
    reset_decrease: u64,
    reset_restarted: u64,
    start_adjusted: u64,
    evicted_idle: u64,
    evicted_cardinality: u64,
}

/// The `cumulative_to_delta` transform. See the module doc.
pub struct CumulativeToDelta {
    initial: InitialValue,
    max_staleness: i64,
    max_series: usize,
    clock: Clock,
    /// When this component started, for `InitialValue::Auto`.
    started: i64,
    /// The wall clock, read on the first convertible record of a batch and cleared by
    /// `end_batch`.
    now: Option<i64>,
    scope: Option<Arc<Scope>>,
    /// The last `(resource, scope)` and their [`group_hash`], so a batch hashes its resource
    /// once. Holding the `Arc`s keeps a pointer comparison from matching a reused address.
    group: Option<(Arc<Resource>, Option<Arc<Scope>>, u64)>,
    series: Vec<Option<Series>>,
    free: Vec<usize>,
    live: usize,
    index: HashMap<u64, SmallVec<[usize; 1]>>,
    /// `(seq, slot)` in update order; an entry whose seq isn't its series' is stale.
    queue: VecDeque<(u64, usize)>,
    next_seq: u64,
    /// An exponential histogram side's delta, built before the baseline is overwritten.
    scratch: Vec<u64>,
    tally: Tally,
    telemetry: Telemetry,
    diag: Diagnostics,
}

impl CumulativeToDelta {
    pub fn new(initial: InitialValue, max_staleness: Duration, max_series: usize) -> Self {
        let clock: Clock = Box::new(wall_clock_nanos);
        Self {
            initial,
            max_staleness: i64::try_from(max_staleness.as_nanos()).unwrap_or(i64::MAX),
            max_series: max_series.max(1),
            started: clock(),
            clock,
            now: None,
            scope: None,
            group: None,
            series: Vec::new(),
            free: Vec::new(),
            live: 0,
            index: HashMap::new(),
            queue: VecDeque::new(),
            next_seq: 0,
            scratch: Vec::new(),
            tally: Tally::default(),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
        }
    }

    /// Counts the ADR's telemetry into `telemetry`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Routes the throttled warnings through `diag`.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Replaces the wall clock, which reads Unix nanoseconds, and restarts the component's start
    /// time from it; for tests.
    pub fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.clock = Box::new(clock);
        self.started = (self.clock)();
        self
    }

    /// The batch's wall clock; its first read in a batch evicts the idle series.
    fn now(&mut self) -> i64 {
        if let Some(now) = self.now {
            return now;
        }
        let now = (self.clock)();
        self.now = Some(now);
        while let Some(&(seq, slot)) = self.queue.front() {
            match &self.series[slot] {
                Some(s) if s.seq == seq => {
                    if now.saturating_sub(s.touched) <= self.max_staleness {
                        break;
                    }
                    self.queue.pop_front();
                    self.remove(slot);
                    self.tally.evicted_idle += 1;
                }
                _ => {
                    self.queue.pop_front();
                }
            }
        }
        now
    }

    fn key_hash(
        &mut self,
        resource: &Arc<Resource>,
        record: &MetricRecord,
        kind: Kind,
        attributes: &AttrMap,
    ) -> u64 {
        let same_scope = |s: &Option<Arc<Scope>>| match (s, &self.scope) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        let group = match &self.group {
            Some((r, s, h)) if Arc::ptr_eq(r, resource) && same_scope(s) => *h,
            _ => {
                let h = group_hash(resource, &self.scope);
                self.group = Some((resource.clone(), self.scope.clone(), h));
                h
            }
        };
        let mut state = DefaultHasher::new();
        group.hash(&mut state);
        record.name.hash(&mut state);
        record.unit.hash(&mut state);
        kind.hash(&mut state);
        hash_attr_map(attributes, &mut state);
        state.finish()
    }

    fn find(
        &self,
        hash: u64,
        resource: &Arc<Resource>,
        record: &MetricRecord,
        kind: Kind,
        attributes: &AttrMap,
    ) -> Option<usize> {
        self.index.get(&hash)?.iter().copied().find(|&slot| {
            let k = &self.series[slot].as_ref().expect("an indexed slot is live").key;
            k.name == record.name
                && k.unit == record.unit
                && k.kind == kind
                && resource_key_eq(&k.resource, resource)
                && scope_key_eq(&k.scope, &self.scope)
                && attr_map_key_eq(&k.attributes, attributes)
        })
    }

    fn remove(&mut self, slot: usize) {
        let series = self.series[slot].take().expect("removing a live slot");
        if let Some(slots) = self.index.get_mut(&series.hash) {
            slots.retain(|s| *s != slot);
            if slots.is_empty() {
                self.index.remove(&series.hash);
            }
        }
        self.free.push(slot);
        self.live -= 1;
    }

    /// Moves `slot` to the back of the update order.
    fn touch(&mut self, slot: usize, now: i64) {
        self.next_seq += 1;
        let seq = self.next_seq;
        let series = self.series[slot].as_mut().expect("touching a live slot");
        series.seq = seq;
        series.touched = now;
        self.queue.push_back((seq, slot));
        // Stale entries are dropped as the front reaches them; compact when they dominate.
        if self.queue.len() > 2 * self.live + 64 {
            let series = &self.series;
            self.queue.retain(|&(seq, slot)| series[slot].as_ref().is_some_and(|s| s.seq == seq));
        }
    }

    fn insert(&mut self, series: Series, now: i64) {
        if self.live >= self.max_series {
            while let Some((seq, slot)) = self.queue.pop_front() {
                if self.series[slot].as_ref().is_some_and(|s| s.seq == seq) {
                    self.remove(slot);
                    self.tally.evicted_cardinality += 1;
                    self.diag.warn_throttled(
                        "max_series_full",
                        format_args!(
                            "cumulative_to_delta tracks max_series ({}) series; evicted the \
                             least recently updated, whose next point records a baseline and \
                             loses the counts since its last",
                            self.max_series
                        ),
                    );
                    break;
                }
            }
        }
        let hash = series.hash;
        let slot = match self.free.pop() {
            Some(slot) => {
                self.series[slot] = Some(series);
                slot
            }
            None => {
                self.series.push(Some(series));
                self.series.len() - 1
            }
        };
        self.index.entry(hash).or_default().push(slot);
        self.live += 1;
        self.touch(slot, now);
    }

    /// Converts one record in place; `false` removes it from its event.
    fn convert(
        &mut self,
        resource: &Arc<Resource>,
        ts: i64,
        attributes: &AttrMap,
        record: &mut MetricRecord,
    ) -> bool {
        let Some(kind) = Kind::of(record) else { return true };
        if let MetricKind::Sum(s) = &record.kind {
            if !s.value.is_finite() {
                self.tally.dropped_non_finite += 1;
                self.diag.warn_throttled(
                    "non_finite_sum",
                    "cumulative_to_delta dropped a cumulative sum whose value isn't finite",
                );
                return false;
            }
        }
        let now = self.now();
        let hash = self.key_hash(resource, record, kind, attributes);
        let start = record.start_timestamp;
        let Some(slot) = self.find(hash, resource, record, kind, attributes) else {
            let emit = match self.initial {
                InitialValue::Keep => true,
                InitialValue::Drop => false,
                InitialValue::Auto => start != 0 && start >= self.started && start != ts,
            };
            let key = Key {
                resource: resource.clone(),
                scope: self.scope.clone(),
                name: record.name,
                unit: record.unit,
                kind,
                attributes: attributes.clone(),
            };
            let base = Baseline::of(&record.kind);
            self.insert(Series { key, hash, start, ts, base, seq: 0, touched: now }, now);
            return if emit {
                self.emit_full(record, kind);
                true
            } else {
                self.tally.skipped_initial += 1;
                false
            };
        };

        let series = self.series[slot].as_mut().expect("a found slot is live");
        if ts <= series.ts {
            if ts == series.ts {
                self.tally.skipped_duplicate += 1;
            } else {
                self.tally.skipped_out_of_order += 1;
            }
            return false;
        }
        let layout = align(&mut series.base, &record.kind);
        let decreased = decreased(&series.base, &record.kind, layout);
        let restarted = start != 0 && start > series.ts && start != ts;
        if decreased || restarted {
            if decreased {
                self.tally.reset_decrease += 1;
            } else {
                self.tally.reset_restarted += 1;
            }
            series.base.rebase(&record.kind);
            series.start = start;
            series.ts = ts;
            self.touch(slot, now);
            self.emit_full(record, kind);
            return true;
        }
        if layout {
            series.base.rebase(&record.kind);
            series.start = start;
            series.ts = ts;
            self.touch(slot, now);
            self.tally.dropped_layout += 1;
            self.diag.warn_throttled(
                "layout_changed",
                "cumulative_to_delta dropped a histogram point whose buckets changed without a \
                 reset: the counts since the previous point are lost",
            );
            return false;
        }
        if start != series.start {
            series.start = start;
            self.tally.start_adjusted += 1;
        }
        difference(&mut series.base, &mut record.kind, &mut self.scratch);
        record.start_timestamp = series.ts;
        series.ts = ts;
        self.touch(slot, now);
        self.tally.converted[kind as usize] += 1;
        true
    }

    /// A first or reset point: its whole value, as a delta.
    fn emit_full(&mut self, record: &mut MetricRecord, kind: Kind) {
        set_delta(&mut record.kind);
        self.tally.converted[kind as usize] += 1;
    }
}

fn set_delta(kind: &mut MetricKind) {
    match kind {
        MetricKind::Sum(s) => s.temporality = Temporality::Delta,
        MetricKind::Histogram(h) => h.temporality = Temporality::Delta,
        MetricKind::ExponentialHistogram(e) => e.temporality = Temporality::Delta,
        MetricKind::Summary(s) => s.temporality = Temporality::Delta,
        _ => unreachable!("only a converted kind is set delta"),
    }
}

/// A `MetricValue` as an exact integer, or `None` for an `F64`.
fn exact_int(v: MetricValue) -> Option<i128> {
    match v {
        MetricValue::I64(n) => Some(i128::from(n)),
        MetricValue::U64(n) => Some(i128::from(n)),
        MetricValue::Bool(b) => Some(i128::from(b)),
        MetricValue::F64(_) => None,
    }
}

/// `cur - prev` in `cur`'s type where it fits (a `Bool` as an `I64`), else an `F64`; `None` when
/// `cur` is below `prev`.
fn value_delta(cur: MetricValue, prev: MetricValue) -> Option<MetricValue> {
    match (exact_int(cur), exact_int(prev)) {
        (Some(c), Some(p)) => {
            let d = c.checked_sub(p).filter(|d| *d >= 0)?;
            let fallback = MetricValue::F64(d as f64);
            Some(match cur {
                MetricValue::U64(_) => u64::try_from(d).map_or(fallback, MetricValue::U64),
                _ => i64::try_from(d).map_or(fallback, MetricValue::I64),
            })
        }
        _ => {
            let (c, p) = (cur.as_f64(), prev.as_f64());
            (c >= p).then_some(MetricValue::F64(c - p))
        }
    }
}

/// The count at absolute index `idx` of an exponential histogram side, `0` outside it.
fn side_get(side: &ExpSide, idx: i64) -> u64 {
    let i = idx - i64::from(side.0);
    usize::try_from(i).ok().and_then(|i| side.1.get(i).copied()).unwrap_or(0)
}

/// Whether some bucket of `base` holds more than `cur`'s bucket at the same index.
fn side_decreased(base: &ExpSide, cur: &ExpSide) -> bool {
    base.1.iter().enumerate().any(|(i, &b)| b > side_get(cur, i64::from(base.0) + i as i64))
}

/// `side` at `scale - shift`: each index `i` becomes `i >> shift`, an arithmetic shift, so a
/// negative index rounds down as the exponential bucket boundaries do.
fn downscale(side: &mut ExpSide, shift: u32) {
    if shift == 0 || side.1.is_empty() {
        return;
    }
    let first = i64::from(side.0) >> shift;
    let last = (i64::from(side.0) + side.1.len() as i64 - 1) >> shift;
    let mut counts = vec![0u64; (last - first + 1) as usize];
    for (i, &c) in side.1.iter().enumerate() {
        let idx = (i64::from(side.0) + i as i64) >> shift;
        counts[(idx - first) as usize] += c;
    }
    *side = (first as i32, counts);
}

/// Moves every bucket of `side` whose upper bound, `2^((i + 1) * 2^-scale)`, is at or below
/// `threshold` into the returned count, leaving those buckets at `0`.
fn fold_below(side: &mut ExpSide, scale: i32, threshold: f64) -> u64 {
    let width = (-f64::from(scale)).exp2();
    let mut folded = 0;
    for (i, c) in side.1.iter_mut().enumerate() {
        let upper = ((f64::from(side.0) + i as f64 + 1.0) * width).exp2();
        if upper > threshold {
            break;
        }
        folded += std::mem::take(c);
    }
    folded
}

/// Brings an exponential baseline to `kind`'s scale and zero threshold where that loses
/// nothing, and returns whether `kind`'s layout differs from the baseline's in a way that can't
/// be differenced: a `Histogram`'s bounds, or an `ExponentialHistogram`'s scale going up or zero
/// threshold going down.
fn align(base: &mut Baseline, kind: &MetricKind) -> bool {
    match (base, kind) {
        (Baseline::Histogram { buckets, .. }, MetricKind::Histogram(h)) => {
            buckets.len() != h.buckets.len()
                || buckets.iter().zip(&h.buckets).any(|(b, c)| b.0.to_bits() != c.0.to_bits())
        }
        (
            Baseline::Exponential { scale, zero_count, zero_threshold, positive, negative, .. },
            MetricKind::ExponentialHistogram(e),
        ) => {
            if e.scale > *scale || e.zero_threshold < *zero_threshold {
                return true;
            }
            if e.scale < *scale {
                let shift = (*scale - e.scale) as u32;
                downscale(positive, shift);
                downscale(negative, shift);
                *scale = e.scale;
            }
            if e.zero_threshold > *zero_threshold {
                *zero_count += fold_below(positive, *scale, e.zero_threshold);
                *zero_count += fold_below(negative, *scale, e.zero_threshold);
                *zero_threshold = e.zero_threshold;
            }
            false
        }
        _ => false,
    }
}

/// Whether `kind` holds less than the baseline anywhere: a `Sum`'s value, a count, or a bucket.
/// Buckets are compared only when `layout` is `false`, since they're aligned only then.
fn decreased(base: &Baseline, kind: &MetricKind, layout: bool) -> bool {
    match (base, kind) {
        (Baseline::Sum(prev), MetricKind::Sum(s)) => value_delta(s.value, *prev).is_none(),
        (Baseline::Histogram { buckets, .. }, MetricKind::Histogram(h)) => {
            let total = |b: &[(f64, u64)]| b.iter().fold(0u64, |a, b| a.saturating_add(b.1));
            total(buckets) > total(&h.buckets)
                || (!layout && buckets.iter().zip(&h.buckets).any(|(b, c)| b.1 > c.1))
        }
        (
            Baseline::Exponential { count, zero_count, positive, negative, .. },
            MetricKind::ExponentialHistogram(e),
        ) => {
            *count > e.count
                || (!layout
                    && (*zero_count > e.zero_count
                        || side_decreased(positive, &e.positive)
                        || side_decreased(negative, &e.negative)))
        }
        (Baseline::Summary { count, .. }, MetricKind::Summary(s)) => *count > s.count,
        _ => false,
    }
}

/// A delta's `min`: the new lifetime minimum when it went below the previous one, which only
/// this interval can have observed; otherwise unknown.
fn interval_min(cur: Option<f64>, prev: Option<f64>) -> Option<f64> {
    match (cur, prev) {
        (Some(c), Some(p)) if c < p => Some(c),
        _ => None,
    }
}

/// [`interval_min`]'s mirror image.
fn interval_max(cur: Option<f64>, prev: Option<f64>) -> Option<f64> {
    match (cur, prev) {
        (Some(c), Some(p)) if c > p => Some(c),
        _ => None,
    }
}

/// A delta's `sum`, absent unless both the point's and the baseline's are finite.
fn sum_delta(cur: Option<f64>, prev: Option<f64>) -> Option<f64> {
    match (finite(cur), prev) {
        (Some(c), Some(p)) => Some(c - p),
        _ => None,
    }
}

/// Replaces `kind` with its difference from `base`, and `base` with `kind`'s point. The caller
/// has ruled out a decrease and a layout change.
fn difference(base: &mut Baseline, kind: &mut MetricKind, scratch: &mut Vec<u64>) {
    match (base, kind) {
        (Baseline::Sum(prev), MetricKind::Sum(s)) => {
            let cur = s.value;
            s.value = value_delta(cur, *prev).expect("no decrease");
            s.temporality = Temporality::Delta;
            *prev = cur;
        }
        (Baseline::Histogram { buckets, sum, min, max }, MetricKind::Histogram(h)) => {
            for (b, c) in buckets.iter_mut().zip(h.buckets.iter_mut()) {
                let cur = c.1;
                c.1 = cur - b.1;
                b.1 = cur;
            }
            let (cur_sum, cur_min, cur_max) = (h.sum, h.min, h.max);
            h.sum = sum_delta(cur_sum, *sum);
            h.min = interval_min(cur_min, *min);
            h.max = interval_max(cur_max, *max);
            h.temporality = Temporality::Delta;
            *sum = finite(cur_sum).or(*sum);
            *min = cur_min;
            *max = cur_max;
        }
        (
            Baseline::Exponential { zero_count, positive, negative, count, sum, min, max, .. },
            MetricKind::ExponentialHistogram(e),
        ) => {
            for (side, cur) in [(positive, &mut e.positive), (negative, &mut e.negative)] {
                scratch.clear();
                let offset = i64::from(cur.0);
                scratch.extend(
                    cur.1.iter().enumerate().map(|(i, c)| c - side_get(side, offset + i as i64)),
                );
                side.0 = cur.0;
                side.1.clear();
                side.1.extend_from_slice(&cur.1);
                cur.1.copy_from_slice(scratch);
            }
            let (cur_zero, cur_count) = (e.zero_count, e.count);
            e.zero_count = cur_zero - *zero_count;
            e.count = cur_count - *count;
            *zero_count = cur_zero;
            *count = cur_count;
            let (cur_sum, cur_min, cur_max) = (e.sum, e.min, e.max);
            e.sum = sum_delta(cur_sum, *sum);
            e.min = interval_min(cur_min, *min);
            e.max = interval_max(cur_max, *max);
            e.temporality = Temporality::Delta;
            *sum = finite(cur_sum).or(*sum);
            *min = cur_min;
            *max = cur_max;
        }
        (Baseline::Summary { count, sum }, MetricKind::Summary(s)) => {
            let (cur_count, cur_sum) = (s.count, s.sum);
            s.count = cur_count - *count;
            // `Summary::sum` has no absent form, so an unknown difference is `NaN`.
            s.sum = sum_delta(Some(cur_sum), *sum).unwrap_or(f64::NAN);
            s.temporality = Temporality::Delta;
            *count = cur_count;
            *sum = finite(Some(cur_sum)).or(*sum);
        }
        _ => unreachable!("a series' baseline matches its kind"),
    }
}

impl Transform for CumulativeToDelta {
    fn observe_scope(&mut self, scope: Option<Arc<Scope>>) {
        self.scope = scope;
    }

    fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
        if event.metrics.is_empty() {
            return true;
        }
        let ts = event.timestamp;
        let before = event.metrics.len();
        let attributes = &event.attributes;
        event.metrics.retain_mut(|record| self.convert(resource, ts, attributes, record));
        event.metrics.len() == before
            || !event.metrics.is_empty()
            || event.log.is_some()
            || event.span.is_some()
    }

    fn end_batch(&mut self) {
        let saw_series = self.now.take().is_some();
        let t = std::mem::take(&mut self.tally);
        let tel = &self.telemetry;
        let count = |name: &'static str, n: u64, tags: &[(&'static str, &'static str)]| {
            if n > 0 {
                tel.count(name, n as f64, tags);
            }
        };
        for kind in [Kind::Sum, Kind::Histogram, Kind::ExponentialHistogram, Kind::Summary] {
            count(
                "logit.transform.metrics.converted",
                t.converted[kind as usize],
                &[("metric_kind", kind.name())],
            );
        }
        let skipped = "logit.transform.metrics.skipped";
        count(skipped, t.skipped_initial, &[("reason", "initial")]);
        count(skipped, t.skipped_duplicate, &[("reason", "duplicate")]);
        count(skipped, t.skipped_out_of_order, &[("reason", "out_of_order")]);
        let dropped = "logit.transform.metrics.dropped";
        count(dropped, t.dropped_layout, &[("reason", "layout_changed")]);
        count(dropped, t.dropped_non_finite, &[("reason", "non_finite")]);
        count("logit.transform.series.reset", t.reset_decrease, &[("cause", "decrease")]);
        count("logit.transform.series.reset", t.reset_restarted, &[("cause", "restarted")]);
        count("logit.transform.series.start_adjusted", t.start_adjusted, &[]);
        count("logit.transform.series.evicted", t.evicted_idle, &[("reason", "idle")]);
        count(
            "logit.transform.series.evicted",
            t.evicted_cardinality,
            &[("reason", "cardinality")],
        );
        if saw_series {
            tel.gauge("logit.transform.series.tracked", self.live as f64, &[]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{
        BodyFormat, EventBatch, ExpHistogram, Histogram, LogRecord, Sum, Summary, Value,
    };
    use logit_pipeline::runtime::process_batch;
    use logit_pipeline::test_util::TelemetryProbe;
    use std::sync::atomic::{AtomicI64, Ordering};

    const S: i64 = 1_000_000_000;
    /// The component's start, and the clock until a test moves it.
    const T0: i64 = 1_000_000 * S;

    struct Rig {
        t: CumulativeToDelta,
        clock: Arc<AtomicI64>,
        probe: TelemetryProbe,
        telemetry: Telemetry,
        resource: Arc<Resource>,
        scope: Option<Arc<Scope>>,
    }

    impl Rig {
        fn new(initial: InitialValue) -> Self {
            Self::bounded(initial, Duration::from_secs(3600), 10_000)
        }

        fn bounded(initial: InitialValue, max_staleness: Duration, max_series: usize) -> Self {
            let clock = Arc::new(AtomicI64::new(T0));
            let probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("c2d", "cumulative_to_delta", "transform");
            let read = clock.clone();
            let t = CumulativeToDelta::new(initial, max_staleness, max_series)
                .with_clock(move || read.load(Ordering::SeqCst))
                .with_telemetry(telemetry.clone());
            Self {
                t,
                clock,
                probe,
                telemetry,
                resource: Arc::new(Resource::default()),
                scope: None,
            }
        }

        /// One batch through the runtime's own `process_batch`: what's left of each event.
        fn run(&mut self, events: Vec<Event>) -> Vec<Event> {
            let batch =
                EventBatch { resource: self.resource.clone(), scope: self.scope.clone(), events };
            process_batch(&mut self.t, batch, &mut Vec::new(), &self.telemetry)
                .map(|b| b.events)
                .unwrap_or_default()
        }

        /// The one record a one-event batch leaves, or `None` when it's removed.
        fn one(&mut self, event: Event) -> Option<MetricRecord> {
            let mut out = self.run(vec![event]);
            assert!(out.len() <= 1);
            out.pop().map(|mut e| {
                assert_eq!(e.metrics.len(), 1);
                e.metrics.remove(0)
            })
        }

        fn sum(&mut self, name: &str, tags: &[(&str, &str)]) -> f64 {
            self.probe.sum(name, tags)
        }
    }

    fn resource_with(key: &str, value: &str) -> Arc<Resource> {
        let mut resource = Resource::default();
        resource.attributes.insert(key, value);
        Arc::new(resource)
    }

    fn event(ts: i64, start: i64, kind: MetricKind) -> Event {
        let mut rec = MetricRecord::new(intern("m"), kind);
        rec.start_timestamp = start;
        Event::metric(ts, AttrMap::new(), rec)
    }

    fn cum(v: f64) -> MetricKind {
        MetricKind::Sum(Sum {
            value: MetricValue::F64(v),
            temporality: Temporality::Cumulative,
            monotonic: true,
        })
    }

    fn sum_of(rec: &MetricRecord) -> (MetricValue, Temporality) {
        match &rec.kind {
            MetricKind::Sum(s) => (s.value, s.temporality),
            other => panic!("expected a Sum, got {other:?}"),
        }
    }

    fn hist(buckets: &[(f64, u64)], sum: Option<f64>, min: f64, max: f64) -> MetricKind {
        MetricKind::Histogram(Histogram {
            buckets: buckets.to_vec(),
            temporality: Temporality::Cumulative,
            sum,
            min: Some(min),
            max: Some(max),
        })
    }

    fn hist_of(rec: &MetricRecord) -> &Histogram {
        match &rec.kind {
            MetricKind::Histogram(h) => h,
            other => panic!("expected a Histogram, got {other:?}"),
        }
    }

    fn exp(scale: i32, zero: (u64, f64), positive: (i32, &[u64]), count: u64) -> MetricKind {
        MetricKind::ExponentialHistogram(ExpHistogram {
            scale,
            zero_count: zero.0,
            zero_threshold: zero.1,
            positive: (positive.0, positive.1.to_vec()),
            negative: (0, vec![]),
            temporality: Temporality::Cumulative,
            count,
            sum: Some(count as f64),
            min: None,
            max: None,
        })
    }

    fn exp_of(rec: &MetricRecord) -> &ExpHistogram {
        match &rec.kind {
            MetricKind::ExponentialHistogram(e) => e,
            other => panic!("expected an ExponentialHistogram, got {other:?}"),
        }
    }

    fn summary(count: u64, sum: f64) -> MetricKind {
        MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 2.0), (0.99, 9.0)],
            count,
            sum,
            temporality: Temporality::Cumulative,
        })
    }

    // ---- Rule 1: the first point ----------------------------------------------------------------

    /// Under `auto`, a first point emits its full value only when its start is nonzero, at or
    /// after the component started, and unequal to its own timestamp.
    #[test]
    fn auto_emits_a_first_point_only_for_a_series_that_started_while_watching() {
        let mut rig = Rig::new(InitialValue::Auto);
        let cases = [
            ("no start", 0, false),
            ("started before the component", T0 - S, false),
            ("start equal to the timestamp", T0 + 5 * S, false),
            ("started while watching", T0, true),
        ];
        for (what, start, emitted) in cases {
            rig.resource = resource_with("case", what);
            let out = rig.one(event(T0 + 5 * S, start, cum(7.0)));
            assert_eq!(out.is_some(), emitted, "{what}");
            if let Some(rec) = out {
                assert_eq!(sum_of(&rec), (MetricValue::F64(7.0), Temporality::Delta), "{what}");
                assert_eq!(rec.start_timestamp, start, "a full value keeps its own start");
            }
        }
        assert_eq!(rig.sum("logit.transform.metrics.skipped", &[("reason", "initial")]), 3.0);
        assert_eq!(rig.sum("logit.transform.metrics.converted", &[("metric_kind", "sum")]), 1.0);
    }

    #[test]
    fn keep_always_emits_a_first_point_and_drop_never_does() {
        let mut keep = Rig::new(InitialValue::Keep);
        let rec = keep.one(event(T0, 0, cum(7.0))).expect("keep emits");
        assert_eq!(sum_of(&rec), (MetricValue::F64(7.0), Temporality::Delta));
        let mut drop = Rig::new(InitialValue::Drop);
        assert!(drop.one(event(T0 + S, T0, cum(7.0))).is_none(), "drop never emits");
        assert_eq!(drop.sum("logit.transform.metrics.skipped", &[("reason", "initial")]), 1.0);
        // Either way the point became the baseline.
        let rec = drop.one(event(T0 + 2 * S, T0, cum(10.0))).expect("differenced");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(3.0));
    }

    // ---- Rule 6: the difference -----------------------------------------------------------------

    /// A later point is its difference from the previous one, in the point's own integer type,
    /// starting at the previous point's timestamp.
    #[test]
    fn a_later_point_is_differenced_from_the_previous_one() {
        let mut rig = Rig::new(InitialValue::Drop);
        let u = |v| {
            MetricKind::Sum(Sum {
                value: MetricValue::U64(v),
                temporality: Temporality::Cumulative,
                monotonic: true,
            })
        };
        assert!(rig.one(event(T0 + S, T0, u(10))).is_none());
        let rec = rig.one(event(T0 + 2 * S, T0, u(25))).expect("differenced");
        assert_eq!(sum_of(&rec), (MetricValue::U64(15), Temporality::Delta));
        assert_eq!(rec.start_timestamp, T0 + S, "the previous point's timestamp");
        let rec = rig.one(event(T0 + 3 * S, T0, u(25))).expect("differenced");
        assert_eq!(sum_of(&rec).0, MetricValue::U64(0), "no change is a zero delta");
        assert_eq!(rig.sum("logit.transform.metrics.converted", &[("metric_kind", "sum")]), 2.0);
    }

    // ---- Rule 2: ordering -----------------------------------------------------------------------

    /// A point at or before the tracked timestamp is skipped and leaves the baseline alone.
    #[test]
    fn a_resent_or_overtaken_point_is_skipped_and_the_baseline_kept() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + 2 * S, T0, cum(10.0)));
        assert!(rig.one(event(T0 + 2 * S, T0, cum(10.0))).is_none(), "a resend");
        assert!(rig.one(event(T0 + S, T0, cum(4.0))).is_none(), "an older point");
        let rec = rig.one(event(T0 + 3 * S, T0, cum(12.0))).expect("differenced");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(2.0), "against the kept baseline");
        assert_eq!(rig.sum("logit.transform.metrics.skipped", &[("reason", "duplicate")]), 1.0);
        assert_eq!(rig.sum("logit.transform.metrics.skipped", &[("reason", "out_of_order")]), 1.0);
        assert_eq!(rig.sum("logit.transform.series.reset", &[]), 0.0);
    }

    // ---- Rule 3: resets -------------------------------------------------------------------------

    /// A value that went down is a counter restart: the new value is emitted whole, whatever
    /// `initial_value` says, and becomes the baseline.
    #[test]
    fn a_decrease_is_a_reset_emitted_in_full() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + S, 0, cum(100.0)));
        let rec = rig.one(event(T0 + 2 * S, 0, cum(3.0))).expect("a reset emits");
        assert_eq!(sum_of(&rec), (MetricValue::F64(3.0), Temporality::Delta));
        assert_eq!(rec.start_timestamp, 0, "a full value keeps its own start");
        let rec = rig.one(event(T0 + 3 * S, 0, cum(5.0))).expect("differenced");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(2.0));
        assert_eq!(rig.sum("logit.transform.series.reset", &[("cause", "decrease")]), 1.0);
        assert_eq!(rig.sum("logit.transform.metrics.converted", &[]), 2.0);
    }

    /// A start moved past the previous point is a restart even when the value went up.
    #[test]
    fn a_start_after_the_previous_point_is_a_restart() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + 10 * S, T0, cum(5.0)));
        let rec = rig.one(event(T0 + 20 * S, T0 + 15 * S, cum(8.0))).expect("a reset emits");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(8.0), "the whole new total");
        assert_eq!(rec.start_timestamp, T0 + 15 * S);
        let rec = rig.one(event(T0 + 30 * S, T0 + 15 * S, cum(9.0))).expect("differenced");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(1.0));
        assert_eq!(rig.sum("logit.transform.series.reset", &[("cause", "restarted")]), 1.0);
        assert_eq!(rig.sum("logit.transform.series.start_adjusted", &[]), 0.0);
    }

    // ---- Rule 5: a start change with no reset ---------------------------------------------------

    /// A start that jitters, moves to before the previous point, or equals the timestamp on
    /// every point is differenced, never re-sent whole.
    #[test]
    fn a_start_change_without_a_reset_is_differenced_with_no_overcount() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + 10 * S, T0, cum(5.0)));
        // Jitter by a millisecond, then back to before the first point.
        let rec = rig.one(event(T0 + 20 * S, T0 + 1_000_000, cum(8.0))).expect("jitter");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(3.0));
        let rec = rig.one(event(T0 + 30 * S, T0 - S, cum(9.0))).expect("earlier start");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(1.0));
        assert_eq!(rig.sum("logit.transform.series.start_adjusted", &[]), 2.0);

        // A producer that stamps the start at the timestamp on every point.
        let mut rig = Rig::new(InitialValue::Auto);
        let mut emitted = 0.0;
        for (i, total) in [(1, 10.0), (2, 14.0), (3, 20.0), (4, 21.0)] {
            let ts = T0 + i * S;
            if let Some(rec) = rig.one(event(ts, ts, cum(total))) {
                emitted += sum_of(&rec).0.as_f64();
            }
        }
        assert_eq!(emitted, 11.0, "the increments after the first point, never a total");
        assert_eq!(rig.sum("logit.transform.series.reset", &[]), 0.0);
        assert_eq!(rig.sum("logit.transform.series.start_adjusted", &[]), 3.0);
    }

    // ---- Histograms -----------------------------------------------------------------------------

    /// Every bucket, the sum, and `min`/`max` where the lifetime extreme moved.
    #[test]
    fn a_histogram_differences_its_buckets_and_keeps_only_exact_extremes() {
        let mut rig = Rig::new(InitialValue::Drop);
        let b = |c: [u64; 3]| [(1.0, c[0]), (5.0, c[1]), (f64::INFINITY, c[2])];
        rig.one(event(T0 + S, T0, hist(&b([1, 2, 3]), Some(10.0), 0.5, 9.0)));
        let rec = rig.one(event(T0 + 2 * S, T0, hist(&b([2, 2, 5]), Some(25.0), 0.25, 9.0)));
        let h = hist_of(rec.as_ref().expect("differenced"));
        assert_eq!(h.buckets, b([1, 0, 2]));
        assert_eq!(h.sum, Some(15.0));
        assert_eq!(h.min, Some(0.25), "a new lifetime low is this interval's");
        assert_eq!(h.max, None, "an unchanged lifetime high says nothing of this interval");
        assert_eq!(h.temporality, Temporality::Delta);
        let rec = rig.one(event(T0 + 3 * S, T0, hist(&b([2, 3, 5]), Some(27.0), 0.25, 12.0)));
        let h = hist_of(rec.as_ref().expect("differenced"));
        assert_eq!((h.min, h.max), (None, Some(12.0)));
    }

    /// A first point emitted in full keeps its `min` and `max`: its interval is the series' life.
    #[test]
    fn a_histogram_emitted_in_full_keeps_its_extremes() {
        let mut rig = Rig::new(InitialValue::Keep);
        let rec = rig.one(event(T0 + S, T0, hist(&[(f64::INFINITY, 2)], Some(3.0), 1.0, 2.0)));
        let h = hist_of(rec.as_ref().expect("keep emits"));
        assert_eq!((h.min, h.max, h.temporality), (Some(1.0), Some(2.0), Temporality::Delta));
    }

    /// A non-finite sum leaves the delta with no sum, and the next one differences against the
    /// last finite sum.
    #[test]
    fn a_non_finite_histogram_sum_is_absent_and_the_last_finite_one_kept() {
        let mut rig = Rig::new(InitialValue::Drop);
        let b = |n| [(f64::INFINITY, n)];
        rig.one(event(T0 + S, T0, hist(&b(1), Some(10.0), 1.0, 1.0)));
        let rec = rig.one(event(T0 + 2 * S, T0, hist(&b(2), Some(f64::NAN), 1.0, 1.0)));
        assert_eq!(hist_of(rec.as_ref().unwrap()).sum, None);
        let rec = rig.one(event(T0 + 3 * S, T0, hist(&b(3), Some(16.0), 1.0, 1.0)));
        assert_eq!(hist_of(rec.as_ref().unwrap()).sum, Some(6.0));
    }

    /// Changed bounds with no reset: dropped and counted, and the point is the new baseline.
    #[test]
    fn a_histogram_whose_bounds_changed_is_dropped_and_rebased() {
        let mut rig = Rig::new(InitialValue::Drop);
        let h = |b: &[(f64, u64)]| hist(b, None, 1.0, 1.0);
        rig.one(event(T0 + S, T0, h(&[(1.0, 1), (f64::INFINITY, 1)])));
        assert!(rig.one(event(T0 + 2 * S, T0, h(&[(2.0, 2), (f64::INFINITY, 1)]))).is_none());
        let rec = rig.one(event(T0 + 3 * S, T0, h(&[(2.0, 3), (f64::INFINITY, 1)])));
        assert_eq!(hist_of(rec.as_ref().unwrap()).buckets, [(2.0, 1), (f64::INFINITY, 0)]);
        let dropped = rig.sum("logit.transform.metrics.dropped", &[("reason", "layout_changed")]);
        assert_eq!(dropped, 1.0);
        // Fewer observations in total is a reset, whatever the bounds did.
        let rec = rig.one(event(T0 + 4 * S, T0, h(&[(9.0, 1)])));
        assert_eq!(hist_of(rec.as_ref().expect("a reset emits")).buckets, [(9.0, 1)]);
        assert_eq!(rig.sum("logit.transform.series.reset", &[("cause", "decrease")]), 1.0);
    }

    /// One bucket going down is a reset, even when the total went up.
    #[test]
    fn one_histogram_bucket_going_down_is_a_reset() {
        let mut rig = Rig::new(InitialValue::Drop);
        let h = |b: &[(f64, u64)]| hist(b, None, 1.0, 1.0);
        rig.one(event(T0 + S, T0, h(&[(1.0, 5), (f64::INFINITY, 0)])));
        let rec = rig.one(event(T0 + 2 * S, T0, h(&[(1.0, 1), (f64::INFINITY, 9)])));
        assert_eq!(hist_of(rec.as_ref().unwrap()).buckets, [(1.0, 1), (f64::INFINITY, 9)]);
        assert_eq!(rig.sum("logit.transform.series.reset", &[("cause", "decrease")]), 1.0);
    }

    // ---- Exponential histograms -----------------------------------------------------------------

    /// Buckets are aligned by absolute index, so a moved offset differences correctly.
    #[test]
    fn an_exponential_histogram_differences_by_absolute_index() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + S, T0, exp(0, (1, 0.0), (2, &[3, 4]), 8)));
        let rec = rig.one(event(T0 + 2 * S, T0, exp(0, (2, 0.0), (1, &[1, 3, 6]), 12)));
        let e = exp_of(rec.as_ref().expect("differenced"));
        assert_eq!(e.positive, (1, vec![1, 0, 2]));
        assert_eq!((e.zero_count, e.count, e.sum), (1, 4, Some(4.0)));
        assert_eq!(e.temporality, Temporality::Delta);
    }

    /// A lower scale merges the baseline's buckets before differencing.
    #[test]
    fn an_exponential_histogram_whose_scale_went_down_downscales_the_baseline() {
        let mut rig = Rig::new(InitialValue::Drop);
        // Scale 1, indices -1..=2: at scale 0 they are -1, 0, 0, 1 (an arithmetic shift).
        rig.one(event(T0 + S, T0, exp(1, (0, 0.0), (-1, &[1, 2, 3, 4]), 10)));
        let rec = rig.one(event(T0 + 2 * S, T0, exp(0, (0, 0.0), (-1, &[2, 6, 5]), 13)));
        let e = exp_of(rec.as_ref().expect("differenced"));
        assert_eq!(e.positive, (-1, vec![1, 1, 1]));
        assert_eq!(e.count, 3);
        assert_eq!(rig.sum("logit.transform.metrics.dropped", &[]), 0.0);
        assert_eq!(rig.sum("logit.transform.series.reset", &[]), 0.0);
    }

    /// A wider zero threshold folds the baseline's low buckets into its zero count first.
    #[test]
    fn an_exponential_histogram_whose_zero_threshold_widened_folds_the_baseline() {
        let mut rig = Rig::new(InitialValue::Drop);
        // Scale 0: bucket 0 is (1, 2], bucket 1 is (2, 4].
        rig.one(event(T0 + S, T0, exp(0, (1, 0.0), (0, &[2, 3]), 6)));
        let rec = rig.one(event(T0 + 2 * S, T0, exp(0, (4, 2.0), (0, &[0, 5]), 9)));
        let e = exp_of(rec.as_ref().expect("differenced"));
        assert_eq!(e.zero_count, 1, "4 now, against 1 + the 2 folded from bucket 0");
        assert_eq!(e.positive, (0, vec![0, 2]));
        assert_eq!(e.count, 3);
    }

    /// A finer scale or a narrower zero threshold can't be differenced: dropped and rebased.
    #[test]
    fn an_exponential_histogram_whose_scale_went_up_is_dropped() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + S, T0, exp(0, (0, 1.0), (0, &[2]), 2)));
        assert!(rig.one(event(T0 + 2 * S, T0, exp(1, (0, 1.0), (0, &[1, 2]), 3))).is_none());
        assert!(rig.one(event(T0 + 3 * S, T0, exp(1, (0, 0.5), (0, &[1, 3]), 4))).is_none());
        let rec = rig.one(event(T0 + 4 * S, T0, exp(1, (0, 0.5), (0, &[2, 3]), 5)));
        assert_eq!(exp_of(rec.as_ref().unwrap()).positive, (0, vec![1, 0]));
        let dropped = rig.sum("logit.transform.metrics.dropped", &[("reason", "layout_changed")]);
        assert_eq!(dropped, 2.0);
    }

    // ---- Summaries ------------------------------------------------------------------------------

    #[test]
    fn a_summary_differences_its_count_and_sum_and_keeps_its_quantiles() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + S, T0, summary(10, 20.0)));
        let rec = rig.one(event(T0 + 2 * S, T0, summary(14, 29.0))).expect("differenced");
        let MetricKind::Summary(s) = &rec.kind else { panic!("expected a Summary") };
        assert_eq!((s.count, s.sum, s.temporality), (4, 9.0, Temporality::Delta));
        assert_eq!(s.quantiles, [(0.5, 2.0), (0.99, 9.0)]);
        let rec = rig.one(event(T0 + 3 * S, T0, summary(2, 3.0))).expect("a reset emits");
        let MetricKind::Summary(s) = &rec.kind else { panic!("expected a Summary") };
        assert_eq!((s.count, s.sum), (2, 3.0));
        let converted = rig.sum("logit.transform.metrics.converted", &[("metric_kind", "summary")]);
        assert_eq!(converted, 2.0);
    }

    // ---- What passes through --------------------------------------------------------------------

    /// Every kind the transform doesn't convert passes untouched and tracks no series.
    #[test]
    fn unconverted_kinds_pass_through_untouched() {
        let mut rig = Rig::new(InitialValue::Drop);
        let mut flagged = MetricRecord::new(intern("m"), cum(1.0));
        flagged.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let records = [
            MetricRecord::new(intern("g"), MetricKind::gauge(1.0)),
            MetricRecord::new(intern("d"), MetricKind::counter(1.0)),
            MetricRecord::new(
                intern("u"),
                MetricKind::Sum(Sum {
                    value: MetricValue::F64(-3.0),
                    temporality: Temporality::Cumulative,
                    monotonic: false,
                }),
            ),
            flagged,
        ];
        for rec in records {
            let out = rig.one(Event::metric(T0, AttrMap::new(), rec.clone()));
            assert_eq!(out.as_ref(), Some(&rec));
        }
        assert_eq!(rig.t.live, 0);
        assert_eq!(rig.probe.gauge("logit.transform.series.tracked", &[]), None);
    }

    /// A non-finite cumulative sum is dropped and counted, and the baseline is unchanged.
    #[test]
    fn a_non_finite_sum_is_dropped_and_the_baseline_kept() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + S, T0, cum(5.0)));
        assert!(rig.one(event(T0 + 2 * S, T0, cum(f64::INFINITY))).is_none());
        let rec = rig.one(event(T0 + 3 * S, T0, cum(6.0))).expect("differenced");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(1.0));
        let dropped = rig.sum("logit.transform.metrics.dropped", &[("reason", "non_finite")]);
        assert_eq!(dropped, 1.0);
    }

    /// An event left with nothing is discarded; one that still carries a log is kept.
    #[test]
    fn an_event_left_empty_is_discarded() {
        let mut rig = Rig::new(InitialValue::Drop);
        let mut with_log = event(T0, T0, cum(1.0));
        with_log.attributes.insert("other", "series");
        with_log.log = Some(LogRecord {
            message: Value::str("msg"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        let out = rig.run(vec![event(T0, T0, cum(1.0)), with_log]);
        assert_eq!(out.len(), 1);
        assert!(out[0].log.is_some() && out[0].metrics.is_empty());
        assert_eq!(rig.sum("logit.component.events.discarded", &[]), 1.0);
    }

    // ---- The series key -------------------------------------------------------------------------

    /// The resource, the scope, and the event's attributes each separate series.
    #[test]
    fn the_series_key_includes_the_resource_scope_and_attributes() {
        let mut rig = Rig::new(InitialValue::Drop);
        rig.one(event(T0 + S, T0, cum(10.0)));
        rig.resource = resource_with("host.name", "b");
        assert!(rig.one(event(T0 + 2 * S, T0, cum(50.0))).is_none(), "another resource");
        rig.scope = Some(Arc::new(Scope { name: "lib".into(), ..Scope::default() }));
        assert!(rig.one(event(T0 + 3 * S, T0, cum(70.0))).is_none(), "another scope");
        let mut attributed = event(T0 + 4 * S, T0, cum(90.0));
        attributed.attributes.insert("route", "/a");
        assert!(rig.one(attributed).is_none(), "other attributes");
        assert_eq!(rig.sum("logit.transform.metrics.skipped", &[("reason", "initial")]), 4.0);
        // Back on the first series, a key-equal resource in a new `Arc` finds it.
        rig.resource = Arc::new(Resource::default());
        rig.scope = None;
        let rec = rig.one(event(T0 + 5 * S, T0, cum(12.0))).expect("differenced");
        assert_eq!(sum_of(&rec).0, MetricValue::F64(2.0));
        assert_eq!(rig.probe.gauge("logit.transform.series.tracked", &[]), Some(4.0));
    }

    // ---- Bounds ---------------------------------------------------------------------------------

    /// A series not updated for longer than `max_staleness` by the wall clock is evicted, and
    /// its next point is a first point.
    #[test]
    fn an_idle_series_is_evicted_by_the_wall_clock() {
        let mut rig = Rig::bounded(InitialValue::Drop, Duration::from_secs(60), 10);
        rig.one(event(T0 + S, T0, cum(10.0)));
        // `max_staleness` later it's still tracked.
        rig.clock.store(T0 + 60 * S, Ordering::SeqCst);
        assert!(rig.one(event(T0 + 2 * S, T0, cum(11.0))).is_some());
        rig.clock.store(T0 + 121 * S, Ordering::SeqCst);
        assert!(rig.one(event(T0 + 3 * S, T0, cum(12.0))).is_none(), "a first point again");
        assert_eq!(rig.sum("logit.transform.series.evicted", &[("reason", "idle")]), 1.0);
        assert_eq!(rig.sum("logit.transform.metrics.skipped", &[("reason", "initial")]), 2.0);
    }

    /// A new series past `max_series` evicts the least recently updated one.
    #[test]
    fn a_series_past_max_series_evicts_the_least_recently_updated() {
        let mut rig = Rig::bounded(InitialValue::Drop, Duration::from_secs(3600), 2);
        let named = |name: &str, ts: i64, v: f64| {
            let mut e = event(ts, T0, cum(v));
            e.attributes.insert("series", name);
            e
        };
        rig.one(named("a", T0 + S, 1.0));
        rig.one(named("b", T0 + S, 1.0));
        assert!(rig.one(named("a", T0 + 2 * S, 2.0)).is_some(), "`a` is now the newer");
        rig.one(named("c", T0 + S, 1.0));
        let evicted = rig.sum("logit.transform.series.evicted", &[("reason", "cardinality")]);
        assert_eq!(evicted, 1.0);
        assert!(rig.one(named("a", T0 + 3 * S, 3.0)).is_some(), "`a` survived");
        assert!(rig.one(named("b", T0 + 3 * S, 3.0)).is_none(), "`b` was evicted");
        assert_eq!(rig.t.live, 2);
    }
}
