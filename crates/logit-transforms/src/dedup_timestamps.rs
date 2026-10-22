//! `dedup_timestamps`: collapses the metric records in one batch that share a series and a
//! timestamp bucket into one record, for a sink whose destination can't hold two readings of one
//! series at one instant (`docs/adr/dedup-timestamps-transform.md`, the canonical statement of
//! every rule below).
//!
//! **The series key** is the record's name, unit, and kind (temporality and monotonicity
//! included), the batch's resource and scope, and the event's attributes, plus
//! `timestamp.div_euclid(precision)`. Resource and scope are the batch's, so within one batch
//! they're equal for every record and aren't hashed or compared.
//!
//! **Which record survives.** In a colliding set, a record that can't merge on its own passes
//! through, counted `passed_through{reason}`: a `GaugeDelta`, a delta `Summary`, or a delta `Sum`
//! or a Datadog rate of `Bool`s, whose sum is a count of another type (`no_merge_rule`); a flagged
//! `NO_RECORDED_VALUE` record of a delta or observation kind (`no_recorded_value`); and a
//! non-finite delta value or histogram `sum` (`non_finite`). The survivor is the
//! latest-timestamp record among the rest, the later in batch order on a tie. The rest of the set
//! folds into it in batch order:
//!
//! - a `Gauge` or a cumulative kind, flagged or not, is superseded (`absorbed`), except a Datadog
//!   `rate` ([`logit_proto::datadog::is_rate`]), an increment over its interval, which sums as a
//!   delta `Sum` does when its interval equals the survivor's, and otherwise passes through
//!   (`no_merge_rule`), as does a rate beside a gauge that isn't one;
//! - a delta `Sum`, `Histogram`, or `ExponentialHistogram`, `Samples`, `SetMembers`,
//!   `Distribution`, or `Set` is merged (`merged`), unless its value type, bucket layout, or
//!   sample rate differs from the survivor's (`mixed_type`, `layout_mismatch`) or its addition
//!   would overflow (`overflow`), in which case it passes through.
//!
//! A merge never changes a value's type, and a count that would overflow is never saturated. The
//! histogram `sum`/`min`/`max` rules are `aggregate`'s ([`fold_extreme`]). A merged record keeps
//! the union of the set's exemplars in batch order, the earliest nonzero `start_timestamp`, the
//! survivor's `flags`, and the survivor's `description`, else the first present.
//!
//! **Passed-through counting.** Each record passed through beside a survivor counts once. When no
//! record of a set can merge, every one but the first counts, so a passed-through pair counts
//! once.
//!
//! **Cost.** One hash and one table probe per metric record; only a collision runs the second
//! pass. The table and the record list are cleared per batch and keep their capacity, so a batch
//! with no collision allocates nothing once they've grown. The runtime counts
//! `logit.component.events.discarded` for an event this leaves with nothing.

use crate::aggregate::{
    attr_map_key_eq, bucket_bounds_match, fold_extreme, hash_attr_map, observed,
};
use bytes::Bytes;
use logit_core::{
    Event, ExpHistogram, Histogram, MetricKind, MetricRecord, MetricValue, Resource, Scope,
    Telemetry, Temporality, Value,
};
use logit_pipeline::Transform;
use logit_proto::datadog;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// Why a colliding record passed through, in `passed_through`'s `reason` tag order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    NonFinite,
    Overflow,
    MixedType,
    LayoutMismatch,
    NoMergeRule,
    NoRecordedValue,
}

impl Reason {
    const ALL: [Reason; 6] = [
        Reason::NonFinite,
        Reason::Overflow,
        Reason::MixedType,
        Reason::LayoutMismatch,
        Reason::NoMergeRule,
        Reason::NoRecordedValue,
    ];

    fn name(self) -> &'static str {
        match self {
            Reason::NonFinite => "non_finite",
            Reason::Overflow => "overflow",
            Reason::MixedType => "mixed_type",
            Reason::LayoutMismatch => "layout_mismatch",
            Reason::NoMergeRule => "no_merge_rule",
            Reason::NoRecordedValue => "no_recorded_value",
        }
    }
}

/// How a kind takes part in a colliding set. The key includes the kind, so one set has one class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    /// `Gauge` and the cumulative kinds: the later record replaces the earlier.
    LastWriteWins,
    /// Delta `Sum`, `Histogram`, and `ExponentialHistogram`: increments, added.
    Delta,
    /// `Samples`, `SetMembers`, `Distribution`, and `Set`: observations, combined.
    Observation,
    /// `GaugeDelta` and a delta `Summary`.
    NoMergeRule,
}

fn class(kind: &MetricKind) -> Class {
    let delta = |t: Temporality| t == Temporality::Delta;
    match kind {
        MetricKind::Gauge(_) => Class::LastWriteWins,
        MetricKind::Sum(s) if delta(s.temporality) => Class::Delta,
        MetricKind::Histogram(h) if delta(h.temporality) => Class::Delta,
        MetricKind::ExponentialHistogram(e) if delta(e.temporality) => Class::Delta,
        MetricKind::Summary(s) if delta(s.temporality) => Class::NoMergeRule,
        MetricKind::Sum(_)
        | MetricKind::Histogram(_)
        | MetricKind::ExponentialHistogram(_)
        | MetricKind::Summary(_) => Class::LastWriteWins,
        MetricKind::GaugeDelta(_) => Class::NoMergeRule,
        MetricKind::Samples(_)
        | MetricKind::SetMembers(_)
        | MetricKind::Distribution(_)
        | MetricKind::Set(_) => Class::Observation,
    }
}

/// The kind's part of the series key: the variant, temporality, and monotonicity.
fn kind_key(kind: &MetricKind) -> (u8, u8, bool) {
    let t = |t: Temporality| match t {
        Temporality::Delta => 1,
        Temporality::Cumulative => 2,
    };
    match kind {
        MetricKind::Sum(s) => (0, t(s.temporality), s.monotonic),
        MetricKind::Gauge(_) => (1, 0, false),
        MetricKind::GaugeDelta(_) => (2, 0, false),
        MetricKind::Samples(_) => (3, 0, false),
        MetricKind::Distribution(_) => (4, 0, false),
        MetricKind::SetMembers(_) => (5, 0, false),
        MetricKind::Set(_) => (6, 0, false),
        MetricKind::Histogram(h) => (7, t(h.temporality), false),
        MetricKind::ExponentialHistogram(e) => (8, t(e.temporality), false),
        MetricKind::Summary(s) => (9, t(s.temporality), false),
    }
}

/// Whether a record can be merged or superseded at all, before it's compared with a survivor. A
/// Datadog `rate` (`rate`) is an increment, so it takes the delta rules.
fn alone(record: &MetricRecord, rate: bool) -> Result<Class, Reason> {
    let class = if rate { Class::Delta } else { class(&record.kind) };
    let finite = |v: Option<f64>| v.is_none_or(f64::is_finite);
    match class {
        Class::LastWriteWins => Ok(class),
        Class::NoMergeRule => Err(Reason::NoMergeRule),
        _ if record.is_no_recorded_value() => Err(Reason::NoRecordedValue),
        Class::Observation => Ok(class),
        Class::Delta => match &record.kind {
            // A `Bool + Bool` sum is a count, an `I64`: no merge keeps the type.
            MetricKind::Sum(s) if matches!(s.value, MetricValue::Bool(_)) => {
                Err(Reason::NoMergeRule)
            }
            MetricKind::Sum(s) if !s.value.is_finite() => Err(Reason::NonFinite),
            MetricKind::Gauge(MetricValue::Bool(_)) => Err(Reason::NoMergeRule),
            MetricKind::Gauge(v) if !v.is_finite() => Err(Reason::NonFinite),
            MetricKind::Histogram(h) if !finite(h.sum) => Err(Reason::NonFinite),
            MetricKind::ExponentialHistogram(e) if !finite(e.sum) => Err(Reason::NonFinite),
            _ => Ok(class),
        },
    }
}

/// `held + incoming` in `held`'s type, or why not.
fn add_value(held: MetricValue, incoming: MetricValue) -> Result<MetricValue, Reason> {
    match (held, incoming) {
        (MetricValue::I64(a), MetricValue::I64(b)) => {
            a.checked_add(b).map(MetricValue::I64).ok_or(Reason::Overflow)
        }
        (MetricValue::U64(a), MetricValue::U64(b)) => {
            a.checked_add(b).map(MetricValue::U64).ok_or(Reason::Overflow)
        }
        (MetricValue::F64(a), MetricValue::F64(b)) => {
            Some(a + b).filter(|v| v.is_finite()).map(MetricValue::F64).ok_or(Reason::Overflow)
        }
        _ => Err(Reason::MixedType),
    }
}

/// A histogram `sum` under `aggregate`'s rule: `None` once a side lacks one. A finite sum leaving
/// the finite range is an overflow.
fn add_sum(held: Option<f64>, incoming: Option<f64>) -> Result<Option<f64>, Reason> {
    match (held, incoming) {
        (Some(a), Some(b)) if (a + b).is_finite() => Ok(Some(a + b)),
        (Some(_), Some(_)) => Err(Reason::Overflow),
        _ => Ok(None),
    }
}

fn merge_histogram(held: &mut Histogram, incoming: &Histogram) -> Result<(), Reason> {
    if !bucket_bounds_match(&held.buckets, &incoming.buckets) {
        return Err(Reason::LayoutMismatch);
    }
    let fits =
        held.buckets.iter().zip(&incoming.buckets).all(|(a, b)| a.1.checked_add(b.1).is_some());
    if !fits {
        return Err(Reason::Overflow);
    }
    let sum = add_sum(held.sum, incoming.sum)?;
    let (held_observed, incoming_observed) = (observed(&held.buckets), observed(&incoming.buckets));
    for (a, b) in held.buckets.iter_mut().zip(&incoming.buckets) {
        a.1 += b.1;
    }
    held.sum = sum;
    held.min = fold_extreme((held_observed, held.min), (incoming_observed, incoming.min), f64::min);
    held.max = fold_extreme((held_observed, held.max), (incoming_observed, incoming.max), f64::max);
    Ok(())
}

/// One side of an exponential histogram: `(offset, counts)`, `counts[i]` at index `offset + i`.
type ExpSide = (i32, Vec<u64>);

fn side_get(side: &ExpSide, idx: i64) -> u64 {
    usize::try_from(idx - i64::from(side.0)).ok().and_then(|i| side.1.get(i).copied()).unwrap_or(0)
}

/// The index range `[lo, hi)` both sides cover, or `None` when both are empty.
fn side_span(a: &ExpSide, b: &ExpSide) -> Option<(i64, i64)> {
    let range = |s: &ExpSide| {
        (!s.1.is_empty()).then(|| (i64::from(s.0), i64::from(s.0) + s.1.len() as i64))
    };
    match (range(a), range(b)) {
        (None, None) => None,
        (Some(r), None) | (None, Some(r)) => Some(r),
        (Some((alo, ahi)), Some((blo, bhi))) => Some((alo.min(blo), ahi.max(bhi))),
    }
}

/// `held + incoming` by absolute bucket index, or `None` on an overflow.
fn add_side(held: &ExpSide, incoming: &ExpSide) -> Option<ExpSide> {
    let Some((lo, hi)) = side_span(held, incoming) else { return Some((held.0, Vec::new())) };
    let offset = i32::try_from(lo).ok()?;
    let counts = (lo..hi)
        .map(|i| side_get(held, i).checked_add(side_get(incoming, i)))
        .collect::<Option<Vec<u64>>>()?;
    Some((offset, counts))
}

fn exp_observed(e: &ExpHistogram) -> bool {
    e.zero_count > 0 || e.positive.1.iter().any(|c| *c > 0) || e.negative.1.iter().any(|c| *c > 0)
}

fn merge_exponential(held: &mut ExpHistogram, incoming: &ExpHistogram) -> Result<(), Reason> {
    if held.scale != incoming.scale
        || held.zero_threshold.to_bits() != incoming.zero_threshold.to_bits()
    {
        return Err(Reason::LayoutMismatch);
    }
    let zero_count = held.zero_count.checked_add(incoming.zero_count).ok_or(Reason::Overflow)?;
    let count = held.count.checked_add(incoming.count).ok_or(Reason::Overflow)?;
    let positive = add_side(&held.positive, &incoming.positive).ok_or(Reason::Overflow)?;
    let negative = add_side(&held.negative, &incoming.negative).ok_or(Reason::Overflow)?;
    let sum = add_sum(held.sum, incoming.sum)?;
    let (held_observed, incoming_observed) = (exp_observed(held), exp_observed(incoming));
    held.min = fold_extreme((held_observed, held.min), (incoming_observed, incoming.min), f64::min);
    held.max = fold_extreme((held_observed, held.max), (incoming_observed, incoming.max), f64::max);
    held.zero_count = zero_count;
    held.count = count;
    held.positive = positive;
    held.negative = negative;
    held.sum = sum;
    Ok(())
}

/// What the records before the survivor in batch order contribute to the kinds that keep an
/// order, put in front of the survivor's once the set is folded.
#[derive(Default)]
struct Prefix {
    values: Vec<f64>,
    members: Vec<Bytes>,
}

/// Merges `incoming` into `held`, the survivor's running value, or says why it can't, leaving
/// `held` unchanged. A `Samples` or `SetMembers` record before the survivor in batch order goes
/// into `prefix` instead.
fn merge(
    held: &mut MetricKind,
    incoming: &mut MetricKind,
    before: bool,
    prefix: &mut Prefix,
) -> Result<(), Reason> {
    match (held, incoming) {
        (MetricKind::Sum(a), MetricKind::Sum(b)) => {
            a.value = add_value(a.value, b.value)?;
            Ok(())
        }
        // Only a Datadog `rate` reaches a merge as a `Gauge`; any other gauge is last write wins.
        (MetricKind::Gauge(a), MetricKind::Gauge(b)) => {
            *a = add_value(*a, *b)?;
            Ok(())
        }
        (MetricKind::Histogram(a), MetricKind::Histogram(b)) => merge_histogram(a, b),
        (MetricKind::ExponentialHistogram(a), MetricKind::ExponentialHistogram(b)) => {
            merge_exponential(a, b)
        }
        (MetricKind::Samples(a), MetricKind::Samples(b)) => {
            if a.sample_rate.to_bits() != b.sample_rate.to_bits() {
                return Err(Reason::LayoutMismatch);
            }
            if before {
                prefix.values.extend(b.values.drain(..));
            } else {
                a.values.extend(b.values.drain(..));
            }
            Ok(())
        }
        (MetricKind::SetMembers(a), MetricKind::SetMembers(b)) => {
            if before {
                prefix.members.append(b);
            } else {
                a.append(b);
            }
            Ok(())
        }
        (MetricKind::Distribution(a), MetricKind::Distribution(b)) => {
            a.merge(b);
            Ok(())
        }
        (MetricKind::Set(a), MetricKind::Set(b)) => {
            a.merge(b);
            Ok(())
        }
        _ => unreachable!("a colliding set holds one kind"),
    }
}

/// Puts `prefix` in front of the survivor's ordered values, and keeps each set member once.
fn apply_prefix(kind: &mut MetricKind, prefix: Prefix) {
    match kind {
        MetricKind::Samples(s) if !prefix.values.is_empty() => {
            let mut values: smallvec::SmallVec<_> = prefix.values.into_iter().collect();
            values.extend(s.values.drain(..));
            s.values = values;
        }
        MetricKind::SetMembers(members) => {
            let mut all = prefix.members;
            all.append(members);
            let mut seen = HashSet::with_capacity(all.len());
            all.retain(|m| seen.insert(m.clone()));
            *members = all;
        }
        _ => {}
    }
}

/// A record's Datadog `rate` carrier: whether it's a rate, and the interval of one that is.
type Rate = (bool, Option<Value>);

/// One keyed record of the batch: `(key hash, event index, record index)`.
type Entry = (u64, u32, u32);

/// One batch's tallies.
#[derive(Default)]
struct Tally {
    merged: u64,
    absorbed: u64,
    passed: [u64; 6],
}

/// The `dedup_timestamps` transform. See the module doc.
pub struct DedupTimestamps {
    precision: i64,
    /// Key hash to how many records carry it, cleared per batch.
    table: HashMap<u64, u32>,
    entries: Vec<Entry>,
    /// `(event, record)` pairs to remove, filled only on a collision.
    removed: Vec<(u32, u32)>,
    tally: Tally,
    telemetry: Telemetry,
}

impl DedupTimestamps {
    /// `precision` must be nonzero (graph rule 96); a zero one is read as one nanosecond.
    pub fn new(precision: Duration) -> Self {
        Self {
            precision: i64::try_from(precision.as_nanos()).unwrap_or(i64::MAX).max(1),
            table: HashMap::new(),
            entries: Vec::new(),
            removed: Vec::new(),
            tally: Tally::default(),
            telemetry: Telemetry::default(),
        }
    }

    /// Counts the ADR's telemetry into `telemetry`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    fn key_hash(&self, event: &Event, record: &MetricRecord) -> u64 {
        let mut state = DefaultHasher::new();
        event.timestamp.div_euclid(self.precision).hash(&mut state);
        record.name.hash(&mut state);
        record.unit.hash(&mut state);
        kind_key(&record.kind).hash(&mut state);
        hash_attr_map(&event.attributes, &mut state);
        state.finish()
    }

    fn same_key(&self, events: &[Event], a: Entry, b: Entry) -> bool {
        let (ea, eb) = (&events[a.1 as usize], &events[b.1 as usize]);
        let (ra, rb) = (&ea.metrics[a.2 as usize], &eb.metrics[b.2 as usize]);
        ea.timestamp.div_euclid(self.precision) == eb.timestamp.div_euclid(self.precision)
            && ra.name == rb.name
            && ra.unit == rb.unit
            && kind_key(&ra.kind) == kind_key(&rb.kind)
            && attr_map_key_eq(&ea.attributes, &eb.attributes)
    }

    /// Resolves one colliding set, its entries in batch order.
    fn resolve(&mut self, resource: &Resource, events: &mut [Event], set: &[Entry]) {
        // Each entry's verdict beside its Datadog `rate` carrier, read the way `datadog_out` reads
        // it: whether it's a rate, and its interval. One vector, so a collision allocates once.
        let verdicts: Vec<(Result<Class, Reason>, Rate)> = set
            .iter()
            .map(|e| {
                let event = &events[e.1 as usize];
                let record = &event.metrics[e.2 as usize];
                let rate = datadog::is_rate(resource, event, &record.kind);
                let interval = rate.then(|| datadog::interval(resource, event).cloned()).flatten();
                (alone(record, rate), (rate, interval))
            })
            .collect();
        // The latest timestamp among the records that can merge, the later on a tie.
        let survivor = set
            .iter()
            .zip(&verdicts)
            .enumerate()
            .filter(|(_, (_, v))| v.0.is_ok())
            .max_by_key(|(i, (e, _))| (events[e.1 as usize].timestamp, *i))
            .map(|(i, _)| i);
        let Some(s) = survivor else {
            for v in verdicts.iter().skip(1) {
                if let Err(reason) = &v.0 {
                    self.tally.passed[*reason as usize] += 1;
                }
            }
            return;
        };
        let class = verdicts[s].0.expect("the survivor can merge");
        let (sev, srec) = (set[s].1 as usize, set[s].2 as usize);
        let slot = &mut events[sev].metrics[srec];
        let placeholder = MetricRecord::new(slot.name, MetricKind::Gauge(MetricValue::F64(0.0)));
        let mut acc = std::mem::replace(slot, placeholder);
        let mut earlier_exemplars = Vec::new();
        let mut prefix = Prefix::default();
        let mut start = acc.start_timestamp;
        let mut description = acc.description;
        for (i, (entry, (verdict, rate))) in set.iter().zip(&verdicts).enumerate() {
            if i == s {
                continue;
            }
            if let Err(reason) = verdict {
                self.tally.passed[*reason as usize] += 1;
                continue;
            }
            // A rate sums only with a rate of its interval, and a gauge that isn't one never
            // folds into a rate or takes one in. The series key holds the carriers, so a set
            // doesn't mix them today; this keeps the rule if the key changes.
            if *rate != verdicts[s].1 {
                self.tally.passed[Reason::NoMergeRule as usize] += 1;
                continue;
            }
            let other = &mut events[entry.1 as usize].metrics[entry.2 as usize];
            let before = i < s;
            if class == Class::LastWriteWins {
                self.tally.absorbed += 1;
            } else {
                if let Err(reason) = merge(&mut acc.kind, &mut other.kind, before, &mut prefix) {
                    self.tally.passed[reason as usize] += 1;
                    continue;
                }
                self.tally.merged += 1;
                if other.start_timestamp != 0 && (start == 0 || other.start_timestamp < start) {
                    start = other.start_timestamp;
                }
            }
            if description.is_none() {
                description = other.description;
            }
            if before {
                earlier_exemplars.append(&mut other.exemplars);
            } else {
                acc.exemplars.append(&mut other.exemplars);
            }
            self.removed.push((entry.1, entry.2));
        }
        if !earlier_exemplars.is_empty() {
            earlier_exemplars.append(&mut acc.exemplars);
            acc.exemplars = earlier_exemplars;
        }
        apply_prefix(&mut acc.kind, prefix);
        acc.start_timestamp = start;
        acc.description = description;
        events[sev].metrics[srec] = acc;
    }

    fn report(&mut self) {
        let t = std::mem::take(&mut self.tally);
        let tel = &self.telemetry;
        if t.merged > 0 {
            tel.count("logit.transform.metrics.merged", t.merged as f64, &[]);
        }
        if t.absorbed > 0 {
            tel.count("logit.transform.metrics.absorbed", t.absorbed as f64, &[]);
        }
        for reason in Reason::ALL {
            let n = t.passed[reason as usize];
            if n > 0 {
                tel.count(
                    "logit.transform.metrics.passed_through",
                    n as f64,
                    &[("reason", reason.name())],
                );
            }
        }
    }
}

impl Transform for DedupTimestamps {
    fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
        true
    }

    fn begin_batch(
        &mut self,
        resource: &Arc<Resource>,
        _scope: Option<&Arc<Scope>>,
        events: &mut Vec<Event>,
    ) {
        self.table.clear();
        self.entries.clear();
        let mut collided = false;
        for (ev, event) in events.iter().enumerate() {
            for (rec, record) in event.metrics.iter().enumerate() {
                let hash = self.key_hash(event, record);
                let n = self.table.entry(hash).or_insert(0);
                *n += 1;
                collided |= *n > 1;
                self.entries.push((hash, ev as u32, rec as u32));
            }
        }
        if !collided {
            return;
        }

        let table = &self.table;
        let mut candidates: Vec<Entry> =
            self.entries.iter().copied().filter(|e| table[&e.0] > 1).collect();
        // By hash, then batch order within it.
        candidates.sort_unstable();
        let mut set = Vec::new();
        for run in candidates.chunk_by(|a, b| a.0 == b.0) {
            // A hash collision between two keys splits into one set per key.
            let mut taken = vec![false; run.len()];
            for i in 0..run.len() {
                if taken[i] {
                    continue;
                }
                set.clear();
                for j in i..run.len() {
                    if !taken[j] && self.same_key(events, run[i], run[j]) {
                        taken[j] = true;
                        set.push(run[j]);
                    }
                }
                if set.len() > 1 {
                    self.resolve(resource, events, &set);
                }
            }
        }

        if !self.removed.is_empty() {
            // Descending, so each removal leaves the indices still to remove in place.
            self.removed.sort_unstable_by(|a, b| b.cmp(a));
            let mut emptied: Vec<u32> = Vec::new();
            for &(ev, rec) in &self.removed {
                let event = &mut events[ev as usize];
                event.metrics.remove(rec as usize);
                if event.metrics.is_empty()
                    && event.log.is_none()
                    && event.span.is_none()
                    && emptied.last() != Some(&ev)
                {
                    emptied.push(ev);
                }
            }
            self.removed.clear();
            if !emptied.is_empty() {
                emptied.reverse();
                let mut next = emptied.iter().peekable();
                let mut index = 0u32;
                events.retain(|_| {
                    let remove = next.peek().is_some_and(|&&e| e == index);
                    if remove {
                        next.next();
                    }
                    index += 1;
                    !remove
                });
            }
        }
        self.report();
    }
}

#[cfg(test)]
mod tests;
