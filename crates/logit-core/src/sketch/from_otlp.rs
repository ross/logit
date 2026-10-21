//! OTLP histograms to Agent sketches, bin for bin as the Datadog OpenTelemetry translator builds
//! them: `datadog-agent`'s `pkg/opentelemetry-mapping-go/otlp/metrics`, which the Collector's
//! `datadogexporter` runs, with `pkg/util/quantile` and `sketches-go` under it. The decision to
//! follow it is [ADR `datadog-otlp-metric-mapping`](../../../../docs/adr/datadog-otlp-metric-mapping.md).
//!
//! Both functions take a delta point. A cumulative point's caller differences it first, as the
//! translator's cache does; the translator drops a cumulative exponential histogram outright.
//!
//! - [`histogram_to_agent`] is `getSketchBuckets` under `histograms.mode: distributions`: each
//!   bucket goes through `quantile.Agent.InsertInterpolate`, which spreads its count linearly
//!   across the Agent keys between its bounds. An open-ended bucket, and a `(0, B]` one, is a
//!   point mass at its finite bound. The summary's `cnt`, `sum`, and `avg` are then the point's
//!   own, and `min`/`max` the outermost populated bounds, clamped by the point's `min`/`max`.
//! - [`exp_histogram_to_agent`] is `exponentialHistogramToDDSketch` followed by
//!   `quantile.ConvertDDSketchIntoSketch`: the buckets as a `sketches-go` DDSketch with
//!   γ = 2^(2^-scale), re-mapped onto the Agent's γ by `ChangeMapping`, which splits each bucket's
//!   count across the target bins it overlaps, then rounded to integer counts with a carried
//!   remainder.
//!
//! The Agent's store is reproduced too: `uint16` bins, a count past 65535 split across bins of one
//! key, and `trimLeft` folding the lowest bins once there are more than 4096. The result folds those
//! bins back into one count per key. Both functions refuse a point whose observations would pass
//! the Agent's cap of 4096 × 65535.
//!
//! Every floating-point step uses `super::gomath`, Go's own `math` as linux/amd64 runs it, since
//! a libm result one ulp away changes counts. `testdata/differential/datadog-sketch/` holds the
//! translator's own output for every case the tests check (its README has the corpus).
//!
//! Where the result differs from Go:
//!
//! - An absent `sum` is treated as a non-finite one: the sketch keeps the summary its insertion
//!   computed. The translator reads protobuf's default `0` and sends it, with an average of `0`;
//!   the ADR's "Histograms" section decides against writing a sum the producer never reported.
//! - `logit`'s [`Histogram`] carries no point `count`, so the count is the sum of the buckets, and
//!   a histogram with no buckets has no sketch. The translator would build one from the point's
//!   count alone.
//! - When every bin `trimLeft` folds away leaves a full overflow bin and the first bin it keeps
//!   overflows too, Go's `copy` overwrites that bin and the result reads one bin past the end of
//!   its buffer, whatever a pooled buffer held. The port keeps the bins the code means to keep.
//! - A `NaN` bound matches the translator on amd64 only. There `int(NaN)` is `math.MinInt64`, so
//!   the key clamps to 1, and the outermost populated bound of a point with no `min` or `max`
//!   becomes the summary's, `NaN` included. arm64's Go converts `NaN` to 0, so a Collector there
//!   keys it differently.

use super::gomath;
use super::{Bin, DdSketch, Mapping};
use crate::{ExpHistogram, Histogram};
use std::fmt;

/// An Agent sketch as the translator sends it: the bins and summary in a [`DdSketch`] under
/// [`Mapping::agent`], and the summary's `avg`, which a [`DdSketch`] doesn't keep. `avg` is
/// `sum / cnt` unless the point's sum was missing or non-finite, when the translator leaves the
/// average it accumulated bin by bin.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentSketch {
    pub sketch: DdSketch,
    pub avg: f64,
}

/// Why a point has no sketch, each the translator's own reason to drop it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversionError {
    /// Nothing to insert: every count is zero.
    Empty,
    /// More observations than the Agent's 4096 × 65535 cap.
    TooManyObservations,
    /// A bucket whose upper bound keys below its lower bound.
    NonMonotonicBounds,
    /// An exponential histogram the translator can't re-map: a scale whose base isn't above 1 or
    /// overflows, a bucket boundary that overflows, or a value past the Agent's highest key.
    Unconvertible,
}

impl fmt::Display for ConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ConversionError::Empty => "no observations to sketch",
            ConversionError::TooManyObservations => "too many observations for one sketch",
            ConversionError::NonMonotonicBounds => "non-monotonic bucket bounds",
            ConversionError::Unconvertible => "buckets outside the Agent sketch's range",
        })
    }
}

impl std::error::Error for ConversionError {}

/// `quantile.Default().MaxCount()`: the Agent's bin limit times a bin's `uint16` capacity.
pub const MAX_OBSERVATIONS: u64 = AGENT_BIN_LIMIT as u64 * MAX_BIN;

const AGENT_BIN_LIMIT: usize = Mapping::AGENT_BIN_LIMIT as usize;
const MAX_BIN: u64 = u16::MAX as u64;
const UV_INF: i32 = Mapping::AGENT_INF_KEY;
const MAX_KEY: i32 = Mapping::AGENT_MAX_KEY;

/// The Agent's `Config`, computed with Go's `math`.
struct AgentConfig {
    gamma_ln: f64,
    bias: i64,
    min: f64,
}

impl AgentConfig {
    fn new() -> Self {
        let agent = Mapping::agent();
        let gamma_ln = agent.gamma_ln;
        let emin = (gomath::log(Mapping::AGENT_MIN) / gamma_ln).floor() as i64;
        let bias = 1 - emin;
        AgentConfig { gamma_ln, bias, min: gomath::pow(Mapping::AGENT_GAMMA, (1 - bias) as f64) }
    }

    /// `Config.key`.
    fn key(&self, v: f64) -> i32 {
        if v < 0.0 {
            return -self.key(-v);
        }
        if v == 0.0 || (v > 0.0 && v < self.min) {
            return 0;
        }
        let i = gomath::to_int((gomath::log(v) / self.gamma_ln).round_ties_even())
            .wrapping_add(self.bias);
        if i > MAX_KEY as i64 {
            UV_INF
        } else if i < 1 {
            1
        } else {
            i as i32
        }
    }

    /// `Config.binLow`, which is also its `f64`.
    fn bin_low(&self, k: i32) -> f64 {
        if k < 0 {
            -self.bin_low(-k)
        } else if k == UV_INF {
            f64::INFINITY
        } else if k == 0 {
            0.0
        } else {
            gomath::pow(Mapping::AGENT_GAMMA, (k as i64 - self.bias) as f64)
        }
    }
}

/// `summary.Summary`.
#[derive(Clone, Copy, Default)]
struct Summary {
    min: f64,
    max: f64,
    sum: f64,
    avg: f64,
    cnt: i64,
}

impl Summary {
    fn insert_n(&mut self, v: f64, n: f64) {
        self.merge(Summary { cnt: n as i64, sum: n * v, min: v, max: v, avg: v });
    }

    fn merge(&mut self, o: Summary) {
        if self.cnt == 0 {
            *self = o;
            return;
        }
        if o.cnt == 0 {
            return;
        }
        if o.max > self.max {
            self.max = o.max;
        }
        if o.min < self.min {
            self.min = o.min;
        }
        self.cnt += o.cnt;
        self.sum += o.sum;
        self.avg += (o.avg - self.avg) * o.cnt as f64 / self.cnt as f64;
    }
}

/// `quantile.sparseStore`'s bin list: signed keys ascending, a key repeated when its count passes
/// `uint16`.
#[derive(Default)]
struct AgentStore {
    bins: Vec<(i32, u64)>,
}

/// `appendSafe`: the remainder first, then full bins.
fn append_safe(bins: &mut Vec<(i32, u64)>, k: i32, n: u64) {
    if n <= MAX_BIN {
        bins.push((k, n));
        return;
    }
    let r = n % MAX_BIN;
    if r != 0 {
        bins.push((k, r));
    }
    for _ in 0..n / MAX_BIN {
        bins.push((k, MAX_BIN));
    }
}

/// `trimLeft` at the Agent's 4096 bins: the lowest bins folded into the first one kept, any
/// overflow kept as full bins ahead of it.
fn trim_left(a: Vec<(i32, u64)>) -> Vec<(i32, u64)> {
    if a.len() <= AGENT_BIN_LIMIT {
        return a;
    }
    let remove = a.len() - AGENT_BIN_LIMIT;
    let mut missing = 0;
    let mut out = Vec::with_capacity(a.len());
    for bin in &a[..remove] {
        missing += bin.1;
        if missing > MAX_BIN {
            out.push((bin.0, MAX_BIN));
            missing -= MAX_BIN;
        }
    }
    let (key, n) = a[remove];
    let next = n + missing;
    let kept = if next > MAX_BIN {
        let kept = (key, MAX_BIN);
        append_safe(&mut out, key, next - MAX_BIN);
        kept
    } else {
        (key, next)
    };
    out.push(kept);
    out.extend_from_slice(&a[remove + 1..]);
    out
}

impl AgentStore {
    /// `sparseStore.insertCounts`. `kcs` holds each key once.
    fn insert_counts(&mut self, kcs: &mut [(i32, u64)]) {
        kcs.sort_by_key(|kc| kc.0);
        let mut tmp = Vec::with_capacity(self.bins.len() + kcs.len());
        let (mut s, mut k) = (0, 0);
        while s < self.bins.len() && k < kcs.len() {
            let b = self.bins[s];
            let (vk, kn) = kcs[k];
            match b.0.cmp(&vk) {
                std::cmp::Ordering::Less => {
                    tmp.push(b);
                    s += 1;
                }
                std::cmp::Ordering::Greater => {
                    append_safe(&mut tmp, vk, kn);
                    k += 1;
                }
                std::cmp::Ordering::Equal => {
                    append_safe(&mut tmp, b.0, b.1 + kn);
                    s += 1;
                    k += 1;
                }
            }
        }
        tmp.extend_from_slice(&self.bins[s..]);
        for &(vk, kn) in &kcs[k..] {
            append_safe(&mut tmp, vk, kn);
        }
        self.bins = trim_left(tmp);
    }

    /// The bins as a [`DdSketch`]'s stores, one count per key: positive keys, `|k|` of negative
    /// ones, and the zero bin.
    fn fold(&self) -> (Vec<Bin>, Vec<Bin>, f64) {
        let (mut positive, mut negative, mut zero) = (Vec::<Bin>::new(), Vec::<Bin>::new(), 0.0);
        for &(k, n) in &self.bins {
            let n = n as f64;
            let store = match k.cmp(&0) {
                std::cmp::Ordering::Equal => {
                    zero += n;
                    continue;
                }
                std::cmp::Ordering::Greater => &mut positive,
                std::cmp::Ordering::Less => &mut negative,
            };
            match store.last_mut() {
                Some(last) if last.key == k.abs() => last.count += n,
                _ => store.push(Bin { key: k.abs(), count: n }),
            }
        }
        negative.reverse();
        (positive, negative, zero)
    }
}

/// `quantile.Agent`, insert by interpolation only.
struct AgentBuilder {
    config: AgentConfig,
    store: AgentStore,
    basic: Summary,
}

impl AgentBuilder {
    /// `Agent.InsertInterpolate`.
    fn insert_interpolate(
        &mut self,
        lower: f64,
        upper: f64,
        count: u64,
    ) -> Result<(), ConversionError> {
        let clamp = |k: i32| match k {
            UV_INF => MAX_KEY,
            k if k == -UV_INF => -MAX_KEY,
            k => k,
        };
        let start = clamp(self.config.key(lower));
        let end = clamp(self.config.key(upper));
        if end < start {
            return Err(ConversionError::NonMonotonicBounds);
        }
        let mut whats_left = count as i64;
        let distance = upper - lower;
        let mut start_key = start;
        let mut lower_b = self.config.bin_low(start_key);
        let mut remainder = 0.0;
        let mut kcs = Vec::new();
        let mut end_key = start + 1;
        while end_key <= end && whats_left > 0 {
            let upper_b = self.config.bin_low(end_key);
            let fkn = ((upper_b - lower_b) / distance) * count as f64;
            if fkn > 1.0 {
                remainder += fkn - gomath::to_int(fkn) as f64;
            }
            let mut kn = gomath::to_int(fkn);
            if remainder > 1.0 {
                kn += 1;
                remainder -= 1.0;
            }
            if kn > 0 {
                kn = kn.min(whats_left);
                self.basic.insert_n(lower_b, kn as f64);
                kcs.push((start_key, kn as u64));
                whats_left -= kn;
                start_key = end_key;
                lower_b = upper_b;
            }
            end_key += 1;
        }
        if whats_left > 0 {
            self.basic.insert_n(self.config.bin_low(start_key), whats_left as f64);
            kcs.push((start_key, whats_left as u64));
        }
        if !kcs.is_empty() {
            self.store.insert_counts(&mut kcs);
        }
        Ok(())
    }
}

fn agent_sketch(store: &AgentStore, s: Summary, exact: bool) -> AgentSketch {
    let (positive, negative, zero_count) = store.fold();
    // Built directly, without `from_parts`'s collapse: `trimLeft` can keep a few more keys than
    // 4096, and collapsing them again would move counts the translator left in place.
    let sketch = DdSketch {
        mapping: Mapping::agent(),
        positive,
        negative,
        zero_count,
        count: s.cnt as f64,
        min: s.min,
        max: s.max,
        sum: s.sum,
        exact_stats: exact,
    };
    AgentSketch { sketch, avg: s.avg }
}

/// `h`'s buckets in OTLP's shape: `counts` one longer than `bounds`, the last bucket open-ended.
/// A last bucket with a finite bound has an empty one above it.
fn otlp_shape(h: &Histogram) -> (Vec<f64>, Vec<u64>) {
    let mut counts: Vec<u64> = h.buckets.iter().map(|b| b.1).collect();
    let mut bounds: Vec<f64> = h.buckets.iter().map(|b| b.0).collect();
    match bounds.last() {
        Some(b) if *b == f64::INFINITY => {
            bounds.pop();
        }
        Some(_) => counts.push(0),
        None => {}
    }
    (bounds, counts)
}

/// Each bucket of `h` as `(lower, upper, count)`, OTLP's `(lower, upper]` read as the translator's
/// `getBounds` reads it: the first bucket's lower bound is `-Inf` and the last one's upper `+Inf`.
pub fn otlp_buckets(h: &Histogram) -> Vec<(f64, f64, u64)> {
    let (bounds, counts) = otlp_shape(h);
    counts
        .iter()
        .enumerate()
        .map(|(j, &n)| {
            let lower = if j > 0 { bounds[j - 1] } else { f64::NEG_INFINITY };
            (lower, bounds.get(j).copied().unwrap_or(f64::INFINITY), n)
        })
        .collect()
}

/// An explicit-bounds histogram as `getSketchBuckets` sketches a delta point. See the module doc.
pub fn histogram_to_agent(h: &Histogram) -> Result<AgentSketch, ConversionError> {
    let (mut bounds, mut counts) = otlp_shape(h);
    let count = counts.iter().fold(0u64, |a, c| a.saturating_add(*c));
    // An absent sum is as unusable as a non-finite one; the translator would read it as 0.
    let sum = h.sum.unwrap_or(f64::NAN);
    let ok = sum.is_finite();

    // A point without buckets is one bucket of its whole count, with empty buckets below its
    // minimum and above its maximum.
    if counts.is_empty() && ok {
        if let Some(min) = h.min {
            counts.push(0);
            bounds.push(min);
        }
        counts.push(count);
        if let Some(max) = h.max {
            counts.push(0);
            bounds.push(max);
        }
    }

    let mut agent = AgentBuilder {
        config: AgentConfig::new(),
        store: AgentStore::default(),
        basic: Summary::default(),
    };
    let (mut min_bound, mut max_bound, mut min_bound_set) = (0.0, 0.0, false);
    let mut inserted = 0.0;
    let mut drop = false;
    for (j, &n) in counts.iter().enumerate() {
        let original_lower = if j > 0 { bounds[j - 1] } else { f64::NEG_INFINITY };
        let original_upper = bounds.get(j).copied().unwrap_or(f64::INFINITY);
        let (mut lower, mut upper) = (original_lower, original_upper);
        if upper == f64::INFINITY {
            upper = lower;
        } else if lower == f64::NEG_INFINITY || (lower == 0.0 && upper > 0.0) {
            lower = upper;
        }
        let n_f = n as f64;
        if !drop {
            inserted += n_f;
            if inserted > MAX_OBSERVATIONS as f64 {
                drop = true;
            } else {
                agent.insert_interpolate(lower, upper, n)?;
            }
        }
        if n > 0 {
            if !min_bound_set {
                min_bound = original_lower;
                min_bound_set = true;
            }
            max_bound = original_upper;
        }
    }
    if drop {
        return Err(ConversionError::TooManyObservations);
    }
    if agent.basic.cnt == 0 {
        return Err(ConversionError::Empty);
    }

    let mut s = agent.basic;
    if ok {
        s.cnt = count as i64;
        s.sum = sum;
        s.avg = s.sum / s.cnt as f64;
    }
    if min_bound_set {
        // `!math.IsInf`, as the translator checks: a `NaN` outermost bound becomes the summary's
        // `min` or `max`.
        if !min_bound.is_infinite() {
            s.min = min_bound;
        }
        if !max_bound.is_infinite() {
            s.max = max_bound;
        }
    }
    // A delta point's min and max are this window's own.
    if let Some(min) = h.min {
        s.min = min;
    }
    if let Some(max) = h.max {
        s.max = max;
    }
    // Open-ended buckets can leave `min > max`; the point's own minimum, when it's the one in
    // place, bounds the values.
    if s.min > s.max {
        if h.min == Some(s.min) {
            s.max = s.min;
        } else {
            s.min = s.max;
        }
    }
    Ok(agent_sketch(&agent.store, s, ok))
}

/// `sketches-go`'s `LogarithmicMapping`.
#[derive(Clone, Copy)]
struct LogMapping {
    gamma: f64,
    offset: f64,
    multiplier: f64,
    min_indexable: f64,
    max_indexable: f64,
}

impl LogMapping {
    fn new(gamma: f64, offset: f64) -> Option<Self> {
        const EXP_OVERFLOW: f64 = 7.094361393031e+02;
        const MIN_NORMAL: f64 = 2.2250738585072014e-308;
        if gamma <= 1.0 {
            return None;
        }
        let multiplier = 1.0 / gomath::log(gamma);
        Some(LogMapping {
            gamma,
            offset,
            multiplier,
            min_indexable: gomath::max(
                gomath::exp((i32::MIN as f64 - offset) / multiplier + 1.0),
                MIN_NORMAL * gamma,
            ),
            max_indexable: gomath::min(
                gomath::exp((i32::MAX as f64 - offset) / multiplier - 1.0),
                gomath::exp(EXP_OVERFLOW) / (2.0 * gamma) * (gamma + 1.0),
            ),
        })
    }

    fn index(&self, v: f64) -> i64 {
        let index = gomath::log(v) * self.multiplier + self.offset;
        if index >= 0.0 {
            gomath::to_int(index)
        } else {
            gomath::to_int(index).wrapping_sub(1)
        }
    }

    fn lower_bound(&self, index: i64) -> f64 {
        gomath::exp((index as f64 - self.offset) / self.multiplier)
    }

    fn value(&self, index: i64) -> f64 {
        self.lower_bound(index) * (1.0 + (1.0 - 2.0 / (1.0 + self.gamma)))
    }

    /// `Equals`: gamma and offset each within a relative 1e-12.
    fn equals(&self, o: &LogMapping) -> bool {
        fn within(x: f64, y: f64) -> bool {
            const TOL: f64 = 1e-12;
            if x == 0.0 || y == 0.0 {
                x.abs() <= TOL && y.abs() <= TOL
            } else {
                (x - y).abs() <= TOL * gomath::max(x.abs(), y.abs())
            }
        }
        within(self.gamma, o.gamma) && within(self.offset, o.offset)
    }
}

/// `sketches-go`'s `DenseStore`, as far as the conversion reads it: a zero count is never added,
/// so every stored count is positive.
#[derive(Default)]
struct DenseStore {
    bins: std::collections::BTreeMap<i64, f64>,
    count: f64,
}

impl DenseStore {
    fn add(&mut self, index: i64, count: f64) {
        if count == 0.0 {
            return;
        }
        *self.bins.entry(index).or_insert(0.0) += count;
        self.count += count;
    }

    fn is_empty(&self) -> bool {
        self.count == 0.0
    }

    fn min_index(&self) -> Option<i64> {
        self.bins.keys().next().copied()
    }

    fn max_index(&self) -> Option<i64> {
        self.bins.keys().next_back().copied()
    }

    fn iter(&self) -> impl Iterator<Item = (i64, f64)> + '_ {
        self.bins.iter().filter(|(_, c)| **c > 0.0).map(|(i, c)| (*i, *c))
    }
}

/// `sketches-go`'s `DDSketch`.
struct LogSketch {
    mapping: LogMapping,
    positive: DenseStore,
    negative: DenseStore,
    zero_count: f64,
}

impl LogSketch {
    fn new(mapping: LogMapping) -> Self {
        LogSketch {
            mapping,
            positive: DenseStore::default(),
            negative: DenseStore::default(),
            zero_count: 0.0,
        }
    }

    /// `AddWithCount`; `false` for a value the mapping can't index.
    fn add(&mut self, value: f64, count: f64) -> bool {
        let m = self.mapping;
        if value > m.min_indexable {
            if value > m.max_indexable {
                return false;
            }
            self.positive.add(m.index(value), count);
        } else if value < -m.min_indexable {
            if value < -m.max_indexable {
                return false;
            }
            self.negative.add(m.index(-value), count);
        } else if value.is_nan() {
            return false;
        } else {
            self.zero_count += count;
        }
        true
    }

    fn count(&self) -> f64 {
        self.zero_count + self.positive.count + self.negative.count
    }

    fn sum(&self) -> f64 {
        let mut sum = 0.0;
        if self.zero_count != 0.0 {
            sum += 0.0 * self.zero_count;
        }
        for (i, c) in self.positive.iter() {
            sum += self.mapping.value(i) * c;
        }
        for (i, c) in self.negative.iter() {
            sum += -self.mapping.value(i) * c;
        }
        sum
    }

    fn max_value(&self) -> Option<f64> {
        if let Some(i) = self.positive.max_index().filter(|_| !self.positive.is_empty()) {
            Some(self.mapping.value(i))
        } else if self.zero_count > 0.0 {
            Some(0.0)
        } else {
            self.negative.min_index().map(|i| -self.mapping.value(i))
        }
    }

    fn min_value(&self) -> Option<f64> {
        if let Some(i) = self.negative.max_index().filter(|_| !self.negative.is_empty()) {
            Some(-self.mapping.value(i))
        } else if self.zero_count > 0.0 {
            Some(0.0)
        } else {
            self.positive.min_index().map(|i| self.mapping.value(i))
        }
    }

    /// `checkBucketBoundaries`: every boundary `ChangeMapping` reads is finite.
    fn boundaries_finite(&self) -> bool {
        for store in [&self.positive, &self.negative] {
            let (Some(lowest), Some(highest)) = (store.min_index(), store.max_index()) else {
                continue;
            };
            let highest = if highest < i64::MAX { highest + 1 } else { highest };
            for index in [lowest, highest] {
                let b = self.mapping.lower_bound(index);
                if b.is_infinite() || b.is_nan() {
                    return false;
                }
            }
        }
        true
    }

    /// `ChangeMapping` with a scale factor of 1, for mappings that differ.
    fn change_mapping(&self, to: LogMapping) -> LogSketch {
        let mut out = LogSketch::new(to);
        for (from, into) in
            [(&self.positive, &mut out.positive), (&self.negative, &mut out.negative)]
        {
            for (index, count) in from.iter() {
                let in_lower = self.mapping.lower_bound(index);
                let in_higher = self.mapping.lower_bound(index + 1);
                let in_size = in_higher - in_lower;
                let mut out_index = to.index(in_lower);
                while to.lower_bound(out_index) < in_higher {
                    let out_lower = to.lower_bound(out_index);
                    let out_higher = to.lower_bound(out_index + 1);
                    let size =
                        gomath::min(out_higher, in_higher) - gomath::max(out_lower, in_lower);
                    into.add(out_index, size / in_size * count);
                    out_index += 1;
                }
            }
        }
        out.zero_count = self.zero_count;
        out
    }
}

/// Whether `e` holds more than [`MAX_OBSERVATIONS`], zero bucket included: the translator's
/// `validateExpHistogramDataPoint`, which drops the whole point, its aggregation series too,
/// before anything else reads it. The total is a float, as the translator's is.
pub fn exp_histogram_oversized(e: &ExpHistogram) -> bool {
    let total =
        e.positive.1.iter().chain(&e.negative.1).fold(e.zero_count as f64, |t, c| t + *c as f64);
    total > MAX_OBSERVATIONS as f64
}

/// An exponential histogram as `MapExponentialHistogramMetrics` sketches a delta point. See the
/// module doc.
pub fn exp_histogram_to_agent(e: &ExpHistogram) -> Result<AgentSketch, ConversionError> {
    if exp_histogram_oversized(e) {
        return Err(ConversionError::TooManyObservations);
    }
    let sum = e.sum.unwrap_or(f64::NAN);
    let ok = sum.is_finite();

    // `exponentialHistogramToDDSketch`: OTLP's bucket index is the DDSketch index under
    // γ = 2^(2^-scale) and an offset of 0.
    let gamma = gomath::pow(2.0, gomath::pow(2.0, -(e.scale as f64)));
    let mapping = LogMapping::new(gamma, 0.0).ok_or(ConversionError::Unconvertible)?;
    let mut input = LogSketch::new(mapping);
    for (store, (offset, counts)) in
        [(&mut input.positive, &e.positive), (&mut input.negative, &e.negative)]
    {
        for (j, c) in counts.iter().enumerate() {
            store.add(j as i64 + *offset as i64, *c as f64);
        }
    }
    input.zero_count += e.zero_count as f64;

    // `createDDSketchWithSketchMapping`: the Agent's γ, offset by half a bin since a DDSketch index
    // floors where an Agent key rounds.
    let config = AgentConfig::new();
    let agent_mapping = LogMapping::new(Mapping::AGENT_GAMMA, config.bias as f64 + 0.5)
        .expect("the Agent's gamma is above 1");
    if !mapping.equals(&agent_mapping) && !input.boundaries_finite() {
        return Err(ConversionError::Unconvertible);
    }
    let mut single = None;
    if input.count() == 1.0 {
        // One observation is the sum; the translator adds it rather than re-map it.
        let mut s = LogSketch::new(agent_mapping);
        if s.add(input.sum(), 1.0) {
            single = Some(s);
        }
    }
    let compatible = match single {
        Some(s) => s,
        None if mapping.equals(&agent_mapping) => input,
        None => input.change_mapping(agent_mapping),
    };

    // `convertDDSketchIntoSketch`: indices at or below 0 are the zero bin, and negative values
    // take negative keys.
    let mut float_counts: Vec<(i32, f64)> = Vec::new();
    let mut zeroes = 0.0;
    for (store, sign) in [(&compatible.positive, 1), (&compatible.negative, -1)] {
        for (index, count) in store.iter() {
            if index >= i16::MAX as i64 {
                return Err(ConversionError::Unconvertible);
            }
            if index <= 0 {
                zeroes += count;
            } else {
                float_counts.push((sign * index as i32, count));
            }
        }
    }
    zeroes += compatible.zero_count;
    if zeroes != 0.0 {
        float_counts.push((0, zeroes));
    }
    // `convertFloatCountsToIntCounts`: each key gets what its running total adds once rounded.
    float_counts.sort_by_key(|kc| kc.0);
    let (mut float_total, mut int_total) = (0.0, 0u64);
    let mut kcs = Vec::with_capacity(float_counts.len());
    for (k, c) in float_counts {
        float_total += c;
        let rounded = (float_total.round() as u64).wrapping_sub(int_total);
        int_total = int_total.wrapping_add(rounded);
        if rounded > 0 {
            kcs.push((k, rounded));
        }
    }
    let mut store = AgentStore::default();
    store.insert_counts(&mut kcs);

    let (Some(max), Some(min)) = (compatible.max_value(), compatible.min_value()) else {
        return Err(ConversionError::Empty);
    };
    let csum = compatible.sum();
    let mut s =
        Summary { cnt: int_total as i64, sum: csum, avg: csum / int_total as f64, max, min };
    if ok {
        s.cnt = e.count as i64;
        s.sum = sum;
        s.avg = s.sum / s.cnt as f64;
        if e.count == 1 {
            s.min = sum;
            s.max = sum;
        }
    }
    if let Some(min) = e.min {
        s.min = min;
        s.max = gomath::max(s.max, min);
    }
    if let Some(max) = e.max {
        s.max = max;
        s.min = gomath::min(s.min, max);
    }
    Ok(agent_sketch(&store, s, ok))
}

#[cfg(test)]
mod tests;
