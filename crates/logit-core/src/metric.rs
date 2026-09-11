use crate::interner::Symbol;
use crate::trace::TraceRef;
use crate::AttrMap;
use smallvec::SmallVec;

/// Whether a series reports fresh increments since the last report (`Delta`) or a running total
/// since a fixed start time (`Cumulative`) -- OTLP's own `AggregationTemporality`, now a real field
/// on [`Sum`]/[`Histogram`]/[`ExpHistogram`] instead of a decode-only well-known attribute the way
/// it used to (`docs/adr/lossless-transit.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temporality {
    Delta,
    Cumulative,
}

#[derive(Debug, Clone)]
pub struct MetricRecord {
    pub name: Symbol,
    pub unit: Option<Symbol>,
    pub description: Option<Symbol>,
    /// Unix nanoseconds this series started accumulating at; `0` means unknown -- OTLP's own
    /// convention for an absent `start_time_unix_nano`, which avoids an `Option<i64>` here.
    pub start_timestamp: i64,
    /// Empty `Vec` allocates nothing on the common no-exemplars path.
    pub exemplars: Vec<Exemplar>,
    pub kind: MetricKind,
}

impl PartialEq for MetricRecord {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.unit == other.unit
            && self.description == other.description
            && self.start_timestamp == other.start_timestamp
            && self.exemplars == other.exemplars
            && self.kind == other.kind
    }
}

impl MetricRecord {
    /// A record carrying just a name and a kind -- `unit`/`description` `None`, `start_timestamp`
    /// `0` (unknown), `exemplars` empty. What most producers and nearly every test want.
    pub fn new(name: Symbol, kind: MetricKind) -> Self {
        MetricRecord {
            name,
            unit: None,
            description: None,
            start_timestamp: 0,
            exemplars: Vec::new(),
            kind,
        }
    }
}

/// Metric kinds are chosen to be *mergeable*: the split-collection topology (`docs/OVERVIEW.md`)
/// means two edge nodes' aggregates may need combining downstream, and that has to be exact where
/// the math allows it (`Sum`, `Gauge`, `Set`) and correctly error-bounded where it can't
/// (`Distribution`). See `docs/design/data-model.md`.
///
/// `Samples`/`SetMembers` carry raw, unsummarized observations exactly as a protocol like statsd
/// hands them over (`ms`/`h`/`d` timings, `s` set members) -- `docs/adr/lossless-transit.md`'s
/// "summarization is opt-in and named" rule: a decoder never pre-summarizes what an explicit
/// `aggregate` stage should decide about. `Distribution`/`Set` are the *summarized* counterparts,
/// produced only by `aggregate`. `ExponentialHistogram` is kept distinct from `Histogram` rather
/// than always materializing explicit buckets on decode, specifically so an OTLP
/// `ExponentialHistogram` round-trips through `otlp_in -> otlp_out` as a fixed point, not a lossy
/// conversion.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricKind {
    /// Replaces the old `Counter` -- a counter is `Sum { temporality: Delta, monotonic: true }`,
    /// via [`MetricKind::counter`].
    Sum(Sum),
    Gauge(f64),
    /// A *relative* adjustment to a gauge's previous value (statsd/DogStatsD's leading `+`/`-`
    /// syntax, `crates/logit-inputs/src/statsd.rs`) -- **unresolved**. This variant must never
    /// reach a sink; it is resolved into an ordinary [`MetricKind::Gauge`] by the `aggregate`
    /// transform (`crates/logit-transforms/src/aggregate.rs`), which is the only component that
    /// carries the running gauge value a delta needs to apply against. See
    /// `docs/adr/relative-gauge-adjustments.md`.
    GaugeDelta(f64),
    /// Raw observations, as statsd `ms`/`h`/`d` values arrive -- no producer until W3
    /// (`docs/plans/lossless-transit.md`).
    Samples(Samples),
    /// Produced only by `aggregate`, merging a run of [`MetricKind::Samples`].
    Distribution(DdSketch),
    /// Raw set members, as statsd `s` arrives -- no producer until W3.
    SetMembers(Vec<bytes::Bytes>),
    /// Produced only by `aggregate`, merging a run of [`MetricKind::SetMembers`]. Still a stub in
    /// W1 -- see [`HyperLogLog`].
    Set(HyperLogLog),
    /// Fixed, explicit bucket bounds, e.g. a Prometheus-style scrape input or an OTLP
    /// `HistogramDataPoint`.
    Histogram(Histogram),
    /// OTLP/Prometheus-native base-2 exponential bucketing -- kept distinct from [`Histogram`] so
    /// `otlp_in -> otlp_out` is a fixed point, not a lossy conversion.
    ExponentialHistogram(ExpHistogram),
    /// Pre-computed quantiles, e.g. some scrape inputs or OTLP `SummaryDataPoint`s report these
    /// directly.
    Summary(Summary),
}

impl MetricKind {
    /// A monotonic delta sum -- what the old `MetricKind::Counter(v)` meant.
    pub fn counter(v: f64) -> Self {
        MetricKind::Sum(Sum { value: v, temporality: Temporality::Delta, monotonic: true })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sum {
    pub value: f64,
    pub temporality: Temporality,
    pub monotonic: bool,
}

/// Sized to keep [`MetricKind`] at its existing 176-byte size. `SAMPLES_INLINE` is measured, not
/// guessed: `size_of::<DdSketch>()` is 176 (a `sketches_ddsketch::DDSketch` inlined directly, no
/// `Box`), and `MetricKind::Distribution(DdSketch)` fits in exactly 176 bytes with no extra
/// discriminant byte -- rustc niche-fills the outer enum tag into spare bit patterns inside
/// `DDSketch`'s own layout. `SmallVec<[f64; N]>` under this workspace's `union` feature costs
/// `max(24, N * 8 + 8)` bytes (confirmed by direct measurement, not the smallvec docs), so
/// `Samples { values, sample_rate: f64 }` costs `N * 8 + 16`. That niche-filling trick is specific
/// to `DDSketch`'s own layout, not available to `Samples`, so once a `Samples` variant is exactly
/// 176 bytes too, `MetricKind` needs a real discriminant on top and grows to 184 -- measured
/// directly (`N = 20` gives `size_of::<Samples>() == 176` and `size_of::<MetricKind>() == 184`).
/// `N = 19` is the largest value that leaves room for that discriminant: `size_of::<Samples>() ==
/// 168`, `size_of::<MetricKind>()` stays the required 176. Both are asserted exactly in
/// `crates/logit-core/tests/type_sizes.rs`.
pub const SAMPLES_INLINE: usize = 19;

#[derive(Debug, Clone, PartialEq)]
pub struct Samples {
    pub values: SmallVec<[f64; SAMPLES_INLINE]>,
    pub sample_rate: f64,
}

impl Samples {
    /// `sample_rate` defaults to `1.0` -- unsampled, the common case.
    pub fn new(values: impl IntoIterator<Item = f64>) -> Self {
        Samples { values: SmallVec::from_iter(values), sample_rate: 1.0 }
    }
}

impl Default for Samples {
    fn default() -> Self {
        Samples { values: SmallVec::new(), sample_rate: 1.0 }
    }
}

/// Fixed-bucket histogram, e.g. a Prometheus-style scrape input or an OTLP `HistogramDataPoint`.
/// Each `(bound, count)` pair is that bucket's own count, not a cumulative running total up to
/// `bound`.
#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    pub buckets: Vec<(f64, u64)>,
    pub temporality: Temporality,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// OTLP's base-2 exponential histogram shape, carried 1:1 rather than materialized into explicit
/// [`Histogram`] buckets on decode -- see [`MetricKind::ExponentialHistogram`]'s doc comment.
/// `positive`/`negative` are each `(offset, bucket_counts)`, mirroring
/// `ExponentialHistogramDataPoint.positive`/`.negative`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExpHistogram {
    pub scale: i32,
    pub zero_count: u64,
    pub zero_threshold: f64,
    pub positive: (i32, Vec<u64>),
    pub negative: (i32, Vec<u64>),
    pub temporality: Temporality,
    pub count: u64,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// Pre-computed quantiles, e.g. some scrape inputs or an OTLP `SummaryDataPoint` report these
/// directly.
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub quantiles: Vec<(f64, f64)>,
    pub count: u64,
    pub sum: f64,
}

/// A single sampled measurement backing a metric point -- OTLP's exemplar concept: the specific
/// trace a particular observation happened under, plus whatever attributes were dropped from the
/// point's own attribute set to reach it. No producer until W4 (`docs/plans/lossless-transit.md`).
#[derive(Debug, Clone, PartialEq)]
pub struct Exemplar {
    pub timestamp: i64,
    pub value: f64,
    pub trace: Option<TraceRef>,
    pub filtered_attributes: AttrMap,
}

/// A mergeable quantile sketch, wrapping `sketches_ddsketch::DDSketch` (per
/// `docs/design/data-model.md` -- merges with a guaranteed relative-error bound, unlike naive
/// percentile-of-percentiles, which is load-bearing for the split-collection topology in
/// `docs/OVERVIEW.md`).
#[derive(Clone)]
pub struct DdSketch(sketches_ddsketch::DDSketch);

// `sketches_ddsketch::DDSketch` doesn't implement `Debug`; summarize instead of deriving.
impl std::fmt::Debug for DdSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdSketch").field("count", &self.0.count()).finish()
    }
}

/// `sketches_ddsketch::DDSketch` has no `PartialEq` of its own (no bin iteration exposed, see this
/// struct's own doc comment) -- compare via [`DdSketch::to_java_bytes`], the only lossless view
/// the wrapped crate exposes, and therefore the only faithful equality check available.
impl PartialEq for DdSketch {
    fn eq(&self, other: &Self) -> bool {
        self.to_java_bytes() == other.to_java_bytes()
    }
}

impl DdSketch {
    pub fn new() -> Self {
        Self(sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::defaults()))
    }

    pub fn add(&mut self, value: f64) {
        self.0.add(value);
    }

    /// Adds `value` as `count` weighted samples -- e.g. what a sampled statsd timing/histogram
    /// line needs to extrapolate `100|ms|@0.1` into ten samples rather than one
    /// (`crates/logit-inputs/src/statsd.rs`). Delegates directly to
    /// `sketches_ddsketch::DDSketch::add_with_count`, which computes the target bin once and
    /// increments its stored count by `count` in constant time -- not a loop calling `add`
    /// `count` times, and not a single-bucket sketch `merge`-d in via binary doubling either:
    /// both would cost real, avoidable work (an O(count) loop, or O(log count) allocations for
    /// the merge alternative) that `add_with_count` doesn't pay. Same zero-additional-allocation
    /// property either way -- the bin `Vec` is allocated once, the first time any sample ever
    /// lands in this sketch -- but O(1) instead of O(count) in CPU cost, which matters because
    /// `count` can be attacker-influenced (a sampled statsd line's extrapolated weight). `count
    /// == 0` is a no-op (`add_with_count`'s own contract).
    pub fn add_weighted(&mut self, value: f64, count: u64) {
        self.0.add_with_count(value, count);
    }

    /// Merges `other` into `self`. Every `DdSketch` in this codebase is built with
    /// `Config::defaults()` (via [`DdSketch::new`]), so the mismatched-config failure case this
    /// can't-actually-happen -- if that stops being true, this needs a real `Result`.
    pub fn merge(&mut self, other: &DdSketch) {
        self.0.merge(&other.0).expect("DdSketch configs always match (Config::defaults())");
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        self.0.quantile(q).ok().flatten()
    }

    pub fn count(&self) -> usize {
        self.0.count()
    }

    /// Serializes to DataDog's canonical "java bytes" sketch format -- a compact, cross-language
    /// binary encoding, not specific to any JVM. This is how a `Distribution` survives a wire or
    /// disk round trip losslessly: `DDSketch`'s own fields are private with no bin iteration
    /// (see this struct's own doc comment), so a codec has no way to reconstruct one from parts --
    /// this blob is the only lossless path in or out. `logit_proto::native`'s wire format uses it
    /// directly; see `docs/design/wire-protocol.md`.
    pub fn to_java_bytes(&self) -> Vec<u8> {
        self.0.to_java_bytes()
    }

    /// The inverse of [`DdSketch::to_java_bytes`]. Fails only on a genuinely malformed blob (wrong
    /// magic, truncated, or an encoding this crate's `sketches_ddsketch` version doesn't
    /// recognize) -- never on a value-range or precision issue, since the format carries the
    /// sketch's bins directly rather than re-deriving them from samples.
    pub fn from_java_bytes(bytes: &[u8]) -> Result<Self, sketches_ddsketch::DecodeError> {
        sketches_ddsketch::DDSketch::from_java_bytes(bytes).map(Self)
    }
}

impl Default for DdSketch {
    fn default() -> Self {
        Self::new()
    }
}

/// Placeholder for a mergeable cardinality estimator (candidate: `cardinality-estimator`). Merges
/// (unions) exactly by construction, which `Set` needs for the same distributed-aggregation
/// reason `Distribution` needs `DdSketch`. See `docs/design/data-model.md`. Still a stub as of
/// the statsd decoder (`crates/logit-inputs/src/statsd.rs`): its `s` (set) metric type returns a
/// decode error rather than silently losing data until this is wired up.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HyperLogLog {
    // TODO: wrap a real HLL implementation.
    _todo: (),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `add_weighted(v, 1)` is the `count == 1` case a sample-rate-1 statsd line always takes --
    /// it must be indistinguishable from the plain `add(v)` path it replaces there.
    #[test]
    fn add_weighted_with_count_one_matches_plain_add() {
        let mut weighted = DdSketch::new();
        weighted.add_weighted(42.0, 1);

        let mut plain = DdSketch::new();
        plain.add(42.0);

        assert_eq!(weighted.count(), plain.count());
        assert_eq!(weighted.quantile(0.5), plain.quantile(0.5));
        assert_eq!(weighted.quantile(0.99), plain.quantile(0.99));
    }

    /// `add_weighted(v, 100)` extrapolates one sample into a hundred identical ones -- `count()`
    /// reports the extrapolated population, and every quantile (not just the median) lands within
    /// `DdSketch`'s documented 1% relative-error bound of `v`, since all 100 samples fall in the
    /// same bucket. Not *exactly* `v`: DDSketch is a bucketed approximation by construction --
    /// `quantile` returns a bucket boundary estimate, not the stored value -- so even a sketch fed
    /// nothing but identical samples doesn't round-trip them exactly.
    #[test]
    fn add_weighted_with_large_count_extrapolates_count_and_every_quantile() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(7.5, 100);

        assert_eq!(sketch.count(), 100);
        for q in [0.0, 0.1, 0.5, 0.9, 0.99, 1.0] {
            let value = sketch.quantile(q).expect("quantile should be present");
            let relative_error = (value - 7.5).abs() / 7.5;
            assert!(
                relative_error <= 0.01,
                "quantile({q}) = {value} is more than 1% away from the true value 7.5"
            );
        }
    }

    /// `add_weighted(v, 0)` must be a true no-op -- the clamp in `statsd.rs` never produces a
    /// zero weight, but the method's own contract should hold regardless of the caller.
    #[test]
    fn add_weighted_with_zero_count_is_a_no_op() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(1.0, 0);
        assert_eq!(sketch.count(), 0);
        assert_eq!(sketch.quantile(0.5), None);
    }

    /// A weighted add still respects `Config::defaults()`'s documented 1% relative-accuracy bound
    /// (`sketches_ddsketch::Config::defaults()`: alpha = 0.01) -- extrapolating via repeated `add`
    /// must not degrade the sketch's error guarantee versus the same number of genuine samples.
    #[test]
    fn add_weighted_quantile_stays_within_the_configured_relative_error_bound() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(200.0, 50);

        let q = sketch.quantile(0.5).expect("quantile should be present");
        let relative_error = (q - 200.0).abs() / 200.0;
        assert!(
            relative_error <= 0.01,
            "quantile {q} is more than 1% away from the true value 200.0"
        );
    }

    #[test]
    fn ddsketch_partial_eq_compares_via_java_bytes() {
        let mut a = DdSketch::new();
        a.add(1.0);
        a.add(2.0);
        let mut b = DdSketch::new();
        b.add(1.0);
        b.add(2.0);
        assert_eq!(a, b);

        let mut c = DdSketch::new();
        c.add(99.0);
        assert_ne!(a, c);
    }

    #[test]
    fn counter_constructor_builds_a_delta_monotonic_sum() {
        assert_eq!(
            MetricKind::counter(3.0),
            MetricKind::Sum(Sum { value: 3.0, temporality: Temporality::Delta, monotonic: true })
        );
    }

    #[test]
    fn samples_new_defaults_sample_rate_to_one() {
        let s = Samples::new([1.0, 2.0, 3.0]);
        assert_eq!(s.sample_rate, 1.0);
        assert_eq!(&s.values[..], &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn metric_record_new_fills_defaults() {
        let name = crate::interner::intern("metric_record_new_test");
        let record = MetricRecord::new(name, MetricKind::Gauge(1.0));
        assert_eq!(record.unit, None);
        assert_eq!(record.description, None);
        assert_eq!(record.start_timestamp, 0);
        assert!(record.exemplars.is_empty());
    }
}
