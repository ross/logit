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

#[derive(Debug, Clone, PartialEq)]
pub struct MetricRecord {
    pub name: Symbol,
    pub unit: Option<Symbol>,
    pub description: Option<Symbol>,
    /// Unix nanoseconds this series started accumulating at; `0` means unknown -- OTLP's own
    /// convention for an absent `start_time_unix_nano`, which avoids an `Option<i64>` here.
    pub start_timestamp: i64,
    /// Empty `Vec` allocates nothing on the common no-exemplars path.
    pub exemplars: Vec<Exemplar>,
    /// OTLP `DataPointFlags` bitmask, `0` default -- exists so a data point flagged
    /// `NO_RECORDED_VALUE` (bit 0, [`MetricRecord::FLAG_NO_RECORDED_VALUE`], see
    /// [`MetricRecord::is_no_recorded_value`]) round-trips as a flagged point carrying its type's
    /// default value, rather than being silently skipped the way the OTLP codec used to treat it
    /// (`docs/adr/metrics-model-v2.md`'s W4 amendment). `otlp_out` keeps and re-encodes a flagged
    /// point unchanged -- that's the fixed point `docs/adr/lossless-transit.md` requires for
    /// `otlp_in -> otlp_out`. Everywhere else -- every non-OTLP sink, and `aggregate`'s fold --
    /// must instead treat a flagged record as carrying no genuine reading: skip it (counted) at a
    /// sink, pass it through unmerged (counted) at `aggregate`, rather than fold its default
    /// numeric payload in as though it were a real sample (`docs/known-gaps.md`'s cross-protocol
    /// table has the one-row summary). Fills the 4 bytes of padding that already followed the
    /// three `Symbol`s above, so [`MetricRecord`] stays 224 bytes -- see
    /// `crates/logit-core/tests/type_sizes.rs`.
    pub flags: u32,
    pub kind: MetricKind,
}

impl MetricRecord {
    /// OTLP `DataPointFlags::FLAG_NO_RECORDED_VALUE` (bit 0) -- the point has no recorded value;
    /// its numeric payload should be treated as absent rather than a genuine `0`/empty reading.
    pub const FLAG_NO_RECORDED_VALUE: u32 = 1 << 0;

    /// A record carrying just a name and a kind -- `unit`/`description` `None`, `start_timestamp`
    /// `0` (unknown), `exemplars` empty, `flags` `0`. What most producers and nearly every test
    /// want.
    pub fn new(name: Symbol, kind: MetricKind) -> Self {
        MetricRecord {
            name,
            unit: None,
            description: None,
            start_timestamp: 0,
            exemplars: Vec::new(),
            flags: 0,
            kind,
        }
    }

    /// Whether this record carries `FLAG_NO_RECORDED_VALUE` -- see the doc on [`Self::flags`]
    /// above for who must check this and what to do about it.
    pub fn is_no_recorded_value(&self) -> bool {
        self.flags & Self::FLAG_NO_RECORDED_VALUE != 0
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
    /// Produced only by `aggregate`, merging a run of [`MetricKind::SetMembers`] into a real,
    /// mergeable cardinality estimate -- see [`HyperLogLog`].
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
    /// Upper bound on [`Samples::weight`]: a `@0.0001` rate would otherwise turn one observation
    /// into ten thousand, and the rate is attacker-influenced wire input. Mirrors
    /// `crates/logit-inputs/src/statsd.rs`'s `MAX_SAMPLE_WEIGHT`, which W3 folds into this one.
    pub const MAX_WEIGHT: u64 = 1000;

    /// `sample_rate` defaults to `1.0` -- unsampled, the common case.
    pub fn new(values: impl IntoIterator<Item = f64>) -> Self {
        Samples { values: SmallVec::from_iter(values), sample_rate: 1.0 }
    }

    /// How many observations each value in `values` stands for: `round(1 / sample_rate)`,
    /// clamped to `[1, MAX_WEIGHT]` -- the extrapolation a consumer sketching these applies per
    /// value (`DdSketch::add_weighted`). A non-finite or non-positive rate (nothing upstream
    /// validates `sample_rate`; the native decoder reads a bare `f64`) degrades to `1`, i.e.
    /// unweighted, rather than to `0`: `f64::clamp` propagates NaN and `NaN as u64` is `0`,
    /// which `add_weighted` treats as a no-op -- every observation would silently vanish.
    pub fn weight(&self) -> u64 {
        if !(self.sample_rate.is_finite() && self.sample_rate > 0.0) {
            return 1;
        }
        let weight = (1.0 / self.sample_rate).round();
        if weight.is_nan() {
            1
        } else {
            (weight as u64).clamp(1, Self::MAX_WEIGHT)
        }
    }

    /// Sketches these raw observations into a fresh [`DdSketch`], weighting each value by
    /// [`Samples::weight`] -- what every consumer that needs to summarize a `Samples` does
    /// (`crates/logit-proto/src/otlp/metrics.rs`, `crates/logit-outputs/src/influxdb.rs`, and
    /// `aggregate`'s `Samples`-mode fallback), pulled into one place so the three call sites agree
    /// by construction rather than by convention. `weight()` is already NaN-safe and clamped to
    /// `[1, MAX_WEIGHT]`, so this never silently drops or explodes a value regardless of how
    /// `sample_rate` arrived.
    pub fn sketch(&self) -> DdSketch {
        let weight = self.weight();
        let mut sketch = DdSketch::new();
        for v in &self.values {
            sketch.add_weighted(*v, weight);
        }
        sketch
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

/// A mergeable HyperLogLog cardinality estimator, wrapping
/// `cardinality_estimator::CardinalityEstimator<[u8]>`. Real state now
/// (`docs/plans/lossless-transit.md`'s W2 -- previously a stub with no cardinality to carry). Merges
/// (unions) exactly by construction, which `Set` needs for the same distributed-aggregation reason
/// [`DdSketch`] needs a real error bound: the split-collection topology (`docs/OVERVIEW.md`) means
/// two edge nodes' `Set` aggregates may need combining downstream, and a union of two independently
/// built HyperLogLogs is the algorithm's whole point, not an approximation layered on top of one.
/// See `docs/design/data-model.md`. Since W3, `crates/logit-inputs/src/statsd.rs` decodes statsd's
/// `s` (set) metric type straight to [`MetricKind::SetMembers`], and `aggregate` (W2) merges it
/// into a real [`MetricKind::Set`].
///
/// **`from_bytes` depends on a capacity invariant, not just a byte layout.** `cardinality-estimator`
/// 1.0.3's `Array::from_vec` (its `src/array.rs`) reconstitutes the wrapped crate's own heap
/// allocation from a plain `Vec<u32>` via `mem::forget` + a raw-parts slice, and later frees it with
/// `Box::from_raw` sized to a *rounded* length -- if the `Vec` we hand it has more spare capacity
/// than that rounded length, the dealloc uses the wrong `Layout` (undefined behavior). `HllBytesReader`
/// below works around this by making `Vec::with_capacity`'s hint exactly match the capacity the
/// crate will free; see its own doc comment for the full mechanism. Don't change `from_bytes`'s
/// `SeqAccess::size_hint` without re-reading that comment -- it isn't a cosmetic hint here, the byte
/// codec's soundness depends on it. See also `docs/known-gaps.md`'s entry for this.
///
/// **Serialization** goes through the wrapped type's own `serde`-based `Serialize`/`Deserialize`
/// impl (enabled by this crate's `with_serde` feature) -- `CardinalityEstimator` has no `as_bytes`/
/// `to_bytes` of its own, and its fields are otherwise unreachable from outside its crate. That impl
/// always writes exactly a `(data: u64, members: Option<Vec<u32>>)` shape (its `serde.rs` source),
/// so [`HyperLogLog::to_bytes`]/[`HyperLogLog::from_bytes`] drive it through a small hand-rolled byte
/// `Serializer`/`Deserializer` pair below, purpose-built for that one shape, rather than pulling in
/// a general data-format crate (`postcard`/`serde_json`/...) just to get bytes out. The resulting
/// bytes are **pinned to this crate's `cardinality-estimator` dependency version** -- there is no
/// cross-version compatibility guarantee, which is fine pre-release (unlike [`DdSketch::to_java_bytes`],
/// this was never meant to be a portable interchange format, just this process's own wire/disk
/// representation of a value it already owns).
pub struct HyperLogLog(cardinality_estimator::CardinalityEstimator<[u8]>);

impl HyperLogLog {
    pub fn new() -> Self {
        HyperLogLog(cardinality_estimator::CardinalityEstimator::new())
    }

    /// Adds one member to the set. Idempotent per distinct byte string: inserting the same member
    /// any number of times never inflates the estimate.
    pub fn insert(&mut self, member: &[u8]) {
        self.0.insert(member);
    }

    /// Unions `other` into `self` -- the operation that makes this type mergeable: `self`'s
    /// estimate afterward is (approximately) the cardinality of the union of both sets, so members
    /// the two estimators share don't get double-counted the way summing two estimates would.
    pub fn merge(&mut self, other: &HyperLogLog) {
        self.0.merge(&other.0);
    }

    /// The estimated number of distinct members inserted so far -- `0` for a fresh, empty
    /// estimator.
    pub fn estimate(&self) -> u64 {
        self.0.estimate() as u64
    }

    /// This estimator's own memory footprint in bytes -- `cardinality_estimator::CardinalityEstimator::size_of`,
    /// exposed here so `event.rs`'s `metric_record_heap_bytes` can count a `Set`'s real allocation
    /// instead of the `0` it used while this type was a zero-sized stub.
    pub fn heap_bytes(&self) -> u64 {
        self.0.size_of() as u64
    }

    /// See this type's own doc comment for the wire shape, and [`HyperLogLog`]'s `PartialEq` impl
    /// for why the leading `data` word is canonicalized here rather than left as
    /// `cardinality_estimator` writes it.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = HllBytesWriter::default();
        serde::Serialize::serialize(&self.0, &mut out)
            .expect("HyperLogLog encoding writes to a Vec<u8> and never fails");
        let mut bytes = out.0;
        // Canonicalize `data` (the first 8 bytes) when a `members` list follows (presence byte at
        // index 8 is `1`: the "array"/"HyperLogLog" representations). `cardinality_estimator`'s
        // own `representation.rs` packs its representation tag into `data`'s low 2 bits
        // (`REPRESENTATION_MASK = 0x3`: `0` small, `1` array, `3` HLL) and, for those two
        // representations, `Representation::try_from(data, opt_vec)` reads *only* `data &
        // REPRESENTATION_MASK` -- every other bit is a raw pointer into the spilled allocation
        // `opt_vec`'s `Vec<u32>` becomes, discarded on decode. Masking down to just the tag bits
        // here (before this blob ever reaches a decoder) makes two independently-built-or-decoded
        // estimators holding the same members serialize to identical bytes regardless of
        // allocation address -- `from_bytes` still reconstructs a fully working estimator, since
        // `try_from` never looks at the discarded bits anyway. When `members` is `None` (the
        // "small" representation, presence byte `0`), `data` *is* the entire logical content --
        // small values are encoded directly into it, no separate allocation -- so it's left
        // untouched.
        if bytes.get(8) == Some(&1) {
            bytes[0] &= 0x3;
            for b in &mut bytes[1..8] {
                *b = 0;
            }
        }
        bytes
    }

    /// The inverse of [`HyperLogLog::to_bytes`]. Fails on a truncated or otherwise malformed blob;
    /// see [`HllDecodeError`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, HllDecodeError> {
        let mut reader = HllBytesReader::new(bytes);
        let inner: cardinality_estimator::CardinalityEstimator<[u8]> =
            serde::Deserialize::deserialize(&mut reader)?;
        Ok(HyperLogLog(inner))
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for HyperLogLog {
    fn clone(&self) -> Self {
        HyperLogLog(self.0.clone())
    }
}

// `cardinality_estimator::CardinalityEstimator` doesn't implement `Debug` -- summarize as its
// estimate instead, the same precedent `DdSketch` follows with `count()`.
impl std::fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog").field("estimate", &self.estimate()).finish()
    }
}

/// A plain `to_bytes() == to_bytes()` comparison -- the same precedent [`DdSketch`]'s `PartialEq`
/// follows via `to_java_bytes`. This only works because [`HyperLogLog::to_bytes`] canonicalizes
/// the volatile part of `cardinality_estimator`'s serialized form first (see that method's own doc
/// comment): without that, two independently-decoded-but-logically-equal estimators would compare
/// unequal, since the raw `data` word `cardinality_estimator` writes embeds an allocation pointer
/// once cardinality spills past the inline "small" representation.
///
/// One real limitation this still inherits from the wrapped crate: both the "small" `data`
/// encoding and the "array"/"HyperLogLog" `members` ordering are insertion-order-dependent, not a
/// sorted/canonical form -- two estimators holding the same members inserted in a different order
/// can compare unequal here even though [`HyperLogLog::estimate`] would agree. Acceptable
/// pre-release, and consistent with `DdSketch`'s own `PartialEq` (bin layout there is
/// insertion-history-dependent too). Every test in this module that checks equality controls
/// insertion order for exactly this reason.
impl PartialEq for HyperLogLog {
    fn eq(&self, other: &Self) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

/// The error [`HyperLogLog::from_bytes`] returns on a blob its hand-rolled codec can't decode --
/// truncated input, or (in principle) a shape mismatch if a future `cardinality-estimator` version
/// changed what its `Serialize` impl writes.
#[derive(Debug)]
pub struct HllDecodeError(String);

impl std::fmt::Display for HllDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "malformed HyperLogLog bytes: {}", self.0)
    }
}

impl std::error::Error for HllDecodeError {}

impl serde::de::Error for HllDecodeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        HllDecodeError(msg.to_string())
    }
}

// The hand-rolled `Serializer` below shares this same error type -- `to_bytes` never actually
// fails (writing to a growable `Vec<u8>` can't), but `serde::ser::Serializer::Error` still has to
// name a concrete type satisfying `serde::ser::Error`.
impl serde::ser::Error for HllDecodeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        HllDecodeError(msg.to_string())
    }
}

// -- A minimal hand-rolled `serde` codec for `CardinalityEstimator`'s tuple shape --------------
//
// `cardinality_estimator::CardinalityEstimator`'s `serde` module (behind this crate's
// `with_serde` feature) implements `Serialize`/`Deserialize` by always writing one 2-tuple:
// `(data: usize, members: Option<Vec<u32>>)` -- `None` for its "small" inline representation,
// `Some` for its "array"/"HyperLogLog" representations (both of which are really a `&[u32]`
// underneath). That's the only shape this codec ever needs to carry, so every other
// `serde::Serializer`/`Deserializer` method below is unreachable for this type in practice and
// returns [`HllDecodeError`] if it's ever hit -- the only way to reach one is a future
// `cardinality-estimator` version changing its `serde.rs` source to write something else, which
// this crate's version pin (see this module's `Cargo.toml` comment) is what actually guards
// against.
//
// Wire layout (little-endian, self-contained -- meant only for this pair to read back, not as a
// portable interchange format):
//   data:    8 bytes, `data as u64`
//   members: 1 byte presence (`0` = `None`, `1` = `Some`), then if `Some`: 4 bytes length
//            (`u32`, little-endian) followed by that many little-endian `u32` elements.

#[derive(Default)]
struct HllBytesWriter(Vec<u8>);

fn hll_unsupported(what: &str) -> HllDecodeError {
    HllDecodeError(format!("HyperLogLog codec doesn't support serializing {what}"))
}

impl serde::ser::Serializer for &mut HllBytesWriter {
    type Ok = ();
    type Error = HllDecodeError;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = serde::ser::Impossible<(), HllDecodeError>;
    type SerializeTupleVariant = serde::ser::Impossible<(), HllDecodeError>;
    type SerializeMap = serde::ser::Impossible<(), HllDecodeError>;
    type SerializeStruct = serde::ser::Impossible<(), HllDecodeError>;
    type SerializeStructVariant = serde::ser::Impossible<(), HllDecodeError>;

    fn serialize_bool(self, _v: bool) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("bool"))
    }
    fn serialize_i8(self, _v: i8) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("i8"))
    }
    fn serialize_i16(self, _v: i16) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("i16"))
    }
    fn serialize_i32(self, _v: i32) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("i32"))
    }
    fn serialize_i64(self, _v: i64) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("i64"))
    }
    fn serialize_u8(self, _v: u8) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("u8"))
    }
    fn serialize_u16(self, _v: u16) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("u16"))
    }
    fn serialize_u32(self, v: u32) -> Result<(), HllDecodeError> {
        self.0.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_u64(self, v: u64) -> Result<(), HllDecodeError> {
        self.0.extend_from_slice(&v.to_le_bytes());
        Ok(())
    }
    fn serialize_f32(self, _v: f32) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("f32"))
    }
    fn serialize_f64(self, _v: f64) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("f64"))
    }
    fn serialize_char(self, _v: char) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("char"))
    }
    fn serialize_str(self, _v: &str) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("str"))
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("bytes"))
    }
    fn serialize_none(self) -> Result<(), HllDecodeError> {
        self.0.push(0);
        Ok(())
    }
    fn serialize_some<T: ?Sized + serde::Serialize>(self, value: &T) -> Result<(), HllDecodeError> {
        self.0.push(1);
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("unit"))
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("unit struct"))
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("unit variant"))
    }
    fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<(), HllDecodeError> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), HllDecodeError> {
        Err(hll_unsupported("newtype variant"))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, HllDecodeError> {
        let len = len.ok_or_else(|| hll_unsupported("a sequence with no known length"))?;
        self.0.extend_from_slice(&(len as u32).to_le_bytes());
        Ok(self)
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, HllDecodeError> {
        Ok(self)
    }
    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, HllDecodeError> {
        Err(hll_unsupported("tuple struct"))
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, HllDecodeError> {
        Err(hll_unsupported("tuple variant"))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, HllDecodeError> {
        Err(hll_unsupported("map"))
    }
    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, HllDecodeError> {
        Err(hll_unsupported("struct"))
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, HllDecodeError> {
        Err(hll_unsupported("struct variant"))
    }
}

impl serde::ser::SerializeSeq for &mut HllBytesWriter {
    type Ok = ();
    type Error = HllDecodeError;
    fn serialize_element<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), HllDecodeError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), HllDecodeError> {
        Ok(())
    }
}

impl serde::ser::SerializeTuple for &mut HllBytesWriter {
    type Ok = ();
    type Error = HllDecodeError;
    fn serialize_element<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), HllDecodeError> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), HllDecodeError> {
        Ok(())
    }
}

// `cardinality_estimator::Representation::from_data`'s own tag bits (`REPRESENTATION_MASK = 0x3`)
// -- mirrored here because they're `pub(crate)` in that crate, unreachable from outside it. Only
// the two tags that ever carry a `members: Some(Vec<u32>)` payload matter to this reader; the
// "small" tag (`0`) never reaches `deserialize_seq` for a well-formed blob (see `tag`'s own doc
// comment on `HllBytesReader`).
const CE_REPRESENTATION_ARRAY: u8 = 1;
const CE_REPRESENTATION_HLL: u8 = 3;

// Mirrors `cardinality_estimator::array::MAX_CAPACITY` (`pub(crate)`, unreachable from here) --
// `Representation::try_from`'s own array-length check (`crates/.../representation.rs`) rejects
// anything outside `3..=128`, so `HllBytesReader` rejects the same range *before* allocating a
// `Vec<u32>` for it (see `validate_members_len`'s doc comment for why "before" matters).
const CE_ARRAY_MAX_CAPACITY: usize = 128;

// `cardinality_estimator::hyperloglog::HyperLogLog::<P, W>::HLL_SLICE_LEN` for this crate's fixed
// `CardinalityEstimator<[u8]>` instantiation, which uses that type's default `P = 12, W = 6` (see
// `HyperLogLog`'s own struct definition upstream and this workspace's root `Cargo.toml` pin
// comment) -- `M * W / 32 + 3` where `M = 1 << P`, i.e. `4096 * 6 / 32 + 3`. `HLL_SLICE_LEN` is
// itself `pub(crate)` upstream, unreachable from here, so this is a literal, cross-checked against
// the crate's own computation by `hll_slice_len_matches_upstream_constant` below (recomputed from
// the same `P`/`W`, not just restated) -- a `cardinality-estimator` upgrade that changes either
// constant, or `CardinalityEstimator`'s default `P`/`W`, needs this literal updated in lockstep, and
// that test would catch a mismatch first.
const CE_HLL_SLICE_LEN: usize = 771;

/// The upstream bug this reader works around, and the mechanism: `cardinality-estimator` 1.0.3's
/// `Array::from_vec(vec, len)` (`crates/.../array.rs`) computes `cap =
/// len.next_power_of_two().max(arr.len())`, calls `arr.resize(cap, 0)`, then leaks the `Vec` with
/// `mem::forget` and keeps only a raw pointer + `cap` as the slice length. Later, freeing that
/// representation (`Array::drop`, and structurally the same in `HyperLogLog::drop` for the HLL
/// representation) reconstructs a boxed slice of exactly `cap` elements via `Box::from_raw` and lets
/// its `Drop` deallocate -- which computes the *layout* (size, for the allocator) from that `cap`,
/// not from whatever capacity the original `Vec` actually had. If the `Vec` we hand `from_vec`
/// carries spare capacity beyond `cap` (e.g. serde's blanket `Vec<T>` deserializer building one via
/// `Vec::with_capacity(seq.size_hint())`, called with the *actual* element count rather than the
/// rounded one -- three members round up to a capacity-4 array, but a freshly allocated
/// capacity-3 `Vec` can come back from the allocator with usable capacity 6, and `resize`'s internal
/// `reserve` is then a no-op, so the excess survives to the final value untouched), the eventual
/// `Box::from_raw`/`dealloc` uses a 4-element layout to free a 6-element allocation: undefined
/// behavior, silently (it doesn't reliably crash; ASan/Miri catch it, a release build often just
/// corrupts the allocator's bookkeeping instead). `HyperLogLog::from`'s HLL-representation path
/// doesn't itself round anything (`HLL_SLICE_LEN` is used directly, no `resize`), but the same class
/// of mismatch is still possible one layer up, in whatever `Vec` this reader hands it, for exactly
/// the same "requested capacity != allocator's actual capacity" reason.
///
/// The fix lives entirely on our side, in `deserialize_seq` below: since serde's `Vec<T>`
/// deserializer allocates with `Vec::with_capacity(seq.size_hint().unwrap_or(0))` *before* reading
/// any elements, reporting the *rounded* capacity as the size hint (`len.next_power_of_two()` for
/// the array representation; `CE_HLL_SLICE_LEN` itself for the HLL one, since that path never
/// rounds) makes the initial allocation already exactly the size `from_vec`/`resize` will settle on
/// -- so `resize` finds enough spare capacity and never reallocates, and the `Vec`'s capacity when
/// it's later forgotten is exactly the rounded length the crate will eventually free. This only
/// works because we know which representation we're decoding: `tag`, stashed by `deserialize_u64`
/// when it reads `data` (this wire shape's only `u64`, always element 0 of the outer tuple, always
/// decoded before the `members` field that might need this). See `validate_members_len` for the
/// companion half of this fix (bounding the untrusted length before it ever reaches
/// `Vec::with_capacity` at all).
///
/// Pinned to `cardinality-estimator` 1.0.3 (this workspace's root `Cargo.toml`); the upstream fix
/// would be for `Array::from_vec` to call `into_boxed_slice()` (or `shrink_to_fit()` before taking
/// the raw parts) so the `Vec`'s capacity and the freed layout are always the same value by
/// construction, regardless of what capacity the input `Vec` started with. If a future version of
/// that crate does this, `hll_slice_len_matches_upstream_constant` and the round-trip tests below
/// still pass either way -- this workaround is extra care, not a correctness requirement this crate
/// could detect the absence of.
struct HllBytesReader<'a> {
    bytes: &'a [u8],
    /// The representation tag (`CE_REPRESENTATION_ARRAY`/`_HLL`, or the small-representation `0`)
    /// decoded from `data`'s low 2 bits, stashed by `deserialize_u64` the moment `data` -- this wire
    /// shape's only `u64`, always the outer tuple's first element -- is read. `None` only before
    /// that happens, which no valid call sequence through this reader ever observes: `deserialize_seq`
    /// (the only reader of this field) is reachable only via the `members: Option<Vec<u32>>` tuple
    /// element, which always decodes after `data`.
    tag: Option<u8>,
}

/// Bounds an untrusted members-list length *before* any `Vec` is allocated for it -- the other half
/// of the `HllBytesReader` workaround (see its doc comment for the allocation-layout bug this whole
/// reader exists to avoid). Even with `deserialize_seq`'s corrected size hint, a hostile or corrupt
/// blob could still claim an enormous length; rejecting anything `Representation::try_from` would
/// also reject (`crates/.../representation.rs`'s own array `3..=128` and HLL `== HLL_SLICE_LEN`
/// checks) here, before `Vec::with_capacity` ever runs, means a bad blob fails cleanly with
/// [`HllDecodeError`] instead of attempting a multi-gigabyte allocation first. Returns the
/// `Vec::with_capacity` hint to use (the *rounded* capacity `from_vec`/`resize` will settle on for
/// the array representation; the length itself for the HLL one, which never rounds -- see
/// `HllBytesReader`'s doc comment).
fn validate_members_len(tag: Option<u8>, len: usize) -> Result<usize, HllDecodeError> {
    match tag {
        Some(CE_REPRESENTATION_ARRAY) => {
            if len <= 2 || len > CE_ARRAY_MAX_CAPACITY {
                return Err(HllDecodeError(format!(
                    "array representation member count {len} out of range \
                     (3..={CE_ARRAY_MAX_CAPACITY})"
                )));
            }
            Ok(len.next_power_of_two())
        }
        Some(CE_REPRESENTATION_HLL) => {
            if len != CE_HLL_SLICE_LEN {
                return Err(HllDecodeError(format!(
                    "HLL representation member count {len} != expected {CE_HLL_SLICE_LEN}"
                )));
            }
            Ok(len)
        }
        other => Err(HllDecodeError(format!(
            "a members list is only valid for the array or HLL representation, got tag {other:?}"
        ))),
    }
}

impl<'a> HllBytesReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        HllBytesReader { bytes, tag: None }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], HllDecodeError> {
        if self.bytes.len() < n {
            return Err(HllDecodeError(format!(
                "expected {n} more bytes, only {} remain",
                self.bytes.len()
            )));
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }

    fn take_u8(&mut self) -> Result<u8, HllDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn take_u32(&mut self) -> Result<u32, HllDecodeError> {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(buf))
    }

    fn take_u64(&mut self) -> Result<u64, HllDecodeError> {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(buf))
    }
}

/// A `SeqAccess`/tuple `SeqAccess` over this reader -- both a tuple's fixed-arity walk and a
/// `Vec<u32>`'s length-prefixed walk use the same shape here (a known element count, each element
/// read by re-entering the `Deserializer`), so one type serves both call sites.
struct HllSeqAccess<'a, 'b> {
    reader: &'b mut HllBytesReader<'a>,
    remaining: usize,
    /// What `size_hint` reports, separate from `remaining` (the true element count still to read).
    /// For the outer 2-tuple, equal to `remaining` -- serde's tuple deserializer reads elements
    /// straight into a fixed slot each, never allocating a `Vec` from this hint, so its value is
    /// moot there. For the members `Vec<u32>` specifically, this is the *rounded* capacity
    /// `validate_members_len` computed, deliberately not equal to `remaining` -- see
    /// `HllBytesReader`'s doc comment for why serde's `Vec::with_capacity(size_hint)` needs that
    /// rounded value rather than the true count to avoid the allocation-layout bug this whole reader
    /// exists to work around.
    cap: usize,
}

impl<'de> serde::de::SeqAccess<'de> for HllSeqAccess<'de, '_> {
    type Error = HllDecodeError;
    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, HllDecodeError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.reader).map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.cap)
    }
}

impl<'de> serde::de::Deserializer<'de> for &mut HllBytesReader<'de> {
    type Error = HllDecodeError;

    fn deserialize_any<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("a self-describing (`deserialize_any`) value"))
    }
    fn deserialize_bool<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("bool"))
    }
    fn deserialize_i8<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("i8"))
    }
    fn deserialize_i16<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("i16"))
    }
    fn deserialize_i32<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("i32"))
    }
    fn deserialize_i64<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("i64"))
    }
    fn deserialize_u8<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("u8"))
    }
    fn deserialize_u16<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("u16"))
    }
    fn deserialize_u32<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        visitor.visit_u32(self.take_u32()?)
    }
    fn deserialize_u64<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        let v = self.take_u64()?;
        // `data` is this wire shape's only `u64` (see this reader's own doc comment) -- stash its
        // representation tag now so `deserialize_seq`, reached next for the `members` field, can
        // size and bound that allocation correctly.
        self.tag = Some((v & 0x3) as u8);
        visitor.visit_u64(v)
    }
    fn deserialize_f32<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("f32"))
    }
    fn deserialize_f64<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("f64"))
    }
    fn deserialize_char<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("char"))
    }
    fn deserialize_str<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("str"))
    }
    fn deserialize_string<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("string"))
    }
    fn deserialize_bytes<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("bytes"))
    }
    fn deserialize_byte_buf<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("byte buf"))
    }
    fn deserialize_option<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        match self.take_u8()? {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            other => Err(HllDecodeError(format!("bad Option presence byte {other}"))),
        }
    }
    fn deserialize_unit<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("unit"))
    }
    fn deserialize_unit_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("unit struct"))
    }
    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_seq<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        let len = self.take_u32()? as usize;
        // This reader's only sequence is the members `Vec<u32>` -- `validate_members_len` both
        // rejects an out-of-range length before any allocation happens for it, and reports the
        // rounded `Vec::with_capacity` hint the wrapped crate needs (see `HllBytesReader`'s doc
        // comment for why `len` itself, unrounded, is unsound to report here).
        let tag = self.tag;
        let cap = validate_members_len(tag, len)?;
        visitor.visit_seq(HllSeqAccess { reader: self, remaining: len, cap })
    }
    fn deserialize_tuple<V: serde::de::Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        // The outer 2-tuple: `cap` is irrelevant here (see `HllSeqAccess::cap`'s doc comment), so
        // it's just `len`.
        visitor.visit_seq(HllSeqAccess { reader: self, remaining: len, cap: len })
    }
    fn deserialize_tuple_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("tuple struct"))
    }
    fn deserialize_map<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("map"))
    }
    fn deserialize_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("struct"))
    }
    fn deserialize_enum<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("enum"))
    }
    fn deserialize_identifier<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("identifier"))
    }
    fn deserialize_ignored_any<V: serde::de::Visitor<'de>>(
        self,
        _visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
        Err(hll_unsupported("ignored any"))
    }
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

    /// The weight must never be `0` -- `add_weighted(v, 0)` is a no-op, so a `0` here would
    /// silently discard every observation. NaN is the case a plain `clamp` gets wrong.
    #[test]
    fn samples_weight_is_never_zero_and_is_clamped() {
        let with_rate = |rate: f64| Samples { values: SmallVec::new(), sample_rate: rate };
        assert_eq!(with_rate(1.0).weight(), 1);
        assert_eq!(with_rate(0.1).weight(), 10);
        assert_eq!(with_rate(0.0001).weight(), Samples::MAX_WEIGHT);
        assert_eq!(with_rate(f64::NAN).weight(), 1, "NaN must degrade to unweighted, not 0");
        assert_eq!(with_rate(f64::INFINITY).weight(), 1);
        assert_eq!(with_rate(f64::NEG_INFINITY).weight(), 1);
        assert_eq!(with_rate(0.0).weight(), 1);
        assert_eq!(with_rate(-0.5).weight(), 1);
        assert_eq!(with_rate(2.0).weight(), 1, "a rate above 1 rounds to 0 and is floored to 1");
    }

    #[test]
    fn metric_record_new_fills_defaults() {
        let name = crate::interner::intern("metric_record_new_test");
        let record = MetricRecord::new(name, MetricKind::Gauge(1.0));
        assert_eq!(record.unit, None);
        assert_eq!(record.description, None);
        assert_eq!(record.start_timestamp, 0);
        assert!(record.exemplars.is_empty());
        assert_eq!(record.flags, 0);
    }

    /// `Samples::sketch` must count every value even when the sketched-into `DdSketch`'s own
    /// `count()` is the only thing being checked -- a NaN-rate `Samples` degrades `weight()` to
    /// `1` (never `0`), so every value still lands as exactly one weighted sample rather than
    /// silently vanishing (the bug `Samples::weight`'s own doc comment already guards against).
    #[test]
    fn sketch_of_a_nan_rate_samples_still_counts_every_value() {
        let mut s = Samples::new([1.0, 2.0, 3.0, 4.0]);
        s.sample_rate = f64::NAN;
        let sketch = s.sketch();
        assert_eq!(sketch.count(), s.values.len());
    }

    #[test]
    fn sketch_weights_values_by_sample_rate() {
        let mut s = Samples::new([7.5]);
        s.sample_rate = 0.1; // weight 10
        let sketch = s.sketch();
        assert_eq!(sketch.count(), 10);
    }

    // -- HyperLogLog ------------------------------------------------------------------------

    #[test]
    fn hyperloglog_empty_estimate_is_zero() {
        let hll = HyperLogLog::new();
        assert_eq!(hll.estimate(), 0);
    }

    /// Inserting the same 1,000 distinct members twice must not change the estimate -- and the
    /// estimate itself must land within a few percent of the true cardinality, DDSketch/HLL's
    /// whole reason for existing over an exact `HashSet` at scale.
    #[test]
    fn hyperloglog_estimate_is_accurate_within_a_few_percent_on_1k_distinct_members() {
        let mut hll = HyperLogLog::new();
        for i in 0..1000u32 {
            hll.insert(&i.to_le_bytes());
        }
        let estimate = hll.estimate();
        let relative_error = (estimate as f64 - 1000.0).abs() / 1000.0;
        assert!(relative_error <= 0.05, "estimate {estimate} is more than 5% away from 1000");

        // Re-inserting the same members again must not move the estimate.
        for i in 0..1000u32 {
            hll.insert(&i.to_le_bytes());
        }
        assert_eq!(hll.estimate(), estimate, "re-inserting existing members must be a no-op");
    }

    /// A merge is a set union: two disjoint member sets add (roughly) linearly, but overlapping
    /// members don't get double-counted -- the property that makes this mergeable across a
    /// split-collection topology in the first place.
    #[test]
    fn hyperloglog_merge_is_a_union_not_a_sum() {
        let mut a = HyperLogLog::new();
        for i in 0..500u32 {
            a.insert(&i.to_le_bytes());
        }
        let mut b = HyperLogLog::new();
        for i in 500..1000u32 {
            b.insert(&i.to_le_bytes());
        }
        a.merge(&b);
        let disjoint_estimate = a.estimate();
        let relative_error = (disjoint_estimate as f64 - 1000.0).abs() / 1000.0;
        assert!(
            relative_error <= 0.05,
            "disjoint union estimate {disjoint_estimate} is more than 5% away from 1000"
        );

        // Now merge in a set that fully overlaps `b` -- the estimate must not grow.
        let mut c = HyperLogLog::new();
        for i in 500..1000u32 {
            c.insert(&i.to_le_bytes());
        }
        a.merge(&c);
        let overlapping_estimate = a.estimate();
        let relative_error = (overlapping_estimate as f64 - disjoint_estimate as f64).abs()
            / disjoint_estimate as f64;
        assert!(
            relative_error <= 0.05,
            "merging an already-included set moved the estimate from {disjoint_estimate} to \
             {overlapping_estimate}"
        );
    }

    #[test]
    fn hyperloglog_to_bytes_from_bytes_round_trips_and_partial_eq_matches() {
        let mut hll = HyperLogLog::new();
        for i in 0..250u32 {
            hll.insert(&i.to_le_bytes());
        }
        let bytes = hll.to_bytes();
        let decoded = HyperLogLog::from_bytes(&bytes).expect("valid HyperLogLog blob");
        assert_eq!(hll, decoded);
        assert_eq!(hll.estimate(), decoded.estimate());
    }

    /// An empty estimator round-trips too -- the "small" representation, which serializes with no
    /// `Some(Vec<u32>)` payload at all.
    #[test]
    fn hyperloglog_empty_round_trips() {
        let hll = HyperLogLog::new();
        let bytes = hll.to_bytes();
        let decoded = HyperLogLog::from_bytes(&bytes).expect("valid HyperLogLog blob");
        assert_eq!(hll, decoded);
        assert_eq!(decoded.estimate(), 0);
    }

    #[test]
    fn hyperloglog_from_bytes_rejects_truncated_input() {
        let mut hll = HyperLogLog::new();
        for i in 0..250u32 {
            hll.insert(&i.to_le_bytes());
        }
        let bytes = hll.to_bytes();
        assert!(HyperLogLog::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        assert!(HyperLogLog::from_bytes(&[]).is_err());
    }

    fn hll_with(n: u32) -> HyperLogLog {
        let mut hll = HyperLogLog::new();
        for i in 0..n {
            hll.insert(&i.to_le_bytes());
        }
        hll
    }

    /// The property `to_bytes`'s canonicalization exists for: two *independently decoded* copies
    /// of a spilled (non-"small") estimator must serialize byte-identically and compare equal --
    /// each representation that actually spills to a separate allocation (`array`, then `hll`
    /// once cardinality outgrows `array`'s capacity), so both get their own case here.
    #[test]
    fn hyperloglog_independently_decoded_copies_of_a_spilled_estimator_are_byte_identical() {
        for n in [10u32, 200u32] {
            let original = hll_with(n);
            let bytes = original.to_bytes();
            let copy_a = HyperLogLog::from_bytes(&bytes).expect("valid blob");
            let copy_b = HyperLogLog::from_bytes(&bytes).expect("valid blob");
            assert_eq!(
                copy_a.to_bytes(),
                copy_b.to_bytes(),
                "two independent decodes of the same {n}-member blob must serialize identically"
            );
            assert_eq!(copy_a, copy_b);
            assert_eq!(copy_a, original);
        }
    }

    /// `to_bytes -> from_bytes -> to_bytes` is a fixed point for every representation
    /// (`small` at 0 members, `array` at 10, `hll` at 200 once cardinality outgrows `array`).
    #[test]
    fn hyperloglog_to_bytes_is_a_fixed_point_across_a_round_trip_for_every_representation() {
        for n in [0u32, 10u32, 200u32] {
            let original = hll_with(n);
            let once = original.to_bytes();
            let decoded = HyperLogLog::from_bytes(&once).expect("valid blob");
            let twice = decoded.to_bytes();
            assert_eq!(once, twice, "{n}-member blob did not reach a fixed point");
        }
    }

    /// `data`'s low 2 bits are `cardinality_estimator`'s own representation tag (`0` small, `1`
    /// array, `3` hll -- `2` is never assigned and its own decoder rejects it as
    /// `InvalidRepresentation`); a `members: Some(..)` list paired with the `small` tag is
    /// likewise rejected (`SmallRepresentationInvalid` -- small never has a members list). Both
    /// are malformed-input cases this codec must surface as an error, not a panic or silent
    /// misdecode.
    #[test]
    fn hyperloglog_from_bytes_rejects_bad_representation_tags() {
        // Tag `2`: valid-looking `data` (arbitrary non-zero high bits are fine, only the low 2
        // bits are the tag) with no members list.
        let mut invalid_tag = vec![0u8; 9];
        invalid_tag[0] = 2;
        assert!(HyperLogLog::from_bytes(&invalid_tag).is_err());

        // Tag `0` (small) but with a `Some` members list -- small never has one.
        let mut small_with_members = Vec::new();
        small_with_members.extend_from_slice(&0u64.to_le_bytes());
        small_with_members.push(1); // presence: Some
        small_with_members.extend_from_slice(&1u32.to_le_bytes()); // one element
        small_with_members.extend_from_slice(&7u32.to_le_bytes());
        assert!(HyperLogLog::from_bytes(&small_with_members).is_err());
    }

    #[test]
    fn hyperloglog_debug_summarizes_as_estimate() {
        let mut hll = HyperLogLog::new();
        hll.insert(b"one");
        hll.insert(b"two");
        let debug = format!("{hll:?}");
        assert!(debug.contains("estimate"), "debug output was {debug:?}");
    }

    // -- P1 pinning tests: `HllBytesReader`'s capacity-invariant workaround -----------------

    /// Non-power-of-two array-representation member counts, plus a count past `Array`'s
    /// `MAX_CAPACITY` that upgrades to the `hll` representation -- exactly the shapes that used to
    /// reach `cardinality-estimator`'s `Array::from_vec` with a `Vec` whose spare capacity didn't
    /// match the length it would round up to and free (`HllBytesReader`'s doc comment has the full
    /// mechanism). `to_bytes` must reach a fixed point and `estimate()` must survive the round trip
    /// unchanged for every one of them; run under Miri (or a debug build with the allocator's own
    /// debug assertions) this also catches the original UB directly, not just a wrong answer.
    #[test]
    fn hyperloglog_round_trips_non_power_of_two_member_counts() {
        for n in [3u32, 5, 9, 17, 100, 300] {
            let original = hll_with(n);
            let bytes = original.to_bytes();
            let decoded = HyperLogLog::from_bytes(&bytes).expect("valid blob");
            assert_eq!(bytes, decoded.to_bytes(), "{n}-member blob did not reach a fixed point");
            assert_eq!(
                original.estimate(),
                decoded.estimate(),
                "{n}-member estimate changed across a round trip"
            );
        }
    }

    /// A blob claiming an array member count above `cardinality-estimator`'s own `MAX_CAPACITY`
    /// (128, mirrored here as `CE_ARRAY_MAX_CAPACITY`) must be rejected by `validate_members_len`
    /// before `Vec::with_capacity` ever allocates for it -- mirrors `Representation::try_from`'s own
    /// `ArrayRepresentationInvalid` check, just enforced earlier.
    #[test]
    fn hyperloglog_from_bytes_rejects_array_count_over_max_capacity() {
        let count = CE_ARRAY_MAX_CAPACITY as u32 + 1;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(CE_REPRESENTATION_ARRAY as u64).to_le_bytes());
        bytes.push(1); // members presence: Some
        bytes.extend_from_slice(&count.to_le_bytes());
        for _ in 0..count {
            bytes.extend_from_slice(&0u32.to_le_bytes());
        }
        assert!(HyperLogLog::from_bytes(&bytes).is_err());
    }

    /// A blob claiming an HLL member count off by one from the crate's fixed `HLL_SLICE_LEN`
    /// (mirrored here as `CE_HLL_SLICE_LEN`) must likewise be rejected before allocating -- mirrors
    /// `Representation::try_from`'s own `HllRepresentationInvalid` check.
    #[test]
    fn hyperloglog_from_bytes_rejects_hll_count_off_by_one() {
        for bad_len in [CE_HLL_SLICE_LEN - 1, CE_HLL_SLICE_LEN + 1] {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&(CE_REPRESENTATION_HLL as u64).to_le_bytes());
            bytes.push(1); // members presence: Some
            bytes.extend_from_slice(&(bad_len as u32).to_le_bytes());
            for _ in 0..bad_len {
                bytes.extend_from_slice(&0u32.to_le_bytes());
            }
            assert!(
                HyperLogLog::from_bytes(&bytes).is_err(),
                "length {bad_len} should be rejected"
            );
        }
    }

    /// `CE_HLL_SLICE_LEN` mirrors `cardinality_estimator::hyperloglog::HyperLogLog::<P,
    /// W>::HLL_SLICE_LEN`, a `pub(crate)` constant in that crate this module can't reference
    /// directly -- recomputed here from the same formula (`M * W / 32 + 3`, `M = 1 << P`) with this
    /// crate's pinned instantiation's default `P = 12, W = 6`, rather than just restating the
    /// literal, so a `cardinality-estimator` upgrade that changes either constant (or the defaults)
    /// is caught here rather than silently mis-sizing every HLL-representation `from_bytes` call.
    #[test]
    fn hll_slice_len_matches_upstream_constant() {
        const P: usize = 12;
        const W: usize = 6;
        let m = 1usize << P;
        assert_eq!(CE_HLL_SLICE_LEN, m * W / 32 + 3);
    }
}
