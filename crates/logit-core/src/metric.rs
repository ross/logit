use crate::interner::Symbol;
use crate::trace::TraceRef;
use crate::AttrMap;
use smallvec::SmallVec;

/// Whether a series reports increments since the last report (`Delta`) or a running total since a
/// fixed start time (`Cumulative`): OTLP's `AggregationTemporality`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temporality {
    Delta,
    Cumulative,
}

impl Temporality {
    /// Every variant's lowercase name, in variant order: what the Lua API and `stdio_out` render
    /// and accept.
    pub const NAMES: [&'static str; 2] = ["delta", "cumulative"];

    /// The lowercase name the Lua API and `stdio_out` render this temporality as.
    pub fn as_str(self) -> &'static str {
        match self {
            Temporality::Delta => "delta",
            Temporality::Cumulative => "cumulative",
        }
    }

    /// The inverse of [`Temporality::as_str`]: lowercase only, no case folding or aliases.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "delta" => Temporality::Delta,
            "cumulative" => Temporality::Cumulative,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MetricRecord {
    pub name: Symbol,
    pub unit: Option<Symbol>,
    pub description: Option<Symbol>,
    /// Unix nanoseconds this series started accumulating at; `0` means unknown, OTLP's convention
    /// for an absent `start_time_unix_nano`, which avoids an `Option<i64>` here.
    pub start_timestamp: i64,
    /// Empty on the common path, where it allocates nothing.
    pub exemplars: Vec<Exemplar>,
    /// OTLP `DataPointFlags` bitmask, `0` by default.
    ///
    /// A point flagged `NO_RECORDED_VALUE` ([`MetricRecord::FLAG_NO_RECORDED_VALUE`], checked by
    /// [`MetricRecord::is_no_recorded_value`]) is kept as a flagged record carrying its kind's
    /// default value. Two wires have their own no-value concept and carry it: `otlp_out`
    /// re-encodes the point unchanged (the `otlp_in -> otlp_out` fixed point
    /// `docs/adr/lossless-transit.md` requires), and `collectd_out` writes a flagged `Gauge` as a
    /// GAUGE `NaN` (`docs/adr/collectd-binary-relay.md`'s "NaN is a flagged point, not a dropped
    /// one"). Every other sink, and `aggregate`'s fold, must treat a flagged record as carrying no
    /// reading: skip it (counted) at a sink, pass it through unmerged (counted) at `aggregate`.
    /// Folding in the default numeric payload would fabricate a sample. `docs/known-gaps.md`'s
    /// cross-protocol table has the per-sink summary.
    ///
    /// Occupies padding after the three `Symbol`s, so it costs [`MetricRecord`] no size
    /// (`crates/logit-core/tests/type_sizes.rs`).
    pub flags: u32,
    pub kind: MetricKind,
}

impl MetricRecord {
    /// OTLP `DataPointFlags::FLAG_NO_RECORDED_VALUE` (bit 0): the numeric payload is absent, not
    /// a real `0`/empty reading.
    pub const FLAG_NO_RECORDED_VALUE: u32 = 1 << 0;

    /// A record with only a name and a kind: no unit or description, `start_timestamp` `0`
    /// (unknown), no exemplars, no flags.
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

    /// Whether this record carries `FLAG_NO_RECORDED_VALUE`; [`Self::flags`] says who must check
    /// it and what to do.
    pub fn is_no_recorded_value(&self) -> bool {
        self.flags & Self::FLAG_NO_RECORDED_VALUE != 0
    }
}

/// Metric kinds are mergeable: in the split-collection topology (`docs/OVERVIEW.md`) two edge
/// nodes' aggregates may be combined downstream, and that must be exact where the math allows
/// (`Sum`, `Gauge`, `Set`) and correctly error-bounded where it can't (`Distribution`). See
/// `docs/design/data-model.md`.
///
/// `Samples`/`SetMembers` carry raw observations as a protocol like statsd hands them over; a
/// decoder never pre-summarizes (`docs/adr/lossless-transit.md`'s "summarization is opt-in and
/// named" rule). `Distribution`/`Set` are the summarized counterparts, produced only by
/// `aggregate`.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricKind {
    /// A counter is `Sum { temporality: Delta, monotonic: true }`; see [`MetricKind::counter`].
    Sum(Sum),
    Gauge(f64),
    /// An unresolved relative adjustment to a gauge's previous value (statsd's leading `+`/`-`).
    /// Must never reach a sink: `aggregate`, the only component holding the running gauge value,
    /// resolves it into a [`MetricKind::Gauge`] (`docs/adr/relative-gauge-adjustments.md`).
    GaugeDelta(f64),
    /// Raw observations: statsd `ms`/`h`/`d`, and `kv_metrics`' `distributions:` entries.
    Samples(Samples),
    /// Produced only by `aggregate`, merging a run of [`MetricKind::Samples`].
    Distribution(DdSketch),
    /// Raw set members, as statsd `s` arrives.
    SetMembers(Vec<bytes::Bytes>),
    /// Produced only by `aggregate`, merging a run of [`MetricKind::SetMembers`].
    Set(HyperLogLog),
    /// Explicit bucket bounds, e.g. a Prometheus scrape or an OTLP `HistogramDataPoint`.
    Histogram(Histogram),
    /// OTLP/Prometheus-native base-2 exponential bucketing. Kept distinct from [`Histogram`]
    /// rather than materialized into explicit buckets so `otlp_in -> otlp_out` is a fixed point.
    ExponentialHistogram(ExpHistogram),
    /// Pre-computed quantiles, e.g. a Prometheus summary or an OTLP `SummaryDataPoint`.
    Summary(Summary),
}

impl MetricKind {
    /// A monotonic delta sum: a counter increment.
    pub fn counter(v: f64) -> Self {
        MetricKind::Sum(Sum { value: v, temporality: Temporality::Delta, monotonic: true })
    }

    /// The snake_case variant name the Lua API and `stdio_out` render as `kind`. There is no
    /// `from_name`: a name alone can't construct a payload.
    pub fn name(&self) -> &'static str {
        match self {
            MetricKind::Sum(_) => "sum",
            MetricKind::Gauge(_) => "gauge",
            MetricKind::GaugeDelta(_) => "gauge_delta",
            MetricKind::Samples(_) => "samples",
            MetricKind::Distribution(_) => "distribution",
            MetricKind::SetMembers(_) => "set_members",
            MetricKind::Set(_) => "set",
            MetricKind::Histogram(_) => "histogram",
            MetricKind::ExponentialHistogram(_) => "exponential_histogram",
            MetricKind::Summary(_) => "summary",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sum {
    pub value: f64,
    pub temporality: Temporality,
    pub monotonic: bool,
}

/// The largest inline capacity that keeps [`MetricKind`] at 176 bytes -- now [`Samples`]'s own
/// footprint plus its discriminant, not the size of its inlined [`DdSketch`] as before (measured).
///
/// `Distribution(DdSketch)` no longer sets the bound: the hand-rolled [`DdSketch`] is 128 bytes,
/// down from a wrapped `sketches_ddsketch::DDSketch`'s 176, which used to niche-fill the tag into
/// spare bit patterns inside its layout. `Samples` has no such niche. Under the workspace's
/// smallvec `union` feature, `Samples` costs `N * 8 + 16` bytes: `N = 20` makes it 176 and grows
/// `MetricKind` to 184 for the tag, while `N = 19` makes it 168 and leaves room for the tag; `N`
/// stays 19 because growing it grows every metric. `crates/logit-core/tests/type_sizes.rs`
/// asserts both sizes.
pub const SAMPLES_INLINE: usize = 19;

#[derive(Debug, Clone, PartialEq)]
pub struct Samples {
    pub values: SmallVec<[f64; SAMPLES_INLINE]>,
    pub sample_rate: f64,
}

impl Samples {
    /// Upper bound on [`Samples::weight`]: a `@0.0001` rate would otherwise turn one observation
    /// into ten thousand, and the rate is attacker-influenced wire input.
    pub const MAX_WEIGHT: u64 = 1000;

    /// Unsampled: `sample_rate` is `1.0`.
    pub fn new(values: impl IntoIterator<Item = f64>) -> Self {
        Samples { values: SmallVec::from_iter(values), sample_rate: 1.0 }
    }

    /// How many observations each value stands for: `round(1 / sample_rate)`, clamped to
    /// `[1, MAX_WEIGHT]`.
    ///
    /// A non-finite or non-positive rate (nothing upstream validates it; the native decoder reads
    /// a bare `f64`) degrades to `1`, never `0`: `NaN as u64` is `0`, and
    /// [`DdSketch::add_weighted`] with `0` is a no-op, so every observation would vanish.
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

    /// Whether [`Samples::weight`] clamps this record: `round(1 / sample_rate)` exceeds
    /// [`Samples::MAX_WEIGHT`] and there is at least one value to under-weight. A rate of exactly
    /// `1 / MAX_WEIGHT` (`@0.001`) isn't clamped, and neither is a non-finite or non-positive rate,
    /// which `weight` degrades to `1` rather than clamps. Consumers that report a clamp decide it
    /// here, not by comparing `weight()` against `MAX_WEIGHT`.
    pub fn is_clamped(&self) -> bool {
        !self.values.is_empty()
            && self.sample_rate.is_finite()
            && self.sample_rate > 0.0
            && (1.0 / self.sample_rate).round() > Self::MAX_WEIGHT as f64
    }

    /// Sketches these observations into a fresh [`DdSketch`], each weighted by
    /// [`Samples::weight`]. Every consumer that summarizes a `Samples` (`aggregate`, and the OTLP,
    /// Prometheus, and Graphite encoders) goes through this, so they agree by construction.
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

/// Fixed-bucket histogram. Each `(bound, count)` is that bucket's own count, not a cumulative
/// total up to `bound` (unlike Prometheus's `le` buckets).
#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    pub buckets: Vec<(f64, u64)>,
    pub temporality: Temporality,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// OTLP's base-2 exponential histogram, carried 1:1. `positive`/`negative` are each
/// `(offset, bucket_counts)`, mirroring `ExponentialHistogramDataPoint.positive`/`.negative`.
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

/// Pre-computed `(quantile, value)` pairs.
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    pub quantiles: Vec<(f64, f64)>,
    pub count: u64,
    pub sum: f64,
}

/// An OTLP exemplar: one observation behind a metric point, the trace it happened under, and the
/// attributes filtered out of the point's own set.
#[derive(Debug, Clone, PartialEq)]
pub struct Exemplar {
    pub timestamp: i64,
    pub value: f64,
    pub trace: Option<TraceRef>,
    pub filtered_attributes: AttrMap,
}

pub use crate::sketch::DdSketch;

/// A mergeable HyperLogLog cardinality estimator wrapping
/// `cardinality_estimator::CardinalityEstimator<[u8]>`.
///
/// `merge` is a real union of two independently built estimators, which `Set` needs for the same
/// reason [`DdSketch`] needs its error bound: the split-collection topology (`docs/OVERVIEW.md`)
/// combines edge nodes' aggregates downstream. See `docs/design/data-model.md`. Don't replace it
/// with a non-mergeable shortcut.
///
/// **`from_bytes` depends on a capacity invariant as well as a byte layout.**
/// `cardinality-estimator` 1.0.3's `Array::from_vec` frees its buffer with a `Layout` computed
/// from a rounded length; a `Vec` with more spare capacity than that is undefined behavior on
/// drop. `HllBytesReader`'s size hint prevents it (its doc has the mechanism), so don't change
/// that hint without reading it. `docs/known-gaps.md` tracks the upstream bug.
///
/// **Serialization** drives the wrapped type's `serde` impl (the `with_serde` feature; the crate
/// has no `to_bytes` and private fields) through the small hand-rolled byte codec below, built
/// for its one `(data, members: Option<Vec<u32>>)` shape. The bytes are tied to the locked
/// `cardinality-estimator` version, with no cross-version guarantee: unlike
/// [`DdSketch::to_bytes`], this isn't an interchange format.
pub struct HyperLogLog(cardinality_estimator::CardinalityEstimator<[u8]>);

impl HyperLogLog {
    pub fn new() -> Self {
        HyperLogLog(cardinality_estimator::CardinalityEstimator::new())
    }

    /// Adds one member. Re-inserting a member never inflates the estimate.
    pub fn insert(&mut self, member: &[u8]) {
        self.0.insert(member);
    }

    /// Unions `other` into `self`: shared members aren't double-counted, as summing two estimates
    /// would.
    pub fn merge(&mut self, other: &HyperLogLog) {
        self.0.merge(&other.0);
    }

    /// The estimated number of distinct members; `0` when empty.
    pub fn estimate(&self) -> u64 {
        self.0.estimate() as u64
    }

    /// The estimator's memory footprint in bytes (`CardinalityEstimator::size_of`).
    pub fn heap_bytes(&self) -> u64 {
        self.0.size_of() as u64
    }

    /// Serializes in the codec's wire layout (see the codec section below), with the leading
    /// `data` word canonicalized so [`HyperLogLog`]'s `PartialEq` can compare bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = HllBytesWriter::default();
        serde::Serialize::serialize(&self.0, &mut out)
            .expect("HyperLogLog encoding writes to a Vec<u8> and never fails");
        let mut bytes = out.0;
        // When a `members` list follows (presence byte 8 is `1`: array or HLL representation),
        // `data`'s low 2 bits are the representation tag and the rest is a raw heap pointer that
        // `Representation::try_from` discards on decode. Zero the pointer bits so equal estimators
        // serialize identically regardless of allocation address. In the small representation
        // (presence `0`), `data` is the whole content and stays untouched.
        if bytes.get(8) == Some(&1) {
            bytes[0] &= 0x3;
            for b in &mut bytes[1..8] {
                *b = 0;
            }
        }
        bytes
    }

    /// The inverse of [`HyperLogLog::to_bytes`]. Fails on a truncated or malformed blob, on
    /// bytes left after it, and on an HLL representation whose zero-register count exceeds its
    /// register count (upstream's `estimate` computes `M - zeros`). The harmonic sum in `data[1]`
    /// is taken as written: it's an `f32` accumulated one register update at a time, so
    /// recomputing it from the registers wouldn't reproduce the bytes, and a bad one skews the
    /// estimate without panicking.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, HllDecodeError> {
        let mut reader = HllBytesReader::new(bytes);
        let inner: cardinality_estimator::CardinalityEstimator<[u8]> =
            serde::Deserialize::deserialize(&mut reader)?;
        // Wrapped before the checks below, so an early return drops it through upstream's `Drop`.
        let hll = HyperLogLog(inner);
        if !reader.bytes.is_empty() {
            return Err(HllDecodeError(format!(
                "{} trailing bytes after the blob",
                reader.bytes.len()
            )));
        }
        if let Some(zeros) = hll_zero_register_count(bytes) {
            if zeros as usize > CE_HLL_REGISTERS {
                return Err(HllDecodeError(format!(
                    "HLL zero-register count {zeros} exceeds the {CE_HLL_REGISTERS} registers"
                )));
            }
        }
        Ok(hll)
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

// `CardinalityEstimator` doesn't implement `Debug`; summarize as the estimate.
impl std::fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog").field("estimate", &self.estimate()).finish()
    }
}

/// Compares [`HyperLogLog::to_bytes`], which strips the allocation pointer from the `data` word
/// so equal estimators produce equal bytes.
///
/// Still insertion-order-dependent: the small `data` encoding and the array/HLL `members` order
/// aren't canonical, so the same members inserted in a different order can compare unequal while
/// [`HyperLogLog::estimate`] agrees. Tests that check equality control insertion order.
impl PartialEq for HyperLogLog {
    fn eq(&self, other: &Self) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

/// A blob [`HyperLogLog::from_bytes`] can't decode: truncated, out-of-range, or a shape a
/// different `cardinality-estimator` version wrote.
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

// The `Serializer` needs a concrete error type too, though writing to a `Vec<u8>` never fails.
impl serde::ser::Error for HllDecodeError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        HllDecodeError(msg.to_string())
    }
}

// -- A minimal hand-rolled `serde` codec for `CardinalityEstimator`'s tuple shape --------------
//
// `CardinalityEstimator`'s `serde` impl always writes one 2-tuple, `(data: usize, members:
// Option<Vec<u32>>)`: `None` for the small representation, `Some` for array or HLL. Every other
// `Serializer`/`Deserializer` method returns `HllDecodeError`; only a `cardinality-estimator`
// upgrade that changes that shape could reach one.
//
// Wire layout (little-endian, read back only by this pair):
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

// `cardinality_estimator`'s representation tags in `data`'s low 2 bits (`pub(crate)` upstream).
// Only the two that carry a `members` list matter here; small (`0`) never reaches
// `deserialize_seq` in a well-formed blob.
const CE_REPRESENTATION_ARRAY: u8 = 1;
const CE_REPRESENTATION_HLL: u8 = 3;

// Mirrors `cardinality_estimator::array::MAX_CAPACITY` (`pub(crate)` upstream):
// `Representation::try_from` rejects an array length outside `3..=128`, and
// `validate_members_len` rejects the same range before allocating.
const CE_ARRAY_MAX_CAPACITY: usize = 128;

// Mirror upstream's `pub(crate)` `HyperLogLog::M` and `HLL_SLICE_LEN` for
// `CardinalityEstimator<[u8]>`'s default `P = 12, W = 6`: `M = 1 << P` registers, and a slice of
// `M * W / 32 + 3` words (zero-register count, harmonic sum, packed registers, one spare).
// `hll_slice_len_matches_what_upstream_serializes` checks the slice length against a blob
// upstream wrote; an upgrade that changes `P`, `W`, or the layout must update these in lockstep.
const CE_HLL_P: usize = 12;
const CE_HLL_W: usize = 6;
const CE_HLL_REGISTERS: usize = 1 << CE_HLL_P;
const CE_HLL_SLICE_LEN: usize = CE_HLL_REGISTERS * CE_HLL_W / 32 + 3;

// Where `members` starts in a blob: the 8-byte `data` word, the presence byte, and the 4-byte
// length (the codec's wire layout above).
const HLL_MEMBERS_OFFSET: usize = 8 + 1 + 4;

/// The HLL representation's `data[0]`, its zero-register count, read from a blob that already
/// decoded; `None` for the small and array representations.
fn hll_zero_register_count(bytes: &[u8]) -> Option<u32> {
    let tag = bytes.first()? & 0x3;
    if tag != CE_REPRESENTATION_HLL || bytes.get(8) != Some(&1) {
        return None;
    }
    let word = bytes.get(HLL_MEMBERS_OFFSET..HLL_MEMBERS_OFFSET + 4)?;
    Some(u32::from_le_bytes(word.try_into().expect("4 bytes")))
}

/// The deserializer behind [`HyperLogLog::from_bytes`], which works around an allocation-layout
/// bug in `cardinality-estimator` 1.0.3.
///
/// Upstream's `Array::from_vec(vec, len)` resizes the `Vec` to
/// `cap = len.next_power_of_two().max(vec.len())`, leaks it with `mem::forget`, and later frees
/// it with `Box::from_raw` of `cap` elements, so the dealloc `Layout` comes from `cap`, not from
/// the `Vec`'s real capacity. serde's `Vec<T>` deserializer allocates
/// `Vec::with_capacity(size_hint)` before reading elements; with the true count as the hint, three
/// members can come back with usable capacity 6, `resize` to 4 doesn't reallocate, and a
/// 6-element allocation is freed as 4: undefined behavior that rarely crashes (Miri and ASan
/// catch it). The HLL representation doesn't round, but the same requested-versus-actual capacity
/// mismatch applies.
///
/// The fix: `deserialize_seq` reports the capacity upstream will free as the size hint
/// (`len.next_power_of_two()` for array, `CE_HLL_SLICE_LEN` for HLL), so the first allocation is
/// already that size and `resize` never reallocates. That needs the representation, which
/// `deserialize_u64` stashes in `tag` when it reads `data`. `validate_members_len` is the other
/// half: it bounds the untrusted length before any allocation.
///
/// The upstream fix would be `into_boxed_slice()` (or `shrink_to_fit()`) in `Array::from_vec`.
/// No test here would notice that fix landing: the workaround stays correct either way.
struct HllBytesReader<'a> {
    bytes: &'a [u8],
    /// `data`'s representation tag, set by `deserialize_u64`. `data` is the wire shape's only
    /// `u64` and always decodes before `members`, so `deserialize_seq` never sees `None` for a
    /// well-formed blob.
    tag: Option<u8>,
}

/// Rejects a members-list length that `Representation::try_from` would also reject (array
/// `3..=128`, HLL `== HLL_SLICE_LEN`) before any `Vec` is allocated, so a hostile length fails
/// cleanly instead of attempting a huge allocation.
///
/// Returns the `Vec::with_capacity` hint `HllBytesReader` needs: the rounded capacity for array,
/// the length itself for HLL.
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

/// A `SeqAccess` with a known element count, serving both the outer tuple and the members
/// `Vec<u32>`.
struct HllSeqAccess<'a, 'b> {
    reader: &'b mut HllBytesReader<'a>,
    remaining: usize,
    /// What `size_hint` reports. For the members `Vec<u32>` it's `validate_members_len`'s rounded
    /// capacity, not `remaining`, per `HllBytesReader`'s doc; for the tuple it's moot.
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
        // `data` is the only `u64`: stash its tag for `deserialize_seq` to size `members`.
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
        // The only sequence is `members`. Reporting the unrounded `len` as the hint is unsound;
        // see `HllBytesReader`.
        let tag = self.tag;
        let cap = validate_members_len(tag, len)?;
        visitor.visit_seq(HllSeqAccess { reader: self, remaining: len, cap })
    }
    fn deserialize_tuple<V: serde::de::Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, HllDecodeError> {
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

    #[test]
    fn temporality_names_round_trip_in_variant_order() {
        let variants = [Temporality::Delta, Temporality::Cumulative];
        for (name, v) in Temporality::NAMES.iter().zip(variants) {
            assert_eq!(v.as_str(), *name);
            assert_eq!(Temporality::from_name(v.as_str()), Some(v));
        }
        assert_eq!(Temporality::from_name("Delta"), None);
    }

    #[test]
    fn metric_kind_name_covers_every_variant() {
        let cases = [
            (MetricKind::counter(1.0), "sum"),
            (MetricKind::Gauge(1.0), "gauge"),
            (MetricKind::GaugeDelta(1.0), "gauge_delta"),
            (MetricKind::Samples(Samples::new([1.0])), "samples"),
            (MetricKind::Distribution(DdSketch::new()), "distribution"),
            (MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"a")]), "set_members"),
            (MetricKind::Set(HyperLogLog::new()), "set"),
            (
                MetricKind::Histogram(Histogram {
                    buckets: vec![(1.0, 1)],
                    temporality: Temporality::Delta,
                    sum: None,
                    min: None,
                    max: None,
                }),
                "histogram",
            ),
            (
                MetricKind::ExponentialHistogram(ExpHistogram {
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: (0, vec![]),
                    negative: (0, vec![]),
                    temporality: Temporality::Delta,
                    count: 0,
                    sum: None,
                    min: None,
                    max: None,
                }),
                "exponential_histogram",
            ),
            (MetricKind::Summary(Summary { quantiles: vec![], count: 0, sum: 0.0 }), "summary"),
        ];
        for (kind, expected) in cases {
            assert_eq!(kind.name(), expected);
        }
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

    /// The weight is never `0` (which would discard every observation), NaN rate included.
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

    #[test]
    fn samples_is_clamped_only_past_max_weight_and_with_values() {
        let with_rate =
            |rate: f64| Samples { values: SmallVec::from_slice(&[1.0]), sample_rate: rate };
        assert!(!with_rate(0.001).is_clamped(), "@0.001 is weight 1000, not clamped");
        assert!(!with_rate(0.0010005).is_clamped(), "rounds to 1000 (999.5)");
        assert!(with_rate(0.00099).is_clamped(), "rounds to 1010");
        assert!(with_rate(1e-320).is_clamped(), "1 / rate overflows to infinity");
        for rate in [1.0, 0.5, 2.0, 0.0, -0.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(!with_rate(rate).is_clamped(), "rate {rate}");
        }
        let empty = Samples { values: SmallVec::new(), sample_rate: 0.0001 };
        assert!(!empty.is_clamped(), "no value is under-weighted");
    }

    /// A NaN rate sketches every value once rather than dropping them.
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

    /// 1,000 members estimate within 5%, and re-inserting them doesn't move the estimate.
    #[test]
    fn hyperloglog_estimate_is_accurate_within_a_few_percent_on_1k_distinct_members() {
        let mut hll = HyperLogLog::new();
        for i in 0..1000u32 {
            hll.insert(&i.to_le_bytes());
        }
        let estimate = hll.estimate();
        let relative_error = (estimate as f64 - 1000.0).abs() / 1000.0;
        assert!(relative_error <= 0.05, "estimate {estimate} is more than 5% away from 1000");

        for i in 0..1000u32 {
            hll.insert(&i.to_le_bytes());
        }
        assert_eq!(hll.estimate(), estimate, "re-inserting existing members must be a no-op");
    }

    /// Disjoint sets add under merge; overlapping members aren't double-counted.
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

    /// The empty (small-representation, no `members`) estimator round-trips.
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

    /// Two independent decodes of an array (10) or HLL (200) blob serialize identically.
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

    /// `to_bytes` is a fixed point across a round trip for small (0), array (10), and HLL (200).
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

    /// The unassigned tag `2`, and a small tag with a `members` list, are errors, not panics.
    #[test]
    fn hyperloglog_from_bytes_rejects_bad_representation_tags() {
        let mut invalid_tag = vec![0u8; 9];
        invalid_tag[0] = 2;
        assert!(HyperLogLog::from_bytes(&invalid_tag).is_err());

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

    // -- `HllBytesReader`'s capacity-invariant workaround ------------------------------------

    /// Non-power-of-two array counts and HLL counts round-trip. `script/unsafe-check miri` runs it
    /// under `-Zmiri-disable-stacked-borrows -Zmiri-permissive-provenance`, where it catches the
    /// `Layout` UB if `HllBytesReader`'s size hint regresses; under Miri's default Stacked Borrows,
    /// or Tree Borrows, it fails first inside upstream (`docs/adr/out-of-ci-unsafe-verification.md`).
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

    /// An array count above `CE_ARRAY_MAX_CAPACITY` is rejected.
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

    /// An HLL count off by one from `CE_HLL_SLICE_LEN` is rejected.
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

    /// A hand-built blob: the `data` word, then the optional `members` list.
    fn hll_blob(data: u64, members: Option<&[u32]>) -> Vec<u8> {
        let mut out = data.to_le_bytes().to_vec();
        match members {
            None => out.push(0),
            Some(m) => {
                out.push(1);
                out.extend_from_slice(&(m.len() as u32).to_le_bytes());
                for v in m {
                    out.extend_from_slice(&v.to_le_bytes());
                }
            }
        }
        out
    }

    /// The HLL representation's `data[0]` counts zero registers, and upstream's `estimate`
    /// computes `M - zeros`: a count past `M` underflows (a debug panic, a release estimate of 0)
    /// and survives every later merge into the window's accumulator.
    #[test]
    fn an_hll_zero_register_count_past_m_is_rejected() {
        let registers = CE_HLL_REGISTERS as u32;
        let with_zeros = |zeros: u32| {
            let mut members = hll_with(5000).to_bytes()[HLL_MEMBERS_OFFSET..]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| u32::from_le_bytes(*c))
                .collect::<Vec<_>>();
            members[0] = zeros;
            hll_blob(CE_REPRESENTATION_HLL as u64, Some(&members))
        };
        for zeros in [registers + 1, u32::MAX] {
            let err = HyperLogLog::from_bytes(&with_zeros(zeros)).expect_err("rejected");
            assert!(err.to_string().contains("zero-register count"), "{err}");
        }
        assert!(HyperLogLog::from_bytes(&with_zeros(registers)).is_ok());
    }

    /// A valid blob followed by anything is malformed, in every representation.
    #[test]
    fn trailing_bytes_after_an_hll_blob_are_rejected() {
        for n in [0u32, 1, 10, 5000] {
            let mut bytes = hll_with(n).to_bytes();
            bytes.push(0);
            let err = HyperLogLog::from_bytes(&bytes).expect_err("rejected");
            assert!(err.to_string().contains("trailing"), "{n} members: {err}");
        }
    }

    /// `CE_HLL_SLICE_LEN` matches the members list upstream's own `Serialize` writes for an HLL
    /// representation, and `CE_HLL_REGISTERS` the zero-register count of a fresh one.
    #[test]
    fn hll_slice_len_matches_what_upstream_serializes() {
        let bytes = hll_with(5000).to_bytes();
        assert_eq!(bytes[0] & 0x3, CE_REPRESENTATION_HLL);
        assert_eq!(bytes.len(), HLL_MEMBERS_OFFSET + CE_HLL_SLICE_LEN * 4);

        let mut one = HyperLogLog::new();
        for i in 0..200u32 {
            one.insert(&i.to_le_bytes());
        }
        let zeros = hll_zero_register_count(&one.to_bytes()).expect("HLL representation");
        assert!(zeros as usize <= CE_HLL_REGISTERS && zeros as usize > CE_HLL_REGISTERS - 200);
    }

    /// Pins the assumption `HllBytesReader`'s size hint rests on: serde_core 1.0.229's `Vec<T>`
    /// visitor allocates `Vec::with_capacity(size_hint::cautious::<T>(seq.size_hint()))`, and
    /// `cautious` caps that at 1 MiB (262,144 `u32`s), far above either representation's. So the
    /// members `Vec` has the capacity upstream later frees: `len.next_power_of_two()` for the
    /// array representation, `CE_HLL_SLICE_LEN` for HLL. A serde release that sizes the `Vec`
    /// differently fails here before it can reinstate the `Layout` UB.
    #[test]
    fn a_members_vec_deserialized_through_the_hll_reader_has_the_capacity_upstream_frees() {
        use serde::Deserialize;
        for n in [3u32, 4, 5, 7, 9, 17, 33, 65, 100, 127, 128, 5000] {
            let bytes = hll_with(n).to_bytes();
            let mut reader = HllBytesReader::new(&bytes);
            let (data, members): (usize, Option<Vec<u32>>) =
                Deserialize::deserialize(&mut reader).expect("valid blob");
            let members = members.expect("array or HLL representation");
            let expected = match (data & 0x3) as u8 {
                CE_REPRESENTATION_ARRAY => members.len().next_power_of_two(),
                CE_REPRESENTATION_HLL => CE_HLL_SLICE_LEN,
                tag => panic!("{n} members: tag {tag}"),
            };
            assert_eq!(members.capacity(), expected, "{n} members");
        }
        let bytes = hll_with(5000).to_bytes();
        let mut reader = HllBytesReader::new(&bytes);
        let (_, members): (usize, Option<Vec<u32>>) =
            Deserialize::deserialize(&mut reader).expect("valid blob");
        assert_eq!(members.expect("HLL representation").capacity(), 771);
    }

    /// Set-union laws on `estimate`: idempotent, commutative, associative, and a merge of two
    /// estimators estimates what one fed both populations does. Byte-level commutativity does not
    /// hold (the array representation keeps insertion order), which `HyperLogLog`'s `PartialEq`
    /// doc records; these compare estimates.
    mod properties {
        use super::*;
        use proptest::prelude::*;

        fn members() -> impl Strategy<Value = Vec<u32>> {
            prop_oneof![
                prop::collection::vec(any::<u32>(), 0..4),
                prop::collection::vec(any::<u32>(), 0..140),
                prop::collection::vec(any::<u32>(), 100..3000),
            ]
        }

        fn hll_of(members: &[u32]) -> HyperLogLog {
            let mut hll = HyperLogLog::new();
            for m in members {
                hll.insert(&m.to_le_bytes());
            }
            hll
        }

        fn union(a: &HyperLogLog, b: &HyperLogLog) -> HyperLogLog {
            let mut out = a.clone();
            out.merge(b);
            out
        }

        proptest! {
            #[test]
            fn hll_merge_is_idempotent_commutative_and_associative(
                a in members(),
                b in members(),
                c in members(),
            ) {
                let (a, b, c) = (hll_of(&a), hll_of(&b), hll_of(&c));
                prop_assert_eq!(union(&a, &a).estimate(), a.estimate());
                prop_assert_eq!(union(&a, &b).estimate(), union(&b, &a).estimate());
                prop_assert_eq!(
                    union(&union(&a, &b), &c).estimate(),
                    union(&a, &union(&b, &c)).estimate()
                );
            }

            #[test]
            fn hll_merge_estimates_what_inserting_both_does(a in members(), b in members()) {
                let both: Vec<u32> = a.iter().chain(&b).copied().collect();
                prop_assert_eq!(union(&hll_of(&a), &hll_of(&b)).estimate(), hll_of(&both).estimate());
            }
        }

        /// A rate drawn from every `f64` bit pattern, with the edges `weight` special-cases
        /// weighted in.
        fn any_rate() -> impl Strategy<Value = f64> {
            prop_oneof![
                4 => any::<u64>().prop_map(f64::from_bits),
                1 => prop_oneof![
                    Just(0.0),
                    Just(-0.0),
                    Just(f64::NAN),
                    Just(f64::INFINITY),
                    Just(f64::MIN_POSITIVE),
                    Just(f64::from_bits(1)),
                    Just(0.001),
                    Just(0.00099),
                    Just(0.0010005),
                ],
                2 => 1e-5..=1.5f64,
            ]
        }

        fn finite_value() -> impl Strategy<Value = f64> {
            prop_oneof![Just(0.0), -1e9..1e9f64]
        }

        fn any_value() -> impl Strategy<Value = f64> {
            prop_oneof![
                4 => finite_value(),
                1 => prop_oneof![Just(f64::NAN), Just(f64::INFINITY), Just(f64::NEG_INFINITY)],
            ]
        }

        // `Samples::weight` is the defense the sample rate needs: it's wire input, and the native
        // decoder reads a bare `f64`.
        proptest! {
            #[test]
            fn weight_is_within_one_and_max_weight_for_every_rate(rate in any_rate()) {
                let s = Samples { values: SmallVec::from_slice(&[1.0]), sample_rate: rate };
                prop_assert!((1..=Samples::MAX_WEIGHT).contains(&s.weight()), "rate {rate:e}");
                if s.is_clamped() {
                    prop_assert_eq!(s.weight(), Samples::MAX_WEIGHT);
                }
            }

            #[test]
            fn sketch_counts_every_finite_value_at_its_weight(
                rate in any_rate(),
                values in prop::collection::vec(finite_value(), 0..40),
            ) {
                let s = Samples { values: SmallVec::from_vec(values), sample_rate: rate };
                prop_assert_eq!(s.sketch().count() as u64, s.values.len() as u64 * s.weight());
            }

            /// A non-finite value has no bin: `DdSketch::add_count` drops it, so the sketch counts
            /// only the finite ones. `aggregate` counts what it drops this way.
            #[test]
            fn sketch_drops_non_finite_values(
                rate in any_rate(),
                values in prop::collection::vec(any_value(), 0..40),
            ) {
                let s = Samples { values: SmallVec::from_vec(values), sample_rate: rate };
                let finite = s.values.iter().filter(|v| v.is_finite()).count() as u64;
                prop_assert_eq!(s.sketch().count() as u64, finite * s.weight());
            }
        }
    }
}
