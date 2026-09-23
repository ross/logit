//! `DdSketch`: a mergeable quantile sketch with Datadog's own bin mappings.
//!
//! Hand-rolled rather than wrapped (`sketches-ddsketch` until
//! [ADR `datadog-agent-and-intake-relay`](../../../docs/adr/datadog-agent-and-intake-relay.md)),
//! because a relay has to be bin-for-bin identical to what Datadog produces and consumes, and
//! Datadog uses two mappings that no published crate exposes:
//!
//! - **The Agent's metrics sketch** (`datadog-agent` `pkg/util/quantile`): `key = round_to_even(
//!   log_γ(v)) + bias`, γ = 1.015625 (`eps = 1/128`, doubled), `bias = 1 - floor(log_γ(1e-9))`,
//!   int16 keys with ±32767 reserved for ±∞, a sparse sorted bin list collapsed from the lowest
//!   key up at 4,096 bins, and an exact `cnt`/`min`/`max`/`sum`/`avg` summary. The wire form
//!   (`SketchPayload.Dogsketch`) carries no mapping parameters, so a receiver has to assume these.
//! - **The logarithmic DDSketch** (`sketches-go`, and the DDSketch protobuf APM stats carry):
//!   `index = floor(log_γ(v) + index_offset)` with `f64` counts, and the mapping on the wire.
//!
//! [`Mapping::agent`] is the default every sketch built in this process uses, so `aggregate`'s
//! output can be sent to Datadog as native sketches; [`Mapping::logarithmic`] is what a decoded
//! stats sketch keeps, so it relays exactly. A merge across mappings re-bins the incoming sketch
//! by each bin's representative value, a bounded-error normalization, never a failure.
//!
//! A quantile is the representative value of the bin holding the rank (see
//! [`DdSketch::quantile`] for why not the Agent's own interpolation), so the relative error is the
//! mapping's: `1 - 1/√γ` (0.78%) under the Agent mapping, `1 - 2/(1+γ)` under a logarithmic one.

use std::fmt;

/// Which of Datadog's two bin mappings a [`Mapping`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingKind {
    /// The Agent's metrics sketch, `pkg/util/quantile` -- see the module doc.
    Agent,
    /// `sketches-go`'s logarithmic mapping, the DDSketch protobuf form.
    Logarithmic,
}

/// A bin mapping: how a value becomes an integer key and back. Two sketches merge bin-for-bin
/// only when their mappings are equal.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Mapping {
    kind: MappingKind,
    gamma: f64,
    gamma_ln: f64,
    /// [`MappingKind::Agent`]: the integer bias, stored as `f64`. [`MappingKind::Logarithmic`]:
    /// `index_offset`, any real.
    offset: f64,
    bin_limit: u32,
}

impl Mapping {
    /// The Agent's `gamma.v`: `1 + 2 * (1/128)`.
    pub const AGENT_GAMMA: f64 = 1.015625;
    /// The Agent's `defaultBinLimit`.
    pub const AGENT_BIN_LIMIT: u32 = 4096;
    /// The Agent's `defaultMin`; smaller magnitudes land in the zero bin.
    pub const AGENT_MIN: f64 = 1e-9;
    /// The Agent's `maxKey`; `Self::AGENT_INF_KEY` is `InfKey(1)`.
    pub const AGENT_MAX_KEY: i32 = 32766;
    pub const AGENT_INF_KEY: i32 = 32767;

    /// The Agent's `Config.Default()`.
    pub fn agent() -> Self {
        // `math.Log1p(eps)` in the Agent, with `eps` already doubled.
        let gamma_ln = (1.0f64 / 64.0).ln_1p();
        let emin = (Self::AGENT_MIN.ln() / gamma_ln).floor();
        Mapping {
            kind: MappingKind::Agent,
            gamma: Self::AGENT_GAMMA,
            gamma_ln,
            offset: 1.0 - emin,
            bin_limit: Self::AGENT_BIN_LIMIT,
        }
    }

    /// `sketches-go`'s `NewLogarithmicMappingWithGamma(gamma, index_offset)` over a
    /// collapsing-lowest store of `bin_limit` bins. `gamma` must be greater than 1; anything
    /// else falls back to the Agent mapping, since a mapping that can't key a value has no use.
    pub fn logarithmic(gamma: f64, index_offset: f64, bin_limit: u32) -> Self {
        if !(gamma.is_finite() && gamma > 1.0 && index_offset.is_finite()) {
            return Self::agent();
        }
        Mapping {
            kind: MappingKind::Logarithmic,
            gamma,
            gamma_ln: gamma.ln(),
            offset: index_offset,
            bin_limit: bin_limit.max(2),
        }
    }

    pub fn kind(&self) -> MappingKind {
        self.kind
    }

    pub fn gamma(&self) -> f64 {
        self.gamma
    }

    /// The logarithmic form's `index_offset`; the Agent form's integer bias.
    pub fn index_offset(&self) -> f64 {
        self.offset
    }

    pub fn bin_limit(&self) -> u32 {
        self.bin_limit
    }

    /// The Agent's `norm.min`: `f64(1)`, the lowest non-zero bin's lower bound.
    fn agent_min(&self) -> f64 {
        self.gamma.powf(1.0 - self.offset)
    }

    /// Whether a magnitude is too small to key and belongs in the zero bin.
    fn is_zero(&self, magnitude: f64) -> bool {
        match self.kind {
            MappingKind::Agent => magnitude < self.agent_min(),
            MappingKind::Logarithmic => magnitude < Self::AGENT_MIN,
        }
    }

    /// The key for a positive, finite, non-zero magnitude.
    fn key(&self, magnitude: f64) -> i32 {
        let log_gamma = magnitude.ln() / self.gamma_ln;
        match self.kind {
            MappingKind::Agent => {
                let i = log_gamma.round_ties_even() + self.offset;
                if i > Self::AGENT_MAX_KEY as f64 {
                    Self::AGENT_INF_KEY
                } else if i < 1.0 {
                    1
                } else {
                    i as i32
                }
            }
            MappingKind::Logarithmic => {
                // `sketches-go`'s `Index`: truncation toward zero, minus one below zero -- not
                // quite `floor` (an exact negative integer goes one lower), reproduced as is.
                let index = log_gamma + self.offset;
                if index >= 0.0 {
                    index as i32
                } else {
                    (index as i32).saturating_sub(1)
                }
            }
        }
    }

    /// The lower bound of a key's bin: the Agent's `f64(k)`, `sketches-go`'s `LowerBound`.
    fn lower_bound(&self, key: i32) -> f64 {
        match self.kind {
            MappingKind::Agent => {
                if key >= Self::AGENT_INF_KEY {
                    f64::INFINITY
                } else {
                    self.gamma.powf(key as f64 - self.offset)
                }
            }
            MappingKind::Logarithmic => ((key as f64 - self.offset) * self.gamma_ln).exp(),
        }
    }

    /// The value a bin stands for when its contents are re-binned or summed: the Agent's `f64(k)`
    /// (its quantile interpolates upward from it), `sketches-go`'s `Value(index)`.
    fn representative(&self, key: i32) -> f64 {
        match self.kind {
            MappingKind::Agent => self.lower_bound(key),
            MappingKind::Logarithmic => {
                self.lower_bound(key) * (2.0 * self.gamma / (1.0 + self.gamma))
            }
        }
    }
}

/// One populated bin: a key under the sketch's [`Mapping`] and its count. Counts are `f64`
/// because the DDSketch protobuf's are (weighted inserts leave fractions); the Agent's `uint16`
/// counts are exact in it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bin {
    pub key: i32,
    pub count: f64,
}

/// A mergeable quantile sketch. See the module doc for the two mappings and why it's hand-rolled.
///
/// `PartialEq` is structural: same mapping, same bins, same summary. Two sketches fed the same
/// values in any order compare equal; two fed different values with identical bins differ by
/// `sum`, which is exact.
#[derive(Clone, PartialEq)]
pub struct DdSketch {
    mapping: Mapping,
    /// Ascending by key, one entry per key, every count positive.
    positive: Vec<Bin>,
    /// Keys of `|v|` for negative values, same invariants.
    negative: Vec<Bin>,
    zero_count: f64,
    count: f64,
    min: f64,
    max: f64,
    sum: f64,
    /// `false` once the summary was derived from bins (a decoded stats sketch carries none), and
    /// stays `false` through every merge that touches it.
    exact_stats: bool,
}

/// Initial capacity of a store's bin `Vec`, allocated on the first insert: 64 bins at 16 bytes is
/// the 1 KiB `sketches-ddsketch`'s 128-`u64` chunk cost, so the per-sketch allocation counts
/// `docs/design/memory.md` pins are unchanged.
const INITIAL_BINS: usize = 64;

impl fmt::Debug for DdSketch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DdSketch")
            .field("mapping", &self.mapping.kind)
            .field("count", &self.count)
            .field("bins", &(self.positive.len() + self.negative.len()))
            .finish()
    }
}

impl Default for DdSketch {
    fn default() -> Self {
        Self::new()
    }
}

impl DdSketch {
    /// An empty sketch under [`Mapping::agent`].
    pub fn new() -> Self {
        Self::with_mapping(Mapping::agent())
    }

    pub fn with_mapping(mapping: Mapping) -> Self {
        DdSketch {
            mapping,
            positive: Vec::new(),
            negative: Vec::new(),
            zero_count: 0.0,
            count: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            exact_stats: true,
        }
    }

    /// Rebuilds a sketch from decoded parts. Bins may arrive unsorted or with repeated keys; they
    /// are normalized, and the store is collapsed to the mapping's bin limit. With `stats` of
    /// `None` (the DDSketch protobuf carries none) the summary is derived from the bins and
    /// [`DdSketch::stats_exact`] reports `false`.
    pub fn from_parts(
        mapping: Mapping,
        positive: Vec<Bin>,
        negative: Vec<Bin>,
        zero_count: f64,
        stats: Option<SketchStats>,
    ) -> Self {
        let mut sketch = Self::with_mapping(mapping);
        sketch.positive = normalize_bins(positive, mapping.bin_limit);
        sketch.negative = normalize_bins(negative, mapping.bin_limit);
        sketch.zero_count =
            if zero_count.is_finite() && zero_count > 0.0 { zero_count } else { 0.0 };
        match stats {
            Some(s) if s.count.is_finite() && s.count >= 0.0 => {
                sketch.count = s.count;
                sketch.min = s.min;
                sketch.max = s.max;
                sketch.sum = s.sum;
                sketch.exact_stats = true;
            }
            _ => sketch.derive_stats(),
        }
        sketch
    }

    pub fn mapping(&self) -> &Mapping {
        &self.mapping
    }

    pub fn positive_bins(&self) -> &[Bin] {
        &self.positive
    }

    pub fn negative_bins(&self) -> &[Bin] {
        &self.negative
    }

    pub fn zero_count(&self) -> f64 {
        self.zero_count
    }

    pub fn add(&mut self, value: f64) {
        self.add_count(value, 1.0);
    }

    /// Adds `value` as `count` observations in constant time: what a sampled statsd line needs
    /// to extrapolate `100|ms|@0.1` into ten samples rather than one. `count == 0` is a no-op.
    pub fn add_weighted(&mut self, value: f64, count: u64) {
        self.add_count(value, count as f64);
    }

    /// Adds `value` with a fractional weight, the DDSketch protobuf's own count type. A
    /// non-finite value or a non-positive count is a no-op: a `NaN` observation has no bin, and
    /// silently counting it as zero would move every quantile.
    pub fn add_count(&mut self, value: f64, count: f64) {
        if !value.is_finite() || !(count.is_finite() && count > 0.0) {
            return;
        }
        self.insert(value, count);
        self.count += count;
        self.sum += value * count;
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }

    fn insert(&mut self, value: f64, count: f64) {
        let magnitude = value.abs();
        if self.mapping.is_zero(magnitude) {
            self.zero_count += count;
            return;
        }
        let key = self.mapping.key(magnitude);
        let store = if value > 0.0 { &mut self.positive } else { &mut self.negative };
        insert_bin(store, key, count, self.mapping.bin_limit);
    }

    /// Merges `other` into `self`. Same mapping: bin-for-bin, exactly. Different mapping: each of
    /// `other`'s bins is re-binned at its representative value, within the coarser mapping's
    /// relative-error bound, and the merged sketch keeps `self`'s mapping.
    pub fn merge(&mut self, other: &DdSketch) {
        if other.count == 0.0 && other.zero_count == 0.0 {
            return;
        }
        if self.mapping == other.mapping {
            self.positive = merge_bins(&self.positive, &other.positive, self.mapping.bin_limit);
            self.negative = merge_bins(&self.negative, &other.negative, self.mapping.bin_limit);
        } else {
            for bin in &other.positive {
                let key = self.mapping.key(other.mapping.representative(bin.key));
                insert_bin(&mut self.positive, key, bin.count, self.mapping.bin_limit);
            }
            for bin in &other.negative {
                let key = self.mapping.key(other.mapping.representative(bin.key));
                insert_bin(&mut self.negative, key, bin.count, self.mapping.bin_limit);
            }
        }
        self.zero_count += other.zero_count;
        self.count += other.count;
        self.sum += other.sum;
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        self.exact_stats &= other.exact_stats;
    }

    /// The number of observations, extrapolated weights included, rounded for a fractional
    /// count.
    pub fn count(&self) -> usize {
        self.count.round() as usize
    }

    /// The exact sum of every value added (`value * count` for weighted adds), `0.0` when empty;
    /// unlike a quantile it never goes through a bin. Derived from bin representatives, and so
    /// approximate, only when [`DdSketch::stats_exact`] is `false`.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    pub fn min(&self) -> Option<f64> {
        (self.count > 0.0).then_some(self.min)
    }

    pub fn max(&self) -> Option<f64> {
        (self.count > 0.0).then_some(self.max)
    }

    /// Whether `count`/`min`/`max`/`sum` were tracked from the observations themselves rather
    /// than derived from bins.
    pub fn stats_exact(&self) -> bool {
        self.exact_stats
    }

    /// The `q`-quantile, `None` when empty. `q` is clamped to `[0, 1]`; `0` and `1` answer with
    /// the exact minimum and maximum.
    ///
    /// Rank-based: the representative value of the bin holding the rank, walking the negative
    /// store from the most negative value, then the zero bin, then the positive store. The rank
    /// is `q * (count - 1)`, rounded to even under the Agent mapping (its `rank()`), truncated
    /// under the logarithmic one (`sketches-go`). The Agent's own `Sketch.Quantile` instead
    /// interpolates upward from `f64(k)` to `f64(k) * γ`, which can miss a single-bin population
    /// by `γ^1.5 - 1`; it isn't what Datadog's backend evaluates for a shipped sketch (that code
    /// is closed), so the bin center, whose `1 - 1/√γ` bound the Agent documents, is used here.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count <= 0.0 || q.is_nan() {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        if q <= 0.0 {
            return Some(self.min);
        }
        if q >= 1.0 {
            return Some(self.max);
        }
        let rank = q * (self.count - 1.0);
        let rank = match self.mapping.kind {
            MappingKind::Agent => rank.round_ties_even(),
            MappingKind::Logarithmic => rank,
        };
        let negative_count: f64 = self.negative.iter().map(|b| b.count).sum();
        Some(if rank < negative_count {
            let key = key_at_rank(&self.negative, negative_count - 1.0 - rank);
            -self.mapping.representative(key)
        } else if rank < negative_count + self.zero_count {
            0.0
        } else {
            let key = key_at_rank(&self.positive, rank - negative_count - self.zero_count);
            self.mapping.representative(key)
        })
    }

    fn derive_stats(&mut self) {
        let mut count = self.zero_count;
        let mut sum = 0.0;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        if self.zero_count > 0.0 {
            min = 0.0;
            max = 0.0;
        }
        for bin in &self.positive {
            let v = self.mapping.representative(bin.key);
            count += bin.count;
            sum += v * bin.count;
            min = min.min(v);
            max = max.max(v);
        }
        for bin in &self.negative {
            let v = -self.mapping.representative(bin.key);
            count += bin.count;
            sum += v * bin.count;
            min = min.min(v);
            max = max.max(v);
        }
        self.count = count;
        self.sum = sum;
        self.min = min;
        self.max = max;
        self.exact_stats = false;
    }

    /// This process's own byte form, the native wire format's `Distribution` payload
    /// (`docs/design/wire-protocol.md`): version, mapping, summary, then each store as
    /// zigzag-varint key deltas with `f64` counts. Not a Datadog wire format; `logit_proto`'s
    /// codecs build those from [`DdSketch::positive_bins`] and friends.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(48 + 12 * (self.positive.len() + self.negative.len()));
        out.push(BYTES_VERSION);
        match self.mapping.kind {
            MappingKind::Agent => out.push(0),
            MappingKind::Logarithmic => {
                out.push(1);
                out.extend_from_slice(&self.mapping.gamma.to_le_bytes());
                out.extend_from_slice(&self.mapping.offset.to_le_bytes());
                out.extend_from_slice(&self.mapping.bin_limit.to_le_bytes());
            }
        }
        out.push(self.exact_stats as u8);
        for v in [self.count, self.min, self.max, self.sum, self.zero_count] {
            out.extend_from_slice(&v.to_le_bytes());
        }
        write_bins(&mut out, &self.positive);
        write_bins(&mut out, &self.negative);
        out
    }

    /// The inverse of [`DdSketch::to_bytes`]. Fails only on a malformed blob, never on a value.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SketchDecodeError> {
        let mut r = Reader { bytes, pos: 0 };
        if r.u8()? != BYTES_VERSION {
            return Err(SketchDecodeError::Version);
        }
        let mapping = match r.u8()? {
            0 => Mapping::agent(),
            1 => {
                let gamma = r.f64()?;
                let offset = r.f64()?;
                let bin_limit = r.u32()?;
                Mapping::logarithmic(gamma, offset, bin_limit)
            }
            _ => return Err(SketchDecodeError::Malformed),
        };
        let exact_stats = r.u8()? != 0;
        let count = r.f64()?;
        let min = r.f64()?;
        let max = r.f64()?;
        let sum = r.f64()?;
        let zero_count = r.f64()?;
        let positive = read_bins(&mut r)?;
        let negative = read_bins(&mut r)?;
        if r.pos != bytes.len() {
            return Err(SketchDecodeError::Malformed);
        }
        let mut sketch = Self::from_parts(
            mapping,
            positive,
            negative,
            zero_count,
            Some(SketchStats { count, min, max, sum }),
        );
        sketch.exact_stats = exact_stats;
        Ok(sketch)
    }
}

/// The exact summary a Datadog Agent sketch carries beside its bins.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SketchStats {
    pub count: f64,
    pub min: f64,
    pub max: f64,
    pub sum: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SketchDecodeError {
    Version,
    Truncated,
    Malformed,
}

impl fmt::Display for SketchDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SketchDecodeError::Version => "unsupported DdSketch byte-format version",
            SketchDecodeError::Truncated => "truncated DdSketch bytes",
            SketchDecodeError::Malformed => "malformed DdSketch bytes",
        })
    }
}

impl std::error::Error for SketchDecodeError {}

const BYTES_VERSION: u8 = 1;

/// Adds `count` to `key`'s bin, keeping `store` sorted and unique, then collapses from the lowest
/// key up when the store exceeds `bin_limit`: the Agent's `trimLeft`, and the collapsing-lowest
/// dense store's policy.
fn insert_bin(store: &mut Vec<Bin>, key: i32, count: f64, bin_limit: u32) {
    match store.binary_search_by(|b| b.key.cmp(&key)) {
        Ok(i) => store[i].count += count,
        Err(i) => {
            if store.capacity() == 0 {
                store.reserve_exact(INITIAL_BINS);
            }
            store.insert(i, Bin { key, count });
            collapse(store, bin_limit);
        }
    }
}

fn collapse(store: &mut Vec<Bin>, bin_limit: u32) {
    let limit = bin_limit as usize;
    if store.len() <= limit {
        return;
    }
    let remove = store.len() - limit;
    let folded: f64 = store[..remove].iter().map(|b| b.count).sum();
    store[remove].count += folded;
    store.drain(..remove);
}

/// A sorted merge of two normalized stores into a fresh one.
fn merge_bins(a: &[Bin], b: &[Bin], bin_limit: u32) -> Vec<Bin> {
    if b.is_empty() {
        return a.to_vec();
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].key.cmp(&b[j].key) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(Bin { key: a[i].key, count: a[i].count + b[j].count });
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    collapse(&mut out, bin_limit);
    out
}

/// Sorts, folds repeated keys, drops empty or non-finite counts, and collapses.
fn normalize_bins(mut bins: Vec<Bin>, bin_limit: u32) -> Vec<Bin> {
    bins.retain(|b| b.count.is_finite() && b.count > 0.0);
    bins.sort_by_key(|b| b.key);
    let mut out: Vec<Bin> = Vec::with_capacity(bins.len());
    for bin in bins {
        match out.last_mut() {
            Some(last) if last.key == bin.key => last.count += bin.count,
            _ => out.push(bin),
        }
    }
    collapse(&mut out, bin_limit);
    out
}

/// `sketches-go`'s `KeyAtRank`: the first key whose cumulative count exceeds `rank`, else the
/// last key.
fn key_at_rank(store: &[Bin], rank: f64) -> i32 {
    let mut seen = 0.0;
    for bin in store {
        seen += bin.count;
        if seen > rank {
            return bin.key;
        }
    }
    store.last().map(|b| b.key).unwrap_or(0)
}

fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn write_bins(out: &mut Vec<u8>, bins: &[Bin]) {
    write_uvarint(out, bins.len() as u64);
    let mut previous = 0i64;
    for bin in bins {
        let delta = bin.key as i64 - previous;
        previous = bin.key as i64;
        write_uvarint(out, ((delta << 1) ^ (delta >> 63)) as u64);
        out.extend_from_slice(&bin.count.to_le_bytes());
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], SketchDecodeError> {
        let end = self.pos.checked_add(n).ok_or(SketchDecodeError::Truncated)?;
        let slice = self.bytes.get(self.pos..end).ok_or(SketchDecodeError::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, SketchDecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, SketchDecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }

    fn f64(&mut self) -> Result<f64, SketchDecodeError> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes")))
    }

    fn uvarint(&mut self) -> Result<u64, SketchDecodeError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(SketchDecodeError::Malformed)
    }
}

fn read_bins(r: &mut Reader<'_>) -> Result<Vec<Bin>, SketchDecodeError> {
    let n = r.uvarint()?;
    // Each bin is at least 9 bytes; a length past the input is malformed, not a huge allocation.
    if n > (r.bytes.len() / 9) as u64 {
        return Err(SketchDecodeError::Malformed);
    }
    let mut bins = Vec::with_capacity(n as usize);
    let mut previous = 0i64;
    for _ in 0..n {
        let zigzag = r.uvarint()?;
        let delta = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
        let key = previous.checked_add(delta).ok_or(SketchDecodeError::Malformed)?;
        previous = key;
        let key = i32::try_from(key).map_err(|_| SketchDecodeError::Malformed)?;
        let count = r.f64()?;
        bins.push(Bin { key, count });
    }
    Ok(bins)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_gamma(v: f64) -> f64 {
        v.ln() / (1.0f64 / 64.0).ln_1p()
    }

    /// The Agent's `config_test.go` vectors: the zero bin below `norm.min`, key 1 at it, key 2 one
    /// γ up, and `maxKey` at `norm.max`; and its documented rule that `key(v) == k` for every `v`
    /// in `[f64(k)/√γ, f64(k)·√γ)`.
    #[test]
    fn agent_mapping_matches_the_agents_own_key_vectors() {
        let m = Mapping::agent();
        let min = m.agent_min();
        assert!(m.is_zero(0.0));
        assert!(m.is_zero(min * 0.999));
        assert!(!m.is_zero(min));
        assert_eq!(m.key(min), 1);
        assert_eq!(m.key(min * Mapping::AGENT_GAMMA), 2);
        assert_eq!(m.key(m.lower_bound(Mapping::AGENT_MAX_KEY)), Mapping::AGENT_MAX_KEY);
        assert_eq!(m.key(f64::MAX), Mapping::AGENT_INF_KEY);
        assert_eq!(m.lower_bound(Mapping::AGENT_INF_KEY), f64::INFINITY);

        let sqrt_gamma = Mapping::AGENT_GAMMA.sqrt();
        for k in [1, 2, 100, 1338, 5000, 20000] {
            let center = m.lower_bound(k);
            assert_eq!(m.key(center), k);
            assert_eq!(m.key(center / sqrt_gamma * 1.0001), k);
            assert_eq!(m.key(center * sqrt_gamma * 0.9999), k);
        }
    }

    /// Where the Agent's rounding and DDSketch's flooring disagree (`2.0` and `10.0` have
    /// `frac(log_γ) ≥ 0.5`), the Agent mapping rounds, as `pkg/util/quantile` does.
    #[test]
    fn agent_mapping_rounds_where_a_logarithmic_mapping_floors() {
        let agent = Mapping::agent();
        let bias = agent.offset;
        let log = Mapping::logarithmic(Mapping::AGENT_GAMMA, 0.0, 4096);
        for (v, rounded, floored) in [(0.5, -45, -45), (1.0, 0, 0), (2.0, 45, 44), (10.0, 149, 148)]
        {
            assert_eq!(agent.key(v), rounded + bias as i32, "agent key for {v}");
            assert_eq!(log.key(v), floored, "logarithmic key for {v}");
            assert_eq!(log_gamma(v).round_ties_even() as i32, rounded);
        }
    }

    /// `sketches-go`'s `Index` floors (truncation, then a decrement below zero), and `Value` is
    /// the lower bound scaled by `2γ / (1 + γ)`.
    #[test]
    fn logarithmic_mapping_floors_and_scales_the_lower_bound() {
        let gamma = 1.02f64;
        let m = Mapping::logarithmic(gamma, 0.0, 2048);
        assert_eq!(m.key(gamma.powf(3.5)), 3);
        assert_eq!(m.key(gamma.powf(-3.5)), -4);
        assert_eq!(m.key(1.0), 0);
        let representative = m.representative(3);
        assert!((representative / gamma.powi(3) - 2.0 * gamma / (1.0 + gamma)).abs() < 1e-12);
    }

    /// The Agent's `store_test.go` merge vector: `0:1 … 10:1` merged with `0:1 … 9:1` under
    /// `binLimit = 3` is `8:18 9:2 10:1` -- every folded key lands in the lowest survivor.
    #[test]
    fn collapse_folds_the_lowest_keys_into_the_lowest_survivor() {
        let a: Vec<Bin> = (0..=10).map(|k| Bin { key: k, count: 1.0 }).collect();
        let b: Vec<Bin> = (0..=9).map(|k| Bin { key: k, count: 1.0 }).collect();
        let merged = merge_bins(&a, &b, 3);
        assert_eq!(
            merged,
            vec![
                Bin { key: 8, count: 18.0 },
                Bin { key: 9, count: 2.0 },
                Bin { key: 10, count: 1.0 }
            ]
        );
    }

    #[test]
    fn add_weighted_with_count_one_matches_plain_add() {
        let mut weighted = DdSketch::new();
        weighted.add_weighted(42.0, 1);
        let mut plain = DdSketch::new();
        plain.add(42.0);
        assert_eq!(weighted, plain);
        assert_eq!(weighted.quantile(0.5), plain.quantile(0.5));
    }

    /// Every quantile of one repeated value stays inside the mapping's relative error, which for
    /// the Agent mapping is `1 - 1/√γ` (0.78%), tighter than the 1% the previous crate promised.
    #[test]
    fn add_weighted_extrapolates_count_and_every_quantile() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(7.5, 100);
        assert_eq!(sketch.count(), 100);
        let bound = 1.0 - 1.0 / Mapping::AGENT_GAMMA.sqrt();
        for q in [0.0, 0.1, 0.5, 0.9, 0.99, 1.0] {
            let value = sketch.quantile(q).expect("quantile present");
            let relative_error = (value - 7.5).abs() / 7.5;
            assert!(relative_error <= bound, "quantile({q}) = {value} is off by {relative_error}");
        }
    }

    #[test]
    fn add_weighted_with_zero_count_and_non_finite_values_are_no_ops() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(1.0, 0);
        sketch.add(f64::NAN);
        sketch.add(f64::INFINITY);
        assert_eq!(sketch.count(), 0);
        assert_eq!(sketch.quantile(0.5), None);
        assert_eq!(sketch, DdSketch::new());
    }

    #[test]
    fn sum_is_exact_and_survives_a_merge() {
        let mut sketch = DdSketch::new();
        assert_eq!(sketch.sum(), 0.0);
        sketch.add(1.0);
        sketch.add(2.5);
        sketch.add(-4.0);
        assert_eq!(sketch.sum(), -0.5);
        assert_eq!(sketch.min(), Some(-4.0));
        assert_eq!(sketch.max(), Some(2.5));

        let mut weighted = DdSketch::new();
        weighted.add_weighted(3.0, 4);
        assert_eq!(weighted.sum(), 12.0);

        sketch.merge(&weighted);
        assert_eq!(sketch.sum(), 11.5);
        assert_eq!(sketch.count(), 7);
        assert!(sketch.stats_exact());
    }

    /// Rank-based over a known population: the extremes are exact, and every other quantile is
    /// the bin center within the mapping's bound. The Agent rounds the rank to even, so `q = 0.1`
    /// over five values (rank 0.4) answers with the first value's bin.
    #[test]
    fn agent_quantile_is_the_bin_center_at_the_rounded_rank() {
        let mut sketch = DdSketch::new();
        for v in [1.0, 2.0, 4.0, 8.0, 16.0] {
            sketch.add(v);
        }
        assert_eq!(sketch.quantile(0.0), Some(1.0));
        assert_eq!(sketch.quantile(1.0), Some(16.0));
        let bound = 1.0 - 1.0 / Mapping::AGENT_GAMMA.sqrt();
        for (q, expected) in [(0.1, 1.0), (0.5, 4.0), (0.75, 8.0)] {
            let v = sketch.quantile(q).unwrap();
            assert!((v - expected).abs() / expected <= bound, "quantile({q}) = {v}");
        }
    }

    #[test]
    fn negative_and_zero_values_order_before_positives() {
        let mut sketch = DdSketch::new();
        for v in [-10.0, -1.0, 0.0, 1.0, 10.0] {
            sketch.add(v);
        }
        assert_eq!(sketch.count(), 5);
        assert_eq!(sketch.zero_count(), 1.0);
        assert_eq!(sketch.negative_bins().len(), 2);
        assert_eq!(sketch.quantile(0.0), Some(-10.0));
        let median = sketch.quantile(0.5).unwrap();
        assert!(median.abs() < 1e-9, "median {median}");
        let low = sketch.quantile(0.25).unwrap();
        assert!((low + 1.0).abs() <= 0.02, "q25 {low}");
    }

    /// Same mapping merges bin-for-bin: merging equals feeding both populations to one sketch.
    #[test]
    fn same_mapping_merge_is_bin_exact() {
        let mut a = DdSketch::new();
        let mut b = DdSketch::new();
        let mut both = DdSketch::new();
        for (i, v) in [0.3, 1.0, 1.0, 2.7, 55.0, 1e6, -3.0].into_iter().enumerate() {
            if i % 2 == 0 {
                a.add(v)
            } else {
                b.add(v)
            }
            both.add(v);
        }
        a.merge(&b);
        assert_eq!(a, both);
    }

    /// A logarithmic (stats) sketch merged into an Agent sketch is re-binned, with every count
    /// kept and the exact summary carried.
    #[test]
    fn cross_mapping_merge_rebins_and_keeps_counts() {
        let mut stats = DdSketch::with_mapping(Mapping::logarithmic(1.0202, 0.0, 2048));
        for v in [10.0, 20.0, 30.0] {
            stats.add(v);
        }
        let mut agent = DdSketch::new();
        agent.add(5.0);
        agent.merge(&stats);
        assert_eq!(agent.count(), 4);
        assert_eq!(agent.mapping().kind(), MappingKind::Agent);
        assert_eq!(agent.sum(), 65.0);
        let median = agent.quantile(0.5).unwrap();
        assert!((median - 10.0).abs() / 10.0 < 0.03 || (median - 20.0).abs() / 20.0 < 0.03);
    }

    #[test]
    fn from_parts_normalizes_bins_and_derives_missing_stats() {
        let mapping = Mapping::logarithmic(1.0202, 0.0, 2048);
        let sketch = DdSketch::from_parts(
            mapping,
            vec![
                Bin { key: 5, count: 1.0 },
                Bin { key: 2, count: 2.0 },
                Bin { key: 5, count: 0.5 },
            ],
            vec![],
            0.0,
            None,
        );
        assert_eq!(
            sketch.positive_bins(),
            &[Bin { key: 2, count: 2.0 }, Bin { key: 5, count: 1.5 }]
        );
        assert!(!sketch.stats_exact());
        assert!((sketch.count as f64 - 3.5).abs() < 1e-12);
        assert_eq!(sketch.min(), Some(mapping.representative(2)));
        assert_eq!(sketch.max(), Some(mapping.representative(5)));
    }

    #[test]
    fn bytes_round_trip_both_mappings_exactly() {
        let mut agent = DdSketch::new();
        for v in [0.25, 1.0, 7.5, 100.0, -2.0, 0.0] {
            agent.add(v);
        }
        let decoded = DdSketch::from_bytes(&agent.to_bytes()).expect("decodes");
        assert_eq!(decoded, agent);
        assert_eq!(decoded.sum(), 106.75);

        let stats = DdSketch::from_parts(
            Mapping::logarithmic(1.0202, 0.5, 2048),
            vec![Bin { key: -3, count: 1.5 }, Bin { key: 40, count: 2.0 }],
            vec![Bin { key: 7, count: 1.0 }],
            2.0,
            None,
        );
        let decoded = DdSketch::from_bytes(&stats.to_bytes()).expect("decodes");
        assert_eq!(decoded, stats);
        assert!(!decoded.stats_exact());
    }

    #[test]
    fn malformed_bytes_are_rejected_not_panicked_on() {
        let bytes = DdSketch::new().to_bytes();
        assert_eq!(
            DdSketch::from_bytes(&bytes[..bytes.len() - 1]),
            Err(SketchDecodeError::Truncated)
        );
        assert_eq!(DdSketch::from_bytes(&[]), Err(SketchDecodeError::Truncated));
        assert_eq!(DdSketch::from_bytes(&[9]), Err(SketchDecodeError::Version));
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(DdSketch::from_bytes(&trailing), Err(SketchDecodeError::Malformed));
        let mut huge = vec![BYTES_VERSION, 0, 1];
        huge.extend(std::iter::repeat_n(0u8, 40));
        huge.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert_eq!(DdSketch::from_bytes(&huge), Err(SketchDecodeError::Malformed));
    }

    #[test]
    fn partial_eq_is_structural() {
        let mut a = DdSketch::new();
        a.add(1.0);
        a.add(2.0);
        let mut b = DdSketch::new();
        b.add(2.0);
        b.add(1.0);
        assert_eq!(a, b);
        let mut c = DdSketch::new();
        c.add(99.0);
        assert_ne!(a, c);
    }

    /// The store never exceeds the mapping's bin limit, and every observation survives the fold.
    #[test]
    fn store_collapses_at_the_bin_limit_without_losing_counts() {
        let mut sketch = DdSketch::with_mapping(Mapping::logarithmic(1.0202, 0.0, 16));
        for i in 1..=200 {
            sketch.add(i as f64 * 1.5);
        }
        assert!(sketch.positive_bins().len() <= 16);
        let total: f64 = sketch.positive_bins().iter().map(|b| b.count).sum();
        assert_eq!(total, 200.0);
        assert_eq!(sketch.count(), 200);
    }
}
