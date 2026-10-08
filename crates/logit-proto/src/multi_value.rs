//! The dotted sub-path table that turns one multi-value metric record into several scalar
//! components, for a sink whose wire carries one number per series name.
//!
//! [`expand_dotted`] walks a record's components in table order and hands each to the caller as a
//! suffix, a value, and its [`Part`]; the caller owns the name, the tags, and the wire. Its
//! consumers are `graphite_out` ([`crate::graphite`]) and `statsd_out`, which types each
//! component `|c` or `|g` by its [`Part`] (`logit_outputs::statsd`'s module doc). `splunk_hec_out`
//! follows the OTel `splunk_hec` exporter's shape instead, which lives in
//! `crate::splunk::metrics`, not here.
//!
//! [`MultiValue`] is the switch a sink exposes for these kinds: `Skip` drops the record, `Expand`
//! renders it through the sink's table.
//!
//! ## Sub-paths
//!
//! Every expanded kind adds **at least one** dotted suffix, so an expanded sub-path can never
//! collide with the bare name the same record carries, nor with a scalar record's name unless that
//! record was already named `x.count` by its producer.
//!
//! | Kind | Sub-paths |
//! |---|---|
//! | `Samples` (via [`logit_core::Samples::sketch`]) / `Distribution` | `.count`, `.sum`, `.min`/`.max` when non-empty, `.q0_5`, `.q0_75`, `.q0_9`, `.q0_95`, `.q0_99` |
//! | `Histogram` | `.count` (Σ bucket counts), `.sum`/`.min`/`.max` when `Some`, `.bucket_<b>` per bucket (its **own** count, not a cumulative running total -- `logit_core::Histogram`'s doc) |
//! | `ExponentialHistogram` | `.count`, `.sum`/`.min`/`.max` when `Some`, `.zero_count`; **no buckets** |
//! | `Summary` | `.count`, `.sum`, `.q<q>` per its own quantiles |
//! | `Set` | `.count` = [`logit_core::HyperLogLog::estimate`] |
//! | `SetMembers` | `.count` = the distinct member count |
//!
//! A non-finite value (a NaN `sum`, an infinite quantile) is skipped, never emitted: the record is
//! already counted degraded by its sink, and a fabricated `inf` is worse than a missing component.
//! An empty sketch yields `.count 0` and `.sum 0` and no extremes or quantiles, since it has no
//! observation to report.
//!
//! A sketch's `.sum`, `.min`, and `.max` are tracked from the observations themselves (plain
//! `f64`s kept alongside the bins and combined on `merge`), not estimated from bins like a
//! quantile. The exception is a sketch decoded from bins alone, such as one from the DDSketch
//! protobuf: there [`logit_core::DdSketch::stats_exact`] is `false`, and all three are derived
//! from bin representatives, so they're approximate.
//!
//! The quantiles are [`crate::otlp::metrics::DISTRIBUTION_QUANTILES`], the five every
//! sketch-to-quantiles degradation in this crate reports, so a metric describes itself identically
//! at `otlp_out`, `prometheus_out`, and `graphite_out`. `influxdb_out` keeps its narrower
//! `[0.5, 0.9, 0.99]`.
//!
//! **Number tokens are injective.** A quantile or bucket bound is formatted with `f64`'s `Display`
//! and every `.` substituted with `_`: `0.99 → q0_99`, `1.5 → bucket_1_5`,
//! `-0.5 → bucket_-0_5`, `f64::INFINITY → bucket_inf`. `Display` emits only `-`, digits, at most
//! one `.`, and `inf`/`-inf`/`NaN`, so two distinct bounds never render to one token (the
//! collision argument in `crates/logit-outputs/src/influxdb.rs`'s `render_fields`, which rejects
//! a *rounded* percentile). Graphite needs the substitution because `.` is its hierarchy
//! separator.

use crate::otlp::metrics::DISTRIBUTION_QUANTILES;
use logit_core::{DdSketch, ExpHistogram, Histogram, MetricKind, Summary};
use std::fmt::Write as _;

/// What a sink does with a metric kind its one-number-per-point wire can't carry natively
/// (`graphite_out`, `splunk_hec_out`, `statsd_out`). Each codec's module doc lists what `Expand`
/// renders.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum MultiValue {
    /// Drop the record, counted `logit.output.metrics.skipped{metric_kind=…}`. The default, since
    /// an expansion's naming convention is one the receiver may know nothing about. `statsd_out`
    /// defaults to `Expand` instead, because a statsd server's own flush writes these dotted
    /// names.
    #[default]
    Skip,
    /// Expand into the per-codec series its module doc lists, counted
    /// `logit.output.metrics.degraded{metric_kind=…}` once per record.
    Expand,
}

/// Which row of the sub-path table a component comes from, so a sink can type it on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Part {
    /// `.count` of a sketch, a histogram, or a summary.
    Count,
    /// `.sum`.
    Sum,
    /// `.bucket_<b>`: one histogram bucket's own count.
    Bucket,
    /// `.zero_count` of an exponential histogram.
    ZeroCount,
    /// `.q<q>` of a sketch or a summary.
    Quantile,
    /// `.min`.
    Min,
    /// `.max`.
    Max,
    /// `.count` of a `Set` or `SetMembers`: a distinct count, which two windows can't add.
    Distinct,
}

impl Part {
    /// Whether two of this component, from two windows or two senders, add to a correct total:
    /// counts, sums, bucket counts, and zero counts. Quantiles, extremes, and distinct counts
    /// don't.
    pub fn is_additive(self) -> bool {
        match self {
            Part::Count | Part::Sum | Part::Bucket | Part::ZeroCount => true,
            Part::Quantile | Part::Min | Part::Max | Part::Distinct => false,
        }
    }
}

/// The buffers [`expand_dotted`] formats a quantile or bucket suffix into. Keep one per encoder:
/// a warm expansion then allocates nothing for its suffixes.
#[derive(Debug, Default)]
pub struct DottedScratch {
    /// The suffix being built, `.q0_99` or `.bucket_-0_5`.
    suffix: String,
    /// One formatted number, before its `.` → `_` substitution.
    number: String,
}

/// Calls `emit(suffix, value, part)` once per component of `kind`, in the module doc's table
/// order, skipping non-finite values. Returns `false` and emits nothing for a scalar kind (`Sum`,
/// `Gauge`, `GaugeDelta`), which has no row in the table.
///
/// Allocates only where a kind requires it: a `Samples` record builds its sketch, and a
/// `SetMembers` record builds a de-duplication buffer.
pub fn expand_dotted(
    kind: &MetricKind,
    scratch: &mut DottedScratch,
    mut emit: impl FnMut(&str, f64, Part),
) -> bool {
    let emit = &mut emit;
    match kind {
        // `sketch()` allocates a `DdSketch` per record, inherent to re-summarizing raw values (as
        // at `influxdb_out`/`otlp_out`).
        MetricKind::Samples(samples) => expand_sketch(&samples.sketch(), scratch, emit),
        MetricKind::Distribution(sketch) => expand_sketch(sketch, scratch, emit),
        MetricKind::Set(hll) => component(emit, ".count", hll.estimate() as f64, Part::Distinct),
        MetricKind::SetMembers(members) => {
            // One de-duplication buffer per record, bounded by its member count.
            let mut distinct: Vec<&[u8]> = members.iter().map(|m| m.as_ref()).collect();
            distinct.sort_unstable();
            distinct.dedup();
            component(emit, ".count", distinct.len() as f64, Part::Distinct);
        }
        MetricKind::Histogram(histogram) => expand_histogram(histogram, scratch, emit),
        MetricKind::ExponentialHistogram(histogram) => expand_exp_histogram(histogram, emit),
        MetricKind::Summary(summary) => expand_summary(summary, scratch, emit),
        MetricKind::Sum(_) | MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => return false,
    }
    true
}

/// `.count`, `.sum`, `.min`/`.max` when the sketch has an observation, then one `.q<q>` per
/// [`DISTRIBUTION_QUANTILES`] the sketch can answer.
fn expand_sketch(
    sketch: &DdSketch,
    scratch: &mut DottedScratch,
    emit: &mut impl FnMut(&str, f64, Part),
) {
    component(emit, ".count", sketch.count() as f64, Part::Count);
    component(emit, ".sum", sketch.sum(), Part::Sum);
    optional(emit, ".min", sketch.min(), Part::Min);
    optional(emit, ".max", sketch.max(), Part::Max);
    for q in DISTRIBUTION_QUANTILES {
        let Some(v) = sketch.quantile(q) else { continue };
        numbered(scratch, emit, ".q", q, v, Part::Quantile);
    }
}

/// `.count` (Σ bucket counts), `.sum`/`.min`/`.max` when present, then `.bucket_<bound>` per
/// bucket, each carrying its **own** count, not a cumulative total (`logit_core::Histogram`).
fn expand_histogram(
    histogram: &Histogram,
    scratch: &mut DottedScratch,
    emit: &mut impl FnMut(&str, f64, Part),
) {
    let count: u64 = histogram.buckets.iter().map(|(_, c)| *c).sum();
    component(emit, ".count", count as f64, Part::Count);
    optional(emit, ".sum", histogram.sum, Part::Sum);
    optional(emit, ".min", histogram.min, Part::Min);
    optional(emit, ".max", histogram.max, Part::Max);
    for (bound, bucket_count) in &histogram.buckets {
        numbered(scratch, emit, ".bucket_", *bound, *bucket_count as f64, Part::Bucket);
    }
}

/// `.count`, `.sum`/`.min`/`.max` when present, `.zero_count`, and **no buckets**.
///
/// Materializing the `(scale, offset, counts)` buckets as `.bucket_<b>` components would be the
/// lossy conversion [`MetricKind::ExponentialHistogram`] exists to avoid, and would mint unbounded
/// series names from one record.
fn expand_exp_histogram(histogram: &ExpHistogram, emit: &mut impl FnMut(&str, f64, Part)) {
    component(emit, ".count", histogram.count as f64, Part::Count);
    optional(emit, ".sum", histogram.sum, Part::Sum);
    optional(emit, ".min", histogram.min, Part::Min);
    optional(emit, ".max", histogram.max, Part::Max);
    component(emit, ".zero_count", histogram.zero_count as f64, Part::ZeroCount);
}

/// `.count`, `.sum`, then one `.q<q>` per quantile the summary carries, keyed on the raw quantile:
/// rounding isn't collision-free (`0.991` and `0.994` would both become `p99`).
fn expand_summary(
    summary: &Summary,
    scratch: &mut DottedScratch,
    emit: &mut impl FnMut(&str, f64, Part),
) {
    component(emit, ".count", summary.count as f64, Part::Count);
    component(emit, ".sum", summary.sum, Part::Sum);
    for (q, v) in &summary.quantiles {
        numbered(scratch, emit, ".q", *q, *v, Part::Quantile);
    }
}

/// One component with a literal suffix; the single place a non-finite value is skipped for every
/// literal row.
fn component(emit: &mut impl FnMut(&str, f64, Part), suffix: &str, value: f64, part: Part) {
    if value.is_finite() {
        emit(suffix, value, part);
    }
}

/// [`component`] for an `Option` field: absent means "not reported", which must not be written as
/// zero.
fn optional(emit: &mut impl FnMut(&str, f64, Part), suffix: &str, value: Option<f64>, part: Part) {
    if let Some(value) = value {
        component(emit, suffix, value, part);
    }
}

/// One component whose suffix is `prefix` plus `token`'s number token, skipped when `value` is
/// non-finite.
fn numbered(
    scratch: &mut DottedScratch,
    emit: &mut impl FnMut(&str, f64, Part),
    prefix: &str,
    token: f64,
    value: f64,
    part: Part,
) {
    if !value.is_finite() {
        return;
    }
    scratch.suffix.clear();
    scratch.suffix.push_str(prefix);
    push_number_token(&mut scratch.suffix, &mut scratch.number, token);
    emit(&scratch.suffix, value, part);
}

/// Appends `v` as a number token: Rust's `{}` rendering with every `.` substituted by `_`.
///
/// **Injective**, so components don't collide (this module's doc): `0.5 → 0_5`, `5 → 5`,
/// `0.05 → 0_05`, `-0.5 → -0_5`, `inf → inf`.
fn push_number_token(out: &mut String, scratch: &mut String, v: f64) {
    scratch.clear();
    let _ = write!(scratch, "{v}");
    for c in scratch.chars() {
        out.push(if c == '.' { '_' } else { c });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{HyperLogLog, Samples, Sum, Temporality};

    fn sketch(values: &[f64]) -> DdSketch {
        let mut sketch = DdSketch::new();
        for v in values {
            sketch.add(*v);
        }
        sketch
    }

    /// Every component of `kind` as `(suffix, value, part)`, and the return value.
    fn expand(kind: &MetricKind) -> (Vec<(String, f64, Part)>, bool) {
        let mut parts = Vec::new();
        let expanded = expand_dotted(kind, &mut DottedScratch::default(), |suffix, v, part| {
            parts.push((suffix.to_string(), v, part))
        });
        (parts, expanded)
    }

    fn suffixes_and_parts(kind: &MetricKind) -> Vec<(String, Part)> {
        expand(kind).0.into_iter().map(|(suffix, _, part)| (suffix, part)).collect()
    }

    fn owned(rows: &[(&str, Part)]) -> Vec<(String, Part)> {
        rows.iter().map(|(suffix, part)| (suffix.to_string(), *part)).collect()
    }

    #[test]
    fn every_kind_yields_the_table_in_order_with_its_part() {
        use Part::*;
        let quantiles = [
            (".count", Count),
            (".sum", Sum),
            (".min", Min),
            (".max", Max),
            (".q0_5", Quantile),
            (".q0_75", Quantile),
            (".q0_9", Quantile),
            (".q0_95", Quantile),
            (".q0_99", Quantile),
        ];
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        hll.insert(b"b");
        let cases: Vec<(MetricKind, Vec<(&str, Part)>)> = vec![
            (MetricKind::Distribution(sketch(&[1.0, 2.0, 3.0])), quantiles.to_vec()),
            (MetricKind::Samples(Samples::new([1.0, 2.0, 3.0])), quantiles.to_vec()),
            (
                MetricKind::Histogram(Histogram {
                    buckets: vec![(0.5, 1), (5.0, 2), (f64::INFINITY, 3)],
                    temporality: Temporality::Delta,
                    sum: Some(12.0),
                    min: Some(0.25),
                    max: Some(9.0),
                }),
                vec![
                    (".count", Count),
                    (".sum", Sum),
                    (".min", Min),
                    (".max", Max),
                    (".bucket_0_5", Bucket),
                    (".bucket_5", Bucket),
                    (".bucket_inf", Bucket),
                ],
            ),
            (
                MetricKind::ExponentialHistogram(ExpHistogram {
                    scale: 0,
                    zero_count: 2,
                    zero_threshold: 0.0,
                    positive: (0, vec![1, 2]),
                    negative: (0, vec![]),
                    temporality: Temporality::Delta,
                    count: 5,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                }),
                vec![
                    (".count", Count),
                    (".sum", Sum),
                    (".min", Min),
                    (".max", Max),
                    (".zero_count", ZeroCount),
                ],
            ),
            (
                MetricKind::Summary(Summary {
                    quantiles: vec![(0.5, 1.0), (0.99, 9.0)],
                    count: 4,
                    sum: 12.0,
                }),
                vec![(".count", Count), (".sum", Sum), (".q0_5", Quantile), (".q0_99", Quantile)],
            ),
            (MetricKind::Set(hll), vec![(".count", Distinct)]),
            (
                MetricKind::SetMembers(vec![
                    bytes::Bytes::from_static(b"a"),
                    bytes::Bytes::from_static(b"b"),
                    bytes::Bytes::from_static(b"a"),
                ]),
                vec![(".count", Distinct)],
            ),
        ];
        for (kind, expected) in cases {
            assert_eq!(suffixes_and_parts(&kind), owned(&expected), "{kind:?}");
            assert!(expand(&kind).1, "{kind:?} has a row in the table");
        }
    }

    #[test]
    fn values_are_the_records_own_numbers() {
        let (parts, _) = expand(&MetricKind::SetMembers(vec![
            bytes::Bytes::from_static(b"a"),
            bytes::Bytes::from_static(b"b"),
            bytes::Bytes::from_static(b"a"),
        ]));
        assert_eq!(parts, vec![(".count".to_string(), 2.0, Part::Distinct)], "two distinct");

        let (parts, _) = expand(&MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (2.0, 3)],
            temporality: Temporality::Delta,
            sum: Some(7.5),
            min: None,
            max: None,
        }));
        assert_eq!(
            parts,
            vec![
                (".count".to_string(), 5.0, Part::Count),
                (".sum".to_string(), 7.5, Part::Sum),
                (".bucket_1".to_string(), 2.0, Part::Bucket),
                (".bucket_2".to_string(), 3.0, Part::Bucket),
            ],
            "count is the bucket total, and each bucket carries its own count"
        );
    }

    #[test]
    fn non_finite_values_are_skipped_not_emitted() {
        let histogram = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(f64::NAN),
            min: Some(f64::NEG_INFINITY),
            max: Some(f64::INFINITY),
        });
        assert_eq!(
            suffixes_and_parts(&histogram),
            owned(&[(".count", Part::Count), (".bucket_1", Part::Bucket)])
        );

        let summary = MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0), (0.9, f64::NAN), (0.99, f64::INFINITY)],
            count: 3,
            sum: f64::NAN,
        });
        assert_eq!(
            suffixes_and_parts(&summary),
            owned(&[(".count", Part::Count), (".q0_5", Part::Quantile)])
        );
    }

    #[test]
    fn a_sketch_reports_its_own_min_and_max_including_a_negative_min() {
        let (parts, _) = expand(&MetricKind::Distribution(sketch(&[-4.0, 2.5, 1.0])));
        let extremes: Vec<_> = parts
            .into_iter()
            .filter(|(_, _, part)| matches!(part, Part::Min | Part::Max))
            .collect();
        assert_eq!(
            extremes,
            vec![(".min".to_string(), -4.0, Part::Min), (".max".to_string(), 2.5, Part::Max)]
        );
    }

    #[test]
    fn an_empty_sketch_yields_count_and_sum_only() {
        let (parts, expanded) = expand(&MetricKind::Distribution(DdSketch::new()));
        assert!(expanded);
        assert_eq!(
            parts,
            vec![(".count".to_string(), 0.0, Part::Count), (".sum".to_string(), 0.0, Part::Sum)]
        );
    }

    #[test]
    fn scalar_kinds_yield_nothing_and_return_false() {
        for kind in [
            MetricKind::Sum(Sum { value: 1.0, temporality: Temporality::Delta, monotonic: true }),
            MetricKind::Gauge(1.0),
            MetricKind::GaugeDelta(1.0),
        ] {
            let (parts, expanded) = expand(&kind);
            assert!(parts.is_empty(), "{kind:?}");
            assert!(!expanded, "{kind:?}");
        }
    }

    #[test]
    fn number_tokens_are_injective_over_distinct_bounds() {
        let bounds =
            [0.5, 5.0, 0.05, -0.5, 50.0, 0.005, f64::INFINITY, f64::NEG_INFINITY, f64::NAN];
        let mut tokens: Vec<String> = bounds
            .iter()
            .map(|b| {
                let mut out = String::new();
                push_number_token(&mut out, &mut String::new(), *b);
                out
            })
            .collect();
        assert_eq!(tokens, ["0_5", "5", "0_05", "-0_5", "50", "0_005", "inf", "-inf", "NaN"]);
        let before = tokens.len();
        tokens.sort_unstable();
        tokens.dedup();
        assert_eq!(tokens.len(), before, "distinct bounds must render to distinct tokens");
    }

    #[test]
    fn only_counts_sums_buckets_and_zero_counts_are_additive() {
        use Part::*;
        for part in [Count, Sum, Bucket, ZeroCount] {
            assert!(part.is_additive(), "{part:?}");
        }
        for part in [Quantile, Min, Max, Distinct] {
            assert!(!part.is_additive(), "{part:?}");
        }
    }

    #[test]
    fn a_warm_scratch_does_not_reallocate() {
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![(0.125, 1), (-1024.75, 2), (f64::INFINITY, 3)],
            temporality: Temporality::Delta,
            sum: None,
            min: None,
            max: None,
        });
        let mut scratch = DottedScratch::default();
        expand_dotted(&kind, &mut scratch, |_, _, _| {});
        let caps = (scratch.suffix.capacity(), scratch.number.capacity());
        assert!(caps.0 > 0 && caps.1 > 0, "the first expansion sizes both buffers");
        expand_dotted(&kind, &mut scratch, |_, _, _| {});
        assert_eq!((scratch.suffix.capacity(), scratch.number.capacity()), caps);
    }
}
