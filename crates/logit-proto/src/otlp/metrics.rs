//! `MetricRecord` ↔ OTLP `Metric` -- the hard direction, both ways.
//!
//! **Encode.** `start_time_unix_nano` and `time_unix_nano` are both stamped with
//! `Event::timestamp`: `MetricRecord::start_timestamp` mapping is W4's, not this module's yet
//! (still `0`/unknown on everything this crate itself produces). Event attributes become the data
//! point's attributes; the metric name/unit come from the `MetricRecord` itself. One `MetricRecord`
//! becomes exactly one OTLP `Metric` with exactly one data point -- this does **not** coalesce
//! same-named metrics across events into one wire-level `Metric.data_points` list the way a
//! canonical OTLP producer would. That's spec-legal (multiple `Metric` entries sharing a name is
//! explicitly permitted; most consumers -- including this crate's own decoder -- treat them as more
//! points of the same series) and keeps this mapping a pure per-record function instead of a
//! batch-wide grouping pass.
//!
//! | `MetricKind` | Encodes to | Fidelity |
//! |---|---|---|
//! | `Sum{value,temporality,monotonic}` | `Sum{temporality,monotonic}` | exact -- both flags ride real fields now, not a well-known attribute |
//! | `Gauge(v)` | `Gauge` | exact |
//! | `Histogram{buckets,temporality,sum,min,max}` | `Histogram{temporality,sum,min,max}` | exact -- `buckets` is already per-bucket, not
//! |   |   | cumulative (`metric.rs`'s doc comment, which describes the *count per bucket*, not the
//! |   |   | series' own temporality); a trailing `f64::INFINITY` bound becomes the implicit final
//! |   |   | bucket OTLP's `explicit_bounds` convention expects. |
//! | `ExponentialHistogram(e)` | `ExponentialHistogram` | exact -- 1:1 field mapping, kept as its own
//! |   |   | variant specifically so `otlp_in -> otlp_out` is a fixed point for this type. |
//! | `Summary{quantiles,count,sum}` | `Summary{count,sum}` | exact |
//! | `Samples(s)` | `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) | **Lossy, deliberately**
//! |   |   | -- sketched into a temporary `DdSketch` first (`add_weighted` per value, weighted by
//! |   |   | `(1/sample_rate).round()` clamped to `[1, 1000]`), then takes the same degraded path
//! |   |   | `Distribution` does. Counted via `logit.output.metrics.degraded{metric_kind="samples"}`. |
//! | `Distribution(sketch)` | `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) | **Lossy,
//! |   |   | deliberately** -- see the module doc's "Lossy metric kinds" note below. Counted via
//! |   |   | `logit.output.metrics.degraded{metric_kind="distribution"}`. |
//! | `SetMembers(members)` | **skipped** | No cardinality to compute from raw members without a
//! |   |   | real HLL wired up -- same shape as `Set`'s skip. Counted via
//! |   |   | `logit.output.metrics.skipped{metric_kind="set_members"}`. |
//! | `Set(hll)` | **skipped** | No cardinality to read (`HyperLogLog` is still a stub) -- same
//! |   |   | precedent `crates/logit-outputs/src/influxdb.rs` already sets for the same kind.
//! |   |   | Counted via `logit.output.metrics.skipped{metric_kind="set"}`, throttled-warned. |
//!
//! `Samples`/`Distribution`/`SetMembers`/`Set` are the qualification [ADR `committed-pregenerated-otlp-protobuf`](../../../../docs/adr/committed-pregenerated-otlp-protobuf.md)
//! spells out against [ADR `native-wire-format-with-otlp-bridge`](../../../../docs/adr/native-wire-format-with-otlp-bridge.md):
//! here it's `logit`'s own model (raw samples/members with no OTLP wire type, a mergeable sketch, a
//! cardinality stub) that can't be losslessly re-expressed *as* OTLP, not the other way around.
//!
//! **Decode.** `Sum` → `MetricKind::Sum{value,temporality,monotonic}` directly -- temporality and
//! monotonicity both ride real fields now; a cumulative `Sum` no longer decodes as a `Gauge`, and
//! this module never stamps or reads a well-known attribute for either flag any more
//! (`docs/adr/lossless-transit.md` retires that convention for temporality). `Histogram` →
//! `Histogram{buckets,temporality,sum,min,max}`, all real fields.
//! `Histogram` reconstructs the trailing infinite bucket when `bucket_counts` has one more entry
//! than `explicit_bounds` (the OTLP-mandated shape). `Summary` → `Summary{quantiles,count,sum}`,
//! all real fields now (`count`/`sum` used to be dropped). `ExponentialHistogram` →
//! `ExponentialHistogram` 1:1 (scale, zero_count, zero_threshold, positive/negative
//! offset+bucket_counts, temporality, count, sum/min/max) -- no bucket materialization, no
//! `MAX_DERIVED_BUCKETS` cap, since the variant now carries the wire shape directly instead of
//! deriving explicit bounds from it.
//!
//! Any point with `flags & DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK` set is skipped and counted,
//! never fails the whole request -- OTLP has its own channel for reporting this back
//! (`partial_success`), wired in PR3.
//!
//! `MetricRecord`'s other new fields (`description`, `start_timestamp`, `exemplars`) are filled
//! with their defaults on decode (`None`/`0`/empty) and ignored on encode -- mapping them is W4's,
//! not this module's yet.
//!
//! Decode-side skips count via `logit.input.metrics.skipped{metric_kind, reason}` -- distinct
//! names from the encode side's `logit.output.metrics.{degraded,skipped}` since these are the two
//! directions of one component (`OtlpEncoder`/`OtlpDecoder`), not two components sharing counters.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::metrics::v1 as pb;
use logit_core::interner::{intern, resolve};
use logit_core::{
    DdSketch, Diagnostics, Event, ExpHistogram, Histogram, MetricKind, MetricRecord, Sum, Summary,
    Telemetry, Temporality,
};

const DISTRIBUTION_QUANTILES: [f64; 5] = [0.5, 0.75, 0.90, 0.95, 0.99];

fn no_recorded_value(flags: u32) -> bool {
    flags & 1 != 0 // DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK
}

fn temporality_to_pb(t: Temporality) -> i32 {
    match t {
        Temporality::Delta => pb::AggregationTemporality::Delta as i32,
        Temporality::Cumulative => pb::AggregationTemporality::Cumulative as i32,
    }
}

/// Any wire value other than `CUMULATIVE` (including `UNSPECIFIED`, which OTLP says "MUST not be
/// used" but a lenient decoder shouldn't fail a whole point over) decodes as `Delta`.
fn temporality_from_pb(raw: i32) -> Temporality {
    if raw == pb::AggregationTemporality::Cumulative as i32 {
        Temporality::Cumulative
    } else {
        Temporality::Delta
    }
}

fn number_data_point(
    attributes: Vec<crate::otlp::generated::opentelemetry::proto::common::v1::KeyValue>,
    ts: u64,
    value: f64,
) -> pb::NumberDataPoint {
    pb::NumberDataPoint {
        attributes,
        start_time_unix_nano: ts,
        time_unix_nano: ts,
        exemplars: Vec::new(),
        flags: 0,
        value: Some(pb::number_data_point::Value::AsDouble(value)),
    }
}

fn number_value(value: Option<pb::number_data_point::Value>) -> f64 {
    match value {
        Some(pb::number_data_point::Value::AsDouble(d)) => d,
        Some(pb::number_data_point::Value::AsInt(i)) => i as f64,
        None => 0.0,
    }
}

/// Encodes one `(Event, MetricRecord)` pair into one OTLP `Metric`, or `None` for `Set`/
/// `SetMembers`/`GaugeDelta` (skipped, counted -- see the module doc's table).
pub(crate) fn encode_metric(
    event: &Event,
    record: &MetricRecord,
    telemetry: &Telemetry,
    diagnostics: &mut Diagnostics,
) -> Option<pb::Metric> {
    let name = resolve(record.name).to_string();
    let unit = record.unit.map(resolve).unwrap_or_default().to_string();
    let attributes = common::attrs_to_key_values(&event.attributes);
    let ts = event.timestamp.max(0) as u64;

    let data = match &record.kind {
        MetricKind::Sum(s) => pb::metric::Data::Sum(pb::Sum {
            data_points: vec![number_data_point(attributes, ts, s.value)],
            aggregation_temporality: temporality_to_pb(s.temporality),
            is_monotonic: s.monotonic,
        }),
        MetricKind::Gauge(v) => pb::metric::Data::Gauge(pb::Gauge {
            data_points: vec![number_data_point(attributes, ts, *v)],
        }),
        MetricKind::Histogram(h) => {
            let bucket_counts: Vec<u64> = h.buckets.iter().map(|(_, c)| *c).collect();
            let explicit_bounds: Vec<f64> =
                h.buckets.iter().filter(|(b, _)| b.is_finite()).map(|(b, _)| *b).collect();
            let count = bucket_counts.iter().sum();
            pb::metric::Data::Histogram(pb::Histogram {
                data_points: vec![pb::HistogramDataPoint {
                    attributes,
                    start_time_unix_nano: ts,
                    time_unix_nano: ts,
                    count,
                    sum: h.sum,
                    bucket_counts,
                    explicit_bounds,
                    exemplars: Vec::new(),
                    flags: 0,
                    min: h.min,
                    max: h.max,
                }],
                aggregation_temporality: temporality_to_pb(h.temporality),
            })
        }
        MetricKind::ExponentialHistogram(e) => {
            pb::metric::Data::ExponentialHistogram(pb::ExponentialHistogram {
                data_points: vec![pb::ExponentialHistogramDataPoint {
                    attributes,
                    start_time_unix_nano: ts,
                    time_unix_nano: ts,
                    count: e.count,
                    sum: e.sum,
                    scale: e.scale,
                    zero_count: e.zero_count,
                    positive: Some(pb::exponential_histogram_data_point::Buckets {
                        offset: e.positive.0,
                        bucket_counts: e.positive.1.clone(),
                    }),
                    negative: Some(pb::exponential_histogram_data_point::Buckets {
                        offset: e.negative.0,
                        bucket_counts: e.negative.1.clone(),
                    }),
                    flags: 0,
                    exemplars: Vec::new(),
                    min: e.min,
                    max: e.max,
                    zero_threshold: e.zero_threshold,
                }],
                aggregation_temporality: temporality_to_pb(e.temporality),
            })
        }
        MetricKind::Summary(s) => pb::metric::Data::Summary(pb::Summary {
            data_points: vec![pb::SummaryDataPoint {
                attributes,
                start_time_unix_nano: ts,
                time_unix_nano: ts,
                count: s.count,
                sum: s.sum,
                quantile_values: s
                    .quantiles
                    .iter()
                    .map(|(q, v)| pb::summary_data_point::ValueAtQuantile {
                        quantile: *q,
                        value: *v,
                    })
                    .collect(),
                flags: 0,
            }],
        }),
        MetricKind::Samples(s) => {
            telemetry.count("logit.output.metrics.degraded", 1.0, &[("metric_kind", "samples")]);
            // Sketch first, then take the same degraded path `Distribution` does -- see the
            // module doc. `Samples::weight` extrapolates a sampled statsd timing/histogram line
            // the same way `crates/logit-inputs/src/statsd.rs` does for its own sketch, bounded
            // and NaN-safe against a hostile or malformed rate.
            let weight = s.weight();
            let mut sketch = DdSketch::new();
            for v in &s.values {
                sketch.add_weighted(*v, weight);
            }
            pb::metric::Data::Summary(pb::Summary {
                data_points: vec![distribution_summary_point(attributes, ts, &sketch)],
            })
        }
        MetricKind::Distribution(sketch) => {
            telemetry.count(
                "logit.output.metrics.degraded",
                1.0,
                &[("metric_kind", "distribution")],
            );
            pb::metric::Data::Summary(pb::Summary {
                data_points: vec![distribution_summary_point(attributes, ts, sketch)],
            })
        }
        MetricKind::SetMembers(_) => {
            telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", "set_members")]);
            diagnostics.warn_throttled(
                "otlp_set_members_metric_skipped",
                format_args!(
                    "metric '{name}' is a SetMembers, which OTLP has no encoding for -- skipped"
                ),
            );
            return None;
        }
        MetricKind::Set(_) => {
            telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", "set")]);
            diagnostics.warn_throttled(
                "otlp_set_metric_skipped",
                format_args!("metric '{name}' is a Set, which OTLP has no encoding for -- skipped"),
            );
            return None;
        }
        // A `GaugeDelta` reaching a sink means the pipeline is missing an `aggregate` component --
        // it is explicitly unresolved (`docs/adr/relative-gauge-adjustments.md`) and must not
        // be encoded as though it were an absolute value. Uses the same greppable
        // `gauge_delta_unresolved` diagnostic key `influxdb_out` reports under, not the generic
        // skip key above, so an operator can find every sink's occurrence of this one failure mode
        // with a single grep.
        MetricKind::GaugeDelta(_) => {
            telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", "gauge_delta")]);
            diagnostics.warn_throttled(
                "gauge_delta_unresolved",
                format_args!(
                    "a relative gauge adjustment reached a sink unresolved -- add an `aggregate` \
                     component between the statsd input and this output"
                ),
            );
            return None;
        }
    };

    Some(pb::Metric {
        name,
        description: String::new(),
        unit,
        metadata: Vec::new(),
        data: Some(data),
    })
}

fn distribution_summary_point(
    attributes: Vec<crate::otlp::generated::opentelemetry::proto::common::v1::KeyValue>,
    ts: u64,
    sketch: &DdSketch,
) -> pb::SummaryDataPoint {
    let quantile_values = DISTRIBUTION_QUANTILES
        .iter()
        .filter_map(|q| {
            sketch
                .quantile(*q)
                .map(|v| pb::summary_data_point::ValueAtQuantile { quantile: *q, value: v })
        })
        .collect();
    pb::SummaryDataPoint {
        attributes,
        start_time_unix_nano: ts,
        time_unix_nano: ts,
        count: sketch.count() as u64,
        sum: 0.0,
        quantile_values,
        flags: 0,
    }
}

/// Decodes one OTLP `Metric` into zero or more `Event`s (one per data point). Never fails the
/// whole point/request -- a malformed or `no_recorded_value` data point is skipped and counted
/// (see the module doc).
pub(crate) fn decode_metric(
    metric: pb::Metric,
    base_attrs: &logit_core::AttrMap,
    telemetry: &Telemetry,
) -> Vec<Event> {
    let name = intern(&metric.name);
    let unit = if metric.unit.is_empty() { None } else { Some(intern(&metric.unit)) };
    let record = |kind: MetricKind| MetricRecord {
        name,
        unit,
        description: None,
        start_timestamp: 0,
        exemplars: Vec::new(),
        kind,
    };

    match metric.data {
        Some(pb::metric::Data::Sum(sum)) => {
            let monotonic = sum.is_monotonic;
            let temporality = temporality_from_pb(sum.aggregation_temporality);
            sum.data_points
                .into_iter()
                .filter_map(|dp| {
                    if no_recorded_value(dp.flags) {
                        telemetry.count(
                            "logit.input.metrics.skipped",
                            1.0,
                            &[("metric_kind", "sum"), ("reason", "no_recorded_value")],
                        );
                        return None;
                    }
                    let mut attrs = base_attrs.clone();
                    let ts = dp.time_unix_nano as i64;
                    let value = number_value(dp.value);
                    common::key_values_into_attrs(dp.attributes, &mut attrs);
                    let kind = MetricKind::Sum(Sum { value, temporality, monotonic });
                    Some(Event::metric(ts, attrs, record(kind)))
                })
                .collect()
        }
        Some(pb::metric::Data::Gauge(gauge)) => gauge
            .data_points
            .into_iter()
            .filter_map(|dp| {
                if no_recorded_value(dp.flags) {
                    telemetry.count(
                        "logit.input.metrics.skipped",
                        1.0,
                        &[("metric_kind", "gauge"), ("reason", "no_recorded_value")],
                    );
                    return None;
                }
                let mut attrs = base_attrs.clone();
                let ts = dp.time_unix_nano as i64;
                let value = number_value(dp.value);
                common::key_values_into_attrs(dp.attributes, &mut attrs);
                Some(Event::metric(ts, attrs, record(MetricKind::Gauge(value))))
            })
            .collect(),
        Some(pb::metric::Data::Histogram(hist)) => {
            let temporality = temporality_from_pb(hist.aggregation_temporality);
            hist.data_points
                .into_iter()
                .filter_map(|dp| {
                    if no_recorded_value(dp.flags) {
                        telemetry.count(
                            "logit.input.metrics.skipped",
                            1.0,
                            &[("metric_kind", "histogram"), ("reason", "no_recorded_value")],
                        );
                        return None;
                    }
                    let mut attrs = base_attrs.clone();
                    let ts = dp.time_unix_nano as i64;
                    let mut buckets = Vec::with_capacity(dp.bucket_counts.len());
                    for (i, count) in dp.bucket_counts.iter().enumerate() {
                        let bound = dp.explicit_bounds.get(i).copied().unwrap_or(f64::INFINITY);
                        buckets.push((bound, *count));
                    }
                    common::key_values_into_attrs(dp.attributes, &mut attrs);
                    let kind = MetricKind::Histogram(Histogram {
                        buckets,
                        temporality,
                        sum: dp.sum,
                        min: dp.min,
                        max: dp.max,
                    });
                    Some(Event::metric(ts, attrs, record(kind)))
                })
                .collect()
        }
        Some(pb::metric::Data::Summary(summary)) => summary
            .data_points
            .into_iter()
            .filter_map(|dp| {
                if no_recorded_value(dp.flags) {
                    telemetry.count(
                        "logit.input.metrics.skipped",
                        1.0,
                        &[("metric_kind", "summary"), ("reason", "no_recorded_value")],
                    );
                    return None;
                }
                let mut attrs = base_attrs.clone();
                let ts = dp.time_unix_nano as i64;
                let quantiles = dp.quantile_values.iter().map(|q| (q.quantile, q.value)).collect();
                common::key_values_into_attrs(dp.attributes, &mut attrs);
                let kind = MetricKind::Summary(Summary { quantiles, count: dp.count, sum: dp.sum });
                Some(Event::metric(ts, attrs, record(kind)))
            })
            .collect(),
        Some(pb::metric::Data::ExponentialHistogram(eh)) => {
            let temporality = temporality_from_pb(eh.aggregation_temporality);
            eh.data_points
                .into_iter()
                .filter_map(|dp| {
                    if no_recorded_value(dp.flags) {
                        telemetry.count(
                            "logit.input.metrics.skipped",
                            1.0,
                            &[
                                ("metric_kind", "exponential_histogram"),
                                ("reason", "no_recorded_value"),
                            ],
                        );
                        return None;
                    }
                    let mut attrs = base_attrs.clone();
                    let ts = dp.time_unix_nano as i64;
                    common::key_values_into_attrs(dp.attributes, &mut attrs);
                    let positive =
                        dp.positive.map(|b| (b.offset, b.bucket_counts)).unwrap_or((0, Vec::new()));
                    let negative =
                        dp.negative.map(|b| (b.offset, b.bucket_counts)).unwrap_or((0, Vec::new()));
                    let kind = MetricKind::ExponentialHistogram(ExpHistogram {
                        scale: dp.scale,
                        zero_count: dp.zero_count,
                        zero_threshold: dp.zero_threshold,
                        positive,
                        negative,
                        temporality,
                        count: dp.count,
                        sum: dp.sum,
                        min: dp.min,
                        max: dp.max,
                    });
                    Some(Event::metric(ts, attrs, record(kind)))
                })
                .collect()
        }
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::{AttrMap, HyperLogLog, Samples};

    fn record(kind: MetricKind) -> MetricRecord {
        MetricRecord::new(intern("m"), kind)
    }

    fn event() -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert("k", "v");
        Event::empty(1000, attrs)
    }

    fn encode(kind: MetricKind) -> Option<pb::Metric> {
        let mut diag = Diagnostics::default();
        encode_metric(&event(), &record(kind), &Telemetry::default(), &mut diag)
    }

    #[test]
    fn a_delta_monotonic_sum_encodes_with_both_flags() {
        let metric = encode(MetricKind::counter(3.0)).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Sum(sum) => {
                assert!(sum.is_monotonic);
                assert_eq!(sum.aggregation_temporality, pb::AggregationTemporality::Delta as i32);
                assert_eq!(number_value(sum.data_points[0].value), 3.0);
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    /// A cumulative, non-monotonic sum round-trips both flags -- the case W1 adds real fields for
    /// instead of a well-known attribute plus an always-`true` `is_monotonic`.
    #[test]
    fn a_cumulative_non_monotonic_sum_round_trips_both_flags() {
        let kind = MetricKind::Sum(Sum {
            value: 7.0,
            temporality: Temporality::Cumulative,
            monotonic: false,
        });
        let metric = encode(kind).unwrap();
        match &metric.data {
            Some(pb::metric::Data::Sum(sum)) => {
                assert!(!sum.is_monotonic);
                assert_eq!(
                    sum.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
            }
            other => panic!("expected Sum, got {other:?}"),
        }
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => {
                assert_eq!(s.value, 7.0);
                assert_eq!(s.temporality, Temporality::Cumulative);
                assert!(!s.monotonic);
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_gauge_encodes_to_a_gauge() {
        let metric = encode(MetricKind::Gauge(4.5)).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Gauge(g) => assert_eq!(number_value(g.data_points[0].value), 4.5),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    fn histogram(temporality: Temporality) -> Histogram {
        Histogram {
            buckets: vec![(1.0, 2u64), (5.0, 3u64), (f64::INFINITY, 1u64)],
            temporality,
            sum: Some(12.5),
            min: Some(0.5),
            max: Some(9.9),
        }
    }

    #[test]
    fn a_histogram_encodes_exactly_with_a_trailing_infinite_bucket_and_its_own_temporality() {
        let h = histogram(Temporality::Cumulative);
        let metric = encode(MetricKind::Histogram(h.clone())).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Histogram(hist) => {
                let dp = &hist.data_points[0];
                assert_eq!(dp.explicit_bounds, vec![1.0, 5.0]);
                assert_eq!(dp.bucket_counts, vec![2, 3, 1]);
                assert_eq!(dp.count, 6);
                assert_eq!(dp.sum, h.sum);
                assert_eq!(dp.min, h.min);
                assert_eq!(dp.max, h.max);
                assert_eq!(
                    hist.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_histogram_decodes_with_the_same_buckets_temporality_and_sum_min_max_it_encoded() {
        let h = histogram(Temporality::Delta);
        let metric = encode(MetricKind::Histogram(h.clone())).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Histogram(got) => assert_eq!(*got, h),
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_summary_encodes_and_decodes_with_count_and_sum() {
        let s = Summary { quantiles: vec![(0.5, 10.0), (0.99, 99.0)], count: 42, sum: 543.2 };
        let metric = encode(MetricKind::Summary(s.clone())).unwrap();
        match &metric.data {
            Some(pb::metric::Data::Summary(summary)) => {
                let dp = &summary.data_points[0];
                assert_eq!(dp.count, 42);
                assert_eq!(dp.sum, 543.2);
                assert_eq!(dp.quantile_values.len(), 2);
            }
            other => panic!("expected Summary, got {other:?}"),
        }
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Summary(got) => assert_eq!(*got, s),
            other => panic!("expected Summary, got {other:?}"),
        }
    }

    fn exp_histogram(temporality: Temporality) -> ExpHistogram {
        ExpHistogram {
            scale: 3,
            zero_count: 2,
            zero_threshold: 0.5,
            positive: (1, vec![4, 5, 6]),
            negative: (2, vec![7, 8]),
            temporality,
            count: 26,
            sum: Some(100.0),
            min: Some(-5.0),
            max: Some(50.0),
        }
    }

    /// The 1:1 mapping this module doc promises: every field survives an encode -> re-encode
    /// comparison of the wire `ExponentialHistogramDataPoint` unchanged.
    #[test]
    fn an_exponential_histogram_encodes_to_identical_wire_fields_both_times() {
        let e = exp_histogram(Temporality::Cumulative);
        let metric = encode(MetricKind::ExponentialHistogram(e.clone())).unwrap();
        let events = decode_metric(metric.clone(), &AttrMap::new(), &Telemetry::default());
        let mut diag = Diagnostics::default();
        let re_metric =
            encode_metric(&events[0], &events[0].metrics[0], &Telemetry::default(), &mut diag)
                .unwrap();

        let extract = |m: &pb::Metric| match &m.data {
            Some(pb::metric::Data::ExponentialHistogram(eh)) => eh.data_points[0].clone(),
            other => panic!("expected ExponentialHistogram, got {other:?}"),
        };
        let first = extract(&metric);
        let second = extract(&re_metric);
        assert_eq!(first.scale, second.scale);
        assert_eq!(first.zero_count, second.zero_count);
        assert_eq!(first.zero_threshold, second.zero_threshold);
        assert_eq!(first.positive, second.positive);
        assert_eq!(first.negative, second.negative);
        assert_eq!(first.count, second.count);
        assert_eq!(first.sum, second.sum);
        assert_eq!(first.min, second.min);
        assert_eq!(first.max, second.max);
    }

    #[test]
    fn an_exponential_histogram_decodes_1_to_1_with_no_bucket_materialization() {
        let e = exp_histogram(Temporality::Delta);
        let metric = encode(MetricKind::ExponentialHistogram(e.clone())).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::ExponentialHistogram(got) => assert_eq!(*got, e),
            other => panic!("expected ExponentialHistogram, got {other:?}"),
        }
    }

    #[test]
    fn a_samples_metric_encodes_as_a_five_quantile_summary_and_is_counted_degraded() {
        let samples = {
            let mut s = Samples::new([1.0, 2.0, 3.0, 4.0, 5.0]);
            s.sample_rate = 1.0;
            s
        };
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&event(), &record(MetricKind::Samples(samples)), &telemetry, &mut diag)
                .unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Summary(s) => {
                assert_eq!(s.data_points[0].quantile_values.len(), 5, "p50/p75/p90/p95/p99");
            }
            other => panic!("expected Summary, got {other:?}"),
        }
        let events = registry.drain(0);
        let degraded = events
            .iter()
            .find(|e| e.attributes.get("metric_kind").and_then(|v| v.as_str()) == Some("samples"));
        assert!(degraded.is_some(), "should count logit.output.metrics.degraded{{metric_kind}}");
    }

    /// A NaN `sample_rate` must not empty the sketch: `Samples::weight` degrades it to `1`, so
    /// the summary still carries every observation (`count == values.len()`), where a bare
    /// `clamp`-then-`as u64` would have produced weight `0` and a `count: 0` point.
    #[test]
    fn a_samples_metric_with_a_nan_sample_rate_keeps_every_observation() {
        let mut samples = Samples::new([120.0, 130.0]);
        samples.sample_rate = f64::NAN;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&event(), &record(MetricKind::Samples(samples)), &telemetry, &mut diag)
                .unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Summary(s) => {
                assert_eq!(s.data_points[0].count, 2);
                assert_eq!(s.data_points[0].quantile_values.len(), 5);
            }
            other => panic!("expected Summary, got {other:?}"),
        }
    }

    #[test]
    fn a_distribution_encodes_as_a_five_quantile_summary_and_is_counted_degraded() {
        let mut sketch = DdSketch::new();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sketch.add(v);
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let metric = encode_metric(
            &event(),
            &record(MetricKind::Distribution(sketch)),
            &telemetry,
            &mut diag,
        )
        .unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Summary(s) => {
                assert_eq!(s.data_points[0].quantile_values.len(), 5, "p50/p75/p90/p95/p99");
            }
            other => panic!("expected Summary, got {other:?}"),
        }
        let events = registry.drain(0);
        let degraded = events.iter().find(|e| {
            e.attributes.get("metric_kind").and_then(|v| v.as_str()) == Some("distribution")
        });
        assert!(degraded.is_some(), "should count logit.output.metrics.degraded{{metric_kind}}");
    }

    #[test]
    fn a_set_members_metric_is_skipped_and_counted_rather_than_encoded_wrongly() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let result = encode_metric(
            &event(),
            &record(MetricKind::SetMembers(vec![bytes::Bytes::from_static(b"alice")])),
            &telemetry,
            &mut diag,
        );
        assert!(result.is_none(), "a SetMembers metric must not produce a Metric at all");
        let events = registry.drain(0);
        let skipped = events.iter().find(|e| {
            e.attributes.get("metric_kind").and_then(|v| v.as_str()) == Some("set_members")
        });
        assert!(skipped.is_some(), "should count logit.output.metrics.skipped{{metric_kind}}");
    }

    #[test]
    fn a_set_metric_is_skipped_and_counted_rather_than_encoded_wrongly() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let result = encode_metric(
            &event(),
            &record(MetricKind::Set(HyperLogLog::default())),
            &telemetry,
            &mut diag,
        );
        assert!(result.is_none(), "a Set metric must not produce a Metric at all");
        let events = registry.drain(0);
        let skipped = events
            .iter()
            .find(|e| e.attributes.get("metric_kind").and_then(|v| v.as_str()) == Some("set"));
        assert!(skipped.is_some(), "should count logit.output.metrics.skipped{{metric_kind}}");
    }

    /// A `GaugeDelta` reaching this encoder means the pipeline is missing an `aggregate`
    /// component (`docs/adr/relative-gauge-adjustments.md`) -- it must be dropped, not
    /// encoded as though it were an absolute value, and reported under the same greppable
    /// `gauge_delta_unresolved` diagnostic key `influxdb_out` uses, not the generic `set`-style
    /// per-kind skip key.
    #[test]
    fn a_gauge_delta_is_skipped_and_reports_its_own_diagnostic_key() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let diag_registry = Registry::new();
        let mut diag = Diagnostics::new("otlp_out")
            .with_telemetry(diag_registry.telemetry_for("otlp_out", "otlp_out", "diag"));
        let result =
            encode_metric(&event(), &record(MetricKind::GaugeDelta(5.0)), &telemetry, &mut diag);
        assert!(result.is_none(), "a GaugeDelta metric must not produce a Metric at all");
        let events = registry.drain(0);
        let skipped = events.iter().find(|e| {
            e.attributes.get("metric_kind").and_then(|v| v.as_str()) == Some("gauge_delta")
        });
        assert!(skipped.is_some(), "should count logit.output.metrics.skipped{{metric_kind}}");
        let diag_events = diag_registry.drain(0);
        let reported = diag_events.iter().find(|e| {
            e.attributes.get("key").and_then(|v| v.as_str()) == Some("gauge_delta_unresolved")
        });
        assert!(
            reported.is_some(),
            "should report under the gauge_delta_unresolved diagnostic key"
        );
    }

    #[test]
    fn a_delta_monotonic_sum_decodes_as_a_sum_with_both_flags() {
        let metric = encode(MetricKind::counter(3.0)).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => {
                assert_eq!(s.value, 3.0);
                assert_eq!(s.temporality, Temporality::Delta);
                assert!(s.monotonic);
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_cumulative_sum_decodes_as_a_sum_not_a_gauge() {
        let metric = pb::Metric {
            name: "m".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: Vec::new(),
            data: Some(pb::metric::Data::Sum(pb::Sum {
                data_points: vec![number_data_point(Vec::new(), 1000, 7.0)],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
        };
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => {
                assert_eq!(s.value, 7.0);
                assert_eq!(s.temporality, Temporality::Cumulative);
                assert!(s.monotonic);
            }
            other => panic!("a cumulative sum must decode as Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_gauge_decodes_as_a_gauge() {
        let metric = encode(MetricKind::Gauge(1.5)).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Gauge(v) => assert_eq!(*v, 1.5),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn a_no_recorded_value_flag_skips_the_point_and_counts_it_rather_than_failing() {
        let mut metric = encode(MetricKind::Gauge(1.0)).unwrap();
        if let Some(pb::metric::Data::Gauge(g)) = &mut metric.data {
            g.data_points[0].flags = 1; // DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let events = decode_metric(metric, &AttrMap::new(), &telemetry);
        assert!(events.is_empty());
        let drained = registry.drain(0);
        assert!(drained
            .iter()
            .any(|e| e.attributes.get("reason").and_then(|v| v.as_str())
                == Some("no_recorded_value")));
    }
}
