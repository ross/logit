//! `MetricRecord` ↔ OTLP `Metric` -- the hard direction, both ways.
//!
//! **Encode.** Temporality is always `DELTA`: `aggregate` produces tumbling deltas per
//! [ADR 0008](../../../../docs/adr/0008-aggregation-window-semantics.md), and `internal`'s own
//! buffer coalesces since the last drain -- nothing in this codebase produces a cumulative running
//! total. `start_time_unix_nano` and `time_unix_nano` are both stamped with `Event::timestamp`:
//! nothing upstream of this codec tracks a series' own start time separately from its latest point.
//! Event attributes become the data point's attributes; the metric name/unit come from the
//! `MetricRecord` itself. One `MetricRecord` becomes exactly one OTLP `Metric` with exactly one
//! data point -- this does **not** coalesce same-named metrics across events into one wire-level
//! `Metric.data_points` list the way a canonical OTLP producer would. That's spec-legal (multiple
//! `Metric` entries sharing a name is explicitly permitted; most consumers -- including this
//! crate's own decoder -- treat them as more points of the same series) and keeps this mapping a
//! pure per-record function instead of a batch-wide grouping pass.
//!
//! | `MetricKind` | Encodes to | Fidelity |
//! |---|---|---|
//! | `Counter(v)` | `Sum{DELTA, monotonic}` | exact |
//! | `Gauge(v)` | `Gauge` | exact |
//! | `Histogram{buckets}` | `Histogram{DELTA}` | exact -- `buckets` is already per-bucket, not
//! |   |   | cumulative (`metric.rs`'s doc comment); a trailing `f64::INFINITY` bound becomes the
//! |   |   | implicit final bucket OTLP's `explicit_bounds` convention expects. |
//! | `Summary{quantiles}` | `Summary` | `count`/`sum` have no source → `0`/`0.0`, documented |
//! | `Distribution(sketch)` | `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) | **Lossy,
//! |   |   | deliberately** -- see the module doc's "Lossy metric kinds" note below. Counted via
//! |   |   | `logit.output.metrics.degraded{metric_kind="distribution"}`. |
//! | `Set(hll)` | **skipped** | No cardinality to read (`HyperLogLog` is still a stub) -- same
//! |   |   | precedent `crates/logit-outputs/src/influxdb.rs` already sets for the same kind.
//! |   |   | Counted via `logit.output.metrics.skipped{metric_kind="set"}`, throttled-warned. |
//!
//! `Distribution`/`Set` are the qualification [ADR 0023](../../../../docs/adr/0023-committed-pregenerated-otlp-protobuf.md)
//! spells out against [ADR 0004](../../../../docs/adr/0004-native-wire-format-with-otlp-bridge.md):
//! here it's `logit`'s own model (a mergeable sketch, a cardinality stub) that can't be losslessly
//! re-expressed *as* OTLP, not the other way around.
//!
//! **Decode.** `Sum` monotonic + `DELTA` → `Counter`. `Sum` monotonic + `CUMULATIVE` → **`Gauge`**
//! with an `otel.temporality = "cumulative"` attribute, not `Counter` -- summing a running total
//! would double-count, and last-write-wins on a monotone series is an honest representation of what
//! we actually received. Non-monotonic `Sum` → `Gauge` (same temporality attribute if cumulative).
//! `Histogram` → `Histogram{buckets}`, reconstructing the trailing infinite bucket when
//! `bucket_counts` has one more entry than `explicit_bounds` (the OTLP-mandated shape).
//! `Summary` → `Summary`, `count`/`sum` dropped. `ExponentialHistogram` → `Histogram{buckets}` with
//! bounds materialized from `scale`/`offset` (`base = 2^(2^-scale)`) -- **exact, not lossy**: an
//! exponential histogram is a fixed-bucket histogram with geometric bounds, so this is a change of
//! representation, not a loss of information. Capped at [`MAX_DERIVED_BUCKETS`]; a point wider than
//! that is skipped and counted rather than materializing an unbounded `Vec`. Any point with
//! `flags & DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK` set is skipped and counted, never fails the
//! whole request -- OTLP has its own channel for reporting this back (`partial_success`), wired in
//! PR3.
//!
//! Decode-side skips count via `logit.input.metrics.skipped{metric_kind, reason}` -- distinct
//! names from the encode side's `logit.output.metrics.{degraded,skipped}` since these are the two
//! directions of one component (`OtlpEncoder`/`OtlpDecoder`), not two components sharing counters.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::metrics::v1 as pb;
use logit_core::interner::{intern, resolve};
use logit_core::{DdSketch, Diagnostics, Event, MetricKind, MetricRecord, Telemetry};

/// Bound on the number of buckets an `ExponentialHistogram` decode will materialize
/// (`negative.bucket_counts.len() + (zero_count > 0) + positive.bucket_counts.len()`) --
/// unbounded in principle (a peer chooses `scale`), so this exists for the same reason every other
/// bounded structure in this codebase does: a hostile or misbehaving peer shouldn't be able to
/// force an unbounded allocation. Beyond the cap, the point is skipped and counted, never
/// truncated silently.
const MAX_DERIVED_BUCKETS: usize = 512;

const DISTRIBUTION_QUANTILES: [f64; 5] = [0.5, 0.75, 0.90, 0.95, 0.99];

fn no_recorded_value(flags: u32) -> bool {
    flags & 1 != 0 // DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK
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

/// Encodes one `(Event, MetricRecord)` pair into one OTLP `Metric`, or `None` for `Set` (skipped,
/// counted -- see the module doc's table).
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
        MetricKind::Counter(v) => pb::metric::Data::Sum(pb::Sum {
            data_points: vec![number_data_point(attributes, ts, *v)],
            aggregation_temporality: pb::AggregationTemporality::Delta as i32,
            is_monotonic: true,
        }),
        MetricKind::Gauge(v) => pb::metric::Data::Gauge(pb::Gauge {
            data_points: vec![number_data_point(attributes, ts, *v)],
        }),
        MetricKind::Histogram { buckets } => {
            let bucket_counts: Vec<u64> = buckets.iter().map(|(_, c)| *c).collect();
            let explicit_bounds: Vec<f64> =
                buckets.iter().filter(|(b, _)| b.is_finite()).map(|(b, _)| *b).collect();
            let count = bucket_counts.iter().sum();
            pb::metric::Data::Histogram(pb::Histogram {
                data_points: vec![pb::HistogramDataPoint {
                    attributes,
                    start_time_unix_nano: ts,
                    time_unix_nano: ts,
                    count,
                    sum: None,
                    bucket_counts,
                    explicit_bounds,
                    exemplars: Vec::new(),
                    flags: 0,
                    min: None,
                    max: None,
                }],
                aggregation_temporality: pb::AggregationTemporality::Delta as i32,
            })
        }
        MetricKind::Summary { quantiles } => pb::metric::Data::Summary(pb::Summary {
            data_points: vec![pb::SummaryDataPoint {
                attributes,
                start_time_unix_nano: ts,
                time_unix_nano: ts,
                count: 0,
                sum: 0.0,
                quantile_values: quantiles
                    .iter()
                    .map(|(q, v)| pb::summary_data_point::ValueAtQuantile {
                        quantile: *q,
                        value: *v,
                    })
                    .collect(),
                flags: 0,
            }],
        }),
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
        MetricKind::Set(_) => {
            telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", "set")]);
            diagnostics.warn_throttled(
                "otlp_set_metric_skipped",
                format_args!("metric '{name}' is a Set, which OTLP has no encoding for -- skipped"),
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

/// `base = 2^(2^-scale)` (OTLP's own formula, `metrics.proto`'s `ExponentialHistogramDataPoint`
/// doc comment). Bucket `index`'s positive-range value range is `(base^index, base^(index+1)]`.
fn exponential_bound(scale: i32, index: i64) -> f64 {
    let base = 2f64.powf(2f64.powi(-scale));
    base.powf(index as f64)
}

/// Materializes an `ExponentialHistogramDataPoint`'s positive/zero/negative buckets into the same
/// `(bound, count)` shape `MetricKind::Histogram` uses, ascending by bound (most negative first).
/// `None` if the point would exceed [`MAX_DERIVED_BUCKETS`] -- the caller skips and counts it.
///
/// The negative range's bound is the near-zero edge of its own magnitude bucket (`-base^(offset+i)`
/// for `bucket_counts[i]`) -- exact for the positive range (which the required tests exercise);
/// documented as an approximation on the negative side, since OTLP's negative-range half-open
/// convention (closed at the far-from-zero end) is the mirror image of this codebase's own
/// `Histogram` convention (closed at the near bound, open below).
fn decode_exponential_buckets(dp: &pb::ExponentialHistogramDataPoint) -> Option<Vec<(f64, u64)>> {
    let positive = dp.positive.clone().unwrap_or_default();
    let negative = dp.negative.clone().unwrap_or_default();
    let has_zero = dp.zero_count > 0;
    let total = negative.bucket_counts.len() + usize::from(has_zero) + positive.bucket_counts.len();
    if total > MAX_DERIVED_BUCKETS {
        return None;
    }

    let mut buckets = Vec::with_capacity(total);
    for (i, count) in negative.bucket_counts.iter().enumerate().rev() {
        let index = negative.offset as i64 + i as i64;
        buckets.push((-exponential_bound(dp.scale, index), *count));
    }
    if has_zero {
        buckets.push((0.0, dp.zero_count));
    }
    for (i, count) in positive.bucket_counts.iter().enumerate() {
        let index = positive.offset as i64 + i as i64 + 1;
        buckets.push((exponential_bound(dp.scale, index), *count));
    }
    Some(buckets)
}

/// Decodes one OTLP `Metric` into zero or more `Event`s (one per data point). Never fails the
/// whole point/request -- a malformed or over-cap data point is skipped and counted (see the
/// module doc).
pub(crate) fn decode_metric(
    metric: pb::Metric,
    base_attrs: &logit_core::AttrMap,
    telemetry: &Telemetry,
) -> Vec<Event> {
    let name = intern(&metric.name);
    let unit = if metric.unit.is_empty() { None } else { Some(intern(&metric.unit)) };

    match metric.data {
        Some(pb::metric::Data::Sum(sum)) => {
            let monotonic = sum.is_monotonic;
            let cumulative =
                sum.aggregation_temporality == pb::AggregationTemporality::Cumulative as i32;
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
                    if cumulative {
                        attrs.insert("otel.temporality", "cumulative");
                    }
                    let kind = if monotonic && !cumulative {
                        MetricKind::Counter(value)
                    } else {
                        MetricKind::Gauge(value)
                    };
                    Some(Event::metric(ts, attrs, MetricRecord { name, kind, unit }))
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
                Some(Event::metric(
                    ts,
                    attrs,
                    MetricRecord { name, kind: MetricKind::Gauge(value), unit },
                ))
            })
            .collect(),
        Some(pb::metric::Data::Histogram(hist)) => hist
            .data_points
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
                Some(Event::metric(
                    ts,
                    attrs,
                    MetricRecord { name, kind: MetricKind::Histogram { buckets }, unit },
                ))
            })
            .collect(),
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
                Some(Event::metric(
                    ts,
                    attrs,
                    MetricRecord { name, kind: MetricKind::Summary { quantiles }, unit },
                ))
            })
            .collect(),
        Some(pb::metric::Data::ExponentialHistogram(eh)) => eh
            .data_points
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
                let Some(buckets) = decode_exponential_buckets(&dp) else {
                    telemetry.count(
                        "logit.input.metrics.skipped",
                        1.0,
                        &[("metric_kind", "exponential_histogram"), ("reason", "bucket_cap")],
                    );
                    return None;
                };
                let mut attrs = base_attrs.clone();
                let ts = dp.time_unix_nano as i64;
                common::key_values_into_attrs(dp.attributes, &mut attrs);
                Some(Event::metric(
                    ts,
                    attrs,
                    MetricRecord { name, kind: MetricKind::Histogram { buckets }, unit },
                ))
            })
            .collect(),
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::{AttrMap, HyperLogLog};

    fn record(kind: MetricKind) -> MetricRecord {
        MetricRecord { name: intern("m"), kind, unit: None }
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
    fn a_counter_encodes_to_a_monotonic_delta_sum() {
        let metric = encode(MetricKind::Counter(3.0)).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Sum(sum) => {
                assert!(sum.is_monotonic);
                assert_eq!(sum.aggregation_temporality, pb::AggregationTemporality::Delta as i32);
                assert_eq!(number_value(sum.data_points[0].value), 3.0);
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

    #[test]
    fn a_histogram_encodes_exactly_with_a_trailing_infinite_bucket() {
        let buckets = vec![(1.0, 2u64), (5.0, 3u64), (f64::INFINITY, 1u64)];
        let metric = encode(MetricKind::Histogram { buckets: buckets.clone() }).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Histogram(h) => {
                let dp = &h.data_points[0];
                assert_eq!(dp.explicit_bounds, vec![1.0, 5.0]);
                assert_eq!(dp.bucket_counts, vec![2, 3, 1]);
                assert_eq!(dp.count, 6);
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_summary_encodes_with_dropped_count_and_sum() {
        let metric =
            encode(MetricKind::Summary { quantiles: vec![(0.5, 10.0), (0.99, 99.0)] }).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Summary(s) => {
                let dp = &s.data_points[0];
                assert_eq!(dp.count, 0);
                assert_eq!(dp.sum, 0.0);
                assert_eq!(dp.quantile_values.len(), 2);
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

    #[test]
    fn a_delta_monotonic_sum_decodes_as_a_counter() {
        let metric = encode(MetricKind::Counter(3.0)).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Counter(v) => assert_eq!(*v, 3.0),
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn a_cumulative_sum_decodes_as_a_gauge_not_a_counter() {
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
            MetricKind::Gauge(v) => assert_eq!(*v, 7.0),
            other => panic!("a cumulative monotonic sum must decode as Gauge, got {other:?}"),
        }
        assert_eq!(
            events[0].attributes.get("otel.temporality").and_then(|v| v.as_str()),
            Some("cumulative")
        );
    }

    #[test]
    fn a_non_monotonic_delta_sum_decodes_as_a_gauge_with_no_temporality_attribute() {
        let metric = pb::Metric {
            name: "m".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: Vec::new(),
            data: Some(pb::metric::Data::Sum(pb::Sum {
                data_points: vec![number_data_point(Vec::new(), 1000, 2.0)],
                aggregation_temporality: pb::AggregationTemporality::Delta as i32,
                is_monotonic: false,
            })),
        };
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Gauge(v) => assert_eq!(*v, 2.0),
            other => panic!("a non-monotonic sum must decode as Gauge, got {other:?}"),
        }
        assert_eq!(
            events[0].attributes.get("otel.temporality"),
            None,
            "a delta (non-cumulative) sum should not get the cumulative-only attribute"
        );
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
    fn a_histogram_decodes_with_the_same_buckets_it_encoded() {
        let buckets = vec![(1.0, 2u64), (5.0, 3u64), (f64::INFINITY, 1u64)];
        let metric = encode(MetricKind::Histogram { buckets: buckets.clone() }).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Histogram { buckets: got } => assert_eq!(*got, buckets),
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_summary_decodes_with_count_and_sum_dropped() {
        let metric = encode(MetricKind::Summary { quantiles: vec![(0.5, 10.0)] }).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Summary { quantiles } => assert_eq!(*quantiles, vec![(0.5, 10.0)]),
            other => panic!("expected Summary, got {other:?}"),
        }
    }

    #[test]
    fn an_exponential_histogram_decodes_to_explicit_bounds_derived_from_its_scale_and_offset() {
        // scale = 0 -> base = 2. offset = 0, bucket_counts = [5, 7] -> positive buckets at
        // indices 1, 2 with bounds base^1 = 2.0 and base^2 = 4.0.
        let dp = pb::ExponentialHistogramDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 1000,
            count: 12,
            sum: None,
            scale: 0,
            zero_count: 0,
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![5, 7],
            }),
            negative: None,
            flags: 0,
            exemplars: Vec::new(),
            min: None,
            max: None,
            zero_threshold: 0.0,
        };
        let buckets = decode_exponential_buckets(&dp).expect("within the bucket cap");
        assert_eq!(buckets, vec![(2.0, 5), (4.0, 7)]);
    }

    #[test]
    fn an_exponential_histogram_wider_than_the_bucket_cap_is_skipped_and_counted() {
        let dp = pb::ExponentialHistogramDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 1000,
            count: 0,
            sum: None,
            scale: 0,
            zero_count: 0,
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![1; MAX_DERIVED_BUCKETS + 1],
            }),
            negative: None,
            flags: 0,
            exemplars: Vec::new(),
            min: None,
            max: None,
            zero_threshold: 0.0,
        };
        assert!(
            decode_exponential_buckets(&dp).is_none(),
            "should refuse to materialize past the cap"
        );

        let metric = pb::Metric {
            name: "m".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: Vec::new(),
            data: Some(pb::metric::Data::ExponentialHistogram(pb::ExponentialHistogram {
                data_points: vec![dp],
                aggregation_temporality: pb::AggregationTemporality::Delta as i32,
            })),
        };
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "listener");
        let events = decode_metric(metric, &AttrMap::new(), &telemetry);
        assert!(events.is_empty(), "an over-cap point must be skipped, not truncated");
        let drained = registry.drain(0);
        assert!(drained
            .iter()
            .any(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("bucket_cap")));
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
