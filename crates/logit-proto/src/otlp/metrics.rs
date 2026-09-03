//! `MetricRecord` ↔ OTLP `Metric` -- the hard direction, both ways.
//!
//! **Encode.** Temporality is `DELTA` by default: `aggregate` produces tumbling deltas per
//! [ADR `aggregation-window-semantics`](../../../../docs/adr/aggregation-window-semantics.md), and `internal`'s own
//! buffer coalesces since the last drain -- neither produces a cumulative running total on its own.
//! The one exception is a `Histogram` whose event carries `otel.temporality = "cumulative"` (the
//! same attribute the decode side stamps below, round-tripped rather than silently dropped back to
//! `DELTA`) -- see the `Histogram` row. `start_time_unix_nano` and `time_unix_nano` are both
//! stamped with `Event::timestamp`: nothing upstream of this codec tracks a series' own start time
//! separately from its latest point. Event attributes become the data point's attributes; the
//! metric name/unit come from the `MetricRecord` itself. One `MetricRecord` becomes exactly one
//! OTLP `Metric` with exactly one data point -- this does **not** coalesce same-named metrics
//! across events into one wire-level `Metric.data_points` list the way a canonical OTLP producer
//! would. That's spec-legal (multiple `Metric` entries sharing a name is explicitly permitted; most
//! consumers -- including this crate's own decoder -- treat them as more points of the same series)
//! and keeps this mapping a pure per-record function instead of a batch-wide grouping pass.
//!
//! | `MetricKind` | Encodes to | Fidelity |
//! |---|---|---|
//! | `Counter(v)` | `Sum{DELTA, monotonic}` | exact |
//! | `Gauge(v)` | `Gauge` | exact |
//! | `Histogram{buckets}` | `Histogram{DELTA, or CUMULATIVE if `otel.temporality = "cumulative"`}` | exact -- `buckets` is already per-bucket, not
//! |   |   | cumulative (`metric.rs`'s doc comment, which describes the *count per bucket*, not the
//! |   |   | series' own temporality); a trailing `f64::INFINITY` bound becomes the implicit final
//! |   |   | bucket OTLP's `explicit_bounds` convention expects. |
//! | `Summary{quantiles}` | `Summary` | `count`/`sum` have no source → `0`/`0.0`, documented |
//! | `Distribution(sketch)` | `Summary` of 5 fixed quantiles (p50/p75/p90/p95/p99) | **Lossy,
//! |   |   | deliberately** -- see the module doc's "Lossy metric kinds" note below. Counted via
//! |   |   | `logit.output.metrics.degraded{metric_kind="distribution"}`. |
//! | `Set(hll)` | **skipped** | No cardinality to read (`HyperLogLog` is still a stub) -- same
//! |   |   | precedent `crates/logit-outputs/src/influxdb.rs` already sets for the same kind.
//! |   |   | Counted via `logit.output.metrics.skipped{metric_kind="set"}`, throttled-warned. |
//!
//! `Distribution`/`Set` are the qualification [ADR `committed-pregenerated-otlp-protobuf`](../../../../docs/adr/committed-pregenerated-otlp-protobuf.md)
//! spells out against [ADR `native-wire-format-with-otlp-bridge`](../../../../docs/adr/native-wire-format-with-otlp-bridge.md):
//! here it's `logit`'s own model (a mergeable sketch, a cardinality stub) that can't be losslessly
//! re-expressed *as* OTLP, not the other way around.
//!
//! **Decode.** `Sum` monotonic + `DELTA` → `Counter`. `Sum` monotonic + `CUMULATIVE` → **`Gauge`**
//! with an `otel.temporality = "cumulative"` attribute, not `Counter` -- summing a running total
//! would double-count, and last-write-wins on a monotone series is an honest representation of what
//! we actually received. Non-monotonic `Sum` → `Gauge` (same temporality attribute if cumulative).
//! `Histogram`/`ExponentialHistogram` → `Histogram{buckets}`, **also** stamping
//! `otel.temporality = "cumulative"` when the wire point is cumulative -- the same reasoning as
//! `Sum`: a cumulative histogram's bucket counts are running totals, and encoding them back out
//! unconditionally as `DELTA` (as an earlier version of this codec did) would tell a downstream
//! consumer to treat a running total as a fresh increment, double-counting on every re-export. The
//! attribute is preserved through a decode → re-encode round trip (see the `Encode` section above),
//! not silently dropped.
//!
//! `Histogram` reconstructs the trailing infinite bucket when `bucket_counts` has one more entry
//! than `explicit_bounds` (the OTLP-mandated shape). `Summary` → `Summary`, `count`/`sum` dropped.
//!
//! `ExponentialHistogram` → `Histogram{buckets}` with bounds materialized from `scale`/`offset`/
//! `zero_threshold` (`base = 2^(2^-scale)`; bucket `index`'s positive-range value range is
//! `(base^index, base^(index+1)]`, mirrored on the negative side; the zero bucket spans
//! `[-zero_threshold, zero_threshold]`) -- **exact** for the ranges an exponential histogram
//! actually reports: each real bucket's boundary is computed from its own `scale`/`offset`/index,
//! never approximated, and the result always closes both ends explicitly (a leading zero-count
//! bucket at the outermost reported edge, a trailing `f64::INFINITY` one) rather than letting this
//! codebase's own `Histogram` convention -- bucket 0 implicitly means `(-infinity, bound]` -- claim
//! a wider range than what the peer actually reported. The one narrower-than-"exact" residual: the
//! negative range's half-open convention (closed at the far-from-zero edge, open at the near-zero
//! edge) is the mirror image of this codebase's own `(prev, bound]` convention, so a value landing
//! on *exactly* a negative bucket boundary is attributed to the neighboring bucket instead of the
//! spec-correct one -- a single-point, measure-zero mislabeling for continuous-valued data, not a
//! range or count error, and not counted (nothing was mis-ranged or lost, only one boundary point's
//! label). See [`decode_exponential_buckets`] for the construction and [`BucketError`] for when it
//! gives up instead: [`MAX_DERIVED_BUCKETS`] (an upper bound against a hostile `scale`/`offset`
//! forcing a huge allocation) or a derived bound that isn't finite (an extreme `scale`) or isn't
//! strictly increasing (e.g. `zero_threshold` overlapping an adjacent exponential bucket) -- both
//! skip and count the point rather than materializing a bogus or silently-too-wide range.
//!
//! Any point with `flags & DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK` set is skipped and counted,
//! never fails the whole request -- OTLP has its own channel for reporting this back
//! (`partial_success`), wired in PR3.
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
            // Honor a decoded `otel.temporality = "cumulative"` attribute instead of hardcoding
            // DELTA unconditionally -- see the module doc's Decode section for why silently
            // flipping a cumulative histogram to DELTA on re-encode would double-count downstream.
            let cumulative = event.attributes.get("otel.temporality").and_then(|v| v.as_str())
                == Some("cumulative");
            let aggregation_temporality = if cumulative {
                pb::AggregationTemporality::Cumulative
            } else {
                pb::AggregationTemporality::Delta
            } as i32;
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
                aggregation_temporality,
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

/// `base = 2^(2^-scale)` (OTLP's own formula, `metrics.proto`'s `ExponentialHistogramDataPoint`
/// doc comment), computed entirely in `f64`. `scale` is a peer-controlled `sint32` the spec leaves
/// unrestricted -- negating it as an `i32` (`-scale`) panics in a debug build for
/// `scale == i32::MIN` (there is no positive `i32` to represent `-i32::MIN`). Casting to `f64`
/// first sidesteps that entirely: every operation below saturates to `f64::INFINITY`/`0.0` instead
/// of panicking, for any `scale`/`index`. The caller ([`decode_exponential_buckets`], via
/// [`push_bucket`]/[`bridge_to`]) rejects a non-finite result rather than treating it as a real
/// bound.
fn exponential_bound(scale: i32, index: i64) -> f64 {
    let inner = 2f64.powf(-(scale as f64));
    let base = 2f64.powf(inner);
    base.powf(index as f64)
}

/// Why an `ExponentialHistogram` data point's buckets couldn't be materialized -- both cases are
/// skip-and-count ([`decode_metric`]'s `ExponentialHistogram` arm), never a panic and never a
/// silently-too-wide range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketError {
    /// Would exceed [`MAX_DERIVED_BUCKETS`].
    OverCap,
    /// A derived bound was non-finite (an extreme `scale` overflowed `f64`'s range) or the
    /// resulting sequence wasn't strictly increasing (e.g. `zero_threshold` overlaps an adjacent
    /// exponential bucket) -- either way, the range this point claims can't be trusted.
    Inconsistent,
}

/// Appends `(bound, count)`, rejecting a bound that isn't finite or doesn't strictly exceed the
/// last one pushed. A non-strictly-increasing sequence would make a re-encode's `explicit_bounds`
/// invalid (OTLP requires it strictly increasing) or silently claim a zero-/negative-width range.
fn push_bucket(buckets: &mut Vec<(f64, u64)>, bound: f64, count: u64) -> Result<(), BucketError> {
    if !bound.is_finite() {
        return Err(BucketError::Inconsistent);
    }
    if let Some(&(last, _)) = buckets.last() {
        if bound <= last {
            return Err(BucketError::Inconsistent);
        }
    }
    buckets.push((bound, count));
    Ok(())
}

/// Closes the gap between whatever was pushed last (or, if nothing has been pushed yet, the
/// implicit `-infinity` this codebase's own `Histogram` convention starts every bucket list at)
/// and `target`, with an explicit zero-count bucket -- e.g. the range below the outermost reported
/// exponential bucket, or between the zero region and the smallest positive bucket, that nothing
/// was actually recorded in. Without this, the *first* real bucket pushed would inherit the
/// implicit `-infinity` start and silently claim everything below it too (the bug this function
/// exists to close: `scale=0, offset=0, bucket_counts=[5]` must decode to "(1, 2] = 5", not
/// "(-infinity, 2] = 5"). A no-op if `target` already equals the last bound (already contiguous);
/// rejects a `target` that would go *backwards* -- an overlap is exactly the "can't be expressed"
/// case [`decode_exponential_buckets`] gives up on.
fn bridge_to(buckets: &mut Vec<(f64, u64)>, target: f64) -> Result<(), BucketError> {
    if !target.is_finite() {
        return Err(BucketError::Inconsistent);
    }
    match buckets.last() {
        None => buckets.push((target, 0)),
        Some(&(last, _)) if target > last => buckets.push((target, 0)),
        Some(&(last, _)) if target == last => {}
        Some(_) => return Err(BucketError::Inconsistent),
    }
    Ok(())
}

/// Materializes an `ExponentialHistogramDataPoint`'s negative/zero/positive buckets into the same
/// `(bound, count)` shape `MetricKind::Histogram` uses -- ascending by bound, most-negative first,
/// always closed at both ends: a leading zero-count bucket at the outermost edge actually reported
/// (rather than the implicit `-infinity` this codebase's own `Histogram` convention would
/// otherwise apply to the first real entry) and a trailing `f64::INFINITY` one, so a re-encode
/// always produces a valid `bucket_counts.len() == explicit_bounds.len() + 1` histogram
/// (`encode_metric`'s `Histogram` arm only treats a *trailing* infinite bound as the implicit
/// overflow bucket; every other entry becomes a real, finite `explicit_bounds` value). See the
/// module doc for the residual single-point boundary caveat on the negative side.
///
/// `Err(BucketError::OverCap)` past [`MAX_DERIVED_BUCKETS`] (an upper bound on the number of
/// entries this can push, including the bridge/terminal ones -- computed before construction so a
/// hostile `scale`/`offset` can't force a large allocation first and get rejected only after).
/// `Err(BucketError::Inconsistent)` for a non-finite derived bound or a non-monotonic sequence.
fn decode_exponential_buckets(
    dp: &pb::ExponentialHistogramDataPoint,
) -> Result<Vec<(f64, u64)>, BucketError> {
    let positive = dp.positive.clone().unwrap_or_default();
    let negative = dp.negative.clone().unwrap_or_default();
    let has_zero = dp.zero_count > 0 || dp.zero_threshold > 0.0;
    // Upper bound on pushes: each real bucket, plus up to 4 zero-count entries this function can
    // add (a leading bridge before the negative range, a bridge into the zero region, the zero
    // region's own entry, a bridge into the positive range) plus the trailing infinite terminal --
    // deliberately generous rather than tracking exactly which bridges fire, since this only needs
    // to be a safe upper bound for the cap check, not a tight allocation estimate.
    let total =
        negative.bucket_counts.len() + positive.bucket_counts.len() + usize::from(has_zero) + 4;
    if total > MAX_DERIVED_BUCKETS {
        return Err(BucketError::OverCap);
    }

    let scale = dp.scale;
    let mut buckets: Vec<(f64, u64)> = Vec::with_capacity(total);

    // Negative range, most-negative bucket first (ascending value). `push_bucket`'s empty-buckets
    // case handles the very first entry here directly (no separate placeholder needed) -- but that
    // would use *this* bucket's own near edge as the implicit `-infinity` start, which is exactly
    // the bug being fixed, so `bridge_to` establishes the true outer edge first.
    if !negative.bucket_counts.is_empty() {
        let far =
            -exponential_bound(scale, negative.offset as i64 + negative.bucket_counts.len() as i64);
        bridge_to(&mut buckets, far)?;
        for (i, count) in negative.bucket_counts.iter().enumerate().rev() {
            let index = negative.offset as i64 + i as i64;
            let bound = -exponential_bound(scale, index);
            push_bucket(&mut buckets, bound, *count)?;
        }
    }

    if has_zero {
        let low = -dp.zero_threshold;
        let high = dp.zero_threshold;
        bridge_to(&mut buckets, low)?;
        push_bucket(&mut buckets, high, dp.zero_count)?;
    }

    if !positive.bucket_counts.is_empty() {
        let near = exponential_bound(scale, positive.offset as i64);
        bridge_to(&mut buckets, near)?;
        for (i, count) in positive.bucket_counts.iter().enumerate() {
            let index = positive.offset as i64 + i as i64 + 1;
            let bound = exponential_bound(scale, index);
            push_bucket(&mut buckets, bound, *count)?;
        }
    }

    if !buckets.is_empty() {
        buckets.push((f64::INFINITY, 0));
    }
    Ok(buckets)
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
        Some(pb::metric::Data::Histogram(hist)) => {
            // Preserved as an `otel.temporality` attribute, the same shape `Sum` already uses --
            // see the module doc's Decode section for why re-encoding a cumulative histogram as
            // DELTA unconditionally would double-count downstream.
            let cumulative =
                hist.aggregation_temporality == pb::AggregationTemporality::Cumulative as i32;
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
                    if cumulative {
                        attrs.insert("otel.temporality", "cumulative");
                    }
                    Some(Event::metric(
                        ts,
                        attrs,
                        MetricRecord { name, kind: MetricKind::Histogram { buckets }, unit },
                    ))
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
                Some(Event::metric(
                    ts,
                    attrs,
                    MetricRecord { name, kind: MetricKind::Summary { quantiles }, unit },
                ))
            })
            .collect(),
        Some(pb::metric::Data::ExponentialHistogram(eh)) => {
            let cumulative =
                eh.aggregation_temporality == pb::AggregationTemporality::Cumulative as i32;
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
                    let buckets = match decode_exponential_buckets(&dp) {
                        Ok(buckets) => buckets,
                        Err(BucketError::OverCap) => {
                            telemetry.count(
                                "logit.input.metrics.skipped",
                                1.0,
                                &[
                                    ("metric_kind", "exponential_histogram"),
                                    ("reason", "bucket_cap"),
                                ],
                            );
                            return None;
                        }
                        Err(BucketError::Inconsistent) => {
                            telemetry.count(
                                "logit.input.metrics.skipped",
                                1.0,
                                &[
                                    ("metric_kind", "exponential_histogram"),
                                    ("reason", "inconsistent_bounds"),
                                ],
                            );
                            return None;
                        }
                    };
                    let mut attrs = base_attrs.clone();
                    let ts = dp.time_unix_nano as i64;
                    common::key_values_into_attrs(dp.attributes, &mut attrs);
                    if cumulative {
                        attrs.insert("otel.temporality", "cumulative");
                    }
                    Some(Event::metric(
                        ts,
                        attrs,
                        MetricRecord { name, kind: MetricKind::Histogram { buckets }, unit },
                    ))
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

    /// A minimal `ExponentialHistogramDataPoint` with everything but `positive`/`negative`/
    /// `zero_count`/`zero_threshold` at a harmless default, so each test only sets what it's
    /// actually exercising.
    fn exponential_dp() -> pb::ExponentialHistogramDataPoint {
        pb::ExponentialHistogramDataPoint {
            attributes: Vec::new(),
            start_time_unix_nano: 0,
            time_unix_nano: 1000,
            count: 0,
            sum: None,
            scale: 0,
            zero_count: 0,
            positive: None,
            negative: None,
            flags: 0,
            exemplars: Vec::new(),
            min: None,
            max: None,
            zero_threshold: 0.0,
        }
    }

    #[test]
    fn an_exponential_histogram_decodes_to_explicit_bounds_derived_from_its_scale_and_offset() {
        // scale = 0 -> base = 2. offset = 0, bucket_counts = [5, 7] -> positive buckets at OTLP
        // indices 0, 1, covering (1, 2] and (2, 4] respectively -- the exact numeric case a
        // reviewer flagged as wrong: the old code returned [(2.0, 5), (4.0, 7)], whose meaning
        // under this codebase's own Histogram convention is "(-infinity, 2] = 5", not "(1, 2] = 5".
        // The leading (1.0, 0) closes that off, and the trailing (+inf, 0) is the terminal bucket
        // every non-empty result carries so a re-encode stays a valid OTLP histogram.
        let dp = pb::ExponentialHistogramDataPoint {
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![5, 7],
            }),
            ..exponential_dp()
        };
        let buckets = decode_exponential_buckets(&dp).expect("within the bucket cap");
        assert_eq!(buckets, vec![(1.0, 0), (2.0, 5), (4.0, 7), (f64::INFINITY, 0)]);
    }

    #[test]
    fn a_single_positive_bucket_decodes_to_exactly_the_range_the_reviewer_named() {
        // The reviewer's own minimal repro: scale=0, offset=0, bucket_counts=[5] must mean
        // "(1, 2] = 5", not the far wider "(-infinity, 2] = 5" the old code produced.
        let dp = pb::ExponentialHistogramDataPoint {
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![5],
            }),
            ..exponential_dp()
        };
        let buckets = decode_exponential_buckets(&dp).expect("within the bucket cap");
        assert_eq!(buckets, vec![(1.0, 0), (2.0, 5), (f64::INFINITY, 0)]);
    }

    #[test]
    fn a_re_encoded_exponential_histogram_is_a_valid_otlp_histogram_shape() {
        let dp = pb::ExponentialHistogramDataPoint {
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![5, 7],
            }),
            ..exponential_dp()
        };
        let buckets = decode_exponential_buckets(&dp).unwrap();
        let metric = encode(MetricKind::Histogram { buckets }).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Histogram(h) => {
                let point = &h.data_points[0];
                assert_eq!(
                    point.bucket_counts.len(),
                    point.explicit_bounds.len() + 1,
                    "OTLP requires bucket_counts.len() == explicit_bounds.len() + 1, got \
                     bucket_counts={:?} explicit_bounds={:?}",
                    point.bucket_counts,
                    point.explicit_bounds
                );
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_zero_bucket_uses_the_real_zero_threshold_not_a_hardcoded_zero() {
        // zero_threshold = 0.5, zero_count = 3 must decode with a zero band of width 1.0
        // (-0.5, 0.5] -- closed with an explicit (-0.5, 0) entry marking "nothing below -0.5" --
        // not collapse to a single (0.0, 3) point that loses the band's width entirely.
        let dp = pb::ExponentialHistogramDataPoint {
            zero_count: 3,
            zero_threshold: 0.5,
            ..exponential_dp()
        };
        let buckets = decode_exponential_buckets(&dp).expect("within the bucket cap");
        assert_eq!(buckets, vec![(-0.5, 0), (0.5, 3), (f64::INFINITY, 0)]);
        assert!(
            !buckets.iter().any(|(b, _)| *b == 0.0),
            "the zero bucket's bound must be the real zero_threshold, not a hardcoded 0.0"
        );
    }

    #[test]
    fn an_exponential_histogram_wider_than_the_bucket_cap_is_skipped_and_counted() {
        let dp = pb::ExponentialHistogramDataPoint {
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![1; MAX_DERIVED_BUCKETS + 1],
            }),
            ..exponential_dp()
        };
        assert_eq!(
            decode_exponential_buckets(&dp),
            Err(BucketError::OverCap),
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
    fn a_scale_of_i32_min_is_rejected_gracefully_instead_of_panicking() {
        // -scale as a plain i32 negation panics for scale == i32::MIN; this must not panic, and
        // must not silently produce a bogus bound either.
        let dp = pb::ExponentialHistogramDataPoint {
            scale: i32::MIN,
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![1],
            }),
            ..exponential_dp()
        };
        assert_eq!(decode_exponential_buckets(&dp), Err(BucketError::Inconsistent));

        // The same, through the full decode_metric path: skipped and counted, not a panic and not
        // a garbage event.
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
        assert!(events.is_empty());
        let drained = registry.drain(0);
        assert!(drained
            .iter()
            .any(|e| e.attributes.get("reason").and_then(|v| v.as_str())
                == Some("inconsistent_bounds")));
    }

    #[test]
    fn a_scale_of_i32_max_does_not_panic_either() {
        // The other extreme -- exercised the same way, since the formula's every step (2f64.powf
        // twice, then a further powf) has its own saturation behavior worth pinning against a
        // panic regardless of which direction scale is extreme in.
        let dp = pb::ExponentialHistogramDataPoint {
            scale: i32::MAX,
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![1, 1],
            }),
            ..exponential_dp()
        };
        let _ = decode_exponential_buckets(&dp); // must not panic, whatever it returns
    }

    #[test]
    fn a_cumulative_histogram_decodes_with_the_cumulative_temporality_attribute() {
        let metric = pb::Metric {
            name: "m".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: Vec::new(),
            data: Some(pb::metric::Data::Histogram(pb::Histogram {
                data_points: vec![pb::HistogramDataPoint {
                    attributes: Vec::new(),
                    start_time_unix_nano: 0,
                    time_unix_nano: 1000,
                    count: 10,
                    sum: None,
                    bucket_counts: vec![4, 6],
                    explicit_bounds: vec![5.0],
                    exemplars: Vec::new(),
                    flags: 0,
                    min: None,
                    max: None,
                }],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
        };
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].attributes.get("otel.temporality").and_then(|v| v.as_str()),
            Some("cumulative"),
            "a cumulative Histogram must be marked, not silently treated as delta"
        );
    }

    #[test]
    fn a_cumulative_histogram_re_encodes_as_cumulative_not_delta() {
        let mut attrs = AttrMap::new();
        attrs.insert("otel.temporality", "cumulative");
        let event = Event::metric(
            1000,
            attrs,
            MetricRecord {
                name: intern("m"),
                kind: MetricKind::Histogram { buckets: vec![(5.0, 4), (f64::INFINITY, 6)] },
                unit: None,
            },
        );
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&event, &event.metrics[0], &Telemetry::default(), &mut diag).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Histogram(h) => assert_eq!(
                h.aggregation_temporality,
                pb::AggregationTemporality::Cumulative as i32,
                "re-encoding a decoded cumulative histogram must not silently flip it to delta"
            ),
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_delta_histogram_has_no_temporality_attribute_and_re_encodes_as_delta() {
        let events = decode_metric(
            encode(MetricKind::Histogram { buckets: vec![(1.0, 1), (f64::INFINITY, 0)] }).unwrap(),
            &AttrMap::new(),
            &Telemetry::default(),
        );
        assert_eq!(events[0].attributes.get("otel.temporality"), None);

        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&events[0], &events[0].metrics[0], &Telemetry::default(), &mut diag)
                .unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Histogram(h) => {
                assert_eq!(h.aggregation_temporality, pb::AggregationTemporality::Delta as i32)
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    #[test]
    fn a_cumulative_exponential_histogram_decodes_with_the_cumulative_temporality_attribute() {
        let dp = pb::ExponentialHistogramDataPoint {
            positive: Some(pb::exponential_histogram_data_point::Buckets {
                offset: 0,
                bucket_counts: vec![5],
            }),
            ..exponential_dp()
        };
        let metric = pb::Metric {
            name: "m".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: Vec::new(),
            data: Some(pb::metric::Data::ExponentialHistogram(pb::ExponentialHistogram {
                data_points: vec![dp],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
        };
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].attributes.get("otel.temporality").and_then(|v| v.as_str()),
            Some("cumulative")
        );
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
