//! `MetricRecord` ↔ OTLP `Metric` -- the hard direction, both ways.
//!
//! **Encode.** `time_unix_nano` is stamped with `Event::timestamp`; `start_time_unix_nano` is
//! `record.start_timestamp` written through verbatim, with no fallback to `Event::timestamp` --
//! OTLP's own `start_time_unix_nano` "unknown" sentinel is `0` (`start_time.proto`'s own doc
//! comment: optional, `0` = unknown), the same convention this model's `start_timestamp == 0`
//! already uses, so a `record.start_timestamp` of `0` must stay `0` on the wire rather than
//! borrowing the event's own timestamp and advertising a zero-width interval a rate-computing
//! backend can misread as real. `record.flags` (OTLP `DataPointFlags`,
//! e.g. `NO_RECORDED_VALUE`) is written into every data point's own `flags`. `record.description`
//! resolves onto `Metric.description` when `Some`, else the wire field stays empty. Event
//! attributes become the data point's attributes; the metric name/unit come from the
//! `MetricRecord` itself. One `MetricRecord` becomes exactly one OTLP `Metric` with exactly one
//! data point -- this does **not** coalesce same-named metrics across events into one wire-level
//! `Metric.data_points` list the way a canonical OTLP producer would. That's spec-legal (multiple
//! `Metric` entries sharing a name is explicitly permitted; most consumers -- including this
//! crate's own decoder -- treat them as more points of the same series) and keeps this mapping a
//! pure per-record function instead of a batch-wide grouping pass.
//!
//! | `MetricKind` | Encodes to | Fidelity |
//! |---|---|---|
//! | `Sum{value,temporality,monotonic}` | `Sum{temporality,monotonic}` | exact -- both flags ride real fields now, not a well-known attribute; carries `record.exemplars` |
//! | `Gauge(v)` | `Gauge` | exact; carries `record.exemplars` |
//! | `Histogram{buckets,temporality,sum,min,max}` | `Histogram{temporality,sum,min,max}` | exact -- `buckets` is already per-bucket, not
//! |   |   | cumulative (`metric.rs`'s doc comment, which describes the *count per bucket*, not the
//! |   |   | series' own temporality); a trailing `f64::INFINITY` bound becomes the implicit final
//! |   |   | bucket OTLP's `explicit_bounds` convention expects; carries `record.exemplars`. |
//! | `ExponentialHistogram(e)` | `ExponentialHistogram` | exact -- 1:1 field mapping, kept as its own
//! |   |   | variant specifically so `otlp_in -> otlp_out` is a fixed point for this type; carries `record.exemplars`. |
//! | `Summary{quantiles,count,sum}` | `Summary{count,sum}` | exact on `quantiles`/`count`/`sum` --
//! |   |   | **but `record.exemplars` is dropped**: `SummaryDataPoint` has no `exemplars` field on
//! |   |   | the wire at all (OTLP spec), so this is a genuine, documented degradation, not this
//! |   |   | module's choice. A `Samples`/`Distribution` degrading into a `Summary` below loses its
//! |   |   | exemplars for the same reason. |
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
//! deriving explicit bounds from it. `start_time_unix_nano` → `record.start_timestamp` verbatim
//! (`0` stays `0`, this model's own "unknown" convention). `Metric.description`, when non-empty,
//! interns onto `record.description`. A data point's `exemplars` decode onto `record.exemplars`
//! for every kind that carries them on the wire (`Sum`/`Gauge`/`Histogram`/`ExponentialHistogram`
//! -- `SummaryDataPoint` has none, per the encode table above).
//!
//! **`NO_RECORDED_VALUE` keeps the point, flagged, rather than skipping it.** A data point with
//! `DataPointFlags::FLAG_NO_RECORDED_VALUE` set (bit 0 of `flags`) decodes like any other point --
//! `record.flags` carries the bit forward and the point's numeric value decodes exactly as sent
//! (typically the wire's own zero value for a flagged point, since a well-behaved producer has
//! nothing meaningful to put there) -- rather than being silently dropped. This is what makes
//! `otlp_in -> otlp_out` a fixed point for a flagged point: encode below writes `record.flags`
//! back onto the re-encoded data point unchanged. `docs/adr/metrics-model-v2.md`'s W4 amendment.
//!
//! `Metric.metadata` stays dropped both ways -- nothing in this model has anywhere to put a
//! metric-level (not data-point-level) attribute set, and OTLP itself documents it as informational
//! only (`metrics.proto`: "Consumers SHOULD NOT need to be aware of these attributes").
//!
//! The one decode-side skip -- a `Metric` with no recognized `data` oneof (`data: None`) -- counts
//! via `logit.input.metrics.skipped{metric_kind="unknown", reason="no_data"}` -- a distinct
//! counter name from the encode side's `logit.output.metrics.{degraded,skipped}` since these are
//! the two directions of one component (`OtlpEncoder`/`OtlpDecoder`), not two components sharing
//! counters.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::metrics::v1 as pb;
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, DdSketch, Diagnostics, Event, Exemplar, ExpHistogram, Histogram, MetricKind,
    MetricRecord, Sum, Summary, Telemetry, Temporality, TraceRef,
};

const DISTRIBUTION_QUANTILES: [f64; 5] = [0.5, 0.75, 0.90, 0.95, 0.99];

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

/// `record.start_timestamp` written through verbatim -- OTLP's own `start_time_unix_nano`
/// convention for "unknown" (`0`) matches this model's, so there is no separate sentinel to
/// translate and no fallback to `Event::timestamp`: see the module doc's encode paragraph for why
/// substituting the event's own timestamp would be wrong, not just unnecessary.
fn start_time(record_start: i64) -> u64 {
    record_start.max(0) as u64
}

/// OTLP's `Exemplar` message has no trace-flags field at all, so `e.trace`'s `TraceRef.flags` is
/// dropped here -- a real, permanent lossy mapping (see `decode_exemplar`'s mirror note and
/// `docs/known-gaps.md`'s cross-protocol table).
fn encode_exemplar(e: &Exemplar) -> pb::Exemplar {
    let (trace_id, span_id) = match &e.trace {
        Some(t) => (t.trace_id.to_vec(), t.span_id.map(|id| id.to_vec()).unwrap_or_default()),
        None => (Vec::new(), Vec::new()),
    };
    pb::Exemplar {
        filtered_attributes: common::attrs_to_key_values(&e.filtered_attributes),
        time_unix_nano: e.timestamp.max(0) as u64,
        span_id,
        trace_id,
        value: Some(pb::exemplar::Value::AsDouble(e.value)),
    }
}

/// `trace`/`span` id bytes become a [`TraceRef`] only when `trace_id` is a genuine, non-empty,
/// non-all-zero 16 bytes -- [`TraceRef::from_bytes`]'s own validity rule (the same one
/// `../logs.rs` applies to a `LogRecord`'s trace context), since an `Exemplar`'s correlation is
/// optional, best-effort metadata a decoder degrades gracefully without. `TraceRef.flags` is
/// hardcoded to `0`: OTLP's `Exemplar` has no wire field to read it from (see `encode_exemplar`'s
/// mirror note) -- a real, permanent lossy mapping, not a decode shortcut.
fn decode_exemplar(e: pb::Exemplar) -> Exemplar {
    let value = match e.value {
        Some(pb::exemplar::Value::AsDouble(d)) => d,
        Some(pb::exemplar::Value::AsInt(i)) => i as f64,
        None => 0.0,
    };
    let trace = TraceRef::from_bytes(&e.trace_id, &e.span_id, 0);
    let mut filtered_attributes = AttrMap::new();
    common::key_values_into_attrs(e.filtered_attributes, &mut filtered_attributes);
    Exemplar { timestamp: e.time_unix_nano as i64, value, trace, filtered_attributes }
}

fn encode_exemplars(exemplars: &[Exemplar]) -> Vec<pb::Exemplar> {
    exemplars.iter().map(encode_exemplar).collect()
}

fn decode_exemplars(exemplars: Vec<pb::Exemplar>) -> Vec<Exemplar> {
    exemplars.into_iter().map(decode_exemplar).collect()
}

#[allow(clippy::too_many_arguments)]
fn number_data_point(
    attributes: Vec<crate::otlp::generated::opentelemetry::proto::common::v1::KeyValue>,
    start_time_unix_nano: u64,
    ts: u64,
    flags: u32,
    exemplars: Vec<pb::Exemplar>,
    value: f64,
) -> pb::NumberDataPoint {
    pb::NumberDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano: ts,
        exemplars,
        flags,
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
    let description = record.description.map(resolve).unwrap_or_default().to_string();
    let attributes = common::attrs_to_key_values(&event.attributes);
    let ts = event.timestamp.max(0) as u64;
    let start = start_time(record.start_timestamp);
    let exemplars = encode_exemplars(&record.exemplars);

    let data = match &record.kind {
        MetricKind::Sum(s) => pb::metric::Data::Sum(pb::Sum {
            data_points: vec![number_data_point(
                attributes,
                start,
                ts,
                record.flags,
                exemplars,
                s.value,
            )],
            aggregation_temporality: temporality_to_pb(s.temporality),
            is_monotonic: s.monotonic,
        }),
        MetricKind::Gauge(v) => pb::metric::Data::Gauge(pb::Gauge {
            data_points: vec![number_data_point(
                attributes,
                start,
                ts,
                record.flags,
                exemplars,
                *v,
            )],
        }),
        MetricKind::Histogram(h) => {
            let bucket_counts: Vec<u64> = h.buckets.iter().map(|(_, c)| *c).collect();
            let explicit_bounds: Vec<f64> =
                h.buckets.iter().filter(|(b, _)| b.is_finite()).map(|(b, _)| *b).collect();
            let count = bucket_counts.iter().sum();
            pb::metric::Data::Histogram(pb::Histogram {
                data_points: vec![pb::HistogramDataPoint {
                    attributes,
                    start_time_unix_nano: start,
                    time_unix_nano: ts,
                    count,
                    sum: h.sum,
                    bucket_counts,
                    explicit_bounds,
                    exemplars,
                    flags: record.flags,
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
                    start_time_unix_nano: start,
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
                    flags: record.flags,
                    exemplars,
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
                start_time_unix_nano: start,
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
                // No `exemplars` field on the wire type at all -- see the module doc's encode
                // table.
                flags: record.flags,
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
                data_points: vec![distribution_summary_point(
                    attributes,
                    start,
                    ts,
                    record.flags,
                    &sketch,
                )],
            })
        }
        MetricKind::Distribution(sketch) => {
            telemetry.count(
                "logit.output.metrics.degraded",
                1.0,
                &[("metric_kind", "distribution")],
            );
            pb::metric::Data::Summary(pb::Summary {
                data_points: vec![distribution_summary_point(
                    attributes,
                    start,
                    ts,
                    record.flags,
                    sketch,
                )],
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

    Some(pb::Metric { name, description, unit, metadata: Vec::new(), data: Some(data) })
}

fn distribution_summary_point(
    attributes: Vec<crate::otlp::generated::opentelemetry::proto::common::v1::KeyValue>,
    start_time_unix_nano: u64,
    ts: u64,
    flags: u32,
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
        start_time_unix_nano,
        time_unix_nano: ts,
        count: sketch.count() as u64,
        sum: 0.0,
        quantile_values,
        flags,
    }
}

/// Decodes one OTLP `Metric` into zero or more `Event`s (one per data point). Never fails the
/// whole point/request -- see the module doc for the `NO_RECORDED_VALUE` handling (the point is
/// kept, flagged, not skipped).
pub(crate) fn decode_metric(
    metric: pb::Metric,
    base_attrs: &logit_core::AttrMap,
    telemetry: &Telemetry,
) -> Vec<Event> {
    let name = intern(&metric.name);
    let unit = if metric.unit.is_empty() { None } else { Some(intern(&metric.unit)) };
    let description =
        if metric.description.is_empty() { None } else { Some(intern(&metric.description)) };
    let record = |kind: MetricKind, start_timestamp: i64, flags: u32, exemplars: Vec<Exemplar>| {
        MetricRecord { name, unit, description, start_timestamp, exemplars, flags, kind }
    };

    match metric.data {
        Some(pb::metric::Data::Sum(sum)) => {
            let monotonic = sum.is_monotonic;
            let temporality = temporality_from_pb(sum.aggregation_temporality);
            sum.data_points
                .into_iter()
                .map(|dp| {
                    let mut attrs = base_attrs.clone();
                    let ts = dp.time_unix_nano as i64;
                    let value = number_value(dp.value);
                    common::key_values_into_attrs(dp.attributes, &mut attrs);
                    let kind = MetricKind::Sum(Sum { value, temporality, monotonic });
                    let exemplars = decode_exemplars(dp.exemplars);
                    let rec = record(kind, dp.start_time_unix_nano as i64, dp.flags, exemplars);
                    Event::metric(ts, attrs, rec)
                })
                .collect()
        }
        Some(pb::metric::Data::Gauge(gauge)) => gauge
            .data_points
            .into_iter()
            .map(|dp| {
                let mut attrs = base_attrs.clone();
                let ts = dp.time_unix_nano as i64;
                let value = number_value(dp.value);
                common::key_values_into_attrs(dp.attributes, &mut attrs);
                let exemplars = decode_exemplars(dp.exemplars);
                let rec = record(
                    MetricKind::Gauge(value),
                    dp.start_time_unix_nano as i64,
                    dp.flags,
                    exemplars,
                );
                Event::metric(ts, attrs, rec)
            })
            .collect(),
        Some(pb::metric::Data::Histogram(hist)) => {
            let temporality = temporality_from_pb(hist.aggregation_temporality);
            hist.data_points
                .into_iter()
                .map(|dp| {
                    let mut attrs = base_attrs.clone();
                    let ts = dp.time_unix_nano as i64;
                    let mut buckets = Vec::with_capacity(dp.bucket_counts.len());
                    for (i, count) in dp.bucket_counts.iter().enumerate() {
                        let bound = dp.explicit_bounds.get(i).copied().unwrap_or(f64::INFINITY);
                        buckets.push((bound, *count));
                    }
                    let start_timestamp = dp.start_time_unix_nano as i64;
                    let flags = dp.flags;
                    let exemplars = decode_exemplars(dp.exemplars);
                    common::key_values_into_attrs(dp.attributes, &mut attrs);
                    let kind = MetricKind::Histogram(Histogram {
                        buckets,
                        temporality,
                        sum: dp.sum,
                        min: dp.min,
                        max: dp.max,
                    });
                    Event::metric(ts, attrs, record(kind, start_timestamp, flags, exemplars))
                })
                .collect()
        }
        Some(pb::metric::Data::Summary(summary)) => summary
            .data_points
            .into_iter()
            .map(|dp| {
                let mut attrs = base_attrs.clone();
                let ts = dp.time_unix_nano as i64;
                let quantiles = dp.quantile_values.iter().map(|q| (q.quantile, q.value)).collect();
                let start_timestamp = dp.start_time_unix_nano as i64;
                let flags = dp.flags;
                common::key_values_into_attrs(dp.attributes, &mut attrs);
                let kind = MetricKind::Summary(Summary { quantiles, count: dp.count, sum: dp.sum });
                // No `exemplars` field on the wire type at all -- see the module doc.
                let rec = record(kind, start_timestamp, flags, Vec::new());
                Event::metric(ts, attrs, rec)
            })
            .collect(),
        Some(pb::metric::Data::ExponentialHistogram(eh)) => {
            let temporality = temporality_from_pb(eh.aggregation_temporality);
            eh.data_points
                .into_iter()
                .map(|dp| {
                    let mut attrs = base_attrs.clone();
                    let ts = dp.time_unix_nano as i64;
                    let start_timestamp = dp.start_time_unix_nano as i64;
                    let flags = dp.flags;
                    let exemplars = decode_exemplars(dp.exemplars);
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
                    Event::metric(ts, attrs, record(kind, start_timestamp, flags, exemplars))
                })
                .collect()
        }
        None => {
            telemetry.count(
                "logit.input.metrics.skipped",
                1.0,
                &[("metric_kind", "unknown"), ("reason", "no_data")],
            );
            Vec::new()
        }
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
                data_points: vec![number_data_point(Vec::new(), 1000, 1000, 0, Vec::new(), 7.0)],
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

    /// A `Metric` with no recognized `data` oneof (`data: None`) decodes to no events, same as
    /// before, but must now also count `logit.input.metrics.skipped{metric_kind="unknown",
    /// reason="no_data"}` -- the counter this test pins was dead code before this fix (review
    /// finding 4).
    #[test]
    fn a_metric_with_no_data_is_skipped_and_counted() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "source");
        let metric = pb::Metric {
            name: "no_data_metric".to_string(),
            description: String::new(),
            unit: String::new(),
            metadata: Vec::new(),
            data: None,
        };
        let events = decode_metric(metric, &AttrMap::new(), &telemetry);
        assert!(events.is_empty(), "a Metric with no data must decode to no events");
        let counted = registry.drain(0);
        let skipped = counted.iter().find(|e| {
            e.attributes.get("metric_kind").and_then(|v| v.as_str()) == Some("unknown")
                && e.attributes.get("reason").and_then(|v| v.as_str()) == Some("no_data")
        });
        assert!(
            skipped.is_some(),
            "should count logit.input.metrics.skipped{{metric_kind=\"unknown\", reason=\"no_data\"}}"
        );
    }

    /// `NO_RECORDED_VALUE` keeps the point (flagged), rather than skipping it -- W4 amendment to
    /// `docs/adr/metrics-model-v2.md`, see the module doc.
    #[test]
    fn a_no_recorded_value_flag_keeps_the_point_flagged_rather_than_skipping_it() {
        let mut metric = encode(MetricKind::Gauge(1.0)).unwrap();
        if let Some(pb::metric::Data::Gauge(g)) = &mut metric.data {
            g.data_points[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        }
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1, "a NO_RECORDED_VALUE point must be kept, not skipped");
        assert_eq!(events[0].metrics[0].flags, MetricRecord::FLAG_NO_RECORDED_VALUE);
    }

    /// Encode writes `record.flags` back onto the data point unchanged -- what makes
    /// `otlp_in -> otlp_out` a fixed point for a flagged Sum/Gauge/Histogram/ExponentialHistogram/
    /// Summary point.
    #[test]
    fn a_flagged_record_round_trips_its_flags_for_every_kind() {
        let flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let kinds = [
            MetricKind::counter(1.0),
            MetricKind::Gauge(2.0),
            MetricKind::Histogram(histogram(Temporality::Delta)),
            MetricKind::ExponentialHistogram(exp_histogram(Temporality::Delta)),
            MetricKind::Summary(Summary { quantiles: vec![(0.5, 1.0)], count: 1, sum: 1.0 }),
        ];
        for kind in kinds {
            let rec = MetricRecord { flags, ..MetricRecord::new(intern("m"), kind.clone()) };
            let mut diag = Diagnostics::default();
            let metric = encode_metric(&event(), &rec, &Telemetry::default(), &mut diag).unwrap();
            let dp_flags = match &metric.data {
                Some(pb::metric::Data::Sum(s)) => s.data_points[0].flags,
                Some(pb::metric::Data::Gauge(g)) => g.data_points[0].flags,
                Some(pb::metric::Data::Histogram(h)) => h.data_points[0].flags,
                Some(pb::metric::Data::ExponentialHistogram(h)) => h.data_points[0].flags,
                Some(pb::metric::Data::Summary(s)) => s.data_points[0].flags,
                None => panic!("expected data for {kind:?}"),
            };
            assert_eq!(dp_flags, flags, "wire flags should carry record.flags for {kind:?}");

            let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
            assert_eq!(
                events[0].metrics[0].flags, flags,
                "decoded flags should round-trip for {kind:?}"
            );
        }
    }

    #[test]
    fn a_non_zero_start_timestamp_is_preferred_over_the_events_timestamp() {
        let mut rec = record(MetricKind::Gauge(1.0));
        rec.start_timestamp = 500;
        let mut diag = Diagnostics::default();
        let metric = encode_metric(&event(), &rec, &Telemetry::default(), &mut diag).unwrap();
        match &metric.data {
            Some(pb::metric::Data::Gauge(g)) => {
                assert_eq!(g.data_points[0].start_time_unix_nano, 500);
            }
            other => panic!("expected Gauge, got {other:?}"),
        }
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events[0].metrics[0].start_timestamp, 500);
    }

    #[test]
    fn a_zero_start_timestamp_stays_zero_on_the_wire() {
        let rec = record(MetricKind::Gauge(1.0)); // start_timestamp: 0 (unknown)
        let mut diag = Diagnostics::default();
        let metric = encode_metric(&event(), &rec, &Telemetry::default(), &mut diag).unwrap();
        match &metric.data {
            Some(pb::metric::Data::Gauge(g)) => {
                assert_eq!(
                    g.data_points[0].start_time_unix_nano, 0,
                    "an unknown start_timestamp must stay 0 on the wire, not borrow the \
                     event's own timestamp"
                );
            }
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn a_description_round_trips_when_present_and_is_empty_when_absent() {
        let mut rec = record(MetricKind::Gauge(1.0));
        rec.description = Some(intern("a metric description"));
        let mut diag = Diagnostics::default();
        let metric = encode_metric(&event(), &rec, &Telemetry::default(), &mut diag).unwrap();
        assert_eq!(metric.description, "a metric description");
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events[0].metrics[0].description.map(resolve), Some("a metric description"));

        let no_description = record(MetricKind::Gauge(1.0));
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&event(), &no_description, &Telemetry::default(), &mut diag).unwrap();
        assert_eq!(metric.description, "");
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events[0].metrics[0].description, None);
    }

    fn exemplar() -> Exemplar {
        let mut filtered_attributes = AttrMap::new();
        filtered_attributes.insert("dropped", "attr");
        Exemplar {
            timestamp: 42,
            value: 3.5,
            trace: Some(TraceRef { trace_id: [7; 16], span_id: Some([8; 8]), flags: 0 }),
            filtered_attributes,
        }
    }

    /// Exemplars round trip for every kind that carries them on the wire.
    #[test]
    fn exemplars_round_trip_for_sum_gauge_histogram_and_exponential_histogram() {
        let kinds = [
            MetricKind::counter(1.0),
            MetricKind::Gauge(2.0),
            MetricKind::Histogram(histogram(Temporality::Delta)),
            MetricKind::ExponentialHistogram(exp_histogram(Temporality::Delta)),
        ];
        for kind in kinds {
            let rec = MetricRecord {
                exemplars: vec![exemplar()],
                ..MetricRecord::new(intern("m"), kind.clone())
            };
            let mut diag = Diagnostics::default();
            let metric = encode_metric(&event(), &rec, &Telemetry::default(), &mut diag).unwrap();
            let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
            assert_eq!(
                events[0].metrics[0].exemplars,
                vec![exemplar()],
                "exemplars should round-trip for {kind:?}"
            );
        }
    }

    /// `SummaryDataPoint` has no `exemplars` field on the wire at all -- a documented degradation,
    /// not a bug (see the module doc's encode table).
    #[test]
    fn a_summary_drops_its_exemplars_because_the_wire_type_has_nowhere_to_put_them() {
        let s = Summary { quantiles: vec![(0.5, 1.0)], count: 1, sum: 1.0 };
        let rec = MetricRecord {
            exemplars: vec![exemplar()],
            ..MetricRecord::new(intern("m"), MetricKind::Summary(s))
        };
        let mut diag = Diagnostics::default();
        let metric = encode_metric(&event(), &rec, &Telemetry::default(), &mut diag).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert!(
            events[0].metrics[0].exemplars.is_empty(),
            "a Summary must not carry exemplars back -- the wire type has no field for them"
        );
    }

    // -- proptest: decode(encode(x)) == x over a generator of MetricRecords -----------------

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        fn arb_temporality() -> impl Strategy<Value = Temporality> {
            prop_oneof![Just(Temporality::Delta), Just(Temporality::Cumulative)]
        }

        fn arb_sum() -> impl Strategy<Value = Sum> {
            (-1e6f64..1e6f64, arb_temporality(), any::<bool>())
                .prop_map(|(value, temporality, monotonic)| Sum { value, temporality, monotonic })
        }

        fn arb_histogram() -> impl Strategy<Value = Histogram> {
            (
                prop::collection::vec((-1e6f64..1e6f64, 0u64..1000u64), 0..5),
                arb_temporality(),
                prop::option::of(-1e6f64..1e6f64),
                prop::option::of(-1e6f64..1e6f64),
                prop::option::of(-1e6f64..1e6f64),
            )
                .prop_map(|(buckets, temporality, sum, min, max)| Histogram {
                    buckets,
                    temporality,
                    sum,
                    min,
                    max,
                })
        }

        fn arb_summary() -> impl Strategy<Value = Summary> {
            (
                prop::collection::vec((0.0f64..1.0f64, -1e6f64..1e6f64), 0..5),
                0u64..1000u64,
                -1e6f64..1e6f64,
            )
                .prop_map(|(quantiles, count, sum)| Summary { quantiles, count, sum })
        }

        /// Restricted to `Sum`/`Gauge`/`Histogram`/`Summary`, per the plan: `ExponentialHistogram`
        /// already has its own dedicated 1:1 fixed-point tests above, and
        /// `Samples`/`Distribution`/`SetMembers`/`Set`/`GaugeDelta` either degrade or skip on
        /// encode by design (see the module doc's table) -- none of those five round-trips
        /// `decode(encode(x)) == x` at all, so a generator that could produce them would be
        /// asserting something this codec never promised.
        fn arb_kind() -> impl Strategy<Value = MetricKind> {
            prop_oneof![
                arb_sum().prop_map(MetricKind::Sum),
                (-1e6f64..1e6f64).prop_map(MetricKind::Gauge),
                arb_histogram().prop_map(MetricKind::Histogram),
                arb_summary().prop_map(MetricKind::Summary),
            ]
        }

        /// A fixed, non-zero id pattern -- `TraceRef::from_bytes`'s own validity rule rejects an
        /// all-zero `trace_id` (see `crates/logit-core/src/trace.rs`), so the generator only needs
        /// to vary *whether* a trace is present, not its exact bytes.
        fn arb_exemplar() -> impl Strategy<Value = Exemplar> {
            (0i64..2_000_000_000_000_000_000i64, -1e6f64..1e6f64, any::<bool>()).prop_map(
                |(timestamp, value, has_trace)| {
                    let trace = has_trace.then_some(TraceRef {
                        trace_id: [0xAB; 16],
                        span_id: Some([0xCD; 8]),
                        flags: 0,
                    });
                    Exemplar { timestamp, value, trace, filtered_attributes: AttrMap::new() }
                },
            )
        }

        /// Non-empty on `Some` -- an empty `Some(String::new())` description would resolve to an
        /// empty wire string, which `decode_metric` (correctly) treats the same as "absent"
        /// (`Metric.description.is_empty()`), so it would decode back as `None`, not the `Some("")`
        /// the record started with. A description is never really an empty string in practice, so
        /// excluding it from the generator isn't a gap this proptest is pretending doesn't exist.
        fn arb_description() -> impl Strategy<Value = Option<String>> {
            prop::option::of("[a-zA-Z][a-zA-Z0-9_]{0,11}")
        }

        fn arb_metric_record() -> impl Strategy<Value = MetricRecord> {
            (
                arb_kind(),
                arb_description(),
                0i64..2_000_000_000_000_000_000i64,
                prop_oneof![Just(0u32), Just(MetricRecord::FLAG_NO_RECORDED_VALUE)],
                prop::option::of(arb_exemplar()),
            )
                .prop_map(|(kind, description, start_timestamp, flags, exemplar)| {
                    let description = description.map(|s| intern(&s));
                    let exemplars = exemplar.map(|e| vec![e]).unwrap_or_default();
                    MetricRecord {
                        name: intern("proptest_metric"),
                        unit: None,
                        description,
                        start_timestamp,
                        exemplars,
                        flags,
                        kind,
                    }
                })
                // A Summary data point has no `exemplars` field on the wire at all (see the
                // module doc), so a record pairing Summary with a non-empty exemplars list can
                // never be a fixed point by construction -- not a case this generator should
                // produce, the same reasoning `a_summary_drops_its_exemplars_...` pins directly.
                .prop_filter("Summary carries no wire exemplars", |r| {
                    !(matches!(r.kind, MetricKind::Summary(_)) && !r.exemplars.is_empty())
                })
        }

        proptest! {
            /// `decode(encode(x)) == x` for every generated `MetricRecord` -- `start_timestamp`
            /// now writes through verbatim regardless of the event's own timestamp (no more
            /// "0 falls back to the event timestamp" encode rule to sidestep), so the event's
            /// timestamp here is just a fixed, arbitrary value, not load-bearing for this fixed
            /// point.
            #[test]
            fn decode_of_encode_is_the_identity(record in arb_metric_record()) {
                let event = Event::empty(0, AttrMap::new());
                let mut diag = Diagnostics::default();
                let metric = encode_metric(&event, &record, &Telemetry::default(), &mut diag)
                    .expect("Sum/Gauge/Histogram/Summary always encode to Some");
                let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
                prop_assert_eq!(events.len(), 1);
                prop_assert_eq!(&events[0].metrics[0], &record);
            }
        }
    }
}
