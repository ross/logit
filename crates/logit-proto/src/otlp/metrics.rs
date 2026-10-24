//! `MetricRecord` ↔ OTLP `Metric`.
//!
//! **Encode.** `time_unix_nano` is `Event::timestamp`. `start_time_unix_nano` is
//! `record.start_timestamp` verbatim, with no fallback: `0` means unknown in both models
//! (`metrics.proto`), and borrowing the event's timestamp would advertise a zero-width interval a
//! rate-computing backend reads as real. `record.flags` (OTLP `DataPointFlags`, e.g.
//! `NO_RECORDED_VALUE`) goes onto the data point's `flags`. `record.description`, when `Some`,
//! becomes `Metric.description`. Event attributes become the data point's attributes.
//!
//! One `MetricRecord` becomes one `Metric` with one data point; same-named records across events
//! aren't coalesced into one `data_points` list. OTLP permits several `Metric` entries sharing a
//! name, consumers (this decoder included) read them as more points of one series, and it keeps
//! encode a per-record function.
//!
//! The tables below are generated from `otlp_out`'s column of `spec/transit/mapping.yaml`, the
//! canonical copy, by `script/transit docs`, and a test fails when they're stale: change the
//! table there, not here. "Consumer" means `otlp_in` reading the request back.
//!
//! <!-- transit:doc:otlp_out -->
//!
//! Per metric kind, and per record feature that decides a record's fate:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `sum/delta_monotonic` | exact | -- | An OTLP `Sum` with delta temporality and the monotonic flag. |
//! | `sum/delta_nonmonotonic` | exact | -- | An OTLP `Sum` with delta temporality, not monotonic. |
//! | `sum/cumulative_monotonic` | exact | -- | An OTLP `Sum` with cumulative temporality and the monotonic flag. |
//! | `sum/cumulative_nonmonotonic` | exact | -- | An OTLP `Sum` with cumulative temporality, not monotonic. |
//! | `sum/f64` | exact | -- | The data point's `as_double`. |
//! | `sum/i64` | exact | -- | The data point's `as_int`. |
//! | `sum/u64` | normalized | `u64_as_int`; `logit.output.metrics.normalized{reason=u64_as_double}` | OTLP has one signed integer, so a `u64` that fits an `int64` is `as_int` and reads back as an `i64`; one above `i64::MAX` is `as_double`, counted, and reads back as an `f64`. |
//! | `sum/bool` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | A boolean is `as_int` `1` or `0` and reads back as that integer. |
//! | `gauge/f64` | exact | -- | An OTLP `Gauge`, `as_double`. |
//! | `gauge/i64` | exact | -- | An OTLP `Gauge`, `as_int`. |
//! | `gauge/u64` | normalized | `u64_as_int`; `logit.output.metrics.normalized{reason=u64_as_double}` | A `u64` that fits an `int64` is `as_int` and reads back as an `i64`; one above `i64::MAX` is `as_double`, counted, and reads back as an `f64`. |
//! | `gauge/bool` | normalized | `bool_value`; `logit.output.metrics.normalized{reason=bool_value}` | A boolean is `as_int` `1` or `0` and reads back as that integer. |
//! | `gauge/negative` | exact | -- | A negative data point, written as is. |
//! | `gauge/prometheus_info` | normalized | `prometheus_info_sum` | A cumulative non-monotonic OTLP `Sum` named `<name>_info`, as the OTel Prometheus compatibility spec's Info section (marked Development) has it, read back as that sum. |
//! | `gauge/prometheus_stateset` | normalized | `prometheus_stateset_sum` | A cumulative non-monotonic OTLP `Sum`, as the OTel Prometheus compatibility spec's StateSet section (marked Development) has it, read back as that sum. |
//! | `gauge_delta/f64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | An unresolved relative adjustment is dropped, with a throttled `gauge_delta_unresolved` warning; an `aggregate` upstream resolves it into a gauge. |
//! | `gauge_delta/i64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/u64` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `gauge_delta/bool` | dropped | `logit.output.metrics.dropped{reason=unresolved_gauge_delta}` | Dropped, as for a float adjustment. |
//! | `samples` | degraded | `logit.output.metrics.degraded{metric_kind=samples}` | Sketched, each value weighted by its inverse sample rate, then written as the `distribution` row describes. |
//! | `set_members` | skipped | `logit.output.metrics.skipped{metric_kind=set_members}` | OTLP has no cardinality type, so set members aren't written, with a throttled `otlp_set_members_metric_skipped` warning. |
//! | `distribution` | degraded | `logit.output.metrics.degraded{metric_kind=distribution}` | An OTLP `Summary` of the sketch's `0` (its minimum), `0.5`, `0.75`, `0.9`, `0.95`, `0.99`, and `1` (its maximum) quantiles with its own count and sum; OTLP has no mergeable sketch. |
//! | `set` | skipped | `logit.output.metrics.skipped{metric_kind=set}` | OTLP has no cardinality type, so a set estimate isn't written, with a throttled `otlp_set_metric_skipped` warning. |
//! | `histogram/delta` | normalized | `inf_bucket` | An OTLP `Histogram` with delta temporality. OTLP requires an overflow count above the highest bound, so a histogram whose last bound is finite reads back with an empty `+Inf` bucket. |
//! | `histogram/cumulative` | normalized | `inf_bucket` | An OTLP `Histogram` with cumulative temporality. OTLP requires an overflow count above the highest bound, so a histogram whose last bound is finite reads back with an empty `+Inf` bucket. |
//! | `histogram/nan_bound` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | OTLP bounds are finite, so a `NaN` bound's count joins the overflow bucket. `count` and every cumulative count are kept. |
//! | `histogram/neg_inf_bound` | degraded | `logit.output.metrics.degraded{reason=non_finite_component}` | OTLP bounds are finite, so a `-Inf` bound's count joins the first bucket. `count` and every cumulative count are kept. |
//! | `histogram/gauge_histogram` | skipped | `logit.output.metrics.skipped{metric_kind=gauge_histogram}` | The OTel Prometheus compatibility spec drops a GaugeHistogram, since OTLP has no histogram that isn't a running count, so it isn't written, with a throttled `otlp_gauge_histogram_skipped` warning. |
//! | `exponential_histogram/delta` | exact | -- | An OTLP `ExponentialHistogram`, field for field. |
//! | `exponential_histogram/cumulative` | exact | -- | An OTLP `ExponentialHistogram`, field for field. |
//! | `summary/delta` | dropped | `logit.output.metrics.dropped{reason=unresolved_delta}` | OTLP defines every summary as cumulative, so a delta one is dropped, with a throttled `delta_summary_unresolved` warning. |
//! | `summary/cumulative` | exact | -- | An OTLP `Summary`. |
//! | `record/no_recorded_value` | exact | -- | The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value_delta_sum` | exact | -- | The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/no_recorded_value_other_kind` | exact | -- | The data point's `flags` carry `NO_RECORDED_VALUE`, and the point is kept. |
//! | `record/service_check` | exact | -- | The status gauge; the check's carriers aren't attributes. |
//! | `record/sample_rate` | exact | -- | Each value is weighted by its inverse rate in the sketch the `samples` row describes. |
//! | `record/superseded` | exact | -- | Each record is its own data point. |
//! | `record/same_instant` | exact | -- | Each record is its own data point. |
//! | `record/same_instant_equal` | exact | -- | Each record is its own data point. |
//! | `record/same_millisecond` | exact | -- | Each record is its own data point, at its own nanosecond. |
//! | `record/same_millisecond_equal` | exact | -- | Each record is its own data point, at its own nanosecond. |
//! | `record/non_finite` | exact | -- | The non-finite `as_double`. |
//! | `log/dogstatsd_event` | normalized | `observed_timestamp` | An OTLP log record. Its `_logit.statsd.event.*` carriers aren't attributes; otlp_out stamps an observed time on a log that has none, and otlp_in stamps the `_logit.otel.severity_*` carriers. |
//! | `log/plain` | normalized | `observed_timestamp` | An OTLP log record. otlp_out stamps an observed time on a log that has none, and otlp_in stamps the `_logit.otel.severity_*` carriers. |
//! | `span/alone` | exact | -- | An OTLP span. |
//! | `span/no_wire_form` | exact | -- | OTLP carries the status, its message, `trace_state`, flags, and dropped counts. |
//! | `event/attributes_only` | skipped | `logit.output.events.skipped{reason=no_payload}` | An event with no metric, log, or span has no OTLP signal and isn't sent. |
//! | `event/string_only` | skipped | `logit.output.events.skipped{reason=no_payload}` | An InfluxDB point whose fields are all strings has no OTLP signal and isn't sent, so its fields are lost. |
//!
//! Per field of a record that reaches the wire, attribute shape, and payload beside metrics:
//!
//! | Row | Class | Reason or counter | What a consumer sees |
//! |---|---|---|---|
//! | `record/unit` | exact | -- | The `Metric`'s `unit`. |
//! | `record/description` | exact | -- | The `Metric`'s `description`. |
//! | `record/start_timestamp` | exact | -- | The data point's `start_time_unix_nano`. |
//! | `record/exemplars` | exact | -- | The data point's exemplars. |
//! | `record/exemplars_on_summary` | lost | structural | A `SummaryDataPoint` has no exemplars field, so a summary's exemplars are left off. |
//! | `record/min_max` | exact | -- | The data point's `min` and `max`. |
//! | `attributes/string_value` | exact | -- | A string attribute. |
//! | `attributes/host_beside_host_name` | exact | -- | A string attribute. |
//! | `attributes/hostname` | exact | -- | A log attribute. |
//! | `attributes/container_id` | exact | -- | A string attribute. |
//! | `attributes/service_identity` | exact | -- | A string attribute. |
//! | `attributes/service_tag` | exact | -- | A string attribute. |
//! | `attributes/service_tag_beside_key` | exact | -- | A string attribute. |
//! | `attributes/bool_value` | exact | -- | A boolean attribute. |
//! | `attributes/number_value` | normalized | `u64_as_int`; `logit.output.attributes.normalized{reason=u64_as_double}` | An integer or double attribute. A `u64` reads back as an `i64`, or, above `i64::MAX`, as a double, approximate beyond 2^53, counted once per such value. |
//! | `attributes/array_value` | exact | -- | An array attribute. |
//! | `attributes/map_value` | exact | -- | A key-value list attribute. |
//! | `attributes/bytes_value` | exact | -- | A bytes attribute. |
//! | `attributes/timestamp_value` | normalized | `timestamp_as_int` | OTLP has no timestamp value, so it's an integer of Unix nanoseconds. |
//! | `attributes/prometheus_target` | lost | structural | The scrape target is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/foreign_carrier` | lost | structural | A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off. The `_logit.prometheus.type` carrier changes nothing; the `family_type` row has the `_logit.metrics.type` values the sink reads. |
//! | `attributes/timestamped` | lost | structural | The flag is private and isn't an OTLP attribute, so it's left off. The point's time carries the timestamp. |
//! | `attributes/rate` | lost | structural | The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval` | lost | structural | The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/rate_on_non_gauge` | lost | structural | The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/family_type` | lost | structural | The flag is private and isn't an OTLP attribute, so it's left off. The sink reads it, writing an info or stateset gauge as a non-monotonic `Sum` and skipping a gauge histogram, by the `prometheus_info`, `prometheus_stateset`, and `gauge_histogram` rows. |
//! | `attributes/family_type_on_other_kind` | lost | structural | The value names another kind, so it's ignored: the record keeps its own kind's OTLP type, and the flag is private and isn't an OTLP attribute. |
//! | `attributes/interval_on_distribution` | lost | structural | The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/interval_on_histogram` | lost | structural | The flag is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/resource_attribute` | exact | -- | A resource attribute. |
//! | `attributes/scope` | exact | -- | The `InstrumentationScope` of the batch. |
//! | `attributes/datadog_carrier_log` | lost | structural | A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off. |
//! | `attributes/no_host` | exact | -- | A record with no host leaves with none. |
//! | `attributes/log_status` | exact | -- | The severity is the `LogRecord`'s own; no `status` attribute is written. |
//! | `attributes/span_kind` | exact | -- | The kind is the `Span`'s own; no `span.kind` attribute is written. |
//! | `attributes/trace_id_high` | exact | -- | The 128-bit trace id is the `Span`'s own; no `_dd.p.tid` attribute is written. |
//! | `log/beside_metrics` | normalized | `observed_timestamp` | An OTLP log record in the logs request, beside the metrics in theirs, with an observed time stamped when it has none. |
//! | `span/beside_metrics` | exact | -- | An OTLP span in the traces request, beside the metrics in theirs. |
//!
//! <!-- transit:end -->
//!
//! What follows is hand-written: details the spec's cells don't state.
//!
//! A `Sum` or `Gauge` value is a `NumberDataPoint`'s `as_int` or `as_double` by its
//! `MetricValue` type: `F64` → `as_double`, `I64` → `as_int`, a `U64` that fits an `int64` →
//! `as_int`. A `U64` above `i64::MAX` has no integer form and leaves as `as_double`, counted
//! `logit.output.metrics.normalized{reason="u64_as_double"}`; a `Bool` leaves as `as_int` `0` or
//! `1`, counted `logit.output.metrics.normalized{reason="bool_value"}`. An `Exemplar`'s value
//! follows the same rule, counted the same way, once per exemplar.
//!
//! - `Histogram{buckets,temporality,sum,min,max}` → `Histogram`: `buckets` holds per-bucket
//!   counts, not cumulative ones, and the `f64::INFINITY` bound's count becomes the overflow
//!   bucket `explicit_bounds` leaves implicit. OTLP requires that overflow count whenever there's
//!   a bound, so a histogram with no `+Inf` or `NaN` bucket writes it as `0` and reads back with
//!   an empty `+Inf` bucket, the `inf_bucket` normalization. Finite bounds are sorted and equal ones merged,
//!   since OTLP requires them strictly increasing. A `-Inf` bound's count joins the first bucket
//!   and a `NaN` bound's joins the overflow, counted
//!   `logit.output.metrics.degraded{reason="non_finite_component"}` once per record
//!   ([`histogram_buckets`]).
//! - `Summary{quantiles,count,sum}` → `Summary`: a quantile outside `[0, 1]` is written as is;
//!   `docs/known-gaps/mappings.md`'s "Cross-protocol semantic gaps" section has the row. OTLP's
//!   summary is cumulative by definition, so a delta one is dropped (`summary/delta` above).
//! - `Samples(s)` and `Distribution(sketch)` → `Summary` of `SUMMARY_QUANTILES` (see "Lossy
//!   metric kinds" below), whose `0` and `1` are the sketch's tracked minimum and maximum. A `Samples` is sketched first (`Samples::sketch`, each value weighted
//!   by `Samples::weight`). The point's `count` and `sum` are the sketch's own; `sum` is
//!   `DdSketch::sum`, exact unlike a quantile (approximate only when `DdSketch::stats_exact` is
//!   `false`, i.e. the sketch's stats were derived from its bins). Its exemplars are left off,
//!   as a `Summary`'s are (`record/exemplars_on_summary` above): the wire type has no field for
//!   them, a structural loss with no counter.
//! - The neutral flag `_logit.metrics.type`, on the event or else the resource, follows the OTel
//!   Prometheus and OpenMetrics compatibility spec's "Prometheus/OpenMetrics to OTLP": a
//!   `Histogram` flagged `gaugehistogram` isn't written, counted
//!   `logit.output.metrics.skipped{metric_kind="gauge_histogram"}`, and a `Gauge` flagged `info` or
//!   `stateset` is a cumulative non-monotonic `Sum`, an `info` one named `<name>_info`. A value on
//!   another kind is ignored. `prometheus_in` sets the flag for those three OpenMetrics family
//!   types, and Lua or `set` can too; `_logit.prometheus.type` is Prometheus's own carrier and
//!   changes nothing here. The spec marks its Info and StateSet sections Development. Its
//!   `target_info` exception, resource attributes in place of a metric, isn't implemented
//!   (`docs/known-gaps/otlp.md`).
//!
//! **Lossy metric kinds.** OTLP has no mergeable-sketch type. `ExponentialHistogram` is the nearest
//! shape, but its base is `2^(2^-scale)` and a `DdSketch`'s γ isn't (1.015625 under
//! `Mapping::agent`, `logit_core::sketch`), so converting would re-bin every bin into a different
//! sketch rather than carry this one. So `Distribution` degrades to fixed quantiles. This is the
//! qualification ADR `committed-pregenerated-otlp-protobuf` makes against ADR
//! `native-wire-format-with-otlp-bridge`: `logit`'s model (raw samples and members, a mergeable
//! sketch, a mergeable cardinality estimator) can't all be re-expressed *as* OTLP.
//!
//! **Decode.** `Sum` → `MetricKind::Sum{value,temporality,monotonic}` from real fields; a
//! cumulative `Sum` stays a `Sum`. A `NumberDataPoint`'s `as_int` decodes as `MetricValue::I64`
//! and `as_double` as `F64`, so `otlp_in -> otlp_out` keeps an integer point an integer. An
//! `Exemplar`'s `as_int` and `as_double` decode the same way, and one with neither is `F64(0.0)`.
//! `Histogram` → `Histogram{buckets,temporality,sum,min,max}`, rebuilding the trailing infinite
//! bucket when `bucket_counts` has one more entry than `explicit_bounds` (the OTLP-mandated
//! shape), even when its count is `0`, so `otlp_in -> otlp_out` writes a conforming point's lists
//! back as they arrived. `Histogram` has no `count` of its own, so a point with no buckets and a
//! nonzero `count` decodes as one `+Inf` bucket holding it, which `otlp_out` writes back as one
//! bucket count and no bounds, a permitted normalization (ADR `lossless-transit`). A point whose
//! `count` isn't the sum of its buckets keeps the buckets, counted
//! `logit.input.metrics.degraded{reason="histogram_count_mismatch"}`. `Summary` → `Summary{quantiles,count,sum}`, `Cumulative`, each quantile
//! read with no range check.
//! `ExponentialHistogram` → `ExponentialHistogram` field for field (scale, zero count and
//! threshold, positive and negative offset and bucket counts, temporality, count, sum/min/max),
//! with no bucket materialization.
//! `start_time_unix_nano` → `record.start_timestamp` verbatim. A non-empty `Metric.description`
//! interns onto `record.description`. `exemplars` decode onto `record.exemplars` for every kind
//! that carries them (all but `Summary`).
//!
//! **`NO_RECORDED_VALUE` keeps the point, flagged.** A point with `FLAG_NO_RECORDED_VALUE` (bit 0
//! of `flags`) decodes like any other: `record.flags` carries the bit, and the value decodes as
//! sent (usually zero). Encode writes `record.flags` back unchanged, so `otlp_in -> otlp_out` is
//! a fixed point for a flagged point (ADR `metrics-model-v2`).
//!
//! `Metric.metadata` is dropped both ways: the model has nowhere to put metric-level attributes,
//! and `metrics.proto` calls them informational ("Consumers SHOULD NOT need to be aware of these
//! attributes").
//!
//! The one decode-side skip, a `Metric` with no `data` oneof, counts
//! `logit.input.metrics.skipped{metric_kind="unknown", reason="no_data"}`. It's a different name
//! from encode's `logit.output.metrics.*` because encoder and decoder serve different components.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::metrics::v1 as pb;
use logit_core::attrs::{metric_type, TYPE_GAUGE_HISTOGRAM, TYPE_INFO, TYPE_STATESET};
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, DdSketch, Diagnostics, Event, Exemplar, ExpHistogram, Histogram, MetricKind,
    MetricRecord, MetricValue, Resource, Sum, Summary, Telemetry, Temporality, TraceRef,
};

/// The quantiles every sketch-to-quantiles degradation in this crate reports. `crate::prometheus`
/// and `crate::graphite` share them so one metric describes itself the same way at every sink.
pub(crate) const DISTRIBUTION_QUANTILES: [f64; 5] = [0.5, 0.75, 0.90, 0.95, 0.99];

/// [`DISTRIBUTION_QUANTILES`] between `0` and `1`, the quantiles a sketch written as a summary
/// reports. `metrics.proto`'s `ValueAtQuantile` reads `0` as the minimum observed and `1` as the
/// maximum, and `DdSketch::quantile` answers them with the sketch's own `min` and `max`, so a
/// summary keeps the extremes. A dotted expansion writes `.min` and `.max` instead, so
/// `crate::multi_value` reads [`DISTRIBUTION_QUANTILES`].
pub(crate) const SUMMARY_QUANTILES: [f64; 7] = [0.0, 0.5, 0.75, 0.90, 0.95, 0.99, 1.0];

fn temporality_to_pb(t: Temporality) -> i32 {
    match t {
        Temporality::Delta => pb::AggregationTemporality::Delta as i32,
        Temporality::Cumulative => pb::AggregationTemporality::Cumulative as i32,
    }
}

/// Any wire value but `CUMULATIVE` decodes as `Delta`, including `UNSPECIFIED`, which OTLP says
/// "MUST not be used" but isn't worth failing a point over.
fn temporality_from_pb(raw: i32) -> Temporality {
    if raw == pb::AggregationTemporality::Cumulative as i32 {
        Temporality::Cumulative
    } else {
        Temporality::Delta
    }
}

/// `record.start_timestamp` verbatim; `0` means unknown in both models, and there's no fallback
/// to `Event::timestamp` (see the module doc's "Encode").
fn start_time(record_start: i64) -> u64 {
    record_start.max(0) as u64
}

/// Drops `TraceRef.flags`: OTLP's `Exemplar` has no trace-flags field, a permanent lossy mapping
/// (`docs/known-gaps/mappings.md`'s cross-protocol table).
fn encode_exemplar(e: &Exemplar, telemetry: &Telemetry) -> pb::Exemplar {
    let (trace_id, span_id) = match &e.trace {
        Some(t) => (t.trace_id.to_vec(), t.span_id.map(|id| id.to_vec()).unwrap_or_default()),
        None => (Vec::new(), Vec::new()),
    };
    pb::Exemplar {
        filtered_attributes: common::attrs_to_key_values(&e.filtered_attributes),
        time_unix_nano: e.timestamp.max(0) as u64,
        span_id,
        trace_id,
        value: Some(match number_wire(e.value, telemetry) {
            NumberWire::Int(v) => pb::exemplar::Value::AsInt(v),
            NumberWire::Double(v) => pb::exemplar::Value::AsDouble(v),
        }),
    }
}

/// The id bytes become a [`TraceRef`] only under [`TraceRef::from_bytes`]'s validity rule (a
/// non-all-zero 16-byte `trace_id`), leniently, as for a log. `TraceRef.flags` is always `0`:
/// OTLP's `Exemplar` has no field to read it from.
fn decode_exemplar(e: pb::Exemplar) -> Exemplar {
    let value = match e.value {
        Some(pb::exemplar::Value::AsDouble(d)) => MetricValue::F64(d),
        Some(pb::exemplar::Value::AsInt(i)) => MetricValue::I64(i),
        None => MetricValue::F64(0.0),
    };
    let trace = TraceRef::from_bytes(&e.trace_id, &e.span_id, 0);
    let mut filtered_attributes = AttrMap::new();
    common::key_values_into_attrs(e.filtered_attributes, &mut filtered_attributes);
    Exemplar { timestamp: common::wire_nanos(e.time_unix_nano), value, trace, filtered_attributes }
}

fn encode_exemplars(exemplars: &[Exemplar], telemetry: &Telemetry) -> Vec<pb::Exemplar> {
    exemplars.iter().map(|e| encode_exemplar(e, telemetry)).collect()
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
    value: MetricValue,
    telemetry: &Telemetry,
) -> pb::NumberDataPoint {
    pb::NumberDataPoint {
        attributes,
        start_time_unix_nano,
        time_unix_nano: ts,
        exemplars,
        flags,
        value: Some(number_to_pb(value, telemetry)),
    }
}

/// The OTLP form of a scalar, shared by `NumberDataPoint.value` and `Exemplar.value`, which are
/// separate generated `oneof`s with the same two arms.
enum NumberWire {
    Int(i64),
    Double(f64),
}

/// `as_int` for an integer that fits an `int64`, else `as_double`. A `U64` above `i64::MAX` and a
/// `Bool` (as `0` or `1`) are counted.
fn number_wire(value: MetricValue, telemetry: &Telemetry) -> NumberWire {
    match value {
        MetricValue::F64(v) => NumberWire::Double(v),
        MetricValue::I64(v) => NumberWire::Int(v),
        MetricValue::U64(v) => match i64::try_from(v) {
            Ok(v) => NumberWire::Int(v),
            Err(_) => {
                telemetry.count(
                    "logit.output.metrics.normalized",
                    1.0,
                    &[("reason", "u64_as_double")],
                );
                NumberWire::Double(v as f64)
            }
        },
        MetricValue::Bool(b) => {
            telemetry.count("logit.output.metrics.normalized", 1.0, &[("reason", "bool_value")]);
            NumberWire::Int(i64::from(b))
        }
    }
}

/// A scalar's `NumberDataPoint.value`; see [`number_wire`].
fn number_to_pb(value: MetricValue, telemetry: &Telemetry) -> pb::number_data_point::Value {
    use pb::number_data_point::Value as V;
    match number_wire(value, telemetry) {
        NumberWire::Int(v) => V::AsInt(v),
        NumberWire::Double(v) => V::AsDouble(v),
    }
}

/// `as_int` decodes as `I64` and `as_double` as `F64`; a point with neither is `F64(0.0)`.
fn number_value(value: Option<pb::number_data_point::Value>) -> MetricValue {
    match value {
        Some(pb::number_data_point::Value::AsDouble(d)) => MetricValue::F64(d),
        Some(pb::number_data_point::Value::AsInt(i)) => MetricValue::I64(i),
        None => MetricValue::F64(0.0),
    }
}

/// Encodes one `(Event, MetricRecord)` pair into one OTLP `Metric`, or `None` for a skipped
/// `Set`/`SetMembers` or gauge histogram, or a dropped `GaugeDelta` or delta `Summary` (counted;
/// see the module doc). `resource` is the batch's, read only for the `_logit.metrics.type` flag.
pub(crate) fn encode_metric(
    resource: &Resource,
    event: &Event,
    record: &MetricRecord,
    telemetry: &Telemetry,
    diagnostics: &mut Diagnostics,
) -> Option<pb::Metric> {
    let mut name = resolve(record.name).to_string();
    let unit = record.unit.map(resolve).unwrap_or_default().to_string();
    let description = record.description.map(resolve).unwrap_or_default().to_string();
    let attributes = common::attrs_to_key_values(&event.attributes);
    let ts = event.timestamp.max(0) as u64;
    let start = start_time(record.start_timestamp);
    // Only the four kinds below carry exemplars on the wire, so only they count a normalization.
    let exemplars = || encode_exemplars(&record.exemplars, telemetry);

    let reading = metric_type(resource, event);

    let data = match &record.kind {
        // The OTel Prometheus compatibility spec ("Prometheus/OpenMetrics to OTLP") drops a
        // GaugeHistogram, and OTLP has no histogram that isn't a running count.
        MetricKind::Histogram(_) if reading == Some(TYPE_GAUGE_HISTOGRAM) => {
            telemetry.count(
                "logit.output.metrics.skipped",
                1.0,
                &[("metric_kind", "gauge_histogram")],
            );
            diagnostics.warn_throttled(
                "otlp_gauge_histogram_skipped",
                format_args!(
                    "metric '{name}' is flagged a gauge histogram, which the OTel Prometheus \
                     compatibility spec drops -- skipped"
                ),
            );
            return None;
        }
        // The same spec's Info and StateSet sections, marked Development, make each a
        // non-monotonic Sum, so the `1` sums when a label is aggregated away. `prometheus_in`
        // strips an info family's `_info` suffix from the name; the OTLP name keeps it, as the
        // Collector's Prometheus receiver does.
        MetricKind::Gauge(v) if matches!(reading, Some(TYPE_INFO | TYPE_STATESET)) => {
            if reading == Some(TYPE_INFO) {
                name.push_str("_info");
            }
            pb::metric::Data::Sum(pb::Sum {
                data_points: vec![number_data_point(
                    attributes,
                    start,
                    ts,
                    record.flags,
                    exemplars(),
                    *v,
                    telemetry,
                )],
                aggregation_temporality: temporality_to_pb(Temporality::Cumulative),
                is_monotonic: false,
            })
        }
        MetricKind::Sum(s) => pb::metric::Data::Sum(pb::Sum {
            data_points: vec![number_data_point(
                attributes,
                start,
                ts,
                record.flags,
                exemplars(),
                s.value,
                telemetry,
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
                exemplars(),
                *v,
                telemetry,
            )],
        }),
        MetricKind::Histogram(h) => {
            let (bucket_counts, explicit_bounds, non_finite) = histogram_buckets(&h.buckets);
            if non_finite {
                telemetry.count(
                    "logit.output.metrics.degraded",
                    1.0,
                    &[("reason", "non_finite_component")],
                );
            }
            let count = bucket_counts.iter().fold(0u64, |acc, c| acc.saturating_add(*c));
            pb::metric::Data::Histogram(pb::Histogram {
                data_points: vec![pb::HistogramDataPoint {
                    attributes,
                    start_time_unix_nano: start,
                    time_unix_nano: ts,
                    count,
                    sum: h.sum,
                    bucket_counts,
                    explicit_bounds,
                    exemplars: exemplars(),
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
                    exemplars: exemplars(),
                    min: e.min,
                    max: e.max,
                    zero_threshold: e.zero_threshold,
                }],
                aggregation_temporality: temporality_to_pb(e.temporality),
            })
        }
        // OTLP defines every summary as cumulative, so a delta one, which only
        // `cumulative_to_delta` makes, has no form here.
        MetricKind::Summary(s) if s.temporality == Temporality::Delta => {
            telemetry.count("logit.output.metrics.dropped", 1.0, &[("reason", "unresolved_delta")]);
            diagnostics.warn_throttled(
                "delta_summary_unresolved",
                format_args!(
                    "metric '{name}' is a delta summary, which OTLP cannot express -- route the \
                     summary to this output from ahead of the `cumulative_to_delta` component"
                ),
            );
            return None;
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
                // `SummaryDataPoint` has no `exemplars` field.
                flags: record.flags,
            }],
        }),
        MetricKind::Samples(s) => {
            telemetry.count("logit.output.metrics.degraded", 1.0, &[("metric_kind", "samples")]);
            // Sketch, then degrade like `Distribution`. `Samples::weight` is the bounded,
            // NaN-safe extrapolation `statsd_in` applies to its own sketch.
            let sketch = s.sketch();
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
        // A `GaugeDelta` here means the pipeline lacks an `aggregate` (ADR
        // `relative-gauge-adjustments`). The diagnostic key is `influxdb_out`'s, so one grep
        // finds this failure at every sink.
        MetricKind::GaugeDelta(_) => {
            telemetry.count(
                "logit.output.metrics.dropped",
                1.0,
                &[("reason", "unresolved_gauge_delta")],
            );
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

/// The model's `(bound, count)` buckets → OTLP's `bucket_counts` and `explicit_bounds`, plus
/// whether a non-finite bound other than `+Inf` was folded away. OTLP wants finite bounds in
/// strictly increasing order and one more count than bounds, the last being the overflow bucket
/// above the highest bound, so:
///
/// - finite bounds are sorted by [`f64::total_cmp`], and equal ones merge, summing their counts;
/// - a `-Inf` bucket's count joins the first bucket, which already covers everything at or below
///   the lowest bound;
/// - a `NaN` bucket has no position, so its count joins the overflow bucket, as it reaches only
///   `+Inf` in `prometheus_out`'s cumulative counts;
/// - every `+Inf` bucket's count is the overflow.
///
/// OTLP's `HistogramDataPoint` requires `bucket_counts` to hold one more element than
/// `explicit_bounds`, except that both may be empty (`metrics.proto`). So a histogram with a bound
/// but no `+Inf` or `NaN` bucket writes an overflow count of `0`, which `otlp_in` reads back as an
/// empty `+Inf` bucket, and one with no buckets writes neither list. `count` stays the sum of every bucket.
fn histogram_buckets(buckets: &[(f64, u64)]) -> (Vec<u64>, Vec<f64>, bool) {
    let mut finite: Vec<(f64, u64)> =
        buckets.iter().copied().filter(|(bound, _)| bound.is_finite()).collect();
    finite.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut bounds: Vec<f64> = Vec::with_capacity(finite.len());
    let mut counts: Vec<u64> = Vec::with_capacity(finite.len() + 1);
    for (bound, count) in finite {
        match (bounds.last(), counts.last_mut()) {
            // `==` rather than the sort's order, so `-0.0` and `0.0` merge as well.
            (Some(last), Some(merged)) if *last == bound => *merged = merged.saturating_add(count),
            _ => {
                bounds.push(bound);
                counts.push(count);
            }
        }
    }
    let mut below = 0u64;
    let mut overflow = None::<u64>;
    let mut non_finite = false;
    for &(bound, count) in buckets {
        if bound == f64::NEG_INFINITY {
            below = below.saturating_add(count);
            non_finite = true;
        } else if bound.is_nan() {
            overflow = Some(overflow.unwrap_or(0).saturating_add(count));
            non_finite = true;
        } else if bound == f64::INFINITY {
            overflow = Some(overflow.unwrap_or(0).saturating_add(count));
        }
    }
    if overflow.is_some() || !bounds.is_empty() {
        counts.push(overflow.unwrap_or(0));
    }
    if let Some(first) = counts.first_mut() {
        *first = first.saturating_add(below);
    } else if non_finite {
        // Only `-Inf` buckets: with no bound to sit under, their count is the one bucket.
        counts.push(below);
    }
    (counts, bounds, non_finite)
}

fn distribution_summary_point(
    attributes: Vec<crate::otlp::generated::opentelemetry::proto::common::v1::KeyValue>,
    start_time_unix_nano: u64,
    ts: u64,
    flags: u32,
    sketch: &DdSketch,
) -> pb::SummaryDataPoint {
    let quantile_values = SUMMARY_QUANTILES
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
        sum: sketch.sum(),
        quantile_values,
        flags,
    }
}

/// A `HistogramDataPoint`'s `bucket_counts` and `explicit_bounds` → the model's buckets, the
/// count past the last bound under `+Inf`. A point with no buckets but a nonzero `count`, which
/// `metrics.proto` allows when only the count and sum are known, becomes one `+Inf` bucket of
/// that count, since `Histogram` has no `count` of its own. A point whose `count` isn't the sum
/// of its buckets keeps its buckets, counted
/// `logit.input.metrics.degraded{reason="histogram_count_mismatch"}`: they're the distribution, and
/// `metrics.proto` requires the two to agree.
fn histogram_buckets_from_pb(
    dp: &pb::HistogramDataPoint,
    telemetry: &Telemetry,
) -> Vec<(f64, u64)> {
    if dp.bucket_counts.is_empty() {
        return if dp.count == 0 { Vec::new() } else { vec![(f64::INFINITY, dp.count)] };
    }
    let mut buckets = Vec::with_capacity(dp.bucket_counts.len());
    let mut total = 0u64;
    for (i, count) in dp.bucket_counts.iter().enumerate() {
        let bound = dp.explicit_bounds.get(i).copied().unwrap_or(f64::INFINITY);
        buckets.push((bound, *count));
        total = total.saturating_add(*count);
    }
    if total != dp.count {
        telemetry.count(
            "logit.input.metrics.degraded",
            1.0,
            &[("reason", "histogram_count_mismatch")],
        );
    }
    buckets
}

/// Decodes one OTLP `Metric` into one `Event` per data point. Never fails; a `NO_RECORDED_VALUE`
/// point is kept, flagged (see the module doc).
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
                    let ts = common::wire_nanos(dp.time_unix_nano);
                    let value = number_value(dp.value);
                    common::key_values_into_attrs(dp.attributes, &mut attrs);
                    let kind = MetricKind::Sum(Sum { value, temporality, monotonic });
                    let exemplars = decode_exemplars(dp.exemplars);
                    let rec = record(
                        kind,
                        common::wire_nanos(dp.start_time_unix_nano),
                        dp.flags,
                        exemplars,
                    );
                    Event::metric(ts, attrs, rec)
                })
                .collect()
        }
        Some(pb::metric::Data::Gauge(gauge)) => gauge
            .data_points
            .into_iter()
            .map(|dp| {
                let mut attrs = base_attrs.clone();
                let ts = common::wire_nanos(dp.time_unix_nano);
                let value = number_value(dp.value);
                common::key_values_into_attrs(dp.attributes, &mut attrs);
                let exemplars = decode_exemplars(dp.exemplars);
                let rec = record(
                    MetricKind::Gauge(value),
                    common::wire_nanos(dp.start_time_unix_nano),
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
                    let ts = common::wire_nanos(dp.time_unix_nano);
                    let buckets = histogram_buckets_from_pb(&dp, telemetry);
                    let start_timestamp = common::wire_nanos(dp.start_time_unix_nano);
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
                let ts = common::wire_nanos(dp.time_unix_nano);
                let quantiles = dp.quantile_values.iter().map(|q| (q.quantile, q.value)).collect();
                let start_timestamp = common::wire_nanos(dp.start_time_unix_nano);
                let flags = dp.flags;
                common::key_values_into_attrs(dp.attributes, &mut attrs);
                // OTLP defines every summary as cumulative.
                let kind = MetricKind::Summary(Summary {
                    quantiles,
                    count: dp.count,
                    sum: dp.sum,
                    temporality: Temporality::Cumulative,
                });
                // `SummaryDataPoint` has no `exemplars` field.
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
                    let ts = common::wire_nanos(dp.time_unix_nano);
                    let start_timestamp = common::wire_nanos(dp.start_time_unix_nano);
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
        encode_metric(
            &Resource::default(),
            &event(),
            &record(kind),
            &Telemetry::default(),
            &mut diag,
        )
    }

    #[test]
    fn a_delta_monotonic_sum_encodes_with_both_flags() {
        let metric = encode(MetricKind::counter(3.0)).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Sum(sum) => {
                assert!(sum.is_monotonic);
                assert_eq!(sum.aggregation_temporality, pb::AggregationTemporality::Delta as i32);
                assert_eq!(number_value(sum.data_points[0].value), MetricValue::F64(3.0));
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    /// Each `MetricValue` picks its `NumberDataPoint` arm; a `U64` above `i64::MAX` and a `Bool`
    /// are counted under their own reasons.
    #[test]
    fn a_scalar_encodes_as_int_or_as_double_by_its_type() {
        use pb::number_data_point::Value as V;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let cases = [
            (MetricValue::F64(1.5), V::AsDouble(1.5), None),
            (MetricValue::I64(-3), V::AsInt(-3), None),
            (MetricValue::U64(i64::MAX as u64), V::AsInt(i64::MAX), None),
            (MetricValue::U64(u64::MAX), V::AsDouble(u64::MAX as f64), Some("u64_as_double")),
            (MetricValue::Bool(true), V::AsInt(1), Some("bool_value")),
        ];
        for (value, expected, reason) in cases {
            let mut diag = Diagnostics::default();
            let metric = encode_metric(
                &Resource::default(),
                &event(),
                &record(MetricKind::Gauge(value)),
                &telemetry,
                &mut diag,
            )
            .unwrap();
            let Some(pb::metric::Data::Gauge(gauge)) = metric.data else { panic!("not a Gauge") };
            assert_eq!(gauge.data_points[0].value, Some(expected), "{value:?}");
            let counted: Vec<String> = registry
                .drain(0)
                .iter()
                .filter_map(|e| e.attributes.get("reason")?.as_str().map(str::to_string))
                .collect();
            assert_eq!(counted, reason.map(str::to_string).into_iter().collect::<Vec<_>>());
        }
    }

    /// A cumulative, non-monotonic sum round-trips both flags.
    #[test]
    fn a_cumulative_non_monotonic_sum_round_trips_both_flags() {
        let kind = MetricKind::Sum(Sum {
            value: MetricValue::F64(7.0),
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
                assert_eq!(s.value, MetricValue::F64(7.0));
                assert_eq!(s.temporality, Temporality::Cumulative);
                assert!(!s.monotonic);
            }
            other => panic!("expected Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_gauge_encodes_to_a_gauge() {
        let metric = encode(MetricKind::gauge(4.5)).unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Gauge(g) => {
                assert_eq!(number_value(g.data_points[0].value), MetricValue::F64(4.5))
            }
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

    /// OTLP requires one more count than bounds, so a histogram whose last bound is finite, as
    /// `aggregate` emits, writes a `0` overflow count, which decodes as an empty `+Inf` bucket.
    #[test]
    fn a_histogram_with_a_finite_last_bound_writes_a_zero_overflow_count() {
        let h = Histogram {
            buckets: vec![(1.0, 2), (5.0, 3), (10.0, 1)],
            temporality: Temporality::Delta,
            sum: Some(12.5),
            min: Some(0.5),
            max: Some(9.9),
        };
        let metric = encode(MetricKind::Histogram(h.clone())).unwrap();
        let Some(pb::metric::Data::Histogram(hist)) = metric.data.clone() else {
            panic!("not a Histogram")
        };
        let dp = &hist.data_points[0];
        assert_eq!(dp.explicit_bounds, vec![1.0, 5.0, 10.0]);
        assert_eq!(dp.bucket_counts, vec![2, 3, 1, 0]);
        assert_eq!(dp.count, 6);
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        let MetricKind::Histogram(got) = &events[0].metrics[0].kind else {
            panic!("not a Histogram")
        };
        assert_eq!(got.buckets, vec![(1.0, 2), (5.0, 3), (10.0, 1), (f64::INFINITY, 0)]);
    }

    /// A histogram with no buckets writes neither list, which OTLP allows.
    #[test]
    fn a_histogram_with_no_buckets_writes_no_counts_or_bounds() {
        let h = Histogram {
            buckets: Vec::new(),
            temporality: Temporality::Delta,
            sum: Some(1.0),
            min: None,
            max: None,
        };
        let metric = encode(MetricKind::Histogram(h)).unwrap();
        let Some(pb::metric::Data::Histogram(hist)) = metric.data else {
            panic!("not a Histogram")
        };
        let dp = &hist.data_points[0];
        assert!(dp.bucket_counts.is_empty());
        assert!(dp.explicit_bounds.is_empty());
    }

    /// Out-of-order bounds with a `NaN` and a `-Inf` among them: the finite bounds sort, the `-Inf`
    /// count joins the first bucket, the `NaN` count joins the overflow, every count lines up with
    /// its bound, and the record counts degraded once.
    #[test]
    fn a_histogram_with_out_of_order_non_finite_bounds_writes_aligned_buckets() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let kind = MetricKind::Histogram(Histogram {
            buckets: vec![
                (5.0, 2),
                (f64::NAN, 4),
                (1.0, 1),
                (f64::NEG_INFINITY, 8),
                (f64::INFINITY, 16),
            ],
            temporality: Temporality::Cumulative,
            sum: None,
            min: None,
            max: None,
        });
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&Resource::default(), &event(), &record(kind), &telemetry, &mut diag)
                .unwrap();
        let Some(pb::metric::Data::Histogram(hist)) = metric.data else {
            panic!("not a Histogram")
        };
        let dp = &hist.data_points[0];
        assert_eq!(dp.explicit_bounds, vec![1.0, 5.0]);
        assert_eq!(dp.bucket_counts, vec![9, 2, 20]);
        assert_eq!(dp.count, 31);
        let degraded: Vec<_> = registry
            .drain(0)
            .into_iter()
            .filter(|e| {
                e.attributes.get("reason").and_then(|v| v.as_str()) == Some("non_finite_component")
            })
            .collect();
        assert_eq!(degraded.len(), 1, "one degraded count per record");
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
    fn a_summary_quantile_outside_the_unit_interval_passes_through_unchanged() {
        // Documented gap (`docs/known-gaps/mappings.md`): neither direction range-checks a quantile.
        let s = Summary {
            quantiles: vec![(1.5, 10.0), (-0.25, 1.0)],
            count: 2,
            sum: 11.0,
            temporality: Temporality::Cumulative,
        };
        let metric = encode(MetricKind::Summary(s.clone())).unwrap();
        match &metric.data {
            Some(pb::metric::Data::Summary(summary)) => {
                let got: Vec<f64> =
                    summary.data_points[0].quantile_values.iter().map(|q| q.quantile).collect();
                assert_eq!(got, vec![1.5, -0.25]);
            }
            other => panic!("expected Summary, got {other:?}"),
        }
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Summary(got) => assert_eq!(*got, s),
            other => panic!("expected Summary, got {other:?}"),
        }
    }

    #[test]
    fn a_summary_encodes_and_decodes_with_count_and_sum() {
        let s = Summary {
            quantiles: vec![(0.5, 10.0), (0.99, 99.0)],
            count: 42,
            sum: 543.2,
            temporality: Temporality::Cumulative,
        };
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

    /// Every `ExponentialHistogramDataPoint` field survives decode and re-encode unchanged.
    #[test]
    fn an_exponential_histogram_encodes_to_identical_wire_fields_both_times() {
        let e = exp_histogram(Temporality::Cumulative);
        let metric = encode(MetricKind::ExponentialHistogram(e.clone())).unwrap();
        let events = decode_metric(metric.clone(), &AttrMap::new(), &Telemetry::default());
        let mut diag = Diagnostics::default();
        let re_metric = encode_metric(
            &Resource::default(),
            &events[0],
            &events[0].metrics[0],
            &Telemetry::default(),
            &mut diag,
        )
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
    fn a_samples_metric_encodes_as_a_summary_with_its_extremes_and_is_counted_degraded() {
        let samples = {
            let mut s = Samples::new([1.0, 2.0, 3.0, 4.0, 5.0]);
            s.sample_rate = 1.0;
            s
        };
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let metric = encode_metric(
            &Resource::default(),
            &event(),
            &record(MetricKind::Samples(samples)),
            &telemetry,
            &mut diag,
        )
        .unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Summary(s) => {
                let quantiles: Vec<(f64, f64)> = s.data_points[0]
                    .quantile_values
                    .iter()
                    .map(|q| (q.quantile, q.value))
                    .collect();
                assert_eq!(quantiles.len(), 7, "min, p50/p75/p90/p95/p99, max");
                assert_eq!(quantiles.first(), Some(&(0.0, 1.0)), "quantile 0 is the minimum");
                assert_eq!(quantiles.last(), Some(&(1.0, 5.0)), "quantile 1 is the maximum");
                assert_eq!(s.data_points[0].count, 5);
                assert_eq!(s.data_points[0].sum, 15.0);
            }
            other => panic!("expected Summary, got {other:?}"),
        }
        let events = registry.drain(0);
        let degraded = events
            .iter()
            .find(|e| e.attributes.get("metric_kind").and_then(|v| v.as_str()) == Some("samples"));
        assert!(degraded.is_some(), "should count logit.output.metrics.degraded{{metric_kind}}");
    }

    /// A NaN `sample_rate` weighs `1`, so the summary still counts every observation; a bare
    /// `clamp`-then-`as u64` would give weight `0` and a `count: 0` point.
    #[test]
    fn a_samples_metric_with_a_nan_sample_rate_keeps_every_observation() {
        let mut samples = Samples::new([120.0, 130.0]);
        samples.sample_rate = f64::NAN;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let metric = encode_metric(
            &Resource::default(),
            &event(),
            &record(MetricKind::Samples(samples)),
            &telemetry,
            &mut diag,
        )
        .unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Summary(s) => {
                assert_eq!(s.data_points[0].count, 2);
                assert_eq!(s.data_points[0].quantile_values.len(), 7);
            }
            other => panic!("expected Summary, got {other:?}"),
        }
    }

    #[test]
    fn a_distribution_encodes_as_a_summary_with_its_extremes_and_is_counted_degraded() {
        let mut sketch = DdSketch::new();
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sketch.add(v);
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::default();
        let metric = encode_metric(
            &Resource::default(),
            &event(),
            &record(MetricKind::Distribution(sketch)),
            &telemetry,
            &mut diag,
        )
        .unwrap();
        match metric.data.unwrap() {
            pb::metric::Data::Summary(s) => {
                let quantiles: Vec<(f64, f64)> = s.data_points[0]
                    .quantile_values
                    .iter()
                    .map(|q| (q.quantile, q.value))
                    .collect();
                assert_eq!(quantiles.len(), 7, "min, p50/p75/p90/p95/p99, max");
                assert_eq!(quantiles.first(), Some(&(0.0, 1.0)), "quantile 0 is the minimum");
                assert_eq!(quantiles.last(), Some(&(1.0, 5.0)), "quantile 1 is the maximum");
                assert_eq!(s.data_points[0].count, 5);
                assert_eq!(s.data_points[0].sum, 15.0);
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
            &Resource::default(),
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
            &Resource::default(),
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

    /// A `GaugeDelta` is dropped, not encoded as an absolute value, and reported under
    /// `gauge_delta_unresolved`.
    #[test]
    fn a_gauge_delta_is_dropped_and_reports_its_own_diagnostic_key() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let diag_registry = Registry::new();
        let mut diag = Diagnostics::new("otlp_out")
            .with_telemetry(diag_registry.telemetry_for("otlp_out", "otlp_out", "diag"));
        let result = encode_metric(
            &Resource::default(),
            &event(),
            &record(MetricKind::GaugeDelta(MetricValue::F64(5.0))),
            &telemetry,
            &mut diag,
        );
        assert!(result.is_none(), "a GaugeDelta metric must not produce a Metric at all");
        let events = registry.drain(0);
        let points = |name: &str, (key, value): (&str, &str)| {
            events
                .iter()
                .filter(|e| e.attributes.get(key).and_then(|v| v.as_str()) == Some(value))
                .flat_map(|e| e.metrics.iter())
                .filter(|m| logit_core::interner::resolve(m.name) == name)
                .count()
        };
        assert_eq!(points("logit.output.metrics.dropped", ("reason", "unresolved_gauge_delta")), 1);
        assert_eq!(points("logit.output.metrics.skipped", ("metric_kind", "gauge_delta")), 0);
        let diag_events = diag_registry.drain(0);
        let reported = diag_events.iter().find(|e| {
            e.attributes.get("key").and_then(|v| v.as_str()) == Some("gauge_delta_unresolved")
        });
        assert!(
            reported.is_some(),
            "should report under the gauge_delta_unresolved diagnostic key"
        );
    }

    /// OTLP's summary is cumulative by definition, so a delta one is dropped, counted, and
    /// reported, never written as a cumulative one.
    #[test]
    fn a_delta_summary_is_dropped_and_counted() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::new("otlp_out");
        let kind = MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0)],
            count: 2,
            sum: 3.0,
            temporality: Temporality::Delta,
        });
        assert!(encode_metric(
            &Resource::default(),
            &event(),
            &record(kind),
            &telemetry,
            &mut diag
        )
        .is_none());
        assert_eq!(diag.occurrences("delta_summary_unresolved"), 1);
        let dropped = registry.drain(0).iter().any(|e| {
            e.attributes.get("reason").and_then(|v| v.as_str()) == Some("unresolved_delta")
                && e.metrics.iter().any(|m| {
                    logit_core::interner::resolve(m.name) == "logit.output.metrics.dropped"
                })
        });
        assert!(dropped, "should count logit.output.metrics.dropped{{reason=unresolved_delta}}");
    }

    /// Whether `registry` holds a count of `name` tagged `tag`.
    fn counted_in(registry: &Registry, name: &str, tag: (&str, &str)) -> bool {
        registry.drain(0).iter().any(|e| {
            e.attributes.get(tag.0).and_then(|v| v.as_str()) == Some(tag.1)
                && e.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == name)
        })
    }

    /// An event carrying `value` under `key`, the neutral type flag or `prometheus_in`'s carrier.
    fn typed_event(key: &str, value: &str) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(key, value);
        Event::empty(1000, attrs)
    }

    /// A batch resource carrying the neutral type flag `value`.
    fn typed_resource(value: &str) -> Resource {
        let mut resource = Resource::default();
        resource.attributes.insert(logit_core::attrs::METRICS_TYPE, value);
        resource
    }

    const NEUTRAL: &str = logit_core::attrs::METRICS_TYPE;
    const CARRIER: &str = crate::prometheus::ATTR_TYPE;

    /// The OTel Prometheus compatibility spec drops a GaugeHistogram: a `Histogram` flagged
    /// `gaugehistogram`, on the event or else the resource.
    #[test]
    fn a_histogram_flagged_gaugehistogram_is_skipped_and_counted() {
        let rec = record(MetricKind::Histogram(histogram(Temporality::Cumulative)));
        for (resource, event) in [
            (Resource::default(), typed_event(NEUTRAL, "gaugehistogram")),
            (typed_resource("gaugehistogram"), event()),
        ] {
            let registry = Registry::new();
            let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
            let mut diag = Diagnostics::new("otlp_out");
            assert!(encode_metric(&resource, &event, &rec, &telemetry, &mut diag).is_none());
            assert_eq!(diag.occurrences("otlp_gauge_histogram_skipped"), 1);
            assert!(counted_in(
                &registry,
                "logit.output.metrics.skipped",
                ("metric_kind", "gauge_histogram")
            ));
        }
    }

    /// The `gaugehistogram` value names a `Histogram`; on a `Gauge` it's ignored.
    #[test]
    fn a_gaugehistogram_flag_on_a_gauge_is_ignored() {
        let mut diag = Diagnostics::default();
        let rec = record(MetricKind::gauge(2.0));
        let event = typed_event(NEUTRAL, "gaugehistogram");
        let metric =
            encode_metric(&Resource::default(), &event, &rec, &Telemetry::default(), &mut diag)
                .unwrap();
        assert_eq!(metric.name, "m");
        assert!(matches!(metric.data, Some(pb::metric::Data::Gauge(_))));
    }

    /// An info or stateset `Gauge` is a cumulative non-monotonic `Sum`, and an info metric gets
    /// back the `_info` suffix `prometheus_in` strips, from the flag on the event or the resource.
    #[test]
    fn a_gauge_flagged_info_or_stateset_encodes_as_a_non_monotonic_sum() {
        for (value, name) in [("info", "m_info"), ("stateset", "m")] {
            for (resource, event) in [
                (Resource::default(), typed_event(NEUTRAL, value)),
                (typed_resource(value), event()),
            ] {
                let mut diag = Diagnostics::default();
                let rec = record(MetricKind::gauge(1.0));
                let metric =
                    encode_metric(&resource, &event, &rec, &Telemetry::default(), &mut diag)
                        .unwrap();
                assert_eq!(metric.name, name, "{value}");
                match metric.data {
                    Some(pb::metric::Data::Sum(sum)) => {
                        assert!(!sum.is_monotonic, "{value}");
                        assert_eq!(
                            sum.aggregation_temporality,
                            pb::AggregationTemporality::Cumulative as i32
                        );
                        assert_eq!(number_value(sum.data_points[0].value), MetricValue::F64(1.0));
                    }
                    other => panic!("{value}: expected Sum, got {other:?}"),
                }
            }
        }
    }

    /// `info` and `stateset` name a `Gauge`; on a cumulative monotonic `Sum` they're ignored.
    #[test]
    fn an_info_or_stateset_flag_on_a_sum_is_ignored() {
        for value in ["info", "stateset"] {
            let mut diag = Diagnostics::default();
            let rec = record(MetricKind::counter(2.0));
            let event = typed_event(NEUTRAL, value);
            let metric =
                encode_metric(&Resource::default(), &event, &rec, &Telemetry::default(), &mut diag)
                    .unwrap();
            assert_eq!(metric.name, "m", "{value}");
            match metric.data {
                Some(pb::metric::Data::Sum(sum)) => assert!(sum.is_monotonic, "{value}"),
                other => panic!("{value}: expected Sum, got {other:?}"),
            }
        }
    }

    /// `prometheus_in`'s carrier is Prometheus's own: a record carrying only it, under any
    /// value, encodes as its kind does, with no rename, no `Sum`, and no skip.
    #[test]
    fn the_prometheus_type_carrier_alone_changes_nothing() {
        for value in ["info", "stateset", "unknown", "untyped"] {
            let mut diag = Diagnostics::default();
            let rec = record(MetricKind::gauge(3.0));
            let metric = encode_metric(
                &Resource::default(),
                &typed_event(CARRIER, value),
                &rec,
                &Telemetry::default(),
                &mut diag,
            )
            .unwrap();
            assert_eq!(metric.name, "m", "{value}");
            assert!(matches!(metric.data, Some(pb::metric::Data::Gauge(_))), "{value}");
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let mut diag = Diagnostics::new("otlp_out");
        let rec = record(MetricKind::Histogram(histogram(Temporality::Cumulative)));
        let event = typed_event(CARRIER, "gaugehistogram");
        let metric = encode_metric(&Resource::default(), &event, &rec, &telemetry, &mut diag);
        assert!(matches!(metric.and_then(|m| m.data), Some(pb::metric::Data::Histogram(_))));
        assert_eq!(diag.occurrences("otlp_gauge_histogram_skipped"), 0);
    }

    fn histogram_metric(dp: pb::HistogramDataPoint) -> pb::Metric {
        pb::Metric {
            name: "h".to_string(),
            data: Some(pb::metric::Data::Histogram(pb::Histogram {
                data_points: vec![dp],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        }
    }

    fn decoded_buckets(events: &[Event]) -> Vec<(f64, u64)> {
        match &events[0].metrics[0].kind {
            MetricKind::Histogram(h) => h.buckets.clone(),
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    /// A point with only `count` and `sum` keeps its population as one `+Inf` bucket, which
    /// `otlp_out` writes back as one bucket count and no bounds.
    #[test]
    fn a_bucketless_histogram_point_decodes_as_one_inf_bucket_of_its_count() {
        let dp = pb::HistogramDataPoint { count: 12, sum: Some(30.0), ..Default::default() };
        let events = decode_metric(histogram_metric(dp), &AttrMap::new(), &Telemetry::default());
        assert_eq!(decoded_buckets(&events), vec![(f64::INFINITY, 12)]);
        let mut diag = Diagnostics::default();
        let metric = encode_metric(
            &Resource::default(),
            &events[0],
            &events[0].metrics[0],
            &Telemetry::default(),
            &mut diag,
        )
        .unwrap();
        match metric.data {
            Some(pb::metric::Data::Histogram(h)) => {
                let dp = &h.data_points[0];
                assert_eq!((dp.count, dp.bucket_counts.clone()), (12, vec![12]));
                assert!(dp.explicit_bounds.is_empty());
            }
            other => panic!("expected Histogram, got {other:?}"),
        }
    }

    /// An empty bucketless point has no population to place, so it stays bucketless.
    #[test]
    fn an_empty_bucketless_histogram_point_decodes_with_no_buckets() {
        let dp = pb::HistogramDataPoint::default();
        let events = decode_metric(histogram_metric(dp), &AttrMap::new(), &Telemetry::default());
        assert_eq!(decoded_buckets(&events), vec![]);
    }

    /// The buckets win over a `count` that disagrees with them, and the mismatch is counted.
    #[test]
    fn a_count_that_disagrees_with_the_buckets_keeps_the_buckets_and_is_counted() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "input");
        let dp = pb::HistogramDataPoint {
            count: 10,
            bucket_counts: vec![3, 4],
            explicit_bounds: vec![1.0],
            ..Default::default()
        };
        let events = decode_metric(histogram_metric(dp), &AttrMap::new(), &telemetry);
        assert_eq!(decoded_buckets(&events), vec![(1.0, 3), (f64::INFINITY, 4)]);
        assert!(counted_in(
            &registry,
            "logit.input.metrics.degraded",
            ("reason", "histogram_count_mismatch")
        ));
    }

    /// A conforming point counts nothing.
    #[test]
    fn a_count_that_agrees_with_the_buckets_counts_nothing() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_in", "otlp_in", "input");
        let dp = pb::HistogramDataPoint {
            count: 7,
            bucket_counts: vec![3, 4],
            explicit_bounds: vec![1.0],
            ..Default::default()
        };
        decode_metric(histogram_metric(dp), &AttrMap::new(), &telemetry);
        assert!(!counted_in(
            &registry,
            "logit.input.metrics.degraded",
            ("reason", "histogram_count_mismatch")
        ));
    }

    #[test]
    fn a_delta_monotonic_sum_decodes_as_a_sum_with_both_flags() {
        let metric = encode(MetricKind::counter(3.0)).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => {
                assert_eq!(s.value, MetricValue::F64(3.0));
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
                data_points: vec![number_data_point(
                    Vec::new(),
                    1000,
                    1000,
                    0,
                    Vec::new(),
                    MetricValue::F64(7.0),
                    &Telemetry::default(),
                )],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
        };
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1);
        match &events[0].metrics[0].kind {
            MetricKind::Sum(s) => {
                assert_eq!(s.value, MetricValue::F64(7.0));
                assert_eq!(s.temporality, Temporality::Cumulative);
                assert!(s.monotonic);
            }
            other => panic!("a cumulative sum must decode as Sum, got {other:?}"),
        }
    }

    #[test]
    fn a_gauge_decodes_as_a_gauge() {
        let metric = encode(MetricKind::gauge(1.5)).unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        match &events[0].metrics[0].kind {
            MetricKind::Gauge(v) => assert_eq!(*v, MetricValue::F64(1.5)),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A `Metric` with no `data` oneof decodes to no events and counts
    /// `logit.input.metrics.skipped{metric_kind="unknown", reason="no_data"}`.
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

    /// A `NO_RECORDED_VALUE` point is kept, flagged, not skipped.
    #[test]
    fn a_no_recorded_value_flag_keeps_the_point_flagged_rather_than_skipping_it() {
        let mut metric = encode(MetricKind::gauge(1.0)).unwrap();
        if let Some(pb::metric::Data::Gauge(g)) = &mut metric.data {
            g.data_points[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        }
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events.len(), 1, "a NO_RECORDED_VALUE point must be kept, not skipped");
        assert_eq!(events[0].metrics[0].flags, MetricRecord::FLAG_NO_RECORDED_VALUE);
    }

    /// Encode writes `record.flags` back unchanged for every kind that has a data point.
    #[test]
    fn a_flagged_record_round_trips_its_flags_for_every_kind() {
        let flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        let kinds = [
            MetricKind::counter(1.0),
            MetricKind::gauge(2.0),
            MetricKind::Histogram(histogram(Temporality::Delta)),
            MetricKind::ExponentialHistogram(exp_histogram(Temporality::Delta)),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 1.0)],
                count: 1,
                sum: 1.0,
                temporality: Temporality::Cumulative,
            }),
        ];
        for kind in kinds {
            let rec = MetricRecord { flags, ..MetricRecord::new(intern("m"), kind.clone()) };
            let mut diag = Diagnostics::default();
            let metric = encode_metric(
                &Resource::default(),
                &event(),
                &rec,
                &Telemetry::default(),
                &mut diag,
            )
            .unwrap();
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
        let mut rec = record(MetricKind::gauge(1.0));
        rec.start_timestamp = 500;
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&Resource::default(), &event(), &rec, &Telemetry::default(), &mut diag)
                .unwrap();
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
        let rec = record(MetricKind::gauge(1.0)); // start_timestamp: 0 (unknown)
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&Resource::default(), &event(), &rec, &Telemetry::default(), &mut diag)
                .unwrap();
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
        let mut rec = record(MetricKind::gauge(1.0));
        rec.description = Some(intern("a metric description"));
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&Resource::default(), &event(), &rec, &Telemetry::default(), &mut diag)
                .unwrap();
        assert_eq!(metric.description, "a metric description");
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events[0].metrics[0].description.map(resolve), Some("a metric description"));

        let no_description = record(MetricKind::gauge(1.0));
        let mut diag = Diagnostics::default();
        let metric = encode_metric(
            &Resource::default(),
            &event(),
            &no_description,
            &Telemetry::default(),
            &mut diag,
        )
        .unwrap();
        assert_eq!(metric.description, "");
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert_eq!(events[0].metrics[0].description, None);
    }

    fn exemplar() -> Exemplar {
        let mut filtered_attributes = AttrMap::new();
        filtered_attributes.insert("dropped", "attr");
        Exemplar {
            timestamp: 42,
            value: MetricValue::F64(3.5),
            trace: Some(TraceRef { trace_id: [7; 16], span_id: Some([8; 8]), flags: 0 }),
            filtered_attributes,
        }
    }

    /// Exemplars round trip for every kind that carries them on the wire.
    #[test]
    fn exemplars_round_trip_for_sum_gauge_histogram_and_exponential_histogram() {
        let kinds = [
            MetricKind::counter(1.0),
            MetricKind::gauge(2.0),
            MetricKind::Histogram(histogram(Temporality::Delta)),
            MetricKind::ExponentialHistogram(exp_histogram(Temporality::Delta)),
        ];
        for kind in kinds {
            let rec = MetricRecord {
                exemplars: vec![exemplar()],
                ..MetricRecord::new(intern("m"), kind.clone())
            };
            let mut diag = Diagnostics::default();
            let metric = encode_metric(
                &Resource::default(),
                &event(),
                &rec,
                &Telemetry::default(),
                &mut diag,
            )
            .unwrap();
            let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
            assert_eq!(
                events[0].metrics[0].exemplars,
                vec![exemplar()],
                "exemplars should round-trip for {kind:?}"
            );
        }
    }

    fn gauge_with_exemplar_value(value: MetricValue) -> MetricRecord {
        MetricRecord {
            exemplars: vec![Exemplar { value, ..exemplar() }],
            ..MetricRecord::new(intern("m"), MetricKind::gauge(1.0))
        }
    }

    /// An `as_int` exemplar decodes as `I64` and encodes back as `as_int`, including past 2^53
    /// where an `f64` can't hold the integer.
    #[test]
    fn an_int_exemplar_keeps_its_integer_through_a_round_trip() {
        for v in [i64::MAX, (1i64 << 53) + 1, -7, 0] {
            let rec = gauge_with_exemplar_value(MetricValue::I64(v));
            let mut diag = Diagnostics::default();
            let metric = encode_metric(
                &Resource::default(),
                &event(),
                &rec,
                &Telemetry::default(),
                &mut diag,
            )
            .unwrap();
            let Some(pb::metric::Data::Gauge(gauge)) = &metric.data else { panic!("not a Gauge") };
            assert_eq!(
                gauge.data_points[0].exemplars[0].value,
                Some(pb::exemplar::Value::AsInt(v))
            );
            let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
            assert_eq!(events[0].metrics[0].exemplars[0].value, MetricValue::I64(v));
        }
    }

    /// An exemplar value takes `number_to_pb`'s arm choice and its counters.
    #[test]
    fn an_exemplar_value_encodes_as_int_or_as_double_by_its_type() {
        use pb::exemplar::Value as V;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
        let cases = [
            (MetricValue::F64(1.5), V::AsDouble(1.5), None),
            (MetricValue::I64(-3), V::AsInt(-3), None),
            (MetricValue::U64(i64::MAX as u64), V::AsInt(i64::MAX), None),
            (MetricValue::U64(u64::MAX), V::AsDouble(u64::MAX as f64), Some("u64_as_double")),
            (MetricValue::Bool(true), V::AsInt(1), Some("bool_value")),
            (MetricValue::Bool(false), V::AsInt(0), Some("bool_value")),
        ];
        for (value, expected, reason) in cases {
            let rec = gauge_with_exemplar_value(value);
            let mut diag = Diagnostics::default();
            let metric =
                encode_metric(&Resource::default(), &event(), &rec, &telemetry, &mut diag).unwrap();
            let Some(pb::metric::Data::Gauge(gauge)) = metric.data else { panic!("not a Gauge") };
            assert_eq!(gauge.data_points[0].exemplars[0].value, Some(expected), "{value:?}");
            let counted: Vec<String> = registry
                .drain(0)
                .iter()
                .filter_map(|e| e.attributes.get("reason")?.as_str().map(str::to_string))
                .collect();
            assert_eq!(counted, reason.map(str::to_string).into_iter().collect::<Vec<_>>());
        }
    }

    /// `SummaryDataPoint` has no `exemplars` field, so a summary's exemplars are dropped.
    #[test]
    fn a_summary_drops_its_exemplars_because_the_wire_type_has_nowhere_to_put_them() {
        let s = Summary {
            quantiles: vec![(0.5, 1.0)],
            count: 1,
            sum: 1.0,
            temporality: Temporality::Cumulative,
        };
        let rec = MetricRecord {
            exemplars: vec![exemplar()],
            ..MetricRecord::new(intern("m"), MetricKind::Summary(s))
        };
        let mut diag = Diagnostics::default();
        let metric =
            encode_metric(&Resource::default(), &event(), &rec, &Telemetry::default(), &mut diag)
                .unwrap();
        let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
        assert!(
            events[0].metrics[0].exemplars.is_empty(),
            "a Summary must not carry exemplars back -- the wire type has no field for them"
        );
    }

    /// An exemplar on a kind whose wire type has no exemplars field is dropped without counting
    /// a value normalization it never performed.
    #[test]
    fn an_exemplar_on_a_kind_without_exemplars_counts_no_normalization() {
        let kinds = [
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 1.0)],
                count: 1,
                sum: 1.0,
                temporality: Temporality::Cumulative,
            }),
            MetricKind::GaugeDelta(MetricValue::F64(1.0)),
        ];
        for kind in kinds {
            for value in [MetricValue::Bool(true), MetricValue::U64(u64::MAX)] {
                let registry = Registry::new();
                let telemetry = registry.telemetry_for("otlp_out", "otlp_out", "sink");
                let rec = MetricRecord {
                    exemplars: vec![Exemplar { value, ..exemplar() }],
                    ..MetricRecord::new(intern("m"), kind.clone())
                };
                let mut diag = Diagnostics::default();
                let _ = encode_metric(&Resource::default(), &event(), &rec, &telemetry, &mut diag);
                let normalized = registry
                    .drain(0)
                    .iter()
                    .filter_map(|e| e.attributes.get("reason")?.as_str().map(str::to_string))
                    .filter(|r| r == "bool_value" || r == "u64_as_double")
                    .count();
                assert_eq!(normalized, 0, "{kind:?} {value:?}");
            }
        }
    }

    // -- proptest: decode(encode(x)) == x over a generator of MetricRecords -----------------

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        fn arb_temporality() -> impl Strategy<Value = Temporality> {
            prop_oneof![Just(Temporality::Delta), Just(Temporality::Cumulative)]
        }

        /// `F64` and `I64` only: a `U64` decodes back as `I64` and a `Bool` as `I64` `0`/`1`.
        fn arb_value() -> impl Strategy<Value = MetricValue> {
            prop_oneof![
                (-1e6f64..1e6f64).prop_map(MetricValue::F64),
                any::<i64>().prop_map(MetricValue::I64),
            ]
        }

        fn arb_sum() -> impl Strategy<Value = Sum> {
            (arb_value(), arb_temporality(), any::<bool>())
                .prop_map(|(value, temporality, monotonic)| Sum { value, temporality, monotonic })
        }

        fn arb_histogram() -> impl Strategy<Value = Histogram> {
            (
                prop::collection::vec((-1e6f64..1e6f64, 0u64..1000u64), 0..5),
                prop::option::of(0u64..1000u64),
                arb_temporality(),
                prop::option::of(-1e6f64..1e6f64),
                prop::option::of(-1e6f64..1e6f64),
                prop::option::of(-1e6f64..1e6f64),
            )
                .prop_map(|(mut buckets, overflow, temporality, sum, min, max)| {
                    // Strictly increasing, as OTLP requires: the encoder sorts and merges the rest.
                    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
                    buckets.dedup_by(|a, b| a.0 == b.0);
                    // The shape a decode produces: any bound means an overflow count, read back
                    // as a trailing `+Inf` bucket.
                    if overflow.is_some() || !buckets.is_empty() {
                        buckets.push((f64::INFINITY, overflow.unwrap_or(0)));
                    }
                    Histogram { buckets, temporality, sum, min, max }
                })
        }

        fn arb_summary() -> impl Strategy<Value = Summary> {
            (
                prop::collection::vec((0.0f64..1.0f64, -1e6f64..1e6f64), 0..5),
                0u64..1000u64,
                -1e6f64..1e6f64,
            )
                .prop_map(|(quantiles, count, sum)| Summary {
                    quantiles,
                    count,
                    sum,
                    temporality: Temporality::Cumulative,
                })
        }

        /// `Sum`/`Gauge`/`Histogram`/`Summary` only: `ExponentialHistogram` has its own tests,
        /// and the other kinds degrade or skip on encode, so no fixed point is promised.
        fn arb_kind() -> impl Strategy<Value = MetricKind> {
            prop_oneof![
                arb_sum().prop_map(MetricKind::Sum),
                arb_value().prop_map(MetricKind::Gauge),
                arb_histogram().prop_map(MetricKind::Histogram),
                arb_summary().prop_map(MetricKind::Summary),
            ]
        }

        /// A fixed, non-zero id (`TraceRef::from_bytes` rejects all-zero); only presence varies.
        fn arb_exemplar() -> impl Strategy<Value = Exemplar> {
            (0i64..2_000_000_000_000_000_000i64, arb_value(), any::<bool>()).prop_map(
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

        /// Non-empty on `Some`: an empty wire description decodes as `None`, so `Some("")` can't
        /// round-trip.
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
                // A Summary drops its exemplars, so it can't round-trip with any.
                .prop_filter("Summary carries no wire exemplars", |r| {
                    !(matches!(r.kind, MetricKind::Summary(_)) && !r.exemplars.is_empty())
                })
        }

        proptest! {
            /// `decode(encode(x)) == x` for every generated `MetricRecord`; the event timestamp
            /// is arbitrary because `start_timestamp` never falls back to it.
            #[test]
            fn decode_of_encode_is_the_identity(record in arb_metric_record()) {
                let event = Event::empty(0, AttrMap::new());
                let mut diag = Diagnostics::default();
                let metric = encode_metric(&Resource::default(), &event, &record, &Telemetry::default(), &mut diag)
                    .expect("Sum/Gauge/Histogram/Summary always encode to Some");
                let events = decode_metric(metric, &AttrMap::new(), &Telemetry::default());
                prop_assert_eq!(events.len(), 1);
                prop_assert_eq!(&events[0].metrics[0], &record);
            }
        }
    }
}
