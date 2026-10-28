//! The OTLP codec: `OtlpEncoder`/`OtlpDecoder` over the committed protobuf types in `generated/`
//! (`crates/logit-proto/proto/README.md` has their provenance; ADR
//! `committed-pregenerated-otlp-protobuf` says why they're committed).
//!
//! **This module doc is the mapping table** for what all three signals share. Each sibling module
//! doc has its own: [`common`] (`Value` ↔ `AnyValue`, attributes, resource and scope), [`logs`]
//! (`Severity`, `BodyFormat`, trace context), [`traces`], and [`metrics`] (temporality and the
//! kinds OTLP can't carry natively). [`json`] is the OTLP/JSON layer in both directions, and
//! [`grpc`] is gRPC message framing and bounded gzip inflation.
//!
//! **Wire types.** This codec encodes and decodes `TracesData`/`LogsData`/`MetricsData`, not
//! `Export*ServiceRequest`. The two are wire-identical (one `repeated Resource* = 1` field), so
//! these bytes are valid request bodies with no collector service types generated. OTLP/JSON
//! matches too: both shapes share the top-level key (`resourceSpans`, ...), and [`json`] keys off
//! field names.
//!
//! **Nesting.** One `EventBatch` encodes as one `Resource*` entry (with the resource's
//! `dropped_attributes_count` and `schema_url`) holding one `Scope*` entry. A `None` scope encodes
//! an empty `InstrumentationScope`, never a fabricated `logit` identity. Decoding is finer: every
//! `(Resource*, Scope*)` pair becomes its own `EventBatch`, **never collapsed**, including several
//! scopes under one resource. A scope group that decodes to no events produces no batch. An
//! all-empty wire scope decodes to `scope: None` (see [`common::pb_to_scope`] for why the fixed
//! point needs that).
//!
//! **An empty batch encodes to no payloads**, never an empty request
//! ([`SignalEncoder::encode_signals`]). An event with no metric, log, or span is in no payload,
//! counted `logit.output.events.skipped{reason="no_payload"}`.
//!
//! **The listener adds sender attributes after decode.** Under `otlp_in`'s `peer:`,
//! `proxy_protocol:`, or `forwarded:`, `network.peer.*` or `client.*` is added to every event, an
//! opt-in addition the sender never sent, and one of ADR `lossless-transit`'s "Permitted
//! normalizations".
//!
//! **`otlp_out`'s mapping tables.** The tables below are generated from `otlp_out`'s column of
//! `spec/transit/mapping.yaml`, the canonical copy, by `script/transit docs`, and a test fails when
//! they're stale: change the table there, not here. "Consumer" means `otlp_in` reading the
//! request back.
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
//! | `log/dogstatsd_event` | exact | -- | An OTLP log record. Its `_logit.statsd.event.*` carriers aren't attributes; the `log` field rows have its fields. |
//! | `log/plain` | exact | -- | An OTLP log record; the `log` field rows have its fields. |
//! | `log/structured_message` | exact | -- | A `kvlistValue` or `arrayValue` body. |
//! | `log/bytes_message` | exact | -- | A `bytesValue` body. |
//! | `log/blank_message` | exact | -- | An empty string body, or a log record with no body. |
//! | `span/alone` | exact | -- | An OTLP span. |
//! | `span/beside_log` | exact | -- | An OTLP span. |
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
//! | `attributes/log_service` | exact | -- | A log attribute. |
//! | `attributes/log_service_field` | exact | -- | A log attribute. |
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
//! | `attributes/null_value` | exact | -- | An `AnyValue` with no value set, read back as null. |
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
//! | `attributes/severity_carrier_otel` | exact | -- | The raw `severity_number` and `severity_text` the carriers hold, which otlp_in reads back into the same carriers beside the band's severity. |
//! | `attributes/severity_carrier_syslog` | lost | structural | A `_logit.` carrier is private and isn't an OTLP attribute, so it's left off and the severity reads back at band resolution. |
//! | `attributes/syslog_header` | normalized | `u64_as_int` | A string or integer attribute; `intValue` is signed, so a `u64` `syslog.facility` or `process.pid` reads back as an `i64`. |
//! | `attributes/syslog_msgid` | exact | -- | A string attribute. |
//! | `attributes/syslog_sd` | exact | -- | A key-value list attribute. |
//! | `attributes/syslog_timestamp` | normalized | `timestamp_as_int` | An RFC 3164 timestamp is a string attribute; OTLP has no timestamp value, so an RFC 5424 one is an integer of Unix nanoseconds. |
//! | `attributes/host_name` | exact | -- | A string attribute. |
//! | `attributes/splunk_envelope` | exact | -- | A resource attribute. |
//! | `attributes/syslog_facility` | exact | -- | A log with no `syslog.facility` leaves with none. |
//! | `attributes/no_host` | exact | -- | A record with no host leaves with none. |
//! | `attributes/log_status` | exact | -- | The severity is the `LogRecord`'s own; no `status` attribute is written. |
//! | `attributes/log_severity_fields` | exact | -- | The severity is the `LogRecord`'s own; no `otel.log.severity.*` attribute is written. |
//! | `attributes/span_kind` | exact | -- | The kind is the `Span`'s own; no `span.kind` attribute is written. |
//! | `attributes/trace_id_high` | exact | -- | The 128-bit trace id is the `Span`'s own; no `_dd.p.tid` attribute is written. |
//! | `log/severity` | exact | -- | The band's `severity_number` and text, or the raw ones the `_logit.otel.severity_*` carriers hold, read back at band resolution. |
//! | `log/severity_unset` | exact | -- | `SEVERITY_NUMBER_UNSPECIFIED` and no text, read back as no severity. |
//! | `log/body_format` | exact | -- | OTLP has no body format, so it leaves as the `logit.body_format` attribute, which otlp_in reads back into the format. |
//! | `log/event_name` | exact | -- | The `LogRecord`'s `event_name`. |
//! | `log/observed_timestamp` | exact | -- | The `LogRecord`'s `observed_time_unix_nano`. |
//! | `log/observed_timestamp_unset` | normalized | `observed_timestamp` | otlp_out stamps a log that has no observed time with the time it encodes the request, as `logs.proto` defines the field. |
//! | `log/trace` | exact | -- | The `LogRecord`'s `trace_id` and `span_id`. |
//! | `log/trace_unset` | normalized | `trace_from_span` | A log with no trace on an event that carries a span leaves with the span's ids and flags `0`; without a span, the log leaves with no trace. |
//! | `log/trace_flags` | exact | -- | The low 8 bits of the `LogRecord`'s `flags`. |
//! | `log/trace_beside_id_attribute` | exact | -- | The `LogRecord`'s trace, span ids, and flags, beside the attributes, as `log/trace` has. |
//! | `log/dropped_attributes_count` | exact | -- | The `LogRecord`'s `dropped_attributes_count`. |
//! | `log/timestamp` | exact | -- | The `LogRecord`'s `time_unix_nano`. |
//! | `log/receipt_timestamp` | exact | -- | The `LogRecord`'s `time_unix_nano`, the time the log was received. |
//! | `log/beside_metrics` | exact | -- | An OTLP log record in the logs request, beside the metrics in theirs. |
//! | `span/beside_metrics` | exact | -- | An OTLP span in the traces request, beside the metrics in theirs. |
//!
//! <!-- transit:end -->

pub mod common;
pub mod grpc;
pub mod json;
pub mod logs;
pub mod metrics;
mod request;
pub mod traces;

pub use request::OtlpRequest;

/// The committed `prost` types. Public so a test outside the crate can build a wire message the
/// encoder never emits, such as a timestamp past `i64::MAX`.
pub mod generated;

use crate::{CodecError, Signal, SignalDecoder, SignalEncoder, SignalPayload};
use bytes::Bytes;
use generated::opentelemetry::proto::logs::v1 as logs_pb;
use generated::opentelemetry::proto::metrics::v1 as metrics_pb;
use generated::opentelemetry::proto::trace::v1 as trace_pb;
use logit_core::{AttrMap, Diagnostics, Event, EventBatch, Telemetry};
use prost::Message;
use std::sync::Arc;

/// The body encoding [`OtlpEncoder`] writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OtlpEncoding {
    /// Protobuf, OTLP's default.
    #[default]
    Protobuf,
    /// OTLP/JSON as the OTel Collector's pdata marshaler writes it ([`json::write_logs`] and its
    /// siblings).
    Json,
}

/// Encodes an [`EventBatch`] into OTLP, one payload per non-empty [`Signal`].
///
/// Holds [`Telemetry`]/[`Diagnostics`] handles so the lossy metric paths ([`metrics`]'s module
/// doc) can count themselves; the default handles are no-ops.
#[derive(Default)]
pub struct OtlpEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
    encoding: OtlpEncoding,
}

impl OtlpEncoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects the payload body encoding; protobuf unless set.
    pub fn with_encoding(mut self, encoding: OtlpEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

impl SignalEncoder for OtlpEncoder {
    fn encode_signals(&mut self, batch: &EventBatch) -> Result<Vec<SignalPayload>, CodecError> {
        let resource = common::resource_to_pb(&batch.resource);
        let resource_schema_url =
            batch.resource.schema_url.as_ref().map(common::bytes_to_string).unwrap_or_default();
        let scope = common::scope_to_pb(batch.scope.as_deref());
        let scope_schema_url = batch
            .scope
            .as_ref()
            .and_then(|s| s.schema_url.as_ref())
            .map(common::bytes_to_string)
            .unwrap_or_default();

        let mut log_records = Vec::new();
        let mut spans = Vec::new();
        let mut metric_points = Vec::new();
        let mut u64_as_double = 0;
        let mut no_payload = 0;

        for event in &batch.events {
            if event.log.is_none() && event.span.is_none() && event.metrics.is_empty() {
                no_payload += 1;
                continue;
            }
            let mut written = false;
            if let Some(log) = &event.log {
                log_records.push(logs::encode_log_record(event, log));
                written = true;
            }
            if let Some(span) = &event.span {
                spans.push(traces::encode_span(event, span));
                written = true;
            }
            for metric in &event.metrics {
                if let Some(m) = metrics::encode_metric(
                    &batch.resource,
                    event,
                    metric,
                    &self.telemetry,
                    &mut self.diagnostics,
                ) {
                    metric_points.push(m);
                    written = true;
                }
            }
            // Only an event something was written for carries its attributes onto the wire.
            if written {
                u64_as_double += common::u64_as_double_count(&event.attributes);
            }
        }

        if no_payload > 0 {
            self.telemetry.count(
                "logit.output.events.skipped",
                no_payload as f64,
                &[("reason", "no_payload")],
            );
        }
        // The resource and scope go out once per payload but count once, as one batch's values.
        if !(log_records.is_empty() && spans.is_empty() && metric_points.is_empty()) {
            u64_as_double += common::u64_as_double_count(&batch.resource.attributes)
                + batch.scope.as_ref().map_or(0, |s| common::u64_as_double_count(&s.attributes));
        }
        if u64_as_double > 0 {
            self.telemetry.count(
                "logit.output.attributes.normalized",
                u64_as_double as f64,
                &[("reason", "u64_as_double")],
            );
        }

        let mut payloads = Vec::with_capacity(3);
        if !log_records.is_empty() {
            let records = log_records.len();
            let data = logs_pb::LogsData {
                resource_logs: vec![logs_pb::ResourceLogs {
                    resource: Some(resource.clone()),
                    scope_logs: vec![logs_pb::ScopeLogs {
                        scope: Some(scope.clone()),
                        log_records,
                        schema_url: scope_schema_url.clone(),
                    }],
                    schema_url: resource_schema_url.clone(),
                }],
            };
            payloads.push(SignalPayload {
                signal: Signal::Logs,
                bytes: self.body(&data, json::write_logs),
                records,
            });
        }
        if !spans.is_empty() {
            let records = spans.len();
            let data = trace_pb::TracesData {
                resource_spans: vec![trace_pb::ResourceSpans {
                    resource: Some(resource.clone()),
                    scope_spans: vec![trace_pb::ScopeSpans {
                        scope: Some(scope.clone()),
                        spans,
                        schema_url: scope_schema_url.clone(),
                    }],
                    schema_url: resource_schema_url.clone(),
                }],
            };
            payloads.push(SignalPayload {
                signal: Signal::Traces,
                bytes: self.body(&data, json::write_traces),
                records,
            });
        }
        if !metric_points.is_empty() {
            let records = metric_points.len();
            let data = metrics_pb::MetricsData {
                resource_metrics: vec![metrics_pb::ResourceMetrics {
                    resource: Some(resource),
                    scope_metrics: vec![metrics_pb::ScopeMetrics {
                        scope: Some(scope),
                        metrics: metric_points,
                        schema_url: scope_schema_url,
                    }],
                    schema_url: resource_schema_url,
                }],
            };
            payloads.push(SignalPayload {
                signal: Signal::Metrics,
                bytes: self.body(&data, json::write_metrics),
                records,
            });
        }
        Ok(payloads)
    }
}

impl OtlpEncoder {
    fn body<M: Message>(&self, data: &M, write_json: fn(&M, &mut Vec<u8>)) -> Bytes {
        match self.encoding {
            OtlpEncoding::Protobuf => Bytes::from(data.encode_to_vec()),
            OtlpEncoding::Json => {
                let mut out = Vec::new();
                write_json(data, &mut out);
                Bytes::from(out)
            }
        }
    }
}

/// The mirror of [`OtlpEncoder`]. See the module doc for why decode can return several batches.
#[derive(Default)]
pub struct OtlpDecoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

impl OtlpDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }
}

/// One `EventBatch` per `(ResourceX, ScopeX)` pair (the module doc's "Nesting"). A record's
/// decoder gets an empty base `AttrMap`: resource and scope attributes live on the batch, never
/// copied onto its events.
fn decode_resource_logs(rl: logs_pb::ResourceLogs) -> Vec<EventBatch> {
    let resource = Arc::new(common::pb_to_resource(rl.resource, &rl.schema_url));
    rl.scope_logs
        .into_iter()
        .filter_map(|scope_logs| {
            let events: Vec<Event> = scope_logs
                .log_records
                .into_iter()
                .map(|record| logs::decode_log_record(record, AttrMap::new()))
                .collect();
            // A scope group with no records is not a batch.
            if events.is_empty() {
                return None;
            }
            let scope = common::pb_to_scope(scope_logs.scope, &scope_logs.schema_url).map(Arc::new);
            Some(EventBatch { resource: resource.clone(), scope, events })
        })
        .collect()
}

fn decode_resource_spans(rs: trace_pb::ResourceSpans) -> Result<Vec<EventBatch>, CodecError> {
    let resource = Arc::new(common::pb_to_resource(rs.resource, &rs.schema_url));
    let mut batches = Vec::new();
    for scope_spans in rs.scope_spans {
        let events: Vec<Event> = scope_spans
            .spans
            .into_iter()
            .map(|span| traces::decode_span(span, AttrMap::new()))
            .collect::<Result<_, _>>()?;
        // A scope group with no records is not a batch.
        if events.is_empty() {
            continue;
        }
        let scope = common::pb_to_scope(scope_spans.scope, &scope_spans.schema_url).map(Arc::new);
        batches.push(EventBatch { resource: resource.clone(), scope, events });
    }
    Ok(batches)
}

impl OtlpDecoder {
    fn decode_resource_metrics(&self, rm: metrics_pb::ResourceMetrics) -> Vec<EventBatch> {
        let resource = Arc::new(common::pb_to_resource(rm.resource, &rm.schema_url));
        rm.scope_metrics
            .into_iter()
            .filter_map(|scope_metrics| {
                let events: Vec<Event> = scope_metrics
                    .metrics
                    .into_iter()
                    .flat_map(|metric| {
                        metrics::decode_metric(metric, &AttrMap::new(), &self.telemetry)
                    })
                    .collect();
                // No events (no metrics, or every data variant unset) is not a batch.
                if events.is_empty() {
                    return None;
                }
                let scope = common::pb_to_scope(scope_metrics.scope, &scope_metrics.schema_url)
                    .map(Arc::new);
                Some(EventBatch { resource: resource.clone(), scope, events })
            })
            .collect()
    }
}

impl SignalDecoder for OtlpDecoder {
    fn decode_signal(
        &mut self,
        signal: Signal,
        bytes: Bytes,
    ) -> Result<Vec<EventBatch>, CodecError> {
        match signal {
            Signal::Logs => {
                let data = logs_pb::LogsData::decode(bytes)
                    .map_err(|e| CodecError::Malformed(e.to_string()))?;
                Ok(data.resource_logs.into_iter().flat_map(decode_resource_logs).collect())
            }
            Signal::Traces => {
                let data = trace_pb::TracesData::decode(bytes)
                    .map_err(|e| CodecError::Malformed(e.to_string()))?;
                let mut batches = Vec::new();
                for rs in data.resource_spans {
                    batches.extend(decode_resource_spans(rs)?);
                }
                Ok(batches)
            }
            Signal::Metrics => {
                let data = metrics_pb::MetricsData::decode(bytes)
                    .map_err(|e| CodecError::Malformed(e.to_string()))?;
                Ok(data
                    .resource_metrics
                    .into_iter()
                    .flat_map(|rm| self.decode_resource_metrics(rm))
                    .collect())
            }
        }
    }
}

impl OtlpDecoder {
    /// [`SignalDecoder::decode_signal`] for OTLP/JSON (an inherent method; see
    /// [`crate::SignalDecoder`]). [`json`] parses `bytes` into the same `prost` structs the
    /// protobuf path uses (ADR `otlp-json-decoding`), and the same `decode_resource_*` functions
    /// take it from there, so every mapping rule holds for both encodings.
    pub fn decode_signal_json(
        &mut self,
        signal: Signal,
        bytes: Bytes,
    ) -> Result<Vec<EventBatch>, CodecError> {
        match signal {
            Signal::Logs => {
                let data = json::logs_data(&bytes)?;
                Ok(data.resource_logs.into_iter().flat_map(decode_resource_logs).collect())
            }
            Signal::Traces => {
                let data = json::traces_data(&bytes)?;
                let mut batches = Vec::new();
                for rs in data.resource_spans {
                    batches.extend(decode_resource_spans(rs)?);
                }
                Ok(batches)
            }
            Signal::Metrics => {
                let data = json::metrics_data(&bytes)?;
                Ok(data
                    .resource_metrics
                    .into_iter()
                    .flat_map(|rm| self.decode_resource_metrics(rm))
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, Event, LogRecord, MetricKind, MetricRecord, Resource, Value};

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn all_three_signals() -> Vec<Event> {
        let log = Event::log(
            1,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 5,
                dropped_attributes_count: 0,
            },
        );
        let metric = Event::metric(
            2,
            AttrMap::new(),
            MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)),
        );
        let span = Event::span(
            3,
            AttrMap::new(),
            logit_core::SpanRecord {
                trace_id: [1; 16],
                span_id: [2; 8],
                parent_span_id: None,
                name: Value::str("s"),
                kind: logit_core::SpanKind::Internal,
                status: logit_core::SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 4,
                flags: 0,
                ext: None,
            },
        );
        vec![log, metric, span]
    }

    #[test]
    fn a_batch_carrying_all_three_signals_encodes_as_three_separate_payloads() {
        let mut encoder = OtlpEncoder::new();
        let payloads = encoder.encode_signals(&batch(all_three_signals())).unwrap();
        let signals: Vec<Signal> = payloads.iter().map(|p| p.signal).collect();
        assert_eq!(signals.len(), 3, "got signals: {signals:?}");
        assert!(signals.contains(&Signal::Logs));
        assert!(signals.contains(&Signal::Metrics));
        assert!(signals.contains(&Signal::Traces));
    }

    #[test]
    fn the_json_encoding_writes_the_protobuf_requests_as_otlp_json() {
        let batch = batch(all_three_signals());
        let proto = OtlpEncoder::new().encode_signals(&batch).unwrap();
        let json =
            OtlpEncoder::new().with_encoding(OtlpEncoding::Json).encode_signals(&batch).unwrap();
        assert_eq!(proto.len(), 3);
        assert_eq!(json.len(), 3);
        for (p, j) in proto.iter().zip(&json) {
            assert_eq!((p.signal, p.records), (j.signal, j.records));
            let mut want = Vec::new();
            match p.signal {
                Signal::Logs => json::write_logs(
                    &logs_pb::LogsData::decode(p.bytes.clone()).unwrap(),
                    &mut want,
                ),
                Signal::Traces => json::write_traces(
                    &trace_pb::TracesData::decode(p.bytes.clone()).unwrap(),
                    &mut want,
                ),
                Signal::Metrics => json::write_metrics(
                    &metrics_pb::MetricsData::decode(p.bytes.clone()).unwrap(),
                    &mut want,
                ),
            }
            assert_eq!(j.bytes, want, "{:?}", p.signal);
            let from_proto = OtlpDecoder::new().decode_signal(p.signal, p.bytes.clone()).unwrap();
            let from_json =
                OtlpDecoder::new().decode_signal_json(j.signal, j.bytes.clone()).unwrap();
            assert_eq!(from_json, from_proto, "{:?}", p.signal);
        }
    }

    #[test]
    fn an_empty_batch_encodes_no_payloads_at_all() {
        let mut encoder = OtlpEncoder::new();
        let payloads = encoder.encode_signals(&batch(vec![])).unwrap();
        assert!(payloads.is_empty(), "got: {payloads:?}");
    }

    #[test]
    fn payload_record_counts_are_the_encoded_records_after_skips() {
        let log = || {
            Event::log(
                1,
                AttrMap::new(),
                LogRecord {
                    message: Value::str("hi"),
                    severity: None,
                    body_format: logit_core::BodyFormat::Raw,
                    trace: None,
                    event_name: None,
                    observed_timestamp: 0,
                    dropped_attributes_count: 0,
                },
            )
        };
        let metric = |kind: MetricKind| {
            Event::metric(
                2,
                AttrMap::new(),
                MetricRecord::new(logit_core::interner::intern("m"), kind),
            )
        };
        let span = Event::span(
            3,
            AttrMap::new(),
            logit_core::SpanRecord {
                trace_id: [1; 16],
                span_id: [2; 8],
                parent_span_id: None,
                name: Value::str("s"),
                kind: logit_core::SpanKind::Internal,
                status: logit_core::SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 4,
                flags: 0,
                ext: None,
            },
        );
        let events = vec![
            log(),
            metric(MetricKind::counter(1.0)),
            span,
            metric(MetricKind::SetMembers(vec![Bytes::from_static(b"a")])),
            log(),
            metric(MetricKind::counter(2.0)),
        ];

        let mut encoder = OtlpEncoder::new();
        let payloads = encoder.encode_signals(&batch(events)).unwrap();
        let summary: Vec<(Signal, usize)> =
            payloads.iter().map(|p| (p.signal, p.records)).collect();
        assert_eq!(summary, vec![(Signal::Logs, 2), (Signal::Traces, 1), (Signal::Metrics, 2)]);
    }

    #[test]
    fn a_request_with_two_resource_spans_decodes_to_two_batches_not_one() {
        let mut resource_a = Resource::default();
        resource_a.attributes.insert("host", "a");
        let mut resource_b = Resource::default();
        resource_b.attributes.insert("host", "b");

        let span_a = Event::span(
            1,
            AttrMap::new(),
            logit_core::SpanRecord {
                trace_id: [1; 16],
                span_id: [1; 8],
                parent_span_id: None,
                name: Value::str("a"),
                kind: logit_core::SpanKind::Internal,
                status: logit_core::SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 2,
                flags: 0,
                ext: None,
            },
        );
        let span_b = Event::span(
            1,
            AttrMap::new(),
            logit_core::SpanRecord {
                trace_id: [2; 16],
                span_id: [2; 8],
                parent_span_id: None,
                name: Value::str("b"),
                kind: logit_core::SpanKind::Internal,
                status: logit_core::SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 2,
                flags: 0,
                ext: None,
            },
        );

        let mut encoder = OtlpEncoder::new();
        let bytes_a = encoder
            .encode_signals(&EventBatch {
                resource: Arc::new(resource_a),
                scope: None,
                events: vec![span_a],
            })
            .unwrap();
        let bytes_b = encoder
            .encode_signals(&EventBatch {
                resource: Arc::new(resource_b),
                scope: None,
                events: vec![span_b],
            })
            .unwrap();

        // One TracesData carrying both ResourceSpans, as a batching intermediary sends.
        let data_a = trace_pb::TracesData::decode(bytes_a[0].bytes.clone()).unwrap();
        let data_b = trace_pb::TracesData::decode(bytes_b[0].bytes.clone()).unwrap();
        let mut combined = data_a;
        combined.resource_spans.extend(data_b.resource_spans);
        let combined_bytes = Bytes::from(combined.encode_to_vec());

        let mut decoder = OtlpDecoder::new();
        let batches = decoder.decode_signal(Signal::Traces, combined_bytes).unwrap();
        assert_eq!(batches.len(), 2, "two ResourceSpans entries must decode to two batches");
        assert_eq!(batches[0].resource.attributes.get("host").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(batches[1].resource.attributes.get("host").and_then(|v| v.as_str()), Some("b"));
    }

    /// Provenance: not from a live collector (`docs/design/memory.md`'s "Fixtures" section).
    /// `prost::Message::encode_to_vec()`'s output for a `TracesData` built with this crate's
    /// generated types (`tools/protogen`): one `ResourceSpans`/`ScopeSpans`/`Span` with a name,
    /// kind, times, one attribute, one event, one link, and an OK status, pasted once as a
    /// literal. Decoding it back checks the vendored `.proto`s' tag numbers and wire types.
    #[rustfmt::skip]
    const OTLP_TRACE_REQUEST: &[u8] = &[
        0x0a, 0xb2, 0x01, 0x12, 0xaf, 0x01, 0x0a, 0x0e, 0x0a, 0x05, 0x6c, 0x6f, 0x67, 0x69, 0x74,
        0x12, 0x05, 0x30, 0x2e, 0x31, 0x2e, 0x30, 0x12, 0x9c, 0x01, 0x0a, 0x10, 0x01, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x12, 0x08,
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x2a, 0x0c, 0x66, 0x69, 0x78, 0x74, 0x75,
        0x72, 0x65, 0x5f, 0x73, 0x70, 0x61, 0x6e, 0x30, 0x01, 0x39, 0x01, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x41, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4a, 0x09, 0x0a,
        0x03, 0x66, 0x6b, 0x31, 0x12, 0x02, 0x10, 0x01, 0x5a, 0x21, 0x09, 0x01, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x12, 0x0a, 0x63, 0x68, 0x65, 0x63, 0x6b, 0x70, 0x6f, 0x69, 0x6e,
        0x74, 0x1a, 0x0a, 0x0a, 0x02, 0x65, 0x6b, 0x12, 0x04, 0x0a, 0x02, 0x65, 0x76, 0x6a, 0x2a,
        0x0a, 0x10, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
        0x03, 0x03, 0x03, 0x12, 0x08, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x22, 0x0c,
        0x0a, 0x02, 0x6c, 0x6b, 0x12, 0x06, 0x0a, 0x04, 0x6c, 0x69, 0x6e, 0x6b, 0x7a, 0x02, 0x18,
        0x01,
    ];

    #[test]
    fn the_golden_trace_request_decodes_and_matches_the_span_it_was_built_from() {
        let data = trace_pb::TracesData::decode(OTLP_TRACE_REQUEST).expect("must decode cleanly");
        assert_eq!(data.resource_spans.len(), 1);
        let rs = &data.resource_spans[0];
        assert_eq!(rs.scope_spans.len(), 1);
        let scope = rs.scope_spans[0].scope.as_ref().unwrap();
        assert_eq!(scope.name, "logit");
        assert_eq!(scope.version, "0.1.0");
        assert_eq!(rs.scope_spans[0].spans.len(), 1);
        let span = &rs.scope_spans[0].spans[0];
        assert_eq!(span.trace_id, vec![1u8; 16]);
        assert_eq!(span.span_id, vec![2u8; 8]);
        assert_eq!(span.name, "fixture_span");
        assert_eq!(span.start_time_unix_nano, 1);
        assert_eq!(span.end_time_unix_nano, 2);
        assert_eq!(span.attributes.len(), 1);
        assert_eq!(span.attributes[0].key, "fk1");
        assert_eq!(span.events.len(), 1);
        assert_eq!(span.events[0].name, "checkpoint");
        assert_eq!(span.links.len(), 1);
        assert_eq!(span.links[0].trace_id, vec![3u8; 16]);
        assert_eq!(span.status.as_ref().unwrap().code, trace_pb::status::StatusCode::Ok as i32);

        let batches =
            decode_resource_spans(rs.clone()).expect("decode_resource_spans should succeed");
        assert_eq!(batches.len(), 1, "one Resource*/Scope* pair must decode to one batch");
        let batch = &batches[0];
        assert_eq!(batch.events.len(), 1);
        let decoded_span = batch.events[0].span.as_ref().unwrap();
        assert_eq!(decoded_span.trace_id, [1u8; 16]);
        assert_eq!(decoded_span.span_id, [2u8; 8]);
        let scope = batch.scope.as_ref().expect("scope must survive onto the batch");
        assert_eq!(scope.name, bytes::Bytes::from_static(b"logit"));
        assert_eq!(scope.version, bytes::Bytes::from_static(b"0.1.0"));
    }

    /// The span in [`OTLP_TRACE_REQUEST`], hand-written as OTLP/JSON; it must decode to an equal
    /// [`EventBatch`]. `parentSpanId` is an explicit `null` where the protobuf omits the field, so
    /// `null` must decode like an absent key.
    const OTLP_TRACE_REQUEST_JSON: &[u8] = br#"{
        "resourceSpans": [{
            "scopeSpans": [{
                "scope": {"name": "logit", "version": "0.1.0"},
                "spans": [{
                    "traceId": "01010101010101010101010101010101",
                    "spanId": "0202020202020202",
                    "parentSpanId": null,
                    "name": "fixture_span",
                    "startTimeUnixNano": "1",
                    "endTimeUnixNano": "2",
                    "attributes": [{"key": "fk1", "value": {"boolValue": true}}],
                    "events": [{"timeUnixNano": "1", "name": "checkpoint",
                                "attributes": [{"key": "ek", "value": {"stringValue": "ev"}}]}],
                    "links": [{"traceId": "03030303030303030303030303030303",
                               "spanId": "0404040404040404",
                               "attributes": [{"key": "lk", "value": {"stringValue": "link"}}]}],
                    "status": {"code": "STATUS_CODE_OK"}
                }]
            }]
        }]
    }"#;

    #[test]
    fn protobuf_and_json_encodings_of_the_same_span_decode_to_the_same_event_batch() {
        let mut proto_decoder = OtlpDecoder::new();
        let proto_batches = proto_decoder
            .decode_signal(Signal::Traces, Bytes::from_static(OTLP_TRACE_REQUEST))
            .expect("the protobuf fixture must decode");

        let mut json_decoder = OtlpDecoder::new();
        let json_batches = json_decoder
            .decode_signal_json(Signal::Traces, Bytes::from_static(OTLP_TRACE_REQUEST_JSON))
            .expect("the JSON fixture must decode");

        assert_eq!(proto_batches.len(), 1);
        assert_eq!(json_batches.len(), 1);
        assert_eq!(
            proto_batches[0], json_batches[0],
            "protobuf and JSON encodings of the same span must decode to the identical EventBatch"
        );
    }

    #[test]
    fn a_request_with_two_scopes_under_one_resource_decodes_to_two_batches() {
        let resource_pb = crate::otlp::generated::opentelemetry::proto::resource::v1::Resource {
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        };
        let scope_a = trace_pb::ScopeSpans {
            scope: Some(
                crate::otlp::generated::opentelemetry::proto::common::v1::InstrumentationScope {
                    name: "scope-a".to_string(),
                    version: String::new(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                },
            ),
            spans: vec![trace_pb::Span {
                trace_id: vec![1; 16],
                span_id: vec![1; 8],
                trace_state: String::new(),
                parent_span_id: Vec::new(),
                flags: 0,
                name: "span-a".to_string(),
                kind: 0,
                start_time_unix_nano: 1,
                end_time_unix_nano: 2,
                attributes: Vec::new(),
                dropped_attributes_count: 0,
                events: Vec::new(),
                dropped_events_count: 0,
                links: Vec::new(),
                dropped_links_count: 0,
                status: None,
            }],
            schema_url: String::new(),
        };
        let mut scope_b = scope_a.clone();
        scope_b.scope.as_mut().unwrap().name = "scope-b".to_string();
        scope_b.spans[0].trace_id = vec![2; 16];
        scope_b.spans[0].span_id = vec![2; 8];
        scope_b.spans[0].name = "span-b".to_string();

        let data = trace_pb::TracesData {
            resource_spans: vec![trace_pb::ResourceSpans {
                resource: Some(resource_pb),
                scope_spans: vec![scope_a, scope_b],
                schema_url: String::new(),
            }],
        };

        let mut decoder = OtlpDecoder::new();
        let batches = decoder
            .decode_signal(Signal::Traces, Bytes::from(data.encode_to_vec()))
            .expect("must decode");
        assert_eq!(
            batches.len(),
            2,
            "two scope groups under one resource must decode to two batches"
        );
        let names: Vec<&[u8]> =
            batches.iter().map(|b| b.scope.as_ref().unwrap().name.as_ref()).collect();
        assert!(names.contains(&b"scope-a".as_slice()));
        assert!(names.contains(&b"scope-b".as_slice()));
    }

    #[test]
    fn a_batch_with_some_scope_re_encodes_the_same_scope_and_schema_url() {
        let mut scope_attributes = AttrMap::new();
        scope_attributes.insert("k", "v");
        let scope = logit_core::Scope {
            name: bytes::Bytes::from_static(b"nginx-otel-module"),
            version: bytes::Bytes::from_static(b"1.0.0"),
            attributes: scope_attributes,
            dropped_attributes_count: 1,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/scope-schema")),
        };
        let span_event = Event::span(
            1,
            AttrMap::new(),
            logit_core::SpanRecord {
                trace_id: [1; 16],
                span_id: [2; 8],
                parent_span_id: None,
                name: Value::str("s"),
                kind: logit_core::SpanKind::Internal,
                status: logit_core::SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 2,
                flags: 0,
                ext: None,
            },
        );
        let input_batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(Arc::new(scope.clone())),
            events: vec![span_event],
        };

        let mut encoder = OtlpEncoder::new();
        let payloads = encoder.encode_signals(&input_batch).unwrap();
        let bytes = payloads.into_iter().find(|p| p.signal == Signal::Traces).unwrap().bytes;

        let mut decoder = OtlpDecoder::new();
        let batches = decoder.decode_signal(Signal::Traces, bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].scope.as_deref(), Some(&scope));
    }

    #[test]
    fn a_resource_logs_with_one_empty_scope_group_decodes_to_exactly_one_batch() {
        let non_empty_log = Event::log(
            1,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let wire_record =
            logs::encode_log_record(&non_empty_log, non_empty_log.log.as_ref().unwrap());

        let non_empty_scope = logs_pb::ScopeLogs {
            scope: Some(
                crate::otlp::generated::opentelemetry::proto::common::v1::InstrumentationScope {
                    name: "scope-a".to_string(),
                    version: String::new(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                },
            ),
            log_records: vec![wire_record],
            schema_url: String::new(),
        };
        let empty_scope = logs_pb::ScopeLogs {
            scope: Some(
                crate::otlp::generated::opentelemetry::proto::common::v1::InstrumentationScope {
                    name: "scope-b".to_string(),
                    version: String::new(),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                },
            ),
            log_records: Vec::new(),
            schema_url: String::new(),
        };
        let data = logs_pb::LogsData {
            resource_logs: vec![logs_pb::ResourceLogs {
                resource: None,
                scope_logs: vec![non_empty_scope, empty_scope],
                schema_url: String::new(),
            }],
        };

        let mut decoder = OtlpDecoder::new();
        let batches = decoder
            .decode_signal(Signal::Logs, Bytes::from(data.encode_to_vec()))
            .expect("must decode");
        assert_eq!(batches.len(), 1, "the empty scope-b group must not produce a batch of its own");
        assert_eq!(batches[0].scope.as_ref().unwrap().name, bytes::Bytes::from_static(b"scope-a"));
    }
}
