//! The OTLP codec: `OtlpEncoder`/`OtlpDecoder`, implementing [`crate::SignalEncoder`]/
//! [`crate::SignalDecoder`] against the vendored, committed protobuf types in `generated/` (see
//! `crates/logit-proto/proto/README.md` for provenance and
//! [ADR `committed-pregenerated-otlp-protobuf`](../../../../docs/adr/committed-pregenerated-otlp-protobuf.md) for why they're
//! committed rather than generated at build time).
//!
//! **This module doc is the mapping table.** The four sibling modules hold one direction/signal
//! each: [`common`] (`Value` ↔ `AnyValue`, attributes, resource/scope nesting -- shared by all
//! three signals), [`logs`] (`Severity`, `BodyFormat`), [`traces`] (near-total), [`metrics`] (the
//! hard part: temporality, histogram/summary/distribution/set). Read each module's own doc for its
//! detail; this one covers only what's common to all of them.
//!
//! **Wire types.** `logit` encodes/decodes `TracesData`/`LogsData`/`MetricsData` (the plain,
//! non-collector top-level messages -- `{ repeated Resource*Signal* = 1; }`), not
//! `Export*ServiceRequest`. The two are wire-identical: an `ExportTraceServiceRequest` has exactly
//! the same single `repeated ResourceSpans resource_spans = 1` field `TracesData` does, so the
//! bytes this crate produces are valid `Export*ServiceRequest` bodies without this crate ever
//! generating or depending on the collector service messages (see `proto/README.md`). PR3's
//! `otlp_in`/`otlp_out` parse `partial_success` themselves from the same bytes, on the response
//! side, without needing generated types for it either -- that shape is small enough to build by
//! hand there.
//!
//! **Nesting**, shared by every signal (detail in [`common`]): one `EventBatch` encodes as one
//! `Resource*` entry (the batch's single `Arc<Resource>`) with one `Scope*` stamped
//! `{ name: "logit", version: env!("CARGO_PKG_VERSION") }`. Decoding walks every `Resource*` entry
//! in a request into its own `EventBatch` -- **never collapsed into one**, since an `EventBatch`
//! holds exactly one resource and a request can legitimately carry data from several.
//!
//! **An empty batch encodes to no payloads at all** -- an OTLP request with zero `Resource*`
//! entries is a valid but pointless wire message, so [`SignalEncoder::encode_signals`] simply
//! returns nothing for a signal with no events to carry, rather than sending an empty request.

pub mod common;
pub mod logs;
pub mod metrics;
pub mod traces;

#[allow(dead_code)] // PR3 (otlp_in/otlp_out) is the first real caller of most of this tree's API.
pub(crate) mod generated;

use crate::{CodecError, Signal, SignalDecoder, SignalEncoder};
use bytes::Bytes;
use generated::opentelemetry::proto::logs::v1 as logs_pb;
use generated::opentelemetry::proto::metrics::v1 as metrics_pb;
use generated::opentelemetry::proto::trace::v1 as trace_pb;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use prost::Message;
use std::sync::Arc;

/// Encodes an [`EventBatch`] into OTLP, one payload per non-empty [`Signal`] it carries. Carries
/// its own [`Telemetry`]/[`Diagnostics`] handle so the lossy metric paths ([`metrics`]'s module
/// doc) can count themselves -- disabled (default-constructed) handles make every one of those
/// calls a no-op, so an `OtlpEncoder` used purely as a codec (no component attached) costs nothing
/// extra to carry.
#[derive(Default)]
pub struct OtlpEncoder {
    telemetry: Telemetry,
    diagnostics: Diagnostics,
}

impl OtlpEncoder {
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

impl SignalEncoder for OtlpEncoder {
    fn encode_signals(&mut self, batch: &EventBatch) -> Result<Vec<(Signal, Bytes)>, CodecError> {
        let resource = common::resource_to_pb(&batch.resource);
        let scope = common::logit_scope();

        let mut log_records = Vec::new();
        let mut spans = Vec::new();
        let mut metric_points = Vec::new();

        for event in &batch.events {
            if let Some(log) = &event.log {
                log_records.push(logs::encode_log_record(event, log));
            }
            if let Some(span) = &event.span {
                spans.push(traces::encode_span(event, span));
            }
            for metric in &event.metrics {
                if let Some(m) =
                    metrics::encode_metric(event, metric, &self.telemetry, &mut self.diagnostics)
                {
                    metric_points.push(m);
                }
            }
        }

        let mut payloads = Vec::with_capacity(3);
        if !log_records.is_empty() {
            let data = logs_pb::LogsData {
                resource_logs: vec![logs_pb::ResourceLogs {
                    resource: Some(resource.clone()),
                    scope_logs: vec![logs_pb::ScopeLogs {
                        scope: Some(scope.clone()),
                        log_records,
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            };
            payloads.push((Signal::Logs, Bytes::from(data.encode_to_vec())));
        }
        if !spans.is_empty() {
            let data = trace_pb::TracesData {
                resource_spans: vec![trace_pb::ResourceSpans {
                    resource: Some(resource.clone()),
                    scope_spans: vec![trace_pb::ScopeSpans {
                        scope: Some(scope.clone()),
                        spans,
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            };
            payloads.push((Signal::Traces, Bytes::from(data.encode_to_vec())));
        }
        if !metric_points.is_empty() {
            let data = metrics_pb::MetricsData {
                resource_metrics: vec![metrics_pb::ResourceMetrics {
                    resource: Some(resource),
                    scope_metrics: vec![metrics_pb::ScopeMetrics {
                        scope: Some(scope),
                        metrics: metric_points,
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                }],
            };
            payloads.push((Signal::Metrics, Bytes::from(data.encode_to_vec())));
        }
        Ok(payloads)
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

fn decode_resource_logs(rl: logs_pb::ResourceLogs) -> EventBatch {
    let resource = common::pb_to_resource(rl.resource);
    let mut events = Vec::new();
    for scope_logs in rl.scope_logs {
        let base_attrs = common::scope_attrs(&scope_logs.scope);
        for record in scope_logs.log_records {
            events.push(logs::decode_log_record(record, base_attrs.clone()));
        }
    }
    EventBatch { resource: Arc::new(resource), events }
}

fn decode_resource_spans(rs: trace_pb::ResourceSpans) -> Result<EventBatch, CodecError> {
    let resource = common::pb_to_resource(rs.resource);
    let mut events = Vec::new();
    for scope_spans in rs.scope_spans {
        let base_attrs = common::scope_attrs(&scope_spans.scope);
        for span in scope_spans.spans {
            events.push(traces::decode_span(span, base_attrs.clone())?);
        }
    }
    Ok(EventBatch { resource: Arc::new(resource), events })
}

impl OtlpDecoder {
    fn decode_resource_metrics(&self, rm: metrics_pb::ResourceMetrics) -> EventBatch {
        let resource = common::pb_to_resource(rm.resource);
        let mut events = Vec::new();
        for scope_metrics in rm.scope_metrics {
            let base_attrs = common::scope_attrs(&scope_metrics.scope);
            for metric in scope_metrics.metrics {
                events.extend(metrics::decode_metric(metric, &base_attrs, &self.telemetry));
            }
        }
        EventBatch { resource: Arc::new(resource), events }
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
                Ok(data.resource_logs.into_iter().map(decode_resource_logs).collect())
            }
            Signal::Traces => {
                let data = trace_pb::TracesData::decode(bytes)
                    .map_err(|e| CodecError::Malformed(e.to_string()))?;
                data.resource_spans.into_iter().map(decode_resource_spans).collect()
            }
            Signal::Metrics => {
                let data = metrics_pb::MetricsData::decode(bytes)
                    .map_err(|e| CodecError::Malformed(e.to_string()))?;
                Ok(data
                    .resource_metrics
                    .into_iter()
                    .map(|rm| self.decode_resource_metrics(rm))
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
        EventBatch { resource: Arc::new(Resource::default()), events }
    }

    #[test]
    fn a_batch_carrying_all_three_signals_encodes_as_three_separate_payloads() {
        let log = Event::log(
            1,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
            },
        );
        let metric = Event::metric(
            2,
            AttrMap::new(),
            MetricRecord {
                name: logit_core::interner::intern("m"),
                kind: MetricKind::Counter(1.0),
                unit: None,
            },
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
            },
        );

        let mut encoder = OtlpEncoder::new();
        let payloads = encoder.encode_signals(&batch(vec![log, metric, span])).unwrap();
        let signals: Vec<Signal> = payloads.iter().map(|(s, _)| *s).collect();
        assert_eq!(signals.len(), 3, "got signals: {signals:?}");
        assert!(signals.contains(&Signal::Logs));
        assert!(signals.contains(&Signal::Metrics));
        assert!(signals.contains(&Signal::Traces));
    }

    #[test]
    fn an_empty_batch_encodes_no_payloads_at_all() {
        let mut encoder = OtlpEncoder::new();
        let payloads = encoder.encode_signals(&batch(vec![])).unwrap();
        assert!(payloads.is_empty(), "got: {payloads:?}");
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
            },
        );

        let mut encoder = OtlpEncoder::new();
        let bytes_a = encoder
            .encode_signals(&EventBatch { resource: Arc::new(resource_a), events: vec![span_a] })
            .unwrap();
        let bytes_b = encoder
            .encode_signals(&EventBatch { resource: Arc::new(resource_b), events: vec![span_b] })
            .unwrap();

        // Hand-assemble one TracesData carrying both ResourceSpans, the shape a batching
        // intermediary (or a test standing in for one) produces.
        let data_a = trace_pb::TracesData::decode(bytes_a[0].1.clone()).unwrap();
        let data_b = trace_pb::TracesData::decode(bytes_b[0].1.clone()).unwrap();
        let mut combined = data_a;
        combined.resource_spans.extend(data_b.resource_spans);
        let combined_bytes = Bytes::from(combined.encode_to_vec());

        let mut decoder = OtlpDecoder::new();
        let batches = decoder.decode_signal(Signal::Traces, combined_bytes).unwrap();
        assert_eq!(batches.len(), 2, "two ResourceSpans entries must decode to two batches");
        assert_eq!(batches[0].resource.attributes.get("host").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(batches[1].resource.attributes.get("host").and_then(|v| v.as_str()), Some("b"));
    }

    /// Captured provenance: NOT from a live collector (none is reachable from this crate's test
    /// suite -- `docs/design/memory.md`'s "Fixtures" section requires exactly that). Self-
    /// constructed instead, by encoding a `TracesData` message with this crate's own generated
    /// `prost` types (one `ResourceSpans` / one `ScopeSpans` / one `Span` with a name, kind,
    /// times, one attribute, one event, one link, and an OK status) and capturing
    /// `prost::Message::encode_to_vec()`'s output, run once and pasted here as a literal --
    /// see `tools/protogen` for the types used. Proves this crate's vendored `.proto`s produce
    /// the field layout OTLP expects (tag numbers, wire types) by decoding it back below and
    /// checking known field values, even without a real collector to compare against.
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

        let batch =
            decode_resource_spans(rs.clone()).expect("decode_resource_spans should succeed");
        assert_eq!(batch.events.len(), 1);
        let decoded_span = batch.events[0].span.as_ref().unwrap();
        assert_eq!(decoded_span.trace_id, [1u8; 16]);
        assert_eq!(decoded_span.span_id, [2u8; 8]);
        assert_eq!(
            batch.events[0].attributes.get("otel.scope.name").and_then(|v| v.as_str()),
            Some("logit")
        );
    }
}
