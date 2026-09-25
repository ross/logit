//! The OTLP codec: `OtlpEncoder`/`OtlpDecoder` over the committed protobuf types in `generated/`
//! (`crates/logit-proto/proto/README.md` has their provenance; ADR
//! `committed-pregenerated-otlp-protobuf` says why they're committed).
//!
//! **This module doc is the mapping table** for what all three signals share. Each sibling module
//! doc has its own: [`common`] (`Value` ↔ `AnyValue`, attributes, resource and scope), [`logs`]
//! (`Severity`, `BodyFormat`, trace context), [`traces`], and [`metrics`] (temporality and the
//! kinds OTLP can't carry natively). [`json`] is the OTLP/JSON dialect layer.
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
//! ([`SignalEncoder::encode_signals`]).

pub mod common;
pub mod json;
pub mod logs;
pub mod metrics;
pub mod traces;

/// The committed `prost` types. Public so a test outside the crate can build a wire message the
/// encoder never emits, such as a timestamp past `i64::MAX`.
pub mod generated;

use crate::{CodecError, Signal, SignalDecoder, SignalEncoder};
use bytes::Bytes;
use generated::opentelemetry::proto::logs::v1 as logs_pb;
use generated::opentelemetry::proto::metrics::v1 as metrics_pb;
use generated::opentelemetry::proto::trace::v1 as trace_pb;
use logit_core::{AttrMap, Diagnostics, Event, EventBatch, Telemetry};
use prost::Message;
use std::sync::Arc;

/// Encodes an [`EventBatch`] into OTLP, one payload per non-empty [`Signal`].
///
/// Holds [`Telemetry`]/[`Diagnostics`] handles so the lossy metric paths ([`metrics`]'s module
/// doc) can count themselves; the default handles are no-ops.
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
                        schema_url: scope_schema_url.clone(),
                    }],
                    schema_url: resource_schema_url.clone(),
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
                        schema_url: scope_schema_url.clone(),
                    }],
                    schema_url: resource_schema_url.clone(),
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
                        schema_url: scope_schema_url,
                    }],
                    schema_url: resource_schema_url,
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

    #[test]
    fn a_batch_carrying_all_three_signals_encodes_as_three_separate_payloads() {
        let log = Event::log(
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
        let (_, bytes) = payloads.into_iter().find(|(s, _)| *s == Signal::Traces).unwrap();

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
