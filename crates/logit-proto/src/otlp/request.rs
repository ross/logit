//! One signal's OTLP export request built from events a caller picks out of a batch, under the
//! batch's resource and scope, as [`OtlpEncoder::request`]. A sink that splits a batch's request
//! (`kafka_out` by trace id and by size, `docs/adr/kafka-otlp-formats.md`, item 1) builds each
//! piece here from the batch's own events, so no sub-batch is built. Each record maps through
//! the same `encode_*` functions [`crate::SignalEncoder::encode_signals`] uses, and the request
//! is the prost value: serializing it is a separate step.

use logit_core::{Event, Resource, Scope};
use prost::Message;

use super::generated::opentelemetry::proto::logs::v1 as logs_pb;
use super::generated::opentelemetry::proto::metrics::v1 as metrics_pb;
use super::generated::opentelemetry::proto::trace::v1 as trace_pb;
use super::{common, logs, metrics, traces, OtlpEncoder};
use crate::Signal;

/// One signal's export request. Each variant is wire-identical to its `Export*ServiceRequest`
/// (the parent module doc's "Wire types").
#[derive(Debug, Clone, PartialEq)]
pub enum OtlpRequest {
    Logs(logs_pb::LogsData),
    Metrics(metrics_pb::MetricsData),
    Traces(trace_pb::TracesData),
}

impl OtlpRequest {
    pub fn signal(&self) -> Signal {
        match self {
            OtlpRequest::Logs(_) => Signal::Logs,
            OtlpRequest::Metrics(_) => Signal::Metrics,
            OtlpRequest::Traces(_) => Signal::Traces,
        }
    }

    /// The protobuf encoding's length, computed without encoding.
    pub fn encoded_len(&self) -> usize {
        match self {
            OtlpRequest::Logs(data) => data.encoded_len(),
            OtlpRequest::Metrics(data) => data.encoded_len(),
            OtlpRequest::Traces(data) => data.encoded_len(),
        }
    }

    /// Appends the OTLP/JSON encoding ([`super::json::write_logs`] and its siblings) to `out`.
    pub fn write_json(&self, out: &mut Vec<u8>) {
        match self {
            OtlpRequest::Logs(data) => super::json::write_logs(data, out),
            OtlpRequest::Metrics(data) => super::json::write_metrics(data, out),
            OtlpRequest::Traces(data) => super::json::write_traces(data, out),
        }
    }

    /// The protobuf encoding.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        match self {
            OtlpRequest::Logs(data) => data.encode_to_vec(),
            OtlpRequest::Metrics(data) => data.encode_to_vec(),
            OtlpRequest::Traces(data) => data.encode_to_vec(),
        }
    }
}

impl OtlpEncoder {
    /// `signal`'s request for `events` under `resource` and `scope`, mapped as
    /// [`crate::SignalEncoder::encode_signals`] maps a whole batch, or `None` when no event
    /// yields a record of that signal.
    ///
    /// A metric the mapping skips or degrades counts through this encoder's telemetry and
    /// diagnostics on every call, so a caller that maps an event more than once mutes their
    /// gate for the repeats.
    pub fn request<'e>(
        &mut self,
        signal: Signal,
        resource: &Resource,
        scope: Option<&Scope>,
        events: impl IntoIterator<Item = &'e Event>,
    ) -> Option<OtlpRequest> {
        let events = events.into_iter();
        let resource_schema_url =
            resource.schema_url.as_ref().map(common::bytes_to_string).unwrap_or_default();
        let scope_schema_url = scope
            .and_then(|s| s.schema_url.as_ref())
            .map(common::bytes_to_string)
            .unwrap_or_default();
        let request = match signal {
            Signal::Logs => {
                let log_records: Vec<_> = events
                    .filter_map(|e| e.log.as_ref().map(|log| logs::encode_log_record(e, log)))
                    .collect();
                if log_records.is_empty() {
                    return None;
                }
                OtlpRequest::Logs(logs_pb::LogsData {
                    resource_logs: vec![logs_pb::ResourceLogs {
                        resource: Some(common::resource_to_pb(resource)),
                        scope_logs: vec![logs_pb::ScopeLogs {
                            scope: Some(common::scope_to_pb(scope)),
                            log_records,
                            schema_url: scope_schema_url,
                        }],
                        schema_url: resource_schema_url,
                    }],
                })
            }
            Signal::Traces => {
                let spans: Vec<_> = events
                    .filter_map(|e| e.span.as_ref().map(|span| traces::encode_span(e, span)))
                    .collect();
                if spans.is_empty() {
                    return None;
                }
                OtlpRequest::Traces(trace_pb::TracesData {
                    resource_spans: vec![trace_pb::ResourceSpans {
                        resource: Some(common::resource_to_pb(resource)),
                        scope_spans: vec![trace_pb::ScopeSpans {
                            scope: Some(common::scope_to_pb(scope)),
                            spans,
                            schema_url: scope_schema_url,
                        }],
                        schema_url: resource_schema_url,
                    }],
                })
            }
            Signal::Metrics => {
                let mut points = Vec::new();
                for event in events {
                    for metric in &event.metrics {
                        points.extend(metrics::encode_metric(
                            event,
                            metric,
                            &self.telemetry,
                            &mut self.diagnostics,
                        ));
                    }
                }
                if points.is_empty() {
                    return None;
                }
                OtlpRequest::Metrics(metrics_pb::MetricsData {
                    resource_metrics: vec![metrics_pb::ResourceMetrics {
                        resource: Some(common::resource_to_pb(resource)),
                        scope_metrics: vec![metrics_pb::ScopeMetrics {
                            scope: Some(common::scope_to_pb(scope)),
                            metrics: points,
                            schema_url: scope_schema_url,
                        }],
                        schema_url: resource_schema_url,
                    }],
                })
            }
        };
        Some(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SignalEncoder;
    use logit_core::{
        AttrMap, EventBatch, LogRecord, MetricKind, MetricRecord, SpanRecord, TraceRef, Value,
    };
    use std::sync::Arc;

    fn log(i: i64) -> Event {
        Event::log(
            i,
            AttrMap::new(),
            LogRecord {
                message: Value::str(format!("m{i}")),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: Some(TraceRef { trace_id: [i as u8; 16], span_id: None, flags: 0 }),
                event_name: None,
                observed_timestamp: 7,
                dropped_attributes_count: 0,
            },
        )
    }

    fn span(i: i64) -> Event {
        Event::span(
            i,
            AttrMap::new(),
            SpanRecord {
                trace_id: [1; 16],
                span_id: [i as u8; 8],
                parent_span_id: None,
                name: Value::str("s"),
                kind: logit_core::SpanKind::Internal,
                status: logit_core::SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: i + 1,
                flags: 0,
                ext: None,
            },
        )
    }

    fn metric(i: i64, kind: MetricKind) -> Event {
        Event::metric(i, AttrMap::new(), MetricRecord::new(logit_core::interner::intern("m"), kind))
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        let mut resource = Resource::default();
        resource.attributes.insert("service.name", Value::str("api"));
        let scope = Scope { name: "lib".into(), ..Scope::default() };
        EventBatch { resource: Arc::new(resource), scope: Some(Arc::new(scope)), events }
    }

    /// A request built from a subset of a batch's events is the request `encode_signals` writes
    /// for a batch of those events alone, signal by signal.
    #[test]
    fn a_subset_request_is_what_encode_signals_writes_for_those_events() {
        let all = batch(vec![
            log(1),
            span(2),
            metric(3, MetricKind::counter(1.0)),
            log(4),
            span(5),
            metric(6, MetricKind::gauge(2.0)),
        ]);
        let picked: Vec<&Event> = [0, 1, 2, 4].iter().map(|&i| &all.events[i]).collect();
        let sub = EventBatch {
            resource: all.resource.clone(),
            scope: all.scope.clone(),
            events: picked.iter().map(|&e| e.clone()).collect(),
        };
        let want = OtlpEncoder::new().encode_signals(&sub).unwrap();
        let mut encoder = OtlpEncoder::new();
        for payload in want {
            let got = encoder
                .request(
                    payload.signal,
                    &all.resource,
                    all.scope.as_deref(),
                    picked.iter().copied(),
                )
                .expect("a request");
            assert_eq!(got.signal(), payload.signal);
            assert_eq!(got.encoded_len(), payload.bytes.len(), "{:?}", payload.signal);
            assert_eq!(got.encode_to_vec(), payload.bytes.to_vec(), "{:?}", payload.signal);
        }
        assert_eq!(
            encoder.request(Signal::Traces, &all.resource, None, [&all.events[0]]),
            None,
            "no event of the signal"
        );
    }
}
