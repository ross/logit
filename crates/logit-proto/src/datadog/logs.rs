//! Datadog logs: `POST /api/v2/logs` (and the legacy `/v1/input`, same body), both directions.
//! The mapping table is in the parent module's doc ("Logs, events, service checks"). The JSON
//! conversion and ordered writer it shares with the other JSON routes live in [`crate::json`].

use super::time::{millis_to_nanos, nanos_to_millis, or_received};
use super::{is_datadog_event, DatadogDecoder, DatadogEncoder, ATTR_HOST_NAME, ATTR_SOURCE};
use crate::json::{json_to_value, value_text, write_i64, write_str, write_value, JsonObject};
use crate::CodecError;
use bytes::Bytes;
use logit_core::attrs::merged;
use logit_core::interner::{intern, resolve};
use logit_core::trace::to_hex;
use logit_core::{
    parse_rfc3339_to_nanos, AttrMap, BodyFormat, Event, EventBatch, LogRecord, Resource, Severity,
    Symbol, Value,
};
use serde_json::Value as Json;
use std::sync::{Arc, LazyLock};

/// `status`: a log's raw Datadog status, kept verbatim beside the normalized severity.
pub const ATTR_STATUS: &str = "status";
/// `hostname`, `service`, `ddsource`, `ddtags`: a decoded log's reserved fields, kept verbatim.
pub const ATTR_HOSTNAME: &str = "hostname";
pub const ATTR_SERVICE: &str = "service";
pub const ATTR_DDSOURCE: &str = "ddsource";
pub const ATTR_DDTAGS: &str = "ddtags";
/// `service.name`: the OTel name the encoder's `service` falls back to.
pub const ATTR_SERVICE_NAME: &str = "service.name";
/// `trace_id`/`span_id`: the OTel-form hex keys Datadog's log intake detects for trace
/// correlation, which the encoder writes from `LogRecord.trace`.
pub const WIRE_TRACE_ID: &str = "trace_id";
pub const WIRE_SPAN_ID: &str = "span_id";

struct LogKeys {
    status: Symbol,
    hostname: Symbol,
    service: Symbol,
    ddsource: Symbol,
    ddtags: Symbol,
    host_name: Symbol,
    service_name: Symbol,
    source: Symbol,
    message: Symbol,
    timestamp: Symbol,
    trace_id: Symbol,
    span_id: Symbol,
}

static KEYS: LazyLock<LogKeys> = LazyLock::new(|| LogKeys {
    status: intern(ATTR_STATUS),
    hostname: intern(ATTR_HOSTNAME),
    service: intern(ATTR_SERVICE),
    ddsource: intern(ATTR_DDSOURCE),
    ddtags: intern(ATTR_DDTAGS),
    host_name: intern(ATTR_HOST_NAME),
    service_name: intern(ATTR_SERVICE_NAME),
    source: intern(ATTR_SOURCE),
    message: intern("message"),
    timestamp: intern("timestamp"),
    trace_id: intern(WIRE_TRACE_ID),
    span_id: intern(WIRE_SPAN_ID),
});

impl DatadogDecoder {
    /// Decodes a `/api/v2/logs` body: a JSON array of log objects, or one bare object. One
    /// [`Event::log`] per item; an item with no `message` is skipped and counted. A bare empty
    /// object is no log and isn't counted: it's the connectivity check an Agent's logs sender
    /// posts before its first batch. The batch `Resource` is empty: a log payload carries no
    /// request-level identity.
    pub fn decode_logs(&mut self, body: &[u8], received_at: i64) -> Result<EventBatch, CodecError> {
        let json = parse_json(body, "logs")?;
        let items = match &json {
            Json::Array(items) => items.as_slice(),
            Json::Object(obj) if obj.is_empty() => &[],
            Json::Object(_) => std::slice::from_ref(&json),
            _ => {
                return Err(CodecError::Malformed(
                    "datadog logs body must be a JSON array or object".into(),
                ))
            }
        };
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            if let Some(event) = self.decode_log_item(item, received_at) {
                events.push(event);
            }
        }
        Ok(new_batch(Resource::default(), events))
    }

    fn decode_log_item(&mut self, item: &Json, received_at: i64) -> Option<Event> {
        let Json::Object(obj) = item else {
            self.skip_log("not_an_object");
            self.diagnostics.warn_throttled("malformed_log", "datadog log item is not an object");
            return None;
        };
        let message = match obj.get("message") {
            None | Some(Json::Null) => {
                self.skip_log("no_message");
                return None;
            }
            Some(Json::String(s)) => Value::str(s.as_str()),
            // The Agent always sends a string. A public client's non-string message becomes its
            // JSON text, which is what the encoder writes for a non-`Str` message anyway.
            Some(other) => Value::str(other.to_string()),
        };
        let mut timestamp = None;
        let mut severity = None;
        let mut attributes = AttrMap::new();
        for (key, value) in obj {
            match key.as_str() {
                "message" => {}
                "timestamp" => timestamp = self.log_timestamp(value),
                _ => {
                    if key == ATTR_STATUS {
                        severity = value.as_str().and_then(status_severity);
                    }
                    attributes.insert(key, json_to_value(value));
                }
            }
        }
        Some(Event::log(
            or_received(timestamp, received_at),
            attributes,
            LogRecord {
                message,
                severity,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        ))
    }

    /// Integer (or fractional) milliseconds, or an RFC 3339 string. Anything else is reported and
    /// falls back to `received_at`.
    fn log_timestamp(&mut self, value: &Json) -> Option<i64> {
        match value {
            Json::Null => None,
            Json::Number(n) => {
                if let Some(ms) = n.as_i64() {
                    Some(millis_to_nanos(ms))
                } else if n.as_u64().is_some() {
                    Some(i64::MAX)
                } else {
                    // `as` saturates, and `serde_json` never yields a non-finite number.
                    n.as_f64().map(|ms| (ms * 1e6) as i64)
                }
            }
            Json::String(s) => match parse_rfc3339_to_nanos(s) {
                Ok(ns) => Some(ns),
                Err(_) => {
                    self.diagnostics
                        .warn_throttled("bad_timestamp", "datadog log timestamp is not RFC 3339");
                    None
                }
            },
            _ => {
                self.diagnostics.warn_throttled(
                    "bad_timestamp",
                    "datadog log timestamp is neither a number nor a string",
                );
                None
            }
        }
    }

    fn skip_log(&self, reason: &'static str) {
        self.telemetry.count("logit.input.logs.skipped", 1.0, &[("reason", reason)]);
    }
}

/// Datadog's log statuses (the Agent's and the intake's status remapper vocabulary), matched
/// case-insensitively.
fn status_severity(status: &str) -> Option<Severity> {
    let lower = status.to_ascii_lowercase();
    Some(match lower.as_str() {
        "emergency" | "emerg" | "alert" | "critical" | "crit" | "fatal" => Severity::Fatal,
        "error" | "err" => Severity::Error,
        "warning" | "warn" => Severity::Warn,
        "notice" | "info" => Severity::Info,
        "debug" => Severity::Debug,
        "trace" => Severity::Trace,
        _ => return None,
    })
}

impl DatadogEncoder {
    /// Encodes every event carrying a `log` as one `/api/v2/logs` JSON array; `None` when none
    /// does. An event without a `log`, or a Datadog event (a `log` carrying `statsd.event.title`,
    /// which [`DatadogEncoder::encode_events`] sends), is skipped silently: that is ordinary
    /// fan-out, not a loss.
    pub fn encode_logs(&self, batch: &EventBatch) -> Option<Bytes> {
        let mut out = Vec::new();
        out.push(b'[');
        let mut any = false;
        for event in &batch.events {
            let Some(log) = &event.log else { continue };
            if is_datadog_event(&batch.resource, event) {
                continue;
            }
            if any {
                out.push(b',');
            }
            any = true;
            self.encode_log(&batch.resource, event, log, &mut out);
        }
        if !any {
            return None;
        }
        out.push(b']');
        Some(Bytes::from(out))
    }

    fn encode_log(&self, resource: &Resource, event: &Event, log: &LogRecord, out: &mut Vec<u8>) {
        let keys = &*KEYS;
        let attrs: Vec<(Symbol, &Value)> = merged(resource, event).collect();
        let find = |key: Symbol| attrs.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
        let mut consumed =
            vec![keys.status, keys.hostname, keys.service, keys.ddsource, keys.ddtags];

        let mut obj = JsonObject::begin(out);
        write_str(obj.key("message"), &value_text(&log.message));
        if let Some(status) = find(keys.status) {
            write_value(obj.key("status"), status);
        } else if let Some(severity) = log.severity {
            write_str(obj.key("status"), severity.as_str());
        }
        write_i64(obj.key("timestamp"), nanos_to_millis(event.timestamp));
        for (key, primary, fallback) in [
            ("hostname", keys.hostname, Some(keys.host_name)),
            ("service", keys.service, Some(keys.service_name)),
            ("ddsource", keys.ddsource, Some(keys.source)),
            ("ddtags", keys.ddtags, None),
        ] {
            if let Some(value) = find(primary) {
                write_value(obj.key(key), value);
            } else if let Some(value) = fallback.and_then(find) {
                write_value(obj.key(key), value);
                consumed.extend(fallback);
            } else if primary == keys.ddsource {
                if let Some(source) = &self.default_source {
                    write_str(obj.key(key), source);
                }
            }
        }
        for (key, value) in &attrs {
            if consumed.contains(key) {
                continue;
            }
            if *key == keys.message || *key == keys.timestamp {
                // Would collide with the wire's own `message`/`timestamp`. A decoded Datadog log
                // never carries either as an attribute.
                self.telemetry.count(
                    "logit.output.tags.dropped",
                    1.0,
                    &[("reason", "reserved_key")],
                );
                continue;
            }
            write_value(obj.key(resolve(*key)), value);
        }
        // An attribute of either name wins and suppresses both, so a relayed Datadog log goes
        // back out as it arrived and its ids are never mixed with a `TraceRef`'s.
        if let Some(trace) = &log.trace {
            if find(keys.trace_id).is_none() && find(keys.span_id).is_none() {
                write_str(obj.key(WIRE_TRACE_ID), &to_hex(&trace.trace_id));
                if let Some(span_id) = &trace.span_id {
                    write_str(obj.key(WIRE_SPAN_ID), &to_hex(span_id));
                }
            }
        }
        obj.finish();
    }
}

// -- plumbing shared by the datadog JSON routes ---------------------------------------------------

pub(super) fn parse_json(body: &[u8], route: &str) -> Result<Json, CodecError> {
    serde_json::from_slice(body)
        .map_err(|e| CodecError::Malformed(format!("datadog {route} body is not JSON: {e}")))
}

pub(super) fn new_batch(resource: Resource, events: Vec<Event>) -> EventBatch {
    EventBatch { resource: Arc::new(resource), scope: None, events }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::{MetricKind, MetricRecord};

    const RECEIVED_AT: i64 = 1_690_000_000_000_000_000;

    fn decode(body: &str) -> EventBatch {
        DatadogDecoder::new().decode_logs(body.as_bytes(), RECEIVED_AT).expect("decodes")
    }

    fn encode(batch: &EventBatch) -> String {
        let bytes = DatadogEncoder::new().encode_logs(batch).expect("encodes");
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    /// The summed value of every `metric` point tagged `tag`. Drains, so call it once per test.
    pub(in crate::datadog) fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> f64 {
        let mut total = 0.0;
        for event in registry.drain(0) {
            if event.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                continue;
            }
            for m in &event.metrics {
                if resolve(m.name) == metric {
                    if let MetricKind::Sum(sum) = &m.kind {
                        total += sum.value;
                    }
                }
            }
        }
        total
    }

    fn metric_event() -> Event {
        Event::metric(0, AttrMap::new(), MetricRecord::new(intern("m"), MetricKind::Gauge(1.0)))
    }

    #[test]
    fn agent_item_maps_every_field() {
        let batch = decode(
            r#"[{"message":"hello world","status":"warning","timestamp":1700000000123,"hostname":"myhost","service":"svc","ddsource":"nginx","ddtags":"env:prod,version:1"}]"#,
        );
        assert!(batch.resource.attributes.is_empty());
        let event = &batch.events[0];
        assert_eq!(event.timestamp, 1_700_000_000_123_000_000);
        let log = event.log.as_ref().unwrap();
        assert_eq!(log.message, Value::str("hello world"));
        assert_eq!(log.severity, Some(Severity::Warn));
        assert_eq!(log.body_format, BodyFormat::Raw);
        assert_eq!(event.attributes.get("status"), Some(&Value::str("warning")));
        assert_eq!(event.attributes.get("hostname"), Some(&Value::str("myhost")));
        assert_eq!(event.attributes.get("service"), Some(&Value::str("svc")));
        assert_eq!(event.attributes.get("ddsource"), Some(&Value::str("nginx")));
        // Not expanded: that is a transform's job.
        assert_eq!(event.attributes.get("ddtags"), Some(&Value::str("env:prod,version:1")));
        assert_eq!(event.attributes.len(), 5);
    }

    #[test]
    fn statuses_map_to_severities() {
        for (status, severity) in [
            ("emerg", Some(Severity::Fatal)),
            ("CRITICAL", Some(Severity::Fatal)),
            ("alert", Some(Severity::Fatal)),
            ("err", Some(Severity::Error)),
            ("warn", Some(Severity::Warn)),
            ("notice", Some(Severity::Info)),
            ("info", Some(Severity::Info)),
            ("debug", Some(Severity::Debug)),
            ("trace", Some(Severity::Trace)),
            ("ok", None),
        ] {
            assert_eq!(status_severity(status), severity, "{status}");
        }
    }

    #[test]
    fn extra_keys_become_typed_attributes_and_a_bare_object_is_one_item() {
        let batch = decode(
            r#"{"message":"m","user":{"id":7,"tags":["a",true,null,1.5]},"big":18446744073709551615,"neg":-3}"#,
        );
        let event = &batch.events[0];
        assert_eq!(event.timestamp, RECEIVED_AT);
        let Some(Value::Map(user)) = event.attributes.get("user") else { panic!("not a map") };
        assert_eq!(user.get("id"), Some(&Value::I64(7)));
        assert_eq!(
            user.get("tags"),
            Some(&Value::Array(vec![
                Value::str("a"),
                Value::Bool(true),
                Value::Null,
                Value::F64(1.5)
            ]))
        );
        assert_eq!(event.attributes.get("big"), Some(&Value::U64(u64::MAX)));
        assert_eq!(event.attributes.get("neg"), Some(&Value::I64(-3)));
    }

    #[test]
    fn non_string_messages_decode_as_their_json_text() {
        let batch = decode(r#"[{"message":{"a":1}}]"#);
        assert_eq!(batch.events[0].log.as_ref().unwrap().message, Value::str(r#"{"a":1}"#));
    }

    #[test]
    fn rfc3339_timestamps_and_missing_messages() {
        let registry = Registry::new();
        let mut decoder = DatadogDecoder::new().with_telemetry(registry.telemetry_for(
            "datadog_in",
            "datadog_in",
            "listener",
        ));
        let batch = decoder
            .decode_logs(
                br#"[{"message":"a","timestamp":"2023-11-14T22:13:20.5Z"},{"status":"info"},{"message":null},{"message":"b","timestamp":"yesterday"}]"#,
                RECEIVED_AT,
            )
            .unwrap();
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.events[0].timestamp, 1_700_000_000_500_000_000);
        assert_eq!(batch.events[1].timestamp, RECEIVED_AT);
        assert_eq!(counted(&registry, "logit.input.logs.skipped", ("reason", "no_message")), 2.0);
    }

    /// An Agent's connectivity check: no log, and nothing counted as skipped. An empty object
    /// inside an array is still a log item with no message.
    #[test]
    fn a_bare_empty_object_is_no_log_and_not_a_skip() {
        let registry = Registry::new();
        let mut decoder = DatadogDecoder::new().with_telemetry(registry.telemetry_for(
            "datadog_in",
            "datadog_in",
            "listener",
        ));
        assert!(decoder.decode_logs(b"{}", RECEIVED_AT).unwrap().events.is_empty());
        assert_eq!(counted(&registry, "logit.input.logs.skipped", ("reason", "no_message")), 0.0);
        assert!(decoder.decode_logs(b"[{}]", RECEIVED_AT).unwrap().events.is_empty());
        assert_eq!(counted(&registry, "logit.input.logs.skipped", ("reason", "no_message")), 1.0);
    }

    #[test]
    fn non_object_items_are_skipped_and_counted() {
        let registry = Registry::new();
        let mut decoder = DatadogDecoder::new().with_telemetry(registry.telemetry_for(
            "datadog_in",
            "datadog_in",
            "listener",
        ));
        let batch = decoder.decode_logs(br#"[7,{"message":"a"}]"#, RECEIVED_AT).unwrap();
        assert_eq!(batch.events.len(), 1);
        assert_eq!(
            counted(&registry, "logit.input.logs.skipped", ("reason", "not_an_object")),
            1.0
        );
    }

    #[test]
    fn unparseable_bodies_are_malformed() {
        let mut decoder = DatadogDecoder::new();
        assert!(decoder.decode_logs(b"not json", 0).is_err());
        assert!(decoder.decode_logs(b"\"a string\"", 0).is_err());
    }

    #[test]
    fn encode_writes_reserved_fields_first_then_attributes() {
        let batch = decode(
            r#"[{"message":"hi","status":"info","timestamp":1700000000000,"hostname":"h","service":"s","ddsource":"src","ddtags":"env:prod","user":{"id":1}}]"#,
        );
        assert_eq!(
            encode(&batch),
            r#"[{"message":"hi","status":"info","timestamp":1700000000000,"hostname":"h","service":"s","ddsource":"src","ddtags":"env:prod","user":{"id":1}}]"#
        );
    }

    #[test]
    fn encode_falls_back_to_model_names_and_the_default_source() {
        let mut resource = Resource::default();
        resource.attributes.insert("service.name", Value::str("checkout"));
        resource.attributes.insert("host.name", Value::str("res-host"));
        let mut attrs = AttrMap::new();
        attrs.insert("host.name", Value::str("evt-host"));
        attrs.insert("n", Value::I64(3));
        attrs.insert("bin", Value::Bytes(Bytes::from_static(b"\x00\x01")));
        attrs.insert("at", Value::Timestamp(0));
        let log = LogRecord {
            message: Value::Map(Box::new(AttrMap::from_iter([("k", Value::str("v"))]))),
            severity: Some(Severity::Error),
            body_format: BodyFormat::Json,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        };
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![Event::log(1_700_000_000_000_000_000, attrs, log), metric_event()],
        };
        let out = DatadogEncoder::new().with_default_source("logit").encode_logs(&batch).unwrap();
        let json: Json = serde_json::from_slice(&out).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 1);
        let item = &json[0];
        assert_eq!(item["message"], r#"{"k":"v"}"#);
        assert_eq!(item["status"], "error");
        assert_eq!(item["timestamp"], 1_700_000_000_000_i64);
        assert_eq!(item["hostname"], "evt-host");
        assert_eq!(item["service"], "checkout");
        assert_eq!(item["ddsource"], "logit");
        assert_eq!(item["n"], 3);
        assert_eq!(item["bin"], "AAE=");
        assert_eq!(item["at"], "1970-01-01T00:00:00.000000000Z");
        assert!(item.get("host.name").is_none());
        assert!(item.get("service.name").is_none());
    }

    #[test]
    fn encode_skips_log_less_batches_and_drops_reserved_attribute_names() {
        let metric_only = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![metric_event()],
        };
        assert!(DatadogEncoder::new().encode_logs(&metric_only).is_none());

        let registry = Registry::new();
        let encoder = DatadogEncoder::new().with_telemetry(registry.telemetry_for(
            "datadog_out",
            "datadog_out",
            "sink",
        ));
        let mut batch = decode(r#"{"message":"m","timestamp":1000}"#);
        batch.events[0].attributes.insert("message", Value::str("clash"));
        let out = String::from_utf8(encoder.encode_logs(&batch).unwrap().to_vec()).unwrap();
        assert_eq!(out, r#"[{"message":"m","timestamp":1000}]"#);
        assert_eq!(
            counted(&registry, "logit.output.tags.dropped", ("reason", "reserved_key")),
            1.0
        );
    }

    fn with_trace(mut batch: EventBatch, trace: logit_core::TraceRef) -> EventBatch {
        batch.events[0].log.as_mut().unwrap().trace = Some(trace);
        batch
    }

    #[test]
    fn encode_writes_a_trace_ref_as_otel_hex_ids() {
        let trace = logit_core::TraceRef {
            trace_id: logit_core::trace::trace_id_bytes(
                0x64de_8e2b_0000_0000,
                0x1234_5678_abcd_ef12,
            ),
            span_id: Some(987_654_321_u64.to_be_bytes()),
            flags: 1,
        };
        let batch = with_trace(decode(r#"{"message":"m","timestamp":1000,"k":"v"}"#), trace);
        assert_eq!(
            encode(&batch),
            r#"[{"message":"m","timestamp":1000,"k":"v","trace_id":"64de8e2b0000000012345678abcdef12","span_id":"000000003ade68b1"}]"#
        );

        let no_span = logit_core::TraceRef { span_id: None, ..trace };
        let batch = with_trace(decode(r#"{"message":"m","timestamp":1000}"#), no_span);
        assert_eq!(
            encode(&batch),
            r#"[{"message":"m","timestamp":1000,"trace_id":"64de8e2b0000000012345678abcdef12"}]"#
        );
    }

    #[test]
    fn a_trace_id_or_span_id_attribute_wins_over_the_trace_ref() {
        let trace =
            logit_core::TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 0 };
        for body in [
            r#"{"message":"m","timestamp":1000,"trace_id":"1234"}"#,
            r#"{"message":"m","timestamp":1000,"span_id":"5678"}"#,
        ] {
            let batch = with_trace(decode(body), trace);
            assert_eq!(encode(&batch), format!("[{body}]"), "the attribute alone goes out");
        }
    }

    #[test]
    fn a_decoded_log_with_trace_ids_re_encodes_unchanged() {
        let body = r#"[{"message":"m","status":"info","timestamp":1000,"dd.trace_id":"1234567890123456789","trace_id":"64de8e2b0000000012345678abcdef12","span_id":"00f067aa0ba902b7"}]"#;
        let batch = decode(body);
        assert_eq!(batch.events[0].log.as_ref().unwrap().trace, None, "the decoder sets no ref");
        let once = encode(&batch);
        let again = decode(&once);
        assert_eq!(again, batch, "decode(encode(x)) is x");
        assert_eq!(encode(&again), once, "and the bytes are stable");
        let json: Json = serde_json::from_str(&once).unwrap();
        assert_eq!(json, serde_json::from_str::<Json>(body).unwrap(), "same keys and values");
    }
}
