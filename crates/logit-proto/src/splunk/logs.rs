//! HEC log events, both directions: an `event` that is neither a metric nor a span. The
//! OpenTelemetry exporter's log fields map onto `LogRecord`'s typed fields.
//!
//! ## Decode
//!
//! | Wire | Model | Counter |
//! |---|---|---|
//! | `event` string | `LogRecord::message` `Str`, `BodyFormat::Raw` | -- |
//! | `event` object / array | `Map` / `Array`, `BodyFormat::Json` | -- |
//! | `event` number / bool | `I64` (`U64` above `i64::MAX`) or `F64` / `Bool`, `BodyFormat::Raw` | -- |
//! | `fields` | event attributes, as the parent module's table says | -- |
//! | `otel.log.severity.number` (an integer 1 to 24) and/or `otel.log.severity.text` | `severity` by OTLP's band-then-text rule; both fields **also kept** as attributes, since the band collapses 24 numbers to 6 | -- |
//! | `otel.log.name`, a non-empty string | `event_name`; the attribute is consumed | -- |
//! | `trace_id` (32 hex) with a valid or absent `span_id` (16 hex), both non-zero | `trace` (`flags` `0`); both attributes consumed | -- |
//! | a `trace_id` or `span_id` that doesn't parse | both stay attributes, and `trace` is `None` | -- |
//!
//! ## Encode
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `message` `Str`, non-empty | `event` string | -- |
//! | `message` `Map` / `Array` / number / `Bool` | `event` as JSON | -- |
//! | `message` `Bytes` | `event` string, lossy UTF-8 | -- |
//! | `message` `Timestamp` | `event` RFC 3339 string | -- |
//! | `message` `Null`, `""`, or a non-finite `F64` | nothing: HEC rejects a blank `event` for the whole request (code 12 or 13) | `logit.output.events.skipped{reason="blank_event"}` |
//! | `severity`, when neither severity attribute is present | `otel.log.severity.text` (`Info`, …) and `.number` (the band's base, `9`, …) | -- |
//! | either severity attribute | that attribute alone, verbatim | -- |
//! | `event_name`, when no `otel.log.name` attribute | `otel.log.name` | -- |
//! | `trace`, when neither a `trace_id` nor a `span_id` attribute is present | `trace_id` and, when the ref has one, `span_id`, lowercase hex; `flags` has no field | -- |
//! | an attribute starting `metric_name`, or `_value`, on a log whose message is the string `metric` | dropped: it would turn the object into a metric event | `logit.output.tags.dropped{reason="reserved_key"}` |
//!
//! `body_format`, `observed_timestamp`, and `dropped_attributes_count` have no HEC field, and
//! decode from the wire's shape (`Json` for an object or array, else `Raw`; `0`; `0`).

use super::metrics::METRIC_EVENT;
use super::{
    begin_object, write_fields, BatchContext, ObjectMeta, SplunkDecoder, SplunkEncoder,
    ATTR_LOG_NAME, ATTR_SEVERITY_NUMBER, ATTR_SEVERITY_TEXT, ATTR_SPAN_ID, ATTR_TRACE_ID,
};
use crate::json::{flatten_into, json_to_value, value_text, write_str, write_value};
use crate::otlp::logs::{decode_severity, severity_number, severity_text};
use crate::{MessageBuf, Signal};
use logit_core::interner::{intern, resolve};
use logit_core::trace::{parse_span_id, parse_trace_id, to_hex};
use logit_core::{
    format_rfc3339_utc, AttrMap, BodyFormat, Event, LogRecord, Severity, Symbol, TraceRef, Value,
};
use serde_json::{Map, Value as Json};
use std::sync::LazyLock;

struct LogKeys {
    severity_text: Symbol,
    severity_number: Symbol,
    log_name: Symbol,
    trace_id: Symbol,
    span_id: Symbol,
}

static KEYS: LazyLock<LogKeys> = LazyLock::new(|| LogKeys {
    severity_text: intern(ATTR_SEVERITY_TEXT),
    severity_number: intern(ATTR_SEVERITY_NUMBER),
    log_name: intern(ATTR_LOG_NAME),
    trace_id: intern(ATTR_TRACE_ID),
    span_id: intern(ATTR_SPAN_ID),
});

fn log_record(message: Value, body_format: BodyFormat) -> LogRecord {
    LogRecord {
        message,
        severity: None,
        body_format,
        trace: None,
        event_name: None,
        observed_timestamp: 0,
        dropped_attributes_count: 0,
    }
}

/// One `/raw` line as a log event.
pub(super) fn raw_line(message: Value, timestamp: i64) -> Event {
    Event::log(timestamp, AttrMap::new(), log_record(message, BodyFormat::Raw))
}

impl SplunkDecoder {
    pub(super) fn decode_log(
        &mut self,
        event: Json,
        fields: Map<String, Json>,
        timestamp: i64,
    ) -> Event {
        let body_format = match event {
            Json::Object(_) | Json::Array(_) => BodyFormat::Json,
            _ => BodyFormat::Raw,
        };
        let mut record = log_record(json_to_value(&event), body_format);
        let mut attributes = AttrMap::new();
        for (key, value) in &fields {
            flatten_into(key, &json_to_value(value), &mut attributes);
        }
        let keys = &*KEYS;
        record.severity = severity_of(&attributes);
        let name =
            attributes.get_sym(keys.log_name).and_then(Value::as_str).filter(|n| !n.is_empty());
        if let Some(name) = name.map(intern) {
            record.event_name = Some(name);
            attributes.remove_sym(keys.log_name);
        }
        record.trace = trace_of(&attributes);
        if record.trace.is_some() {
            attributes.remove_sym(keys.trace_id);
            attributes.remove_sym(keys.span_id);
        }
        Event::log(timestamp, attributes, record)
    }
}

/// OTLP's rule: the number's band, else a case-insensitive text match.
fn severity_of(attributes: &AttrMap) -> Option<Severity> {
    let keys = &*KEYS;
    let number = match attributes.get_sym(keys.severity_number) {
        Some(Value::I64(n)) => i32::try_from(*n).unwrap_or(0),
        Some(Value::U64(n)) => i32::try_from(*n).unwrap_or(0),
        _ => 0,
    };
    let text = attributes.get_sym(keys.severity_text).and_then(Value::as_str).unwrap_or("");
    decode_severity(number, text)
}

/// A `TraceRef` from a valid `trace_id` and a valid or absent `span_id`.
fn trace_of(attributes: &AttrMap) -> Option<TraceRef> {
    let keys = &*KEYS;
    let trace_id = parse_trace_id(attributes.get_sym(keys.trace_id)?.as_str()?)?;
    let span_id = match attributes.get_sym(keys.span_id) {
        None => None,
        Some(value) => Some(parse_span_id(value.as_str()?)?),
    };
    Some(TraceRef { trace_id, span_id, flags: 0 })
}

impl SplunkEncoder {
    pub(super) fn encode_log(
        &mut self,
        ctx: &BatchContext<'_>,
        event: &Event,
        log: &LogRecord,
        event_index: usize,
        out: &mut MessageBuf<ObjectMeta>,
    ) {
        let blank = match &log.message {
            Value::Null => true,
            Value::Str(s) => s.is_empty(),
            Value::F64(f) => !f.is_finite(),
            _ => false,
        };
        if blank {
            self.telemetry.count("logit.output.events.skipped", 1.0, &[("reason", "blank_event")]);
            self.diagnostics.warn_throttled(
                "blank_event",
                "log with an empty message not sent: HEC rejects a blank event",
            );
            return;
        }

        let keys = &*KEYS;
        let mut fields = SplunkEncoder::object_fields(ctx.resource, event);
        if log.message.as_str() == Some(METRIC_EVENT) {
            let reserved: Vec<Symbol> = fields
                .iter()
                .map(|(key, _)| key)
                .filter(|key| {
                    let key = resolve(*key);
                    key.starts_with("metric_name") || key == "_value"
                })
                .collect();
            self.reserved_key_dropped(reserved.len());
            for key in reserved {
                fields.remove_sym(key);
            }
        }
        if let Some(severity) = log.severity {
            let has_text = fields.get_sym(keys.severity_text).is_some();
            let has_number = fields.get_sym(keys.severity_number).is_some();
            if !has_text && !has_number {
                fields.insert_sym(keys.severity_text, Value::str(severity_text(severity)));
                fields.insert_sym(
                    keys.severity_number,
                    Value::I64(i64::from(severity_number(severity))),
                );
            }
        }
        if let Some(name) = log.event_name {
            if fields.get_sym(keys.log_name).is_none() {
                fields.insert_sym(keys.log_name, Value::str(resolve(name)));
            }
        }
        if let Some(trace) = &log.trace {
            // An attribute of either name wins and suppresses both, so ids from two sources are
            // never mixed on one object.
            if fields.get_sym(keys.trace_id).is_none() && fields.get_sym(keys.span_id).is_none() {
                fields.insert_sym(keys.trace_id, Value::str(to_hex(&trace.trace_id)));
                if let Some(span_id) = &trace.span_id {
                    fields.insert_sym(keys.span_id, Value::str(to_hex(span_id)));
                }
            }
        }

        self.scratch.clear();
        let mut obj = begin_object(&mut self.scratch, event.timestamp, &ctx.envelope);
        let slot = obj.key("event");
        match &log.message {
            Value::Str(_) | Value::Bytes(_) => write_str(slot, &value_text(&log.message)),
            Value::Timestamp(ns) => write_str(slot, &format_rfc3339_utc(*ns)),
            other => write_value(slot, other),
        }
        if !fields.is_empty() {
            write_fields(&mut obj, &fields, |_| {});
        }
        obj.finish();
        out.push_with(&self.scratch, ObjectMeta { event_index, signal: Signal::Logs, records: 1 });
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{counted, decode, encode, encoder, RECEIVED_AT};
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::{EventBatch, Resource};
    use std::sync::Arc;

    fn log(batches: &[EventBatch]) -> (&Event, &LogRecord) {
        let event = &batches[0].events[0];
        (event, event.log.as_ref().unwrap())
    }

    #[test]
    fn body_kinds_map_to_typed_messages() {
        let batches = decode(
            r#"{"event":"line"}{"event":{"a":[1,"x"]}}{"event":[true]}{"event":42}{"event":1.5}{"event":false}"#,
        );
        let got: Vec<(Value, BodyFormat)> = batches[0]
            .events
            .iter()
            .map(|e| {
                let log = e.log.as_ref().unwrap();
                (log.message.clone(), log.body_format)
            })
            .collect();
        let map = Value::Map(Box::new(AttrMap::from_iter([(
            "a",
            Value::Array(vec![Value::I64(1), Value::str("x")]),
        )])));
        assert_eq!(
            got,
            vec![
                (Value::str("line"), BodyFormat::Raw),
                (map, BodyFormat::Json),
                (Value::Array(vec![Value::Bool(true)]), BodyFormat::Json),
                (Value::I64(42), BodyFormat::Raw),
                (Value::F64(1.5), BodyFormat::Raw),
                (Value::Bool(false), BodyFormat::Raw),
            ]
        );
        assert!(batches[0].events.iter().all(|e| e.timestamp == RECEIVED_AT));
    }

    #[test]
    fn otel_fields_decode_into_typed_fields() {
        let batches = decode(
            r#"{"event":"m","time":1,"fields":{"otel.log.severity.text":"WARN","otel.log.severity.number":14,"otel.log.name":"app.start","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","k":"v"}}"#,
        );
        let (event, log) = log(&batches);
        assert_eq!(log.severity, Some(Severity::Warn));
        assert_eq!(log.event_name, Some(intern("app.start")));
        let trace = log.trace.unwrap();
        assert_eq!(to_hex(&trace.trace_id), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(trace.span_id.map(|s| to_hex(&s)), Some("b7ad6b7169203331".into()));
        assert_eq!(event.attributes.get(ATTR_SEVERITY_TEXT), Some(&Value::str("WARN")));
        assert_eq!(event.attributes.get(ATTR_SEVERITY_NUMBER), Some(&Value::I64(14)));
        assert_eq!(event.attributes.get(ATTR_LOG_NAME), None);
        assert_eq!(event.attributes.get(ATTR_TRACE_ID), None);
        assert_eq!(event.attributes.len(), 3);
    }

    #[test]
    fn an_invalid_trace_pair_stays_attributes() {
        for fields in [
            r#"{"trace_id":"xyz","span_id":"b7ad6b7169203331"}"#,
            r#"{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"0000000000000000"}"#,
            r#"{"span_id":"b7ad6b7169203331"}"#,
        ] {
            let batches = decode(&format!(r#"{{"event":"m","fields":{fields}}}"#));
            let (event, log) = log(&batches);
            assert_eq!(log.trace, None, "{fields}");
            assert!(event.attributes.get(ATTR_SPAN_ID).is_some(), "{fields}");
        }
        let batches =
            decode(r#"{"event":"m","fields":{"trace_id":"0af7651916cd43dd8448eb211c80319c"}}"#);
        let (event, log) = log(&batches);
        assert_eq!(log.trace.unwrap().span_id, None);
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn nested_fields_flatten_to_dotted_keys() {
        let batches = decode(r#"{"event":"m","fields":{"a":{"b":{"c":1}},"e":{},"l":[{"x":1}]}}"#);
        let (event, _) = log(&batches);
        assert_eq!(event.attributes.get("a.b.c"), Some(&Value::I64(1)));
        assert_eq!(event.attributes.get("l"), Some(&Value::str(r#"[{"x":1}]"#)));
        assert_eq!(event.attributes.len(), 2);
    }

    #[test]
    fn encode_writes_the_otel_exporter_shape() {
        let mut resource = Resource::default();
        resource.attributes.insert("host.name", Value::str("web-1"));
        resource.attributes.insert("com.splunk.sourcetype", Value::str("otel"));
        resource.attributes.insert("service.name", Value::str("cart"));
        let mut attrs = AttrMap::new();
        attrs.insert(
            "http",
            Value::Map(Box::new(AttrMap::from_iter([("method", Value::str("GET"))]))),
        );
        let record = LogRecord {
            message: Value::str("hello"),
            severity: Some(Severity::Info),
            body_format: BodyFormat::Raw,
            trace: Some(TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 1 }),
            event_name: Some(intern("login")),
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        };
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![Event::log(1_700_000_000_250_000_000, attrs, record)],
        };
        let body = encode(std::slice::from_ref(&batch));
        let json: Json = serde_json::from_str(&body).unwrap();
        assert_eq!(json["time"], serde_json::json!(1_700_000_000.25));
        assert_eq!(json["host"], "web-1");
        assert_eq!(json["sourcetype"], "otel");
        assert_eq!(json["event"], "hello");
        let fields = &json["fields"];
        assert_eq!(fields["service.name"], "cart");
        assert_eq!(fields["http.method"], "GET");
        assert_eq!(fields["otel.log.severity.text"], "Info");
        assert_eq!(fields["otel.log.severity.number"], 9);
        assert_eq!(fields["otel.log.name"], "login");
        assert_eq!(fields["trace_id"], "abababababababababababababababab");
        assert_eq!(fields["span_id"], "cdcdcdcdcdcdcdcd");
        assert!(body.starts_with(
            r#"{"time":1700000000.25,"host":"web-1","sourcetype":"otel","event":"hello","fields":{"#
        ));
    }

    #[test]
    fn blank_messages_are_skipped_and_counted() {
        let registry = Registry::new();
        let events = [Value::Null, Value::str(""), Value::F64(f64::NAN)]
            .into_iter()
            .map(|message| Event::log(0, AttrMap::new(), log_record(message, BodyFormat::Raw)))
            .collect();
        let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
        let mut out = MessageBuf::default();
        encoder(&registry).encode_objects(&batch, &mut out);
        assert!(out.is_empty());
        assert_eq!(
            counted(&registry, "logit.output.events.skipped", ("reason", "blank_event")),
            3.0
        );
    }

    #[test]
    fn a_log_saying_metric_drops_metric_fields() {
        let registry = Registry::new();
        let mut attrs = AttrMap::new();
        attrs.insert("metric_name:cpu", Value::F64(1.0));
        attrs.insert("keep", Value::str("x"));
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::log(0, attrs, log_record(Value::str("metric"), BodyFormat::Raw))],
        };
        let mut out = MessageBuf::default();
        encoder(&registry).encode_objects(&batch, &mut out);
        assert_eq!(
            out.iter().next().unwrap(),
            br#"{"time":0,"event":"metric","fields":{"keep":"x"}}"#
        );
        assert_eq!(
            counted(&registry, "logit.output.tags.dropped", ("reason", "reserved_key")),
            1.0
        );
    }

    #[test]
    fn a_bytes_message_leaves_as_text_and_severity_attributes_win() {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_SEVERITY_NUMBER, Value::I64(10));
        let mut record =
            log_record(Value::Bytes(bytes::Bytes::from_static(b"raw")), BodyFormat::Raw);
        record.severity = Some(Severity::Info);
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::log(0, attrs, record)],
        };
        assert_eq!(
            encode(&[batch]),
            r#"{"time":0,"event":"raw","fields":{"otel.log.severity.number":10}}"#
        );
    }
}
