//! HEC span events, both directions, in the shape the OpenTelemetry Collector's `splunk_hec`
//! exporter writes: `event` is a span object, `time` its start in seconds, and `fields` the span's
//! resource attributes. The object's members follow the exporter's `hecSpan` struct order, and
//! `kind` and `status.code` take the enum names it writes (`SPAN_KIND_SERVER`,
//! `STATUS_CODE_UNSET`), both as recorded from Collector contrib 0.161.0
//! (`testdata/interop/splunk/otel-services-collector-000.bin`).
//!
//! ## Decode
//!
//! An `event` object is a span when it carries a non-zero 32-hex `trace_id`, a non-zero 16-hex
//! `span_id`, and integer `start_time` and `end_time`. Any other member that doesn't parse, or a
//! member the exporter never writes, makes the whole object a log with a `Map` body instead,
//! counted `logit.input.spans.degraded{reason="malformed_span"}`.
//!
//! | Wire | Model | Counter |
//! |---|---|---|
//! | `trace_id`, `span_id` | `SpanRecord::trace_id`, `span_id` | -- |
//! | `parent_span_id`: 16 hex, or `""` / `null` | `parent_span_id`: `Some`, or `None` | -- |
//! | `start_time`, `end_time` (Unix ns) | `Event::timestamp`, `end_timestamp`; the envelope `time` is ignored | -- |
//! | `name` string, or absent | `name` `Str`, `""` when absent | -- |
//! | `kind`: `Unspecified`/`Internal`/`Server`/`Client`/`Producer`/`Consumer`, the `SPAN_KIND_*` names, or `0` to `5` | `SpanKind`; unspecified → `Internal` | -- |
//! | `status.code`: `Unset`/`Ok`/`Error`, the `STATUS_CODE_*` names, or `0` to `2`; absent → `Unset` | `SpanStatus` | -- |
//! | `status.message`, non-empty | `ext.status_message` | -- |
//! | `attributes` object | event attributes, nesting kept | -- |
//! | `events[]`: `{attributes, name, timestamp}` | `SpanEvent`s | -- |
//! | `links[]`: `{attributes, trace_id, span_id, trace_state}` | `SpanLink`s, `trace_state` `""` → `None` | -- |
//! | envelope `fields` | the batch **resource**'s attributes, beside the carriers; a field spelled like a carrier the envelope also sets loses to it | `logit.input.spans.degraded{reason="carrier_collision"}` |
//!
//! ## Encode
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Event::timestamp` | envelope `time` (seconds) and `start_time` (ns) | -- |
//! | the ids | lowercase hex; no parent → `parent_span_id` `""` | -- |
//! | `name` | a string (a non-`Str` name as its text) | -- |
//! | event attributes | `attributes`, omitted when empty | -- |
//! | `kind`, `status` | the exporter's enum names (`SPAN_KIND_*`, `STATUS_CODE_*`); `status` always written, `{"message":"","code":"STATUS_CODE_UNSET"}` at its emptiest | -- |
//! | `events`, `links` | omitted when empty | -- |
//! | the resource's non-carrier attributes | `fields`, flattened | -- |
//! | `flags`, `ext.trace_state`, the dropped counts, a link's `flags` or dropped count, an event's dropped count | nothing: the exporter's span object has no field for them | `logit.output.spans.degraded{reason="no_wire_form"}`, once per span |

use super::{begin_object, write_fields, BatchContext, ObjectMeta, SplunkDecoder, SplunkEncoder};
use crate::json::{
    flatten_into, json_to_value, value_text, write_i64, write_str, write_value, JsonObject,
};
use crate::{MessageBuf, Signal};
use logit_core::interner::resolve;
use logit_core::trace::{parse_span_id, parse_trace_id, to_hex};
use logit_core::{
    AttrMap, Event, SpanEvent, SpanExt, SpanKind, SpanLink, SpanRecord, SpanStatus, Value,
};
use serde_json::{Map, Value as Json};

/// Whether `object` carries the four members that make it a span.
pub(super) fn is_span(object: &Map<String, Json>) -> bool {
    let hex = |key: &str| object.get(key).and_then(Json::as_str);
    hex("trace_id").and_then(parse_trace_id).is_some()
        && hex("span_id").and_then(parse_span_id).is_some()
        && object.get("start_time").and_then(Json::as_i64).is_some()
        && object.get("end_time").and_then(Json::as_i64).is_some()
}

impl SplunkDecoder {
    /// `None` when a member doesn't parse; the caller keeps the object as a log.
    pub(super) fn decode_span(&mut self, object: &Map<String, Json>) -> Option<Event> {
        let mut record = SpanRecord {
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: None,
            name: Value::str(""),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 0,
            flags: 0,
            ext: None,
        };
        let mut start = 0;
        let mut status_message = None;
        let mut attributes = AttrMap::new();
        for (key, value) in object {
            match key.as_str() {
                "trace_id" => record.trace_id = parse_trace_id(value.as_str()?)?,
                "span_id" => record.span_id = parse_span_id(value.as_str()?)?,
                "parent_span_id" => record.parent_span_id = parent_span_id(value)?,
                "name" => record.name = Value::str(value.as_str()?),
                "kind" => record.kind = span_kind(value)?,
                "status" => {
                    let (status, message) = span_status(value)?;
                    record.status = status;
                    status_message = message;
                }
                "start_time" => start = value.as_i64()?,
                "end_time" => record.end_timestamp = value.as_i64()?,
                "attributes" => attributes = attribute_map(value)?,
                "events" => record.events = span_events(value)?,
                "links" => record.links = span_links(value)?,
                _ => return None,
            }
        }
        if let Some(message) = status_message {
            record.ext =
                Some(Box::new(SpanExt { status_message: Some(message), ..SpanExt::default() }));
        }
        Some(Event::span(start, attributes, record))
    }
}

fn parent_span_id(value: &Json) -> Option<Option<[u8; 8]>> {
    match value {
        Json::Null => Some(None),
        Json::String(s) if s.is_empty() => Some(None),
        Json::String(s) => parse_span_id(s).map(Some),
        _ => None,
    }
}

fn span_kind(value: &Json) -> Option<SpanKind> {
    let kind = match value {
        Json::String(s) => match s.as_str() {
            "Unspecified" | "Internal" | "SPAN_KIND_UNSPECIFIED" | "SPAN_KIND_INTERNAL" => {
                SpanKind::Internal
            }
            "Server" | "SPAN_KIND_SERVER" => SpanKind::Server,
            "Client" | "SPAN_KIND_CLIENT" => SpanKind::Client,
            "Producer" | "SPAN_KIND_PRODUCER" => SpanKind::Producer,
            "Consumer" | "SPAN_KIND_CONSUMER" => SpanKind::Consumer,
            _ => return None,
        },
        Json::Number(n) => match n.as_u64()? {
            0 | 1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => return None,
        },
        _ => return None,
    };
    Some(kind)
}

fn span_kind_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "SPAN_KIND_INTERNAL",
        SpanKind::Server => "SPAN_KIND_SERVER",
        SpanKind::Client => "SPAN_KIND_CLIENT",
        SpanKind::Producer => "SPAN_KIND_PRODUCER",
        SpanKind::Consumer => "SPAN_KIND_CONSUMER",
    }
}

type StatusAndMessage = (SpanStatus, Option<bytes::Bytes>);

fn span_status(value: &Json) -> Option<StatusAndMessage> {
    let object = match value {
        Json::Null => return Some((SpanStatus::Unset, None)),
        Json::Object(object) => object,
        _ => return None,
    };
    let mut status = SpanStatus::Unset;
    let mut message = None;
    for (key, value) in object {
        match key.as_str() {
            "code" => {
                status = match value {
                    Json::String(s) => match s.as_str() {
                        "Unset" | "STATUS_CODE_UNSET" => SpanStatus::Unset,
                        "Ok" | "STATUS_CODE_OK" => SpanStatus::Ok,
                        "Error" | "STATUS_CODE_ERROR" => SpanStatus::Error,
                        _ => return None,
                    },
                    Json::Number(n) => match n.as_u64()? {
                        0 => SpanStatus::Unset,
                        1 => SpanStatus::Ok,
                        2 => SpanStatus::Error,
                        _ => return None,
                    },
                    _ => return None,
                }
            }
            "message" => {
                let text = value.as_str()?;
                message = (!text.is_empty()).then(|| bytes::Bytes::from(text.to_string()));
            }
            _ => return None,
        }
    }
    Some((status, message))
}

fn span_status_name(status: SpanStatus) -> &'static str {
    match status {
        SpanStatus::Unset => "STATUS_CODE_UNSET",
        SpanStatus::Ok => "STATUS_CODE_OK",
        SpanStatus::Error => "STATUS_CODE_ERROR",
    }
}

/// An `attributes` member: an object (nesting kept), or `null` for none.
fn attribute_map(value: &Json) -> Option<AttrMap> {
    let mut attributes = AttrMap::new();
    match value {
        Json::Null => {}
        Json::Object(object) => {
            for (key, value) in object {
                attributes.insert(key, json_to_value(value));
            }
        }
        _ => return None,
    }
    Some(attributes)
}

fn items(value: &Json) -> Option<&[Json]> {
    match value {
        Json::Null => Some(&[]),
        Json::Array(items) => Some(items),
        _ => None,
    }
}

fn span_events(value: &Json) -> Option<Vec<SpanEvent>> {
    let mut events = Vec::new();
    for item in items(value)? {
        let mut event = SpanEvent {
            timestamp: 0,
            name: Value::str(""),
            attributes: AttrMap::new(),
            dropped_attributes_count: 0,
        };
        for (key, value) in item.as_object()? {
            match key.as_str() {
                "attributes" => event.attributes = attribute_map(value)?,
                "name" => event.name = Value::str(value.as_str()?),
                "timestamp" => event.timestamp = value.as_i64()?,
                _ => return None,
            }
        }
        events.push(event);
    }
    Some(events)
}

fn span_links(value: &Json) -> Option<Vec<SpanLink>> {
    let mut links = Vec::new();
    for item in items(value)? {
        let mut link = SpanLink {
            trace_id: [0; 16],
            span_id: [0; 8],
            attributes: AttrMap::new(),
            flags: 0,
            trace_state: None,
            dropped_attributes_count: 0,
        };
        let object = item.as_object()?;
        for (key, value) in object {
            match key.as_str() {
                "attributes" => link.attributes = attribute_map(value)?,
                "trace_id" => link.trace_id = parse_trace_id(value.as_str()?)?,
                "span_id" => link.span_id = parse_span_id(value.as_str()?)?,
                "trace_state" => {
                    let state = value.as_str()?;
                    link.trace_state =
                        (!state.is_empty()).then(|| bytes::Bytes::from(state.to_string()));
                }
                _ => return None,
            }
        }
        if !object.contains_key("trace_id") || !object.contains_key("span_id") {
            return None;
        }
        links.push(link);
    }
    Some(links)
}

/// Whether the span carries anything the exporter's span object has no field for.
fn has_unwritable_fields(span: &SpanRecord) -> bool {
    let ext = span.ext.as_deref().is_some_and(|ext| {
        ext.trace_state.is_some()
            || ext.dropped_attributes_count != 0
            || ext.dropped_events_count != 0
            || ext.dropped_links_count != 0
    });
    span.flags != 0
        || ext
        || span.events.iter().any(|e| e.dropped_attributes_count != 0)
        || span.links.iter().any(|l| l.flags != 0 || l.dropped_attributes_count != 0)
}

fn write_attributes(obj: &mut JsonObject<'_>, attributes: &AttrMap) {
    if attributes.is_empty() {
        return;
    }
    let mut inner = JsonObject::begin(obj.key("attributes"));
    for (key, value) in attributes.iter() {
        write_value(inner.key(resolve(key)), value);
    }
    inner.finish();
}

impl SplunkEncoder {
    pub(super) fn encode_span(
        &mut self,
        ctx: &BatchContext<'_>,
        event: &Event,
        span: &SpanRecord,
        event_index: usize,
        out: &mut MessageBuf<ObjectMeta>,
    ) {
        if has_unwritable_fields(span) {
            self.telemetry.count("logit.output.spans.degraded", 1.0, &[("reason", "no_wire_form")]);
        }
        let mut fields = AttrMap::new();
        for (key, value) in ctx.resource.attributes.iter() {
            if !super::is_carrier(key) {
                flatten_into(resolve(key), value, &mut fields);
            }
        }

        self.scratch.clear();
        let mut obj = begin_object(&mut self.scratch, event.timestamp, &ctx.envelope);
        {
            let mut hec = JsonObject::begin(obj.key("event"));
            write_str(hec.key("trace_id"), &to_hex(&span.trace_id));
            write_str(hec.key("span_id"), &to_hex(&span.span_id));
            let parent = span.parent_span_id.map(|p| to_hex(&p)).unwrap_or_default();
            write_str(hec.key("parent_span_id"), &parent);
            write_str(hec.key("name"), &value_text(&span.name));
            write_attributes(&mut hec, &event.attributes);
            write_i64(hec.key("end_time"), span.end_timestamp);
            write_str(hec.key("kind"), span_kind_name(span.kind));
            {
                let mut status = JsonObject::begin(hec.key("status"));
                let message = span
                    .ext
                    .as_deref()
                    .and_then(|ext| ext.status_message.as_ref())
                    .map(|m| String::from_utf8_lossy(m).into_owned())
                    .unwrap_or_default();
                write_str(status.key("message"), &message);
                write_str(status.key("code"), span_status_name(span.status));
                status.finish();
            }
            write_i64(hec.key("start_time"), event.timestamp);
            if !span.events.is_empty() {
                let slot = hec.key("events");
                slot.push(b'[');
                for (i, span_event) in span.events.iter().enumerate() {
                    if i > 0 {
                        slot.push(b',');
                    }
                    let mut item = JsonObject::begin(slot);
                    write_attributes(&mut item, &span_event.attributes);
                    write_str(item.key("name"), &value_text(&span_event.name));
                    write_i64(item.key("timestamp"), span_event.timestamp);
                    item.finish();
                }
                slot.push(b']');
            }
            if !span.links.is_empty() {
                let slot = hec.key("links");
                slot.push(b'[');
                for (i, link) in span.links.iter().enumerate() {
                    if i > 0 {
                        slot.push(b',');
                    }
                    let mut item = JsonObject::begin(slot);
                    write_attributes(&mut item, &link.attributes);
                    write_str(item.key("trace_id"), &to_hex(&link.trace_id));
                    write_str(item.key("span_id"), &to_hex(&link.span_id));
                    let state = link
                        .trace_state
                        .as_ref()
                        .map(|s| String::from_utf8_lossy(s).into_owned())
                        .unwrap_or_default();
                    write_str(item.key("trace_state"), &state);
                    item.finish();
                }
                slot.push(b']');
            }
            hec.finish();
        }
        if !fields.is_empty() {
            write_fields(&mut obj, &fields, |_| {});
        }
        obj.finish();
        out.push_with(
            &self.scratch,
            ObjectMeta { event_index, signal: Signal::Traces, records: 1 },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{counted, decode, decoder, encode, encoder, RECEIVED_AT};
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::{EventBatch, Resource};
    use std::sync::Arc;

    const SPAN: &str = r#"{"time":1700000000.5,"host":"web-1","sourcetype":"otel","event":{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","parent_span_id":"","name":"GET /","attributes":{"http.method":"GET","n":{"deep":1}},"end_time":1700000000750000000,"kind":"SPAN_KIND_SERVER","status":{"message":"boom","code":"STATUS_CODE_ERROR"},"start_time":1700000000500000000,"events":[{"attributes":{"k":"v"},"name":"retry","timestamp":1700000000600000000}],"links":[{"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7","trace_state":"a=b"}]},"fields":{"service.name":"cart"}}"#;

    #[test]
    fn the_exporter_span_shape_decodes_to_a_span() {
        let batches = decode(SPAN);
        let batch = &batches[0];
        assert_eq!(batch.resource.attributes.get("service.name"), Some(&Value::str("cart")));
        assert_eq!(batch.resource.attributes.get("host.name"), Some(&Value::str("web-1")));
        let event = &batch.events[0];
        assert_eq!(event.timestamp, 1_700_000_000_500_000_000);
        assert_eq!(event.attributes.get("http.method"), Some(&Value::str("GET")));
        assert!(matches!(event.attributes.get("n"), Some(Value::Map(_))), "nesting kept");
        let span = event.span.as_ref().unwrap();
        assert_eq!(to_hex(&span.trace_id), "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(span.parent_span_id, None);
        assert_eq!(span.kind, SpanKind::Server);
        assert_eq!(span.status, SpanStatus::Error);
        assert_eq!(span.ext.as_ref().unwrap().status_message.as_deref(), Some(&b"boom"[..]));
        assert_eq!(span.end_timestamp, 1_700_000_000_750_000_000);
        assert_eq!(span.events[0].name, Value::str("retry"));
        assert_eq!(span.links[0].trace_state.as_deref(), Some(&b"a=b"[..]));
        // Attribute keys leave in interning order, so compare the JSON, not the bytes.
        let once = encode(&batches);
        let json = |s: &str| serde_json::from_str::<Json>(s).unwrap();
        assert_eq!(json(&once), json(SPAN), "the exporter's object comes back member for member");
        assert_eq!(decode(&once), batches);
    }

    #[test]
    fn kind_and_status_spellings_normalize() {
        for (kind, want) in [
            (r#""SPAN_KIND_CLIENT""#, SpanKind::Client),
            ("4", SpanKind::Producer),
            (r#""Unspecified""#, SpanKind::Internal),
            ("0", SpanKind::Internal),
        ] {
            let body = format!(
                r#"{{"event":{{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","start_time":1,"end_time":2,"kind":{kind},"status":{{"code":1}}}}}}"#
            );
            let batches = decode(&body);
            let span = batches[0].events[0].span.as_ref().unwrap();
            assert_eq!(span.kind, want, "{kind}");
            assert_eq!(span.status, SpanStatus::Ok);
        }
    }

    #[test]
    fn a_malformed_span_falls_back_to_a_map_log() {
        let registry = Registry::new();
        for bad in [
            r#""kind":"Sideways""#,
            r#""parent_span_id":"xyz""#,
            r#""extra":1"#,
            r#""links":[{"trace_id":"0af7651916cd43dd8448eb211c80319c"}]"#,
        ] {
            let body = format!(
                r#"{{"event":{{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","start_time":1,"end_time":2,{bad}}}}}"#
            );
            let batches = decoder(&registry).decode_events(body.as_bytes(), RECEIVED_AT).unwrap();
            let event = &batches[0].events[0];
            assert!(event.span.is_none(), "{bad}");
            assert!(matches!(event.log.as_ref().unwrap().message, Value::Map(_)), "{bad}");
        }
        assert_eq!(
            counted(&registry, "logit.input.spans.degraded", ("reason", "malformed_span")),
            4.0
        );

        // Missing a detection member: an ordinary log, not counted.
        let batches =
            decode(r#"{"event":{"trace_id":"0af7651916cd43dd8448eb211c80319c","start_time":1}}"#);
        assert!(batches[0].events[0].log.is_some());
    }

    #[test]
    fn a_field_spelled_like_a_carrier_loses_to_the_envelope() {
        let registry = Registry::new();
        let body = r#"{"host":"env","event":{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","start_time":1,"end_time":2},"fields":{"host.name":"field","com.splunk.index":"i"}}"#;
        let batches = decoder(&registry).decode_events(body.as_bytes(), RECEIVED_AT).unwrap();
        let attrs = &batches[0].resource.attributes;
        assert_eq!(attrs.get("host.name"), Some(&Value::str("env")));
        assert_eq!(attrs.get("com.splunk.index"), Some(&Value::str("i")), "no envelope index");
        assert_eq!(
            counted(&registry, "logit.input.spans.degraded", ("reason", "carrier_collision")),
            1.0
        );
    }

    #[test]
    fn unwritable_span_fields_are_counted_once() {
        let registry = Registry::new();
        let mut batch = decode(SPAN).remove(0);
        let span = batch.events[0].span.as_mut().unwrap();
        span.flags = 1;
        span.links[0].flags = 1;
        let batch = EventBatch { resource: Arc::new(Resource::default()), ..batch };
        let mut out = MessageBuf::default();
        encoder(&registry).encode_objects(&batch, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(
            counted(&registry, "logit.output.spans.degraded", ("reason", "no_wire_form")),
            1.0
        );
    }
}
