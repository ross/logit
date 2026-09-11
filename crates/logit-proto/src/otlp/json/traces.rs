//! `TracesData` from OTLP/JSON. See `super`'s module doc for the dialect rules this leans on.

use super::{
    array_field, enum_field, get, hex_bytes, instrumentation_scope, key_values, malformed,
    object_field, require_object, resource, str_field, u32_field, u64_field, JsonValue,
};
use crate::otlp::generated::opentelemetry::proto::trace::v1 as pb;
use crate::CodecError;

pub(crate) fn traces_data(bytes: &[u8]) -> Result<pb::TracesData, CodecError> {
    let root: JsonValue = serde_json::from_slice(bytes).map_err(|e| malformed(e.to_string()))?;
    let obj = require_object(&root, "the request body")?;
    let resource_spans = array_field(obj, "resourceSpans", "resource_spans")?
        .iter()
        .map(resource_spans)
        .collect::<Result<_, _>>()?;
    Ok(pb::TracesData { resource_spans })
}

fn resource_spans(v: &JsonValue) -> Result<pb::ResourceSpans, CodecError> {
    let obj = require_object(v, "a resourceSpans entry")?;
    let resource = match object_field(obj, "resource", "resource")? {
        Some(r) => Some(resource(r)?),
        None => None,
    };
    let scope_spans = array_field(obj, "scopeSpans", "scope_spans")?
        .iter()
        .map(scope_spans)
        .collect::<Result<_, _>>()?;
    Ok(pb::ResourceSpans {
        resource,
        scope_spans,
        schema_url: str_field(obj, "schemaUrl", "schema_url")?,
    })
}

fn scope_spans(v: &JsonValue) -> Result<pb::ScopeSpans, CodecError> {
    let obj = require_object(v, "a scopeSpans entry")?;
    let scope = instrumentation_scope(get(obj, "scope", "scope"))?;
    let spans = array_field(obj, "spans", "spans")?.iter().map(span).collect::<Result<_, _>>()?;
    Ok(pb::ScopeSpans { scope, spans, schema_url: str_field(obj, "schemaUrl", "schema_url")? })
}

fn span(v: &JsonValue) -> Result<pb::Span, CodecError> {
    let obj = require_object(v, "a span")?;
    let trace_id = match get(obj, "traceId", "trace_id") {
        Some(x) => hex_bytes(x, 16, "traceId")?,
        None => Vec::new(),
    };
    let span_id = match get(obj, "spanId", "span_id") {
        Some(x) => hex_bytes(x, 8, "spanId")?,
        None => Vec::new(),
    };
    let parent_span_id = match get(obj, "parentSpanId", "parent_span_id") {
        Some(x) => hex_bytes(x, 8, "parentSpanId")?,
        None => Vec::new(),
    };
    let kind = enum_field(obj, "kind", "kind", |s| {
        pb::span::SpanKind::from_str_name(s).map(|k| k as i32)
    })?;
    let events =
        array_field(obj, "events", "events")?.iter().map(span_event).collect::<Result<_, _>>()?;
    let links =
        array_field(obj, "links", "links")?.iter().map(span_link).collect::<Result<_, _>>()?;
    let status = match object_field(obj, "status", "status")? {
        Some(s) => Some(status(s)?),
        None => None,
    };
    Ok(pb::Span {
        trace_id,
        span_id,
        trace_state: str_field(obj, "traceState", "trace_state")?,
        parent_span_id,
        flags: u32_field(obj, "flags", "flags")?,
        name: str_field(obj, "name", "name")?,
        kind,
        start_time_unix_nano: u64_field(obj, "startTimeUnixNano", "start_time_unix_nano")?,
        end_time_unix_nano: u64_field(obj, "endTimeUnixNano", "end_time_unix_nano")?,
        attributes: key_values(obj, "attributes", "attributes")?,
        dropped_attributes_count: 0,
        events,
        dropped_events_count: 0,
        links,
        dropped_links_count: 0,
        status,
    })
}

fn span_event(v: &JsonValue) -> Result<pb::span::Event, CodecError> {
    let obj = require_object(v, "a span event")?;
    Ok(pb::span::Event {
        time_unix_nano: u64_field(obj, "timeUnixNano", "time_unix_nano")?,
        name: str_field(obj, "name", "name")?,
        attributes: key_values(obj, "attributes", "attributes")?,
        dropped_attributes_count: 0,
    })
}

fn span_link(v: &JsonValue) -> Result<pb::span::Link, CodecError> {
    let obj = require_object(v, "a span link")?;
    let trace_id = match get(obj, "traceId", "trace_id") {
        Some(x) => hex_bytes(x, 16, "traceId")?,
        None => Vec::new(),
    };
    let span_id = match get(obj, "spanId", "span_id") {
        Some(x) => hex_bytes(x, 8, "spanId")?,
        None => Vec::new(),
    };
    Ok(pb::span::Link {
        trace_id,
        span_id,
        trace_state: str_field(obj, "traceState", "trace_state")?,
        attributes: key_values(obj, "attributes", "attributes")?,
        dropped_attributes_count: 0,
        flags: u32_field(obj, "flags", "flags")?,
    })
}

fn status(obj: &super::JsonMap) -> Result<pb::Status, CodecError> {
    Ok(pb::Status {
        message: str_field(obj, "message", "message")?,
        code: enum_field(obj, "code", "code", |s| {
            pb::status::StatusCode::from_str_name(s).map(|c| c as i32)
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minimal_span_decodes() {
        let json = br#"{
            "resourceSpans": [{
                "resource": {"attributes": [{"key": "host", "value": {"stringValue": "a"}}]},
                "scopeSpans": [{
                    "scope": {"name": "test"},
                    "spans": [{
                        "traceId": "0102030405060708090a0b0c0d0e0f10",
                        "spanId": "0102030405060708",
                        "name": "op",
                        "kind": "SPAN_KIND_SERVER",
                        "startTimeUnixNano": "1",
                        "endTimeUnixNano": "2",
                        "status": {"code": "STATUS_CODE_OK"}
                    }]
                }]
            }]
        }"#;
        let data = traces_data(json).expect("should decode");
        assert_eq!(data.resource_spans.len(), 1);
        let span = &data.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.trace_id, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        assert_eq!(span.span_id, vec![1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(span.kind, pb::span::SpanKind::Server as i32);
        assert_eq!(span.status.as_ref().unwrap().code, pb::status::StatusCode::Ok as i32);
    }

    #[test]
    fn a_short_trace_id_is_rejected_with_a_message_naming_the_field() {
        let json = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{"traceId": "0102"}]}]}]}"#;
        let err = traces_data(json).unwrap_err().to_string();
        assert!(err.contains("traceId"), "error should name the field, got: {err}");
    }

    #[test]
    fn an_absent_parent_span_id_decodes_as_no_parent() {
        let json = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": "0102030405060708090a0b0c0d0e0f10",
            "spanId": "0102030405060708"
        }]}]}]}"#;
        let data = traces_data(json).expect("should decode");
        assert!(data.resource_spans[0].scope_spans[0].spans[0].parent_span_id.is_empty());
    }

    #[test]
    fn a_null_parent_span_id_decodes_as_no_parent_the_same_as_an_absent_one() {
        let null_parent = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": "0102030405060708090a0b0c0d0e0f10",
            "spanId": "0102030405060708",
            "parentSpanId": null
        }]}]}]}"#;
        let absent_parent = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": "0102030405060708090a0b0c0d0e0f10",
            "spanId": "0102030405060708"
        }]}]}]}"#;
        let with_null =
            traces_data(null_parent).expect("a null parentSpanId must not fail the batch");
        assert!(with_null.resource_spans[0].scope_spans[0].spans[0].parent_span_id.is_empty());
        assert_eq!(with_null, traces_data(absent_parent).unwrap());
    }

    #[test]
    fn a_null_trace_id_decodes_the_same_as_an_absent_one_and_is_left_to_downstream_to_reject() {
        let with_null = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": null, "spanId": null, "name": "op"
        }]}]}]}"#;
        let absent = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{"name": "op"}]}]}]}"#;
        let a = traces_data(with_null).expect("a null id must not fail this layer");
        assert!(a.resource_spans[0].scope_spans[0].spans[0].trace_id.is_empty());
        assert!(a.resource_spans[0].scope_spans[0].spans[0].span_id.is_empty());
        assert_eq!(a, traces_data(absent).unwrap());
    }

    #[test]
    fn a_null_trace_id_or_span_id_on_a_span_link_decodes_as_no_linked_span() {
        let json = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": "0102030405060708090a0b0c0d0e0f10",
            "spanId": "0102030405060708",
            "links": [{"traceId": null, "spanId": null}]
        }]}]}]}"#;
        let data = traces_data(json).expect("a null link id must not fail the batch");
        let link = &data.resource_spans[0].scope_spans[0].spans[0].links[0];
        assert!(link.trace_id.is_empty());
        assert!(link.span_id.is_empty());
    }

    #[test]
    fn snake_case_and_camel_case_agree() {
        let camel = br#"{"resourceSpans": [{"scopeSpans": [{"spans": [{
            "traceId": "0102030405060708090a0b0c0d0e0f10", "spanId": "0102030405060708",
            "startTimeUnixNano": "5"
        }]}]}]}"#;
        let snake = br#"{"resource_spans": [{"scope_spans": [{"spans": [{
            "trace_id": "0102030405060708090a0b0c0d0e0f10", "span_id": "0102030405060708",
            "start_time_unix_nano": "5"
        }]}]}]}"#;
        let a = traces_data(camel).unwrap();
        let b = traces_data(snake).unwrap();
        assert_eq!(a, b);
    }
}
