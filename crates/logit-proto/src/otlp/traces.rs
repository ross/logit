//! `Event`/`SpanRecord` ↔ OTLP `Span` -- near-total.
//!
//! `start_time_unix_nano`/`end_time_unix_nano` map directly to `Event::timestamp`/
//! `SpanRecord::end_timestamp`. `SpanKind`/`SpanStatus` map directly (OTLP's
//! `SPAN_KIND_UNSPECIFIED` decodes to `Internal`, matching the OTLP spec's own recommendation).
//! `parent_span_id: Option<[u8; 8]>` ⇔ OTLP's empty-bytes-means-none convention. `SpanEvent`/
//! `SpanLink` are direct.
//!
//! **`Status.message` has no field on `SpanRecord`** (only the `SpanStatus` enum does) -- it
//! round-trips through an `otel.status_message` attribute, inserted on decode and consumed
//! (removed, so it isn't duplicated as a plain attribute too) on encode. Same idiom `logs.rs` uses
//! for `BodyFormat`.
//!
//! **Dropped, documented, not errors:** `trace_state`, `flags` (on both `Span` and `Span.Link`),
//! `dropped_attributes_count`, `dropped_events_count`, `dropped_links_count`.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::trace::v1 as pb;
use crate::CodecError;
use logit_core::{AttrMap, Event, SpanEvent, SpanKind, SpanLink, SpanRecord, SpanStatus, Value};

fn encode_span_kind(kind: SpanKind) -> pb::span::SpanKind {
    match kind {
        SpanKind::Internal => pb::span::SpanKind::Internal,
        SpanKind::Server => pb::span::SpanKind::Server,
        SpanKind::Client => pb::span::SpanKind::Client,
        SpanKind::Producer => pb::span::SpanKind::Producer,
        SpanKind::Consumer => pb::span::SpanKind::Consumer,
    }
}

/// OTLP's `SPAN_KIND_UNSPECIFIED` (and any value this build of the enum doesn't recognize) decodes
/// to `Internal`, per the OTLP spec's own recommendation for readers.
fn decode_span_kind(raw: i32) -> SpanKind {
    match pb::span::SpanKind::try_from(raw).unwrap_or(pb::span::SpanKind::Unspecified) {
        pb::span::SpanKind::Unspecified | pb::span::SpanKind::Internal => SpanKind::Internal,
        pb::span::SpanKind::Server => SpanKind::Server,
        pb::span::SpanKind::Client => SpanKind::Client,
        pb::span::SpanKind::Producer => SpanKind::Producer,
        pb::span::SpanKind::Consumer => SpanKind::Consumer,
    }
}

fn encode_status_code(status: SpanStatus) -> pb::status::StatusCode {
    match status {
        SpanStatus::Unset => pb::status::StatusCode::Unset,
        SpanStatus::Ok => pb::status::StatusCode::Ok,
        SpanStatus::Error => pb::status::StatusCode::Error,
    }
}

fn decode_status_code(raw: i32) -> SpanStatus {
    match pb::status::StatusCode::try_from(raw).unwrap_or(pb::status::StatusCode::Unset) {
        pb::status::StatusCode::Unset => SpanStatus::Unset,
        pb::status::StatusCode::Ok => SpanStatus::Ok,
        pb::status::StatusCode::Error => SpanStatus::Error,
    }
}

fn encode_span_event(event: &SpanEvent) -> pb::span::Event {
    pb::span::Event {
        time_unix_nano: event.timestamp.max(0) as u64,
        name: event.name.as_str().unwrap_or_default().to_string(),
        attributes: common::attrs_to_key_values(&event.attributes),
        dropped_attributes_count: 0,
    }
}

fn decode_span_event(event: pb::span::Event) -> SpanEvent {
    let mut attributes = AttrMap::new();
    common::key_values_into_attrs(event.attributes, &mut attributes);
    SpanEvent { timestamp: event.time_unix_nano as i64, name: Value::str(event.name), attributes }
}

fn encode_span_link(link: &SpanLink) -> pb::span::Link {
    pb::span::Link {
        trace_id: link.trace_id.to_vec(),
        span_id: link.span_id.to_vec(),
        trace_state: String::new(),
        attributes: common::attrs_to_key_values(&link.attributes),
        dropped_attributes_count: 0,
        flags: 0,
    }
}

fn decode_span_link(link: pb::span::Link) -> Result<SpanLink, CodecError> {
    let trace_id = ids::trace_id(&link.trace_id)?;
    let span_id = ids::span_id(&link.span_id)?;
    let mut attributes = AttrMap::new();
    common::key_values_into_attrs(link.attributes, &mut attributes);
    Ok(SpanLink { trace_id, span_id, attributes })
}

/// `trace_id`/`span_id` length validation, shared by a `Span` and its `Link`s -- OTLP requires
/// exactly 16 and 8 bytes respectively; anything else is malformed input, not a value to coerce.
mod ids {
    use crate::CodecError;

    pub(super) fn trace_id(bytes: &[u8]) -> Result<[u8; 16], CodecError> {
        bytes.try_into().map_err(|_| {
            CodecError::Malformed(format!("trace_id must be 16 bytes, got {}", bytes.len()))
        })
    }

    pub(super) fn span_id(bytes: &[u8]) -> Result<[u8; 8], CodecError> {
        bytes.try_into().map_err(|_| {
            CodecError::Malformed(format!("span_id must be 8 bytes, got {}", bytes.len()))
        })
    }
}

pub(crate) fn encode_span(event: &Event, span: &SpanRecord) -> pb::Span {
    let mut attributes = event.attributes.clone();
    // Consumed here, not left behind as a plain attribute too -- see the module doc.
    let status_message =
        attributes.remove("otel.status_message").and_then(|v| v.as_str().map(str::to_string));

    pb::Span {
        trace_id: span.trace_id.to_vec(),
        span_id: span.span_id.to_vec(),
        trace_state: String::new(),
        // OTLP's own convention: empty bytes, not a distinguished "no parent" sentinel.
        parent_span_id: span.parent_span_id.map(|id| id.to_vec()).unwrap_or_default(),
        flags: 0,
        name: span.name.as_str().unwrap_or_default().to_string(),
        kind: encode_span_kind(span.kind) as i32,
        start_time_unix_nano: event.timestamp.max(0) as u64,
        end_time_unix_nano: span.end_timestamp.max(0) as u64,
        attributes: common::attrs_to_key_values(&attributes),
        dropped_attributes_count: 0,
        events: span.events.iter().map(encode_span_event).collect(),
        dropped_events_count: 0,
        links: span.links.iter().map(encode_span_link).collect(),
        dropped_links_count: 0,
        status: Some(pb::Status {
            message: status_message.unwrap_or_default(),
            code: encode_status_code(span.status) as i32,
        }),
    }
}

/// `base_attrs` is the scope-derived base (`common::scope_attrs`), cloned once per span by the
/// caller.
pub(crate) fn decode_span(span: pb::Span, mut attrs: AttrMap) -> Result<Event, CodecError> {
    let trace_id = ids::trace_id(&span.trace_id)?;
    let span_id = ids::span_id(&span.span_id)?;
    let parent_span_id = if span.parent_span_id.is_empty() {
        None
    } else {
        Some(ids::span_id(&span.parent_span_id)?)
    };

    common::key_values_into_attrs(span.attributes, &mut attrs);
    let (message, code) = match span.status {
        Some(status) => (status.message, status.code),
        None => (String::new(), pb::status::StatusCode::Unset as i32),
    };
    if !message.is_empty() {
        attrs.insert("otel.status_message", message.as_str());
    }

    let links: Vec<SpanLink> =
        span.links.into_iter().map(decode_span_link).collect::<Result<_, _>>()?;
    let record = SpanRecord {
        trace_id,
        span_id,
        parent_span_id,
        name: Value::str(span.name),
        kind: decode_span_kind(span.kind),
        status: decode_status_code(code),
        events: span.events.into_iter().map(decode_span_event).collect(),
        links,
        end_timestamp: span.end_time_unix_nano as i64,
    };
    Ok(Event::span(span.start_time_unix_nano as i64, attrs, record))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_span(parent: Option<[u8; 8]>) -> (Event, SpanRecord) {
        let mut link_attrs = AttrMap::new();
        link_attrs.insert("linked", true);
        let mut event_attrs = AttrMap::new();
        event_attrs.insert("span_event_key", "span_event_value");

        let record = SpanRecord {
            trace_id: [7; 16],
            span_id: [9; 8],
            parent_span_id: parent,
            name: Value::str("do_the_thing"),
            kind: SpanKind::Server,
            status: SpanStatus::Ok,
            events: vec![SpanEvent {
                timestamp: 100,
                name: Value::str("checkpoint"),
                attributes: event_attrs,
            }],
            links: vec![SpanLink { trace_id: [8; 16], span_id: [10; 8], attributes: link_attrs }],
            end_timestamp: 200,
        };
        let event = Event::span(50, AttrMap::new(), record.clone());
        (event, record)
    }

    #[test]
    fn a_span_round_trips_including_its_links_and_span_events() {
        let (event, span) = sample_span(Some([3; 8]));
        let encoded = encode_span(&event, &span);
        let decoded = decode_span(encoded, AttrMap::new()).expect("decode should succeed");
        let decoded_span = decoded.span.expect("decoded event should carry a span");

        assert_eq!(decoded_span.trace_id, span.trace_id);
        assert_eq!(decoded_span.span_id, span.span_id);
        assert_eq!(decoded_span.parent_span_id, span.parent_span_id);
        assert_eq!(decoded_span.kind, span.kind);
        assert_eq!(decoded_span.status, span.status);
        assert_eq!(decoded_span.end_timestamp, span.end_timestamp);
        assert_eq!(decoded.timestamp, event.timestamp);
        assert_eq!(decoded_span.events.len(), 1, "the span event should survive");
        assert_eq!(decoded_span.events[0].name, span.events[0].name);
        assert_eq!(
            decoded_span.events[0].attributes.get("span_event_key"),
            span.events[0].attributes.get("span_event_key")
        );
        assert_eq!(decoded_span.links.len(), 1, "the span link should survive");
        assert_eq!(decoded_span.links[0].trace_id, span.links[0].trace_id);
        assert_eq!(decoded_span.links[0].span_id, span.links[0].span_id);
    }

    #[test]
    fn a_root_span_encodes_an_empty_parent_span_id_and_decodes_back_to_none() {
        let (event, span) = sample_span(None);
        let encoded = encode_span(&event, &span);
        assert!(encoded.parent_span_id.is_empty(), "a root span's parent_span_id must be empty");

        let decoded = decode_span(encoded, AttrMap::new()).unwrap();
        assert_eq!(decoded.span.unwrap().parent_span_id, None);
    }

    #[test]
    fn a_status_message_becomes_an_attribute_and_comes_back() {
        let mut span = SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: Value::str("op"),
            kind: SpanKind::Internal,
            status: SpanStatus::Error,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 10,
        };
        // Decode: an incoming Status.message has nowhere to live but an attribute.
        let mut pb_span = encode_span(&Event::span(0, AttrMap::new(), span.clone()), &span);
        pb_span.status = Some(pb::Status {
            message: "boom".to_string(),
            code: pb::status::StatusCode::Error as i32,
        });
        let decoded = decode_span(pb_span, AttrMap::new()).unwrap();
        assert_eq!(
            decoded.attributes.get("otel.status_message").and_then(|v| v.as_str()),
            Some("boom")
        );

        // Encode: that attribute comes back out as Status.message, not duplicated as a plain
        // attribute.
        span = decoded.span.clone().unwrap();
        let re_encoded = encode_span(&decoded, &span);
        assert_eq!(re_encoded.status.unwrap().message, "boom");
        assert!(
            re_encoded.attributes.iter().all(|kv| kv.key != "otel.status_message"),
            "otel.status_message must not also appear as a plain attribute"
        );
    }

    #[test]
    fn an_unspecified_span_kind_decodes_to_internal() {
        assert_eq!(decode_span_kind(pb::span::SpanKind::Unspecified as i32), SpanKind::Internal);
    }

    #[test]
    fn a_malformed_trace_id_length_is_rejected() {
        let (event, span) = sample_span(None);
        let mut encoded = encode_span(&event, &span);
        encoded.trace_id = vec![1, 2, 3];
        assert!(decode_span(encoded, AttrMap::new()).is_err());
    }
}
