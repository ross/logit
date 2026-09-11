//! `Event`/`SpanRecord` ↔ OTLP `Span` -- total.
//!
//! `start_time_unix_nano`/`end_time_unix_nano` map directly to `Event::timestamp`/
//! `SpanRecord::end_timestamp`. `SpanKind`/`SpanStatus` map directly (OTLP's
//! `SPAN_KIND_UNSPECIFIED` decodes to `Internal`, matching the OTLP spec's own recommendation).
//! `parent_span_id: Option<[u8; 8]>` ⇔ OTLP's empty-bytes-means-none convention. `Span.flags` ↔
//! `SpanRecord::flags` directly, both plain `u32`s.
//!
//! **`Status.message`, `trace_state`, and the three `dropped_*_count` fields map onto
//! `SpanRecord::ext: Option<Box<SpanExt>>`,** not attributes -- these are real typed fields now
//! (`docs/adr/metrics-model-v2.md`), so the well-known status-message attribute convention this
//! module used before these typed fields existed is retired: encode no longer reads it, decode no
//! longer stamps it. Encode builds `Some(Box<SpanExt>)` only
//! when at least one of its five fields is non-default (an all-default `SpanExt` and `None` encode
//! identically -- empty `trace_state`, empty `Status.message`, `0` dropped counts -- so there is no
//! reason to allocate the box for the overwhelmingly common span that carries none of these).
//!
//! **`SpanLink` gains real `flags`/`trace_state`/`dropped_attributes_count`,** mapped directly
//! (not boxed -- a link is already a `Vec` element, see `docs/adr/metrics-model-v2.md`'s reasoning
//! for why only `SpanRecord` itself needed the box). **`SpanEvent.dropped_attributes_count`** maps
//! directly too.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::trace::v1 as pb;
use crate::CodecError;
use logit_core::{
    AttrMap, Event, SpanEvent, SpanExt, SpanKind, SpanLink, SpanRecord, SpanStatus, Value,
};

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
        dropped_attributes_count: event.dropped_attributes_count,
    }
}

fn decode_span_event(event: pb::span::Event) -> SpanEvent {
    let mut attributes = AttrMap::new();
    common::key_values_into_attrs(event.attributes, &mut attributes);
    SpanEvent {
        timestamp: event.time_unix_nano as i64,
        name: Value::str(event.name),
        attributes,
        dropped_attributes_count: event.dropped_attributes_count,
    }
}

fn encode_span_link(link: &SpanLink) -> pb::span::Link {
    pb::span::Link {
        trace_id: link.trace_id.to_vec(),
        span_id: link.span_id.to_vec(),
        trace_state: link.trace_state.as_ref().map(common::bytes_to_string).unwrap_or_default(),
        attributes: common::attrs_to_key_values(&link.attributes),
        dropped_attributes_count: link.dropped_attributes_count,
        flags: link.flags,
    }
}

fn decode_span_link(link: pb::span::Link) -> Result<SpanLink, CodecError> {
    let trace_id = ids::trace_id(&link.trace_id)?;
    let span_id = ids::span_id(&link.span_id)?;
    let mut attributes = AttrMap::new();
    common::key_values_into_attrs(link.attributes, &mut attributes);
    Ok(SpanLink {
        trace_id,
        span_id,
        attributes,
        flags: link.flags,
        trace_state: common::string_to_bytes(link.trace_state),
        dropped_attributes_count: link.dropped_attributes_count,
    })
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

/// Builds `Some(Box<SpanExt>)` only when at least one of its five fields is non-default -- see
/// the module doc. `None` and an all-default `SpanExt` are indistinguishable on the wire, so there
/// is no reason to allocate the box for the overwhelmingly common span that carries neither a
/// status message, a `tracestate`, nor any dropped counts.
fn ext_from_wire(
    status_message: &str,
    trace_state: &str,
    dropped_attributes_count: u32,
    dropped_events_count: u32,
    dropped_links_count: u32,
) -> Option<Box<SpanExt>> {
    if status_message.is_empty()
        && trace_state.is_empty()
        && dropped_attributes_count == 0
        && dropped_events_count == 0
        && dropped_links_count == 0
    {
        return None;
    }
    Some(Box::new(SpanExt {
        status_message: common::string_to_bytes(status_message.to_string()),
        trace_state: common::string_to_bytes(trace_state.to_string()),
        dropped_attributes_count,
        dropped_events_count,
        dropped_links_count,
    }))
}

pub(crate) fn encode_span(event: &Event, span: &SpanRecord) -> pb::Span {
    let attributes = common::attrs_to_key_values(&event.attributes);
    let ext = span.ext.as_deref();
    let status_message = ext
        .and_then(|e| e.status_message.as_ref())
        .map(common::bytes_to_string)
        .unwrap_or_default();
    let trace_state =
        ext.and_then(|e| e.trace_state.as_ref()).map(common::bytes_to_string).unwrap_or_default();
    let dropped_attributes_count = ext.map(|e| e.dropped_attributes_count).unwrap_or(0);
    let dropped_events_count = ext.map(|e| e.dropped_events_count).unwrap_or(0);
    let dropped_links_count = ext.map(|e| e.dropped_links_count).unwrap_or(0);

    pb::Span {
        trace_id: span.trace_id.to_vec(),
        span_id: span.span_id.to_vec(),
        trace_state,
        // OTLP's own convention: empty bytes, not a distinguished "no parent" sentinel.
        parent_span_id: span.parent_span_id.map(|id| id.to_vec()).unwrap_or_default(),
        flags: span.flags,
        name: span.name.as_str().unwrap_or_default().to_string(),
        kind: encode_span_kind(span.kind) as i32,
        start_time_unix_nano: event.timestamp.max(0) as u64,
        end_time_unix_nano: span.end_timestamp.max(0) as u64,
        attributes,
        dropped_attributes_count,
        events: span.events.iter().map(encode_span_event).collect(),
        dropped_events_count,
        links: span.links.iter().map(encode_span_link).collect(),
        dropped_links_count,
        status: Some(pb::Status {
            message: status_message,
            code: encode_status_code(span.status) as i32,
        }),
    }
}

/// `base_attrs` is the resource-level base (`common`'s own doc), cloned once per span by the
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
    let (message, code) = match &span.status {
        Some(status) => (status.message.as_str(), status.code),
        None => ("", pb::status::StatusCode::Unset as i32),
    };
    let ext = ext_from_wire(
        message,
        &span.trace_state,
        span.dropped_attributes_count,
        span.dropped_events_count,
        span.dropped_links_count,
    );

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
        flags: span.flags,
        ext,
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
                dropped_attributes_count: 2,
            }],
            links: vec![SpanLink {
                trace_id: [8; 16],
                span_id: [10; 8],
                attributes: link_attrs,
                flags: 1,
                trace_state: Some(bytes::Bytes::from_static(b"vendor=value")),
                dropped_attributes_count: 3,
            }],
            end_timestamp: 200,
            flags: 1,
            ext: Some(Box::new(SpanExt {
                status_message: None,
                trace_state: Some(bytes::Bytes::from_static(b"vendor=root")),
                dropped_attributes_count: 4,
                dropped_events_count: 5,
                dropped_links_count: 6,
            })),
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
        assert_eq!(decoded_span.flags, span.flags);
        assert_eq!(decoded_span.events.len(), 1, "the span event should survive");
        assert_eq!(decoded_span.events[0].name, span.events[0].name);
        assert_eq!(
            decoded_span.events[0].attributes.get("span_event_key"),
            span.events[0].attributes.get("span_event_key")
        );
        assert_eq!(
            decoded_span.events[0].dropped_attributes_count,
            span.events[0].dropped_attributes_count
        );
        assert_eq!(decoded_span.links.len(), 1, "the span link should survive");
        assert_eq!(decoded_span.links[0].trace_id, span.links[0].trace_id);
        assert_eq!(decoded_span.links[0].span_id, span.links[0].span_id);
        assert_eq!(decoded_span.links[0].flags, span.links[0].flags);
        assert_eq!(decoded_span.links[0].trace_state, span.links[0].trace_state);
        assert_eq!(
            decoded_span.links[0].dropped_attributes_count,
            span.links[0].dropped_attributes_count
        );
        assert_eq!(decoded_span.ext, span.ext);
    }

    #[test]
    fn a_root_span_encodes_an_empty_parent_span_id_and_decodes_back_to_none() {
        let (event, span) = sample_span(None);
        let encoded = encode_span(&event, &span);
        assert!(encoded.parent_span_id.is_empty(), "a root span's parent_span_id must be empty");

        let decoded = decode_span(encoded, AttrMap::new()).unwrap();
        assert_eq!(decoded.span.unwrap().parent_span_id, None);
    }

    fn bare_span() -> SpanRecord {
        SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: Value::str("op"),
            kind: SpanKind::Internal,
            status: SpanStatus::Error,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 10,
            flags: 0,
            ext: None,
        }
    }

    /// `Status.message` maps onto `SpanExt::status_message`, a real field -- not the retired
    /// attribute convention this module used before it existed.
    #[test]
    fn a_status_message_maps_onto_span_ext_not_an_attribute() {
        let span = bare_span();
        let mut pb_span = encode_span(&Event::span(0, AttrMap::new(), span.clone()), &span);
        pb_span.status = Some(pb::Status {
            message: "boom".to_string(),
            code: pb::status::StatusCode::Error as i32,
        });
        let decoded = decode_span(pb_span, AttrMap::new()).unwrap();
        assert!(
            decoded.attributes.is_empty(),
            "a status message must land on SpanExt now, not leak out as any kind of attribute"
        );
        let decoded_span = decoded.span.clone().unwrap();
        assert_eq!(
            decoded_span.ext.as_ref().and_then(|e| e.status_message.as_deref()),
            Some(b"boom".as_slice())
        );

        // Encode: the field comes back out as Status.message.
        let re_encoded = encode_span(&decoded, &decoded_span);
        assert_eq!(re_encoded.status.unwrap().message, "boom");
    }

    /// The overwhelmingly common span (no status message, no `tracestate`, nothing dropped) must
    /// decode to `ext: None`, not `Some(Box<SpanExt>)` full of defaults -- the two are
    /// indistinguishable on the wire, so allocating the box would be pure waste.
    #[test]
    fn a_span_with_nothing_extra_decodes_with_ext_none() {
        let span = bare_span();
        let encoded = encode_span(&Event::span(0, AttrMap::new(), span.clone()), &span);
        let decoded = decode_span(encoded, AttrMap::new()).unwrap();
        assert_eq!(decoded.span.unwrap().ext, None);
    }

    /// Each of `SpanExt`'s five fields independently earns the box on decode -- exercised one at a
    /// time so a future field that forgets to join the "any non-default" check is caught.
    #[test]
    fn any_single_non_default_ext_field_earns_the_box_on_decode() {
        let span = bare_span();
        let base = encode_span(&Event::span(0, AttrMap::new(), span.clone()), &span);

        let mut trace_state_only = base.clone();
        trace_state_only.trace_state = "vendor=value".to_string();
        assert!(decode_span(trace_state_only, AttrMap::new()).unwrap().span.unwrap().ext.is_some());

        let mut dropped_attrs_only = base.clone();
        dropped_attrs_only.dropped_attributes_count = 1;
        assert!(decode_span(dropped_attrs_only, AttrMap::new())
            .unwrap()
            .span
            .unwrap()
            .ext
            .is_some());

        let mut dropped_events_only = base.clone();
        dropped_events_only.dropped_events_count = 1;
        assert!(decode_span(dropped_events_only, AttrMap::new())
            .unwrap()
            .span
            .unwrap()
            .ext
            .is_some());

        let mut dropped_links_only = base;
        dropped_links_only.dropped_links_count = 1;
        assert!(decode_span(dropped_links_only, AttrMap::new())
            .unwrap()
            .span
            .unwrap()
            .ext
            .is_some());
    }

    #[test]
    fn span_flags_map_directly_both_ways() {
        let mut span = bare_span();
        span.flags = 0x9; // SAMPLED | CONTEXT_HAS_IS_REMOTE, an arbitrary non-zero bitmask
        let event = Event::span(0, AttrMap::new(), span.clone());
        let encoded = encode_span(&event, &span);
        assert_eq!(encoded.flags, 0x9);
        let decoded = decode_span(encoded, AttrMap::new()).unwrap();
        assert_eq!(decoded.span.unwrap().flags, 0x9);
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
