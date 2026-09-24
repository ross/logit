//! The native `logit`-to-`logit` codec: a dictionary-first, hand-rolled binary encoding of an
//! [`EventBatch`], framed by [`crate::frame`]. `docs/design/wire-protocol.md` has the layout;
//! ADR `native-wire-format-encoding` says why it's hand-rolled rather than `rkyv` or `postcard`.
//!
//! **This is the same format for a socket and a file.** Nothing here assumes a connection: the
//! `buffer.disk:` spool, `file_out`'s native format, and the `logit_in`/`logit_out` transport all
//! use this module unmodified.
//!
//! Rules this module upholds:
//! - A `Symbol` (`lasso::Spur`) is never written raw (see [`dict`]). Every key, metric name, and
//!   unit crosses the wire as a dictionary string referenced by index.
//! - Every frame is independently decodable: its own dictionary, resource, scope, and events. A
//!   file is a plain concatenation of frames: append, read sequentially, `frame::resync` past a
//!   torn write.
//! - No proper prefix of a valid encoding decodes (`tests/robustness.rs`'s
//!   `assert_every_truncation_fails_cleanly`), so no section is an optional trailer.
//! - Every record type is TLV-framed (`record::write_field`/`for_each_field`), so a reader skips
//!   a field tag it doesn't know. `logit` is pre-release, so that's hygiene against a torn write,
//!   not version negotiation: growing a fixed enum (`Value`, `MetricKind`) is a straight reshape
//!   of this module. [`value`] and [`record`] say what an unknown tag degrades to.

pub mod control;
pub mod dict;
pub mod record;
pub mod value;
pub mod varint;

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use logit_core::interner::intern;
use logit_core::{Event, EventBatch, Provenance, Resource};

use crate::frame::{read_frame, write_frame, Compression};
use crate::native::dict::{Dict, DictBuilder};
use crate::native::varint::{read_u8, read_uvarint, write_uvarint};
use crate::{CodecError, Decoder, Encoder};

/// The frame header's `codec` byte for this payload format, without provenance.
pub const CODEC_NATIVE_V1: u8 = 1;

/// [`encode_batch`]'s payload plus a mandatory length-prefixed [`Provenance`] trailer (ADR
/// `batch-provenance-on-delivered`).
///
/// A separate codec rather than an optional trailer on v1, which would let a payload truncated at
/// the trailer boundary decode as "no provenance". `Hello.codecs`/`HelloAck.codec`
/// ([`control`]) negotiate v1 or v2; a peer offering only v1 still talks, without provenance.
pub const CODEC_NATIVE_V2: u8 = 2;

const TRAILER_TAG_ORIGIN: u8 = 1;
const TRAILER_TAG_PREVIOUS: u8 = 2;

/// Bounds a trailer field's declared length before it slices `bytes`; a component id is far
/// shorter.
const MAX_SANE_TRAILER_FIELD_BYTES: usize = 4096;

/// Encodes one [`EventBatch`] into the v1 payload: dictionary, len-prefixed [`Resource`] TLV,
/// mandatory [`logit_core::Scope`] section (presence byte, then a len-prefixed TLV if present),
/// then a counted list of len-prefixed events (`docs/design/wire-protocol.md`'s "Batch grammar").
///
/// The scope section sits right after the resource, never as an optional trailing section,
/// which would let a truncated payload decode as "no scope".
///
/// The dictionary is written first but built last: [`DictBuilder`] collects symbols while the
/// other sections encode into their own buffers, so the batch is walked once.
pub fn encode_batch(batch: &EventBatch) -> Bytes {
    let mut dict = DictBuilder::default();

    let mut resource_buf = BytesMut::new();
    record::write_resource(&mut resource_buf, &mut dict, &batch.resource);

    let mut scope_buf = BytesMut::new();
    match &batch.scope {
        Some(scope) => {
            scope_buf.extend_from_slice(&[1]);
            let mut tmp = BytesMut::new();
            record::write_scope(&mut tmp, &mut dict, scope);
            write_uvarint(&mut scope_buf, tmp.len() as u64);
            scope_buf.extend_from_slice(&tmp);
        }
        None => scope_buf.extend_from_slice(&[0]),
    }

    let mut events_buf = BytesMut::new();
    write_uvarint(&mut events_buf, batch.events.len() as u64);
    for event in &batch.events {
        let body = record::write_event(&mut dict, event);
        write_uvarint(&mut events_buf, body.len() as u64);
        events_buf.extend_from_slice(&body);
    }

    let mut out = BytesMut::new();
    dict.write(&mut out);
    write_uvarint(&mut out, resource_buf.len() as u64);
    out.extend_from_slice(&resource_buf);
    out.extend_from_slice(&scope_buf);
    out.extend_from_slice(&events_buf);
    out.freeze()
}

/// Bounds a declared event count before it sizes an allocation, like [`dict::Dict::read`]'s cap.
const MAX_SANE_EVENT_COUNT: usize = 16 * 1024 * 1024;

/// The inverse of [`encode_batch`].
pub fn decode_batch(bytes: &mut Bytes) -> Result<EventBatch, CodecError> {
    let dict = Dict::read(bytes)?;

    let resource_len = read_uvarint(bytes)? as usize;
    if bytes.len() < resource_len {
        return Err(CodecError::Malformed(format!(
            "resource section declares {resource_len} bytes but only {} remain",
            bytes.len()
        )));
    }
    let mut resource_body = bytes.split_to(resource_len);
    let resource = Arc::new(record::read_resource(&mut resource_body, &dict)?);

    let scope = match read_u8(bytes)? {
        0 => None,
        1 => {
            let scope_len = read_uvarint(bytes)? as usize;
            if bytes.len() < scope_len {
                return Err(CodecError::Malformed(format!(
                    "scope section declares {scope_len} bytes but only {} remain",
                    bytes.len()
                )));
            }
            let mut scope_body = bytes.split_to(scope_len);
            Some(Arc::new(record::read_scope(&mut scope_body, &dict)?))
        }
        other => {
            return Err(CodecError::Malformed(format!("bad scope presence byte {other}")));
        }
    };

    let event_count = read_uvarint(bytes)? as usize;
    if event_count > MAX_SANE_EVENT_COUNT {
        return Err(CodecError::Malformed(format!(
            "batch declares {event_count} events, over the {MAX_SANE_EVENT_COUNT} sanity cap"
        )));
    }
    let mut events = Vec::with_capacity(event_count.min(4096));
    for _ in 0..event_count {
        let body_len = read_uvarint(bytes)? as usize;
        if bytes.len() < body_len {
            return Err(CodecError::Malformed(format!(
                "event declares {body_len} bytes but only {} remain",
                bytes.len()
            )));
        }
        let mut body = bytes.split_to(body_len);
        events.push(record::read_event(&mut body, &dict)?);
    }
    Ok(EventBatch { resource, scope, events })
}

/// [`encode_batch`] followed by the [`CODEC_NATIVE_V2`] provenance trailer.
///
/// The trailer is `tag(u8) + len(uvarint) + payload` entries with strings inline, not
/// dictionary-indexed: two strings per batch give a dictionary nothing to amortize. An absent
/// field writes no entry, but the trailer's length prefix is always written, `0x00` when empty.
pub fn encode_batch_v2(batch: &EventBatch, provenance: Provenance) -> Bytes {
    let v1 = encode_batch(batch);

    let mut trailer = BytesMut::new();
    if let Some(origin) = provenance.origin_str() {
        write_trailer_field(&mut trailer, TRAILER_TAG_ORIGIN, origin);
    }
    if let Some(previous) = provenance.previous_str() {
        write_trailer_field(&mut trailer, TRAILER_TAG_PREVIOUS, previous);
    }

    let mut out = BytesMut::with_capacity(v1.len() + 5 + trailer.len());
    out.extend_from_slice(&v1);
    write_uvarint(&mut out, trailer.len() as u64);
    out.extend_from_slice(&trailer);
    out.freeze()
}

fn write_trailer_field(out: &mut BytesMut, tag: u8, s: &str) {
    out.extend_from_slice(&[tag]);
    write_uvarint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// The inverse of [`encode_batch_v2`]. A plain v1 payload fails here rather than decoding as
/// "no provenance": [`decode_batch`] consumes all of it and the trailer-length read finds nothing.
pub fn decode_batch_v2(bytes: &mut Bytes) -> Result<(EventBatch, Provenance), CodecError> {
    let batch = decode_batch(bytes)?;

    let trailer_len = read_uvarint(bytes)? as usize;
    if bytes.len() < trailer_len {
        return Err(CodecError::Malformed(format!(
            "provenance trailer declares {trailer_len} bytes but only {} remain",
            bytes.len()
        )));
    }
    let mut trailer = bytes.split_to(trailer_len);

    let mut provenance = Provenance::default();
    while !trailer.is_empty() {
        let tag = read_u8(&mut trailer)?;
        let len = read_uvarint(&mut trailer)? as usize;
        if len > MAX_SANE_TRAILER_FIELD_BYTES {
            return Err(CodecError::Malformed(format!(
                "provenance trailer field {tag} declares {len} bytes, over the \
                 {MAX_SANE_TRAILER_FIELD_BYTES} sanity cap"
            )));
        }
        if trailer.len() < len {
            return Err(CodecError::Malformed(format!(
                "provenance trailer field {tag} declares {len} bytes but only {} remain",
                trailer.len()
            )));
        }
        let field = trailer.split_to(len);
        match tag {
            TRAILER_TAG_ORIGIN => provenance.origin = Some(intern(trailer_str(&field)?)),
            TRAILER_TAG_PREVIOUS => provenance.previous = Some(intern(trailer_str(&field)?)),
            _unknown => { /* skipped, not rejected: torn-write hygiene (module doc) */ }
        }
    }
    Ok((batch, provenance))
}

fn trailer_str(bytes: &[u8]) -> Result<&str, CodecError> {
    std::str::from_utf8(bytes)
        .map_err(|e| CodecError::Malformed(format!("provenance trailer field not utf-8: {e}")))
}

/// The [`Encoder`]: [`encode_batch`]'s payload framed under [`CODEC_NATIVE_V1`].
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeEncoder {
    pub compression: Compression,
}

impl NativeEncoder {
    pub fn new(compression: Compression) -> Self {
        Self { compression }
    }
}

impl Encoder for NativeEncoder {
    fn encode(&mut self, batch: &EventBatch) -> Result<Bytes, CodecError> {
        let payload = encode_batch(batch);
        write_frame(CODEC_NATIVE_V1, self.compression, &payload)
    }
}

/// The [`Decoder`] for [`CODEC_NATIVE_V1`] frames. Ignores `received_at`: a native event keeps
/// its original timestamp end to end.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeDecoder;

impl Decoder for NativeDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        _received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<logit_core::Scope>>), CodecError> {
        let mut bytes = bytes;
        let (codec, mut payload) = read_frame(&mut bytes)?;
        if codec != CODEC_NATIVE_V1 {
            return Err(CodecError::Unsupported(format!(
                "frame codec byte {codec}, expected native v1 ({CODEC_NATIVE_V1})"
            )));
        }
        let batch = decode_batch(&mut payload)?;
        out.extend(batch.events);
        Ok((batch.resource, batch.scope))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        AttrMap, BodyFormat, DdSketch, LogRecord, MetricKind, MetricRecord, Severity, SpanKind,
        SpanRecord, SpanStatus, Value,
    };

    fn sample_batch() -> EventBatch {
        let mut resource_attrs = AttrMap::new();
        resource_attrs.insert("service.name", "orders-api");
        let resource = Arc::new(Resource { attributes: resource_attrs, ..Resource::default() });

        let mut log_attrs = AttrMap::new();
        log_attrs.insert("host", "web-1");
        let log_event = Event::log(
            1,
            log_attrs,
            LogRecord {
                message: Value::str("hello"),
                severity: Some(Severity::Info),
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );

        let mut sketch = DdSketch::new();
        sketch.add(0.5);
        let metric_event = Event::metric(
            2,
            AttrMap::new(),
            MetricRecord {
                unit: Some(logit_core::interner::intern("s")),
                ..MetricRecord::new(
                    logit_core::interner::intern("mod_test_metric"),
                    MetricKind::Distribution(sketch),
                )
            },
        );

        let span_event = Event::span(
            3,
            AttrMap::new(),
            SpanRecord {
                trace_id: [1; 16],
                span_id: [2; 8],
                parent_span_id: None,
                name: Value::str("span"),
                kind: SpanKind::Internal,
                status: SpanStatus::Ok,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: 4,
                flags: 0,
                ext: None,
            },
        );

        EventBatch { resource, scope: None, events: vec![log_event, metric_event, span_event] }
    }

    #[test]
    fn encode_decode_batch_round_trips() {
        let batch = sample_batch();
        let payload = encode_batch(&batch);
        let decoded = decode_batch(&mut payload.clone()).unwrap();

        assert_eq!(decoded.resource.attributes, batch.resource.attributes);
        assert_eq!(decoded.events.len(), batch.events.len());
        assert!(decoded.events[0].log.is_some());
        assert!(decoded.events[1].metrics.len() == 1);
        assert!(decoded.events[2].span.is_some());
    }

    #[test]
    fn encoder_decoder_round_trip_through_the_frame() {
        let batch = sample_batch();
        let mut encoder = NativeEncoder::default();
        let framed = encoder.encode(&batch).unwrap();

        let mut decoder = NativeDecoder;
        let mut events = Vec::new();
        let (resource, scope) = decoder.decode_into(framed, 999, &mut events).unwrap();

        assert_eq!(resource.attributes, batch.resource.attributes);
        assert!(scope.is_none());
        assert_eq!(events.len(), 3);
        // Original timestamps survive; `received_at` (999) never overwrites them.
        assert_eq!(events[0].timestamp, 1);
        assert_eq!(events[1].timestamp, 2);
        assert_eq!(events[2].timestamp, 3);
    }

    #[test]
    fn encoder_decoder_round_trip_with_lz4_compression() {
        let batch = sample_batch();
        let mut encoder = NativeEncoder::new(Compression::Lz4);
        let framed = encoder.encode(&batch).unwrap();

        let mut decoder = NativeDecoder;
        let mut events = Vec::new();
        let (resource, _scope) = decoder.decode_into(framed, 0, &mut events).unwrap();
        assert_eq!(resource.attributes, batch.resource.attributes);
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn encoding_with_zstd_returns_unsupported_rather_than_panicking() {
        let mut encoder = NativeEncoder::new(Compression::Zstd);
        assert!(matches!(encoder.encode(&sample_batch()), Err(CodecError::Unsupported(_))));
    }

    #[test]
    fn an_empty_batch_round_trips() {
        let batch =
            EventBatch { resource: Arc::new(Resource::default()), scope: None, events: Vec::new() };
        let payload = encode_batch(&batch);
        let decoded = decode_batch(&mut payload.clone()).unwrap();
        assert!(decoded.events.is_empty());
        assert!(decoded.resource.attributes.is_empty());
        assert!(decoded.scope.is_none());
    }

    #[test]
    fn a_batch_with_a_fully_populated_scope_round_trips() {
        let mut scope_attributes = AttrMap::new();
        scope_attributes.insert("k", "v");
        let scope = std::sync::Arc::new(logit_core::Scope {
            name: bytes::Bytes::from_static(b"nginx-otel-module"),
            version: bytes::Bytes::from_static(b"1.0.0"),
            attributes: scope_attributes,
            dropped_attributes_count: 2,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/schema")),
        });
        let mut batch = sample_batch();
        batch.scope = Some(scope.clone());

        let payload = encode_batch(&batch);
        let decoded = decode_batch(&mut payload.clone()).unwrap();
        assert_eq!(decoded.scope.as_deref(), Some(&*scope));
    }

    /// The scope survives the `Encoder`/`Decoder` trait seam, not just the free functions.
    #[test]
    fn a_batch_with_a_scope_round_trips_through_the_decoder_trait() {
        let scope = std::sync::Arc::new(logit_core::Scope {
            name: bytes::Bytes::from_static(b"trait_test_scope"),
            version: bytes::Bytes::from_static(b"2.0.0"),
            ..Default::default()
        });
        let mut batch = sample_batch();
        batch.scope = Some(scope);

        let mut encoder = NativeEncoder::default();
        let framed = encoder.encode(&batch).unwrap();

        let mut decoder = NativeDecoder;
        let decoded = decoder.decode(framed).unwrap();

        assert_eq!(decoded, batch);
    }

    #[test]
    fn decode_rejects_a_frame_with_a_foreign_codec_byte() {
        let batch = sample_batch();
        let payload = encode_batch(&batch);
        let framed = write_frame(0xEE, Compression::None, &payload).unwrap();

        let mut decoder = NativeDecoder;
        let mut events = Vec::new();
        let err = decoder.decode_into(framed, 0, &mut events).unwrap_err();
        assert!(matches!(err, CodecError::Unsupported(_)));
    }

    #[test]
    fn concatenated_frames_written_to_a_buffer_are_each_independently_decodable() {
        // A "file": two encoded batches back to back, sharing no dictionary.
        let mut encoder = NativeEncoder::default();
        let batch_a = sample_batch();
        let mut batch_b = sample_batch();
        batch_b.events.truncate(1);

        let mut file = BytesMut::new();
        file.extend_from_slice(&encoder.encode(&batch_a).unwrap());
        file.extend_from_slice(&encoder.encode(&batch_b).unwrap());
        let mut cursor = file.freeze();

        // A file reader loops `read_frame` to find each frame boundary.
        let (codec_a, mut payload_a) = read_frame(&mut cursor).unwrap();
        assert_eq!(codec_a, CODEC_NATIVE_V1);
        let decoded_a = decode_batch(&mut payload_a).unwrap();
        assert_eq!(decoded_a.events.len(), batch_a.events.len());

        let (codec_b, mut payload_b) = read_frame(&mut cursor).unwrap();
        assert_eq!(codec_b, CODEC_NATIVE_V1);
        let decoded_b = decode_batch(&mut payload_b).unwrap();
        assert_eq!(decoded_b.events.len(), 1);
        assert!(cursor.is_empty(), "both frames should be fully consumed");
    }

    fn sample_provenance() -> Provenance {
        Provenance {
            origin: Some(logit_core::interner::intern("mod_test_nginx_in")),
            previous: Some(logit_core::interner::intern("mod_test_enrich")),
        }
    }

    #[test]
    fn encode_decode_batch_v2_round_trips_provenance() {
        let batch = sample_batch();
        let provenance = sample_provenance();
        let mut payload = encode_batch_v2(&batch, provenance);
        let (decoded, decoded_provenance) = decode_batch_v2(&mut payload).unwrap();

        assert_eq!(decoded.events.len(), batch.events.len());
        assert_eq!(decoded_provenance, provenance);
        assert!(payload.is_empty(), "decode_batch_v2 should consume the whole payload");
    }

    #[test]
    fn encode_decode_batch_v2_round_trips_both_fields_absent() {
        let batch = sample_batch();
        let mut payload = encode_batch_v2(&batch, Provenance::default());
        let (_decoded, provenance) = decode_batch_v2(&mut payload).unwrap();
        assert_eq!(provenance, Provenance::default());
    }

    /// Two absent fields encode as the one-byte `trailer_len = 0`.
    #[test]
    fn an_absent_provenance_field_costs_one_byte_total() {
        let batch = sample_batch();
        let without = encode_batch(&batch);
        let with_empty_provenance = encode_batch_v2(&batch, Provenance::default());
        assert_eq!(with_empty_provenance.len(), without.len() + 1);
    }

    /// No proper prefix of a valid v2 encoding decodes, trailer included.
    #[test]
    fn decode_batch_v2_rejects_every_proper_prefix_of_a_valid_encoding() {
        let valid = encode_batch_v2(&sample_batch(), sample_provenance());
        for len in 0..valid.len() {
            let mut truncated = valid.slice(0..len);
            assert!(
                decode_batch_v2(&mut truncated).is_err(),
                "a {len}-byte truncation of a valid v2 payload decoded successfully"
            );
        }
    }

    /// A v1 payload fed to `decode_batch_v2` fails rather than decoding as "no provenance".
    #[test]
    fn decode_batch_v2_rejects_a_plain_v1_payload() {
        let mut v1_payload = encode_batch(&sample_batch());
        assert!(decode_batch_v2(&mut v1_payload).is_err());
    }

    /// An unrecognized trailer tag is skipped, not rejected.
    #[test]
    fn decode_batch_v2_skips_an_unrecognized_trailer_tag() {
        let batch = sample_batch();
        let v1 = encode_batch(&batch);

        let mut trailer = BytesMut::new();
        write_trailer_field(&mut trailer, TRAILER_TAG_ORIGIN, "mod_test_skip_origin");
        write_trailer_field(&mut trailer, 99, "a future field this reader doesn't know");

        let mut out = BytesMut::new();
        out.extend_from_slice(&v1);
        write_uvarint(&mut out, trailer.len() as u64);
        out.extend_from_slice(&trailer);
        let mut payload = out.freeze();

        let (_decoded, provenance) = decode_batch_v2(&mut payload).unwrap();
        assert_eq!(provenance.origin_str(), Some("mod_test_skip_origin"));
    }
}
