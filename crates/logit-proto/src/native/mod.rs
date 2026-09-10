//! The native `logit`-to-`logit` codec: dictionary-first, hand-rolled binary encoding of an
//! [`EventBatch`], framed by [`crate::frame`]. See `docs/design/wire-protocol.md` for the design,
//! and `docs/adr/native-wire-format-encoding.md` for why this beat `rkyv` and a serde/`postcard`
//! encoding in the bake-off that decided it.
//!
//! **This is the same format for a socket and a file.** [`NativeEncoder`]/[`NativeDecoder`]
//! implement the ordinary [`crate::Encoder`]/[`crate::Decoder`] traits every other codec in this
//! crate does -- nothing here assumes a connection; [`crate::frame::write_frame`]/`read_frame`
//! work identically appending to a file. A durable buffer or a `logit_in`/`logit_out` transport
//! (both future work, `docs/design/wire-protocol.md`'s connection protocol) both build on exactly
//! this module, unmodified.
//!
//! **Correctness rules this module exists to uphold** (`docs/design/wire-protocol.md`,
//! `docs/design/data-model.md`):
//! - A `Symbol` (`lasso::Spur`) is never written raw -- see [`dict`]'s own doc comment. Every key,
//!   metric name, and unit crosses the wire as a string in the dictionary, referenced by index.
//! - Every frame is independently decodable: its own dictionary, its own resource, its own events.
//!   A file is a plain concatenation of frames -- append, sequential read, `frame::resync` past a
//!   torn write.
//! - `Event`'s own fields are the one place this format is forward-compatible without a version
//!   bump: `record::write_event`/`read_event`'s per-field framing lets an older reader skip a
//!   field it doesn't recognize. Growing a fixed enum (`Value`, `MetricKind`) is a different kind
//!   of change and is not attempted losslessly by an old reader -- see [`value`]'s and
//!   [`record`]'s own doc comments for exactly what each degrades to.

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

/// The `codec` byte [`crate::frame::FrameHeader`] carries for this payload format -- what lets a
/// reader reject a frame that says "native" but whose codec byte says otherwise, or (eventually)
/// dispatch among several codecs sharing the same frame header.
pub const CODEC_NATIVE_V1: u8 = 1;

/// [`encode_batch`]'s payload, plus a mandatory length-prefixed [`Provenance`] trailer -- see
/// [`encode_batch_v2`]/[`decode_batch_v2`] and `docs/adr/batch-provenance-on-delivered.md`. Not an
/// in-place change to v1: `crates/logit-proto/tests/robustness.rs`'s
/// `assert_every_truncation_fails_cleanly` pins the invariant that no proper prefix of a valid
/// encoding is itself valid, which an *optional* trailer on the existing format would silently
/// break (a payload truncated exactly at the trailer boundary would decode as "no provenance,"
/// a valid-looking result, not an error). `Hello.codecs`/`HelloAck.codec`
/// (`crate::native::control`) negotiate which of the two a `logit_out`/`logit_in` pair actually
/// uses; either side offering only v1 still talks, with provenance simply absent.
pub const CODEC_NATIVE_V2: u8 = 2;

const TRAILER_TAG_ORIGIN: u8 = 1;
const TRAILER_TAG_PREVIOUS: u8 = 2;

/// A trailer field's value is bounded to catch a corrupt/hostile length before it's used to slice
/// `bytes` -- a component id is config-sized text, never remotely close to this.
const MAX_SANE_TRAILER_FIELD_BYTES: usize = 4096;

/// Encodes one [`EventBatch`] into the dictionary-first payload `docs/design/wire-protocol.md`
/// describes: the dictionary section, then the resource's attributes, then a length-prefixed list
/// of events (each itself [`record::write_event`]'s TLV field stream).
///
/// The dictionary is written *first* on the wire but built *last*, in the sense that
/// [`DictBuilder`] accumulates symbols as encoding proceeds and its own bytes aren't emitted until
/// every symbol that will ever be interned already has been -- one pass over the batch, not two.
pub fn encode_batch(batch: &EventBatch) -> Bytes {
    let mut dict = DictBuilder::default();

    let mut resource_buf = BytesMut::new();
    value::write_attr_map(&mut resource_buf, &mut dict, &batch.resource.attributes);

    let mut events_buf = BytesMut::new();
    write_uvarint(&mut events_buf, batch.events.len() as u64);
    for event in &batch.events {
        let body = record::write_event(&mut dict, event);
        write_uvarint(&mut events_buf, body.len() as u64);
        events_buf.extend_from_slice(&body);
    }

    let mut out = BytesMut::new();
    dict.write(&mut out);
    out.extend_from_slice(&resource_buf);
    out.extend_from_slice(&events_buf);
    out.freeze()
}

/// A batch with more events than this could plausibly be a well-formed single batch -- guards
/// `Vec::with_capacity` below against a corrupt or hostile length field, the same reasoning as
/// [`dict::Dict::read`]'s own cap.
const MAX_SANE_EVENT_COUNT: usize = 16 * 1024 * 1024;

/// The inverse of [`encode_batch`].
pub fn decode_batch(bytes: &mut Bytes) -> Result<EventBatch, CodecError> {
    let dict = Dict::read(bytes)?;
    let resource_attrs = value::read_attr_map(bytes, &dict)?;
    let resource = Arc::new(Resource { attributes: resource_attrs });

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
    Ok(EventBatch { resource, events })
}

/// [`encode_batch`], plus a mandatory length-prefixed [`Provenance`] trailer -- see
/// [`CODEC_NATIVE_V2`]'s own doc comment for why this is a separate codec rather than a change to
/// v1. Calls [`encode_batch`] as a subroutine and appends to its output; v1's own encoding is
/// untouched by this addition.
///
/// The trailer is its own small TLV section (same `tag(u8) + len(uvarint) + payload` shape as
/// `crate::native::control`), holding each present field's string *inline*, not dictionary-
/// indexed: `origin`/`previous` are at most two scalar strings written once per batch, so there's
/// no repetition within one payload for a dictionary to pay off on -- unlike attribute/metric
/// keys, which repeat once per event (`docs/design/wire-protocol.md`'s "dictionary-first
/// batches"). A `None` field costs nothing (no tag entry at all); the trailer's own length prefix
/// is what stays mandatory, always at least one byte, even when both fields are absent.
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

/// The inverse of [`encode_batch_v2`]. Decodes the v1-shaped prefix via [`decode_batch`] itself
/// (inheriting its truncation/bit-flip robustness unchanged, `crates/logit-proto/tests/
/// robustness.rs`), then reads the mandatory trailer -- so a plain v1 payload (no trailer at all)
/// fails here rather than silently decoding as "no provenance": once [`decode_batch`] consumes
/// every byte a v1 encoding has, the trailer-length `read_uvarint` below finds nothing left and
/// errors, exactly the "no proper prefix of a valid encoding is itself valid" property this
/// module holds for v1.
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
            _unknown => { /* forward compatibility -- a future field is skipped, not rejected */ }
        }
    }
    Ok((batch, provenance))
}

fn trailer_str(bytes: &[u8]) -> Result<&str, CodecError> {
    std::str::from_utf8(bytes)
        .map_err(|e| CodecError::Malformed(format!("provenance trailer field not utf-8: {e}")))
}

/// Encodes an [`EventBatch`] to a complete, framed byte string -- [`encode_batch`]'s payload
/// wrapped by [`crate::frame::write_frame`] under [`CODEC_NATIVE_V1`]. The `logit_proto::Encoder`
/// implementor.
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

/// The `logit_proto::Decoder` implementor. Unlike every other decoder in this crate,
/// [`NativeDecoder::decode_into`] ignores `received_at`: a native frame's events already carry
/// their own original timestamps end to end (that fidelity is the entire point of this format
/// existing), so there is no "receipt time" to stamp over them the way `statsd_in`/`syslog_in`
/// do for protocols that don't reliably carry one.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeDecoder;

impl Decoder for NativeDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        _received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<Arc<Resource>, CodecError> {
        let mut bytes = bytes;
        let (codec, mut payload) = read_frame(&mut bytes)?;
        if codec != CODEC_NATIVE_V1 {
            return Err(CodecError::Unsupported(format!(
                "frame codec byte {codec}, expected native v1 ({CODEC_NATIVE_V1})"
            )));
        }
        let batch = decode_batch(&mut payload)?;
        out.extend(batch.events);
        Ok(batch.resource)
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
        let resource = Arc::new(Resource { attributes: resource_attrs });

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
            },
        );

        let mut sketch = DdSketch::new();
        sketch.add(0.5);
        let metric_event = Event::metric(
            2,
            AttrMap::new(),
            MetricRecord {
                name: logit_core::interner::intern("mod_test_metric"),
                kind: MetricKind::Distribution(sketch),
                unit: Some(logit_core::interner::intern("s")),
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
            },
        );

        EventBatch { resource, events: vec![log_event, metric_event, span_event] }
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
        let resource = decoder.decode_into(framed, 999, &mut events).unwrap();

        assert_eq!(resource.attributes, batch.resource.attributes);
        assert_eq!(events.len(), 3);
        // The whole point of this decoder: original timestamps survive, `received_at` (999) never
        // overwrites them.
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
        let resource = decoder.decode_into(framed, 0, &mut events).unwrap();
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
        let batch = EventBatch { resource: Arc::new(Resource::default()), events: Vec::new() };
        let payload = encode_batch(&batch);
        let decoded = decode_batch(&mut payload.clone()).unwrap();
        assert!(decoded.events.is_empty());
        assert!(decoded.resource.attributes.is_empty());
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
        // The on-disk half of the requirement: a "file" here is just two encoded batches back to
        // back, with no shared dictionary or index between them.
        let mut encoder = NativeEncoder::default();
        let batch_a = sample_batch();
        let mut batch_b = sample_batch();
        batch_b.events.truncate(1);

        let mut file = BytesMut::new();
        file.extend_from_slice(&encoder.encode(&batch_a).unwrap());
        file.extend_from_slice(&encoder.encode(&batch_b).unwrap());
        let mut cursor = file.freeze();

        // `decode_into` (the `Decoder` trait method) takes one already-framed buffer at a time --
        // finding successive frame boundaries in a longer buffer (a file, a stream) is
        // `read_frame`'s job, driven in a loop by the caller. Exercise that loop directly, the
        // shape a file reader would actually use.
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

    /// An absent field costs no dictionary entry and no tag byte at all -- the trailer for two
    /// absent fields is exactly the one-byte `trailer_len = 0` varint.
    #[test]
    fn an_absent_provenance_field_costs_one_byte_total() {
        let batch = sample_batch();
        let without = encode_batch(&batch);
        let with_empty_provenance = encode_batch_v2(&batch, Provenance::default());
        assert_eq!(with_empty_provenance.len(), without.len() + 1);
    }

    /// The core invariant this module holds for v1 (`crates/logit-proto/tests/robustness.rs`'s
    /// `assert_every_truncation_fails_cleanly`) must also hold for v2's trailer: no proper prefix
    /// of a valid v2 encoding decodes successfully. This is what the mandatory length-prefixed
    /// trailer buys over an optional one.
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

    /// A plain v1 payload has no trailer at all -- fed to `decode_batch_v2`, `decode_batch` inside
    /// it consumes every byte, and the trailer-length read then finds nothing left. This must fail
    /// rather than silently decode as "no provenance": v1 and v2 are distinct codecs, not one a
    /// superset of the other, and `logit_in`'s codec dispatch (not this function) is what decides
    /// which to call.
    #[test]
    fn decode_batch_v2_rejects_a_plain_v1_payload() {
        let mut v1_payload = encode_batch(&sample_batch());
        assert!(decode_batch_v2(&mut v1_payload).is_err());
    }

    /// An unrecognized trailer tag is skipped, not rejected -- the same forward-compatibility
    /// contract `record::read_event`'s field loop holds for a future `Event` field.
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
