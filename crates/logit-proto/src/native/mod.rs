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
//! - A payload is consumed whole: every length-carved section, field, and value rejects bytes its
//!   reader left over (`varint::ensure_consumed`), so a payload decodes only if re-encoding it
//!   reproduces it byte for byte.
//! - Every decode is charged against a [`DecodeBudget`] (see [`budget`]), so a small payload
//!   can't expand past a bounded multiple of the frame cap.
//!
//! These two rules, and the budget's 4x multiplier, are decided in ADR `untrusted-input-bounds`.

pub mod budget;
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
use crate::native::varint::{ensure_consumed, read_u8, read_uvarint, uvarint_len, write_uvarint};

pub use crate::native::budget::{DecodeBudget, DEFAULT_DECODE_BUDGET};
use crate::{CodecError, Decoder, Encoder};

/// The frame header's `codec` byte for a bare batch ([`encode_batch`]): the file format
/// (`format: native`) and the perf harness's telemetry dump. A file has no sender and no hop, so
/// it carries no trailer.
pub const CODEC_BATCH: u8 = 1;

/// The frame header's `codec` byte for a hop batch ([`encode_hop_batch`]): a prefix carrying the
/// batch's required [`SeqId`] (ADR `native-hop-identity-and-sequence`), [`encode_batch`]'s
/// payload, then a mandatory length-prefixed trailer carrying its [`Provenance`] (ADR
/// `batch-provenance-on-delivered`). It's what `logit_out` sends and what the disk spool records
/// (ADR `native-hop-no-compatibility`).
///
/// The pair leads so a reader names the frame before it decodes the batch: `logit_in` answers a
/// batch it can't take with a rejected `Ack` naming that pair (ADR `native-hop-ack-status`).
///
/// A separate codec rather than an optional prefix and trailer on [`CODEC_BATCH`], which would
/// let a payload truncated at the trailer boundary decode as "no provenance".
pub const CODEC_HOP_BATCH: u8 = 2;

/// The longest hop prefix: the 16-byte identity and a 10-byte uvarint sequence. A reader holding
/// this many bytes of a hop payload, or all of a shorter one, can read its [`SeqId`]
/// ([`read_hop_prefix`]).
pub const HOP_PREFIX_MAX_LEN: usize = 16 + 10;

const TRAILER_TAG_ORIGIN: u8 = 1;
const TRAILER_TAG_PREVIOUS: u8 = 2;

/// Bounds a trailer field's declared length before it slices `bytes`; a component id is far
/// shorter.
const MAX_SANE_TRAILER_FIELD_BYTES: usize = 4096;

/// One batch's place on the native hop: the identity of the sink store it entered and its number
/// there (ADR `native-hop-identity-and-sequence`). `seq` starts at 1; a decoded 0 is malformed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeqId {
    pub id: [u8; 16],
    pub seq: u64,
}

/// Encodes one [`EventBatch`] into the bare [`CODEC_BATCH`] payload: dictionary, len-prefixed
/// [`Resource`] TLV, mandatory [`logit_core::Scope`] section (presence byte, then a len-prefixed
/// TLV if present), then a counted list of len-prefixed events (`docs/design/wire-protocol.md`'s
/// "Batch grammar").
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

/// A first, cheap check on a declared event count, like [`dict::Dict::read`]'s cap. The real
/// bound is the [`DecodeBudget`]: at `size_of::<Event>()` a slot, this many events would need
/// ~14 GiB, far past any budget.
const MAX_SANE_EVENT_COUNT: usize = 16 * 1024 * 1024;

/// The inverse of [`encode_batch`], charging `budget` as it decodes. Bytes after the last event
/// are `Malformed`.
pub fn decode_batch(bytes: &mut Bytes, budget: &DecodeBudget) -> Result<EventBatch, CodecError> {
    let batch = decode_batch_body(bytes, budget)?;
    ensure_consumed(bytes, "batch")?;
    Ok(batch)
}

/// [`decode_batch`] without the end-of-payload check, which [`decode_hop_body`] makes after its
/// trailer instead.
fn decode_batch_body(bytes: &mut Bytes, budget: &DecodeBudget) -> Result<EventBatch, CodecError> {
    let dict = Dict::read(bytes, budget)?;

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
    // An event is at least its one-byte length prefix.
    budget.charge_list("batch", event_count, 1, bytes.len(), std::mem::size_of::<Event>())?;
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

/// The [`CODEC_HOP_BATCH`] payload: the prefix (`seq.id`, 16 bytes, then `seq.seq` as a
/// uvarint), [`encode_batch`]'s payload, then the provenance trailer.
///
/// The trailer is `tag(u8) + len(uvarint) + payload` entries with strings inline, not
/// dictionary-indexed: two strings per batch give a dictionary nothing to amortize. An absent
/// provenance field writes no entry, but the trailer's length prefix is always written.
///
/// The payload's length is computed first and the prefix and trailer written straight into the
/// output: a separate buffer costs allocations that `disk_queue_push_one_batch` pins.
pub fn encode_hop_batch(batch: &EventBatch, provenance: Provenance, seq: SeqId) -> Bytes {
    let body = encode_batch(batch);

    let origin = provenance.origin_str();
    let previous = provenance.previous_str();
    let field_len = |len: usize| 1 + uvarint_len(len as u64) + len;
    let trailer_len =
        origin.map_or(0, |s| field_len(s.len())) + previous.map_or(0, |s| field_len(s.len()));

    let prefix_len = seq.id.len() + uvarint_len(seq.seq);
    let total = prefix_len + body.len() + uvarint_len(trailer_len as u64) + trailer_len;
    let mut out = BytesMut::with_capacity(total);
    out.extend_from_slice(&seq.id);
    write_uvarint(&mut out, seq.seq);
    out.extend_from_slice(&body);
    write_uvarint(&mut out, trailer_len as u64);
    if let Some(origin) = origin {
        write_trailer_field(&mut out, TRAILER_TAG_ORIGIN, origin.as_bytes());
    }
    if let Some(previous) = previous {
        write_trailer_field(&mut out, TRAILER_TAG_PREVIOUS, previous.as_bytes());
    }
    debug_assert_eq!(out.len(), total);
    out.freeze()
}

fn write_trailer_field(out: &mut BytesMut, tag: u8, value: &[u8]) {
    out.extend_from_slice(&[tag]);
    write_uvarint(out, value.len() as u64);
    out.extend_from_slice(value);
}

/// Reads a hop payload's prefix off the front of `bytes`, leaving the batch and its trailer. A
/// prefix shorter than 16 bytes plus a whole uvarint, or a sequence of 0, is `Malformed`.
///
/// `logit_in` reads the prefix alone, then [`decode_hop_body`], so a body it can't take is still
/// named; [`decode_hop_batch`] is the two together.
pub fn read_hop_prefix(bytes: &mut Bytes) -> Result<SeqId, CodecError> {
    if bytes.len() < 16 {
        return Err(CodecError::Malformed(format!(
            "hop prefix needs a 16-byte sender identity, but only {} bytes remain",
            bytes.len()
        )));
    }
    let id: [u8; 16] = bytes.split_to(16)[..].try_into().expect("split_to(16) yields 16 bytes");
    let seq = read_uvarint(bytes)
        .map_err(|e| CodecError::Malformed(format!("hop prefix sequence: {e}")))?;
    if seq == 0 {
        return Err(CodecError::Malformed("hop prefix sequence is 0".into()));
    }
    Ok(SeqId { id, seq })
}

/// What follows a hop payload's prefix: the batch, charged against `budget`, then the provenance
/// trailer, which costs the budget nothing. Bytes after the trailer are `Malformed`, as is any
/// trailer field that overruns the trailer or the field cap, whatever its tag, and a payload with
/// no trailer length at all: a bare [`CODEC_BATCH`] payload fails here rather than decoding as "no
/// provenance".
pub fn decode_hop_body(
    bytes: &mut Bytes,
    budget: &DecodeBudget,
) -> Result<(EventBatch, Provenance), CodecError> {
    let batch = decode_batch_body(bytes, budget)?;

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
    ensure_consumed(bytes, "batch")?;
    Ok((batch, provenance))
}

/// The inverse of [`encode_hop_batch`]: [`read_hop_prefix`], then [`decode_hop_body`]. The sender
/// pair is required (ADR `native-hop-no-compatibility`, decision 2).
pub fn decode_hop_batch(
    bytes: &mut Bytes,
    budget: &DecodeBudget,
) -> Result<(EventBatch, Provenance, SeqId), CodecError> {
    let seq = read_hop_prefix(bytes)?;
    let (batch, provenance) = decode_hop_body(bytes, budget)?;
    Ok((batch, provenance, seq))
}

fn trailer_str(bytes: &[u8]) -> Result<&str, CodecError> {
    std::str::from_utf8(bytes)
        .map_err(|e| CodecError::Malformed(format!("provenance trailer field not utf-8: {e}")))
}

/// The file format's [`Encoder`]: [`encode_batch`]'s payload framed under [`CODEC_BATCH`].
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
        write_frame(CODEC_BATCH, self.compression, &payload)
    }
}

/// The file format's [`Decoder`], for [`CODEC_BATCH`] frames. Ignores `received_at`: a native
/// event keeps its original timestamp end to end. Decodes each frame under
/// [`DEFAULT_DECODE_BUDGET`].
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
        if codec != CODEC_BATCH {
            return Err(CodecError::Unsupported(format!(
                "frame codec byte {codec}, expected the bare native batch ({CODEC_BATCH})"
            )));
        }
        let EventBatch { resource, scope, events } =
            decode_batch(&mut payload, &DecodeBudget::default())?;
        // Moved, not copied, into an empty `out`, so each event is held once at peak.
        if out.is_empty() {
            *out = events;
        } else {
            out.extend(events);
        }
        Ok((resource, scope))
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
        let decoded = decode_batch(&mut payload.clone(), &DecodeBudget::default()).unwrap();

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
        let decoded = decode_batch(&mut payload.clone(), &DecodeBudget::default()).unwrap();
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
        let decoded = decode_batch(&mut payload.clone(), &DecodeBudget::default()).unwrap();
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
        assert_eq!(codec_a, CODEC_BATCH);
        let decoded_a = decode_batch(&mut payload_a, &DecodeBudget::default()).unwrap();
        assert_eq!(decoded_a.events.len(), batch_a.events.len());

        let (codec_b, mut payload_b) = read_frame(&mut cursor).unwrap();
        assert_eq!(codec_b, CODEC_BATCH);
        let decoded_b = decode_batch(&mut payload_b, &DecodeBudget::default()).unwrap();
        assert_eq!(decoded_b.events.len(), 1);
        assert!(cursor.is_empty(), "both frames should be fully consumed");
    }

    fn sample_provenance() -> Provenance {
        Provenance {
            origin: Some(logit_core::interner::intern("mod_test_nginx_in")),
            previous: Some(logit_core::interner::intern("mod_test_enrich")),
        }
    }

    fn sample_seq() -> SeqId {
        SeqId { id: *b"mod-test-sender!", seq: 300 }
    }

    #[test]
    fn encode_decode_hop_batch_round_trips_provenance_and_the_pair() {
        let batch = sample_batch();
        let mut payload = encode_hop_batch(&batch, sample_provenance(), sample_seq());
        let (decoded, provenance, seq) =
            decode_hop_batch(&mut payload, &DecodeBudget::default()).unwrap();

        assert_eq!(decoded.events.len(), batch.events.len());
        assert_eq!(provenance, sample_provenance());
        assert_eq!(seq, sample_seq());
        assert!(payload.is_empty(), "decode_hop_batch should consume the whole payload");
    }

    #[test]
    fn encode_decode_hop_batch_round_trips_both_provenance_fields_absent() {
        let batch = sample_batch();
        let mut payload = encode_hop_batch(&batch, Provenance::default(), sample_seq());
        let (_decoded, provenance, seq) =
            decode_hop_batch(&mut payload, &DecodeBudget::default()).unwrap();
        assert_eq!(provenance, Provenance::default());
        assert_eq!(seq, sample_seq());
    }

    #[test]
    fn seq_id_sizes() {
        assert_eq!(std::mem::size_of::<SeqId>(), 24);
        assert_eq!(std::mem::size_of::<Option<SeqId>>(), 32);
    }

    /// An absent provenance field writes no entry: a trailer with no provenance is its length
    /// prefix alone.
    #[test]
    fn an_absent_provenance_field_costs_nothing() {
        let batch = sample_batch();
        let pair_only = encode_hop_batch(&batch, Provenance::default(), sample_seq());
        let origin = logit_core::interner::intern("mod_test_nginx_in");
        let with_origin = encode_hop_batch(
            &batch,
            Provenance { origin: Some(origin), previous: None },
            sample_seq(),
        );
        // One tag byte, a one-byte length, and the string.
        assert_eq!(with_origin.len(), pair_only.len() + 2 + "mod_test_nginx_in".len());
    }

    /// Over the bare batch, the 16-byte identity, the uvarint sequence, and the one-byte trailer
    /// length.
    #[test]
    fn a_hop_batch_costs_the_pair_and_nothing_more() {
        let batch = sample_batch();
        let without = encode_batch(&batch);
        let id = [7; 16];
        let one = encode_hop_batch(&batch, Provenance::default(), SeqId { id, seq: 1 });
        assert_eq!(one.len(), 16 + 1 + without.len() + 1);
        assert_eq!(&one[..17], &[[7; 16].as_slice(), &[1]].concat()[..], "the pair leads");
        let two_byte = encode_hop_batch(&batch, Provenance::default(), SeqId { id, seq: 128 });
        assert_eq!(two_byte.len(), 16 + 2 + without.len() + 1);
    }

    type TrailerField<'a> = (u8, &'a [u8]);

    /// `prefix`, the bare batch payload, and a trailer of `fields` (each `(tag, value)` written
    /// as-is), as one hop payload.
    fn hop_with(prefix: &[u8], fields: &[TrailerField]) -> Bytes {
        let mut trailer = BytesMut::new();
        for (tag, value) in fields {
            write_trailer_field(&mut trailer, *tag, value);
        }
        let mut out = BytesMut::new();
        out.extend_from_slice(prefix);
        out.extend_from_slice(&encode_batch(&sample_batch()));
        write_uvarint(&mut out, trailer.len() as u64);
        out.extend_from_slice(&trailer);
        out.freeze()
    }

    const ID: &[u8; 16] = b"mod-test-sender!";

    #[test]
    fn read_hop_prefix_reads_the_pair_and_leaves_the_rest() {
        for seq in [1, 127, 128, u64::MAX] {
            let mut payload =
                encode_hop_batch(&sample_batch(), sample_provenance(), SeqId { id: *ID, seq });
            assert_eq!(read_hop_prefix(&mut payload).unwrap(), SeqId { id: *ID, seq });
            let (_batch, provenance) =
                decode_hop_body(&mut payload, &DecodeBudget::default()).unwrap();
            assert_eq!(provenance, sample_provenance());
            assert!(payload.is_empty());
        }
    }

    /// The longest prefix fits [`HOP_PREFIX_MAX_LEN`], so a reader holding that many bytes can
    /// name any frame.
    #[test]
    fn the_longest_prefix_is_hop_prefix_max_len() {
        let payload = encode_hop_batch(
            &sample_batch(),
            sample_provenance(),
            SeqId { id: *ID, seq: u64::MAX },
        );
        let mut prefix = payload.slice(..HOP_PREFIX_MAX_LEN);
        assert_eq!(read_hop_prefix(&mut prefix).unwrap().seq, u64::MAX);
        assert!(prefix.is_empty());
    }

    #[test]
    fn a_truncated_or_zero_prefix_is_malformed() {
        // A 10th byte over 1 sets bits past 63.
        let mut overflowing = [0xff; 10];
        overflowing[9] = 0x02;
        let cases: &[(&str, Vec<u8>)] = &[
            ("empty", vec![]),
            ("15-byte identity", ID[..15].to_vec()),
            ("identity only", ID.to_vec()),
            ("truncated sequence", [&ID[..], &[0x81]].concat()),
            ("sequence 0", [&ID[..], &[0]].concat()),
            ("overflowing sequence", [&ID[..], &overflowing].concat()),
            ("eleven-byte sequence", [&ID[..], &[0x80; 10], &[0]].concat()),
        ];
        for (name, prefix) in cases {
            match read_hop_prefix(&mut Bytes::from(prefix.clone())) {
                Err(CodecError::Malformed(_)) => {}
                other => panic!("{name}: expected Malformed, got {other:?}"),
            }
        }
    }

    /// The trailer's fields obey its cap whatever their tag.
    #[test]
    fn an_over_cap_trailer_field_is_malformed() {
        let big = vec![0u8; MAX_SANE_TRAILER_FIELD_BYTES + 1];
        let mut payload = hop_with(&[&ID[..], &[1]].concat(), &[(99, &big)]);
        assert!(decode_hop_batch(&mut payload, &DecodeBudget::default()).is_err());
    }

    /// No proper prefix of a valid hop encoding decodes, trailer included.
    #[test]
    fn decode_hop_batch_rejects_every_proper_prefix_of_a_valid_encoding() {
        let valid = encode_hop_batch(&sample_batch(), sample_provenance(), sample_seq());
        for len in 0..valid.len() {
            let mut truncated = valid.slice(0..len);
            assert!(
                decode_hop_batch(&mut truncated, &DecodeBudget::default()).is_err(),
                "a {len}-byte truncation of a valid hop payload decoded successfully"
            );
        }
    }

    /// A bare batch payload fed to `decode_hop_batch` fails rather than decoding as "no
    /// provenance".
    #[test]
    fn decode_hop_batch_rejects_a_bare_batch_payload() {
        let mut bare = encode_batch(&sample_batch());
        assert!(decode_hop_batch(&mut bare, &DecodeBudget::default()).is_err());
    }

    /// An unrecognized trailer tag is skipped, not rejected.
    #[test]
    fn decode_hop_batch_skips_an_unrecognized_trailer_tag() {
        let mut payload = hop_with(
            &[&ID[..], &[1]].concat(),
            &[
                (TRAILER_TAG_ORIGIN, b"mod_test_skip_origin"),
                (99, b"a field this reader doesn't know"),
            ],
        );
        let (_decoded, provenance, seq) =
            decode_hop_batch(&mut payload, &DecodeBudget::default()).unwrap();
        assert_eq!(provenance.origin_str(), Some("mod_test_skip_origin"));
        assert_eq!(seq, SeqId { id: *ID, seq: 1 });
    }
}
