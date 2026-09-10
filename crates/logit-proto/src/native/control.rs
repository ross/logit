//! The `logit_in`/`logit_out` connection-level control messages: `Hello`/`HelloAck` (the
//! version/codec/compression handshake), `Ack` (per-batch acknowledgement), and `Reject` (a clean
//! refusal, e.g. no common codec). These travel inside an ordinary [`crate::frame`] frame with
//! [`crate::frame::FLAG_CONTROL`] set -- a control frame's `codec` byte is meaningless (there is no
//! payload codec to name), and its `compression` is always [`crate::frame::Compression::None`]:
//! these messages are tiny and fixed-shape, not worth compressing, and compression itself is one of
//! the things `Hello`/`HelloAck` are still negotiating.
//!
//! **Same TLV shape as [`crate::native::record`], same reason.** Every message field is
//! `tag(u8) + len(uvarint) + payload(len bytes)`, so a field this reader doesn't recognize (a
//! later protocol version's addition) is skipped whole rather than corrupting the rest of the
//! message -- see [`decode`]'s own doc comment. `crates/logit-inputs/src/logit.rs` and
//! `crates/logit-outputs/src/logit.rs` are the only callers; this module itself stays sync and
//! tokio-free, like the rest of `logit-proto`.

use bytes::{Bytes, BytesMut};

use crate::native::varint::{read_u8, read_uvarint, write_uvarint};
use crate::CodecError;

/// The connection-protocol version `Hello`/`HelloAck` negotiate over -- independent of
/// [`crate::frame::VERSION`] (the frame *header's* version), since the frame envelope and the
/// connection handshake can evolve on separate schedules.
pub const PROTOCOL_VERSION: u16 = 1;

/// A [`Reject`] reason -- codes, not an enum, so a future reason can be added without a version
/// bump (an old reader that doesn't recognize a code still has the human-readable `message` to
/// fall back on).
pub const REJECT_VERSION_MISMATCH: u16 = 1;
pub const REJECT_NO_COMMON_CODEC: u16 = 2;
pub const REJECT_FRAME_TOO_LARGE: u16 = 3;
pub const REJECT_GOING_AWAY: u16 = 4;
pub const REJECT_INTERNAL: u16 = 5;

/// `Reject.message` is operator/log-facing text, not wire-critical data -- bounded so a hostile or
/// buggy peer can't force an unbounded allocation with it. Same reasoning as
/// `crate::native::dict`'s `MAX_SANE_DICT_ENTRIES`.
const MAX_REJECT_MESSAGE_BYTES: usize = 1024;

/// `Hello.codecs`/`Hello.compressions` and `HelloAck`'s own single choices are drawn from a small,
/// fixed universe (`crate::frame::Compression` has 3 variants total) -- 16 is already generous
/// headroom for either list to grow, and bounds the allocation a hostile peer's declared count can
/// force before a single byte of the list has been read.
const MAX_CHOICE_LIST_ENTRIES: usize = 16;

const MSG_HELLO: u8 = 1;
const MSG_HELLO_ACK: u8 = 2;
const MSG_ACK: u8 = 3;
const MSG_REJECT: u8 = 4;

// -- shared TLV field helpers, same shape as `native::record`'s own `write_field`/tag loop --------

fn write_field(out: &mut BytesMut, tag: u8, build: impl FnOnce(&mut BytesMut)) {
    let mut tmp = BytesMut::new();
    build(&mut tmp);
    out.extend_from_slice(&[tag]);
    write_uvarint(out, tmp.len() as u64);
    out.extend_from_slice(&tmp);
}

/// Reads one `tag + len + payload` field off the front of `body`, honouring `PROTOCOL_VERSION`-
/// exceeded declared bounds via `body.len()` itself (a length-prefixed field can never claim more
/// than what the connection actually sent). Returns `None` once `body` is exhausted.
fn read_field(body: &mut Bytes) -> Result<Option<(u8, Bytes)>, CodecError> {
    if body.is_empty() {
        return Ok(None);
    }
    let tag = read_u8(body)?;
    let len = read_uvarint(body)? as usize;
    if body.len() < len {
        return Err(CodecError::Malformed(format!(
            "control field {tag} declares {len} bytes but only {} remain",
            body.len()
        )));
    }
    Ok(Some((tag, body.split_to(len))))
}

fn write_u16(out: &mut BytesMut, tag: u8, v: u16) {
    write_field(out, tag, |buf| write_uvarint(buf, v as u64));
}

fn write_u32(out: &mut BytesMut, tag: u8, v: u32) {
    write_field(out, tag, |buf| write_uvarint(buf, v as u64));
}

fn write_bytes_field(out: &mut BytesMut, tag: u8, v: &[u8]) {
    write_field(out, tag, |buf| buf.extend_from_slice(v));
}

fn read_u16_field(mut field: Bytes) -> Result<u16, CodecError> {
    let v = read_uvarint(&mut field)?;
    u16::try_from(v).map_err(|_| CodecError::Malformed(format!("field value {v} doesn't fit u16")))
}

fn read_u32_field(mut field: Bytes) -> Result<u32, CodecError> {
    let v = read_uvarint(&mut field)?;
    u32::try_from(v).map_err(|_| CodecError::Malformed(format!("field value {v} doesn't fit u32")))
}

fn read_choice_list(field: Bytes, what: &str) -> Result<Vec<u8>, CodecError> {
    if field.len() > MAX_CHOICE_LIST_ENTRIES {
        return Err(CodecError::Malformed(format!(
            "{what} declares {} entries, over the {MAX_CHOICE_LIST_ENTRIES} sanity cap",
            field.len()
        )));
    }
    Ok(field.to_vec())
}

// -- Hello -----------------------------------------------------------------------------------

/// Sent first, by the connecting side (`logit_out`) -- offers a protocol version and every
/// codec/compression it can speak, plus the largest frame it will accept and the flow-control
/// window it advertises. `window` is negotiated and recorded even though this plan's sender only
/// ever uses 1 in flight -- see `docs/plans/native-transport.md`'s "In-flight" decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub version: u16,
    /// `crate::native::CODEC_NATIVE_V1`-shaped codec bytes this side can decode, in preference
    /// order. Bounded to [`MAX_CHOICE_LIST_ENTRIES`] on decode.
    pub codecs: Vec<u8>,
    /// `crate::frame::Compression as u8`-shaped bytes this side can decompress, in preference
    /// order. Bounded to [`MAX_CHOICE_LIST_ENTRIES`] on decode.
    pub compressions: Vec<u8>,
    pub max_frame_bytes: u32,
    pub window: u32,
}

const HELLO_FIELD_VERSION: u8 = 1;
const HELLO_FIELD_CODECS: u8 = 2;
const HELLO_FIELD_COMPRESSIONS: u8 = 3;
const HELLO_FIELD_MAX_FRAME_BYTES: u8 = 4;
const HELLO_FIELD_WINDOW: u8 = 5;

impl Hello {
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_HELLO]);
        write_u16(&mut out, HELLO_FIELD_VERSION, self.version);
        write_bytes_field(&mut out, HELLO_FIELD_CODECS, &self.codecs);
        write_bytes_field(&mut out, HELLO_FIELD_COMPRESSIONS, &self.compressions);
        write_u32(&mut out, HELLO_FIELD_MAX_FRAME_BYTES, self.max_frame_bytes);
        write_u32(&mut out, HELLO_FIELD_WINDOW, self.window);
        out.freeze()
    }

    /// Decodes a `Hello` whose leading message-type byte has already been read and checked by the
    /// caller ([`decode`]) -- `body` is just the TLV field stream that follows it.
    fn decode_fields(mut body: Bytes) -> Result<Self, CodecError> {
        let mut version = 0u16;
        let mut codecs = Vec::new();
        let mut compressions = Vec::new();
        let mut max_frame_bytes = 0u32;
        let mut window = 0u32;
        while let Some((tag, field)) = read_field(&mut body)? {
            match tag {
                HELLO_FIELD_VERSION => version = read_u16_field(field)?,
                HELLO_FIELD_CODECS => codecs = read_choice_list(field, "Hello.codecs")?,
                HELLO_FIELD_COMPRESSIONS => {
                    compressions = read_choice_list(field, "Hello.compressions")?
                }
                HELLO_FIELD_MAX_FRAME_BYTES => max_frame_bytes = read_u32_field(field)?,
                HELLO_FIELD_WINDOW => window = read_u32_field(field)?,
                // Forward compatibility -- see this module's doc comment.
                _unknown => {}
            }
        }
        Ok(Hello { version, codecs, compressions, max_frame_bytes, window })
    }

    /// Decodes a whole `Hello` control payload, message-type byte included. Fails with
    /// [`CodecError::Malformed`] if the payload's message type isn't `Hello`'s.
    pub fn decode(bytes: &mut Bytes) -> Result<Self, CodecError> {
        expect_msg_type(bytes, MSG_HELLO, "Hello")?;
        Self::decode_fields(bytes.split_off(0))
    }
}

// -- HelloAck ----------------------------------------------------------------------------------

/// The server's reply to a valid [`Hello`]: the chosen (intersection) codec and compression, this
/// listener's own frame-size ceiling, and its advertised window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HelloAck {
    pub version: u16,
    pub codec: u8,
    pub compression: u8,
    pub max_frame_bytes: u32,
    pub window: u32,
}

const HELLO_ACK_FIELD_VERSION: u8 = 1;
const HELLO_ACK_FIELD_CODEC: u8 = 2;
const HELLO_ACK_FIELD_COMPRESSION: u8 = 3;
const HELLO_ACK_FIELD_MAX_FRAME_BYTES: u8 = 4;
const HELLO_ACK_FIELD_WINDOW: u8 = 5;

impl HelloAck {
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_HELLO_ACK]);
        write_u16(&mut out, HELLO_ACK_FIELD_VERSION, self.version);
        write_field(&mut out, HELLO_ACK_FIELD_CODEC, |buf| buf.extend_from_slice(&[self.codec]));
        write_field(&mut out, HELLO_ACK_FIELD_COMPRESSION, |buf| {
            buf.extend_from_slice(&[self.compression])
        });
        write_u32(&mut out, HELLO_ACK_FIELD_MAX_FRAME_BYTES, self.max_frame_bytes);
        write_u32(&mut out, HELLO_ACK_FIELD_WINDOW, self.window);
        out.freeze()
    }

    fn decode_fields(mut body: Bytes) -> Result<Self, CodecError> {
        let mut version = 0u16;
        let mut codec = 0u8;
        let mut compression = 0u8;
        let mut max_frame_bytes = 0u32;
        let mut window = 0u32;
        while let Some((tag, mut field)) = read_field(&mut body)? {
            match tag {
                HELLO_ACK_FIELD_VERSION => version = read_u16_field(field)?,
                HELLO_ACK_FIELD_CODEC => codec = read_u8(&mut field)?,
                HELLO_ACK_FIELD_COMPRESSION => compression = read_u8(&mut field)?,
                HELLO_ACK_FIELD_MAX_FRAME_BYTES => max_frame_bytes = read_u32_field(field)?,
                HELLO_ACK_FIELD_WINDOW => window = read_u32_field(field)?,
                _unknown => {}
            }
        }
        Ok(HelloAck { version, codec, compression, max_frame_bytes, window })
    }

    pub fn decode(bytes: &mut Bytes) -> Result<Self, CodecError> {
        expect_msg_type(bytes, MSG_HELLO_ACK, "HelloAck")?;
        Self::decode_fields(bytes.split_off(0))
    }
}

// -- Ack -------------------------------------------------------------------------------------

/// Cumulative acknowledgement: "I have forwarded every data frame up to and including sequence
/// `seq`." Sequence numbers are implicit -- TCP is ordered, so the Nth data frame on a connection
/// is always seq N -- see `docs/plans/native-transport.md`'s "Sequence numbers" decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    pub seq: u64,
}

const ACK_FIELD_SEQ: u8 = 1;

impl Ack {
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_ACK]);
        write_field(&mut out, ACK_FIELD_SEQ, |buf| write_uvarint(buf, self.seq));
        out.freeze()
    }

    fn decode_fields(mut body: Bytes) -> Result<Self, CodecError> {
        let mut seq = 0u64;
        while let Some((tag, mut field)) = read_field(&mut body)? {
            match tag {
                ACK_FIELD_SEQ => seq = read_uvarint(&mut field)?,
                _unknown => {}
            }
        }
        Ok(Ack { seq })
    }

    pub fn decode(bytes: &mut Bytes) -> Result<Self, CodecError> {
        expect_msg_type(bytes, MSG_ACK, "Ack")?;
        Self::decode_fields(bytes.split_off(0))
    }
}

// -- Reject ------------------------------------------------------------------------------------

/// A clean refusal -- version mismatch, no common codec, an oversized frame, a graceful
/// going-away, or an internal error -- always followed by the sender closing the connection. See
/// the `REJECT_*` constants above for `code`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reject {
    pub code: u16,
    pub message: String,
}

const REJECT_FIELD_CODE: u8 = 1;
const REJECT_FIELD_MESSAGE: u8 = 2;

impl Reject {
    pub fn encode(&self) -> Bytes {
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_REJECT]);
        write_u16(&mut out, REJECT_FIELD_CODE, self.code);
        write_bytes_field(&mut out, REJECT_FIELD_MESSAGE, self.message.as_bytes());
        out.freeze()
    }

    fn decode_fields(mut body: Bytes) -> Result<Self, CodecError> {
        let mut code = 0u16;
        let mut message = String::new();
        while let Some((tag, field)) = read_field(&mut body)? {
            match tag {
                REJECT_FIELD_CODE => code = read_u16_field(field)?,
                REJECT_FIELD_MESSAGE => {
                    if field.len() > MAX_REJECT_MESSAGE_BYTES {
                        return Err(CodecError::Malformed(format!(
                            "Reject.message is {} bytes, over the {MAX_REJECT_MESSAGE_BYTES} \
                             sanity cap",
                            field.len()
                        )));
                    }
                    message = String::from_utf8_lossy(&field).into_owned();
                }
                _unknown => {}
            }
        }
        Ok(Reject { code, message })
    }

    pub fn decode(bytes: &mut Bytes) -> Result<Self, CodecError> {
        expect_msg_type(bytes, MSG_REJECT, "Reject")?;
        Self::decode_fields(bytes.split_off(0))
    }
}

// -- dispatch: read a control payload without knowing its type ahead of time -----------------

/// One of the four control messages -- what a reader that doesn't yet know which message is
/// coming next (any point after the handshake, where either side may send `Ack` or `Reject`)
/// decodes into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlMessage {
    Hello(Hello),
    HelloAck(HelloAck),
    Ack(Ack),
    Reject(Reject),
}

impl ControlMessage {
    pub fn encode(&self) -> Bytes {
        match self {
            ControlMessage::Hello(m) => m.encode(),
            ControlMessage::HelloAck(m) => m.encode(),
            ControlMessage::Ack(m) => m.encode(),
            ControlMessage::Reject(m) => m.encode(),
        }
    }

    /// Reads the leading message-type byte and dispatches to the matching message's own field
    /// decoder -- an unrecognized message type is a decode error (unlike an unrecognized *field*
    /// within a known message, which is skipped): a message type this reader has never heard of
    /// carries no field layout it could possibly make sense of.
    pub fn decode(bytes: &mut Bytes) -> Result<Self, CodecError> {
        let msg_type = read_u8(bytes)?;
        let body = bytes.split_off(0);
        match msg_type {
            MSG_HELLO => Ok(ControlMessage::Hello(Hello::decode_fields(body)?)),
            MSG_HELLO_ACK => Ok(ControlMessage::HelloAck(HelloAck::decode_fields(body)?)),
            MSG_ACK => Ok(ControlMessage::Ack(Ack::decode_fields(body)?)),
            MSG_REJECT => Ok(ControlMessage::Reject(Reject::decode_fields(body)?)),
            other => Err(CodecError::Malformed(format!("unknown control message type {other}"))),
        }
    }
}

fn expect_msg_type(bytes: &mut Bytes, expected: u8, name: &str) -> Result<(), CodecError> {
    let msg_type = read_u8(bytes)?;
    if msg_type != expected {
        return Err(CodecError::Malformed(format!(
            "expected a {name} control message (type {expected}), got type {msg_type}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_round_trips() {
        let hello = Hello {
            version: PROTOCOL_VERSION,
            codecs: vec![1],
            compressions: vec![0, 1],
            max_frame_bytes: 64 * 1024 * 1024,
            window: 1,
        };
        let mut encoded = hello.encode();
        assert_eq!(Hello::decode(&mut encoded).unwrap(), hello);
    }

    #[test]
    fn hello_with_empty_lists_round_trips() {
        let hello = Hello {
            version: PROTOCOL_VERSION,
            codecs: vec![],
            compressions: vec![],
            max_frame_bytes: 0,
            window: 0,
        };
        let mut encoded = hello.encode();
        assert_eq!(Hello::decode(&mut encoded).unwrap(), hello);
    }

    #[test]
    fn hello_at_the_max_choice_list_length_round_trips() {
        let hello = Hello {
            version: PROTOCOL_VERSION,
            codecs: (0..MAX_CHOICE_LIST_ENTRIES as u8).collect(),
            compressions: vec![0],
            max_frame_bytes: 1,
            window: 1,
        };
        let mut encoded = hello.encode();
        assert_eq!(Hello::decode(&mut encoded).unwrap(), hello);
    }

    #[test]
    fn hello_over_the_max_choice_list_length_is_rejected() {
        let hello = Hello {
            version: PROTOCOL_VERSION,
            codecs: vec![0; MAX_CHOICE_LIST_ENTRIES + 1],
            compressions: vec![],
            max_frame_bytes: 1,
            window: 1,
        };
        let mut encoded = hello.encode();
        assert!(matches!(Hello::decode(&mut encoded), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn hello_ack_round_trips() {
        let ack = HelloAck {
            version: PROTOCOL_VERSION,
            codec: 1,
            compression: 1,
            max_frame_bytes: 4096,
            window: 1,
        };
        let mut encoded = ack.encode();
        assert_eq!(HelloAck::decode(&mut encoded).unwrap(), ack);
    }

    #[test]
    fn ack_round_trips() {
        let ack = Ack { seq: 42 };
        let mut encoded = ack.encode();
        assert_eq!(Ack::decode(&mut encoded).unwrap(), ack);
    }

    #[test]
    fn reject_round_trips() {
        let reject =
            Reject { code: REJECT_NO_COMMON_CODEC, message: "no shared codec".to_string() };
        let mut encoded = reject.encode();
        assert_eq!(Reject::decode(&mut encoded).unwrap(), reject);
    }

    #[test]
    fn reject_at_the_max_message_length_round_trips() {
        let reject =
            Reject { code: REJECT_INTERNAL, message: "x".repeat(MAX_REJECT_MESSAGE_BYTES) };
        let mut encoded = reject.encode();
        assert_eq!(Reject::decode(&mut encoded).unwrap(), reject);
    }

    #[test]
    fn reject_over_the_max_message_length_is_rejected() {
        let reject =
            Reject { code: REJECT_INTERNAL, message: "x".repeat(MAX_REJECT_MESSAGE_BYTES + 1) };
        let mut encoded = reject.encode();
        assert!(matches!(Reject::decode(&mut encoded), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn an_unknown_field_tag_is_skipped_without_disturbing_known_fields() {
        // Hand-build a Hello payload with an extra, unrecognized field tag 99 inserted before the
        // real fields -- a future protocol version's addition, from this reader's perspective.
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_HELLO]);
        write_field(&mut out, 99, |buf| buf.extend_from_slice(b"future field, ignore me"));
        write_u16(&mut out, HELLO_FIELD_VERSION, PROTOCOL_VERSION);
        write_bytes_field(&mut out, HELLO_FIELD_CODECS, &[1]);
        write_bytes_field(&mut out, HELLO_FIELD_COMPRESSIONS, &[0]);
        write_u32(&mut out, HELLO_FIELD_MAX_FRAME_BYTES, 1024);
        write_u32(&mut out, HELLO_FIELD_WINDOW, 1);
        let mut bytes = out.freeze();

        let hello = Hello::decode(&mut bytes).unwrap();
        assert_eq!(hello.version, PROTOCOL_VERSION);
        assert_eq!(hello.codecs, vec![1]);
        assert_eq!(hello.compressions, vec![0]);
        assert_eq!(hello.max_frame_bytes, 1024);
        assert_eq!(hello.window, 1);
    }

    #[test]
    fn decoding_the_wrong_message_type_is_a_clear_error() {
        let ack = Ack { seq: 1 };
        let mut encoded = ack.encode();
        assert!(matches!(HelloAck::decode(&mut encoded), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn control_message_dispatches_on_the_leading_type_byte() {
        for msg in [
            ControlMessage::Hello(Hello {
                version: PROTOCOL_VERSION,
                codecs: vec![1],
                compressions: vec![0],
                max_frame_bytes: 1,
                window: 1,
            }),
            ControlMessage::HelloAck(HelloAck {
                version: PROTOCOL_VERSION,
                codec: 1,
                compression: 0,
                max_frame_bytes: 1,
                window: 1,
            }),
            ControlMessage::Ack(Ack { seq: 7 }),
            ControlMessage::Reject(Reject { code: REJECT_GOING_AWAY, message: "bye".to_string() }),
        ] {
            let mut encoded = msg.encode();
            assert_eq!(ControlMessage::decode(&mut encoded).unwrap(), msg);
        }
    }

    #[test]
    fn control_message_rejects_an_unknown_message_type() {
        let mut bytes = Bytes::from_static(&[0xEE]);
        assert!(matches!(ControlMessage::decode(&mut bytes), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn flag_control_marks_a_hello_frame() {
        // The connection layer's own responsibility (not this module's), exercised here as a
        // documentation test: a control message is framed with `crate::frame::FLAG_CONTROL` set,
        // `codec`/`compression` otherwise meaningless.
        use crate::frame::{
            read_frame_with_header, write_frame_with_flags, Compression, FLAG_CONTROL,
        };

        let hello = Hello {
            version: PROTOCOL_VERSION,
            codecs: vec![1],
            compressions: vec![0, 1],
            max_frame_bytes: 4096,
            window: 1,
        };
        let framed =
            write_frame_with_flags(0, Compression::None, FLAG_CONTROL, &hello.encode()).unwrap();
        let mut bytes = framed;
        let (header, mut payload) = read_frame_with_header(&mut bytes).unwrap();
        assert_eq!(header.flags & FLAG_CONTROL, FLAG_CONTROL);
        assert_eq!(Hello::decode(&mut payload).unwrap(), hello);
    }
}
