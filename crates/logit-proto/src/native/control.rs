//! The `logit_in`/`logit_out` control messages: `Hello`/`HelloAck` (the version, codec, and
//! compression handshake), `Ack` (cumulative acknowledgement), and `Reject` (a clean refusal).
//! ADR `native-transport-handshake-and-ack` has the protocol.
//!
//! A control message rides in an ordinary frame with [`crate::frame::FLAG_CONTROL`] set. Its
//! `codec` byte is meaningless, and its `compression` is always
//! [`crate::frame::Compression::None`]: the messages are tiny, and compression is itself being
//! negotiated.
//!
//! Every field is `tag(u8) + len(uvarint) + payload`, like [`crate::native::record`], so a field
//! from a later protocol version is skipped whole. An unknown message type is an error (see
//! [`ControlMessage::decode`]).

use bytes::{Bytes, BytesMut};

use crate::native::varint::{read_u8, read_uvarint, write_uvarint};
use crate::CodecError;

/// The connection-protocol version `Hello`/`HelloAck` negotiate, independent of the frame
/// header's [`crate::frame::VERSION`].
pub const PROTOCOL_VERSION: u16 = 1;

/// A [`Reject`] reason. Codes, not an enum, so a new reason needs no version bump: a reader that
/// doesn't know a code still has `message`.
pub const REJECT_VERSION_MISMATCH: u16 = 1;
pub const REJECT_NO_COMMON_CODEC: u16 = 2;
pub const REJECT_FRAME_TOO_LARGE: u16 = 3;
pub const REJECT_GOING_AWAY: u16 = 4;
pub const REJECT_INTERNAL: u16 = 5;

/// Bounds `Reject.message` so a hostile peer can't force an unbounded allocation. Decode checks
/// the wire bytes against it, then truncates the lossy UTF-8 conversion to it on a char boundary.
const MAX_REJECT_MESSAGE_BYTES: usize = 1024;

/// Bounds `Hello.codecs`/`Hello.compressions`, each drawn from a handful of values, before a
/// peer's declared count sizes an allocation.
const MAX_CHOICE_LIST_ENTRIES: usize = 16;

const MSG_HELLO: u8 = 1;
const MSG_HELLO_ACK: u8 = 2;
const MSG_ACK: u8 = 3;
const MSG_REJECT: u8 = 4;

// -- shared TLV field helpers, the same shape as `native::record`'s ---------------------------

fn write_field(out: &mut BytesMut, tag: u8, build: impl FnOnce(&mut BytesMut)) {
    let mut tmp = BytesMut::new();
    build(&mut tmp);
    out.extend_from_slice(&[tag]);
    write_uvarint(out, tmp.len() as u64);
    out.extend_from_slice(&tmp);
}

/// Reads one `tag + len + payload` field off the front of `body`; `None` once it's exhausted. A
/// declared length past the end of `body` is `Malformed`.
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

/// Sent first, by the connecting side: its protocol version, every codec and compression it
/// speaks, the largest frame it accepts, and its flow-control window. `window` is negotiated but
/// unused; the sender keeps one frame in flight (`docs/plans/native-transport.md`'s "In-flight"
/// decision).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    pub version: u16,
    /// Frame `codec` bytes this side can decode, in preference order. Bounded to
    /// [`MAX_CHOICE_LIST_ENTRIES`] on decode.
    pub codecs: Vec<u8>,
    /// `crate::frame::Compression as u8` bytes this side can decompress, in preference order.
    /// Bounded to [`MAX_CHOICE_LIST_ENTRIES`] on decode.
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

    /// Decodes the TLV fields after a message-type byte the caller already checked.
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
                // A later protocol version's field; skip it.
                _unknown => {}
            }
        }
        Ok(Hello { version, codecs, compressions, max_frame_bytes, window })
    }

    /// Decodes a whole `Hello` payload, message-type byte included; a different message type is
    /// [`CodecError::Malformed`].
    pub fn decode(bytes: &mut Bytes) -> Result<Self, CodecError> {
        expect_msg_type(bytes, MSG_HELLO, "Hello")?;
        Self::decode_fields(bytes.split_off(0))
    }
}

// -- HelloAck ----------------------------------------------------------------------------------

/// The listener's reply to a valid [`Hello`]: the chosen codec and compression from both sides'
/// offers, its own frame-size ceiling, and its window.
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

/// Cumulative acknowledgement: every data frame through `seq` is forwarded. Sequence numbers are
/// implicit: TCP is ordered, so the Nth data frame on a connection is seq N.
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

/// A clean refusal, always followed by the sender closing the connection. `code` is one of the
/// `REJECT_*` constants.
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
                    // Each invalid byte becomes a 3-byte U+FFFD, so the lossy string can exceed
                    // the cap; cut it back so a decoded message always re-encodes within it.
                    let mut cut = message.len().min(MAX_REJECT_MESSAGE_BYTES);
                    while !message.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    message.truncate(cut);
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

/// Any control message, for a reader that doesn't know which comes next (after the handshake,
/// either `Ack` or `Reject`).
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

    /// Dispatches on the leading message-type byte. An unknown message type is `Malformed`,
    /// unlike an unknown field, which is skipped: there's no known layout to read it by.
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

    /// 342 bytes of `0xFF` pass the byte cap, and each becomes a 3-byte U+FFFD under the lossy
    /// UTF-8 conversion: 1026 bytes, which a second decode would refuse.
    #[test]
    fn a_reject_message_of_invalid_utf8_re_encodes_within_the_cap() {
        let mut wire = vec![MSG_REJECT, REJECT_FIELD_MESSAGE, 0xd6, 0x02];
        wire.extend(std::iter::repeat_n(0xFF, 342));
        let decoded = Reject::decode(&mut Bytes::from(wire)).unwrap();
        assert!(decoded.message.len() <= MAX_REJECT_MESSAGE_BYTES, "{}", decoded.message.len());
        assert_eq!(Reject::decode(&mut decoded.encode()).unwrap(), decoded);
    }

    #[test]
    fn an_unknown_field_tag_is_skipped_without_disturbing_known_fields() {
        // A Hello with an unknown field tag 99 ahead of the real fields.
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
        // Framing is the connection layer's job; this shows the expected shape.
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
