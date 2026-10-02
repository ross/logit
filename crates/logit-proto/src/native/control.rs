//! The `logit_in`/`logit_out` control messages: `Hello`/`HelloAck` (the version, codec, and
//! compression handshake), `Ack` (frames are handled), and `Reject` (a clean refusal).
//! ADR `native-transport-handshake-and-ack` has the protocol.
//!
//! `Ack` names a sender identity and a sequence, and covers every frame of that identity at or
//! below it (`docs/adr/native-hop-named-acks.md`, decision 1). `Hello.senders` lists the
//! identities of the frames a connection will resend, and `HelloAck.marks` answers each with the
//! receiver's high-water mark (the same ADR, decision 4).
//!
//! A control message rides in an ordinary frame with [`crate::frame::FLAG_CONTROL`] set. Its
//! `codec` byte is meaningless, and its `compression` is always
//! [`crate::frame::Compression::None`]: the messages are tiny, and compression is itself being
//! negotiated.
//!
//! Every field is `tag(u8) + len(uvarint) + payload`, like [`crate::native::record`]. Unlike a
//! record, a control message carries its defined fields and nothing else: a missing, repeated, or
//! unknown field is [`CodecError::Malformed`], as is an unknown message type (ADR
//! `native-hop-no-compatibility`, decision 4).

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

/// The most entries `Hello.senders` and `HelloAck.marks` may hold; more is
/// [`CodecError::Malformed`].
pub const MAX_HELLO_SENDERS: usize = 16;

/// A sender identity's width on the wire.
const SENDER_ID_BYTES: usize = 16;

/// One `HelloAck.marks` entry: the identity, then the mark as a big-endian `u64`.
const MARK_ENTRY_BYTES: usize = SENDER_ID_BYTES + 8;

/// The longest control message payload a reader accepts, checked against a frame header before
/// the body is allocated. The longest message this version writes is a `Reject` whose message is
/// at [`MAX_REJECT_MESSAGE_BYTES`], 1033 bytes. The largest valid `Hello` (16 senders) is 315
/// bytes and the largest valid `HelloAck` (16 marks) is 413; the rest is headroom for a longer
/// `Reject.message` or list. No valid control message reaches it, so it bounds a malformed
/// length.
pub const MAX_CONTROL_MESSAGE_BYTES: u32 = 4096;

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

/// Stores one decoded field, refusing a second occurrence of its tag.
fn set_once<T>(slot: &mut Option<T>, value: T, msg: &str, field: &str) -> Result<(), CodecError> {
    if slot.is_some() {
        return Err(CodecError::Malformed(format!("{msg} repeats {field}")));
    }
    *slot = Some(value);
    Ok(())
}

fn required<T>(slot: Option<T>, msg: &str, field: &str) -> Result<T, CodecError> {
    slot.ok_or_else(|| CodecError::Malformed(format!("{msg} is missing {field}")))
}

fn unknown_tag(msg: &str, tag: u8) -> CodecError {
    CodecError::Malformed(format!("{msg} has an unknown field tag {tag}"))
}

fn read_window_field(field: Bytes, msg: &str) -> Result<u32, CodecError> {
    match read_u32_field(field)? {
        0 => Err(CodecError::Malformed(format!("{msg}.window is 0; it must be at least 1"))),
        window => Ok(window),
    }
}

fn read_senders_field(field: Bytes) -> Result<Vec<[u8; 16]>, CodecError> {
    if !field.len().is_multiple_of(SENDER_ID_BYTES) {
        return Err(CodecError::Malformed(format!(
            "Hello.senders is {} bytes, not a multiple of {SENDER_ID_BYTES}",
            field.len()
        )));
    }
    let entries = field.len() / SENDER_ID_BYTES;
    if entries > MAX_HELLO_SENDERS {
        return Err(CodecError::Malformed(format!(
            "Hello.senders declares {entries} entries, over the {MAX_HELLO_SENDERS} cap"
        )));
    }
    let (ids, _) = field.as_chunks::<SENDER_ID_BYTES>();
    Ok(ids.to_vec())
}

fn read_marks_field(field: Bytes) -> Result<Vec<([u8; 16], u64)>, CodecError> {
    if !field.len().is_multiple_of(MARK_ENTRY_BYTES) {
        return Err(CodecError::Malformed(format!(
            "HelloAck.marks is {} bytes, not a multiple of {MARK_ENTRY_BYTES}",
            field.len()
        )));
    }
    let entries = field.len() / MARK_ENTRY_BYTES;
    if entries > MAX_HELLO_SENDERS {
        return Err(CodecError::Malformed(format!(
            "HelloAck.marks declares {entries} entries, over the {MAX_HELLO_SENDERS} cap"
        )));
    }
    let (entries, _) = field.as_chunks::<MARK_ENTRY_BYTES>();
    Ok(entries
        .iter()
        .map(|entry| {
            let (id, mark) = entry.split_at(SENDER_ID_BYTES);
            (
                id.try_into().expect("split_at yields 16 bytes"),
                u64::from_be_bytes(mark.try_into().expect("split_at yields 8 bytes")),
            )
        })
        .collect())
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
/// speaks, the largest frame it accepts, and its send window: how many frames it may have in
/// flight before the oldest is acknowledged, at least 1. The sender uses the smaller of its own
/// and `HelloAck`'s (ADR `native-hop-send-window`, decision 1).
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
    /// At least 1; a decoded 0 is [`CodecError::Malformed`].
    pub window: u32,
    /// The distinct identities of the frames this connection will resend, in in-flight order;
    /// empty when nothing is being resent. At most [`MAX_HELLO_SENDERS`].
    pub senders: Vec<[u8; 16]>,
}

const HELLO_FIELD_VERSION: u8 = 1;
const HELLO_FIELD_CODECS: u8 = 2;
const HELLO_FIELD_COMPRESSIONS: u8 = 3;
const HELLO_FIELD_MAX_FRAME_BYTES: u8 = 4;
const HELLO_FIELD_WINDOW: u8 = 5;
const HELLO_FIELD_SENDERS: u8 = 6;

impl Hello {
    pub fn encode(&self) -> Bytes {
        debug_assert!(self.window >= 1, "Hello.window must be at least 1");
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_HELLO]);
        write_u16(&mut out, HELLO_FIELD_VERSION, self.version);
        write_bytes_field(&mut out, HELLO_FIELD_CODECS, &self.codecs);
        write_bytes_field(&mut out, HELLO_FIELD_COMPRESSIONS, &self.compressions);
        write_u32(&mut out, HELLO_FIELD_MAX_FRAME_BYTES, self.max_frame_bytes);
        write_u32(&mut out, HELLO_FIELD_WINDOW, self.window);
        write_field(&mut out, HELLO_FIELD_SENDERS, |buf| {
            for id in &self.senders {
                buf.extend_from_slice(id);
            }
        });
        out.freeze()
    }

    /// Decodes the TLV fields after a message-type byte the caller already checked.
    fn decode_fields(mut body: Bytes) -> Result<Self, CodecError> {
        const MSG: &str = "Hello";
        let mut version = None;
        let mut codecs = None;
        let mut compressions = None;
        let mut max_frame_bytes = None;
        let mut window = None;
        let mut senders = None;
        while let Some((tag, field)) = read_field(&mut body)? {
            match tag {
                HELLO_FIELD_VERSION => {
                    set_once(&mut version, read_u16_field(field)?, MSG, "version")?
                }
                HELLO_FIELD_CODECS => {
                    set_once(&mut codecs, read_choice_list(field, "Hello.codecs")?, MSG, "codecs")?
                }
                HELLO_FIELD_COMPRESSIONS => set_once(
                    &mut compressions,
                    read_choice_list(field, "Hello.compressions")?,
                    MSG,
                    "compressions",
                )?,
                HELLO_FIELD_MAX_FRAME_BYTES => {
                    set_once(&mut max_frame_bytes, read_u32_field(field)?, MSG, "max_frame_bytes")?
                }
                HELLO_FIELD_WINDOW => {
                    set_once(&mut window, read_window_field(field, MSG)?, MSG, "window")?
                }
                HELLO_FIELD_SENDERS => {
                    set_once(&mut senders, read_senders_field(field)?, MSG, "senders")?
                }
                tag => return Err(unknown_tag(MSG, tag)),
            }
        }
        Ok(Hello {
            version: required(version, MSG, "version")?,
            codecs: required(codecs, MSG, "codecs")?,
            compressions: required(compressions, MSG, "compressions")?,
            max_frame_bytes: required(max_frame_bytes, MSG, "max_frame_bytes")?,
            window: required(window, MSG, "window")?,
            senders: required(senders, MSG, "senders")?,
        })
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
/// offers, its own frame-size ceiling, its window, at least 1, and its marks for the identities
/// `Hello.senders` listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloAck {
    pub version: u16,
    pub codec: u8,
    pub compression: u8,
    pub max_frame_bytes: u32,
    /// At least 1; a decoded 0 is [`CodecError::Malformed`].
    pub window: u32,
    /// One `(identity, mark)` per `Hello.senders` identity the receiver holds, in the order
    /// asked; an identity it doesn't hold is omitted. At most [`MAX_HELLO_SENDERS`].
    pub marks: Vec<([u8; 16], u64)>,
}

const HELLO_ACK_FIELD_VERSION: u8 = 1;
const HELLO_ACK_FIELD_CODEC: u8 = 2;
const HELLO_ACK_FIELD_COMPRESSION: u8 = 3;
const HELLO_ACK_FIELD_MAX_FRAME_BYTES: u8 = 4;
const HELLO_ACK_FIELD_WINDOW: u8 = 5;
const HELLO_ACK_FIELD_MARKS: u8 = 6;

impl HelloAck {
    pub fn encode(&self) -> Bytes {
        debug_assert!(self.window >= 1, "HelloAck.window must be at least 1");
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_HELLO_ACK]);
        write_u16(&mut out, HELLO_ACK_FIELD_VERSION, self.version);
        write_field(&mut out, HELLO_ACK_FIELD_CODEC, |buf| buf.extend_from_slice(&[self.codec]));
        write_field(&mut out, HELLO_ACK_FIELD_COMPRESSION, |buf| {
            buf.extend_from_slice(&[self.compression])
        });
        write_u32(&mut out, HELLO_ACK_FIELD_MAX_FRAME_BYTES, self.max_frame_bytes);
        write_u32(&mut out, HELLO_ACK_FIELD_WINDOW, self.window);
        write_field(&mut out, HELLO_ACK_FIELD_MARKS, |buf| {
            for (id, mark) in &self.marks {
                buf.extend_from_slice(id);
                buf.extend_from_slice(&mark.to_be_bytes());
            }
        });
        out.freeze()
    }

    fn decode_fields(mut body: Bytes) -> Result<Self, CodecError> {
        const MSG: &str = "HelloAck";
        let mut version = None;
        let mut codec = None;
        let mut compression = None;
        let mut max_frame_bytes = None;
        let mut window = None;
        let mut marks = None;
        while let Some((tag, mut field)) = read_field(&mut body)? {
            match tag {
                HELLO_ACK_FIELD_VERSION => {
                    set_once(&mut version, read_u16_field(field)?, MSG, "version")?
                }
                HELLO_ACK_FIELD_CODEC => set_once(&mut codec, read_u8(&mut field)?, MSG, "codec")?,
                HELLO_ACK_FIELD_COMPRESSION => {
                    set_once(&mut compression, read_u8(&mut field)?, MSG, "compression")?
                }
                HELLO_ACK_FIELD_MAX_FRAME_BYTES => {
                    set_once(&mut max_frame_bytes, read_u32_field(field)?, MSG, "max_frame_bytes")?
                }
                HELLO_ACK_FIELD_WINDOW => {
                    set_once(&mut window, read_window_field(field, MSG)?, MSG, "window")?
                }
                HELLO_ACK_FIELD_MARKS => {
                    set_once(&mut marks, read_marks_field(field)?, MSG, "marks")?
                }
                tag => return Err(unknown_tag(MSG, tag)),
            }
        }
        Ok(HelloAck {
            version: required(version, MSG, "version")?,
            codec: required(codec, MSG, "codec")?,
            compression: required(compression, MSG, "compression")?,
            max_frame_bytes: required(max_frame_bytes, MSG, "max_frame_bytes")?,
            window: required(window, MSG, "window")?,
            marks: required(marks, MSG, "marks")?,
        })
    }

    pub fn decode(bytes: &mut Bytes) -> Result<Self, CodecError> {
        expect_msg_type(bytes, MSG_HELLO_ACK, "HelloAck")?;
        Self::decode_fields(bytes.split_off(0))
    }
}

// -- Ack -------------------------------------------------------------------------------------

/// Every data frame of sender identity `id` with a sequence at or below `seq` that this
/// connection carried is handled: forwarded, or recognized as a resend and not forwarded. `seq`
/// is a frame the connection carried, never the receiver's mark. Grants no credit
/// (`docs/adr/native-hop-named-acks.md`, decision 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack {
    pub id: [u8; 16],
    /// At least 1; a decoded 0 is [`CodecError::Malformed`], as in a frame's trailer.
    pub seq: u64,
}

const ACK_FIELD_ID: u8 = 1;
const ACK_FIELD_SEQ: u8 = 2;

impl Ack {
    pub fn encode(&self) -> Bytes {
        debug_assert!(self.seq >= 1, "Ack.seq must be at least 1");
        let mut out = BytesMut::new();
        out.extend_from_slice(&[MSG_ACK]);
        write_bytes_field(&mut out, ACK_FIELD_ID, &self.id);
        write_field(&mut out, ACK_FIELD_SEQ, |buf| write_uvarint(buf, self.seq));
        out.freeze()
    }

    fn decode_fields(mut body: Bytes) -> Result<Self, CodecError> {
        const MSG: &str = "Ack";
        let mut id = None;
        let mut seq = None;
        while let Some((tag, mut field)) = read_field(&mut body)? {
            match tag {
                ACK_FIELD_ID => {
                    let value: [u8; 16] = field[..].try_into().map_err(|_| {
                        CodecError::Malformed(format!(
                            "Ack.id is {} bytes, not {SENDER_ID_BYTES}",
                            field.len()
                        ))
                    })?;
                    set_once(&mut id, value, MSG, "id")?
                }
                ACK_FIELD_SEQ => {
                    let value = read_uvarint(&mut field)?;
                    if value == 0 {
                        return Err(CodecError::Malformed("Ack.seq is 0".to_string()));
                    }
                    set_once(&mut seq, value, MSG, "seq")?
                }
                tag => return Err(unknown_tag(MSG, tag)),
            }
        }
        Ok(Ack { id: required(id, MSG, "id")?, seq: required(seq, MSG, "seq")? })
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
        const MSG: &str = "Reject";
        let mut code = None;
        let mut message: Option<String> = None;
        while let Some((tag, field)) = read_field(&mut body)? {
            match tag {
                REJECT_FIELD_CODE => set_once(&mut code, read_u16_field(field)?, MSG, "code")?,
                REJECT_FIELD_MESSAGE => {
                    if message.is_some() {
                        return Err(CodecError::Malformed(format!("{MSG} repeats message")));
                    }
                    if field.len() > MAX_REJECT_MESSAGE_BYTES {
                        return Err(CodecError::Malformed(format!(
                            "Reject.message is {} bytes, over the {MAX_REJECT_MESSAGE_BYTES} \
                             sanity cap",
                            field.len()
                        )));
                    }
                    let mut text = String::from_utf8_lossy(&field).into_owned();
                    // Each invalid byte becomes a 3-byte U+FFFD, so the lossy string can exceed
                    // the cap; cut it back so a decoded message always re-encodes within it.
                    let mut cut = text.len().min(MAX_REJECT_MESSAGE_BYTES);
                    while !text.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    text.truncate(cut);
                    message = Some(text);
                }
                tag => return Err(unknown_tag(MSG, tag)),
            }
        }
        Ok(Reject {
            code: required(code, MSG, "code")?,
            message: required(message, MSG, "message")?,
        })
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

    /// Dispatches on the leading message-type byte. An unknown message type is `Malformed`, as
    /// is an unknown field inside a known one.
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
            senders: vec![],
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
            window: 1,
            senders: vec![],
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
            senders: vec![],
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
            senders: vec![],
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
            marks: vec![],
        };
        let mut encoded = ack.encode();
        assert_eq!(HelloAck::decode(&mut encoded).unwrap(), ack);
    }

    const ID: [u8; 16] = [7; 16];

    #[test]
    fn an_ack_round_trips_its_identity_and_sequence() {
        for seq in [1, 300, u64::MAX] {
            let ack = Ack { id: ID, seq };
            let mut encoded = ack.encode();
            assert_eq!(Ack::decode(&mut encoded).unwrap(), ack);
            let mut encoded = ack.encode();
            assert_eq!(ControlMessage::decode(&mut encoded).unwrap(), ControlMessage::Ack(ack));
        }
    }

    #[test]
    fn an_ack_missing_either_field_is_malformed() {
        let mut id_only = BytesMut::from(&[MSG_ACK][..]);
        write_bytes_field(&mut id_only, ACK_FIELD_ID, &ID);
        let mut seq_only = BytesMut::from(&[MSG_ACK][..]);
        write_field(&mut seq_only, ACK_FIELD_SEQ, |buf| write_uvarint(buf, 5));
        for (bytes, missing) in
            [(id_only, "seq"), (seq_only, "id"), (BytesMut::from(&[MSG_ACK][..]), "id")]
        {
            let result = Ack::decode(&mut bytes.freeze());
            assert!(
                matches!(&result, Err(CodecError::Malformed(m)) if m == &format!("Ack is missing {missing}")),
                "{result:?}"
            );
        }
    }

    #[test]
    fn an_ack_with_a_sequence_of_zero_is_malformed() {
        let mut bytes = BytesMut::from(&[MSG_ACK][..]);
        write_bytes_field(&mut bytes, ACK_FIELD_ID, &ID);
        write_field(&mut bytes, ACK_FIELD_SEQ, |buf| write_uvarint(buf, 0));
        let result = ControlMessage::decode(&mut bytes.freeze());
        assert!(
            matches!(&result, Err(CodecError::Malformed(m)) if m == "Ack.seq is 0"),
            "{result:?}"
        );
    }

    #[test]
    fn an_ack_with_a_short_identity_is_malformed() {
        for len in [0, 15, 17] {
            let mut bytes = BytesMut::from(&[MSG_ACK][..]);
            write_bytes_field(&mut bytes, ACK_FIELD_ID, &vec![1; len]);
            write_field(&mut bytes, ACK_FIELD_SEQ, |buf| write_uvarint(buf, 1));
            let result = Ack::decode(&mut bytes.freeze());
            assert!(
                matches!(&result, Err(CodecError::Malformed(m)) if m.starts_with("Ack.id is")),
                "{len}: {result:?}"
            );
        }
    }

    fn hello_with(senders: Vec<[u8; 16]>) -> Hello {
        Hello {
            version: PROTOCOL_VERSION,
            codecs: vec![1],
            compressions: vec![0],
            max_frame_bytes: 1024,
            window: 1,
            senders,
        }
    }

    fn hello_ack_with(marks: Vec<([u8; 16], u64)>) -> HelloAck {
        HelloAck {
            version: PROTOCOL_VERSION,
            codec: 1,
            compression: 0,
            max_frame_bytes: 1024,
            window: 1,
            marks,
        }
    }

    #[test]
    fn hello_senders_and_hello_ack_marks_round_trip() {
        let full_senders: Vec<[u8; 16]> = (0..MAX_HELLO_SENDERS as u8).map(|i| [i; 16]).collect();
        for senders in [vec![], full_senders.clone()] {
            let hello = hello_with(senders);
            assert_eq!(Hello::decode(&mut hello.encode()).unwrap(), hello);
        }
        let full_marks: Vec<([u8; 16], u64)> =
            full_senders.iter().enumerate().map(|(i, id)| (*id, u64::MAX - i as u64)).collect();
        for marks in [vec![], full_marks] {
            let ack = hello_ack_with(marks);
            assert_eq!(HelloAck::decode(&mut ack.encode()).unwrap(), ack);
        }
    }

    /// Replaces the field tagged `tag` in an encoded message with `payload`.
    fn with_raw_field(encoded: Bytes, tag: u8, payload: &[u8]) -> Bytes {
        let mut body = encoded;
        let msg_type = read_u8(&mut body).unwrap();
        let mut out = BytesMut::from(&[msg_type][..]);
        while let Some((t, field)) = read_field(&mut body).unwrap() {
            write_bytes_field(&mut out, t, if t == tag { payload } else { &field });
        }
        out.freeze()
    }

    #[test]
    fn a_senders_or_marks_list_over_the_cap_or_misaligned_is_malformed() {
        let over = MAX_HELLO_SENDERS + 1;
        for (payload, want) in [
            (vec![1; SENDER_ID_BYTES * over], "over the 16 cap"),
            (vec![1; SENDER_ID_BYTES + 1], "not a multiple of 16"),
            (vec![1; SENDER_ID_BYTES - 1], "not a multiple of 16"),
        ] {
            let mut bytes =
                with_raw_field(hello_with(vec![]).encode(), HELLO_FIELD_SENDERS, &payload);
            let result = ControlMessage::decode(&mut bytes);
            assert!(
                matches!(&result, Err(CodecError::Malformed(m)) if m.starts_with("Hello.senders") && m.contains(want)),
                "{result:?}"
            );
        }
        for (payload, want) in [
            (vec![1; MARK_ENTRY_BYTES * over], "over the 16 cap"),
            (vec![1; MARK_ENTRY_BYTES + 16], "not a multiple of 24"),
            (vec![1; SENDER_ID_BYTES], "not a multiple of 24"),
        ] {
            let mut bytes =
                with_raw_field(hello_ack_with(vec![]).encode(), HELLO_ACK_FIELD_MARKS, &payload);
            let result = ControlMessage::decode(&mut bytes);
            assert!(
                matches!(&result, Err(CodecError::Malformed(m)) if m.starts_with("HelloAck.marks") && m.contains(want)),
                "{result:?}"
            );
        }
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

    /// Every message this version writes, at its largest, fits [`MAX_CONTROL_MESSAGE_BYTES`], and
    /// the largest is the 1033-byte `Reject` its doc names.
    #[test]
    fn the_largest_message_of_each_type_fits_the_control_message_cap() {
        let largest = [
            Hello {
                version: u16::MAX,
                codecs: vec![u8::MAX; MAX_CHOICE_LIST_ENTRIES],
                compressions: vec![u8::MAX; MAX_CHOICE_LIST_ENTRIES],
                max_frame_bytes: u32::MAX,
                window: u32::MAX,
                senders: vec![[u8::MAX; 16]; MAX_HELLO_SENDERS],
            }
            .encode(),
            HelloAck {
                version: u16::MAX,
                codec: u8::MAX,
                compression: u8::MAX,
                max_frame_bytes: u32::MAX,
                window: u32::MAX,
                marks: vec![([u8::MAX; 16], u64::MAX); MAX_HELLO_SENDERS],
            }
            .encode(),
            Ack { id: [u8::MAX; 16], seq: u64::MAX }.encode(),
            Reject { code: u16::MAX, message: "x".repeat(MAX_REJECT_MESSAGE_BYTES) }.encode(),
        ];
        let lens: Vec<usize> = largest.iter().map(Bytes::len).collect();
        assert_eq!(lens, [315, 413, 31, 1033]);
        assert!(lens.iter().all(|&len| len <= MAX_CONTROL_MESSAGE_BYTES as usize), "{lens:?}");
    }

    /// 342 bytes of `0xFF` pass the byte cap, and each becomes a 3-byte U+FFFD under the lossy
    /// UTF-8 conversion: 1026 bytes, which a second decode would refuse.
    #[test]
    fn a_reject_message_of_invalid_utf8_re_encodes_within_the_cap() {
        let mut wire = vec![MSG_REJECT, REJECT_FIELD_CODE, 1, 5, REJECT_FIELD_MESSAGE, 0xd6, 0x02];
        wire.extend(std::iter::repeat_n(0xFF, 342));
        let decoded = Reject::decode(&mut Bytes::from(wire)).unwrap();
        assert!(decoded.message.len() <= MAX_REJECT_MESSAGE_BYTES, "{}", decoded.message.len());
        assert_eq!(Reject::decode(&mut decoded.encode()).unwrap(), decoded);
    }

    /// A valid `Hello`, `HelloAck`, `Ack`, and `Reject`, each as `(message type, fields)` with every
    /// field as its own `tag + len + payload` byte string, so a test can drop, repeat, or replace
    /// one.
    fn valid_messages() -> Vec<(u8, Vec<Bytes>)> {
        fn fields(encoded: Bytes) -> (u8, Vec<Bytes>) {
            let mut body = encoded;
            let msg_type = read_u8(&mut body).unwrap();
            let mut out = Vec::new();
            while !body.is_empty() {
                let before = body.clone();
                read_field(&mut body).unwrap();
                out.push(before.slice(..before.len() - body.len()));
            }
            (msg_type, out)
        }
        vec![
            fields(hello_with(vec![ID, [8; 16]]).encode()),
            fields(hello_ack_with(vec![(ID, 3)]).encode()),
            fields(Ack { id: ID, seq: 9 }.encode()),
            fields(Reject { code: REJECT_INTERNAL, message: "no".to_string() }.encode()),
        ]
    }

    fn assemble(msg_type: u8, fields: &[Bytes]) -> Bytes {
        let mut out = BytesMut::new();
        out.extend_from_slice(&[msg_type]);
        for field in fields {
            out.extend_from_slice(field);
        }
        out.freeze()
    }

    #[test]
    fn every_valid_message_decodes_from_its_fields() {
        for (msg_type, fields) in valid_messages() {
            let mut bytes = assemble(msg_type, &fields);
            ControlMessage::decode(&mut bytes).unwrap();
        }
    }

    #[test]
    fn a_message_missing_any_field_is_malformed() {
        for (msg_type, fields) in valid_messages() {
            for drop in 0..fields.len() {
                let mut kept = fields.clone();
                kept.remove(drop);
                let mut bytes = assemble(msg_type, &kept);
                let result = ControlMessage::decode(&mut bytes);
                assert!(
                    matches!(&result, Err(CodecError::Malformed(m)) if m.contains("is missing")),
                    "type {msg_type} without field {drop}: {result:?}"
                );
            }
        }
    }

    #[test]
    fn a_message_repeating_any_field_is_malformed() {
        for (msg_type, fields) in valid_messages() {
            for repeat in 0..fields.len() {
                let mut doubled = fields.clone();
                doubled.push(fields[repeat].clone());
                let mut bytes = assemble(msg_type, &doubled);
                let result = ControlMessage::decode(&mut bytes);
                assert!(
                    matches!(&result, Err(CodecError::Malformed(m)) if m.contains("repeats")),
                    "type {msg_type} with field {repeat} twice: {result:?}"
                );
            }
        }
    }

    #[test]
    fn an_unknown_field_tag_is_malformed() {
        for (msg_type, mut fields) in valid_messages() {
            let mut unknown = BytesMut::new();
            write_field(&mut unknown, 99, |buf| buf.extend_from_slice(b"unknown"));
            fields.insert(0, unknown.freeze());
            let mut bytes = assemble(msg_type, &fields);
            let result = ControlMessage::decode(&mut bytes);
            assert!(
                matches!(&result, Err(CodecError::Malformed(m)) if m.contains("unknown field tag 99")),
                "type {msg_type}: {result:?}"
            );
        }
    }

    #[test]
    fn a_window_of_zero_is_malformed() {
        for (msg_type, window_tag) in
            [(MSG_HELLO, HELLO_FIELD_WINDOW), (MSG_HELLO_ACK, HELLO_ACK_FIELD_WINDOW)]
        {
            let (_, mut fields) =
                valid_messages().into_iter().find(|(t, _)| *t == msg_type).unwrap();
            let window = fields.iter().position(|f| f[0] == window_tag).unwrap();
            let mut zero = BytesMut::new();
            write_u32(&mut zero, window_tag, 0);
            fields[window] = zero.freeze();
            let mut bytes = assemble(msg_type, &fields);
            let result = ControlMessage::decode(&mut bytes);
            assert!(
                matches!(&result, Err(CodecError::Malformed(m)) if m.contains("window is 0")),
                "type {msg_type}: {result:?}"
            );
        }
    }

    #[test]
    fn decoding_the_wrong_message_type_is_a_clear_error() {
        let mut encoded = Ack { id: ID, seq: 1 }.encode();
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
                senders: vec![],
            }),
            ControlMessage::HelloAck(HelloAck {
                version: PROTOCOL_VERSION,
                codec: 1,
                compression: 0,
                max_frame_bytes: 1,
                window: 1,
                marks: vec![],
            }),
            ControlMessage::Ack(Ack { id: ID, seq: 42 }),
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
            senders: vec![],
        };
        let framed =
            write_frame_with_flags(0, Compression::None, FLAG_CONTROL, &hello.encode()).unwrap();
        let mut bytes = framed;
        let (header, mut payload) = read_frame_with_header(&mut bytes).unwrap();
        assert_eq!(header.flags & FLAG_CONTROL, FLAG_CONTROL);
        assert_eq!(Hello::decode(&mut payload).unwrap(), hello);
    }
}
