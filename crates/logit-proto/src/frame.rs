//! The native frame header. See `docs/design/wire-protocol.md` for the full format, including the
//! dictionary-first payload encoding this header wraps.

pub const MAGIC: [u8; 4] = *b"LGIT";
pub const VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

/// Fixed, versioned framing so an incompatible future payload format can be rejected (or,
/// eventually, negotiated) cleanly rather than corrupting the stream.
#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub version: u16,
    pub flags: u16,
    pub codec: u8,
    pub compression: Compression,
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    pub crc32c: u32,
}

// TODO: `FrameHeader::encode`/`decode` to/from the 20-byte wire layout in
// `docs/design/wire-protocol.md`, the dictionary-first payload encoder/decoder (pending the
// rkyv-vs-hand-rolled benchmark), and the connection/handshake state machine. Left as the header
// type only in this skeleton pass.
