//! The native frame header: a fixed, versioned envelope around one codec's payload bytes. See
//! `docs/design/wire-protocol.md` for the full format, including the dictionary-first payload
//! encoding this header wraps (`crate::native`).
//!
//! A frame is the unit both the wire and a file agree on: `write_frame`/`read_frame` work
//! identically writing to a `TcpStream` or appending to a file, which is what lets a durable
//! buffer and a socket write share one encoder (`docs/design/wire-protocol.md`'s "logit-to-logit"
//! and on-disk goals are the same format, not two).

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::CodecError;

pub const MAGIC: [u8; 4] = *b"LGIT";
pub const VERSION: u16 = 1;

/// The largest payload a single frame may declare, checked before `uncompressed_len` -- a raw,
/// unvalidated `u32` off the wire -- is ever used to size an allocation. A frame at this cap is
/// already far larger than any batch `logit` produces; a crafted 30-byte lz4 frame could otherwise
/// name a multi-gigabyte `uncompressed_len` and force the allocation attempt before a single byte
/// of payload had been looked at. Same reasoning as `crate::native::dict`'s
/// `MAX_SANE_DICT_ENTRIES` and `crate::native`'s `MAX_SANE_EVENT_COUNT`.
///
/// **Deliberately asymmetric with `write_frame`.** This bound guards a decoder reading untrusted
/// bytes; nothing stops `write_frame` from encoding a payload larger than this and producing a
/// frame its own `read_frame` would then reject. `pub` so a writer that must never produce a frame
/// its own reader would refuse -- the durable sink buffer (`docs/known-gaps.md`) -- can check an
/// encoded frame against this bound itself before ever writing it, dropping the batch instead of
/// spooling something unreadable.
pub const MAX_SANE_UNCOMPRESSED_LEN: u32 = 64 * 1024 * 1024;

/// The largest `compressed_len` a frame may declare, checked the same way and for the same
/// reason as `MAX_SANE_UNCOMPRESSED_LEN` above -- but not simply reused as the same value: lz4's
/// worst case expands rather than shrinks a payload, so a legitimately-written frame whose
/// payload sits at the uncompressed cap can declare slightly more compressed bytes than that,
/// and reusing the uncompressed cap directly here would make `write_frame` able to produce a
/// frame its own `read_frame` then refuses. This is the uncompressed cap plus lz4's own
/// documented worst-case expansion, wide enough to admit exactly that legitimate case while
/// still bounding the allocation a corrupted length field can force.
const MAX_SANE_COMPRESSED_LEN: u32 =
    MAX_SANE_UNCOMPRESSED_LEN + MAX_SANE_UNCOMPRESSED_LEN / 255 + 16;

/// The fixed header size in bytes: 4 (magic) + 2 (version) + 2 (flags) + 1 (codec) +
/// 1 (compression) + 2 (reserved) + 4 (uncompressed_len) + 4 (compressed_len) + 4 (crc32c) = 24.
/// Chosen over the 20 bytes an earlier skeleton comment named so every multi-byte field after
/// `compression` starts on a 4-byte boundary -- `reserved` is spare room for a flag or a narrow
/// field a later version needs, not padding to be removed.
pub const HEADER_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Compression {
    #[default]
    None = 0,
    Lz4 = 1,
    /// Reserved, not encodable or decodable yet -- the real `zstd` crate builds C via `zstd-sys`,
    /// which breaks ADR `containerized-development`'s "no host toolchain" property, and the
    /// pure-Rust alternatives aren't yet competitive on ratio or speed. See
    /// `docs/adr/native-wire-format-encoding.md`. `write_frame` and `read_frame` both reject this
    /// discriminant with [`CodecError::Unsupported`] rather than silently treating it as `None`.
    Zstd = 2,
}

impl Compression {
    fn from_u8(b: u8) -> Result<Self, CodecError> {
        match b {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Lz4),
            2 => Ok(Compression::Zstd),
            other => Err(CodecError::Malformed(format!("unknown compression byte {other}"))),
        }
    }
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

impl FrameHeader {
    fn write(&self, out: &mut BytesMut) {
        out.put_slice(&MAGIC);
        out.put_u16_le(self.version);
        out.put_u16_le(self.flags);
        out.put_u8(self.codec);
        out.put_u8(self.compression as u8);
        out.put_u16_le(0); // reserved
        out.put_u32_le(self.uncompressed_len);
        out.put_u32_le(self.compressed_len);
        out.put_u32_le(self.crc32c);
    }

    /// Reads exactly [`HEADER_LEN`] bytes off the front of `bytes`, advancing it past the header
    /// so the caller's remaining slice is the payload. Rejects a wrong magic or an unrecognized
    /// version outright -- both mean this isn't a frame this reader can make sense of at all,
    /// as opposed to a within-format decode error. `pub` so a stream or file reader outside this
    /// module (a durable spool's segment reader, a socket reader) can peel off one header at a
    /// time without going through the whole-frame `read_frame` -- `docs/design/wire-protocol.md`.
    ///
    /// Too few bytes to hold a header is [`CodecError::Truncated`], not [`CodecError::Malformed`]:
    /// that's exactly "come back with more bytes" for a live stream, or "this is where a torn
    /// write ends" for a file, neither of which is a claim that the bytes present are wrong.
    pub fn read(bytes: &mut Bytes) -> Result<Self, CodecError> {
        if bytes.len() < HEADER_LEN {
            return Err(CodecError::Truncated { needed: HEADER_LEN - bytes.len() });
        }
        let mut magic = [0u8; 4];
        bytes.copy_to_slice(&mut magic);
        if magic != MAGIC {
            return Err(CodecError::Malformed(format!("bad magic {magic:?}, expected {MAGIC:?}")));
        }
        let version = bytes.get_u16_le();
        if version != VERSION {
            return Err(CodecError::Unsupported(format!(
                "frame version {version}, this reader only understands {VERSION}"
            )));
        }
        let flags = bytes.get_u16_le();
        let codec = bytes.get_u8();
        let compression = Compression::from_u8(bytes.get_u8())?;
        let _reserved = bytes.get_u16_le();
        let uncompressed_len = bytes.get_u32_le();
        let compressed_len = bytes.get_u32_le();
        let crc32c = bytes.get_u32_le();
        Ok(FrameHeader {
            version,
            flags,
            codec,
            compression,
            uncompressed_len,
            compressed_len,
            crc32c,
        })
    }
}

/// Frames `payload` under `codec`, compressing it with `compression` first if requested and
/// checksumming the bytes that actually go on the wire (the *compressed* bytes, matching
/// `docs/design/wire-protocol.md`'s "crc32c over the (possibly compressed) payload" -- a corrupt
/// compressed stream is caught before decompression ever runs on it, rather than handing
/// `lz4_flex` untrusted input and hoping it fails safely).
///
/// Rejects `Compression::Zstd` with [`CodecError::Unsupported`] rather than panicking, symmetric
/// with [`read_frame`]'s existing decode-side rejection -- `Compression` is a public enum whose
/// `Zstd` variant any caller may construct.
pub fn write_frame(
    codec: u8,
    compression: Compression,
    payload: &[u8],
) -> Result<Bytes, CodecError> {
    let compressed = match compression {
        Compression::None => payload.to_vec(),
        Compression::Lz4 => lz4_compress(payload),
        Compression::Zstd => {
            return Err(CodecError::Unsupported(
                "zstd frames are not encodable yet -- see Compression::Zstd's own doc comment"
                    .to_string(),
            ))
        }
    };
    let crc = crc32c::crc32c(&compressed);
    let header = FrameHeader {
        version: VERSION,
        flags: 0,
        codec,
        compression,
        uncompressed_len: payload.len() as u32,
        compressed_len: compressed.len() as u32,
        crc32c: crc,
    };
    let mut out = BytesMut::with_capacity(HEADER_LEN + compressed.len());
    header.write(&mut out);
    out.put_slice(&compressed);
    Ok(out.freeze())
}

/// The inverse of [`write_frame`]: reads one frame off the front of `bytes` (advancing it past
/// that frame, so a caller holding a longer buffer of concatenated frames -- a file, a stream --
/// can call this in a loop), verifies the checksum, decompresses, and returns `(codec, payload)`.
pub fn read_frame(bytes: &mut Bytes) -> Result<(u8, Bytes), CodecError> {
    let header = FrameHeader::read(bytes)?;
    if header.uncompressed_len > MAX_SANE_UNCOMPRESSED_LEN {
        return Err(CodecError::Malformed(format!(
            "frame declares {} uncompressed bytes, over the {MAX_SANE_UNCOMPRESSED_LEN} sanity cap",
            header.uncompressed_len
        )));
    }
    if header.compressed_len > MAX_SANE_COMPRESSED_LEN {
        return Err(CodecError::Malformed(format!(
            "frame declares {} compressed bytes, over the {MAX_SANE_COMPRESSED_LEN} sanity cap",
            header.compressed_len
        )));
    }
    if (bytes.len() as u64) < header.compressed_len as u64 {
        return Err(CodecError::Truncated { needed: header.compressed_len as usize - bytes.len() });
    }
    let compressed = bytes.split_to(header.compressed_len as usize);
    if crc32c::crc32c(&compressed) != header.crc32c {
        return Err(CodecError::Malformed("crc32c mismatch -- frame is corrupt".to_string()));
    }
    let payload = match header.compression {
        Compression::None => compressed,
        Compression::Lz4 => {
            let decompressed = lz4_decompress(&compressed, header.uncompressed_len as usize)
                .map_err(|e| CodecError::Malformed(format!("lz4 decompress: {e:?}")))?;
            Bytes::from(decompressed)
        }
        Compression::Zstd => {
            return Err(CodecError::Unsupported(
                "zstd frames are not decodable yet -- see Compression::Zstd's own doc comment"
                    .to_string(),
            ))
        }
    };
    if payload.len() != header.uncompressed_len as usize {
        return Err(CodecError::Malformed(format!(
            "decompressed to {} bytes, header declared {}",
            payload.len(),
            header.uncompressed_len
        )));
    }
    Ok((header.codec, payload))
}

/// `lz4_flex::block::compress_into` needs a pre-sized output buffer rather than allocating one
/// itself (the crate's modern, non-deprecated API) -- `get_maximum_output_size` gives the worst
/// case, and the buffer is truncated to what was actually written.
fn lz4_compress(payload: &[u8]) -> Vec<u8> {
    let max_len = lz4_flex::block::get_maximum_output_size(payload.len());
    let mut out = vec![0u8; max_len];
    let written = lz4_flex::block::compress_into(payload, &mut out)
        .expect("a buffer sized by get_maximum_output_size is always large enough");
    out.truncate(written);
    out
}

/// The inverse of [`lz4_compress`]. `uncompressed_len` comes straight from this frame's own
/// header, so the output buffer is allocated at exactly the right size -- no guessing, no resize.
/// The buffer is then truncated to what `decompress_into` actually wrote: `lz4_flex` is happy to
/// write *fewer* bytes than the buffer holds, so without this the returned `Vec` would always be
/// `uncompressed_len` long regardless, and [`read_frame`]'s length check below would be a
/// tautology that silently accepted a short decompression padded with zero bytes.
fn lz4_decompress(
    compressed: &[u8],
    uncompressed_len: usize,
) -> Result<Vec<u8>, lz4_flex::block::DecompressError> {
    let mut out = vec![0u8; uncompressed_len];
    let written = lz4_flex::block::decompress_into(compressed, &mut out)?;
    out.truncate(written);
    Ok(out)
}

/// Scans forward in `bytes` for the next occurrence of [`MAGIC`] -- how a reader resynchronizes
/// after a torn write (a process killed mid-append to a durable buffer file) instead of failing
/// the whole file. Returns the number of bytes skipped, or `None` if `MAGIC` doesn't occur at all
/// (the remainder is definitely not a frame start and should be discarded). Doesn't itself
/// validate that a frame actually starts there -- a caller should still expect
/// [`read_frame`]/[`FrameHeader::read`] to reject a spurious match (e.g. `MAGIC` occurring inside
/// a log message's bytes) and resume scanning past it.
pub fn resync(bytes: &[u8]) -> Option<usize> {
    bytes.windows(MAGIC.len()).position(|w| w == MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_uncompressed() {
        let payload = b"hello logit";
        let framed = write_frame(1, Compression::None, payload).unwrap();
        let mut bytes = framed;
        let (codec, out) = read_frame(&mut bytes).unwrap();
        assert_eq!(codec, 1);
        assert_eq!(&out[..], payload);
        assert!(bytes.is_empty(), "read_frame should consume exactly one frame");
    }

    #[test]
    fn round_trips_lz4_compressed() {
        let payload = "repeat ".repeat(200);
        let framed = write_frame(7, Compression::Lz4, payload.as_bytes()).unwrap();
        // A real repeated payload should actually compress -- otherwise this test isn't
        // exercising the lz4 path at all.
        assert!(framed.len() < payload.len(), "expected compression to shrink the payload");
        let mut bytes = framed;
        let (codec, out) = read_frame(&mut bytes).unwrap();
        assert_eq!(codec, 7);
        assert_eq!(&out[..], payload.as_bytes());
    }

    #[test]
    fn concatenated_frames_are_each_independently_decodable() {
        let a = write_frame(1, Compression::None, b"first").unwrap();
        let b = write_frame(2, Compression::Lz4, b"second, a bit longer to compress").unwrap();
        let mut both = BytesMut::new();
        both.put_slice(&a);
        both.put_slice(&b);
        let mut cursor = both.freeze();

        let (codec_a, payload_a) = read_frame(&mut cursor).unwrap();
        assert_eq!(codec_a, 1);
        assert_eq!(&payload_a[..], b"first");

        let (codec_b, payload_b) = read_frame(&mut cursor).unwrap();
        assert_eq!(codec_b, 2);
        assert_eq!(&payload_b[..], b"second, a bit longer to compress");

        assert!(cursor.is_empty());
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bad = write_frame(1, Compression::None, b"x").unwrap();
        // Corrupt the first magic byte.
        let mut mutated = BytesMut::from(&bad[..]);
        mutated[0] = b'X';
        bad = mutated.freeze();
        assert!(matches!(read_frame(&mut bad.clone()), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn rejects_unknown_version() {
        let framed = write_frame(1, Compression::None, b"x").unwrap();
        let mut mutated = BytesMut::from(&framed[..]);
        mutated[4] = 0xFF; // version low byte
        mutated[5] = 0xFF;
        let mut bad = mutated.freeze();
        assert!(matches!(read_frame(&mut bad), Err(CodecError::Unsupported(_))));
    }

    #[test]
    fn rejects_corrupt_crc() {
        let framed = write_frame(1, Compression::None, b"hello").unwrap();
        let mut mutated = BytesMut::from(&framed[..]);
        let last = mutated.len() - 1;
        mutated[last] ^= 0xFF; // flip a payload byte without touching the header's crc field
        let mut bad = mutated.freeze();
        assert!(
            matches!(read_frame(&mut bad), Err(CodecError::Malformed(msg)) if msg.contains("crc32c"))
        );
    }

    #[test]
    fn a_header_truncated_by_one_byte_is_truncated_not_malformed() {
        let framed = write_frame(1, Compression::None, b"hello").unwrap();
        let mut short = framed.slice(..HEADER_LEN - 1);
        assert!(matches!(read_frame(&mut short), Err(CodecError::Truncated { needed: 1 })));
    }

    #[test]
    fn a_body_truncated_by_one_byte_is_truncated_not_malformed() {
        let framed = write_frame(1, Compression::None, b"hello").unwrap();
        let mut short = framed.slice(..framed.len() - 1);
        assert!(matches!(read_frame(&mut short), Err(CodecError::Truncated { needed: 1 })));
    }

    #[test]
    fn write_frame_rejects_zstd_rather_than_panicking() {
        assert!(matches!(
            write_frame(1, Compression::Zstd, b"payload"),
            Err(CodecError::Unsupported(_))
        ));
    }

    #[test]
    fn rejects_zstd_as_unsupported_until_it_is_implemented() {
        // write_frame now rejects Compression::Zstd with Unsupported directly (see the test
        // above), so this constructs the header by hand to reach the *decode*-side rejection
        // specifically.
        let mut header = BytesMut::new();
        header.put_slice(&MAGIC);
        header.put_u16_le(VERSION);
        header.put_u16_le(0);
        header.put_u8(1);
        header.put_u8(Compression::Zstd as u8);
        header.put_u16_le(0);
        header.put_u32_le(0);
        header.put_u32_le(0);
        header.put_u32_le(crc32c::crc32c(&[]));
        let mut bytes = header.freeze();
        assert!(matches!(read_frame(&mut bytes), Err(CodecError::Unsupported(_))));
    }

    #[test]
    fn resync_finds_the_next_frame_start_after_garbage() {
        let garbage = b"not a frame, just noise";
        let framed = write_frame(1, Compression::None, b"payload").unwrap();
        let mut buf = BytesMut::new();
        buf.put_slice(garbage);
        buf.put_slice(&framed);
        let bytes = buf.freeze();

        let skip = resync(&bytes).expect("magic should be found");
        assert_eq!(skip, garbage.len());
        let mut rest = bytes.slice(skip..);
        let (codec, payload) = read_frame(&mut rest).unwrap();
        assert_eq!(codec, 1);
        assert_eq!(&payload[..], b"payload");
    }

    #[test]
    fn resync_returns_none_when_magic_never_occurs() {
        assert_eq!(resync(b"nothing here looks like a frame"), None);
    }

    #[test]
    fn rejects_a_frame_that_decompresses_shorter_than_its_header_declares() {
        let payload = "repeat ".repeat(200);
        let framed = write_frame(1, Compression::Lz4, payload.as_bytes()).unwrap();
        let mut mutated = BytesMut::from(&framed[..]);
        let inflated = (payload.len() + 16) as u32;
        mutated[12..16].copy_from_slice(&inflated.to_le_bytes());
        let mut bad = mutated.freeze();
        assert!(matches!(read_frame(&mut bad), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn rejects_an_uncompressed_len_over_the_sanity_cap() {
        let framed = write_frame(1, Compression::Lz4, b"small").unwrap();
        let mut mutated = BytesMut::from(&framed[..]);
        mutated[12..16].copy_from_slice(&(MAX_SANE_UNCOMPRESSED_LEN + 1).to_le_bytes());
        let mut bad = mutated.freeze();
        assert!(
            matches!(read_frame(&mut bad), Err(CodecError::Malformed(msg)) if msg.contains("sanity cap"))
        );
    }

    /// F5: `compressed_len` (header bytes 16..20, verified against `FrameHeader::write` above --
    /// magic 0..4, version 4..6, flags 6..8, codec 8, compression 9, reserved 10..12,
    /// uncompressed_len 12..16, compressed_len 16..20, crc32c 20..24) previously had no upper
    /// bound before being used to size a read, unlike `uncompressed_len` a few lines above it.
    /// This is the proof it's now capped, and that a corrupted length field is correctly
    /// classified as `Malformed` (corruption -- resync past it) rather than `Truncated`
    /// (indistinguishable from a genuine short read/clean end-of-file).
    #[test]
    fn rejects_a_compressed_len_over_the_sanity_cap() {
        let framed = write_frame(1, Compression::None, b"small").unwrap();
        let mut mutated = BytesMut::from(&framed[..]);
        mutated[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        let mut bad = mutated.freeze();
        assert!(
            matches!(read_frame(&mut bad), Err(CodecError::Malformed(msg)) if msg.contains("sanity cap")),
            "an oversized compressed_len must be Malformed, not Truncated -- a Truncated result \
             here would be indistinguishable from a genuine clean end-of-file to a caller like \
             `DiskQueue::read_record_at`'s `walk_segment`, silently discarding every record after \
             it instead of resyncing"
        );
    }
}
