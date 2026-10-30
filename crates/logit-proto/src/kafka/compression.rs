//! The five record-batch codecs, run through `kafka-protocol`'s custom-compression hook so none of
//! the crate's own codec features is built (`docs/adr/kafka-output.md`, item 1).
//!
//! Only the records section of a batch is compressed, never its 61-byte header. What each codec
//! writes:
//!
//! | Codec | Written as | Why this form |
//! |---|---|---|
//! | `none` | the records unchanged | |
//! | `gzip` | one gzip stream (`flate2`) | what both reference clients write |
//! | `snappy` | one raw Snappy block (`snap::raw`) | librdkafka's form; Java writes xerial framing, and brokers and consumers read both |
//! | `lz4` | one LZ4 frame (`lz4_flex`), FLG `0x60`, BD `0x40` | version 1, independent blocks, no checksums, no content size, 64 KB blocks: the bytes both reference clients write |
//! | `zstd` | one zstd frame (`ruzstd`, its fastest level) | |
//!
//! [`decompress`] reads every codec a producer may write, and is bounded the way
//! `crate::otlp::grpc::inflate_bounded` and `crate::prometheus::compression::decompress_bounded`
//! are: it never produces more than one byte past the caller's bound, and that byte is the error.
//! A Snappy block declares its length, so a block that would pass the bound is refused before it
//! expands. A zstd frame's window isn't held to the bound, as `decompress_bounded` holds it,
//! because the Java client's frames declare windows larger than their content; `ruzstd`'s default
//! 128 MiB window limit caps that allocation.
//!
//! Snappy decompression reads both forms: a payload that opens with the xerial magic is
//! xerial-framed (a 16-byte header, then raw blocks, each behind a big-endian `u32` length), and
//! anything else is one raw block, the same fallback the Java client's reader has.

use std::io::{Read, Write};

use bytes::{BufMut, BytesMut};
use kafka_protocol::records::Compression;
use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};

/// Compresses `records`, a batch's encoded records section, into `out` under `codec`.
pub fn compress(codec: Compression, records: &[u8], out: &mut BytesMut) -> anyhow::Result<()> {
    match codec {
        Compression::None => out.put_slice(records),
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(out.writer(), flate2::Compression::default());
            encoder.write_all(records)?;
            encoder.finish()?;
        }
        Compression::Snappy => out.put_slice(&snap::raw::Encoder::new().compress_vec(records)?),
        Compression::Lz4 => {
            let info = FrameInfo::new()
                .block_size(BlockSize::Max64KB)
                .block_mode(BlockMode::Independent)
                .block_checksums(false)
                .content_checksum(false)
                .content_size(None);
            let mut encoder = FrameEncoder::with_frame_info(info, out.writer());
            encoder.write_all(records)?;
            encoder.finish()?;
        }
        Compression::Zstd => out.put_slice(&ruzstd::encoding::compress_to_vec(
            records,
            ruzstd::encoding::CompressionLevel::Fastest,
        )),
    }
    Ok(())
}

/// The magic that opens a xerial-framed Snappy payload, as snappy-java's `SnappyOutputStream`
/// writes it.
pub const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

/// A records section that decompresses past the caller's bound. [`decompress`] returns it inside
/// its `anyhow::Error`, so a caller that counts an oversized batch apart from a corrupt one
/// downcasts to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a {codec:?} records section decompresses past the {max}-byte limit")]
pub struct TooLarge {
    pub codec: Compression,
    pub max: usize,
}

/// Decompresses a batch's records section written under `codec`, producing at most `max + 1`
/// bytes: output past `max` is [`TooLarge`].
pub fn decompress(codec: Compression, payload: &[u8], max: usize) -> anyhow::Result<Vec<u8>> {
    let too_large = || anyhow::Error::new(TooLarge { codec, max });
    let out = match codec {
        Compression::None => {
            if payload.len() > max {
                return Err(too_large());
            }
            payload.to_vec()
        }
        Compression::Gzip => read_bounded(flate2::read::GzDecoder::new(payload), max)?,
        Compression::Snappy if payload.starts_with(&XERIAL_MAGIC) => {
            xerial_decompress(payload, max)?
        }
        Compression::Snappy => {
            let mut out = Vec::new();
            snappy_block(&mut out, payload, max)?;
            out
        }
        Compression::Lz4 => read_bounded(lz4_flex::frame::FrameDecoder::new(payload), max)?,
        Compression::Zstd => {
            let decoder = ruzstd::decoding::StreamingDecoder::new(payload)
                .map_err(|err| anyhow::anyhow!("zstd frame header: {err}"))?;
            read_bounded(decoder, max)?
        }
    };
    if out.len() > max {
        return Err(too_large());
    }
    Ok(out)
}

/// Reads `reader` to its end or to `max + 1` bytes, whichever comes first.
fn read_bounded(reader: impl Read, max: usize) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    reader.take((max as u64).saturating_add(1)).read_to_end(&mut out)?;
    Ok(out)
}

/// Appends one raw Snappy block to `out`, refusing a block whose declared length would take
/// `out` past `max` before expanding a byte of it.
fn snappy_block(out: &mut Vec<u8>, block: &[u8], max: usize) -> anyhow::Result<()> {
    let len = snap::raw::decompress_len(block)?;
    if out.len().saturating_add(len) > max {
        return Err(anyhow::Error::new(TooLarge { codec: Compression::Snappy, max }));
    }
    let start = out.len();
    out.resize(start + len, 0);
    let written = snap::raw::Decoder::new().decompress(block, &mut out[start..])?;
    out.truncate(start + written);
    Ok(())
}

/// Reads a xerial-framed Snappy payload: the magic, a big-endian version and compatible version,
/// then raw Snappy blocks, each behind its big-endian `u32` length. Output past `max` is
/// [`TooLarge`], refused from each block's declared length before the block is expanded.
pub fn xerial_decompress(payload: &[u8], max: usize) -> anyhow::Result<Vec<u8>> {
    const HEADER: usize = XERIAL_MAGIC.len() + 8;
    anyhow::ensure!(payload.len() >= HEADER, "xerial snappy header is truncated");
    let mut rest = &payload[HEADER..];
    let mut out = Vec::new();
    while !rest.is_empty() {
        anyhow::ensure!(rest.len() >= 4, "xerial snappy block length is truncated");
        let len = u32::from_be_bytes(rest[..4].try_into().expect("four bytes")) as usize;
        rest = &rest[4..];
        anyhow::ensure!(rest.len() >= len, "xerial snappy block is truncated");
        snappy_block(&mut out, &rest[..len], max)?;
        rest = &rest[len..];
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    fn compressed(codec: Compression, data: &[u8]) -> Vec<u8> {
        let mut out = BytesMut::new();
        compress(codec, data, &mut out).unwrap();
        out.to_vec()
    }

    #[test]
    fn every_codec_round_trips() {
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        for codec in ALL {
            let out = compressed(codec, &data);
            assert_eq!(decompress(codec, &out, data.len()).unwrap(), data, "{codec:?}");
        }
    }

    /// The LZ4 frame descriptor both reference clients write: magic, FLG `0x60` (version 1,
    /// independent blocks, no checksums, no content size), BD `0x40` (64 KB blocks).
    #[test]
    fn lz4_writes_the_reference_clients_frame_descriptor() {
        let out = compressed(Compression::Lz4, b"records");
        assert_eq!(&out[..6], &[0x04, 0x22, 0x4d, 0x18, 0x60, 0x40]);
    }

    #[test]
    fn snappy_is_one_raw_block_not_xerial() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let out = compressed(Compression::Snappy, data);
        assert!(!out.starts_with(&XERIAL_MAGIC));
        assert_eq!(snap::raw::Decoder::new().decompress_vec(&out).unwrap(), data);
    }

    #[test]
    fn gzip_and_zstd_write_their_standard_magic() {
        assert_eq!(&compressed(Compression::Gzip, b"x")[..3], &[0x1f, 0x8b, 0x08]);
        assert_eq!(&compressed(Compression::Zstd, b"x")[..4], &[0x28, 0xb5, 0x2f, 0xfd]);
    }

    /// A xerial payload of `parts`, one block each, as snappy-java writes a payload longer than
    /// its block size.
    fn xerial(parts: &[&[u8]]) -> Vec<u8> {
        let mut payload = XERIAL_MAGIC.to_vec();
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        for part in parts {
            let block = snap::raw::Encoder::new().compress_vec(part).unwrap();
            payload.extend_from_slice(&(block.len() as u32).to_be_bytes());
            payload.extend_from_slice(&block);
        }
        payload
    }

    #[test]
    fn xerial_reader_concatenates_its_blocks() {
        let payload = xerial(&[b"first half, ", b"second half"]);
        assert_eq!(
            decompress(Compression::Snappy, &payload, 1024).unwrap(),
            b"first half, second half"
        );
    }

    /// `payload` holds `len` bytes under `codec`: it decodes with `len` as the bound and is
    /// [`TooLarge`] with one byte less.
    fn assert_bounded_at(codec: Compression, payload: &[u8], len: usize) {
        assert_eq!(decompress(codec, payload, len).unwrap().len(), len, "{codec:?} at the bound");
        let err = decompress(codec, payload, len - 1).unwrap_err();
        assert_eq!(
            err.downcast_ref::<TooLarge>(),
            Some(&TooLarge { codec, max: len - 1 }),
            "{codec:?} one byte over: {err:#}"
        );
    }

    /// Long runs, so each compressed form is far smaller than what it expands to.
    fn runs() -> Vec<u8> {
        (0..100_000u32).map(|i| (i / 1000) as u8).collect()
    }

    #[test]
    fn none_decodes_at_its_bound_and_refuses_one_byte_over() {
        let data = runs();
        assert_bounded_at(Compression::None, &compressed(Compression::None, &data), data.len());
    }

    #[test]
    fn gzip_decodes_at_its_bound_and_refuses_one_byte_over() {
        let data = runs();
        assert_bounded_at(Compression::Gzip, &compressed(Compression::Gzip, &data), data.len());
    }

    #[test]
    fn raw_snappy_decodes_at_its_bound_and_refuses_one_byte_over() {
        let data = runs();
        assert_bounded_at(Compression::Snappy, &compressed(Compression::Snappy, &data), data.len());
    }

    /// The bound spans blocks: the second block is refused for what the first already produced.
    #[test]
    fn xerial_snappy_decodes_at_its_bound_and_refuses_one_byte_over() {
        let data = runs();
        let (first, second) = data.split_at(60_000);
        assert_bounded_at(Compression::Snappy, &xerial(&[first, second]), data.len());
    }

    #[test]
    fn lz4_decodes_at_its_bound_and_refuses_one_byte_over() {
        let data = runs();
        assert_bounded_at(Compression::Lz4, &compressed(Compression::Lz4, &data), data.len());
    }

    #[test]
    fn zstd_decodes_at_its_bound_and_refuses_one_byte_over() {
        let data = runs();
        assert_bounded_at(Compression::Zstd, &compressed(Compression::Zstd, &data), data.len());
    }

    #[test]
    fn a_truncated_xerial_block_is_an_error() {
        let mut payload = XERIAL_MAGIC.to_vec();
        payload.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 9, 1]);
        assert!(decompress(Compression::Snappy, &payload, 1024).is_err());
    }
}
