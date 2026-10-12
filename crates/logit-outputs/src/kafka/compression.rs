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
//! The decompressors exist for the tests' fake broker and for reading the recorded corpus, so
//! they're test-only. Snappy decompression reads both forms: a payload that opens with the xerial
//! magic is xerial-framed (a 16-byte header, then raw blocks, each behind a big-endian `u32`
//! length), and anything else is one raw block, the same fallback the Java client's reader has.

use std::io::Write;

use bytes::{BufMut, BytesMut};
use kafka_protocol::records::Compression;
use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};

/// Compresses `records`, a batch's encoded records section, into `out` under `codec`.
pub(crate) fn compress(
    codec: Compression,
    records: &[u8],
    out: &mut BytesMut,
) -> anyhow::Result<()> {
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
#[cfg(test)]
pub(crate) const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

/// Decompresses a batch's records section written under `codec`.
#[cfg(test)]
pub(crate) fn decompress(codec: Compression, payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    use std::io::Read;

    let mut out = Vec::new();
    match codec {
        Compression::None => out.extend_from_slice(payload),
        Compression::Gzip => {
            flate2::read::GzDecoder::new(payload).read_to_end(&mut out)?;
        }
        Compression::Snappy if payload.starts_with(&XERIAL_MAGIC) => {
            out = xerial_decompress(payload)?;
        }
        Compression::Snappy => out = snap::raw::Decoder::new().decompress_vec(payload)?,
        Compression::Lz4 => {
            lz4_flex::frame::FrameDecoder::new(payload).read_to_end(&mut out)?;
        }
        Compression::Zstd => {
            ruzstd::decoding::StreamingDecoder::new(payload)
                .map_err(|err| anyhow::anyhow!("zstd frame header: {err}"))?
                .read_to_end(&mut out)?;
        }
    }
    Ok(out)
}

/// Reads a xerial-framed Snappy payload: the magic, a big-endian version and compatible version,
/// then raw Snappy blocks, each behind its big-endian `u32` length.
#[cfg(test)]
fn xerial_decompress(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    const HEADER: usize = XERIAL_MAGIC.len() + 8;
    anyhow::ensure!(payload.len() >= HEADER, "xerial snappy header is truncated");
    let mut rest = &payload[HEADER..];
    let mut out = Vec::new();
    let mut decoder = snap::raw::Decoder::new();
    while !rest.is_empty() {
        anyhow::ensure!(rest.len() >= 4, "xerial snappy block length is truncated");
        let len = u32::from_be_bytes(rest[..4].try_into().expect("four bytes")) as usize;
        rest = &rest[4..];
        anyhow::ensure!(rest.len() >= len, "xerial snappy block is truncated");
        out.extend_from_slice(&decoder.decompress_vec(&rest[..len])?);
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
            assert_eq!(decompress(codec, &out).unwrap(), data, "{codec:?}");
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

    /// Two xerial blocks, as snappy-java writes a payload longer than its block size.
    #[test]
    fn xerial_reader_concatenates_its_blocks() {
        let mut payload = XERIAL_MAGIC.to_vec();
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        for part in [&b"first half, "[..], b"second half"] {
            let block = snap::raw::Encoder::new().compress_vec(part).unwrap();
            payload.extend_from_slice(&(block.len() as u32).to_be_bytes());
            payload.extend_from_slice(&block);
        }
        assert_eq!(decompress(Compression::Snappy, &payload).unwrap(), b"first half, second half");
    }

    #[test]
    fn a_truncated_xerial_block_is_an_error() {
        let mut payload = XERIAL_MAGIC.to_vec();
        payload.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 9, 1]);
        assert!(decompress(Compression::Snappy, &payload).is_err());
    }
}
