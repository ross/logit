//! Remote-write body compression: the `Content-Encoding` a request carries, and a bounded
//! decompressor for each. `prometheus_in`'s receiver and `prometheus_out`'s sender share it, so
//! the two ends can't disagree about what `snappy` or `zstd` means on the wire.
//!
//! | `Content-Encoding` | Format | Who sends it |
//! |---|---|---|
//! | `snappy` | Snappy **block** format (`snap::raw`), never the framed one | both remote-write specs, every sender |
//! | `zstd` | one or more zstd frames (RFC 8878) | the VictoriaMetrics remote write protocol: vmagent by default, over 1.0 only |
//!
//! [`decompress_bounded`] never decodes more than one byte past `max`. Snappy's block header
//! declares its decompressed length, so [`snap::raw::decompress_len`] is checked before a byte is
//! expanded. A zstd frame's content size is optional and its header can claim any window, so the
//! zstd arm guards three ways:
//!
//! 1. **Declared content size.** A frame that declares one (vmagent's always do) is refused before
//!    any block is decoded if it, plus the frames before it, exceeds `max`.
//! 2. **Window size.** The decoder's window limit is `max`, so a frame declaring a larger window
//!    fails to initialize, before the window is allocated.
//! 3. **Streaming output.** Each frame is read through a byte budget of `max + 1` minus what the
//!    earlier frames produced, so a frame that declares no size and inflates past `max` stops one
//!    byte over rather than being decoded whole.
//!
//! Concatenated frames and skippable frames are both legal zstd and both accepted. A frame whose
//! content checksum, or non-zero declared content size, doesn't match its output is
//! [`DecompressError::Malformed`].
//!
//! [ADR `victoriametrics-interop`](../../../../docs/adr/victoriametrics-interop.md) records why
//! zstd is here and why `ruzstd` implements it.

use ruzstd::decoding::errors::{FrameDecoderError, FrameHeaderError, ReadFrameHeaderError};
use ruzstd::decoding::{FrameDecoder, StreamingDecoder};
use ruzstd::encoding::CompressionLevel;
use std::io::Read;

/// `Content-Encoding: snappy`, the Snappy **block** format both remote-write specs mandate.
pub const CONTENT_ENCODING_SNAPPY: &str = "snappy";
/// `Content-Encoding: zstd`, the VictoriaMetrics remote write protocol's encoding.
pub const CONTENT_ENCODING_ZSTD: &str = "zstd";

/// A remote-write body's compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encoding {
    #[default]
    Snappy,
    Zstd,
}

impl Encoding {
    /// The encoding a `Content-Encoding` value names, ASCII case ignored. `None` for anything
    /// else, an empty (absent) header included: remote-write has no identity mode.
    pub fn from_header(value: &str) -> Option<Encoding> {
        if value.eq_ignore_ascii_case(CONTENT_ENCODING_SNAPPY) {
            Some(Encoding::Snappy)
        } else if value.eq_ignore_ascii_case(CONTENT_ENCODING_ZSTD) {
            Some(Encoding::Zstd)
        } else {
            None
        }
    }

    /// The `Content-Encoding` value, which is also the `encoding` telemetry tag.
    pub fn as_str(self) -> &'static str {
        match self {
            Encoding::Snappy => CONTENT_ENCODING_SNAPPY,
            Encoding::Zstd => CONTENT_ENCODING_ZSTD,
        }
    }
}

/// Why [`decompress_bounded`] refused a body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecompressError {
    /// Not a valid body in the named encoding. The string is the reason, for a `400`'s text.
    #[error("invalid {encoding} body: {reason}")]
    Malformed { encoding: &'static str, reason: String },
    /// The body would decompress past `max`. `declared` is the size a header claimed (Snappy's
    /// length, or zstd content sizes so far plus this frame's), `None` when the bound was hit by a
    /// window claim or while streaming.
    #[error("{}", too_large_message(*.declared, *.max))]
    TooLarge { declared: Option<usize>, max: usize },
}

fn too_large_message(declared: Option<usize>, max: usize) -> String {
    match declared {
        Some(declared) => {
            format!("decompressed request would be {declared} bytes, over the {max}-byte limit")
        }
        None => format!("decompressed request exceeds the {max}-byte limit"),
    }
}

/// [`compress`] failed. Snappy's block format can't hold more than `u32::MAX` bytes; zstd
/// never fails.
#[derive(Debug, thiserror::Error)]
#[error("snappy-compressing a remote-write request body: {0}")]
pub struct CompressError(#[from] snap::Error);

/// Compresses one request body. zstd uses `ruzstd`'s `Fastest` level, about libzstd level 1 and
/// the best `ruzstd` implements.
pub fn compress(encoding: Encoding, body: &[u8]) -> Result<Vec<u8>, CompressError> {
    match encoding {
        Encoding::Snappy => Ok(snap::raw::Encoder::new().compress_vec(body)?),
        Encoding::Zstd => Ok(ruzstd::encoding::compress_to_vec(body, CompressionLevel::Fastest)),
    }
}

/// Decompresses one request body, never decoding more than `max + 1` bytes of output (the module
/// doc's guards).
pub fn decompress_bounded(
    encoding: Encoding,
    body: &[u8],
    max: usize,
) -> Result<Vec<u8>, DecompressError> {
    match encoding {
        Encoding::Snappy => decompress_snappy(body, max),
        Encoding::Zstd => decompress_zstd(body, max),
    }
}

fn decompress_snappy(body: &[u8], max: usize) -> Result<Vec<u8>, DecompressError> {
    let malformed = |err: snap::Error| DecompressError::Malformed {
        encoding: "snappy",
        reason: err.to_string(),
    };
    let declared = snap::raw::decompress_len(body).map_err(malformed)?;
    if declared > max {
        return Err(DecompressError::TooLarge { declared: Some(declared), max });
    }
    snap::raw::Decoder::new().decompress_vec(body).map_err(malformed)
}

fn decompress_zstd(body: &[u8], max: usize) -> Result<Vec<u8>, DecompressError> {
    let malformed = |reason: String| DecompressError::Malformed { encoding: "zstd", reason };
    if body.is_empty() {
        return Err(malformed("empty body, no zstd frame".to_string()));
    }
    let mut input = body;
    let mut out = Vec::new();
    let mut decoder = FrameDecoder::new();
    // Guard 2: a frame claiming a larger window fails `init` before the window is allocated.
    decoder.set_max_window_size(max as u64);
    while !input.is_empty() {
        let mut frame = match StreamingDecoder::new_with_decoder(&mut input, &mut decoder) {
            Ok(frame) => frame,
            Err(FrameDecoderError::ReadFrameHeaderError(ReadFrameHeaderError::SkipFrame {
                length,
                ..
            })) => {
                // The header read consumed the magic number and length; the payload is opaque.
                let length = length as usize;
                if length > input.len() {
                    return Err(malformed(format!(
                        "skippable frame claims {length} bytes, {} remain",
                        input.len()
                    )));
                }
                input = &input[length..];
                continue;
            }
            // Over the cap, or over the spec's own maximum, which `ruzstd` reports apart.
            Err(
                FrameDecoderError::WindowSizeTooBig { .. }
                | FrameDecoderError::FrameHeaderError(FrameHeaderError::WindowTooBig { .. }),
            ) => {
                return Err(DecompressError::TooLarge { declared: None, max });
            }
            Err(err) => return Err(malformed(err.to_string())),
        };
        // Guard 1: `content_size()` is `0` when the frame declares none, which passes.
        let declared = usize::try_from(frame.decoder.content_size()).unwrap_or(usize::MAX);
        if declared > max - out.len() {
            return Err(DecompressError::TooLarge {
                declared: Some(out.len().saturating_add(declared)),
                max,
            });
        }
        // Guard 3: one byte past what's left, so an overrun is seen rather than truncated to fit.
        let before = out.len();
        let budget = (max - before) as u64 + 1;
        (&mut frame)
            .take(budget)
            .read_to_end(&mut out)
            .map_err(|err| malformed(err.to_string()))?;
        if out.len() > max {
            return Err(DecompressError::TooLarge { declared: None, max });
        }
        // `0` is also what an undeclared size reads as, so only a non-zero one is checked.
        let produced = out.len() - before;
        if declared != 0 && produced != declared {
            return Err(malformed(format!(
                "frame declares {declared} bytes of content and decoded to {produced}"
            )));
        }
        let checksums =
            (frame.decoder.get_checksum_from_data(), frame.decoder.get_calculated_checksum());
        if let (Some(sent), Some(computed)) = checksums {
            if sent != computed {
                return Err(malformed(format!(
                    "content checksum {sent:#010x} doesn't match the decoded {computed:#010x}"
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

    /// `ruzstd`'s encoder declares a 128 KiB window whatever the input, so a cap under that
    /// refuses its frames by guard 2; tests that compress with it decode under this cap.
    const ENCODER_WINDOW: usize = 128 * 1024;

    /// A zstd frame header: descriptor byte, then an optional window descriptor, then an optional
    /// 4-byte content size. `window` is the window-descriptor byte (`0x00` = 1 KiB, `0x50` =
    /// 1 MiB, `0x68` = 8 MiB), absent in single-segment mode.
    fn frame_header(content_size: Option<u32>, window: u8) -> Vec<u8> {
        let mut header = ZSTD_MAGIC.to_vec();
        match content_size {
            // FCS flag 2 (4 bytes), no single segment, no checksum, no dictionary.
            Some(size) => {
                header.extend([0x80, window]);
                header.extend(size.to_le_bytes());
            }
            None => header.extend([0x00, window]),
        }
        header
    }

    /// An RLE block of `size` copies of `byte`: 3 header bytes plus one.
    fn rle_block(byte: u8, size: u32, last: bool) -> Vec<u8> {
        let header = (size << 3) | (1 << 1) | u32::from(last);
        let mut block = header.to_le_bytes()[..3].to_vec();
        block.push(byte);
        block
    }

    /// A frame with no declared content size and a 1 KiB window, `blocks` RLE blocks of 1 KiB.
    fn undeclared_rle_frame(blocks: usize) -> Vec<u8> {
        let mut frame = frame_header(None, 0x00);
        for i in 0..blocks {
            frame.extend(rle_block(b'a', 1024, i + 1 == blocks));
        }
        frame
    }

    #[test]
    fn from_header_names_both_encodings_and_nothing_else() {
        assert_eq!(Encoding::from_header("snappy"), Some(Encoding::Snappy));
        assert_eq!(Encoding::from_header("ZSTD"), Some(Encoding::Zstd));
        for other in ["", "gzip", "identity", "snappy, zstd"] {
            assert_eq!(Encoding::from_header(other), None, "{other:?}");
        }
    }

    #[test]
    fn both_encodings_round_trip() {
        let body: Vec<u8> = (0..200_000u32).flat_map(|i| (i % 251).to_le_bytes()).collect();
        for encoding in [Encoding::Snappy, Encoding::Zstd] {
            let compressed = compress(encoding, &body).unwrap();
            assert!(compressed.len() < body.len(), "{encoding:?} compresses");
            assert_eq!(decompress_bounded(encoding, &compressed, body.len()).unwrap(), body);
        }
    }

    #[test]
    fn an_empty_body_round_trips_under_zstd() {
        let compressed = compress(Encoding::Zstd, b"").unwrap();
        assert_eq!(decompress_bounded(Encoding::Zstd, &compressed, ENCODER_WINDOW).unwrap(), b"");
    }

    #[test]
    fn a_body_one_byte_over_the_cap_is_too_large_under_both() {
        let body = vec![7u8; 4097];
        let snappy = compress(Encoding::Snappy, &body).unwrap();
        assert_eq!(
            decompress_bounded(Encoding::Snappy, &snappy, 4096),
            Err(DecompressError::TooLarge { declared: Some(4097), max: 4096 })
        );
        let body = vec![7u8; ENCODER_WINDOW + 1];
        let zstd = compress(Encoding::Zstd, &body).unwrap();
        assert_eq!(
            decompress_bounded(Encoding::Zstd, &zstd, ENCODER_WINDOW),
            Err(DecompressError::TooLarge { declared: None, max: ENCODER_WINDOW })
        );
        assert_eq!(decompress_bounded(Encoding::Zstd, &zstd, ENCODER_WINDOW + 1).unwrap(), body);
    }

    /// Guard 1: the header's content size is refused with no block present to decode, so the
    /// refusal can only have come from the header.
    #[test]
    fn a_declared_content_size_over_the_cap_is_refused_before_decoding() {
        let header_only = frame_header(Some(5000), 0x00);
        assert_eq!(
            decompress_bounded(Encoding::Zstd, &header_only, 4096),
            Err(DecompressError::TooLarge { declared: Some(5000), max: 4096 })
        );
    }

    /// Guard 2: a window over the cap, with no content size, fails before a block is read; so
    /// does one past the spec's maximum (`0xff`), which `ruzstd` reports as a header error.
    #[test]
    fn a_window_over_the_cap_is_refused() {
        for window in [0x68, 0xff] {
            let header_only = frame_header(None, window);
            assert_eq!(
                decompress_bounded(Encoding::Zstd, &header_only, 4 * 1024 * 1024),
                Err(DecompressError::TooLarge { declared: None, max: 4 * 1024 * 1024 }),
                "window descriptor {window:#04x}"
            );
        }
    }

    /// Guard 3: no content size, a small window, and output past the cap.
    #[test]
    fn an_undeclared_frame_that_inflates_past_the_cap_is_too_large() {
        let bomb = undeclared_rle_frame(5);
        assert!(bomb.len() < 64, "20 bytes of input");
        assert_eq!(
            decompress_bounded(Encoding::Zstd, &bomb, 4096),
            Err(DecompressError::TooLarge { declared: None, max: 4096 })
        );
        assert_eq!(decompress_bounded(Encoding::Zstd, &bomb, 5120).unwrap(), vec![b'a'; 5120]);
    }

    /// The streaming budget and the declared sizes both count across concatenated frames.
    #[test]
    fn the_cap_counts_across_concatenated_frames() {
        let two_undeclared = [undeclared_rle_frame(3), undeclared_rle_frame(3)].concat();
        assert_eq!(decompress_bounded(Encoding::Zstd, &two_undeclared, 6144).unwrap().len(), 6144);
        assert_eq!(
            decompress_bounded(Encoding::Zstd, &two_undeclared, 4096),
            Err(DecompressError::TooLarge { declared: None, max: 4096 })
        );

        let mut declared = frame_header(Some(3072), 0x00);
        for i in 0..3 {
            declared.extend(rle_block(b'b', 1024, i == 2));
        }
        let two_declared = [declared.clone(), declared].concat();
        assert_eq!(
            decompress_bounded(Encoding::Zstd, &two_declared, 4096),
            Err(DecompressError::TooLarge { declared: Some(6144), max: 4096 })
        );
    }

    /// A frame declaring more content than its blocks hold is corrupt, not short.
    #[test]
    fn a_declared_content_size_the_blocks_disagree_with_is_malformed() {
        let mut frame = frame_header(Some(3000), 0x00);
        frame.extend(rle_block(b'c', 1024, true));
        let err = decompress_bounded(Encoding::Zstd, &frame, 4096).unwrap_err();
        assert!(err.to_string().contains("declares 3000 bytes"), "{err}");
    }

    #[test]
    fn a_skippable_frame_is_skipped() {
        let mut body = vec![0x50, 0x2a, 0x4d, 0x18];
        body.extend(3u32.to_le_bytes());
        body.extend([1, 2, 3]);
        body.extend(compress(Encoding::Zstd, b"payload").unwrap());
        assert_eq!(decompress_bounded(Encoding::Zstd, &body, ENCODER_WINDOW).unwrap(), b"payload");

        let mut truncated = vec![0x50, 0x2a, 0x4d, 0x18];
        truncated.extend(99u32.to_le_bytes());
        assert!(matches!(
            decompress_bounded(Encoding::Zstd, &truncated, 1024),
            Err(DecompressError::Malformed { .. })
        ));
    }

    #[test]
    fn malformed_zstd_is_malformed() {
        let valid = compress(Encoding::Zstd, b"some remote-write body").unwrap();
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("empty", Vec::new()),
            ("not zstd", b"not zstd at all".to_vec()),
            ("truncated", valid[..valid.len() - 3].to_vec()),
            ("trailing garbage", [valid.clone(), b"xx".to_vec()].concat()),
            ("header only", ZSTD_MAGIC.to_vec()),
        ];
        for (name, body) in cases {
            let err = decompress_bounded(Encoding::Zstd, &body, ENCODER_WINDOW).unwrap_err();
            assert!(matches!(err, DecompressError::Malformed { encoding: "zstd", .. }), "{name}");
        }
    }

    /// `ruzstd` writes a content checksum by default; a flipped bit in it is caught.
    #[test]
    fn a_checksum_mismatch_is_malformed() {
        let mut body = compress(Encoding::Zstd, b"checksummed body").unwrap();
        let last = body.len() - 1;
        body[last] ^= 0xff;
        let err = decompress_bounded(Encoding::Zstd, &body, ENCODER_WINDOW).unwrap_err();
        assert!(err.to_string().contains("checksum"), "{err}");
    }

    #[test]
    fn malformed_snappy_is_malformed() {
        let err = decompress_bounded(Encoding::Snappy, b"not snappy at all", 1024).unwrap_err();
        assert!(matches!(err, DecompressError::Malformed { encoding: "snappy", .. }), "{err}");
        assert!(err.to_string().starts_with("invalid snappy body: "), "{err}");
    }
}
