//! Bounded zstd decompression for `datadog_in` ([`crate::datadog`]), over `ruzstd`, a pure-Rust
//! decoder ([ADR `datadog-agent-and-intake-relay`](../../../../docs/adr/datadog-agent-and-intake-relay.md),
//! decision 4).
//!
//! [`decompress`] reads every frame in a request body into one buffer, bounded three ways:
//!
//! - **The output.** Frames are read through `Read::take(cap + 1)` into a `Vec` allocated once at
//!   `cap + 1`, so the buffer never grows and a body inflating past `cap` is caught rather than
//!   truncated to fit: the `otlp_in` `inflate` pattern
//!   ([ADR `otlp-compression-and-decompression-bounds`](../../../../docs/adr/otlp-compression-and-decompression-bounds.md)).
//! - **The window.** Each frame's decoder caps the declared window at
//!   `max(cap, MIN_WINDOW_CAP)` (8 MiB). `ruzstd` reserves a frame's declared window up front,
//!   before a byte is decoded, so without a cap a 20-byte frame header could ask for the format's
//!   3.75 TiB maximum. The floor exists because a Go `klauspost/compress` streaming writer (what a
//!   Datadog Agent's forwarder uses) declares an 8 MiB window regardless of how little it actually
//!   writes, so capping the window at a smaller route's own `cap` would `413` a legitimately small
//!   body compressed under that window. Raising the window ceiling doesn't raise how much decoded
//!   data a route accepts: the output bound below is still `cap`, unaffected by the floor. A frame
//!   declaring a window above `max(cap, MIN_WINDOW_CAP)` is [`ZstdError::TooLarge`], the same
//!   answer an oversized output gets.
//! - **The checksum.** `ruzstd` reads a frame's content checksum but never compares it. With the
//!   crate's `hash` feature on, [`decompress`] compares it with the one computed while decoding,
//!   and a mismatch is [`ZstdError::Malformed`].
//!
//! **Every frame, not the first.** A `StreamingDecoder` decodes one frame. The zstd format allows a
//! body of several concatenated frames and of skippable frames (magic `0x184D2A50..=0x184D2A5F`,
//! a 4-byte little-endian length, then that many opaque bytes), and a Datadog Agent's Go
//! compressor can send both. So once a frame ends with input left over, a new decoder is built on
//! the rest (reusing the first one's buffers), and a skippable frame is stepped over.

use ruzstd::decoding::errors::{FrameDecoderError, ReadFrameHeaderError};
use ruzstd::decoding::{FrameDecoder, StreamingDecoder};
use std::io::Read;

/// The floor under a frame's declared-window cap, regardless of how small a route's own
/// decompressed cap is. A Go `klauspost/compress` streaming writer -- what a Datadog Agent's
/// forwarder uses -- declares an 8 MiB window on every frame it writes, independent of the
/// payload size, so a route capped below 8 MiB (the 5 MiB metrics/logs cap, say) would otherwise
/// `413` every legitimate body that writer produces.
const MIN_WINDOW_CAP: usize = 8 * 1024 * 1024;

/// Why [`decompress`] failed. The caller answers `413` for [`Self::TooLarge`] and `400` for
/// [`Self::Malformed`], as `otlp_in` does for gzip.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ZstdError {
    /// The output passed `cap`, or a frame declared a window larger than
    /// `max(cap, MIN_WINDOW_CAP)`.
    TooLarge,
    /// Not a valid zstd body: a bad magic number, a corrupt block, a truncated frame or skippable
    /// frame, or a content checksum that doesn't match.
    Malformed(String),
}

/// Decompresses every frame in `input`, bounded to `cap` bytes of output (this module's doc). An
/// empty `input` holds no frames and decompresses to an empty body.
pub(crate) fn decompress(input: &[u8], cap: usize) -> Result<Vec<u8>, ZstdError> {
    let mut frames = Frames { rest: input, current: None, spare: None, cap, error: None };
    let mut out = Vec::with_capacity(cap + 1);
    let read = (&mut frames).take(cap as u64 + 1).read_to_end(&mut out);
    // `Frames::read` stores the typed cause before it returns the `io::Error` that stops
    // `read_to_end`, so the stored value is what failed.
    if let Some(error) = frames.error.take() {
        return Err(error);
    }
    if let Err(err) = read {
        return Err(ZstdError::Malformed(err.to_string()));
    }
    if out.len() > cap {
        return Err(ZstdError::TooLarge);
    }
    Ok(out)
}

/// A `Read` over every frame in a body, one `StreamingDecoder` at a time.
struct Frames<'a> {
    /// The input not yet handed to a decoder. Empty while `current` holds it.
    rest: &'a [u8],
    /// The frame being decoded; its reader owns the remaining input.
    current: Option<StreamingDecoder<&'a [u8], FrameDecoder>>,
    /// The previous frame's decoder, kept so the next frame reuses its buffers.
    spare: Option<FrameDecoder>,
    cap: usize,
    /// The typed cause of the `io::Error` a `read` returned, for [`decompress`] to report.
    error: Option<ZstdError>,
}

impl Frames<'_> {
    fn fail(&mut self, error: ZstdError) -> std::io::Error {
        let err = std::io::Error::other(match &error {
            ZstdError::TooLarge => "zstd frame window exceeds the window cap".to_string(),
            ZstdError::Malformed(message) => message.clone(),
        });
        self.error = Some(error);
        err
    }

    /// Starts the next data frame in `rest`, stepping over skippable frames. `Ok(false)` once the
    /// input is exhausted.
    fn next_frame(&mut self) -> std::io::Result<bool> {
        loop {
            if self.rest.is_empty() {
                return Ok(false);
            }
            let mut decoder = self.spare.take().unwrap_or_default();
            decoder.set_max_window_size(self.cap.max(MIN_WINDOW_CAP) as u64);
            // `new_with_decoder` reads the frame header from `rest`, advancing it; on a skippable
            // frame it has consumed the 8-byte magic and length, and `length` bytes remain to skip.
            match StreamingDecoder::new_with_decoder(self.rest, decoder) {
                Ok(streaming) => {
                    self.rest = &[];
                    self.current = Some(streaming);
                    return Ok(true);
                }
                Err(FrameDecoderError::ReadFrameHeaderError(ReadFrameHeaderError::SkipFrame {
                    length,
                    ..
                })) => {
                    // The header read advanced a copy of the slice, not `rest`: skip the 8 header
                    // bytes and then the payload.
                    let skip = 8usize.saturating_add(length as usize);
                    let Some(after) = self.rest.get(skip..) else {
                        return Err(self
                            .fail(ZstdError::Malformed("truncated zstd skippable frame".into())));
                    };
                    self.rest = after;
                }
                Err(FrameDecoderError::WindowSizeTooBig { .. }) => {
                    return Err(self.fail(ZstdError::TooLarge));
                }
                Err(err) => {
                    return Err(
                        self.fail(ZstdError::Malformed(format!("invalid zstd frame: {err}")))
                    )
                }
            }
        }
    }

    /// Checks the finished frame's content checksum, when it carried one, and hands its input and
    /// buffers back for the next frame.
    fn finish_frame(&mut self) -> std::io::Result<()> {
        let streaming = self.current.take().expect("finish_frame is called with a frame open");
        let (rest, decoder) = streaming.into_parts();
        if let Some(sent) = decoder.get_checksum_from_data() {
            if decoder.get_calculated_checksum() != Some(sent) {
                return Err(
                    self.fail(ZstdError::Malformed("zstd content checksum mismatch".into()))
                );
            }
        }
        self.rest = rest;
        self.spare = Some(decoder);
        Ok(())
    }
}

impl Read for Frames<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if self.current.is_none() && !self.next_frame()? {
                return Ok(0);
            }
            let streaming = self.current.as_mut().expect("next_frame opened a frame");
            match streaming.read(buf) {
                Ok(0) => self.finish_frame()?,
                Ok(n) => return Ok(n),
                Err(err) => {
                    return Err(self.fail(ZstdError::Malformed(format!("invalid zstd body: {err}"))))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruzstd::encoding::{compress_to_vec, CompressionLevel};

    fn frame(data: &[u8]) -> Vec<u8> {
        compress_to_vec(data, CompressionLevel::Fastest)
    }

    /// A skippable frame: magic `0x184D2A50`, a little-endian length, then that many bytes.
    fn skippable(payload: &[u8]) -> Vec<u8> {
        let mut out = 0x184D_2A50u32.to_le_bytes().to_vec();
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn a_single_frame_round_trips() {
        let data = b"{\"series\":[]}".repeat(100);
        assert_eq!(decompress(&frame(&data), 1 << 20), Ok(data));
    }

    #[test]
    fn two_concatenated_frames_decode_as_one_body() {
        let mut body = frame(b"first half, ");
        body.extend(frame(b"second half"));
        assert_eq!(decompress(&body, 1 << 20), Ok(b"first half, second half".to_vec()));
    }

    #[test]
    fn a_skippable_frame_before_a_data_frame_is_stepped_over() {
        let mut body = skippable(b"opaque metadata the decoder must ignore");
        body.extend(frame(b"payload"));
        body.extend(skippable(b""));
        assert_eq!(decompress(&body, 1 << 20), Ok(b"payload".to_vec()));
    }

    #[test]
    fn a_truncated_skippable_frame_is_malformed() {
        let mut body = skippable(b"twelve bytes");
        body.truncate(body.len() - 1);
        assert!(matches!(decompress(&body, 1 << 20), Err(ZstdError::Malformed(_))));
    }

    /// A hand-built frame header: descriptor `0x00` (no single segment, so a window descriptor
    /// follows; no checksum; no dictionary; no content size) and window descriptor `0x70`, exponent
    /// 14, so a 16 MiB window, then one empty last raw block. Valid zstd; only its window is large.
    #[test]
    fn a_frame_declaring_a_window_above_the_cap_is_too_large() {
        let mut body = 0xFD2F_B528u32.to_le_bytes().to_vec();
        body.extend_from_slice(&[0x00, 0x70]);
        body.extend_from_slice(&[0x01, 0x00, 0x00]);
        assert_eq!(
            decompress(&body, 16 * 1024 * 1024),
            Ok(Vec::new()),
            "the frame is valid at a 16 MiB cap"
        );
        assert_eq!(
            decompress(&body, 5 * 1024 * 1024),
            Err(ZstdError::TooLarge),
            "a 5 MiB cap is still floored at MIN_WINDOW_CAP (8 MiB), below the 16 MiB window"
        );
    }

    /// `ruzstd`'s encoder declares a 128 KiB window, so every cap below is at least that: these
    /// tests are about the output bound, not the window one.
    #[test]
    fn an_output_above_the_cap_is_too_large() {
        let cap = 256 * 1024;
        let body = frame(&vec![0u8; cap + 1]);
        assert!(body.len() < cap, "the compressed body itself is small");
        assert_eq!(decompress(&body, cap), Err(ZstdError::TooLarge));
        assert_eq!(decompress(&frame(&vec![0u8; cap]), cap), Ok(vec![0u8; cap]), "exactly the cap");
    }

    #[test]
    fn two_frames_that_together_pass_the_cap_are_too_large() {
        let cap = 200 * 1024;
        let mut body = frame(&vec![1u8; 120 * 1024]);
        body.extend(frame(&vec![2u8; 120 * 1024]));
        assert_eq!(decompress(&body, cap), Err(ZstdError::TooLarge));
    }

    #[test]
    fn a_corrupted_checksum_is_malformed() {
        let mut body = frame(b"checksummed payload");
        let last = body.len() - 1;
        body[last] ^= 0xFF;
        assert!(matches!(decompress(&body, 1 << 20), Err(ZstdError::Malformed(_))), "{body:?}");
    }

    #[test]
    fn garbage_is_malformed() {
        assert!(matches!(decompress(b"not zstd at all", 1 << 20), Err(ZstdError::Malformed(_))));
    }

    #[test]
    fn trailing_garbage_after_a_frame_is_malformed() {
        let mut body = frame(b"payload");
        body.extend_from_slice(b"junk");
        assert!(matches!(decompress(&body, 1 << 20), Err(ZstdError::Malformed(_))));
    }

    #[test]
    fn an_empty_body_decodes_to_nothing() {
        assert_eq!(decompress(b"", 1 << 20), Ok(Vec::new()));
    }
}
