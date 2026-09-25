//! OTLP/gRPC message framing and bounded gzip inflation, shared by `otlp_in`'s gRPC and HTTP
//! handlers and the `otlp_grpc` fuzz target.
//!
//! **The length prefix.** A gRPC message on the wire is a 5-byte prefix and then the message:
//! one `Compressed-Flag` byte (`0` identity, `1` compressed with the request's `grpc-encoding`)
//! and a 4-byte big-endian `Message-Length` (gRPC over HTTP/2, "Length-Prefixed-Message").
//! [`unframe`] reads one such message off the front of a body. Bytes after it are ignored.
//!
//! **The inflation bound.** [`inflate_bounded`] reads at most `max + 1` decompressed bytes and
//! rejects the `+1`th as [`InflateError::TooLarge`]; ADR
//! `otlp-compression-and-decompression-bounds` has the why.

use bytes::Bytes;
use std::io::Read;

/// Splits one length-prefixed gRPC message off `bytes`, returning its compressed flag and its
/// payload. `None` for fewer than 5 bytes, a flag byte other than `0`/`1`, or a declared length
/// past the end of `bytes`.
pub fn unframe(bytes: &[u8]) -> Option<(bool, &[u8])> {
    if bytes.len() < 5 {
        return None;
    }
    let compressed = match bytes[0] {
        0 => false,
        1 => true,
        _ => return None,
    };
    let len = u32::from_be_bytes(bytes[1..5].try_into().expect("checked len >= 5 above")) as usize;
    bytes.get(5..5 + len).map(|payload| (compressed, payload))
}

/// Why [`inflate_bounded`] failed: not valid gzip, or decompressed past the cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflateError {
    Malformed,
    TooLarge,
}

/// Inflates gzip `compressed` into at most `max` bytes. See the module doc's "The inflation
/// bound".
pub fn inflate_bounded(compressed: &[u8], max: usize) -> Result<Bytes, InflateError> {
    let mut decoder = flate2::read::GzDecoder::new(compressed).take(max as u64 + 1);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).map_err(|_| InflateError::Malformed)?;
    if out.len() > max {
        return Err(InflateError::TooLarge);
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn frame(flag: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = vec![flag];
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    #[test]
    fn unframe_returns_the_flag_and_the_declared_payload() {
        assert_eq!(unframe(&frame(0, b"abc")), Some((false, &b"abc"[..])));
        assert_eq!(unframe(&frame(1, b"abc")), Some((true, &b"abc"[..])));
        assert_eq!(unframe(&frame(0, b"")), Some((false, &b""[..])));
    }

    #[test]
    fn unframe_ignores_bytes_after_the_first_message() {
        let mut two = frame(0, b"one");
        two.extend_from_slice(&frame(0, b"two"));
        assert_eq!(unframe(&two), Some((false, &b"one"[..])));
    }

    #[test]
    fn unframe_rejects_a_short_prefix_an_unknown_flag_and_an_overrunning_length() {
        assert_eq!(unframe(&[0, 0, 0, 0]), None);
        assert_eq!(unframe(&frame(2, b"abc")), None);
        let mut overrun = frame(0, b"abc");
        overrun.pop();
        assert_eq!(unframe(&overrun), None);
        assert_eq!(unframe(&[0, 0xff, 0xff, 0xff, 0xff]), None);
    }

    #[test]
    fn inflate_bounded_round_trips_gzip() {
        assert_eq!(inflate_bounded(&gzip(b"hello"), 5).unwrap(), Bytes::from_static(b"hello"));
    }

    #[test]
    fn inflate_bounded_rejects_one_byte_past_the_cap() {
        let zeros = vec![0u8; 1025];
        assert_eq!(inflate_bounded(&gzip(&zeros), 1024), Err(InflateError::TooLarge));
        assert_eq!(inflate_bounded(&gzip(&zeros), 1025).unwrap().len(), 1025);
    }

    #[test]
    fn inflate_bounded_rejects_bytes_that_are_not_gzip() {
        assert_eq!(inflate_bounded(b"not gzip", 1024), Err(InflateError::Malformed));
    }
}
