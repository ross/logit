//! LEB128 unsigned varints, and zigzag-encoded signed ones: every count, length, and dictionary
//! index in the native payload, and the control messages' fields.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::CodecError;

/// Writes `v` as an unsigned LEB128 varint: 7 payload bits per byte, high bit set on every byte
/// but the last.
pub fn write_uvarint(out: &mut BytesMut, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            out.put_u8(byte | 0x80);
        } else {
            out.put_u8(byte);
            break;
        }
    }
}

/// The inverse of [`write_uvarint`]. Stops at 10 bytes, a `u64`'s maximum, so a stream with the
/// continuation bit always set can't spin forever.
///
/// The 10th byte carries bit 63 alone, so it must be `0x00` or `0x01`; anything else overflows a
/// `u64` and is `Malformed` rather than silently truncated. An over-long encoding of a smaller
/// value (`80 00` for 0) is accepted: no writer emits one, and rejecting it costs a compare per
/// byte.
pub fn read_uvarint(bytes: &mut Bytes) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    for i in 0..10 {
        if bytes.is_empty() {
            return Err(CodecError::Malformed("truncated varint".to_string()));
        }
        let byte = bytes.get_u8();
        result |= ((byte & 0x7f) as u64) << (i * 7);
        if byte & 0x80 == 0 {
            if i == 9 && byte > 1 {
                return Err(tenth_byte_overflows(byte));
            }
            return Ok(result);
        }
    }
    Err(CodecError::Malformed("varint longer than 10 bytes".to_string()))
}

#[cold]
fn tenth_byte_overflows(byte: u8) -> CodecError {
    CodecError::Malformed(format!(
        "varint's 10th byte {byte:#04x} overflows a u64 (only bit 63 is left)"
    ))
}

/// Maps `v` into the unsigned space `write_uvarint` carries so a small negative stays small; two's
/// complement would make every negative `i64` 10 bytes.
pub fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

pub fn write_ivarint(out: &mut BytesMut, v: i64) {
    write_uvarint(out, zigzag_encode(v));
}

pub fn read_ivarint(bytes: &mut Bytes) -> Result<i64, CodecError> {
    read_uvarint(bytes).map(zigzag_decode)
}

/// Fails with [`CodecError::Malformed`] naming `what` if a length-carved slice has bytes its
/// reader didn't consume. Every carve in the native payload ends with this check, so a payload
/// decodes only if re-encoding it would reproduce it.
#[inline]
pub(crate) fn ensure_consumed(bytes: &Bytes, what: &str) -> Result<(), CodecError> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(trailing_bytes(what, bytes.len()))
    }
}

#[cold]
pub(crate) fn trailing_bytes(what: &str, left: usize) -> CodecError {
    CodecError::Malformed(format!("{what} had trailing bytes ({left} left over)"))
}

/// Reads one byte, returning a `CodecError` on empty input where `Buf::get_u8` would panic.
pub fn read_u8(bytes: &mut Bytes) -> Result<u8, CodecError> {
    if bytes.is_empty() {
        return Err(CodecError::Malformed("unexpected end of input reading a byte".to_string()));
    }
    Ok(bytes.get_u8())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_uvarint(v: u64) {
        let mut buf = BytesMut::new();
        write_uvarint(&mut buf, v);
        let mut bytes = buf.freeze();
        assert_eq!(read_uvarint(&mut bytes).unwrap(), v);
        assert!(bytes.is_empty(), "read_uvarint should consume exactly its own bytes");
    }

    #[test]
    fn round_trips_boundary_values() {
        for v in [0u64, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
            round_trip_uvarint(v);
        }
    }

    fn round_trip_ivarint(v: i64) {
        let mut buf = BytesMut::new();
        write_ivarint(&mut buf, v);
        let mut bytes = buf.freeze();
        assert_eq!(read_ivarint(&mut bytes).unwrap(), v);
    }

    #[test]
    fn zigzag_round_trips_negative_and_positive_and_extremes() {
        for v in [0i64, 1, -1, 63, -64, i32::MAX as i64, i32::MIN as i64, i64::MAX, i64::MIN] {
            round_trip_ivarint(v);
        }
    }

    #[test]
    fn zigzag_keeps_small_negative_magnitudes_small_on_the_wire() {
        // -1 costs one byte, not ten.
        let mut buf = BytesMut::new();
        write_ivarint(&mut buf, -1);
        assert_eq!(buf.len(), 1, "zigzag(-1) should be a single-byte varint");
    }

    #[test]
    fn read_uvarint_rejects_truncated_input() {
        // A continuation byte with nothing after it.
        let mut bytes = Bytes::from_static(&[0x80]);
        assert!(matches!(read_uvarint(&mut bytes), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn read_uvarint_rejects_an_unterminated_run_past_ten_bytes() {
        let mut bytes = Bytes::from_static(&[0x80; 11]);
        assert!(matches!(read_uvarint(&mut bytes), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn read_u8_rejects_empty_input_instead_of_panicking() {
        let mut bytes = Bytes::new();
        assert!(matches!(read_u8(&mut bytes), Err(CodecError::Malformed(_))));
    }
}
