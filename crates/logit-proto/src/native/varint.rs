//! LEB128 unsigned varints, plus zigzag encoding for signed integers reusing the same varint.
//! The building block every count, length, and dictionary index in `native`'s payload is written
//! with.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::CodecError;

/// Writes `v` as an unsigned LEB128 varint: 7 payload bits per byte, high bit set on every byte
/// but the last. Streams directly into `out` -- no intermediate buffer, since each byte's value
/// only depends on bits already shifted out.
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

/// The inverse of [`write_uvarint`]. Bounded to 10 bytes (the maximum a 64-bit value ever needs)
/// so a corrupt stream with the continuation bit always set can't spin forever.
pub fn read_uvarint(bytes: &mut Bytes) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    for i in 0..10 {
        if bytes.is_empty() {
            return Err(CodecError::Malformed("truncated varint".to_string()));
        }
        let byte = bytes.get_u8();
        result |= ((byte & 0x7f) as u64) << (i * 7);
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(CodecError::Malformed("varint longer than 10 bytes".to_string()))
}

/// Zigzag-encodes a signed value into the unsigned space `write_uvarint` carries -- small
/// magnitudes (positive or negative) stay small on the wire, unlike two's-complement, which would
/// make every negative `i64` encode as a full 10-byte varint.
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

/// Reads one byte, without `bytes::Buf::get_u8`'s panic-on-empty behavior -- every call site in
/// this codec is decoding untrusted wire input, so "ran out of bytes" must be a `CodecError`, not
/// a panic.
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
        // The whole point of zigzag over raw two's-complement: -1 should cost one byte, not ten.
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
