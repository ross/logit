//! Random ids in the forms other systems expect: W3C trace and span ids, UUID v4 and v7 (RFC
//! 9562), and ULID (<https://github.com/ulid/spec>), for the Lua `random` global and
//! `splunk_hec_out`'s channel. See `docs/adr/lua-random-ids.md`.
//!
//! Every random byte comes from [`crate::trace::random_id_bytes`], a per-thread SplitMix64: an id
//! here needs only not to repeat, not to resist prediction. No `uuid` or `rand` dependency.
//!
//! v7 and ULID encode a time, and this module never reads a clock: the caller passes unix
//! milliseconds, which must fit the formats' 48-bit field.

use crate::trace::{push_hex, random_id_bytes};

/// The largest unix-millisecond value a v7 UUID or a ULID can carry, `2^48 - 1`.
pub const MAX_UNIX_MS: u64 = (1 << 48) - 1;

/// Crockford's base32 alphabet, which the ULID spec uses: no `I`, `L`, `O`, or `U`.
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// A millisecond time past [`MAX_UNIX_MS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unix milliseconds {0} exceed the 48-bit field (max {MAX_UNIX_MS})")]
pub struct TimeOutOfRange(pub u64);

/// A 16-byte trace id, never all-zero (all-zero is invalid; see [`crate::TraceRef::from_bytes`]).
pub fn trace_id() -> [u8; 16] {
    nonzero_from(random_id_bytes::<16>)
}

/// An 8-byte span id, never all-zero, as [`trace_id`].
pub fn span_id() -> [u8; 8] {
    nonzero_from(random_id_bytes::<8>)
}

/// Draws from `next` until it returns a value that isn't all zero bytes.
fn nonzero_from<const N: usize>(mut next: impl FnMut() -> [u8; N]) -> [u8; N] {
    loop {
        let id = next();
        if id != [0; N] {
            return id;
        }
    }
}

/// Fills `out` with random bytes.
pub fn fill(out: &mut [u8]) {
    for chunk in out.chunks_mut(8) {
        let bytes = random_id_bytes::<8>();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
}

/// A random version-4 UUID: 122 random bits, version nibble `4`, variant bits `10`.
pub fn uuid_v4() -> [u8; 16] {
    let mut b = random_id_bytes::<16>();
    set_version(&mut b, 4);
    b
}

/// A version-7 UUID: `unix_ms` as 48 big-endian bits, version nibble `7`, variant bits `10`, and
/// 74 random bits. Two ids minted in one millisecond aren't ordered between themselves.
pub fn uuid_v7(unix_ms: u64) -> Result<[u8; 16], TimeOutOfRange> {
    let mut b = random_id_bytes::<16>();
    put_ms(&mut b, unix_ms)?;
    set_version(&mut b, 7);
    Ok(b)
}

/// A ULID: `unix_ms` as 48 big-endian bits, then 80 random bits. Not the spec's monotonic
/// variant: two ULIDs minted in one millisecond aren't ordered between themselves.
pub fn ulid(unix_ms: u64) -> Result<[u8; 16], TimeOutOfRange> {
    let mut b = random_id_bytes::<16>();
    put_ms(&mut b, unix_ms)?;
    Ok(b)
}

fn put_ms(b: &mut [u8; 16], unix_ms: u64) -> Result<(), TimeOutOfRange> {
    if unix_ms > MAX_UNIX_MS {
        return Err(TimeOutOfRange(unix_ms));
    }
    b[..6].copy_from_slice(&unix_ms.to_be_bytes()[2..]);
    Ok(())
}

/// RFC 9562's version nibble (byte 6's high half) and `10` variant (byte 8's top two bits).
fn set_version(b: &mut [u8; 16], version: u8) {
    b[6] = (b[6] & 0x0f) | (version << 4);
    b[8] = (b[8] & 0x3f) | 0x80;
}

/// A UUID in its `8-4-4-4-12` hyphenated, lowercase hex form.
pub fn format_uuid(b: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (i, range) in [0..4, 4..6, 6..8, 8..10, 10..16].into_iter().enumerate() {
        if i > 0 {
            out.push('-');
        }
        push_hex(&mut out, &b[range]);
    }
    out
}

/// A ULID's 26-character Crockford base32 form, uppercase. 26 characters hold 130 bits, so the
/// leading one encodes only the top 3 bits and is never above `7`.
pub fn format_ulid(b: &[u8; 16]) -> String {
    let v = u128::from_be_bytes(*b);
    (0..26).map(|i| CROCKFORD[((v >> (125 - 5 * i)) & 0x1f) as usize] as char).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ms field of a v7 UUID or a ULID, its first 48 bits.
    fn ms_of(b: &[u8; 16]) -> u64 {
        let mut be = [0u8; 8];
        be[2..].copy_from_slice(&b[..6]);
        u64::from_be_bytes(be)
    }

    /// Decodes a ULID string's first 10 characters, the ms field.
    fn ulid_ms(s: &str) -> u64 {
        s.bytes().take(10).fold(0, |acc, c| {
            let digit = CROCKFORD.iter().position(|&a| a == c).expect("a Crockford digit");
            (acc << 5) | digit as u64
        })
    }

    #[test]
    fn nonzero_from_skips_all_zero_draws() {
        let mut draws = [[0u8; 8], [0u8; 8], [0, 0, 0, 0, 0, 0, 0, 1]].into_iter();
        assert_eq!(nonzero_from(|| draws.next().expect("a third draw")), [0, 0, 0, 0, 0, 0, 0, 1]);
        assert!(draws.next().is_none(), "stops at the first nonzero draw");
    }

    #[test]
    fn trace_and_span_ids_are_nonzero_and_vary() {
        assert_ne!(trace_id(), [0; 16]);
        assert_ne!(span_id(), [0; 8]);
        assert_ne!(trace_id(), trace_id());
        assert_ne!(span_id(), span_id());
    }

    #[test]
    fn fill_covers_a_length_not_a_multiple_of_eight() {
        let mut out = [0u8; 256];
        fill(&mut out);
        // 13 zero bytes in a row out of SplitMix64 would be a 2^-104 event.
        assert!(out[243..].iter().any(|&b| b != 0), "the tail chunk is written");
        let mut odd = [0u8; 3];
        fill(&mut odd);
    }

    #[test]
    fn uuid_v4_carries_version_4_and_the_rfc_variant() {
        for _ in 0..64 {
            let b = uuid_v4();
            assert_eq!(b[6] >> 4, 4);
            assert_eq!(b[8] >> 6, 0b10);
        }
    }

    #[test]
    fn uuid_v7_carries_version_7_the_variant_and_the_ms_field() {
        for ms in [0, 1, 1_469_918_176_385, MAX_UNIX_MS] {
            let b = uuid_v7(ms).expect("in range");
            assert_eq!(b[6] >> 4, 7);
            assert_eq!(b[8] >> 6, 0b10);
            assert_eq!(ms_of(&b), ms);
        }
    }

    #[test]
    fn a_time_past_48_bits_is_rejected() {
        assert_eq!(uuid_v7(MAX_UNIX_MS + 1), Err(TimeOutOfRange(MAX_UNIX_MS + 1)));
        assert_eq!(ulid(u64::MAX), Err(TimeOutOfRange(u64::MAX)));
    }

    #[test]
    fn ulid_ms_round_trips_through_bytes_and_text() {
        for ms in [0, 1, 1_469_918_176_385, MAX_UNIX_MS] {
            let b = ulid(ms).expect("in range");
            assert_eq!(ms_of(&b), ms);
            assert_eq!(ulid_ms(&format_ulid(&b)), ms);
        }
    }

    /// The ULID spec's README example: `01ARYZ6S41` is the time part of a ULID minted at
    /// 1469918176385 ms.
    #[test]
    fn ulid_matches_the_spec_time_vector() {
        let s = format_ulid(&ulid(1_469_918_176_385).expect("in range"));
        assert_eq!(&s[..10], "01ARYZ6S41");
    }

    #[test]
    fn format_ulid_is_26_crockford_chars_led_by_at_most_7() {
        assert_eq!(format_ulid(&[0xff; 16]), format!("7{}", "Z".repeat(25)));
        assert_eq!(format_ulid(&[0; 16]), "0".repeat(26));
        let s = format_ulid(&ulid(0).expect("in range"));
        assert_eq!(s.len(), 26);
        assert!(s.bytes().all(|c| CROCKFORD.contains(&c)));
    }

    #[test]
    fn format_uuid_is_hyphenated_lowercase_hex() {
        let b: [u8; 16] = core::array::from_fn(|i| (i as u8) * 0x11);
        assert_eq!(format_uuid(&b), "00112233-4455-6677-8899-aabbccddeeff");
    }
}
