//! Application trace context: a log record's reference to the trace/span it was emitted under.
//!
//! Distinct from `logit`'s own pipeline trace context (`logit_pipeline::fanout::TraceContext`,
//! the Lua `trace` global), which names the node visit that processed a batch. A [`TraceRef`]
//! names the application's trace, carried by OTLP's `LogRecord.trace_id`/`span_id`/`flags`. See
//! `docs/adr/log-record-trace-context.md`.
//!
//! The hex and id helpers live here so one module decides what a valid id is: `parse_*` applies
//! the same all-zero-is-invalid rule as [`TraceRef::from_bytes`].

use std::fmt::Write;

/// A reference to an application trace/span, carried on a [`crate::LogRecord`]. One struct rather
/// than two `Option`s makes a span without a trace unrepresentable (OTLP: "if `SpanId` is
/// present, `TraceId` SHOULD be also present").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceRef {
    pub trace_id: [u8; 16],
    /// `None` for a log correlated to a trace but no span in it.
    pub span_id: Option<[u8; 8]>,
    /// W3C trace flags (the low 8 bits of OTLP's `LogRecord.flags`); bit 0 is `SAMPLED`. `0`
    /// means unset, not "not sampled": the wire doesn't distinguish them.
    pub flags: u8,
}

impl TraceRef {
    /// Applies OTLP's log validity rule: `None` unless `trace` is 16 non-zero bytes; `span` kept
    /// only if it's 8 non-zero bytes. An invalid `trace` drops `flags` too.
    ///
    /// Degrades rather than errors: a log's trace is optional correlation metadata, unlike a
    /// span's required `trace_id`, which the OTLP decoder rejects outright.
    pub fn from_bytes(trace: &[u8], span: &[u8], flags: u8) -> Option<TraceRef> {
        let trace_id: [u8; 16] = trace.try_into().ok()?;
        if trace_id == [0; 16] {
            return None;
        }
        let span_id = <[u8; 8]>::try_from(span).ok().filter(|id| *id != [0; 8]);
        Some(TraceRef { trace_id, span_id, flags })
    }
}

/// Appends lowercase hex to `out`.
pub fn push_hex(out: &mut String, bytes: &[u8]) {
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
}

/// `bytes` as a new lowercase hex `String`.
pub fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    push_hex(&mut out, bytes);
    out
}

/// Parses 32 case-insensitive hex characters into a trace id; `None` if the wrong length,
/// non-hex, or all-zero (invalid per OTLP).
pub fn parse_trace_id(s: &str) -> Option<[u8; 16]> {
    parse_hex::<16>(s).filter(|id| *id != [0; 16])
}

/// [`parse_trace_id`] for a 16-character span id.
pub fn parse_span_id(s: &str) -> Option<[u8; 8]> {
    parse_hex::<8>(s).filter(|id| *id != [0; 8])
}

/// Parses a Datadog trace id as a log carries it (`dd.trace_id`): a decimal uint64, which fills the
/// low 64 bits with the high half zero, or 32 hex characters (a 128-bit id, what newer tracers
/// inject). `None` if neither, or all-zero.
///
/// Never 16 hex characters: a 16-digit string is decimal here, where [`parse_trace_id`]'s callers
/// read hex. The attribute's format decides the grammar; nothing guesses from the value.
pub fn parse_trace_id_datadog(s: &str) -> Option<[u8; 16]> {
    if s.len() == 32 {
        return parse_trace_id(s);
    }
    parse_decimal_u64(s).filter(|&low| low != 0).map(|low| trace_id_bytes(0, low))
}

/// Parses a Datadog span id as a log carries it (`dd.span_id`): a decimal uint64. `None` if not
/// one, or zero.
pub fn parse_span_id_datadog(s: &str) -> Option<[u8; 8]> {
    parse_decimal_u64(s).filter(|&id| id != 0).map(u64::to_be_bytes)
}

/// `_dd.p.tid`'s value as a 128-bit trace id's high 64 bits, by Go's `strconv.ParseUint(v, 16,
/// 64)` (what the Agent's `Get128BitTraceID` calls): 1 to 16 hex digits, either case, no prefix.
pub fn parse_trace_id_high(s: &str) -> Option<u64> {
    if s.is_empty() || s.len() > 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

/// A 128-bit id from its halves, big-endian.
pub fn trace_id_bytes(high: u64, low: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&high.to_be_bytes());
    id[8..].copy_from_slice(&low.to_be_bytes());
    id
}

/// A 128-bit id's `(high, low)` halves.
pub fn trace_id_halves(id: &[u8; 16]) -> (u64, u64) {
    let high = u64::from_be_bytes(id[..8].try_into().expect("8 bytes"));
    let low = u64::from_be_bytes(id[8..].try_into().expect("8 bytes"));
    (high, low)
}

/// 1 to 20 ASCII digits that fit a `u64`, by Go's `strconv.ParseUint(v, 10, 64)`: leading zeros
/// allowed, no sign, no whitespace. `u64::from_str` alone would accept a leading `+`.
fn parse_decimal_u64(s: &str) -> Option<u64> {
    if s.is_empty() || s.len() > 20 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Parses a W3C Trace Context `traceparent` header value
/// (<https://www.w3.org/TR/trace-context/>): `00-<32 hex trace-id>-<16 hex parent-id>-<2 hex
/// flags>`, 55 ASCII characters, case-insensitive. Returns `(trace_id, parent_id, flags)`.
///
/// `parent_id` is the caller's span, never the receiving service's
/// (`docs/adr/trace-context-span-lifting.md`). Only version `00` is accepted: the spec asks for
/// leniency toward unknown versions, but `ff` is forbidden and no other version exists. All-zero
/// ids are invalid. The flags are hex here because this header says so; the standalone
/// `trace.flags` attribute is decimal.
pub fn parse_traceparent(s: &str) -> Option<([u8; 16], [u8; 8], u8)> {
    let b = s.as_bytes();
    if b.len() != 55 || b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
        return None;
    }
    if &b[0..2] != b"00" {
        return None;
    }
    let trace_id = parse_trace_id(&s[3..35])?;
    let parent_id = parse_span_id(&s[36..52])?;
    let flags = parse_hex::<1>(&s[53..55])?[0];
    Some((trace_id, parent_id, flags))
}

/// Mints `N` random bytes for a trace or span id from a per-thread SplitMix64.
///
/// Not `tracing::span::Id`, which a `Registry` recycles after a span closes, and no `rand`
/// dependency. Not security-relevant: listeners are private by deployment shape
/// (`docs/OVERVIEW.md`), and a trace id isn't a capability. Callers: the pipeline `TraceContext`
/// and `trace_context`'s opt-in `mint_id` (`docs/adr/trace-context-span-lifting.md`).
pub fn random_id_bytes<const N: usize>() -> [u8; N] {
    use std::cell::Cell;
    thread_local! {
        // Seeded lazily per thread from `initial_seed`, never a constant: a constant seed gives
        // every fresh thread the same first id, merging unrelated traces.
        static STATE: Cell<u64> = Cell::new(initial_seed());
    }
    let mut out = [0u8; N];
    let mut filled = 0;
    while filled < N {
        let mut z = STATE.with(|c| {
            let z = c.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
            c.set(z);
            z
        });
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        for b in z.to_le_bytes() {
            if filled >= N {
                break;
            }
            out[filled] = b;
            filled += 1;
        }
    }
    out
}

/// This thread's starting seed. `RandomState::new()` is OS-keyed per process and varies per call;
/// hashing the `ThreadId` also separates threads that call at the same instant. It needs only
/// not to repeat, not to resist prediction.
fn initial_seed() -> u64 {
    use std::hash::BuildHasher;
    std::collections::hash_map::RandomState::new().hash_one(std::thread::current().id())
}

fn parse_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 || !s.is_ascii() {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = [0u8; N];
    for i in 0..N {
        let hi = (bytes[i * 2] as char).to_digit(16)?;
        let lo = (bytes[i * 2 + 1] as char).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_bytes_accepts_a_valid_trace_and_span() {
        let r = TraceRef::from_bytes(&[1; 16], &[2; 8], 1).unwrap();
        assert_eq!(r.trace_id, [1; 16]);
        assert_eq!(r.span_id, Some([2; 8]));
        assert_eq!(r.flags, 1);
    }

    #[test]
    fn from_bytes_accepts_a_trace_with_no_span() {
        let r = TraceRef::from_bytes(&[1; 16], &[], 0).unwrap();
        assert_eq!(r.trace_id, [1; 16]);
        assert_eq!(r.span_id, None);
    }

    #[test]
    fn from_bytes_rejects_wrong_length_trace() {
        assert!(TraceRef::from_bytes(&[1; 15], &[], 0).is_none());
        assert!(TraceRef::from_bytes(&[1; 17], &[], 0).is_none());
    }

    #[test]
    fn from_bytes_rejects_all_zero_trace_and_drops_flags_with_it() {
        assert!(TraceRef::from_bytes(&[0; 16], &[2; 8], 1).is_none());
    }

    #[test]
    fn from_bytes_drops_a_wrong_length_or_all_zero_span_but_keeps_the_trace() {
        let r = TraceRef::from_bytes(&[1; 16], &[2; 7], 0).unwrap();
        assert_eq!(r.span_id, None, "wrong-length span id should be dropped, not error");
        let r = TraceRef::from_bytes(&[1; 16], &[0; 8], 0).unwrap();
        assert_eq!(r.span_id, None, "all-zero span id should be dropped, not error");
    }

    #[test]
    fn hex_round_trips_through_to_hex_and_parse() {
        let id = [0xab; 16];
        let hex = to_hex(&id);
        assert_eq!(hex, "ab".repeat(16));
        assert_eq!(parse_trace_id(&hex), Some(id));
    }

    #[test]
    fn parse_trace_id_is_case_insensitive() {
        assert_eq!(parse_trace_id(&"AB".repeat(16)), parse_trace_id(&"ab".repeat(16)));
    }

    #[test]
    fn parse_trace_id_rejects_wrong_length_non_hex_and_all_zero() {
        assert_eq!(parse_trace_id(&"ab".repeat(15)), None, "too short");
        assert_eq!(parse_trace_id(&"ab".repeat(17)), None, "too long");
        assert_eq!(parse_trace_id(&"zz".repeat(16)), None, "not hex");
        assert_eq!(parse_trace_id(&"00".repeat(16)), None, "all zero");
    }

    #[test]
    fn parse_span_id_rejects_wrong_length_and_all_zero() {
        assert_eq!(parse_span_id(&"ab".repeat(7)), None);
        assert_eq!(parse_span_id(&"ab".repeat(9)), None);
        assert_eq!(parse_span_id(&"00".repeat(8)), None);
        assert_eq!(parse_span_id(&"cd".repeat(8)), Some([0xcd; 8]));
    }

    const W3C_EXAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn traceparent_parses_the_spec_example() {
        let (trace, parent, flags) = parse_traceparent(W3C_EXAMPLE).unwrap();
        assert_eq!(to_hex(&trace), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(to_hex(&parent), "00f067aa0ba902b7");
        assert_eq!(flags, 1);
    }

    #[test]
    fn traceparent_flags_are_hex_and_case_is_ignored() {
        let upper = W3C_EXAMPLE.to_ascii_uppercase();
        assert_eq!(parse_traceparent(&upper), parse_traceparent(W3C_EXAMPLE));
        let with_hex_flags = format!("{}-ff", &W3C_EXAMPLE[..52]);
        assert_eq!(parse_traceparent(&with_hex_flags).unwrap().2, 0xff);
        let with_hex_flags = format!("{}-08", &W3C_EXAMPLE[..52]);
        assert_eq!(
            parse_traceparent(&with_hex_flags).unwrap().2,
            8,
            "08 hex is 8, not a decimal 8"
        );
    }

    #[test]
    fn traceparent_rejects_wrong_version_length_zero_ids_and_non_hex() {
        for bad in [
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4bf92f3577b34da6a3ce929d0e0e47zz-00f067aa0ba902b7-01",
            "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "",
        ] {
            assert_eq!(parse_traceparent(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn random_ids_are_not_constant_across_calls_or_threads() {
        let a: [u8; 16] = random_id_bytes();
        let b: [u8; 16] = random_id_bytes();
        assert_ne!(a, b);
        assert_ne!(a, [0; 16]);
        // A fresh thread's first call differs, which a constant seed would break.
        let there = std::thread::spawn(random_id_bytes::<16>).join().expect("no panic");
        assert_ne!(a, there);
        let short: [u8; 8] = random_id_bytes();
        assert_ne!(short, [0; 8]);
    }

    // Datadog's documented forms: a decimal uint64 `dd.trace_id` (older tracers), a 128-bit hex
    // one (newer tracers), and `_dd.p.tid` carrying that hex's high half.
    const DD_DECIMAL: &str = "1234567890123456789";
    const DD_HEX_128: &str = "64de8e2b0000000012345678abcdef12";
    const DD_TID: &str = "64de8e2b00000000";

    #[test]
    fn datadog_trace_id_parses_a_decimal_uint64_into_the_low_half() {
        let id = parse_trace_id_datadog(DD_DECIMAL).unwrap();
        assert_eq!(trace_id_halves(&id), (0, 1_234_567_890_123_456_789));
        assert_eq!(to_hex(&id), "0000000000000000112210f47de98115");
        let max = parse_trace_id_datadog(&u64::MAX.to_string()).unwrap();
        assert_eq!(trace_id_halves(&max), (0, u64::MAX), "20 digits, the u64 ceiling");
        let padded = parse_trace_id_datadog("0000000000000000042").unwrap();
        assert_eq!(trace_id_halves(&padded), (0, 42), "leading zeros, as Go's ParseUint allows");
    }

    #[test]
    fn datadog_trace_id_parses_128_bit_hex() {
        let id = parse_trace_id_datadog(DD_HEX_128).unwrap();
        assert_eq!(trace_id_halves(&id), (0x64de_8e2b_0000_0000, 0x1234_5678_abcd_ef12));
        assert_eq!(Some(id), parse_trace_id(DD_HEX_128));
        assert_eq!(
            trace_id_bytes(parse_trace_id_high(DD_TID).unwrap(), 0x1234_5678_abcd_ef12),
            id,
            "_dd.p.tid supplies the same high half"
        );
    }

    #[test]
    fn datadog_trace_id_reads_16_digits_as_decimal_never_hex() {
        let id = parse_trace_id_datadog("1234567890123456").unwrap();
        assert_eq!(trace_id_halves(&id), (0, 1_234_567_890_123_456));
        assert_eq!(parse_trace_id_datadog(&"ab".repeat(8)), None, "16 hex is not a Datadog form");
    }

    #[test]
    fn datadog_trace_id_rejects_overflow_signs_zero_and_junk() {
        let zero_hex = "00".repeat(16);
        let non_hex = "zz".repeat(16);
        for bad in [
            "18446744073709551616",
            "123456789012345678901",
            "+1",
            "-1",
            " 1",
            "1 ",
            "0",
            "00000000000000000000",
            "",
            "0x1234",
            "12a",
            &zero_hex,
            &non_hex,
        ] {
            assert_eq!(parse_trace_id_datadog(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn datadog_span_id_is_decimal_only() {
        assert_eq!(
            parse_span_id_datadog(DD_DECIMAL),
            Some(1_234_567_890_123_456_789_u64.to_be_bytes())
        );
        assert_eq!(
            parse_span_id_datadog("1234567890123456"),
            Some(1_234_567_890_123_456_u64.to_be_bytes()),
            "16 digits are decimal"
        );
        for bad in ["0", "", "+5", "18446744073709551616", "abcdef0123456789", "ab"] {
            assert_eq!(parse_span_id_datadog(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn trace_id_high_follows_go_parse_uint_base_16() {
        assert_eq!(parse_trace_id_high(DD_TID), Some(0x64de_8e2b_0000_0000));
        assert_eq!(parse_trace_id_high("ABC"), Some(0xabc));
        assert_eq!(parse_trace_id_high("0"), Some(0));
        assert_eq!(parse_trace_id_high(""), None);
        assert_eq!(parse_trace_id_high("0x12"), None);
        assert_eq!(parse_trace_id_high("+12"), None);
        assert_eq!(parse_trace_id_high("11112222333344445"), None, "17 digits");
    }

    #[test]
    fn trace_id_halves_inverts_trace_id_bytes() {
        let id = trace_id_bytes(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        assert_eq!(id, core::array::from_fn(|i| i as u8 + 1));
        assert_eq!(trace_id_halves(&id), (0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10));
    }

    #[test]
    fn push_hex_matches_to_hex() {
        let mut out = String::from("prefix ");
        push_hex(&mut out, &[0x0a, 0xff]);
        assert_eq!(out, "prefix 0aff");
    }
}
