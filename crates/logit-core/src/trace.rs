//! Application trace context: a log record's reference to the trace/span it was emitted under.
//!
//! Distinct from `logit`'s own *pipeline* trace context (`logit_pipeline::fanout::TraceContext`,
//! exposed to Lua as the `trace` global, `crates/logit-script/src/trace.rs`) -- that names which
//! `logit` node-visit processed a batch; a [`TraceRef`] on a [`crate::LogRecord`] names the
//! *application's* trace, carried on the wire by OTLP's `LogRecord.trace_id`/`span_id`/`flags`
//! fields. See `docs/adr/log-record-trace-context.md`.
//!
//! Hex helpers live here too (`push_hex`/`to_hex`/`parse_trace_id`/`parse_span_id`) -- trace-id
//! semantics, not generic hex encoding: `parse_*` enforces the same "all-zero is invalid" rule
//! [`TraceRef::from_bytes`] does, so there is exactly one place that decides what a valid id is,
//! not two independently-maintained ones.

use std::fmt::Write;

/// A reference to an application trace/span, carried on a [`crate::LogRecord`]. Bundled rather
/// than two flat `Option`s on `LogRecord` itself: OTLP's own contract is "if `SpanId` is present,
/// `TraceId` SHOULD be also present" -- a `TraceRef` makes "span without trace" unrepresentable
/// instead of merely undocumented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceRef {
    pub trace_id: [u8; 16],
    /// `None` when a log is correlated to a trace but not a specific span within it -- OTLP
    /// allows `TraceId` alone.
    pub span_id: Option<[u8; 8]>,
    /// W3C trace flags (low 8 bits of OTLP's `LogRecord.flags`); bit 0 is the `SAMPLED` flag.
    /// `0` means unset, not "not sampled" -- OTLP doesn't distinguish the two over the wire.
    pub flags: u8,
}

impl TraceRef {
    /// The OTLP validity rule for a log record (`logs.proto`: "receivers SHOULD assume the log
    /// record is not associated with a trace" if `trace_id` is absent or invalid): `trace`
    /// valid only if exactly 16 non-zero bytes, `span` kept only if `trace` is valid *and* `span`
    /// is exactly 8 non-zero bytes. An invalid `trace` drops `flags` too -- there is no "flags
    /// with no trace" case to keep. Infallible by design: unlike a `Span`'s `trace_id` (required,
    /// rejected outright when malformed -- `crates/logit-proto/src/otlp/traces.rs`'s `mod ids`),
    /// a log's is optional correlation metadata a decoder degrades gracefully without, per spec.
    pub fn from_bytes(trace: &[u8], span: &[u8], flags: u8) -> Option<TraceRef> {
        let trace_id: [u8; 16] = trace.try_into().ok()?;
        if trace_id == [0; 16] {
            return None;
        }
        let span_id = <[u8; 8]>::try_from(span).ok().filter(|id| *id != [0; 8]);
        Some(TraceRef { trace_id, span_id, flags })
    }
}

/// Appends lowercase hex to `out` -- the shared rendering `crates/logit-script/src/trace.rs` (the
/// `trace` Lua global) and `crates/logit-outputs/src/stdio.rs` (span/log rendering) both need.
pub fn push_hex(out: &mut String, bytes: &[u8]) {
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
}

/// A fresh `String` of lowercase hex -- the shape the Lua `trace` global and `event.log.trace_id`
/// both want (a value to hand back, not a buffer to append to).
pub fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    push_hex(&mut out, bytes);
    out
}

/// Parses a 32-character case-insensitive hex string into a trace id, rejecting anything the
/// wrong length, non-hex, or all-zero (OTLP: an all-zero id is invalid, same rule
/// [`TraceRef::from_bytes`] applies to bytes off the wire).
pub fn parse_trace_id(s: &str) -> Option<[u8; 16]> {
    parse_hex::<16>(s).filter(|id| *id != [0; 16])
}

/// Parses a 16-character case-insensitive hex string into a span id, same rules as
/// [`parse_trace_id`] at half the length.
pub fn parse_span_id(s: &str) -> Option<[u8; 8]> {
    parse_hex::<8>(s).filter(|id| *id != [0; 8])
}

/// Parses a W3C Trace Context `traceparent` header value
/// (<https://www.w3.org/TR/trace-context/>): `00-<32 hex trace-id>-<16 hex parent-id>-<2 hex
/// flags>`, exactly 55 ASCII characters, case-insensitive hex. Returns `(trace_id, parent_id,
/// flags)` -- named `parent_id` deliberately: the span id in a `traceparent` is the *caller's*
/// span, never the receiving service's own (`docs/adr/trace-context-span-lifting.md`). Only
/// version `00` is accepted: the spec says a receiver "MUST" treat an unknown version leniently
/// when the rest parses, but `ff` is forbidden outright and no other version exists, so being
/// strict here rejects nothing real and keeps the parser trivially auditable. The ids follow
/// the same non-zero rule as [`parse_trace_id`]/[`parse_span_id`] -- all-zero is invalid per the
/// spec too. The flags octet is hex *because this header defines it so*; the standalone
/// `trace.flags` attribute stays decimal (`crates/logit-transforms/src/trace_context.rs`), and
/// the two never mix.
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

/// Mints `N` random bytes for a fresh trace or span id -- a per-thread SplitMix64, good enough
/// to mint distinct ids without a new `rand` dependency or `tracing::span::Id` (a `Registry`
/// recycles those after a span closes, so they're not a safe source of identity here -- two
/// spans minutes apart could share one). Not security-relevant: `logit`'s listeners are private
/// by deployment shape (`docs/OVERVIEW.md`), the same premise `docs/known-gaps.md`'s interner
/// entry leans on, and a trace id is not a capability. Two callers: `logit`'s own pipeline
/// `TraceContext` (`crates/logit-pipeline/src/fanout.rs`, where this originally lived) and
/// `trace_context`'s opt-in `mint_id` (`docs/adr/trace-context-span-lifting.md`). Lives here,
/// next to `parse_trace_id`/`parse_span_id`, so exactly one module decides what an id is.
pub fn random_id_bytes<const N: usize>() -> [u8; N] {
    use std::cell::Cell;
    thread_local! {
        // Seeded once, lazily, on this thread's first call -- not a compile-time constant.
        // Caught in review: a `const` seed here is identical on every thread and every process
        // run, so the *first* call on any two fresh threads returned the same bytes,
        // deterministically merging unrelated traces. `initial_seed` below is real per-run
        // (OS-random) and per-thread entropy instead.
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

/// This thread's starting seed: real entropy, not a shared constant. `RandomState::new()` is
/// keyed from OS randomness at process start and refreshed by an internal per-call counter, so it
/// already differs call to call within one process; mixing in this thread's `ThreadId` makes two
/// threads calling this at nearly the same instant diverge too, rather than relying on
/// `RandomState`'s own per-call drift alone. Not security-relevant, same as `random_id_bytes`'s
/// own doc comment above -- this only needs to not repeat, not resist prediction.
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
        // A fresh thread's *first* call must differ from this thread's -- the bug a constant
        // seed would reintroduce (see `random_id_bytes`'s own comment).
        let there = std::thread::spawn(random_id_bytes::<16>).join().expect("no panic");
        assert_ne!(a, there);
        let short: [u8; 8] = random_id_bytes();
        assert_ne!(short, [0; 8]);
    }

    #[test]
    fn push_hex_matches_to_hex() {
        let mut out = String::from("prefix ");
        push_hex(&mut out, &[0x0a, 0xff]);
        assert_eq!(out, "prefix 0aff");
    }
}
