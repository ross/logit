//! HEC `time` to and from `Event::timestamp` nanoseconds. The wire is epoch seconds with an
//! optional fraction, as a JSON number (or, from some clients, a numeric string). Decoding reads the
//! number's own text, not an `f64`, because an epoch-magnitude value with nanosecond decimals has
//! more significant digits than an `f64` holds.

use crate::datadog::time::seconds_f64_to_nanos;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Parses HEC `time` text into nanoseconds, keeping every digit of `DIGIT+ ("." DIGIT*)?` with an
/// optional leading `-`. An exponent form (`1.7e9`), or more than nine nonzero fractional digits,
/// falls back to an `f64` read. `None` when the text is not a number.
pub fn parse_hec_time(text: &str) -> Option<i64> {
    let (negative, magnitude) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    if let Some(nanos) = logit_core::parse_decimal_nanos(magnitude, NANOS_PER_SECOND as i64) {
        return Some(if negative { -nanos } else { nanos });
    }
    let seconds: f64 = text.parse().ok()?;
    seconds_f64_to_nanos(seconds)
}

/// Writes `nanos` as HEC `time`: whole seconds, then `.` and up to nine fractional digits with
/// trailing zeros trimmed. A whole second has no `.`. [`parse_hec_time`] reads every value this
/// writes back to the same `i64`.
pub fn write_hec_time(out: &mut Vec<u8>, nanos: i64) {
    if nanos < 0 {
        out.push(b'-');
    }
    let magnitude = nanos.unsigned_abs();
    out.extend_from_slice((magnitude / NANOS_PER_SECOND).to_string().as_bytes());
    let fraction = magnitude % NANOS_PER_SECOND;
    if fraction != 0 {
        let digits = format!("{fraction:09}");
        out.push(b'.');
        out.extend_from_slice(digits.trim_end_matches('0').as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn written(nanos: i64) -> String {
        let mut out = Vec::new();
        write_hec_time(&mut out, nanos);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn every_digit_survives_a_decimal_read() {
        assert_eq!(parse_hec_time("1700000000.123456789"), Some(1_700_000_000_123_456_789));
        assert_eq!(parse_hec_time("1700000000"), Some(1_700_000_000_000_000_000));
        assert_eq!(parse_hec_time("1700000000."), Some(1_700_000_000_000_000_000));
        assert_eq!(parse_hec_time("0"), Some(0));
        assert_eq!(parse_hec_time("1.5000000000"), Some(1_500_000_000), "zero padding past ns");
    }

    #[test]
    fn negative_exponent_and_overlong_forms() {
        assert_eq!(parse_hec_time("-1.5"), Some(-1_500_000_000));
        assert_eq!(parse_hec_time("-0"), Some(0));
        assert_eq!(parse_hec_time("1.7e9"), Some(1_700_000_000_000_000_000));
        assert_eq!(parse_hec_time("1.0000000001"), Some(1_000_000_000), "f64 fallback rounds");
        assert_eq!(parse_hec_time("abc"), None);
        assert_eq!(parse_hec_time(""), None);
    }

    #[test]
    fn writing_trims_trailing_zeros_and_whole_seconds_have_no_point() {
        assert_eq!(written(1_700_000_000_000_000_000), "1700000000");
        assert_eq!(written(1_700_000_000_120_000_000), "1700000000.12");
        assert_eq!(written(1_700_000_000_000_000_001), "1700000000.000000001");
        assert_eq!(written(0), "0");
        assert_eq!(written(-1_500_000_000), "-1.5");
        assert_eq!(written(-1), "-0.000000001");
    }

    #[test]
    fn written_times_read_back_unchanged() {
        for nanos in [
            0,
            1,
            -1,
            999_999_999,
            -999_999_999,
            1_700_000_000_123_456_789,
            -1_700_000_000_123_456_789,
            i64::MAX,
            i64::MIN + 1,
        ] {
            assert_eq!(parse_hec_time(&written(nanos)), Some(nanos), "{nanos}");
        }
    }
}
