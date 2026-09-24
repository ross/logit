//! Hand-rolled RFC 3339 UTC timestamp formatting and parsing, plus exact decimal-to-nanos.
//!
//! `stdio_out` formats Unix-nanosecond timestamps for humans; `syslog_in` (RFC 5424 TIMESTAMP)
//! and `trace_context`'s span lifting (`span.*_rfc3339`, `span.*_s`,
//! `docs/design/data-model.md`'s "Well-known attribute names") parse them. Only UTC and
//! civil-date <-> days-since-epoch are needed, a few dozen lines of Howard Hinnant's algorithms;
//! a `time`/`chrono` dependency would be an ADR-scale decision, as with `logit-config`'s
//! hand-rolled duration codec.
//!
//! TODO: replace with a real crate once the crate list is finalized.

/// Formats Unix nanoseconds as RFC 3339 in UTC (`Z`, never an offset) with nine fractional
/// digits: `2026-08-30T18:20:41.512847391Z`. Never panics: every `i64`, including the extremes,
/// maps to a calendar date.
pub fn format_rfc3339_utc(nanos: i64) -> String {
    // Euclidean, not truncating: before the epoch, `/`/`%` would give a negative remainder (-1ns
    // as 0s and -1ns); flooring keeps `nanos_of_sec`/`secs_of_day` in `[0, N)`.
    let secs = nanos.div_euclid(1_000_000_000);
    let nanos_of_sec = nanos.rem_euclid(1_000_000_000) as u32;

    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400) as u32;

    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{nanos_of_sec:09}Z")
}

/// Days since 1970-01-01 to a proleptic-Gregorian `(year, month, day)`: Howard Hinnant's
/// `civil_from_days` (<http://howardhinnant.github.io/date_algorithms.html#civil_from_days>),
/// with `div_euclid`/`rem_euclid` in place of the reference's negative-`z` branch (both floor).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Shift the epoch to 0000-03-01 so February, with its leap day, ends the algorithm's year.
    let z = days + 719_468;
    let era = z.div_euclid(146_097); // 146,097 days = one 400-year Gregorian cycle
    let doe = z.rem_euclid(146_097); // day-of-era, [0, 146096]

    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // year-of-era, [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day-of-year, [0, 365]
    let mp = (5 * doy + 2) / 153; // "March-indexed" month, [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // back to [1, 12], Jan/Feb last
    let year = if month <= 2 { y + 1 } else { y }; // undo the March-1st epoch shift

    (year, month, day)
}

/// Why an RFC 3339 timestamp couldn't become an instant. The cases call for different handling:
/// [`Malformed`](TimestampError::Malformed) is bad input (for `syslog_in`, skipped like a bad
/// PRI); [`OutOfRange`](TimestampError::OutOfRange) is well-formed but outside what an `i64` of
/// nanoseconds can represent, so keep whatever carried it and drop only that value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampError {
    Malformed,
    OutOfRange,
}

/// Parses an RFC 3339 timestamp (RFC 5424's TIMESTAMP, and the `span.*_rfc3339` attributes) into
/// Unix nanoseconds.
///
/// Accepts an uppercase `T`, an uppercase `Z` or `+HH:MM`/`-HH:MM` offset, and 1-9 fractional
/// digits (RFC 5424 allows six; nine is what nanoseconds hold). Rejects a nonexistent date
/// (`2023-02-29`), an out-of-range offset, and a leap second (`:60`): RFC 5424 forbids all three,
/// and normalizing them would attach a wrong instant. A well-formed timestamp outside roughly
/// 1677-09-21..2262-04-11 is [`TimestampError::OutOfRange`], not `Malformed`.
pub fn parse_rfc3339_to_nanos(s: &str) -> Result<i64, TimestampError> {
    let (days, seconds_of_day, offset_seconds, nanos_frac) =
        parse_rfc3339_components(s).ok_or(TimestampError::Malformed)?;
    let total_seconds = days * 86_400 + seconds_of_day - offset_seconds;
    total_seconds
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(nanos_frac))
        .ok_or(TimestampError::OutOfRange)
}

/// The well-formedness half of [`parse_rfc3339_to_nanos`]: `(days since epoch, seconds of day,
/// offset seconds, fractional nanos)`. Split out so the caller's overflow check can tell
/// out-of-range from malformed.
fn parse_rfc3339_components(s: &str) -> Option<(i64, i64, i64, i64)> {
    let b = s.as_bytes();
    if b.len() < 20 {
        return None; // "YYYY-MM-DDTHH:MM:SSZ" is the shortest legal form.
    }
    let digits = |start: usize, n: usize| -> Option<i64> {
        let slice = s.get(start..start + n)?;
        slice.bytes().all(|c| c.is_ascii_digit()).then(|| slice.parse().ok())?
    };

    let year = digits(0, 4)?;
    if b[4] != b'-' {
        return None;
    }
    let month = digits(5, 2)?;
    if b[7] != b'-' {
        return None;
    }
    let day = digits(8, 2)?;
    // RFC 5424 (unlike RFC 3339 itself) requires uppercase `T` and `Z`.
    if b[10] != b'T' {
        return None;
    }
    let hour = digits(11, 2)?;
    if b[13] != b':' {
        return None;
    }
    let minute = digits(14, 2)?;
    if b[16] != b':' {
        return None;
    }
    let second = digits(17, 2)?;

    let mut idx = 19;
    let mut nanos_frac: i64 = 0;
    if b.get(idx) == Some(&b'.') {
        idx += 1;
        let frac_start = idx;
        while b.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        let frac_len = idx - frac_start;
        // 1-9 digits; RFC 5424's `TIME-SECFRAC` (`"." 1*6DIGIT`) is a subset.
        if !(1..=9).contains(&frac_len) {
            return None;
        }
        let frac = &s[frac_start..idx];
        let mut padded = [b'0'; 9];
        for (dst, src) in padded.iter_mut().zip(frac.bytes()) {
            *dst = src;
        }
        nanos_frac = std::str::from_utf8(&padded).ok()?.parse().ok()?;
    }

    let offset_seconds: i64 = match b.get(idx) {
        Some(b'Z') => {
            idx += 1;
            0
        }
        Some(sign @ (b'+' | b'-')) => {
            let sign: i64 = if *sign == b'-' { -1 } else { 1 };
            let oh = digits(idx + 1, 2)?;
            if b.get(idx + 3) != Some(&b':') {
                return None;
            }
            let om = digits(idx + 4, 2)?;
            if !(0..=23).contains(&oh) || !(0..=59).contains(&om) {
                return None;
            }
            idx += 6;
            sign * (oh * 3600 + om * 60)
        }
        _ => return None,
    };
    if idx != b.len() {
        return None; // trailing garbage
    }
    // RFC 5424 forbids the leap second RFC 3339 allows, so `second` stops at 59.
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }

    let days = days_from_civil(year, month, day);
    let seconds_of_day = hour * 3600 + minute * 60 + second;
    Some((days, seconds_of_day, offset_seconds, nanos_frac))
}

fn is_leap_year(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Days in `month` of `y`; `0` for a month outside `1..=12`.
fn days_in_month(y: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Howard Hinnant's `days_from_civil`, the inverse of [`civil_from_days`]: proleptic Gregorian,
/// correct for any year (<http://howardhinnant.github.io/date_algorithms.html>).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// Parses an unsigned decimal in one unit into an exact integer count of a finer unit, with no
/// `f64`: `parse_decimal_nanos("1725400000.123456789", 1_000_000_000)` is
/// `1_725_400_000_123_456_789`. An epoch-magnitude value like nginx's `$msec` scaled to
/// nanoseconds has more significant digits than an `f64` holds, so a float route would round.
///
/// `scale` is target units per source unit (`1_000_000_000` for seconds, `1_000` for micros).
/// Nonzero digits past `log10(scale)` places return `None` rather than truncating; trailing zeros
/// there are accepted. Grammar: `DIGIT+ ("." DIGIT*)?`. Any other shape, or `i64` overflow,
/// returns `None`.
pub fn parse_decimal_nanos(s: &str, scale: i64) -> Option<i64> {
    let (whole, frac) = match s.split_once('.') {
        Some((w, f)) => (w, f),
        None => (s, ""),
    };
    if whole.is_empty() || !whole.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if !frac.bytes().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let whole: i64 = whole.parse().ok()?;
    let mut out = whole.checked_mul(scale)?;
    // Each fractional digit is worth `scale / 10^position` target units; stays integral.
    let mut place = scale;
    for c in frac.bytes() {
        let digit = i64::from(c - b'0');
        if place == 1 {
            // Below one target unit: zero is padding, anything else is unrepresentable.
            if digit != 0 {
                return None;
            }
            continue;
        }
        place /= 10;
        out = out.checked_add(digit.checked_mul(place)?)?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_utc_with_no_fraction_parses() {
        // `a_known_instant_formats_correctly`'s instant, minus its fraction.
        assert_eq!(parse_rfc3339_to_nanos("2026-08-30T18:20:41Z"), Ok(1_788_114_041_000_000_000));
    }

    #[test]
    fn rfc3339_round_trips_through_format() {
        let nanos: i64 = 1_788_114_041_512_847_391;
        assert_eq!(parse_rfc3339_to_nanos(&format_rfc3339_utc(nanos)), Ok(nanos));
    }

    #[test]
    fn rfc3339_fraction_is_padded_to_nanos_and_capped_at_nine_digits() {
        assert_eq!(parse_rfc3339_to_nanos("1970-01-01T00:00:00.5Z"), Ok(500_000_000));
        assert_eq!(parse_rfc3339_to_nanos("1970-01-01T00:00:00.123456789Z"), Ok(123_456_789));
        assert_eq!(
            parse_rfc3339_to_nanos("1970-01-01T00:00:00.1234567890Z"),
            Err(TimestampError::Malformed),
            "ten fractional digits can't be represented"
        );
        assert_eq!(parse_rfc3339_to_nanos("1970-01-01T00:00:00.Z"), Err(TimestampError::Malformed));
    }

    #[test]
    fn rfc3339_offset_is_applied() {
        assert_eq!(parse_rfc3339_to_nanos("1970-01-01T01:00:00+01:00"), Ok(0));
        assert_eq!(parse_rfc3339_to_nanos("1969-12-31T23:00:00-01:00"), Ok(0));
        assert_eq!(
            parse_rfc3339_to_nanos("1970-01-01T00:00:00+24:00"),
            Err(TimestampError::Malformed)
        );
    }

    #[test]
    fn rfc3339_rejects_impossible_dates_lowercase_and_leap_seconds() {
        for bad in [
            "2024-02-30T00:00:00Z",
            "2023-02-29T00:00:00Z",
            "2024-13-01T00:00:00Z",
            "2024-01-01t00:00:00Z",
            "2024-01-01T00:00:00z",
            "2024-01-01T23:59:60Z",
            "2024-01-01T00:00:00Zjunk",
            "not a date",
        ] {
            assert_eq!(parse_rfc3339_to_nanos(bad), Err(TimestampError::Malformed), "{bad}");
        }
        assert!(parse_rfc3339_to_nanos("2024-02-29T00:00:00Z").is_ok(), "2024 is a leap year");
    }

    #[test]
    fn rfc3339_far_future_is_out_of_range_not_malformed() {
        assert_eq!(parse_rfc3339_to_nanos("3000-01-01T00:00:00Z"), Err(TimestampError::OutOfRange));
    }

    #[test]
    fn decimal_nanos_is_digit_exact_where_f64_would_round() {
        // 19 significant digits, past an f64's ~16.
        assert_eq!(
            parse_decimal_nanos("1725400000.123456789", 1_000_000_000),
            Some(1_725_400_000_123_456_789)
        );
        let via_f64 = ("1725400000.123456789".parse::<f64>().unwrap() * 1e9).round() as i64;
        assert_ne!(via_f64, 1_725_400_000_123_456_789, "the f64 route really does round");
    }

    #[test]
    fn decimal_nanos_pads_short_fractions_and_accepts_no_fraction() {
        assert_eq!(
            parse_decimal_nanos("1725400000.123", 1_000_000_000),
            Some(1_725_400_000_123_000_000)
        );
        assert_eq!(parse_decimal_nanos("0.004", 1_000_000_000), Some(4_000_000));
        assert_eq!(parse_decimal_nanos("7", 1_000_000_000), Some(7_000_000_000));
        assert_eq!(parse_decimal_nanos("7.", 1_000_000_000), Some(7_000_000_000));
        assert_eq!(parse_decimal_nanos("12.5", 1_000), Some(12_500));
    }

    #[test]
    fn decimal_nanos_rejects_sub_target_unit_digits() {
        assert_eq!(parse_decimal_nanos("1.0000000001", 1_000_000_000), None, "ten places");
        assert_eq!(
            parse_decimal_nanos("1.0000000000", 1_000_000_000),
            Some(1_000_000_000),
            "trailing zeros past the unit are padding, not precision"
        );
        assert_eq!(parse_decimal_nanos("1.5", 1), None, "a fraction of a nanosecond");
        assert_eq!(parse_decimal_nanos("1.0", 1), Some(1));
        assert_eq!(parse_decimal_nanos("1.5", 1_000), Some(1_500));
    }

    #[test]
    fn decimal_nanos_rejects_every_other_shape() {
        for bad in ["", ".5", "-1", "+1", "1e9", "1,5", " 1", "1 ", "abc", "1.2.3", "0x10"] {
            assert_eq!(parse_decimal_nanos(bad, 1_000_000_000), None, "{bad:?}");
        }
    }

    #[test]
    fn decimal_nanos_overflow_is_none_not_a_panic() {
        assert_eq!(parse_decimal_nanos("9223372036854775808", 1), None, "i64::MAX + 1");
        assert_eq!(parse_decimal_nanos("9223372037", 1_000_000_000), None, "overflows on scale");
        assert_eq!(parse_decimal_nanos("9223372036.854775807", 1_000_000_000), Some(i64::MAX));
    }

    #[test]
    fn epoch_formats_as_the_unix_epoch_instant() {
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00.000000000Z");
    }

    #[test]
    fn a_known_instant_formats_correctly() {
        // Expected value from a reference RFC 3339 formatter, not from this one.
        let nanos: i64 = 1_788_114_041_512_847_391;
        assert_eq!(format_rfc3339_utc(nanos), "2026-08-30T18:20:41.512847391Z");
    }

    #[test]
    fn a_pre_epoch_negative_instant_formats_correctly() {
        assert_eq!(format_rfc3339_utc(-1_000_000_000), "1969-12-31T23:59:59.000000000Z");
    }

    #[test]
    fn a_sub_second_pre_epoch_instant_keeps_a_positive_nanosecond_remainder() {
        assert_eq!(format_rfc3339_utc(-1), "1969-12-31T23:59:59.999999999Z");
    }

    #[test]
    fn a_leap_year_date_formats_correctly() {
        let days_since_epoch: i64 = 19_782; // 2024-02-29 is this many days after 1970-01-01
        let nanos = days_since_epoch * 86_400 * 1_000_000_000;
        assert_eq!(format_rfc3339_utc(nanos), "2024-02-29T00:00:00.000000000Z");
    }

    #[test]
    fn nanosecond_precision_is_zero_padded_to_nine_digits() {
        assert_eq!(format_rfc3339_utc(1), "1970-01-01T00:00:00.000000001Z");
    }

    #[test]
    fn i64_min_and_max_do_not_panic() {
        // A `Value::Timestamp` is attacker-influenced; the date itself doesn't matter.
        let _ = format_rfc3339_utc(i64::MIN);
        let _ = format_rfc3339_utc(i64::MAX);
    }

    #[test]
    fn i64_max_lands_on_the_expected_far_future_date() {
        // The widely cited upper limit of 64-bit nanosecond timestamps.
        assert_eq!(format_rfc3339_utc(i64::MAX), "2262-04-11T23:47:16.854775807Z");
    }
}
