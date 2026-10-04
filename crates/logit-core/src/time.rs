//! Unix-nanosecond timestamps to and from text: the RFC 3339 and RFC 3164 renders, the strict RFC
//! 3339 parser the codecs share, and exact decimal-to-nanos.
//!
//! - **jiff does the calendar math** (`docs/adr/jiff-for-calendar-time.md`): civil-date
//!   validation, days since the epoch, and every render.
//! - **[`parse_rfc3339_to_nanos`] is RFC 5424's grammar, not jiff's.** A codec relays what it
//!   reads (`syslog_in`, `docker_in`, `datadog_in`, `trace_context`'s `span.*_rfc3339`), so byte
//!   tests fix the accepted forms before jiff validates the date: uppercase `T` and `Z`, a
//!   `±HH:MM` offset, 1-9 fractional digits, and nothing after. jiff's own RFC 3339 parser is
//!   wider (a space separator, lowercase `z`, a `[zone]` suffix, a clamped `:60`); that leniency
//!   belongs to the `timestamp` transform, through `crate::zoned::rfc3339_lenient`.
//! - **[`parse_decimal_nanos`] stays hand-rolled.** It is digit-exact unit scaling, not calendar
//!   math, and jiff has no decimal-to-nanos parse that avoids an `f64`. Its [`DecimalError`]
//!   tells a malformed string from a well-formed one whose count overflows an `i64`.

use std::fmt;

use jiff::civil::DateTime;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::fmt::temporal::DateTimePrinter;
use jiff::fmt::StdFmtWrite;
use jiff::tz::Offset;
use jiff::Timestamp;

/// Nine fractional digits and `Z`: the one RFC 3339 render.
const RFC3339_PRINTER: DateTimePrinter = DateTimePrinter::new().precision(Some(9));

/// Formats Unix nanoseconds as RFC 3339 in UTC (`Z`, never an offset) with nine fractional
/// digits: `2026-08-30T18:20:41.512847391Z`. Never panics: every `i64`, including the extremes,
/// maps to a calendar date. Allocates the returned `String`; [`write_rfc3339_utc`] writes into a
/// caller's buffer instead.
pub fn format_rfc3339_utc(nanos: i64) -> String {
    // 30 bytes is the rendered length for every year in `0000..=9999`.
    let mut out = String::with_capacity(30);
    write_rfc3339_utc(&mut out, nanos);
    out
}

/// [`format_rfc3339_utc`], written into `out` with no allocation of its own. A `fmt::Error` from
/// `out` is discarded; a `String` never returns one.
pub fn write_rfc3339_utc(out: &mut impl fmt::Write, nanos: i64) {
    let _ = RFC3339_PRINTER.print_timestamp(&timestamp_of(nanos), StdFmtWrite(out));
}

/// RFC 3164's TIMESTAMP, `Mmm dd hh:mm:ss` in UTC with the day space-padded (`Sep  2 14:03:11`)
/// and no year, written into `out` with no allocation of its own. Never panics.
pub fn write_rfc3164_utc(out: &mut impl fmt::Write, nanos: i64) {
    let civil = Offset::UTC.to_datetime(timestamp_of(nanos));
    let _ = BrokenDownTime::from(civil).format("%b %e %H:%M:%S", StdFmtWrite(out));
}

/// jiff's `Timestamp` spans about years -9999..=9999, so every `i64` of nanoseconds
/// (1677-09-21..2262-04-11) converts.
fn timestamp_of(nanos: i64) -> Timestamp {
    Timestamp::from_nanosecond(i128::from(nanos)).expect("every i64 of nanoseconds is in range")
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
/// Accepts `YYYY-MM-DDTHH:MM:SS`, an optional `.` and 1-9 fractional digits (RFC 5424 allows six;
/// nine is what nanoseconds hold), then `Z` or `+HH:MM`/`-HH:MM` with hours `00..=23`, and nothing
/// else; `T` and `Z` are uppercase. Rejects a nonexistent date (`2023-02-29`) and a leap second
/// (`:60`): RFC 5424 forbids both, and normalizing them would attach a wrong instant. A
/// well-formed timestamp outside roughly 1677-09-21..2262-04-11 is
/// [`TimestampError::OutOfRange`], not `Malformed`.
pub fn parse_rfc3339_to_nanos(s: &str) -> Result<i64, TimestampError> {
    let (civil, offset_seconds) =
        parse_rfc3339_civil(s.as_bytes()).ok_or(TimestampError::Malformed)?;
    // `duration_since` is exact across jiff's whole civil range, so a date jiff's `Timestamp`
    // can't hold (`9999-12-31T23:59:59Z`) still reads as out of range rather than malformed.
    let since_epoch = civil.duration_since(DateTime::constant(1970, 1, 1, 0, 0, 0, 0));
    let nanos = since_epoch.as_nanos() - i128::from(offset_seconds) * 1_000_000_000;
    i64::try_from(nanos).map_err(|_| TimestampError::OutOfRange)
}

/// The grammar half of [`parse_rfc3339_to_nanos`]: byte tests for RFC 5424's fixed layout, then
/// jiff's `DateTime::new` for the calendar (month and day ranges, leap years, `:60`). Returns the
/// civil time as written and the offset in seconds east of UTC.
fn parse_rfc3339_civil(b: &[u8]) -> Option<(DateTime, i32)> {
    // "YYYY-MM-DDTHH:MM:SSZ" is the shortest legal form.
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let year = digits(b, 0, 4)?;
    let month = digits(b, 5, 2)?;
    let day = digits(b, 8, 2)?;
    let hour = digits(b, 11, 2)?;
    let minute = digits(b, 14, 2)?;
    let second = digits(b, 17, 2)?;

    let mut idx = 19;
    let mut subsec: i32 = 0;
    if b[idx] == b'.' {
        idx += 1;
        let start = idx;
        while b.get(idx).is_some_and(u8::is_ascii_digit) {
            idx += 1;
        }
        // 1-9 digits; RFC 5424's `TIME-SECFRAC` (`"." 1*6DIGIT`) is a subset.
        let len = idx - start;
        if !(1..=9).contains(&len) {
            return None;
        }
        subsec = digits(b, start, len)? * 10i32.pow(9 - len as u32);
    }

    let offset_seconds = match b.get(idx)? {
        b'Z' if idx + 1 == b.len() => 0,
        sign @ (b'+' | b'-') if idx + 6 == b.len() && b[idx + 3] == b':' => {
            let oh = digits(b, idx + 1, 2)?;
            let om = digits(b, idx + 4, 2)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let magnitude = oh * 3600 + om * 60;
            if *sign == b'-' {
                -magnitude
            } else {
                magnitude
            }
        }
        _ => return None,
    };

    // Every field is at most four digits, so the narrowing casts are lossless.
    let civil = DateTime::new(
        year as i16,
        month as i8,
        day as i8,
        hour as i8,
        minute as i8,
        second as i8,
        subsec,
    )
    .ok()?;
    Some((civil, offset_seconds))
}

/// `n` ASCII digits of `b` from `start` as a number; `None` for any other byte. `n` is at most 9,
/// so the value fits an `i32`.
fn digits(b: &[u8], start: usize, n: usize) -> Option<i32> {
    b.get(start..start + n)?
        .iter()
        .try_fold(0i32, |acc, &c| c.is_ascii_digit().then(|| acc * 10 + i32::from(c - b'0')))
}

/// Why [`parse_decimal_nanos`] refused a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalError {
    /// Not the grammar, or nonzero digits finer than the target unit.
    Invalid,
    /// Well formed, but the count doesn't fit an `i64`.
    Overflow,
}

/// Parses an unsigned decimal in one unit into an exact integer count of a finer unit, with no
/// `f64`: `parse_decimal_nanos("1725400000.123456789", 1_000_000_000)` is
/// `1_725_400_000_123_456_789`. An epoch-magnitude value like nginx's `$msec` scaled to
/// nanoseconds has more significant digits than an `f64` holds, so a float route would round.
///
/// `scale` is target units per source unit (`1_000_000_000` for seconds, `1_000` for micros).
/// Nonzero digits past `log10(scale)` places are `Invalid` rather than truncating; trailing zeros
/// there are accepted. Grammar: `DIGIT+ ("." DIGIT*)?`. Any other shape, or excess precision,
/// is [`DecimalError::Invalid`]; a well-formed value whose count overflows an `i64` is
/// [`DecimalError::Overflow`].
pub fn parse_decimal_nanos(s: &str, scale: i64) -> Result<i64, DecimalError> {
    let (whole, frac) = match s.split_once('.') {
        Some((w, f)) => (w, f),
        None => (s, ""),
    };
    if whole.is_empty() || !whole.bytes().all(|c| c.is_ascii_digit()) {
        return Err(DecimalError::Invalid);
    }
    if !frac.bytes().all(|c| c.is_ascii_digit()) {
        return Err(DecimalError::Invalid);
    }
    let whole: i64 = whole.parse().map_err(|_| DecimalError::Overflow)?;
    let mut out = whole.checked_mul(scale).ok_or(DecimalError::Overflow)?;
    // Each fractional digit is worth `scale / 10^position` target units; stays integral.
    let mut place = scale;
    for c in frac.bytes() {
        let digit = i64::from(c - b'0');
        if place == 1 {
            // Below one target unit: zero is padding, anything else is unrepresentable.
            if digit != 0 {
                return Err(DecimalError::Invalid);
            }
            continue;
        }
        place /= 10;
        out = digit
            .checked_mul(place)
            .and_then(|d| out.checked_add(d))
            .ok_or(DecimalError::Overflow)?;
    }
    Ok(out)
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
            Ok(1_725_400_000_123_456_789)
        );
        let via_f64 = ("1725400000.123456789".parse::<f64>().unwrap() * 1e9).round() as i64;
        assert_ne!(via_f64, 1_725_400_000_123_456_789, "the f64 route really does round");
    }

    #[test]
    fn decimal_nanos_pads_short_fractions_and_accepts_no_fraction() {
        assert_eq!(
            parse_decimal_nanos("1725400000.123", 1_000_000_000),
            Ok(1_725_400_000_123_000_000)
        );
        assert_eq!(parse_decimal_nanos("0.004", 1_000_000_000), Ok(4_000_000));
        assert_eq!(parse_decimal_nanos("7", 1_000_000_000), Ok(7_000_000_000));
        assert_eq!(parse_decimal_nanos("7.", 1_000_000_000), Ok(7_000_000_000));
        assert_eq!(parse_decimal_nanos("12.5", 1_000), Ok(12_500));
    }

    #[test]
    fn decimal_nanos_rejects_sub_target_unit_digits() {
        assert_eq!(
            parse_decimal_nanos("1.0000000001", 1_000_000_000),
            Err(DecimalError::Invalid),
            "ten places"
        );
        assert_eq!(
            parse_decimal_nanos("1.0000000000", 1_000_000_000),
            Ok(1_000_000_000),
            "trailing zeros past the unit are padding, not precision"
        );
        assert_eq!(
            parse_decimal_nanos("1.5", 1),
            Err(DecimalError::Invalid),
            "a fraction of a nanosecond"
        );
        assert_eq!(parse_decimal_nanos("1.0", 1), Ok(1));
        assert_eq!(parse_decimal_nanos("1.5", 1_000), Ok(1_500));
    }

    #[test]
    fn decimal_nanos_rejects_every_other_shape() {
        for bad in ["", ".5", "-1", "+1", "1e9", "1,5", " 1", "1 ", "abc", "1.2.3", "0x10"] {
            assert_eq!(
                parse_decimal_nanos(bad, 1_000_000_000),
                Err(DecimalError::Invalid),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn decimal_nanos_overflow_is_distinct_from_invalid() {
        assert_eq!(
            parse_decimal_nanos("9223372036854775808", 1),
            Err(DecimalError::Overflow),
            "i64::MAX + 1"
        );
        assert_eq!(
            parse_decimal_nanos("9223372037", 1_000_000_000),
            Err(DecimalError::Overflow),
            "overflows on scale"
        );
        assert_eq!(
            parse_decimal_nanos("9223372036.854775808", 1_000_000_000),
            Err(DecimalError::Overflow),
            "fraction path"
        );
        assert_eq!(parse_decimal_nanos("9223372036.854775807", 1_000_000_000), Ok(i64::MAX));
        for text in ["1.5", "", ".5"] {
            assert_eq!(parse_decimal_nanos(text, 1), Err(DecimalError::Invalid), "{text:?}");
        }
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
        // A `Value::Timestamp` comes from the wire; both renders must cover every `i64`.
        assert_eq!(format_rfc3339_utc(i64::MIN), "1677-09-21T00:12:43.145224192Z");
        let mut out = String::new();
        write_rfc3164_utc(&mut out, i64::MIN);
        out.push('|');
        write_rfc3164_utc(&mut out, i64::MAX);
        assert_eq!(out, "Sep 21 00:12:43|Apr 11 23:47:16");
    }

    #[test]
    fn i64_max_lands_on_the_expected_far_future_date() {
        // The widely cited upper limit of 64-bit nanosecond timestamps.
        assert_eq!(format_rfc3339_utc(i64::MAX), "2262-04-11T23:47:16.854775807Z");
    }

    #[test]
    fn the_i64_extremes_round_trip_and_one_past_is_out_of_range() {
        for nanos in [i64::MIN, i64::MAX] {
            assert_eq!(parse_rfc3339_to_nanos(&format_rfc3339_utc(nanos)), Ok(nanos));
        }
        assert_eq!(
            parse_rfc3339_to_nanos("2262-04-11T23:47:16.854775808Z"),
            Err(TimestampError::OutOfRange)
        );
        assert_eq!(
            parse_rfc3339_to_nanos("1677-09-21T00:12:43.145224191Z"),
            Err(TimestampError::OutOfRange)
        );
    }

    #[test]
    fn rfc3339_dates_past_jiffs_timestamp_range_are_out_of_range() {
        // Valid civil dates jiff's `Timestamp` itself can't hold once the offset is applied.
        assert_eq!(parse_rfc3339_to_nanos("9999-12-31T23:59:59Z"), Err(TimestampError::OutOfRange));
        assert_eq!(parse_rfc3339_to_nanos("0000-01-01T00:00:00Z"), Err(TimestampError::OutOfRange));
        assert_eq!(
            parse_rfc3339_to_nanos("0000-01-01T00:00:00+23:59"),
            Err(TimestampError::OutOfRange)
        );
    }

    #[test]
    fn rfc3339_rejects_what_jiffs_own_parser_accepts() {
        for bad in [
            "2024-01-01 00:00:00Z",
            "2024-01-01T00:00:00Z[UTC]",
            "2024-01-01T00:00:00+00:00[UTC]",
            "2024-01-01T00:00:00,5Z",
            "2024-01-01T00:00:00+0100",
            "2024-01-01T00:00:00+01",
            "2024-01-01T00:00:00+01:00:00",
            "2024-01-01T00:00Z",
            "20240101T000000Z",
            "+2024-01-01T00:00:00Z",
            "2024-01-01T00:00:00",
            "2024-01-01T24:00:00Z",
            "2024-01-01T00:60:00Z",
            "2024-01-01T00:00:00+23:60",
            "2024-01-01T00:00:00-24:00",
        ] {
            assert_eq!(parse_rfc3339_to_nanos(bad), Err(TimestampError::Malformed), "{bad}");
        }
        assert_eq!(parse_rfc3339_to_nanos("1970-01-01T23:59:00+23:59"), Ok(0));
        assert_eq!(parse_rfc3339_to_nanos("1970-01-01T00:00:00-00:00"), Ok(0));
    }

    #[test]
    fn rfc3164_render_space_pads_the_day() {
        let mut out = String::new();
        // 2026-09-02T14:03:11.999Z: the fraction is dropped, not rounded.
        write_rfc3164_utc(&mut out, 1_788_357_791_999_000_000);
        assert_eq!(out, "Sep  2 14:03:11");
        out.clear();
        write_rfc3164_utc(&mut out, -1);
        assert_eq!(out, "Dec 31 23:59:59");
    }
}
