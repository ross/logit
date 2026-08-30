//! Hand-rolled RFC 3339 UTC timestamp formatting.
//!
//! `stdio_out` (`crates/logit-outputs/src/stdio.rs`) needs to turn a Unix-nanosecond timestamp --
//! an event's own `timestamp`, or a `Value::Timestamp` attribute -- into something a human can
//! read, and needs nothing fancier than that: no parsing, no timezones other than UTC, no
//! calendar arithmetic beyond "what civil date is this many days after the epoch". That's cheap
//! enough to hand-roll (the civil-from-days conversion below is met in about thirty deterministic
//! lines) and not worth a `time`/`chrono` workspace dependency for -- adding one is an ADR-scale
//! decision (see `AGENTS.md`'s "design constraints" and `logit-config`'s own hand-rolled
//! humantime-flavored duration codec, which this follows the same reasoning as).
//!
//! TODO: replace with a real crate once the crate list is finalized.

/// Formats a Unix-epoch nanosecond timestamp as RFC 3339, always in UTC (a trailing `Z`, never an
/// offset), with full nanosecond precision: `2026-08-30T18:20:41.512847391Z`. Never panics -- every
/// `i64` nanosecond count, including `i64::MIN`/`i64::MAX`, maps to *some* (possibly absurd, e.g.
/// year -292277022657) calendar date, because the underlying civil-from-days conversion is exact
/// over the full `i64` day range, not just the range any real event will ever carry.
pub fn format_rfc3339_utc(nanos: i64) -> String {
    // `div_euclid`/`rem_euclid`, not `/`/`%`: for a negative `nanos` (any instant before the
    // epoch), plain truncating division would round toward zero and produce a negative remainder
    // (e.g. -1ns would split into 0 seconds and -1 nanosecond-of-second) -- euclidean division
    // instead always floors, so `nanos_of_sec`/`secs_of_day` stay in their natural `[0, N)` range
    // no matter which side of the epoch `nanos` falls on.
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

/// Converts a day count since the Unix epoch (1970-01-01, day 0) into a proleptic-Gregorian
/// `(year, month, day)` -- exact over the entire `i64` range, no lookup tables. This is Howard
/// Hinnant's `civil_from_days` algorithm
/// (<http://howardhinnant.github.io/date_algorithms.html#civil_from_days>), rewritten with
/// `div_euclid`/`rem_euclid` in place of the reference implementation's branchy negative-`z`
/// adjustment -- same result, since both are just ways of getting a floored (not truncated)
/// division/modulo pair.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Shift the epoch from 1970-01-01 to 0000-03-01 -- the algorithm's day-of-era math assumes a
    // year that starts on March 1st, so February (with its leap-day wrinkle) always falls at the
    // *end* of its "year" instead of needing special-cased handling.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_formats_as_the_unix_epoch_instant() {
        assert_eq!(format_rfc3339_utc(0), "1970-01-01T00:00:00.000000000Z");
    }

    #[test]
    fn a_known_instant_formats_correctly() {
        // 2026-08-30T18:20:41.512847391Z, chosen independently of this crate (computed via a
        // reference RFC 3339 formatter, not derived from `format_rfc3339_utc` itself).
        let nanos: i64 = 1_788_114_041_512_847_391;
        assert_eq!(format_rfc3339_utc(nanos), "2026-08-30T18:20:41.512847391Z");
    }

    #[test]
    fn a_pre_epoch_negative_instant_formats_correctly() {
        // One second before the epoch: 1969-12-31T23:59:59Z, not "1970-01-00" or a panic from a
        // truncating (round-toward-zero) division.
        assert_eq!(format_rfc3339_utc(-1_000_000_000), "1969-12-31T23:59:59.000000000Z");
    }

    #[test]
    fn a_sub_second_pre_epoch_instant_keeps_a_positive_nanosecond_remainder() {
        // -1ns: truncating division would split this into 0 seconds and -1 nanosecond, an
        // out-of-range remainder. Euclidean division must floor instead, landing on the *previous*
        // second with a positive 999999999ns remainder.
        assert_eq!(format_rfc3339_utc(-1), "1969-12-31T23:59:59.999999999Z");
    }

    #[test]
    fn a_leap_year_date_formats_correctly() {
        // 2024-02-29T00:00:00Z -- 2024 is a leap year (divisible by 4, not by 100).
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
        // Not asserting a specific (necessarily absurd) calendar date -- just that formatting the
        // extreme ends of the representable range never panics, since a `Value::Timestamp`
        // attribute is user/attacker-influenced data this sink must never crash on.
        let _ = format_rfc3339_utc(i64::MIN);
        let _ = format_rfc3339_utc(i64::MAX);
    }

    #[test]
    fn i64_max_lands_on_the_expected_far_future_date() {
        // i64::MAX nanoseconds is 9,223,372,036 seconds and change past the epoch --
        // 2262-04-11T23:47:16.854775807Z, a widely-cited fact about 64-bit nanosecond timestamps'
        // range (this is the same limit `logit-outputs::influxdb`'s `allocate_timestamp` doc
        // comment cites for `i64::MAX` nanoseconds).
        assert_eq!(format_rfc3339_utc(i64::MAX), "2262-04-11T23:47:16.854775807Z");
    }
}
