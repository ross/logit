//! Calendar resolution: time zones, strftime-style patterns, and civil-time-to-instant rules,
//! shared by graph validation and the `timestamp` transform so one function decides what a valid
//! zone or pattern is (`docs/adr/timestamp-transform.md`).
//!
//! - **jiff lives here only.** Nothing outside this module names a jiff type; callers see
//!   [`Zone`], [`Pattern`], and unix nanoseconds as `i64`.
//! - **`UTC` and fixed offsets need no database.** `UTC` matches in any case; offsets are
//!   `±HH:MM`, `±HHMM`, or `±HH` with hours `00..=23`. Any other name is looked up in the system
//!   tzdb (`/usr/share/zoneinfo`, or `TZDIR`), case-insensitively. A lookup that fails against an
//!   empty database is [`ZoneError::NoDatabase`], so an image missing `tzdata` reads differently
//!   from a typo ([`ZoneError::Unknown`]).
//! - **A pattern's kind is decided by its text, never by a value.** `%z`, `%:z`, or `%s` makes it
//!   an [`PatternKind::Instant`]; otherwise it's civil time read in the zone, with the year
//!   inferred when the pattern has none. [`Pattern::compile`] proves a pattern parses its own
//!   rendering of a reference instant, so an unusable pattern fails at validation, not per event.
//! - **Year inference** (RFC 3164 and a year-less pattern): of the receipt instant's year in the
//!   zone, the year before, and the year after, the candidate whose instant is closest to receipt
//!   wins, ties going to the earlier. A candidate that isn't a valid date (Feb 29 in a non-leap
//!   year) is skipped.
//! - **DST**: in a fold, the occurrence closest to receipt wins, ties going to the earlier; in a
//!   gap, the civil time is read with the offset in force before the gap, which lands it after
//!   the gap (`02:30` in a spring-forward gap reads as `03:30` daylight time).
//! - **Range**: jiff's `Timestamp` covers about years -9999..=9999, wider than an `i64` of
//!   nanoseconds (1677-09-21..2262-04-11). A value inside jiff's range and outside `i64` is
//!   [`ResolveError::OutOfRange`], with `past` saying which side; one jiff can't represent is
//!   [`ResolveError::Invalid`].
//!
//! [`rfc3164`] and a [`pattern`] call allocate nothing on success: the zone is borrowed, the
//! fields parse into jiff's stack-held `BrokenDownTime`, and offset lookups read the loaded tzdb.
//! A failed parse may allocate jiff's error. [`Zone::parse`] and [`Pattern::compile`] allocate
//! and run once per component, at startup.

use jiff::civil::DateTime;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::{AmbiguousOffset, Offset, TimeZone, TimeZoneDatabase};
use jiff::{Timestamp, Zoned};

/// A resolved time zone: `UTC`, a fixed offset, or an IANA zone loaded from the system tzdb.
/// `Clone` is a refcount bump.
#[derive(Clone, Debug)]
pub struct Zone(TimeZone);

/// Why a `timezone:` value didn't resolve.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ZoneError {
    #[error("unknown time zone '{0}'")]
    Unknown(String),
    #[error("no system time zone database found (install the tzdata package or set TZDIR)")]
    NoDatabase,
}

impl Zone {
    /// The UTC zone, the default.
    pub fn utc() -> Zone {
        Zone(TimeZone::UTC)
    }

    /// Resolves `UTC`, a fixed offset, or an IANA name against the system tzdb.
    pub fn parse(name: &str) -> Result<Zone, ZoneError> {
        Self::parse_in(name, jiff::tz::db())
    }

    /// [`Zone::parse`] against a given database, so a test can supply an empty one.
    pub(crate) fn parse_in(name: &str, db: &TimeZoneDatabase) -> Result<Zone, ZoneError> {
        if name.eq_ignore_ascii_case("utc") {
            return Ok(Zone::utc());
        }
        if let Some(offset) = fixed_offset(name) {
            return Ok(Zone(TimeZone::fixed(offset)));
        }
        match db.get(name) {
            Ok(tz) => Ok(Zone(tz)),
            Err(_) if db.is_definitively_empty() => Err(ZoneError::NoDatabase),
            Err(_) => Err(ZoneError::Unknown(name.to_string())),
        }
    }

    /// Whether this is UTC or a zero fixed offset.
    pub fn is_utc(&self) -> bool {
        self.0 == TimeZone::UTC || self.0.to_fixed_offset().is_ok_and(|o| o == Offset::UTC)
    }
}

/// `±HH:MM`, `±HHMM`, or `±HH`, hours `00..=23` and minutes `00..=59`. jiff's own offset range
/// reaches `±25:59:59`, wider than any real zone, so the check is local.
fn fixed_offset(name: &str) -> Option<Offset> {
    let b = name.as_bytes();
    let sign = match b.first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let two = |i: usize| -> Option<i32> {
        let (h, l) = (*b.get(i)?, *b.get(i + 1)?);
        (h.is_ascii_digit() && l.is_ascii_digit()).then(|| i32::from((h - b'0') * 10 + (l - b'0')))
    };
    let (hours, minutes) = match b.len() {
        3 => (two(1)?, 0),
        5 => (two(1)?, two(3)?),
        6 if b[3] == b':' => (two(1)?, two(4)?),
        _ => return None,
    };
    if hours > 23 || minutes > 59 {
        return None;
    }
    Offset::from_seconds(sign * (hours * 3600 + minutes * 60)).ok()
}

/// What a pattern's result is, decided by its text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PatternKind {
    /// Carries `%z`, `%:z`, or `%s`: an instant, read with no zone.
    Instant,
    /// A civil date and time, read in the zone.
    Civil,
    /// A civil month, day, and time with no year, which is inferred.
    CivilNoYear,
}

/// A strftime-style pattern as jiff's `fmt::strtime` defines it, validated by [`Pattern::compile`].
#[derive(Clone, Debug)]
pub struct Pattern {
    text: String,
    kind: PatternKind,
}

/// Why a pattern is unusable.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PatternError {
    #[error("pattern is empty")]
    Empty,
    #[error("pattern uses {0}, which can't be parsed")]
    Directive(&'static str),
    #[error("pattern has no time of day")]
    NoTimeOfDay,
    #[error("pattern does not round-trip a reference instant: {0}")]
    RoundTrip(String),
}

/// The instant [`Pattern::compile`] renders and parses back: every field distinct, so a
/// directive read into the wrong slot shows up as a mismatch.
const REFERENCE_NANOS: i128 = 981_216_306_007_008_009; // 2001-02-03T16:05:06.007008009Z

/// A parsed result this far below the reference still round-trips: a pattern may stop at
/// minutes or seconds. Anything wrong in the hour or above lands outside it.
const ROUND_TRIP_SLACK_NANOS: i128 = 60_000_000_000;

impl Pattern {
    /// Validates `text` and decides its [`PatternKind`]. Rejects an empty pattern; `%Z`, `%Q`, and
    /// `%:Q` (a zone abbreviation is ambiguous, and nothing per event looks a zone up by name); a
    /// pattern with no hour and no `%s`; and one that can't parse its own rendering of a reference
    /// instant back to that instant (an unknown directive, `%I` with no `%p`, an instant with no
    /// year).
    pub fn compile(text: &str) -> Result<Pattern, PatternError> {
        if text.is_empty() {
            return Err(PatternError::Empty);
        }
        if let Some(directive) = rejected_directive(text) {
            return Err(PatternError::Directive(directive));
        }
        let reference = Timestamp::from_nanosecond(REFERENCE_NANOS).expect("in range");
        let reference_zoned = Zoned::new(reference, TimeZone::UTC);
        let mut rendered = String::new();
        BrokenDownTime::from(&reference_zoned)
            .format(text, &mut rendered)
            .map_err(|e| PatternError::RoundTrip(e.to_string()))?;
        let mut parsed = BrokenDownTime::parse(text, &rendered)
            .map_err(|e| PatternError::RoundTrip(e.to_string()))?;
        if parsed.hour().is_none() && parsed.timestamp().is_none() {
            return Err(PatternError::NoTimeOfDay);
        }
        let kind = if parsed.offset().is_some() || parsed.timestamp().is_some() {
            PatternKind::Instant
        } else if parsed.year().is_none() {
            PatternKind::CivilNoYear
        } else {
            PatternKind::Civil
        };
        let back = match kind {
            PatternKind::Instant => parsed.to_timestamp(),
            PatternKind::Civil | PatternKind::CivilNoYear => {
                if kind == PatternKind::CivilNoYear {
                    parsed
                        .set_year(Some(reference_zoned.year()))
                        .map_err(|e| PatternError::RoundTrip(e.to_string()))?;
                }
                parsed.to_datetime().and_then(|dt| Offset::UTC.to_timestamp(dt))
            }
        }
        .map_err(|e| PatternError::RoundTrip(e.to_string()))?;
        let behind = REFERENCE_NANOS - back.as_nanosecond();
        if !(0..ROUND_TRIP_SLACK_NANOS).contains(&behind) {
            return Err(PatternError::RoundTrip(format!(
                "'{rendered}' parses back as {back}, not {reference}"
            )));
        }
        Ok(Pattern { text: text.to_string(), kind })
    }

    pub fn kind(&self) -> PatternKind {
        self.kind
    }

    /// Whether resolving a value reads the configured zone.
    pub fn reads_zone(&self) -> bool {
        self.kind != PatternKind::Instant
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

/// The first `%Z`, `%Q`, or `%:Q` in `text`, skipping jiff's flag, width, and precision syntax
/// between `%` and the directive, and `%%`.
fn rejected_directive(text: &str) -> Option<&'static str> {
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'%' {
            i += 1;
            continue;
        }
        i += 1;
        while i < b.len() && matches!(b[i], b'_' | b'-' | b'0'..=b'9' | b'^' | b'#' | b'.') {
            i += 1;
        }
        let mut colons = 0;
        while i < b.len() && b[i] == b':' {
            colons += 1;
            i += 1;
        }
        match (b.get(i), colons) {
            (Some(b'Z'), _) => return Some("%Z"),
            (Some(b'Q'), 0) => return Some("%Q"),
            (Some(b'Q'), _) => return Some("%:Q"),
            _ => {}
        }
        i += 1;
    }
    None
}

/// Why a value didn't resolve. `Invalid` doesn't parse; `OutOfRange` parsed to an instant outside
/// an `i64` of nanoseconds, `past` saying which side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveError {
    Invalid,
    OutOfRange { past: bool },
}

fn to_i64(nanos: i128) -> Result<i64, ResolveError> {
    i64::try_from(nanos).map_err(|_| ResolveError::OutOfRange { past: nanos < 0 })
}

/// RFC 3339 through jiff's RFC 3339 and Temporal parser: an offset is required; a space
/// separator, a lowercase `z`, and an RFC 9557 `[zone]` suffix are accepted; a `:60` leap second
/// clamps to `:59`.
pub fn rfc3339_lenient(s: &str) -> Result<i64, ResolveError> {
    let ts: Timestamp = s.parse().map_err(|_| ResolveError::Invalid)?;
    to_i64(ts.as_nanosecond())
}

const MONTHS: [&[u8; 3]; 12] = [
    b"jan", b"feb", b"mar", b"apr", b"may", b"jun", b"jul", b"aug", b"sep", b"oct", b"nov", b"dec",
];

/// RFC 3164's 15-byte `Mmm dd hh:mm:ss` (day zero- or space-padded, month in any case), read as
/// civil time in `zone` with the year inferred from `receipt` (unix nanoseconds).
pub fn rfc3164(s: &str, zone: &Zone, receipt: i64) -> Result<i64, ResolveError> {
    let b = s.as_bytes();
    if b.len() != 15 || b[3] != b' ' || b[6] != b' ' || b[9] != b':' || b[12] != b':' {
        return Err(ResolveError::Invalid);
    }
    let month = MONTHS
        .iter()
        .position(|m| b[..3].eq_ignore_ascii_case(&m[..]))
        .ok_or(ResolveError::Invalid)? as i8
        + 1;
    let digit = |c: u8| c.is_ascii_digit().then(|| (c - b'0') as i8);
    let two = |i: usize| Some(digit(b[i])? * 10 + digit(b[i + 1])?);
    let day = match b[4] {
        b' ' => digit(b[5]),
        _ => two(4),
    }
    .filter(|d| (1..=31).contains(d))
    .ok_or(ResolveError::Invalid)?;
    let hour = two(7).filter(|h| *h <= 23).ok_or(ResolveError::Invalid)?;
    let minute = two(10).filter(|m| *m <= 59).ok_or(ResolveError::Invalid)?;
    let second = two(13).filter(|s| *s <= 59).ok_or(ResolveError::Invalid)?;
    infer_year(zone, |year| DateTime::new(year, month, day, hour, minute, second, 0).ok(), receipt)
}

/// `s` under a compiled pattern, which must match the whole value. An [`PatternKind::Instant`]
/// ignores `zone`; civil kinds read in it, inferring a missing year from `receipt`.
pub fn pattern(p: &Pattern, s: &str, zone: &Zone, receipt: i64) -> Result<i64, ResolveError> {
    let mut parsed = BrokenDownTime::parse(&p.text, s).map_err(|_| ResolveError::Invalid)?;
    match p.kind {
        PatternKind::Instant => {
            let ts = parsed.to_timestamp().map_err(|_| ResolveError::Invalid)?;
            to_i64(ts.as_nanosecond())
        }
        PatternKind::Civil => {
            let dt = parsed.to_datetime().map_err(|_| ResolveError::Invalid)?;
            civil_to_instant(zone, dt, receipt)
        }
        PatternKind::CivilNoYear => infer_year(
            zone,
            |year| {
                parsed.set_year(Some(year)).ok()?;
                parsed.to_datetime().ok()
            },
            receipt,
        ),
    }
}

/// Civil `dt` in `zone` to unix nanoseconds: in a fold, the occurrence closest to `receipt`
/// (ties to the earlier); in a gap, jiff's `compatible` (the offset before the gap).
fn civil_to_instant(zone: &Zone, dt: DateTime, receipt: i64) -> Result<i64, ResolveError> {
    let ambiguous = zone.0.to_ambiguous_timestamp(dt);
    let ts = match ambiguous.offset() {
        AmbiguousOffset::Fold { before, after } => {
            let earlier = before.to_timestamp(dt).map_err(|_| ResolveError::Invalid)?;
            let later = after.to_timestamp(dt).map_err(|_| ResolveError::Invalid)?;
            let distance = |t: Timestamp| (t.as_nanosecond() - i128::from(receipt)).abs();
            if distance(later) < distance(earlier) {
                later
            } else {
                earlier
            }
        }
        AmbiguousOffset::Gap { .. } | AmbiguousOffset::Unambiguous { .. } => {
            ambiguous.compatible().map_err(|_| ResolveError::Invalid)?
        }
    };
    to_i64(ts.as_nanosecond())
}

/// Tries the receipt instant's year in `zone`, the year before, and the year after, and keeps
/// the instant closest to `receipt` (ties to the earlier). `set_year` builds the civil time for a
/// candidate year, `None` when that year makes it invalid. With no candidate resolving, an
/// out-of-range candidate's error wins over `Invalid`.
fn infer_year(
    zone: &Zone,
    mut set_year: impl FnMut(i16) -> Option<DateTime>,
    receipt: i64,
) -> Result<i64, ResolveError> {
    let receipt_ts =
        Timestamp::from_nanosecond(i128::from(receipt)).map_err(|_| ResolveError::Invalid)?;
    let year = zone.0.to_datetime(receipt_ts).year();
    let mut best: Option<i64> = None;
    let mut fallback = ResolveError::Invalid;
    for candidate in [year.checked_sub(1), Some(year), year.checked_add(1)] {
        let Some(dt) = candidate.and_then(&mut set_year) else { continue };
        match civil_to_instant(zone, dt, receipt) {
            Ok(n) => {
                let closer = best.is_none_or(|b| {
                    (i128::from(n) - i128::from(receipt)).abs()
                        < (i128::from(b) - i128::from(receipt)).abs()
                });
                if closer {
                    best = Some(n);
                }
            }
            Err(e @ ResolveError::OutOfRange { .. }) => fallback = e,
            Err(ResolveError::Invalid) => {}
        }
    }
    best.ok_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::parse_rfc3339_to_nanos;

    /// Unix nanoseconds for an RFC 3339 literal, through the crate's own hand-rolled parser so
    /// expectations don't come from jiff.
    fn at(s: &str) -> i64 {
        parse_rfc3339_to_nanos(s).unwrap()
    }

    fn zone(name: &str) -> Zone {
        Zone::parse(name).unwrap()
    }

    // -- Zone -------------------------------------------------------------------------------------

    #[test]
    fn utc_in_any_case_is_utc() {
        for name in ["UTC", "utc", "Utc"] {
            assert!(zone(name).is_utc(), "{name}");
        }
        assert!(Zone::utc().is_utc());
        assert!(zone("+00:00").is_utc());
        assert!(!zone("+01:00").is_utc());
    }

    #[test]
    fn fixed_offsets_in_three_forms() {
        let civil = DateTime::new(2026, 6, 1, 12, 0, 0, 0).unwrap();
        let cases = [
            ("+05:30", "2026-06-01T06:30:00Z"),
            ("-0800", "2026-06-01T20:00:00Z"),
            ("+05", "2026-06-01T07:00:00Z"),
        ];
        for (name, want) in cases {
            assert_eq!(civil_to_instant(&zone(name), civil, 0), Ok(at(want)), "{name}");
        }
    }

    #[test]
    fn malformed_offsets_and_z_are_not_zones() {
        for name in ["Z", "+24:00", "+05:60", "+5", "+0530x", "05:30", "+05-30", "+05:3"] {
            assert!(Zone::parse(name).is_err(), "{name}");
            assert!(fixed_offset(name).is_none(), "{name}");
        }
    }

    #[test]
    fn iana_names_resolve_through_the_system_database() {
        assert!(!zone("America/New_York").is_utc());
        zone("Etc/UTC");
        zone("america/new_york");
        assert_eq!(
            Zone::parse("Nowhere/Here").unwrap_err(),
            ZoneError::Unknown("Nowhere/Here".into())
        );
    }

    #[test]
    fn an_empty_database_is_no_database_but_utc_and_offsets_still_parse() {
        // `from_dir` refuses a directory with no TZif files, so the empty seam is `none()`.
        let dir = std::env::temp_dir().join(format!("logit-zoned-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        assert!(TimeZoneDatabase::from_dir(&dir).is_err());
        std::fs::remove_dir_all(&dir).ok();

        let db = TimeZoneDatabase::none();
        assert!(db.is_definitively_empty());
        assert_eq!(Zone::parse_in("America/New_York", &db).unwrap_err(), ZoneError::NoDatabase);
        assert!(Zone::parse_in("UTC", &db).is_ok());
        assert!(Zone::parse_in("+01:00", &db).is_ok());
    }

    // -- Pattern::compile -------------------------------------------------------------------------

    #[test]
    fn compile_rejects_unusable_patterns() {
        assert_eq!(Pattern::compile("").unwrap_err(), PatternError::Empty);
        assert_eq!(Pattern::compile("%Y %Z").unwrap_err(), PatternError::Directive("%Z"));
        assert_eq!(Pattern::compile("%H %Q").unwrap_err(), PatternError::Directive("%Q"));
        assert_eq!(Pattern::compile("%H %:Q").unwrap_err(), PatternError::Directive("%:Q"));
        assert_eq!(Pattern::compile("%Y-%m-%d").unwrap_err(), PatternError::NoTimeOfDay);
        for text in ["%I:%M:%S", "%J", "%Y-%m-%d %H:%M:%S %"] {
            assert!(
                matches!(Pattern::compile(text), Err(PatternError::RoundTrip(_))),
                "{text}: {:?}",
                Pattern::compile(text)
            );
        }
    }

    #[test]
    fn an_escaped_percent_is_not_a_directive() {
        let p = Pattern::compile("%Y-%m-%d %H:%M:%S %%Z").unwrap();
        assert_eq!(p.kind(), PatternKind::Civil);
    }

    #[test]
    fn an_offset_with_no_year_is_rejected_at_compile() {
        assert!(matches!(Pattern::compile("%b %e %H:%M:%S %z"), Err(PatternError::RoundTrip(_))));
    }

    #[test]
    fn compile_classifies_by_text() {
        let cases = [
            ("%d/%b/%Y:%H:%M:%S %z", PatternKind::Instant),
            ("%Y-%m-%dT%H:%M:%S%:z", PatternKind::Instant),
            ("%s", PatternKind::Instant),
            ("%Y-%m-%d %H:%M:%S", PatternKind::Civil),
            ("%Y-%m-%d %H:%M:%S.%f UTC", PatternKind::Civil),
            ("%Y-%m-%d %H:%M", PatternKind::Civil),
            ("%b %e %H:%M:%S", PatternKind::CivilNoYear),
        ];
        for (text, kind) in cases {
            let p = Pattern::compile(text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(p.kind(), kind, "{text}");
            assert_eq!(p.reads_zone(), kind != PatternKind::Instant, "{text}");
            assert_eq!(p.text(), text);
        }
    }

    // -- rfc3339_lenient --------------------------------------------------------------------------

    #[test]
    fn rfc3339_lenient_forms() {
        let base = at("2026-10-04T12:34:56Z");
        assert_eq!(rfc3339_lenient("2026-10-04T12:34:56Z"), Ok(base));
        assert_eq!(rfc3339_lenient("2026-10-04T14:34:56+02:00"), Ok(base));
        assert_eq!(rfc3339_lenient("2026-10-04T12:34:56.123456789Z"), Ok(base + 123_456_789));
        assert_eq!(rfc3339_lenient("2026-10-04 12:34:56Z"), Ok(base));
        assert_eq!(rfc3339_lenient("2026-10-04T12:34:56z"), Ok(base));
        assert_eq!(rfc3339_lenient("2026-10-04T14:34:56+02:00[Europe/Berlin]"), Ok(base));
    }

    #[test]
    fn rfc3339_lenient_clamps_a_leap_second() {
        assert_eq!(rfc3339_lenient("2016-12-31T23:59:60Z"), Ok(at("2016-12-31T23:59:59Z")),);
    }

    #[test]
    fn rfc3339_lenient_requires_an_offset() {
        assert_eq!(rfc3339_lenient("2026-10-04T12:34:56"), Err(ResolveError::Invalid));
        assert_eq!(
            rfc3339_lenient("2026-10-04T12:34:56[Europe/Berlin]"),
            Err(ResolveError::Invalid)
        );
        assert_eq!(rfc3339_lenient("yesterday"), Err(ResolveError::Invalid));
    }

    /// jiff's `Timestamp` spans about years -9999..=9999 (it stops at 9999-12-30T22:00Z, a day
    /// short, so any civil time in any offset converts); an `i64` of nanoseconds covers 1677-09-21
    /// to 2262-04-11. Between the two is `OutOfRange`, signed; beyond jiff is `Invalid`.
    #[test]
    fn the_i64_boundary_is_out_of_range_by_side() {
        assert_eq!(
            rfc3339_lenient("9999-01-01T00:00:00Z"),
            Err(ResolveError::OutOfRange { past: false })
        );
        assert_eq!(
            rfc3339_lenient("1600-01-01T00:00:00Z"),
            Err(ResolveError::OutOfRange { past: true })
        );
        assert_eq!(rfc3339_lenient("2262-04-11T23:47:16.854775807Z"), Ok(i64::MAX));
        assert_eq!(
            rfc3339_lenient("2262-04-11T23:47:16.854775808Z"),
            Err(ResolveError::OutOfRange { past: false })
        );
        assert_eq!(
            rfc3339_lenient("9999-12-30T22:00:00Z"),
            Err(ResolveError::OutOfRange { past: false })
        );
        assert_eq!(rfc3339_lenient("9999-12-30T22:00:01Z"), Err(ResolveError::Invalid));
        assert_eq!(rfc3339_lenient("9999-12-31T23:59:59Z"), Err(ResolveError::Invalid));
        assert_eq!(rfc3339_lenient("+010000-01-01T00:00:00Z"), Err(ResolveError::Invalid));
    }

    // -- rfc3164 ----------------------------------------------------------------------------------

    #[test]
    fn rfc3164_infers_the_closest_year() {
        let utc = Zone::utc();
        assert_eq!(
            rfc3164("Dec 31 23:59:59", &utc, at("2026-01-01T00:00:05Z")),
            Ok(at("2025-12-31T23:59:59Z"))
        );
        assert_eq!(
            rfc3164("Jan  1 00:00:01", &utc, at("2025-12-31T23:59:58Z")),
            Ok(at("2026-01-01T00:00:01Z"))
        );
        assert_eq!(
            rfc3164("Jun 15 08:00:00", &utc, at("2026-06-15T08:00:30Z")),
            Ok(at("2026-06-15T08:00:00Z"))
        );
    }

    #[test]
    fn rfc3164_feb_29_needs_a_leap_candidate() {
        let utc = Zone::utc();
        assert_eq!(
            rfc3164("Feb 29 12:00:00", &utc, at("2028-03-01T00:00:00Z")),
            Ok(at("2028-02-29T12:00:00Z"))
        );
        assert_eq!(
            rfc3164("Feb 29 12:00:00", &utc, at("2026-06-01T00:00:00Z")),
            Err(ResolveError::Invalid)
        );
        assert_eq!(
            rfc3164("Feb 29 12:00:00", &utc, at("2025-01-15T00:00:00Z")),
            Ok(at("2024-02-29T12:00:00Z"))
        );
    }

    #[test]
    fn rfc3164_takes_the_receipt_year_in_the_zone() {
        let la = zone("America/Los_Angeles");
        // Receipt is 2025-12-31 19:00 PST: the stamp is an hour later, the same local day.
        assert_eq!(
            rfc3164("Dec 31 20:00:00", &la, at("2026-01-01T03:00:00Z")),
            Ok(at("2026-01-01T04:00:00Z"))
        );
        // Receipt is 2029-12-31 19:00 PST, so the candidates are 2028..=2030 and the leap day
        // 2028-02-29 resolves. Taken in UTC (2030), the candidates would be 2029..=2031, none a
        // leap year.
        assert_eq!(
            rfc3164("Feb 29 12:00:00", &la, at("2030-01-01T03:00:00Z")),
            Ok(at("2028-02-29T20:00:00Z"))
        );
        assert_eq!(
            rfc3164("Feb 29 12:00:00", &Zone::utc(), at("2030-01-01T03:00:00Z")),
            Err(ResolveError::Invalid)
        );
    }

    #[test]
    fn a_dst_fold_picks_the_occurrence_closest_to_receipt() {
        let ny = zone("America/New_York");
        // 2026-11-01 01:30 happens at 05:30Z (EDT) and again at 06:30Z (EST).
        assert_eq!(
            rfc3164("Nov  1 01:30:00", &ny, at("2026-11-01T05:45:00Z")),
            Ok(at("2026-11-01T05:30:00Z"))
        );
        assert_eq!(
            rfc3164("Nov  1 01:30:00", &ny, at("2026-11-01T06:45:00Z")),
            Ok(at("2026-11-01T06:30:00Z"))
        );
        // Equidistant: the earlier.
        assert_eq!(
            rfc3164("Nov  1 01:30:00", &ny, at("2026-11-01T06:00:00Z")),
            Ok(at("2026-11-01T05:30:00Z"))
        );
    }

    #[test]
    fn a_dst_gap_reads_with_the_offset_before_it() {
        let ny = zone("America/New_York");
        // 2026-03-08 02:30 doesn't exist; read at EST (-05:00) it is 07:30Z, 03:30 EDT.
        assert_eq!(
            rfc3164("Mar  8 02:30:00", &ny, at("2026-03-08T08:00:00Z")),
            Ok(at("2026-03-08T07:30:00Z"))
        );
    }

    #[test]
    fn rfc3164_shape() {
        let utc = Zone::utc();
        let receipt = at("2026-10-04T12:00:00Z");
        assert_eq!(rfc3164("Oct 04 11:00:00", &utc, receipt), Ok(at("2026-10-04T11:00:00Z")));
        assert_eq!(rfc3164("Oct  4 11:00:00", &utc, receipt), Ok(at("2026-10-04T11:00:00Z")));
        assert_eq!(rfc3164("oct  4 11:00:00", &utc, receipt), Ok(at("2026-10-04T11:00:00Z")));
        for bad in [
            "Oct 4 11:00:00",
            "Oct  4 11:00:000",
            "Foo  4 11:00:00",
            "Feb 30 11:00:00",
            "Oct 00 11:00:00",
            "Oct 32 11:00:00",
            "Oct  4 24:00:00",
            "Oct  4 11:60:00",
            "Oct  4 11:00:60",
            "Oct  4 1a:00:00",
            "Oct  4T11:00:00",
            "Oct 4  11:00:00",
            "",
        ] {
            assert_eq!(rfc3164(bad, &utc, receipt), Err(ResolveError::Invalid), "{bad:?}");
        }
    }

    // -- pattern ----------------------------------------------------------------------------------

    #[test]
    fn nginx_time_local() {
        let p = Pattern::compile("%d/%b/%Y:%H:%M:%S %z").unwrap();
        assert_eq!(
            pattern(&p, "10/Oct/2000:13:55:36 -0700", &zone("Europe/Berlin"), 0),
            Ok(at("2000-10-10T20:55:36Z"))
        );
    }

    #[test]
    fn a_civil_pattern_reads_in_the_zone() {
        let p = Pattern::compile("%Y-%m-%d %H:%M:%S.%f UTC").unwrap();
        let s = "2026-10-04 12:34:56.789 UTC";
        assert_eq!(pattern(&p, s, &Zone::utc(), 0), Ok(at("2026-10-04T12:34:56.789Z")));
        assert_eq!(pattern(&p, s, &zone("Europe/Berlin"), 0), Ok(at("2026-10-04T10:34:56.789Z")));
    }

    #[test]
    fn a_yearless_pattern_infers_the_year_and_accepts_a_space_padded_day() {
        let p = Pattern::compile("%b %e %H:%M:%S").unwrap();
        assert_eq!(
            pattern(&p, "Oct  4 12:00:00", &Zone::utc(), at("2026-10-04T12:00:05Z")),
            Ok(at("2026-10-04T12:00:00Z"))
        );
        assert_eq!(
            pattern(&p, "Dec 31 23:59:59", &Zone::utc(), at("2027-01-01T00:00:05Z")),
            Ok(at("2026-12-31T23:59:59Z"))
        );
    }

    #[test]
    fn a_pattern_must_match_the_whole_value() {
        let p = Pattern::compile("%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(
            pattern(&p, "2026-10-04 12:00:00 extra", &Zone::utc(), 0),
            Err(ResolveError::Invalid)
        );
        assert_eq!(pattern(&p, "2026-10-04", &Zone::utc(), 0), Err(ResolveError::Invalid));
        assert_eq!(pattern(&p, "2026-02-30 12:00:00", &Zone::utc(), 0), Err(ResolveError::Invalid));
    }

    #[test]
    fn an_epoch_seconds_pattern() {
        let p = Pattern::compile("%s").unwrap();
        assert_eq!(pattern(&p, "1759579200", &Zone::utc(), 0), Ok(1_759_579_200_000_000_000));
    }

    #[test]
    fn a_pattern_out_of_i64_range_is_signed() {
        let p = Pattern::compile("%Y-%m-%dT%H:%M:%S%:z").unwrap();
        assert_eq!(
            pattern(&p, "2300-01-01T00:00:00+00:00", &Zone::utc(), 0),
            Err(ResolveError::OutOfRange { past: false })
        );
    }
}
