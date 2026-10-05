---
created: 2026-10-04
updated: 2026-10-04
---

# jiff is the one calendar implementation, behind the codecs' strict RFC 5424 contract

## Status
Accepted. Carries out [ADR `timestamp-transform`](timestamp-transform.md)'s note that the
hand-rolled calendar code in `logit_core::time` moves onto jiff in its own record.

## Context
Before this record, `logit` converted between civil dates and instants in three places, each a
copy of Howard Hinnant's civil-date algorithms: `logit_core::time` (the RFC 3339 render and a
hand-rolled RFC 3339 parser), `syslog_out`'s RFC 3164 render, and `logit-perf`'s result
timestamps. `logit-config` parsed `Duration` fields with its own four-unit codec (`ms`, `s`, `m`,
`h`, through an `f64`), marked with a `TODO` to replace it with a crate.

[ADR `timestamp-transform`](timestamp-transform.md) added jiff to `logit-core` for time zones and
strftime patterns. With jiff already in the build, the copies are three implementations of what one
dependency does, and a fix to one of them doesn't reach the others.

The RFC 3339 parser is not a general one. `syslog_in`, `docker_in`, `datadog_in`, and
`trace_context`'s `span.*_rfc3339` all read it, and the lossless relays' test vectors pin its
contract: RFC 5424 §6.2.3's subset of RFC 3339, with a nonexistent date or a `:60` leap second
rejected rather than normalized. jiff's own RFC 3339 parser is wider: it accepts a space separator,
a lowercase `z`, and an RFC 9557 `[zone]` suffix, and it clamps `:60` to `:59`.

## Decision
1. **jiff does all calendar math.** Every conversion between a civil date and an instant, and every
   calendar render, goes through jiff. `logit_core::time` keeps its public functions and
   signatures (`parse_rfc3339_to_nanos`, `format_rfc3339_utc`, `write_rfc3339_utc`,
   `TimestampError`) and adds `write_rfc3164_utc`, which `syslog_out` calls. `logit-perf` renders
   through `format_rfc3339_utc`. No copy of the civil-date algorithms remains.
2. **The RFC 3339 parser keeps its strict contract as a wrapper.** Byte tests fix the grammar:
   `YYYY-MM-DDTHH:MM:SS`, an optional `.` and 1-9 fractional digits, then an uppercase `Z` or
   `±HH:MM` with hours `00..=23`, and nothing after. jiff's `civil::DateTime::new` validates the
   calendar, so a nonexistent date, hour `24`, and second `60` are `Malformed`. The instant is the
   civil time's exact distance from the epoch, minus the offset; a value outside an `i64` of
   nanoseconds is `OutOfRange`. That includes dates jiff's own `Timestamp` can't hold, such as
   `9999-12-31T23:59:59Z` and `0000-01-01T00:00:00+23:59`.
3. **Renders never fail.** `write_rfc3339_utc` prints nine fractional digits and `Z` through jiff's
   `DateTimePrinter`; `write_rfc3164_utc` prints `%b %e %H:%M:%S` through `BrokenDownTime`. Both
   write into the caller's buffer with no allocation of their own, and every `i64` of nanoseconds
   (1677-09-21..2262-04-11) is inside jiff's range, so neither can panic.
4. **`Duration` config fields use jiff's friendly duration format, minus calendar units.**
   - Accepted: one or more `<number><unit>` designators, with or without spaces between them
     (`10s`, `1h30m`, `1h 30m`, `90 seconds`). Units are `ns`, `us`, `ms`, `s`, `m`/`min`, `h`,
     and `d`, plus jiff's long labels for each. A `d` is 24 hours. The smallest unit may carry a
     fraction of 1-9 digits when it is hours or smaller (`1.5s`, `0.5h`). The codec trims
     surrounding whitespace.
   - Rejected: `w`, `mo`, and `y` (their length depends on the calendar), a negative value (`-5s`,
     `5s ago`), and a number with no unit. Zero stays the codec's to accept and a field's own rule
     to reject.
   - Serialized in compact designator form with no spaces (`10s`, `100ms`, `1h30m`, `48h`), which
     is what the published schema's defaults show.
5. **What stays hand-rolled.** Digit-exact unit scaling isn't calendar math, and jiff has no parse
   from a decimal string to integer nanoseconds that avoids an `f64`. These stay as they are:
   `logit_core::time::parse_decimal_nanos`, the `f64` seconds conversion in `logit-transforms`, the
   Datadog and Splunk codecs' time helpers in `logit-proto`, graphite's second/fraction split, and
   collectd's `cdtime`.

## Alternatives considered
- **Keep the hand-rolled code.** Rejected: three copies of the same algorithm to keep in step, beside
  a dependency that already does the work and is tested far more widely.
- **Let the codecs use jiff's RFC 3339 parser directly.** Rejected: it accepts forms RFC 5424
  forbids and clamps a leap second, so `syslog_in` would accept and rewrite timestamps it rejects
  today, and the lossless relays' test vectors would change. That leniency belongs to the
  `timestamp` transform's `format: rfc3339`, which reads application logs.
- **The `humantime` crate for durations.** Rejected: a second time crate for one codec, when jiff's
  friendly format covers the same grammar and more.
- **Accept weeks, months, and years as fixed lengths.** Rejected: a month or a year has no fixed
  length, and a timeout or interval that silently picks one is a surprise. Weeks are rejected with
  them so that every accepted unit up to `d` is one an operator reads the same way.

## Consequences
- `logit-config` depends on jiff.
- A duration such as `2d`, `1h30m`, `90 seconds`, or `250us` is now valid config. The previous
  codec's `.5s` and `5.s` (a fraction with no leading or trailing digit) and fractions longer than
  nine digits are no longer accepted.
- A default in `schema/logit.schema.json` renders in the largest units that divide it (`1m` rather
  than `60s`, `100ms` rather than `0.1s`).
- The `TODO`s in `logit_core::time` and `logit-config`'s duration codec are closed.
- `crate::zoned` and `crate::time` are the two modules that name jiff types in `logit-core`; no jiff
  type appears in `logit-core`'s public API.
