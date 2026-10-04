//! `timestamp`: resolves `event.timestamp` from one attribute, so a replayed backlog or a tailed
//! file carries the record's own time instead of receipt time. See
//! `docs/adr/timestamp-transform.md`; calendar work lives in `logit_core::zoned`.
//!
//! What each attribute value resolves to under each [`TimestampFormat`] (`-` is `invalid`):
//!
//! | Value | `unix_seconds` | `unix_millis`/`_micros`/`_nanos` | `rfc3339` | `rfc3164` | `pattern` |
//! |---|---|---|---|---|---|
//! | `Timestamp(n)` | `n` | `n` | `n` | `n` | `n` |
//! | `Str` | decimal, fraction digit-exact | decimal, fraction digit-exact | `zoned::rfc3339_lenient` | `zoned::rfc3164` | `zoned::pattern` |
//! | `I64`, `U64` | scaled, checked | scaled, checked | - | - | - |
//! | `F64` | `f64` seconds, fraction rounded to the nanosecond | - | - | - | - |
//! | anything else | - | - | - | - | - |
//!
//! A scaled integer or decimal string that overflows an `i64` of nanoseconds, and a calendar value outside it, is a
//! skew skip on its side, not `invalid`: it parsed, and it's far from receipt.
//!
//! Per event, in order; the first that applies is the outcome:
//!
//! 1. The event carries a span: `skipped{reason="span"}`. Its timestamp is the span's start,
//!    which `trace_context` owns.
//! 2. The attribute is absent, `Null`, `""`, or `"-"`: `missing`.
//! 3. The value doesn't resolve under the table: `invalid`.
//! 4. The instant is more than `max_skew` before or after the event's current timestamp:
//!    `skew_past` or `skew_future`. An instant `max_skew` away, no further, applies.
//! 5. A metric's non-zero `start_timestamp` is later than the instant: `start`.
//! 6. Otherwise the instant becomes `event.timestamp`, and a log whose `observed_timestamp` is 0
//!    takes the previous `event.timestamp` there. A metric-only event applies too.
//!
//! A skip forwards the event untouched; nothing is ever dropped. On apply the source attribute is
//! removed unless `keep_source`. That matters to `syslog_out`: a kept `syslog.timestamp` is
//! written verbatim on RFC 3164, while a removed one is re-rendered from `event.timestamp` in UTC.
//!
//! Counters, tallied per event and emitted from `end_batch` for non-zero cells only:
//! `logit.transform.timestamp.resolved` and `logit.transform.timestamp.skipped{reason}`.

use crate::{f64_seconds_to_nanos, present_value};
use logit_core::interner::intern;
use logit_core::zoned::{self, Pattern, ResolveError, Zone};
use logit_core::{parse_decimal_nanos, DecimalError, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;
use std::time::Duration;

/// How the source attribute's value is read. Mirrors `logit_config::TimestampFormat`, with a
/// pattern already compiled; `logit-cli` converts.
#[derive(Clone, Debug)]
pub enum TimestampFormat {
    Rfc3339,
    Rfc3164,
    UnixSeconds,
    UnixMillis,
    UnixMicros,
    UnixNanos,
    Pattern(Pattern),
}

impl TimestampFormat {
    /// Nanoseconds per unit for the `unix_*` formats.
    fn unix_scale(&self) -> Option<i64> {
        match self {
            TimestampFormat::UnixSeconds => Some(1_000_000_000),
            TimestampFormat::UnixMillis => Some(1_000_000),
            TimestampFormat::UnixMicros => Some(1_000),
            TimestampFormat::UnixNanos => Some(1),
            _ => None,
        }
    }
}

/// Why an event wasn't resolved: the `reason` tag, and an index into [`Tally::skipped`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Skip {
    Missing = 0,
    Invalid = 1,
    SkewPast = 2,
    SkewFuture = 3,
    Span = 4,
    Start = 5,
}

impl Skip {
    const ALL: [Skip; 6] =
        [Skip::Missing, Skip::Invalid, Skip::SkewPast, Skip::SkewFuture, Skip::Span, Skip::Start];

    fn reason(self) -> &'static str {
        match self {
            Skip::Missing => "missing",
            Skip::Invalid => "invalid",
            Skip::SkewPast => "skew_past",
            Skip::SkewFuture => "skew_future",
            Skip::Span => "span",
            Skip::Start => "start",
        }
    }

    fn from_resolve(e: ResolveError) -> Skip {
        match e {
            ResolveError::Invalid => Skip::Invalid,
            ResolveError::OutOfRange { past: true } => Skip::SkewPast,
            ResolveError::OutOfRange { past: false } => Skip::SkewFuture,
        }
    }
}

/// Per-batch outcome counts, emitted and reset by `end_batch`.
#[derive(Default)]
struct Tally {
    resolved: u64,
    skipped: [u64; 6],
}

pub struct TimestampResolver {
    from: Symbol,
    format: TimestampFormat,
    zone: Zone,
    max_skew_nanos: i128,
    keep_source: bool,
    telemetry: Telemetry,
    tally: Tally,
}

impl TimestampResolver {
    /// Builds a resolver; graph validation has already checked every argument.
    pub fn new(
        from: &str,
        format: TimestampFormat,
        zone: Zone,
        max_skew: Duration,
        keep_source: bool,
    ) -> Self {
        Self {
            from: intern(from),
            format,
            zone,
            max_skew_nanos: i128::try_from(max_skew.as_nanos()).unwrap_or(i128::MAX),
            keep_source,
            telemetry: Telemetry::default(),
            tally: Tally::default(),
        }
    }

    /// Attaches a telemetry handle. There's no `Diagnostics` builder: every skip is expected
    /// traffic, counted.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// The table in the module doc: `value` under the configured format, in unix nanoseconds.
    fn resolve(&self, value: &Value, receipt: i64) -> Result<i64, Skip> {
        if let Value::Timestamp(n) = value {
            return Ok(*n);
        }
        if let Some(scale) = self.format.unix_scale() {
            return match value {
                Value::Str(_) => {
                    let s = value.as_str().ok_or(Skip::Invalid)?;
                    parse_decimal_nanos(s, scale).map_err(|e| match e {
                        // The grammar has no sign, so an overflow is always in the future.
                        DecimalError::Overflow => Skip::SkewFuture,
                        DecimalError::Invalid => Skip::Invalid,
                    })
                }
                Value::I64(n) => n.checked_mul(scale).ok_or(if *n < 0 {
                    Skip::SkewPast
                } else {
                    Skip::SkewFuture
                }),
                Value::U64(n) => i64::try_from(*n)
                    .ok()
                    .and_then(|n| n.checked_mul(scale))
                    .ok_or(Skip::SkewFuture),
                Value::F64(f) if scale == 1_000_000_000 => {
                    f64_seconds_to_nanos(*f).ok_or(if !f.is_finite() {
                        Skip::Invalid
                    } else if *f < 0.0 {
                        Skip::SkewPast
                    } else {
                        Skip::SkewFuture
                    })
                }
                _ => Err(Skip::Invalid),
            };
        }
        let s = match value {
            Value::Str(_) => value.as_str().ok_or(Skip::Invalid)?,
            _ => return Err(Skip::Invalid),
        };
        match &self.format {
            TimestampFormat::Rfc3339 => zoned::rfc3339_lenient(s),
            TimestampFormat::Rfc3164 => zoned::rfc3164(s, &self.zone, receipt),
            TimestampFormat::Pattern(p) => zoned::pattern(p, s, &self.zone, receipt),
            _ => unreachable!("unix formats returned above"),
        }
        .map_err(Skip::from_resolve)
    }

    /// Steps 1 through 5 of the module doc's order, without touching the event.
    fn decide(&self, event: &Event) -> Result<i64, Skip> {
        if event.span.is_some() {
            return Err(Skip::Span);
        }
        let value = present_value(event.attributes.get_sym(self.from)).ok_or(Skip::Missing)?;
        let receipt = event.timestamp;
        let n = self.resolve(value, receipt)?;
        let delta = i128::from(n) - i128::from(receipt);
        if delta > self.max_skew_nanos {
            return Err(Skip::SkewFuture);
        }
        if delta < -self.max_skew_nanos {
            return Err(Skip::SkewPast);
        }
        if event.metrics.iter().any(|m| m.start_timestamp != 0 && m.start_timestamp > n) {
            return Err(Skip::Start);
        }
        Ok(n)
    }
}

impl Transform for TimestampResolver {
    /// Applies a resolved instant or counts one skip; always forwards the event.
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        match self.decide(event) {
            Ok(n) => {
                let prior = event.timestamp;
                event.timestamp = n;
                if let Some(log) = &mut event.log {
                    if log.observed_timestamp == 0 {
                        log.observed_timestamp = prior;
                    }
                }
                if !self.keep_source {
                    event.attributes.remove_sym(self.from);
                }
                self.tally.resolved += 1;
            }
            Err(skip) => self.tally.skipped[skip as usize] += 1,
        }
        true
    }

    fn end_batch(&mut self) {
        if self.tally.resolved > 0 {
            self.telemetry.count(
                "logit.transform.timestamp.resolved",
                self.tally.resolved as f64,
                &[],
            );
        }
        for skip in Skip::ALL {
            let n = self.tally.skipped[skip as usize];
            if n > 0 {
                self.telemetry.count(
                    "logit.transform.timestamp.skipped",
                    n as f64,
                    &[("reason", skip.reason())],
                );
            }
        }
        self.tally = Tally::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        parse_rfc3339_to_nanos, AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, Registry,
        SpanKind, SpanRecord, SpanStatus,
    };

    const RECEIPT: i64 = 1_759_579_200_000_000_000; // 2025-10-04T12:00:00Z
    const DAY: Duration = Duration::from_secs(86_400);
    const DAY_NANOS: i64 = 86_400_000_000_000;

    fn at(s: &str) -> i64 {
        parse_rfc3339_to_nanos(s).unwrap()
    }

    fn log_at(receipt: i64, pairs: &[(&str, Value)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, v.clone());
        }
        Event::log(
            receipt,
            attrs,
            LogRecord {
                message: Value::str("msg"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn log_with(value: Value) -> Event {
        log_at(RECEIPT, &[("ts", value)])
    }

    fn resolver(format: TimestampFormat) -> TimestampResolver {
        TimestampResolver::new("ts", format, Zone::utc(), DAY, false)
    }

    fn resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    /// Runs one event through a fresh resolver and returns the event and its one outcome.
    fn run(format: TimestampFormat, mut event: Event) -> (Event, Result<i64, &'static str>) {
        let mut t = resolver(format);
        let before = event.clone();
        let outcome = t.decide(&event).map_err(Skip::reason);
        assert!(t.process(&resource(), &mut event));
        if outcome.is_err() {
            // Compared as Debug text so a NaN attribute equals itself.
            assert_eq!(
                format!("{event:?}"),
                format!("{before:?}"),
                "a skip leaves the event as it arrived"
            );
        }
        (event, outcome)
    }

    fn outcome(format: TimestampFormat, value: Value) -> Result<i64, &'static str> {
        run(format, log_with(value)).1
    }

    fn pattern(text: &str) -> TimestampFormat {
        TimestampFormat::Pattern(Pattern::compile(text).unwrap())
    }

    // -- The value x format table -----------------------------------------------------------------

    #[test]
    fn a_timestamp_value_passes_through_under_every_format() {
        let n = RECEIPT - 5;
        for format in [
            TimestampFormat::UnixSeconds,
            TimestampFormat::UnixMillis,
            TimestampFormat::UnixNanos,
            TimestampFormat::Rfc3339,
            TimestampFormat::Rfc3164,
            pattern("%Y-%m-%d %H:%M:%S"),
        ] {
            assert_eq!(outcome(format.clone(), Value::Timestamp(n)), Ok(n), "{format:?}");
        }
    }

    #[test]
    fn strings_under_each_format() {
        let base = RECEIPT - 1_500_000_000; // 11:59:58.5
        let cases = [
            (TimestampFormat::UnixSeconds, "1759579198.5"),
            (TimestampFormat::UnixMillis, "1759579198500"),
            (TimestampFormat::UnixMicros, "1759579198500000"),
            (TimestampFormat::UnixNanos, "1759579198500000000"),
            (TimestampFormat::Rfc3339, "2025-10-04T11:59:58.5Z"),
            (pattern("%Y-%m-%d %H:%M:%S%.f"), "2025-10-04 11:59:58.5"),
        ];
        for (format, s) in cases {
            assert_eq!(outcome(format.clone(), Value::str(s)), Ok(base), "{format:?} {s}");
        }
        assert_eq!(
            outcome(TimestampFormat::Rfc3164, Value::str("Oct  4 11:59:58")),
            Ok(base - 500_000_000)
        );
        for format in [
            TimestampFormat::UnixSeconds,
            TimestampFormat::Rfc3339,
            TimestampFormat::Rfc3164,
            pattern("%Y-%m-%d %H:%M:%S"),
        ] {
            assert_eq!(outcome(format.clone(), Value::str("nope")), Err("invalid"), "{format:?}");
        }
    }

    #[test]
    fn integers_under_each_unix_format() {
        let secs = RECEIPT / 1_000_000_000;
        let cases = [
            (TimestampFormat::UnixSeconds, secs),
            (TimestampFormat::UnixMillis, secs * 1_000),
            (TimestampFormat::UnixMicros, secs * 1_000_000),
            (TimestampFormat::UnixNanos, RECEIPT),
        ];
        for (format, n) in cases {
            assert_eq!(outcome(format.clone(), Value::I64(n)), Ok(RECEIPT), "{format:?}");
            assert_eq!(outcome(format.clone(), Value::U64(n as u64)), Ok(RECEIPT), "{format:?}");
        }
        assert_eq!(outcome(TimestampFormat::Rfc3339, Value::I64(secs)), Err("invalid"));
        assert_eq!(outcome(TimestampFormat::Rfc3164, Value::I64(secs)), Err("invalid"));
        assert_eq!(outcome(pattern("%s"), Value::I64(secs)), Err("invalid"));
    }

    #[test]
    fn an_integer_overflow_is_skew_by_sign() {
        assert_eq!(
            outcome(TimestampFormat::UnixSeconds, Value::I64(i64::MAX / 10)),
            Err("skew_future")
        );
        assert_eq!(
            outcome(TimestampFormat::UnixSeconds, Value::I64(i64::MIN / 10)),
            Err("skew_past")
        );
        assert_eq!(outcome(TimestampFormat::UnixNanos, Value::U64(u64::MAX)), Err("skew_future"));
        assert_eq!(
            outcome(TimestampFormat::UnixNanos, Value::U64(i64::MAX as u64 + 1)),
            Err("skew_future")
        );
        assert_eq!(
            outcome(TimestampFormat::Rfc3339, Value::str("2300-01-01T00:00:00Z")),
            Err("skew_future")
        );
    }

    #[test]
    fn floats_only_as_unix_seconds() {
        assert_eq!(
            outcome(TimestampFormat::UnixSeconds, Value::F64(1_759_579_199.25)),
            Ok(RECEIPT - 750_000_000)
        );
        for format in [
            TimestampFormat::UnixMillis,
            TimestampFormat::UnixMicros,
            TimestampFormat::UnixNanos,
            TimestampFormat::Rfc3339,
        ] {
            assert_eq!(outcome(format.clone(), Value::F64(1.0e12)), Err("invalid"), "{format:?}");
        }
        assert_eq!(outcome(TimestampFormat::UnixSeconds, Value::F64(f64::NAN)), Err("invalid"));
        assert_eq!(
            outcome(TimestampFormat::UnixSeconds, Value::F64(f64::INFINITY)),
            Err("invalid")
        );
        assert_eq!(outcome(TimestampFormat::UnixSeconds, Value::F64(-1.0e300)), Err("skew_past"));
        assert_eq!(outcome(TimestampFormat::UnixSeconds, Value::F64(1.0e300)), Err("skew_future"));
    }

    #[test]
    fn other_value_types_are_invalid() {
        for value in [Value::Bool(true), Value::Bytes(bytes::Bytes::from_static(b"1759579200"))] {
            assert_eq!(outcome(TimestampFormat::UnixSeconds, value.clone()), Err("invalid"));
            assert_eq!(outcome(TimestampFormat::Rfc3339, value), Err("invalid"));
        }
    }

    #[test]
    fn missing_spellings() {
        let (_, absent) = run(TimestampFormat::Rfc3339, log_at(RECEIPT, &[]));
        assert_eq!(absent, Err("missing"));
        for value in [Value::Null, Value::str(""), Value::str("-")] {
            assert_eq!(outcome(TimestampFormat::Rfc3339, value), Err("missing"));
        }
    }

    // -- Skew -------------------------------------------------------------------------------------

    #[test]
    fn max_skew_away_applies_and_one_nanosecond_more_skips() {
        let f = TimestampFormat::UnixNanos;
        assert_eq!(outcome(f.clone(), Value::I64(RECEIPT + DAY_NANOS)), Ok(RECEIPT + DAY_NANOS));
        assert_eq!(outcome(f.clone(), Value::I64(RECEIPT - DAY_NANOS)), Ok(RECEIPT - DAY_NANOS));
        assert_eq!(outcome(f.clone(), Value::I64(RECEIPT + DAY_NANOS + 1)), Err("skew_future"));
        assert_eq!(outcome(f, Value::I64(RECEIPT - DAY_NANOS - 1)), Err("skew_past"));
    }

    // -- What the event carries -------------------------------------------------------------------

    #[test]
    fn a_span_event_is_left_alone() {
        let mut event = Event::span(
            RECEIPT,
            AttrMap::new(),
            SpanRecord {
                trace_id: [1; 16],
                span_id: [1; 8],
                parent_span_id: None,
                name: Value::str("op"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: vec![],
                links: vec![],
                end_timestamp: RECEIPT,
                flags: 0,
                ext: None,
            },
        );
        event.attributes.insert("ts", Value::I64(RECEIPT - 10));
        let (_, result) = run(TimestampFormat::UnixNanos, event);
        assert_eq!(result, Err("span"));
    }

    fn metric_event(start: i64) -> Event {
        let mut record = MetricRecord::new(intern("m"), MetricKind::counter(1.0));
        record.start_timestamp = start;
        let mut attrs = AttrMap::new();
        attrs.insert("ts", Value::I64(RECEIPT - 1_000));
        Event::metric(RECEIPT, attrs, record)
    }

    #[test]
    fn a_metric_only_event_applies_unless_its_start_is_later() {
        let (event, result) = run(TimestampFormat::UnixNanos, metric_event(0));
        assert_eq!(result, Ok(RECEIPT - 1_000));
        assert_eq!(event.timestamp, RECEIPT - 1_000);
        assert!(event.attributes.get("ts").is_none());

        let (_, result) = run(TimestampFormat::UnixNanos, metric_event(RECEIPT - 2_000));
        assert_eq!(result, Ok(RECEIPT - 1_000), "a start at or before the instant is fine");
        let (_, result) = run(TimestampFormat::UnixNanos, metric_event(RECEIPT - 1_000));
        assert_eq!(result, Ok(RECEIPT - 1_000));
        let (_, result) = run(TimestampFormat::UnixNanos, metric_event(RECEIPT - 999));
        assert_eq!(result, Err("start"));
    }

    #[test]
    fn observed_timestamp_takes_the_prior_timestamp_only_when_unset() {
        let (event, _) = run(TimestampFormat::UnixNanos, log_with(Value::I64(RECEIPT - 7)));
        assert_eq!(event.timestamp, RECEIPT - 7);
        assert_eq!(event.log.unwrap().observed_timestamp, RECEIPT);

        let mut preset = log_with(Value::I64(RECEIPT - 7));
        preset.log.as_mut().unwrap().observed_timestamp = 42;
        let (event, _) = run(TimestampFormat::UnixNanos, preset);
        assert_eq!(event.log.unwrap().observed_timestamp, 42);
    }

    #[test]
    fn keep_source_decides_whether_the_attribute_stays() {
        let mut keep =
            TimestampResolver::new("ts", TimestampFormat::UnixNanos, Zone::utc(), DAY, true);
        let mut event = log_with(Value::I64(RECEIPT - 7));
        keep.process(&resource(), &mut event);
        assert_eq!(event.timestamp, RECEIPT - 7);
        assert_eq!(event.attributes.get("ts"), Some(&Value::I64(RECEIPT - 7)));

        let (event, _) = run(TimestampFormat::UnixNanos, log_with(Value::I64(RECEIPT - 7)));
        assert!(event.attributes.get("ts").is_none());
    }

    #[test]
    fn rfc3164_in_a_dst_zone_end_to_end() {
        let mut t = TimestampResolver::new(
            "syslog.timestamp",
            TimestampFormat::Rfc3164,
            Zone::parse("America/New_York").unwrap(),
            DAY,
            false,
        );
        // Received half an hour after midnight UTC on New Year's Day: still Dec 31 in New York.
        let receipt = at("2026-01-01T00:30:00Z");
        let mut event = log_at(receipt, &[("syslog.timestamp", Value::str("Dec 31 19:20:00"))]);
        t.process(&resource(), &mut event);
        assert_eq!(event.timestamp, at("2026-01-01T00:20:00Z"));
        assert_eq!(event.log.unwrap().observed_timestamp, receipt);
    }

    // -- Telemetry --------------------------------------------------------------------------------

    fn counters(registry: &Registry) -> Vec<(String, Option<String>, f64)> {
        let mut out = Vec::new();
        for e in registry.drain(0) {
            for m in &e.metrics {
                if let MetricKind::Sum(sum) = &m.kind {
                    let name = logit_core::interner::resolve(m.name).to_string();
                    if name.starts_with("logit.transform.timestamp.") {
                        let reason =
                            e.attributes.get("reason").and_then(|v| v.as_str()).map(String::from);
                        out.push((name, reason, sum.value));
                    }
                }
            }
        }
        out.sort_by(|a, b| a.partial_cmp(b).unwrap());
        out
    }

    #[test]
    fn end_batch_emits_non_zero_cells_once() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("resolved", "timestamp", "transform");
        let mut t = resolver(TimestampFormat::UnixNanos).with_telemetry(telemetry);
        let r = resource();
        for event in [
            log_with(Value::I64(RECEIPT - 1)),
            log_with(Value::I64(RECEIPT - 2)),
            log_with(Value::Null),
            log_with(Value::Bool(true)),
            log_with(Value::I64(RECEIPT + 2 * DAY_NANOS)),
        ] {
            let mut event = event;
            t.process(&r, &mut event);
        }
        assert!(counters(&registry).is_empty(), "nothing before end_batch");
        t.end_batch();
        let skipped = "logit.transform.timestamp.skipped".to_string();
        assert_eq!(
            counters(&registry),
            vec![
                ("logit.transform.timestamp.resolved".to_string(), None, 2.0),
                (skipped.clone(), Some("invalid".into()), 1.0),
                (skipped.clone(), Some("missing".into()), 1.0),
                (skipped, Some("skew_future".into()), 1.0),
            ]
        );
        t.end_batch();
        assert!(counters(&registry).is_empty(), "an empty batch emits nothing");
    }
}
