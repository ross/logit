//! `trace_context`: lifts a trace/span reference off an event's attributes onto its `LogRecord`
//! (`docs/adr/log-record-trace-context.md`). With a `span:` block it also turns an access-log line
//! into a `SpanRecord` on the same event, whose start becomes the event's timestamp
//! (`docs/adr/trace-context-span-lifting.md`), so a request through haproxy -> nginx -> app shows
//! up as one trace with a span per tier.
//!
//! Reads the attribute names in `docs/design/data-model.md`'s "Well-known attribute names"
//! section: `traceparent`, `trace.id`/`trace.flags`/`span.id` (the three renameable ones),
//! `span.parent_id`, `span.name`, `span.kind`, `span.status`, and `span.start`/`span.end`/
//! `span.duration` in integer nanoseconds or a unit-suffixed form (`_us`/`_ms`/`_s`/`_rfc3339`).
//!
//! [`IdFormat`] picks the grammar of the trace and span id attributes, and nothing else (the
//! flags stay decimal 0-255 under either):
//!
//! | Attribute | `Otel` (default) | `Datadog` |
//! |---|---|---|
//! | trace id (`trace.id` / `dd.trace_id`) | `Str`, 32 hex | `Str`, a decimal uint64 (low 64 bits, high zero) or 32 hex; `U64`, or an `I64` above 0, as the same uint64 |
//! | span id (`span.id` / `dd.span_id`) | `Str`, 16 hex | `Str` decimal uint64, `U64`, or an `I64` above 0 |
//! | high half (`trace_id_high`, unset by default) | -- | `Str`, 1 to 16 hex (`_dd.p.tid`'s form), applied only when the trace id's high half is zero |
//!
//! A 16-digit string is therefore hex under `Otel` and decimal under `Datadog`: the configured
//! format decides, never the value. `traceparent` and `span.parent_id` are W3C hex under either
//! format, and 16 hex is never a Datadog id. See `docs/adr/log-record-trace-context.md`'s
//! Datadog amendment.
//!
//! All-or-nothing: everything is parsed before anything is mutated, so a lift either applies
//! completely or leaves the event as it arrived and counts one `.skipped{reason}`.

use logit_core::trace::{
    parse_span_id, parse_span_id_datadog, parse_trace_id, parse_trace_id_datadog,
    parse_trace_id_high, trace_id_bytes, trace_id_halves,
};
use logit_core::{
    parse_decimal_nanos, parse_rfc3339_to_nanos, parse_traceparent, random_id_bytes, AttrMap,
    Event, Resource, SpanKind, SpanRecord, SpanStatus, Telemetry, TraceRef, Value,
};
use logit_pipeline::Transform;
use std::sync::Arc;
use std::time::Duration;

/// The W3C header as a tier received it: the trace id, this span's parent id, and the flags,
/// each overridden by its explicit attribute.
const TRACEPARENT: &str = "traceparent";
const SPAN_PARENT_ID: &str = "span.parent_id";
const SPAN_NAME: &str = "span.name";
const SPAN_KIND: &str = "span.kind";
const SPAN_STATUS: &str = "span.status";

/// A timing attribute's unit, which only the attribute's name carries, never the value. The base
/// form is integer nanoseconds (OTLP's unit); a suffix labels a coarser source clock (haproxy's
/// `request_date(us)`, nginx's `$msec`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Nanos,
    Micros,
    Millis,
    Seconds,
    Rfc3339,
}

const NANOS_PER_SECOND: i64 = 1_000_000_000;

const START_FORMS: [(&str, Unit); 5] = [
    ("span.start", Unit::Nanos),
    ("span.start_us", Unit::Micros),
    ("span.start_ms", Unit::Millis),
    ("span.start_s", Unit::Seconds),
    ("span.start_rfc3339", Unit::Rfc3339),
];
const END_FORMS: [(&str, Unit); 5] = [
    ("span.end", Unit::Nanos),
    ("span.end_us", Unit::Micros),
    ("span.end_ms", Unit::Millis),
    ("span.end_s", Unit::Seconds),
    ("span.end_rfc3339", Unit::Rfc3339),
];
const DURATION_FORMS: [(&str, Unit); 4] = [
    ("span.duration", Unit::Nanos),
    ("span.duration_us", Unit::Micros),
    ("span.duration_ms", Unit::Millis),
    ("span.duration_s", Unit::Seconds),
];

/// The `span:` block. Mirrors `logit_config::SpanLiftConfig`; `logit-cli` converts.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanLift {
    /// Mint a fresh span id when the `span_id` attribute is absent instead of skipping the event.
    pub mint_id: bool,
    /// The span name when `span.name` is absent.
    pub name: String,
    /// The span kind when `span.kind` is absent.
    pub kind: SpanKind,
    /// A resolved start or end further than this from the event's receipt time is a
    /// `skipped{reason="skew"}`, never written.
    pub max_skew: Duration,
}

/// The id grammar of the trace id and span id attributes (the module doc's table). Mirrors
/// `logit_config::TraceIdFormat` plus its `trace_id_high` field; `logit-cli` converts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum IdFormat {
    /// 32-hex trace ids and 16-hex span ids.
    #[default]
    Otel,
    /// Decimal uint64 ids as Datadog tracers inject them into logs, or a 32-hex trace id.
    Datadog {
        /// The attribute holding the high 64 bits of a trace id that arrived without them.
        trace_id_high: Option<String>,
    },
}

impl IdFormat {
    fn trace_id(&self, value: &Value) -> Option<[u8; 16]> {
        match self {
            IdFormat::Otel => value.as_str().and_then(parse_trace_id),
            IdFormat::Datadog { .. } => match value {
                Value::Str(_) => value.as_str().and_then(parse_trace_id_datadog),
                _ => datadog_integer(value).map(|low| trace_id_bytes(0, low)),
            },
        }
    }

    fn span_id(&self, value: &Value) -> Option<[u8; 8]> {
        match self {
            IdFormat::Otel => value.as_str().and_then(parse_span_id),
            IdFormat::Datadog { .. } => match value {
                Value::Str(_) => value.as_str().and_then(parse_span_id_datadog),
                _ => datadog_integer(value).map(u64::to_be_bytes),
            },
        }
    }

    fn trace_id_high_field(&self) -> Option<&str> {
        match self {
            IdFormat::Otel => None,
            IdFormat::Datadog { trace_id_high } => trace_id_high.as_deref(),
        }
    }
}

/// A Datadog id that a JSON decoder turned into a number (an unquoted `"dd.trace_id": 123`): the
/// uint64 itself, non-zero. A float loses a 64-bit id's low digits, so it's never one.
fn datadog_integer(value: &Value) -> Option<u64> {
    match value {
        Value::U64(n) => Some(*n),
        Value::I64(n) => u64::try_from(*n).ok(),
        _ => None,
    }
    .filter(|&n| n != 0)
}

/// [`SpanLift`] prepared once: the default name is a `Value` the per-event path only
/// refcount-bumps, and the skew window is in nanoseconds.
struct SpanDefaults {
    mint_id: bool,
    name: Value,
    kind: SpanKind,
    max_skew_nanos: u64,
}

/// Why a lift didn't apply: the `reason` tag on `.skipped`, a `&'static str` per
/// `docs/design/internal-telemetry.md`'s cardinality rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Skip {
    /// No trace id anywhere: neither the configured attribute nor a `traceparent`.
    Missing,
    /// Something present didn't parse: an id in the configured [`IdFormat`]'s grammar (a 16-hex
    /// `dd.span_id`, say), a `trace_id_high`, the flags, a `traceparent`, a `span.kind`/
    /// `span.status` name, a timing value, or two forms of one timing quantity at once.
    Invalid,
    /// A `span:` block needs this line's own span id, none was present, and `mint_id` is off.
    SpanId,
    /// The timing attributes present can't determine both a start and an end, or determine an
    /// impossible span (negative duration, end before start, arithmetic overflow).
    Timing,
    /// The resolved start or end is further from receipt time than `max_skew` allows.
    Skew,
}

impl Skip {
    fn reason(self) -> &'static str {
        match self {
            Skip::Missing => "missing",
            Skip::Invalid => "invalid",
            Skip::SpanId => "span_id",
            Skip::Timing => "timing",
            Skip::Skew => "skew",
        }
    }
}

/// Everything a successful lift writes, computed from a borrowed `AttrMap` before any mutation.
struct Lifted {
    trace: TraceRef,
    /// `Some` only with a `span:` block; the span's start is `timestamp` below.
    span: Option<SpanRecord>,
    timestamp: Option<i64>,
    minted: bool,
}

/// An attribute is present only if it carries a value. `Null`, `""`, and `"-"` are how nginx
/// (`escape=json` renders unset as `""`, plain formats as `-`) and haproxy (an unset `txn` var)
/// spell "nothing here"; as present-but-invalid they would turn every edge request into an
/// `invalid` skip.
fn present<'a>(attrs: &'a AttrMap, key: &str) -> Option<&'a Value> {
    match attrs.get(key)? {
        Value::Null => None,
        value @ Value::Str(_) => match value.as_str() {
            Some("") | Some("-") => None,
            _ => Some(value),
        },
        value => Some(value),
    }
}

/// A standalone `flags` field as an integer 0-255, decimal only. Never hex: a `traceparent`'s
/// hex flags octet pasted here would parse as a different value (`"10"` is 10, not `0x10`).
/// `parse_traceparent` reads that octet as hex; the two paths never mix.
fn numeric_flags(value: &Value) -> Option<u8> {
    match value {
        Value::I64(n) => u8::try_from(*n).ok(),
        Value::U64(n) => u8::try_from(*n).ok(),
        Value::Str(_) => value.as_str()?.parse::<u16>().ok().and_then(|n| u8::try_from(n).ok()),
        _ => None,
    }
}

/// One timing value to exact nanoseconds, in the unit its attribute name declared.
///
/// `instant` marks a start/end, which may also be a `Value::Timestamp` or an `_rfc3339` string;
/// a duration may not. A float in `Nanos`/`Micros`/`Millis` is rejected, not rounded: an `f64`
/// can't hold an epoch-nanosecond instant (2^53 < 1.7e18), so it's a producer bug. `Seconds`
/// accepts a float (nginx's `$msec`/`$request_time`), and parses a `Str` digit-exact.
fn timing_nanos(value: &Value, unit: Unit, instant: bool) -> Option<i64> {
    let scale = match unit {
        Unit::Nanos => 1,
        Unit::Micros => 1_000,
        Unit::Millis => 1_000_000,
        Unit::Seconds => NANOS_PER_SECOND,
        Unit::Rfc3339 => {
            return if instant { parse_rfc3339_to_nanos(value.as_str()?).ok() } else { None };
        }
    };
    match value {
        Value::I64(n) => n.checked_mul(scale),
        Value::U64(n) => i64::try_from(*n).ok()?.checked_mul(scale),
        Value::Str(_) => parse_decimal_nanos(value.as_str()?, scale),
        Value::Timestamp(n) if instant && unit == Unit::Nanos => Some(*n),
        Value::F64(f) if unit == Unit::Seconds => f64_seconds_to_nanos(*f),
        _ => None,
    }
}

/// `f64` seconds to nanoseconds, scaling the integer part exactly (in `i128`, so a huge float is
/// `None`, not a wrap) and rounding only the fraction. At epoch magnitude that's good to about a
/// microsecond, finer than any producer that emits float seconds.
fn f64_seconds_to_nanos(seconds: f64) -> Option<i64> {
    if !seconds.is_finite() {
        return None;
    }
    let whole = seconds.trunc();
    let frac = seconds - whole;
    // `as i128` saturates rather than wrapping; `checked_mul`/`try_from` then reject it.
    let whole_nanos = (whole as i128).checked_mul(i128::from(NANOS_PER_SECOND))?;
    let whole_nanos = i64::try_from(whole_nanos).ok()?;
    let frac_nanos = (frac * NANOS_PER_SECOND as f64).round() as i64;
    whole_nanos.checked_add(frac_nanos)
}

/// Looks up one quantity (start, end, or duration) across all its forms. Two forms at once
/// (`span.start` and `span.start_ms`) is `Invalid`, not resolved by precedence. `Ok(None)` means
/// none was supplied.
fn quantity(attrs: &AttrMap, forms: &[(&str, Unit)], instant: bool) -> Result<Option<i64>, Skip> {
    let mut found = None;
    for (key, unit) in forms {
        if let Some(value) = present(attrs, key) {
            if found.is_some() {
                return Err(Skip::Invalid);
            }
            found = Some(timing_nanos(value, *unit, instant).ok_or(Skip::Invalid)?);
        }
    }
    Ok(found)
}

/// Lifts `trace_id`/`span_id`/`flags` (and, with [`with_span`], a whole span) off configured
/// attributes onto `event.log.trace` and `event.span`.
///
/// A successful lift overwrites what's there: operator intent beats wire-carried data, as with
/// `Set`.
///
/// [`with_span`]: TraceContext::with_span
pub struct TraceContext {
    trace_id_field: String,
    span_id_field: Option<String>,
    flags_field: Option<String>,
    format: IdFormat,
    keep_source: bool,
    span: Option<SpanDefaults>,
    telemetry: Telemetry,
}

impl TraceContext {
    /// Takes the resolved field names: [`IdFormat`] changes only how their values parse, not
    /// which attributes are read (`logit_config::TraceIdFormat::resolve_fields` picks those).
    pub fn new(
        trace_id_field: String,
        span_id_field: Option<String>,
        flags_field: Option<String>,
        keep_source: bool,
    ) -> Self {
        Self {
            trace_id_field,
            span_id_field,
            flags_field,
            format: IdFormat::Otel,
            keep_source,
            span: None,
            telemetry: Telemetry::default(),
        }
    }

    /// Parses the trace id and span id attributes in `format`'s grammar instead of the default
    /// [`IdFormat::Otel`].
    pub fn with_format(mut self, format: IdFormat) -> Self {
        self.format = format;
        self
    }

    /// Mints a `SpanRecord` per lifted line (the `span:` block).
    pub fn with_span(mut self, span: SpanLift) -> Self {
        self.span = Some(SpanDefaults {
            mint_id: span.mint_id,
            name: Value::str(span.name),
            kind: span.kind,
            max_skew_nanos: u64::try_from(span.max_skew.as_nanos()).unwrap_or(u64::MAX),
        });
        self
    }

    /// Attaches a telemetry handle.
    ///
    /// There's no `Diagnostics` builder: a missing or unparseable attribute is a counted skip,
    /// not a warning.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Computes the whole lift without touching the event. An explicit attribute beats the same
    /// piece of a `traceparent`.
    fn lift(&self, attrs: &AttrMap, receipt: i64) -> Result<Lifted, Skip> {
        let traceparent = match present(attrs, TRACEPARENT) {
            None => None,
            Some(value) => Some(value.as_str().and_then(parse_traceparent).ok_or(Skip::Invalid)?),
        };

        let trace_id = match present(attrs, &self.trace_id_field) {
            Some(value) => self.format.trace_id(value).ok_or(Skip::Invalid)?,
            None => traceparent.map(|(trace, _, _)| trace).ok_or(Skip::Missing)?,
        };
        // Parsed whenever present, like `flags`, so a bad value is `Invalid` even when a 128-bit
        // trace id leaves it unused.
        let trace_id = match self.format.trace_id_high_field().and_then(|f| present(attrs, f)) {
            Some(value) => {
                let high = value.as_str().and_then(parse_trace_id_high).ok_or(Skip::Invalid)?;
                match trace_id_halves(&trace_id) {
                    (0, low) => trace_id_bytes(high, low),
                    _ => trace_id,
                }
            }
            None => trace_id,
        };

        let flags = match self.flags_field.as_deref().and_then(|field| present(attrs, field)) {
            // Unparseable is `Invalid`, not treated as absent: the operator configured this
            // field, and ignoring it could hide an upstream problem.
            Some(value) => numeric_flags(value).ok_or(Skip::Invalid)?,
            None => traceparent.map(|(_, _, flags)| flags).unwrap_or(0),
        };

        let own_span_id =
            match self.span_id_field.as_deref().and_then(|field| present(attrs, field)) {
                Some(value) => Some(self.format.span_id(value).ok_or(Skip::Invalid)?),
                None => None,
            };

        let Some(defaults) = &self.span else {
            return Ok(Lifted {
                trace: TraceRef { trace_id, span_id: own_span_id, flags },
                span: None,
                timestamp: None,
                minted: false,
            });
        };

        let (span_id, minted) = match own_span_id {
            Some(id) => (id, false),
            None if defaults.mint_id => (random_id_bytes(), true),
            None => return Err(Skip::SpanId),
        };
        let parent_span_id = match present(attrs, SPAN_PARENT_ID) {
            Some(value) => Some(value.as_str().and_then(parse_span_id).ok_or(Skip::Invalid)?),
            None => traceparent.map(|(_, parent, _)| parent),
        };
        let name = match present(attrs, SPAN_NAME) {
            Some(value @ Value::Str(_)) => value.clone(),
            Some(_) => return Err(Skip::Invalid),
            None => defaults.name.clone(),
        };
        let kind = match present(attrs, SPAN_KIND) {
            Some(value) => value.as_str().and_then(SpanKind::from_name).ok_or(Skip::Invalid)?,
            None => defaults.kind,
        };
        let status = match present(attrs, SPAN_STATUS) {
            Some(value) => value.as_str().and_then(SpanStatus::from_name).ok_or(Skip::Invalid)?,
            None => SpanStatus::Unset,
        };

        let start = quantity(attrs, &START_FORMS, true)?;
        let end = quantity(attrs, &END_FORMS, true)?;
        let duration = quantity(attrs, &DURATION_FORMS, false)?;
        if duration.is_some_and(|d| d < 0) {
            return Err(Skip::Timing);
        }
        // Any two determine the third. A lone start or duration takes receipt time as the end:
        // a line is written at request end and arrives moments later, and it's the only way a
        // stock nginx line carrying just `request_time` yields a span. A lone end says nothing
        // about the start.
        let (start, end) = match (start, end, duration) {
            (Some(s), Some(e), _) => (s, e),
            (Some(s), None, Some(d)) => (s, s.checked_add(d).ok_or(Skip::Timing)?),
            (None, Some(e), Some(d)) => (e.checked_sub(d).ok_or(Skip::Timing)?, e),
            (Some(s), None, None) => (s, receipt),
            (None, None, Some(d)) => (receipt.checked_sub(d).ok_or(Skip::Timing)?, receipt),
            (None, Some(_), None) | (None, None, None) => return Err(Skip::Timing),
        };
        if end < start {
            return Err(Skip::Timing);
        }
        let skewed = |instant: i64| {
            instant
                .checked_sub(receipt)
                .map(|delta| delta.unsigned_abs() > defaults.max_skew_nanos)
                .unwrap_or(true)
        };
        if skewed(start) || skewed(end) {
            return Err(Skip::Skew);
        }

        Ok(Lifted {
            trace: TraceRef { trace_id, span_id: Some(span_id), flags },
            span: Some(SpanRecord {
                trace_id,
                span_id,
                parent_span_id,
                name,
                kind,
                status,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: end,
                // The `TraceRef`'s flags, so the span's `SAMPLED` bit matches its log's.
                flags: flags as u32,
                ext: None,
            }),
            timestamp: Some(start),
            minted,
        })
    }

    /// Removes every attribute this configuration reads; removing an absent key is a cheap probe,
    /// so nothing tracks which were present.
    fn remove_consumed(&self, attrs: &mut AttrMap) {
        attrs.remove(&self.trace_id_field);
        if let Some(field) = &self.span_id_field {
            attrs.remove(field);
        }
        if let Some(field) = &self.flags_field {
            attrs.remove(field);
        }
        if let Some(field) = self.format.trace_id_high_field() {
            attrs.remove(field);
        }
        attrs.remove(TRACEPARENT);
        if self.span.is_some() {
            for key in [SPAN_PARENT_ID, SPAN_NAME, SPAN_KIND, SPAN_STATUS] {
                attrs.remove(key);
            }
            for (key, _) in START_FORMS.iter().chain(&END_FORMS).chain(&DURATION_FORMS) {
                attrs.remove(key);
            }
        }
    }
}

impl Transform for TraceContext {
    /// An event with no log passes through untouched. Otherwise it's either a complete lift
    /// (`.lifted`, plus `.spans{id}` with a `span:` block) or a `.skipped{reason}` ([`Skip`])
    /// that leaves the event as it arrived. A lift overwrites `log.trace` (with `span:`, also
    /// `event.span` and `event.timestamp`, the span's start) and, unless `keep_source`, removes
    /// every convention attribute it read.
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        if event.log.is_none() {
            return true;
        }
        match self.lift(&event.attributes, event.timestamp) {
            Ok(lifted) => {
                event.log.as_mut().expect("checked above").trace = Some(lifted.trace);
                if let Some(timestamp) = lifted.timestamp {
                    event.timestamp = timestamp;
                }
                if lifted.span.is_some() {
                    event.span = lifted.span;
                    self.telemetry.count(
                        "logit.transform.trace_context.spans",
                        1.0,
                        &[("id", if lifted.minted { "minted" } else { "present" })],
                    );
                }
                if !self.keep_source {
                    self.remove_consumed(&mut event.attributes);
                }
                self.telemetry.count("logit.transform.trace_context.lifted", 1.0, &[]);
            }
            Err(skip) => {
                self.telemetry.count(
                    "logit.transform.trace_context.skipped",
                    1.0,
                    &[("reason", skip.reason())],
                );
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{BodyFormat, LogRecord, Registry};

    const RECEIPT: i64 = 1_725_400_000_500_000_000;
    const W3C_EXAMPLE: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const W3C_TRACE: [u8; 16] = [
        0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47,
        0x36,
    ];
    const W3C_PARENT: [u8; 8] = [0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7];

    fn log_event(pairs: &[(&str, Value)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, v.clone());
        }
        Event::log(
            RECEIPT,
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

    fn metric_only_event() -> Event {
        Event::metric(
            0,
            AttrMap::new(),
            logit_core::MetricRecord::new(
                logit_core::interner::intern("m"),
                logit_core::MetricKind::counter(1.0),
            ),
        )
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn hex_trace() -> String {
        "ab".repeat(16)
    }

    fn hex_span() -> String {
        "cd".repeat(8)
    }

    /// Underscore field names, no span block.
    fn legacy() -> TraceContext {
        TraceContext::new("trace_id".to_string(), None, None, false)
    }

    /// The convention defaults (`logit_config`'s), no span block.
    fn convention() -> TraceContext {
        TraceContext::new(
            "trace.id".to_string(),
            Some("span.id".to_string()),
            Some("trace.flags".to_string()),
            false,
        )
    }

    fn span_lift() -> SpanLift {
        SpanLift {
            mint_id: false,
            name: "http.request".to_string(),
            kind: SpanKind::Server,
            max_skew: Duration::from_secs(3600),
        }
    }

    fn with_span() -> TraceContext {
        convention().with_span(span_lift())
    }

    /// The minimal attribute set a span lift needs: ids plus one timing pair.
    fn span_attrs() -> Vec<(&'static str, Value)> {
        vec![
            ("trace.id", Value::str(hex_trace())),
            ("span.id", Value::str(hex_span())),
            ("span.start", Value::I64(RECEIPT - 10_000_000)),
            ("span.duration", Value::I64(4_000_000)),
        ]
    }

    /// Finds one counter in already-drained telemetry; `Registry::drain` empties the buffer, so
    /// a test checking several counters drains once.
    fn find_counter(events: &[Event], name: &str, tag: Option<(&str, &str)>) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == name
                        && tag.is_none_or(|(k, want)| {
                            e.attributes.get(k).and_then(|v| v.as_str()) == Some(want)
                        }) =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    /// Drains the registry and finds one counter.
    fn counter(registry: &Registry, name: &str, tag: Option<(&str, &str)>) -> Option<f64> {
        find_counter(&registry.drain(0), name, tag)
    }

    fn instrumented(t: TraceContext) -> (TraceContext, Arc<Registry>) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("web_trace", "trace_context", "transform");
        (t.with_telemetry(telemetry), registry)
    }

    // -- The log-only lift ------------------------------------------------------------------------

    #[test]
    fn a_valid_trace_id_is_lifted_and_removed_by_default() {
        let mut t = legacy();
        let mut event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            event.log.unwrap().trace,
            Some(TraceRef { trace_id: [0xab; 16], span_id: None, flags: 0 })
        );
        assert!(
            event.attributes.get("trace_id").is_none(),
            "the source attribute should be removed"
        );
    }

    #[test]
    fn keep_source_leaves_the_attribute_in_place() {
        let mut t = TraceContext::new("trace_id".to_string(), None, None, true);
        let mut event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        assert!(t.process(&default_resource(), &mut event));
        assert!(event.log.unwrap().trace.is_some());
        assert!(event.attributes.get("trace_id").is_some());
    }

    #[test]
    fn span_id_and_flags_are_lifted_when_configured_and_present() {
        let mut t = TraceContext::new(
            "trace_id".to_string(),
            Some("span_id".to_string()),
            Some("flags".to_string()),
            false,
        );
        let mut event = log_event(&[
            ("trace_id", Value::str(hex_trace())),
            ("span_id", Value::str(hex_span())),
            ("flags", Value::I64(1)),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            event.log.unwrap().trace,
            Some(TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 1 })
        );
        assert!(event.attributes.get("span_id").is_none());
        assert!(event.attributes.get("flags").is_none());
    }

    #[test]
    fn flags_accepts_a_decimal_string_but_not_hex() {
        let mut t =
            TraceContext::new("trace_id".to_string(), None, Some("flags".to_string()), false);
        let mut event =
            log_event(&[("trace_id", Value::str(hex_trace())), ("flags", Value::str("1"))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace.unwrap().flags, 1);
    }

    #[test]
    fn a_missing_trace_id_attribute_is_skipped_not_an_error() {
        let mut t = legacy();
        let mut event = log_event(&[]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace, None);
    }

    #[test]
    fn an_unparseable_trace_id_is_skipped_and_leaves_the_attribute_in_place() {
        let mut t = legacy();
        let mut event = log_event(&[("trace_id", Value::str("not-hex"))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace, None);
        assert!(
            event.attributes.get("trace_id").is_some(),
            "a failed lift must not destroy the evidence"
        );
    }

    #[test]
    fn an_unparseable_span_id_skips_the_whole_lift_even_though_trace_id_was_valid() {
        let mut t =
            TraceContext::new("trace_id".to_string(), Some("span_id".to_string()), None, false);
        let mut event =
            log_event(&[("trace_id", Value::str(hex_trace())), ("span_id", Value::str("not-hex"))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace, None, "an invalid span_id must not partially apply");
        assert!(event.attributes.get("trace_id").is_some());
        assert!(event.attributes.get("span_id").is_some());
    }

    #[test]
    fn a_configured_span_id_field_absent_from_the_event_is_not_an_error() {
        let mut t =
            TraceContext::new("trace_id".to_string(), Some("span_id".to_string()), None, false);
        let mut event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            event.log.unwrap().trace,
            Some(TraceRef { trace_id: [0xab; 16], span_id: None, flags: 0 })
        );
    }

    #[test]
    fn an_event_with_no_log_passes_through_untouched() {
        let mut t = legacy();
        let mut event = metric_only_event();
        event.attributes.insert("trace_id", Value::str(hex_trace()));
        assert!(t.process(&default_resource(), &mut event));
        assert!(event.log.is_none());
        assert!(event.attributes.get("trace_id").is_some(), "nothing should have been touched");
    }

    #[test]
    fn a_successful_lift_overwrites_an_existing_trace() {
        let mut t = legacy();
        let mut event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        event.log.as_mut().unwrap().trace =
            Some(TraceRef { trace_id: [1; 16], span_id: None, flags: 0 });
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace.unwrap().trace_id, [0xab; 16]);
    }

    #[test]
    fn a_missing_lift_records_a_skipped_missing_counter() {
        let (mut t, registry) = instrumented(legacy());
        let mut event = log_event(&[]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            counter(
                &registry,
                "logit.transform.trace_context.skipped",
                Some(("reason", "missing"))
            ),
            Some(1.0)
        );
    }

    #[test]
    fn a_successful_lift_records_a_lifted_counter() {
        let (mut t, registry) = instrumented(legacy());
        let mut event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(counter(&registry, "logit.transform.trace_context.lifted", None), Some(1.0));
    }

    // -- The convention: defaults, `traceparent`, absent spellings ------------------------------

    #[test]
    fn the_convention_defaults_lift_dotted_names_with_no_overrides() {
        let mut t = convention();
        let mut event = log_event(&[
            ("trace.id", Value::str(hex_trace())),
            ("span.id", Value::str(hex_span())),
            ("trace.flags", Value::U64(1)),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            event.log.unwrap().trace,
            Some(TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 1 })
        );
        assert!(event.attributes.is_empty(), "all three consumed: {:?}", event.attributes);
    }

    #[test]
    fn a_traceparent_alone_lifts_trace_id_and_flags_but_never_becomes_the_logs_span_id() {
        let mut t = convention();
        let mut event = log_event(&[("traceparent", Value::str(W3C_EXAMPLE))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            event.log.unwrap().trace,
            Some(TraceRef { trace_id: W3C_TRACE, span_id: None, flags: 1 }),
            "the header's span id is the caller's, not this line's"
        );
        assert!(event.attributes.get("traceparent").is_none(), "consumed");
    }

    #[test]
    fn traceparent_flags_are_hex_while_the_standalone_field_is_decimal() {
        let header = format!("{}-10", &W3C_EXAMPLE[..52]);
        let mut t = convention();
        let mut event = log_event(&[("traceparent", Value::str(header.clone()))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace.unwrap().flags, 0x10, "header octet is hex");

        let mut event =
            log_event(&[("traceparent", Value::str(header)), ("trace.flags", Value::str("10"))]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            event.log.unwrap().trace.unwrap().flags,
            10,
            "explicit field is decimal, and wins"
        );
    }

    #[test]
    fn an_explicit_trace_id_beats_the_traceparents() {
        let mut t = convention();
        let mut event = log_event(&[
            ("traceparent", Value::str(W3C_EXAMPLE)),
            ("trace.id", Value::str(hex_trace())),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace.unwrap().trace_id, [0xab; 16]);
    }

    #[test]
    fn a_malformed_traceparent_is_invalid_even_with_a_valid_explicit_trace_id() {
        let (mut t, registry) = instrumented(convention());
        let mut event = log_event(&[
            ("traceparent", Value::str("00-nope")),
            ("trace.id", Value::str(hex_trace())),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(event.log.unwrap().trace, None);
        assert_eq!(
            counter(
                &registry,
                "logit.transform.trace_context.skipped",
                Some(("reason", "invalid"))
            ),
            Some(1.0)
        );
    }

    #[test]
    fn empty_dash_and_null_values_count_as_absent() {
        let mut t = convention();
        for absent in [Value::str(""), Value::str("-"), Value::Null] {
            let mut event = log_event(&[
                ("trace.id", Value::str(hex_trace())),
                ("span.id", absent.clone()),
                ("traceparent", absent.clone()),
                ("trace.flags", absent),
            ]);
            assert!(t.process(&default_resource(), &mut event));
            assert_eq!(
                event.log.unwrap().trace,
                Some(TraceRef { trace_id: [0xab; 16], span_id: None, flags: 0 })
            );
        }
        let mut event = log_event(&[("trace.id", Value::str(""))]);
        let (mut t, registry) = instrumented(convention());
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            counter(
                &registry,
                "logit.transform.trace_context.skipped",
                Some(("reason", "missing"))
            ),
            Some(1.0),
            "an empty trace id is absent, not invalid"
        );
    }

    #[test]
    fn a_null_span_id_field_disables_that_lookup() {
        let mut t = TraceContext::new("trace.id".to_string(), None, None, false);
        let mut event =
            log_event(&[("trace.id", Value::str(hex_trace())), ("span.id", Value::str("not-hex"))]);
        assert!(t.process(&default_resource(), &mut event));
        assert!(event.log.unwrap().trace.is_some(), "span.id was never read, so can't be invalid");
        assert!(event.attributes.get("span.id").is_some(), "and is not consumed either");
    }

    // -- The span block: ids, defaults, minting ------------------------------------------------

    #[test]
    fn a_span_is_minted_on_the_same_event_with_start_as_the_timestamp() {
        let (mut t, registry) = instrumented(with_span());
        let mut attrs = span_attrs();
        attrs.push(("traceparent", Value::str(W3C_EXAMPLE)));
        attrs.push(("host", Value::str("example")));
        let mut event = log_event(&attrs);
        assert!(t.process(&default_resource(), &mut event));

        let span = event.span.as_ref().expect("a span");
        assert_eq!(span.trace_id, [0xab; 16], "explicit trace.id beat the traceparent's");
        assert_eq!(span.span_id, [0xcd; 8]);
        assert_eq!(span.parent_span_id, Some(W3C_PARENT), "parent from the traceparent");
        assert_eq!(span.name.as_str(), Some("http.request"));
        assert_eq!(span.kind, SpanKind::Server);
        assert_eq!(span.status, SpanStatus::Unset);
        assert!(span.events.is_empty() && span.links.is_empty());
        assert_eq!(event.timestamp, RECEIPT - 10_000_000, "start replaces receipt time");
        assert_eq!(span.end_timestamp, RECEIPT - 6_000_000);
        assert_eq!(
            event.log.as_ref().unwrap().trace,
            Some(TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 1 }),
            "the log correlates to its own span; flags came from the traceparent"
        );
        assert_eq!(event.attributes.len(), 1, "only `host` survives: {:?}", event.attributes);
        assert!(event.attributes.get("host").is_some());
        let drained = registry.drain(0);
        assert_eq!(find_counter(&drained, "logit.transform.trace_context.lifted", None), Some(1.0));
        assert_eq!(
            find_counter(&drained, "logit.transform.trace_context.spans", Some(("id", "present"))),
            Some(1.0)
        );
    }

    #[test]
    fn a_missing_span_id_is_a_span_id_skip_unless_minting_is_on() {
        let (mut t, registry) = instrumented(with_span());
        let attrs: Vec<_> = span_attrs().into_iter().filter(|(k, _)| *k != "span.id").collect();
        let mut event = log_event(&attrs);
        assert!(t.process(&default_resource(), &mut event));
        assert!(event.span.is_none());
        assert_eq!(event.log.as_ref().unwrap().trace, None, "all-or-nothing");
        assert_eq!(event.timestamp, RECEIPT, "timestamp untouched");
        assert_eq!(event.attributes.len(), 3, "nothing consumed");
        assert_eq!(
            counter(
                &registry,
                "logit.transform.trace_context.skipped",
                Some(("reason", "span_id"))
            ),
            Some(1.0)
        );

        let (mut t, registry) =
            instrumented(convention().with_span(SpanLift { mint_id: true, ..span_lift() }));
        let mut event = log_event(&attrs);
        assert!(t.process(&default_resource(), &mut event));
        let span = event.span.expect("minted");
        assert_ne!(span.span_id, [0; 8]);
        assert_eq!(event.log.unwrap().trace.unwrap().span_id, Some(span.span_id));
        assert_eq!(
            counter(&registry, "logit.transform.trace_context.spans", Some(("id", "minted"))),
            Some(1.0)
        );
    }

    #[test]
    fn span_attributes_override_the_configured_defaults() {
        let mut t = with_span();
        let mut attrs = span_attrs();
        attrs.extend([
            ("traceparent", Value::str(W3C_EXAMPLE)),
            ("span.parent_id", Value::str("ef".repeat(8))),
            ("span.name", Value::str("GET /")),
            ("span.kind", Value::str("client")),
            ("span.status", Value::str("error")),
        ]);
        let mut event = log_event(&attrs);
        assert!(t.process(&default_resource(), &mut event));
        let span = event.span.unwrap();
        assert_eq!(span.parent_span_id, Some([0xef; 8]), "explicit parent beats the header's");
        assert_eq!(span.name.as_str(), Some("GET /"));
        assert_eq!(span.kind, SpanKind::Client);
        assert_eq!(span.status, SpanStatus::Error);
        assert!(event.attributes.is_empty(), "every span.* consumed: {:?}", event.attributes);
    }

    #[test]
    fn an_unknown_kind_or_status_or_non_string_name_is_invalid() {
        for (key, value) in [
            ("span.kind", Value::str("SERVER")),
            ("span.status", Value::str("failed")),
            ("span.name", Value::I64(7)),
            ("span.parent_id", Value::str("zz")),
        ] {
            let (mut t, registry) = instrumented(with_span());
            let mut attrs = span_attrs();
            attrs.push((key, value));
            let mut event = log_event(&attrs);
            assert!(t.process(&default_resource(), &mut event));
            assert!(event.span.is_none(), "{key}");
            assert_eq!(
                counter(
                    &registry,
                    "logit.transform.trace_context.skipped",
                    Some(("reason", "invalid"))
                ),
                Some(1.0),
                "{key}"
            );
        }
    }

    #[test]
    fn keep_source_retains_every_convention_attribute() {
        let mut t = TraceContext::new(
            "trace.id".to_string(),
            Some("span.id".to_string()),
            Some("trace.flags".to_string()),
            true,
        )
        .with_span(span_lift());
        let mut attrs = span_attrs();
        attrs.push(("traceparent", Value::str(W3C_EXAMPLE)));
        let mut event = log_event(&attrs);
        assert!(t.process(&default_resource(), &mut event));
        assert!(event.span.is_some());
        assert_eq!(event.attributes.len(), attrs.len());
    }

    // -- Timing: every form, every resolution --------------------------------------------------

    fn span_with(timing: &[(&str, Value)]) -> Result<(i64, i64), &'static str> {
        let (mut t, registry) = instrumented(with_span());
        let mut attrs =
            vec![("trace.id", Value::str(hex_trace())), ("span.id", Value::str(hex_span()))];
        attrs.extend(timing.iter().cloned());
        let mut event = log_event(&attrs);
        assert!(t.process(&default_resource(), &mut event));
        match event.span {
            Some(span) => Ok((event.timestamp, span.end_timestamp)),
            None => {
                let drained = registry.drain(0);
                for reason in ["invalid", "span_id", "timing", "skew", "missing"] {
                    if find_counter(
                        &drained,
                        "logit.transform.trace_context.skipped",
                        Some(("reason", reason)),
                    )
                    .is_some()
                    {
                        return Err(reason);
                    }
                }
                panic!("no span and no skip counter")
            }
        }
    }

    #[test]
    fn any_two_of_start_end_duration_determine_the_third() {
        let s = RECEIPT - 10_000_000;
        let e = RECEIPT - 6_000_000;
        assert_eq!(
            span_with(&[("span.start", Value::I64(s)), ("span.end", Value::I64(e))]),
            Ok((s, e))
        );
        assert_eq!(
            span_with(&[("span.start", Value::I64(s)), ("span.duration", Value::I64(4_000_000))]),
            Ok((s, e))
        );
        assert_eq!(
            span_with(&[("span.end", Value::I64(e)), ("span.duration", Value::I64(4_000_000))]),
            Ok((s, e))
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(s)),
                ("span.end", Value::I64(e)),
                ("span.duration", Value::I64(999)),
            ]),
            Ok((s, e)),
            "start+end win; a duration alongside is consumed but ignored"
        );
    }

    #[test]
    fn a_lone_start_or_duration_borrows_receipt_time_as_the_end() {
        let s = RECEIPT - 10_000_000;
        assert_eq!(span_with(&[("span.start", Value::I64(s))]), Ok((s, RECEIPT)));
        assert_eq!(
            span_with(&[("span.duration_ms", Value::I64(10))]),
            Ok((s, RECEIPT)),
            "the unchanged-nginx-line case: request_time alone still yields a span"
        );
        assert_eq!(
            span_with(&[("span.end", Value::I64(RECEIPT))]),
            Err("timing"),
            "a lone end says nothing about the start"
        );
        assert_eq!(span_with(&[]), Err("timing"));
    }

    #[test]
    fn unit_suffixed_integer_forms_scale_exactly() {
        let s = RECEIPT - 10_000_000;
        assert_eq!(
            span_with(&[
                ("span.start_us", Value::I64(s / 1_000)),
                ("span.duration_ms", Value::U64(4))
            ]),
            Ok((s, s + 4_000_000))
        );
        assert_eq!(
            span_with(&[
                ("span.start_ms", Value::I64(s / 1_000_000)),
                ("span.duration_us", Value::I64(4_000))
            ]),
            Ok((s, s + 4_000_000))
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::str(s.to_string())),
                ("span.duration", Value::str("4000000"))
            ]),
            Ok((s, s + 4_000_000)),
            "an all-digit string is an integer"
        );
    }

    #[test]
    fn the_seconds_form_takes_floats_and_is_digit_exact_from_a_string() {
        // nginx's `$msec`/`$request_time` as JSON floats. An f64 at epoch magnitude resolves to
        // ~0.24us, so this lands within a microsecond but isn't exact; the quoted form below is.
        let end_s = 1_725_400_000.123_f64;
        let (start, end) =
            span_with(&[("span.end_s", Value::F64(end_s)), ("span.duration_s", Value::F64(0.004))])
                .unwrap();
        assert!((end - 1_725_400_000_123_000_000).abs() < 1_000, "{end}");
        assert_eq!(end - start, 4_000_000, "the duration is exact: 0.004 has no epoch magnitude");
        // The same values quoted: exact to the last digit, past what f64 could carry.
        let got = span_with(&[
            ("span.end_s", Value::str("1725400000.123456789")),
            ("span.duration_s", Value::str("0.000000789")),
        ])
        .unwrap();
        assert_eq!(got, (1_725_400_000_123_456_000, 1_725_400_000_123_456_789));
        assert_eq!(
            span_with(&[
                ("span.start_s", Value::I64(1_725_400_000)),
                ("span.end_s", Value::U64(1_725_400_001))
            ]),
            Ok((1_725_400_000_000_000_000, 1_725_400_001_000_000_000))
        );
    }

    #[test]
    fn a_float_in_an_integer_denominated_form_is_invalid_not_rounded() {
        assert_eq!(
            span_with(&[("span.start", Value::F64(1.7254e18)), ("span.duration", Value::I64(1))]),
            Err("invalid")
        );
        assert_eq!(
            span_with(&[("span.start_us", Value::I64(1)), ("span.duration_ms", Value::F64(4.0))]),
            Err("invalid")
        );
    }

    #[test]
    fn a_timestamp_value_is_an_instant_only_in_the_base_form() {
        let s = RECEIPT - 10_000_000;
        assert_eq!(
            span_with(&[
                ("span.start", Value::Timestamp(s)),
                ("span.end", Value::Timestamp(RECEIPT))
            ]),
            Ok((s, RECEIPT))
        );
        assert_eq!(
            span_with(&[
                ("span.start_ms", Value::Timestamp(s)),
                ("span.end", Value::Timestamp(RECEIPT))
            ]),
            Err("invalid")
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::Timestamp(s)),
                ("span.duration", Value::Timestamp(1))
            ]),
            Err("invalid"),
            "a duration is not an instant"
        );
    }

    #[test]
    fn the_rfc3339_form_parses_instants_only() {
        // RECEIPT = 2024-09-03T21:46:40.5Z.
        let got = span_with(&[
            ("span.start_rfc3339", Value::str("2024-09-03T21:46:40.4Z")),
            ("span.end_rfc3339", Value::str("2024-09-03T23:46:40.5+02:00")),
        ])
        .unwrap();
        assert_eq!(got, (RECEIPT - 100_000_000, RECEIPT));
        assert_eq!(
            span_with(&[
                ("span.start_rfc3339", Value::str("yesterday")),
                ("span.duration", Value::I64(1))
            ]),
            Err("invalid")
        );
    }

    #[test]
    fn two_forms_of_one_quantity_are_invalid() {
        let s = RECEIPT - 10_000_000;
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(s)),
                ("span.start_ms", Value::I64(s / 1_000_000)),
                ("span.duration", Value::I64(1)),
            ]),
            Err("invalid")
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(s)),
                ("span.duration_ms", Value::I64(1)),
                ("span.duration_s", Value::F64(0.001)),
            ]),
            Err("invalid")
        );
    }

    #[test]
    fn negative_durations_and_ends_before_starts_are_timing_skips() {
        assert_eq!(
            span_with(&[("span.start", Value::I64(RECEIPT)), ("span.duration", Value::I64(-1))]),
            Err("timing")
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(RECEIPT)),
                ("span.end", Value::I64(RECEIPT - 1))
            ]),
            Err("timing")
        );
        assert_eq!(
            span_with(&[("span.start", Value::I64(RECEIPT + 1))]),
            Err("timing"),
            "a lone start after receipt would end before it began"
        );
    }

    #[test]
    fn arithmetic_overflow_is_a_skip_not_a_panic() {
        assert_eq!(
            span_with(&[("span.start_s", Value::I64(i64::MAX)), ("span.duration", Value::I64(1))]),
            Err("invalid"),
            "scaling overflows at parse time"
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(i64::MAX - 1)),
                ("span.duration", Value::I64(5))
            ]),
            Err("timing"),
            "start + duration overflows at resolution time"
        );
        assert_eq!(
            span_with(&[("span.start", Value::I64(i64::MIN)), ("span.end", Value::I64(i64::MAX))]),
            Err("skew")
        );
    }

    #[test]
    fn a_start_or_end_outside_max_skew_is_a_skew_skip() {
        let hour = 3_600 * NANOS_PER_SECOND;
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(RECEIPT - hour - 1)),
                ("span.duration", Value::I64(1))
            ]),
            Err("skew")
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(RECEIPT - hour)),
                ("span.duration", Value::I64(1))
            ]),
            Ok((RECEIPT - hour, RECEIPT - hour + 1)),
            "exactly at the window is fine"
        );
        assert_eq!(
            span_with(&[
                ("span.start", Value::I64(RECEIPT)),
                ("span.end", Value::I64(RECEIPT + hour + 1))
            ]),
            Err("skew")
        );
    }

    #[test]
    fn a_failed_span_lift_leaves_the_event_exactly_as_it_arrived() {
        let mut t = with_span();
        let mut attrs = span_attrs();
        attrs.push(("span.kind", Value::str("bogus")));
        let before = log_event(&attrs);
        let mut after = before.clone();
        assert!(t.process(&default_resource(), &mut after));
        assert_eq!(after.timestamp, before.timestamp);
        assert_eq!(after.log.as_ref().unwrap().trace, None);
        assert!(after.span.is_none());
        assert_eq!(after.attributes.len(), before.attributes.len());
        for (k, v) in &attrs {
            assert_eq!(after.attributes.get(k).map(|v| format!("{v:?}")), Some(format!("{v:?}")));
        }
    }

    #[test]
    fn an_event_with_no_log_passes_through_untouched_even_with_a_span_block() {
        let mut t = with_span();
        let mut event = metric_only_event();
        for (k, v) in span_attrs() {
            event.attributes.insert(k, v);
        }
        assert!(t.process(&default_resource(), &mut event));
        assert!(event.span.is_none());
        assert_eq!(event.attributes.len(), 4);
    }

    // -- `IdFormat::Datadog` ---------------------------------------------------------------------

    /// Datadog's documented examples: a decimal `dd.trace_id`, a 128-bit hex one, and a
    /// `_dd.p.tid` carrying that hex id's high half.
    const DD_DECIMAL: &str = "1234567890123456789";
    const DD_DECIMAL_LOW: u64 = 1_234_567_890_123_456_789;
    const DD_HEX_128: &str = "64de8e2b0000000012345678abcdef12";
    const DD_TID: &str = "64de8e2b00000000";
    const DD_TID_HIGH: u64 = 0x64de_8e2b_0000_0000;

    /// `format: datadog`'s resolved defaults (`logit_config`'s), no span block.
    fn datadog(trace_id_high: Option<&str>, keep_source: bool) -> TraceContext {
        TraceContext::new(
            "dd.trace_id".to_string(),
            Some("dd.span_id".to_string()),
            None,
            keep_source,
        )
        .with_format(IdFormat::Datadog { trace_id_high: trace_id_high.map(str::to_string) })
    }

    /// Runs `t` over `pairs` and returns the lifted reference, or the skip reason.
    fn lift_with(t: TraceContext, pairs: &[(&str, Value)]) -> Result<TraceRef, &'static str> {
        let (mut t, registry) = instrumented(t);
        let mut event = log_event(pairs);
        assert!(t.process(&default_resource(), &mut event));
        match event.log.unwrap().trace {
            Some(trace) => Ok(trace),
            None => {
                let drained = registry.drain(0);
                for reason in ["invalid", "missing", "span_id", "timing", "skew"] {
                    if find_counter(
                        &drained,
                        "logit.transform.trace_context.skipped",
                        Some(("reason", reason)),
                    )
                    .is_some()
                    {
                        return Err(reason);
                    }
                }
                panic!("no trace and no skip counter")
            }
        }
    }

    #[test]
    fn datadog_decimal_ids_are_lifted_into_the_low_half_and_consumed() {
        let mut t = datadog(None, false);
        let mut event = log_event(&[
            ("dd.trace_id", Value::str(DD_DECIMAL)),
            ("dd.span_id", Value::str("987654321")),
            ("dd.service", Value::str("web")),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        assert_eq!(
            event.log.unwrap().trace,
            Some(TraceRef {
                trace_id: trace_id_bytes(0, DD_DECIMAL_LOW),
                span_id: Some(987_654_321_u64.to_be_bytes()),
                flags: 0,
            })
        );
        assert!(event.attributes.get("dd.trace_id").is_none());
        assert!(event.attributes.get("dd.span_id").is_none());
        assert!(event.attributes.get("dd.service").is_some(), "only the ids are consumed");
    }

    #[test]
    fn datadog_accepts_a_128_bit_hex_trace_id() {
        let trace =
            lift_with(datadog(None, false), &[("dd.trace_id", Value::str(DD_HEX_128))]).unwrap();
        assert_eq!(trace_id_halves(&trace.trace_id), (DD_TID_HIGH, 0x1234_5678_abcd_ef12));
    }

    #[test]
    fn datadog_accepts_integer_ids_from_an_unquoted_json_number() {
        let trace = lift_with(
            datadog(None, false),
            &[("dd.trace_id", Value::U64(u64::MAX)), ("dd.span_id", Value::I64(42))],
        )
        .unwrap();
        assert_eq!(trace.trace_id, trace_id_bytes(0, u64::MAX));
        assert_eq!(trace.span_id, Some(42_u64.to_be_bytes()));
        for bad in [Value::I64(-1), Value::I64(0), Value::U64(0), Value::F64(1.0)] {
            assert_eq!(
                lift_with(datadog(None, false), &[("dd.trace_id", bad.clone())]),
                Err("invalid"),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_16_digit_id_is_hex_under_otel_and_decimal_under_datadog() {
        let digits = "1234567890123456";
        let otel = lift_with(
            convention(),
            &[("trace.id", Value::str(hex_trace())), ("span.id", Value::str(digits))],
        )
        .unwrap();
        assert_eq!(otel.span_id, Some([0x12, 0x34, 0x56, 0x78, 0x90, 0x12, 0x34, 0x56]));
        let dd = lift_with(
            datadog(None, false),
            &[("dd.trace_id", Value::str(digits)), ("dd.span_id", Value::str(digits))],
        )
        .unwrap();
        assert_eq!(dd.trace_id, trace_id_bytes(0, 1_234_567_890_123_456));
        assert_eq!(dd.span_id, Some(1_234_567_890_123_456_u64.to_be_bytes()));
    }

    #[test]
    fn datadog_rejects_16_hex_and_otel_rejects_decimal() {
        assert_eq!(
            lift_with(datadog(None, false), &[("dd.trace_id", Value::str(hex_span()))]),
            Err("invalid"),
            "16 hex is no Datadog form"
        );
        assert_eq!(
            lift_with(
                datadog(None, false),
                &[("dd.trace_id", Value::str(DD_DECIMAL)), ("dd.span_id", Value::str(hex_span()))]
            ),
            Err("invalid")
        );
        assert_eq!(
            lift_with(convention(), &[("trace.id", Value::str(DD_DECIMAL))]),
            Err("invalid"),
            "otel is unchanged: decimal is not an otel form"
        );
        assert_eq!(
            lift_with(datadog(None, false), &[("dd.trace_id", Value::str("18446744073709551616"))]),
            Err("invalid"),
            "above u64::MAX"
        );
    }

    #[test]
    fn trace_id_high_fills_a_zero_high_half_and_is_consumed() {
        let mut t = datadog(Some("_dd.p.tid"), false);
        let mut event = log_event(&[
            ("dd.trace_id", Value::str("1311768467750121234")),
            ("_dd.p.tid", Value::str(DD_TID)),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        let trace = event.log.unwrap().trace.unwrap();
        assert_eq!(trace.trace_id, logit_core::trace::parse_trace_id(DD_HEX_128).unwrap());
        assert!(event.attributes.get("_dd.p.tid").is_none(), "consumed with the ids");
    }

    #[test]
    fn trace_id_high_never_replaces_a_nonzero_high_half() {
        let trace = lift_with(
            datadog(Some("_dd.p.tid"), false),
            &[("dd.trace_id", Value::str(DD_HEX_128)), ("_dd.p.tid", Value::str("ffff"))],
        )
        .unwrap();
        assert_eq!(trace_id_halves(&trace.trace_id).0, DD_TID_HIGH, "the id's own high half wins");
    }

    #[test]
    fn trace_id_high_absent_leaves_the_high_half_zero_and_invalid_skips() {
        let trace = lift_with(
            datadog(Some("_dd.p.tid"), false),
            &[("dd.trace_id", Value::str(DD_DECIMAL))],
        )
        .unwrap();
        assert_eq!(trace_id_halves(&trace.trace_id), (0, DD_DECIMAL_LOW));
        for bad in [Value::str("0x64de"), Value::str("11112222333344445"), Value::U64(1)] {
            assert_eq!(
                lift_with(
                    datadog(Some("_dd.p.tid"), false),
                    &[("dd.trace_id", Value::str(DD_HEX_128)), ("_dd.p.tid", bad.clone())]
                ),
                Err("invalid"),
                "validated even when a 128-bit id leaves it unused: {bad:?}"
            );
        }
    }

    #[test]
    fn keep_source_keeps_trace_id_high_too() {
        let mut t = datadog(Some("_dd.p.tid"), true);
        let mut event = log_event(&[
            ("dd.trace_id", Value::str(DD_DECIMAL)),
            ("dd.span_id", Value::str("7")),
            ("_dd.p.tid", Value::str(DD_TID)),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        assert!(event.log.unwrap().trace.is_some());
        assert_eq!(event.attributes.len(), 3, "nothing consumed: {:?}", event.attributes);
    }

    #[test]
    fn a_traceparent_is_still_honored_under_datadog() {
        let trace =
            lift_with(datadog(None, false), &[("traceparent", Value::str(W3C_EXAMPLE))]).unwrap();
        assert_eq!(trace, TraceRef { trace_id: W3C_TRACE, span_id: None, flags: 1 });
        let trace = lift_with(
            datadog(None, false),
            &[("traceparent", Value::str(W3C_EXAMPLE)), ("dd.trace_id", Value::str(DD_DECIMAL))],
        )
        .unwrap();
        assert_eq!(trace.trace_id, trace_id_bytes(0, DD_DECIMAL_LOW), "an explicit id wins");
        assert_eq!(trace.flags, 1, "flags still come from the header");
    }

    #[test]
    fn the_span_block_works_under_datadog_with_hex_w3c_parents() {
        let mut t = datadog(Some("_dd.p.tid"), false).with_span(span_lift());
        let mut event = log_event(&[
            ("dd.trace_id", Value::str(DD_DECIMAL)),
            ("dd.span_id", Value::str("42")),
            ("_dd.p.tid", Value::str(DD_TID)),
            ("span.parent_id", Value::str("ef".repeat(8))),
            ("span.start", Value::I64(RECEIPT - 10_000_000)),
            ("span.duration", Value::I64(4_000_000)),
        ]);
        assert!(t.process(&default_resource(), &mut event));
        let span = event.span.as_ref().expect("a span");
        assert_eq!(span.trace_id, trace_id_bytes(DD_TID_HIGH, DD_DECIMAL_LOW));
        assert_eq!(span.span_id, 42_u64.to_be_bytes());
        assert_eq!(span.parent_span_id, Some([0xef; 8]), "span.parent_id stays W3C hex");
        assert_eq!(event.timestamp, RECEIPT - 10_000_000);
        assert_eq!(event.log.as_ref().unwrap().trace.unwrap().span_id, Some(42_u64.to_be_bytes()));
        assert!(
            event.attributes.is_empty(),
            "every read attribute consumed: {:?}",
            event.attributes
        );
    }
}
