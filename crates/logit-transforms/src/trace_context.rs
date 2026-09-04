//! `trace_context`: lifts an application trace/span reference off an event's attributes onto its
//! `LogRecord` (`docs/adr/log-record-trace-context.md`) -- the common "my JSON log body already
//! has a `trace.id` field" case, without writing Lua (`event.log.trace_id`,
//! `crates/logit-script/src/proxy.rs`'s `LogProxy`) -- and, with a `span:` block
//! (`docs/adr/trace-context-span-lifting.md`), turns that log line into a real `SpanRecord` on
//! the same event: an access log's ids plus its own start/end/duration become a server span
//! whose start is the event's timestamp, so a request crossing haproxy -> nginx -> app shows up
//! in a trace store as one trace with one span per tier.
//!
//! Reads the well-known attribute names in `docs/design/data-model.md`'s "Well-known attribute
//! names" section: `traceparent` (W3C, parsed natively -- `logit_core::trace::parse_traceparent`),
//! `trace.id`/`trace.flags`/`span.id` (the three renameable ones), `span.parent_id`, `span.name`,
//! `span.kind`, `span.status`, and the timing attributes `span.start`/`span.end`/`span.duration`
//! in integer nanoseconds or one of their unit-suffixed forms (`_us`/`_ms`/`_s`/`_rfc3339`).
//! Everything is parsed before anything is mutated: a lift either applies completely (log trace
//! set, span minted, timestamp rewritten, consumed attributes removed) or leaves the event
//! exactly as it arrived and counts one `.skipped{reason}` -- never a partial result.

use logit_core::trace::{parse_span_id, parse_trace_id};
use logit_core::{
    parse_decimal_nanos, parse_rfc3339_to_nanos, parse_traceparent, random_id_bytes, AttrMap,
    Event, Resource, SpanKind, SpanRecord, SpanStatus, Telemetry, TraceRef, Value,
};
use logit_pipeline::Transform;
use std::sync::Arc;
use std::time::Duration;

/// The W3C header, logged as-is by a tier that received one -- yields the trace id, this span's
/// *parent* id, and the flags, each overridable by the explicit attribute.
const TRACEPARENT: &str = "traceparent";
const SPAN_PARENT_ID: &str = "span.parent_id";
const SPAN_NAME: &str = "span.name";
const SPAN_KIND: &str = "span.kind";
const SPAN_STATUS: &str = "span.status";

/// How a timing attribute's value is denominated -- the unit is only ever in the attribute's
/// *name* (`docs/design/data-model.md`), never inside the value. The base form is integer
/// nanoseconds, OTLP's own `*_time_unix_nano` unit; the suffixed forms exist for producers whose
/// clocks are coarser than that (haproxy's `request_date(us)`, nginx's `$msec`), as an honest
/// label of the source's resolution rather than a convenience.
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

/// The `span:` block, as the transform consumes it (`logit_config::SpanLiftConfig` is the config
/// vocabulary; `crates/logit-cli/src/pipeline.rs` maps between them).
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

/// [`SpanLift`] pre-digested for the per-event path: the default name is a `Value` built once
/// (so the success path only refcount-bumps its `Bytes`), the skew window is already in nanos.
struct SpanDefaults {
    mint_id: bool,
    name: Value,
    kind: SpanKind,
    max_skew_nanos: u64,
}

/// Why a lift didn't apply -- each is a `reason` tag on `.skipped`. All `&'static str` by
/// construction, per `docs/design/internal-telemetry.md`'s cardinality rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Skip {
    /// No trace id anywhere: neither the configured attribute nor a `traceparent`.
    Missing,
    /// Something present didn't parse: an id, the flags, a `traceparent`, a `span.kind`/
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

/// Everything a successful lift will write, computed against a borrowed `AttrMap` before
/// anything is mutated -- the all-or-nothing contract in one type.
struct Lifted {
    trace: TraceRef,
    /// `Some` only with a `span:` block; the span's start is `timestamp` below.
    span: Option<SpanRecord>,
    timestamp: Option<i64>,
    minted: bool,
}

/// An attribute counts as present only if it carries a value: `Null`, `""`, and `"-"` are how
/// nginx (`escape=json` renders an unset variable as `""`, the plain formats as `-`) and haproxy
/// (an unset `txn` var) spell "nothing here," and treating them as present-but-invalid would
/// make every non-proxied or edge request a noisy skip instead of a quiet one.
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

/// `flags` as a plain integer 0-255, decimal only -- never hex, since a W3C `traceparent`'s own
/// two-hex-digit flags octet would silently parse as a *different* decimal value if accepted
/// here (`"08"` is 8 as decimal, not the flags byte `0x08`). A `traceparent` is parsed whole by
/// `parse_traceparent`, where the octet *is* hex by that header's definition; this reads only a
/// standalone numeric field, and the two never mix.
fn numeric_flags(value: &Value) -> Option<u8> {
    match value {
        Value::I64(n) => u8::try_from(*n).ok(),
        Value::U64(n) => u8::try_from(*n).ok(),
        Value::Str(_) => value.as_str()?.parse::<u16>().ok().and_then(|n| u8::try_from(n).ok()),
        _ => None,
    }
}

fn span_kind(name: &str) -> Option<SpanKind> {
    Some(match name {
        "server" => SpanKind::Server,
        "client" => SpanKind::Client,
        "producer" => SpanKind::Producer,
        "consumer" => SpanKind::Consumer,
        "internal" => SpanKind::Internal,
        _ => return None,
    })
}

fn span_status(name: &str) -> Option<SpanStatus> {
    Some(match name {
        "ok" => SpanStatus::Ok,
        "error" => SpanStatus::Error,
        "unset" => SpanStatus::Unset,
        _ => return None,
    })
}

/// One timing value to exact nanoseconds, by the unit its attribute name declared. `instant`
/// distinguishes a start/end (which may also arrive as a `Value::Timestamp` or, under the
/// `_rfc3339` form, a date string) from a duration (which may not). A float in an integer-
/// denominated form (`Nanos`/`Micros`/`Millis`) is rejected outright rather than rounded: an
/// `f64` can't represent an epoch-nanosecond instant (2^53 < 1.7e18), so a JSON float there is a
/// producer bug, not a value to guess at. The `Seconds` form is the one place a float is
/// legitimate (nginx's `$msec`/`$request_time`), and a `Str` there is walked digit-exact.
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

/// `f64` seconds to nanoseconds without going through one `f64` multiply: the integer part is
/// scaled exactly (in `i128`, so a huge float is a `None`, not a wrap), only the sub-second part
/// is rounded. At epoch magnitude an `f64` carries ~16 significant digits, so the fraction is
/// good to roughly a microsecond -- already finer than any producer that emits float seconds.
fn f64_seconds_to_nanos(seconds: f64) -> Option<i64> {
    if !seconds.is_finite() {
        return None;
    }
    let whole = seconds.trunc();
    let frac = seconds - whole;
    // `as i128` saturates on out-of-range floats rather than wrapping; the range check below
    // then catches it.
    let whole_nanos = (whole as i128).checked_mul(i128::from(NANOS_PER_SECOND))?;
    let whole_nanos = i64::try_from(whole_nanos).ok()?;
    let frac_nanos = (frac * NANOS_PER_SECOND as f64).round() as i64;
    whole_nanos.checked_add(frac_nanos)
}

/// Looks one quantity (start, end, or duration) up across every form it may arrive in. Exactly
/// one form may be present -- two (`span.start` and `span.start_ms`, say) is a contradiction
/// this refuses to resolve by precedence. `Ok(None)` is "not supplied at all."
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

/// Lifts `trace_id`/`span_id`/`flags` off configured attribute names (and, with [`with_span`],
/// the rest of the span convention) onto `event.log.trace` and `event.span`, overwriting on
/// success -- operator intent, the same posture `Set` has toward wire-carried data. Stateless:
/// no `flush_interval`/`flush`.
///
/// [`with_span`]: TraceContext::with_span
pub struct TraceContext {
    trace_id_field: String,
    span_id_field: Option<String>,
    flags_field: Option<String>,
    keep_source: bool,
    span: Option<SpanDefaults>,
    telemetry: Telemetry,
}

impl TraceContext {
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
            keep_source,
            span: None,
            telemetry: Telemetry::default(),
        }
    }

    /// Opts in to minting a `SpanRecord` per lifted line -- the `span:` block.
    pub fn with_span(mut self, span: SpanLift) -> Self {
        self.span = Some(SpanDefaults {
            mint_id: span.mint_id,
            name: Value::str(span.name),
            kind: span.kind,
            max_skew_nanos: u64::try_from(span.max_skew.as_nanos()).unwrap_or(u64::MAX),
        });
        self
    }

    /// See [`crate::Set::with_telemetry`] -- same reasoning, no `Diagnostics` here either: a
    /// missing or unparseable attribute is a documented skip, not a warning-worthy failure.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// The whole lift, computed without touching the event. Precedence throughout: an explicit
    /// attribute beats the corresponding piece of a `traceparent`.
    fn lift(&self, attrs: &AttrMap, receipt: i64) -> Result<Lifted, Skip> {
        let traceparent = match present(attrs, TRACEPARENT) {
            None => None,
            Some(value) => Some(value.as_str().and_then(parse_traceparent).ok_or(Skip::Invalid)?),
        };

        let trace_id = match present(attrs, &self.trace_id_field) {
            Some(value) => value.as_str().and_then(parse_trace_id).ok_or(Skip::Invalid)?,
            None => traceparent.map(|(trace, _, _)| trace).ok_or(Skip::Missing)?,
        };

        let flags = match self.flags_field.as_deref().and_then(|field| present(attrs, field)) {
            // Present but unparseable is a harder failure than absent -- an operator configured
            // this field expecting it to mean something, so silently dropping it (as "absent"
            // would) could hide a real upstream problem.
            Some(value) => numeric_flags(value).ok_or(Skip::Invalid)?,
            None => traceparent.map(|(_, _, flags)| flags).unwrap_or(0),
        };

        let own_span_id =
            match self.span_id_field.as_deref().and_then(|field| present(attrs, field)) {
                Some(value) => Some(value.as_str().and_then(parse_span_id).ok_or(Skip::Invalid)?),
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
            Some(value) => value.as_str().and_then(span_kind).ok_or(Skip::Invalid)?,
            None => defaults.kind,
        };
        let status = match present(attrs, SPAN_STATUS) {
            Some(value) => value.as_str().and_then(span_status).ok_or(Skip::Invalid)?,
            None => SpanStatus::Unset,
        };

        let start = quantity(attrs, &START_FORMS, true)?;
        let end = quantity(attrs, &END_FORMS, true)?;
        let duration = quantity(attrs, &DURATION_FORMS, false)?;
        if duration.is_some_and(|d| d < 0) {
            return Err(Skip::Timing);
        }
        // Any two determine the third; a lone start or duration borrows receipt time as the
        // end (a line written at request end arrives moments later, so this is the honest
        // fallback -- and the only way an unchanged nginx line carrying just `request_time`
        // yields a span at all). A lone end is not enough: nothing says when it began.
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
            }),
            timestamp: Some(start),
            minted,
        })
    }

    /// Removes exactly the convention attributes this configuration reads -- an absent key is a
    /// free probe, so this needn't track which were actually present.
    fn remove_consumed(&self, attrs: &mut AttrMap) {
        attrs.remove(&self.trace_id_field);
        if let Some(field) = &self.span_id_field {
            attrs.remove(field);
        }
        if let Some(field) = &self.flags_field {
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
    /// An event with no log passes through untouched -- there is nowhere to put a lifted
    /// `TraceRef`, and no access line to derive a span from. Otherwise the outcome is one of:
    /// a complete lift (`.lifted`, plus `.spans{id}` with a `span:` block), or a
    /// `.skipped{reason}` that leaves the event exactly as it arrived -- attributes, timestamp,
    /// `log.trace`, and `span` all untouched. `missing` is "no trace id at all"; `invalid` is
    /// "something present didn't parse"; `span_id`, `timing`, and `skew` are the `span:` block's
    /// own reasons (see [`Skip`]). A successful lift overwrites `log.trace` (and, with `span:`,
    /// `event.span` and `event.timestamp` -- the span's start) and, unless `keep_source`,
    /// removes every convention attribute it read.
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        if event.log.is_none() {
            return Some(event);
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
        Some(event)
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
            },
        )
    }

    fn metric_only_event() -> Event {
        Event::metric(
            0,
            AttrMap::new(),
            logit_core::MetricRecord {
                name: logit_core::interner::intern("m"),
                kind: logit_core::MetricKind::Counter(1.0),
                unit: None,
            },
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

    /// The pre-convention shape: explicit underscore field names, no span block.
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

    /// Finds one counter in an already-drained set of telemetry events. Callers drain once --
    /// `Registry::drain` empties the buffer, so two lookups against the registry itself would
    /// only ever find the first.
    fn find_counter(events: &[Event], name: &str, tag: Option<(&str, &str)>) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name) == name
                        && tag.is_none_or(|(k, want)| {
                            e.attributes.get(k).and_then(|v| v.as_str()) == Some(want)
                        }) =>
                {
                    Some(*v)
                }
                _ => None,
            })
        })
    }

    /// One-shot lookup: drains the registry. Use [`find_counter`] on a single drain when a test
    /// needs more than one counter.
    fn counter(registry: &Registry, name: &str, tag: Option<(&str, &str)>) -> Option<f64> {
        find_counter(&registry.drain(0), name, tag)
    }

    fn instrumented(t: TraceContext) -> (TraceContext, Arc<Registry>) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("web_trace", "trace_context", "transform");
        (t.with_telemetry(telemetry), registry)
    }

    // -- The log-only lift, unchanged in behavior from before the span block ---------------------

    #[test]
    fn a_valid_trace_id_is_lifted_and_removed_by_default() {
        let mut t = legacy();
        let event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        let event = t.process(&default_resource(), event).unwrap();
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
        let event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        let event = t.process(&default_resource(), event).unwrap();
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
        let event = log_event(&[
            ("trace_id", Value::str(hex_trace())),
            ("span_id", Value::str(hex_span())),
            ("flags", Value::I64(1)),
        ]);
        let event = t.process(&default_resource(), event).unwrap();
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
        let event = log_event(&[("trace_id", Value::str(hex_trace())), ("flags", Value::str("1"))]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace.unwrap().flags, 1);
    }

    #[test]
    fn a_missing_trace_id_attribute_is_skipped_not_an_error() {
        let mut t = legacy();
        let event = log_event(&[]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace, None);
    }

    #[test]
    fn an_unparseable_trace_id_is_skipped_and_leaves_the_attribute_in_place() {
        let mut t = legacy();
        let event = log_event(&[("trace_id", Value::str("not-hex"))]);
        let event = t.process(&default_resource(), event).unwrap();
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
        let event =
            log_event(&[("trace_id", Value::str(hex_trace())), ("span_id", Value::str("not-hex"))]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace, None, "an invalid span_id must not partially apply");
        assert!(event.attributes.get("trace_id").is_some());
        assert!(event.attributes.get("span_id").is_some());
    }

    #[test]
    fn a_configured_span_id_field_absent_from_the_event_is_not_an_error() {
        let mut t =
            TraceContext::new("trace_id".to_string(), Some("span_id".to_string()), None, false);
        let event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        let event = t.process(&default_resource(), event).unwrap();
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
        let event = t.process(&default_resource(), event).unwrap();
        assert!(event.log.is_none());
        assert!(event.attributes.get("trace_id").is_some(), "nothing should have been touched");
    }

    #[test]
    fn a_successful_lift_overwrites_an_existing_trace() {
        let mut t = legacy();
        let mut event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        event.log.as_mut().unwrap().trace =
            Some(TraceRef { trace_id: [1; 16], span_id: None, flags: 0 });
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace.unwrap().trace_id, [0xab; 16]);
    }

    #[test]
    fn a_missing_lift_records_a_skipped_missing_counter() {
        let (mut t, registry) = instrumented(legacy());
        t.process(&default_resource(), log_event(&[])).unwrap();
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
        t.process(&default_resource(), log_event(&[("trace_id", Value::str(hex_trace()))]))
            .unwrap();
        assert_eq!(counter(&registry, "logit.transform.trace_context.lifted", None), Some(1.0));
    }

    // -- The convention: defaults, `traceparent`, absent spellings ------------------------------

    #[test]
    fn the_convention_defaults_lift_dotted_names_with_no_overrides() {
        let mut t = convention();
        let event = log_event(&[
            ("trace.id", Value::str(hex_trace())),
            ("span.id", Value::str(hex_span())),
            ("trace.flags", Value::U64(1)),
        ]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(
            event.log.unwrap().trace,
            Some(TraceRef { trace_id: [0xab; 16], span_id: Some([0xcd; 8]), flags: 1 })
        );
        assert!(event.attributes.is_empty(), "all three consumed: {:?}", event.attributes);
    }

    #[test]
    fn a_traceparent_alone_lifts_trace_id_and_flags_but_never_becomes_the_logs_span_id() {
        let mut t = convention();
        let event = log_event(&[("traceparent", Value::str(W3C_EXAMPLE))]);
        let event = t.process(&default_resource(), event).unwrap();
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
        let event = log_event(&[("traceparent", Value::str(header.clone()))]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace.unwrap().flags, 0x10, "header octet is hex");

        let event =
            log_event(&[("traceparent", Value::str(header)), ("trace.flags", Value::str("10"))]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(
            event.log.unwrap().trace.unwrap().flags,
            10,
            "explicit field is decimal, and wins"
        );
    }

    #[test]
    fn an_explicit_trace_id_beats_the_traceparents() {
        let mut t = convention();
        let event = log_event(&[
            ("traceparent", Value::str(W3C_EXAMPLE)),
            ("trace.id", Value::str(hex_trace())),
        ]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace.unwrap().trace_id, [0xab; 16]);
    }

    #[test]
    fn a_malformed_traceparent_is_invalid_even_with_a_valid_explicit_trace_id() {
        let (mut t, registry) = instrumented(convention());
        let event = log_event(&[
            ("traceparent", Value::str("00-nope")),
            ("trace.id", Value::str(hex_trace())),
        ]);
        let event = t.process(&default_resource(), event).unwrap();
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
            let event = log_event(&[
                ("trace.id", Value::str(hex_trace())),
                ("span.id", absent.clone()),
                ("traceparent", absent.clone()),
                ("trace.flags", absent),
            ]);
            let event = t.process(&default_resource(), event).unwrap();
            assert_eq!(
                event.log.unwrap().trace,
                Some(TraceRef { trace_id: [0xab; 16], span_id: None, flags: 0 })
            );
        }
        let event = log_event(&[("trace.id", Value::str(""))]);
        let (mut t, registry) = instrumented(convention());
        t.process(&default_resource(), event).unwrap();
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
        let event =
            log_event(&[("trace.id", Value::str(hex_trace())), ("span.id", Value::str("not-hex"))]);
        let event = t.process(&default_resource(), event).unwrap();
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
        let event = t.process(&default_resource(), log_event(&attrs)).unwrap();

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
        let event = t.process(&default_resource(), log_event(&attrs)).unwrap();
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
        let event = t.process(&default_resource(), log_event(&attrs)).unwrap();
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
        let event = t.process(&default_resource(), log_event(&attrs)).unwrap();
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
            let event = t.process(&default_resource(), log_event(&attrs)).unwrap();
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
        let event = t.process(&default_resource(), log_event(&attrs)).unwrap();
        assert!(event.span.is_some());
        assert_eq!(event.attributes.len(), attrs.len());
    }

    // -- Timing: every form, every resolution --------------------------------------------------

    fn span_with(timing: &[(&str, Value)]) -> Result<(i64, i64), &'static str> {
        let (mut t, registry) = instrumented(with_span());
        let mut attrs =
            vec![("trace.id", Value::str(hex_trace())), ("span.id", Value::str(hex_span()))];
        attrs.extend(timing.iter().cloned());
        let event = t.process(&default_resource(), log_event(&attrs)).unwrap();
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
        // nginx: `$msec` as a JSON float (ms resolution), `$request_time` likewise. An f64 at
        // epoch magnitude resolves to ~0.24us, so the float route lands within a microsecond of
        // the decimal it was printed from -- finer than the ms the source actually had, but
        // *not* exact; the quoted route below is.
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
        // RECEIPT = 2024-09-03T21:46:40.5Z, exactly.
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
        let after = t.process(&default_resource(), before.clone()).unwrap();
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
        let event = t.process(&default_resource(), event).unwrap();
        assert!(event.span.is_none());
        assert_eq!(event.attributes.len(), 4);
    }
}
