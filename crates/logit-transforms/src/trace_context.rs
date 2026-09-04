//! `trace_context`: lifts an application trace/span reference off an event's attributes onto its
//! `LogRecord` (`docs/adr/log-record-trace-context.md`) -- the common "my JSON log body already
//! has a `trace_id` field" case, without writing Lua (`event.log.trace_id`,
//! `crates/logit-script/src/proxy.rs`'s `LogProxy`).

use logit_core::trace::{parse_span_id, parse_trace_id};
use logit_core::{Event, Resource, Telemetry, TraceRef, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Which named attribute, if any, holds the flags value, decimal (never hex) -- see
/// [`TraceContext::process`]'s doc comment for the accepted `Value` shapes.
struct Lift<'a> {
    trace_id: &'a str,
    span_id: Option<&'a str>,
    flags: Option<&'a str>,
}

/// The outcome of attempting a lift for one event -- distinguishes "nothing to lift" from "found
/// something but it didn't parse," since only the latter is worth a `reason="invalid"` telemetry
/// point (`.skipped{reason="missing"}` for the former).
enum LiftOutcome {
    Lifted(TraceRef),
    Missing,
    Invalid,
}

fn lift(fields: &Lift, attrs: &logit_core::AttrMap) -> LiftOutcome {
    let Some(trace_id_value) = attrs.get(fields.trace_id) else {
        return LiftOutcome::Missing;
    };
    let Some(trace_id) = trace_id_value.as_str().and_then(parse_trace_id) else {
        return LiftOutcome::Invalid;
    };

    let span_id = match fields.span_id.and_then(|field| attrs.get(field)) {
        None => None,
        Some(value) => match value.as_str().and_then(parse_span_id) {
            Some(id) => Some(id),
            // Present but unparseable is a harder failure than absent -- an operator configured
            // this field expecting it to mean something, so silently dropping it (as "absent"
            // would) could hide a real upstream problem.
            None => return LiftOutcome::Invalid,
        },
    };

    let flags = match fields.flags.and_then(|field| attrs.get(field)) {
        None => 0,
        Some(value) => match numeric_flags(value) {
            Some(f) => f,
            None => return LiftOutcome::Invalid,
        },
    };

    LiftOutcome::Lifted(TraceRef { trace_id, span_id, flags })
}

/// `flags` as a plain integer 0-255, decimal only -- never hex, since a W3C `traceparent`'s own
/// two-hex-digit flags octet would silently parse as a *different* decimal value if accepted
/// here (`"08"` is 8 as decimal, not the flags byte `0x08`). Splitting a `traceparent` string is
/// a script's job (`docs/design/lua-api.md`); this transform only reads an already-numeric field.
fn numeric_flags(value: &Value) -> Option<u8> {
    match value {
        Value::I64(n) => u8::try_from(*n).ok(),
        Value::U64(n) => u8::try_from(*n).ok(),
        Value::Str(_) => value.as_str()?.parse::<u16>().ok().and_then(|n| u8::try_from(n).ok()),
        _ => None,
    }
}

/// Lifts `trace_id`/`span_id`/`flags` off configured attribute names onto `event.log.trace`,
/// overwriting on success -- operator intent, the same posture `Set` has toward wire-carried
/// data. Stateless: no `flush_interval`/`flush`.
pub struct TraceContext {
    trace_id_field: String,
    span_id_field: Option<String>,
    flags_field: Option<String>,
    keep_source: bool,
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
            telemetry: Telemetry::default(),
        }
    }

    /// See [`crate::Set::with_telemetry`] -- same reasoning, no `Diagnostics` here either: a
    /// missing or unparseable attribute is a documented skip, not a warning-worthy failure.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for TraceContext {
    /// An event with no log passes through untouched -- there is nowhere to put a lifted
    /// `TraceRef`. Otherwise: `trace_id_field` missing from `event.attributes` is
    /// `.skipped{reason="missing"}`; present but not a 32-character hex string (or `span_id`/
    /// `flags`, if configured, present but unparseable) is `.skipped{reason="invalid"}` --
    /// neither ever fails the event or touches its existing `log.trace`. A successful lift
    /// overwrites `log.trace` and, unless `keep_source`, removes exactly the attribute(s) it
    /// read -- counted `.lifted`.
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        if event.log.is_none() {
            return Some(event);
        }
        let fields = Lift {
            trace_id: &self.trace_id_field,
            span_id: self.span_id_field.as_deref(),
            flags: self.flags_field.as_deref(),
        };
        match lift(&fields, &event.attributes) {
            LiftOutcome::Lifted(trace) => {
                event.log.as_mut().expect("checked above").trace = Some(trace);
                if !self.keep_source {
                    event.attributes.remove(&self.trace_id_field);
                    if let Some(field) = &self.span_id_field {
                        event.attributes.remove(field);
                    }
                    if let Some(field) = &self.flags_field {
                        event.attributes.remove(field);
                    }
                }
                self.telemetry.count("logit.transform.trace_context.lifted", 1.0, &[]);
            }
            LiftOutcome::Missing => {
                self.telemetry.count(
                    "logit.transform.trace_context.skipped",
                    1.0,
                    &[("reason", "missing")],
                );
            }
            LiftOutcome::Invalid => {
                self.telemetry.count(
                    "logit.transform.trace_context.skipped",
                    1.0,
                    &[("reason", "invalid")],
                );
            }
        }
        Some(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, Registry};

    fn log_event(pairs: &[(&str, Value)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, v.clone());
        }
        Event::log(
            0,
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

    #[test]
    fn a_valid_trace_id_is_lifted_and_removed_by_default() {
        let mut t = TraceContext::new("trace_id".to_string(), None, None, false);
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
        let mut t = TraceContext::new("trace_id".to_string(), None, None, false);
        let event = log_event(&[]);
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace, None);
    }

    #[test]
    fn an_unparseable_trace_id_is_skipped_and_leaves_the_attribute_in_place() {
        let mut t = TraceContext::new("trace_id".to_string(), None, None, false);
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
        let mut t = TraceContext::new("trace_id".to_string(), None, None, false);
        let mut event = metric_only_event();
        event.attributes.insert("trace_id", Value::str(hex_trace()));
        let event = t.process(&default_resource(), event).unwrap();
        assert!(event.log.is_none());
        assert!(event.attributes.get("trace_id").is_some(), "nothing should have been touched");
    }

    #[test]
    fn a_successful_lift_overwrites_an_existing_trace() {
        let mut t = TraceContext::new("trace_id".to_string(), None, None, false);
        let mut event = log_event(&[("trace_id", Value::str(hex_trace()))]);
        event.log.as_mut().unwrap().trace =
            Some(TraceRef { trace_id: [1; 16], span_id: None, flags: 0 });
        let event = t.process(&default_resource(), event).unwrap();
        assert_eq!(event.log.unwrap().trace.unwrap().trace_id, [0xab; 16]);
    }

    #[test]
    fn a_missing_lift_records_a_skipped_missing_counter() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("web_trace", "trace_context", "transform");
        let mut t =
            TraceContext::new("trace_id".to_string(), None, None, false).with_telemetry(telemetry);
        t.process(&default_resource(), log_event(&[])).unwrap();

        let events = registry.drain(0);
        let skipped = events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.trace_context.skipped"
                        && e.attributes.get("reason").and_then(|v| v.as_str())
                            == Some("missing") =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert_eq!(skipped, Some(1.0));
    }

    #[test]
    fn a_successful_lift_records_a_lifted_counter() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("web_trace", "trace_context", "transform");
        let mut t =
            TraceContext::new("trace_id".to_string(), None, None, false).with_telemetry(telemetry);
        t.process(&default_resource(), log_event(&[("trace_id", Value::str(hex_trace()))]))
            .unwrap();

        let events = registry.drain(0);
        let lifted = events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.trace_context.lifted" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert_eq!(lifted, Some(1.0));
    }
}
