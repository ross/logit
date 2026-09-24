//! `regex`: matches a pattern against a log message (or, with `field:`, a named attribute),
//! turning every named capture group into an attribute of that name. See
//! `docs/adr/regex-transform.md`.
//!
//! No flush state, but `RegexParser` reuses one `CaptureLocations` across events.
//!
//! A root module named `regex` makes `use regex::Regex;` ambiguous with the extern crate (E0659);
//! the leading `::` below disambiguates.

use ::regex::{CaptureLocations, Regex};
use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

pub struct RegexParser {
    re: Regex,
    /// Interned attribute name per capture-group index; `None` for group 0 and unnamed groups.
    names: Vec<Option<Symbol>>,
    /// Filled in place by `captures_read`, so no `Captures` is allocated per line.
    locs: CaptureLocations,
    /// `None` matches `log.message`; `Some` matches that attribute instead.
    field: Option<Symbol>,
    telemetry: Telemetry,
}

impl RegexParser {
    /// Compiles `pattern`.
    ///
    /// Graph rule 31 already compiled the same pattern, so the error is unreachable from
    /// `logit-cli`'s `build_spec`.
    pub fn new(pattern: &str, field: Option<&str>) -> Result<Self, ::regex::Error> {
        let re = Regex::new(pattern)?;
        let names = re.capture_names().map(|n| n.map(intern)).collect();
        let locs = re.capture_locations();
        Ok(Self { re, names, locs, field: field.map(intern), telemetry: Telemetry::default() })
    }

    /// Attaches a telemetry handle.
    ///
    /// There's no `Diagnostics` builder: a non-matching line is a skip, never an error.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for RegexParser {
    /// Writes each named capture of the first match into `event.attributes`.
    ///
    /// Each value is a `Value::Str` slice of the matched buffer, never a fresh `String`. A
    /// non-participating or empty capture adds no attribute (not `Value::Null`, not `""`).
    ///
    /// No log, a message or `field` attribute that isn't `Str`/`Bytes`, non-UTF-8 bytes, or no
    /// match all pass the event through untouched with no diagnostic. Every event records one of
    /// `logit.transform.matched`/`.matched.skipped`, however many attributes it gained.
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        let bytes: Bytes = match self.field {
            None => match event.log.as_ref().map(|log| &log.message) {
                Some(Value::Str(b) | Value::Bytes(b)) => b.clone(),
                _ => {
                    self.telemetry.count("logit.transform.matched.skipped", 1.0, &[]);
                    return true;
                }
            },
            Some(field) => match event.attributes.get_sym(field) {
                Some(Value::Str(b) | Value::Bytes(b)) => b.clone(),
                _ => {
                    self.telemetry.count("logit.transform.matched.skipped", 1.0, &[]);
                    return true;
                }
            },
        };

        let Ok(hay) = std::str::from_utf8(&bytes) else {
            self.telemetry.count("logit.transform.matched.skipped", 1.0, &[]);
            return true;
        };

        if self.re.captures_read(&mut self.locs, hay).is_none() {
            self.telemetry.count("logit.transform.matched.skipped", 1.0, &[]);
            return true;
        }

        for i in 1..self.locs.len() {
            let Some(sym) = self.names[i] else { continue };
            if let Some((start, end)) = self.locs.get(i) {
                if end > start {
                    event.attributes.insert_sym(sym, Value::Str(bytes.slice(start..end)));
                }
            }
        }

        self.telemetry.count("logit.transform.matched", 1.0, &[]);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::{AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, Registry};
    use logit_core::{SpanEvent, SpanKind, SpanRecord, SpanStatus};

    fn log_event(message: &str) -> Event {
        Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str(message),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn event_with_attrs(attrs: &[(&str, Value)]) -> Event {
        let mut map = AttrMap::new();
        for (k, v) in attrs {
            map.insert(k, v.clone());
        }
        Event::log(
            0,
            map,
            LogRecord {
                message: Value::str("unused"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn attr<'a>(event: &'a Event, key: &str) -> Option<&'a Value> {
        event.attributes.get(key)
    }

    #[test]
    fn a_matching_line_populates_every_named_capture() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+) path=(?P<path>\S+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("status=200 path=/health");
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
        assert_eq!(attr(&event, "path"), Some(&Value::str("/health")));
    }

    #[test]
    fn only_the_first_match_contributes() {
        let mut re = RegexParser::new(r"id=(?P<id>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("id=1 and then id=2");
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "id"), Some(&Value::str("1")));
    }

    #[test]
    fn an_unnamed_group_contributes_no_attribute() {
        let mut re = RegexParser::new(r"(\d+)-(?P<id>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("42-99");
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "id"), Some(&Value::str("99")));
        assert_eq!(event.attributes.len(), 1, "the unnamed group must not add an attribute");
    }

    #[test]
    fn a_non_participating_optional_group_omits_its_attribute() {
        let mut re = RegexParser::new(r"(?P<a>x)?(?P<b>y)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("y");
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), None, "a non-participating group must add no key at all");
        assert_eq!(attr(&event, "b"), Some(&Value::str("y")));
    }

    #[test]
    fn a_group_matching_the_empty_string_omits_its_attribute() {
        let mut re = RegexParser::new(r"(?P<a>x*)(?P<b>y)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("y");
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), None, "an empty-string capture must add no key at all");
        assert_eq!(attr(&event, "b"), Some(&Value::str("y")));
    }

    #[test]
    fn a_numeric_looking_capture_stays_a_str() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("status=200");
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
        assert!(
            !matches!(attr(&event, "status"), Some(Value::U64(_)) | Some(Value::I64(_))),
            "a regex capture must never coerce to a numeric Value"
        );
    }

    #[test]
    fn a_non_matching_line_passes_through_with_attributes_untouched() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("no match here");
        event.attributes.insert("existing", Value::str("kept"));
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(event.attributes.len(), 1);
        assert_eq!(attr(&event, "existing"), Some(&Value::str("kept")));
    }

    #[test]
    fn a_non_string_message_passes_through_untouched() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::I64(200),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        assert!(re.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_non_utf8_message_passes_through_untouched() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::Bytes(Bytes::from_static(&[0xff, 0xfe, 0xfd])),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        assert!(re.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_metric_only_event_passes_through_untouched() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern("m"), MetricKind::counter(1.0)),
        );
        assert!(re.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_span_only_event_passes_through_untouched() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = Event::span(
            0,
            AttrMap::new(),
            SpanRecord {
                trace_id: [0; 16],
                span_id: [0; 8],
                parent_span_id: None,
                name: Value::str("span"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: Vec::<SpanEvent>::new(),
                links: Vec::new(),
                end_timestamp: 0,
                flags: 0,
                ext: None,
            },
        );
        assert!(re.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn the_message_and_body_format_are_left_untouched() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("status=200");
        let original_message = event.log.as_ref().unwrap().message.clone();
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(event.log.as_ref().unwrap().message, original_message);
        assert_eq!(event.log.as_ref().unwrap().body_format, BodyFormat::Raw);
    }

    #[test]
    fn a_log_event_that_also_carries_a_metric_keeps_its_metric() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("status=200");
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
        assert_eq!(event.metrics.len(), 1, "the metric should ride through unaffected");
    }

    #[test]
    fn a_capture_overwrites_a_pre_existing_attribute_of_the_same_name() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("status=200");
        event.attributes.insert("status", Value::str("old"));
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
    }

    #[test]
    fn a_capture_may_overwrite_the_field_it_was_read_from() {
        let mut re =
            RegexParser::new(r"traceparent='(?P<message>[^']+)'", Some("message")).unwrap();
        let resource = default_resource();
        let mut event =
            event_with_attrs(&[("message", Value::str("INSERT ... traceparent='abc123'"))]);
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "message"), Some(&Value::str("abc123")));
    }

    #[test]
    fn field_reads_the_named_attribute_instead_of_the_log_message() {
        let mut re =
            RegexParser::new(r"traceparent='(?P<traceparent>[^']+)'", Some("sql")).unwrap();
        let resource = default_resource();
        let mut event = event_with_attrs(&[("sql", Value::str("INSERT ... traceparent='abc123'"))]);
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "traceparent"), Some(&Value::str("abc123")));
    }

    #[test]
    fn field_naming_a_missing_attribute_passes_through_untouched() {
        let mut re =
            RegexParser::new(r"traceparent='(?P<traceparent>[^']+)'", Some("sql")).unwrap();
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        assert!(re.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn field_naming_a_non_string_attribute_passes_through_untouched() {
        let mut re =
            RegexParser::new(r"traceparent='(?P<traceparent>[^']+)'", Some("sql")).unwrap();
        let resource = default_resource();
        let mut event = event_with_attrs(&[("sql", Value::I64(1))]);
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "traceparent"), None);
        assert_eq!(attr(&event, "sql"), Some(&Value::I64(1)), "the untouched attribute survives");
    }

    /// Whether `inner` lies within `outer`'s buffer: a captured `Value::Str` must be a slice of
    /// the message, never a fresh allocation.
    fn points_into(outer: &Bytes, inner: &Bytes) -> bool {
        let outer_start = outer.as_ptr() as usize;
        let outer_end = outer_start + outer.len();
        let inner_start = inner.as_ptr() as usize;
        let inner_end = inner_start + inner.len();
        inner_start >= outer_start && inner_end <= outer_end
    }

    #[test]
    fn a_captured_value_shares_the_message_buffer() {
        let mut re = RegexParser::new(r"status=(?P<status>\d+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("status=200");
        let message = match &event.log.as_ref().unwrap().message {
            Value::Str(b) => b.clone(),
            _ => panic!("expected Str"),
        };
        assert!(re.process(&resource, &mut event), "always forwards");
        match attr(&event, "status") {
            Some(Value::Str(captured)) => {
                assert!(points_into(&message, captured), "capture must slice the message buffer")
            }
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn a_capture_containing_multibyte_utf8_survives() {
        let mut re = RegexParser::new(r"user=(?P<user>\S+)", None).unwrap();
        let resource = default_resource();
        let mut event = log_event("user=Jos\u{e9}"); // "José"
        assert!(re.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "user"), Some(&Value::str("Jos\u{e9}")));
    }

    fn matched_count(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == name => Some(sum.value),
                _ => None,
            })
        })
    }

    #[test]
    fn a_matched_line_records_matched_not_skipped() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("sshd_regex", "regex", "transform");
        let mut re =
            RegexParser::new(r"status=(?P<status>\d+)", None).unwrap().with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = log_event("status=200");
        re.process(&resource, &mut event);

        let events = registry.drain(0);
        assert_eq!(matched_count(&events, "logit.transform.matched"), Some(1.0));
        assert_eq!(matched_count(&events, "logit.transform.matched.skipped"), None);
    }

    #[test]
    fn a_non_matching_line_records_skipped_not_matched() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("sshd_regex", "regex", "transform");
        let mut re =
            RegexParser::new(r"status=(?P<status>\d+)", None).unwrap().with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = log_event("no match here");
        re.process(&resource, &mut event);

        let events = registry.drain(0);
        assert_eq!(matched_count(&events, "logit.transform.matched"), None);
        assert_eq!(matched_count(&events, "logit.transform.matched.skipped"), Some(1.0));
    }
}
