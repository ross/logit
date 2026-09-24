//! The `json` transform: parses a log message as a JSON object and merges its top-level keys into
//! the event's attributes (`docs/adr/json-parsing-into-attributes.md`). A nested object stays a
//! `Value::Map`. Never flushes.

use bytes::Bytes;
use logit_core::interner::KeyCache;
use logit_core::{AttrMap, Diagnostics, Event, Resource, Symbol, Value};
use logit_pipeline::Transform;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::fmt;
use std::sync::Arc;

/// Parses an event's log message into `event.attributes`.
///
/// Reads only `log.message` (a `Str` or `Bytes`) and writes only `attributes`; any other event
/// passes through untouched. A message that fails to parse, or isn't a JSON object, also passes
/// through untouched, with a throttled diagnostic (`Diagnostics::warn_throttled`): dropping an
/// event over one malformed line is worse than a no-op, and a malformed high-volume source must
/// not flood stderr.
pub struct JsonParser {
    /// Parse from the first `{` and ignore anything after the object closes. Off by default: the
    /// whole line is the JSON, and trailing non-whitespace is a parse failure.
    skip_to_brace: bool,
    invalid_utf8: InvalidUtf8,
    diag: Diagnostics,
    /// The top-level object's pairs, reused across events. Only the top level is reused: it's
    /// merged into `event.attributes` and discarded, while a nested object becomes its own
    /// `AttrMap` via `collect_attrmap`.
    scratch: Vec<(Symbol, Value)>,
    /// Keys seen so far, shared by the top-level and nested objects, so a repeated key costs one
    /// `memcmp` instead of an interner probe. The `json-parse` load-test scenario is why it
    /// exists; `KeyCache` documents the bound.
    keys: KeyCache,
}

/// What [`JsonParser`] does with a message that isn't valid UTF-8.
///
/// Mirrors `logit_config::JsonInvalidUtf8`; `logit-cli` converts.
/// `docs/adr/http-access-normalization.md` says why `Replace` exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InvalidUtf8 {
    /// The parse fails and the event passes through untouched.
    #[default]
    Reject,
    /// Retry a failed parse on a copy with every invalid sequence replaced by U+FFFD. A message
    /// that parses as it arrived never pays for the check or the copy.
    Replace,
}

impl JsonParser {
    pub fn new(skip_to_brace: bool) -> Self {
        Self {
            skip_to_brace,
            invalid_utf8: InvalidUtf8::Reject,
            diag: Diagnostics::default(),
            scratch: Vec::new(),
            keys: KeyCache::new(),
        }
    }

    pub fn with_invalid_utf8(mut self, invalid_utf8: InvalidUtf8) -> Self {
        self.invalid_utf8 = invalid_utf8;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl Transform for JsonParser {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        let Some(log) = &event.log else { return true };
        let raw = match &log.message {
            Value::Str(b) | Value::Bytes(b) => b,
            _ => return true,
        };

        let body = if self.skip_to_brace {
            match raw.iter().position(|&b| b == b'{') {
                Some(i) => raw.slice(i..),
                None => {
                    self.diag.warn_throttled(
                        "no_brace",
                        "no '{' found in message, passing event through unparsed",
                    );
                    return true;
                }
            }
        } else {
            raw.clone()
        };

        // Parsed apart from `event.attributes` and merged only on success, so a failure partway
        // through a malformed object leaves the event's attributes untouched.
        self.scratch.clear();
        let parsed = if self.skip_to_brace {
            parse_object_prefix(&body, &mut self.scratch, &mut self.keys)
        } else {
            parse_object(&body, &mut self.scratch, &mut self.keys)
        };
        // Only a failed parse of invalid UTF-8 is copied and retried. The repaired copy is a fresh
        // `Bytes`, so every zero-copy `Value::Str` the retry produces slices it, never the invalid
        // original: that keeps `Value::Str`'s valid-UTF-8 invariant. A second failure takes the
        // ordinary `parse_failure` path.
        let parsed = match parsed {
            Err(_)
                if self.invalid_utf8 == InvalidUtf8::Replace
                    && std::str::from_utf8(&body).is_err() =>
            {
                self.scratch.clear();
                let repaired = Bytes::from(String::from_utf8_lossy(&body).into_owned());
                let retried = if self.skip_to_brace {
                    parse_object_prefix(&repaired, &mut self.scratch, &mut self.keys)
                } else {
                    parse_object(&repaired, &mut self.scratch, &mut self.keys)
                };
                if retried.is_ok() {
                    self.diag.warn_throttled(
                        "invalid_utf8",
                        "message is not valid UTF-8; parsed after replacing invalid sequences with U+FFFD",
                    );
                }
                retried
            }
            other => other,
        };
        match parsed {
            Ok(()) => {
                // By `Symbol`: `KeySeed` already interned each key, and a `resolve` +
                // `insert(&str)` round trip costs two interner probes per key (about a fifth of
                // the `json-parse` load-test scenario's samples when measured).
                for (key, value) in self.scratch.drain(..) {
                    event.attributes.insert_sym(key, value);
                }
            }
            Err(err) => {
                // Clear now, not at the next call: a partial object's `Value::Str`s may still
                // slice this event's message buffer and would keep it alive.
                self.scratch.clear();
                self.diag.warn_throttled(
                    "parse_failure",
                    format_args!("failed to parse message as JSON, passing event through: {err}"),
                );
            }
        }

        true
    }
}

/// Parses `json` as one JSON object into `out`; anything but trailing whitespace after it fails.
fn parse_object(
    json: &Bytes,
    out: &mut Vec<(Symbol, Value)>,
    keys: &mut KeyCache,
) -> Result<(), serde_json::Error> {
    let mut de = serde_json::Deserializer::from_slice(json);
    TopLevelSeed { base: json, out, keys }.deserialize(&mut de)?;
    de.end()?;
    Ok(())
}

/// Parses the first complete JSON object in `json` into `out` and ignores the rest, so
/// `skip_to_brace` handles a line like `INFO {"a":1} took=3ms`.
fn parse_object_prefix(
    json: &Bytes,
    out: &mut Vec<(Symbol, Value)>,
    keys: &mut KeyCache,
) -> Result<(), serde_json::Error> {
    let mut de = serde_json::Deserializer::from_slice(json);
    TopLevelSeed { base: json, out, keys }.deserialize(&mut de)
}

/// Slices `base` for a `&str` serde_json borrowed from it (`Visitor::visit_borrowed_str`), so an
/// unescaped string stays zero-copy (`docs/design/data-model.md`'s "Values" section).
///
/// Checks the pointer range and falls back to a copy rather than calling `Bytes::slice_ref`,
/// which panics on a non-subset and would take down the transform node over one input.
fn borrowed_str_bytes(base: &Bytes, s: &str) -> Bytes {
    let base_start = base.as_ptr() as usize;
    let base_end = base_start + base.len();
    let s_start = s.as_ptr() as usize;
    let s_end = s_start + s.len();
    if s_start >= base_start && s_end <= base_end {
        base.slice((s_start - base_start)..(s_end - base_start))
    } else {
        Bytes::copy_from_slice(s.as_bytes())
    }
}

/// Deserializes a JSON value straight into a [`Value`], skipping a `serde_json::Value` tree and
/// its conversion, so an unescaped string can stay a slice of `base` ([`borrowed_str_bytes`]).
struct ValueSeed<'b, 'k> {
    base: &'b Bytes,
    /// Reborrowed per value so nested keys share the top level's [`KeyCache`].
    keys: &'k mut KeyCache,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_, '_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(ValueVisitor { base: self.base, keys: self.keys })
    }
}

struct ValueVisitor<'b, 'k> {
    base: &'b Bytes,
    keys: &'k mut KeyCache,
}

impl<'de> Visitor<'de> for ValueVisitor<'_, '_> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(Value::I64(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(Value::U64(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(Value::F64(v))
    }

    // Unescaped: `v` is a slice of `self.base`, so it stays zero-copy.
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Value, E> {
        Ok(Value::Str(borrowed_str_bytes(self.base, v)))
    }

    // Escaped: `v` is in serde_json's scratch buffer, not `self.base`, so it must be copied.
    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Str(Bytes::copy_from_slice(v.as_bytes())))
    }

    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::Str(Bytes::from(v)))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) =
            seq.next_element_seed(ValueSeed { base: self.base, keys: &mut *self.keys })?
        {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Value, A::Error> {
        Ok(Value::Map(Box::new(collect_attrmap(map, self.base, self.keys)?)))
    }
}

/// The top-level seed: a bare scalar or array is a parse error by construction.
///
/// The pairs go into the caller's reused `Vec`, not an `AttrMap`, because they're merged into
/// `event.attributes` and discarded, unlike a nested object ([`collect_attrmap`]).
struct TopLevelSeed<'b, 'o, 'k> {
    base: &'b Bytes,
    out: &'o mut Vec<(Symbol, Value)>,
    keys: &'k mut KeyCache,
}

impl<'de> DeserializeSeed<'de> for TopLevelSeed<'_, '_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(TopLevelVisitor {
            base: self.base,
            out: self.out,
            keys: self.keys,
        })
    }
}

struct TopLevelVisitor<'b, 'o, 'k> {
    base: &'b Bytes,
    out: &'o mut Vec<(Symbol, Value)>,
    keys: &'k mut KeyCache,
}

impl<'de> Visitor<'de> for TopLevelVisitor<'_, '_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a JSON object")
    }

    // A duplicate key pushes twice; `process` merges in push order and `insert_sym` overwrites,
    // so the later value wins without a lookup per key here.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key_seed(KeySeed { keys: &mut *self.keys })? {
            let value =
                map.next_value_seed(ValueSeed { base: self.base, keys: &mut *self.keys })?;
            self.out.push((key, value));
        }
        Ok(())
    }
}

/// Deserializes an object key straight to its [`Symbol`] through the parser's [`KeyCache`].
///
/// `next_key::<String>()` would allocate a `String` per key, which was most of `json`'s
/// allocations (`docs/design/memory.md`). A key seen before costs one `memcmp` and never reaches
/// the process-wide interner.
struct KeySeed<'k> {
    keys: &'k mut KeyCache,
}

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = Symbol;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Symbol, D::Error> {
        deserializer.deserialize_str(KeyVisitor { keys: self.keys })
    }
}

struct KeyVisitor<'k> {
    keys: &'k mut KeyCache,
}

impl<'de> Visitor<'de> for KeyVisitor<'_> {
    type Value = Symbol;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a string")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Symbol, E> {
        Ok(self.keys.get_or_intern(v))
    }

    // An escaped key sits in serde_json's scratch buffer only long enough to compare or intern,
    // so it needs no `String` either.
    fn visit_str<E>(self, v: &str) -> Result<Symbol, E> {
        Ok(self.keys.get_or_intern(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Symbol, E> {
        Ok(self.keys.get_or_intern(&v))
    }
}

/// Collects a nested JSON object into its own `AttrMap` for a `Value::Map`.
fn collect_attrmap<'de, A: MapAccess<'de>>(
    mut map: A,
    base: &Bytes,
    keys: &mut KeyCache,
) -> Result<AttrMap, A::Error> {
    let mut attrs = AttrMap::new();
    while let Some(key) = map.next_key_seed(KeySeed { keys: &mut *keys })? {
        let value = map.next_value_seed(ValueSeed { base, keys: &mut *keys })?;
        // `insert_sym` overwrites, so a duplicate key's last value wins.
        attrs.insert_sym(key, value);
    }
    Ok(attrs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::{self, intern, resolve};
    use logit_core::Registry;
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, SpanEvent, SpanKind};
    use logit_core::{SpanRecord, SpanStatus};

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

    fn message_of(event: &Event) -> &Value {
        &event.log.as_ref().expect("event should carry a log").message
    }

    /// A log whose message is raw bytes rather than a `Str` -- what `syslog_in` hands over for a
    /// datagram whose MSG isn't valid UTF-8 (`crates/logit-inputs/src/syslog.rs`).
    fn bytes_log_event(message: &'static [u8]) -> Event {
        let mut event = log_event("");
        event.log.as_mut().unwrap().message = Value::Bytes(Bytes::from_static(message));
        event
    }

    /// The `key` of every diagnostics point in `registry`, throttled or not. `Registry::drain`
    /// takes the points, so call it once per test.
    fn fired_diagnostics(registry: &Registry) -> Vec<String> {
        registry
            .drain(0)
            .iter()
            .filter_map(|e| e.attributes.get("key").and_then(|v| v.as_str()).map(str::to_owned))
            .collect()
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn attr<'a>(event: &'a Event, key: &str) -> Option<&'a Value> {
        event.attributes.get(key)
    }

    #[test]
    fn a_flat_object_populates_attributes_with_the_right_value_variants() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1,"b":-2,"c":1.5,"d":true,"e":null,"f":"hi"}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");

        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
        assert_eq!(attr(&event, "b"), Some(&Value::I64(-2)));
        assert_eq!(attr(&event, "c"), Some(&Value::F64(1.5)));
        assert_eq!(attr(&event, "d"), Some(&Value::Bool(true)));
        assert_eq!(attr(&event, "e"), Some(&Value::Null));
        assert_eq!(attr(&event, "f"), Some(&Value::str("hi")));
    }

    #[test]
    fn a_nested_object_becomes_a_map_and_an_array_stays_an_array() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"http":{"status":200},"tags":["a","b"]}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");

        let mut http = AttrMap::new();
        http.insert("status", Value::U64(200));
        assert_eq!(attr(&event, "http"), Some(&Value::Map(Box::new(http))));
        assert_eq!(
            attr(&event, "tags"),
            Some(&Value::Array(vec![Value::str("a"), Value::str("b")]))
        );
    }

    #[test]
    fn an_escaped_string_decodes_correctly() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"msg":"a\nb"}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("a\nb")));
    }

    #[test]
    fn a_metric_event_passes_through_with_attributes_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern("m"), MetricKind::counter(1.0)),
        );
        assert!(parser.process(&resource, &mut event), "metric-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_span_event_passes_through_with_attributes_untouched() {
        let mut parser = JsonParser::new(false);
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
        assert!(parser.process(&resource, &mut event), "span-only events pass through");
        assert!(event.attributes.is_empty());
    }

    /// A log event that also carries a metric is parsed, and the metric rides through untouched.
    #[test]
    fn a_log_event_that_also_carries_a_metric_is_parsed_and_keeps_its_metric() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1}"#);
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
        assert!(parser.process(&resource, &mut event), "mixed events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
        assert_eq!(event.metrics.len(), 1, "the metric should ride through unaffected");
        assert!(matches!(
            event.metrics[0].kind,
            MetricKind::Sum(logit_core::Sum { value, .. }) if value == 1.0
        ));
    }

    #[test]
    fn malformed_json_passes_through_with_attributes_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str(r#"{"a":}"#));
    }

    /// A raw 0xE9 inside a string (nginx's `escape=json` passes high bytes through) fails the
    /// whole line under the default `Reject`.
    #[test]
    fn invalid_utf8_inside_a_string_is_rejected_by_default() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("json", "json", "transform");
        let mut parser = JsonParser::new(false)
            .with_diagnostics(Diagnostics::new("json").with_telemetry(telemetry));
        let resource = default_resource();
        let mut event = bytes_log_event(b"{\"ua\":\"caf\xe9 client\",\"status\":200}");
        assert!(parser.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty(), "nothing parsed under Reject");
        let fired = fired_diagnostics(&registry);
        assert!(fired.iter().any(|k| k == "parse_failure"), "{fired:?}");
        assert!(!fired.iter().any(|k| k == "invalid_utf8"), "{fired:?}");
    }

    #[test]
    fn invalid_utf8_replace_parses_the_line_with_the_bad_byte_replaced() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("json", "json", "transform");
        let mut parser = JsonParser::new(false)
            .with_invalid_utf8(InvalidUtf8::Replace)
            .with_diagnostics(Diagnostics::new("json").with_telemetry(telemetry));
        let resource = default_resource();
        let mut event = bytes_log_event(b"{\"ua\":\"caf\xe9 client\",\"status\":200}");
        assert!(parser.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "ua"), Some(&Value::str("caf\u{FFFD} client")));
        assert_eq!(attr(&event, "status"), Some(&Value::U64(200)));
        assert_eq!(
            message_of(&event),
            &Value::Bytes(Bytes::from_static(b"{\"ua\":\"caf\xe9 client\",\"status\":200}"))
        );
        let fired = fired_diagnostics(&registry);
        assert!(fired.iter().any(|k| k == "invalid_utf8"), "{fired:?}");
        assert!(!fired.iter().any(|k| k == "parse_failure"), "the retry succeeded: {fired:?}");
    }

    /// Valid UTF-8 that is malformed JSON takes the `parse_failure` path with no retry.
    #[test]
    fn invalid_utf8_replace_does_not_retry_a_valid_utf8_parse_failure() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("json", "json", "transform");
        let mut parser = JsonParser::new(false)
            .with_invalid_utf8(InvalidUtf8::Replace)
            .with_diagnostics(Diagnostics::new("json").with_telemetry(telemetry));
        let resource = default_resource();
        let mut event = log_event(r#"{"a":}"#);
        assert!(parser.process(&resource, &mut event));
        assert!(event.attributes.is_empty());
        let fired = fired_diagnostics(&registry);
        assert!(fired.iter().any(|k| k == "parse_failure"), "{fired:?}");
        assert!(!fired.iter().any(|k| k == "invalid_utf8"), "{fired:?}");
    }

    /// Invalid UTF-8 and malformed JSON reports `parse_failure`, not a repair.
    #[test]
    fn invalid_utf8_replace_falls_through_to_parse_failure_when_the_json_is_also_broken() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("json", "json", "transform");
        let mut parser = JsonParser::new(false)
            .with_invalid_utf8(InvalidUtf8::Replace)
            .with_diagnostics(Diagnostics::new("json").with_telemetry(telemetry));
        let resource = default_resource();
        let mut event = bytes_log_event(b"{\"ua\":\"caf\xe9\",");
        assert!(parser.process(&resource, &mut event));
        assert!(event.attributes.is_empty());
        let fired = fired_diagnostics(&registry);
        assert!(fired.iter().any(|k| k == "parse_failure"), "{fired:?}");
        assert!(!fired.iter().any(|k| k == "invalid_utf8"), "{fired:?}");
    }

    #[test]
    fn invalid_utf8_replace_works_with_skip_to_brace() {
        let mut parser = JsonParser::new(true).with_invalid_utf8(InvalidUtf8::Replace);
        let resource = default_resource();
        let mut event = bytes_log_event(b"INFO {\"k\":\"\xff\"} trailing");
        assert!(parser.process(&resource, &mut event));
        assert_eq!(attr(&event, "k"), Some(&Value::str("\u{FFFD}")));
    }

    #[test]
    fn a_valid_top_level_array_or_scalar_passes_through_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();

        let mut event = log_event("[1,2]");
        assert!(parser.process(&resource, &mut event), "passes through");
        assert!(event.attributes.is_empty());

        let mut event = log_event(r#""hi""#);
        assert!(parser.process(&resource, &mut event), "passes through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn without_skip_to_brace_a_prefixed_line_fails_to_parse() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"2026-08-29 INFO {"a":1}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn skip_to_brace_parses_a_prefixed_line() {
        let mut parser = JsonParser::new(true);
        let resource = default_resource();
        let mut event = log_event(r#"2026-08-29 INFO {"a":1}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
    }

    #[test]
    fn skip_to_brace_with_no_brace_at_all_passes_through_untouched() {
        let mut parser = JsonParser::new(true);
        let resource = default_resource();
        let mut event = log_event("2026-08-29 INFO no json here");
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn skip_to_brace_tolerates_trailing_content_after_the_object() {
        let mut parser = JsonParser::new(true);
        let resource = default_resource();
        let mut event = log_event(r#"INFO {"a":1} took=3ms"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
    }

    #[test]
    fn without_skip_to_brace_trailing_content_after_the_object_is_rejected() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1} took=3ms"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_parsed_key_overwrites_a_pre_existing_attribute_of_the_same_name() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1}"#);
        event.attributes.insert("a", Value::str("old"));
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
    }

    /// Last-writer-wins holds on both merge paths: the top-level drain and `collect_attrmap`.
    #[test]
    fn a_duplicate_key_within_one_object_takes_the_last_value_at_every_depth() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1,"n":{"b":1,"b":2},"a":2}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");

        assert_eq!(attr(&event, "a"), Some(&Value::U64(2)));
        let mut nested = AttrMap::new();
        nested.insert("b", Value::U64(2));
        assert_eq!(attr(&event, "n"), Some(&Value::Map(Box::new(nested))));
        assert_eq!(event.attributes.len(), 2, "a duplicate must overwrite, not add an entry");
    }

    // -- the per-parser key cache ------------------------------------------------------------

    #[test]
    fn keys_in_a_different_order_on_the_next_event_resolve_to_the_same_symbols() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut first = log_event(r#"{"a":1,"b":2,"c":3}"#);
        assert!(parser.process(&resource, &mut first), "log events pass through");
        assert_eq!(parser.keys.len(), 3);

        let mut second = log_event(r#"{"c":30,"a":10}"#);
        assert!(parser.process(&resource, &mut second), "log events pass through");
        let mut third = log_event(r#"{"b":200,"c":300,"a":100,"d":400}"#);
        assert!(parser.process(&resource, &mut third), "log events pass through");

        assert_eq!(attr(&second, "a"), Some(&Value::U64(10)));
        assert_eq!(attr(&second, "c"), Some(&Value::U64(30)));
        assert_eq!(attr(&second, "b"), None);
        assert_eq!(attr(&third, "a"), Some(&Value::U64(100)));
        assert_eq!(attr(&third, "b"), Some(&Value::U64(200)));
        assert_eq!(attr(&third, "c"), Some(&Value::U64(300)));
        assert_eq!(attr(&third, "d"), Some(&Value::U64(400)));
        for key in ["a", "b", "c"] {
            let sym =
                |e: &Event| e.attributes.iter().find(|(k, _)| resolve(*k) == key).map(|(k, _)| k);
            assert_eq!(sym(&first), sym(&third), "{key}");
        }
        assert_eq!(parser.keys.len(), 4, "only the genuinely new key `d` was added");
    }

    /// `nextest` runs each test in its own process (`docs/design/memory.md` §7), so
    /// `interner::len()` here reflects only this test.
    #[test]
    fn an_optional_key_missing_from_one_event_does_not_touch_the_interner() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut warm = log_event(r#"{"json_opt_a":1,"json_opt_b":2,"json_opt_c":3}"#);
        parser.process(&resource, &mut warm);

        let before = interner::len();
        let mut event = log_event(r#"{"json_opt_a":1,"json_opt_c":3}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "json_opt_a"), Some(&Value::U64(1)));
        assert_eq!(attr(&event, "json_opt_c"), Some(&Value::U64(3)));
        assert_eq!(interner::len(), before, "every key was a cache hit");
        assert_eq!(parser.keys.len(), 3);
    }

    #[test]
    fn a_nested_key_that_repeats_a_top_level_name_shares_its_symbol_and_cache_entry() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"id":1,"user":{"id":2}}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");

        assert_eq!(attr(&event, "id"), Some(&Value::U64(1)));
        let Some(Value::Map(user)) = attr(&event, "user") else { panic!("user should be a map") };
        assert_eq!(user.get("id"), Some(&Value::U64(2)));
        assert_eq!(parser.keys.len(), 2, "`id` and `user` -- the nested `id` is the same entry");
    }

    #[test]
    fn an_escaped_key_resolves_to_the_same_symbol_as_its_unescaped_form() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a\nb":1}"#);
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a\nb"), Some(&Value::U64(1)));
        assert_eq!(parser.keys.len(), 1);

        let mut event = log_event("{\"a\nb\":2}".replace('\n', "\\u000a").as_str());
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a\nb"), Some(&Value::U64(2)));
        assert_eq!(parser.keys.len(), 1);
    }

    #[test]
    fn more_distinct_keys_than_the_cache_holds_still_all_parse() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let n = KeyCache::MAX_ENTRIES + 8;
        let body: Vec<String> = (0..n).map(|i| format!(r#""json_cap_{i}":{i}"#)).collect();
        let mut event = log_event(&format!("{{{}}}", body.join(",")));
        assert!(parser.process(&resource, &mut event), "log events pass through");

        assert_eq!(event.attributes.len(), n);
        for i in 0..n {
            assert_eq!(attr(&event, &format!("json_cap_{i}")), Some(&Value::U64(i as u64)));
        }
        assert_eq!(parser.keys.len(), KeyCache::MAX_ENTRIES);
    }

    #[test]
    fn a_failed_parse_leaves_the_cache_usable() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut bad = log_event(r#"{"a":1,"b":"#);
        assert!(parser.process(&resource, &mut bad), "log events pass through");
        assert!(bad.attributes.is_empty());

        let mut good = log_event(r#"{"a":1,"b":2}"#);
        assert!(parser.process(&resource, &mut good), "log events pass through");
        assert_eq!(attr(&good, "a"), Some(&Value::U64(1)));
        assert_eq!(attr(&good, "b"), Some(&Value::U64(2)));
        assert_eq!(parser.keys.len(), 2);
    }

    #[test]
    fn an_empty_object_parses_and_inserts_nothing() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event("{}");
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn the_message_and_body_format_are_left_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1}"#);
        let original_message = message_of(&event).clone();
        assert!(parser.process(&resource, &mut event), "log events pass through");
        assert_eq!(message_of(&event), &original_message);
        assert_eq!(
            event.log.as_ref().expect("event should carry a log").body_format,
            BodyFormat::Raw
        );
    }
}
