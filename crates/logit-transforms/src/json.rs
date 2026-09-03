//! The built-in `json` transform: parses a log record's message as JSON and merges the resulting
//! key/values into the event's attributes, where every downstream component -- native transform,
//! Lua script (via `EventProxy`), or sink -- can already see them. See
//! `docs/adr/json-parsing-into-attributes.md` for the design decisions this implements.
//!
//! Stateless -- unlike `Aggregator`, this never flushes, so `impl Transform` only overrides
//! `process`, taking the trait's default `flush_interval`/`flush`.

use bytes::Bytes;
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Diagnostics, Event, Resource, Symbol, Value};
use logit_pipeline::Transform;
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::fmt;
use std::sync::Arc;

/// Parses `event.attributes` out of an event's log message, if it has one. An event with no log,
/// and a log whose message isn't a string, pass through untouched -- there's nothing to parse.
/// Any metrics/span already on the event ride through unaffected either way -- only `log.message`
/// is read and only `attributes` is written. A message that fails to parse (or, with
/// `skip_to_brace` off, isn't a JSON object at all) also passes through untouched, with a
/// count-throttled diagnostic (`Diagnostics::warn_throttled`,
/// `docs/adr/service-lifecycle-and-output-retry.md`) -- dropping telemetry over one malformed
/// line is worse than a no-op, and a high-volume malformed source must not flood stderr one line
/// per event either.
pub struct JsonParser {
    /// Skip everything before the first `{` and parse from there, tolerating trailing content
    /// after the object closes. Off by default: the whole line is assumed to be the JSON data,
    /// and trailing non-whitespace after it is a parse failure.
    skip_to_brace: bool,
    diag: Diagnostics,
    /// Scratch buffer the top-level object's key/value pairs are parsed into, reused across
    /// events instead of a fresh `AttrMap` built by `deserialize` on every call (mirroring
    /// `InfluxLineEncoder`'s reused buffers, `crates/logit-outputs/src/influxdb.rs` -- same idea,
    /// applied to the parsed pairs instead of a `String`). Cleared at the start of every `process`
    /// (see the comment there for why the *intermediate* still has to exist at all). A nested
    /// object still becomes its own freshly-allocated `AttrMap` (`Value::Map`, via
    /// `collect_attrmap`) -- only the top-level result, which is merged into `event.attributes`
    /// and thrown away, is worth reusing.
    scratch: Vec<(Symbol, Value)>,
}

impl JsonParser {
    pub fn new(skip_to_brace: bool) -> Self {
        Self { skip_to_brace, diag: Diagnostics::default(), scratch: Vec::new() }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }
}

impl Transform for JsonParser {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        let Some(log) = &event.log else { return Some(event) };
        let raw = match &log.message {
            Value::Str(b) | Value::Bytes(b) => b,
            _ => return Some(event),
        };

        let body = if self.skip_to_brace {
            match raw.iter().position(|&b| b == b'{') {
                Some(i) => raw.slice(i..),
                None => {
                    self.diag.warn_throttled(
                        "no_brace",
                        "no '{' found in message, passing event through unparsed",
                    );
                    return Some(event);
                }
            }
        } else {
            raw.clone()
        };

        // Parsed into `self.scratch`, cleared here, rather than a fresh `AttrMap` -- but still
        // built up separately from `event.attributes` and only merged in on full success: a
        // failure partway through a malformed object must leave the event's existing attributes
        // untouched, not half-populated. `scratch` reaching `Ok` is what gates the merge below;
        // reusing its storage across calls doesn't change that contract, since a fresh
        // `self.scratch.clear()` at the top of the very next call throws away anything a failed
        // parse left behind.
        self.scratch.clear();
        let parsed = if self.skip_to_brace {
            parse_object_prefix(&body, &mut self.scratch)
        } else {
            parse_object(&body, &mut self.scratch)
        };
        match parsed {
            Ok(()) => {
                // Moved out of `scratch`, not cloned: `scratch` is the sole owner of each `Value`
                // here and is about to be emptied anyway, so there's nothing left for a clone to
                // preserve. Still a `resolve` -> `intern` round trip per key -- `AttrMap::insert`
                // takes `&str`, not `Symbol` -- which a `Symbol`-keyed insert would remove; left
                // as a follow-up (see PR description) rather than adding one to
                // `logit-core::attrs` here, since another workstream is already changing
                // `AttrMap::get`'s interning in that file.
                for (key, value) in self.scratch.drain(..) {
                    event.attributes.insert(resolve(key), value);
                }
            }
            Err(err) => {
                // Only ever holds a partial object here; drop it promptly rather than letting it
                // sit until the next `process` call clears it, since every `Value::Str` in it may
                // still be a slice of this event's message buffer (see `borrowed_str_bytes`).
                self.scratch.clear();
                self.diag.warn_throttled(
                    "parse_failure",
                    format_args!("failed to parse message as JSON, passing event through: {err}"),
                );
            }
        }

        Some(event)
    }
}

/// Parses `json` as a single JSON object into `out`, requiring the whole buffer be consumed (only
/// trailing whitespace allowed) -- the default-mode contract: "the whole line is the JSON data."
fn parse_object(json: &Bytes, out: &mut Vec<(Symbol, Value)>) -> Result<(), serde_json::Error> {
    let mut de = serde_json::Deserializer::from_slice(json);
    TopLevelSeed { base: json, out }.deserialize(&mut de)?;
    de.end()?;
    Ok(())
}

/// Parses the first complete JSON object out of `json` into `out`, ignoring anything after it --
/// the `skip_to_brace`-mode contract: "start parsing here," which is what makes a line like
/// `INFO {"a":1} took=3ms` work at all.
fn parse_object_prefix(
    json: &Bytes,
    out: &mut Vec<(Symbol, Value)>,
) -> Result<(), serde_json::Error> {
    let mut de = serde_json::Deserializer::from_slice(json);
    TopLevelSeed { base: json, out }.deserialize(&mut de)
}

/// Reconstructs a `Bytes` sharing `base`'s underlying allocation for a `&str` serde_json reported
/// as borrowed directly from the input it was given (`Visitor::visit_borrowed_str` -- no
/// unescaping happened, so `s` is genuinely a sub-slice of `base`). Verifies the pointer range
/// explicitly and falls back to a copy rather than calling `Bytes::slice_ref` unguarded, which
/// panics on a non-subset -- a panic here would take down the whole transform node over one
/// malformed input, not just fail to parse it. See `docs/design/data-model.md`'s "`bytes::Bytes`
/// everywhere strings and blobs appear" -- this is what keeps an unescaped string value a
/// zero-copy slice of the original message buffer rather than a fresh allocation.
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

/// Deserializes a JSON value directly into a [`Value`], rather than through an intermediate
/// `serde_json::Value` tree and a separate conversion -- halves the allocation per line, and lets
/// an unescaped string stay a zero-copy slice of `base` (see [`borrowed_str_bytes`]).
struct ValueSeed<'b> {
    base: &'b Bytes,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(ValueVisitor { base: self.base })
    }
}

struct ValueVisitor<'b> {
    base: &'b Bytes,
}

impl<'de> Visitor<'de> for ValueVisitor<'_> {
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

    // The unescaped case: `v` is borrowed straight from the input buffer, so it's a genuine
    // sub-slice of `self.base` -- stays zero-copy.
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Value, E> {
        Ok(Value::Str(borrowed_str_bytes(self.base, v)))
    }

    // The escaped case: serde_json had to unescape into a scratch buffer, so `v` doesn't live in
    // `self.base` at all -- must copy.
    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Str(Bytes::copy_from_slice(v.as_bytes())))
    }

    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::Str(Bytes::from(v)))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element_seed(ValueSeed { base: self.base })? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Value, A::Error> {
        Ok(Value::Map(Box::new(collect_attrmap(map, self.base)?)))
    }
}

/// The top-level seed: requires the parsed value be a JSON *object*, so a bare scalar or array at
/// the top level is a parse error by construction (there are no key/values to merge) rather than
/// a post-hoc check after a successful-but-useless parse. Unlike a nested object
/// ([`ValueVisitor::visit_map`], via [`collect_attrmap`]), the top-level result is never stored on
/// an `Event` -- it's merged into `event.attributes` and discarded -- so it's collected straight
/// into the caller's reused `Vec` instead of a freshly-allocated `AttrMap`.
struct TopLevelSeed<'b, 'o> {
    base: &'b Bytes,
    out: &'o mut Vec<(Symbol, Value)>,
}

impl<'de> DeserializeSeed<'de> for TopLevelSeed<'_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(TopLevelVisitor { base: self.base, out: self.out })
    }
}

struct TopLevelVisitor<'b, 'o> {
    base: &'b Bytes,
    out: &'o mut Vec<(Symbol, Value)>,
}

impl<'de> Visitor<'de> for TopLevelVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a JSON object")
    }

    // No last-writer-wins bookkeeping needed here, unlike `collect_attrmap`: a duplicate key
    // within this object just pushes twice, and merging into `event.attributes` in push order
    // (see `JsonParser::process`) makes the later push win, since `AttrMap::insert` overwrites an
    // existing key rather than adding a second entry -- the same outcome, reached without a
    // binary search per key on a buffer that's thrown away right after.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key_seed(KeySeed)? {
            let value = map.next_value_seed(ValueSeed { base: self.base })?;
            self.out.push((key, value));
        }
        Ok(())
    }
}

/// Deserializes a JSON object key straight to its interned [`Symbol`], rather than the owned
/// `String` `next_key::<String>()` would otherwise allocate for every key regardless of whether it
/// needed unescaping. `AttrMap` interns every key on insert anyway, so nothing about the interning
/// itself changes -- this only removes the `String` that used to exist solely to be interned and
/// then dropped. Measured: this is where most of `json`'s allocations were (`docs/design/
/// memory.md`), not the intermediate map itself.
struct KeySeed;

impl<'de> DeserializeSeed<'de> for KeySeed {
    type Value = Symbol;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Symbol, D::Error> {
        deserializer.deserialize_str(KeyVisitor)
    }
}

struct KeyVisitor;

impl<'de> Visitor<'de> for KeyVisitor {
    type Value = Symbol;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a string")
    }

    // The unescaped case: borrowed straight from the input, never materialized as an owned
    // `String` at all.
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Symbol, E> {
        Ok(intern(v))
    }

    // The escaped case: `v` lives in serde_json's own scratch buffer, not `self.base` -- but
    // `intern` only needs it long enough to hash and copy (on a never-before-seen key) or look up
    // (on a repeat), so still no `String` of our own.
    fn visit_str<E>(self, v: &str) -> Result<Symbol, E> {
        Ok(intern(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Symbol, E> {
        Ok(intern(&v))
    }
}

/// Used by [`ValueVisitor::visit_map`] to walk a *nested* JSON object's entries into an owned,
/// independent `AttrMap` (`Value::Map`) -- unlike the top level, which goes through
/// [`TopLevelVisitor`] instead and skips building an `AttrMap` at all (see its doc comment).
fn collect_attrmap<'de, A: MapAccess<'de>>(mut map: A, base: &Bytes) -> Result<AttrMap, A::Error> {
    let mut attrs = AttrMap::new();
    while let Some(key) = map.next_key_seed(KeySeed)? {
        let value = map.next_value_seed(ValueSeed { base })?;
        // Last-writer-wins on a duplicate key within one object -- plain `AttrMap::insert`
        // semantics, same as a parsed key overwriting a pre-existing attribute of the same name.
        attrs.insert(resolve(key), value);
    }
    Ok(attrs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
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
            },
        )
    }

    fn message_of(event: &Event) -> &Value {
        &event.log.as_ref().expect("event should carry a log").message
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
        let event = log_event(r#"{"a":1,"b":-2,"c":1.5,"d":true,"e":null,"f":"hi"}"#);
        let event = parser.process(&resource, event).expect("log events pass through");

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
        let event = log_event(r#"{"http":{"status":200},"tags":["a","b"]}"#);
        let event = parser.process(&resource, event).expect("log events pass through");

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
        let event = log_event(r#"{"msg":"a\nb"}"#);
        let event = parser.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("a\nb")));
    }

    #[test]
    fn a_metric_event_passes_through_with_attributes_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord { name: intern("m"), kind: MetricKind::Counter(1.0), unit: None },
        );
        let event = parser.process(&resource, event).expect("metric-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_span_event_passes_through_with_attributes_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let event = Event::span(
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
            },
        );
        let event = parser.process(&resource, event).expect("span-only events pass through");
        assert!(event.attributes.is_empty());
    }

    /// The genuinely new shape the multi-payload model makes possible: a log event that also
    /// carries a metric. `json` only ever reads `log.message` and writes `attributes`, so the
    /// metric should ride through completely untouched while the log half is parsed normally.
    #[test]
    fn a_log_event_that_also_carries_a_metric_is_parsed_and_keeps_its_metric() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1}"#);
        event.metrics.push(MetricRecord {
            name: intern("m"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });
        let event = parser.process(&resource, event).expect("mixed events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
        assert_eq!(event.metrics.len(), 1, "the metric should ride through unaffected");
        assert!(matches!(event.metrics[0].kind, MetricKind::Counter(v) if v == 1.0));
    }

    #[test]
    fn malformed_json_passes_through_with_attributes_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let event = log_event(r#"{"a":}"#);
        let event = parser.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str(r#"{"a":}"#));
    }

    #[test]
    fn a_valid_top_level_array_or_scalar_passes_through_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();

        let event = parser.process(&resource, log_event("[1,2]")).expect("passes through");
        assert!(event.attributes.is_empty());

        let event = parser.process(&resource, log_event(r#""hi""#)).expect("passes through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn without_skip_to_brace_a_prefixed_line_fails_to_parse() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let event = log_event(r#"2026-08-29 INFO {"a":1}"#);
        let event = parser.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn skip_to_brace_parses_a_prefixed_line() {
        let mut parser = JsonParser::new(true);
        let resource = default_resource();
        let event = log_event(r#"2026-08-29 INFO {"a":1}"#);
        let event = parser.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
    }

    #[test]
    fn skip_to_brace_with_no_brace_at_all_passes_through_untouched() {
        let mut parser = JsonParser::new(true);
        let resource = default_resource();
        let event = log_event("2026-08-29 INFO no json here");
        let event = parser.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn skip_to_brace_tolerates_trailing_content_after_the_object() {
        let mut parser = JsonParser::new(true);
        let resource = default_resource();
        let event = log_event(r#"INFO {"a":1} took=3ms"#);
        let event = parser.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
    }

    #[test]
    fn without_skip_to_brace_trailing_content_after_the_object_is_rejected() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let event = log_event(r#"{"a":1} took=3ms"#);
        let event = parser.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_parsed_key_overwrites_a_pre_existing_attribute_of_the_same_name() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"{"a":1}"#);
        event.attributes.insert("a", Value::str("old"));
        let event = parser.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::U64(1)));
    }

    #[test]
    fn an_empty_object_parses_and_inserts_nothing() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let event = log_event("{}");
        let event = parser.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn the_message_and_body_format_are_left_untouched() {
        let mut parser = JsonParser::new(false);
        let resource = default_resource();
        let event = log_event(r#"{"a":1}"#);
        let original_message = message_of(&event).clone();
        let event = parser.process(&resource, event).expect("log events pass through");
        assert_eq!(message_of(&event), &original_message);
        assert_eq!(
            event.log.as_ref().expect("event should carry a log").body_format,
            BodyFormat::Raw
        );
    }
}
