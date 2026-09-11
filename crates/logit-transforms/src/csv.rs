//! The built-in `csv` transform: splits a log record's message as one CSV row and merges the
//! named columns into the event's attributes -- the delimiter-separated sibling of `json`. See
//! `docs/adr/csv-positional-columns.md` for the design decisions this implements: an explicit,
//! positional `columns:` schema (no header-row mode), RFC 4180 quoting within one line (embedded
//! newlines out of scope), no type coercion, and a wrong field count passing the event through
//! unchanged.
//!
//! Stateless -- like `json`/`scale`, only `process` is overridden; `flush_interval`/`flush` keep
//! the `Transform` trait's defaults.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{Diagnostics, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Splits `event.log.message` on `delimiter` per RFC 4180 and merges the named columns into
/// `event.attributes`, verbatim as `Value::Str` -- no type coercion, no renaming, no prefix. See
/// the module doc comment/ADR for the full design.
pub struct CsvParser {
    /// Interned once at construction, never per event.
    columns: Vec<Symbol>,
    delimiter: u8,
    /// The configured columns rendered back as the header line they'd appear as in the source
    /// file, built once at construction. A message byte-equal to this is the source's header
    /// row, recognized by *value* rather than position -- see the ADR for why position is
    /// unusable here (`read_from: end`, checkpointed restart, rotation, fan-in).
    header_line: Vec<u8>,
    /// Per-field `(start, end, needs_unescape)` byte offsets into the current message, reused
    /// across events. Offsets, not `Value`s, deliberately: nothing here holds a `Bytes`
    /// refcount, so unlike `json`'s scratch this can never pin an event's message buffer alive
    /// past `process`.
    scratch: Vec<(u32, u32, bool)>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl CsvParser {
    /// `columns` is the config-declared schema (left to right); `delimiter` is a single ASCII
    /// byte, already validated at graph-resolution time (`docs/design/pipeline-graph.md`'s rule
    /// 29) to be neither `"` nor `\n`/`\r`/non-ASCII.
    pub fn new(columns: Vec<String>, delimiter: u8) -> Self {
        let header_line = columns.join(&(delimiter as char).to_string()).into_bytes();
        Self {
            columns: columns.iter().map(|c| intern(c)).collect(),
            delimiter,
            header_line,
            scratch: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for CsvParser {
    /// An event with no log, or a log whose message isn't a string, passes through untouched --
    /// there's nothing to parse. Any metrics/span already on the event ride through unaffected
    /// either way. An empty message is a routine, silently-skipped case (no diagnostic) --
    /// `logit.transform.rows.skipped{reason="empty"}` records it. A message that isn't valid
    /// UTF-8 also passes through unparsed with a throttled `invalid_utf8` diagnostic -- checked
    /// once for the whole message, since every column would otherwise be minted as `Value::Str`,
    /// whose invariant is valid UTF-8. A message equal to the configured header line passes
    /// through unparsed with a throttled `header_row` diagnostic. A malformed row (bad quoting)
    /// or one with the wrong field count also passes through unchanged, attributes untouched,
    /// with a throttled `parse_failure`/`field_count` diagnostic naming what went wrong.
    /// Otherwise every column lands as `Value::Str`, last-writer-wins on collision with a
    /// pre-existing attribute of the same name.
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        let Some(log) = &event.log else { return Some(event) };
        let raw = match &log.message {
            Value::Str(b) | Value::Bytes(b) => b.clone(),
            _ => return Some(event),
        };

        // An empty line is routine, not exceptional. Silent skip, no diagnostic.
        if raw.is_empty() {
            self.telemetry.count("logit.transform.rows.skipped", 1.0, &[("reason", "empty")]);
            return Some(event);
        }

        // Every field below is handed to `Value::Str`, whose invariant is valid UTF-8 -- four
        // `.expect("Value::Str is always valid UTF-8")` call sites downstream
        // (`logit_core::Value::as_str`, `logit-proto`'s OTLP encoder, `stdio`/`syslog`'s
        // renderers) panic outright if that's violated. A `Value::Bytes` message carries no such
        // guarantee (an OTLP body's `bytes_value` decodes straight into one,
        // `crates/logit-proto/src/otlp/common.rs`), so validate here -- once, for the whole
        // message, not per field. One check is sufficient: `delimiter` is a single ASCII byte and
        // `"` is ASCII (rule 29, `crates/logit-pipeline/src/graph.rs`), so every boundary
        // `split_row` computes falls on an ASCII byte and never inside a multi-byte sequence, and
        // `unescape` only ever deletes an ASCII `"` -- both keep a valid whole valid in its parts.
        if std::str::from_utf8(&raw).is_err() {
            self.diag.warn_throttled(
                "invalid_utf8",
                "message is not valid UTF-8, passing event through unparsed",
            );
            return Some(event);
        }

        if raw[..] == self.header_line[..] {
            self.diag.warn_throttled(
                "header_row",
                "message is the configured header row, passing event through unparsed",
            );
            return Some(event);
        }

        self.scratch.clear();
        if let Err(err) = split_row(&raw, self.delimiter, &mut self.scratch) {
            self.diag.warn_throttled(
                "parse_failure",
                format_args!("malformed CSV row, passing event through: {err}"),
            );
            return Some(event);
        }
        if self.scratch.len() != self.columns.len() {
            self.diag.warn_throttled(
                "field_count",
                format_args!(
                    "expected {} fields, got {} -- passing event through unparsed",
                    self.columns.len(),
                    self.scratch.len()
                ),
            );
            return Some(event);
        }

        for (&sym, &(start, end, needs_unescape)) in self.columns.iter().zip(self.scratch.iter()) {
            let field = raw.slice(start as usize..end as usize);
            let value = if needs_unescape { unescape(&field) } else { field };
            event.attributes.insert_sym(sym, Value::Str(value));
        }
        self.telemetry.count("logit.transform.rows.parsed", 1.0, &[]);
        Some(event)
    }
}

/// Why a [`split_row`] call failed. `Display`ed into a `Diagnostics::warn_throttled` message, so
/// no need for this to be more than the two shapes an RFC-4180-minus-embedded-newlines grammar
/// can actually produce.
#[derive(Debug, PartialEq, Eq)]
enum RowError {
    /// End of input reached inside a quoted field, with no closing `"` -- either a genuinely
    /// malformed row, or (per the ADR) the first half of a record whose embedded newline already
    /// split it into two separate events.
    UnterminatedQuote,
    /// A closing `"` was followed by something other than the delimiter or end-of-line (e.g.
    /// `"a"b,c`) -- a quoted field must be the *entire* field, not just a prefix of it.
    TrailingAfterQuote,
}

impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowError::UnterminatedQuote => write!(f, "unterminated quoted field"),
            RowError::TrailingAfterQuote => {
                write!(f, "unexpected content after a closing quote")
            }
        }
    }
}

/// Splits `line` on `delim` per RFC 4180 quoting, appending each field's `(start, end,
/// needs_unescape)` byte-offset triple to `out` (cleared by the caller first). `needs_unescape`
/// is set only for a quoted field containing a doubled `""` -- the caller's cue to run
/// [`unescape`] rather than slice `line` directly.
///
/// Precondition: `!line.is_empty()` (the caller special-cases an empty message before ever
/// calling this).
fn split_row(line: &Bytes, delim: u8, out: &mut Vec<(u32, u32, bool)>) -> Result<(), RowError> {
    let n = line.len();
    let mut i = 0usize;

    loop {
        let start;
        let end;
        let needs_unescape;
        let next;

        if line[i] == b'"' {
            // Quoted field. Everything until the closing quote is data, including `delim`.
            start = i + 1;
            let mut j = start;
            let mut esc = false;
            loop {
                if j >= n {
                    return Err(RowError::UnterminatedQuote);
                }
                if line[j] == b'"' {
                    if j + 1 < n && line[j + 1] == b'"' {
                        esc = true; // a doubled quote: one literal `"`, keep scanning
                        j += 2;
                        continue;
                    }
                    break; // j is the closing quote
                }
                j += 1;
            }
            end = j; // exclusive: the closing quote's own index
            needs_unescape = esc;
            next = j + 1; // index just past the closing quote
                          // A closing quote must be followed by the delimiter or end-of-line.
            if next < n && line[next] != delim {
                return Err(RowError::TrailingAfterQuote);
            }
        } else {
            // Unquoted field: runs to the next delimiter or end-of-line. A `"` *inside* an
            // unquoted field is data.
            start = i;
            let mut j = i;
            while j < n && line[j] != delim {
                j += 1;
            }
            end = j;
            needs_unescape = false;
            next = j;
        }

        out.push((start as u32, end as u32, needs_unescape));

        if next >= n {
            break; // end of line: that was the last field
        }
        debug_assert_eq!(line[next], delim);
        i = next + 1;
        if i == n {
            // a trailing delimiter means a final empty field
            out.push((n as u32, n as u32, false));
            break;
        }
    }
    Ok(())
}

/// Collapses each doubled `""` to one `"`. The only path in this transform that allocates --
/// exactly once: `bytes::Bytes::from(Vec<u8>)` takes the cheap `into_boxed_slice` path (no extra
/// allocation beyond the vec's own buffer) only when the vec's length equals its capacity, so the
/// output length is counted in a first pass and the vec is sized exactly, rather than
/// `field.len()` (always an overestimate whenever there's a doubled quote to collapse, which
/// would otherwise force `Bytes::from` down its second, eagerly-allocating path).
fn unescape(field: &Bytes) -> Bytes {
    let mut out_len = 0;
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'"' && i + 1 < field.len() && field[i + 1] == b'"' {
            i += 2;
        } else {
            i += 1;
        }
        out_len += 1;
    }

    let mut out = Vec::with_capacity(out_len);
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'"' && i + 1 < field.len() && field[i + 1] == b'"' {
            out.push(b'"');
            i += 2;
        } else {
            out.push(field[i]);
            i += 1;
        }
    }
    debug_assert_eq!(out.len(), out.capacity());
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn bytes_log_event(message: &'static [u8]) -> Event {
        Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::Bytes(Bytes::from_static(message)),
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

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn attr<'a>(event: &'a Event, key: &str) -> Option<&'a Value> {
        event.attributes.get(key)
    }

    fn parser(columns: &[&str]) -> CsvParser {
        CsvParser::new(columns.iter().map(|c| c.to_string()).collect(), b',')
    }

    // -- split_row -------------------------------------------------------------------------

    fn split(input: &str, delim: u8) -> Result<Vec<String>, RowError> {
        let bytes = Bytes::copy_from_slice(input.as_bytes());
        let mut out = Vec::new();
        split_row(&bytes, delim, &mut out)?;
        Ok(out
            .into_iter()
            .map(|(start, end, needs_unescape)| {
                let field = bytes.slice(start as usize..end as usize);
                let field = if needs_unescape { unescape(&field) } else { field };
                String::from_utf8(field.to_vec()).unwrap()
            })
            .collect())
    }

    #[test]
    fn split_row_worked_examples() {
        assert_eq!(split("a,b,c", b',').unwrap(), vec!["a", "b", "c"]);
        assert_eq!(split("a,,c", b',').unwrap(), vec!["a", "", "c"]);
        assert_eq!(split("a,b,", b',').unwrap(), vec!["a", "b", ""]);
        assert_eq!(split(r#""a,b",c"#, b',').unwrap(), vec!["a,b", "c"]);
        assert_eq!(split(r#""a""b",c"#, b',').unwrap(), vec!["a\"b", "c"]);
        assert_eq!(split(r#"""#, b',').unwrap_err(), RowError::UnterminatedQuote);
        assert_eq!(split(r#""",a"#, b',').unwrap(), vec!["", "a"]);
        assert_eq!(split(r#"he said "hi",b"#, b',').unwrap(), vec!["he said \"hi\"", "b"]);
        assert_eq!(split(r#""a"b,c"#, b',').unwrap_err(), RowError::TrailingAfterQuote);
        assert_eq!(split(r#"a,"b"#, b',').unwrap_err(), RowError::UnterminatedQuote);
    }

    #[test]
    fn split_row_single_unterminated_quote() {
        assert_eq!(split("\"", b',').unwrap_err(), RowError::UnterminatedQuote);
    }

    // -- Happy path --------------------------------------------------------------------------

    #[test]
    fn a_plain_row_populates_every_column() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let event = log_event("1,2,3");
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("3")));
    }

    #[test]
    fn every_value_is_a_string_even_when_it_looks_numeric() {
        let mut csv = parser(&["status", "bytes"]);
        let resource = default_resource();
        let event = log_event("200,612");
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
        assert_eq!(attr(&event, "bytes"), Some(&Value::str("612")));
    }

    #[test]
    fn an_empty_field_becomes_an_empty_string_not_null() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let event = log_event("1,,3");
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "b"), Some(&Value::str("")));
    }

    #[test]
    fn a_trailing_delimiter_yields_a_final_empty_field() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let event = log_event("1,2,");
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "c"), Some(&Value::str("")));
    }

    #[test]
    fn a_tab_delimiter_parses_a_tsv_row() {
        let mut csv = CsvParser::new(vec!["a".to_string(), "b".to_string()], b'\t');
        let resource = default_resource();
        let event = log_event("1\t2");
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    // -- Quoting -------------------------------------------------------------------------------

    #[test]
    fn a_quoted_field_containing_the_delimiter_keeps_it_as_data() {
        let mut csv = parser(&["path", "status"]);
        let resource = default_resource();
        let event = log_event(r#""/a,b",200"#);
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "path"), Some(&Value::str("/a,b")));
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
    }

    #[test]
    fn a_doubled_quote_inside_a_quoted_field_becomes_one_quote() {
        let mut csv = parser(&["msg", "status"]);
        let resource = default_resource();
        let event = log_event(r#""a""b",200"#);
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("a\"b")));
    }

    #[test]
    fn an_empty_quoted_field_is_an_empty_string() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = log_event(r#""",x"#);
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("")));
    }

    #[test]
    fn a_quote_inside_an_unquoted_field_is_data() {
        let mut csv = parser(&["msg", "status"]);
        let resource = default_resource();
        let event = log_event(r#"he said "hi",200"#);
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "msg"), Some(&Value::str(r#"he said "hi""#)));
    }

    #[test]
    fn an_unterminated_quote_passes_the_event_through_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = log_event(r#"a,"b"#);
        let original = message_of(&log_event(r#"a,"b"#)).clone();
        let event = csv.process(&resource, event).expect("always forwards");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &original);
    }

    #[test]
    fn content_after_a_closing_quote_passes_the_event_through_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = log_event(r#""a"b,c"#);
        let event = csv.process(&resource, event).expect("always forwards");
        assert!(event.attributes.is_empty());
    }

    // -- Field count -----------------------------------------------------------------------

    #[test]
    fn too_few_fields_passes_through_with_attributes_untouched() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let event = log_event("1,2");
        let event = csv.process(&resource, event).expect("always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn too_many_fields_passes_through_with_attributes_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = log_event("1,2,3");
        let event = csv.process(&resource, event).expect("always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_wrong_count_does_not_leave_attributes_half_populated() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let mut event = log_event("1,2");
        event.attributes.insert("preexisting", Value::str("keep-me"));
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(event.attributes.len(), 1);
        assert_eq!(attr(&event, "preexisting"), Some(&Value::str("keep-me")));
    }

    // -- Header ------------------------------------------------------------------------------

    #[test]
    fn a_row_equal_to_the_configured_header_passes_through_unparsed() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = log_event("a,b");
        let event = csv.process(&resource, event).expect("always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_header_row_arriving_after_data_rows_is_still_recognized() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let data = csv.process(&resource, log_event("1,2")).expect("always forwards");
        assert_eq!(attr(&data, "a"), Some(&Value::str("1")));

        let header = csv.process(&resource, log_event("a,b")).expect("always forwards");
        assert!(header.attributes.is_empty(), "the header row must still be recognized");
    }

    #[test]
    fn a_header_row_does_not_change_how_the_next_row_parses() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        drop(csv.process(&resource, log_event("a,b")));
        let event = csv.process(&resource, log_event("1,2")).expect("always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    // -- UTF-8 validation --------------------------------------------------------------------

    /// The bytes here are valid CSV *framing* (two fields around a comma) but invalid UTF-8, so
    /// `split_row` would happily produce two fields -- proving the gate is the UTF-8 check and
    /// not some incidental parse failure.
    #[test]
    fn an_invalid_utf8_message_passes_the_event_through_untouched() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("csv", "csv", "transform");
        let diag = Diagnostics::new("csv").with_telemetry(telemetry);
        let mut csv = parser(&["a", "b"]).with_diagnostics(diag);
        let resource = default_resource();
        let event = bytes_log_event(&[0xff, b',', 0xfe]);
        let event = csv.process(&resource, event).expect("always forwards");
        assert!(event.attributes.is_empty(), "no Value::Str may be minted from invalid UTF-8");
        assert_eq!(message_of(&event), &Value::Bytes(Bytes::from_static(&[0xff, b',', 0xfe])));

        let events = registry.drain(0);
        let fired = events
            .iter()
            .any(|e| e.attributes.get("key").and_then(|v| v.as_str()) == Some("invalid_utf8"));
        assert!(fired, "expected logit.component.diagnostics{{key=\"invalid_utf8\"}}");
    }

    /// The check rejects invalid UTF-8, not `Value::Bytes` as a message kind -- a bytes-valued
    /// OTLP body that happens to be text still parses exactly like a `Value::Str` one.
    #[test]
    fn a_valid_utf8_bytes_message_parses_like_a_string_message() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = csv.process(&resource, bytes_log_event(b"1,2")).expect("always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    /// Why one whole-message check is enough for every field: the delimiter and `"` are both
    /// ASCII (rule 29), so no field boundary can land inside a multi-byte sequence.
    #[test]
    fn a_multi_byte_utf8_field_is_sliced_intact() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = csv.process(&resource, log_event("héllo,wörld")).expect("always forwards");
        assert_eq!(attr(&event, "a").and_then(Value::as_str), Some("héllo"));
        assert_eq!(attr(&event, "b").and_then(Value::as_str), Some("wörld"));
    }

    // -- Empty/non-candidates ----------------------------------------------------------------

    #[test]
    fn an_empty_line_passes_through_with_no_diagnostic() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("csv", "csv", "transform");
        let mut csv = parser(&["a", "b"]).with_telemetry(telemetry);
        let resource = default_resource();
        let event = csv.process(&resource, log_event("")).expect("always forwards");
        assert!(event.attributes.is_empty());

        let events = registry.drain(0);
        let skipped = events.iter().any(|e| {
            e.metrics
                .iter()
                .any(|m| logit_core::interner::resolve(m.name) == "logit.transform.rows.skipped")
        });
        assert!(skipped, "an empty line should record the skipped counter");
    }

    #[test]
    fn a_metric_event_passes_through_with_attributes_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)),
        );
        let event = csv.process(&resource, event).expect("metric-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_span_event_passes_through_with_attributes_untouched() {
        let mut csv = parser(&["a", "b"]);
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
                flags: 0,
                ext: None,
            },
        );
        let event = csv.process(&resource, event).expect("span-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_log_event_that_also_carries_a_metric_is_parsed_and_keeps_its_metric() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event("1,2");
        event
            .metrics
            .push(MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)));
        let event = csv.process(&resource, event).expect("mixed events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(event.metrics.len(), 1, "the metric should ride through unaffected");
    }

    #[test]
    fn a_non_string_message_passes_through_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::I64(42),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let event = csv.process(&resource, event).expect("always forwards");
        assert!(event.attributes.is_empty());
    }

    // -- Merge -------------------------------------------------------------------------------

    #[test]
    fn a_parsed_column_overwrites_a_pre_existing_attribute_of_the_same_name() {
        let mut csv = parser(&["a"]);
        let resource = default_resource();
        let mut event = log_event("1");
        event.attributes.insert("a", Value::str("old"));
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
    }

    #[test]
    fn the_message_and_body_format_are_left_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = log_event("1,2");
        let original_message = message_of(&event).clone();
        let event = csv.process(&resource, event).expect("always forwards");
        assert_eq!(message_of(&event), &original_message);
        assert_eq!(
            event.log.as_ref().expect("event should carry a log").body_format,
            BodyFormat::Raw
        );
    }

    // -- Zero-copy -----------------------------------------------------------------------------

    #[test]
    fn an_unquoted_fields_value_is_a_zero_copy_slice_of_the_message() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let event = log_event("hello,world");
        let message_ptr_range = match message_of(&event) {
            Value::Str(b) => (b.as_ptr() as usize, b.as_ptr() as usize + b.len()),
            other => panic!("expected Str, got {other:?}"),
        };
        let event = csv.process(&resource, event).expect("always forwards");
        match attr(&event, "a") {
            Some(Value::Str(b)) => {
                let start = b.as_ptr() as usize;
                let end = start + b.len();
                assert!(
                    start >= message_ptr_range.0 && end <= message_ptr_range.1,
                    "field 'a' should be a slice of the original message buffer"
                );
            }
            other => panic!("expected Str, got {other:?}"),
        }
    }
}
