//! `csv`: splits a log message as one CSV row and merges the named columns into the event's
//! attributes. See `docs/adr/csv-positional-columns.md`: an explicit, positional `columns:`
//! schema (no header-row mode), RFC 4180 quoting within one line (no embedded newlines), no type
//! coercion, and a wrong field count passing the event through unchanged.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{Diagnostics, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Splits `event.log.message` on `delimiter` per RFC 4180 and merges the named columns into
/// `event.attributes` verbatim as `Value::Str`: no coercion, renaming, or prefix.
pub struct CsvParser {
    columns: Vec<Symbol>,
    delimiter: u8,
    /// The configured columns joined as the source's header line. A message byte-equal to it is
    /// the header row, recognized by value because position is unusable (`read_from: end`,
    /// checkpointed restart, rotation, fan-in; see the ADR).
    header_line: Vec<u8>,
    /// Per-field `(start, end, needs_unescape)` offsets into the current message, reused across
    /// events. Offsets rather than `Value`s, so no `Bytes` refcount pins a message past `process`.
    scratch: Vec<(u32, u32, bool)>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl CsvParser {
    /// `columns` is the schema, left to right. Graph rule 32 has checked that `delimiter` is one
    /// ASCII byte other than `"`, `\n`, or `\r`.
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
    /// Merges the row's columns as `Value::Str`, overwriting any existing attribute of the name.
    ///
    /// Every failure forwards the event unchanged:
    /// - no log, or a message that isn't `Str`/`Bytes`: nothing to parse;
    /// - an empty message: no diagnostic, counted as
    ///   `logit.transform.rows.skipped{reason="empty"}`;
    /// - invalid UTF-8, the header line, bad quoting, or the wrong field count: a throttled
    ///   `invalid_utf8`/`header_row`/`parse_failure`/`field_count` diagnostic.
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        let Some(log) = &event.log else { return true };
        let raw = match &log.message {
            Value::Str(b) | Value::Bytes(b) => b.clone(),
            _ => return true,
        };

        if raw.is_empty() {
            self.telemetry.count("logit.transform.rows.skipped", 1.0, &[("reason", "empty")]);
            return true;
        }

        // Every field becomes a `Value::Str`, which must be valid UTF-8: `Value::as_str`, the
        // OTLP encoder, and the stdio/syslog renderers `.expect` it. A `Value::Bytes` message
        // (an OTLP `bytes_value` body) has no such guarantee. One whole-message check suffices:
        // the delimiter and `"` are ASCII (rule 32), so no field boundary splits a multi-byte
        // sequence, and `unescape` only deletes an ASCII `"`.
        if std::str::from_utf8(&raw).is_err() {
            self.diag.warn_throttled(
                "invalid_utf8",
                "message is not valid UTF-8, passing event through unparsed",
            );
            return true;
        }

        if raw[..] == self.header_line[..] {
            self.diag.warn_throttled(
                "header_row",
                "message is the configured header row, passing event through unparsed",
            );
            return true;
        }

        self.scratch.clear();
        if let Err(err) = split_row(&raw, self.delimiter, &mut self.scratch) {
            self.diag.warn_throttled(
                "parse_failure",
                format_args!("malformed CSV row, passing event through: {err}"),
            );
            return true;
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
            return true;
        }

        for (&sym, &(start, end, needs_unescape)) in self.columns.iter().zip(self.scratch.iter()) {
            let field = raw.slice(start as usize..end as usize);
            let value = if needs_unescape { unescape(&field) } else { field };
            event.attributes.insert_sym(sym, Value::Str(value));
        }
        self.telemetry.count("logit.transform.rows.parsed", 1.0, &[]);
        true
    }
}

/// Why a [`split_row`] call failed; `Display`ed into the `parse_failure` diagnostic.
#[derive(Debug, PartialEq, Eq)]
enum RowError {
    /// End of input inside a quoted field: a malformed row, or the first half of a record an
    /// embedded newline already split into two events.
    UnterminatedQuote,
    /// A closing `"` followed by something other than the delimiter or end of line (`"a"b,c`).
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
/// needs_unescape)` offsets to `out`.
///
/// `needs_unescape` marks a quoted field containing a doubled `""`, which needs [`unescape`]
/// rather than a slice. Requires a non-empty `line`.
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
            // Unquoted field, to the next delimiter or end of line; a `"` inside it is data.
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

/// Collapses each doubled `""` to one `"`: the only path in this transform that allocates.
///
/// It allocates once because the first pass sizes the `Vec` exactly: `Bytes::from(Vec<u8>)`
/// avoids a second allocation only when length equals capacity, and `field.len()` would
/// overestimate.
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
        let mut event = log_event("1,2,3");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("3")));
    }

    #[test]
    fn every_value_is_a_string_even_when_it_looks_numeric() {
        let mut csv = parser(&["status", "bytes"]);
        let resource = default_resource();
        let mut event = log_event("200,612");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
        assert_eq!(attr(&event, "bytes"), Some(&Value::str("612")));
    }

    #[test]
    fn an_empty_field_becomes_an_empty_string_not_null() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let mut event = log_event("1,,3");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "b"), Some(&Value::str("")));
    }

    #[test]
    fn a_trailing_delimiter_yields_a_final_empty_field() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let mut event = log_event("1,2,");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "c"), Some(&Value::str("")));
    }

    #[test]
    fn a_tab_delimiter_parses_a_tsv_row() {
        let mut csv = CsvParser::new(vec!["a".to_string(), "b".to_string()], b'\t');
        let resource = default_resource();
        let mut event = log_event("1\t2");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    // -- Quoting -------------------------------------------------------------------------------

    #[test]
    fn a_quoted_field_containing_the_delimiter_keeps_it_as_data() {
        let mut csv = parser(&["path", "status"]);
        let resource = default_resource();
        let mut event = log_event(r#""/a,b",200"#);
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "path"), Some(&Value::str("/a,b")));
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
    }

    #[test]
    fn a_doubled_quote_inside_a_quoted_field_becomes_one_quote() {
        let mut csv = parser(&["msg", "status"]);
        let resource = default_resource();
        let mut event = log_event(r#""a""b",200"#);
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("a\"b")));
    }

    #[test]
    fn an_empty_quoted_field_is_an_empty_string() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event(r#""",x"#);
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("")));
    }

    #[test]
    fn a_quote_inside_an_unquoted_field_is_data() {
        let mut csv = parser(&["msg", "status"]);
        let resource = default_resource();
        let mut event = log_event(r#"he said "hi",200"#);
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "msg"), Some(&Value::str(r#"he said "hi""#)));
    }

    #[test]
    fn an_unterminated_quote_passes_the_event_through_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event(r#"a,"b"#);
        let original = message_of(&log_event(r#"a,"b"#)).clone();
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &original);
    }

    #[test]
    fn content_after_a_closing_quote_passes_the_event_through_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event(r#""a"b,c"#);
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    // -- Field count -----------------------------------------------------------------------

    #[test]
    fn too_few_fields_passes_through_with_attributes_untouched() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let mut event = log_event("1,2");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn too_many_fields_passes_through_with_attributes_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event("1,2,3");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_wrong_count_does_not_leave_attributes_half_populated() {
        let mut csv = parser(&["a", "b", "c"]);
        let resource = default_resource();
        let mut event = log_event("1,2");
        event.attributes.insert("preexisting", Value::str("keep-me"));
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(event.attributes.len(), 1);
        assert_eq!(attr(&event, "preexisting"), Some(&Value::str("keep-me")));
    }

    // -- Header ------------------------------------------------------------------------------

    #[test]
    fn a_row_equal_to_the_configured_header_passes_through_unparsed() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event("a,b");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_header_row_arriving_after_data_rows_is_still_recognized() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut data = log_event("1,2");
        assert!(csv.process(&resource, &mut data), "always forwards");
        assert_eq!(attr(&data, "a"), Some(&Value::str("1")));

        let mut header = log_event("a,b");
        assert!(csv.process(&resource, &mut header), "always forwards");
        assert!(header.attributes.is_empty(), "the header row must still be recognized");
    }

    #[test]
    fn a_header_row_does_not_change_how_the_next_row_parses() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut header = log_event("a,b");
        let _ = csv.process(&resource, &mut header);
        let mut event = log_event("1,2");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    // -- UTF-8 validation --------------------------------------------------------------------

    /// Valid CSV framing but invalid UTF-8, so the UTF-8 check, not parsing, is what rejects it.
    #[test]
    fn an_invalid_utf8_message_passes_the_event_through_untouched() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("csv", "csv", "transform");
        let diag = Diagnostics::new("csv").with_telemetry(telemetry);
        let mut csv = parser(&["a", "b"]).with_diagnostics(diag);
        let resource = default_resource();
        let mut event = bytes_log_event(&[0xff, b',', 0xfe]);
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty(), "no Value::Str may be minted from invalid UTF-8");
        assert_eq!(message_of(&event), &Value::Bytes(Bytes::from_static(&[0xff, b',', 0xfe])));

        let events = registry.drain(0);
        let fired = events
            .iter()
            .any(|e| e.attributes.get("key").and_then(|v| v.as_str()) == Some("invalid_utf8"));
        assert!(fired, "expected logit.component.diagnostics{{key=\"invalid_utf8\"}}");
    }

    /// A `Value::Bytes` message holding valid UTF-8 parses like a `Value::Str` one.
    #[test]
    fn a_valid_utf8_bytes_message_parses_like_a_string_message() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = bytes_log_event(b"1,2");
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    /// No field boundary lands inside a multi-byte sequence (the delimiter is ASCII, rule 32).
    #[test]
    fn a_multi_byte_utf8_field_is_sliced_intact() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event("héllo,wörld");
        assert!(csv.process(&resource, &mut event), "always forwards");
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
        let mut event = log_event("");
        assert!(csv.process(&resource, &mut event), "always forwards");
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
        let mut event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)),
        );
        assert!(csv.process(&resource, &mut event), "metric-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_span_event_passes_through_with_attributes_untouched() {
        let mut csv = parser(&["a", "b"]);
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
        assert!(csv.process(&resource, &mut event), "span-only events pass through");
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
        assert!(csv.process(&resource, &mut event), "mixed events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(event.metrics.len(), 1, "the metric should ride through unaffected");
    }

    #[test]
    fn a_non_string_message_passes_through_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = Event::log(
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
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert!(event.attributes.is_empty());
    }

    // -- Merge -------------------------------------------------------------------------------

    #[test]
    fn a_parsed_column_overwrites_a_pre_existing_attribute_of_the_same_name() {
        let mut csv = parser(&["a"]);
        let resource = default_resource();
        let mut event = log_event("1");
        event.attributes.insert("a", Value::str("old"));
        assert!(csv.process(&resource, &mut event), "always forwards");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
    }

    #[test]
    fn the_message_and_body_format_are_left_untouched() {
        let mut csv = parser(&["a", "b"]);
        let resource = default_resource();
        let mut event = log_event("1,2");
        let original_message = message_of(&event).clone();
        assert!(csv.process(&resource, &mut event), "always forwards");
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
        let mut event = log_event("hello,world");
        let message_ptr_range = match message_of(&event) {
            Value::Str(b) => (b.as_ptr() as usize, b.as_ptr() as usize + b.len()),
            other => panic!("expected Str, got {other:?}"),
        };
        assert!(csv.process(&resource, &mut event), "always forwards");
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
