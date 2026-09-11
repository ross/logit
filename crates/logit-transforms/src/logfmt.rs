//! The built-in `logfmt` and `kv` transforms: parse a log record's message as `key=value` pairs
//! and merge them into the event's attributes, exactly like `json` but for two different flat,
//! text (not JSON) grammars. See `docs/adr/logfmt-and-kv-parsing.md` for the design decisions
//! this implements.
//!
//! `logfmt` is the fixed, de-facto convention (`level=info msg="hello world" dur=3ms`) --
//! whitespace-delimited pairs, `"`-quoted values with backslash escapes, zero required config.
//! `kv` is a literal splitter with **required** `pair_sep`/`kv_sep` and no quoting at all
//! (`a=1&b=2`, `a: 1, b: 2`) -- two distinct `ComponentKind`s sharing this module and a scan tail,
//! not one kind with a mode flag, mirroring `keep`/`remove` and `keep_signals`/`drop_signals`'s
//! existing precedent.
//!
//! Both are stateless -- like `json`, only `process` is overridden; `flush_interval`/`flush` keep
//! the `Transform` trait's defaults. Both always produce `Value::Str` (or `Value::Bool(true)` for
//! an opted-in bareword) -- never numeric coercion, unlike `json`'s type-by-JSON-syntax rule; a
//! downstream `scale`/`kv_metrics` still works fine against a `Value::Str` (`crate::numeric`
//! parses it).

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{AttrMap, Diagnostics, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::fmt;
use std::sync::Arc;

const fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

/// `true` if `s` contains nothing but the four whitespace bytes [`is_ws`] recognizes (including
/// the empty string) -- the line between "there was genuinely nothing to parse" (no diagnostic)
/// and "there was content, but none of it parsed" (a real `parse_failure`).
fn is_blank(s: &str) -> bool {
    s.bytes().all(is_ws)
}

enum ParseError {
    /// A `"` was opened but never closed (or its closing `"` was consumed as an escape target) --
    /// carries the byte offset of the opening quote, for the diagnostic message.
    UnterminatedQuote(usize),
    /// Nothing in the line produced a real pair -- see each parser's own `saw_pair` bookkeeping.
    NoPairs,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnterminatedQuote(pos) => {
                write!(f, "unterminated quote starting at byte {pos}")
            }
            ParseError::NoPairs => write!(f, "no key=value pairs found"),
        }
    }
}

/// Moves every pair out of `scratch` into `attrs`, via [`AttrMap::insert_sym`] rather than
/// `resolve()` -> `insert(&str)` -- `scratch`'s keys are already `Symbol`s (interned straight off
/// the message's byte slice), so re-resolving one to a `&str` just to re-intern it would be pure
/// waste. Last-write-wins on a duplicate key is free here: `scratch` may push the same key twice
/// (a duplicate in the source line), and `insert_sym` overwrites on collision, so merging in push
/// order makes the later occurrence win -- identical to `json`'s own duplicate-key policy.
fn merge_into(scratch: &mut Vec<(Symbol, Value)>, attrs: &mut AttrMap) {
    for (key, value) in scratch.drain(..) {
        attrs.insert_sym(key, value);
    }
}

/// Consumes the five escapes `logfmt` understands; anything else (including a lone trailing
/// backslash, which [`scan_quoted`] never hands this since it always pairs a backslash with the
/// byte after it) is preserved verbatim, backslash included. The only allocating path in this
/// module -- every other value is a [`bytes::Bytes::slice`] of the original message.
///
/// `shrink_to_fit` before the final conversion is load-bearing, not cosmetic: every escape this
/// function understands consumes two source bytes and emits one, so `out.len()` is *always*
/// strictly less than the `with_capacity(bytes.len())` estimate whenever there was any escape to
/// resolve at all -- and `bytes::Bytes::from(Vec<u8>)` allocates a second, separate `Shared`
/// control block up front (eagerly, not lazily) whenever `len() != capacity()`, rather than the
/// single deferred-promotion allocation it costs when they match. `shrink_to_fit` turns that
/// mismatch into a `realloc` (already paid for by the initial `with_capacity`, and not what
/// `crates/logit-bench/tests/allocations.rs` asserts on) instead of a second, independent `alloc`.
fn unescape(bytes: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            match bytes[i + 1] {
                b'\\' => out.push(b'\\'),
                b'"' => out.push(b'"'),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                other => {
                    out.push(b'\\');
                    out.push(other);
                }
            }
            i += 2;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out.shrink_to_fit();
    Bytes::from(out)
}

/// Scans a `"`-quoted value starting at `s[i] == b'"'`. Returns the content's `(start, end)` byte
/// range (exclusive of the quotes), whether it contained any backslash escape, and the byte index
/// just past the closing `"`. An escape is consumed two bytes at a time (backslash + whatever
/// follows) without interpreting it here -- [`unescape`] does that, only when needed.
fn scan_quoted(s: &[u8], i: usize) -> Result<(usize, usize, bool, usize), ParseError> {
    let n = s.len();
    let mut j = i + 1;
    let mut has_escape = false;
    while j < n {
        match s[j] {
            b'\\' => {
                if j + 1 >= n {
                    return Err(ParseError::UnterminatedQuote(i));
                }
                has_escape = true;
                j += 2;
            }
            b'"' => return Ok((i + 1, j, has_escape, j + 1)),
            _ => j += 1,
        }
    }
    Err(ParseError::UnterminatedQuote(i))
}

/// Parses `text` (the whole message, already verified valid UTF-8 by the caller) as logfmt into
/// `out`. `raw` shares `text`'s underlying bytes -- every unquoted or escape-free-quoted value is
/// sliced straight out of it (`raw.slice`, a `Bytes` refcount bump), never copied. A key is
/// interned straight off `text`'s own bytes (`&text[key_start..key_end]`), never through an owned
/// `String` -- safe because every delimiter this scanner splits on (whitespace, `=`, `"`) is a
/// single-byte ASCII character, so a byte range this function ever slices always lands on a `text`
/// char boundary. See the EBNF and algorithm in `docs/adr/logfmt-and-kv-parsing.md`.
fn parse_logfmt(
    raw: &Bytes,
    text: &str,
    bare_keys: bool,
    out: &mut Vec<(Symbol, Value)>,
    telemetry: &Telemetry,
) -> Result<(), ParseError> {
    let s = text.as_bytes();
    let n = s.len();
    let mut i = 0;
    let mut saw_pair = false;

    loop {
        while i < n && is_ws(s[i]) {
            i += 1;
        }
        if i >= n {
            break;
        }

        let key_start = i;
        while i < n && !is_ws(s[i]) && s[i] != b'=' {
            i += 1;
        }
        let key_end = i;

        if key_end == key_start {
            // A leading '=' with no key (`=1 a=2`): resynchronize at the next whitespace rather
            // than treating the rest of the line as unparseable.
            while i < n && !is_ws(s[i]) {
                i += 1;
            }
            telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
            continue;
        }

        let value = if i < n && s[i] == b'=' {
            i += 1;
            saw_pair = true;
            if i < n && s[i] == b'"' {
                let (content_start, content_end, has_escape, next) = scan_quoted(s, i)?;
                let value = if has_escape {
                    Value::Str(unescape(&s[content_start..content_end]))
                } else {
                    Value::Str(raw.slice(content_start..content_end))
                };
                i = next;
                value
            } else {
                let v_start = i;
                while i < n && !is_ws(s[i]) {
                    i += 1;
                }
                Value::Str(raw.slice(v_start..i))
            }
        } else if bare_keys {
            // A bareword deliberately does *not* set `saw_pair` -- a line of nothing but
            // barewords still fails as `NoPairs` even with `bare_keys` on, the same as it would
            // with `bare_keys` off. See `docs/adr/logfmt-and-kv-parsing.md`.
            Value::Bool(true)
        } else {
            telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
            continue;
        };

        out.push((intern(&text[key_start..key_end]), value));
        telemetry.count("logit.transform.pairs.parsed", 1.0, &[]);
    }

    if !saw_pair {
        return Err(ParseError::NoPairs);
    }
    Ok(())
}

/// Finds `needle`'s first occurrence in `haystack` at or after byte offset `from`, or `None`.
/// Plain byte search -- both `logfmt`/`kv`'s separators and `text` are valid UTF-8, and finding a
/// complete, valid UTF-8 string as a byte substring of another always lands on a char boundary
/// (UTF-8's self-synchronization property), so no separate boundary check is needed.
fn find_bytes(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from > haystack.len() {
        return None;
    }
    haystack[from..].windows(needle.len()).position(|w| w == needle).map(|i| from + i)
}

/// Trims [`is_ws`] bytes off both ends of `bytes[start..end]`, returning the trimmed range.
fn trim_ws_range(bytes: &[u8], mut start: usize, mut end: usize) -> std::ops::Range<usize> {
    while start < end && is_ws(bytes[start]) {
        start += 1;
    }
    while end > start && is_ws(bytes[end - 1]) {
        end -= 1;
    }
    start..end
}

/// Parses one `kv` segment (`text[seg_start..seg_end]`, already isolated by [`parse_kv`]'s
/// `pair_sep` split) into at most one pair, pushed onto `out`. See [`parse_kv`]'s own doc comment
/// for the three distinct empty/bareword/no-separator shapes this handles.
#[allow(clippy::too_many_arguments)]
fn parse_kv_segment(
    raw: &Bytes,
    text: &str,
    seg_start: usize,
    seg_end: usize,
    kv_sep_bytes: &[u8],
    bare_keys: bool,
    out: &mut Vec<(Symbol, Value)>,
    saw_pair: &mut bool,
    telemetry: &Telemetry,
) {
    let bytes = text.as_bytes();
    let segment = &bytes[seg_start..seg_end];

    match find_bytes(segment, kv_sep_bytes, 0) {
        Some(idx) => {
            let key_range = trim_ws_range(segment, 0, idx);
            if key_range.is_empty() {
                // Key empty after trimming -- segment skipped, counted.
                telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
                return;
            }
            let value_range = trim_ws_range(segment, idx + kv_sep_bytes.len(), segment.len());
            let key = &text[(seg_start + key_range.start)..(seg_start + key_range.end)];
            let value_abs = (seg_start + value_range.start)..(seg_start + value_range.end);
            *saw_pair = true;
            out.push((intern(key), Value::Str(raw.slice(value_abs))));
            telemetry.count("logit.transform.pairs.parsed", 1.0, &[]);
        }
        None => {
            let key_range = trim_ws_range(segment, 0, segment.len());
            if key_range.is_empty() {
                // A genuinely empty segment (`a=1&&b=2`, or one that's all whitespace) --
                // skipped silently: nothing here to report as a failure.
                telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
                return;
            }
            if !bare_keys {
                telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
                return;
            }
            let key = &text[(seg_start + key_range.start)..(seg_start + key_range.end)];
            out.push((intern(key), Value::Bool(true)));
            telemetry.count("logit.transform.pairs.parsed", 1.0, &[]);
        }
    }
}

/// Parses `text` as `kv`: `pair_sep`-separated segments, each split on the *first* `kv_sep`
/// occurrence within it. No quoting, no escapes -- a value containing `pair_sep` is not
/// representable (`logfmt` is the component for that shape). See
/// `docs/adr/logfmt-and-kv-parsing.md`.
fn parse_kv(
    raw: &Bytes,
    text: &str,
    pair_sep: &str,
    kv_sep: &str,
    bare_keys: bool,
    out: &mut Vec<(Symbol, Value)>,
    telemetry: &Telemetry,
) -> Result<(), ParseError> {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let pair_sep_bytes = pair_sep.as_bytes();
    let kv_sep_bytes = kv_sep.as_bytes();

    let mut saw_pair = false;
    let mut cursor = 0usize;
    loop {
        let seg_end = find_bytes(bytes, pair_sep_bytes, cursor).unwrap_or(n);
        parse_kv_segment(
            raw,
            text,
            cursor,
            seg_end,
            kv_sep_bytes,
            bare_keys,
            out,
            &mut saw_pair,
            telemetry,
        );
        if seg_end >= n {
            break;
        }
        cursor = seg_end + pair_sep_bytes.len();
    }

    if !saw_pair {
        return Err(ParseError::NoPairs);
    }
    Ok(())
}

/// Reads `event.log.message` as a zero-copy `Bytes` handle (a refcount bump, not a copy) and
/// verifies it's valid UTF-8 -- shared by [`Logfmt::process`] and [`Kv::process`]. `None` means
/// "nothing to parse, pass the event through as-is": no log, a non-string message, or a message
/// that isn't UTF-8 (the last case reports `invalid_utf8` first, since `Value::Str` must always be
/// valid UTF-8 -- there's no way to represent the failure any other way).
fn message_bytes(event: &Event, diag: &mut Diagnostics) -> Option<Bytes> {
    let log = event.log.as_ref()?;
    let raw = match &log.message {
        Value::Str(b) | Value::Bytes(b) => b.clone(),
        _ => return None,
    };
    if std::str::from_utf8(&raw).is_err() {
        diag.warn_throttled(
            "invalid_utf8",
            "log message is not valid UTF-8, passing event through unparsed",
        );
        return None;
    }
    Some(raw)
}

/// Parses a log record's message as logfmt (`level=info msg="hello world" dur=3ms`), merging the
/// resulting key/values into the event's attributes. Additive and pass-through-on-failure, exactly
/// like `json`. See `docs/adr/logfmt-and-kv-parsing.md`.
#[derive(Default)]
pub struct Logfmt {
    /// Treat a token with no `=` as a boolean-true flag. Off by default -- see
    /// `logit_config::ComponentKind::Logfmt::bare_keys`'s doc comment for why.
    bare_keys: bool,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// Scratch the parsed pairs land in before the all-or-nothing merge, reused across events --
    /// exactly `JsonParser::scratch`, and for the same reason: a line that fails partway must
    /// leave `event.attributes` untouched, not half-populated.
    scratch: Vec<(Symbol, Value)>,
}

impl Logfmt {
    pub fn new(bare_keys: bool) -> Self {
        Self { bare_keys, ..Self::default() }
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

impl Transform for Logfmt {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        let Some(raw) = message_bytes(&event, &mut self.diag) else { return Some(event) };
        // Constructed only from a buffer `message_bytes` already verified is valid UTF-8.
        let text = std::str::from_utf8(&raw).expect("message_bytes verified valid UTF-8");

        self.scratch.clear();
        match parse_logfmt(&raw, text, self.bare_keys, &mut self.scratch, &self.telemetry) {
            Ok(()) => merge_into(&mut self.scratch, &mut event.attributes),
            Err(err) => {
                self.scratch.clear();
                if !(matches!(err, ParseError::NoPairs) && is_blank(text)) {
                    self.diag.warn_throttled(
                        "parse_failure",
                        format_args!(
                            "failed to parse message as logfmt, passing event through: {err}"
                        ),
                    );
                }
            }
        }

        Some(event)
    }
}

/// `logfmt`'s literal, configurable-separator sibling: no quoting, no escapes. See
/// `docs/adr/logfmt-and-kv-parsing.md` for why this is a distinct `ComponentKind`, sharing this
/// module rather than a `logfmt` mode flag.
pub struct Kv {
    pair_sep: String,
    kv_sep: String,
    bare_keys: bool,
    diag: Diagnostics,
    telemetry: Telemetry,
    scratch: Vec<(Symbol, Value)>,
}

impl Kv {
    pub fn new(pair_sep: String, kv_sep: String, bare_keys: bool) -> Self {
        Self {
            pair_sep,
            kv_sep,
            bare_keys,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            scratch: Vec::new(),
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

impl Transform for Kv {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        let Some(raw) = message_bytes(&event, &mut self.diag) else { return Some(event) };
        let text = std::str::from_utf8(&raw).expect("message_bytes verified valid UTF-8");

        self.scratch.clear();
        let result = parse_kv(
            &raw,
            text,
            &self.pair_sep,
            &self.kv_sep,
            self.bare_keys,
            &mut self.scratch,
            &self.telemetry,
        );
        match result {
            Ok(()) => merge_into(&mut self.scratch, &mut event.attributes),
            Err(err) => {
                self.scratch.clear();
                if !(matches!(err, ParseError::NoPairs) && is_blank(text)) {
                    self.diag.warn_throttled(
                        "parse_failure",
                        format_args!("failed to parse message as kv, passing event through: {err}"),
                    );
                }
            }
        }

        Some(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern as intern_for_test;
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, SpanEvent, SpanKind};
    use logit_core::{Registry, SpanRecord, SpanStatus};

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

    fn log_event_bytes(message: &[u8]) -> Event {
        Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::Bytes(Bytes::copy_from_slice(message)),
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

    fn points_into(haystack: &Bytes, needle: &Bytes) -> bool {
        let base = haystack.as_ptr() as usize;
        let start = needle.as_ptr() as usize;
        start >= base && start + needle.len() <= base + haystack.len()
    }

    // -----------------------------------------------------------------------------------------
    // logfmt grammar
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_flat_line_populates_attributes_as_strings() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("level=info status=200");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "level"), Some(&Value::str("info")));
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")), "never coerce to a number");
        assert_ne!(attr(&event, "status"), Some(&Value::U64(200)));
    }

    #[test]
    fn a_quoted_value_keeps_its_spaces() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event(r#"msg="hello world""#);
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("hello world")));
    }

    #[test]
    fn an_escaped_quote_inside_a_quoted_value_decodes() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event(r#"msg="say \"hi\"""#);
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str(r#"say "hi""#)));
    }

    #[test]
    fn a_known_escape_decodes_and_an_unknown_one_is_preserved_verbatim() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event(r#"a="line1\nline2" b="tab\there" c="what\A""#);
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("line1\nline2")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("tab\there")));
        assert_eq!(attr(&event, "c"), Some(&Value::str(r"what\A")), "unknown escape kept verbatim");
    }

    #[test]
    fn an_empty_quoted_value_is_an_empty_string() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event(r#"msg="""#);
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("")));
    }

    #[test]
    fn an_empty_value_is_an_empty_string_not_a_flag() {
        let mut logfmt = Logfmt::new(true);
        let resource = default_resource();
        let event = log_event("k=");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "k"), Some(&Value::str("")));
    }

    #[test]
    fn a_bareword_is_skipped_by_default() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("level=info cached status=200");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "level"), Some(&Value::str("info")));
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
        assert_eq!(attr(&event, "cached"), None);
    }

    #[test]
    fn a_bareword_becomes_true_with_bare_keys_enabled() {
        let mut logfmt = Logfmt::new(true);
        let resource = default_resource();
        let event = log_event("level=info cached status=200");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "cached"), Some(&Value::Bool(true)));
    }

    #[test]
    fn a_bareword_at_end_of_line_follows_the_same_rule() {
        let mut default_off = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("level=info cached");
        let event = default_off.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "cached"), None);

        let mut bare_on = Logfmt::new(true);
        let event = log_event("level=info cached");
        let event = bare_on.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "cached"), Some(&Value::Bool(true)));
    }

    #[test]
    fn an_unterminated_quote_passes_the_event_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event(r#"a=1 msg="oops"#);
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str(r#"a=1 msg="oops"#));
    }

    #[test]
    fn a_keyless_equals_is_skipped_and_the_rest_of_the_line_still_parses() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("=1 a=2");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("2")));
        assert_eq!(event.attributes.len(), 1);
    }

    #[test]
    fn a_line_with_no_equals_at_all_passes_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("just some prose");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str("just some prose"));
    }

    #[test]
    fn an_empty_or_blank_message_passes_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();

        let event = logfmt.process(&resource, log_event("")).expect("log events pass through");
        assert!(event.attributes.is_empty());

        let event = logfmt.process(&resource, log_event("   ")).expect("log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_duplicate_key_takes_the_last_value() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("a=1 a=2");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("2")));
    }

    #[test]
    fn a_parsed_key_overwrites_a_pre_existing_attribute_of_the_same_name() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=1");
        event.attributes.insert("a", Value::str("old"));
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
    }

    #[test]
    fn tabs_and_crlf_separate_pairs_like_spaces() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("a=1\tb=2\r\nc=3");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("3")));
    }

    #[test]
    fn a_value_containing_an_equals_sign_keeps_it() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("a=b=c");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("b=c")));
    }

    #[test]
    fn parsed_values_are_zero_copy_slices_of_the_message() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let message = Bytes::from_static(b"msg=\"hello world\" status=200");
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::Str(message.clone()),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let event = logfmt.process(&resource, event).expect("log events pass through");

        let Some(Value::Str(msg)) = attr(&event, "msg") else { panic!("msg should be a Str") };
        assert!(points_into(&message, msg), "escape-free quoted value should slice the message");
        let Some(Value::Str(status)) = attr(&event, "status") else {
            panic!("status should be a Str")
        };
        assert!(points_into(&message, status), "unquoted value should slice the message");
    }

    // -----------------------------------------------------------------------------------------
    // kv grammar
    // -----------------------------------------------------------------------------------------

    fn amp_kv(bare_keys: bool) -> Kv {
        Kv::new("&".to_string(), "=".to_string(), bare_keys)
    }

    #[test]
    fn kv_splits_on_configured_separators() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let event = log_event("a=1&b=2&c=hello");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("hello")));
    }

    #[test]
    fn kv_trims_whitespace_around_keys_and_values() {
        let mut kv = Kv::new(",".to_string(), "=".to_string(), false);
        let resource = default_resource();
        let event = log_event("a=1, b=2,  c = 3 ");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("3")));
    }

    #[test]
    fn kv_splits_at_the_first_kv_sep_only() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let event = log_event("a=b=c");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("b=c")));
    }

    #[test]
    fn kv_handles_multi_byte_separators() {
        let mut kv = Kv::new(" :: ".to_string(), " -> ".to_string(), false);
        let resource = default_resource();
        let event = log_event("a -> 1 :: b -> 2");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    #[test]
    fn kv_skips_an_empty_segment() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let event = log_event("a=1&&b=2");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(event.attributes.len(), 2);
    }

    #[test]
    fn kv_skips_a_segment_with_an_empty_key() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let event = log_event("a=1&=2&b=3");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("3")));
        assert_eq!(event.attributes.len(), 2);
    }

    #[test]
    fn kv_bareword_follows_the_same_rule_as_logfmt() {
        let mut default_off = amp_kv(false);
        let resource = default_resource();
        let event = log_event("a=1&cached&b=2");
        let event = default_off.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "cached"), None);
        assert_eq!(event.attributes.len(), 2);

        let mut bare_on = amp_kv(true);
        let event = log_event("a=1&cached&b=2");
        let event = bare_on.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "cached"), Some(&Value::Bool(true)));
    }

    #[test]
    fn kv_with_no_separator_anywhere_passes_the_event_through_untouched() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let event = log_event("just some prose");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str("just some prose"));
    }

    #[test]
    fn kv_duplicate_key_takes_the_last_value() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let event = log_event("a=1&a=2");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("2")));
    }

    #[test]
    fn kv_values_are_always_strings_never_coerced() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let event = log_event("a=1&b=true");
        let event = kv.process(&resource, event).expect("log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("true")));
    }

    #[test]
    fn kv_values_are_zero_copy_slices_of_the_message() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let message = Bytes::from_static(b"a=1&b=hello");
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::Str(message.clone()),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let event = kv.process(&resource, event).expect("log events pass through");
        let Some(Value::Str(b)) = attr(&event, "b") else { panic!("b should be a Str") };
        assert!(points_into(&message, b), "kv value should slice the message");
    }

    // -----------------------------------------------------------------------------------------
    // Shared pass-through shape (both `logfmt` and `kv` -- exercised via `Logfmt` since the
    // behavior is identical by construction: both share `message_bytes`/`merge_into`).
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_metric_event_passes_through_with_attributes_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern_for_test("m"), MetricKind::counter(1.0)),
        );
        let event = logfmt.process(&resource, event).expect("metric-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_span_event_passes_through_with_attributes_untouched() {
        let mut logfmt = Logfmt::new(false);
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
        let event = logfmt.process(&resource, event).expect("span-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_log_event_that_also_carries_a_metric_is_parsed_and_keeps_its_metric() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=1");
        event.metrics.push(MetricRecord::new(intern_for_test("m"), MetricKind::counter(1.0)));
        let event = logfmt.process(&resource, event).expect("mixed events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(event.metrics.len(), 1, "the metric should ride through unaffected");
        assert!(matches!(
            event.metrics[0].kind,
            MetricKind::Sum(logit_core::Sum { value, .. }) if value == 1.0
        ));
    }

    #[test]
    fn a_non_string_message_passes_through() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut map = AttrMap::new();
        map.insert("a", Value::str("1"));
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::Map(Box::new(map)),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn an_event_with_no_log_passes_through() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern_for_test("m"), MetricKind::counter(1.0)),
        );
        assert!(event.log.is_none());
        let event = logfmt.process(&resource, event).expect("events with no log pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn an_invalid_utf8_message_passes_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event_bytes(b"a=1 \xff\xfe b=2");
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn the_message_and_body_format_are_left_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let event = log_event("a=1");
        let original_message = message_of(&event).clone();
        let event = logfmt.process(&resource, event).expect("log events pass through");
        assert_eq!(message_of(&event), &original_message);
        assert_eq!(
            event.log.as_ref().expect("event should carry a log").body_format,
            BodyFormat::Raw
        );
    }

    // -----------------------------------------------------------------------------------------
    // Diagnostics
    // -----------------------------------------------------------------------------------------

    #[test]
    fn an_unterminated_quote_reports_a_throttled_parse_failure() {
        let registry = Registry::new();
        let mut logfmt = Logfmt::new(false).with_diagnostics(
            Diagnostics::new("logfmt").with_telemetry(registry.telemetry_for(
                "logfmt",
                "logfmt",
                "transform",
            )),
        );
        let resource = default_resource();
        let event = log_event(r#"a=1 msg="oops"#);
        drop(logfmt.process(&resource, event));

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attributes.get("key").and_then(|v| v.as_str()), Some("parse_failure"));
        match &events[0].metrics[0].kind {
            MetricKind::Sum(sum) => assert_eq!(sum.value, 1.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }
}
