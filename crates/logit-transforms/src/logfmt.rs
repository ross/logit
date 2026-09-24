//! `logfmt` and `kv`: parse a log message as `key=value` pairs and merge them into the event's
//! attributes, as `json` does for JSON. See `docs/adr/logfmt-and-kv-parsing.md`.
//!
//! `logfmt` is the de-facto convention (`level=info msg="hello world" dur=3ms`):
//! whitespace-delimited pairs, `"`-quoted values with backslash escapes, no required config. `kv`
//! is a literal splitter with required `pair_sep`/`kv_sep` and no quoting (`a=1&b=2`,
//! `a: 1, b: 2`). They're two `ComponentKind`s sharing this module, not one kind with a mode flag.
//!
//! Both are stateless and always produce `Value::Str` (or `Value::Bool(true)` for an opted-in
//! bareword), never a number; `scale`/`kv_metrics` downstream coerce through `crate::numeric`.

use bytes::Bytes;
use logit_core::interner::KeyCache;
use logit_core::{AttrMap, Diagnostics, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::fmt;
use std::sync::Arc;

const fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

/// `true` if `s` is empty or only [`is_ws`] bytes: nothing to parse, so `NoPairs` gets no
/// `parse_failure` diagnostic.
fn is_blank(s: &str) -> bool {
    s.bytes().all(is_ws)
}

enum ParseError {
    /// A `"` never closed (or its closing `"` was an escape target), at this byte offset.
    UnterminatedQuote(usize),
    /// Nothing in the line produced a `key<sep>value` pair; barewords don't count.
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

/// Moves every pair out of `scratch` into `attrs` by `Symbol`.
///
/// Merging in push order makes a duplicate key's later occurrence win, as in `json`.
fn merge_into(scratch: &mut Vec<(Symbol, Value)>, attrs: &mut AttrMap) {
    for (key, value) in scratch.drain(..) {
        attrs.insert_sym(key, value);
    }
}

/// Resolves the five escapes `logfmt` understands; any other escape is kept verbatim, backslash
/// included.
///
/// The only allocating path in this module; every other value is a [`bytes::Bytes::slice`] of the
/// message. `shrink_to_fit` matters: any escape makes `out` shorter than its capacity, and
/// `Bytes::from(Vec<u8>)` then allocates a separate `Shared` block eagerly. Shrinking turns that
/// into a `realloc`, which `crates/logit-bench/tests/allocations.rs` doesn't count as an `alloc`.
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

/// Scans a `"`-quoted value starting at `s[i] == b'"'`.
///
/// Returns the content's `(start, end)` range (quotes excluded), whether it has a backslash
/// escape, and the index just past the closing `"`. Escapes are skipped two bytes at a time and
/// left for [`unescape`].
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

/// Parses `text`, the whole message as validated UTF-8, as logfmt into `out`.
///
/// `raw` shares `text`'s bytes, so every value without an escape is a `raw.slice`, never a copy.
/// Keys are sliced straight off `text`: every delimiter (whitespace, `=`, `"`) is one ASCII byte,
/// so a slice always lands on a char boundary. They resolve through [`KeyCache`], one `memcmp`
/// for a repeated key. The grammar is in `docs/adr/logfmt-and-kv-parsing.md`.
fn parse_logfmt(
    raw: &Bytes,
    text: &str,
    bare_keys: bool,
    out: &mut Vec<(Symbol, Value)>,
    keys: &mut KeyCache,
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
            // No key (`=1 a=2`): resynchronize at the next whitespace.
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
            // A bareword doesn't set `saw_pair`: a line of only barewords is still `NoPairs`.
            Value::Bool(true)
        } else {
            telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
            continue;
        };

        out.push((keys.get_or_intern(&text[key_start..key_end]), value));
        telemetry.count("logit.transform.pairs.parsed", 1.0, &[]);
    }

    if !saw_pair {
        return Err(ParseError::NoPairs);
    }
    Ok(())
}

/// Finds `needle`'s first occurrence in `haystack` at or after byte offset `from`.
///
/// A plain byte search: a valid UTF-8 needle found in valid UTF-8 always lands on a char boundary
/// (self-synchronization), so no boundary check is needed.
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

/// Parses one `kv` segment, `text[seg_start..seg_end]`, into at most one pair on `out`.
///
/// A segment with an empty key, or with no `kv_sep` and either blank or `bare_keys` off, is
/// skipped and counted as `pairs.skipped`.
#[allow(clippy::too_many_arguments)]
fn parse_kv_segment(
    raw: &Bytes,
    text: &str,
    seg_start: usize,
    seg_end: usize,
    kv_sep_bytes: &[u8],
    bare_keys: bool,
    out: &mut Vec<(Symbol, Value)>,
    keys: &mut KeyCache,
    saw_pair: &mut bool,
    telemetry: &Telemetry,
) {
    let bytes = text.as_bytes();
    let segment = &bytes[seg_start..seg_end];

    match find_bytes(segment, kv_sep_bytes, 0) {
        Some(idx) => {
            let key_range = trim_ws_range(segment, 0, idx);
            if key_range.is_empty() {
                telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
                return;
            }
            let value_range = trim_ws_range(segment, idx + kv_sep_bytes.len(), segment.len());
            let key = &text[(seg_start + key_range.start)..(seg_start + key_range.end)];
            let value_abs = (seg_start + value_range.start)..(seg_start + value_range.end);
            *saw_pair = true;
            out.push((keys.get_or_intern(key), Value::Str(raw.slice(value_abs))));
            telemetry.count("logit.transform.pairs.parsed", 1.0, &[]);
        }
        None => {
            let key_range = trim_ws_range(segment, 0, segment.len());
            if key_range.is_empty() {
                // An empty segment (`a=1&&b=2`, or all whitespace).
                telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
                return;
            }
            if !bare_keys {
                telemetry.count("logit.transform.pairs.skipped", 1.0, &[]);
                return;
            }
            let key = &text[(seg_start + key_range.start)..(seg_start + key_range.end)];
            out.push((keys.get_or_intern(key), Value::Bool(true)));
            telemetry.count("logit.transform.pairs.parsed", 1.0, &[]);
        }
    }
}

/// Parses `text` as `kv`: `pair_sep`-separated segments, each split on its first `kv_sep`.
///
/// No quoting or escapes, so a value containing `pair_sep` isn't representable; that shape needs
/// `logfmt`.
#[allow(clippy::too_many_arguments)]
fn parse_kv(
    raw: &Bytes,
    text: &str,
    pair_sep: &str,
    kv_sep: &str,
    bare_keys: bool,
    out: &mut Vec<(Symbol, Value)>,
    keys: &mut KeyCache,
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
            keys,
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

/// The log message as a zero-copy `Bytes`, verified valid UTF-8.
///
/// `None` passes the event through: no log, a message that isn't `Str`/`Bytes`, or invalid UTF-8
/// (reported as `invalid_utf8`, since every parsed value is a `Value::Str`).
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

/// Parses a log message as logfmt and merges the pairs into the event's attributes.
///
/// Additive, and a failure forwards the event unchanged, as with `json`.
#[derive(Default)]
pub struct Logfmt {
    /// Treat a token with no `=` as `true`; see `logit_config`'s `bare_keys` doc for the default.
    bare_keys: bool,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// Parsed pairs before the all-or-nothing merge, so a line failing partway leaves
    /// `event.attributes` untouched. Reused across events.
    scratch: Vec<(Symbol, Value)>,
    /// `&str -> Symbol` memo: a stream's small key set repeats every line, so after the first
    /// line each key is a `memcmp` rather than an interner hash and shard lock.
    keys: KeyCache,
}

impl Logfmt {
    pub fn new(bare_keys: bool) -> Self {
        Self { bare_keys, keys: KeyCache::new(), ..Self::default() }
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
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        let Some(raw) = message_bytes(event, &mut self.diag) else { return true };
        let text = std::str::from_utf8(&raw).expect("message_bytes verified valid UTF-8");

        self.scratch.clear();
        let parsed = parse_logfmt(
            &raw,
            text,
            self.bare_keys,
            &mut self.scratch,
            &mut self.keys,
            &self.telemetry,
        );
        match parsed {
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

        true
    }
}

/// Parses a log message with configured separators and no quoting or escapes, merging the pairs
/// into the event's attributes.
pub struct Kv {
    pair_sep: String,
    kv_sep: String,
    bare_keys: bool,
    diag: Diagnostics,
    telemetry: Telemetry,
    scratch: Vec<(Symbol, Value)>,
    /// See `Logfmt::keys`.
    keys: KeyCache,
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
            keys: KeyCache::new(),
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
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        let Some(raw) = message_bytes(event, &mut self.diag) else { return true };
        let text = std::str::from_utf8(&raw).expect("message_bytes verified valid UTF-8");

        self.scratch.clear();
        let result = parse_kv(
            &raw,
            text,
            &self.pair_sep,
            &self.kv_sep,
            self.bare_keys,
            &mut self.scratch,
            &mut self.keys,
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

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::{intern as intern_for_test, resolve};
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
        let mut event = log_event("level=info status=200");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "level"), Some(&Value::str("info")));
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")), "never coerce to a number");
        assert_ne!(attr(&event, "status"), Some(&Value::U64(200)));
    }

    /// Reordered repeat keys hit the cache and only a new key grows it. `nextest` runs each test
    /// in its own process, so `interner::len()` reflects only this test.
    #[test]
    fn repeat_keys_in_any_order_are_cache_hits_and_never_touch_the_interner() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut first = log_event("lf_cache_a=1 lf_cache_b=2 lf_cache_c=3");
        assert!(logfmt.process(&resource, &mut first), "log events pass through");
        assert_eq!(logfmt.keys.len(), 3);

        let before = logit_core::interner::len();
        let mut second = log_event("lf_cache_c=30 lf_cache_a=10");
        assert!(logfmt.process(&resource, &mut second), "log events pass through");
        assert_eq!(attr(&second, "lf_cache_a"), Some(&Value::str("10")));
        assert_eq!(attr(&second, "lf_cache_c"), Some(&Value::str("30")));
        assert_eq!(attr(&second, "lf_cache_b"), None);
        assert_eq!(logit_core::interner::len(), before, "every key was a cache hit");
        for key in ["lf_cache_a", "lf_cache_c"] {
            let sym =
                |e: &Event| e.attributes.iter().find(|(k, _)| resolve(*k) == key).map(|(k, _)| k);
            assert_eq!(sym(&first), sym(&second), "{key}");
        }

        let mut third = log_event("lf_cache_d=4 lf_cache_a=100");
        assert!(logfmt.process(&resource, &mut third), "log events pass through");
        assert_eq!(attr(&third, "lf_cache_d"), Some(&Value::str("4")));
        assert_eq!(logfmt.keys.len(), 4, "only the genuinely new key was added");
    }

    /// `kv`'s pair and bareword push sites both hit the key cache.
    #[test]
    fn kv_repeat_keys_are_cache_hits() {
        let mut kv = Kv::new("&".into(), "=".into(), true);
        let resource = default_resource();
        let mut warm = log_event("kv_cache_a=1&kv_cache_flag&kv_cache_b=2");
        let _ = kv.process(&resource, &mut warm);
        assert_eq!(kv.keys.len(), 3);

        let before = logit_core::interner::len();
        let mut event = log_event("kv_cache_b=20&kv_cache_a=10&kv_cache_flag");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "kv_cache_a"), Some(&Value::str("10")));
        assert_eq!(attr(&event, "kv_cache_b"), Some(&Value::str("20")));
        assert_eq!(attr(&event, "kv_cache_flag"), Some(&Value::Bool(true)));
        assert_eq!(logit_core::interner::len(), before, "every key was a cache hit");
        assert_eq!(kv.keys.len(), 3);
    }

    #[test]
    fn a_quoted_value_keeps_its_spaces() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"msg="hello world""#);
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("hello world")));
    }

    #[test]
    fn an_escaped_quote_inside_a_quoted_value_decodes() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"msg="say \"hi\"""#);
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str(r#"say "hi""#)));
    }

    #[test]
    fn a_known_escape_decodes_and_an_unknown_one_is_preserved_verbatim() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"a="line1\nline2" b="tab\there" c="what\A""#);
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("line1\nline2")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("tab\there")));
        assert_eq!(attr(&event, "c"), Some(&Value::str(r"what\A")), "unknown escape kept verbatim");
    }

    #[test]
    fn an_empty_quoted_value_is_an_empty_string() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"msg="""#);
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "msg"), Some(&Value::str("")));
    }

    #[test]
    fn an_empty_value_is_an_empty_string_not_a_flag() {
        let mut logfmt = Logfmt::new(true);
        let resource = default_resource();
        let mut event = log_event("k=");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "k"), Some(&Value::str("")));
    }

    #[test]
    fn a_bareword_is_skipped_by_default() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("level=info cached status=200");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "level"), Some(&Value::str("info")));
        assert_eq!(attr(&event, "status"), Some(&Value::str("200")));
        assert_eq!(attr(&event, "cached"), None);
    }

    #[test]
    fn a_bareword_becomes_true_with_bare_keys_enabled() {
        let mut logfmt = Logfmt::new(true);
        let resource = default_resource();
        let mut event = log_event("level=info cached status=200");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "cached"), Some(&Value::Bool(true)));
    }

    #[test]
    fn a_bareword_at_end_of_line_follows_the_same_rule() {
        let mut default_off = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("level=info cached");
        assert!(default_off.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "cached"), None);

        let mut bare_on = Logfmt::new(true);
        let mut event = log_event("level=info cached");
        assert!(bare_on.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "cached"), Some(&Value::Bool(true)));
    }

    #[test]
    fn an_unterminated_quote_passes_the_event_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event(r#"a=1 msg="oops"#);
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str(r#"a=1 msg="oops"#));
    }

    #[test]
    fn a_keyless_equals_is_skipped_and_the_rest_of_the_line_still_parses() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("=1 a=2");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("2")));
        assert_eq!(event.attributes.len(), 1);
    }

    #[test]
    fn a_line_with_no_equals_at_all_passes_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("just some prose");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str("just some prose"));
    }

    #[test]
    fn an_empty_or_blank_message_passes_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();

        let mut event = log_event("");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());

        let mut event = log_event("   ");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_duplicate_key_takes_the_last_value() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=1 a=2");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("2")));
    }

    #[test]
    fn a_parsed_key_overwrites_a_pre_existing_attribute_of_the_same_name() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=1");
        event.attributes.insert("a", Value::str("old"));
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
    }

    #[test]
    fn tabs_and_crlf_separate_pairs_like_spaces() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=1\tb=2\r\nc=3");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("3")));
    }

    #[test]
    fn a_value_containing_an_equals_sign_keeps_it() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=b=c");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("b=c")));
    }

    #[test]
    fn parsed_values_are_zero_copy_slices_of_the_message() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let message = Bytes::from_static(b"msg=\"hello world\" status=200");
        let mut event = Event::log(
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
        assert!(logfmt.process(&resource, &mut event), "log events pass through");

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
        let mut event = log_event("a=1&b=2&c=hello");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("hello")));
    }

    #[test]
    fn kv_trims_whitespace_around_keys_and_values() {
        let mut kv = Kv::new(",".to_string(), "=".to_string(), false);
        let resource = default_resource();
        let mut event = log_event("a=1, b=2,  c = 3 ");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(attr(&event, "c"), Some(&Value::str("3")));
    }

    #[test]
    fn kv_splits_at_the_first_kv_sep_only() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let mut event = log_event("a=b=c");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("b=c")));
    }

    #[test]
    fn kv_handles_multi_byte_separators() {
        let mut kv = Kv::new(" :: ".to_string(), " -> ".to_string(), false);
        let resource = default_resource();
        let mut event = log_event("a -> 1 :: b -> 2");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
    }

    #[test]
    fn kv_skips_an_empty_segment() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let mut event = log_event("a=1&&b=2");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("2")));
        assert_eq!(event.attributes.len(), 2);
    }

    #[test]
    fn kv_skips_a_segment_with_an_empty_key() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let mut event = log_event("a=1&=2&b=3");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("3")));
        assert_eq!(event.attributes.len(), 2);
    }

    #[test]
    fn kv_bareword_follows_the_same_rule_as_logfmt() {
        let mut default_off = amp_kv(false);
        let resource = default_resource();
        let mut event = log_event("a=1&cached&b=2");
        assert!(default_off.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "cached"), None);
        assert_eq!(event.attributes.len(), 2);

        let mut bare_on = amp_kv(true);
        let mut event = log_event("a=1&cached&b=2");
        assert!(bare_on.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "cached"), Some(&Value::Bool(true)));
    }

    #[test]
    fn kv_with_no_separator_anywhere_passes_the_event_through_untouched() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let mut event = log_event("just some prose");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
        assert_eq!(message_of(&event), &Value::str("just some prose"));
    }

    #[test]
    fn kv_duplicate_key_takes_the_last_value() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let mut event = log_event("a=1&a=2");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("2")));
    }

    #[test]
    fn kv_values_are_always_strings_never_coerced() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let mut event = log_event("a=1&b=true");
        assert!(kv.process(&resource, &mut event), "log events pass through");
        assert_eq!(attr(&event, "a"), Some(&Value::str("1")));
        assert_eq!(attr(&event, "b"), Some(&Value::str("true")));
    }

    #[test]
    fn kv_values_are_zero_copy_slices_of_the_message() {
        let mut kv = amp_kv(false);
        let resource = default_resource();
        let message = Bytes::from_static(b"a=1&b=hello");
        let mut event = Event::log(
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
        assert!(kv.process(&resource, &mut event), "log events pass through");
        let Some(Value::Str(b)) = attr(&event, "b") else { panic!("b should be a Str") };
        assert!(points_into(&message, b), "kv value should slice the message");
    }

    // -----------------------------------------------------------------------------------------
    // Shared pass-through shape, exercised via `Logfmt`; `kv` shares `message_bytes`/`merge_into`.
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_metric_event_passes_through_with_attributes_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern_for_test("m"), MetricKind::counter(1.0)),
        );
        assert!(logfmt.process(&resource, &mut event), "metric-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_span_event_passes_through_with_attributes_untouched() {
        let mut logfmt = Logfmt::new(false);
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
        assert!(logfmt.process(&resource, &mut event), "span-only events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn a_log_event_that_also_carries_a_metric_is_parsed_and_keeps_its_metric() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=1");
        event.metrics.push(MetricRecord::new(intern_for_test("m"), MetricKind::counter(1.0)));
        assert!(logfmt.process(&resource, &mut event), "mixed events pass through");
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
        let mut event = Event::log(
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
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn an_event_with_no_log_passes_through() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern_for_test("m"), MetricKind::counter(1.0)),
        );
        assert!(event.log.is_none());
        assert!(logfmt.process(&resource, &mut event), "events with no log pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn an_invalid_utf8_message_passes_through_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event_bytes(b"a=1 \xff\xfe b=2");
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn the_message_and_body_format_are_left_untouched() {
        let mut logfmt = Logfmt::new(false);
        let resource = default_resource();
        let mut event = log_event("a=1");
        let original_message = message_of(&event).clone();
        assert!(logfmt.process(&resource, &mut event), "log events pass through");
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
        let mut event = log_event(r#"a=1 msg="oops"#);
        let _ = logfmt.process(&resource, &mut event);

        let events = registry.drain(0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attributes.get("key").and_then(|v| v.as_str()), Some("parse_failure"));
        match &events[0].metrics[0].kind {
            MetricKind::Sum(sum) => assert_eq!(sum.value, 1.0),
            other => panic!("expected Sum, got {other:?}"),
        }
    }
}
