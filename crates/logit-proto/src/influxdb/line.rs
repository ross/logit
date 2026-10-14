//! The line-protocol lexer: [`lines`] splits a write body into logical lines, and [`parse_line`]
//! turns one into a [`Line`], or nothing for a blank line or a comment. [`super`]'s module doc
//! has the grammar as a table; this file is its implementation.
//!
//! **Zero copy.** Every name and string in a [`Line`] borrows the body unless unescaping changed
//! its bytes, so [`super::decode`] can hand tag values out as slices of the request buffer.
//!
//! **The escape-scanning rule.** A delimiter in a measurement, tag key, tag value, or field key is
//! escaped when the byte before it is a backslash, whatever came before that backslash. That's
//! influxd's scanner, and it's why `v\\` followed by a space escapes the space (probe rows 1.7 and
//! 2.5): a key can't end in a backslash. Unescaping then drops a backslash only before `,`, `=`,
//! or a space, so `\\` stays two backslashes (probe row 1.3).

use bytes::Bytes;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

/// The latest instant influxd stores, in nanoseconds; the earliest is its negation. A timestamp
/// outside the range is a parse error (probe rows 10.7 and 10.14).
pub const MAX_TIMESTAMP_NS: i64 = i64::MAX - 1;
/// See [`MAX_TIMESTAMP_NS`].
pub const MIN_TIMESTAMP_NS: i64 = -MAX_TIMESTAMP_NS;

/// The reason a line failed, worded as influxd words it where the probe recorded the wording.
pub type Reason = Cow<'static, str>;

/// One field's value, typed by its wire spelling.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue<'a> {
    /// A bare number: `1`, `1.5`, `.5`, `5.`, `1e3`, `-0`.
    F64(f64),
    /// `Ni`.
    I64(i64),
    /// `Nu`.
    U64(u64),
    /// `t`, `T`, `true`, `True`, `TRUE`, and the five `false` spellings.
    Bool(bool),
    /// A `"`-quoted string, unescaped.
    Str(Cow<'a, str>),
}

impl FieldValue<'_> {
    /// influxd's name for the type, as its type-conflict message spells it.
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldValue::F64(_) => "float",
            FieldValue::I64(_) => "integer",
            FieldValue::U64(_) => "unsigned",
            FieldValue::Bool(_) => "boolean",
            FieldValue::Str(_) => "string",
        }
    }
}

/// One parsed point.
#[derive(Debug, Clone, PartialEq)]
pub struct Line<'a> {
    pub measurement: Cow<'a, str>,
    /// Sorted by key, which is the series key's canonical order. Keys are distinct: a repeated
    /// tag key is a parse error.
    pub tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
    /// Wire order, keys distinct: a repeated field key keeps its first position and its last
    /// value.
    pub fields: Vec<(Cow<'a, str>, FieldValue<'a>)>,
    /// The raw wire value, in the request's `precision`; the decoder scales it.
    pub timestamp: Option<i64>,
}

/// A line that failed to parse, which fails the whole body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineError {
    /// The 1-based index of the logical line in the body, for logs and telemetry. influxd's error
    /// JSON carries no line number, so [`super::response::parse_error_body`] doesn't either.
    pub line: usize,
    /// The offending line's text, leading whitespace removed, as influxd quotes it.
    pub text: Bytes,
    pub reason: Reason,
}

impl fmt::Display for LineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "line {}: unable to parse '{}': {}",
            self.line,
            String::from_utf8_lossy(&self.text),
            self.reason
        )
    }
}

impl std::error::Error for LineError {}

/// Splits `body` into logical lines, yielding each with its 1-based index.
///
/// A newline ends a line unless it's escaped by the backslash before it or sits inside a quoted
/// string field value (probe row 3.4). This is influxd's own `scanLine`: quote tracking starts
/// at the line's first space, and a `"` opens a string only after an unquoted `=` that no later
/// unquoted `,` has answered, so a quote in a measurement or tag value toggles nothing. One
/// consequence, kept for fidelity: a comment line holding `="` swallows the lines after it until
/// a closing quote.
pub fn lines(body: &[u8]) -> Lines<'_> {
    Lines { body, pos: 0, line: 0 }
}

/// The iterator [`lines`] returns.
#[derive(Debug, Clone)]
pub struct Lines<'a> {
    body: &'a [u8],
    pos: usize,
    line: usize,
}

impl<'a> Iterator for Lines<'a> {
    type Item = (usize, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.body.len() {
            return None;
        }
        let start = self.pos;
        let end = line_end(self.body, start);
        // Past the newline; `end == len` leaves `pos` past the end, which ends iteration.
        self.pos = end + 1;
        self.line += 1;
        Some((self.line, &self.body[start..end]))
    }
}

/// The index of the newline ending the line that starts at `start`, or `buf.len()`.
///
/// The `i + 2 < len` bound is influxd's: a backslash in the last two bytes of the body escapes
/// nothing for splitting.
fn line_end(buf: &[u8], start: usize) -> usize {
    let mut i = start;
    let mut quoted = false;
    let mut fields = false;
    let mut equals = 0usize;
    let mut commas = 0usize;
    while i < buf.len() {
        let b = buf[i];
        if b == b'\\' && i + 2 < buf.len() {
            i += 2;
            continue;
        }
        if b == b' ' {
            fields = true;
        }
        if fields {
            if !quoted && b == b'=' {
                equals += 1;
                i += 1;
                continue;
            }
            if !quoted && b == b',' {
                commas += 1;
                i += 1;
                continue;
            }
            if b == b'"' && equals > commas {
                quoted = !quoted;
                i += 1;
                continue;
            }
        }
        if b == b'\n' && !quoted {
            break;
        }
        i += 1;
    }
    i
}

/// Whether `b` is whitespace influxd skips (`skipWhitespace` in `models/points.go`): a space, a
/// tab, or NUL. A tab or NUL never ends a section (probe row 4.3); only a run that a space starts,
/// or the line's leading run, may contain one (row 4.5).
fn is_skippable(b: u8) -> bool {
    b == b' ' || b == b'\t' || b == 0
}

fn skip_ws(s: &[u8], mut i: usize) -> usize {
    while i < s.len() && is_skippable(s[i]) {
        i += 1;
    }
    i
}

/// The index of the first byte at or after `from` that is in `stops` and not escaped by the byte
/// before it, or `s.len()`.
fn scan_until(s: &[u8], from: usize, stops: &[u8]) -> usize {
    let mut i = from;
    while i < s.len() {
        if stops.contains(&s[i]) && !(i > 0 && s[i - 1] == b'\\') {
            return i;
        }
        i += 1;
    }
    s.len()
}

/// Parses one logical line from [`lines`]. `Ok(None)` is a blank or whitespace-only line or a
/// whole-line `#` comment, leading whitespace allowed.
pub fn parse_line(raw: &[u8]) -> Result<Option<Line<'_>>, Reason> {
    let start = skip_ws(raw, 0);
    if start >= raw.len() || raw[start] == b'#' {
        return Ok(None);
    }
    let s = &raw[start..];

    // Measurement: its first byte is taken whatever it is but a comma.
    if s[0] == b',' {
        return Err("missing measurement".into());
    }
    let m_end = scan_until(s, 1, b", ");
    if m_end == s.len() {
        return Err("missing fields".into());
    }
    let measurement = unescape_key(&s[..m_end], KEY_ESCAPES, "measurement")?;

    let mut tags = Vec::new();
    let mut i = m_end;
    if s[m_end] == b',' {
        i = m_end + 1;
        loop {
            if i >= s.len() || matches!(s[i], b',' | b' ' | b'=') {
                return Err("missing tag key".into());
            }
            let k_end = scan_until(s, i, b"=, ");
            if k_end == s.len() || s[k_end] != b'=' {
                return Err("missing tag value".into());
            }
            let j = k_end + 1;
            // influxd tests a tag value's first byte for `,` and space only, so a value may start
            // with an unescaped `=`.
            if j == s.len() || matches!(s[j], b',' | b' ') {
                return Err("missing tag value".into());
            }
            let v_end = scan_until(s, j + 1, b"=, ");
            if v_end < s.len() && s[v_end] == b'=' {
                return Err("invalid tag format".into());
            }
            if v_end == s.len() {
                return Err("missing fields".into());
            }
            let key = unescape_key(&s[i..k_end], KEY_ESCAPES, "tag key")?;
            let value = unescape_key(&s[j..v_end], KEY_ESCAPES, "tag value")?;
            tags.push((key, value));
            i = v_end;
            if s[v_end] == b',' {
                i += 1;
            } else {
                break;
            }
        }
    }
    check_tags(&mut tags)?;

    i = skip_ws(s, i);
    if i == s.len() {
        return Err("missing fields".into());
    }

    let mut fields = Fields::new();
    loop {
        if s[i] == b'=' {
            return Err("missing field key".into());
        }
        if matches!(s[i], b',' | b' ') {
            return Err("invalid field format".into());
        }
        let k_end = scan_until(s, i, b"=, ");
        if k_end == s.len() || s[k_end] != b'=' {
            return Err("invalid field format".into());
        }
        let key = unescape_key(&s[i..k_end], FIELD_KEY_ESCAPES, "field key")?;
        if key == "time" {
            return Err(format!(
                "invalid field name: input field \"time\" on measurement \"{measurement}\" is invalid"
            )
            .into());
        }
        let j = k_end + 1;
        if j == s.len() || matches!(s[j], b',' | b' ') {
            return Err("missing field value".into());
        }
        let (value, v_end) = if s[j] == b'"' {
            let (value, end) = parse_string(s, j + 1)?;
            if end < s.len() && !matches!(s[end], b',' | b' ') {
                return Err("unbalanced quotes".into());
            }
            (value, end)
        } else {
            let mut v_end = j;
            while v_end < s.len() && !matches!(s[v_end], b',' | b' ') {
                v_end += 1;
            }
            (parse_scalar(&s[j..v_end])?, v_end)
        };
        fields.push(key, value, &measurement)?;
        i = v_end;
        if i == s.len() {
            return Ok(Some(Line { measurement, tags, fields: fields.list, timestamp: None }));
        }
        if s[i] == b',' {
            i += 1;
            if i == s.len() {
                return Err("invalid field format".into());
            }
        } else {
            break;
        }
    }

    i = skip_ws(s, i);
    if i == s.len() {
        return Ok(Some(Line { measurement, tags, fields: fields.list, timestamp: None }));
    }
    // The timestamp ends at a space only, and only spaces may follow it (`scanTime` and
    // `parsePoint` in `models/points.go`): a tab or NUL there is not whitespace.
    let mut t_end = i;
    while t_end < s.len() && s[t_end] != b' ' {
        t_end += 1;
    }
    let timestamp = parse_timestamp(&s[i..t_end])?;
    if s[t_end..].iter().any(|&b| b != b' ') {
        return Err("point is invalid".into());
    }
    Ok(Some(Line { measurement, tags, fields: fields.list, timestamp: Some(timestamp) }))
}

/// Sorts `tags` into series-key order and rejects a repeated key or one influxd reserves.
fn check_tags(tags: &mut [(Cow<'_, str>, Cow<'_, str>)]) -> Result<(), Reason> {
    for (key, _) in tags.iter() {
        if matches!(key.as_ref(), "_measurement" | "_field" | "time") {
            return Err(format!("cannot use reserved tag key \"{key}\"").into());
        }
    }
    tags.sort_by(|a, b| a.0.cmp(&b.0));
    if tags.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err("duplicate tags".into());
    }
    Ok(())
}

/// A line's fields past which a repeat is found through a map rather than a scan, so a line of
/// thousands of fields parses in linear time.
const FIELD_SCAN_LIMIT: usize = 16;

/// A line's fields, keys distinct.
struct Fields<'a> {
    list: Vec<(Cow<'a, str>, FieldValue<'a>)>,
    /// Key to position in `list`, built once `list` passes [`FIELD_SCAN_LIMIT`].
    index: Option<HashMap<Cow<'a, str>, usize>>,
}

impl<'a> Fields<'a> {
    fn new() -> Self {
        Self { list: Vec::new(), index: None }
    }

    /// Appends a field, or overwrites a same-line repeat in place: the last value wins at the
    /// first position when the types match (probe row 6.2); differing types are influxd's `422`,
    /// which the decoder answers as a parse error.
    fn push(
        &mut self,
        key: Cow<'a, str>,
        value: FieldValue<'a>,
        measurement: &str,
    ) -> Result<(), Reason> {
        let existing = match &self.index {
            Some(index) => index.get(&key).copied(),
            None => self.list.iter().position(|(k, _)| *k == key),
        };
        if let Some(at) = existing {
            let current = &mut self.list[at].1;
            if current.type_name() != value.type_name() {
                return Err(format!(
                    "field type conflict: input field \"{key}\" on measurement \"{measurement}\" \
                     is type {}, already exists as type {}",
                    value.type_name(),
                    current.type_name()
                )
                .into());
            }
            *current = value;
            return Ok(());
        }
        if let Some(index) = &mut self.index {
            index.insert(key.clone(), self.list.len());
        }
        self.list.push((key, value));
        if self.index.is_none() && self.list.len() > FIELD_SCAN_LIMIT {
            let index = self.list.iter().enumerate().map(|(at, (k, _))| (k.clone(), at)).collect();
            self.index = Some(index);
        }
        Ok(())
    }
}

/// The bytes a backslash escapes in a measurement, tag key, and tag value.
const KEY_ESCAPES: &[u8] = b",= ";
/// A field key's: influxd also unescapes `\"` there, and only there (probe rows 2.9 and 2.11).
const FIELD_KEY_ESCAPES: &[u8] = b",= \"";

/// Unescapes a measurement, tag key, tag value, or field key: a backslash before one of
/// `escapes` is dropped, and every other backslash is kept. Borrows when nothing was dropped.
fn unescape_key<'a>(
    raw: &'a [u8],
    escapes: &[u8],
    what: &'static str,
) -> Result<Cow<'a, str>, Reason> {
    let escaped = |w: &[u8]| w[0] == b'\\' && escapes.contains(&w[1]);
    if !raw.windows(2).any(escaped) {
        return std::str::from_utf8(raw)
            .map(Cow::Borrowed)
            .map_err(|_| format!("invalid UTF-8 in {what}").into());
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if i + 1 < raw.len() && escaped(&raw[i..i + 2]) {
            out.push(raw[i + 1]);
            i += 2;
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map(Cow::Owned).map_err(|_| format!("invalid UTF-8 in {what}").into())
}

/// A quoted string value starting after its opening quote at `from`: returns the unescaped value
/// and the index past the closing quote. `\"` is a quote and `\\` one backslash; any other
/// backslash is a literal character (probe rows 3.1 to 3.8).
fn parse_string(s: &[u8], from: usize) -> Result<(FieldValue<'_>, usize), Reason> {
    let mut i = from;
    let mut escaped = false;
    loop {
        if i >= s.len() {
            return Err("unbalanced quotes".into());
        }
        match s[i] {
            b'\\' if i + 1 < s.len() && matches!(s[i + 1], b'"' | b'\\') => {
                escaped = true;
                i += 2;
            }
            b'"' => break,
            _ => i += 1,
        }
    }
    let raw = &s[from..i];
    let invalid = || Reason::from("invalid UTF-8 in string field value");
    let value = if escaped {
        let mut out = Vec::with_capacity(raw.len());
        let mut k = 0;
        while k < raw.len() {
            if raw[k] == b'\\' && k + 1 < raw.len() && matches!(raw[k + 1], b'"' | b'\\') {
                out.push(raw[k + 1]);
                k += 2;
            } else {
                out.push(raw[k]);
                k += 1;
            }
        }
        Cow::Owned(String::from_utf8(out).map_err(|_| invalid())?)
    } else {
        Cow::Borrowed(std::str::from_utf8(raw).map_err(|_| invalid())?)
    };
    Ok((FieldValue::Str(value), i + 1))
}

/// An unquoted field value: a boolean or a number. The first byte picks the grammar, as influxd's
/// reasons show: a sign, a dot, or a digit is a number; `n`/`N` is a failed number (`NaN`); `t`,
/// `T`, `f`, `F`, and anything else is a boolean (`+1` and `inf` fail as `invalid boolean`).
fn parse_scalar(tok: &[u8]) -> Result<FieldValue<'static>, Reason> {
    match tok[0] {
        b'-' | b'.' | b'0'..=b'9' => parse_number(tok),
        b'n' | b'N' => Err("invalid number".into()),
        _ => match tok {
            b"t" | b"T" | b"true" | b"True" | b"TRUE" => Ok(FieldValue::Bool(true)),
            b"f" | b"F" | b"false" | b"False" | b"FALSE" => Ok(FieldValue::Bool(false)),
            _ => Err("invalid boolean".into()),
        },
    }
}

fn is_digits(b: &[u8]) -> bool {
    !b.is_empty() && b.iter().all(u8::is_ascii_digit)
}

/// `Ni`, `Nu`, or a bare float. Grammar-checked before any `str::parse`, which would otherwise
/// take `inf`, `NaN`, `+1`, and `1_000` forms influxd refuses (probe section 9).
fn parse_number(tok: &[u8]) -> Result<FieldValue<'static>, Reason> {
    // `tok` is ASCII-led and every accepted form is ASCII; a non-ASCII byte fails a grammar check
    // before `text` is read.
    let (last, body) = tok.split_last().expect("a field value is never empty");
    match last {
        b'i' => {
            let digits = body.strip_prefix(b"-").unwrap_or(body);
            if !is_digits(digits) {
                return Err("invalid number".into());
            }
            let text = std::str::from_utf8(body).expect("ASCII digits");
            text.parse::<i64>().map(FieldValue::I64).map_err(|_| {
                format!(
                    "unable to parse integer {text}: strconv.ParseInt: parsing \"{text}\": value \
                     out of range"
                )
                .into()
            })
        }
        b'u' => {
            if !is_digits(body) {
                return Err("invalid number".into());
            }
            let text = std::str::from_utf8(body).expect("ASCII digits");
            text.parse::<u64>().map(FieldValue::U64).map_err(|_| {
                format!(
                    "unable to parse unsigned {text}: strconv.ParseUint: parsing \"{text}\": \
                     value out of range"
                )
                .into()
            })
        }
        _ => parse_float(tok),
    }
}

/// `-?(digits[.digits*] | .digits)([eE][+-]?digits)?`, finite.
fn parse_float(tok: &[u8]) -> Result<FieldValue<'static>, Reason> {
    let mut i = usize::from(tok[0] == b'-');
    let int_start = i;
    while i < tok.len() && tok[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = i - int_start;
    if i < tok.len() && tok[i] == b'.' {
        i += 1;
        let frac_start = i;
        while i < tok.len() && tok[i].is_ascii_digit() {
            i += 1;
        }
        digits += i - frac_start;
    }
    if digits == 0 {
        return Err("invalid number".into());
    }
    if i < tok.len() && matches!(tok[i], b'e' | b'E') {
        i += 1;
        if i < tok.len() && matches!(tok[i], b'+' | b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < tok.len() && tok[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return Err("invalid float".into());
        }
    }
    if i != tok.len() {
        return Err("invalid number".into());
    }
    let text = std::str::from_utf8(tok).expect("checked ASCII");
    match text.parse::<f64>() {
        Ok(v) if v.is_finite() => Ok(FieldValue::F64(v)),
        _ => Err("invalid float".into()),
    }
}

/// `-?digits` as an `i64`, unscaled.
fn parse_timestamp(tok: &[u8]) -> Result<i64, Reason> {
    let digits = tok.strip_prefix(b"-").unwrap_or(tok);
    if !is_digits(digits) {
        return Err("bad timestamp".into());
    }
    let text = std::str::from_utf8(tok).expect("ASCII digits");
    text.parse::<i64>()
        .map_err(|_| format!("strconv.ParseInt: parsing \"{text}\": value out of range").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(raw: &str) -> Line<'_> {
        match parse_line(raw.as_bytes()) {
            Ok(Some(line)) => line,
            other => panic!("{raw:?} must parse, got {other:?}"),
        }
    }

    fn err(raw: &str) -> String {
        match parse_line(raw.as_bytes()) {
            Err(reason) => reason.into_owned(),
            other => panic!("{raw:?} must fail, got {other:?}"),
        }
    }

    fn tag<'a>(line: &'a Line<'_>, key: &str) -> &'a str {
        line.tags.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_ref()).expect("tag present")
    }

    fn field<'a>(line: &'a Line<'a>, key: &str) -> &'a FieldValue<'a> {
        line.fields.iter().find(|(k, _)| k == key).map(|(_, v)| v).expect("field present")
    }

    fn keys(line: &Line<'_>) -> Vec<String> {
        line.fields.iter().map(|(k, _)| k.to_string()).collect()
    }

    fn body_lines(body: &str) -> Vec<(usize, &str)> {
        lines(body.as_bytes()).map(|(n, l)| (n, std::str::from_utf8(l).unwrap())).collect()
    }

    // -- 1. measurement escaping ---------------------------------------------------------------

    // probe: influxd 2.9.1, rows 1.1 to 1.4
    #[test]
    fn measurement_escapes() {
        assert_eq!(ok(r"p01a\,c f=1i").measurement, "p01a,c");
        assert_eq!(ok(r"p01b\ c f=1i").measurement, "p01b c");
        assert_eq!(ok(r"p01c\\x f=1i").measurement, r"p01c\\x", "\\\\ is two backslashes");
        assert_eq!(ok("p01d=e f=1i").measurement, "p01d=e", "= needs no escape");
    }

    // probe: influxd 2.9.1, rows 1.5 and 1.8 (accepted, unreadable at influxd)
    #[test]
    fn an_escaped_equals_in_a_measurement_unescapes() {
        assert_eq!(ok(r"p01e\=f f=1i").measurement, "p01e=f");
        assert_eq!(ok(r"p01h\\\,x f=1i").measurement, r"p01h\\,x");
    }

    // probe: influxd 2.9.1, rows 1.6 and 1.7
    #[test]
    fn a_trailing_backslash_escapes_the_space() {
        assert_eq!(err(r"p01f\ f=1i"), "missing fields");
        assert_eq!(err(r"p01g\\ f=1i"), "missing fields");
    }

    // -- 2. tag keys, tag values, field keys ---------------------------------------------------

    // probe: influxd 2.9.1, rows 2.1 to 2.4
    #[test]
    fn tag_escapes() {
        let line = ok(r"p02a,t\,k=v\,x f=1i");
        assert_eq!(tag(&line, "t,k"), "v,x");
        let line = ok(r"p02b,t\=k=v\=x f=1i");
        assert_eq!(tag(&line, "t=k"), "v=x");
        let line = ok(r"p02c,t\ k=v\ x f=1i");
        assert_eq!(tag(&line, "t k"), "v x");
        let line = ok(r"p02d,t\\k=v\\x f=1i");
        assert_eq!(tag(&line, r"t\\k"), r"v\\x");
    }

    // probe: influxd 2.9.1, rows 2.5, 2.6, 2.10, 2.12
    #[test]
    fn a_tag_value_cannot_end_in_a_backslash_or_hold_a_raw_equals() {
        assert_eq!(err(r"p02e,tk=v\\ f=1i"), "invalid tag format");
        assert_eq!(err(r"p02f,tk=v\ f=1i"), "invalid tag format");
        assert_eq!(err("p02j,tk=v=w f=1i"), "invalid tag format");
        assert_eq!(err(r"p21_a,a=v\\,b=2 f=1i"), "invalid tag format");
    }

    // probe: influxd 2.9.1, row 2.7
    #[test]
    fn field_key_escapes() {
        let line = ok(r"p02g,tk=v f\,k=1i,f\=k=2i,f\ k=3i,f\\k=4i");
        assert_eq!(keys(&line), ["f,k", "f=k", "f k", r"f\\k"]);
    }

    // probe: influxd 2.9.1, rows 2.8 and 2.16
    #[test]
    fn a_field_key_cannot_end_in_a_backslash() {
        assert_eq!(err(r"p02h,tk=v f\\=1i"), "invalid field format");
        assert_eq!(err(r"p21_e,a=v f=1i,g\\=2i"), "invalid field format");
    }

    // probe: influxd 2.9.1, rows 2.9, 2.11, 2.13, 2.14
    #[test]
    fn a_backslash_before_anything_else_is_kept() {
        assert_eq!(tag(&ok(r#"p02i,tk=v\"q f=1i"#), "tk"), r#"v\"q"#);
        // A field key also unescapes `\"` (row 2.11); a tag value doesn't (row 2.9).
        assert_eq!(keys(&ok(r#"p02k,tk=v f\"k=1i"#)), [r#"f"k"#]);
        assert_eq!(tag(&ok(r"p21_d,a=v\\=x f=1i"), "a"), r"v\=x");
        assert_eq!(tag(&ok(r"p21_h,a=b\x f=1i"), "a"), r"b\x");
    }

    // probe: influxd 2.9.1, row 2.15
    #[test]
    fn utf8_round_trips_in_every_position() {
        let line = ok("p21_f,a=é😀 é😀=\"é😀\" 1700000000000000000");
        assert_eq!(tag(&line, "a"), "é😀");
        assert_eq!(field(&line, "é😀"), &FieldValue::Str("é😀".into()));
        assert_eq!(line.timestamp, Some(1_700_000_000_000_000_000));
    }

    #[test]
    fn invalid_utf8_fails() {
        assert_eq!(parse_line(b"m,t=\xff f=1i").unwrap_err(), "invalid UTF-8 in tag value");
        assert_eq!(parse_line(b"m f=\"\xff\"").unwrap_err(), "invalid UTF-8 in string field value");
    }

    #[test]
    fn unescaped_names_borrow_and_escaped_ones_own() {
        let line = ok(r"cpu,host=a\ b f=1i");
        assert!(matches!(line.measurement, Cow::Borrowed(_)));
        assert!(matches!(line.tags[0].1, Cow::Owned(_)));
        assert!(matches!(line.tags[0].0, Cow::Borrowed(_)));
        // A backslash that escapes nothing still borrows.
        assert!(matches!(ok(r"p01c\\x f=1i").measurement, Cow::Borrowed(_)));
    }

    // -- 3. string fields ----------------------------------------------------------------------

    // probe: influxd 2.9.1, rows 3.1 to 3.8
    #[test]
    fn string_field_escapes() {
        let s = |raw: &str| match field(&ok(raw), "f") {
            FieldValue::Str(s) => s.to_string(),
            other => panic!("{other:?}"),
        };
        assert_eq!(s(r#"p03a f="a\"b\\c""#), r#"a"b\c"#);
        assert_eq!(s(r#"p03b f="x\ny""#), r"x\ny");
        assert_eq!(s(r#"p03c f="a, b=c d""#), "a, b=c d");
        assert_eq!(s("p03d f=\"line1\nline2\""), "line1\nline2");
        assert_eq!(s(r#"p03e f="tab\there""#), r"tab\there");
        assert_eq!(s(r#"p03f f="trail\\""#), r"trail\");
        assert_eq!(err(r#"p03g f="bad\""#), "unbalanced quotes");
        assert_eq!(s(r#"p21_i f="a\xb""#), r"a\xb");
    }

    // probe: influxd 2.9.1, row 3.4: a raw newline inside quotes doesn't end the line
    #[test]
    fn the_splitter_keeps_a_quoted_newline_in_its_line() {
        assert_eq!(
            body_lines("p03d f=\"line1\nline2\"\nnext f=1i"),
            [(1, "p03d f=\"line1\nline2\""), (2, "next f=1i")]
        );
        // A quote in a tag value opens nothing.
        assert_eq!(body_lines("m,t=a\"b f=1i\nn f=2i"), [(1, "m,t=a\"b f=1i"), (2, "n f=2i")]);
        // An escaped quote inside a string doesn't close it.
        assert_eq!(body_lines("m f=\"a\\\"\nb\"\nn f=1i").len(), 2);
    }

    // -- 4. whitespace, comments, line endings -------------------------------------------------

    // probe: influxd 2.9.1, row 4.1
    #[test]
    fn comments_and_blank_lines_are_skipped() {
        let body = "# comment\np04a f=1i\n   # indented comment\np04b f=1i\n\np04c f=1i\n";
        let parsed: Vec<_> =
            lines(body.as_bytes()).filter_map(|(_, l)| parse_line(l).unwrap()).collect();
        let names: Vec<_> = parsed.iter().map(|l| l.measurement.to_string()).collect();
        assert_eq!(names, ["p04a", "p04b", "p04c"]);
    }

    // probe: influxd 2.9.1, row 4.2: CR is not stripped
    #[test]
    fn crlf_is_an_error() {
        let body = "p04d f=1i\r\np04e f=2i\r\n";
        let errors: Vec<_> = lines(body.as_bytes()).map(|(_, l)| parse_line(l)).collect();
        assert_eq!(errors.len(), 2);
        assert!(errors.iter().all(|r| r.as_ref().unwrap_err() == "invalid number"));
    }

    // probe: influxd 2.9.1, row 4.3
    #[test]
    fn a_tab_is_not_a_separator() {
        assert_eq!(err("p04f\tf=1i"), "missing fields");
    }

    // probe: influxd 2.9.1, rows 4.4 to 4.7 and 4.11
    #[test]
    fn runs_of_spaces_and_surrounding_whitespace_are_accepted() {
        assert_eq!(
            ok("p04g   f=1i    1700000000000000000").timestamp,
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(ok("p04h,a=b  \t f=1i").measurement, "p04h");
        assert_eq!(ok("   p04i f=1i").measurement, "p04i");
        assert_eq!(ok("p04j f=1i  ").timestamp, None);
        assert_eq!(body_lines("p04n f=1i\n\n\n\n").len(), 4);
    }

    // probe: influxd 2.9.1, rows 4.8 to 4.10
    #[test]
    fn malformed_field_lists_and_trailing_comments_fail() {
        assert_eq!(err("p04k f=1i,  g=2i"), "invalid field format");
        assert_eq!(err("p04l f=1i,g=2i,"), "invalid field format");
        assert_eq!(err("p04m f=1i #trailing"), "bad timestamp");
    }

    // influxd's `scanTime` ends a timestamp at a space only, and `parsePoint` allows only spaces
    // after it (`models/points.go`).
    #[test]
    fn only_spaces_may_end_or_follow_a_timestamp() {
        assert_eq!(err("m f=1i 123\t"), "bad timestamp");
        assert_eq!(err("m f=1i 123\0"), "bad timestamp");
        assert_eq!(err("m f=1i 123 \t"), "point is invalid");
        assert_eq!(ok("m f=1i 123  ").timestamp, Some(123));
    }

    // influxd's `skipWhitespace` skips NUL as it does a space or a tab (`models/points.go`).
    #[test]
    fn nul_is_whitespace_where_whitespace_is_skipped() {
        assert!(matches!(parse_line(b"\0"), Ok(None)), "a NUL-only line is blank");
        assert_eq!(ok("\0m f=1i").measurement, "m");
        assert_eq!(ok("m \0f=1i").fields[0].0, "f");
        assert_eq!(ok("m f=1i \x00123").timestamp, Some(123));
        assert_eq!(ok("m f=1i \0").timestamp, None);
    }

    // influxd's `scanTagsValue` checks a tag value's first byte for `,` and a space only, and
    // looks for an unescaped `=` from the second byte on.
    #[test]
    fn a_tag_value_may_start_with_an_equals_sign() {
        assert_eq!(tag(&ok("m,t==v f=1i"), "t"), "=v");
        assert_eq!(tag(&ok("m,t== f=1i"), "t"), "=");
        assert_eq!(err("m,t=="), "missing fields");
        assert_eq!(err("m,t=v=w f=1i"), "invalid tag format");
        assert_eq!(err("m,t= f=1i"), "missing tag value");
    }

    // -- 5. empty and comment-only bodies ------------------------------------------------------

    // probe: influxd 2.9.1, rows 5.1 to 5.3
    #[test]
    fn empty_and_comment_only_bodies_have_no_points() {
        for body in ["", "   \n\t\n", "# only\n# comments\n"] {
            assert!(
                lines(body.as_bytes()).all(|(_, l)| parse_line(l).unwrap().is_none()),
                "{body:?}"
            );
        }
    }

    // -- 6. duplicates in one line -------------------------------------------------------------

    // probe: influxd 2.9.1, rows 6.1, 6.2, 6.4
    #[test]
    fn duplicate_keys_in_one_line() {
        assert_eq!(err("p06a,k=1,k=2 f=1i"), "duplicate tags");
        let line = ok("p06b,k=1 f=1i,f=2i");
        assert_eq!(line.fields, [("f".into(), FieldValue::I64(2))]);
        let line = ok("p18_o x=1i,x=2i,x=3i");
        assert_eq!(line.fields, [("x".into(), FieldValue::I64(3))]);
        // The repeat keeps its first position.
        assert_eq!(keys(&ok("m a=1i,b=1i,a=2i")), ["a", "b"]);
    }

    // probe: influxd 2.9.1, row 6.3 (influxd answers 422; `logit` answers 400)
    #[test]
    fn a_same_line_type_conflict_fails() {
        assert_eq!(
            err("p06c,k=1 f=1i,f=2.5"),
            "field type conflict: input field \"f\" on measurement \"p06c\" is type float, \
             already exists as type integer"
        );
    }

    #[test]
    fn a_repeat_past_the_scan_limit_is_found() {
        let mut raw: String = (0..40).map(|i| format!("f{i}=1i,")).collect();
        raw.insert_str(0, "m ");
        let wide = format!("{raw}f3=9i");
        let line = ok(&wide);
        assert_eq!(line.fields.len(), 40);
        assert_eq!(line.fields[3], ("f3".into(), FieldValue::I64(9)));
        assert!(err(&format!("{raw}f39=2.5")).starts_with("field type conflict"));
    }

    // -- 7. reserved names ---------------------------------------------------------------------

    // probe: influxd 2.9.1, rows 7.1 to 7.3
    #[test]
    fn reserved_tag_keys_fail() {
        assert_eq!(err("p07a,_measurement=x f=1i"), "cannot use reserved tag key \"_measurement\"");
        assert_eq!(err("p07b,_field=x f=1i"), "cannot use reserved tag key \"_field\"");
        assert_eq!(err("p07c,time=x f=1i"), "cannot use reserved tag key \"time\"");
    }

    // probe: influxd 2.9.1, row 7.4 (influxd answers 422; `logit` answers 400)
    #[test]
    fn a_time_field_key_fails() {
        assert_eq!(
            err("p07d time=1i"),
            "invalid field name: input field \"time\" on measurement \"p07d\" is invalid"
        );
    }

    // probe: influxd 2.9.1, rows 7.5 to 7.11
    #[test]
    fn other_underscore_names_are_ordinary() {
        assert_eq!(ok("_p07e f=1i").measurement, "_p07e");
        assert_eq!(tag(&ok("p07f,_time=x f=1i"), "_time"), "x");
        assert_eq!(tag(&ok("p07g,_value=x f=1i"), "_value"), "x");
        assert_eq!(keys(&ok("p07h _field=1i")), ["_field"]);
        assert_eq!(keys(&ok("p07i _value=1i")), ["_value"]);
        assert_eq!(ok("p07j,_start=x,_stop=y f=1i").tags.len(), 2);
        let line = ok("p07k,name=x,f=y f=1i");
        assert_eq!(tag(&line, "f"), "y");
        assert_eq!(keys(&line), ["f"]);
    }

    // -- 8. booleans ---------------------------------------------------------------------------

    // probe: influxd 2.9.1, rows 8.1 to 8.15
    #[test]
    fn boolean_spellings() {
        for t in ["t", "T", "true", "True", "TRUE"] {
            assert_eq!(field(&ok(&format!("m f={t}")), "f"), &FieldValue::Bool(true), "{t}");
        }
        for f in ["f", "F", "false", "False", "FALSE"] {
            assert_eq!(field(&ok(&format!("m f={f}")), "f"), &FieldValue::Bool(false), "{f}");
        }
        for bad in ["tRUE", "TrUe", "yes"] {
            assert_eq!(err(&format!("m f={bad}")), "invalid boolean", "{bad}");
        }
        assert_eq!(field(&ok("m f=1"), "f"), &FieldValue::F64(1.0));
        assert_eq!(field(&ok("m f=\"t\""), "f"), &FieldValue::Str("t".into()));
    }

    // -- 9. numbers ----------------------------------------------------------------------------

    // probe: influxd 2.9.1, the accepted rows of section 9
    #[test]
    fn accepted_numbers() {
        let v = |tok: &str| -> FieldValue<'static> {
            let raw = format!("m f={tok}");
            match field(&ok(&raw), "f") {
                FieldValue::F64(x) => FieldValue::F64(*x),
                FieldValue::I64(x) => FieldValue::I64(*x),
                FieldValue::U64(x) => FieldValue::U64(*x),
                other => panic!("{tok}: {other:?}"),
            }
        };
        assert_eq!(v("1"), FieldValue::F64(1.0));
        assert_eq!(v("1i"), FieldValue::I64(1));
        assert_eq!(v("1u"), FieldValue::U64(1));
        assert_eq!(v("-1i"), FieldValue::I64(-1));
        assert_eq!(v("1e3"), FieldValue::F64(1000.0));
        assert_eq!(v("1E-3"), FieldValue::F64(0.001));
        assert_eq!(v(".5"), FieldValue::F64(0.5));
        assert_eq!(v("5."), FieldValue::F64(5.0));
        assert_eq!(v("9223372036854775807i"), FieldValue::I64(i64::MAX));
        assert_eq!(v("-9223372036854775808i"), FieldValue::I64(i64::MIN));
        assert_eq!(v("18446744073709551615u"), FieldValue::U64(u64::MAX));
        assert_eq!(v("1.5e+3"), FieldValue::F64(1500.0));
        match v("-0") {
            FieldValue::F64(z) => assert!(z == 0.0 && z.is_sign_negative()),
            other => panic!("{other:?}"),
        }
        match v("-0.0") {
            FieldValue::F64(z) => assert!(z == 0.0 && z.is_sign_negative()),
            other => panic!("{other:?}"),
        }
        assert_eq!(v("1.7976931348623157e308"), FieldValue::F64(f64::MAX));
        assert_eq!(v("007i"), FieldValue::I64(7));
        assert_eq!(v("0u"), FieldValue::U64(0));
    }

    // probe: influxd 2.9.1, the refused rows of section 9, with influxd's reasons
    #[test]
    fn refused_numbers() {
        for (tok, reason) in [
            ("1.0i", "invalid number"),
            ("+1", "invalid boolean"),
            ("inf", "invalid boolean"),
            ("NaN", "invalid number"),
            ("0x10", "invalid number"),
            ("-1u", "invalid number"),
            ("+1i", "invalid boolean"),
            ("1e", "invalid float"),
            ("1_000i", "invalid number"),
            ("Inf", "invalid boolean"),
            ("+Inf", "invalid boolean"),
            ("-Inf", "invalid number"),
            ("nan", "invalid number"),
            ("1e309", "invalid float"),
            ("1ii", "invalid number"),
            ("1.0u", "invalid number"),
            ("1e3i", "invalid number"),
        ] {
            assert_eq!(err(&format!("p09 f={tok}")), reason, "{tok}");
        }
        assert_eq!(
            err("p09_14 f=9223372036854775808i"),
            "unable to parse integer 9223372036854775808: strconv.ParseInt: parsing \
             \"9223372036854775808\": value out of range"
        );
        assert_eq!(
            err("p09_15 f=18446744073709551616u"),
            "unable to parse unsigned 18446744073709551616: strconv.ParseUint: parsing \
             \"18446744073709551616\": value out of range"
        );
        assert_eq!(err("p09_31 f= 1"), "missing field value");
    }

    // -- 10. timestamps ------------------------------------------------------------------------

    // probe: influxd 2.9.1, rows 10.1, 10.6, 10.8, 10.13, 10.16 to 10.20 (range and precision
    // are the decoder's: `decode.rs`'s tests cover rows 10.2 to 10.5, 10.7, 10.9 to 10.15)
    #[test]
    fn timestamp_tokens() {
        assert_eq!(
            ok("p10_ns f=1i 1700000000123456789").timestamp,
            Some(1_700_000_000_123_456_789)
        );
        assert_eq!(ok("m f=1i -1").timestamp, Some(-1));
        assert_eq!(ok("m f=1i 0").timestamp, Some(0));
        assert_eq!(
            err("p10_big f=1i 10413792000000000000"),
            "strconv.ParseInt: parsing \"10413792000000000000\": value out of range"
        );
        assert_eq!(
            err("m f=1i 9223372036854775808"),
            "strconv.ParseInt: parsing \"9223372036854775808\": value out of range"
        );
        assert_eq!(err("p10_dec f=1i 1700000000.5"), "bad timestamp");
        assert_eq!(err("m f=1i +1700000000000000000"), "bad timestamp");
        assert_eq!(err("m f=1i abc"), "bad timestamp");
        assert_eq!(err("m f=1i 1700000000000000000 5"), "point is invalid");
    }

    // -- 18. malformed shapes ------------------------------------------------------------------

    // probe: influxd 2.9.1, section 18's malformed shapes and rows 13.1, 13.2
    #[test]
    fn malformed_shapes() {
        for (raw, reason) in [
            ("p18_h,t= x=1i", "missing tag value"),
            ("p18_i,=v x=1i", "missing tag key"),
            ("p18_j,t x=1i", "missing tag value"),
            (",t=v x=1i", "missing measurement"),
            ("p18_k, x=1i", "missing tag key"),
            ("p18_l x=\"unterminated", "unbalanced quotes"),
            ("p18_m x=1i,y=\"a\"b\"", "unbalanced quotes"),
            ("p18_n =1i", "missing field key"),
            ("p13_b f=", "missing field value"),
            ("p13_b f=1i,", "invalid field format"),
            ("p13_d g", "invalid field format"),
            ("m", "missing fields"),
            ("m,t=v", "missing fields"),
            ("m ", "missing fields"),
        ] {
            assert_eq!(err(raw), reason, "{raw:?}");
        }
    }

    #[test]
    fn tags_come_back_sorted() {
        let line = ok("m,b=2,a=1,c=3 f=1i");
        let order: Vec<_> = line.tags.iter().map(|(k, _)| k.as_ref()).collect();
        assert_eq!(order, ["a", "b", "c"]);
    }

    #[test]
    fn the_splitter_numbers_lines_from_one_and_keeps_blank_ones() {
        assert_eq!(body_lines("a f=1\n\nb f=2"), [(1, "a f=1"), (2, ""), (3, "b f=2")]);
        assert_eq!(body_lines("a f=1\n"), [(1, "a f=1")]);
        assert!(body_lines("").is_empty());
    }
}
