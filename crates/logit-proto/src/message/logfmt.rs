//! The `logfmt` and `kv` transforms' parse cores: a log message as `key=value` pairs.
//!
//! The `logfmt` and `kv` transforms (`crates/logit-transforms/src/logfmt.rs`) wrap them with
//! their all-or-nothing merge, UTF-8 check, and diagnostics; the grammars are in
//! [ADR `logfmt-and-kv-parsing`](../../../../docs/adr/logfmt-and-kv-parsing.md). `logfmt` is
//! whitespace-delimited pairs with `"`-quoted values and backslash escapes; `kv` is a literal
//! splitter on configured separators with no quoting. Every value is a `Value::Str`, or
//! `Value::Bool(true)` for an opted-in bareword, never a number. Each parser counts
//! `logit.transform.pairs.parsed` and `logit.transform.pairs.skipped` per pair on the
//! [`Telemetry`] it's given.
//!
//! `logfmt`'s token boundaries, where the ADR's "whitespace-delimited" leaves them open:
//!
//! - A closing `"` ends a token, whitespace or not: `a="x"b=1` is `a` and `b`, and `a="x"y` is
//!   `a` and the bareword `y`. go-logfmt's decoder (`github.com/go-logfmt/logfmt`, `decode.go`'s
//!   `ScanKeyval`) reads both lines the same way: it returns after the closing quote and starts
//!   the next key at the next non-whitespace byte.
//! - A key is any run of bytes other than whitespace and `=`, so it can hold a `"`; an unquoted
//!   value is any run of non-whitespace bytes, so it can hold `=` and `"`.
//!
//! `kv`'s segment rules:
//!
//! - A segment that's empty or blank (`a=1&&b=2`, or a trailing `pair_sep`), a segment whose key
//!   is empty after trimming (`=1`), and a bareword with `bare_keys` off each add nothing and
//!   count one `pairs.skipped`; none of them gets a diagnostic.
//! - [`ParseError::NoPairs`] means no segment produced a `key<kv_sep>value` pair, so a line whose
//!   every `kv_sep` segment has an empty key (`=1&=2`) fails like a line with no `kv_sep`.
//! - Both separators are non-empty: graph rule 30 rejects an empty one, which would split between
//!   every byte. The byte search here returns no match for an empty needle rather than looping.

use bytes::Bytes;
use logit_core::interner::KeyCache;
use logit_core::{Symbol, Telemetry, Value};
use std::fmt;

const fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

/// `true` if `s` is empty or only whitespace (space, tab, CR, LF): nothing to parse, so a
/// transform gives its [`ParseError::NoPairs`] no `parse_failure` diagnostic.
#[inline]
pub fn is_blank(s: &str) -> bool {
    s.bytes().all(is_ws)
}

/// Why [`parse_logfmt`] or [`parse_kv`] failed; the transforms `Display` it into their
/// `parse_failure` diagnostic.
pub enum ParseError {
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

/// Resolves the five escapes `logfmt` understands; any other escape is kept verbatim, backslash
/// included.
///
/// The only value that allocates; every other value is a [`bytes::Bytes::slice`] of the message.
/// The other allocations in a parse are `out` growing and a key missing the [`KeyCache`], which
/// interns it. `shrink_to_fit` matters: any escape makes `out` shorter than its capacity, and
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
/// escape, and the index after the closing `"`. Escapes are skipped two bytes at a time and
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
///
/// On `Err`, `out` may hold the pairs before the failure and the caller clears it.
#[inline]
pub fn parse_logfmt(
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
/// (self-synchronization), so no boundary check is needed. An empty `needle` finds nothing; graph
/// rule 30 keeps both of `kv`'s separators non-empty.
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
/// `logfmt`. `text` and `raw` are as in [`parse_logfmt`]. Fails only with
/// [`ParseError::NoPairs`], with every pair it found on `out`; the caller clears it.
#[allow(clippy::too_many_arguments)]
#[inline]
pub fn parse_kv(
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
