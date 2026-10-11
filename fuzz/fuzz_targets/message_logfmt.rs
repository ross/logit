//! A log message through `message::logfmt::parse_logfmt`, as the `logfmt` transform hands one
//! over. Byte 0's low bit is `bare_keys`; the rest is the message, made valid UTF-8 by lossy
//! replacement because the transform checks UTF-8 before the parse.
//!
//! Oracles, each over every input:
//! - keys: every key is non-empty and holds no whitespace and no `=`;
//! - values: a `Value::Str` that shares the message's buffer (`subslice::within`) is a whole
//!   token's value, between `=` and whitespace or the end, or between two `"`; one that doesn't
//!   is empty or an escape's copy, so the message holds a `\`. `Value::Bool(true)` appears only
//!   under `bare_keys`;
//! - round trip: writing the pairs back as `key="escaped"`, a bareword as its bare key, and
//!   parsing that reads the same pairs;
//! - failure: `UnterminatedQuote(i)` names a `"` in the message, and a `NoPairs` parse found
//!   nothing but barewords.
//!
//! The scan's forward progress is this target terminating: libFuzzer's `-timeout` reports a hang.
//!
//! An input that could hold a key over [`MAX_KEY_BYTES`] is skipped before the parse: one with a
//! longer run of bytes that are neither whitespace nor `=`, since every key is such a run's
//! substring. That also skips a long unquoted value, which takes the same scan as a short one.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_core::interner::{resolve, KeyCache};
use logit_core::subslice::within;
use logit_core::{Symbol, Telemetry, Value};
use logit_proto::message::logfmt::{parse_logfmt, ParseError};
use std::cell::RefCell;

/// The longest key an input may hold. The parse interns every key it meets for the process's
/// life, into an arena whose buckets double, so long distinct keys take one fork child's arena
/// past the malloc limit within a job. Nothing in the scan depends on a key's length.
const MAX_KEY_BYTES: usize = 32;

thread_local! {
    static KEYS: RefCell<KeyCache> = RefCell::new(KeyCache::new());
}

const fn is_ws(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\r' | b'\n')
}

/// Whether some run of bytes other than whitespace and `=` is longer than [`MAX_KEY_BYTES`].
fn may_hold_a_long_key(raw: &[u8]) -> bool {
    raw.split(|&b| is_ws(b) || b == b'=').any(|run| run.len() > MAX_KEY_BYTES)
}

fn parse(raw: &Bytes, bare_keys: bool) -> (Result<(), ParseError>, Vec<(Symbol, Value)>) {
    let text = std::str::from_utf8(raw).expect("the target parses valid UTF-8 only");
    let mut out = Vec::new();
    let result = KEYS.with(|keys| {
        parse_logfmt(raw, text, bare_keys, &mut out, &mut keys.borrow_mut(), &Telemetry::default())
    });
    (result, out)
}

/// The pairs as a line `parse_logfmt` reads back to the same pairs.
fn render(pairs: &[(Symbol, Value)]) -> Vec<u8> {
    let mut line = Vec::new();
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i > 0 {
            line.push(b' ');
        }
        line.extend_from_slice(resolve(*key).as_bytes());
        match value {
            Value::Bool(true) => {}
            Value::Str(s) => {
                line.extend_from_slice(b"=\"");
                for &b in s.iter() {
                    match b {
                        b'\\' => line.extend_from_slice(b"\\\\"),
                        b'"' => line.extend_from_slice(b"\\\""),
                        b'\n' => line.extend_from_slice(b"\\n"),
                        b'\r' => line.extend_from_slice(b"\\r"),
                        b'\t' => line.extend_from_slice(b"\\t"),
                        b => line.push(b),
                    }
                }
                line.push(b'"');
            }
            other => panic!("values: {other:?} is neither a string nor a bareword"),
        }
    }
    line
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else { return };
    let bare_keys = selector & 1 == 1;
    let raw = Bytes::from(String::from_utf8_lossy(body).into_owned());
    if may_hold_a_long_key(&raw) {
        return;
    }
    let (result, out) = parse(&raw, bare_keys);

    match result {
        Ok(()) => {}
        Err(ParseError::UnterminatedQuote(i)) => {
            assert_eq!(raw.get(i), Some(&b'"'), "failure: UnterminatedQuote({i}) isn't a quote");
            return;
        }
        Err(ParseError::NoPairs) => {
            assert!(
                out.iter().all(|(_, v)| *v == Value::Bool(true)),
                "failure: NoPairs after parsing a pair"
            );
            return;
        }
    }

    for (key, value) in &out {
        let key = resolve(*key);
        assert!(!key.is_empty() && !key.bytes().any(|b| is_ws(b) || b == b'='), "keys: {key:?}");
        match value {
            Value::Bool(true) => assert!(bare_keys, "values: a bareword with bare_keys off"),
            // `Bytes::slice` hands out its static empty `Bytes` for an empty range.
            Value::Str(s) if s.is_empty() => {}
            Value::Str(s) if within(&raw, s) => {
                let start = s.as_ptr() as usize - raw.as_ptr() as usize;
                let end = start + s.len();
                let before = start.checked_sub(1).map(|i| raw[i]);
                let after = raw.get(end).copied();
                let quoted = before == Some(b'"') && after == Some(b'"');
                let bare = before == Some(b'=') && after.is_none_or(is_ws);
                assert!(quoted || bare, "values: {start}..{end} isn't one token's value");
            }
            Value::Str(_) => assert!(raw.contains(&b'\\'), "values: a copy with no escape"),
            other => panic!("values: {other:?}"),
        }
    }

    let line = Bytes::from(render(&out));
    let (again, reparsed) = parse(&line, bare_keys);
    assert!(again.is_ok(), "round trip: {:?} fails", String::from_utf8_lossy(&line));
    assert_eq!(reparsed, out, "round trip: {:?}", String::from_utf8_lossy(&line));
});
