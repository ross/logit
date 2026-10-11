//! A log message through `message::logfmt::parse_kv`, as the `kv` transform hands one over. Byte
//! 0's low bit is `bare_keys`, and the rest of it picks a `(pair_sep, kv_sep)` pair from
//! [`SEPARATORS`], every one valid under graph rule 30; the rest of the input is the message,
//! made valid UTF-8 by lossy replacement because the transform checks UTF-8 before the parse.
//!
//! Oracles, each over every input:
//! - differential: a reference built from `str::split`, `split_once`, and `trim_matches` over
//!   the ADR's rules (`docs/adr/logfmt-and-kv-parsing.md`, and the segment rules in
//!   `crates/logit-proto/src/message/logfmt.rs`'s module doc) reads the same pairs and barewords
//!   in the same order, duplicates included, and fails with `NoPairs` where the parse does, with
//!   the same barewords left in `out`;
//! - zero copy: every non-empty value shares the message's buffer.
//!
//! An input with a key over [`MAX_KEY_BYTES`] is skipped before the parse.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_core::interner::{resolve, KeyCache};
use logit_core::subslice::within;
use logit_core::{Telemetry, Value};
use logit_proto::message::logfmt::{parse_kv, ParseError};
use std::cell::RefCell;

/// Separator pairs operators configure: a query string, space- and comma-separated pairs,
/// `a: 1, b: 2`, cookie-style `;`, LTSV's tab and `:`, a value after a space, and two
/// multi-byte pairs, one of them non-ASCII.
const SEPARATORS: [(&str, &str); 9] = [
    ("&", "="),
    (" ", "="),
    (",", "="),
    (", ", ": "),
    (";", "="),
    ("\t", ":"),
    (",", " "),
    (" :: ", " -> "),
    ("¦", "→"),
];

/// The longest key an input may hold. The parse interns every key it meets for the process's
/// life, into an arena whose buckets double, so long distinct keys take one fork child's arena
/// past the malloc limit within a job. Nothing in the scan depends on a key's length.
const MAX_KEY_BYTES: usize = 32;

thread_local! {
    static KEYS: RefCell<KeyCache> = RefCell::new(KeyCache::new());
}

#[derive(Debug, PartialEq)]
enum Want<'a> {
    Pair(&'a str, &'a str),
    Bare(&'a str),
}

impl Want<'_> {
    fn key(&self) -> &str {
        match self {
            Want::Pair(key, _) | Want::Bare(key) => key,
        }
    }
}

fn is_ws(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\r' | '\n')
}

/// The reference reading, and whether it holds a pair: `false` is `NoPairs`.
fn reference<'a>(
    text: &'a str,
    pair_sep: &str,
    kv_sep: &str,
    bare_keys: bool,
) -> (Vec<Want<'a>>, bool) {
    let mut out = Vec::new();
    let mut saw_pair = false;
    for segment in text.split(pair_sep) {
        match segment.split_once(kv_sep) {
            Some((key, value)) => {
                let key = key.trim_matches(is_ws);
                if !key.is_empty() {
                    out.push(Want::Pair(key, value.trim_matches(is_ws)));
                    saw_pair = true;
                }
            }
            None => {
                let key = segment.trim_matches(is_ws);
                if !key.is_empty() && bare_keys {
                    out.push(Want::Bare(key));
                }
            }
        }
    }
    (out, saw_pair)
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else { return };
    let bare_keys = selector & 1 == 1;
    let (pair_sep, kv_sep) = SEPARATORS[(selector >> 1) as usize % SEPARATORS.len()];
    let raw = Bytes::from(String::from_utf8_lossy(body).into_owned());
    let text = std::str::from_utf8(&raw).expect("lossy conversion is valid UTF-8");

    let (want, saw_pair) = reference(text, pair_sep, kv_sep, bare_keys);
    if want.iter().any(|w| w.key().len() > MAX_KEY_BYTES) {
        return;
    }

    let mut out = Vec::new();
    let result = KEYS.with(|keys| {
        let keys = &mut *keys.borrow_mut();
        parse_kv(&raw, text, pair_sep, kv_sep, bare_keys, &mut out, keys, &Telemetry::default())
    });
    match (result, saw_pair) {
        (Ok(()), true) | (Err(ParseError::NoPairs), false) => {}
        (Err(ParseError::UnterminatedQuote(i)), _) => panic!("kv has no quotes, yet {i}"),
        (Ok(()), false) => panic!("differential: Ok where the reference finds no pair"),
        (Err(ParseError::NoPairs), true) => {
            panic!("differential: NoPairs where the reference reads {want:?}")
        }
    }

    let got: Vec<Want> = out
        .iter()
        .map(|(key, value)| match value {
            Value::Str(s) => {
                assert!(s.is_empty() || within(&raw, s), "zero copy: a copied value");
                Want::Pair(resolve(*key), std::str::from_utf8(s).expect("a valid UTF-8 value"))
            }
            Value::Bool(true) => Want::Bare(resolve(*key)),
            other => panic!("differential: {other:?}"),
        })
        .collect();
    assert_eq!(got, want, "differential: {text:?} on {pair_sep:?} and {kv_sep:?}");
});
