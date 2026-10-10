//! A log message through `message::json::parse_object` or `parse_object_prefix`, as the `json`
//! transform hands one over. Byte 0's low bit picks the entry point (`0` `parse_object`, `1`
//! `parse_object_prefix`, the `skip_to_brace` path); the rest is the message.
//!
//! Oracles, each over every input:
//! - differential: `serde_json::Value`, from the same `serde_json` build, accepts the message
//!   (as one object, or under the prefix mode as the first value of a `StreamDeserializer` that is
//!   an object) if and only if the parse does, and on success reads the same pairs. The
//!   reference's numbers map as the parse's visitor sees them: `U64` when `is_u64`, else `I64`
//!   when `is_i64`, else `F64` compared by bits. A duplicate key's last value wins at every depth,
//!   and maps compare by key;
//! - strings: every `Value::Str` is valid UTF-8 and equals the reference's string. One that
//!   shares the message's buffer (`subslice::within`) sits between the two quotes of its own
//!   string token, so a zero-copy slice is never a slice of the wrong bytes.
//!
//! On `Err` the core may leave a partial prefix in `out`; the transform clears it, and
//! `crates/logit-transforms/src/json.rs`'s tests pin that.
//!
//! The differential skips a parse holding the key [`RAW_VALUE_TOKEN`] at any depth. The workspace
//! builds `serde_json` with `raw_value`, under which `serde_json::Value` reads an object whose
//! first key is that token as the JSON inside its string value, where the parse keeps an ordinary
//! one-pair object; no producer writes the key.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_core::interner::{resolve, KeyCache};
use logit_core::subslice::within;
use logit_core::{Symbol, Value};
use logit_proto::message::json::{parse_object, parse_object_prefix};
use std::cell::RefCell;
use std::collections::BTreeMap;

/// The key `serde_json`'s `raw_value` feature reserves for a `RawValue` (its `src/raw.rs`).
const RAW_VALUE_TOKEN: &str = "$serde_json::private::RawValue";

fn holds_raw_value_token(value: &Value) -> bool {
    match value {
        Value::Map(map) => {
            map.iter().any(|(k, v)| resolve(k) == RAW_VALUE_TOKEN || holds_raw_value_token(v))
        }
        Value::Array(items) => items.iter().any(holds_raw_value_token),
        _ => false,
    }
}

thread_local! {
    // One cache across inputs, as one `json` transform keeps one across events.
    static KEYS: RefCell<KeyCache> = RefCell::new(KeyCache::new());
}

/// The reference's reading of the message, or `None` where it rejects it.
fn reference(body: &[u8], prefix: bool) -> Option<serde_json::Map<String, serde_json::Value>> {
    let value = if prefix {
        serde_json::Deserializer::from_slice(body).into_iter::<serde_json::Value>().next()?.ok()?
    } else {
        serde_json::from_slice::<serde_json::Value>(body).ok()?
    };
    match value {
        serde_json::Value::Object(map) => Some(map),
        _ => None,
    }
}

fn check_str(base: &Bytes, got: &Bytes, want: &str, path: &str) {
    assert!(std::str::from_utf8(got).is_ok(), "strings: {path} is not valid UTF-8");
    assert_eq!(&got[..], want.as_bytes(), "strings: {path} differs from the reference");
    if within(base, got) {
        let start = got.as_ptr() as usize - base.as_ptr() as usize;
        let end = start + got.len();
        assert!(
            start > 0 && base[start - 1] == b'"' && base.get(end) == Some(&b'"'),
            "strings: {path} shares bytes {start}..{end}, which aren't one string token's content"
        );
    }
}

fn check_value(base: &Bytes, got: &Value, want: &serde_json::Value, path: &str) {
    use serde_json::Value as J;
    match (got, want) {
        (Value::Null, J::Null) => {}
        (Value::Bool(a), J::Bool(b)) => assert_eq!(a, b, "differential: {path}"),
        (got, J::Number(n)) => {
            if let Some(u) = n.as_u64() {
                assert_eq!(got, &Value::U64(u), "differential: {path}");
            } else if let Some(i) = n.as_i64() {
                assert_eq!(got, &Value::I64(i), "differential: {path}");
            } else {
                let f = n.as_f64().expect("a serde_json number is u64, i64, or f64");
                match got {
                    Value::F64(g) => {
                        assert_eq!(g.to_bits(), f.to_bits(), "differential: {path}: {g} vs {f}")
                    }
                    other => panic!("differential: {path}: {other:?} where the reference has {f}"),
                }
            }
        }
        (Value::Str(s), J::String(w)) => check_str(base, s, w, path),
        (Value::Array(items), J::Array(want)) => {
            assert_eq!(items.len(), want.len(), "differential: {path} array length");
            for (i, (g, w)) in items.iter().zip(want).enumerate() {
                check_value(base, g, w, &format!("{path}[{i}]"));
            }
        }
        (Value::Map(map), J::Object(want)) => {
            let got: BTreeMap<&str, &Value> = map.iter().map(|(k, v)| (resolve(k), v)).collect();
            check_map(base, &got, want, path);
        }
        (got, want) => panic!("differential: {path}: {got:?} where the reference has {want}"),
    }
}

fn check_map(
    base: &Bytes,
    got: &BTreeMap<&str, &Value>,
    want: &serde_json::Map<String, serde_json::Value>,
    path: &str,
) {
    let got_keys: Vec<&str> = got.keys().copied().collect();
    // Sorted, in case a feature elsewhere in the build turns on `preserve_order`.
    let mut want_keys: Vec<&str> = want.keys().map(String::as_str).collect();
    want_keys.sort_unstable();
    assert_eq!(got_keys, want_keys, "differential: {path} keys");
    for (key, value) in got {
        check_value(base, value, &want[*key], &format!("{path}.{key}"));
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else { return };
    let prefix = selector & 1 == 1;
    let base = Bytes::copy_from_slice(body);
    let mut out: Vec<(Symbol, Value)> = Vec::new();
    let result = KEYS.with(|keys| {
        let keys = &mut *keys.borrow_mut();
        if prefix {
            parse_object_prefix(&base, &mut out, keys)
        } else {
            parse_object(&base, &mut out, keys)
        }
    });

    if result.is_ok()
        && out.iter().any(|(k, v)| resolve(*k) == RAW_VALUE_TOKEN || holds_raw_value_token(v))
    {
        return;
    }

    let want = reference(body, prefix);
    let want = match (result, want) {
        (Ok(()), Some(want)) => want,
        (Err(_), None) => return,
        (result, want) => panic!(
            "differential: the parse says {result:?}, the reference {}",
            if want.is_some() { "accepts" } else { "rejects" }
        ),
    };

    // The top level pushes a duplicate key twice; merging in push order keeps the later value,
    // as the transform's `insert_sym` does.
    let mut got = BTreeMap::new();
    for (key, value) in &out {
        got.insert(resolve(*key), value);
    }
    check_map(&base, &got, &want, "$");
});
