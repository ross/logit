//! JSON plumbing the JSON codecs share (`datadog`'s JSON routes, `splunk`): the
//! `serde_json::Value` → [`Value`] conversion, a small ordered JSON writer, and [`flatten_into`]'s
//! dotted-key flattening. The writer exists because the workspace's `serde_json` has no
//! `preserve_order`, and a route that owes a producer's key order (the Datadog Agent's events, the
//! OpenTelemetry Collector's HEC span object) writes members in call order.

use base64::Engine;
use logit_core::interner::{intern, resolve};
use logit_core::{format_rfc3339_utc, AttrMap, Value};
use serde_json::Value as Json;
use std::borrow::Cow;

/// `serde_json::Value` → [`Value`]: object → `Map`, array → `Array`, an integer → `I64` (`U64`
/// above `i64::MAX`), any other number → `F64`, string → `Str`, bool, null.
pub(crate) fn json_to_value(json: &Json) -> Value {
    match json {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::I64(i)
            } else if let Some(u) = n.as_u64() {
                Value::U64(u)
            } else {
                Value::F64(n.as_f64().unwrap_or(0.0))
            }
        }
        Json::String(s) => Value::str(s.as_str()),
        Json::Array(items) => Value::Array(items.iter().map(json_to_value).collect()),
        Json::Object(obj) => {
            let mut map = AttrMap::new();
            for (k, v) in obj {
                map.insert(k, json_to_value(v));
            }
            Value::Map(Box::new(map))
        }
    }
}

/// A `Value` as the text a JSON string field carries (a Datadog `message` or check name, a HEC
/// `event` string): `Str` as-is, `Bytes` as lossy UTF-8 (a raw log line, not binary data), anything else
/// as its JSON text.
pub(crate) fn value_text(value: &Value) -> Cow<'_, str> {
    match value {
        Value::Str(_) => Cow::Borrowed(value.as_str().unwrap_or_default()),
        Value::Bytes(b) => String::from_utf8_lossy(b),
        other => {
            let mut buf = Vec::new();
            write_value(&mut buf, other);
            Cow::Owned(String::from_utf8(buf).unwrap_or_default())
        }
    }
}

/// Writes one JSON object's members in call order.
pub(crate) struct JsonObject<'a> {
    out: &'a mut Vec<u8>,
    first: bool,
}

impl<'a> JsonObject<'a> {
    pub(crate) fn begin(out: &'a mut Vec<u8>) -> Self {
        out.push(b'{');
        Self { out, first: true }
    }

    /// Writes `"key":` (after a separating comma, past the first member) and hands back the
    /// buffer for the caller to write the value into.
    pub(crate) fn key(&mut self, key: &str) -> &mut Vec<u8> {
        if !self.first {
            self.out.push(b',');
        }
        self.first = false;
        write_str(self.out, key);
        self.out.push(b':');
        self.out
    }

    pub(crate) fn finish(self) {
        self.out.push(b'}');
    }
}

pub(crate) fn write_str(out: &mut Vec<u8>, s: &str) {
    // Serializing a `&str` into a `Vec` can't fail.
    let _ = serde_json::to_writer(&mut *out, s);
}

pub(crate) fn write_i64(out: &mut Vec<u8>, n: i64) {
    out.extend_from_slice(n.to_string().as_bytes());
}

/// `Value` → JSON: `Bytes` → a base64 string, `Timestamp` → an RFC 3339 string, a non-finite
/// `F64` → `null` (JSON has no spelling for it), `Map`/`Array` nested.
pub(crate) fn write_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::I64(n) => write_i64(out, *n),
        Value::U64(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Value::F64(f) if f.is_finite() => {
            let _ = serde_json::to_writer(&mut *out, f);
        }
        Value::F64(_) => out.extend_from_slice(b"null"),
        Value::Str(_) => write_str(out, value.as_str().unwrap_or_default()),
        Value::Bytes(b) => write_str(out, &base64::engine::general_purpose::STANDARD.encode(b)),
        Value::Timestamp(ns) => write_str(out, &format_rfc3339_utc(*ns)),
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_value(out, item);
            }
            out.push(b']');
        }
        Value::Map(map) => {
            let mut obj = JsonObject::begin(out);
            for (key, item) in map.iter() {
                write_value(obj.key(resolve(key)), item);
            }
            obj.finish();
        }
    }
}

/// A stack-safety bound on [`flatten_into`]'s recursion, the `flatten` transform's `MAX_DEPTH`. A
/// value still nested at this depth is written whole, as its JSON text.
pub(crate) const FLATTEN_MAX_DEPTH: usize = 32;

/// Writes `value` into `out` under `prefix`, a nested `Map` expanded into dot-joined keys
/// (`{"a":{"b":1}}` under `x` becomes `x.a.b = 1`), with the `flatten` transform's leaf rule:
///
/// - a non-container value, an empty `Array`, or an `Array` of non-containers is a leaf, written
///   as-is;
/// - an empty `Map` writes nothing;
/// - an `Array` holding a `Map` or `Array` is written as its JSON text (`Str`), because a flat
///   wire has nowhere to put an element's own keys;
/// - a `Map` still nested at [`FLATTEN_MAX_DEPTH`] is written as its JSON text.
///
/// A key collision is last write wins, as in `flatten`: `out` is an [`AttrMap`], so a later
/// insert overwrites.
pub(crate) fn flatten_into(prefix: &str, value: &Value, out: &mut AttrMap) {
    let mut path = String::from(prefix);
    flatten_at(&mut path, value, out, 0);
}

fn flatten_at(path: &mut String, value: &Value, out: &mut AttrMap, depth: usize) {
    match value {
        Value::Map(map) if map.is_empty() => {}
        Value::Map(map) if depth < FLATTEN_MAX_DEPTH => {
            let base = path.len();
            for (key, item) in map.iter() {
                path.truncate(base);
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(resolve(key));
                flatten_at(path, item, out, depth + 1);
            }
            path.truncate(base);
        }
        Value::Map(_) => out.insert_sym(intern(path), Value::str(json_text(value))),
        Value::Array(items)
            if items.iter().any(|item| matches!(item, Value::Map(_) | Value::Array(_))) =>
        {
            out.insert_sym(intern(path), Value::str(json_text(value)))
        }
        other => out.insert_sym(intern(path), other.clone()),
    }
}

/// `value`'s JSON text, as [`write_value`] renders it.
pub(crate) fn json_text(value: &Value) -> String {
    let mut buf = Vec::new();
    write_value(&mut buf, value);
    String::from_utf8(buf).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&'static str, Value)]) -> Value {
        Value::Map(Box::new(pairs.iter().cloned().collect()))
    }

    fn flat(value: &Value) -> Vec<(String, Value)> {
        let mut out = AttrMap::new();
        flatten_into("f", value, &mut out);
        let mut pairs: Vec<_> =
            out.iter().map(|(k, v)| (resolve(k).to_string(), v.clone())).collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
    }

    #[test]
    fn nested_maps_become_dotted_keys_and_leaves_stay_typed() {
        let value = map(&[
            ("a", map(&[("b", Value::I64(1)), ("c", map(&[("d", Value::Bool(true))]))])),
            ("s", Value::str("x")),
        ]);
        assert_eq!(
            flat(&value),
            vec![
                ("f.a.b".into(), Value::I64(1)),
                ("f.a.c.d".into(), Value::Bool(true)),
                ("f.s".into(), Value::str("x")),
            ]
        );
    }

    #[test]
    fn empty_maps_vanish_and_arrays_of_containers_become_json_text() {
        let value = map(&[
            ("empty", map(&[])),
            ("tags", Value::Array(vec![Value::str("a"), Value::I64(2)])),
            ("none", Value::Array(vec![])),
            ("items", Value::Array(vec![map(&[("n", Value::I64(1))])])),
        ]);
        assert_eq!(
            flat(&value),
            vec![
                ("f.items".into(), Value::str(r#"[{"n":1}]"#)),
                ("f.none".into(), Value::Array(vec![])),
                ("f.tags".into(), Value::Array(vec![Value::str("a"), Value::I64(2)])),
            ]
        );
    }

    #[test]
    fn a_scalar_is_written_at_the_prefix_and_an_empty_prefix_adds_no_dot() {
        assert_eq!(flat(&Value::F64(1.5)), vec![("f".into(), Value::F64(1.5))]);
        let mut out = AttrMap::new();
        flatten_into("", &map(&[("k", Value::Null)]), &mut out);
        assert_eq!(out.get("k"), Some(&Value::Null));
    }

    #[test]
    fn nesting_past_the_depth_bound_is_written_whole() {
        let mut value = Value::I64(7);
        for _ in 0..FLATTEN_MAX_DEPTH + 1 {
            value = map(&[("k", value)]);
        }
        let pairs = flat(&value);
        assert_eq!(pairs.len(), 1);
        let (key, leaf) = &pairs[0];
        assert_eq!(key.matches(".k").count(), FLATTEN_MAX_DEPTH);
        assert_eq!(leaf, &Value::str(r#"{"k":7}"#));
    }
}
