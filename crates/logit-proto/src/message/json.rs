//! The `json` transform's parse core: one JSON object into `(Symbol, Value)` pairs.
//!
//! The `json` transform (`crates/logit-transforms/src/json.rs`) wraps it with its all-or-nothing
//! merge, its `invalid_utf8` retry, and its diagnostics; the mapping is in
//! [ADR `json-parsing-into-attributes`](../../../../docs/adr/json-parsing-into-attributes.md). A
//! nested object becomes a `Value::Map`, an array a `Value::Array`, and an unescaped string a
//! zero-copy slice of the message.
//!
//! On `Err`, `out` may hold the pairs parsed before the failure. The caller clears it: a partial
//! pair's `Value::Str` may slice the message and would keep its buffer alive.

use bytes::Bytes;
use logit_core::interner::KeyCache;
use logit_core::subslice;
use logit_core::{AttrMap, Symbol, Value};
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use std::fmt;

/// Parses `json` as one JSON object, appending its top-level pairs to `out` in source order;
/// anything but trailing whitespace after the object fails.
///
/// A duplicate key is pushed twice, so a caller merging in push order keeps the later value.
/// `keys` resolves every key, top-level and nested; reuse one across calls. On `Err`, `out` may
/// hold a partial prefix and the caller clears it.
#[inline]
pub fn parse_object(
    json: &Bytes,
    out: &mut Vec<(Symbol, Value)>,
    keys: &mut KeyCache,
) -> Result<(), serde_json::Error> {
    let mut de = serde_json::Deserializer::from_slice(json);
    TopLevelSeed { base: json, out, keys }.deserialize(&mut de)?;
    de.end()?;
    Ok(())
}

/// [`parse_object`] for the first complete JSON object in `json`, ignoring anything after it, so
/// the `json` transform's `skip_to_brace` handles a line like `INFO {"a":1} took=3ms`.
///
/// On `Err`, `out` may hold a partial prefix and the caller clears it.
#[inline]
pub fn parse_object_prefix(
    json: &Bytes,
    out: &mut Vec<(Symbol, Value)>,
    keys: &mut KeyCache,
) -> Result<(), serde_json::Error> {
    let mut de = serde_json::Deserializer::from_slice(json);
    TopLevelSeed { base: json, out, keys }.deserialize(&mut de)
}

/// Slices `base`, the message being parsed, for a `&str` serde_json borrowed from it
/// (`Visitor::visit_borrowed_str`), so an unescaped string stays zero-copy.
fn borrowed_str_bytes(base: &Bytes, s: &str) -> Bytes {
    subslice::share(base, s.as_bytes())
}

/// Deserializes a JSON value straight into a [`Value`], skipping a `serde_json::Value` tree and
/// its conversion, so an unescaped string can stay a slice of `base` ([`borrowed_str_bytes`]).
struct ValueSeed<'b, 'k> {
    base: &'b Bytes,
    /// Reborrowed per value so nested keys share the top level's [`KeyCache`].
    keys: &'k mut KeyCache,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_, '_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(ValueVisitor { base: self.base, keys: self.keys })
    }
}

struct ValueVisitor<'b, 'k> {
    base: &'b Bytes,
    keys: &'k mut KeyCache,
}

impl<'de> Visitor<'de> for ValueVisitor<'_, '_> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(Value::I64(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(Value::U64(v))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Value, E> {
        Ok(Value::F64(v))
    }

    // Unescaped: `v` is a slice of `self.base`, so it stays zero-copy.
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Value, E> {
        Ok(Value::Str(borrowed_str_bytes(self.base, v)))
    }

    // Escaped: `v` is in serde_json's scratch buffer, not `self.base`, so it must be copied.
    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Str(Bytes::copy_from_slice(v.as_bytes())))
    }

    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::Str(Bytes::from(v)))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) =
            seq.next_element_seed(ValueSeed { base: self.base, keys: &mut *self.keys })?
        {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Value, A::Error> {
        Ok(Value::Map(Box::new(collect_attrmap(map, self.base, self.keys)?)))
    }
}

/// The top-level seed: a bare scalar or array is a parse error by construction.
///
/// The pairs go into the caller's reused `Vec`, not an `AttrMap`, because the caller merges them
/// into `event.attributes` and discards them, unlike a nested object ([`collect_attrmap`]).
struct TopLevelSeed<'b, 'o, 'k> {
    base: &'b Bytes,
    out: &'o mut Vec<(Symbol, Value)>,
    keys: &'k mut KeyCache,
}

impl<'de> DeserializeSeed<'de> for TopLevelSeed<'_, '_, '_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(TopLevelVisitor {
            base: self.base,
            out: self.out,
            keys: self.keys,
        })
    }
}

struct TopLevelVisitor<'b, 'o, 'k> {
    base: &'b Bytes,
    out: &'o mut Vec<(Symbol, Value)>,
    keys: &'k mut KeyCache,
}

impl<'de> Visitor<'de> for TopLevelVisitor<'_, '_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a JSON object")
    }

    // A duplicate key pushes twice; the caller merges in push order and `insert_sym` overwrites,
    // so the later value wins without a lookup per key here.
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key_seed(KeySeed { keys: &mut *self.keys })? {
            let value =
                map.next_value_seed(ValueSeed { base: self.base, keys: &mut *self.keys })?;
            self.out.push((key, value));
        }
        Ok(())
    }
}

/// Deserializes an object key straight to its [`Symbol`] through the parser's [`KeyCache`].
///
/// `next_key::<String>()` would allocate a `String` per key, which was most of `json`'s
/// allocations (`docs/design/memory.md`). A key seen before costs one `memcmp` and never reaches
/// the process-wide interner.
struct KeySeed<'k> {
    keys: &'k mut KeyCache,
}

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = Symbol;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Symbol, D::Error> {
        deserializer.deserialize_str(KeyVisitor { keys: self.keys })
    }
}

struct KeyVisitor<'k> {
    keys: &'k mut KeyCache,
}

impl<'de> Visitor<'de> for KeyVisitor<'_> {
    type Value = Symbol;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a string")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Symbol, E> {
        Ok(self.keys.get_or_intern(v))
    }

    // An escaped key sits in serde_json's scratch buffer only long enough to compare or intern,
    // so it needs no `String` either.
    fn visit_str<E>(self, v: &str) -> Result<Symbol, E> {
        Ok(self.keys.get_or_intern(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Symbol, E> {
        Ok(self.keys.get_or_intern(&v))
    }
}

/// Collects a nested JSON object into its own `AttrMap` for a `Value::Map`.
fn collect_attrmap<'de, A: MapAccess<'de>>(
    mut map: A,
    base: &Bytes,
    keys: &mut KeyCache,
) -> Result<AttrMap, A::Error> {
    let mut attrs = AttrMap::new();
    while let Some(key) = map.next_key_seed(KeySeed { keys: &mut *keys })? {
        let value = map.next_value_seed(ValueSeed { base, keys: &mut *keys })?;
        // `insert_sym` overwrites, so a duplicate key's last value wins.
        attrs.insert_sym(key, value);
    }
    Ok(attrs)
}
