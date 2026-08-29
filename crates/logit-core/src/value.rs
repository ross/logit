//! The value type carried by attributes and log/metric payloads.
//!
//! This is deliberately the same type the Lua API exposes (`docs/design/lua-api.md`) -- one
//! definition, not a Rust type and a parallel Lua conversion kept in sync by hand.

use crate::AttrMap;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    /// Arbitrary bytes -- not assumed to be valid UTF-8.
    Bytes(Bytes),
    /// UTF-8 text. Kept distinct from `Bytes` so codecs and scripts can rely on validity.
    Str(Bytes),
    /// Unix nanoseconds.
    Timestamp(i64),
    Array(Vec<Value>),
    /// Boxed: `AttrMap` holds `Value`s inline (see `docs/design/data-model.md`'s small-map
    /// layout), so an unboxed `Value` here would make `Value` and `AttrMap` infinitely sized.
    Map(Box<AttrMap>),
}

impl Value {
    pub fn str(s: impl Into<String>) -> Self {
        Value::Str(Bytes::from(s.into()))
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            // Constructed only from valid UTF-8 (see `str`/`From<&str>`), so this cannot panic.
            Value::Str(b) => {
                Some(std::str::from_utf8(b).expect("Value::Str is always valid UTF-8"))
            }
            _ => None,
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::str(s)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::I64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::F64(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
