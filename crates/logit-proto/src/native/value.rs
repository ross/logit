//! `Value`/`AttrMap` wire encoding: every value is `tag(1) + len(varint) + payload(len bytes)`.
//!
//! The `len` prefix is what makes an unrecognized tag skippable -- a future `Value` variant this
//! reader doesn't know about can still be stepped over by byte count, so the rest of the
//! surrounding structure (sibling attributes, other event fields) decodes normally. There is no
//! `Value::Unknown` escape hatch to return in that case (`Value` is a closed enum with no
//! extensibility variant, `docs/design/data-model.md`'s own open question), so an unrecognized tag
//! decodes to [`Value::Null`] -- the same "absent" sentinel `docs/design/data-model.md`'s
//! well-known-attributes table already documents for `""`/`"-"`, not a new convention invented
//! here. This degrades a value this reader can't represent rather than corrupting the byte stream
//! trying to skip it blindly.

use bytes::{Buf, Bytes, BytesMut};
use logit_core::{AttrMap, Value};

use crate::native::dict::{Dict, DictBuilder};
use crate::native::varint::{read_ivarint, read_u8, read_uvarint, write_ivarint, write_uvarint};
use crate::CodecError;

const TAG_NULL: u8 = 0;
const TAG_BOOL: u8 = 1;
const TAG_I64: u8 = 2;
const TAG_U64: u8 = 3;
const TAG_F64: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_STR: u8 = 6;
const TAG_TIMESTAMP: u8 = 7;
const TAG_ARRAY: u8 = 8;
const TAG_MAP: u8 = 9;

/// The deepest `Value::Array`/`Value::Map` nesting this reader will follow. Generous -- real
/// telemetry attribute values are flat or one level deep, and this is in the same range as
/// `serde_json`'s own 128-level recursion limit, which bounds the deepest `Value` the `json`
/// transform can construct in the first place -- but bounded, because the decode side is
/// recursive: without a cap, a crafted payload of nothing but nested array headers overflows the
/// stack and aborts the process rather than returning an error. Encode-side (`write_value`) is
/// deliberately NOT capped: it only ever encodes a `Value` that already exists in memory, so a
/// depth that would overflow the encoder would already have overflowed whatever built the value.
const MAX_VALUE_DEPTH: usize = 128;

pub fn write_value(out: &mut BytesMut, dict: &mut DictBuilder, value: &Value) {
    match value {
        Value::Null => {
            out.extend_from_slice(&[TAG_NULL]);
            write_uvarint(out, 0);
        }
        Value::Bool(b) => {
            out.extend_from_slice(&[TAG_BOOL]);
            write_uvarint(out, 1);
            out.extend_from_slice(&[*b as u8]);
        }
        Value::I64(v) => write_varint_payload(out, TAG_I64, {
            let mut tmp = BytesMut::new();
            write_ivarint(&mut tmp, *v);
            tmp
        }),
        Value::U64(v) => write_varint_payload(out, TAG_U64, {
            let mut tmp = BytesMut::new();
            write_uvarint(&mut tmp, *v);
            tmp
        }),
        Value::F64(v) => {
            out.extend_from_slice(&[TAG_F64]);
            write_uvarint(out, 8);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Value::Bytes(b) => {
            out.extend_from_slice(&[TAG_BYTES]);
            write_uvarint(out, b.len() as u64);
            out.extend_from_slice(b);
        }
        Value::Str(b) => {
            out.extend_from_slice(&[TAG_STR]);
            write_uvarint(out, b.len() as u64);
            out.extend_from_slice(b);
        }
        Value::Timestamp(v) => write_varint_payload(out, TAG_TIMESTAMP, {
            let mut tmp = BytesMut::new();
            write_ivarint(&mut tmp, *v);
            tmp
        }),
        Value::Array(items) => {
            let mut tmp = BytesMut::new();
            write_uvarint(&mut tmp, items.len() as u64);
            for item in items {
                write_value(&mut tmp, dict, item);
            }
            out.extend_from_slice(&[TAG_ARRAY]);
            write_uvarint(out, tmp.len() as u64);
            out.extend_from_slice(&tmp);
        }
        Value::Map(map) => {
            let mut tmp = BytesMut::new();
            write_attr_map(&mut tmp, dict, map);
            out.extend_from_slice(&[TAG_MAP]);
            write_uvarint(out, tmp.len() as u64);
            out.extend_from_slice(&tmp);
        }
    }
}

fn write_varint_payload(out: &mut BytesMut, tag: u8, payload: BytesMut) {
    out.extend_from_slice(&[tag]);
    write_uvarint(out, payload.len() as u64);
    out.extend_from_slice(&payload);
}

pub fn read_value(bytes: &mut Bytes, dict: &Dict) -> Result<Value, CodecError> {
    read_value_at(bytes, dict, 0)
}

fn read_value_at(bytes: &mut Bytes, dict: &Dict, depth: usize) -> Result<Value, CodecError> {
    if depth > MAX_VALUE_DEPTH {
        return Err(CodecError::Malformed(format!(
            "value nesting deeper than the {MAX_VALUE_DEPTH}-level cap"
        )));
    }
    let tag = read_u8(bytes)?;
    let len = read_uvarint(bytes)? as usize;
    if bytes.len() < len {
        return Err(CodecError::Malformed(format!(
            "value tag {tag} declares {len} bytes but only {} remain",
            bytes.len()
        )));
    }
    let mut payload = bytes.split_to(len);
    match tag {
        TAG_NULL => Ok(Value::Null),
        TAG_BOOL => {
            if payload.is_empty() {
                return Err(CodecError::Malformed("Bool value has no payload byte".to_string()));
            }
            Ok(Value::Bool(payload.get_u8() != 0))
        }
        TAG_I64 => Ok(Value::I64(read_ivarint(&mut payload)?)),
        TAG_U64 => Ok(Value::U64(read_uvarint(&mut payload)?)),
        TAG_F64 => {
            if payload.len() != 8 {
                return Err(CodecError::Malformed(format!(
                    "F64 value declared {} bytes, expected 8",
                    payload.len()
                )));
            }
            let mut buf = [0u8; 8];
            payload.copy_to_slice(&mut buf);
            Ok(Value::F64(f64::from_le_bytes(buf)))
        }
        TAG_BYTES => Ok(Value::Bytes(payload)),
        TAG_STR => {
            std::str::from_utf8(&payload)
                .map_err(|e| CodecError::Malformed(format!("Str value not utf-8: {e}")))?;
            Ok(Value::Str(payload))
        }
        TAG_TIMESTAMP => Ok(Value::Timestamp(read_ivarint(&mut payload)?)),
        TAG_ARRAY => {
            let count = read_uvarint(&mut payload)? as usize;
            let mut items = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                items.push(read_value_at(&mut payload, dict, depth + 1)?);
            }
            Ok(Value::Array(items))
        }
        TAG_MAP => Ok(Value::Map(Box::new(read_attr_map_at(&mut payload, dict, depth + 1)?))),
        // Forward compatibility: a tag this reader doesn't recognize (a future Value variant)
        // was still framed as tag+len+payload, so the `len`-byte skip above already consumed it
        // in full -- there is nothing left to do but degrade to the documented "absent" sentinel.
        // See this module's own doc comment for why Null, not an error.
        _unknown => Ok(Value::Null),
    }
}

pub fn write_attr_map(out: &mut BytesMut, dict: &mut DictBuilder, map: &AttrMap) {
    write_uvarint(out, map.len() as u64);
    for (key, value) in map.iter() {
        write_uvarint(out, dict.intern(key) as u64);
        write_value(out, dict, value);
    }
}

pub fn read_attr_map(bytes: &mut Bytes, dict: &Dict) -> Result<AttrMap, CodecError> {
    read_attr_map_at(bytes, dict, 0)
}

fn read_attr_map_at(bytes: &mut Bytes, dict: &Dict, depth: usize) -> Result<AttrMap, CodecError> {
    let count = read_uvarint(bytes)? as usize;
    let mut map = AttrMap::new();
    for _ in 0..count {
        let idx = read_uvarint(bytes)? as u32;
        let key = dict.get(idx)?;
        let value = read_value_at(bytes, dict, depth)?;
        map.insert_sym(key, value);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict_round_trip(value: &Value) -> Value {
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_value(&mut buf, &mut builder, value);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let mut dict_bytes = dict_bytes.freeze();
        let dict = Dict::read(&mut dict_bytes).unwrap();

        let mut bytes = buf.freeze();
        let out = read_value(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty(), "read_value should consume exactly one value");
        out
    }

    #[test]
    fn round_trips_every_scalar_variant() {
        for value in [
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::I64(-42),
            Value::I64(i64::MIN),
            Value::U64(u64::MAX),
            Value::F64(3.5),
            Value::Bytes(bytes::Bytes::from_static(b"\x00\x01\xff")),
            Value::str("hello"),
            Value::Timestamp(1_725_091_200_000_000_000),
        ] {
            assert_eq!(dict_round_trip(&value), value);
        }
    }

    #[test]
    fn round_trips_a_nested_array_and_map() {
        let array = Value::Array(vec![Value::I64(1), Value::str("two"), Value::Bool(true)]);
        assert_eq!(dict_round_trip(&array), array);

        let mut inner = AttrMap::new();
        inner.insert("k", Value::I64(7));
        let map = Value::Map(Box::new(inner));
        assert_eq!(dict_round_trip(&map), map);
    }

    #[test]
    fn round_trips_an_attr_map_with_several_keys() {
        let mut map = AttrMap::new();
        map.insert("host", "web-1");
        map.insert("env", "prod");
        map.insert("count", Value::I64(3));

        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_attr_map(&mut buf, &mut builder, &map);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let mut dict_bytes = dict_bytes.freeze();
        let dict = Dict::read(&mut dict_bytes).unwrap();

        let mut bytes = buf.freeze();
        let out = read_attr_map(&mut bytes, &dict).unwrap();
        assert_eq!(out, map);
    }

    /// The version-skew gate: a `Value` tag this reader doesn't recognize must decode to `Null`
    /// and leave every byte after it correctly positioned -- proven here by putting a real value
    /// right after the unknown one and confirming it still reads back exactly.
    #[test]
    fn an_unrecognized_value_tag_degrades_to_null_without_corrupting_what_follows() {
        let mut buf = BytesMut::new();
        // A value tag (200) no version of this codec will ever assign, with a 3-byte payload a
        // future variant might plausibly have used.
        buf.extend_from_slice(&[200]);
        write_uvarint(&mut buf, 3);
        buf.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        // A real, known value immediately after it.
        write_value(&mut buf, &mut DictBuilder::default(), &Value::I64(99));

        let mut bytes = buf.freeze();
        let mut empty_dict_bytes = BytesMut::new();
        write_uvarint(&mut empty_dict_bytes, 0);
        let dict = Dict::read(&mut empty_dict_bytes.freeze()).unwrap();
        let unknown = read_value(&mut bytes, &dict).unwrap();
        assert_eq!(unknown, Value::Null);
        let known = read_value(&mut bytes, &dict).unwrap();
        assert_eq!(known, Value::I64(99));
        assert!(bytes.is_empty());
    }

    #[test]
    fn rejects_value_nesting_past_the_depth_cap() {
        let mut value = Value::I64(1);
        for _ in 0..(MAX_VALUE_DEPTH + 2) {
            value = Value::Array(vec![value]);
        }
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_value(&mut buf, &mut builder, &value);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();

        let mut bytes = buf.freeze();
        assert!(matches!(read_value(&mut bytes, &dict), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn rejects_map_nesting_past_the_depth_cap() {
        let mut value = Value::I64(1);
        for _ in 0..(MAX_VALUE_DEPTH + 2) {
            let mut m = AttrMap::new();
            m.insert("n", value);
            value = Value::Map(Box::new(m));
        }
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_value(&mut buf, &mut builder, &value);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();

        let mut bytes = buf.freeze();
        assert!(matches!(read_value(&mut bytes, &dict), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn a_value_nested_to_exactly_the_depth_cap_still_round_trips() {
        let mut value = Value::I64(1);
        for _ in 0..MAX_VALUE_DEPTH {
            value = Value::Array(vec![value]);
        }
        assert_eq!(dict_round_trip(&value), value);
    }
}
