//! `Value`/`AttrMap` wire encoding: every value is `tag(1) + len(varint) + payload(len bytes)`.
//!
//! The `len` prefix makes an unrecognized tag skippable by byte count, so sibling attributes and
//! later fields still decode. `Value` has no `Unknown` variant (`docs/design/data-model.md`), so an
//! unrecognized tag decodes to [`Value::Null`], the "absent" sentinel data-model.md already uses
//! for `""`/`"-"`.
//!
//! A payload is consumed whole: bytes after a scalar's value, an array's last item, or a map's
//! last entry are `Malformed`, so decoding and re-encoding a valid payload reproduces it byte for
//! byte.
//!
//! [`read_attr_map`] inserts each entry into `AttrMap`'s sorted storage as it reads it, which is
//! quadratic for a large map whose keys arrive in descending symbol order. That is a documented
//! non-goal: `docs/known-gaps.md`, "A native attribute map with keys in descending dictionary
//! order inserts in quadratic time".

use bytes::{Buf, Bytes, BytesMut};
use logit_core::{AttrMap, Symbol, Value};

use crate::native::dict::{Dict, DictBuilder};
use crate::native::varint::{
    ensure_consumed, read_ivarint, read_u8, read_uvarint, write_ivarint, write_uvarint,
};
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

/// The deepest `Value::Array`/`Value::Map` nesting the recursive decoder follows. Without a cap, a
/// crafted payload of nested array headers overflows the stack and aborts the process. It sits
/// near `serde_json`'s 128-level limit, which bounds what the `json` transform can build.
/// `write_value` isn't capped: a `Value` too deep to encode couldn't have been built.
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
    let value = match tag {
        TAG_NULL => Value::Null,
        TAG_BOOL => {
            if payload.is_empty() {
                return Err(CodecError::Malformed("Bool value has no payload byte".to_string()));
            }
            Value::Bool(payload.get_u8() != 0)
        }
        TAG_I64 => Value::I64(read_ivarint(&mut payload)?),
        TAG_U64 => Value::U64(read_uvarint(&mut payload)?),
        TAG_F64 => {
            if payload.len() != 8 {
                return Err(CodecError::Malformed(format!(
                    "F64 value declared {} bytes, expected 8",
                    payload.len()
                )));
            }
            let mut buf = [0u8; 8];
            payload.copy_to_slice(&mut buf);
            Value::F64(f64::from_le_bytes(buf))
        }
        // A `Bytes`/`Str` value is a slice of the frame's buffer: nothing to charge.
        TAG_BYTES => return Ok(Value::Bytes(payload)),
        TAG_STR => {
            std::str::from_utf8(&payload)
                .map_err(|e| CodecError::Malformed(format!("Str value not utf-8: {e}")))?;
            return Ok(Value::Str(payload));
        }
        TAG_TIMESTAMP => Value::Timestamp(read_ivarint(&mut payload)?),
        TAG_ARRAY => {
            let count = read_uvarint(&mut payload)? as usize;
            // An item is at least a tag and a length byte.
            dict.budget().charge_list(
                "array value",
                count,
                2,
                payload.len(),
                std::mem::size_of::<Value>(),
            )?;
            let mut items = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                items.push(read_value_at(&mut payload, dict, depth + 1)?);
            }
            Value::Array(items)
        }
        TAG_MAP => {
            dict.budget().charge(std::mem::size_of::<AttrMap>() as u64)?;
            Value::Map(Box::new(read_attr_map_at(&mut payload, dict, depth + 1)?))
        }
        // An unrecognized tag: the `len`-byte skip above already consumed it, so degrade to
        // Null (see the module doc).
        _unknown => return Ok(Value::Null),
    };
    ensure_consumed(&payload, "value")?;
    Ok(value)
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
    // An entry is at least a key index, a value tag, and a value length byte.
    dict.budget().charge_list(
        "attribute map",
        count,
        3,
        bytes.len(),
        std::mem::size_of::<(Symbol, Value)>(),
    )?;
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
    use crate::native::budget::DecodeBudget;

    fn dict_round_trip(value: &Value) -> Value {
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_value(&mut buf, &mut builder, value);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let mut dict_bytes = dict_bytes.freeze();
        let budget = DecodeBudget::unlimited();
        let dict = Dict::read(&mut dict_bytes, &budget).unwrap();

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
        let budget = DecodeBudget::unlimited();
        let dict = Dict::read(&mut dict_bytes, &budget).unwrap();

        let mut bytes = buf.freeze();
        let out = read_attr_map(&mut bytes, &dict).unwrap();
        assert_eq!(out, map);
    }

    /// An unrecognized `Value` tag decodes to `Null`, and the value after it still reads back.
    #[test]
    fn an_unrecognized_value_tag_degrades_to_null_without_corrupting_what_follows() {
        let mut buf = BytesMut::new();
        // Tag 200 is unassigned.
        buf.extend_from_slice(&[200]);
        write_uvarint(&mut buf, 3);
        buf.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        write_value(&mut buf, &mut DictBuilder::default(), &Value::I64(99));

        let mut bytes = buf.freeze();
        let mut empty_dict_bytes = BytesMut::new();
        write_uvarint(&mut empty_dict_bytes, 0);
        let budget = DecodeBudget::unlimited();
        let dict = Dict::read(&mut empty_dict_bytes.freeze(), &budget).unwrap();
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
        let budget = DecodeBudget::unlimited();
        let dict = Dict::read(&mut dict_bytes.freeze(), &budget).unwrap();

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
        let budget = DecodeBudget::unlimited();
        let dict = Dict::read(&mut dict_bytes.freeze(), &budget).unwrap();

        let mut bytes = buf.freeze();
        assert!(matches!(read_value(&mut bytes, &dict), Err(CodecError::Malformed(_))));
    }

    /// `TAG_MAP` adds a level and `read_attr_map_at` passes it through unchanged, so alternating
    /// arrays and maps counts one level per nesting, not zero or two.
    #[test]
    fn alternating_arrays_and_maps_count_one_level_each() {
        fn nest(levels: usize) -> Value {
            let mut value = Value::I64(1);
            for level in 0..levels {
                value = if level % 2 == 0 {
                    Value::Array(vec![value])
                } else {
                    let mut m = AttrMap::new();
                    m.insert("n", value);
                    Value::Map(Box::new(m))
                };
            }
            value
        }
        let at_cap = nest(MAX_VALUE_DEPTH);
        assert_eq!(dict_round_trip(&at_cap), at_cap);

        let past_cap = nest(MAX_VALUE_DEPTH + 1);
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_value(&mut buf, &mut builder, &past_cap);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let budget = DecodeBudget::unlimited();
        let dict = Dict::read(&mut dict_bytes.freeze(), &budget).unwrap();
        assert!(matches!(
            read_value(&mut buf.freeze(), &dict),
            Err(CodecError::Malformed(msg)) if msg.contains("nesting")
        ));
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
