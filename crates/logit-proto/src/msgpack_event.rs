//! The reader for `kafka_out`'s `format: msgpack`: one MessagePack map per event, or several
//! back to back in one input, into the [`EventBatch`]es that wrote them. `kafka_in` reads its
//! records through it.
//!
//! **What it shares with [`crate::event_json`].** The grammar is the one
//! `crates/logit-outputs/src/ndjson.rs`'s module doc gives, so this module only parses the bytes
//! into the tree both readers walk (`crate::event_tree`); the walk itself, the `types` table, the
//! resource and scope runs, and the `Arc` sharing are `event_json`'s
//! (`EventJsonDecoder::decode_object`). A key the grammar doesn't use is ignored, as there.
//!
//! **Where it differs**, by `crates/logit-outputs/src/msgpack.rs`'s module doc, the table of the
//! value forms:
//!
//! - A `bin` is `Value::Bytes`, tagged in the `types` table or not. JSON's `b"..."` text under a
//!   `bytes` tag also reads.
//! - A float is `F64`, a non-finite one included. Every int format is `I64`, or `U64` past
//!   `i64::MAX` or under a `u64` tag, as a JSON integer is. Where the grammar wants an `f64`,
//!   JSON's `"NaN"`, `"inf"`, and `"-inf"` strings also read.
//! - A map key must be a `str`, and an `ext` value is malformed: the writer emits neither
//!   otherwise.
//! - Input is several values back to back, not lines; empty input is no batch.
//!
//! **Bounds**, checked before anything is allocated: an array's or a map's declared count must
//! fit the bytes left (an entry takes at least one byte each), nesting stops at [`MAX_DEPTH`]
//! (`serde_json`'s own limit, so a value one format reads the other does too), and the parsed
//! trees of one input are charged to a [`DecodeBudget`], one tree node and each string's or
//! `bin`'s length at a time. Each failure is `CodecError::Malformed` with its own text, or
//! `CodecError::BudgetExceeded`.

use std::mem::size_of;

use bytes::Bytes;
use logit_core::EventBatch;

use crate::event_json::{push_line, EventJsonDecoder};
use crate::event_tree::{Node, Obj};
use crate::msgpack::{Reader, Type};
use crate::native::DecodeBudget;
use crate::CodecError;

/// The deepest nesting of arrays and maps a value may have; the event map is depth 1.
pub const MAX_DEPTH: usize = 128;

/// Reads `format: msgpack` values. Holds the last event's resource and scope, so events that
/// repeat them share one `Arc`, across calls as within one.
#[derive(Debug, Default)]
pub struct MsgpackEventDecoder {
    inner: EventJsonDecoder,
}

impl MsgpackEventDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes every map of `bytes` into one batch per run of events with equal resource and
    /// scope, in input order, under the default [`DecodeBudget`]. A `bin` value borrows from
    /// `bytes`.
    pub fn decode_batches(&mut self, bytes: &Bytes) -> Result<Vec<EventBatch>, CodecError> {
        self.decode_batches_with_budget(bytes, &DecodeBudget::default())
    }

    /// [`Self::decode_batches`] charging `budget`.
    pub fn decode_batches_with_budget(
        &mut self,
        bytes: &Bytes,
        budget: &DecodeBudget,
    ) -> Result<Vec<EventBatch>, CodecError> {
        let mut r = Reader::new(bytes);
        let mut batches = Vec::new();
        while !r.is_empty() {
            if r.peek_type()? != Type::Map {
                return Err(malformed(format!("a value is a {}, not a map", r.peek_type()?)));
            }
            let Node::Map(obj) = read_node(&mut r, bytes, 1, budget)? else {
                unreachable!("a map reads as a map");
            };
            push_line(&mut batches, self.inner.decode_object(&obj)?);
        }
        Ok(batches)
    }
}

fn malformed(msg: impl Into<String>) -> CodecError {
    CodecError::Malformed(format!("msgpack: {}", msg.into()))
}

/// Reads one value at nesting `depth` (its own, when it's a container).
fn read_node(
    r: &mut Reader<'_>,
    src: &Bytes,
    depth: usize,
    budget: &DecodeBudget,
) -> Result<Node, CodecError> {
    budget.charge(size_of::<Node>() as u64)?;
    Ok(match r.peek_type()? {
        Type::Nil => {
            r.read_nil()?;
            Node::Null
        }
        Type::Bool => Node::Bool(r.read_bool()?),
        Type::Int => Node::I64(r.read_i64()?),
        Type::Uint => {
            let v = r.read_u64()?;
            i64::try_from(v).map_or(Node::U64(v), Node::I64)
        }
        Type::Float => Node::F64(r.read_f64()?),
        Type::Str => Node::Str(read_string(r, budget)?),
        Type::Bin => {
            let bin = r.read_bin()?;
            budget.charge(bin.len() as u64)?;
            Node::Bin(src.slice_ref(bin))
        }
        Type::Array => {
            check_depth(depth)?;
            let n = r.read_array_len()?;
            check_count(r, "an array", n, n)?;
            let mut items = Vec::with_capacity(n);
            for _ in 0..n {
                items.push(read_node(r, src, depth + 1, budget)?);
            }
            Node::Array(items)
        }
        Type::Map => {
            check_depth(depth)?;
            let n = r.read_map_len()?;
            check_count(r, "a map", n, n.saturating_mul(2))?;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                if r.peek_type()? != Type::Str {
                    return Err(malformed(format!("a map key is a {}, not a str", r.peek_type()?)));
                }
                let key = read_string(r, budget)?;
                entries.push((key, read_node(r, src, depth + 1, budget)?));
            }
            Node::Map(Obj(entries))
        }
        Type::Ext => return Err(malformed("an ext value, which the grammar never holds")),
    })
}

fn read_string(r: &mut Reader<'_>, budget: &DecodeBudget) -> Result<String, CodecError> {
    let s = r.read_str()?;
    budget.charge((size_of::<String>() + s.len()) as u64)?;
    Ok(s.to_owned())
}

fn check_depth(depth: usize) -> Result<(), CodecError> {
    if depth > MAX_DEPTH {
        return Err(malformed(format!("nesting deeper than {MAX_DEPTH}")));
    }
    Ok(())
}

/// Refuses a declared count of `n` entries needing at least `least` bytes when fewer are left,
/// before a `Vec` of `n` is allocated.
fn check_count(r: &Reader<'_>, what: &str, n: usize, least: usize) -> Result<(), CodecError> {
    if least > r.remaining() {
        return Err(malformed(format!(
            "{what} declares {n} entries with {} bytes left",
            r.remaining()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack::Writer;
    use logit_core::Value;

    const T0: &str = "1970-01-01T00:00:00.000000000Z";

    /// An event map with `attributes` written by `attrs` (`n` entries) and an optional resource
    /// `host` attribute.
    fn event(w: &mut Writer, host: Option<&str>, n: usize, attrs: impl FnOnce(&mut Writer)) {
        w.write_map_len(2 + usize::from(host.is_some()));
        w.write_str("timestamp");
        w.write_str(T0);
        w.write_str("attributes");
        w.write_map_len(n);
        attrs(w);
        if let Some(host) = host {
            w.write_str("resource");
            w.write_map_len(1);
            w.write_str("attributes");
            w.write_map_len(1);
            w.write_str("host");
            w.write_str(host);
        }
    }

    fn decode(w: &Writer) -> Result<Vec<EventBatch>, CodecError> {
        MsgpackEventDecoder::new().decode_batches(&Bytes::copy_from_slice(w.as_slice()))
    }

    fn error(w: &Writer) -> String {
        decode(w).expect_err("refused").to_string()
    }

    #[test]
    fn value_forms_read_as_the_writer_writes_them() {
        let mut w = Writer::new();
        event(&mut w, None, 6, |w| {
            w.write_str("bin");
            w.write_bin(b"\xff\x00");
            w.write_str("nan");
            w.write_f64(f64::NAN);
            w.write_str("float");
            w.write_f64(200.0);
            w.write_str("int");
            w.write_i64(-3);
            w.write_str("big");
            w.write_u64(u64::MAX);
            w.write_str("nested");
            w.write_array_len(2);
            w.write_nil();
            w.write_map_len(1);
            w.write_str("k");
            w.write_bool(true);
        });
        let batches = decode(&w).unwrap();
        let a = &batches[0].events[0].attributes;
        assert_eq!(a.get("bin"), Some(&Value::Bytes(Bytes::from_static(b"\xff\x00"))));
        assert!(matches!(a.get("nan"), Some(Value::F64(f)) if f.is_nan()));
        assert_eq!(a.get("float"), Some(&Value::F64(200.0)));
        assert_eq!(a.get("int"), Some(&Value::I64(-3)));
        assert_eq!(a.get("big"), Some(&Value::U64(u64::MAX)));
        let Some(Value::Array(nested)) = a.get("nested") else { panic!("an array") };
        assert_eq!(nested[0], Value::Null);
        assert!(matches!(&nested[1], Value::Map(m) if m.get("k") == Some(&Value::Bool(true))));
    }

    #[test]
    fn maps_back_to_back_group_by_resource_and_share_its_arc() {
        let mut w = Writer::new();
        for host in ["a", "a", "b", "a"] {
            event(&mut w, Some(host), 0, |_| {});
        }
        let batches = decode(&w).unwrap();
        assert_eq!(batches.iter().map(|b| b.events.len()).collect::<Vec<_>>(), [2, 1, 1]);
        assert!(decode(&Writer::new()).unwrap().is_empty(), "empty input is no batch");
    }

    #[test]
    fn each_malformed_input_is_refused_with_its_own_reason() {
        let mut whole = Writer::new();
        event(&mut whole, None, 1, |w| {
            w.write_str("k");
            w.write_str("v");
        });
        let mut truncated = Writer::new();
        truncated.write_map_len(2);
        truncated.write_str("timestamp");
        truncated.write_str(T0);
        let mut wrong_type = Writer::new();
        wrong_type.write_map_len(1);
        wrong_type.write_str("timestamp");
        wrong_type.write_i64(0);
        let mut past_input = Writer::new();
        past_input.write_map_len(1);
        past_input.write_str("attributes");
        past_input.write_array_len(65_535);
        past_input.write_nil();
        let mut deep = Writer::new();
        deep.write_map_len(2);
        deep.write_str("timestamp");
        deep.write_str(T0);
        deep.write_str("attributes");
        deep.write_map_len(1);
        deep.write_str("k");
        for _ in 0..MAX_DEPTH {
            deep.write_array_len(1);
        }
        deep.write_nil();
        let mut not_a_map = Writer::new();
        not_a_map.write_array_len(0);
        let mut int_key = Writer::new();
        int_key.write_map_len(1);
        int_key.write_i64(1);
        int_key.write_nil();

        let reasons = [
            error(&truncated),
            error(&wrong_type),
            error(&past_input),
            error(&deep),
            error(&not_a_map),
            error(&int_key),
        ];
        let want = [
            "truncated msgpack input",
            "timestamp is not a string",
            "declares 65535 entries with 1 bytes left",
            "nesting deeper than 128",
            "a value is a array, not a map",
            "a map key is a uint, not a str",
        ];
        for (got, want) in reasons.iter().zip(want) {
            assert!(got.contains(want), "{got:?} lacks {want:?}");
        }
        // One level shallower is within the bound.
        let mut shallow = Writer::new();
        shallow.write_map_len(2);
        shallow.write_str("timestamp");
        shallow.write_str(T0);
        shallow.write_str("attributes");
        shallow.write_map_len(1);
        shallow.write_str("k");
        for _ in 0..MAX_DEPTH - 2 {
            shallow.write_array_len(1);
        }
        shallow.write_nil();
        assert!(decode(&shallow).is_ok());
        // A complete event after an incomplete one doesn't rescue it.
        let mut tail = truncated.as_slice().to_vec();
        tail.extend_from_slice(whole.as_slice());
        assert!(MsgpackEventDecoder::new().decode_batches(&Bytes::from(tail)).is_err());
    }

    #[test]
    fn a_tree_past_its_budget_is_refused() {
        let mut w = Writer::new();
        event(&mut w, None, 1, |w| {
            w.write_str("k");
            w.write_bin(&[0; 4096]);
        });
        let bytes = Bytes::copy_from_slice(w.as_slice());
        let got = MsgpackEventDecoder::new()
            .decode_batches_with_budget(&bytes, &DecodeBudget::new(4096))
            .expect_err("over the budget");
        assert!(matches!(got, CodecError::BudgetExceeded { limit: 4096 }), "{got}");
        assert!(MsgpackEventDecoder::new().decode_batches(&bytes).is_ok());
    }
}
