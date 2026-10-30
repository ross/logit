//! The parsed tree of one event object, which [`crate::event_json`] reads into an
//! [`logit_core::Event`]. Its two front ends build it: a JSON line through serde, and a
//! MessagePack map through [`crate::msgpack_event`]. A [`Node`] keeps the distinctions
//! MessagePack makes and JSON doesn't (a `bin`, a non-finite float), so the one reader serves
//! both grammars.

use std::fmt;

use bytes::Bytes;

use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};

/// One value of the tree.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Node {
    Null,
    Bool(bool),
    /// Any integer that fits an `i64`.
    I64(i64),
    /// An integer past `i64::MAX`.
    U64(u64),
    /// A float, non-finite included: JSON's numbers with a fraction or an exponent, and
    /// MessagePack's float32 and float64.
    F64(f64),
    Str(String),
    /// A MessagePack `bin`; JSON has none.
    Bin(Bytes),
    Array(Vec<Node>),
    Map(Obj),
}

/// An object's entries, sorted by key with one entry per key, the last value of a repeated key
/// kept: a `serde_json::Map`'s order and its duplicate rule. A nested map's key order reaches
/// the event's [`logit_core::Value::Map`], and from there a sink that renders it as text, so
/// both front ends build through [`Obj::new`] to read alike.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Obj(Vec<(String, Node)>);

impl Obj {
    pub(crate) fn new(mut entries: Vec<(String, Node)>) -> Self {
        // A stable sort keeps a repeated key's entries in input order, so the last one wins.
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut out: Vec<(String, Node)> = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            match out.last_mut() {
                Some(last) if last.0 == k => last.1 = v,
                _ => out.push((k, v)),
            }
        }
        Obj(out)
    }

    pub(crate) fn get(&self, key: &str) -> Option<&Node> {
        self.0.binary_search_by(|(k, _)| k.as_str().cmp(key)).ok().map(|i| &self.0[i].1)
    }
}

impl<'a> IntoIterator for &'a Obj {
    type Item = &'a (String, Node);
    type IntoIter = std::slice::Iter<'a, (String, Node)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl Node {
    pub(crate) fn as_object(&self) -> Option<&Obj> {
        match self {
            Node::Map(obj) => Some(obj),
            _ => None,
        }
    }

    pub(crate) fn as_array(&self) -> Option<&Vec<Node>> {
        match self {
            Node::Array(items) => Some(items),
            _ => None,
        }
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Node::Str(s) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self {
            Node::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub(crate) fn as_u64(&self) -> Option<u64> {
        match self {
            Node::I64(n) => u64::try_from(*n).ok(),
            Node::U64(n) => Some(*n),
            _ => None,
        }
    }

    pub(crate) fn as_i64(&self) -> Option<i64> {
        match self {
            Node::I64(n) => Some(*n),
            _ => None,
        }
    }

    /// Any number as an `f64`.
    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self {
            Node::I64(n) => Some(*n as f64),
            Node::U64(n) => Some(*n as f64),
            Node::F64(f) => Some(*f),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(NodeVisitor)
    }
}

struct NodeVisitor;

impl<'de> Visitor<'de> for NodeVisitor {
    type Value = Node;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Node, E> {
        Ok(Node::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Node, E> {
        Ok(Node::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Node, E> {
        Ok(Node::I64(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Node, E> {
        Ok(i64::try_from(v).map_or(Node::U64(v), Node::I64))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Node, E> {
        Ok(Node::F64(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Node, E> {
        Ok(Node::Str(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> Result<Node, E> {
        Ok(Node::Str(v))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Node, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element()? {
            items.push(item);
        }
        Ok(Node::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Node, A::Error> {
        let mut entries = Vec::new();
        while let Some(entry) = map.next_entry::<String, Node>()? {
            entries.push(entry);
        }
        Ok(Node::Map(Obj::new(entries)))
    }
}
