//! **Arm E: per-embedding capacity.** `AttrMap` is embedded in six places that have nothing to do
//! with an event's own attribute width, and each pays `Event`'s inline capacity whether or not it
//! can use it: `Resource`, `Scope`, `SeriesKey`, `SpanEvent`, `SpanLink`, and every boxed
//! `Value::Map`. This module builds the two cases the survey says are worth measuring.
//!
//! **(1) `Value::Map`.** A shipped nested map is `Value::Map(Box<AttrMap>)`: one heap allocation of
//! `size_of::<AttrMap>()`, the full eight inline slots, for a map the survey measures at a
//! median width of **3** (`docs/design/data-shapes.md` §5.3, pino-http). A `Vec`-backed sorted map
//! is 24 bytes, which fits inside `Value`'s existing 40 without the `Box` at all, and allocates
//! exactly the entries it holds. [`ThinValue`]/[`ThinMap`] are that representation; the `*_today`
//! side of each comparison is the real `logit_core` types, not a mirror of them.
//!
//! **(2) `Scope` and `Resource`.** A `Scope` carries a median of **0** attributes and a `Resource`
//! 0-6 outside a collector, 17 (p90 28, max 29) behind one (§3-§4). `Scope` is embedded in the
//! batch and `Resource` is `Arc`-shared across it, so the two are paid at different rates, and
//! the benches keep them apart.
//!
//! **What this mirror simplifies, and which way it cuts.**
//!
//! - [`ThinValue`] is a mirror of `Value`, not the real thing: ten variants with the same payloads
//!   in the same order, differing only in the `Map` arm. `tests/attr_arms.rs` pins
//!   `size_of::<ThinValue>() == size_of::<Value>()`, so the entry stride is identical and only
//!   where a nested map's storage lives changes. It can't reproduce the rest of `Value`'s
//!   obligations (`serde`, the Lua proxy, the wire codecs), which is the part a production change
//!   would have to pay for and this arm doesn't measure.
//! - `ThinMap` has no inline capacity, so it allocates on its **first** entry where an `AttrMap`
//!   does not. That is the trade: at a median nested width of 3 the allocation is 144 bytes against
//!   a 400-byte `Box`, a one-entry map pays an allocation the boxed `AttrMap` also pays, and a
//!   *zero*-entry map pays nothing where a shipped `Scope` pays 392 bytes of embedded footprint.
//! - The nested fixtures are built by hand at `docs/design/data-shapes.md`'s measured widths
//!   rather than parsed out of `fixtures::PINO_HTTP_LOG_BODY`, so no JSON parsing sits inside a
//!   timed region. The shape (10 top-level attributes, 4 maps, median width 3, depth 2) is the
//!   fixture's, checked against it in `tests/attr_arms.rs`.

use super::shapes;
use bytes::Bytes;
use logit_core::interner::{intern, Symbol};
use logit_core::{AttrMap, Value};

/// `Value` with one variant changed: a nested map is held inline as a [`ThinMap`] (24 bytes)
/// instead of `Box<AttrMap>` (8 bytes pointing at ~400). Every other variant is identical, so the
/// enum's size is unchanged.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum ThinValue {
    #[default]
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Bytes),
    Str(Bytes),
    Timestamp(i64),
    Array(Vec<ThinValue>),
    /// Unboxed, and exactly sized.
    Map(ThinMap),
}

/// A heap-only, exactly-sized sorted map: `AttrMap`'s semantics with none of its inline capacity.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ThinMap(Vec<(Symbol, ThinValue)>);

impl ThinMap {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Pre-sized, which the `Vec` backing allows and `AttrMap` doesn't expose.
    pub fn with_capacity(n: usize) -> Self {
        Self(Vec::with_capacity(n))
    }

    /// `AttrMap::insert_sym`'s semantics: sorted position, last write wins.
    pub fn insert_sym(&mut self, key: Symbol, value: ThinValue) {
        match self.0.binary_search_by_key(&key, |(k, _)| *k) {
            Ok(i) => self.0[i].1 = value,
            Err(i) => self.0.insert(i, (key, value)),
        }
    }

    pub fn get_sym(&self, key: Symbol) -> Option<&ThinValue> {
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| &self.0[i].1)
    }

    pub fn remove_sym(&mut self, key: Symbol) -> Option<ThinValue> {
        self.0.binary_search_by_key(&key, |(k, _)| *k).ok().map(|i| self.0.remove(i).1)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn iter(&self) -> impl Iterator<Item = (Symbol, &ThinValue)> {
        self.0.iter().map(|(k, v)| (*k, v))
    }
}

/// The thin mirror of [`shapes::value`], so both sides of every comparison carry the same value
/// mix.
pub fn thin_value(mix: shapes::Mix, i: usize) -> ThinValue {
    match shapes::value(mix, i) {
        Value::Str(b) => ThinValue::Str(b),
        Value::I64(v) => ThinValue::I64(v),
        Value::F64(v) => ThinValue::F64(v),
        Value::Bool(v) => ThinValue::Bool(v),
        Value::Timestamp(v) => ThinValue::Timestamp(v),
        other => unreachable!("shapes::value produces no {other:?}"),
    }
}

/// The thin mirror of [`shapes::attr_map`]: the same arrival-order scratch, built into a
/// pre-sized [`ThinMap`]. Keys are already interned and values already built, so a timed region
/// around this measures the map and nothing else.
pub fn thin_map(scratch: &[(Symbol, Value)]) -> ThinMap {
    let mut map = ThinMap::with_capacity(scratch.len());
    for (i, (key, _)) in scratch.iter().enumerate() {
        map.insert_sym(*key, thin_value_at(scratch, i));
    }
    map
}

/// The scratch's `i`th value, converted. Split out so [`thin_map`] reads as the mirror of
/// `shapes::attr_map`'s loop.
fn thin_value_at(scratch: &[(Symbol, Value)], i: usize) -> ThinValue {
    match &scratch[i].1 {
        Value::Str(b) => ThinValue::Str(b.clone()),
        Value::Bytes(b) => ThinValue::Bytes(b.clone()),
        Value::I64(v) => ThinValue::I64(*v),
        Value::U64(v) => ThinValue::U64(*v),
        Value::F64(v) => ThinValue::F64(*v),
        Value::Bool(v) => ThinValue::Bool(*v),
        Value::Timestamp(v) => ThinValue::Timestamp(*v),
        Value::Null => ThinValue::Null,
        other => unreachable!("the flat shapes carry no {other:?}"),
    }
}

/// The nested widths `docs/design/data-shapes.md` §5.3 measured for pino-http: four maps, median
/// width 3, depth 2.
pub const NESTED_WIDTH: usize = 3;

/// One nested group's interned keys: the attribute the map hangs off, the keys inside it, and, for
/// the two-level pino shape, a `headers` map inside that.
pub struct NestedGroup {
    pub key: Symbol,
    pub inner: Vec<Symbol>,
    pub headers_key: Option<Symbol>,
    pub headers: Vec<Symbol>,
}

/// Every key a nested fixture needs, interned once. Built outside the timed region, because
/// `intern` is a hash and a shard lock and this arm is not measuring the interner.
pub struct NestedKeys {
    pub top: Vec<Symbol>,
    pub groups: Vec<NestedGroup>,
}

/// The pino-http record's keys: eight scalars, two groups, each with a nested `headers` map.
pub fn pino_keys() -> NestedKeys {
    NestedKeys {
        top: (0..8).map(|i| intern(&format!("w3b.pino.top.{i:02}"))).collect(),
        groups: ["req", "res"]
            .into_iter()
            .map(|name| NestedGroup {
                key: intern(&format!("w3b.pino.{name}")),
                inner: (0..NESTED_WIDTH - 1)
                    .map(|i| intern(&format!("w3b.pino.{name}.{i:02}")))
                    .collect(),
                headers_key: Some(intern(&format!("w3b.pino.{name}.headers"))),
                headers: (0..NESTED_WIDTH)
                    .map(|i| intern(&format!("w3b.pino.{name}.hdr.{i:02}")))
                    .collect(),
            })
            .collect(),
    }
}

/// A synthetic record's keys: eight scalars and `maps` flat nested maps, so the per-map cost can be
/// read as a slope either side of the measured record.
pub fn synthetic_keys(maps: usize) -> NestedKeys {
    NestedKeys {
        top: (0..8).map(|i| intern(&format!("w3b.syn.top.{i:02}"))).collect(),
        groups: (0..maps)
            .map(|m| NestedGroup {
                key: intern(&format!("w3b.syn.map.{m}")),
                inner: (0..NESTED_WIDTH).map(|i| intern(&format!("w3b.syn.m{m}.{i:02}"))).collect(),
                headers_key: None,
                headers: Vec::new(),
            })
            .collect(),
    }
}

/// The nested record as shipped: every nested map a **boxed `AttrMap`**, a full-size heap
/// allocation however few entries it holds.
pub fn nested_today(keys: &NestedKeys, mix: shapes::Mix) -> AttrMap {
    let mut root = AttrMap::new();
    for (i, key) in keys.top.iter().enumerate() {
        root.insert_sym(*key, shapes::value(mix, i));
    }
    for (slot, group) in keys.groups.iter().enumerate() {
        let mut inner = AttrMap::new();
        for (i, key) in group.inner.iter().enumerate() {
            inner.insert_sym(*key, shapes::value(mix, i));
        }
        if let Some(headers_key) = group.headers_key {
            let mut headers = AttrMap::new();
            for (i, key) in group.headers.iter().enumerate() {
                headers.insert_sym(*key, shapes::value(mix, i + slot));
            }
            inner.insert_sym(headers_key, Value::Map(Box::new(headers)));
        }
        root.insert_sym(group.key, Value::Map(Box::new(inner)));
    }
    root
}

/// [`nested_today`]'s thin counterpart: the same shape with each nested map exactly sized and held
/// inline in its `ThinValue`.
pub fn nested_thin(keys: &NestedKeys, mix: shapes::Mix) -> ThinMap {
    let mut root = ThinMap::with_capacity(keys.top.len() + keys.groups.len());
    for (i, key) in keys.top.iter().enumerate() {
        root.insert_sym(*key, thin_value(mix, i));
    }
    for (slot, group) in keys.groups.iter().enumerate() {
        let mut inner =
            ThinMap::with_capacity(group.inner.len() + usize::from(group.headers_key.is_some()));
        for (i, key) in group.inner.iter().enumerate() {
            inner.insert_sym(*key, thin_value(mix, i));
        }
        if let Some(headers_key) = group.headers_key {
            let mut headers = ThinMap::with_capacity(group.headers.len());
            for (i, key) in group.headers.iter().enumerate() {
                headers.insert_sym(*key, thin_value(mix, i + slot));
            }
            inner.insert_sym(headers_key, ThinValue::Map(headers));
        }
        root.insert_sym(group.key, ThinValue::Map(inner));
    }
    root
}
