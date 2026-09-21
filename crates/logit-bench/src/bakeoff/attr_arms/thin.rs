//! **Arm E -- per-embedding capacity.** `AttrMap` is embedded in six places that have nothing to do
//! with an event's own attribute width, and each pays `Event`'s inline capacity whether or not it
//! can use it: `Resource`, `Scope`, `SeriesKey`, `SpanEvent`, `SpanLink`, and every boxed
//! `Value::Map`. This module builds the two cases the survey says are worth measuring.
//!
//! **(1) `Value::Map`.** Today a nested map is `Value::Map(Box<AttrMap>)`: one heap allocation of
//! `size_of::<AttrMap>()` -- the full eight inline slots -- for a map the survey measures at a
//! median width of **3** (`docs/design/data-shapes.md` §5.3, pino-http). A `Vec`-backed sorted map
//! is 24 bytes, which fits inside `Value`'s existing 40 without the `Box` at all, and allocates
//! exactly the entries it holds. [`ThinValue`]/[`ThinMap`] are that representation; the "today"
//! side of the comparison is the real `logit_core` types, not a mirror of them.
//!
//! **(2) `Scope` and `Resource`.** A `Scope` carries a median of **0** attributes and a `Resource`
//! 0-6 outside a collector, 17 (p90 28, max 29) behind one (§3-§4). `Scope` is embedded in the
//! batch, `Resource` is `Arc`-shared across it -- so the two are paid at completely different
//! rates, and the benches keep them apart.
//!
//! **What this mirror simplifies, and which way it cuts.**
//!
//! - [`ThinValue`] is a mirror of `Value`, not the real thing: ten variants with the same payloads
//!   in the same order, differing only in the `Map` arm. `tests/attr_arms.rs` pins
//!   `size_of::<ThinValue>() == size_of::<Value>()`, so the entry stride is identical and the only
//!   thing that changes is where a nested map's storage lives. What it cannot reproduce is the
//!   rest of `Value`'s real obligations (`serde`, the Lua proxy, the wire codecs), which is
//!   precisely the part a production change would have to pay for and this arm does not measure.
//! - `ThinMap` has no inline capacity at all, so it allocates on its **first** entry where an
//!   `AttrMap` does not. That is the trade, not an oversight: at a median nested width of 3 the
//!   allocation is 144 bytes against a 400-byte `Box`, but a one-entry map pays an allocation the
//!   boxed `AttrMap` also pays, and a *zero*-entry map pays nothing where today's `Scope` pays 392
//!   bytes of embedded footprint.
//! - The nested fixtures here are built by hand at `docs/design/data-shapes.md`'s measured widths
//!   rather than parsed out of `fixtures::PINO_HTTP_LOG_BODY`, so no JSON parsing sits inside a
//!   timed region. The shape (10 top-level attributes, 4 maps, median width 3, depth 2) is the
//!   fixture's, checked against it in `tests/attr_arms.rs`.

use super::shapes;
use bytes::Bytes;
use logit_core::interner::{intern, Symbol};
use logit_core::{AttrMap, Value};

/// `Value` with one variant changed: a nested map is held inline as a [`ThinMap`] (24 bytes)
/// instead of `Box<AttrMap>` (8 bytes pointing at ~400). Every other variant is identical, so the
/// enum's size is unchanged -- see this module's doc.
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

    /// Pre-sized, which the `Vec` backing makes possible and `AttrMap` today does not expose at
    /// all (`docs/plans/event-sizing.md`'s "what nobody can do today").
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

/// A flat map of `width` entries under `prefix`, built the way one is built today.
pub fn flat_today(prefix: &str, width: usize, mix: shapes::Mix) -> AttrMap {
    let scratch = shapes::scratch(prefix, width, mix);
    let mut map = AttrMap::new();
    for (k, v) in &scratch {
        map.insert_sym(*k, v.clone());
    }
    map
}

/// [`flat_today`]'s thin counterpart, pre-sized (which is half of what the representation buys).
pub fn flat_thin(prefix: &str, width: usize, mix: shapes::Mix) -> ThinMap {
    let scratch = shapes::scratch(prefix, width, mix);
    let mut map = ThinMap::with_capacity(scratch.len());
    for (i, (k, _)) in scratch.iter().enumerate() {
        map.insert_sym(*k, thin_value(mix, i));
    }
    map
}

/// The nested widths `docs/design/data-shapes.md` §5.3 measured for pino-http: four maps, median
/// width 3, depth 2.
pub const NESTED_WIDTH: usize = 3;

/// The pino-http record's post-`json` shape as it exists today: ten top-level attributes, two of
/// which are `Value::Map`, each of those carrying a nested `headers` map -- **four boxed
/// `AttrMap`s**, each one a full-size heap allocation for three entries.
pub fn nested_today(mix: shapes::Mix) -> AttrMap {
    let mut root = AttrMap::new();
    for i in 0..8 {
        root.insert(&format!("w3b.pino.top.{i:02}"), shapes::value(mix, i));
    }
    for (slot, name) in ["req", "res"].into_iter().enumerate() {
        let mut inner = AttrMap::new();
        for i in 0..NESTED_WIDTH - 1 {
            inner.insert(&format!("w3b.pino.{name}.{i:02}"), shapes::value(mix, i));
        }
        let mut headers = AttrMap::new();
        for i in 0..NESTED_WIDTH {
            headers.insert(&format!("w3b.pino.{name}.hdr.{i:02}"), shapes::value(mix, i + slot));
        }
        inner.insert(&format!("w3b.pino.{name}.headers"), Value::Map(Box::new(headers)));
        root.insert(&format!("w3b.pino.{name}"), Value::Map(Box::new(inner)));
    }
    root
}

/// [`nested_today`]'s thin counterpart: the same ten attributes and the same four maps, each map
/// exactly sized and held inline in its `ThinValue`.
pub fn nested_thin(mix: shapes::Mix) -> ThinMap {
    let mut root = ThinMap::with_capacity(10);
    for i in 0..8 {
        root.insert_sym(intern(&format!("w3b.pino.top.{i:02}")), thin_value(mix, i));
    }
    for (slot, name) in ["req", "res"].into_iter().enumerate() {
        let mut inner = ThinMap::with_capacity(NESTED_WIDTH);
        for i in 0..NESTED_WIDTH - 1 {
            inner.insert_sym(intern(&format!("w3b.pino.{name}.{i:02}")), thin_value(mix, i));
        }
        let mut headers = ThinMap::with_capacity(NESTED_WIDTH);
        for i in 0..NESTED_WIDTH {
            headers.insert_sym(
                intern(&format!("w3b.pino.{name}.hdr.{i:02}")),
                thin_value(mix, i + slot),
            );
        }
        inner.insert_sym(intern(&format!("w3b.pino.{name}.headers")), ThinValue::Map(headers));
        root.insert_sym(intern(&format!("w3b.pino.{name}")), ThinValue::Map(inner));
    }
    root
}

/// A synthetic record with `maps` nested maps of [`NESTED_WIDTH`] entries among eight scalars --
/// the 1-map and 4-map points either side of the measured record, so the per-map cost is a slope
/// rather than one number.
pub fn synthetic_today(maps: usize, mix: shapes::Mix) -> AttrMap {
    let mut root = AttrMap::new();
    for i in 0..8 {
        root.insert(&format!("w3b.syn.top.{i:02}"), shapes::value(mix, i));
    }
    for m in 0..maps {
        let mut inner = AttrMap::new();
        for i in 0..NESTED_WIDTH {
            inner.insert(&format!("w3b.syn.m{m}.{i:02}"), shapes::value(mix, i));
        }
        root.insert(&format!("w3b.syn.map.{m}"), Value::Map(Box::new(inner)));
    }
    root
}

/// [`synthetic_today`]'s thin counterpart.
pub fn synthetic_thin(maps: usize, mix: shapes::Mix) -> ThinMap {
    let mut root = ThinMap::with_capacity(8 + maps);
    for i in 0..8 {
        root.insert_sym(intern(&format!("w3b.syn.top.{i:02}")), thin_value(mix, i));
    }
    for m in 0..maps {
        let mut inner = ThinMap::with_capacity(NESTED_WIDTH);
        for i in 0..NESTED_WIDTH {
            inner.insert_sym(intern(&format!("w3b.syn.m{m}.{i:02}")), thin_value(mix, i));
        }
        root.insert_sym(intern(&format!("w3b.syn.map.{m}")), ThinValue::Map(inner));
    }
    root
}
