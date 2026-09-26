//! Property tests for `aggregate` against a reference written independently of it, for cluster 8
//! of `docs/plans/critical-sections-inventory.md`. The contract they check is
//! `docs/adr/aggregation-window-semantics.md`'s "Amendment: series identity, merge laws, and
//! accounting as a stated contract" section.
//!
//! - XFORM-01, series identity: `SeriesKey` equality and hashing agree with [`ref_eq`], a
//!   structural comparison that never calls `value_key_eq` or `hash_value`, and `Aggregator`
//!   partitions records into series and `(resource, scope)` groups the way that reference does.
//! - XFORM-02, merge dispatch: [`Model`] restates `process` and a tumbling `flush`, and
//!   `aggregator_matches_the_reference_model` checks every emitted series, forwarded record, link,
//!   and counter against it over random batches and flushes. Random ops rarely put more than
//!   eight contexts on one series, so `the_model_caps_links_per_series` drives the link cap
//!   directly. The merge-law properties check per-window order independence and a two-stage
//!   relay, and the unit tests at the end pin the outcomes the ADR lists as order-dependent by
//!   design.
//! - XFORM-03 and XFORM-04, retention: [`Model::flush`] keeps a retainable series across the
//!   flush, evicts it after `series_retention` idle flushes or by the global `max_retained_series`
//!   cap (most idle first, then newest), and clamps its start time into its window. The model
//!   checks every survivor's state and the series accounting identity. `aggregate`'s own tests
//!   drive the cap at volume (`cap_soak`).
//!
//! Case counts are floors: a `PROPTEST_CASES` above one raises it for a deeper run.

use super::*;
use logit_core::interner::{intern, resolve};
use proptest::prelude::*;
use std::collections::hash_map::DefaultHasher;

/// `cases`, or `PROPTEST_CASES` when that is larger.
fn config(cases: u32) -> ProptestConfig {
    let default = ProptestConfig::default();
    ProptestConfig { cases: cases.max(default.cases), ..default }
}

// -- Strategies -------------------------------------------------------------------------------

/// A quiet `NaN` with the sign bit set and a payload other than `f64::NAN`'s.
const OTHER_NAN: u64 = 0xfff8_0000_0000_0001;

fn float() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.0),
        Just(-0.0),
        Just(1.0),
        Just(f64::NAN),
        Just(f64::from_bits(OTHER_NAN)),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(f64::from_bits(1)),
    ]
}

fn text() -> impl Strategy<Value = &'static [u8]> {
    prop_oneof![Just(&b""[..]), Just(&b"a"[..]), Just(&b"b"[..])]
}

fn leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        prop_oneof![Just(0i64), Just(1), Just(-1), any::<i64>()].prop_map(Value::I64),
        prop_oneof![Just(0u64), Just(1), any::<u64>()].prop_map(Value::U64),
        float().prop_map(Value::F64),
        text().prop_map(|t| Value::Str(Bytes::from_static(t))),
        text().prop_map(|t| Value::Bytes(Bytes::from_static(t))),
        prop_oneof![Just(0i64), Just(1)].prop_map(Value::Timestamp),
    ]
}

fn map_of(pairs: Vec<(&'static str, Value)>) -> Value {
    let mut map = AttrMap::new();
    for (k, v) in pairs {
        map.insert(k, v);
    }
    Value::Map(Box::new(map))
}

/// A leaf, or an `Array`/`Map` of up to three, two levels deep, empties included. `Map` keys come
/// from `{x, y}`, so two generated maps share keys often.
fn value() -> impl Strategy<Value = Value> {
    leaf().prop_recursive(2, 16, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..=3).prop_map(Value::Array),
            prop::collection::vec((prop_oneof![Just("x"), Just("y")], inner), 0..=3)
                .prop_map(map_of),
        ]
    })
}

const KEYS: [&str; 6] = ["k0", "k1", "k2", "k3", "k4", "k5"];
const HALF: usize = 4;
const POOL: usize = 2 * HALF;

/// Values an attribute set draws from: four generated values, then a twin of each at index
/// `i + HALF`. A twin is a separately built structural copy or a near miss, so pool entries sit on
/// both sides of the equality boundary.
fn pool() -> impl Strategy<Value = Vec<Value>> {
    (prop::collection::vec(value(), HALF), prop::collection::vec(any::<bool>(), HALF)).prop_map(
        |(mut values, near)| {
            let twins: Vec<Value> = values
                .iter()
                .zip(near)
                .map(|(v, near)| if near { near_miss(v) } else { copy(v) })
                .collect();
            values.extend(twins);
            values
        },
    )
}

/// A structural copy that shares no allocation with `v`.
fn copy(v: &Value) -> Value {
    match v {
        Value::Str(b) => Value::Str(Bytes::copy_from_slice(b)),
        Value::Bytes(b) => Value::Bytes(Bytes::copy_from_slice(b)),
        Value::Array(items) => Value::Array(items.iter().map(copy).collect()),
        Value::Map(map) => {
            let mut out = AttrMap::new();
            for (k, v) in map.iter() {
                out.insert_sym(k, copy(v));
            }
            Value::Map(Box::new(out))
        }
        other => other.clone(),
    }
}

/// A value that IEEE `==`, a numeric cast, or a byte compare could mistake for `v`, and that
/// series identity keeps apart.
fn near_miss(v: &Value) -> Value {
    match v {
        Value::Null => Value::Array(vec![]),
        Value::Bool(b) => Value::Bool(!b),
        Value::I64(n) => Value::U64(*n as u64),
        Value::U64(n) => Value::F64(*n as f64),
        Value::F64(x) => Value::F64(-x),
        Value::Str(b) => Value::Bytes(b.clone()),
        Value::Bytes(b) => Value::Str(b.clone()),
        Value::Timestamp(t) => Value::I64(*t),
        Value::Array(items) if items.len() >= 2 => {
            Value::Array(items.iter().rev().map(copy).collect())
        }
        Value::Array(items) => match items.first() {
            Some(first) => Value::Array(vec![near_miss(first)]),
            None => map_of(vec![]),
        },
        Value::Map(map) => {
            let mut out = AttrMap::new();
            for (i, (k, v)) in map.iter().enumerate() {
                out.insert_sym(k, if i == 0 { near_miss(v) } else { copy(v) });
            }
            if map.is_empty() {
                Value::Array(vec![])
            } else {
                Value::Map(Box::new(out))
            }
        }
    }
}

/// Up to three distinct keys, each paired with a pool index, in a shuffled insertion order.
fn attr_spec() -> impl Strategy<Value = Vec<(&'static str, usize)>> {
    prop::sample::subsequence(KEYS.to_vec(), 0..=3)
        .prop_flat_map(|keys| {
            let n = keys.len();
            (Just(keys), prop::collection::vec(0..POOL, n))
        })
        .prop_map(|(keys, idx)| keys.into_iter().zip(idx).collect::<Vec<_>>())
        .prop_shuffle()
}

#[derive(Debug, Clone)]
struct KeySpec {
    name: &'static str,
    unit: Option<&'static str>,
    attrs: Vec<(&'static str, usize)>,
}

fn key_spec() -> impl Strategy<Value = KeySpec> {
    (prop_oneof![Just("m0"), Just("m1")], prop_oneof![Just(None), Just(Some("ms"))], attr_spec())
        .prop_map(|(name, unit, attrs)| KeySpec { name, unit, attrs })
}

fn attr_map(attrs: &[(&'static str, usize)], pool: &[Value]) -> AttrMap {
    let mut map = AttrMap::new();
    for (k, i) in attrs {
        map.insert(k, pool[*i].clone());
    }
    map
}

fn series_key(spec: &KeySpec, pool: &[Value]) -> SeriesKey {
    SeriesKey {
        name: intern(spec.name),
        unit: spec.unit.map(intern),
        attributes: attr_map(&spec.attrs, pool),
    }
}

/// A permutation of `0..3`, for reordering an attribute spec of up to three entries.
fn permutation() -> impl Strategy<Value = Vec<usize>> {
    Just(vec![0, 1, 2]).prop_shuffle()
}

/// `attrs` in the order `perm` ranks their positions.
fn reorder(attrs: &[(&'static str, usize)], perm: &[usize]) -> Vec<(&'static str, usize)> {
    let mut ranked: Vec<(usize, (&'static str, usize))> =
        attrs.iter().enumerate().map(|(i, entry)| (perm[i], *entry)).collect();
    ranked.sort_by_key(|(rank, _)| *rank);
    ranked.into_iter().map(|(_, entry)| entry).collect()
}

fn hash_of(key: &SeriesKey) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

// -- Reference --------------------------------------------------------------------------------

/// Structural equality under the ADR's series-identity rules, written without `value_key_eq`:
/// variants exact, `F64` by bit pattern, `Array` in order, `Map` as a list sorted by key string.
fn ref_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::I64(a), Value::I64(b)) => a == b,
        (Value::U64(a), Value::U64(b)) => a == b,
        (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
        (Value::Str(a), Value::Str(b)) | (Value::Bytes(a), Value::Bytes(b)) => a[..] == b[..],
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| ref_eq(a, b))
        }
        (Value::Map(a), Value::Map(b)) => ref_attrs_eq(&sorted(a), &sorted(b)),
        _ => false,
    }
}

fn sorted(map: &AttrMap) -> Vec<(String, Value)> {
    let mut pairs: Vec<(String, Value)> =
        map.iter().map(|(k, v)| (resolve(k).to_string(), v.clone())).collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}

fn ref_attrs_eq(a: &[(String, Value)], b: &[(String, Value)]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|((ka, va), (kb, vb))| ka == kb && ref_eq(va, vb))
}

/// The reference's series key: strings, and attributes sorted by key string.
#[derive(Debug, Clone)]
struct RefKey {
    name: String,
    unit: Option<String>,
    attrs: Vec<(String, Value)>,
}

impl RefKey {
    fn of(name: Symbol, unit: Option<Symbol>, attributes: &AttrMap) -> Self {
        RefKey {
            name: resolve(name).to_string(),
            unit: unit.map(|u| resolve(u).to_string()),
            attrs: sorted(attributes),
        }
    }

    fn of_spec(spec: &KeySpec, pool: &[Value]) -> Self {
        RefKey::of(intern(spec.name), spec.unit.map(intern), &attr_map(&spec.attrs, pool))
    }
}

fn ref_key_eq(a: &RefKey, b: &RefKey) -> bool {
    a.name == b.name && a.unit == b.unit && ref_attrs_eq(&a.attrs, &b.attrs)
}

// -- Near misses ------------------------------------------------------------------------------

/// Pairs the ADR names as distinct although IEEE `==`, a numeric cast, or a byte compare would
/// call them equal.
fn near_misses() -> Vec<(Value, Value)> {
    let str_ = |s: &'static [u8]| Value::Str(Bytes::from_static(s));
    let bytes = |s: &'static [u8]| Value::Bytes(Bytes::from_static(s));
    vec![
        (Value::I64(1), Value::U64(1)),
        (Value::I64(1), Value::F64(1.0)),
        (Value::U64(1), Value::F64(1.0)),
        (Value::I64(0), Value::U64(0)),
        (Value::I64(1), Value::Timestamp(1)),
        (str_(b"a"), bytes(b"a")),
        (str_(b""), bytes(b"")),
        (Value::F64(0.0), Value::F64(-0.0)),
        (Value::F64(f64::NAN), Value::F64(f64::from_bits(OTHER_NAN))),
        (Value::F64(f64::NAN), Value::F64(-f64::NAN)),
        (Value::Array(vec![]), map_of(vec![])),
        (Value::Null, Value::Array(vec![])),
        (
            Value::Array(vec![Value::I64(1), Value::I64(2)]),
            Value::Array(vec![Value::I64(2), Value::I64(1)]),
        ),
    ]
}

/// A near miss placed bare, inside a one-element `Array`, or under a `Map` key.
fn wrap(value: Value, how: u8) -> Value {
    match how {
        0 => value,
        1 => Value::Array(vec![value]),
        _ => map_of(vec![("x", value)]),
    }
}

// -- Properties -------------------------------------------------------------------------------

proptest! {
    #![proptest_config(config(512))]

    /// `SeriesKey`'s `==` is the reference's, and equal keys hash equal. A key rebuilt from its
    /// own spec is equal to itself, `NaN` attributes included.
    #[test]
    fn series_key_equality_matches_the_structural_reference(
        pool in pool(),
        a in key_spec(),
        independent in key_spec(),
        mode in 0u8..3,
        perm in permutation(),
        twin in prop::collection::vec(any::<bool>(), 3),
    ) {
        // `b` is `a` reordered, `a` reordered with some values swapped for their pool twins, or
        // independent: equal keys and near misses are both common.
        let b = match mode {
            0 => KeySpec { attrs: reorder(&a.attrs, &perm), ..a.clone() },
            1 => {
                let swapped: Vec<(&'static str, usize)> = a
                    .attrs
                    .iter()
                    .zip(&twin)
                    .map(|(&(k, i), &t)| (k, if t { (i + HALF) % POOL } else { i }))
                    .collect();
                KeySpec { attrs: reorder(&swapped, &perm), ..a.clone() }
            }
            _ => independent,
        };

        let (key_a, key_b) = (series_key(&a, &pool), series_key(&b, &pool));
        let expected = ref_key_eq(&RefKey::of_spec(&a, &pool), &RefKey::of_spec(&b, &pool));
        prop_assert_eq!(key_a == key_b, expected, "a = {:?}, b = {:?}, pool = {:?}", a, b, pool);
        if expected {
            prop_assert_eq!(hash_of(&key_a), hash_of(&key_b));
        }

        let rebuilt = series_key(&a, &pool);
        prop_assert!(key_a == rebuilt, "a key must equal itself rebuilt: {:?}", a);
        prop_assert_eq!(hash_of(&key_a), hash_of(&rebuilt));
    }

    /// The same attribute set inserted in two orders is one key with one hash.
    #[test]
    fn insertion_order_changes_neither_equality_nor_hash(
        pool in pool(),
        spec in key_spec(),
        perm in permutation(),
    ) {
        let other = KeySpec { attrs: reorder(&spec.attrs, &perm), ..spec.clone() };

        let (a, b) = (series_key(&spec, &pool), series_key(&other, &pool));
        prop_assert!(a == b, "{:?} vs {:?}", spec, other);
        prop_assert_eq!(hash_of(&a), hash_of(&b));
    }

    /// Two otherwise equal keys that differ only in numeric variant, `Str` against `Bytes`, the
    /// sign of zero, a `NaN` payload, or `Array` order are two series.
    #[test]
    fn near_miss_values_are_distinct_series(
        pool in pool(),
        spec in key_spec(),
        which in 0..near_misses().len(),
        how in 0u8..3,
        swap in any::<bool>(),
        slot in prop::sample::select(KEYS.to_vec()),
    ) {
        let (mut left, mut right) = near_misses().swap_remove(which);
        if swap {
            std::mem::swap(&mut left, &mut right);
        }
        let mut a = series_key(&spec, &pool);
        let mut b = series_key(&spec, &pool);
        a.attributes.insert(slot, wrap(left.clone(), how));
        b.attributes.insert(slot, wrap(right.clone(), how));

        prop_assert!(a != b, "{:?} and {:?} (wrap {}) must be distinct", left, right, how);
        let (ra, rb) = (
            RefKey::of(a.name, a.unit, &a.attributes),
            RefKey::of(b.name, b.unit, &b.attributes),
        );
        prop_assert!(!ref_key_eq(&ra, &rb), "the reference agrees they're distinct");
    }

    /// Gauges fed through `Aggregator` flush as the reference's partition: one series per class
    /// of `ref_key_eq`-equal keys, carrying the class's attributes and the value of its latest
    /// record.
    #[test]
    fn aggregator_series_partition_matches_the_reference(
        pool in pool(),
        specs in prop::collection::vec(key_spec(), 0..40),
    ) {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = Arc::new(Resource::default());

        // Each class is its first record's key and the index of its latest record.
        let mut classes: Vec<(RefKey, usize)> = Vec::new();
        for (i, spec) in specs.iter().enumerate() {
            let mut record = MetricRecord::new(intern(spec.name), MetricKind::Gauge(i as f64));
            record.unit = spec.unit.map(intern);
            let mut event = Event::metric(i as i64, attr_map(&spec.attrs, &pool), record);
            prop_assert!(!agg.process(&resource, &mut event), "a gauge-only event is absorbed");

            let key = RefKey::of_spec(spec, &pool);
            match classes.iter_mut().find(|(k, _)| ref_key_eq(k, &key)) {
                Some((_, latest)) => *latest = i,
                None => classes.push((key, i)),
            }
        }

        let flushed: Vec<Event> = agg
            .flush(1_000)
            .into_iter()
            .flat_map(|(_, _, events)| events.into_iter().map(|(event, _)| event))
            .collect();
        prop_assert_eq!(flushed.len(), classes.len());

        let mut matched = vec![false; classes.len()];
        for event in &flushed {
            prop_assert_eq!(event.metrics.len(), 1);
            let record = &event.metrics[0];
            let emitted = RefKey::of(record.name, record.unit, &event.attributes);
            let found = classes.iter().position(|(k, _)| ref_key_eq(k, &emitted));
            prop_assert!(found.is_some(), "emitted series {:?} is no reference class", emitted);
            let class = found.unwrap_or_default();
            prop_assert!(!matched[class], "two emitted series for class {:?}", emitted);
            matched[class] = true;
            match record.kind {
                MetricKind::Gauge(v) => prop_assert_eq!(v, classes[class].1 as f64),
                ref other => prop_assert!(false, "expected a Gauge, got {:?}", other),
            }
        }
    }

    /// `groups` holds one entry per distinct `(resource, scope)` class the window has seen, where
    /// a class is equal content with bitwise floats, whichever `Arc` carries it.
    #[test]
    fn groups_match_the_bitwise_resource_and_scope_classes(
        ops in prop::collection::vec(
            prop_oneof![
                4 => (0..RESOURCES, 0..SCOPES, 1..=3usize).prop_map(|(r, s, n)| Op::Batch(r, s, n)),
                1 => Just(Op::Flush),
            ],
            1..=30,
        ),
    ) {
        let (resources, resource_class) = resource_pool();
        let (scopes, scope_class) = scope_pool();
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let mut seen: Vec<(usize, usize)> = Vec::new();
        let mut now = 0;

        for op in ops {
            match op {
                Op::Batch(r, s, n) => {
                    agg.observe_scope(scopes[s].clone());
                    for _ in 0..n {
                        let mut event = Event::metric(
                            now,
                            AttrMap::new(),
                            MetricRecord::new(intern("hits"), MetricKind::counter(1.0)),
                        );
                        prop_assert!(!agg.process(&resources[r], &mut event));
                    }
                    let class = (resource_class[r], scope_class[s]);
                    if !seen.contains(&class) {
                        seen.push(class);
                    }
                    prop_assert_eq!(agg.groups.len(), seen.len(), "classes seen: {:?}", seen);
                }
                Op::Flush => {
                    now += 10;
                    prop_assert_eq!(agg.flush(now).len(), seen.len());
                    // A delta counter doesn't survive the flush, so neither does its group.
                    prop_assert_eq!(agg.groups.len(), 0);
                    seen.clear();
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Op {
    Batch(usize, usize, usize),
    Flush,
}

const RESOURCES: usize = 7;
const SCOPES: usize = 4;

fn resource_with(key: &str, value: Value) -> Resource {
    let mut attributes = AttrMap::new();
    attributes.insert(key, value);
    Resource { attributes, ..Resource::default() }
}

/// Resources and their classes, assigned by hand: two `Arc`s of the default resource; `-0.0` and
/// `0.0` apart; one `NaN` resource through a shared `Arc` and through a separate equal one.
fn resource_pool() -> (Vec<Arc<Resource>>, [usize; RESOURCES]) {
    let nan = Arc::new(resource_with("x", Value::F64(f64::NAN)));
    let resources = vec![
        Arc::new(Resource::default()),
        Arc::new(Resource::default()),
        Arc::new(resource_with("host", Value::F64(-0.0))),
        Arc::new(resource_with("host", Value::F64(0.0))),
        nan.clone(),
        nan,
        Arc::new(resource_with("x", Value::F64(f64::NAN))),
    ];
    (resources, [0, 0, 1, 2, 3, 3, 3])
}

/// Scopes and their classes: none; `A` and an equal `A′` in its own `Arc`; `B`.
fn scope_pool() -> (Vec<Option<Arc<Scope>>>, [usize; SCOPES]) {
    let scope = |name: &'static [u8]| {
        let mut attributes = AttrMap::new();
        attributes.insert("x", Value::F64(f64::NAN));
        Some(Arc::new(Scope { name: Bytes::from_static(name), attributes, ..Scope::default() }))
    };
    (vec![None, scope(b"a"), scope(b"a"), scope(b"b")], [0, 1, 1, 2])
}

// -- Reference model (XFORM-02) ---------------------------------------------------------------
//
// `Model` restates `process` and a tumbling `flush` from the ADR, independently of `Aggregator`:
// its own pass-through table, merge rules, sample weighting, and counters. Its accumulators hold
// what a merge must preserve (observations, members, `u128` bucket totals) rather than sketches,
// so an emitted value is checked against the ADR's per-kind equality, never against the
// aggregator's bytes.

/// Members `Set` and `SetMembers` records draw from.
const ALPHABET: [&[u8]; 10] = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i", b"j"];

const LAYOUTS: [&[f64]; 3] =
    [&[1.0, 10.0, f64::INFINITY], &[5.0, f64::INFINITY], &[f64::NAN, f64::INFINITY]];

/// A record's kind, keeping what a sketch or `HyperLogLog` would hide from the model.
#[derive(Debug, Clone)]
enum KindSpec {
    Plain(MetricKind),
    /// A `Distribution` built as `Samples::sketch()` of these samples.
    Dist(Samples),
    /// A `Set` of these members.
    Set(Vec<Bytes>),
}

impl KindSpec {
    fn build(&self) -> MetricKind {
        match self {
            KindSpec::Plain(kind) => kind.clone(),
            KindSpec::Dist(samples) => MetricKind::Distribution(samples.sketch()),
            KindSpec::Set(members) => MetricKind::Set(hll_of(members)),
        }
    }
}

fn hll_of<'a>(members: impl IntoIterator<Item = &'a Bytes>) -> logit_core::HyperLogLog {
    let mut hll = logit_core::HyperLogLog::new();
    for m in members {
        hll.insert(m);
    }
    hll
}

#[derive(Debug, Clone)]
struct RecordSpec {
    name: &'static str,
    unit: Option<&'static str>,
    description: Option<&'static str>,
    kind: KindSpec,
    no_recorded_value: bool,
}

impl RecordSpec {
    fn build(&self) -> MetricRecord {
        let mut record = MetricRecord::new(intern(self.name), self.kind.build());
        record.unit = self.unit.map(intern);
        record.description = self.description.map(intern);
        if self.no_recorded_value {
            record.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        }
        record
    }
}

/// Bounded values with a rare `NaN`, infinity, or `-0.0`.
fn scalar() -> impl Strategy<Value = f64> {
    prop_oneof![
        12 => -1e3..1e3f64,
        1 => Just(f64::NAN),
        1 => Just(f64::INFINITY),
        1 => Just(-0.0),
    ]
}

/// `{0} ∪ ±[1e-6, 1e9]`, the range whose agent-mapping bins stay under the sketch's bin limit,
/// with a rare non-finite value a sketch drops.
fn sample_value() -> impl Strategy<Value = f64> {
    prop_oneof![
        2 => Just(0.0),
        6 => 1e-6..1e9f64,
        6 => -1e9..-1e-6f64,
        1 => Just(f64::NAN),
        1 => Just(f64::NEG_INFINITY),
    ]
}

/// Rates around `1 / Samples::MAX_WEIGHT`, plus the ones `weight` degrades to 1.
fn rate() -> impl Strategy<Value = f64> {
    prop::sample::select(vec![
        1.0,
        0.5,
        0.1,
        0.001,
        0.00099,
        0.0010005,
        0.0011,
        1e-4,
        0.0,
        -1.0,
        f64::NAN,
    ])
}

fn samples() -> impl Strategy<Value = Samples> {
    (prop::collection::vec(sample_value(), 0..=12), rate())
        .prop_map(|(values, sample_rate)| Samples { values: values.into(), sample_rate })
}

fn members() -> impl Strategy<Value = Vec<Bytes>> {
    prop::collection::vec(prop::sample::select(ALPHABET.to_vec()), 0..6)
        .prop_map(|m| m.into_iter().map(Bytes::from_static).collect())
}

fn temporality() -> impl Strategy<Value = Temporality> {
    prop_oneof![Just(Temporality::Delta), Just(Temporality::Cumulative)]
}

fn stat() -> impl Strategy<Value = Option<f64>> {
    prop_oneof![3 => Just(None), 6 => (-1e3..1e3f64).prop_map(Some), 1 => Just(Some(f64::NAN))]
}

fn histogram(layout: usize, temporality: Temporality) -> impl Strategy<Value = MetricKind> {
    let counts = prop::sample::select(vec![0, 1, 7, u64::MAX - 1, u64::MAX]);
    (prop::collection::vec(counts, LAYOUTS[layout].len()), stat(), stat(), stat()).prop_map(
        move |(counts, sum, min, max)| {
            MetricKind::Histogram(logit_core::Histogram {
                buckets: LAYOUTS[layout].iter().copied().zip(counts).collect(),
                temporality,
                sum,
                min,
                max,
            })
        },
    )
}

fn exp_histogram() -> MetricKind {
    MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
        scale: 0,
        zero_count: 0,
        zero_threshold: 0.0,
        positive: (0, vec![1]),
        negative: (0, vec![]),
        temporality: Temporality::Delta,
        count: 1,
        sum: None,
        min: None,
        max: None,
    })
}

fn summary() -> MetricKind {
    MetricKind::Summary(logit_core::Summary { quantiles: vec![(0.5, 1.0)], count: 1, sum: 1.0 })
}

fn kind_spec() -> impl Strategy<Value = KindSpec> {
    let plain = |s: BoxedStrategy<MetricKind>| s.prop_map(KindSpec::Plain);
    prop_oneof![
        3 => plain((scalar(), temporality(), any::<bool>())
            .prop_map(|(value, temporality, monotonic)| {
                MetricKind::Sum(Sum { value, temporality, monotonic })
            })
            .boxed()),
        2 => plain(scalar().prop_map(MetricKind::Gauge).boxed()),
        2 => plain(scalar().prop_map(MetricKind::GaugeDelta).boxed()),
        3 => plain(samples().prop_map(MetricKind::Samples).boxed()),
        1 => samples().prop_map(KindSpec::Dist),
        1 => members().prop_map(KindSpec::Set),
        2 => plain(members().prop_map(MetricKind::SetMembers).boxed()),
        3 => plain(
            (0..LAYOUTS.len(), temporality())
                .prop_flat_map(|(layout, t)| histogram(layout, t))
                .boxed()
        ),
        1 => plain(prop_oneof![Just(exp_histogram()), Just(summary())].boxed()),
    ]
}

fn record_spec() -> impl Strategy<Value = RecordSpec> {
    (
        prop::sample::select(vec!["m0", "m1", "m2"]),
        prop_oneof![Just(None), Just(Some("ms"))],
        prop_oneof![Just(None), Just(Some("d0")), Just(Some("d1"))],
        kind_spec(),
        prop::bool::weighted(0.1),
    )
        .prop_map(|(name, unit, description, kind, no_recorded_value)| RecordSpec {
            name,
            unit,
            description,
            kind,
            no_recorded_value,
        })
}

/// Attribute sets an event draws from: two distinct series keys and a `NaN` one.
fn event_attributes(i: usize) -> AttrMap {
    let mut attrs = AttrMap::new();
    match i {
        0 => {}
        1 => attrs.insert("k0", Value::Str(Bytes::from_static(b"a"))),
        _ => attrs.insert("k0", Value::F64(f64::NAN)),
    }
    attrs
}

#[derive(Debug, Clone)]
struct EventSpec {
    timestamp: i64,
    attributes: usize,
    records: Vec<RecordSpec>,
    log: bool,
}

impl EventSpec {
    fn build(&self) -> Event {
        let mut event = Event::empty(self.timestamp, event_attributes(self.attributes));
        event.metrics.extend(self.records.iter().map(RecordSpec::build));
        if self.log {
            event.log = Some(logit_core::LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            });
        }
        event
    }
}

fn event_spec() -> impl Strategy<Value = EventSpec> {
    (
        // 1000 runs ahead of every flush clock a run reaches, for the start-time clamp.
        prop_oneof![1 => Just(i64::MIN), 6 => 0..=5i64, 1 => Just(1_000)],
        0..3usize,
        prop::collection::vec(record_spec(), 0..=3),
        prop::bool::weighted(0.2),
    )
        .prop_map(|(timestamp, attributes, records, log)| EventSpec {
            timestamp,
            attributes,
            records,
            log,
        })
}

const CONTEXTS: usize = 10;

fn context(i: usize) -> TraceContext {
    TraceContext { trace_id: [i as u8 + 1; 16], span_id: [i as u8 + 1; 8] }
}

#[derive(Debug, Clone)]
enum ModelOp {
    Batch { resource: usize, scope: usize, context: usize, events: Vec<EventSpec> },
    Flush,
}

fn model_op() -> impl Strategy<Value = ModelOp> {
    prop_oneof![
        4 => (0..RESOURCES, 0..SCOPES, 0..CONTEXTS, prop::collection::vec(event_spec(), 1..=4))
            .prop_map(|(resource, scope, context, events)| ModelOp::Batch {
                resource,
                scope,
                context,
                events,
            }),
        1 => Just(ModelOp::Flush),
    ]
}

#[derive(Debug, Clone, Copy)]
struct ModelConfig {
    temporality: AggregateTemporality,
    /// `None` for `distributions: sketch`, else `samples` with this cap.
    samples_cap: Option<usize>,
    /// `None` for `sets: estimate`, else `members` with this cap.
    members_cap: Option<usize>,
    series_retention: u32,
    max_retained_series: usize,
}

impl ModelConfig {
    /// Whether graph rule 39 accepts the retention bounds: cumulative needs both, and retention
    /// needs a cap.
    fn valid(&self) -> bool {
        let cumulative = self.temporality == AggregateTemporality::Cumulative;
        !(cumulative && self.series_retention == 0)
            && !(self.series_retention > 0 && self.max_retained_series == 0)
    }

    fn aggregator(&self) -> Aggregator {
        let (distributions, samples_cap) = match self.samples_cap {
            None => (Distributions::Sketch, 1000),
            Some(cap) => (Distributions::Samples, cap),
        };
        let (sets, members_cap) = match self.members_cap {
            None => (Sets::Estimate, 1000),
            Some(cap) => (Sets::Members, cap),
        };
        Aggregator::new(Duration::from_secs(10))
            .with_temporality(self.temporality)
            .with_distributions(distributions, samples_cap)
            .with_sets(sets, members_cap)
            .with_series_retention(self.series_retention, self.max_retained_series)
    }
}

/// Every mode, and retention bounds including those rule 39 rejects: a test filters them with
/// [`ModelConfig::valid`].
fn model_config() -> impl Strategy<Value = ModelConfig> {
    (
        prop_oneof![Just(AggregateTemporality::Delta), Just(AggregateTemporality::Cumulative)],
        prop::option::of(1..=8usize),
        prop::option::of(1..=6usize),
        0..=3u32,
        0..=5usize,
    )
        .prop_map(
            |(temporality, samples_cap, members_cap, series_retention, max_retained_series)| {
                ModelConfig {
                    temporality,
                    samples_cap,
                    members_cap,
                    series_retention,
                    max_retained_series,
                }
            },
        )
}

/// `Samples::weight`, restated: `round(1 / rate)` in `[1, 1000]`; a non-finite or non-positive
/// rate is 1.
fn ref_weight(rate: f64) -> u64 {
    if !(rate.is_finite() && rate > 0.0) {
        return 1;
    }
    let w = (1.0 / rate).round();
    if w >= 1000.0 {
        1000
    } else if w >= 1.0 {
        w as u64
    } else {
        1
    }
}

/// `Samples::is_clamped`, restated.
fn ref_clamped(values: &[f64], rate: f64) -> bool {
    !values.is_empty() && rate.is_finite() && rate > 0.0 && (1.0 / rate).round() > 1000.0
}

#[derive(Debug, Clone)]
enum RefAcc {
    Sum {
        total: f64,
        monotonic: bool,
    },
    Gauge {
        value: f64,
        at: i64,
    },
    /// Finite observations and their weights.
    Dist(Vec<(f64, u64)>),
    RawSamples {
        values: Vec<f64>,
        rate: f64,
    },
    Set(std::collections::BTreeSet<Bytes>),
    RawMembers(Vec<Bytes>),
    Hist {
        bounds: Vec<u64>,
        counts: Vec<u128>,
        sum: Option<f64>,
        min: Option<f64>,
        max: Option<f64>,
    },
}

#[derive(Debug, Clone)]
struct RefSeries {
    resource: usize,
    scope: usize,
    key: RefKey,
    acc: RefAcc,
    contexts: Vec<usize>,
    dropped_contexts: u64,
    description: Option<Symbol>,
    /// Absorbed a record since the last flush.
    updated: bool,
    /// Flushes survived with no update.
    idle: u32,
    /// The start time: the opening event's timestamp raised to the window start, lowered to the
    /// flush clock when a retained `Sum`/`Histogram` first emits.
    first_seen: i64,
    /// Order of opening, the cap's second key.
    open_seq: u64,
}

/// What one [`Model::flush`] emits and evicts.
struct RefFlush {
    /// Every updated series, with the `start_timestamp` it's emitted under.
    emitted: Vec<(RefSeries, i64)>,
    /// Updated series that don't survive the flush: `emitted_and_removed` in the ADR's identity.
    removed: u64,
    evicted_idle: u64,
    /// Cap evictions of a series updated this flush, and of an idle one.
    evicted_active: u64,
    evicted_cap_idle: u64,
}

/// Counters the model expects between two flushes, keyed by counter name and tag value.
#[derive(Debug, Default, PartialEq)]
struct RefCounts(std::collections::BTreeMap<(&'static str, &'static str), u64>);

impl RefCounts {
    fn add(&mut self, name: &'static str, tag: &'static str, n: u64) {
        if n > 0 {
            *self.0.entry((name, tag)).or_default() += n;
        }
    }
}

const ABSORBED: &str = "logit.transform.metrics.absorbed";
const PASSED: &str = "logit.transform.metrics.passed_through";
const UNSEEDED: &str = "logit.transform.gauge.delta.unseeded";
const CLAMPED: &str = "logit.transform.samples.weight_clamped";
const NON_FINITE_DROPPED: &str = "logit.transform.samples.non_finite_dropped";
const SAMPLES_FALLBACK: &str = "logit.transform.samples.fallback";
const MEMBERS_FALLBACK: &str = "logit.transform.set_members.fallback";
const LINKS_DROPPED: &str = "logit.transform.links.dropped";
const EVICTED: &str = "logit.transform.series.evicted";
/// Every counter the model predicts, and the tag each is read under ("" for none).
const MODELED_COUNTERS: [(&str, &str); 8] = [
    (ABSORBED, ""),
    (PASSED, "reason"),
    (UNSEEDED, ""),
    (CLAMPED, ""),
    (NON_FINITE_DROPPED, ""),
    (SAMPLES_FALLBACK, "reason"),
    (MEMBERS_FALLBACK, "reason"),
    (LINKS_DROPPED, "reason"),
];

enum Fate {
    Absorbed,
    Passed(&'static str),
}

struct Model {
    config: ModelConfig,
    series: Vec<RefSeries>,
    counts: RefCounts,
    next_open_seq: u64,
    /// The previous flush's clock, `None` before the first flush.
    window_start: Option<i64>,
}

impl Model {
    fn new(config: ModelConfig) -> Self {
        Model {
            config,
            series: Vec::new(),
            counts: RefCounts::default(),
            next_open_seq: 0,
            window_start: None,
        }
    }

    /// Whether `kind` has no merge rule under this config: the ADR's list, restated.
    fn no_merge_rule(&self, kind: &MetricKind) -> bool {
        match kind {
            MetricKind::Sum(s) => s.temporality == Temporality::Cumulative,
            MetricKind::Histogram(h) => {
                h.temporality == Temporality::Cumulative
                    || self.config.temporality == AggregateTemporality::Delta
            }
            MetricKind::ExponentialHistogram(_) | MetricKind::Summary(_) => true,
            _ => false,
        }
    }

    /// The indices of `event`'s records `process` leaves on it, in order.
    fn process(
        &mut self,
        resource: usize,
        scope: usize,
        ctx: usize,
        event: &EventSpec,
    ) -> Vec<usize> {
        let mut forwarded = Vec::new();
        for (i, spec) in event.records.iter().enumerate() {
            match self.absorb(resource, scope, ctx, event, spec) {
                Fate::Absorbed => self.counts.add(ABSORBED, "", 1),
                Fate::Passed(reason) => {
                    self.counts.add(PASSED, reason, 1);
                    forwarded.push(i);
                }
            }
        }
        forwarded
    }

    fn absorb(
        &mut self,
        resource: usize,
        scope: usize,
        ctx: usize,
        event: &EventSpec,
        spec: &RecordSpec,
    ) -> Fate {
        let kind = spec.kind.build();
        if spec.no_recorded_value {
            return Fate::Passed("no_recorded_value");
        }
        if self.no_merge_rule(&kind) {
            return Fate::Passed("no_merge_rule");
        }
        if matches!(kind, MetricKind::Sum(s) if !s.value.is_finite()) {
            return Fate::Passed("non_finite");
        }

        let key = RefKey::of(
            intern(spec.name),
            spec.unit.map(intern),
            &event_attributes(event.attributes),
        );
        let found = self
            .series
            .iter()
            .position(|s| s.resource == resource && s.scope == scope && ref_key_eq(&s.key, &key));
        let opened = found.is_none();
        let index = found.unwrap_or_else(|| {
            let acc = self.open(&spec.kind);
            let first_seen = match self.window_start {
                Some(start) if start > event.timestamp => start,
                _ => event.timestamp,
            };
            self.series.push(RefSeries {
                resource,
                scope,
                key,
                acc,
                contexts: Vec::new(),
                dropped_contexts: 0,
                description: spec.description.map(intern),
                updated: false,
                idle: 0,
                first_seen,
                open_seq: self.next_open_seq,
            });
            self.next_open_seq += 1;
            self.series.len() - 1
        });

        let config = self.config;
        let counts = &mut self.counts;
        let series = &mut self.series[index];
        let fate = merge(&mut series.acc, &spec.kind, event.timestamp, config, counts);
        if let Fate::Absorbed = fate {
            series.updated = true;
            if opened && matches!(kind, MetricKind::GaugeDelta(_)) {
                counts.add(UNSEEDED, "", 1);
            }
            if !series.contexts.contains(&ctx) {
                if series.contexts.len() >= 8 {
                    series.dropped_contexts += 1;
                } else {
                    series.contexts.push(ctx);
                }
            }
        }
        fate
    }

    /// The empty accumulator a series of this kind opens with under this config.
    fn open(&self, spec: &KindSpec) -> RefAcc {
        match spec.build() {
            MetricKind::Sum(s) => RefAcc::Sum { total: 0.0, monotonic: s.monotonic },
            MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => {
                RefAcc::Gauge { value: 0.0, at: i64::MIN }
            }
            MetricKind::Samples(s) => match self.config.samples_cap {
                None => RefAcc::Dist(Vec::new()),
                Some(_) => RefAcc::RawSamples { values: Vec::new(), rate: s.sample_rate },
            },
            MetricKind::Distribution(_) => RefAcc::Dist(Vec::new()),
            MetricKind::Set(_) => RefAcc::Set(Default::default()),
            MetricKind::SetMembers(_) => match self.config.members_cap {
                None => RefAcc::Set(Default::default()),
                Some(_) => RefAcc::RawMembers(Vec::new()),
            },
            MetricKind::Histogram(h) => RefAcc::Hist {
                bounds: h.buckets.iter().map(|(b, _)| b.to_bits()).collect(),
                counts: vec![0; h.buckets.len()],
                sum: h.sum.map(|_| 0.0),
                min: None,
                max: None,
            },
            other => unreachable!("{other:?} has no merge rule"),
        }
    }

    /// Emits every updated series and keeps the retainable ones: a gauge, or a cumulative-mode
    /// `Sum`/`Histogram`, while `series_retention > 0`. An idle series emits nothing and is evicted
    /// at its `series_retention`th idle flush. Past `max_retained_series`, the most idle go first
    /// and the newest among equally idle ones.
    fn flush(&mut self, now: i64) -> RefFlush {
        let config = self.config;
        let cumulative = config.temporality == AggregateTemporality::Cumulative;
        let mut out = RefFlush {
            emitted: Vec::new(),
            removed: 0,
            evicted_idle: 0,
            evicted_active: 0,
            evicted_cap_idle: 0,
        };
        let mut kept = Vec::new();
        for mut series in std::mem::take(&mut self.series) {
            if !series.updated {
                series.idle += 1;
                if series.idle < config.series_retention {
                    kept.push(series);
                } else {
                    out.evicted_idle += 1;
                }
                continue;
            }
            let retainable = match series.acc {
                RefAcc::Gauge { .. } => true,
                RefAcc::Sum { .. } | RefAcc::Hist { .. } => cumulative,
                _ => false,
            };
            if config.series_retention == 0 || !retainable {
                out.removed += 1;
                out.emitted.push((series, 0));
                continue;
            }
            let start = if matches!(series.acc, RefAcc::Gauge { .. }) {
                0
            } else {
                series.first_seen = series.first_seen.min(now);
                series.first_seen
            };
            out.emitted.push((series.clone(), start));
            series.updated = false;
            series.idle = 0;
            series.contexts.clear();
            series.dropped_contexts = 0;
            if let RefAcc::Gauge { at, .. } = &mut series.acc {
                *at = i64::MIN;
            }
            kept.push(series);
        }
        if kept.len() > config.max_retained_series {
            let excess = kept.len() - config.max_retained_series;
            kept.sort_by(|a, b| b.idle.cmp(&a.idle).then(b.open_seq.cmp(&a.open_seq)));
            for series in kept.drain(..excess) {
                if series.idle == 0 {
                    out.evicted_active += 1;
                } else {
                    out.evicted_cap_idle += 1;
                }
            }
        }
        self.series = kept;
        self.window_start = Some(now);
        out
    }
}

/// Finite observations of `values` at `rate`'s weight, and how many values weren't finite.
fn observations(values: &[f64], rate: f64) -> (Vec<(f64, u64)>, u64) {
    let w = ref_weight(rate);
    let finite: Vec<(f64, u64)> =
        values.iter().filter(|v| v.is_finite()).map(|v| (*v, w)).collect();
    let dropped = (values.len() - finite.len()) as u64;
    (finite, dropped)
}

/// Folds one record into `acc` under the ADR's per-kind rules.
fn merge(
    acc: &mut RefAcc,
    spec: &KindSpec,
    timestamp: i64,
    config: ModelConfig,
    counts: &mut RefCounts,
) -> Fate {
    let kind = spec.build();
    match (&mut *acc, &kind) {
        (RefAcc::Sum { total, .. }, MetricKind::Sum(s)) => *total += s.value,
        (RefAcc::Gauge { value, at }, MetricKind::Gauge(v)) => {
            if timestamp >= *at {
                *value = *v;
                *at = timestamp;
            }
        }
        (RefAcc::Gauge { value, .. }, MetricKind::GaugeDelta(d)) => *value += d,
        (RefAcc::Dist(obs), MetricKind::Samples(s)) => {
            let (finite, dropped) = observations(&s.values, s.sample_rate);
            obs.extend(finite);
            counts.add(NON_FINITE_DROPPED, "", dropped);
            counts.add(CLAMPED, "", u64::from(ref_clamped(&s.values, s.sample_rate)));
        }
        (RefAcc::RawSamples { values, rate }, MetricKind::Samples(s)) => {
            let cap = config.samples_cap.unwrap_or(usize::MAX);
            let reason = if rate.to_bits() != s.sample_rate.to_bits() {
                Some("rate_mismatch")
            } else if values.len() + s.values.len() > cap {
                Some("cap")
            } else {
                None
            };
            match reason {
                None => values.extend(s.values.iter().copied()),
                Some(reason) => {
                    let mut obs = Vec::new();
                    for (vs, r) in [(&values[..], *rate), (&s.values[..], s.sample_rate)] {
                        let (finite, dropped) = observations(vs, r);
                        obs.extend(finite);
                        counts.add(NON_FINITE_DROPPED, "", dropped);
                        counts.add(CLAMPED, "", u64::from(ref_clamped(vs, r)));
                    }
                    counts.add(SAMPLES_FALLBACK, reason, 1);
                    *acc = RefAcc::Dist(obs);
                }
            }
        }
        (RefAcc::Dist(obs), MetricKind::Distribution(_)) => {
            let KindSpec::Dist(s) = spec else { unreachable!() };
            obs.extend(observations(&s.values, s.sample_rate).0);
        }
        (RefAcc::RawSamples { values, rate }, MetricKind::Distribution(_)) => {
            let KindSpec::Dist(s) = spec else { unreachable!() };
            let (mut obs, dropped) = observations(values, *rate);
            counts.add(NON_FINITE_DROPPED, "", dropped);
            counts.add(CLAMPED, "", u64::from(ref_clamped(values, *rate)));
            obs.extend(observations(&s.values, s.sample_rate).0);
            *acc = RefAcc::Dist(obs);
        }
        (RefAcc::Set(set), MetricKind::Set(_)) => {
            let KindSpec::Set(members) = spec else { unreachable!() };
            set.extend(members.iter().cloned());
        }
        (RefAcc::Set(set), MetricKind::SetMembers(members)) => set.extend(members.iter().cloned()),
        (RefAcc::RawMembers(held), MetricKind::Set(_)) => {
            let KindSpec::Set(members) = spec else { unreachable!() };
            *acc = RefAcc::Set(held.iter().chain(members).cloned().collect());
        }
        (RefAcc::RawMembers(held), MetricKind::SetMembers(members)) => {
            for m in members {
                if !held.contains(m) {
                    held.push(m.clone());
                }
            }
            if held.len() > config.members_cap.unwrap_or(usize::MAX) {
                counts.add(MEMBERS_FALLBACK, "cap", 1);
                *acc = RefAcc::Set(held.iter().cloned().collect());
            }
        }
        (RefAcc::Hist { bounds, counts: held, sum, min, max }, MetricKind::Histogram(h)) => {
            let incoming: Vec<u64> = h.buckets.iter().map(|(b, _)| b.to_bits()).collect();
            if *bounds != incoming {
                return Fate::Passed("histogram_bounds_mismatch");
            }
            let held_observed = held.iter().any(|c| *c > 0);
            let incoming_observed = h.buckets.iter().any(|(_, c)| *c > 0);
            for (total, (_, c)) in held.iter_mut().zip(&h.buckets) {
                *total += u128::from(*c);
            }
            *sum = match (*sum, h.sum) {
                (Some(a), Some(b)) => Some(a + b),
                _ => None,
            };
            let fold = |held: Option<f64>, incoming: Option<f64>, pick: fn(f64, f64) -> f64| {
                if !incoming_observed {
                    held
                } else if !held_observed {
                    incoming
                } else {
                    held.zip(incoming).map(|(a, b)| pick(a, b))
                }
            };
            *min = fold(*min, h.min, f64::min);
            *max = fold(*max, h.max, f64::max);
        }
        _ => return Fate::Passed("kind_conflict"),
    }
    Fate::Absorbed
}

/// `a` and `b` are one value: bitwise, or both `NaN`.
fn same_f64(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
}

fn same_opt(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => same_f64(a, b),
        (None, None) => true,
        _ => false,
    }
}

/// The `q`-quantile a sketch's rank rule picks from the true observations: the Agent mapping
/// rounds `q * (count - 1)` to even and takes the observation holding that rank.
fn true_quantile(obs: &[(f64, u64)], q: f64) -> f64 {
    let mut sorted = obs.to_vec();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
    let count: u64 = sorted.iter().map(|o| o.1).sum();
    let rank = (q * (count - 1) as f64).round_ties_even();
    let mut seen = 0.0;
    for (v, w) in &sorted {
        seen += *w as f64;
        if seen > rank {
            return *v;
        }
    }
    sorted.last().map_or(0.0, |o| o.0)
}

/// The ADR's `Distribution` equality against the true observations: `count()` exact, `sum()`
/// within `1e-9·max(1, Σ|v·w|)`, `min`/`max` exact, quantiles within the mapping's relative
/// accuracy.
fn check_sketch(sketch: &logit_core::DdSketch, obs: &[(f64, u64)]) -> Result<(), TestCaseError> {
    let count: u64 = obs.iter().map(|o| o.1).sum();
    prop_assert_eq!(sketch.count() as u64, count);
    let sum: f64 = obs.iter().map(|(v, w)| v * *w as f64).sum();
    let scale: f64 = obs.iter().map(|(v, w)| (v * *w as f64).abs()).sum();
    prop_assert!(
        (sketch.sum() - sum).abs() <= 1e-9 * scale.max(1.0),
        "sum {} against {}",
        sketch.sum(),
        sum
    );
    let min = obs.iter().map(|o| o.0).reduce(f64::min);
    let max = obs.iter().map(|o| o.0).reduce(f64::max);
    prop_assert_eq!(sketch.min(), min);
    prop_assert_eq!(sketch.max(), max);
    if count > 0 {
        // Relative to the true value. The Agent's bin center is `1 - 1/√γ` from the estimate,
        // which is `√γ - 1` from a value at the bin's lower edge.
        let gamma = sketch.mapping().gamma();
        let alpha = match sketch.mapping().kind() {
            logit_core::MappingKind::Agent => gamma.sqrt() - 1.0,
            logit_core::MappingKind::Logarithmic => 1.0 - 2.0 / (1.0 + gamma),
        };
        for q in [0.25, 0.5, 0.9, 0.99] {
            let truth = true_quantile(obs, q);
            let estimate = sketch.quantile(q).unwrap_or(f64::NAN);
            prop_assert!(
                (estimate - truth).abs() <= alpha * truth.abs() + 1e-9 * gamma,
                "q{} estimate {} against {}",
                q,
                estimate,
                truth
            );
        }
    }
    Ok(())
}

/// The emitted value against the model's accumulator, by the ADR's per-kind equality.
fn check_emitted(
    kind: &MetricKind,
    acc: &RefAcc,
    config: ModelConfig,
) -> Result<(), TestCaseError> {
    let mode = record_temporality(config.temporality);
    match (kind, acc) {
        (MetricKind::Sum(s), RefAcc::Sum { total, monotonic }) => {
            prop_assert!(same_f64(s.value, *total), "sum {} against {}", s.value, total);
            prop_assert_eq!(s.temporality, mode);
            prop_assert_eq!(s.monotonic, *monotonic);
        }
        (MetricKind::Gauge(v), RefAcc::Gauge { value, .. }) => {
            prop_assert!(same_f64(*v, *value), "gauge {} against {}", v, value);
        }
        (MetricKind::Distribution(sketch), RefAcc::Dist(obs)) => {
            prop_assert_eq!(sketch.mapping().kind(), logit_core::MappingKind::Agent);
            check_sketch(sketch, obs)?;
        }
        (MetricKind::Samples(s), RefAcc::RawSamples { values, rate }) => {
            prop_assert_eq!(s.sample_rate.to_bits(), rate.to_bits());
            let sorted = |v: &[f64]| {
                let mut bits: Vec<u64> = v.iter().map(|x| x.to_bits()).collect();
                bits.sort_unstable();
                bits
            };
            prop_assert_eq!(sorted(&s.values), sorted(values));
        }
        (MetricKind::Set(hll), RefAcc::Set(set)) => {
            // Exact below the HyperLogLog's small-set threshold, which ALPHABET stays under.
            prop_assert_eq!(hll.estimate(), set.len() as u64);
        }
        (MetricKind::SetMembers(members), RefAcc::RawMembers(held)) => {
            prop_assert_eq!(members, held);
        }
        (MetricKind::Histogram(h), RefAcc::Hist { bounds, counts, sum, min, max }) => {
            let emitted: Vec<u64> = h.buckets.iter().map(|(b, _)| b.to_bits()).collect();
            prop_assert_eq!(&emitted, bounds);
            let expected: Vec<u64> =
                counts.iter().map(|c| u64::try_from(*c).unwrap_or(u64::MAX)).collect();
            let emitted: Vec<u64> = h.buckets.iter().map(|(_, c)| *c).collect();
            prop_assert_eq!(emitted, expected);
            prop_assert_eq!(h.temporality, Temporality::Cumulative);
            prop_assert!(same_opt(h.sum, *sum), "sum {:?} against {:?}", h.sum, sum);
            prop_assert!(same_opt(h.min, *min), "min {:?} against {:?}", h.min, min);
            prop_assert!(same_opt(h.max, *max), "max {:?} against {:?}", h.max, max);
        }
        (kind, acc) => prop_assert!(false, "emitted {:?} for {:?}", kind, acc),
    }
    Ok(())
}

/// A counter's total in drained telemetry, over every point carrying `tag_value` under `tag`
/// (or every point, when `tag` is empty).
fn telemetry_total(events: &[Event], name: &str, tag: &str, tag_value: &str) -> u64 {
    events
        .iter()
        .filter(|e| {
            tag.is_empty() || e.attributes.get(tag).and_then(|v| v.as_str()) == Some(tag_value)
        })
        .flat_map(|e| &e.metrics)
        .filter(|m| resolve(m.name) == name)
        .map(|m| match &m.kind {
            MetricKind::Sum(s) => s.value as u64,
            _ => 0,
        })
        .sum()
}

fn telemetry_gauge(events: &[Event], name: &str) -> Option<f64> {
    events.iter().flat_map(|e| &e.metrics).find_map(|m| match &m.kind {
        MetricKind::Gauge(v) if resolve(m.name) == name => Some(*v),
        _ => None,
    })
}

/// Tag values the tagged counters are read under.
const TAG_VALUES: [&str; 8] = [
    "no_recorded_value",
    "no_merge_rule",
    "kind_conflict",
    "histogram_bounds_mismatch",
    "non_finite",
    "rate_mismatch",
    "cap",
    "contexts",
];

/// Every counter the model predicts, read back from drained telemetry. A tagged counter must
/// carry no tag value outside [`TAG_VALUES`].
fn observed_counts(events: &[Event]) -> Result<RefCounts, TestCaseError> {
    let mut observed = RefCounts::default();
    for (name, tag) in MODELED_COUNTERS {
        let total = telemetry_total(events, name, "", "");
        if tag.is_empty() {
            observed.add(name, "", total);
            continue;
        }
        let mut tagged = 0;
        for value in TAG_VALUES {
            let n = telemetry_total(events, name, tag, value);
            observed.add(name, value, n);
            tagged += n;
        }
        prop_assert_eq!(tagged, total, "{} carries an unlisted {}", name, tag);
    }
    Ok(observed)
}

fn run_model(config: ModelConfig, ops: &[ModelOp]) -> Result<(), TestCaseError> {
    let (resources, resource_class) = resource_pool();
    let (scopes, scope_class) = scope_pool();
    let registry = logit_core::Registry::new();
    let mut agg = config.aggregator().with_telemetry(registry.telemetry_for(
        "model",
        "aggregate",
        "transform",
    ));
    let mut model = Model::new(config);
    let mut now = 0;
    // Records `process` received since the last flush.
    let mut metrics_in: u64 = 0;

    for op in ops {
        match op {
            ModelOp::Batch { resource, scope, context: ctx, events } => {
                agg.observe_scope(scopes[*scope].clone());
                agg.observe_batch_context(context(*ctx));
                for spec in events {
                    metrics_in += spec.records.len() as u64;
                    let original = spec.build();
                    let mut event = spec.build();
                    let kept = agg.process(&resources[*resource], &mut event);
                    let forwarded =
                        model.process(resource_class[*resource], scope_class[*scope], *ctx, spec);

                    // (2) Forwarded records are untouched, in order, and `process` returns false
                    // only when nothing is left.
                    let expected: Vec<String> =
                        forwarded.iter().map(|i| format!("{:?}", original.metrics[*i])).collect();
                    let actual: Vec<String> =
                        event.metrics.iter().map(|m| format!("{m:?}")).collect();
                    prop_assert_eq!(actual, expected);
                    let empty = spec.records.is_empty();
                    prop_assert_eq!(kept, empty || !forwarded.is_empty() || spec.log);
                }
            }
            ModelOp::Flush => {
                now += 10;
                let groups = {
                    let mut classes: Vec<(usize, usize)> =
                        model.series.iter().map(|s| (s.resource, s.scope)).collect();
                    classes.sort_unstable();
                    classes.dedup();
                    classes.len()
                };
                let active = model.series.iter().filter(|s| s.updated).count();
                let retained = model.series.len() - active;
                let series_before: usize = agg.groups.iter().map(|g| g.series.len()).sum();
                prop_assert_eq!(series_before, model.series.len());
                let flushed = agg.flush(now);
                let RefFlush {
                    emitted: mut expected,
                    removed,
                    evicted_idle,
                    evicted_active,
                    evicted_cap_idle,
                } = model.flush(now);
                let mut counts = std::mem::take(&mut model.counts);
                counts.add(
                    LINKS_DROPPED,
                    "contexts",
                    expected.iter().map(|(s, _)| s.dropped_contexts).sum(),
                );

                // (1) and (3): one emitted series per model series, with its value, key,
                // description, and links.
                let mut emitted = 0;
                for (resource, scope, events) in &flushed {
                    let r = resources.iter().position(|x| Arc::ptr_eq(x, resource));
                    let r = resource_class[r.expect("an emitted resource comes from the pool")];
                    let s = match scope {
                        None => 0,
                        Some(scope) => {
                            let i = scopes
                                .iter()
                                .position(|x| x.as_ref().is_some_and(|x| Arc::ptr_eq(x, scope)));
                            scope_class[i.expect("an emitted scope comes from the pool")]
                        }
                    };
                    for (event, links) in events {
                        emitted += 1;
                        prop_assert_eq!(event.timestamp, now);
                        prop_assert!(event.log.is_none() && event.span.is_none());
                        prop_assert_eq!(event.metrics.len(), 1);
                        let record = &event.metrics[0];
                        prop_assert_eq!(record.flags, 0);
                        let key = RefKey::of(record.name, record.unit, &event.attributes);
                        let i = expected.iter().position(|(m, _)| {
                            m.resource == r && m.scope == s && ref_key_eq(&m.key, &key)
                        });
                        prop_assert!(i.is_some(), "emitted {:?} is no model series", key);
                        let (series, start) = expected.swap_remove(i.unwrap_or_default());
                        // (9) A start time lies inside its window, fresh for a re-created series.
                        prop_assert_eq!(record.start_timestamp, start, "{:?}", key);
                        prop_assert_eq!(record.description, series.description);
                        check_emitted(&record.kind, &series.acc, config)?;
                        let link_ids: Vec<[u8; 16]> = links.iter().map(|l| l.trace_id).collect();
                        let model_ids: Vec<[u8; 16]> =
                            series.contexts.iter().map(|c| context(*c).trace_id).collect();
                        prop_assert_eq!(link_ids, model_ids);
                    }
                }
                prop_assert!(expected.is_empty(), "model series not emitted: {:?}", expected);

                // (4) and (5): gauges and counters.
                let events = registry.drain(now);
                prop_assert_eq!(emitted, active);
                prop_assert_eq!(
                    telemetry_gauge(&events, "logit.transform.series.active"),
                    Some(active as f64)
                );
                prop_assert_eq!(
                    telemetry_gauge(&events, "logit.transform.series.retained"),
                    Some(retained as f64)
                );
                prop_assert_eq!(
                    telemetry_gauge(&events, "logit.transform.resource.groups"),
                    Some(groups as f64)
                );
                let passed: u64 =
                    TAG_VALUES.iter().map(|r| telemetry_total(&events, PASSED, "reason", r)).sum();
                prop_assert_eq!(
                    metrics_in,
                    telemetry_total(&events, ABSORBED, "", "") + passed,
                    "metrics_in == absorbed + passed_through"
                );
                metrics_in = 0;
                prop_assert_eq!(observed_counts(&events)?, counts);
                prop_assert_eq!(telemetry_total(&events, EVICTED, "reason", "idle"), evicted_idle);
                prop_assert_eq!(
                    telemetry_total(&events, EVICTED, "state", "active"),
                    evicted_active
                );
                prop_assert_eq!(
                    telemetry_total(&events, EVICTED, "state", "idle"),
                    evicted_cap_idle
                );
                prop_assert_eq!(
                    telemetry_total(&events, EVICTED, "reason", "cardinality"),
                    evicted_active + evicted_cap_idle
                );

                // Survivors, from the aggregator's own state.
                let series_after: usize = agg.groups.iter().map(|g| g.series.len()).sum();
                prop_assert_eq!(
                    series_before as u64,
                    removed
                        + series_after as u64
                        + evicted_idle
                        + evicted_active
                        + evicted_cap_idle,
                    "series_at_flush_start == emitted_and_removed + kept + evicted"
                );
                // (6) The cap holds, and retention 0 keeps nothing.
                prop_assert!(series_after <= config.max_retained_series);
                if config.series_retention == 0 {
                    prop_assert_eq!(series_after, 0);
                }
                // (7) No empty group, and one per surviving (resource, scope) class.
                let mut classes: Vec<(usize, usize)> =
                    model.series.iter().map(|s| (s.resource, s.scope)).collect();
                classes.sort_unstable();
                classes.dedup();
                prop_assert_eq!(agg.groups.len(), classes.len());
                prop_assert_eq!(series_after, model.series.len());
                for group in &agg.groups {
                    prop_assert!(!group.series.is_empty());
                    let r = resources.iter().position(|x| Arc::ptr_eq(x, &group.resource));
                    let r = resource_class[r.expect("a group's resource comes from the pool")];
                    let s = match &group.scope {
                        None => 0,
                        Some(scope) => {
                            let i = scopes
                                .iter()
                                .position(|x| x.as_ref().is_some_and(|x| Arc::ptr_eq(x, scope)));
                            scope_class[i.expect("a group's scope comes from the pool")]
                        }
                    };
                    // (8) Every survivor is the model's, reset for the next window.
                    for (key, state) in &group.series {
                        let key = RefKey::of(key.name, key.unit, &key.attributes);
                        let m = model
                            .series
                            .iter()
                            .find(|m| m.resource == r && m.scope == s && ref_key_eq(&m.key, &key));
                        prop_assert!(m.is_some(), "survivor {:?} is no model survivor", key);
                        let m = m.expect("checked above");
                        prop_assert!(!state.updated_this_window);
                        prop_assert!(state.contexts.seen.is_empty() && state.contexts.dropped == 0);
                        prop_assert_eq!(state.idle_windows, m.idle);
                        prop_assert_eq!(state.open_seq, m.open_seq);
                        prop_assert_eq!(state.first_seen, m.first_seen);
                        if let Accumulator::Gauge { at, .. } = state.accumulator {
                            prop_assert_eq!(at, i64::MIN);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// -- Merge laws -------------------------------------------------------------------------------

/// A kind family whose records merge with each other: 0 delta `Sum`, 1 `Samples`,
/// 2 `Distribution`, 3 `Set`, 4 `SetMembers`, 5 delta `Histogram` (cumulative mode).
const FAMILIES: usize = 6;

fn family_kind(family: usize) -> BoxedStrategy<KindSpec> {
    match family {
        0 => (-1e12..1e12f64, any::<bool>())
            .prop_map(|(value, monotonic)| {
                KindSpec::Plain(MetricKind::Sum(Sum {
                    value,
                    temporality: Temporality::Delta,
                    monotonic,
                }))
            })
            .boxed(),
        1 => samples().prop_map(|s| KindSpec::Plain(MetricKind::Samples(s))).boxed(),
        2 => samples().prop_map(KindSpec::Dist).boxed(),
        3 => members().prop_map(KindSpec::Set).boxed(),
        4 => members().prop_map(|m| KindSpec::Plain(MetricKind::SetMembers(m))).boxed(),
        _ => histogram(0, Temporality::Delta).prop_map(KindSpec::Plain).boxed(),
    }
}

fn family_config(family: usize) -> BoxedStrategy<ModelConfig> {
    let temporality =
        if family == 5 { AggregateTemporality::Cumulative } else { AggregateTemporality::Delta };
    (prop::option::of(1..=8usize), prop::option::of(1..=6usize))
        .prop_map(move |(samples_cap, members_cap)| ModelConfig {
            temporality,
            samples_cap,
            members_cap,
            // Rule 39: cumulative needs both bounds. One series, one flush: retention changes
            // only the emitted start time, which the laws don't compare.
            series_retention: u32::from(family == 5),
            max_retained_series: usize::from(family == 5),
        })
        .boxed()
}

/// What one series emits after absorbing `kinds` in order, one record per event.
fn emit_one(config: ModelConfig, kinds: &[MetricKind]) -> Result<MetricKind, TestCaseError> {
    let resource = Arc::new(Resource::default());
    let mut agg = config.aggregator();
    for kind in kinds {
        let mut event =
            Event::metric(0, AttrMap::new(), MetricRecord::new(intern("m"), kind.clone()));
        prop_assert!(!agg.process(&resource, &mut event), "{:?} is absorbed", kind);
    }
    let mut flushed = agg.flush(10);
    prop_assert_eq!(flushed.len(), 1);
    let (_, _, mut events) = flushed.remove(0);
    prop_assert_eq!(events.len(), 1);
    let (mut event, _) = events.remove(0);
    Ok(event.metrics.remove(0).kind)
}

/// `Σ|vᵢ|` over the operands' values (each weighted, for a sample), the scale a rounding bound
/// is taken against.
fn magnitude(specs: &[KindSpec]) -> f64 {
    let weighted = |s: &Samples| {
        let w = ref_weight(s.sample_rate) as f64;
        s.values.iter().filter(|v| v.is_finite()).map(|v| (v * w).abs()).sum::<f64>()
    };
    specs
        .iter()
        .map(|spec| match spec {
            KindSpec::Plain(MetricKind::Sum(s)) => s.value.abs(),
            KindSpec::Plain(MetricKind::Histogram(h)) => h.sum.map_or(0.0, f64::abs),
            KindSpec::Plain(MetricKind::Samples(s)) | KindSpec::Dist(s) => weighted(s),
            _ => 0.0,
        })
        .sum()
}

/// Two emissions of one population are equal by the ADR's merge-law equality. `n` operands were
/// folded, and `scale` is `Σ|vᵢ|` for the rounding bound.
fn law_eq(x: &MetricKind, y: &MetricKind, n: usize, scale: f64) -> Result<(), TestCaseError> {
    let rounding = (n.saturating_sub(1)) as f64 * f64::EPSILON * scale;
    let close = |a: f64, b: f64| {
        if n <= 2 {
            same_f64(a, b)
        } else {
            same_f64(a, b) || (a - b).abs() <= rounding
        }
    };
    match (x, y) {
        (MetricKind::Sum(a), MetricKind::Sum(b)) => {
            prop_assert!(close(a.value, b.value), "{} against {}", a.value, b.value);
        }
        (MetricKind::Distribution(a), MetricKind::Distribution(b)) => {
            prop_assert_eq!(a.mapping(), b.mapping());
            prop_assert_eq!(a.positive_bins(), b.positive_bins());
            prop_assert_eq!(a.negative_bins(), b.negative_bins());
            prop_assert_eq!(a.zero_count(), b.zero_count());
            prop_assert_eq!(a.count(), b.count());
            prop_assert_eq!(a.min(), b.min());
            prop_assert_eq!(a.max(), b.max());
            let tolerance = 1e-9 * scale.max(1.0);
            prop_assert!((a.sum() - b.sum()).abs() <= tolerance, "{} against {}", a.sum(), b.sum());
        }
        (MetricKind::Samples(a), MetricKind::Samples(b)) => {
            prop_assert_eq!(a.sample_rate.to_bits(), b.sample_rate.to_bits());
            let sorted = |v: &[f64]| {
                let mut bits: Vec<u64> = v.iter().map(|x| x.to_bits()).collect();
                bits.sort_unstable();
                bits
            };
            prop_assert_eq!(sorted(&a.values), sorted(&b.values));
        }
        (MetricKind::Set(a), MetricKind::Set(b)) => prop_assert_eq!(a.estimate(), b.estimate()),
        (MetricKind::SetMembers(a), MetricKind::SetMembers(b)) => {
            let set = |m: &[Bytes]| m.iter().cloned().collect::<std::collections::BTreeSet<_>>();
            prop_assert_eq!(a.len(), b.len());
            prop_assert_eq!(set(a), set(b));
        }
        (MetricKind::Histogram(a), MetricKind::Histogram(b)) => {
            prop_assert_eq!(&a.buckets, &b.buckets);
            match (a.sum, b.sum) {
                (Some(sa), Some(sb)) => prop_assert!(close(sa, sb), "{} against {}", sa, sb),
                (sa, sb) => prop_assert_eq!(sa, sb),
            }
            prop_assert!(
                same_opt(a.min, b.min) && same_opt(a.max, b.max),
                "{:?} against {:?}",
                a,
                b
            );
        }
        (x, y) => prop_assert!(false, "{:?} against {:?}", x, y),
    }
    Ok(())
}

fn laws_input() -> impl Strategy<Value = (ModelConfig, Vec<KindSpec>)> {
    (0..FAMILIES).prop_flat_map(|family| {
        (family_config(family), prop::collection::vec(family_kind(family), 2..=4))
    })
}

/// Ten distinct contexts on one series in one window: the model and the aggregator both keep the
/// first eight as links and count two dropped.
#[test]
fn the_model_caps_links_per_series() {
    let gauge = RecordSpec {
        name: "m0",
        unit: None,
        description: None,
        kind: KindSpec::Plain(MetricKind::Gauge(1.0)),
        no_recorded_value: false,
    };
    let event = EventSpec { timestamp: 0, attributes: 0, records: vec![gauge], log: false };
    let config = ModelConfig {
        temporality: AggregateTemporality::Delta,
        samples_cap: None,
        members_cap: None,
        series_retention: 0,
        max_retained_series: 0,
    };
    let mut ops: Vec<ModelOp> = (0..CONTEXTS)
        .map(|context| ModelOp::Batch {
            resource: 0,
            scope: 0,
            context,
            events: vec![event.clone()],
        })
        .collect();
    ops.push(ModelOp::Flush);
    if let Err(e) = run_model(config, &ops) {
        panic!("{e}");
    }
}

proptest! {
    #![proptest_config(config(128))]

    /// The model's expectations hold for every emitted series and counter, over random batches
    /// and flushes under every mode.
    #[test]
    fn aggregator_matches_the_reference_model(
        config in model_config(),
        ops in prop::collection::vec(model_op(), 1..=60),
    ) {
        prop_assume!(config.valid());
        run_model(config, &ops)?;
    }
}

proptest! {
    #![proptest_config(config(256))]

    /// Absorbing one window's records in reverse order emits an equal value.
    #[test]
    fn merge_is_order_independent_within_a_window((config, specs) in laws_input()) {
        let kinds: Vec<MetricKind> = specs.iter().map(KindSpec::build).collect();
        let reversed: Vec<MetricKind> = kinds.iter().rev().cloned().collect();
        let forward = emit_one(config, &kinds)?;
        let backward = emit_one(config, &reversed)?;
        law_eq(&forward, &backward, kinds.len(), magnitude(&specs))?;
    }

    /// In delta mode, a relay of two stages (`a`, `b` through the first; its output and `c`
    /// through the second) emits what one stage absorbing `a`, `b`, and `c` does.
    #[test]
    fn a_relay_of_two_stages_matches_one_stage(
        (config, specs) in (0..FAMILIES - 1).prop_flat_map(|family| {
            (family_config(family), prop::collection::vec(family_kind(family), 3))
        }),
    ) {
        let kinds: Vec<MetricKind> = specs.iter().map(KindSpec::build).collect();
        let direct = emit_one(config, &kinds)?;
        let upstream = emit_one(config, &kinds[..2])?;
        let relayed = emit_one(config, &[upstream, kinds[2].clone()])?;
        law_eq(&direct, &relayed, 3, magnitude(&specs))?;
    }

    /// A relay of gauges agrees with one stage when every upstream timestamp is at or before the
    /// first stage's flush and the later record's is at or after it.
    #[test]
    fn a_gauge_relay_matches_one_stage_when_the_later_record_follows_the_flush(
        a in scalar(), b in scalar(), c in scalar(),
        ta in 0..=10i64, tb in 0..=10i64, tc in 10..=15i64,
    ) {
        let resource = Arc::new(Resource::default());
        let gauge = |v: f64, ts: i64| {
            Event::metric(ts, AttrMap::new(), MetricRecord::new(intern("g"), MetricKind::Gauge(v)))
        };
        let emitted = |agg: &mut Aggregator, now: i64| {
            let (_, _, mut events) = agg.flush(now).remove(0);
            let (mut event, _) = events.remove(0);
            (event.timestamp, event.metrics.remove(0).kind)
        };

        let mut direct = Aggregator::new(Duration::from_secs(10));
        for mut event in [gauge(a, ta), gauge(b, tb), gauge(c, tc)] {
            direct.process(&resource, &mut event);
        }
        let (_, direct) = emitted(&mut direct, 20);

        let mut first = Aggregator::new(Duration::from_secs(10));
        for mut event in [gauge(a, ta), gauge(b, tb)] {
            first.process(&resource, &mut event);
        }
        let (now1, relayed) = emitted(&mut first, 10);
        let MetricKind::Gauge(relayed) = relayed else { unreachable!() };
        let mut second = Aggregator::new(Duration::from_secs(10));
        for mut event in [gauge(relayed, now1), gauge(c, tc)] {
            second.process(&resource, &mut event);
        }
        let (_, relayed) = emitted(&mut second, 20);
        match (direct, relayed) {
            (MetricKind::Gauge(x), MetricKind::Gauge(y)) => {
                prop_assert!(same_f64(x, y) && same_f64(x, c), "{} and {} against {}", x, y, c);
            }
            other => prop_assert!(false, "{:?}", other),
        }
    }
}

// -- Order-dependent outcomes -----------------------------------------------------------------
//
// The ADR's "depend on order by design" list, pinned as examples. `aggregate`'s own tests pin
// the gauge-and-delta interleavings (`absolute_then_delta_adds_to_the_absolute`,
// `delta_then_absolute_is_subsumed_by_the_absolute`,
// `a_delta_never_advances_the_last_write_wins_timestamp`) and the first record's `monotonic`
// (`sum_merge_carries_the_first_records_monotonic_flag`).

/// What one series emits from `kinds` at `timestamps`, and the records `process` forwarded.
fn run_order(agg: Aggregator, records: &[(MetricKind, i64)]) -> (Vec<MetricKind>, Vec<MetricKind>) {
    let mut agg = agg;
    let resource = Arc::new(Resource::default());
    let mut forwarded = Vec::new();
    for (kind, ts) in records {
        let mut event =
            Event::metric(*ts, AttrMap::new(), MetricRecord::new(intern("m"), kind.clone()));
        agg.process(&resource, &mut event);
        forwarded.extend(event.metrics.into_iter().map(|m| m.kind));
    }
    let emitted = agg
        .flush(100)
        .into_iter()
        .flat_map(|(_, _, events)| events)
        .map(|(mut event, _)| event.metrics.remove(0).kind)
        .collect();
    (emitted, forwarded)
}

#[test]
fn a_gauge_tie_goes_to_the_later_arrival() {
    for (first, second) in [(1.0, 2.0), (2.0, 1.0)] {
        let records = [(MetricKind::Gauge(first), 3), (MetricKind::Gauge(second), 3)];
        let (emitted, _) = run_order(Aggregator::new(Duration::from_secs(10)), &records);
        assert_eq!(emitted, vec![MetricKind::Gauge(second)]);
    }
}

#[test]
fn a_kind_conflict_keeps_the_first_arrival() {
    let (gauge, counter) = (MetricKind::Gauge(1.0), MetricKind::counter(2.0));
    for (first, second) in [(gauge.clone(), counter.clone()), (counter, gauge)] {
        let records = [(first.clone(), 0), (second.clone(), 0)];
        let (emitted, forwarded) = run_order(Aggregator::new(Duration::from_secs(10)), &records);
        assert_eq!((emitted, forwarded), (vec![first], vec![second]));
    }
}

#[test]
fn a_histogram_bounds_mismatch_keeps_the_first_arrival() {
    let histogram = |bound: f64| {
        MetricKind::Histogram(logit_core::Histogram {
            buckets: vec![(bound, 1), (f64::INFINITY, 0)],
            temporality: Temporality::Delta,
            sum: None,
            min: None,
            max: None,
        })
    };
    for (first, second) in [(1.0, 2.0), (2.0, 1.0)] {
        let agg = Aggregator::new(Duration::from_secs(10))
            .with_temporality(AggregateTemporality::Cumulative)
            .with_series_retention(1, 1);
        let (emitted, forwarded) = run_order(agg, &[(histogram(first), 0), (histogram(second), 0)]);
        let MetricKind::Histogram(h) = &emitted[0] else { panic!("{emitted:?}") };
        assert_eq!(h.buckets[0], (first, 1));
        assert_eq!(forwarded, vec![histogram(second)]);
    }
}

/// A series opens with an empty sketch that adopts the first record's mapping; a later record
/// under another mapping is re-binned into it. `Mapping::logarithmic` is what Datadog APM stats
/// carry.
#[test]
fn a_sketch_series_takes_the_first_records_mapping() {
    let logarithmic = logit_core::Mapping::logarithmic(1.02, 0.0, 2048);
    let sketch = |mapping: logit_core::Mapping| {
        let mut sketch = logit_core::DdSketch::with_mapping(mapping);
        for v in [1.0, 10.0, 100.0] {
            sketch.add(v);
        }
        MetricKind::Distribution(sketch)
    };
    let agent = logit_core::Mapping::agent();
    for (first, second) in [(agent, logarithmic), (logarithmic, agent)] {
        let records = [(sketch(first), 0), (sketch(second), 0)];
        let (emitted, _) = run_order(Aggregator::new(Duration::from_secs(10)), &records);
        let MetricKind::Distribution(merged) = &emitted[0] else { panic!("{emitted:?}") };
        assert_eq!(*merged.mapping(), first);
        assert_eq!(merged.count(), 6);
    }
}

/// Finding G: a cumulative histogram's `sum` that one window lacked stays `None` for the rest of
/// the series' life, because a later window's sum can't restore the missing contribution.
#[test]
fn a_histogram_sum_once_none_stays_none_across_windows() {
    let histogram = |sum: Option<f64>| {
        let kind = MetricKind::Histogram(logit_core::Histogram {
            buckets: vec![(1.0, 1), (f64::INFINITY, 0)],
            temporality: Temporality::Delta,
            sum,
            min: None,
            max: None,
        });
        Event::metric(0, AttrMap::new(), MetricRecord::new(intern("h"), kind))
    };
    let resource = Arc::new(Resource::default());
    let mut agg = Aggregator::new(Duration::from_secs(10))
        .with_temporality(AggregateTemporality::Cumulative)
        .with_series_retention(5, 100);
    let mut sums = Vec::new();
    for (now, sum) in [(10, Some(1.0)), (20, None), (30, Some(2.0))] {
        agg.process(&resource, &mut histogram(sum));
        let (_, _, mut events) = agg.flush(now).remove(0);
        let (mut event, _) = events.remove(0);
        let MetricKind::Histogram(h) = event.metrics.remove(0).kind else { panic!() };
        sums.push(h.sum);
    }
    assert_eq!(sums, vec![Some(1.0), None, None]);
}
