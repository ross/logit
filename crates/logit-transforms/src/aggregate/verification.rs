//! Property tests for `aggregate` against a reference written independently of it, for cluster 8
//! of `docs/plans/critical-sections-inventory.md`. The contract they check is
//! `docs/adr/aggregation-window-semantics.md`'s "Amendment: series identity, merge laws, and
//! accounting as a stated contract" section.
//!
//! Covered so far: XFORM-01, series identity. `SeriesKey` equality and hashing agree with
//! [`ref_eq`], a structural comparison that never calls `value_key_eq` or `hash_value`, and
//! `Aggregator` partitions records into series and `(resource, scope)` groups the way that
//! reference does. XFORM-02 to XFORM-04 extend this module with a reference model and reuse its
//! strategies.
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
#[derive(Debug)]
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
