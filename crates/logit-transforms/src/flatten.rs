//! `flatten`: rewrites a nested `Value::Map`/`Value::Array` attribute into flat, dot-joined keys:
//! `{"foo": {"key": "bar"}}` becomes `foo.key = "bar"`, `{"tags": ["a","b"]}` becomes
//! `tags.0`/`tags.1`, and the two compose (`{"items": [{"name": "x"}]}` becomes `items.0.name`).
//! See `docs/adr/flatten-transform.md`.
//!
//! **Never deletes an attribute**: every source value it removes is written back, as leaves or
//! whole. A key collision is last write wins, with no diagnostic. There is no cap on how many keys
//! one value expands into; [`MAX_DEPTH`] is the only bound (`docs/known-gaps.md`).
//!
//! No flush state; it reuses per-instance [`Scratch`] buffers for the path and interned keys.

use logit_core::interner::{intern, resolve, KeyCache};
use logit_core::{AttrMap, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::fmt::Write as _;
use std::sync::Arc;

/// A stack-safety bound on recursion depth into one value, not a policy cap.
///
/// Like `logit_proto::native::value::MAX_VALUE_DEPTH`, it stops a hostile document overflowing the
/// stack; it's smaller because each level here also builds a path and mints a `Symbol`. The
/// deepest shape `docs/design/data-shapes.md` measured is 5, so it refuses nothing observed. A
/// value that hits it is written back whole at the path reached, never half-expanded, and counted
/// as `logit.transform.values.unflattened{reason="max_depth"}`.
const MAX_DEPTH: usize = 32;

/// Mirrors `logit_config::FlattenArrays`; `logit-cli` converts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrays {
    Index,
    Skip,
}

/// Which top-level attributes (or resource attributes) to expand.
///
/// Mirrors `logit_config::FlattenFields`/`FlattenKeyword`; compiled once into [`CompiledFields`].
#[derive(Debug, Clone)]
pub enum Fields {
    All,
    None,
    Named(Vec<String>),
}

/// [`Fields`] with named entries interned once, in [`Flatten::new`].
enum CompiledFields {
    All,
    None,
    /// Config order, scanned linearly: a handful of names beats a set.
    Named(Vec<Symbol>),
}

impl CompiledFields {
    fn compile(fields: Fields) -> Self {
        match fields {
            Fields::All => CompiledFields::All,
            Fields::None => CompiledFields::None,
            Fields::Named(names) => {
                CompiledFields::Named(names.iter().map(|n| intern(n)).collect())
            }
        }
    }

    fn selects(&self, key: Symbol) -> bool {
        match self {
            CompiledFields::All => true,
            CompiledFields::None => false,
            CompiledFields::Named(names) => names.contains(&key),
        }
    }

    fn selects_nothing(&self) -> bool {
        matches!(self, CompiledFields::None)
    }
}

/// Buffers reused across every event and batch; no reallocation in steady state.
#[derive(Default)]
struct Scratch {
    /// This call's selected top-level entries, owned: all are taken out of the map before any is
    /// expanded (see [`flatten_map`]). Selecting and taking are two passes over this one buffer,
    /// with a `Value::Null` stand-in between, so it costs no more than collecting bare `Symbol`s.
    pending: Vec<(Symbol, Value)>,
    /// The current path, appended to and truncated back (`push`/`truncate`, not `format!`), so
    /// nothing allocates below its high-water mark.
    path: String,
    /// `path -> Symbol` memo; a stable input shape repeats the same paths in the same order.
    keys: KeyCache,
}

pub struct Flatten {
    attribute_fields: CompiledFields,
    resource_fields: CompiledFields,
    arrays: Arrays,
    scratch: Scratch,
    /// The last `(input, output)` resource pair, matched by `Arc::ptr_eq` on the input.
    cache: Option<(Arc<Resource>, Arc<Resource>)>,
    telemetry: Telemetry,
}

impl Flatten {
    pub fn new(attributes: Fields, resource: Fields, arrays: Arrays) -> Self {
        Self {
            attribute_fields: CompiledFields::compile(attributes),
            resource_fields: CompiledFields::compile(resource),
            arrays,
            scratch: Scratch::default(),
            cache: None,
            telemetry: Telemetry::default(),
        }
    }

    /// Attaches a telemetry handle; flattening a decoded value can't fail, so no `Diagnostics`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

/// Whether a top-level value gets removed and expanded: a non-empty `Map`, or a non-empty `Array`
/// under `arrays: index`. Anything else stays where it is, uncounted.
fn expandable(value: &Value, arrays: Arrays) -> bool {
    match value {
        Value::Map(m) => !m.is_empty(),
        Value::Array(a) => arrays == Arrays::Index && !a.is_empty(),
        _ => false,
    }
}

/// Expands every selected, expandable top-level attribute in `attrs` in place, for both event and
/// resource attributes.
///
/// Three phases, because `AttrMap` has no `iter_mut`/`retain`/`drain` and can't be mutated while
/// iterated: select (read only), take every selected value out, then expand each.
///
/// Taking all selected values before expanding any keeps the result independent of
/// [`AttrMap::iter`]'s order, which is process-global intern order, not document order. With a
/// nested `a = {"b": 9}` and a literal sibling `a.b = {"x": 1}`, expanding `a` first would
/// overwrite `a.b`'s subtree before it was walked. Taken up front, both land side by side
/// (`a.b` and `a.b.x`) in either order. Two expansions producing the same leaf path still
/// collide, last write wins (`docs/adr/flatten-transform.md`).
fn flatten_map(
    attrs: &mut AttrMap,
    fields: &CompiledFields,
    arrays: Arrays,
    scratch: &mut Scratch,
    telemetry: &Telemetry,
) {
    scratch.pending.clear();
    // Phase 1: select. `Value::Null` stands in until phase 2 takes the value; a real `Null` is
    // never `expandable`, so it can't be confused with a stand-in.
    for (sym, value) in attrs.iter() {
        if fields.selects(sym) && expandable(value, arrays) {
            scratch.pending.push((sym, Value::Null));
        }
    }
    // Phase 2: take, before phase 3 writes anything. An entry the map no longer holds is dropped
    // from `pending` rather than panicking.
    scratch.pending.retain_mut(|entry| match attrs.remove_sym(entry.0) {
        Some(value) => {
            entry.1 = value;
            true
        }
        None => false,
    });
    // Phase 3: expand, by index, because `expand` needs `scratch` while `pending` is in it.
    for i in 0..scratch.pending.len() {
        let sym = scratch.pending[i].0;
        let value = std::mem::replace(&mut scratch.pending[i].1, Value::Null);
        scratch.path.clear();
        scratch.path.push_str(resolve(sym));
        expand(value, 1, arrays, attrs, scratch, telemetry);
    }
}

/// Writes `value` into `attrs` at `scratch.path`, recursing into a non-empty `Map`/`Array` (index
/// mode) up to [`MAX_DEPTH`].
///
/// A leaf, an empty container, or a value at the depth bound is written back whole.
fn expand(
    value: Value,
    depth: usize,
    arrays: Arrays,
    attrs: &mut AttrMap,
    scratch: &mut Scratch,
    telemetry: &Telemetry,
) {
    match value {
        Value::Map(map) if !map.is_empty() => {
            if depth >= MAX_DEPTH {
                write_unexpanded(Value::Map(map), attrs, scratch, telemetry);
                return;
            }
            for (key, child) in map.into_pairs() {
                let mark = scratch.path.len();
                scratch.path.push('.');
                scratch.path.push_str(resolve(key));
                expand(child, depth + 1, arrays, attrs, scratch, telemetry);
                scratch.path.truncate(mark);
            }
        }
        Value::Array(items) if arrays == Arrays::Index && !items.is_empty() => {
            if depth >= MAX_DEPTH {
                write_unexpanded(Value::Array(items), attrs, scratch, telemetry);
                return;
            }
            for (i, child) in items.into_iter().enumerate() {
                let mark = scratch.path.len();
                scratch.path.push('.');
                // Straight into `path`; `format!("{i}")` would allocate a `String`.
                let _ = write!(scratch.path, "{i}");
                expand(child, depth + 1, arrays, attrs, scratch, telemetry);
                scratch.path.truncate(mark);
            }
        }
        // A non-container value, an empty `Map`/`Array`, or an `Array` under `arrays: skip`.
        leaf => {
            let sym = scratch.keys.get_or_intern(&scratch.path);
            attrs.insert_sym(sym, leaf);
            telemetry.count("logit.transform.values.flattened", 1.0, &[]);
        }
    }
}

fn write_unexpanded(
    value: Value,
    attrs: &mut AttrMap,
    scratch: &mut Scratch,
    telemetry: &Telemetry,
) {
    let sym = scratch.keys.get_or_intern(&scratch.path);
    attrs.insert_sym(sym, value);
    telemetry.count("logit.transform.values.unflattened", 1.0, &[("reason", "max_depth")]);
}

impl Transform for Flatten {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        flatten_map(
            &mut event.attributes,
            &self.attribute_fields,
            self.arrays,
            &mut self.scratch,
            &self.telemetry,
        );
        true
    }

    fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> {
        if self.resource_fields.selects_nothing() {
            return None;
        }
        if let Some((cached_in, cached_out)) = &self.cache {
            if Arc::ptr_eq(cached_in, resource) {
                return Some(cached_out.clone());
            }
        }
        let mut attrs = resource.attributes.clone();
        flatten_map(
            &mut attrs,
            &self.resource_fields,
            self.arrays,
            &mut self.scratch,
            &self.telemetry,
        );
        // Carry `dropped_attributes_count`/`schema_url` over, as `Set::map_resource` does.
        let out = Arc::new(Resource {
            attributes: attrs,
            dropped_attributes_count: resource.dropped_attributes_count,
            schema_url: resource.schema_url.clone(),
        });
        self.cache = Some((resource.clone(), out.clone()));
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve as resolve_sym;
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, Registry};

    fn event_with_attrs(pairs: &[(&str, Value)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, v.clone());
        }
        Event::log(
            0,
            attrs,
            LogRecord {
                message: Value::str("msg"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn map(pairs: &[(&str, Value)]) -> Value {
        let mut m = AttrMap::new();
        for (k, v) in pairs {
            m.insert(k, v.clone());
        }
        Value::Map(Box::new(m))
    }

    fn flatten_all() -> Flatten {
        Flatten::new(Fields::All, Fields::None, Arrays::Index)
    }

    // -- core shape ---------------------------------------------------------------------------

    #[test]
    fn a_nested_map_becomes_dotted_keys() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event = event_with_attrs(&[("foo", map(&[("key", Value::str("bar"))]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("foo"), None, "the source attribute is consumed");
        assert_eq!(event.attributes.get("foo.key"), Some(&Value::str("bar")));
    }

    #[test]
    fn the_source_attribute_is_removed_once_its_leaves_are_written() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event = event_with_attrs(&[
            ("foo", map(&[("key", Value::str("bar"))])),
            ("status", Value::I64(200)),
        ]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.len(), 2, "foo.key plus the untouched status");
        assert_eq!(event.attributes.get("status"), Some(&Value::I64(200)));
    }

    #[test]
    fn an_array_flattens_by_index() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event =
            event_with_attrs(&[("tags", Value::Array(vec![Value::str("a"), Value::str("b")]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("tags.0"), Some(&Value::str("a")));
        assert_eq!(event.attributes.get("tags.1"), Some(&Value::str("b")));
        assert_eq!(event.attributes.get("tags"), None);
    }

    #[test]
    fn nesting_and_arrays_compose() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event =
            event_with_attrs(&[("items", Value::Array(vec![map(&[("name", Value::str("x"))])]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("items.0.name"), Some(&Value::str("x")));
    }

    #[test]
    fn recursion_composes_at_depth_three() {
        let mut f = flatten_all();
        let resource = default_resource();
        let inner = map(&[("c", Value::I64(1))]);
        let mut middle = AttrMap::new();
        middle.insert("b", inner);
        let mut event = event_with_attrs(&[("a", Value::Map(Box::new(middle)))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("a.b.c"), Some(&Value::I64(1)));
    }

    #[test]
    fn flattening_is_idempotent() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event = event_with_attrs(&[("foo", map(&[("key", Value::str("bar"))]))]);
        assert!(f.process(&resource, &mut event));
        let first: Vec<(&str, Value)> =
            event.attributes.iter().map(|(k, v)| (resolve_sym(k), v.clone())).collect();
        assert!(f.process(&resource, &mut event), "nothing nested left to select");
        let second: Vec<(&str, Value)> =
            event.attributes.iter().map(|(k, v)| (resolve_sym(k), v.clone())).collect();
        assert_eq!(first, second);
    }

    #[test]
    fn a_flat_event_fires_no_counter() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("flat", "flatten", "transform");
        let mut f = flatten_all().with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("status", Value::I64(200))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("status"), Some(&Value::I64(200)));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.values.flattened"), None);
    }

    // -- leaf rule ------------------------------------------------------------------------------

    #[test]
    fn an_empty_map_attribute_is_left_untouched() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event = event_with_attrs(&[("foo", map(&[]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("foo"), Some(&map(&[])));
    }

    #[test]
    fn an_empty_array_attribute_is_left_untouched() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event = event_with_attrs(&[("tags", Value::Array(vec![]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("tags"), Some(&Value::Array(vec![])));
    }

    #[test]
    fn an_empty_container_nested_inside_is_written_back_as_a_leaf() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", map(&[("b", map(&[])), ("c", Value::I64(1))]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("a.b"), Some(&map(&[])), "b's existence survives");
        assert_eq!(event.attributes.get("a.c"), Some(&Value::I64(1)));
    }

    #[test]
    fn arrays_skip_treats_a_nested_array_as_a_leaf() {
        let mut f = Flatten::new(Fields::All, Fields::None, Arrays::Skip);
        let resource = default_resource();
        let tags = Value::Array(vec![Value::I64(1), Value::I64(2)]);
        let mut event = event_with_attrs(&[("a", map(&[("tags", tags.clone())]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("a.tags"), Some(&tags));
    }

    #[test]
    fn arrays_skip_leaves_a_top_level_array_untouched() {
        let mut f = Flatten::new(Fields::All, Fields::None, Arrays::Skip);
        let resource = default_resource();
        let tags = Value::Array(vec![Value::str("a"), Value::str("b")]);
        let mut event = event_with_attrs(&[("tags", tags.clone())]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("tags"), Some(&tags));
    }

    // -- depth wall -------------------------------------------------------------------------

    #[test]
    fn a_value_deeper_than_max_depth_is_written_back_whole_and_counted() {
        let mut value = Value::I64(1);
        for i in 0..(MAX_DEPTH + 2) {
            let mut m = AttrMap::new();
            m.insert(&format!("l{i}"), value);
            value = Value::Map(Box::new(m));
        }
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("flat", "flatten", "transform");
        let mut f = flatten_all().with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("deep", value)]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("deep"), None);
        assert_eq!(event.attributes.len(), 1, "exactly one attribute survives, wherever it landed");

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.values.unflattened"), Some(1.0));
    }

    // -- selection ------------------------------------------------------------------------------

    #[test]
    fn a_named_list_restricts_which_top_level_attributes_expand() {
        let mut f =
            Flatten::new(Fields::Named(vec!["http".to_string()]), Fields::None, Arrays::Index);
        let resource = default_resource();
        let mut event = event_with_attrs(&[
            ("http", map(&[("status", Value::I64(200))])),
            ("other", map(&[("x", Value::I64(1))])),
        ]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("http.status"), Some(&Value::I64(200)));
        assert_eq!(event.attributes.get("other"), Some(&map(&[("x", Value::I64(1))])));
    }

    #[test]
    fn a_field_name_is_a_literal_attribute_name_not_a_path() {
        let mut f = Flatten::new(
            Fields::Named(vec!["http.status".to_string()]),
            Fields::None,
            Arrays::Index,
        );
        let resource = default_resource();
        // "http.status" as a literal top-level attribute, holding a nested value of its own.
        let mut event = event_with_attrs(&[("http.status", map(&[("code", Value::I64(200))]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("http.status.code"), Some(&Value::I64(200)));
    }

    #[test]
    fn a_configured_field_the_event_lacks_is_a_silent_no_op() {
        let mut f =
            Flatten::new(Fields::Named(vec!["http".to_string()]), Fields::None, Arrays::Index);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("status", Value::I64(200))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("status"), Some(&Value::I64(200)));
    }

    #[test]
    fn attributes_none_expands_nothing() {
        let mut f = Flatten::new(Fields::None, Fields::None, Arrays::Index);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("foo", map(&[("key", Value::str("bar"))]))]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("foo"), Some(&map(&[("key", Value::str("bar"))])));
    }

    // -- collision --------------------------------------------------------------------------

    #[test]
    fn a_flattened_key_overwrites_an_existing_attribute_last_write_wins() {
        let mut f = flatten_all();
        let resource = default_resource();
        let mut event = event_with_attrs(&[
            ("http", map(&[("status", Value::I64(200))])),
            ("http.status", Value::I64(999)),
        ]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(
            event.attributes.get("http.status"),
            Some(&Value::I64(200)),
            "the flattened leaf, written after the literal attribute, wins"
        );
    }

    // -- dotted siblings ----------------------------------------------------------------------

    /// With the nested key interned first, expanding `fsib1` into `fsib1.b` doesn't destroy the
    /// literal `fsib1.b` sibling's subtree. The names are unique to this test, so it controls
    /// intern (and so iteration) order.
    #[test]
    fn a_nested_attribute_and_a_dotted_sibling_both_survive() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("flat", "flatten", "transform");
        let mut f = flatten_all().with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[
            ("fsib1", map(&[("b", Value::I64(9))])),
            ("fsib1.b", map(&[("x", Value::I64(1))])),
        ]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("fsib1.b"), Some(&Value::I64(9)));
        assert_eq!(
            event.attributes.get("fsib1.b.x"),
            Some(&Value::I64(1)),
            "the sibling's own subtree must survive the leaf written over its key"
        );
        assert_eq!(event.attributes.len(), 2);

        let events = registry.drain(0);
        assert_eq!(
            counter_value(&events, "logit.transform.values.flattened"),
            Some(2.0),
            "one count per leaf written, not one per source key removed"
        );
    }

    /// `a_nested_attribute_and_a_dotted_sibling_both_survive` with the intern order reversed.
    #[test]
    fn the_same_dotted_sibling_pair_survives_in_the_other_intern_order() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("flat", "flatten", "transform");
        let mut f = flatten_all().with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[
            ("fsib2.b", map(&[("x", Value::I64(1))])),
            ("fsib2", map(&[("b", Value::I64(9))])),
        ]);
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.attributes.get("fsib2.b"), Some(&Value::I64(9)));
        assert_eq!(event.attributes.get("fsib2.b.x"), Some(&Value::I64(1)));
        assert_eq!(event.attributes.len(), 2);

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.values.flattened"), Some(2.0));
    }

    // -- resource -----------------------------------------------------------------------------

    #[test]
    fn resource_attributes_are_untouched_by_default() {
        let mut f = flatten_all();
        assert!(f.map_resource(&default_resource()).is_none());
    }

    #[test]
    fn resource_attributes_flatten_when_configured() {
        let mut f = Flatten::new(Fields::None, Fields::All, Arrays::Index);
        let mut attrs = AttrMap::new();
        attrs.insert("k8s", map(&[("pod", Value::str("web-1"))]));
        let resource = Arc::new(Resource { attributes: attrs, ..Resource::default() });
        let mapped = f.map_resource(&resource).expect("resource: all is non-empty selection");
        assert_eq!(mapped.attributes.get("k8s.pod"), Some(&Value::str("web-1")));
    }

    #[test]
    fn map_resource_carries_dropped_attributes_count_and_schema_url_forward() {
        let mut f = Flatten::new(Fields::None, Fields::All, Arrays::Index);
        let resource = Arc::new(Resource {
            attributes: AttrMap::new(),
            dropped_attributes_count: 3,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/schema")),
        });
        let mapped = f.map_resource(&resource).unwrap();
        assert_eq!(mapped.dropped_attributes_count, 3);
        assert_eq!(mapped.schema_url, resource.schema_url);
    }

    #[test]
    fn map_resource_is_cached_by_arc_ptr_eq() {
        let mut f = Flatten::new(Fields::None, Fields::All, Arrays::Index);
        let mut attrs = AttrMap::new();
        attrs.insert("k8s", map(&[("pod", Value::str("web-1"))]));
        let resource = Arc::new(Resource { attributes: attrs, ..Resource::default() });
        let first = f.map_resource(&resource).unwrap();
        let second = f.map_resource(&resource).unwrap();
        assert!(Arc::ptr_eq(&first, &second), "an unchanged input Arc must hit the cache");
    }

    #[test]
    fn map_resource_returns_none_when_resource_is_none() {
        let mut f = flatten_all();
        assert!(f.map_resource(&default_resource()).is_none());
    }

    // -- non-interference -----------------------------------------------------------------------

    #[test]
    fn log_metrics_and_span_payloads_are_untouched() {
        let resource = default_resource();
        let mut event = event_with_attrs(&[("foo", map(&[("key", Value::str("bar"))]))]);
        event
            .metrics
            .push(MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)));
        let original_message = event.log.as_ref().unwrap().message.clone();

        let mut f = flatten_all();
        assert!(f.process(&resource, &mut event));
        assert_eq!(event.metrics.len(), 1, "flatten must not touch metrics");
        assert_eq!(event.log.as_ref().unwrap().message, original_message);
    }

    // -- telemetry ------------------------------------------------------------------------------

    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve_sym(m.name) == name => Some(sum.value),
                _ => None,
            })
        })
    }

    #[test]
    fn every_counter_fires_with_the_documented_name() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("flat", "flatten", "transform");
        let mut f = flatten_all().with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("foo", map(&[("key", Value::str("bar"))]))]);
        assert!(f.process(&resource, &mut event));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.values.flattened"), Some(1.0));
    }
}
