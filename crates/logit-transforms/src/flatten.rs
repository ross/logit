//! `flatten`: rewrites a nested `Value::Map`/`Value::Array` attribute into flat, dot-joined keys
//! -- `{"foo": {"key": "bar"}}` becomes `foo.key = "bar"`, `{"tags": ["a","b"]}` becomes
//! `tags.0`/`tags.1`, and the two compose (`{"items": [{"name": "x"}]}` becomes `items.0.name`).
//! See `docs/adr/flatten-transform.md`.
//!
//! Effectively stateless -- like `keep_values`/`scale`, only `process`/`map_resource` are
//! overridden, and `flush_interval`/`flush` keep the `Transform` trait's defaults -- but unlike
//! them it carries reused per-instance scratch buffers ([`Scratch`]) rather than nothing at all,
//! because the walk itself needs somewhere to build a path and memoize interned keys across
//! events. `json`'s `JsonParser` is the model for both, applied here to a walk over an
//! already-parsed `Value` rather than a parser's own input.

use logit_core::interner::{intern, resolve, KeyCache};
use logit_core::{AttrMap, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::fmt::Write as _;
use std::sync::Arc;

/// A hard stack-safety bound on how deep `flatten` will descend into one nested value -- not a
/// policy cap (there is deliberately no `max_keys`-style config knob bounding how many keys one
/// value may expand into; see `docs/adr/flatten-transform.md`'s Consequences). Mirrors
/// `logit_proto::native::value::MAX_VALUE_DEPTH`'s reasoning (a decode-time depth cap exists for
/// the same reason: unbounded recursion on a hostile or self-similar document is a stack
/// overflow) at a smaller depth -- this walk also builds a path string and mints a `Symbol` per
/// level, so it costs more per level than a decode-only walk, and the deepest shape
/// `docs/design/data-shapes.md`'s survey measured is 5 (CloudTrail's `userIdentity` chain), so
/// this refuses nothing observed. A value that hits the wall is written back whole, still nested,
/// at the path reached -- never half-expanded -- and counted
/// `logit.transform.values.unflattened{reason="max_depth"}`.
const MAX_DEPTH: usize = 32;

/// Mirrors `logit_config::FlattenArrays` -- `logit-transforms` deliberately doesn't depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so the CLI converts, the same
/// pattern `Normalize`/`MatchMode` already follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arrays {
    Index,
    Skip,
}

/// Which top-level attributes (or resource attributes) to expand -- mirrors
/// `logit_config::FlattenFields`/`FlattenKeyword`, the config-facing shape [`Flatten::new`]
/// takes. Compiled once, at construction, into [`CompiledFields`] -- a named entry costs an
/// `intern` only the one time the component is built, never per event.
#[derive(Debug, Clone)]
pub enum Fields {
    All,
    None,
    Named(Vec<String>),
}

/// [`Fields`], compiled: a named entry's `Symbol`s, interned once at construction
/// ([`Flatten::new`]) -- [`crate::KeepValues`]'s `Clamp` reasoning applied to field selection
/// instead of a value allow-list.
enum CompiledFields {
    All,
    None,
    /// Config order, not sorted -- a linear scan beats a set at the sizes this is configured for
    /// (a handful of named attributes), the same call `Clamp::allow` already makes.
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

/// Reused across every event/batch this component sees, never reallocated in steady state.
#[derive(Default)]
struct Scratch {
    /// Top-level keys selected for expansion this call, lifted out before any mutation --
    /// `AttrMap` has no `iter_mut`/`retain`, and can't be mutated while `iter()` borrows it.
    pending: Vec<Symbol>,
    /// One buffer for the whole walk; a child path is built by appending onto the parent's and
    /// truncated back on the way out (`push`/`truncate`, not `format!`), so no allocation happens
    /// below the buffer's high-water mark.
    path: String,
    /// `path -> Symbol` memo. A stable input shape produces the same paths in the same order on
    /// every event, which is exactly `KeyCache`'s steady-state case.
    keys: KeyCache,
}

pub struct Flatten {
    attribute_fields: CompiledFields,
    resource_fields: CompiledFields,
    arrays: Arrays,
    scratch: Scratch,
    /// A one-entry cache of the last resource this component mapped, keyed by `Arc::ptr_eq` on
    /// the input -- [`crate::KeepValues::map_resource`]'s caching idiom.
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

    /// Attaches a telemetry handle -- see [`crate::Keep::with_telemetry`] for why there's no
    /// `Diagnostics` builder alongside it: flattening an already-decoded value can't fail.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

/// Whether a value is worth removing and re-expanding at all -- a non-empty `Map`, or a non-empty
/// `Array` when arrays expand by index. Everything else (a scalar, an empty container, or an
/// array under `arrays: skip`) is left exactly where it is, untouched and uncounted.
fn expandable(value: &Value, arrays: Arrays) -> bool {
    match value {
        Value::Map(m) => !m.is_empty(),
        Value::Array(a) => arrays == Arrays::Index && !a.is_empty(),
        _ => false,
    }
}

/// Expands every selected, expandable top-level attribute in `attrs` in place. Shared by
/// `process` (`event.attributes`) and `map_resource` (a rebuilt `Resource`'s attributes).
///
/// Two-phase, because `AttrMap` has no `iter_mut`/`retain`/`drain` and can't be mutated while
/// iterated: phase 1 only *reads* `attrs` to decide which top-level keys to expand, phase 2
/// removes and expands each in turn.
fn flatten_map(
    attrs: &mut AttrMap,
    fields: &CompiledFields,
    arrays: Arrays,
    scratch: &mut Scratch,
    telemetry: &Telemetry,
) {
    scratch.pending.clear();
    for (sym, value) in attrs.iter() {
        if fields.selects(sym) && expandable(value, arrays) {
            scratch.pending.push(sym);
        }
    }
    for i in 0..scratch.pending.len() {
        let sym = scratch.pending[i];
        let value = attrs.remove_sym(sym).expect("selected from this map in phase 1, above");
        scratch.path.clear();
        scratch.path.push_str(resolve(sym));
        expand(value, 1, arrays, attrs, scratch, telemetry);
    }
}

/// Recursively writes `value` into `attrs` at `scratch.path`, descending into a `Map`/`Array`
/// (index mode) up to [`MAX_DEPTH`], and writing everything else -- a genuine leaf, an empty
/// container, or a value that hit the depth wall -- back whole at the path reached.
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
                // Writes the decimal digits straight into the warm `path` buffer -- no
                // intermediate `String` the way `format!("{i}")` would allocate one.
                let _ = write!(scratch.path, "{i}");
                expand(child, depth + 1, arrays, attrs, scratch, telemetry);
                scratch.path.truncate(mark);
            }
        }
        // A genuine leaf (any non-container value), an empty `Map`/`Array` reached mid-walk, or
        // (via the two `MAX_DEPTH` arms above) a non-empty container that hit the depth wall --
        // all three are written back whole at the current path.
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
        // `dropped_attributes_count`/`schema_url` aren't configurable through `flatten` -- carry
        // them over from the input resource explicitly, `KeepValues::map_resource`'s reasoning
        // exactly.
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
        // Build a chain nested MAX_DEPTH+2 levels deep so the wall is guaranteed to bite.
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
        // The top-level attribute was removed and rewritten somewhere -- not left at "deep".
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
