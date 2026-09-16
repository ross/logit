//! `keep_values`: clamps attribute (and/or resource-attribute) values to an operator-configured
//! allow-list, per field -- `keep`'s value-side sibling, for a tag whose valid set the operator
//! knows but the producer doesn't enforce. See `docs/adr/value-allowlist-cardinality-clamp.md`.
//!
//! Stateless -- like `scale`/`set`, only `process`/`map_resource` are overridden;
//! `flush_interval`/`flush` keep the `Transform` trait's defaults.

use crate::value_matches;
use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Mirrors `logit_config::NormalizeStep` -- `logit-transforms` deliberately doesn't depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so the CLI converts, the same
/// pattern `MatchMode`/`SignalSet` already follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Normalize {
    Lower,
}

/// One field's clamp, interned once at construction ([`KeepValues::new`]) rather than per event --
/// [`Scale`](crate::Scale)'s `CompiledScale` reasoning applied here too.
struct Clamp {
    field: Symbol,
    normalize: Vec<Normalize>,
    /// Config order, not sorted -- a linear scan beats a set at the sizes this is configured for
    /// (a handful of vhosts, tenants, regions), and allocates nothing to build or to search.
    allow: Vec<Value>,
    other: Option<Value>,
}

impl Clamp {
    fn new(
        field: String,
        normalize: Vec<Normalize>,
        allow: Vec<Value>,
        other: Option<Value>,
    ) -> Self {
        Self { field: intern(&field), normalize, allow, other }
    }

    /// Applies every configured step in order, returning `None` if none of them actually changed
    /// the value -- the common case (an already-conforming value), and what keeps that path
    /// allocation-free: a caller that gets `None` back knows there is nothing new to write back.
    fn normalize(&self, value: &Value) -> Option<Value> {
        let mut current: Option<Value> = None;
        for step in &self.normalize {
            let input = current.as_ref().unwrap_or(value);
            if let Some(next) = step.apply(input) {
                current = Some(next);
            }
        }
        current
    }
}

impl Normalize {
    /// `None` if this step doesn't change `value` -- either because the variant never applies
    /// (anything but `Str`/`Bytes`) or because it's already in the target form.
    fn apply(self, value: &Value) -> Option<Value> {
        match self {
            Normalize::Lower => lower(value),
        }
    }
}

/// ASCII-lowercases a `Str`/`Bytes` value bytewise, returning `None` if it has no uppercase ASCII
/// byte to change (including every non-`Str`/`Bytes` variant). Bytewise, not
/// `str::to_lowercase`'s Unicode case folding -- see the ADR: it can't change a string's byte
/// length (so `Str`'s UTF-8 validity is preserved for free) and it's total on non-UTF-8 `Bytes`
/// with no separate validation step.
fn lower(value: &Value) -> Option<Value> {
    let bytes = match value {
        Value::Str(b) | Value::Bytes(b) => b,
        _ => return None,
    };
    if !bytes.iter().any(u8::is_ascii_uppercase) {
        return None;
    }
    let lowered: Vec<u8> = bytes.iter().map(u8::to_ascii_lowercase).collect();
    Some(match value {
        Value::Str(_) => Value::Str(Bytes::from(lowered)),
        Value::Bytes(_) => Value::Bytes(Bytes::from(lowered)),
        _ => unreachable!("checked above"),
    })
}

/// Config for one field, taken by [`KeepValues::new`] -- `(field, normalize steps, allow list,
/// other)`, `Set::new`'s plain-tuple convention.
pub type ClampConfig = (String, Vec<Normalize>, Vec<Value>, Option<Value>);

pub struct KeepValues {
    resource_fields: Vec<Clamp>,
    attribute_fields: Vec<Clamp>,
    /// A one-entry cache of the last resource this component mapped, keyed by `Arc::ptr_eq` on
    /// the input -- [`Set`](crate::Set)'s `map_resource` caching idiom, applied to a clamp instead
    /// of a stamp.
    cache: Option<(Arc<Resource>, Arc<Resource>)>,
    telemetry: Telemetry,
}

impl KeepValues {
    pub fn new(resource: Vec<ClampConfig>, attributes: Vec<ClampConfig>) -> Self {
        let compile = |fields: Vec<ClampConfig>| {
            fields
                .into_iter()
                .map(|(field, normalize, allow, other)| Clamp::new(field, normalize, allow, other))
                .collect()
        };
        Self {
            resource_fields: compile(resource),
            attribute_fields: compile(attributes),
            cache: None,
            telemetry: Telemetry::default(),
        }
    }

    /// Attaches a telemetry handle -- see
    /// [`Keep::with_telemetry`](crate::Keep::with_telemetry) for why there's no `Diagnostics`
    /// builder alongside it: clamping a value to a fixed allow-list can't fail.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

/// Resolves one field's clamp against `attrs`, applying it in place. Shared by `process`
/// (`event.attributes`) and `map_resource` (a rebuilt `Resource`'s attributes).
fn clamp_field(clamp: &Clamp, attrs: &mut logit_core::AttrMap, telemetry: &Telemetry) {
    let field_tag = [("field", logit_core::interner::resolve(clamp.field))];
    // Borrow ends here, before any mutation below -- `scale.rs`'s "resolve into an owned local
    // first" shape, needed because `insert_sym`/`remove_sym` need `attrs` mutably.
    let (normalized, matched) = match attrs.get_sym(clamp.field) {
        None => return, // absent: a silent no-op for this field, never a stamp
        Some(actual) => {
            let normalized = clamp.normalize(actual);
            let effective = normalized.as_ref().unwrap_or(actual);
            let matched = clamp.allow.iter().any(|allowed| value_matches(allowed, effective));
            (normalized, matched)
        }
    };
    if normalized.is_some() {
        telemetry.count("logit.transform.values.normalized", 1.0, &field_tag);
    }
    if matched {
        telemetry.count("logit.transform.values.allowed", 1.0, &field_tag);
        // Still write back a normalized-but-allowed value -- the cardinality win `normalize:`
        // exists for applies whether or not the value happened to already be on the allow-list.
        if let Some(value) = normalized {
            attrs.insert_sym(clamp.field, value);
        }
        return;
    }
    telemetry.count("logit.transform.values.clamped", 1.0, &field_tag);
    match &clamp.other {
        Some(other) => attrs.insert_sym(clamp.field, other.clone()),
        None => {
            attrs.remove_sym(clamp.field);
        }
    }
}

impl Transform for KeepValues {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        for clamp in &self.attribute_fields {
            clamp_field(clamp, &mut event.attributes, &self.telemetry);
        }
        Some(event)
    }

    fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> {
        if self.resource_fields.is_empty() {
            return None;
        }
        if let Some((cached_in, cached_out)) = &self.cache {
            if Arc::ptr_eq(cached_in, resource) {
                return Some(cached_out.clone());
            }
        }
        let mut attrs = resource.attributes.clone();
        for clamp in &self.resource_fields {
            clamp_field(clamp, &mut attrs, &self.telemetry);
        }
        // `dropped_attributes_count`/`schema_url` aren't configurable through `keep_values` --
        // carry them over from the input resource explicitly, `Set::map_resource`'s reasoning
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
    use logit_core::interner::resolve;
    use logit_core::{AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, Registry};

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

    fn allow_only(field: &str, allow: &[&str]) -> KeepValues {
        KeepValues::new(
            vec![],
            vec![(field.to_string(), vec![], allow.iter().map(|v| Value::str(*v)).collect(), None)],
        )
    }

    fn allow_with_other(field: &str, allow: &[&str], other: &str) -> KeepValues {
        KeepValues::new(
            vec![],
            vec![(
                field.to_string(),
                vec![],
                allow.iter().map(|v| Value::str(*v)).collect(),
                Some(Value::str(other)),
            )],
        )
    }

    #[test]
    fn an_allowed_value_is_untouched() {
        let mut kv = allow_only("host", &["static.local", "proxy.local"]);
        let resource = default_resource();
        let event = event_with_attrs(&[("host", Value::str("static.local"))]);
        let event = kv.process(&resource, event).expect("never drops");
        assert_eq!(event.attributes.get("host"), Some(&Value::str("static.local")));
    }

    #[test]
    fn a_disallowed_value_is_replaced_with_other() {
        let mut kv = allow_with_other("host", &["static.local"], "other");
        let resource = default_resource();
        let event = event_with_attrs(&[("host", Value::str("junk.example"))]);
        let event = kv.process(&resource, event).expect("never drops");
        assert_eq!(event.attributes.get("host"), Some(&Value::str("other")));
    }

    #[test]
    fn a_disallowed_value_is_removed_when_other_is_absent() {
        let mut kv = allow_only("host", &["static.local"]);
        let resource = default_resource();
        let event = event_with_attrs(&[("host", Value::str("junk.example"))]);
        let event = kv.process(&resource, event).expect("never drops");
        assert_eq!(event.attributes.get("host"), None);
    }

    #[test]
    fn an_absent_attribute_is_a_no_op_never_a_stamp() {
        let mut kv = allow_with_other("host", &["static.local"], "other");
        let resource = default_resource();
        let event = kv.process(&resource, event_with_attrs(&[])).expect("never drops");
        assert_eq!(event.attributes.get("host"), None, "must not invent the field");
    }

    #[test]
    fn numeric_values_coerce_across_representations() {
        let mut kv = KeepValues::new(
            vec![],
            vec![("status".to_string(), vec![], vec![Value::I64(200)], None)],
        );
        let resource = default_resource();
        let event = event_with_attrs(&[("status", Value::str("200"))]);
        let event = kv.process(&resource, event).expect("never drops");
        assert_eq!(
            event.attributes.get("status"),
            Some(&Value::str("200")),
            "a coercing match must leave the original representation untouched"
        );
    }

    #[test]
    fn bool_never_coerces() {
        let mut kv = KeepValues::new(
            vec![],
            vec![("sampled".to_string(), vec![], vec![Value::Bool(true)], None)],
        );
        let resource = default_resource();
        let event = event_with_attrs(&[("sampled", Value::str("true"))]);
        let event = kv.process(&resource, event).expect("never drops");
        assert_eq!(event.attributes.get("sampled"), None, "a bool must not match a string");
    }

    #[test]
    fn a_resource_attribute_is_clamped_through_map_resource() {
        let mut kv = KeepValues::new(
            vec![("env".to_string(), vec![], vec![Value::str("prod")], Some(Value::str("other")))],
            vec![],
        );
        let mut attrs = AttrMap::new();
        attrs.insert("env", Value::str("staging"));
        let resource = Arc::new(Resource { attributes: attrs, ..Resource::default() });
        let mapped = kv.map_resource(&resource).expect("resource_fields is non-empty");
        assert_eq!(mapped.attributes.get("env"), Some(&Value::str("other")));
    }

    #[test]
    fn map_resource_returns_none_when_resource_fields_is_empty() {
        let mut kv = allow_only("host", &["static.local"]);
        let resource = default_resource();
        assert!(kv.map_resource(&resource).is_none());
    }

    #[test]
    fn map_resource_hits_its_cache_on_the_same_arc() {
        let mut kv = KeepValues::new(
            vec![("env".to_string(), vec![], vec![Value::str("prod")], None)],
            vec![],
        );
        let mut attrs = AttrMap::new();
        attrs.insert("env", Value::str("prod"));
        let resource = Arc::new(Resource { attributes: attrs, ..Resource::default() });
        let first = kv.map_resource(&resource).unwrap();
        let second = kv.map_resource(&resource).unwrap();
        assert!(Arc::ptr_eq(&first, &second), "an unchanged input Arc must hit the cache");
    }

    #[test]
    fn map_resource_carries_dropped_count_and_schema_url_forward() {
        let mut kv = KeepValues::new(
            vec![("env".to_string(), vec![], vec![Value::str("prod")], None)],
            vec![],
        );
        let resource = Arc::new(Resource {
            attributes: AttrMap::new(),
            dropped_attributes_count: 3,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/schema")),
        });
        let mapped = kv.map_resource(&resource).unwrap();
        assert_eq!(mapped.dropped_attributes_count, 3);
        assert_eq!(mapped.schema_url, resource.schema_url);
    }

    #[test]
    fn log_metrics_and_span_are_untouched() {
        let resource = default_resource();
        let mut event = event_with_attrs(&[("host", Value::str("junk.example"))]);
        event
            .metrics
            .push(MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)));
        let original_message = event.log.as_ref().unwrap().message.clone();

        let mut kv = allow_with_other("host", &["static.local"], "other");
        let event = kv.process(&resource, event).unwrap();
        assert_eq!(event.metrics.len(), 1, "keep_values must not touch metrics");
        assert_eq!(event.log.as_ref().unwrap().message, original_message);
    }

    #[test]
    fn clamping_is_idempotent_when_other_is_in_allow() {
        let mut kv = KeepValues::new(
            vec![],
            vec![(
                "host".to_string(),
                vec![],
                vec![Value::str("static.local"), Value::str("other")],
                Some(Value::str("other")),
            )],
        );
        let resource = default_resource();
        let event = event_with_attrs(&[("host", Value::str("junk.example"))]);
        let once = kv.process(&resource, event).unwrap();
        assert_eq!(once.attributes.get("host"), Some(&Value::str("other")));
        let twice = kv.process(&resource, once).unwrap();
        assert_eq!(twice.attributes.get("host"), Some(&Value::str("other")));
    }

    // -- normalize --------------------------------------------------------------------------

    fn lower_allow(field: &str, allow: &[&str], other: Option<&str>) -> KeepValues {
        KeepValues::new(
            vec![],
            vec![(
                field.to_string(),
                vec![Normalize::Lower],
                allow.iter().map(|v| Value::str(*v)).collect(),
                other.map(Value::str),
            )],
        )
    }

    #[test]
    fn an_uppercase_allowed_value_is_written_back_lowercased() {
        let mut kv = lower_allow("host", &["static.local"], None);
        let resource = default_resource();
        let event = event_with_attrs(&[("host", Value::str("STATIC.Local"))]);
        let event = kv.process(&resource, event).unwrap();
        assert_eq!(
            event.attributes.get("host"),
            Some(&Value::str("static.local")),
            "an allowed value must be written back in its normalized form, not the original"
        );
    }

    #[test]
    fn normalization_happens_before_the_allow_test() {
        // Without lowering first, "STATIC.Local" would never match "static.local" and would fall
        // through to `other` -- proving the order, not just the write-back.
        let mut kv = lower_allow("host", &["static.local"], Some("other"));
        let resource = default_resource();
        let event = event_with_attrs(&[("host", Value::str("STATIC.Local"))]);
        let event = kv.process(&resource, event).unwrap();
        assert_eq!(event.attributes.get("host"), Some(&Value::str("static.local")));
    }

    #[test]
    fn an_already_lowercase_value_does_not_record_normalized() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("host_clamp", "keep_values", "transform");
        let mut kv = lower_allow("host", &["static.local"], None).with_telemetry(telemetry);
        let resource = default_resource();
        kv.process(&resource, event_with_attrs(&[("host", Value::str("static.local"))])).unwrap();

        let events = registry.drain(0);
        assert_eq!(
            counter_value(&events, "logit.transform.values.normalized"),
            None,
            "a value already in normalized form must not fire the normalized counter"
        );
    }

    #[test]
    fn non_string_values_are_never_normalized() {
        for value in [Value::I64(1), Value::Bool(true), Value::Null] {
            let mut kv = lower_allow("f", &["x"], None);
            let resource = default_resource();
            let event = event_with_attrs(&[("f", value.clone())]);
            let event = kv.process(&resource, event).unwrap();
            assert_eq!(event.attributes.get("f"), None, "{value:?} never matches 'x' and clamps");
        }
    }

    #[test]
    fn a_non_utf8_bytes_value_lowercases_its_ascii_bytes_only() {
        let mut kv = KeepValues::new(
            vec![],
            vec![(
                "raw".to_string(),
                vec![Normalize::Lower],
                vec![Value::Bytes(bytes::Bytes::from_static(b"a\xffb"))],
                None,
            )],
        );
        let resource = default_resource();
        let event =
            event_with_attrs(&[("raw", Value::Bytes(bytes::Bytes::from_static(b"A\xffB")))]);
        let event = kv.process(&resource, event).unwrap();
        assert_eq!(
            event.attributes.get("raw"),
            Some(&Value::Bytes(bytes::Bytes::from_static(b"a\xffb")))
        );
    }

    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == name => Some(sum.value),
                _ => None,
            })
        })
    }

    #[test]
    fn telemetry_records_allowed_and_clamped_per_field() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("host_clamp", "keep_values", "transform");
        let mut kv = allow_with_other("host", &["static.local"], "other").with_telemetry(telemetry);
        let resource = default_resource();
        kv.process(&resource, event_with_attrs(&[("host", Value::str("static.local"))])).unwrap();
        kv.process(&resource, event_with_attrs(&[("host", Value::str("junk.example"))])).unwrap();

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.values.allowed"), Some(1.0));
        assert_eq!(counter_value(&events, "logit.transform.values.clamped"), Some(1.0));
    }
}
