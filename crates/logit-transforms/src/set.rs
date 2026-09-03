//! `set`: stamps operator-configured constant values onto every event's attributes and/or the
//! batch's resource -- the mechanism `docs/adr/operator-declared-resource-attributes.md` settles
//! on for "give this traffic a real `service.name`" instead of a per-input config field. See that
//! ADR for why a graph component, not a listener-specific field.

use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Resource, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Stamps a fixed set of key/value pairs onto every event's attributes (`process`) and/or every
/// batch's resource (`map_resource`), overwriting on key collision -- a configured value always
/// wins over whatever the wire carried. Either list may be empty; a `Set` with both empty is
/// rejected at graph-validation time (`crates/logit-pipeline/src/graph.rs`) as a certain no-op,
/// the same rule `kv_metrics` already has.
pub struct Set {
    /// Interned once, at construction, from `logit-cli::pipeline::to_set_pairs`'s config
    /// conversion -- keeps the per-event hot path free of interner lookups
    /// (`AttrMap::insert_sym`'s doc comment).
    resource_pairs: Vec<(Symbol, Value)>,
    attribute_pairs: Vec<(Symbol, Value)>,
    /// A one-entry cache of the last resource this component mapped, keyed by `Arc::ptr_eq` on
    /// the *input* -- every listener shipped today stamps one `Arc<Resource>` per decoder
    /// instance onto every batch it produces (`docs/design/data-model.md`), so in practice this
    /// turns every `map_resource` call after the first into a free `Arc` clone rather than a
    /// rebuild. `None` until the first batch with a non-empty `resource_pairs` arrives.
    cache: Option<(Arc<Resource>, Arc<Resource>)>,
    telemetry: Telemetry,
}

impl Set {
    /// `resource`/`attributes` are plain `(String, Value)` pairs -- `logit-transforms` doesn't
    /// depend on `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), so the
    /// `SetValue -> Value` conversion happens in `logit-cli::pipeline` before this is called.
    pub fn new(resource: Vec<(String, Value)>, attributes: Vec<(String, Value)>) -> Self {
        Self {
            resource_pairs: resource.into_iter().map(|(k, v)| (intern(&k), v)).collect(),
            attribute_pairs: attributes.into_iter().map(|(k, v)| (intern(&k), v)).collect(),
            cache: None,
            telemetry: Telemetry::default(),
        }
    }

    /// Attaches a telemetry handle -- see [`Keep::with_telemetry`](crate::Keep::with_telemetry)
    /// for why there's no `Diagnostics` builder alongside it: stamping fixed values can't fail.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for Set {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        for (key, value) in &self.attribute_pairs {
            event.attributes.insert_sym(*key, value.clone());
        }
        Some(event)
    }

    fn map_resource(&mut self, resource: &Arc<Resource>) -> Option<Arc<Resource>> {
        if self.resource_pairs.is_empty() {
            return None;
        }
        if let Some((cached_in, cached_out)) = &self.cache {
            if Arc::ptr_eq(cached_in, resource) {
                return Some(cached_out.clone());
            }
        }
        let mut attrs = resource.attributes.clone();
        for (key, value) in &self.resource_pairs {
            attrs.insert_sym(*key, value.clone());
        }
        let out = Arc::new(Resource { attributes: attrs });
        self.telemetry.count("logit.transform.set.resource.rebuilt", 1.0, &[]);
        self.cache = Some((resource.clone(), out.clone()));
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, Registry};

    fn event() -> Event {
        Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("msg"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
            },
        )
    }

    fn resource(pairs: &[(&str, &str)]) -> Arc<Resource> {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, Value::str(*v));
        }
        Arc::new(Resource { attributes: attrs })
    }

    #[test]
    fn process_inserts_configured_attributes() {
        let mut set = Set::new(vec![], vec![("env".to_string(), Value::str("prod"))]);
        let resource = resource(&[]);
        let event = set.process(&resource, event()).unwrap();
        assert_eq!(event.attributes.get("env"), Some(&Value::str("prod")));
    }

    #[test]
    fn process_overwrites_an_existing_attribute() {
        let mut set = Set::new(vec![], vec![("env".to_string(), Value::str("prod"))]);
        let resource = resource(&[]);
        let mut e = event();
        e.attributes.insert("env", Value::str("dev"));
        let event = set.process(&resource, e).unwrap();
        assert_eq!(event.attributes.get("env"), Some(&Value::str("prod")));
    }

    #[test]
    fn process_with_no_configured_attributes_is_a_no_op() {
        let mut set = Set::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource(&[]);
        let event = set.process(&resource, event()).unwrap();
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn map_resource_is_none_with_no_configured_resource_pairs() {
        let mut set = Set::new(vec![], vec![("env".to_string(), Value::str("prod"))]);
        let resource = resource(&[]);
        assert!(set.map_resource(&resource).is_none());
    }

    #[test]
    fn map_resource_inserts_and_overwrites_resource_attributes() {
        let mut set = Set::new(
            vec![
                ("service.name".to_string(), Value::str("nginx")),
                ("service.namespace".to_string(), Value::str("demo")),
            ],
            vec![],
        );
        let resource = resource(&[("service.name", "unknown_service")]);
        let mapped = set.map_resource(&resource).expect("configured resource pairs should map");
        assert_eq!(mapped.attributes.get("service.name"), Some(&Value::str("nginx")));
        assert_eq!(mapped.attributes.get("service.namespace"), Some(&Value::str("demo")));
    }

    #[test]
    fn map_resource_caches_by_ptr_eq_on_the_input() {
        let mut set = Set::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource(&[]);

        let first = set.map_resource(&resource).unwrap();
        let second = set.map_resource(&resource).unwrap();
        assert!(Arc::ptr_eq(&first, &second), "the same input Arc should hit the cache");
    }

    #[test]
    fn map_resource_rebuilds_for_a_distinct_input_arc() {
        let mut set = Set::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let a = resource(&[]);
        let b = resource(&[]); // a distinct Arc, not ptr_eq to `a` even though value-equal

        let mapped_a = set.map_resource(&a).unwrap();
        let mapped_b = set.map_resource(&b).unwrap();
        assert!(
            !Arc::ptr_eq(&mapped_a, &mapped_b),
            "a distinct input Arc must not reuse the cached output"
        );
    }

    #[test]
    fn map_resource_cache_miss_records_a_rebuild_counter() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("web_identity", "set", "transform");
        let mut set = Set::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![])
            .with_telemetry(telemetry);
        let resource = resource(&[]);

        set.map_resource(&resource);
        set.map_resource(&resource); // cache hit -- must not double-count

        let events = registry.drain(0);
        let rebuilt = events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.set.resource.rebuilt" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert_eq!(rebuilt, Some(1.0));
    }

    #[test]
    fn set_does_not_touch_log_metrics_or_span() {
        let mut set = Set::new(vec![], vec![("env".to_string(), Value::str("prod"))]);
        let resource = resource(&[]);
        let e = event();
        let original_message = e.log.as_ref().unwrap().message.clone();
        let event = set.process(&resource, e).unwrap();
        assert_eq!(event.log.as_ref().unwrap().message, original_message);
    }
}
