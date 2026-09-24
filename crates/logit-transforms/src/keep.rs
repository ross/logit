//! `keep`/`remove`: stateless attribute allowlist and denylist transforms, sharing one filter.
//!
//! **Prefer `keep`.** A denylist only protects against fields the config author already knows
//! about: a directive later added to an nginx `log_format` becomes a new InfluxDB tag dimension
//! with `remove` alone. `keep` drops anything not named, known or not.
//!
//! **Place `keep` before `aggregate` in a pipeline.** `aggregate`'s `SeriesKey` includes all of
//! `event.attributes`, so an unpruned high-cardinality attribute (client address, user agent, a
//! full request path) that reaches `aggregate` explodes both series cardinality and per-window
//! memory. `keep` ahead of it is what bounds the tag set `aggregate` keys on.

use logit_core::interner::resolve;
use logit_core::{AttrMap, Event, Resource, Telemetry};
use logit_pipeline::Transform;
use std::collections::HashSet;
use std::sync::Arc;

/// Retains only the named attributes, dropping the rest.
///
/// An empty `fields` list is legal and drops every attribute; it isn't a config error.
pub struct Keep {
    fields: HashSet<String>,
    telemetry: Telemetry,
}

impl Keep {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields: fields.into_iter().collect(), telemetry: Telemetry::default() }
    }

    /// Attaches a telemetry handle.
    ///
    /// There's no `Diagnostics` builder: filtering against a fixed set can't fail, so there's
    /// nothing to warn about.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for Keep {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        event.attributes =
            filtered(&event.attributes, &self.telemetry, |key| self.fields.contains(key));
        true
    }
}

/// Drops the named attributes, keeping the rest.
pub struct Remove {
    fields: HashSet<String>,
    telemetry: Telemetry,
}

impl Remove {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields: fields.into_iter().collect(), telemetry: Telemetry::default() }
    }

    /// Attaches a telemetry handle; see [`Keep::with_telemetry`].
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for Remove {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        event.attributes =
            filtered(&event.attributes, &self.telemetry, |key| !self.fields.contains(key));
        true
    }
}

/// Rebuilds an `AttrMap` from the entries `retain` accepts, for [`Keep`] and [`Remove`].
///
/// `AttrMap::iter` yields sorted-`Symbol` order, so the survivors keep their relative order.
///
/// Records `logit.transform.attributes.kept`/`.dropped`: the other end of the cardinality story
/// `aggregate`'s `logit.transform.series.active` gauge tells, showing whether `keep` is bounding
/// what reaches `aggregate` (`docs/design/internal-telemetry.md`).
fn filtered(attrs: &AttrMap, telemetry: &Telemetry, retain: impl Fn(&str) -> bool) -> AttrMap {
    let mut out = AttrMap::new();
    for (sym, value) in attrs.iter() {
        let key = resolve(sym);
        if retain(key) {
            out.insert(key, value.clone());
        }
    }
    telemetry.count("logit.transform.attributes.kept", out.len() as f64, &[]);
    telemetry.count("logit.transform.attributes.dropped", (attrs.len() - out.len()) as f64, &[]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, Registry, Value};

    fn event_with_attrs(pairs: &[(&str, &str)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, Value::str(*v));
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

    fn attr_keys(event: &Event) -> Vec<&'static str> {
        event.attributes.iter().map(|(k, _)| resolve(k)).collect()
    }

    #[test]
    fn keep_drops_everything_not_named() {
        let mut keep = Keep::new(vec!["a".to_string(), "c".to_string()]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3")]);
        assert!(keep.process(&resource, &mut event));
        assert_eq!(attr_keys(&event), vec!["a", "c"]);
    }

    #[test]
    fn keep_preserves_the_relative_order_of_what_remains() {
        // `AttrMap` sorts by `Symbol` (interning order), neither alphabetically nor by insertion,
        // so the expected order is whatever a fresh `AttrMap` of the survivors has.
        let mut keep = Keep::new(vec!["m".to_string(), "z".to_string(), "a".to_string()]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("z", "1"), ("a", "2"), ("m", "3"), ("x", "4")]);
        assert!(keep.process(&resource, &mut event));
        let kept = attr_keys(&event);

        let mut expected = AttrMap::new();
        expected.insert("z", Value::str("1"));
        expected.insert("a", Value::str("2"));
        expected.insert("m", Value::str("3"));
        let expected_order: Vec<&str> = expected.iter().map(|(k, _)| resolve(k)).collect();

        assert_eq!(kept.len(), 3);
        assert_eq!(kept, expected_order, "surviving keys should stay in AttrMap's sorted order");
    }

    #[test]
    fn keep_is_a_no_op_on_an_event_with_no_attributes() {
        let mut keep = Keep::new(vec!["a".to_string()]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        assert!(keep.process(&resource, &mut event));
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn keep_with_an_empty_list_drops_every_attribute() {
        let mut keep = Keep::new(vec![]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1"), ("b", "2")]);
        assert!(keep.process(&resource, &mut event));
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn keep_naming_an_absent_attribute_is_not_an_error() {
        let mut keep = Keep::new(vec!["nonexistent".to_string()]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1")]);
        assert!(keep.process(&resource, &mut event));
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn remove_with_multiple_fields_drops_exactly_those() {
        let mut remove = Remove::new(vec!["a".to_string(), "c".to_string()]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]);
        assert!(remove.process(&resource, &mut event));
        assert_eq!(attr_keys(&event), vec!["b", "d"]);
    }

    #[test]
    fn neither_transform_touches_log_metrics_or_span() {
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1")]);
        event.metrics.push(MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
        let original_message = event.log.as_ref().unwrap().message.clone();

        let mut keep = Keep::new(vec![]);
        assert!(keep.process(&resource, &mut event));
        assert_eq!(event.metrics.len(), 1, "keep must not touch metrics");
        assert_eq!(event.log.as_ref().unwrap().message, original_message);

        let mut remove = Remove::new(vec!["a".to_string()]);
        assert!(remove.process(&resource, &mut event));
        assert_eq!(event.metrics.len(), 1, "remove must not touch metrics");
        assert_eq!(event.log.as_ref().unwrap().message, original_message);
    }

    // Takes drained `events`, not a `&Registry`: `Registry::drain` empties every buffer, so a
    // second drain in the same test would see nothing.
    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == name => Some(sum.value),
                _ => None,
            })
        })
    }

    #[test]
    fn keep_records_kept_and_dropped_attribute_counts() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("keep_fields", "keep", "transform");
        let mut keep = Keep::new(vec!["a".to_string(), "c".to_string()]).with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3")]);
        assert!(keep.process(&resource, &mut event));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.attributes.kept"), Some(2.0));
        assert_eq!(counter_value(&events, "logit.transform.attributes.dropped"), Some(1.0));
    }

    #[test]
    fn remove_records_kept_and_dropped_attribute_counts() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("remove_fields", "remove", "transform");
        let mut remove = Remove::new(vec!["a".to_string()]).with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3")]);
        assert!(remove.process(&resource, &mut event));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.attributes.kept"), Some(2.0));
        assert_eq!(counter_value(&events, "logit.transform.attributes.dropped"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        // Without `.with_telemetry`, filtering still works and records nothing.
        let mut keep = Keep::new(vec!["a".to_string()]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1")]);
        assert!(keep.process(&resource, &mut event));
        assert_eq!(attr_keys(&event), vec!["a"]);
    }
}
