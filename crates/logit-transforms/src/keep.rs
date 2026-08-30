//! `keep`/`remove`: attribute allowlist and denylist transforms. Both are stateless -- only
//! `process` is overridden, `flush_interval`/`flush` keep the `Transform` trait's defaults -- and
//! share one piece of filtering machinery, differing only in which side of the named set survives.
//!
//! **`keep` is the important one, deliberately.** A denylist (`remove`) can only ever protect
//! against fields a config author already knows about; a new field appearing in a log format
//! later (an nginx `log_format` gaining a directive, say) would silently become a new InfluxDB tag
//! dimension with `remove` alone. `keep`'s allowlist makes that impossible by construction --
//! anything not explicitly named is dropped, known or not.
//!
//! **Place `keep` before `aggregate` in a pipeline.** `aggregate`'s `SeriesKey` includes the whole
//! of `event.attributes` (`crates/logit-transforms/src/aggregate.rs`), so an un-pruned
//! high-cardinality attribute (client address, user agent, a full request path) sitting on an
//! event when it reaches `aggregate` explodes both series cardinality and per-window memory --
//! `keep` ahead of it is what bounds the tag set `aggregate` ever keys on.

use logit_core::interner::resolve;
use logit_core::{AttrMap, Event, Resource};
use logit_pipeline::Transform;
use std::collections::HashSet;
use std::sync::Arc;

/// Retains only the named attributes, dropping the rest. An **empty** `fields` list is legal and
/// means "drop every attribute" -- a real, if blunt, operation, not rejected as a config error.
pub struct Keep {
    fields: HashSet<String>,
}

impl Keep {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields: fields.into_iter().collect() }
    }
}

impl Transform for Keep {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        event.attributes = filtered(&event.attributes, |key| self.fields.contains(key));
        Some(event)
    }
}

/// Drops the named attributes, keeping the rest.
pub struct Remove {
    fields: HashSet<String>,
}

impl Remove {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields: fields.into_iter().collect() }
    }
}

impl Transform for Remove {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        event.attributes = filtered(&event.attributes, |key| !self.fields.contains(key));
        Some(event)
    }
}

/// Shared by [`Keep`] and [`Remove`]: rebuilds an `AttrMap` from whichever entries `retain`
/// accepts. `AttrMap::iter` yields sorted-`Symbol` order (its own doc comment), and inserting in
/// that order into a fresh `AttrMap` reproduces the same sorted order for whatever survives --
/// preserving relative order "for free," but see the tests for an explicit assertion of that
/// rather than a silent assumption.
fn filtered(attrs: &AttrMap, retain: impl Fn(&str) -> bool) -> AttrMap {
    let mut out = AttrMap::new();
    for (sym, value) in attrs.iter() {
        let key = resolve(sym);
        if retain(key) {
            out.insert(key, value.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{BodyFormat, LogRecord, MetricKind, MetricRecord, Value};

    fn event_with_attrs(pairs: &[(&str, &str)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, Value::str(*v));
        }
        Event::log(
            0,
            attrs,
            LogRecord { message: Value::str("msg"), severity: None, body_format: BodyFormat::Raw },
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
        let event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let event = keep.process(&resource, event).unwrap();
        assert_eq!(attr_keys(&event), vec!["a", "c"]);
    }

    #[test]
    fn keep_preserves_the_relative_order_of_what_remains() {
        // `AttrMap` sorts by interned `Symbol` (assignment order), not alphabetically and not by
        // insertion order -- so the right invariant to check is "whatever order a fresh `AttrMap`
        // holding just the surviving keys would already have," not a specific string ordering.
        let mut keep = Keep::new(vec!["m".to_string(), "z".to_string(), "a".to_string()]);
        let resource = default_resource();
        let event = event_with_attrs(&[("z", "1"), ("a", "2"), ("m", "3"), ("x", "4")]);
        let event = keep.process(&resource, event).unwrap();
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
        let event = keep.process(&resource, event_with_attrs(&[])).unwrap();
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn keep_with_an_empty_list_drops_every_attribute() {
        let mut keep = Keep::new(vec![]);
        let resource = default_resource();
        let event = keep.process(&resource, event_with_attrs(&[("a", "1"), ("b", "2")])).unwrap();
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn keep_naming_an_absent_attribute_is_not_an_error() {
        let mut keep = Keep::new(vec!["nonexistent".to_string()]);
        let resource = default_resource();
        let event = keep.process(&resource, event_with_attrs(&[("a", "1")])).unwrap();
        assert!(event.attributes.is_empty());
    }

    #[test]
    fn remove_with_multiple_fields_drops_exactly_those() {
        let mut remove = Remove::new(vec!["a".to_string(), "c".to_string()]);
        let resource = default_resource();
        let event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]);
        let event = remove.process(&resource, event).unwrap();
        assert_eq!(attr_keys(&event), vec!["b", "d"]);
    }

    #[test]
    fn neither_transform_touches_log_metrics_or_span() {
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", "1")]);
        event.metrics.push(MetricRecord {
            name: intern("m"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });
        let original_message = event.log.as_ref().unwrap().message.clone();

        let mut keep = Keep::new(vec![]);
        let event = keep.process(&resource, event).unwrap();
        assert_eq!(event.metrics.len(), 1, "keep must not touch metrics");
        assert_eq!(event.log.as_ref().unwrap().message, original_message);

        let mut remove = Remove::new(vec!["a".to_string()]);
        let event = remove.process(&resource, event).unwrap();
        assert_eq!(event.metrics.len(), 1, "remove must not touch metrics");
        assert_eq!(event.log.as_ref().unwrap().message, original_message);
    }
}
