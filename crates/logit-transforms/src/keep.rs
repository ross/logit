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
use logit_core::{AttrMap, Event, Resource, Telemetry};
use logit_pipeline::Transform;
use std::collections::HashSet;
use std::sync::Arc;

/// Retains only the named attributes, dropping the rest. An **empty** `fields` list is legal and
/// means "drop every attribute" -- a real, if blunt, operation, not rejected as a config error.
pub struct Keep {
    fields: HashSet<String>,
    telemetry: Telemetry,
}

impl Keep {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields: fields.into_iter().collect(), telemetry: Telemetry::default() }
    }

    /// Attaches a telemetry handle -- no `Diagnostics` builder alongside it, unlike most other
    /// transforms: filtering an `AttrMap` against a fixed set can't fail and has nothing to warn
    /// about, so there's no `warn_throttled` call site for one to bridge.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for Keep {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        event.attributes =
            filtered(&event.attributes, &self.telemetry, |key| self.fields.contains(key));
        Some(event)
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

    /// See [`Keep::with_telemetry`] -- same reasoning, no `Diagnostics` here either.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for Remove {
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        event.attributes =
            filtered(&event.attributes, &self.telemetry, |key| !self.fields.contains(key));
        Some(event)
    }
}

/// Shared by [`Keep`] and [`Remove`]: rebuilds an `AttrMap` from whichever entries `retain`
/// accepts. `AttrMap::iter` yields sorted-`Symbol` order (its own doc comment), and inserting in
/// that order into a fresh `AttrMap` reproduces the same sorted order for whatever survives --
/// preserving relative order "for free," but see the tests for an explicit assertion of that
/// rather than a silent assumption.
///
/// Records `logit.transform.attributes.kept`/`.dropped` -- the cardinality story `aggregate`'s
/// `logit.transform.series.active` gauge tells from the other end: `keep` is documented as the
/// mechanism that's supposed to bound what reaches `aggregate`, so knowing how much it's actually
/// suppressing (or not) is what confirms that's really happening (`docs/design/
/// internal-telemetry.md`).
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

    // Takes already-drained `events`, not a `&Registry` -- `Registry::drain` is consuming (it
    // empties every buffer via `mem::take`), so calling it once per assertion in the same test
    // would make every assertion after the first see an already-emptied registry.
    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v) if resolve(m.name) == name => Some(*v),
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
        let event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3")]);
        keep.process(&resource, event).unwrap();

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
        let event = event_with_attrs(&[("a", "1"), ("b", "2"), ("c", "3")]);
        remove.process(&resource, event).unwrap();

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.attributes.kept"), Some(2.0));
        assert_eq!(counter_value(&events, "logit.transform.attributes.dropped"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        // No `.with_telemetry(...)` call at all -- should behave exactly as before this change,
        // just without any recorded points (nothing to assert beyond "doesn't panic").
        let mut keep = Keep::new(vec!["a".to_string()]);
        let resource = default_resource();
        let event = keep.process(&resource, event_with_attrs(&[("a", "1")])).unwrap();
        assert_eq!(attr_keys(&event), vec!["a"]);
    }
}
