//! `has_attributes`/`drop_attributes`: filter events by an operator-configured key/value match
//! against a batch's resource and/or an event's own attributes. See
//! `docs/adr/attribute-filtering-components.md`. The config is `set`'s, field for field: whatever
//! `set` can stamp, these can match.
//!
//! **A map is a conjunction.** Every configured pair, within a map and across both `resource:` and
//! `attributes:`, must match; under an any-of reading, a sibling branch sharing one pair would
//! leak through. There is no `mode:`: sibling `has_attributes` fanning into one consumer already
//! express "any of these".
//!
//! **`drop_attributes` is the complement of `has_attributes` on the same config, taken at the top
//! level, not per pair**: it drops an event only when every configured pair matches, and forwards
//! one matching some but not all. `DropAttributes::process` is `HasAttributes::process` with a
//! single `!`.
//!
//! **Absent is `false`**: a configured key the event/resource doesn't carry never matches. That is
//! why the complement is top-level: negated per pair, `drop_attributes {stream: a}` would drop
//! every event without a `stream` key, black-holing untagged traffic. "Key exists with any value"
//! isn't expressible; `docs/adr/routing-by-condition-is-lua.md` retired `exists` to keep this a
//! bounded matcher rather than a predicate language.

use crate::value_matches;
use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Resource, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// The match shared by [`HasAttributes`] and [`DropAttributes`].
struct Matcher {
    /// Interned once at construction.
    resource_pairs: Vec<(Symbol, Value)>,
    attribute_pairs: Vec<(Symbol, Value)>,
    /// The last resource's match result, keyed by `Arc::ptr_eq`; constant within a batch.
    ///
    /// Can't go stale: every producer builds a fresh `Arc<Resource>` rather than mutating one,
    /// the owned clone keeps the address from being recycled while cached, and a false miss (a
    /// value-equal but distinct `Arc`) only costs a recompute.
    ///
    /// `native::decode` mints a fresh `Arc<Resource>` per frame, so behind `logit_in` this always
    /// misses. It still helps a listener that reuses one `Arc` per decoder instance, and a miss
    /// allocates nothing (one `ptr_eq` plus one `Arc` clone), so there's no miss counter.
    cache: Option<(Arc<Resource>, bool)>,
}

impl Matcher {
    fn new(resource: Vec<(String, Value)>, attributes: Vec<(String, Value)>) -> Self {
        Self {
            resource_pairs: resource.into_iter().map(|(k, v)| (intern(&k), v)).collect(),
            attribute_pairs: attributes.into_iter().map(|(k, v)| (intern(&k), v)).collect(),
            cache: None,
        }
    }

    /// `true` iff every configured pair matches.
    ///
    /// Checks the (cached) resource first and short-circuits on a miss, skipping a lookup per
    /// attribute pair.
    fn matches(&mut self, resource: &Arc<Resource>, event: &Event) -> bool {
        if !self.resource_pairs.is_empty() {
            let resource_matched = match &self.cache {
                Some((cached, matched)) if Arc::ptr_eq(cached, resource) => *matched,
                _ => {
                    let matched = self.resource_pairs.iter().all(|(key, configured)| {
                        resource
                            .attributes
                            .get_sym(*key)
                            .is_some_and(|actual| value_matches(configured, actual))
                    });
                    self.cache = Some((resource.clone(), matched));
                    matched
                }
            };
            if !resource_matched {
                return false;
            }
        }
        self.attribute_pairs.iter().all(|(key, configured)| {
            event.attributes.get_sym(*key).is_some_and(|actual| value_matches(configured, actual))
        })
    }

    #[cfg(test)]
    fn cached_resource_matched(&self) -> Option<bool> {
        self.cache.as_ref().map(|(_, matched)| *matched)
    }
}

/// Forwards an event whose resource/attributes match every configured pair, dropping the rest.
///
/// Never mutates a forwarded event. See the module doc for the matching rules.
pub struct HasAttributes {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl HasAttributes {
    /// Builds the filter; the signature is [`crate::Set::new`]'s, since it matches what `set`
    /// stamps.
    pub fn new(resource: Vec<(String, Value)>, attributes: Vec<(String, Value)>) -> Self {
        Self { matcher: Matcher::new(resource, attributes), telemetry: Telemetry::default() }
    }

    /// Attaches a telemetry handle; matching fixed values can't fail, so no `Diagnostics`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    #[cfg(test)]
    fn cached_resource_matched(&self) -> Option<bool> {
        self.matcher.cached_resource_matched()
    }
}

impl Transform for HasAttributes {
    fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
        let matched = self.matcher.matches(resource, event);
        forward(matched, &self.telemetry)
    }
}

/// Drops an event whose resource/attributes match every configured pair, forwarding the rest.
///
/// The complement of [`HasAttributes`] on the same config, taken at the top level, not per pair.
/// Unlike `drop_signals`, it drops whole events and never touches a payload.
pub struct DropAttributes {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl DropAttributes {
    /// Builds the filter; see [`HasAttributes::new`].
    pub fn new(resource: Vec<(String, Value)>, attributes: Vec<(String, Value)>) -> Self {
        Self { matcher: Matcher::new(resource, attributes), telemetry: Telemetry::default() }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    #[cfg(test)]
    fn cached_resource_matched(&self) -> Option<bool> {
        self.matcher.cached_resource_matched()
    }
}

impl Transform for DropAttributes {
    fn process(&mut self, resource: &Arc<Resource>, event: &mut Event) -> bool {
        let matched = self.matcher.matches(resource, event);
        // This `!` is the only difference from `HasAttributes`: an event drops only when every
        // pair matches, not when any one does.
        forward(!matched, &self.telemetry)
    }
}

/// Records the verdict and returns `keep`.
///
/// The `0.0` on the forward path registers `events.filtered` so it reads zero rather than absent.
fn forward(keep: bool, telemetry: &Telemetry) -> bool {
    telemetry.count("logit.transform.events.filtered", if keep { 0.0 } else { 1.0 }, &[]);
    keep
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, Registry};

    fn event_with_attrs(attrs: &[(&str, Value)]) -> Event {
        let mut map = AttrMap::new();
        for (k, v) in attrs {
            map.insert(k, v.clone());
        }
        let mut event = Event::log(
            0,
            map,
            LogRecord {
                message: Value::str("msg"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        event
            .metrics
            .push(MetricRecord::new(logit_core::interner::intern("m"), MetricKind::counter(1.0)));
        event
    }

    fn resource_with_attrs(attrs: &[(&str, Value)]) -> Arc<Resource> {
        let mut map = AttrMap::new();
        for (k, v) in attrs {
            map.insert(k, v.clone());
        }
        Arc::new(Resource { attributes: map, ..Default::default() })
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if logit_core::interner::resolve(m.name) == name => {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    // -- HasAttributes: single attribute ------------------------------------------------------

    #[test]
    fn has_attributes_forwards_an_event_whose_single_attribute_matches() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(has.process(&resource, &mut event));
    }

    #[test]
    fn has_attributes_drops_an_event_whose_value_differs() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("stream", Value::str("b"))]);
        assert!(!has.process(&resource, &mut event));
    }

    #[test]
    fn has_attributes_drops_an_event_missing_the_configured_key() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        assert!(!has.process(&resource, &mut event));
    }

    // -- HasAttributes: AND -------------------------------------------------------------------

    #[test]
    fn has_attributes_requires_every_configured_attribute_to_match() {
        let mut has = HasAttributes::new(
            vec![],
            vec![("stream".to_string(), Value::str("a")), ("tier".to_string(), Value::str("gold"))],
        );
        let resource = default_resource();

        let mut both =
            event_with_attrs(&[("stream", Value::str("a")), ("tier", Value::str("gold"))]);
        assert!(has.process(&resource, &mut both), "both pairs match");

        let mut one_only = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(!has.process(&resource, &mut one_only), "only one of two pairs matches");
    }

    #[test]
    fn has_attributes_requires_both_maps_to_match() {
        let mut has = HasAttributes::new(
            vec![("service.name".to_string(), Value::str("nginx"))],
            vec![("stream".to_string(), Value::str("a"))],
        );
        let matching_resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        let other_resource = resource_with_attrs(&[("service.name", Value::str("haproxy"))]);

        let mut event = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(
            has.process(&matching_resource, &mut event),
            "both resource and attribute pairs match"
        );
        assert!(
            !has.process(&other_resource, &mut event),
            "attribute pair matches but resource pair does not"
        );

        let mut mismatched_event = event_with_attrs(&[("stream", Value::str("b"))]);
        assert!(
            !has.process(&matching_resource, &mut mismatched_event),
            "resource pair matches but attribute pair does not"
        );
    }

    #[test]
    fn has_attributes_matches_a_resource_attribute_without_touching_the_event() {
        let mut has =
            HasAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        let mut event = event_with_attrs(&[]);
        assert!(has.process(&resource, &mut event));
    }

    #[test]
    fn a_resource_only_config_never_consults_the_event_attributes() {
        let mut has =
            HasAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        // An event whose own attributes could never satisfy a stream/tier-shaped config still
        // forwards, because a resource-only config never looks at event.attributes at all.
        let mut event = event_with_attrs(&[("stream", Value::str("unrelated"))]);
        assert!(has.process(&resource, &mut event));
    }

    // -- value_matches integration --------------------------------------------------------------

    #[test]
    fn has_attributes_coerces_across_numeric_variants() {
        for actual in [Value::I64(200), Value::U64(200), Value::F64(200.0), Value::str("200")] {
            let mut has = HasAttributes::new(vec![], vec![("status".to_string(), Value::I64(200))]);
            let resource = default_resource();
            let mut event = event_with_attrs(&[("status", actual.clone())]);
            assert!(has.process(&resource, &mut event), "{actual:?} should coerce-match 200");
        }
    }

    #[test]
    fn has_attributes_compares_bytes_and_str_alike() {
        let mut has = HasAttributes::new(vec![], vec![("host".to_string(), Value::str("web-1"))]);
        let resource = default_resource();
        let mut event =
            event_with_attrs(&[("host", Value::Bytes(bytes::Bytes::from_static(b"web-1")))]);
        assert!(has.process(&resource, &mut event));
    }

    #[test]
    fn has_attributes_does_not_coerce_a_bool_to_a_string() {
        let mut has = HasAttributes::new(vec![], vec![("sampled".to_string(), Value::Bool(true))]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("sampled", Value::str("true"))]);
        assert!(!has.process(&resource, &mut event), "Bool must never coerce to a string");
    }

    // -- never mutates --------------------------------------------------------------------------

    #[test]
    fn has_attributes_never_mutates_a_forwarded_event() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let mut event =
            event_with_attrs(&[("stream", Value::str("a")), ("other", Value::str("x"))]);
        let original_message = event.log.as_ref().map(|l| l.message.clone());
        let original_metrics = event.metrics.len();

        assert!(has.process(&resource, &mut event), "matches");
        assert_eq!(event.attributes.get("other"), Some(&Value::str("x")));
        assert_eq!(event.log.as_ref().map(|l| l.message.clone()), original_message);
        assert_eq!(event.metrics.len(), original_metrics);
        assert!(event.span.is_none());
    }

    // -- DropAttributes: the exact complement ----------------------------------------------------

    #[test]
    fn drop_attributes_is_the_exact_complement_of_has_attributes() {
        let config = || {
            vec![("stream".to_string(), Value::str("a")), ("tier".to_string(), Value::str("gold"))]
        };
        let cases: Vec<Event> = vec![
            event_with_attrs(&[("stream", Value::str("a")), ("tier", Value::str("gold"))]),
            event_with_attrs(&[("stream", Value::str("a"))]),
            event_with_attrs(&[("tier", Value::str("gold"))]),
            event_with_attrs(&[]),
            event_with_attrs(&[("stream", Value::str("b")), ("tier", Value::str("gold"))]),
        ];

        for mut event in cases {
            let mut has = HasAttributes::new(vec![], config());
            let mut drop = DropAttributes::new(vec![], config());
            let resource = default_resource();

            let has_forwards = has.process(&resource, &mut event);
            let drop_forwards = drop.process(&resource, &mut event);
            assert_eq!(
                has_forwards, !drop_forwards,
                "has_attributes and drop_attributes must exactly partition every event"
            );
        }
    }

    #[test]
    fn drop_attributes_drops_only_when_every_pair_matches() {
        let mut drop = DropAttributes::new(
            vec![],
            vec![("stream".to_string(), Value::str("a")), ("tier".to_string(), Value::str("gold"))],
        );
        let resource = default_resource();

        let mut both =
            event_with_attrs(&[("stream", Value::str("a")), ("tier", Value::str("gold"))]);
        assert!(!drop.process(&resource, &mut both), "every pair matches -- dropped");

        let mut one_only = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(
            drop.process(&resource, &mut one_only),
            "only one of two pairs matches -- forwarded, not dropped"
        );
    }

    #[test]
    fn drop_attributes_forwards_an_event_missing_the_configured_key() {
        let mut drop = DropAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        assert!(
            drop.process(&resource, &mut event),
            "an event that never carried the key isn't one of the ones told to drop"
        );
    }

    #[test]
    fn the_two_kinds_partition_a_stream() {
        let events = vec![
            event_with_attrs(&[("stream", Value::str("a"))]),
            event_with_attrs(&[("stream", Value::str("b"))]),
            event_with_attrs(&[]),
        ];
        let resource = default_resource();

        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let mut drop = DropAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);

        for mut event in events {
            let has_result = has.process(&resource, &mut event);
            let drop_result = drop.process(&resource, &mut event);
            assert!(
                has_result != drop_result,
                "every event lands in exactly one of the two kinds, never both, never neither"
            );
        }
    }

    // -- resource caching -------------------------------------------------------------------------

    #[test]
    fn the_resource_match_is_cached_per_resource_arc() {
        let mut has =
            HasAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);

        assert_eq!(has.cached_resource_matched(), None, "nothing cached before the first match");
        let mut event = event_with_attrs(&[]);
        has.process(&resource, &mut event);
        assert_eq!(has.cached_resource_matched(), Some(true), "the first call populates the cache");
        let mut event = event_with_attrs(&[]);
        has.process(&resource, &mut event);
        assert_eq!(
            has.cached_resource_matched(),
            Some(true),
            "the same Arc twice should hit the cache, not recompute"
        );
    }

    #[test]
    fn drop_attributes_caches_the_resource_match_too() {
        let mut drop =
            DropAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);

        assert_eq!(drop.cached_resource_matched(), None);
        let mut event = event_with_attrs(&[]);
        drop.process(&resource, &mut event);
        assert_eq!(drop.cached_resource_matched(), Some(true));
    }

    #[test]
    fn two_value_equal_but_distinct_resource_arcs_do_not_falsely_hit_the_cache() {
        let mut has =
            HasAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let a = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        let b = resource_with_attrs(&[("service.name", Value::str("nginx"))]); // distinct Arc

        let mut event_a = event_with_attrs(&[]);
        assert!(has.process(&a, &mut event_a));
        let mut event_b = event_with_attrs(&[]);
        assert!(
            has.process(&b, &mut event_b),
            "a distinct but value-equal Arc must still be evaluated correctly"
        );
    }

    // -- telemetry ----------------------------------------------------------------------------

    #[test]
    fn both_record_filtered_events() {
        let registry = Registry::new();
        let has_telemetry = registry.telemetry_for("stream_a", "has_attributes", "transform");
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))])
            .with_telemetry(has_telemetry);
        let resource = default_resource();

        let mut event_a = event_with_attrs(&[("stream", Value::str("a"))]);
        has.process(&resource, &mut event_a);
        let mut event_b = event_with_attrs(&[("stream", Value::str("b"))]);
        has.process(&resource, &mut event_b);

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.events.filtered"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(has.process(&resource, &mut event));
    }
}
