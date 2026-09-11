//! `has_attributes`/`drop_attributes`: filter events by an operator-configured key/value match
//! against a batch's resource and/or an event's own attributes -- the fan-out-after-`logit_in`
//! gap `docs/adr/attribute-filtering-components.md` closes. Config is `crate::Set`'s config, field
//! for field: whatever `set` can stamp is exactly what these can match.
//!
//! **A map is a conjunction.** Every configured pair, within a map and across both `resource:` and
//! `attributes:`, must match. This is what makes `has_attributes` and `set` inverses -- under an
//! `any_of` reading a sibling branch sharing just one pair would leak through. There is no `mode:`
//! field: N sibling `has_attributes`, one pair each, feeding one consumer already expresses "any of
//! these" in the graph (fan-in is free), so a mode flag would just be a second way to spell
//! something already composable.
//!
//! **`drop_attributes` is the exact complement of `has_attributes` on the same config, taken at the
//! top level, not per pair**: it drops an event only when *every* configured pair matches; an event
//! matching some-but-not-all is forwarded. This is structural here, not a convention to remember --
//! `DropAttributes::process` is `HasAttributes::process` with a single `!`. The alternative reading
//! ("drop if *any* pair matches") is the complement of "forward iff *none* match", which is a
//! different filter, not this one's inverse.
//!
//! **Absent is `false`** -- a configured key the event/resource doesn't carry never matches. This is
//! what forces the top-level complement above: under a per-pair negation, `drop_attributes {stream:
//! a}` would drop every event that doesn't carry `stream` at all, a silent black hole for untagged
//! traffic and the opposite of the else-branch behavior the fan-out topology needs. "Key exists with
//! any value" is deliberately not expressible -- `exists` was one of the predicate grammar's
//! functions that `docs/adr/routing-by-condition-is-lua.md` retired, and not shipping it is part of
//! keeping this a bounded matcher rather than a predicate language.

use crate::value_matches;
use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Resource, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Shared by [`HasAttributes`] and [`DropAttributes`] -- both kinds are this plus a `!` at the one
/// call site in each `process`.
struct Matcher {
    /// Interned once, at construction, from `logit-cli::pipeline::to_set_pairs`'s config
    /// conversion -- `Set::new`'s hot-path convention exactly.
    resource_pairs: Vec<(Symbol, Value)>,
    attribute_pairs: Vec<(Symbol, Value)>,
    /// A one-entry cache of the last resource's match result, keyed by `Arc::ptr_eq` on the input
    /// -- `Set::map_resource`'s caching idiom, applied to a read instead of a rebuild. The match
    /// result is constant for a whole batch, since `resource` doesn't change between events in one
    /// batch.
    ///
    /// Can't go stale: `Resource` is immutable behind its `Arc` (every producer -- `Set::
    /// map_resource`, `logit-script`'s resource proxy, `native::decode` -- builds a fresh
    /// `Arc::new` rather than mutating one in place), the cache holds an owned `Arc` clone so the
    /// address it's keyed on can't be recycled by a *different* `Resource` while cached, and a
    /// false miss (a value-equal but distinct `Arc`) costs a recompute, never a wrong answer.
    ///
    /// `native::decode` mints a fresh `Arc<Resource>` per frame, so in the headline
    /// fan-out-after-`logit_in` topology this cache always misses -- kept anyway, since it helps
    /// sidecar shapes where a listener reuses one `Arc` per decoder instance, and a miss here costs
    /// one `ptr_eq` plus one `Arc` clone, allocating nothing (unlike `Set`'s miss, which rebuilds
    /// an `AttrMap`). No cache-miss telemetry counter for that reason: a miss costs nothing
    /// measurable, and a counter would advertise a cost that isn't there.
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

    /// `true` iff every configured pair matches -- resource pairs against `resource.attributes`,
    /// attribute pairs against `event.attributes`. Resource first, and short-circuits on a miss:
    /// one cached branch beats a binary search per attribute pair for the common case.
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
/// Never mutates a forwarded event -- like `HasSignal`, this only ever decides whether to forward,
/// never what to forward. See the module doc for the AND rule and absent-is-`false`.
pub struct HasAttributes {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl HasAttributes {
    /// `resource`/`attributes` are plain `(String, Value)` pairs -- deliberately [`crate::Set::new`]'s
    /// signature, argument for argument: this matches on exactly what `set` stamps, and
    /// `logit-cli::pipeline::to_set_pairs` builds both from the same config shape.
    pub fn new(resource: Vec<(String, Value)>, attributes: Vec<(String, Value)>) -> Self {
        Self { matcher: Matcher::new(resource, attributes), telemetry: Telemetry::default() }
    }

    /// See [`crate::Keep::with_telemetry`] -- same reasoning, no `Diagnostics` here either:
    /// matching a fixed set of configured values can't fail.
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
    fn process(&mut self, resource: &Arc<Resource>, event: Event) -> Option<Event> {
        let matched = self.matcher.matches(resource, &event);
        forward(matched, event, &self.telemetry)
    }
}

/// Drops an event whose resource/attributes match every configured pair, forwarding the rest -- the
/// exact complement of [`HasAttributes`] on the same config. See the module doc for why the
/// complement is taken at the top level, not per pair.
pub struct DropAttributes {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl DropAttributes {
    /// See [`HasAttributes::new`] -- identical signature and reasoning.
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
    fn process(&mut self, resource: &Arc<Resource>, event: Event) -> Option<Event> {
        let matched = self.matcher.matches(resource, &event);
        // The single `!` here is the entire difference between `HasAttributes` and
        // `DropAttributes` -- which is what makes this the exact boolean complement structurally,
        // rather than by convention. With several pairs configured, this drops an event only when
        // *every* pair matches, not when any one does -- same conjunction `Matcher::matches` always
        // evaluates, just inverted at the very end.
        forward(!matched, event, &self.telemetry)
    }
}

/// Shared by both kinds. `keep` is "should this event be forwarded" -- already resolved by the
/// caller (`HasAttributes` passes its match result through; `DropAttributes` passes its negation).
/// The `0.0` on the forward path is deliberate, not a no-op: it registers the series so it appears
/// at zero rather than being absent, mirroring `HasSignal::process`'s own reasoning.
fn forward(keep: bool, event: Event, telemetry: &Telemetry) -> Option<Event> {
    telemetry.count("logit.transform.events.filtered", if keep { 0.0 } else { 1.0 }, &[]);
    keep.then_some(event)
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
        let event = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(has.process(&resource, event).is_some());
    }

    #[test]
    fn has_attributes_drops_an_event_whose_value_differs() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let event = event_with_attrs(&[("stream", Value::str("b"))]);
        assert!(has.process(&resource, event).is_none());
    }

    #[test]
    fn has_attributes_drops_an_event_missing_the_configured_key() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let event = event_with_attrs(&[]);
        assert!(has.process(&resource, event).is_none());
    }

    // -- HasAttributes: AND -------------------------------------------------------------------

    #[test]
    fn has_attributes_requires_every_configured_attribute_to_match() {
        let mut has = HasAttributes::new(
            vec![],
            vec![("stream".to_string(), Value::str("a")), ("tier".to_string(), Value::str("gold"))],
        );
        let resource = default_resource();

        let both = event_with_attrs(&[("stream", Value::str("a")), ("tier", Value::str("gold"))]);
        assert!(has.process(&resource, both).is_some(), "both pairs match");

        let one_only = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(has.process(&resource, one_only).is_none(), "only one of two pairs matches");
    }

    #[test]
    fn has_attributes_requires_both_maps_to_match() {
        let mut has = HasAttributes::new(
            vec![("service.name".to_string(), Value::str("nginx"))],
            vec![("stream".to_string(), Value::str("a"))],
        );
        let matching_resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        let other_resource = resource_with_attrs(&[("service.name", Value::str("haproxy"))]);

        let event = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(
            has.process(&matching_resource, event.clone()).is_some(),
            "both resource and attribute pairs match"
        );
        assert!(
            has.process(&other_resource, event).is_none(),
            "attribute pair matches but resource pair does not"
        );

        let mismatched_event = event_with_attrs(&[("stream", Value::str("b"))]);
        assert!(
            has.process(&matching_resource, mismatched_event).is_none(),
            "resource pair matches but attribute pair does not"
        );
    }

    #[test]
    fn has_attributes_matches_a_resource_attribute_without_touching_the_event() {
        let mut has =
            HasAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        let event = event_with_attrs(&[]);
        assert!(has.process(&resource, event).is_some());
    }

    #[test]
    fn a_resource_only_config_never_consults_the_event_attributes() {
        let mut has =
            HasAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let resource = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        // An event whose own attributes could never satisfy a stream/tier-shaped config still
        // forwards, because a resource-only config never looks at event.attributes at all.
        let event = event_with_attrs(&[("stream", Value::str("unrelated"))]);
        assert!(has.process(&resource, event).is_some());
    }

    // -- value_matches integration --------------------------------------------------------------

    #[test]
    fn has_attributes_coerces_across_numeric_variants() {
        for actual in [Value::I64(200), Value::U64(200), Value::F64(200.0), Value::str("200")] {
            let mut has = HasAttributes::new(vec![], vec![("status".to_string(), Value::I64(200))]);
            let resource = default_resource();
            let event = event_with_attrs(&[("status", actual.clone())]);
            assert!(has.process(&resource, event).is_some(), "{actual:?} should coerce-match 200");
        }
    }

    #[test]
    fn has_attributes_compares_bytes_and_str_alike() {
        let mut has = HasAttributes::new(vec![], vec![("host".to_string(), Value::str("web-1"))]);
        let resource = default_resource();
        let event =
            event_with_attrs(&[("host", Value::Bytes(bytes::Bytes::from_static(b"web-1")))]);
        assert!(has.process(&resource, event).is_some());
    }

    #[test]
    fn has_attributes_does_not_coerce_a_bool_to_a_string() {
        let mut has = HasAttributes::new(vec![], vec![("sampled".to_string(), Value::Bool(true))]);
        let resource = default_resource();
        let event = event_with_attrs(&[("sampled", Value::str("true"))]);
        assert!(has.process(&resource, event).is_none(), "Bool must never coerce to a string");
    }

    // -- never mutates --------------------------------------------------------------------------

    #[test]
    fn has_attributes_never_mutates_a_forwarded_event() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let event = event_with_attrs(&[("stream", Value::str("a")), ("other", Value::str("x"))]);
        let original_message = event.log.as_ref().map(|l| l.message.clone());
        let original_metrics = event.metrics.len();

        let event = has.process(&resource, event).expect("matches");
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

        for event in cases {
            let mut has = HasAttributes::new(vec![], config());
            let mut drop = DropAttributes::new(vec![], config());
            let resource = default_resource();

            let has_forwards = has.process(&resource, event.clone()).is_some();
            let drop_forwards = drop.process(&resource, event).is_some();
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

        let both = event_with_attrs(&[("stream", Value::str("a")), ("tier", Value::str("gold"))]);
        assert!(drop.process(&resource, both).is_none(), "every pair matches -- dropped");

        let one_only = event_with_attrs(&[("stream", Value::str("a"))]);
        assert!(
            drop.process(&resource, one_only).is_some(),
            "only one of two pairs matches -- forwarded, not dropped"
        );
    }

    #[test]
    fn drop_attributes_forwards_an_event_missing_the_configured_key() {
        let mut drop = DropAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        let event = event_with_attrs(&[]);
        assert!(
            drop.process(&resource, event).is_some(),
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

        for event in events {
            let has_result = has.process(&resource, event.clone());
            let drop_result = drop.process(&resource, event);
            assert!(
                has_result.is_some() != drop_result.is_some(),
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
        has.process(&resource, event_with_attrs(&[]));
        assert_eq!(has.cached_resource_matched(), Some(true), "the first call populates the cache");
        has.process(&resource, event_with_attrs(&[]));
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
        drop.process(&resource, event_with_attrs(&[]));
        assert_eq!(drop.cached_resource_matched(), Some(true));
    }

    #[test]
    fn two_value_equal_but_distinct_resource_arcs_do_not_falsely_hit_the_cache() {
        let mut has =
            HasAttributes::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![]);
        let a = resource_with_attrs(&[("service.name", Value::str("nginx"))]);
        let b = resource_with_attrs(&[("service.name", Value::str("nginx"))]); // distinct Arc

        assert!(has.process(&a, event_with_attrs(&[])).is_some());
        assert!(
            has.process(&b, event_with_attrs(&[])).is_some(),
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

        has.process(&resource, event_with_attrs(&[("stream", Value::str("a"))]));
        has.process(&resource, event_with_attrs(&[("stream", Value::str("b"))]));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.events.filtered"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        let mut has = HasAttributes::new(vec![], vec![("stream".to_string(), Value::str("a"))]);
        let resource = default_resource();
        assert!(has.process(&resource, event_with_attrs(&[("stream", Value::str("a"))])).is_some());
    }
}
