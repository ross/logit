//! `route`: native equality-only routing, one of `docs/adr/target-components.md`'s two router
//! kinds (`lua`'s `event:to(..)` is the other). `by:` names one key to read per event (the
//! batch's provenance `origin`/`previous`, an event attribute, or a resource attribute), and
//! `routes:` maps each value of it to a target id. An absent key, or a value no route names, is
//! `Destination::Forward`: it goes to the router's own consumers, or, with none,
//! `logit_pipeline::runtime::run_router` drops and counts it as
//! `logit.component.events.dropped{reason="unrouted"}`.
//!
//! **Every route value is resolved to its target's slot once, in [`Route::new`].** `targets` is
//! [`logit_pipeline::graph::targets_of`]'s output, the slot order the node runtime builds its
//! `Vec<Fanout>` in, so `Destination::To(n)` means `targets[n]` there. Graph rules 48/51
//! guarantee every `routes:` value names a target in that list, so a miss is a validation bug and
//! [`Route::new`] panics rather than returning a `Result`.
//!
//! **`Origin`/`Previous` compare interned `Symbol`s,** with provenance cached from
//! `observe_provenance` as `has_provenance` does.
//!
//! **`Attribute`/`Resource` compare through `crate::value_matches`,** `has_attributes`' coercing
//! comparator. A route value is always a YAML string, so this is what lets a configured `"500"`
//! match an attribute that arrived as `Value::I64(500)`, and vice versa. The key is interned once
//! and read with `AttrMap::get_sym`, so the per-event path allocates nothing.
//!
//! **Linear scan, not a `HashMap`:** `routes:` is a handful of operator-authored alternatives, and
//! a map would pay a hash, a probe, and a build allocation for no win at that size.
//!
//! **No predicate.** One key, equality only, no operators or boolean algebra across keys;
//! anything more is a `lua` component with `targets:` (`docs/adr/routing-by-condition-is-lua.md`).
//!
//! **No layer-3 telemetry, and so no `Telemetry` field.** The runtime's `run_router`/`route_batch`
//! already count batches and events on every destination's `Fanout`, plus the unrouted drops.

use crate::value_matches;
use logit_config::{ProvenanceField, RouteBy};
use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Provenance, Resource, Value};
use logit_pipeline::{Destination, Router};
use std::collections::BTreeMap;
use std::sync::Arc;

/// What [`Route`] reads from each event, with every configured value resolved to a target slot.
enum By {
    Origin(Vec<(Symbol, u16)>),
    Previous(Vec<(Symbol, u16)>),
    Attribute(Symbol, Vec<(Value, u16)>),
    Resource(Symbol, Vec<(Value, u16)>),
}

/// The native `route` component; see the module doc.
pub struct Route {
    by: By,
    /// Cached from `observe_provenance`, which fires once per batch before its events reach
    /// `route`. Read only for `Origin`/`Previous`.
    provenance: Provenance,
}

impl Route {
    /// Resolves every `routes` value to its slot in `targets` (`ResolvedComponent::targets`).
    ///
    /// Panics if a value isn't in `targets`, which graph rules 48/51 rule out for a resolved
    /// graph.
    pub fn new(by: RouteBy, routes: &BTreeMap<String, String>, targets: &[String]) -> Self {
        let slot_of = |target: &str| -> u16 {
            targets
                .iter()
                .position(|t| t == target)
                .unwrap_or_else(|| {
                    panic!(
                        "route target '{target}' is not among this router's targets {targets:?} \
                         -- rules 48/51 should have guaranteed this resolves"
                    )
                })
                .try_into()
                .expect("more targets than a u16 slot can address")
        };
        let by = match by {
            RouteBy::Provenance(ProvenanceField::Origin) => By::Origin(
                routes.iter().map(|(value, target)| (intern(value), slot_of(target))).collect(),
            ),
            RouteBy::Provenance(ProvenanceField::Previous) => By::Previous(
                routes.iter().map(|(value, target)| (intern(value), slot_of(target))).collect(),
            ),
            RouteBy::Attribute(key) => By::Attribute(
                intern(&key),
                routes
                    .iter()
                    .map(|(value, target)| (Value::str(value.clone()), slot_of(target)))
                    .collect(),
            ),
            RouteBy::Resource(key) => By::Resource(
                intern(&key),
                routes
                    .iter()
                    .map(|(value, target)| (Value::str(value.clone()), slot_of(target)))
                    .collect(),
            ),
        };
        Self { by, provenance: Provenance::default() }
    }
}

impl Router for Route {
    fn observe_provenance(&mut self, provenance: Provenance) {
        self.provenance = provenance;
    }

    fn route(&mut self, resource: &Arc<Resource>, event: &Event) -> Destination {
        match &self.by {
            By::Origin(routes) => match self.provenance.origin {
                Some(sym) => by_symbol(routes, sym),
                None => Destination::Forward,
            },
            By::Previous(routes) => match self.provenance.previous {
                Some(sym) => by_symbol(routes, sym),
                None => Destination::Forward,
            },
            By::Attribute(key, routes) => match event.attributes.get_sym(*key) {
                Some(actual) => by_value(routes, actual),
                None => Destination::Forward,
            },
            By::Resource(key, routes) => match resource.attributes.get_sym(*key) {
                Some(actual) => by_value(routes, actual),
                None => Destination::Forward,
            },
        }
    }
}

/// The slot of the route naming `actual`, else `Forward`.
fn by_symbol(routes: &[(Symbol, u16)], actual: Symbol) -> Destination {
    routes
        .iter()
        .find(|(sym, _)| *sym == actual)
        .map_or(Destination::Forward, |(_, slot)| Destination::To(*slot))
}

/// The slot of the first route whose value [`value_matches`] `actual`, else `Forward`.
fn by_value(routes: &[(Value, u16)], actual: &Value) -> Destination {
    routes
        .iter()
        .find(|(configured, _)| value_matches(configured, actual))
        .map_or(Destination::Forward, |(_, slot)| Destination::To(*slot))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord};

    fn event() -> Event {
        Event::log(
            0,
            AttrMap::new(),
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

    fn event_with_attr(key: &str, value: Value) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(key, value);
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

    fn resource_with_attr(key: &str, value: Value) -> Arc<Resource> {
        let mut attrs = AttrMap::new();
        attrs.insert(key, value);
        Arc::new(Resource { attributes: attrs, ..Default::default() })
    }

    fn routes(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(v, t)| (v.to_string(), t.to_string())).collect()
    }

    fn targets(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn provenance(origin: Option<&str>, previous: Option<&str>) -> Provenance {
        Provenance { origin: origin.map(intern), previous: previous.map(intern) }
    }

    // -- Origin -----------------------------------------------------------------------------

    #[test]
    fn routes_by_origin_to_the_matching_targets_slot() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Origin),
            &routes(&[("edge_host", "host_stream"), ("edge_app", "app_stream")]),
            &targets(&["host_stream", "app_stream"]),
        );
        route.observe_provenance(provenance(Some("edge_host"), None));
        assert_eq!(route.route(&default_resource(), &event()), Destination::To(0));

        route.observe_provenance(provenance(Some("edge_app"), None));
        assert_eq!(route.route(&default_resource(), &event()), Destination::To(1));
    }

    #[test]
    fn an_unknown_origin_forwards() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Origin),
            &routes(&[("edge_host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        route.observe_provenance(provenance(Some("edge_unknown"), None));
        assert_eq!(route.route(&default_resource(), &event()), Destination::Forward);
    }

    #[test]
    fn an_absent_origin_forwards() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Origin),
            &routes(&[("edge_host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        // Provenance::default() -- no Fanout has ever stamped this batch.
        assert_eq!(route.route(&default_resource(), &event()), Destination::Forward);
    }

    // -- Previous ---------------------------------------------------------------------------

    #[test]
    fn routes_by_previous_to_the_matching_targets_slot() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Previous),
            &routes(&[("tag_host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        route.observe_provenance(provenance(Some("edge_in"), Some("tag_host")));
        assert_eq!(route.route(&default_resource(), &event()), Destination::To(0));
    }

    #[test]
    fn a_default_provenance_forwards() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Previous),
            &routes(&[("tag_host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        assert_eq!(route.route(&default_resource(), &event()), Destination::Forward);
    }

    // -- Attribute --------------------------------------------------------------------------

    #[test]
    fn routes_by_attribute_to_the_matching_targets_slot() {
        let mut route = Route::new(
            RouteBy::Attribute("stream".to_string()),
            &routes(&[("host", "host_stream"), ("app", "app_stream")]),
            &targets(&["host_stream", "app_stream"]),
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("host"))),
            Destination::To(0)
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("app"))),
            Destination::To(1)
        );
    }

    #[test]
    fn an_absent_attribute_key_forwards() {
        let mut route = Route::new(
            RouteBy::Attribute("stream".to_string()),
            &routes(&[("host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        assert_eq!(route.route(&default_resource(), &event()), Destination::Forward);
    }

    #[test]
    fn an_unknown_attribute_value_forwards() {
        let mut route = Route::new(
            RouteBy::Attribute("stream".to_string()),
            &routes(&[("host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("db"))),
            Destination::Forward
        );
    }

    #[test]
    fn many_to_one_attribute_values_collapse_to_the_same_slot() {
        let mut route = Route::new(
            RouteBy::Attribute("stream".to_string()),
            &routes(&[("host", "t"), ("node", "t")]),
            &targets(&["t"]),
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("host"))),
            Destination::To(0)
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("node"))),
            Destination::To(0)
        );
    }

    #[test]
    fn a_numeric_attribute_coerce_matches_a_string_route_value() {
        let mut route = Route::new(
            RouteBy::Attribute("status".to_string()),
            &routes(&[("500", "errors")]),
            &targets(&["errors"]),
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("status", Value::I64(500))),
            Destination::To(0),
            "config \"500\" must coerce-match an actual Value::I64(500)"
        );
    }

    #[test]
    fn a_string_attribute_matches_a_route_value_that_looks_numeric() {
        // The reverse direction of the coercion above: the actual attribute arrived as a string
        // (e.g. off a logfmt line) but still matches the same configured route value.
        let mut route = Route::new(
            RouteBy::Attribute("status".to_string()),
            &routes(&[("500", "errors")]),
            &targets(&["errors"]),
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("status", Value::str("500"))),
            Destination::To(0)
        );
    }

    // -- Resource ---------------------------------------------------------------------------

    #[test]
    fn routes_by_resource_to_the_matching_targets_slot() {
        let mut route = Route::new(
            RouteBy::Resource("service.name".to_string()),
            &routes(&[("nginx", "nginx_stream")]),
            &targets(&["nginx_stream"]),
        );
        let resource = resource_with_attr("service.name", Value::str("nginx"));
        assert_eq!(route.route(&resource, &event()), Destination::To(0));
    }

    #[test]
    fn an_absent_resource_key_forwards() {
        let mut route = Route::new(
            RouteBy::Resource("service.name".to_string()),
            &routes(&[("nginx", "nginx_stream")]),
            &targets(&["nginx_stream"]),
        );
        assert_eq!(route.route(&default_resource(), &event()), Destination::Forward);
    }
}
