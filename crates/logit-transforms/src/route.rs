//! `route`: equality-only routing, the native half of `docs/adr/target-components.md`'s two
//! router kinds (`lua`/`lua_file`'s `event:to(..)` is the other, W5). `by:` names exactly one key
//! to read per event -- a batch's cached provenance `origin`/`previous`, an event's own
//! attribute, or the batch's resource attribute -- and `routes:` maps each value that key might
//! take to a target id. An absent key, or a value no route names, is unrouted
//! (`logit_pipeline::Destination::Forward`): the router's own consumers, or dropped and counted
//! `logit.component.events.dropped{reason="unrouted"}` if it has none (`crate::runtime::
//! run_router`, not this module -- see below).
//!
//! **Every route value is resolved to its target's slot once, in [`Route::new`], not per event.**
//! `targets` is [`logit_pipeline::graph::targets_of`]'s output for this component -- the same
//! slot order the node runtime builds its `Vec<Fanout>` in -- so `Destination::To(n)` here means
//! exactly `targets[n]` there. Rules 48/51 (`docs/design/pipeline-graph.md`) guarantee every
//! `routes:` value names a target in that list before this is ever constructed; if one didn't,
//! that would be a graph-validation bug, not a runtime condition to recover from, so
//! [`Route::new`] panics rather than threading a `Result` through a hot-path constructor nothing
//! else on this trait needs.
//!
//! **`Origin`/`Previous` compare interned `Symbol`s -- plain integer equality, no string compare,
//! no allocation.** This is `has_provenance`'s own `origin`/`previous` matching
//! (`crate::provenance`), pointed at a destination instead of a keep/drop verdict: the route
//! values are interned once here, the batch's provenance is cached from `observe_provenance`
//! exactly as `has_provenance` caches it, and `route` is a linear scan of a handful of `Symbol`s
//! per event.
//!
//! **`Attribute`/`Resource` reuse `has_attributes`' own coercing comparator (`crate::
//! value_matches`), not a second equality.** A route value is always a plain YAML string (`routes:`
//! is a `BTreeMap<String, String>`, `logit_config::ComponentKind::Route`), converted once to
//! `Value::str(..)` at construction; `value_matches` is what lets a configured `"500"` match an
//! attribute that arrived as `Value::I64(500)` off JSON and vice versa, the same coercion
//! `has_attributes` guarantees. The key itself is interned once and read back with `AttrMap::
//! get_sym`, `has_attributes`' own allocation-free lookup -- no string hashing, no allocation, on
//! the hot path.
//!
//! **Why a linear scan, not a `HashMap`.** `routes:` is an operator-authored list of a handful of
//! alternatives (`has_provenance`/`has_attributes` make the same call for the same reason) --
//! `Symbol`/`Value` equality is a few integer or byte compares, and a hash map would pay a hash
//! and a probe for no measurable win at this size while costing an allocation to build.
//!
//! **Why no predicate.** `by:` is exactly one of `{provenance: origin}`, `{provenance: previous}`,
//! `{attribute: <key>}`, `{resource: <key>}` -- one key read per event, equality only, no
//! operators, no boolean algebra across keys. `docs/adr/routing-by-condition-is-lua.md`'s holding
//! is unchanged: anything needing an operator is still a `lua` component with `targets:` (W5).
//!
//! **No layer-3 telemetry.** Like `generate_in` (`crates/logit-inputs/src/generate.rs`'s own
//! "Telemetry" section), this component records no points of its own: the node runtime's uniform
//! per-component instrumentation (`crate::runtime::run_router`/`route_batch`) already counts
//! batches/events in and out on every destination's own `Fanout`, plus `events.dropped{reason=
//! "unrouted"}` for the forward partition when there are no ordinary consumers -- a second counter
//! here would only restate what layer 2 already has. `Route` therefore carries no `Telemetry`
//! field at all, rather than one nothing ever reads.

use crate::value_matches;
use logit_config::{ProvenanceField, RouteBy};
use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Provenance, Resource, Value};
use logit_pipeline::{Destination, Router};
use std::collections::BTreeMap;
use std::sync::Arc;

/// What [`Route`] reads from each event, with every configured value already resolved to a
/// target slot (see the module doc). `Origin`/`Previous` compare `Symbol`s; `Attribute`/
/// `Resource` compare `Value`s through [`value_matches`] after an interned-key lookup.
enum By {
    Origin(Vec<(Symbol, u16)>),
    Previous(Vec<(Symbol, u16)>),
    Attribute(Symbol, Vec<(Value, u16)>),
    Resource(Symbol, Vec<(Value, u16)>),
}

/// The native `route` component (`docs/adr/target-components.md`). See the module doc for the
/// per-`by:` semantics and why each is allocation-free per event.
pub struct Route {
    by: By,
    /// Cached from `observe_provenance`, which fires once per incoming batch before any of that
    /// batch's events reach `route` -- `crate::provenance::Matcher`'s own caching, applied here.
    /// Irrelevant (and never read) for `Attribute`/`Resource` routing.
    provenance: Provenance,
}

impl Route {
    /// `targets` is the slot order (`logit_pipeline::graph::targets_of`'s output for this
    /// component, `ResolvedComponent::targets`) -- every `routes` value is resolved to its
    /// position in it, once, here. Panics if a value isn't in `targets`: rules 48/51 guarantee
    /// every `routes:` value names a target this router directs at, so that can only happen if a
    /// caller builds a `Route` from a `routes`/`targets` pair that didn't come from a resolved
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

/// Linear scan for `Origin`/`Previous`: `Symbol` equality is a plain integer compare, and
/// `routes` is a handful of alternatives at most (see the module doc's "why a linear scan").
fn by_symbol(routes: &[(Symbol, u16)], actual: Symbol) -> Destination {
    routes
        .iter()
        .find(|(sym, _)| *sym == actual)
        .map_or(Destination::Forward, |(_, slot)| Destination::To(*slot))
}

/// Linear scan for `Attribute`/`Resource`, through [`value_matches`] -- `has_attributes`' own
/// coercing comparator, so a configured `"500"` matches an actual `Value::I64(500)` and vice
/// versa.
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
