//! `route`: native equality-only routing, one of `docs/adr/target-components.md`'s two router
//! kinds (`lua`'s `event:to(..)` is the other). `by:` names one key to read per event (the
//! batch's provenance `origin`/`previous`, an event attribute, a resource attribute, or the
//! signals the event carries), and `routes:` maps each value of it to a target id or a list of
//! them. An absent key, or a value no route names, is `Destinations::FORWARD`: it goes to the
//! router's own consumers, which graph rule 50 requires (a `null_out` among them when the operator
//! wants those events discarded).
//!
//! **Every route value is resolved to a [`Destinations`] set once, in [`Route::new`].** `targets`
//! is [`logit_pipeline::graph::targets_of`]'s output, the slot order the node runtime builds its
//! `Vec<Fanout>` in, so target `n` means `targets[n]` there. Graph rules 48/51 guarantee every
//! `routes:` id names a target in that list and that there are at most
//! [`Destinations::MAX_TARGETS`], so a miss is a validation bug and [`Route::new`] panics rather
//! than returning a `Result`.
//!
//! **A list is the only way one value reaches several targets.** Matching is first-match over
//! `routes:` in key order, so two keys that both match one value never union.
//!
//! **`Signal` is not first-match: it unions.** An event's mark is the union, over each signal it
//! carries, of that signal's targets, with `Forward` for a carried signal no route names, so a
//! log-and-metrics event with only `logs` routed reaches the logs target and the router's own
//! consumers, whole both times. An event carrying no signal is `Forward`, where `has_signal`
//! would drop it: a router never drops. All eight carried-signal combinations are resolved in
//! [`Route::new`], so the per-event path is three presence checks and an index.
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
//! already count batches and events on every destination's `Fanout`, and the copies a list or a
//! mixed-signal event makes.

use crate::value_matches;
use logit_config::{ProvenanceField, RouteBy, RouteTargets, Signal};
use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Provenance, Resource, Value};
use logit_pipeline::{signal_index, signal_masks, Destinations, Router};
use std::collections::BTreeMap;
use std::sync::Arc;

/// What [`Route`] reads from each event, with every configured value resolved to its targets.
enum By {
    Origin(Vec<(Symbol, Destinations)>),
    Previous(Vec<(Symbol, Destinations)>),
    Attribute(Symbol, Vec<(Value, Destinations)>),
    Resource(Symbol, Vec<(Value, Destinations)>),
    /// [`signal_masks`]' table, indexed by [`signal_index`] of the signals an event carries.
    Signal([Destinations; 8]),
}

/// The native `route` component; see the module doc.
pub struct Route {
    by: By,
    /// Cached from `observe_provenance`, which fires once per batch before its events reach
    /// `route`. Read only for `Origin`/`Previous`.
    provenance: Provenance,
}

impl Route {
    /// Resolves every `routes` id to its slot in `targets` (`ResolvedComponent::targets`).
    ///
    /// Panics if an id isn't in `targets`, if there are more than
    /// [`Destinations::MAX_TARGETS`], or if a `by: signal` key names no signal, all of which
    /// graph rules 48/51 rule out for a resolved graph.
    pub fn new(by: RouteBy, routes: &BTreeMap<String, RouteTargets>, targets: &[String]) -> Self {
        assert!(
            targets.len() <= Destinations::MAX_TARGETS,
            "route has {} targets, over the {} rule 51 allows",
            targets.len(),
            Destinations::MAX_TARGETS
        );
        let mark_of = |ids: &RouteTargets| -> Destinations {
            ids.ids().fold(Destinations::NONE, |mark, target| {
                let slot = targets.iter().position(|t| t == target).unwrap_or_else(|| {
                    panic!(
                        "route target '{target}' is not among this router's targets {targets:?} \
                         -- rules 48/51 should have guaranteed this resolves"
                    )
                });
                mark | Destinations::target(slot as u16)
            })
        };
        let by = match by {
            RouteBy::Provenance(ProvenanceField::Origin) => By::Origin(
                routes.iter().map(|(value, ids)| (intern(value), mark_of(ids))).collect(),
            ),
            RouteBy::Provenance(ProvenanceField::Previous) => By::Previous(
                routes.iter().map(|(value, ids)| (intern(value), mark_of(ids))).collect(),
            ),
            RouteBy::Attribute(key) => By::Attribute(
                intern(&key),
                routes
                    .iter()
                    .map(|(value, ids)| (Value::str(value.clone()), mark_of(ids)))
                    .collect(),
            ),
            RouteBy::Resource(key) => By::Resource(
                intern(&key),
                routes
                    .iter()
                    .map(|(value, ids)| (Value::str(value.clone()), mark_of(ids)))
                    .collect(),
            ),
            RouteBy::Signal => {
                let mut per_signal = [Destinations::FORWARD; 3];
                for (key, ids) in routes {
                    let signal = Signal::from_name(key).unwrap_or_else(|| {
                        panic!("route key '{key}' is not a signal -- rule 51 should have caught it")
                    });
                    let bit = match signal {
                        Signal::Logs => 0,
                        Signal::Metrics => 1,
                        Signal::Traces => 2,
                    };
                    per_signal[bit] = mark_of(ids);
                }
                By::Signal(signal_masks(per_signal, Destinations::FORWARD))
            }
        };
        Self { by, provenance: Provenance::default() }
    }
}

impl Router for Route {
    fn observe_provenance(&mut self, provenance: Provenance) {
        self.provenance = provenance;
    }

    fn route(&mut self, resource: &Arc<Resource>, event: &Event) -> Destinations {
        match &self.by {
            By::Origin(routes) => match self.provenance.origin {
                Some(sym) => by_symbol(routes, sym),
                None => Destinations::FORWARD,
            },
            By::Previous(routes) => match self.provenance.previous {
                Some(sym) => by_symbol(routes, sym),
                None => Destinations::FORWARD,
            },
            By::Attribute(key, routes) => match event.attributes.get_sym(*key) {
                Some(actual) => by_value(routes, actual),
                None => Destinations::FORWARD,
            },
            By::Resource(key, routes) => match resource.attributes.get_sym(*key) {
                Some(actual) => by_value(routes, actual),
                None => Destinations::FORWARD,
            },
            By::Signal(masks) => masks[signal_index(event)],
        }
    }
}

/// The targets of the route naming `actual`, else `Forward`.
fn by_symbol(routes: &[(Symbol, Destinations)], actual: Symbol) -> Destinations {
    routes.iter().find(|(sym, _)| *sym == actual).map_or(Destinations::FORWARD, |(_, mark)| *mark)
}

/// The targets of the first route whose value [`value_matches`] `actual`, else `Forward`.
fn by_value(routes: &[(Value, Destinations)], actual: &Value) -> Destinations {
    routes
        .iter()
        .find(|(configured, _)| value_matches(configured, actual))
        .map_or(Destinations::FORWARD, |(_, mark)| *mark)
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

    fn routes(pairs: &[(&str, &str)]) -> BTreeMap<String, RouteTargets> {
        pairs.iter().map(|(v, t)| (v.to_string(), RouteTargets::from(*t))).collect()
    }

    fn route_lists(pairs: &[(&str, &[&str])]) -> BTreeMap<String, RouteTargets> {
        pairs
            .iter()
            .map(|(v, ts)| {
                (v.to_string(), RouteTargets(ts.iter().map(|t| t.to_string()).collect()))
            })
            .collect()
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
        assert_eq!(route.route(&default_resource(), &event()), Destinations::target(0));

        route.observe_provenance(provenance(Some("edge_app"), None));
        assert_eq!(route.route(&default_resource(), &event()), Destinations::target(1));
    }

    #[test]
    fn an_unknown_origin_forwards() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Origin),
            &routes(&[("edge_host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        route.observe_provenance(provenance(Some("edge_unknown"), None));
        assert_eq!(route.route(&default_resource(), &event()), Destinations::FORWARD);
    }

    #[test]
    fn an_absent_origin_forwards() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Origin),
            &routes(&[("edge_host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        // Provenance::default() -- no Fanout has ever stamped this batch.
        assert_eq!(route.route(&default_resource(), &event()), Destinations::FORWARD);
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
        assert_eq!(route.route(&default_resource(), &event()), Destinations::target(0));
    }

    #[test]
    fn a_default_provenance_forwards() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Previous),
            &routes(&[("tag_host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        assert_eq!(route.route(&default_resource(), &event()), Destinations::FORWARD);
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
            Destinations::target(0)
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("app"))),
            Destinations::target(1)
        );
    }

    #[test]
    fn an_absent_attribute_key_forwards() {
        let mut route = Route::new(
            RouteBy::Attribute("stream".to_string()),
            &routes(&[("host", "host_stream")]),
            &targets(&["host_stream"]),
        );
        assert_eq!(route.route(&default_resource(), &event()), Destinations::FORWARD);
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
            Destinations::FORWARD
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
            Destinations::target(0)
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("node"))),
            Destinations::target(0)
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
            Destinations::target(0),
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
            Destinations::target(0)
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
        assert_eq!(route.route(&resource, &event()), Destinations::target(0));
    }

    #[test]
    fn an_absent_resource_key_forwards() {
        let mut route = Route::new(
            RouteBy::Resource("service.name".to_string()),
            &routes(&[("nginx", "nginx_stream")]),
            &targets(&["nginx_stream"]),
        );
        assert_eq!(route.route(&default_resource(), &event()), Destinations::FORWARD);
    }

    // -- Lists ------------------------------------------------------------------------------

    #[test]
    fn a_list_value_names_every_listed_target() {
        let mut route = Route::new(
            RouteBy::Attribute("stream".to_string()),
            &route_lists(&[("host", &["archive", "host_stream"]), ("app", &["app_stream"])]),
            &targets(&["archive", "host_stream", "app_stream"]),
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("host"))),
            Destinations::target(0) | Destinations::target(1)
        );
        assert_eq!(
            route.route(&default_resource(), &event_with_attr("stream", Value::str("app"))),
            Destinations::target(2)
        );
    }

    #[test]
    fn matching_is_first_match_so_two_matching_keys_never_union() {
        // "500" and "500.0" both coerce-match `I64(500)`; the first in key order wins, so a list
        // is the only way one value reaches several targets.
        let mut route = Route::new(
            RouteBy::Attribute("status".to_string()),
            &routes(&[("500", "a"), ("500.0", "b")]),
            &targets(&["a", "b"]),
        );
        let mark = route.route(&default_resource(), &event_with_attr("status", Value::I64(500)));
        assert_eq!(mark, Destinations::target(0), "first match only");

        let mut alone = Route::new(
            RouteBy::Attribute("status".to_string()),
            &routes(&[("500.0", "b")]),
            &targets(&["b"]),
        );
        assert_eq!(
            alone.route(&default_resource(), &event_with_attr("status", Value::I64(500))),
            Destinations::target(0),
            "\"500.0\" alone must match too, or the case above proves nothing"
        );
    }

    #[test]
    fn a_list_value_works_under_provenance() {
        let mut route = Route::new(
            RouteBy::Provenance(ProvenanceField::Origin),
            &route_lists(&[("edge", &["a", "b"])]),
            &targets(&["a", "b"]),
        );
        route.observe_provenance(provenance(Some("edge"), None));
        assert_eq!(
            route.route(&default_resource(), &event()),
            Destinations::target(0) | Destinations::target(1)
        );
    }

    // -- Signal -----------------------------------------------------------------------------

    fn metric_record() -> logit_core::MetricRecord {
        logit_core::MetricRecord::new(intern("m"), logit_core::MetricKind::counter(1.0))
    }

    fn span_record() -> logit_core::SpanRecord {
        use logit_core::{SpanKind, SpanRecord, SpanStatus};
        SpanRecord {
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: None,
            name: Value::str("op"),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 0,
            flags: 0,
            ext: None,
        }
    }

    fn by_signal(pairs: &[(&str, &str)], ids: &[&str]) -> Route {
        Route::new(RouteBy::Signal, &routes(pairs), &targets(ids))
    }

    #[test]
    fn by_signal_routes_a_single_signal_event_to_its_target() {
        let mut route =
            by_signal(&[("logs", "l"), ("metrics", "m"), ("traces", "t")], &["l", "m", "t"]);
        assert_eq!(route.route(&default_resource(), &event()), Destinations::target(0));
        let metric = Event::metric(0, AttrMap::new(), metric_record());
        assert_eq!(route.route(&default_resource(), &metric), Destinations::target(1));
        let span = Event::span(0, AttrMap::new(), span_record());
        assert_eq!(route.route(&default_resource(), &span), Destinations::target(2));
    }

    #[test]
    fn by_signal_sends_a_mixed_event_to_every_carried_signals_target() {
        let mut route =
            by_signal(&[("logs", "l"), ("metrics", "m"), ("traces", "t")], &["l", "m", "t"]);
        let mut mixed = event();
        mixed.metrics.push(metric_record());
        mixed.span = Some(span_record());
        assert_eq!(
            route.route(&default_resource(), &mixed),
            Destinations::target(0) | Destinations::target(1) | Destinations::target(2)
        );
    }

    #[test]
    fn by_signal_also_forwards_a_mixed_event_whose_other_signal_has_no_route() {
        let mut route = by_signal(&[("logs", "l")], &["l"]);
        let mut mixed = event();
        mixed.metrics.push(metric_record());
        assert_eq!(
            route.route(&default_resource(), &mixed),
            Destinations::target(0) | Destinations::FORWARD,
            "the metrics payload must not drop out of the else-branch"
        );
        let metric = Event::metric(0, AttrMap::new(), metric_record());
        assert_eq!(route.route(&default_resource(), &metric), Destinations::FORWARD);
    }

    #[test]
    fn by_signal_forwards_an_event_with_no_payload() {
        let mut route = by_signal(&[("logs", "l")], &["l"]);
        let mut empty = event();
        empty.log = None;
        assert_eq!(route.route(&default_resource(), &empty), Destinations::FORWARD);
    }
}
