//! `has_provenance`/`drop_provenance`: filter events by an operator-configured match against a
//! batch's `origin`/`previous` (`docs/adr/batch-provenance-on-delivered.md`) -- the `has_signal`/
//! `has_attributes` shape applied to graph identity instead of payload presence or event data.
//! See `docs/adr/provenance-filtering-components.md`.
//!
//! **`origin:`/`previous:` are each a list of alternatives, OR'd within the field; the two fields
//! AND together when both are configured.** This is `has_signal`'s disjunction-within-one-list
//! shape (`crate::signals`), applied per field, combined with `has_attributes`'
//! conjunction-across-fields shape (`crate::attributes`) -- not a new matching primitive, just
//! those two composed. An empty list means "not checked" for that field, the same "absent
//! contributes nothing" convention `has_attributes`' empty maps use.
//!
//! **`drop_provenance` is the exact complement of `has_provenance` on the same config, taken at
//! the top level, not per field**: it drops an event only when the *whole* configured match
//! succeeds; an event matching only `origin:` but not `previous:` (when both are configured) is
//! forwarded. Structural here, not a convention to remember -- `DropProvenance::process` is
//! `HasProvenance::process` with a single `!`, exactly `has_attributes`'/`drop_attributes`' own
//! relationship.
//!
//! **A batch with no provenance at all (`Provenance::default()`) never matches a non-empty
//! field** -- the same "absent is `false`" rule `has_attributes` has for a missing attribute.
//! `Fanout` stamps `origin`/`previous` on every real hop
//! (`docs/adr/batch-provenance-on-delivered.md`), so this only matters for a batch observed
//! before any `Fanout` ever touched it (a bench or unit test constructing a bare `Provenance`
//! directly).

use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Provenance, Resource, Telemetry};
use logit_pipeline::Transform;
use std::sync::Arc;

/// Shared by [`HasProvenance`] and [`DropProvenance`] -- both kinds are this plus a `!` at the one
/// call site in each `process`.
struct Matcher {
    /// Interned once, at construction, from `logit-cli::pipeline::to_symbol_list`'s config
    /// conversion. Empty means "not checked" for this field.
    origin: Vec<Symbol>,
    previous: Vec<Symbol>,
    /// Cached from `observe_provenance`, which fires once per incoming batch before any of that
    /// batch's events reach `process` -- constant for the whole batch, exactly `Aggregator::
    /// observe_batch_context`'s existing pattern for `TraceContext`
    /// (`crates/logit-transforms/src/aggregate.rs`), applied to `Provenance` instead.
    provenance: Provenance,
}

impl Matcher {
    fn new(origin: Vec<String>, previous: Vec<String>) -> Self {
        Self {
            origin: origin.iter().map(|s| intern(s)).collect(),
            previous: previous.iter().map(|s| intern(s)).collect(),
            provenance: Provenance::default(),
        }
    }

    /// `true` iff every configured field matches: `origin` is one of the listed alternatives (if
    /// `origin:` is non-empty) *and* `previous` is one of the listed alternatives (if `previous:`
    /// is non-empty). A `Vec::contains` linear scan, not a `HashSet` -- both lists are a handful
    /// of component ids at most, and `Symbol` equality is a plain integer compare.
    fn matches(&self) -> bool {
        let origin_ok = self.origin.is_empty()
            || self.provenance.origin.is_some_and(|o| self.origin.contains(&o));
        let previous_ok = self.previous.is_empty()
            || self.provenance.previous.is_some_and(|p| self.previous.contains(&p));
        origin_ok && previous_ok
    }
}

/// Forwards an event whose batch's `origin`/`previous` match every configured field, dropping the
/// rest. Never mutates a forwarded event -- like `HasAttributes`, this only ever decides whether
/// to forward, never what to forward. See the module doc for the AND-across-fields/OR-within-field
/// rule and absent-is-`false`.
pub struct HasProvenance {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl HasProvenance {
    /// `origin`/`previous` are plain component-id strings, interned once here --
    /// `logit-cli::pipeline::to_symbol_list` builds both from `ComponentKind::HasProvenance`'s
    /// identically-shaped config.
    pub fn new(origin: Vec<String>, previous: Vec<String>) -> Self {
        Self { matcher: Matcher::new(origin, previous), telemetry: Telemetry::default() }
    }

    /// See [`crate::Keep::with_telemetry`] -- same reasoning, no `Diagnostics` here either:
    /// matching a fixed set of configured values can't fail.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for HasProvenance {
    fn observe_provenance(&mut self, provenance: Provenance) {
        self.matcher.provenance = provenance;
    }

    fn process(&mut self, _resource: &Arc<Resource>, event: Event) -> Option<Event> {
        forward(self.matcher.matches(), event, &self.telemetry)
    }
}

/// Drops an event whose batch's `origin`/`previous` match every configured field, forwarding the
/// rest -- the exact complement of [`HasProvenance`] on the same config. See the module doc for
/// why the complement is taken at the top level, not per field.
pub struct DropProvenance {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl DropProvenance {
    /// See [`HasProvenance::new`] -- identical signature and reasoning.
    pub fn new(origin: Vec<String>, previous: Vec<String>) -> Self {
        Self { matcher: Matcher::new(origin, previous), telemetry: Telemetry::default() }
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for DropProvenance {
    fn observe_provenance(&mut self, provenance: Provenance) {
        self.matcher.provenance = provenance;
    }

    fn process(&mut self, _resource: &Arc<Resource>, event: Event) -> Option<Event> {
        // The single `!` here is the entire difference between `HasProvenance` and
        // `DropProvenance` -- which is what makes this the exact boolean complement structurally,
        // rather than by convention. With both fields configured, this drops an event only when
        // *both* match, not when either one does -- same conjunction `Matcher::matches` always
        // evaluates, just inverted at the very end.
        forward(!self.matcher.matches(), event, &self.telemetry)
    }
}

/// Shared by both kinds -- identical to `crate::attributes`' own `forward` helper. `keep` is
/// "should this event be forwarded," already resolved by the caller. The `0.0` on the forward
/// path is deliberate, not a no-op: it registers the series so it appears at zero rather than
/// being absent, mirroring `HasAttributes::process`'s own reasoning.
fn forward(keep: bool, event: Event, telemetry: &Telemetry) -> Option<Event> {
    telemetry.count("logit.transform.events.filtered", if keep { 0.0 } else { 1.0 }, &[]);
    keep.then_some(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, Registry, Value};

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

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn provenance(origin: Option<&str>, previous: Option<&str>) -> Provenance {
        Provenance { origin: origin.map(intern), previous: previous.map(intern) }
    }

    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name) == name =>
                {
                    Some(*v)
                }
                _ => None,
            })
        })
    }

    // -- HasProvenance: single field ------------------------------------------------------------

    #[test]
    fn has_provenance_forwards_an_event_whose_origin_matches() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("nginx_in"), None));
        assert!(has.process(&default_resource(), event()).is_some());
    }

    #[test]
    fn has_provenance_drops_an_event_whose_origin_differs() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("syslog_in"), None));
        assert!(has.process(&default_resource(), event()).is_none());
    }

    #[test]
    fn has_provenance_drops_an_event_with_no_origin_set() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(Provenance::default());
        assert!(has.process(&default_resource(), event()).is_none());
    }

    // -- HasProvenance: OR within a field --------------------------------------------------------

    #[test]
    fn has_provenance_matches_any_listed_origin() {
        let mut has = HasProvenance::new(
            vec!["edge_nginx_in".to_string(), "edge_syslog_in".to_string()],
            vec![],
        );

        has.observe_provenance(provenance(Some("edge_syslog_in"), None));
        assert!(has.process(&default_resource(), event()).is_some(), "second alternative matches");

        has.observe_provenance(provenance(Some("edge_haproxy_in"), None));
        assert!(has.process(&default_resource(), event()).is_none(), "unlisted origin drops");
    }

    // -- HasProvenance: AND across fields ---------------------------------------------------------

    #[test]
    fn has_provenance_requires_both_configured_fields_to_match() {
        let mut has =
            HasProvenance::new(vec!["nginx_in".to_string()], vec!["parse_json".to_string()]);

        has.observe_provenance(provenance(Some("nginx_in"), Some("parse_json")));
        assert!(has.process(&default_resource(), event()).is_some(), "both fields match");

        has.observe_provenance(provenance(Some("nginx_in"), Some("scale")));
        assert!(has.process(&default_resource(), event()).is_none(), "only origin matches");

        has.observe_provenance(provenance(Some("syslog_in"), Some("parse_json")));
        assert!(has.process(&default_resource(), event()).is_none(), "only previous matches");
    }

    #[test]
    fn an_unconfigured_field_is_not_checked() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        // previous: is unconfigured -- any value, or none at all, should be irrelevant.
        has.observe_provenance(provenance(Some("nginx_in"), Some("anything")));
        assert!(has.process(&default_resource(), event()).is_some());
        has.observe_provenance(provenance(Some("nginx_in"), None));
        assert!(has.process(&default_resource(), event()).is_some());
    }

    // -- never mutates ----------------------------------------------------------------------------

    #[test]
    fn has_provenance_never_mutates_a_forwarded_event() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("nginx_in"), None));
        let mut attrs = AttrMap::new();
        attrs.insert("other", Value::str("x"));
        let ev = Event::log(
            0,
            attrs,
            LogRecord {
                message: Value::str("m"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
            },
        );
        let out = has.process(&default_resource(), ev).expect("matches");
        assert_eq!(out.attributes.get("other"), Some(&Value::str("x")));
    }

    // -- DropProvenance: the exact complement ------------------------------------------------------

    #[test]
    fn drop_provenance_is_the_exact_complement_of_has_provenance() {
        let cases = [
            provenance(Some("nginx_in"), Some("parse_json")),
            provenance(Some("nginx_in"), Some("scale")),
            provenance(Some("syslog_in"), Some("parse_json")),
            provenance(None, None),
        ];

        for p in cases {
            let mut has =
                HasProvenance::new(vec!["nginx_in".to_string()], vec!["parse_json".to_string()]);
            let mut drop =
                DropProvenance::new(vec!["nginx_in".to_string()], vec!["parse_json".to_string()]);
            has.observe_provenance(p);
            drop.observe_provenance(p);

            let has_forwards = has.process(&default_resource(), event()).is_some();
            let drop_forwards = drop.process(&default_resource(), event()).is_some();
            assert_eq!(
                has_forwards, !drop_forwards,
                "has_provenance and drop_provenance must exactly partition every batch"
            );
        }
    }

    #[test]
    fn drop_provenance_forwards_an_event_missing_the_configured_origin() {
        let mut drop = DropProvenance::new(vec!["nginx_in".to_string()], vec![]);
        drop.observe_provenance(Provenance::default());
        assert!(
            drop.process(&default_resource(), event()).is_some(),
            "a batch that never carried the configured origin isn't one told to drop"
        );
    }

    // -- telemetry ------------------------------------------------------------------------------

    #[test]
    fn both_record_filtered_events() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("filter", "has_provenance", "transform");
        let mut has =
            HasProvenance::new(vec!["nginx_in".to_string()], vec![]).with_telemetry(telemetry);

        has.observe_provenance(provenance(Some("nginx_in"), None));
        has.process(&default_resource(), event());
        has.observe_provenance(provenance(Some("syslog_in"), None));
        has.process(&default_resource(), event());

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.events.filtered"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("nginx_in"), None));
        assert!(has.process(&default_resource(), event()).is_some());
    }

    // -- end-to-end: a real batch through Fanout/run_transform -----------------------------------

    /// Drives a real two-source graph (`web_in`, `api_in` fan into `filter`, `filter` feeds `out`)
    /// through `logit_pipeline::run`, proving `observe_provenance` -> cache -> `process` actually
    /// works against the real `Fanout`/`run_transform` wiring, not just the isolated `Matcher`
    /// unit tests above. `web_in`'s own component id becomes the batch's `origin`
    /// (`Fanout::stamp`, `crates/logit-pipeline/src/fanout.rs`) the moment it enters the graph, so
    /// a `has_provenance` node configured `origin: [web_in]` must forward `web_in`'s batch and
    /// drop `api_in`'s -- exactly the central-collector fan-in shape this feature exists for.
    #[tokio::test]
    async fn has_provenance_filters_a_real_fan_in_by_the_sending_nodes_own_id() {
        use async_trait::async_trait;
        use logit_config::{BufferConfig, Component, ComponentKind, Config, ReceiveConfig};
        use logit_core::EventBatch;
        use logit_pipeline::{
            graph, run, Fanout, InputRuntimeConfig, NodeSpec, SinkQueueConfig, SinkStoreConfig,
            WriteLoopConfig,
        };
        use std::collections::HashMap;
        use std::time::Duration;

        fn tagged_event(source: &str) -> Event {
            let mut attrs = AttrMap::new();
            attrs.insert("source", Value::str(source));
            Event::log(
                0,
                attrs,
                LogRecord {
                    message: Value::str(source),
                    severity: None,
                    body_format: BodyFormat::Raw,
                    trace: None,
                },
            )
        }

        /// Sends its one batch, then idles -- keeps the node alive so `run`'s graph doesn't tear
        /// down before the assertion side reads from `out`, exactly `logit_pipeline::runtime`'s
        /// own `OneShotInput` test double.
        struct OneShotInput {
            batch: Option<EventBatch>,
        }

        #[async_trait]
        impl logit_pipeline::Input for OneShotInput {
            async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
                if let Some(batch) = self.batch.take() {
                    sink.send(batch).await;
                }
                std::future::pending::<()>().await;
                Ok(())
            }
        }

        struct RecordingOutput {
            tx: std::sync::mpsc::Sender<EventBatch>,
        }

        #[async_trait]
        impl logit_pipeline::Output for RecordingOutput {
            async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
                let _ = self.tx.send(batch.clone());
                Ok(())
            }
        }

        let statsd_in = || ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() };
        let mut components = HashMap::new();
        components.insert(
            "web_in".to_string(),
            Component {
                buffer: BufferConfig::default(),
                receive: ReceiveConfig::default(),
                sources: vec![],
                kind: statsd_in(),
            },
        );
        components.insert(
            "api_in".to_string(),
            Component {
                buffer: BufferConfig::default(),
                receive: ReceiveConfig::default(),
                sources: vec![],
                kind: statsd_in(),
            },
        );
        components.insert(
            "filter".to_string(),
            Component {
                buffer: BufferConfig::default(),
                receive: ReceiveConfig::default(),
                sources: vec!["web_in".to_string(), "api_in".to_string()],
                kind: ComponentKind::HasProvenance {
                    origin: vec!["web_in".to_string()],
                    previous: vec![],
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: BufferConfig::default(),
                receive: ReceiveConfig::default(),
                sources: vec!["filter".to_string()],
                kind: ComponentKind::InfluxDbOut {
                    url: "http://localhost:8086".to_string(),
                    org: "org".to_string(),
                    bucket: "bucket".to_string(),
                    token: "TOKEN".to_string(),
                },
            },
        );

        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let web_batch =
            EventBatch { resource: default_resource(), events: vec![tagged_event("web")] };
        let api_batch =
            EventBatch { resource: default_resource(), events: vec![tagged_event("api")] };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "web_in".to_string(),
            NodeSpec::Input(
                Box::new(OneShotInput { batch: Some(web_batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "api_in".to_string(),
            NodeSpec::Input(
                Box::new(OneShotInput { batch: Some(api_batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "filter".to_string(),
            NodeSpec::Transform(Box::new(HasProvenance::new(vec!["web_in".to_string()], vec![]))),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        tokio::spawn(run(g, specs));

        // Both recv attempts happen on the same blocking thread, sequentially, against the same
        // receiver -- the first proves web_in's batch (and only its tagged event) reaches `out`;
        // the second, with a short timeout, proves api_in's batch never does.
        let (first, second) = tokio::task::spawn_blocking(move || {
            let first = result_rx.recv_timeout(Duration::from_secs(5));
            let second = result_rx.recv_timeout(Duration::from_millis(200));
            (first, second)
        })
        .await
        .expect("blocking task should not panic");

        let received = first.expect("web_in's batch should reach out");
        assert_eq!(received.events.len(), 1);
        assert_eq!(received.events[0].attributes.get("source"), Some(&Value::str("web")));
        assert!(second.is_err(), "api_in's batch must be dropped by the filter, not forwarded");
    }
}
