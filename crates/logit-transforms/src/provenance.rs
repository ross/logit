//! `has_provenance`/`drop_provenance`: filter events by an operator-configured match against a
//! batch's `origin`/`previous` (`docs/adr/batch-provenance-on-delivered.md`). See
//! `docs/adr/provenance-filtering-components.md`.
//!
//! **`origin:`/`previous:` are each a list of alternatives, OR'd within the field; the two fields
//! AND together when both are configured.** An empty list means that field isn't checked.
//!
//! **`drop_provenance` is the complement of `has_provenance` on the same config, taken at the top
//! level, not per field**: it drops an event only when the whole configured match succeeds, so an
//! event matching `origin:` but not `previous:` is forwarded. `DropProvenance::process` is
//! `HasProvenance::process` with a single `!`, as with `has_attributes`/`drop_attributes`.
//!
//! **A batch with no provenance (`Provenance::default()`) never matches a non-empty field.**
//! `Fanout` stamps `origin`/`previous` on every real hop, so this only arises for a batch no
//! `Fanout` has touched (a bench or unit test).

use logit_core::interner::{intern, Symbol};
use logit_core::{Event, Provenance, Resource, Telemetry};
use logit_pipeline::Transform;
use std::sync::Arc;

/// The match shared by [`HasProvenance`] and [`DropProvenance`].
struct Matcher {
    /// Empty means this field isn't checked.
    origin: Vec<Symbol>,
    previous: Vec<Symbol>,
    /// Cached from `observe_provenance`, which fires once per batch before its events reach
    /// `process`, as `Aggregator::observe_batch_context` caches `TraceContext`.
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

    /// `true` iff each non-empty list contains the batch's value for its field.
    ///
    /// A linear scan, not a `HashSet`: each list is a handful of component ids at most.
    fn matches(&self) -> bool {
        let origin_ok = self.origin.is_empty()
            || self.provenance.origin.is_some_and(|o| self.origin.contains(&o));
        let previous_ok = self.previous.is_empty()
            || self.provenance.previous.is_some_and(|p| self.previous.contains(&p));
        origin_ok && previous_ok
    }
}

/// Forwards an event whose batch's `origin`/`previous` match every configured field, dropping the
/// rest.
///
/// Never mutates a forwarded event. See the module doc for the matching rules.
pub struct HasProvenance {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl HasProvenance {
    /// Builds the filter from component-id lists, interned here.
    pub fn new(origin: Vec<String>, previous: Vec<String>) -> Self {
        Self { matcher: Matcher::new(origin, previous), telemetry: Telemetry::default() }
    }

    /// Attaches a telemetry handle; matching fixed values can't fail, so no `Diagnostics`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for HasProvenance {
    fn observe_provenance(&mut self, provenance: Provenance) {
        self.matcher.provenance = provenance;
    }

    fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
        forward(self.matcher.matches(), &self.telemetry)
    }
}

/// Drops an event whose batch's `origin`/`previous` match every configured field, forwarding the
/// rest.
///
/// The complement of [`HasProvenance`] on the same config, taken at the top level, not per field.
pub struct DropProvenance {
    matcher: Matcher,
    telemetry: Telemetry,
}

impl DropProvenance {
    /// Builds the filter; see [`HasProvenance::new`].
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

    fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
        // This `!` is the only difference from `HasProvenance`: with both fields configured, an
        // event drops only when both match.
        forward(!self.matcher.matches(), &self.telemetry)
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
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
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
                logit_core::MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == name =>
                {
                    Some(sum.value)
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
        let mut ev = event();
        assert!(has.process(&default_resource(), &mut ev));
    }

    #[test]
    fn has_provenance_drops_an_event_whose_origin_differs() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("syslog_in"), None));
        let mut ev = event();
        assert!(!has.process(&default_resource(), &mut ev));
    }

    #[test]
    fn has_provenance_drops_an_event_with_no_origin_set() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(Provenance::default());
        let mut ev = event();
        assert!(!has.process(&default_resource(), &mut ev));
    }

    // -- HasProvenance: OR within a field --------------------------------------------------------

    #[test]
    fn has_provenance_matches_any_listed_origin() {
        let mut has = HasProvenance::new(
            vec!["edge_nginx_in".to_string(), "edge_syslog_in".to_string()],
            vec![],
        );

        has.observe_provenance(provenance(Some("edge_syslog_in"), None));
        let mut ev = event();
        assert!(has.process(&default_resource(), &mut ev), "second alternative matches");

        has.observe_provenance(provenance(Some("edge_haproxy_in"), None));
        let mut ev = event();
        assert!(!has.process(&default_resource(), &mut ev), "unlisted origin drops");
    }

    // -- HasProvenance: AND across fields ---------------------------------------------------------

    #[test]
    fn has_provenance_requires_both_configured_fields_to_match() {
        let mut has =
            HasProvenance::new(vec!["nginx_in".to_string()], vec!["parse_json".to_string()]);

        has.observe_provenance(provenance(Some("nginx_in"), Some("parse_json")));
        let mut ev = event();
        assert!(has.process(&default_resource(), &mut ev), "both fields match");

        has.observe_provenance(provenance(Some("nginx_in"), Some("scale")));
        let mut ev = event();
        assert!(!has.process(&default_resource(), &mut ev), "only origin matches");

        has.observe_provenance(provenance(Some("syslog_in"), Some("parse_json")));
        let mut ev = event();
        assert!(!has.process(&default_resource(), &mut ev), "only previous matches");
    }

    #[test]
    fn an_unconfigured_field_is_not_checked() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("nginx_in"), Some("anything")));
        let mut ev = event();
        assert!(has.process(&default_resource(), &mut ev));
        has.observe_provenance(provenance(Some("nginx_in"), None));
        let mut ev = event();
        assert!(has.process(&default_resource(), &mut ev));
    }

    // -- never mutates ----------------------------------------------------------------------------

    #[test]
    fn has_provenance_never_mutates_a_forwarded_event() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("nginx_in"), None));
        let mut attrs = AttrMap::new();
        attrs.insert("other", Value::str("x"));
        let mut ev = Event::log(
            0,
            attrs,
            LogRecord {
                message: Value::str("m"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        assert!(has.process(&default_resource(), &mut ev), "matches");
        assert_eq!(ev.attributes.get("other"), Some(&Value::str("x")));
    }

    // -- DropProvenance: the exact complement ---------------------------------------------------

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

            let mut has_ev = event();
            let has_forwards = has.process(&default_resource(), &mut has_ev);
            let mut drop_ev = event();
            let drop_forwards = drop.process(&default_resource(), &mut drop_ev);
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
        let mut ev = event();
        assert!(
            drop.process(&default_resource(), &mut ev),
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
        has.process(&default_resource(), &mut event());
        has.observe_provenance(provenance(Some("syslog_in"), None));
        has.process(&default_resource(), &mut event());

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.events.filtered"), Some(1.0));
    }

    #[test]
    fn a_disabled_telemetry_handle_is_the_default() {
        let mut has = HasProvenance::new(vec!["nginx_in".to_string()], vec![]);
        has.observe_provenance(provenance(Some("nginx_in"), None));
        let mut ev = event();
        assert!(has.process(&default_resource(), &mut ev));
    }

    // -- end-to-end: a real batch through Fanout/run_transform -----------------------------------

    /// Through the real runtime, `origin: [web_in]` forwards `web_in`'s batch and drops `api_in`'s.
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
                    event_name: None,
                    observed_timestamp: 0,
                    dropped_attributes_count: 0,
                },
            )
        }

        /// Sends its one batch, then idles so the graph stays up until `out` is read.
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

        let statsd_in = || ComponentKind::StatsdIn {
            bind: "127.0.0.1:0".to_string(),
            transport: logit_config::StatsdTransport::default(),
            tls: None,
            handshake_timeout: logit_config::default_handshake_timeout(),
            idle_timeout: None,
        };
        let mut components = HashMap::new();
        components.insert(
            "web_in".to_string(),
            Component {
                buffer: BufferConfig::default(),
                receive: ReceiveConfig::default(),
                sources: vec![],
                targets: Vec::new(),
                kind: statsd_in(),
            },
        );
        components.insert(
            "api_in".to_string(),
            Component {
                buffer: BufferConfig::default(),
                receive: ReceiveConfig::default(),
                sources: vec![],
                targets: Vec::new(),
                kind: statsd_in(),
            },
        );
        components.insert(
            "filter".to_string(),
            Component {
                buffer: BufferConfig::default(),
                receive: ReceiveConfig::default(),
                sources: vec!["web_in".to_string(), "api_in".to_string()],
                targets: Vec::new(),
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
                targets: Vec::new(),
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
        let web_batch = EventBatch {
            resource: default_resource(),
            scope: None,
            events: vec![tagged_event("web")],
        };
        let api_batch = EventBatch {
            resource: default_resource(),
            scope: None,
            events: vec![tagged_event("api")],
        };

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

        // The second, short recv proves api_in's batch never arrives.
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
