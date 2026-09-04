//! Built-in native transform components -- no Lua VM involved, per `docs/design/lua-api.md`'s
//! "built-in native processors ... meant to sit in front of user Lua" split. Each implements
//! `logit_pipeline::Transform`, letting the node runtime run it as an ordinary tokio task (no
//! dedicated OS thread, unlike a Lua component -- `docs/design/pipeline-graph.md`'s "Node kinds"
//! section). `aggregate`, `json`, `kv_metrics`, `keep`, `remove`, `set`, `trace_context`,
//! `has_signal`, `keep_signals`, and `drop_signals` are implemented so far; more (`logfmt`, `kv`,
//! `regex`, `csv`, `rename`, `filter`, `sample`, `throttle`, `dedup`) are expected to land here
//! too.

mod aggregate;
mod json;
mod keep;
mod kv_metrics;
mod set;
mod signals;
mod trace_context;

pub use aggregate::Aggregator;
pub use json::JsonParser;
pub use keep::{Keep, Remove};
pub use kv_metrics::{KvMetrics, MetricSpec};
pub use set::Set;
pub use signals::{DropSignals, HasSignal, KeepSignals, MatchMode, SignalSet};
pub use trace_context::TraceContext;

/// Integration coverage across module boundaries -- each transform above is unit-tested in its
/// own module; this proves they compose the way a real pipeline actually wires them.
#[cfg(test)]
mod chained_pipeline_test {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::{AttrMap, BodyFormat, Event, LogRecord, MetricKind, Resource, Value};
    use logit_pipeline::Transform;
    use std::sync::Arc;
    use std::time::Duration;

    /// The workstream's headline test: a `json -> kv_metrics -> keep -> aggregate` chain fed one
    /// synthetic nginx-shaped log event produces correctly-tagged counter/gauge/distribution
    /// metrics and nothing else -- specifically, that the tags surviving into `aggregate`'s
    /// `SeriesKey` are exactly what `keep` named. This is what proves `keep`'s documented
    /// placement ahead of `aggregate` (`crate::keep`'s module doc comment,
    /// `docs/adr/kv-metrics-semantics.md`) actually bounds series cardinality end to end,
    /// not just in isolation.
    #[test]
    fn json_kv_metrics_keep_aggregate_chain_produces_correctly_tagged_metrics() {
        let resource = Arc::new(Resource::default());

        let raw = r#"{"status":200,"body_bytes_sent":512,"request_time":0.012,
                       "client_ip":"10.0.0.1","user_agent":"curl/8.0"}"#;
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str(raw),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
            },
        );

        // json: the raw body becomes attributes.
        let mut json = JsonParser::new(false);
        let event = json.process(&resource, event).expect("json always forwards");
        assert_eq!(event.attributes.len(), 5, "every top-level JSON key should have landed");

        // kv_metrics: two counters (one no-field, one field-backed) and a distribution.
        let mut kv = KvMetrics::new(
            vec![
                MetricSpec { name: "nginx.requests".to_string(), field: None, unit: None },
                MetricSpec {
                    name: "nginx.bytes_sent".to_string(),
                    field: Some("body_bytes_sent".to_string()),
                    unit: None,
                },
            ],
            vec![],
            vec![MetricSpec {
                name: "nginx.request_time".to_string(),
                field: Some("request_time".to_string()),
                unit: Some("s".to_string()),
            }],
        );
        let event = kv.process(&resource, event).expect("kv_metrics always forwards");
        assert_eq!(event.metrics.len(), 3, "two counters and one distribution should be derived");

        // keep: only `status` is allowed to survive as a tag -- client_ip/user_agent (and the
        // now-redundant body_bytes_sent/request_time) must not reach aggregate.
        let mut keep = Keep::new(vec!["status".to_string()]);
        let event = keep.process(&resource, event).expect("keep always forwards");
        let kept: Vec<&str> = event.attributes.iter().map(|(k, _)| resolve(k)).collect();
        assert_eq!(kept, vec!["status"], "only the kept attribute should survive");

        // aggregate: every metric here is mergeable, so it's fully absorbed -- the log half
        // (still present) is forwarded on its own as the remainder.
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let passed = agg.process(&resource, event).expect("the log half should be forwarded");
        assert!(passed.metrics.is_empty(), "every metric should have been absorbed");
        assert_eq!(passed.log.as_ref().unwrap().message, Value::str(raw));

        let flushed = agg.flush(1_000_000_000);
        assert_eq!(flushed.len(), 1, "one resource group");
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 3, "three distinct series -- nothing else");

        for (series_event, _links) in events {
            let tags: Vec<&str> = series_event.attributes.iter().map(|(k, _)| resolve(k)).collect();
            assert_eq!(
                tags,
                vec!["status"],
                "every series' tags must be exactly what keep named, no more and no less"
            );
            assert_eq!(series_event.metrics.len(), 1);

            let record = &series_event.metrics[0];
            match resolve(record.name) {
                "nginx.requests" => {
                    assert!(matches!(record.kind, MetricKind::Counter(v) if v == 1.0));
                }
                "nginx.bytes_sent" => {
                    assert!(matches!(record.kind, MetricKind::Counter(v) if v == 512.0));
                }
                "nginx.request_time" => match &record.kind {
                    MetricKind::Distribution(sketch) => {
                        assert_eq!(sketch.count(), 1);
                        assert_eq!(record.unit.map(resolve), Some("s"));
                    }
                    other => panic!("expected Distribution, got {other:?}"),
                },
                other => panic!("unexpected series name: {other}"),
            }
        }
    }

    /// The workstream B (Loki-direct) shape from `docs/plans/otlp-logs-and-resource-identity.md`:
    /// a `json -> kv_metrics -> keep_signals[logs]` chain proves the derived metrics are stripped
    /// off before a logs-only sink would see the event, while the log body itself survives
    /// untouched -- `keep_signals` mutates the payload, unlike `has_signal`.
    #[test]
    fn json_kv_metrics_keep_signals_chain_strips_derived_metrics_and_keeps_the_log() {
        let resource = Arc::new(Resource::default());

        let raw = r#"{"status":200,"body_bytes_sent":512}"#;
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str(raw),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
            },
        );

        let mut json = JsonParser::new(false);
        let event = json.process(&resource, event).expect("json always forwards");

        let mut kv = KvMetrics::new(
            vec![MetricSpec {
                name: "nginx.bytes_sent".to_string(),
                field: Some("body_bytes_sent".to_string()),
                unit: None,
            }],
            vec![],
            vec![],
        );
        let event = kv.process(&resource, event).expect("kv_metrics always forwards");
        assert_eq!(event.metrics.len(), 1, "one derived counter");

        let mut keep_logs =
            KeepSignals::new(SignalSet { logs: true, metrics: false, traces: false });
        let event = keep_logs.process(&resource, event).expect("the log half survives");
        assert!(
            event.metrics.is_empty(),
            "derived metrics must be stripped before a logs-only sink"
        );
        assert_eq!(
            event.log.as_ref().unwrap().message,
            Value::str(raw),
            "the log body is untouched"
        );
    }
}
