//! The built-in `kv_metrics` transform: turns attributes already on an event (typically merged
//! there by `json`) into metrics on that *same* event -- nginx's access-log body becomes
//! `nginx.requests`/`nginx.bytes_sent`/`nginx.request_time` without a second round trip through a
//! Lua script. See `docs/adr/kv-metrics-semantics.md` for the skip rules, the numeric
//! coercion rules, and the deliberate absence of a `tags:` field on this config surface -- tag
//! selection is `keep`'s job (`crate::keep`), not something restated on every metrics producer.
//!
//! Stateless -- like `json`, only `process` is overridden; `flush_interval`/`flush` keep the
//! `Transform` trait's defaults.

use crate::numeric;
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, DdSketch, Diagnostics, Event, MetricKind, MetricRecord, Resource, Symbol, Telemetry,
};
use logit_pipeline::Transform;
use std::sync::Arc;

/// One counter/gauge/distribution entry, as configured. Mirrors `logit_config::MetricSpec`
/// field-for-field but is a distinct type: `logit-transforms` doesn't depend on `logit-config`
/// (`docs/design/pipeline-graph.md`'s crate layout), so `logit-cli::pipeline::build_spec` is the
/// place that converts one into the other.
pub struct MetricSpec {
    pub name: String,
    /// The attribute to read this metric's value from. `None` means "+1 per event" for a counter
    /// or "set to 1" for a gauge; a distribution with no `field` is a config error rejected at
    /// graph-validation time (`crates/logit-pipeline/src/graph.rs`), not here -- a distribution of
    /// nothing is meaningless. Names an attribute literally, not a path: `field: http.status`
    /// means the attribute named `http.status`, never a `status` key nested under `http` in a
    /// `Value::Map` (`docs/adr/kv-metrics-semantics.md`).
    pub field: Option<String>,
    pub unit: Option<String>,
}

/// [`MetricSpec`], interned once at construction ([`KvMetrics::new`]) rather than per event --
/// `intern`/`resolve` are hash lookups, and this runs on the hot path once per metric per event.
struct CompiledMetric {
    name: Symbol,
    field: Option<String>,
    unit: Option<Symbol>,
}

impl From<MetricSpec> for CompiledMetric {
    fn from(spec: MetricSpec) -> Self {
        CompiledMetric {
            name: intern(&spec.name),
            field: spec.field,
            unit: spec.unit.as_deref().map(intern),
        }
    }
}

pub struct KvMetrics {
    counters: Vec<CompiledMetric>,
    gauges: Vec<CompiledMetric>,
    distributions: Vec<CompiledMetric>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl KvMetrics {
    pub fn new(
        counters: Vec<MetricSpec>,
        gauges: Vec<MetricSpec>,
        distributions: Vec<MetricSpec>,
    ) -> Self {
        Self {
            counters: counters.into_iter().map(CompiledMetric::from).collect(),
            gauges: gauges.into_iter().map(CompiledMetric::from).collect(),
            distributions: distributions.into_iter().map(CompiledMetric::from).collect(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Attaches a telemetry handle -- see `process`'s `logit.transform.derived`/`.derived.skipped`
    /// counters.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for KvMetrics {
    /// Appends zero or more metrics to `event.metrics`, in config order (counters, then gauges,
    /// then distributions) -- never replacing what's already there, and never dropping the event:
    /// this always returns `Some`. `log`/`span`/`attributes`/`timestamp` are untouched.
    ///
    /// Records `logit.transform.derived{metric_kind}`/`.derived.skipped{metric_kind}` for every
    /// configured metric, whether or not `metric_value`/`numeric` below actually produced a value
    /// -- the skipped-vs-derived ratio is the visible signal for the documented silent-skip path
    /// (a missing field, a non-numeric value) this transform deliberately never turns into a
    /// diagnostic (`docs/design/internal-telemetry.md`). Tagged `metric_kind`, not `kind` -- `kind`
    /// is reserved for a point's own component-kind identity
    /// (`crates/logit-core/src/telemetry.rs::ComponentBuffer::drain`).
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        for m in &self.counters {
            if let Some(value) = metric_value(m, &event.attributes) {
                event.metrics.push(MetricRecord {
                    name: m.name,
                    kind: MetricKind::Counter(value),
                    unit: m.unit,
                });
                self.telemetry.count("logit.transform.derived", 1.0, &[("metric_kind", "counter")]);
            } else {
                self.telemetry.count(
                    "logit.transform.derived.skipped",
                    1.0,
                    &[("metric_kind", "counter")],
                );
            }
        }
        for m in &self.gauges {
            if let Some(value) = metric_value(m, &event.attributes) {
                event.metrics.push(MetricRecord {
                    name: m.name,
                    kind: MetricKind::Gauge(value),
                    unit: m.unit,
                });
                self.telemetry.count("logit.transform.derived", 1.0, &[("metric_kind", "gauge")]);
            } else {
                self.telemetry.count(
                    "logit.transform.derived.skipped",
                    1.0,
                    &[("metric_kind", "gauge")],
                );
            }
        }
        for m in &self.distributions {
            // Graph validation (`crates/logit-pipeline/src/graph.rs`) already rejects a
            // fieldless distribution before a config carrying one ever reaches `build_spec` --
            // this is defense in depth for a direct `KvMetrics::new` caller (e.g. a test) that
            // bypasses graph resolution, not a path a real config can take.
            let Some(field) = &m.field else {
                self.diag.warn_throttled(
                    "distribution_no_field",
                    format_args!(
                        "distribution '{}' has no field configured -- a distribution of nothing \
                         is meaningless; skipping (graph validation should already reject this)",
                        resolve(m.name)
                    ),
                );
                continue;
            };
            if let Some(value) = event.attributes.get(field).and_then(numeric) {
                let mut sketch = DdSketch::new();
                sketch.add(value);
                event.metrics.push(MetricRecord {
                    name: m.name,
                    kind: MetricKind::Distribution(sketch),
                    unit: m.unit,
                });
                self.telemetry.count(
                    "logit.transform.derived",
                    1.0,
                    &[("metric_kind", "distribution")],
                );
            } else {
                self.telemetry.count(
                    "logit.transform.derived.skipped",
                    1.0,
                    &[("metric_kind", "distribution")],
                );
            }
        }
        Some(event)
    }
}

/// A counter/gauge entry's value for this event: `1.0` with no `field` (per-event
/// increment/set-to-1), or the named attribute's coerced numeric value -- `None` when the field
/// is missing, non-numeric, or non-finite, meaning "skip this metric for this event," never an
/// error and never a dropped event (`docs/adr/kv-metrics-semantics.md`). This is the common
/// path, not an edge case: nginx's `$upstream_response_time` is `-` on a non-proxied request and a
/// comma-separated list on a retried one.
fn metric_value(m: &CompiledMetric, attrs: &AttrMap) -> Option<f64> {
    match &m.field {
        None => Some(1.0),
        Some(field) => attrs.get(field).and_then(numeric),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{BodyFormat, LogRecord, SpanEvent, SpanKind, SpanRecord, SpanStatus, Value};

    fn spec(name: &str, field: Option<&str>) -> MetricSpec {
        MetricSpec { name: name.to_string(), field: field.map(String::from), unit: None }
    }

    fn spec_with_unit(name: &str, field: Option<&str>, unit: &str) -> MetricSpec {
        MetricSpec {
            name: name.to_string(),
            field: field.map(String::from),
            unit: Some(unit.to_string()),
        }
    }

    fn event_with_attrs(attrs: &[(&str, Value)]) -> Event {
        let mut map = AttrMap::new();
        for (k, v) in attrs {
            map.insert(k, v.clone());
        }
        Event::log(
            0,
            map,
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

    fn metric_named<'a>(event: &'a Event, name: &str) -> Option<&'a MetricRecord> {
        event.metrics.iter().find(|m| resolve(m.name) == name)
    }

    fn counter_value(record: &MetricRecord) -> f64 {
        match record.kind {
            MetricKind::Counter(v) => v,
            _ => panic!("expected Counter"),
        }
    }

    fn gauge_value(record: &MetricRecord) -> f64 {
        match record.kind {
            MetricKind::Gauge(v) => v,
            _ => panic!("expected Gauge"),
        }
    }

    #[test]
    fn a_counter_with_no_field_increments_by_exactly_one_per_event() {
        let mut kv = KvMetrics::new(vec![spec("hits", None)], vec![], vec![]);
        let resource = default_resource();
        let event = kv.process(&resource, event_with_attrs(&[])).expect("always forwards");
        let m = metric_named(&event, "hits").expect("counter should be present");
        assert_eq!(counter_value(m), 1.0);
    }

    #[test]
    fn counter_gauge_and_distribution_each_read_a_present_numeric_field() {
        let mut kv = KvMetrics::new(
            vec![spec("bytes", Some("body_bytes_sent"))],
            vec![spec("conns", Some("active"))],
            vec![spec("request_time", Some("request_time"))],
        );
        let resource = default_resource();
        let event = event_with_attrs(&[
            ("body_bytes_sent", Value::U64(512)),
            ("active", Value::I64(3)),
            ("request_time", Value::F64(0.012)),
        ]);
        let event = kv.process(&resource, event).expect("always forwards");

        assert_eq!(counter_value(metric_named(&event, "bytes").unwrap()), 512.0);
        assert_eq!(gauge_value(metric_named(&event, "conns").unwrap()), 3.0);
        match &metric_named(&event, "request_time").unwrap().kind {
            MetricKind::Distribution(sketch) => {
                assert_eq!(sketch.count(), 1);
                let q = sketch.quantile(0.5).expect("single-sample sketch has a median");
                assert!((q - 0.012).abs() < 0.001, "got {q}");
            }
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn a_missing_field_skips_only_that_metric() {
        let mut kv = KvMetrics::new(
            vec![spec("present", Some("a")), spec("missing", Some("nope"))],
            vec![],
            vec![],
        );
        let resource = default_resource();
        let event = event_with_attrs(&[("a", Value::U64(1))]);
        let event = kv.process(&resource, event).expect("always forwards");

        assert!(metric_named(&event, "present").is_some());
        assert!(metric_named(&event, "missing").is_none());
        assert_eq!(event.metrics.len(), 1, "only the resolvable metric should be emitted");
    }

    #[test]
    fn a_non_numeric_string_field_skips_only_that_metric() {
        let mut kv = KvMetrics::new(vec![spec("dash", Some("upstream_time"))], vec![], vec![]);
        let resource = default_resource();
        let event = event_with_attrs(&[("upstream_time", Value::str("-"))]);
        let event = kv.process(&resource, event).expect("always forwards");
        assert!(event.metrics.is_empty());
        assert!(
            event.log.is_some(),
            "the log half must survive a skipped metric untouched (also covered below)"
        );
    }

    #[test]
    fn a_non_finite_value_skips_only_that_metric() {
        let mut kv = KvMetrics::new(
            vec![spec("nan_c", Some("a")), spec("inf_c", Some("b"))],
            vec![],
            vec![],
        );
        let resource = default_resource();
        let event =
            event_with_attrs(&[("a", Value::F64(f64::NAN)), ("b", Value::F64(f64::INFINITY))]);
        let event = kv.process(&resource, event).expect("always forwards");
        assert!(event.metrics.is_empty(), "NaN/inf must never become a metric value");
    }

    #[test]
    fn a_skipped_metric_leaves_other_derived_metrics_and_the_log_half_intact() {
        let mut kv = KvMetrics::new(
            vec![spec("good", Some("a")), spec("bad", Some("missing"))],
            vec![],
            vec![],
        );
        let resource = default_resource();
        let event = event_with_attrs(&[("a", Value::U64(1))]);
        let event = kv.process(&resource, event).expect("always forwards");
        assert!(metric_named(&event, "good").is_some());
        assert_eq!(
            event.log.as_ref().unwrap().message,
            Value::str("msg"),
            "the log half must be untouched"
        );
    }

    #[test]
    fn a_quoted_numeric_json_string_field_still_coerces() {
        let mut kv = KvMetrics::new(vec![spec("status", Some("status"))], vec![], vec![]);
        let resource = default_resource();
        let event = event_with_attrs(&[("status", Value::str("200"))]);
        let event = kv.process(&resource, event).expect("always forwards");
        assert_eq!(counter_value(metric_named(&event, "status").unwrap()), 200.0);
    }

    #[test]
    fn bool_null_array_and_map_fields_never_coerce() {
        for value in [
            Value::Bool(true),
            Value::Null,
            Value::Array(vec![Value::U64(1)]),
            Value::Map(Box::new(AttrMap::new())),
        ] {
            let mut kv = KvMetrics::new(vec![spec("m", Some("f"))], vec![], vec![]);
            let resource = default_resource();
            let event = event_with_attrs(&[("f", value.clone())]);
            let event = kv.process(&resource, event).expect("always forwards");
            assert!(event.metrics.is_empty(), "{value:?} should not coerce to a metric value");
        }
    }

    #[test]
    fn unit_lands_on_the_emitted_metric_record() {
        let mut kv = KvMetrics::new(vec![spec_with_unit("bytes", None, "By")], vec![], vec![]);
        let resource = default_resource();
        let event = kv.process(&resource, event_with_attrs(&[])).expect("always forwards");
        let m = metric_named(&event, "bytes").unwrap();
        assert_eq!(m.unit.map(resolve), Some("By"));
    }

    #[test]
    fn pre_existing_metrics_are_preserved_and_new_ones_appended_after() {
        let mut kv = KvMetrics::new(vec![spec("new_counter", None)], vec![], vec![]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        event.metrics.push(MetricRecord {
            name: intern("existing"),
            kind: MetricKind::Counter(9.0),
            unit: None,
        });
        let event = kv.process(&resource, event).expect("always forwards");
        assert_eq!(event.metrics.len(), 2);
        assert_eq!(resolve(event.metrics[0].name), "existing");
        assert_eq!(resolve(event.metrics[1].name), "new_counter");
    }

    #[test]
    fn log_span_attributes_and_timestamp_are_untouched() {
        let mut kv = KvMetrics::new(vec![spec("c", None)], vec![], vec![]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("a", Value::U64(1))]);
        event.timestamp = 12345;
        event.span = Some(SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: Value::str("span"),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: Vec::<SpanEvent>::new(),
            links: Vec::new(),
            end_timestamp: 0,
        });
        let original_attrs = event.attributes.clone();
        let original_log = event.log.clone();

        let event = kv.process(&resource, event).expect("always forwards");

        assert_eq!(event.timestamp, 12345);
        assert!(event.span.is_some());
        assert_eq!(event.attributes, original_attrs);
        assert_eq!(
            event.log.as_ref().map(|l| &l.message),
            original_log.as_ref().map(|l| &l.message)
        );
    }

    #[test]
    fn an_event_with_no_attributes_emits_only_the_no_field_metrics() {
        let mut kv = KvMetrics::new(
            vec![spec("hits", None), spec("bytes", Some("body_bytes_sent"))],
            vec![spec("up", None)],
            vec![spec("rt", Some("request_time"))],
        );
        let resource = default_resource();
        let event = event_with_attrs(&[]);
        let event = kv.process(&resource, event).expect("always forwards");
        assert_eq!(event.metrics.len(), 2, "only the two no-field metrics should be emitted");
        assert!(metric_named(&event, "hits").is_some());
        assert!(metric_named(&event, "up").is_some());
        assert!(metric_named(&event, "bytes").is_none());
        assert!(metric_named(&event, "rt").is_none());
    }

    // Takes already-drained `events`, not a `&Registry` -- `Registry::drain` is consuming (it
    // empties every buffer via `mem::take`), so calling it once per assertion in the same test
    // would make every assertion after the first see an already-emptied registry.
    fn derived_count(events: &[Event], name: &str, kind: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            if e.attributes.get("metric_kind").and_then(|v| v.as_str()) != Some(kind) {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v) if resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    #[test]
    fn a_derived_metric_records_derived_not_skipped() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("derive", "kv_metrics", "transform");
        let mut kv =
            KvMetrics::new(vec![spec("hits", None)], vec![], vec![]).with_telemetry(telemetry);
        let resource = default_resource();
        kv.process(&resource, event_with_attrs(&[])).unwrap();

        let events = registry.drain(0);
        assert_eq!(derived_count(&events, "logit.transform.derived", "counter"), Some(1.0));
        assert_eq!(derived_count(&events, "logit.transform.derived.skipped", "counter"), None);
    }

    #[test]
    fn a_skipped_metric_records_skipped_not_derived() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("derive", "kv_metrics", "transform");
        let mut kv = KvMetrics::new(vec![spec("missing", Some("nope"))], vec![], vec![])
            .with_telemetry(telemetry);
        let resource = default_resource();
        kv.process(&resource, event_with_attrs(&[])).unwrap();

        let events = registry.drain(0);
        assert_eq!(derived_count(&events, "logit.transform.derived", "counter"), None);
        assert_eq!(derived_count(&events, "logit.transform.derived.skipped", "counter"), Some(1.0));
    }

    #[test]
    fn derived_and_skipped_are_tagged_by_their_own_kind() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("derive", "kv_metrics", "transform");
        let mut kv = KvMetrics::new(
            vec![spec("present", Some("a"))],
            vec![spec("missing_gauge", Some("nope"))],
            vec![spec("rt", Some("request_time"))],
        )
        .with_telemetry(telemetry);
        let resource = default_resource();
        let event = event_with_attrs(&[("a", Value::U64(1)), ("request_time", Value::F64(0.5))]);
        kv.process(&resource, event).unwrap();

        let events = registry.drain(0);
        assert_eq!(derived_count(&events, "logit.transform.derived", "counter"), Some(1.0));
        assert_eq!(derived_count(&events, "logit.transform.derived.skipped", "gauge"), Some(1.0));
        assert_eq!(derived_count(&events, "logit.transform.derived", "distribution"), Some(1.0));
    }
}
