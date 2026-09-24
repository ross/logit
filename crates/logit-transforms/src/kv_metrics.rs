//! `kv_metrics`: turns attributes already on an event (typically from `json`) into metrics on
//! that same event, such as nginx's `nginx.requests`/`nginx.bytes_sent`/`nginx.request_time`.
//! See `docs/adr/kv-metrics-semantics.md` for the skip and numeric-coercion rules, and for why
//! there's no `tags:` field (tag selection is `keep`'s job).
//!
//! No flush state; its only state is a per-batch telemetry [`Tally`].

use crate::numeric;
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, Diagnostics, Event, MetricKind, MetricRecord, Resource, Samples, Symbol, Telemetry,
};
use logit_pipeline::Transform;
use std::sync::Arc;

/// One counter/gauge/distribution entry, as configured.
///
/// Mirrors `logit_config::MetricSpec` field for field; `logit-cli`'s `build_spec` converts.
pub struct MetricSpec {
    pub name: String,
    /// The attribute to read the value from.
    ///
    /// `None` means +1 per event for a counter or 1 for a gauge; graph rule 11 rejects it on a
    /// distribution. Names an attribute literally: `http.status` is the attribute named
    /// `http.status`, never a `status` key nested in a `Value::Map` under `http`.
    pub field: Option<String>,
    pub unit: Option<String>,
}

/// [`MetricSpec`] interned once, in [`KvMetrics::new`], to keep interner probes off the per-event
/// path.
///
/// Interning `field` lets `process` use [`AttrMap::get_sym`] (a binary search) instead of
/// `AttrMap::get`'s hash-then-search. `AttrMap::get`'s reason not to intern arbitrary keys
/// (interner growth, `docs/design/memory.md` §4) doesn't apply to operator-written field names.
struct CompiledMetric {
    name: Symbol,
    field: Option<Symbol>,
    unit: Option<Symbol>,
}

impl From<MetricSpec> for CompiledMetric {
    fn from(spec: MetricSpec) -> Self {
        CompiledMetric {
            name: intern(&spec.name),
            field: spec.field.as_deref().map(intern),
            unit: spec.unit.as_deref().map(intern),
        }
    }
}

/// The three metric kinds this transform derives, as an index into [`Tally`] and the
/// `metric_kind` tag value each one reports under.
#[derive(Clone, Copy)]
enum Kind {
    Counter = 0,
    Gauge = 1,
    Distribution = 2,
}

impl Kind {
    const ALL: [Kind; 3] = [Kind::Counter, Kind::Gauge, Kind::Distribution];

    fn tag(self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Distribution => "distribution",
        }
    }
}

/// Per-batch `derived`/`skipped` counts, one pair per [`Kind`], emitted by
/// [`KvMetrics::end_batch`] in at most six `Telemetry::count` calls.
///
/// Each `Telemetry::count` is a `PointKey` build, a mutex acquire, and a hash-map upsert
/// (`ComponentBuffer::upsert`); calling it per metric per event showed in the `json-parse`
/// load-test flamegraph. `logit-bench`'s `kv_metrics` bench can't see that cost: its fixture has
/// no telemetry handle. Drained points are the same either way, since counts sum until a drain.
#[derive(Default)]
struct Tally {
    derived: [u64; 3],
    skipped: [u64; 3],
}

impl Tally {
    fn record(&mut self, kind: Kind, derived: bool) {
        if derived {
            self.derived[kind as usize] += 1;
        } else {
            self.skipped[kind as usize] += 1;
        }
    }

    /// Emits the non-zero cells and resets, so a config with no gauges emits no `gauge` point.
    fn flush(&mut self, telemetry: &Telemetry) {
        for kind in Kind::ALL {
            let i = kind as usize;
            if self.derived[i] > 0 {
                telemetry.count(
                    "logit.transform.derived",
                    self.derived[i] as f64,
                    &[("metric_kind", kind.tag())],
                );
            }
            if self.skipped[i] > 0 {
                telemetry.count(
                    "logit.transform.derived.skipped",
                    self.skipped[i] as f64,
                    &[("metric_kind", kind.tag())],
                );
            }
        }
        *self = Tally::default();
    }
}

pub struct KvMetrics {
    counters: Vec<CompiledMetric>,
    gauges: Vec<CompiledMetric>,
    distributions: Vec<CompiledMetric>,
    diag: Diagnostics,
    telemetry: Telemetry,
    tally: Tally,
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
            tally: Tally::default(),
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Attaches a telemetry handle for the `logit.transform.derived`/`.derived.skipped` counters.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for KvMetrics {
    /// Appends the configured metrics to `event.metrics` in config order (counters, gauges, then
    /// distributions); never replaces existing metrics or drops the event.
    ///
    /// Tallies `logit.transform.derived{metric_kind}` or `.derived.skipped{metric_kind}` for every
    /// configured metric. A missing or non-numeric field is never a diagnostic, so the ratio is
    /// the only visible signal for it. The tag is `metric_kind` because `kind` is reserved for the
    /// point's component kind (`ComponentBuffer::drain`).
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        // One growth for the whole append: `MetricList` keeps one record inline, so on the
        // reference config (four metrics) the second push would spill and a later one regrow.
        let total = self.counters.len() + self.gauges.len() + self.distributions.len();
        event.metrics.reserve(total);

        for m in &self.counters {
            let derived = match metric_value(m, &event.attributes) {
                Some(value) => {
                    let mut record = MetricRecord::new(m.name, MetricKind::counter(value));
                    record.unit = m.unit;
                    event.metrics.push(record);
                    true
                }
                None => false,
            };
            self.tally.record(Kind::Counter, derived);
        }
        for m in &self.gauges {
            let derived = match metric_value(m, &event.attributes) {
                Some(value) => {
                    let mut record = MetricRecord::new(m.name, MetricKind::Gauge(value));
                    record.unit = m.unit;
                    event.metrics.push(record);
                    true
                }
                None => false,
            };
            self.tally.record(Kind::Gauge, derived);
        }
        for m in &self.distributions {
            // Graph rule 11 rejects a fieldless distribution; this guards a direct
            // `KvMetrics::new` caller that bypasses graph resolution.
            let Some(field) = m.field else {
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
            let derived = match event.attributes.get_sym(field).and_then(numeric) {
                Some(value) => {
                    // Raw `Samples`, not a one-sample `DdSketch`: summarizing is `aggregate`'s
                    // job (`docs/adr/lossless-transit.md`), and one sample sits inline with no
                    // bins `Vec` per event.
                    let mut record =
                        MetricRecord::new(m.name, MetricKind::Samples(Samples::new([value])));
                    record.unit = m.unit;
                    event.metrics.push(record);
                    true
                }
                None => false,
            };
            self.tally.record(Kind::Distribution, derived);
        }
        true
    }

    /// Emits and resets the batch's [`Tally`].
    fn end_batch(&mut self) {
        self.tally.flush(&self.telemetry);
    }
}

/// A counter/gauge entry's value for this event: `1.0` with no `field`, else the attribute's
/// coerced numeric value.
///
/// `None` (missing, non-numeric, or non-finite) skips this metric for this event, never an error.
/// That's a common path: nginx's `$upstream_response_time` is `-` on a non-proxied request and a
/// comma-separated list on a retried one.
fn metric_value(m: &CompiledMetric, attrs: &AttrMap) -> Option<f64> {
    match m.field {
        None => Some(1.0),
        Some(field) => attrs.get_sym(field).and_then(numeric),
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
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
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
            MetricKind::Sum(sum) => sum.value,
            _ => panic!("expected Sum"),
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
        let mut event = event_with_attrs(&[]);
        assert!(kv.process(&resource, &mut event), "always forwards");
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
        let mut event = event_with_attrs(&[
            ("body_bytes_sent", Value::U64(512)),
            ("active", Value::I64(3)),
            ("request_time", Value::F64(0.012)),
        ]);
        assert!(kv.process(&resource, &mut event), "always forwards");

        assert_eq!(counter_value(metric_named(&event, "bytes").unwrap()), 512.0);
        assert_eq!(gauge_value(metric_named(&event, "conns").unwrap()), 3.0);
        match &metric_named(&event, "request_time").unwrap().kind {
            // Raw, unsampled, exactly one observation -- summarizing is `aggregate`'s job.
            MetricKind::Samples(samples) => {
                assert_eq!(samples.values.as_slice(), &[0.012]);
                assert_eq!(samples.sample_rate, 1.0);
                assert!(!samples.values.spilled(), "one value must sit inline, no heap");
            }
            other => panic!("expected Samples, got {other:?}"),
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
        let mut event = event_with_attrs(&[("a", Value::U64(1))]);
        assert!(kv.process(&resource, &mut event), "always forwards");

        assert!(metric_named(&event, "present").is_some());
        assert!(metric_named(&event, "missing").is_none());
        assert_eq!(event.metrics.len(), 1, "only the resolvable metric should be emitted");
    }

    #[test]
    fn a_non_numeric_string_field_skips_only_that_metric() {
        let mut kv = KvMetrics::new(vec![spec("dash", Some("upstream_time"))], vec![], vec![]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[("upstream_time", Value::str("-"))]);
        assert!(kv.process(&resource, &mut event), "always forwards");
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
        let mut event =
            event_with_attrs(&[("a", Value::F64(f64::NAN)), ("b", Value::F64(f64::INFINITY))]);
        assert!(kv.process(&resource, &mut event), "always forwards");
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
        let mut event = event_with_attrs(&[("a", Value::U64(1))]);
        assert!(kv.process(&resource, &mut event), "always forwards");
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
        let mut event = event_with_attrs(&[("status", Value::str("200"))]);
        assert!(kv.process(&resource, &mut event), "always forwards");
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
            let mut event = event_with_attrs(&[("f", value.clone())]);
            assert!(kv.process(&resource, &mut event), "always forwards");
            assert!(event.metrics.is_empty(), "{value:?} should not coerce to a metric value");
        }
    }

    #[test]
    fn unit_lands_on_the_emitted_metric_record() {
        let mut kv = KvMetrics::new(vec![spec_with_unit("bytes", None, "By")], vec![], vec![]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        assert!(kv.process(&resource, &mut event), "always forwards");
        let m = metric_named(&event, "bytes").unwrap();
        assert_eq!(m.unit.map(resolve), Some("By"));
    }

    #[test]
    fn pre_existing_metrics_are_preserved_and_new_ones_appended_after() {
        let mut kv = KvMetrics::new(vec![spec("new_counter", None)], vec![], vec![]);
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        event.metrics.push(MetricRecord::new(intern("existing"), MetricKind::counter(9.0)));
        assert!(kv.process(&resource, &mut event), "always forwards");
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
            flags: 0,
            ext: None,
        });
        let original_attrs = event.attributes.clone();
        let original_log = event.log.clone();

        assert!(kv.process(&resource, &mut event), "always forwards");

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
        let mut event = event_with_attrs(&[]);
        assert!(kv.process(&resource, &mut event), "always forwards");
        assert_eq!(event.metrics.len(), 2, "only the two no-field metrics should be emitted");
        assert!(metric_named(&event, "hits").is_some());
        assert!(metric_named(&event, "up").is_some());
        assert!(metric_named(&event, "bytes").is_none());
        assert!(metric_named(&event, "rt").is_none());
    }

    // Takes drained `events`, not a `&Registry`: `Registry::drain` empties every buffer, so a
    // second drain in the same test would see nothing.
    fn derived_count(events: &[Event], name: &str, kind: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            if e.attributes.get("metric_kind").and_then(|v| v.as_str()) != Some(kind) {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == name => Some(sum.value),
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
        let mut event = event_with_attrs(&[]);
        assert!(kv.process(&resource, &mut event));

        assert!(registry.drain(0).is_empty(), "no telemetry before end_batch");
        kv.end_batch();

        let events = registry.drain(0);
        assert_eq!(derived_count(&events, "logit.transform.derived", "counter"), Some(1.0));
        assert_eq!(derived_count(&events, "logit.transform.derived.skipped", "counter"), None);
    }

    #[test]
    fn a_batch_of_events_emits_one_summed_point_per_kind() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("derive", "kv_metrics", "transform");
        let mut kv = KvMetrics::new(
            vec![spec("hits", None), spec("bytes", Some("body_bytes_sent"))],
            vec![],
            vec![spec("rt", Some("request_time"))],
        )
        .with_telemetry(telemetry);
        let resource = default_resource();
        for i in 0..5 {
            // `bytes` is present on the even events only; `rt` never.
            let attrs: Vec<(&str, Value)> =
                if i % 2 == 0 { vec![("body_bytes_sent", Value::U64(1))] } else { vec![] };
            let mut event = event_with_attrs(&attrs);
            assert!(kv.process(&resource, &mut event));
        }
        kv.end_batch();

        let events = registry.drain(0);
        assert_eq!(derived_count(&events, "logit.transform.derived", "counter"), Some(8.0));
        assert_eq!(derived_count(&events, "logit.transform.derived.skipped", "counter"), Some(2.0));
        assert_eq!(
            derived_count(&events, "logit.transform.derived.skipped", "distribution"),
            Some(5.0)
        );
        assert_eq!(derived_count(&events, "logit.transform.derived", "distribution"), None);
        assert_eq!(derived_count(&events, "logit.transform.derived", "gauge"), None);
        assert_eq!(derived_count(&events, "logit.transform.derived.skipped", "gauge"), None);

        // The tally reset: a second, empty batch emits nothing new.
        kv.end_batch();
        assert!(registry.drain(0).is_empty(), "flushed tally must not re-emit");
    }

    #[test]
    fn a_skipped_metric_records_skipped_not_derived() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("derive", "kv_metrics", "transform");
        let mut kv = KvMetrics::new(vec![spec("missing", Some("nope"))], vec![], vec![])
            .with_telemetry(telemetry);
        let resource = default_resource();
        let mut event = event_with_attrs(&[]);
        assert!(kv.process(&resource, &mut event));
        kv.end_batch();

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
        let mut event =
            event_with_attrs(&[("a", Value::U64(1)), ("request_time", Value::F64(0.5))]);
        assert!(kv.process(&resource, &mut event));
        kv.end_batch();

        let events = registry.drain(0);
        assert_eq!(derived_count(&events, "logit.transform.derived", "counter"), Some(1.0));
        assert_eq!(derived_count(&events, "logit.transform.derived.skipped", "gauge"), Some(1.0));
        assert_eq!(derived_count(&events, "logit.transform.derived", "distribution"), Some(1.0));
    }
}
