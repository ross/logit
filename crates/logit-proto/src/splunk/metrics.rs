//! HEC metric events, both directions: `"event":"metric"` with the measurements in `fields`.
//! Splunk has one metric shape, `name = number`, so every model kind with more than one number is
//! skipped or expanded under [`MultiValue`](crate::MultiValue), `graphite_out`'s switch.
//!
//! ## Decode
//!
//! A metric event is `event` `"metric"`, or no `event` at all, with at least one
//! `metric_name:<n>` field (the multi-metric form) or both `metric_name` and `_value` (the
//! single-metric form). It decodes to one `Event` carrying one `MetricRecord` per measurement, in
//! name order. Splunk indexes the object with no `event` as a metric, and SC4S sends its own
//! metrics that way, every measurement a numeric string (`"metric_name:spl.sc4syslog.dst.written":
//! "0"`).
//!
//! | Wire | Model | Counter |
//! |---|---|---|
//! | `metric_name:<n>`: a JSON number, `"+Inf"` / `"-Inf"` / `"NaN"`, or a string holding a finite number | a record named `<n>` | -- |
//! | `metric_name` (a non-empty string) + `_value` | a record named by `metric_name` | -- |
//! | `metric_type` `"Sum"` | every record `Sum{Cumulative, monotonic: true}`; the field is consumed | -- |
//! | `metric_type` `"Gauge"` or absent | every record `Gauge`; the field is consumed | -- |
//! | `metric_type` any other value (the exporter's `Histogram`, `Summary`) | every record `Gauge`; the field stays an attribute, verbatim | -- |
//! | every other field | an attribute, as the parent module's table says | -- |
//! | `metric_name:` with an empty name | the record is skipped | `logit.input.metrics.skipped{reason="bad_name"}` |
//! | a value that is neither a number nor one of the three strings | the record is skipped | `skipped{reason="bad_value"}` |
//! | the single-metric form naming a record the multi-metric form already carries | the multi-metric record wins | `skipped{reason="duplicate_name"}` |
//! | `metric_name` without `_value`, `_value` without `metric_name`, or a non-string or empty `metric_name` | the pair is skipped | `skipped{reason="incomplete_single_metric"}` |
//! | a metric event left with no record | the event is skipped | `logit.input.events.skipped{reason="no_metric"}` |
//!
//! ## Encode
//!
//! Every event's records share one set of dimensions: the parent module's `fields` rule, less the
//! reserved keys. Scalar records go out as multi-metric objects, one per `metric_type`; an
//! expanded record adds its own objects. Every object's `fields` are the dimensions, then
//! `metric_type`, then each `metric_name:<n>` in record order.
//!
//! | Model | Wire | Counter |
//! |---|---|---|
//! | `Gauge(v)` | `metric_name:<n>: v`, `metric_type` the event's `metric_type` attribute (verbatim) when present, else `"Gauge"` | -- |
//! | `Sum{Cumulative, monotonic}` | `metric_name:<n>: v`, `metric_type` `"Sum"` | -- |
//! | a delta or non-monotonic `Sum` | as above: `metric_type` `"Sum"` has no temporality or monotonicity carrier | `logit.output.metrics.degraded{metric_kind="delta_sum"\|"non_monotonic_sum"}` |
//! | a non-finite value | the string `"+Inf"`, `"-Inf"`, or `"NaN"` | -- |
//! | `GaugeDelta` | skipped: not a Splunk concept, and an `aggregate` resolves it upstream | `skipped{metric_kind="gauge_delta"}` |
//! | `ExponentialHistogram` | skipped under both `MultiValue` settings, as the exporter does | `skipped{metric_kind="exponential_histogram"}` |
//! | a record flagged `NO_RECORDED_VALUE` | skipped | `skipped{metric_kind="no_recorded_value"}` |
//! | `Samples`, `Distribution`, `Set`, `SetMembers`, `Histogram`, `Summary` under `MultiValue::Skip` | skipped | `skipped{metric_kind="samples"\|"distribution"\|"set"\|"set_members"\|"histogram"\|"summary"}` |
//! | the same under `MultiValue::Expand` | the table below | `degraded{metric_kind=…}`, once per record |
//! | a name outside `[A-Za-z0-9_.:]`, starting with a digit or `_`, or containing `metric_name` | each other character → `_`, `metric_name` → `metricname`, then an `m` prefix on a leading digit or `_` | `logit.output.metrics.normalized{reason="name_sanitized"}` |
//! | a name another record of the same object already wrote (after sanitizing) | skipped | `skipped{reason="duplicate_name"}` |
//! | a `metric_name`, `_value`, or `metric_name:*` attribute; a `metric_type` attribute on an event with a `Sum`; an `le` or `qt` attribute on an expanded bucket or quantile object | dropped from that object | `logit.output.tags.dropped{reason="reserved_key"}` |
//!
//! `unit`, `description`, `start_timestamp`, `exemplars`, and `flags` have no HEC field.
//!
//! ## `MultiValue::Expand`
//!
//! `Histogram` and `Summary` take the OpenTelemetry exporter's shape, so a Splunk dashboard built
//! for the Collector's output (`mstats` with `histperc`) reads them unchanged. `Samples` and
//! `Distribution` take `influxdb_out`'s summary set under `metric_type` `Summary`.
//!
//! | Kind | Objects |
//! |---|---|
//! | `Histogram` | `<n>_sum` (when `Some`) and `<n>_count` (Σ counts), `metric_type` `Histogram`; one object per cumulative bucket with `<n>_bucket` and an `le` dimension (`+Inf` last, appended when the highest bound is finite) |
//! | `Summary` | `<n>_sum` and `<n>_count`, `metric_type` `Summary`; one object per quantile with `<n>_<q>` and a `qt` dimension |
//! | `Samples` | `<n>_count` and `<n>_sum` (each value weighted by its inverse sample rate), `<n>_min`, `<n>_max` when non-empty, `metric_type` `Summary` |
//! | `Distribution` | `<n>_count`, `<n>_sum`, then `<n>_p50`, `<n>_p90`, `<n>_p99` each when the sketch answers it finitely, `metric_type` `Summary` |
//! | `Set` | `<n>`: the HyperLogLog estimate, `metric_type` `Gauge` |
//! | `SetMembers` | `<n>`: the distinct member count, `metric_type` `Gauge` |
//!
//! `le` and `qt` values are the bound or quantile as `f64`'s `Display` writes it (`0.005`, `1000`),
//! the shortest round-trip form Go's `FormatFloat(v, 'f', -1, 64)` also writes.

use super::{
    begin_object, write_fields, BatchContext, ObjectMeta, SplunkDecoder, SplunkEncoder,
    ATTR_METRIC_TYPE,
};
use crate::json::{flatten_into, json_to_value, write_str, write_value};
use crate::prometheus::cumulative_counts;
use crate::{MessageBuf, MultiValue, Signal};
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, DdSketch, Event, Histogram, MetricKind, MetricList, MetricRecord, Samples, Sum,
    Summary, Symbol, Temporality, Value,
};
use serde_json::{Map, Value as Json};
use std::borrow::Cow;

/// The `event` value that marks a metric event.
pub const METRIC_EVENT: &str = "metric";
/// The multi-metric form's field prefix.
pub const MULTI_METRIC_PREFIX: &str = "metric_name:";
/// The single-metric form's two fields.
pub const SINGLE_METRIC_NAME: &str = "metric_name";
pub const SINGLE_METRIC_VALUE: &str = "_value";

const GAUGE: &str = "Gauge";
const SUM: &str = "Sum";
const HISTOGRAM: &str = "Histogram";
const SUMMARY: &str = "Summary";

/// `Distribution`'s expanded quantiles and their suffixes: `influxdb_out`'s summary set.
const DISTRIBUTION_QUANTILES: [(f64, &str); 3] = [(0.5, "_p50"), (0.9, "_p90"), (0.99, "_p99")];

/// Whether `fields` carry a measurement in either form.
pub(super) fn is_metric_fields(fields: &Map<String, Json>) -> bool {
    fields.keys().any(|k| k.starts_with(MULTI_METRIC_PREFIX))
        || (fields.contains_key(SINGLE_METRIC_NAME) && fields.contains_key(SINGLE_METRIC_VALUE))
}

/// A measurement: a JSON number, the exporter's strings for the non-finite values, or a string
/// holding a finite number, which Splunk indexes as that number.
fn metric_value(value: &Json) -> Option<f64> {
    match value {
        Json::Number(n) => n.as_f64(),
        Json::String(s) => match s.as_str() {
            "+Inf" => Some(f64::INFINITY),
            "-Inf" => Some(f64::NEG_INFINITY),
            "NaN" => Some(f64::NAN),
            text => text.trim().parse::<f64>().ok().filter(|v| v.is_finite()),
        },
        _ => None,
    }
}

impl SplunkDecoder {
    pub(super) fn decode_metric(
        &mut self,
        fields: Map<String, Json>,
        timestamp: i64,
    ) -> Option<Event> {
        let (is_sum, consume_type) = match fields.get(ATTR_METRIC_TYPE) {
            Some(Json::String(t)) if t == SUM => (true, true),
            Some(Json::String(t)) if t == GAUGE => (false, true),
            _ => (false, false),
        };
        let mut records: Vec<(String, f64)> = Vec::new();
        let mut attributes = AttrMap::new();
        for (key, value) in &fields {
            if let Some(name) = key.strip_prefix(MULTI_METRIC_PREFIX) {
                if name.is_empty() {
                    self.skip_metric("bad_name");
                } else if let Some(v) = metric_value(value) {
                    records.push((name.to_string(), v));
                } else {
                    self.skip_metric("bad_value");
                }
            } else if key == SINGLE_METRIC_NAME
                || key == SINGLE_METRIC_VALUE
                || (key == ATTR_METRIC_TYPE && consume_type)
            {
                continue;
            } else {
                flatten_into(key, &json_to_value(value), &mut attributes);
            }
        }
        match (fields.get(SINGLE_METRIC_NAME), fields.get(SINGLE_METRIC_VALUE)) {
            (None, None) => {}
            (Some(Json::String(name)), Some(value)) if !name.is_empty() => {
                if records.iter().any(|(n, _)| n == name) {
                    self.skip_metric("duplicate_name");
                } else if let Some(v) = metric_value(value) {
                    records.push((name.clone(), v));
                } else {
                    self.skip_metric("bad_value");
                }
            }
            _ => self.skip_metric("incomplete_single_metric"),
        }
        if records.is_empty() {
            self.skip_event("no_metric");
            return None;
        }
        records.sort_by(|a, b| a.0.cmp(&b.0));
        let kind = |v: f64| {
            if is_sum {
                MetricKind::Sum(Sum {
                    value: v,
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                })
            } else {
                MetricKind::Gauge(v)
            }
        };
        let metrics: MetricList =
            records.into_iter().map(|(n, v)| MetricRecord::new(intern(&n), kind(v))).collect();
        Some(Event { timestamp, attributes, log: None, metrics, span: None })
    }

    fn skip_metric(&self, reason: &'static str) {
        self.telemetry.count("logit.input.metrics.skipped", 1.0, &[("reason", reason)]);
    }
}

/// `name` forced into HEC's metric-name grammar: `[A-Za-z0-9_.:]`, no leading digit or `_`, and
/// no `metric_name` substring. Borrows when `name` already conforms.
pub fn sanitize_metric_name(name: &str) -> Cow<'_, str> {
    let conforms = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':');
    let bad_lead = |s: &str| s.starts_with(|c: char| c.is_ascii_digit() || c == '_');
    if !name.is_empty()
        && name.chars().all(conforms)
        && !bad_lead(name)
        && !name.contains(SINGLE_METRIC_NAME)
    {
        return Cow::Borrowed(name);
    }
    let mut out: String = name.chars().map(|c| if conforms(c) { c } else { '_' }).collect();
    while out.contains(SINGLE_METRIC_NAME) {
        out = out.replace(SINGLE_METRIC_NAME, "metricname");
    }
    if out.is_empty() || bad_lead(&out) {
        out.insert(0, 'm');
    }
    Cow::Owned(out)
}

/// One metric object being built: its `metric_type`, an optional extra dimension (`le`, `qt`),
/// and its measurements.
struct MetricObject {
    metric_type: Value,
    extra: Option<(&'static str, String)>,
    values: Vec<(String, f64)>,
}

/// An event's metric objects, in first-use order.
#[derive(Default)]
struct Objects {
    objects: Vec<MetricObject>,
    duplicates: usize,
}

impl Objects {
    /// Adds `name = value` to the scalar object for `metric_type`, opening it on first use.
    fn scalar(&mut self, metric_type: &Value, name: String, value: f64) {
        let at = match self
            .objects
            .iter()
            .position(|o| o.extra.is_none() && &o.metric_type == metric_type)
        {
            Some(at) => at,
            None => {
                self.objects.push(MetricObject {
                    metric_type: metric_type.clone(),
                    extra: None,
                    values: Vec::new(),
                });
                self.objects.len() - 1
            }
        };
        let values = &mut self.objects[at].values;
        if values.iter().any(|(n, _)| *n == name) {
            self.duplicates += 1;
        } else {
            values.push((name, value));
        }
    }

    /// Opens an object of its own carrying one extra dimension.
    fn dimensioned(
        &mut self,
        metric_type: &str,
        dim: &'static str,
        dim_value: String,
        name: String,
        value: f64,
    ) {
        self.objects.push(MetricObject {
            metric_type: Value::str(metric_type),
            extra: Some((dim, dim_value)),
            values: vec![(name, value)],
        });
    }
}

impl SplunkEncoder {
    pub(super) fn encode_metrics(
        &mut self,
        ctx: &BatchContext<'_>,
        event: &Event,
        event_index: usize,
        out: &mut MessageBuf<ObjectMeta>,
    ) {
        let mut dims = SplunkEncoder::object_fields(ctx.resource, event);
        let reserved: Vec<Symbol> = dims
            .iter()
            .map(|(key, _)| key)
            .filter(|key| {
                let key = resolve(*key);
                key == SINGLE_METRIC_NAME
                    || key == SINGLE_METRIC_VALUE
                    || key.starts_with(MULTI_METRIC_PREFIX)
            })
            .collect();
        let mut reserved_dropped = reserved.len();
        for key in reserved {
            dims.remove_sym(key);
        }
        let type_attr = dims.remove(ATTR_METRIC_TYPE);
        let gauge_type = type_attr.clone().unwrap_or_else(|| Value::str(GAUGE));
        let sum_type = Value::str(SUM);

        let mut objects = Objects::default();
        for record in &event.metrics {
            if record.is_no_recorded_value() {
                self.skip_kind("no_recorded_value");
                continue;
            }
            let name = self.sanitized(resolve(record.name));
            match &record.kind {
                MetricKind::Gauge(v) => objects.scalar(&gauge_type, name, *v),
                MetricKind::Sum(sum) => {
                    if sum.temporality == Temporality::Delta {
                        self.degrade_kind("delta_sum");
                    } else if !sum.monotonic {
                        self.degrade_kind("non_monotonic_sum");
                    }
                    objects.scalar(&sum_type, name, sum.value);
                }
                MetricKind::GaugeDelta(_) => self.skip_kind("gauge_delta"),
                MetricKind::ExponentialHistogram(_) => self.skip_kind("exponential_histogram"),
                kind => match self.multi_value {
                    MultiValue::Skip => self.skip_kind(kind.name()),
                    MultiValue::Expand => {
                        self.degrade_kind(kind.name());
                        expand(kind, &name, &mut objects);
                    }
                },
            }
        }
        if type_attr.is_some() && objects.objects.iter().any(|o| o.metric_type == sum_type) {
            reserved_dropped += 1;
        }
        if objects.duplicates > 0 {
            self.telemetry.count(
                "logit.output.metrics.skipped",
                objects.duplicates as f64,
                &[("reason", "duplicate_name")],
            );
        }

        for object in &objects.objects {
            let mut fields = dims.clone();
            if let Some((dim, value)) = &object.extra {
                if fields.get(dim).is_some() {
                    reserved_dropped += 1;
                }
                fields.insert(dim, Value::str(value.as_str()));
            }
            self.scratch.clear();
            let mut obj = begin_object(&mut self.scratch, event.timestamp, &ctx.envelope);
            write_str(obj.key("event"), METRIC_EVENT);
            let mut key = String::new();
            write_fields(&mut obj, &fields, |inner| {
                write_value(inner.key(ATTR_METRIC_TYPE), &object.metric_type);
                for (name, value) in &object.values {
                    key.clear();
                    key.push_str(MULTI_METRIC_PREFIX);
                    key.push_str(name);
                    write_measurement(inner.key(&key), *value);
                }
            });
            obj.finish();
            out.push_with(
                &self.scratch,
                ObjectMeta { event_index, signal: Signal::Metrics, records: object.values.len() },
            );
        }
        self.reserved_key_dropped(reserved_dropped);
    }

    fn sanitized(&self, name: &str) -> String {
        let sanitized = sanitize_metric_name(name);
        if let Cow::Owned(_) = sanitized {
            self.telemetry.count(
                "logit.output.metrics.normalized",
                1.0,
                &[("reason", "name_sanitized")],
            );
        }
        sanitized.into_owned()
    }

    fn skip_kind(&self, kind: &'static str) {
        self.telemetry.count("logit.output.metrics.skipped", 1.0, &[("metric_kind", kind)]);
    }

    fn degrade_kind(&self, kind: &'static str) {
        self.telemetry.count("logit.output.metrics.degraded", 1.0, &[("metric_kind", kind)]);
    }
}

/// A measurement: a finite value as a JSON number, a non-finite one as the exporter's string.
fn write_measurement(out: &mut Vec<u8>, value: f64) {
    if value.is_finite() {
        write_value(out, &Value::F64(value));
    } else if value.is_nan() {
        write_str(out, "NaN");
    } else if value > 0.0 {
        write_str(out, "+Inf");
    } else {
        write_str(out, "-Inf");
    }
}

/// A bound or quantile as an `le`/`qt` dimension value.
fn dimension_number(v: f64) -> String {
    if v == f64::INFINITY {
        "+Inf".to_string()
    } else if v == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        v.to_string()
    }
}

/// The `MultiValue::Expand` table in the module doc. Scalar kinds and `ExponentialHistogram`
/// never reach it.
fn expand(kind: &MetricKind, name: &str, objects: &mut Objects) {
    let summary = Value::str(SUMMARY);
    let gauge = Value::str(GAUGE);
    let suffixed = |suffix: &str| format!("{name}{suffix}");
    match kind {
        MetricKind::Samples(samples) => expand_samples(samples, &summary, &suffixed, objects),
        MetricKind::Distribution(sketch) => expand_sketch(sketch, &summary, &suffixed, objects),
        MetricKind::Set(hll) => objects.scalar(&gauge, name.to_string(), hll.estimate() as f64),
        MetricKind::SetMembers(members) => {
            let mut distinct: Vec<&[u8]> = members.iter().map(|m| m.as_ref()).collect();
            distinct.sort_unstable();
            distinct.dedup();
            objects.scalar(&gauge, name.to_string(), distinct.len() as f64);
        }
        MetricKind::Histogram(histogram) => expand_histogram(histogram, name, &suffixed, objects),
        MetricKind::Summary(s) => expand_summary(s, name, &summary, &suffixed, objects),
        MetricKind::Sum(_)
        | MetricKind::Gauge(_)
        | MetricKind::GaugeDelta(_)
        | MetricKind::ExponentialHistogram(_) => unreachable!("never expanded"),
    }
}

fn expand_samples(
    samples: &Samples,
    summary: &Value,
    suffixed: &dyn Fn(&str) -> String,
    objects: &mut Objects,
) {
    let weight = samples.weight() as f64;
    let count = samples.values.len() as f64 * weight;
    let sum: f64 = samples.values.iter().map(|v| v * weight).sum();
    objects.scalar(summary, suffixed("_count"), count);
    objects.scalar(summary, suffixed("_sum"), sum);
    let min = samples.values.iter().copied().reduce(f64::min);
    let max = samples.values.iter().copied().reduce(f64::max);
    if let (Some(min), Some(max)) = (min, max) {
        objects.scalar(summary, suffixed("_min"), min);
        objects.scalar(summary, suffixed("_max"), max);
    }
}

fn expand_sketch(
    sketch: &DdSketch,
    summary: &Value,
    suffixed: &dyn Fn(&str) -> String,
    objects: &mut Objects,
) {
    objects.scalar(summary, suffixed("_count"), sketch.count() as f64);
    objects.scalar(summary, suffixed("_sum"), sketch.sum());
    for (q, suffix) in DISTRIBUTION_QUANTILES {
        if let Some(v) = sketch.quantile(q).filter(|v| v.is_finite()) {
            objects.scalar(summary, suffixed(suffix), v);
        }
    }
}

fn expand_histogram(
    histogram: &Histogram,
    name: &str,
    suffixed: &dyn Fn(&str) -> String,
    objects: &mut Objects,
) {
    let histogram_type = Value::str(HISTOGRAM);
    let (buckets, total) = cumulative_counts(&histogram.buckets);
    if let Some(sum) = histogram.sum {
        objects.scalar(&histogram_type, suffixed("_sum"), sum);
    }
    objects.scalar(&histogram_type, suffixed("_count"), total as f64);
    let bucket_name = format!("{name}_bucket");
    for (bound, count) in buckets {
        objects.dimensioned(
            HISTOGRAM,
            "le",
            dimension_number(bound),
            bucket_name.clone(),
            count as f64,
        );
    }
}

fn expand_summary(
    s: &Summary,
    name: &str,
    summary: &Value,
    suffixed: &dyn Fn(&str) -> String,
    objects: &mut Objects,
) {
    objects.scalar(summary, suffixed("_sum"), s.sum);
    objects.scalar(summary, suffixed("_count"), s.count as f64);
    for (q, v) in &s.quantiles {
        let q_text = dimension_number(*q);
        objects.dimensioned(SUMMARY, "qt", q_text.clone(), format!("{name}_{q_text}"), *v);
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{counted, decode, decoder, encode, encoder, RECEIVED_AT};
    use super::*;
    use logit_core::telemetry::Registry;
    use logit_core::{EventBatch, HyperLogLog, Resource};
    use std::sync::Arc;

    fn record_values(event: &Event) -> Vec<(String, MetricKind)> {
        event.metrics.iter().map(|m| (resolve(m.name).to_string(), m.kind.clone())).collect()
    }

    fn cumulative(v: f64) -> MetricKind {
        MetricKind::Sum(Sum { value: v, temporality: Temporality::Cumulative, monotonic: true })
    }

    fn metric_batch(kinds: Vec<(&str, MetricKind)>, attrs: &[(&'static str, Value)]) -> EventBatch {
        let metrics: MetricList =
            kinds.into_iter().map(|(n, k)| MetricRecord::new(intern(n), k)).collect();
        let attributes = attrs.iter().cloned().collect();
        EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event { timestamp: 0, attributes, log: None, metrics, span: None }],
        }
    }

    fn objects_of(encoder: &mut SplunkEncoder, batch: &EventBatch) -> Vec<String> {
        let mut out = MessageBuf::default();
        encoder.encode_objects(batch, &mut out);
        out.iter().map(|o| String::from_utf8(o.to_vec()).unwrap()).collect()
    }

    #[test]
    fn both_forms_decode_in_name_order_and_metric_type_sets_the_kind() {
        let batches = decode(
            r#"{"event":"metric","time":1,"fields":{"metric_name:b":2,"metric_name:a":1.5,"metric_name":"c","_value":3,"region":"us"}}{"event":"metric","time":1,"fields":{"metric_type":"Sum","metric_name:n":"+Inf"}}{"event":"metric","time":1,"fields":{"metric_type":"Histogram","metric_name:h_bucket":4,"le":"0.5"}}"#,
        );
        let events = &batches[0].events;
        assert_eq!(
            record_values(&events[0]),
            vec![
                ("a".into(), MetricKind::Gauge(1.5)),
                ("b".into(), MetricKind::Gauge(2.0)),
                ("c".into(), MetricKind::Gauge(3.0)),
            ]
        );
        assert_eq!(events[0].attributes.get("region"), Some(&Value::str("us")));
        assert_eq!(events[0].attributes.len(), 1);
        assert_eq!(record_values(&events[1]), vec![("n".into(), cumulative(f64::INFINITY))]);
        assert!(events[1].attributes.is_empty(), "metric_type Sum is consumed");
        assert_eq!(events[2].attributes.get(ATTR_METRIC_TYPE), Some(&Value::str("Histogram")));
        assert_eq!(record_values(&events[2]), vec![("h_bucket".into(), MetricKind::Gauge(4.0))]);
    }

    #[test]
    fn bad_measurements_are_skipped_and_counted() {
        let registry = Registry::new();
        let batches = decoder(&registry)
            .decode_events(
                br#"{"event":"metric","fields":{"metric_name:":1,"metric_name:x":"high","metric_name:y":2,"metric_name":"y","_value":3}}{"event":"metric","fields":{"metric_name":"z","_value":{}}}{"event":"metric","fields":{"metric_name:a":1,"metric_name":"b"}}"#,
                RECEIVED_AT,
            )
            .unwrap();
        let events = &batches[0].events;
        assert_eq!(events.len(), 2);
        assert_eq!(record_values(&events[0]), vec![("y".into(), MetricKind::Gauge(2.0))]);
        for (reason, n) in [
            ("bad_name", 1.0),
            ("bad_value", 2.0),
            ("duplicate_name", 1.0),
            ("incomplete_single_metric", 1.0),
        ] {
            assert_eq!(
                counted(&registry, "logit.input.metrics.skipped", ("reason", reason)),
                n,
                "{reason}"
            );
        }
        assert_eq!(counted(&registry, "logit.input.events.skipped", ("reason", "no_metric")), 1.0);
    }

    /// SC4S 3.40.0's own metrics, one object as it posted them: no `event`, every value a string.
    /// A 10.4.3 Splunk indexed this shape as a metric (`docs/plans/splunk-relay.md`, W5's run).
    #[test]
    fn sc4s_metrics_with_no_event_and_string_values_decode() {
        let batches = decode(
            r#"{"time": "1790358237", "host": "splunk-sc4s-fixture", "source": "sc4s", "sourcetype": "sc4s:metrics:v2", "index": "_metrics", "fields": {"sc4s_proto": "0", "module": "parser", "name": "p_cef_ts_end", "metric_name:spl.sc4syslog.parser.processed": "0", "metric_name:spl.sc4syslog.parser.discarded": "3"}}"#,
        );
        let event = &batches[0].events[0];
        assert!(event.log.is_none());
        assert_eq!(
            record_values(event),
            vec![
                ("spl.sc4syslog.parser.discarded".into(), MetricKind::Gauge(3.0)),
                ("spl.sc4syslog.parser.processed".into(), MetricKind::Gauge(0.0)),
            ]
        );
        assert_eq!(event.attributes.get("module"), Some(&Value::str("parser")));
        let relayed = encode(&batches);
        assert!(relayed.contains(r#""event":"metric""#), "{relayed}");
        assert_eq!(decode(&relayed), batches);
    }

    #[test]
    fn a_metric_word_without_measurements_is_a_log() {
        let batches = decode(r#"{"event":"metric","fields":{"metric_name":"x"}}"#);
        assert!(batches[0].events[0].log.is_some());
    }

    #[test]
    fn scalar_records_group_by_metric_type() {
        let batch = metric_batch(
            vec![
                ("g", MetricKind::Gauge(1.0)),
                ("s", cumulative(2.0)),
                ("inf", MetricKind::Gauge(f64::NEG_INFINITY)),
            ],
            &[("region", Value::str("us"))],
        );
        assert_eq!(
            encode(&[batch]),
            concat!(
                r#"{"time":0,"event":"metric","fields":{"region":"us","metric_type":"Gauge","metric_name:g":1.0,"metric_name:inf":"-Inf"}}"#,
                r#"{"time":0,"event":"metric","fields":{"region":"us","metric_type":"Sum","metric_name:s":2.0}}"#,
            )
        );
    }

    #[test]
    fn a_verbatim_metric_type_attribute_rides_on_gauges() {
        let batch = metric_batch(
            vec![("h_bucket", MetricKind::Gauge(4.0))],
            &[("metric_type", Value::str("Histogram")), ("le", Value::str("0.5"))],
        );
        let body = encode(&[batch]);
        assert!(body.contains(r#""metric_type":"Histogram","metric_name:h_bucket":4.0"#), "{body}");
    }

    #[test]
    fn delta_and_non_monotonic_sums_are_counted_degraded() {
        let registry = Registry::new();
        let batch = metric_batch(
            vec![
                ("d", MetricKind::counter(1.0)),
                (
                    "n",
                    MetricKind::Sum(Sum {
                        value: 1.0,
                        temporality: Temporality::Cumulative,
                        monotonic: false,
                    }),
                ),
                ("gd", MetricKind::GaugeDelta(1.0)),
            ],
            &[],
        );
        let objects = objects_of(&mut encoder(&registry), &batch);
        assert_eq!(objects.len(), 1);
        assert_eq!(
            counted(&registry, "logit.output.metrics.degraded", ("metric_kind", "delta_sum")),
            1.0
        );
        let registry2 = Registry::new();
        objects_of(&mut encoder(&registry2), &batch);
        assert_eq!(
            counted(
                &registry2,
                "logit.output.metrics.degraded",
                ("metric_kind", "non_monotonic_sum")
            ),
            1.0
        );
        let registry3 = Registry::new();
        objects_of(&mut encoder(&registry3), &batch);
        assert_eq!(
            counted(&registry3, "logit.output.metrics.skipped", ("metric_kind", "gauge_delta")),
            1.0
        );
    }

    fn multi_kinds() -> Vec<(&'static str, MetricKind)> {
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        let mut sketch = DdSketch::new();
        for v in [1.0, 2.0, 3.0] {
            sketch.add(v);
        }
        vec![
            ("samples", MetricKind::Samples(Samples::new([1.0, 3.0]))),
            ("dist", MetricKind::Distribution(sketch)),
            ("set", MetricKind::Set(hll)),
            (
                "members",
                MetricKind::SetMembers(vec![
                    bytes::Bytes::from_static(b"a"),
                    bytes::Bytes::from_static(b"a"),
                ]),
            ),
            (
                "hist",
                MetricKind::Histogram(Histogram {
                    buckets: vec![(1.0, 2), (0.5, 1)],
                    temporality: Temporality::Cumulative,
                    sum: Some(2.5),
                    min: None,
                    max: None,
                }),
            ),
            (
                "summ",
                MetricKind::Summary(Summary { quantiles: vec![(0.99, 9.0)], count: 4, sum: 10.0 }),
            ),
            (
                "exp",
                MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
                    scale: 0,
                    zero_count: 0,
                    zero_threshold: 0.0,
                    positive: (0, vec![1]),
                    negative: (0, vec![]),
                    temporality: Temporality::Delta,
                    count: 1,
                    sum: None,
                    min: None,
                    max: None,
                }),
            ),
        ]
    }

    #[test]
    fn skip_drops_every_multi_number_kind_and_counts_it() {
        let registry = Registry::new();
        let objects = objects_of(&mut encoder(&registry), &metric_batch(multi_kinds(), &[]));
        assert!(objects.is_empty());
        for kind in [
            "samples",
            "distribution",
            "set",
            "set_members",
            "histogram",
            "summary",
            "exponential_histogram",
        ] {
            assert_eq!(
                counted(&registry, "logit.output.metrics.skipped", ("metric_kind", kind)),
                1.0,
                "{kind}"
            );
        }
    }

    #[test]
    fn expand_writes_the_exporter_shape_and_counts_each_record_once() {
        let registry = Registry::new();
        let mut enc = encoder(&registry).with_multi_value(MultiValue::Expand);
        let objects = objects_of(&mut enc, &metric_batch(multi_kinds(), &[]));
        let summary = r#"{"time":0,"event":"metric","fields":{"metric_type":"Summary","metric_name:samples_count":2.0,"metric_name:samples_sum":4.0,"metric_name:samples_min":1.0,"metric_name:samples_max":3.0,"metric_name:dist_count":3.0,"metric_name:dist_sum":6.0,"#;
        assert!(objects[0].starts_with(summary), "{}", objects[0]);
        assert!(objects[0].contains(r#""metric_name:dist_p50":"#), "{}", objects[0]);
        assert!(
            objects[0].contains(r#""metric_name:summ_sum":10.0,"metric_name:summ_count":4.0}}"#),
            "{}",
            objects[0]
        );
        assert_eq!(
            objects[1..].to_vec(),
            vec![
                r#"{"time":0,"event":"metric","fields":{"metric_type":"Gauge","metric_name:set":1.0,"metric_name:members":1.0}}"#.to_string(),
                r#"{"time":0,"event":"metric","fields":{"metric_type":"Histogram","metric_name:hist_sum":2.5,"metric_name:hist_count":3.0}}"#.to_string(),
                r#"{"time":0,"event":"metric","fields":{"le":"0.5","metric_type":"Histogram","metric_name:hist_bucket":1.0}}"#.to_string(),
                r#"{"time":0,"event":"metric","fields":{"le":"1","metric_type":"Histogram","metric_name:hist_bucket":3.0}}"#.to_string(),
                r#"{"time":0,"event":"metric","fields":{"le":"+Inf","metric_type":"Histogram","metric_name:hist_bucket":3.0}}"#.to_string(),
                r#"{"time":0,"event":"metric","fields":{"qt":"0.99","metric_type":"Summary","metric_name:summ_0.99":9.0}}"#.to_string(),
            ]
        );
        for kind in ["samples", "distribution", "set", "set_members", "histogram", "summary"] {
            assert_eq!(
                counted(&registry, "logit.output.metrics.degraded", ("metric_kind", kind)),
                1.0,
                "{kind}"
            );
        }
    }

    #[test]
    fn expanded_series_decode_back_as_the_exporters_gauges() {
        let mut enc = SplunkEncoder::new().with_multi_value(MultiValue::Expand);
        let batch = metric_batch(multi_kinds(), &[]);
        let mut body = String::new();
        for object in objects_of(&mut enc, &batch) {
            body.push_str(&object);
        }
        let batches = decode(&body);
        let bucket = batches[0]
            .events
            .iter()
            .find(|e| e.attributes.get("le") == Some(&Value::str("+Inf")))
            .unwrap();
        assert_eq!(bucket.attributes.get(ATTR_METRIC_TYPE), Some(&Value::str("Histogram")));
        assert_eq!(record_values(bucket), vec![("hist_bucket".into(), MetricKind::Gauge(3.0))]);
        let e1 = encode(&batches);
        assert_eq!(decode(&e1), batches, "the expanded form decodes to a fixed point");
        assert_eq!(encode(&decode(&e1)), e1);
    }

    #[test]
    fn the_sanitizer_forces_names_into_the_grammar() {
        for (name, want) in [
            ("cpu.user", "cpu.user"),
            ("a:b_c", "a:b_c"),
            ("http-requests/total", "http_requests_total"),
            ("9lives", "m9lives"),
            ("_hidden", "m_hidden"),
            ("my.metric_name.x", "my.metricname.x"),
            ("", "m"),
            ("é", "m_"),
        ] {
            assert_eq!(sanitize_metric_name(name), want, "{name}");
        }
        assert!(matches!(sanitize_metric_name("ok"), Cow::Borrowed(_)));
    }

    #[test]
    fn sanitizing_is_counted_and_duplicate_names_are_skipped() {
        let registry = Registry::new();
        let batch = metric_batch(
            vec![("a-b", MetricKind::Gauge(1.0)), ("a_b", MetricKind::Gauge(2.0))],
            &[("metric_name:x", Value::F64(1.0)), ("_value", Value::I64(1))],
        );
        let objects = objects_of(&mut encoder(&registry), &batch);
        assert_eq!(
            objects,
            vec![
                r#"{"time":0,"event":"metric","fields":{"metric_type":"Gauge","metric_name:a_b":1.0}}"#
            ]
        );
        assert_eq!(
            counted(&registry, "logit.output.metrics.normalized", ("reason", "name_sanitized")),
            1.0
        );
        assert_eq!(
            counted(&registry, "logit.output.metrics.skipped", ("reason", "duplicate_name")),
            1.0
        );
        assert_eq!(
            counted(&registry, "logit.output.tags.dropped", ("reason", "reserved_key")),
            2.0
        );
    }

    #[test]
    fn a_no_recorded_value_record_is_skipped() {
        let registry = Registry::new();
        let mut batch = metric_batch(vec![("g", MetricKind::Gauge(1.0))], &[]);
        batch.events[0].metrics[0].flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        assert!(objects_of(&mut encoder(&registry), &batch).is_empty());
        assert_eq!(
            counted(
                &registry,
                "logit.output.metrics.skipped",
                ("metric_kind", "no_recorded_value")
            ),
            1.0
        );
    }
}
