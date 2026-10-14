//! `bucket`: classifies a numeric attribute into a named class by range, per field, so a number
//! becomes a bounded string value the equality components (`has_attributes`, `drop_attributes`,
//! `route`) can match. `keep_values`' numeric sibling. Stateless. See
//! `docs/adr/bucket-transform.md`.
//!
//! Per configured field, on each event:
//!
//! - Source absent: nothing happens for that field. `bucket` never stamps a class on an event
//!   that didn't carry the number.
//! - Source a finite number (`I64`, `U64`, `F64`, or a `Str` that parses to one, through
//!   [`numeric`]): the first range that contains it writes its class into `into`.
//! - Source present but not a finite number, or a number no range contains: `other` is written
//!   if configured, else nothing.
//!
//! After a write, `keep_source: false` removes the source, unless `into` names the source, which
//! the write has already replaced.

use crate::numeric;
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// One range's upper bound. Mirrors `logit_config::BucketRange`'s `lt`/`le`/neither;
/// `logit-cli` converts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BucketBound {
    /// `value < bound`.
    Lt(f64),
    /// `value <= bound`.
    Le(f64),
    /// Matches every number: the catch-all, last in its list.
    Any,
}

impl BucketBound {
    fn contains(self, n: f64) -> bool {
        match self {
            BucketBound::Lt(bound) => n < bound,
            BucketBound::Le(bound) => n <= bound,
            BucketBound::Any => true,
        }
    }
}

/// Config for one source field, in the shape `logit_config::BucketSpec` validates.
#[derive(Debug, Clone, PartialEq)]
pub struct BucketField {
    pub field: String,
    pub into: String,
    /// In config order; the first range containing the number wins.
    pub ranges: Vec<(BucketBound, String)>,
    pub other: Option<String>,
    pub keep_source: bool,
}

/// One field's classifier, interned once in [`Bucket::new`]. Each class is built as a `Value`
/// once, so a write is a `Bytes` refcount bump rather than a copy.
struct Classifier {
    source: Symbol,
    into: Symbol,
    ranges: Vec<(BucketBound, Value)>,
    other: Option<Value>,
    /// `keep_source: false` with `into` naming another field.
    remove_source: bool,
}

pub struct Bucket {
    fields: Vec<Classifier>,
    telemetry: Telemetry,
}

impl Bucket {
    pub fn new(fields: Vec<BucketField>) -> Self {
        let fields = fields
            .into_iter()
            .map(|f| Classifier {
                source: intern(&f.field),
                into: intern(&f.into),
                ranges: f
                    .ranges
                    .into_iter()
                    .map(|(bound, class)| (bound, Value::str(class)))
                    .collect(),
                other: f.other.map(Value::str),
                remove_source: !f.keep_source && f.into != f.field,
            })
            .collect();
        Self { fields, telemetry: Telemetry::default() }
    }

    /// Attaches a telemetry handle.
    ///
    /// There's no `Diagnostics` builder: a value no range matches is steady-state messy data,
    /// not a failure.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

fn classify(classifier: &Classifier, attrs: &mut AttrMap, telemetry: &Telemetry) {
    let field_tag = [("field", resolve(classifier.source))];
    let class = match attrs.get_sym(classifier.source) {
        None => return,
        Some(value) => {
            let matched = numeric(value).and_then(|n| {
                classifier.ranges.iter().find(|(bound, _)| bound.contains(n)).map(|(_, c)| c)
            });
            match matched {
                Some(class) => {
                    telemetry.count("logit.transform.values.bucketed", 1.0, &field_tag);
                    Some(class)
                }
                None => {
                    telemetry.count("logit.transform.values.unmatched", 1.0, &field_tag);
                    classifier.other.as_ref()
                }
            }
        }
    };
    let Some(class) = class else { return };
    attrs.insert_sym(classifier.into, class.clone());
    if classifier.remove_source {
        attrs.remove_sym(classifier.source);
    }
}

impl Transform for Bucket {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        for classifier in &self.fields {
            classify(classifier, &mut event.attributes, &self.telemetry);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{BodyFormat, LogRecord, MetricKind, Registry};

    fn event_with_attrs(pairs: &[(&str, Value)]) -> Event {
        let mut attrs = AttrMap::new();
        for (k, v) in pairs {
            attrs.insert(k, v.clone());
        }
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

    fn field(
        ranges: &[(BucketBound, &str)],
        other: Option<&str>,
        keep_source: bool,
    ) -> BucketField {
        BucketField {
            field: "d".to_string(),
            into: "class".to_string(),
            ranges: ranges.iter().map(|(b, c)| (*b, c.to_string())).collect(),
            other: other.map(str::to_string),
            keep_source,
        }
    }

    /// `lt: 0.1` fast, `le: 1` ok, catch-all slow.
    fn latency(other: Option<&str>) -> Bucket {
        Bucket::new(vec![field(
            &[
                (BucketBound::Lt(0.1), "fast"),
                (BucketBound::Le(1.0), "ok"),
                (BucketBound::Any, "slow"),
            ],
            other,
            true,
        )])
    }

    /// Runs `bucket` over one event carrying `d = value` and returns its `class` attribute.
    fn class_of(bucket: &mut Bucket, value: Value) -> Option<Value> {
        let mut event = event_with_attrs(&[("d", value)]);
        assert!(bucket.process(&Arc::new(Resource::default()), &mut event), "never drops");
        event.attributes.get("class").cloned()
    }

    #[test]
    fn lt_excludes_its_bound() {
        let mut b = latency(None);
        assert_eq!(class_of(&mut b, Value::F64(0.099)), Some(Value::str("fast")));
        assert_eq!(class_of(&mut b, Value::F64(0.1)), Some(Value::str("ok")));
    }

    #[test]
    fn le_includes_its_bound() {
        let mut b = latency(None);
        assert_eq!(class_of(&mut b, Value::F64(1.0)), Some(Value::str("ok")));
        assert_eq!(class_of(&mut b, Value::F64(1.000_001)), Some(Value::str("slow")));
    }

    #[test]
    fn the_catch_all_takes_everything_above_the_last_bound() {
        let mut b = latency(None);
        assert_eq!(class_of(&mut b, Value::I64(3600)), Some(Value::str("slow")));
    }

    #[test]
    fn first_matching_range_wins() {
        let mut b = Bucket::new(vec![field(
            &[(BucketBound::Le(0.0), "none"), (BucketBound::Any, "some")],
            None,
            true,
        )]);
        assert_eq!(class_of(&mut b, Value::I64(0)), Some(Value::str("none")));
        assert_eq!(class_of(&mut b, Value::I64(-5)), Some(Value::str("none")));
        assert_eq!(class_of(&mut b, Value::U64(1)), Some(Value::str("some")));
    }

    #[test]
    fn lt_then_le_on_one_bound_classes_that_single_value() {
        let mut b = Bucket::new(vec![field(
            &[
                (BucketBound::Lt(0.0), "neg"),
                (BucketBound::Le(0.0), "zero"),
                (BucketBound::Any, "pos"),
            ],
            None,
            true,
        )]);
        assert_eq!(class_of(&mut b, Value::I64(-1)), Some(Value::str("neg")));
        assert_eq!(class_of(&mut b, Value::I64(0)), Some(Value::str("zero")));
        assert_eq!(class_of(&mut b, Value::I64(1)), Some(Value::str("pos")));
    }

    #[test]
    fn a_numeric_string_coerces() {
        let mut b = latency(None);
        assert_eq!(class_of(&mut b, Value::str("0.05")), Some(Value::str("fast")));
        assert_eq!(class_of(&mut b, Value::str("2")), Some(Value::str("slow")));
    }

    #[test]
    fn an_absent_source_is_a_no_op_never_a_stamp() {
        let mut b = latency(Some("unknown"));
        let mut event = event_with_attrs(&[]);
        assert!(b.process(&Arc::new(Resource::default()), &mut event));
        assert_eq!(event.attributes.get("class"), None, "must not invent the class");
    }

    #[test]
    fn a_non_numeric_value_writes_other_when_set() {
        let mut b = latency(Some("unknown"));
        for value in [Value::str("-"), Value::Bool(true), Value::Null, Value::F64(f64::NAN)] {
            assert_eq!(class_of(&mut b, value.clone()), Some(Value::str("unknown")), "{value:?}");
        }
    }

    #[test]
    fn a_non_numeric_value_writes_nothing_without_other() {
        let mut b = latency(None);
        for value in [Value::str("fast"), Value::F64(f64::INFINITY), Value::str("inf")] {
            assert_eq!(class_of(&mut b, value.clone()), None, "{value:?}");
        }
    }

    #[test]
    fn a_number_no_range_matches_is_treated_as_non_numeric() {
        let ranges = [(BucketBound::Lt(1000.0), "under")];
        let mut without = Bucket::new(vec![field(&ranges, None, true)]);
        assert_eq!(class_of(&mut without, Value::I64(1000)), None);
        let mut with = Bucket::new(vec![field(&ranges, Some("over"), true)]);
        assert_eq!(class_of(&mut with, Value::I64(1000)), Some(Value::str("over")));
    }

    #[test]
    fn keep_source_true_keeps_the_number() {
        let mut b = latency(None);
        let mut event = event_with_attrs(&[("d", Value::F64(0.5))]);
        assert!(b.process(&Arc::new(Resource::default()), &mut event));
        assert_eq!(event.attributes.get("d"), Some(&Value::F64(0.5)));
        assert_eq!(event.attributes.get("class"), Some(&Value::str("ok")));
    }

    #[test]
    fn keep_source_false_removes_the_source_after_a_write() {
        let mut b =
            Bucket::new(vec![field(&[(BucketBound::Lt(1.0), "fast")], Some("unknown"), false)]);
        let resource = Arc::new(Resource::default());
        let mut matched = event_with_attrs(&[("d", Value::F64(0.5))]);
        assert!(b.process(&resource, &mut matched));
        assert_eq!(matched.attributes.get("d"), None);
        assert_eq!(matched.attributes.get("class"), Some(&Value::str("fast")));

        let mut other = event_with_attrs(&[("d", Value::str("n/a"))]);
        assert!(b.process(&resource, &mut other));
        assert_eq!(other.attributes.get("d"), None, "writing `other` is a write too");
        assert_eq!(other.attributes.get("class"), Some(&Value::str("unknown")));
    }

    #[test]
    fn keep_source_false_keeps_the_source_when_nothing_is_written() {
        let mut b = Bucket::new(vec![field(&[(BucketBound::Lt(1.0), "fast")], None, false)]);
        let mut event = event_with_attrs(&[("d", Value::F64(5.0))]);
        assert!(b.process(&Arc::new(Resource::default()), &mut event));
        assert_eq!(event.attributes.get("d"), Some(&Value::F64(5.0)));
        assert_eq!(event.attributes.get("class"), None);
    }

    #[test]
    fn into_naming_the_source_replaces_it_in_place() {
        for keep_source in [true, false] {
            let mut b = Bucket::new(vec![BucketField {
                into: "d".to_string(),
                ..field(&[(BucketBound::Lt(1.0), "fast")], None, keep_source)
            }]);
            let mut event = event_with_attrs(&[("d", Value::F64(0.5))]);
            assert!(b.process(&Arc::new(Resource::default()), &mut event));
            assert_eq!(
                event.attributes.get("d"),
                Some(&Value::str("fast")),
                "keep_source: {keep_source} must leave the class, not remove it"
            );
        }
    }

    #[test]
    fn log_and_metrics_are_untouched() {
        let mut event = event_with_attrs(&[("d", Value::F64(0.5))]);
        event.metrics.push(logit_core::MetricRecord::new(intern("m"), MetricKind::counter(1.0)));
        let original_message = event.log.as_ref().unwrap().message.clone();
        let mut b = latency(None);
        assert!(b.process(&Arc::new(Resource::default()), &mut event));
        assert_eq!(event.metrics.len(), 1);
        assert_eq!(event.log.as_ref().unwrap().message, original_message);
    }

    fn counter_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Sum(sum) if resolve(m.name) == name => Some(sum.value.as_f64()),
                _ => None,
            })
        })
    }

    #[test]
    fn telemetry_records_bucketed_and_unmatched_per_field() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("latency", "bucket", "transform");
        let mut b = Bucket::new(vec![field(&[(BucketBound::Lt(1.0), "fast")], None, true)])
            .with_telemetry(telemetry);
        class_of(&mut b, Value::F64(0.5));
        class_of(&mut b, Value::F64(2.0));
        class_of(&mut b, Value::str("x"));
        let mut absent = event_with_attrs(&[]);
        assert!(b.process(&Arc::new(Resource::default()), &mut absent));

        let events = registry.drain(0);
        assert_eq!(counter_value(&events, "logit.transform.values.bucketed"), Some(1.0));
        assert_eq!(
            counter_value(&events, "logit.transform.values.unmatched"),
            Some(2.0),
            "an absent source counts as neither"
        );
    }
}
