//! `scale`: multiplies named numeric attributes by a constant factor, in place -- unit conversion
//! (nginx's `request_time` in seconds -> milliseconds, say, to share a measurement name with a
//! source that already reports milliseconds) without a Lua script. See
//! `docs/adr/scale-transform.md`.
//!
//! Stateless -- like `json`/`kv_metrics`, only `process` is overridden; `flush_interval`/`flush`
//! keep the `Transform` trait's defaults.

use crate::numeric;
use logit_core::interner::{intern, resolve};
use logit_core::{Event, Resource, Symbol, Telemetry, Value};
use logit_pipeline::Transform;
use std::sync::Arc;

/// One `field -> factor` entry, interned once at construction ([`Scale::new`]) rather than per
/// event -- `intern`/`resolve` are hash lookups, and this runs on the hot path once per field per
/// event (mirroring `kv_metrics::CompiledMetric`'s own reasoning).
struct CompiledScale {
    field: Symbol,
    factor: f64,
}

pub struct Scale {
    fields: Vec<CompiledScale>,
    telemetry: Telemetry,
}

impl Scale {
    pub fn new(fields: Vec<(String, f64)>) -> Self {
        Self {
            fields: fields
                .into_iter()
                .map(|(field, factor)| CompiledScale { field: intern(&field), factor })
                .collect(),
            telemetry: Telemetry::default(),
        }
    }

    /// Attaches a telemetry handle -- see [`Set::with_telemetry`](crate::Set::with_telemetry) for
    /// why there's no `Diagnostics` builder alongside it: a missing or non-numeric field is a
    /// silent skip, never an error.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl Transform for Scale {
    /// Multiplies each configured field's current value by its factor and writes the result back
    /// under the same name, always as `Value::F64` -- even when the product is exact, so a
    /// field's type never flaps between events depending on whether the multiplication happened
    /// to stay integral (a real hazard for OTLP/Loki consumers downstream, which see `scale`'s
    /// output as the type it declares). A missing or non-numeric field, or a non-finite product,
    /// is a silent skip for that field only, never a dropped event -- the same posture
    /// `kv_metrics` takes toward its own fields (`docs/adr/kv-metrics-semantics.md`).
    /// `log`/`span`/other attributes/`timestamp` are untouched, and this always returns `Some`.
    ///
    /// Records `logit.transform.scaled`/`.scaled.skipped` per configured field, mirroring
    /// `kv_metrics`'s `logit.transform.derived{,.skipped}` -- the skipped-vs-scaled ratio is the
    /// visible signal for this transform's documented silent-skip path.
    fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        for f in &self.fields {
            let scaled = event
                .attributes
                .get(resolve(f.field))
                .and_then(numeric)
                .map(|v| v * f.factor)
                .filter(|v| v.is_finite());
            if let Some(value) = scaled {
                event.attributes.insert_sym(f.field, Value::F64(value));
                self.telemetry.count("logit.transform.scaled", 1.0, &[]);
            } else {
                self.telemetry.count("logit.transform.scaled.skipped", 1.0, &[]);
            }
        }
        Some(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, BodyFormat, LogRecord, Registry};

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

    #[test]
    fn an_integer_field_scales_to_a_float() {
        let mut scale = Scale::new(vec![("request_time".to_string(), 1000.0)]);
        let resource = default_resource();
        let event = event_with_attrs(&[("request_time", Value::I64(2))]);
        let event = scale.process(&resource, event).expect("always forwards");
        assert_eq!(event.attributes.get("request_time"), Some(&Value::F64(2000.0)));
    }

    #[test]
    fn a_float_field_scales() {
        let mut scale = Scale::new(vec![("request_time".to_string(), 1000.0)]);
        let resource = default_resource();
        let event = event_with_attrs(&[("request_time", Value::F64(0.012))]);
        let event = scale.process(&resource, event).expect("always forwards");
        match event.attributes.get("request_time") {
            Some(Value::F64(v)) => assert!((v - 12.0).abs() < 1e-9, "got {v}"),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn a_numeric_string_field_still_coerces() {
        let mut scale = Scale::new(vec![("request_time".to_string(), 1000.0)]);
        let resource = default_resource();
        let event = event_with_attrs(&[("request_time", Value::str("0.5"))]);
        let event = scale.process(&resource, event).expect("always forwards");
        assert_eq!(event.attributes.get("request_time"), Some(&Value::F64(500.0)));
    }

    #[test]
    fn a_missing_field_is_a_no_op() {
        let mut scale = Scale::new(vec![("nope".to_string(), 1000.0)]);
        let resource = default_resource();
        let event = event_with_attrs(&[]);
        let event = scale.process(&resource, event).expect("always forwards");
        assert!(event.attributes.get("nope").is_none());
    }

    #[test]
    fn bool_null_array_and_map_fields_never_coerce() {
        for value in [
            Value::Bool(true),
            Value::Null,
            Value::Array(vec![Value::U64(1)]),
            Value::Map(Box::new(AttrMap::new())),
        ] {
            let mut scale = Scale::new(vec![("f".to_string(), 1000.0)]);
            let resource = default_resource();
            let event = event_with_attrs(&[("f", value.clone())]);
            let event = scale.process(&resource, event).expect("always forwards");
            assert_eq!(
                event.attributes.get("f"),
                Some(&value),
                "{value:?} should be left untouched, never coerced"
            );
        }
    }

    #[test]
    fn a_non_finite_product_is_skipped_leaving_the_original_value() {
        let mut scale = Scale::new(vec![("f".to_string(), f64::MAX)]);
        let resource = default_resource();
        let event = event_with_attrs(&[("f", Value::F64(f64::MAX))]);
        let event = scale.process(&resource, event).expect("always forwards");
        assert_eq!(
            event.attributes.get("f"),
            Some(&Value::F64(f64::MAX)),
            "an overflowing product must not overwrite the original value"
        );
    }

    #[test]
    fn other_attributes_and_the_log_body_are_untouched() {
        let mut scale = Scale::new(vec![("request_time".to_string(), 1000.0)]);
        let resource = default_resource();
        let event =
            event_with_attrs(&[("request_time", Value::F64(0.01)), ("status", Value::I64(200))]);
        let original_log = event.log.clone();
        let event = scale.process(&resource, event).expect("always forwards");
        assert_eq!(event.attributes.get("status"), Some(&Value::I64(200)));
        assert_eq!(
            event.log.as_ref().map(|l| &l.message),
            original_log.as_ref().map(|l| &l.message)
        );
    }

    #[test]
    fn multiple_fields_scale_independently_in_one_pass() {
        let mut scale = Scale::new(vec![("a".to_string(), 2.0), ("b".to_string(), 10.0)]);
        let resource = default_resource();
        let event = event_with_attrs(&[("a", Value::I64(3)), ("b", Value::I64(4))]);
        let event = scale.process(&resource, event).expect("always forwards");
        assert_eq!(event.attributes.get("a"), Some(&Value::F64(6.0)));
        assert_eq!(event.attributes.get("b"), Some(&Value::F64(40.0)));
    }

    fn scaled_count(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Counter(v) if resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    #[test]
    fn a_scaled_field_records_scaled_not_skipped() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("nginx_scale", "scale", "transform");
        let mut scale =
            Scale::new(vec![("request_time".to_string(), 1000.0)]).with_telemetry(telemetry);
        let resource = default_resource();
        scale.process(&resource, event_with_attrs(&[("request_time", Value::F64(0.01))])).unwrap();

        let events = registry.drain(0);
        assert_eq!(scaled_count(&events, "logit.transform.scaled"), Some(1.0));
        assert_eq!(scaled_count(&events, "logit.transform.scaled.skipped"), None);
    }

    #[test]
    fn a_skipped_field_records_skipped_not_scaled() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("nginx_scale", "scale", "transform");
        let mut scale = Scale::new(vec![("nope".to_string(), 1000.0)]).with_telemetry(telemetry);
        let resource = default_resource();
        scale.process(&resource, event_with_attrs(&[])).unwrap();

        let events = registry.drain(0);
        assert_eq!(scaled_count(&events, "logit.transform.scaled"), None);
        assert_eq!(scaled_count(&events, "logit.transform.scaled.skipped"), Some(1.0));
    }
}
