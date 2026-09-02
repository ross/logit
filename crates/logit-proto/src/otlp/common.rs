//! `Value` ↔ `AnyValue`, `AttrMap` ↔ `Vec<KeyValue>`, and the `InstrumentationScope` this crate
//! always stamps -- shared by `logs.rs`/`metrics.rs`/`traces.rs`, since every OTLP signal nests
//! attributes and scope the same way.
//!
//! **`Value` ↔ `AnyValue` is total, except three documented, one-directional cases** (see
//! `docs/known-gaps.md`'s "Cross-protocol semantic gaps" entry) -- all three share one root cause:
//! OTLP's `AnyValue` has exactly one integer variant (`IntValue`, signed 64-bit), so it cannot
//! distinguish "this was a `U64`", "this was a `Timestamp`", and "this was actually an `I64`" once
//! encoded. Nothing short of a `logit`-specific extension field would fix that -- not attempted
//! here, since it would mean a non-standard OTLP a real collector couldn't read.
//! - `Value::U64` within `i64::MAX` encodes as `IntValue` (the same representation `Value::I64`
//!   uses) and decodes back as `Value::I64`, not `Value::U64` -- exact numerically, but the
//!   "this was unsigned" fact doesn't survive.
//! - `Value::U64` above `i64::MAX` has no lossless `AnyValue` representation at all -- it encodes
//!   as `DoubleValue` instead, exact up to `f64`'s 2^53 integer range and lossy above it, and
//!   decodes back as `Value::F64`.
//! - `Value::Timestamp` has no OTLP value type of its own -- it encodes as `IntValue` too, so it
//!   decodes back as `Value::I64`, not `Value::Timestamp`.
//!
//! **Nesting.** A batch's single `Arc<Resource>` becomes one `Resource*` message
//! ([`resource_to_pb`]/[`pb_to_resource`]); every signal stamps one fixed
//! `InstrumentationScope { name: "logit", version: env!("CARGO_PKG_VERSION") }`
//! ([`logit_scope`]). On decode, scope name/version land as `otel.scope.name`/`otel.scope.version`
//! attributes ([`scope_attrs`]), a base every record's own attributes are layered onto
//! (`AttrMap::insert`'s overwrite-on-collision semantics mean a data-point-level attribute always
//! wins over a same-named scope one). Resource attributes are never copied into `Event::attributes`
//! at all -- they stay on `EventBatch::resource`, `Arc`-shared across every event exactly the way
//! every other codec in this crate already treats a batch's resource (see `crates/logit-core/src/
//! event.rs`). A downstream consumer that wants the full resource → scope → data-point precedence
//! merge-joins resource and event attributes at the point it renders them, the same way
//! `crates/logit-outputs/src/influxdb.rs`'s `render_tag_suffix` already does for line-protocol tags.

use crate::otlp::generated::opentelemetry::proto::common::v1 as pb;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{AttrMap, Resource, Value};

/// Converts one [`Value`] into an [`pb::AnyValue`]. See the module doc for the two lossy cases.
pub(crate) fn value_to_any_value(value: &Value) -> pb::AnyValue {
    use pb::any_value::Value as Any;
    let inner = match value {
        Value::Null => None,
        Value::Bool(b) => Some(Any::BoolValue(*b)),
        Value::I64(i) => Some(Any::IntValue(*i)),
        // Lossy above i64::MAX (equivalently, above 2^63 - 1): OTLP's IntValue is signed, so a
        // U64 that doesn't fit becomes a DoubleValue instead of silently wrapping negative.
        // Exact for any U64 up to f64's 2^53 exact-integer range, approximate beyond it.
        Value::U64(u) => Some(if *u <= i64::MAX as u64 {
            Any::IntValue(*u as i64)
        } else {
            Any::DoubleValue(*u as f64)
        }),
        Value::F64(f) => Some(Any::DoubleValue(*f)),
        Value::Bytes(b) => Some(Any::BytesValue(b.to_vec())),
        // `Value::Str` is documented to always hold valid UTF-8 (see `crate::value::Value::str`).
        Value::Str(b) => Some(Any::StringValue(
            std::str::from_utf8(b).expect("Value::Str is always valid UTF-8").to_string(),
        )),
        // No distinct OTLP value type -- IntValue is what `Value::I64` also encodes to, so this
        // is indistinguishable from an I64 once on the wire (see the module doc).
        Value::Timestamp(ts) => Some(Any::IntValue(*ts)),
        Value::Array(items) => Some(Any::ArrayValue(pb::ArrayValue {
            values: items.iter().map(value_to_any_value).collect(),
        })),
        Value::Map(map) => {
            Some(Any::KvlistValue(pb::KeyValueList { values: attrs_to_key_values(map) }))
        }
    };
    pb::AnyValue { value: inner }
}

/// The mirror of [`value_to_any_value`]. Total: every `AnyValue` variant, including an empty one
/// (`value: None`), decodes to some `Value`.
pub(crate) fn any_value_to_value(any: pb::AnyValue) -> Value {
    use pb::any_value::Value as Any;
    match any.value {
        None => Value::Null,
        Some(Any::StringValue(s)) => Value::str(s),
        Some(Any::BoolValue(b)) => Value::Bool(b),
        Some(Any::IntValue(i)) => Value::I64(i),
        Some(Any::DoubleValue(d)) => Value::F64(d),
        Some(Any::ArrayValue(a)) => {
            Value::Array(a.values.into_iter().map(any_value_to_value).collect())
        }
        Some(Any::KvlistValue(kv)) => {
            let mut attrs = AttrMap::new();
            key_values_into_attrs(kv.values, &mut attrs);
            Value::Map(Box::new(attrs))
        }
        Some(Any::BytesValue(b)) => Value::Bytes(Bytes::from(b)),
        // Profiling-signal-only (see the field's own doc comment in common.proto); logs/metrics/
        // traces never set it. Treated as absent rather than fabricating a string we don't have.
        Some(Any::StringValueStrindex(_)) => Value::Null,
    }
}

/// Renders `attrs` as OTLP `KeyValue`s, in `AttrMap`'s own sorted-`Symbol` iteration order.
pub(crate) fn attrs_to_key_values(attrs: &AttrMap) -> Vec<pb::KeyValue> {
    attrs
        .iter()
        .map(|(key, value)| pb::KeyValue {
            key: resolve(key).to_string(),
            value: Some(value_to_any_value(value)),
            // Profiling-signal-only field (see common.proto); logit never sets it.
            key_strindex: 0,
        })
        .collect()
}

/// Inserts every `KeyValue` into `attrs`, later entries overwriting an earlier one at the same
/// key -- `AttrMap::insert`'s own semantics, unchanged here.
pub(crate) fn key_values_into_attrs(kvs: Vec<pb::KeyValue>, attrs: &mut AttrMap) {
    for kv in kvs {
        let value = kv.value.map(any_value_to_value).unwrap_or(Value::Null);
        attrs.insert(&kv.key, value);
    }
}

/// The one `InstrumentationScope` every encoded request stamps. Not configurable -- there is
/// exactly one `logit` producing this data, so there is exactly one scope.
pub(crate) fn logit_scope() -> pb::InstrumentationScope {
    pb::InstrumentationScope {
        name: "logit".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        attributes: Vec::new(),
        dropped_attributes_count: 0,
    }
}

/// The `otel.scope.name`/`otel.scope.version` + scope-level attributes base every record in one
/// `ScopeLogs`/`ScopeSpans`/`ScopeMetrics` group starts from -- cloned once per record, then
/// overlaid with that record's own attributes (see the module doc's precedence note).
pub(crate) fn scope_attrs(scope: &Option<pb::InstrumentationScope>) -> AttrMap {
    let mut attrs = AttrMap::new();
    if let Some(scope) = scope {
        if !scope.name.is_empty() {
            attrs.insert("otel.scope.name", scope.name.as_str());
        }
        if !scope.version.is_empty() {
            attrs.insert("otel.scope.version", scope.version.as_str());
        }
        key_values_into_attrs(scope.attributes.clone(), &mut attrs);
    }
    attrs
}

pub(crate) fn resource_to_pb(
    resource: &Resource,
) -> crate::otlp::generated::opentelemetry::proto::resource::v1::Resource {
    crate::otlp::generated::opentelemetry::proto::resource::v1::Resource {
        attributes: attrs_to_key_values(&resource.attributes),
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    }
}

pub(crate) fn pb_to_resource(
    resource: Option<crate::otlp::generated::opentelemetry::proto::resource::v1::Resource>,
) -> Resource {
    let mut attrs = AttrMap::new();
    if let Some(resource) = resource {
        key_values_into_attrs(resource.attributes, &mut attrs);
    }
    Resource { attributes: attrs }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_value_variant_round_trips_through_anyvalue() {
        let cases = vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::I64(-42),
            Value::F64(3.5),
            Value::Bytes(Bytes::from_static(b"\x00\x01\xff")),
            Value::str("hello"),
        ];
        for value in cases {
            let round_tripped = any_value_to_value(value_to_any_value(&value));
            assert_eq!(round_tripped, value, "value {value:?} should round-trip unchanged");
        }

        // U64 and Timestamp are the two variants that do NOT round-trip to themselves -- OTLP's
        // AnyValue has exactly one integer type (signed IntValue) and no timestamp type at all, so
        // both decode back as a plain I64 (documented known gap, module doc).
        let cases_that_become_i64 = [
            (Value::U64(42), Value::I64(42)),
            (Value::Timestamp(1_700_000_000_000_000_000), Value::I64(1_700_000_000_000_000_000)),
        ];
        for (original, expected) in cases_that_become_i64 {
            assert_eq!(
                any_value_to_value(value_to_any_value(&original)),
                expected,
                "{original:?} must decode back as I64 -- AnyValue can't tell it apart from one"
            );
        }
    }

    #[test]
    fn a_u64_above_i64_max_encodes_as_a_double_not_a_negative_int() {
        let value = Value::U64(u64::MAX);
        let any = value_to_any_value(&value);
        match any.value {
            Some(pb::any_value::Value::DoubleValue(d)) => {
                assert_eq!(d, u64::MAX as f64, "should encode the double approximation, got {d}")
            }
            other => panic!("expected DoubleValue for a U64 above i64::MAX, got {other:?}"),
        }
        // Decodes back as F64 -- there is no way to recover it was ever a U64.
        assert_eq!(any_value_to_value(value_to_any_value(&value)), Value::F64(u64::MAX as f64));
    }

    #[test]
    fn a_null_value_round_trips_as_an_empty_anyvalue() {
        let any = value_to_any_value(&Value::Null);
        assert!(any.value.is_none(), "Value::Null should encode as AnyValue's empty oneof");
        assert_eq!(any_value_to_value(any), Value::Null);
    }

    #[test]
    fn a_nested_map_and_array_round_trip() {
        let mut inner = AttrMap::new();
        inner.insert("k1", "v1");
        inner.insert("k2", Value::I64(7));
        let value = Value::Array(vec![
            Value::str("a"),
            Value::Map(Box::new(inner)),
            Value::Array(vec![Value::I64(1), Value::I64(2)]),
        ]);

        let round_tripped = any_value_to_value(value_to_any_value(&value));
        assert_eq!(round_tripped, value, "a nested array/map should round-trip unchanged");
    }

    #[test]
    fn scope_name_and_version_land_as_prefixed_attributes() {
        let scope = Some(pb::InstrumentationScope {
            name: "logit".to_string(),
            version: "0.1.0".to_string(),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
        });
        let attrs = scope_attrs(&scope);
        assert_eq!(attrs.get("otel.scope.name").and_then(|v| v.as_str()), Some("logit"));
        assert_eq!(attrs.get("otel.scope.version").and_then(|v| v.as_str()), Some("0.1.0"));
    }

    #[test]
    fn a_missing_scope_produces_no_scope_attributes() {
        assert!(scope_attrs(&None).is_empty());
    }
}
