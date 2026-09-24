//! `Value` ↔ `AnyValue`, `AttrMap` ↔ `Vec<KeyValue>`, and resource and scope, shared by every
//! signal.
//!
//! **`Value` ↔ `AnyValue` is total except for three one-way cases** (`docs/known-gaps.md`'s
//! "Cross-protocol semantic gaps"). All three come from `AnyValue` having one integer variant,
//! signed `IntValue`; fixing them would take a non-standard extension a collector couldn't read.
//! - `Value::U64` up to `i64::MAX` encodes as `IntValue` and decodes as `Value::I64`: numerically
//!   exact, but no longer unsigned.
//! - `Value::U64` above `i64::MAX` encodes as `DoubleValue`, exact up to 2^53 and lossy above, and
//!   decodes as `Value::F64`.
//! - `Value::Timestamp` encodes as `IntValue` and decodes as `Value::I64`.
//!
//! **Nesting** (grouping rules in `super`'s module doc). A batch's resource becomes one
//! `Resource*` message ([`resource_to_pb`]/[`pb_to_resource`]); its `schema_url` is the wrapping
//! `Resource*` message's field, not the inner `Resource`'s. Its scope becomes one `Scope*` message
//! ([`scope_to_pb`]/[`pb_to_scope`]) the same way. Resource and scope attributes stay on the batch
//! and are never copied into `Event::attributes`; a sink that wants resource → scope → point
//! precedence merges them at render time, as `crates/logit-outputs/src/influxdb.rs`'s
//! `render_tag_suffix` does.

use crate::otlp::generated::opentelemetry::proto::common::v1 as pb;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{AttrMap, Resource, Scope, Value};

/// Converts one [`Value`] into an [`pb::AnyValue`]. See the module doc for the lossy cases.
pub(crate) fn value_to_any_value(value: &Value) -> pb::AnyValue {
    use pb::any_value::Value as Any;
    let inner = match value {
        Value::Null => None,
        Value::Bool(b) => Some(Any::BoolValue(*b)),
        Value::I64(i) => Some(Any::IntValue(*i)),
        // IntValue is signed, so a U64 above i64::MAX becomes a DoubleValue rather than wrapping
        // negative: exact up to 2^53, approximate beyond.
        Value::U64(u) => Some(if *u <= i64::MAX as u64 {
            Any::IntValue(*u as i64)
        } else {
            Any::DoubleValue(*u as f64)
        }),
        Value::F64(f) => Some(Any::DoubleValue(*f)),
        Value::Bytes(b) => Some(Any::BytesValue(b.to_vec())),
        // `Value::Str` always holds valid UTF-8 (`logit_core::Value::str`).
        Value::Str(b) => Some(Any::StringValue(
            std::str::from_utf8(b).expect("Value::Str is always valid UTF-8").to_string(),
        )),
        // No OTLP timestamp type; decodes as an I64 (see the module doc).
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
        // Profiling-signal-only (common.proto); logs, metrics, and traces never set it.
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
            // Profiling-signal-only (common.proto).
            key_strindex: 0,
        })
        .collect()
}

/// Inserts every `KeyValue` into `attrs`; on a duplicate key, the later entry wins.
pub(crate) fn key_values_into_attrs(kvs: Vec<pb::KeyValue>, attrs: &mut AttrMap) {
    for kv in kvs {
        let value = kv.value.map(any_value_to_value).unwrap_or(Value::Null);
        attrs.insert(&kv.key, value);
    }
}

/// Renders a model `Bytes` field that OTLP types as a string (`schema_url`, scope `name` and
/// `version`). Invalid UTF-8, which a conforming producer never sends, becomes U+FFFD.
pub(crate) fn bytes_to_string(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// The inverse of [`bytes_to_string`]. An empty string is `None`, the model's "unset" for every
/// `Option<Bytes>` field (`Resource::schema_url`, `Scope::schema_url`, `SpanExt`'s fields).
pub(crate) fn string_to_bytes(s: String) -> Option<Bytes> {
    if s.is_empty() {
        None
    } else {
        Some(Bytes::from(s))
    }
}

pub(crate) fn resource_to_pb(
    resource: &Resource,
) -> crate::otlp::generated::opentelemetry::proto::resource::v1::Resource {
    crate::otlp::generated::opentelemetry::proto::resource::v1::Resource {
        attributes: attrs_to_key_values(&resource.attributes),
        dropped_attributes_count: resource.dropped_attributes_count,
        entity_refs: Vec::new(),
    }
}

/// `schema_url` is the wrapping `Resource*` message's field (`ResourceLogs.schema_url`, ...), so
/// it arrives separately from `resource`.
pub(crate) fn pb_to_resource(
    resource: Option<crate::otlp::generated::opentelemetry::proto::resource::v1::Resource>,
    schema_url: &str,
) -> Resource {
    let mut attrs = AttrMap::new();
    let mut dropped_attributes_count = 0;
    if let Some(resource) = resource {
        key_values_into_attrs(resource.attributes, &mut attrs);
        dropped_attributes_count = resource.dropped_attributes_count;
    }
    Resource {
        attributes: attrs,
        dropped_attributes_count,
        schema_url: string_to_bytes(schema_url.to_string()),
    }
}

/// A batch's scope as the one `InstrumentationScope` its request carries. `None` becomes an empty
/// `InstrumentationScope`, never a fabricated `logit` identity (ADR `lossless-transit`).
pub(crate) fn scope_to_pb(scope: Option<&Scope>) -> pb::InstrumentationScope {
    match scope {
        None => pb::InstrumentationScope::default(),
        Some(scope) => pb::InstrumentationScope {
            name: bytes_to_string(&scope.name),
            version: bytes_to_string(&scope.version),
            attributes: attrs_to_key_values(&scope.attributes),
            dropped_attributes_count: scope.dropped_attributes_count,
        },
    }
}

/// The mirror of [`scope_to_pb`]. `schema_url` is the wrapping `Scope*` message's field
/// (`ScopeLogs.schema_url`, ...).
///
/// Returns `None` when the wire scope is all empty (absent, or empty name and version, no
/// attributes, no dropped count) and `schema_url` is empty too. [`scope_to_pb`] encodes `None`
/// that way, so without the collapse a batch with no scope (every statsd, syslog, or native one)
/// wouldn't be an OTLP decode/encode fixed point (ADR `lossless-transit`).
pub(crate) fn pb_to_scope(
    scope: Option<pb::InstrumentationScope>,
    schema_url: &str,
) -> Option<Scope> {
    let mut attributes = AttrMap::new();
    let (name, version, dropped_attributes_count) = match scope {
        Some(scope) => {
            key_values_into_attrs(scope.attributes, &mut attributes);
            (scope.name, scope.version, scope.dropped_attributes_count)
        }
        None => (String::new(), String::new(), 0),
    };
    if name.is_empty()
        && version.is_empty()
        && attributes.is_empty()
        && dropped_attributes_count == 0
        && schema_url.is_empty()
    {
        return None;
    }
    Some(Scope {
        name: Bytes::from(name),
        version: Bytes::from(version),
        attributes,
        dropped_attributes_count,
        schema_url: string_to_bytes(schema_url.to_string()),
    })
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

        // U64 and Timestamp both decode as I64 (the module doc's lossy cases).
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
        // Decodes as F64.
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
    fn a_none_scope_encodes_as_an_empty_instrumentation_scope_not_a_fabricated_identity() {
        let pb_scope = scope_to_pb(None);
        assert_eq!(pb_scope.name, "", "must never invent a \"logit\" scope identity");
        assert_eq!(pb_scope.version, "");
        assert!(pb_scope.attributes.is_empty());
        assert_eq!(pb_scope.dropped_attributes_count, 0);
    }

    #[test]
    fn a_populated_scope_round_trips_through_pb_and_back() {
        let mut attrs = AttrMap::new();
        attrs.insert("k", "v");
        let scope = Scope {
            name: Bytes::from_static(b"nginx-otel-module"),
            version: Bytes::from_static(b"1.0.0"),
            attributes: attrs,
            dropped_attributes_count: 3,
            schema_url: Some(Bytes::from_static(b"https://example.com/schema")),
        };
        let pb_scope = scope_to_pb(Some(&scope));
        assert_eq!(pb_scope.name, "nginx-otel-module");
        assert_eq!(pb_scope.version, "1.0.0");
        assert_eq!(pb_scope.dropped_attributes_count, 3);

        let decoded = pb_to_scope(Some(pb_scope), "https://example.com/schema");
        assert_eq!(decoded, Some(scope));
    }

    #[test]
    fn a_missing_scope_message_decodes_to_a_default_scope_but_keeps_a_present_schema_url() {
        // A schema_url alone keeps the scope from collapsing to None.
        let decoded = pb_to_scope(None, "https://example.com/schema")
            .expect("a present schema_url must not collapse to None");
        assert_eq!(decoded.name, Bytes::new());
        assert_eq!(decoded.version, Bytes::new());
        assert!(decoded.attributes.is_empty());
        assert_eq!(decoded.schema_url, Some(Bytes::from_static(b"https://example.com/schema")));
    }

    /// An all-empty wire scope with an empty `schema_url` decodes to `None`, not
    /// `Some(Scope::default())`.
    #[test]
    fn a_fully_empty_scope_and_schema_url_decodes_to_none() {
        assert_eq!(pb_to_scope(None, ""), None);
        assert_eq!(
            pb_to_scope(Some(pb::InstrumentationScope::default()), ""),
            None,
            "an explicitly-present but all-default InstrumentationScope message is still \
             indistinguishable from no scope at all"
        );
    }

    #[test]
    fn resource_dropped_attributes_count_and_schema_url_round_trip() {
        let mut attrs = AttrMap::new();
        attrs.insert("service.name", "orders-api");
        let resource = Resource {
            attributes: attrs,
            dropped_attributes_count: 5,
            schema_url: Some(Bytes::from_static(b"https://example.com/schema")),
        };
        let pb_resource = resource_to_pb(&resource);
        assert_eq!(pb_resource.dropped_attributes_count, 5);

        let decoded = pb_to_resource(Some(pb_resource), "https://example.com/schema");
        assert_eq!(decoded, resource);
    }

    #[test]
    fn a_missing_resource_schema_url_decodes_to_none() {
        let decoded = pb_to_resource(None, "");
        assert_eq!(decoded.schema_url, None);
    }
}
