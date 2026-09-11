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
//! ([`resource_to_pb`]/[`pb_to_resource`]), carrying the resource's own `dropped_attributes_count`
//! and (at the wrapping `Resource*` message's own `schema_url` field, not part of the `Resource`
//! message itself) its `schema_url`. A batch's single `Option<Arc<Scope>>` becomes one `Scope*`
//! message the same way ([`scope_to_pb`]/[`pb_to_scope`]): `batch.scope == None` encodes an empty
//! `InstrumentationScope` (empty name -- never a fabricated `"logit"`/version; see `../mod.rs`'s own
//! doc for why nothing invents an identity that was never there), never a fixed, hardcoded scope.
//! Decode groups every `(Resource*, Scope*)` pair in a request into its own `EventBatch` -- see
//! `../mod.rs`'s own "Nesting" note for the full grouping rule and why a request with several scopes
//! under one resource decodes to several batches, never flattened into one. Resource attributes are
//! never copied into `Event::attributes` at all -- they stay on `EventBatch::resource`, `Arc`-shared
//! across every event exactly the way every other codec in this crate already treats a batch's
//! resource (see `crates/logit-core/src/event.rs`); the same is true of scope attributes, which now
//! live on `EventBatch::scope` rather than being copied per event. A downstream consumer that wants
//! the full resource → scope → data-point precedence merge-joins resource, scope, and event
//! attributes at the point it renders them, the same way `crates/logit-outputs/src/influxdb.rs`'s
//! `render_tag_suffix` already does for line-protocol tags.

use crate::otlp::generated::opentelemetry::proto::common::v1 as pb;
use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::{AttrMap, Resource, Scope, Value};

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

/// A textual OTLP field (`schema_url`, `InstrumentationScope.name`/`.version`) stored as `Bytes`
/// on `logit`'s own model -- lossy only in the sense any non-UTF-8 byte sequence a well-behaved
/// producer would never send becomes the Unicode replacement character, the same tradeoff
/// `Value::Str`'s own "always valid UTF-8" contract already makes throughout this crate.
pub(crate) fn bytes_to_string(bytes: &Bytes) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// The inverse of [`bytes_to_string`]: an empty string is "unset" (`None`), matching every other
/// `Option<Bytes>` field in the model (`Resource::schema_url`, `Scope::schema_url`, `SpanExt`'s own
/// fields) where the wire's empty-string convention and the model's `None` convention agree.
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

/// `schema_url` is the wrapping `Resource*` message's own field (`ResourceLogs.schema_url` etc.),
/// not part of the inner `Resource` message itself -- see the module doc's "Nesting" note -- so it
/// arrives as a separate parameter rather than living on `resource`.
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

/// One `EventBatch`'s `Option<Arc<Scope>>` -> the one `InstrumentationScope` its request carries.
/// `None` (no OTLP-sourced scope at all) becomes an empty `InstrumentationScope` -- empty name,
/// nothing invented -- never a fabricated `"logit"`/version identity (`../mod.rs`'s own doc, and
/// `docs/adr/lossless-transit.md`'s retirement of that convention).
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

/// The mirror of [`scope_to_pb`]. `schema_url` is the wrapping `Scope*` message's own field
/// (`ScopeLogs.schema_url` etc.), same reasoning as [`pb_to_resource`]'s own `schema_url`
/// parameter.
/// `None` when the wire scope is entirely empty -- no `InstrumentationScope` message at all, or
/// one with an empty name/version, no attributes, and `dropped_attributes_count == 0` -- **and**
/// the wrapping `Scope*` message's own `schema_url` is also empty. An all-empty scope on the wire
/// is indistinguishable from "no scope was ever there" (both encode identically via
/// [`scope_to_pb`]/an empty `schema_url` string), so decode has to collapse them to the same
/// result: otherwise `EventBatch { scope: None, .. }` -- every statsd/syslog/native-sourced batch,
/// none of which ever had an OTLP scope to begin with -- would not be a decode/encode fixed point
/// (`docs/adr/lossless-transit.md`'s round-trip requirement).
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
        // The wrapping Scope* message's own schema_url is independent of whether an
        // InstrumentationScope message itself was present -- see pb_to_scope's own doc comment. A
        // present schema_url alone is enough to keep this from collapsing to None.
        let decoded = pb_to_scope(None, "https://example.com/schema")
            .expect("a present schema_url must not collapse to None");
        assert_eq!(decoded.name, Bytes::new());
        assert_eq!(decoded.version, Bytes::new());
        assert!(decoded.attributes.is_empty());
        assert_eq!(decoded.schema_url, Some(Bytes::from_static(b"https://example.com/schema")));
    }

    /// The fixed-point half of the same rule: a wire scope that is entirely empty -- no message
    /// at all, or one whose every field is the zero/empty value -- and an empty wrapping
    /// `schema_url` must decode to `None`, not `Some(Scope::default())`, or
    /// `EventBatch { scope: None, .. }` (every statsd/syslog/native-sourced batch) would not
    /// survive an OTLP decode/encode round trip.
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
