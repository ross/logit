//! OTLP/JSON decoding: `serde_json::Value` → the same generated `prost` structs `../mod.rs`'s
//! protobuf path decodes into, so every downstream rule (`../logs.rs`/`../traces.rs`/
//! `../metrics.rs`, and the `EventBatch` construction in `../mod.rs`) runs unchanged regardless of
//! which wire encoding a request arrived in. See [ADR `otlp-json-decoding`](../../../../../../docs/adr/otlp-json-decoding.md)
//! for why this is hand-written against `serde_json::Value` rather than generated (short version:
//! `pbjson` implements proto3 JSON's bytes-as-base64 rule, and OTLP's hex-encoded trace/span ids
//! are exactly where OTLP deviates from that rule -- a real `traceId` would base64-decode to the
//! wrong length).
//!
//! **This module doc is the dialect table**, the same role `../mod.rs`'s module doc plays for wire
//! types. Five rules, from the [OTLP spec](https://opentelemetry.io/docs/specs/otlp/):
//!
//! - **Field names** are lowerCamelCase (`traceId`, `startTimeUnixNano`). The spec: *"The keys of
//!   JSON objects are field names converted to lowerCamelCase. Original field names are not
//!   valid."* [`get`] accepts the original snake_case name too -- **deliberate leniency beyond the
//!   spec**, not an implementation of it, matching what the OTel Collector's own JSON unmarshaler
//!   does and what Postel's law argues for in a receiver.
//! - **`traceId`/`spanId`/`parentSpanId`** are case-insensitive hex strings, *not* base64 --
//!   OTLP's own documented deviation from proto3 JSON's normal bytes-as-base64 rule. Every other
//!   `bytes` field (`AnyValue.bytesValue`) follows proto3 JSON and *is* base64. See [`hex_bytes`]
//!   vs [`base64_bytes`].
//! - **64-bit integers** (`timeUnixNano`, `startTimeUnixNano`, `asInt`, ...) may be a JSON number
//!   or a decimal string -- proto3 JSON's own rule for 64-bit fields, since not every JSON parser
//!   preserves 64-bit integer precision. See [`u64_field`]/[`i64_field`].
//! - **Enums** (`SpanKind`, `StatusCode`, `SeverityNumber`, `AggregationTemporality`) are the
//!   integer value on the wire per spec (*"only integer enum values are allowed in OTLP JSON
//!   Protobuf Encoding; the enum name strings MUST NOT be used"*). [`enum_field`] accepts the
//!   proto enum name too (e.g. `"SPAN_KIND_SERVER"`) -- again leniency beyond the spec, for the
//!   same reason snake_case keys are accepted.
//! - **An explicit `null` is the same as an absent key** -- proto3 JSON's own rule (*"null is accepted
//!   and treated as the default value"*), implemented once in [`get`] rather than at each field. This
//!   is what makes `{"parentSpanId": null}` on a root span decode identically to the protobuf encoding
//!   of that same span, which simply doesn't emit the field: producers in any language where "unset"
//!   serializes as `null` rather than an omitted key are common, and a whole batch must not 400 over
//!   one of them. A `null` *element inside an array* is a different thing and is still an error -- see
//!   `metrics.rs`'s `u64_array`/`f64_array`.
//!
//! **`ExportTraceServiceRequest`/`ExportLogsServiceRequest`/`ExportMetricsServiceRequest` decode
//! the same as `TracesData`/`LogsData`/`MetricsData`.** Both message shapes have exactly one field
//! -- `repeated ResourceSpans resource_spans = 1` and its log/metric equivalents -- and since this
//! layer keys off field *names* rather than protobuf tag numbers, the top-level key
//! (`resourceSpans`/`resourceLogs`/`resourceMetrics`) is the same for either. One parser reads
//! both; see `../mod.rs`'s "Wire types" paragraph for the protobuf-side version of this fact.
//!
//! **Unknown keys are silently ignored** -- forward compatibility with a newer OTLP minor version
//! adding a field must not fail an entire batch.
//!
//! **`exemplars` is parsed nowhere in this module and always encodes empty.** `../metrics.rs`'s
//! `decode_metric` never reads a data point's `exemplars` field (every read there is `Vec::new()`
//! on the *encode* side only) -- so an exemplar array in the input is dropped before it would ever
//! be looked at, the same way protobuf's own exemplar bytes would be decoded into a `Vec<Exemplar>`
//! that `decode_metric` never touches either. Extending this module to preserve exemplars would
//! need `decode_metric` to grow a reason to read them first.

mod logs;
mod metrics;
mod traces;

pub(crate) use logs::logs_data;
pub(crate) use metrics::metrics_data;
pub(crate) use traces::traces_data;

use crate::otlp::generated::opentelemetry::proto::common::v1 as pb;
use crate::otlp::generated::opentelemetry::proto::resource::v1 as respb;
use crate::CodecError;
use base64::Engine;
use serde_json::{Map, Value as JsonValue};

type JsonMap = Map<String, JsonValue>;

fn malformed(msg: impl Into<String>) -> CodecError {
    CodecError::Malformed(msg.into())
}

/// The two-name lookup every field read in this module goes through -- see the module doc's
/// leniency note, and its `null` rule: **an explicit `null` is reported as absent here, once**, so
/// that neither an accessor below nor a hand-written call site has to remember proto3 JSON's "a
/// `null` means the field's default" rule. Folding it in here rather than at each call site is what
/// makes the rule unbypassable: a producer that serializes "unset" as `null` rather than omitting
/// the key (Python's `json.dumps` of a `None`, a Go pointer field, a JS `undefined`-turned-`null`)
/// gets the same decode as one that omits it, and as the protobuf encoding of the same message,
/// which simply doesn't emit the field.
fn get<'a>(obj: &'a JsonMap, camel: &str, snake: &str) -> Option<&'a JsonValue> {
    obj.get(camel).or_else(|| obj.get(snake)).filter(|v| !v.is_null())
}

fn require_object<'a>(v: &'a JsonValue, what: &str) -> Result<&'a JsonMap, CodecError> {
    v.as_object().ok_or_else(|| malformed(format!("{what} must be a JSON object")))
}

fn object_field<'a>(
    obj: &'a JsonMap,
    camel: &str,
    snake: &str,
) -> Result<Option<&'a JsonMap>, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(None),
        Some(JsonValue::Object(o)) => Ok(Some(o)),
        Some(_) => Err(malformed(format!("{camel} must be a JSON object"))),
    }
}

/// Missing/null is `&[]`, not an error -- a repeated field's default is the empty list.
fn array_field<'a>(
    obj: &'a JsonMap,
    camel: &str,
    snake: &str,
) -> Result<&'a [JsonValue], CodecError> {
    match get(obj, camel, snake) {
        None => Ok(&[]),
        Some(JsonValue::Array(a)) => Ok(a.as_slice()),
        Some(_) => Err(malformed(format!("{camel} must be an array"))),
    }
}

fn str_field(obj: &JsonMap, camel: &str, snake: &str) -> Result<String, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(String::new()),
        Some(JsonValue::String(s)) => Ok(s.clone()),
        Some(_) => Err(malformed(format!("{camel} must be a string"))),
    }
}

fn bool_field(obj: &JsonMap, camel: &str, snake: &str) -> Result<bool, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(false),
        Some(JsonValue::Bool(b)) => Ok(*b),
        Some(_) => Err(malformed(format!("{camel} must be a boolean"))),
    }
}

/// A 64-bit unsigned field: a JSON number, or (proto3 JSON's own rule for 64-bit fields) a decimal
/// string.
fn parse_u64(v: &JsonValue, field: &str) -> Result<u64, CodecError> {
    match v {
        JsonValue::Number(n) => n
            .as_u64()
            .ok_or_else(|| malformed(format!("{field} must be a non-negative integer, got {n}"))),
        JsonValue::String(s) => s
            .parse::<u64>()
            .map_err(|_| malformed(format!("{field} must be a decimal integer string, got {s:?}"))),
        other => Err(malformed(format!("{field} must be a number or numeric string, got {other}"))),
    }
}

fn u64_field(obj: &JsonMap, camel: &str, snake: &str) -> Result<u64, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(0),
        Some(v) => parse_u64(v, camel),
    }
}

fn u32_field(obj: &JsonMap, camel: &str, snake: &str) -> Result<u32, CodecError> {
    let v = u64_field(obj, camel, snake)?;
    u32::try_from(v).map_err(|_| malformed(format!("{camel} must fit in 32 bits, got {v}")))
}

fn parse_i64(v: &JsonValue, field: &str) -> Result<i64, CodecError> {
    match v {
        JsonValue::Number(n) => n
            .as_i64()
            .ok_or_else(|| malformed(format!("{field} must be a 64-bit integer, got {n}"))),
        JsonValue::String(s) => s
            .parse::<i64>()
            .map_err(|_| malformed(format!("{field} must be a decimal integer string, got {s:?}"))),
        other => Err(malformed(format!("{field} must be a number or numeric string, got {other}"))),
    }
}

fn i32_field(obj: &JsonMap, camel: &str, snake: &str) -> Result<i32, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(0),
        Some(v) => {
            let i = parse_i64(v, camel)?;
            i32::try_from(i).map_err(|_| malformed(format!("{camel} must fit in 32 bits, got {i}")))
        }
    }
}

/// A `double` field: a JSON number, or proto3 JSON's `"NaN"`/`"Infinity"`/`"-Infinity"` (the OTel
/// Collector's own `file` exporter emits these for a genuinely non-finite histogram sum).
fn parse_f64(v: &JsonValue, field: &str) -> Result<f64, CodecError> {
    match v {
        JsonValue::Number(n) => {
            n.as_f64().ok_or_else(|| malformed(format!("{field} is not a representable number")))
        }
        JsonValue::String(s) => match s.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => s.parse::<f64>().map_err(|_| {
                malformed(format!(
                    "{field} must be a number, or \"NaN\"/\"Infinity\"/\"-Infinity\", got {s:?}"
                ))
            }),
        },
        other => Err(malformed(format!("{field} must be a number, got {other}"))),
    }
}

fn f64_field(obj: &JsonMap, camel: &str, snake: &str) -> Result<f64, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(0.0),
        Some(v) => parse_f64(v, camel),
    }
}

/// The mirror of [`f64_field`] for the three OTLP fields that are genuinely optional
/// (`HistogramDataPoint`/`ExponentialHistogramDataPoint`'s `sum`/`min`/`max`) -- an absent key
/// must decode to `None`, not `Some(0.0)`, or the trailing-infinite-bucket reconstruction in
/// `../metrics.rs` gets a value it was never sent.
fn f64_field_opt(obj: &JsonMap, camel: &str, snake: &str) -> Result<Option<f64>, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(None),
        Some(v) => Ok(Some(parse_f64(v, camel)?)),
    }
}

/// An enum field: the integer value (spec-conformant), or -- leniency, see the module doc -- the
/// proto enum name via `from_str_name`, one of prost's own generated methods, so this never hand-
/// maintains a name table that could drift from `generated/`.
fn enum_field(
    obj: &JsonMap,
    camel: &str,
    snake: &str,
    from_str_name: impl Fn(&str) -> Option<i32>,
) -> Result<i32, CodecError> {
    match get(obj, camel, snake) {
        None => Ok(0),
        Some(JsonValue::Number(n)) => {
            let i = n.as_i64().ok_or_else(|| malformed(format!("{camel} must be an integer")))?;
            i32::try_from(i)
                .map_err(|_| malformed(format!("{camel}: enum value out of range: {i}")))
        }
        Some(JsonValue::String(s)) => from_str_name(s)
            .ok_or_else(|| malformed(format!("{camel}: unrecognized enum value {s:?}"))),
        Some(other) => Err(malformed(format!("{camel} must be a number or string, got {other}"))),
    }
}

/// Case-insensitive hex, no separators. An empty string decodes to an empty `Vec` unconditionally
/// (OTLP's "no parent"/"not associated with a trace" convention -- valid for `parentSpanId` and a
/// `Link`'s ids, and for a required id like `Span.traceId` the empty `Vec` this produces is
/// rejected downstream by `../traces.rs`'s `ids::trace_id`/`ids::span_id`, the same generic
/// wrong-length error the protobuf path already gives). A non-empty string must match
/// `expected_len` exactly. Never actually sees a JSON `null` -- every call site reads its argument
/// through [`get`], which already reports an explicit `null` as absent.
fn hex_bytes(v: &JsonValue, expected_len: usize, field: &str) -> Result<Vec<u8>, CodecError> {
    let s = v.as_str().ok_or_else(|| malformed(format!("{field} must be a hex string")))?;
    if s.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = hex_decode(s)
        .ok_or_else(|| malformed(format!("{field} must be case-insensitive hex, got {s:?}")))?;
    if bytes.len() != expected_len {
        return Err(malformed(format!(
            "{field} must be {expected_len} bytes, got {} ({s:?})",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Some(out)
}

/// Standard-alphabet base64 with padding -- proto3 JSON's own rule for a plain `bytes` field
/// (unlike a trace/span id, `AnyValue.bytesValue` does *not* deviate from it). The URL-safe
/// alphabet is a different, non-conforming choice some ad hoc producers make; accepting it
/// silently would hide exactly the kind of producer bug this decoder exists to surface.
fn base64_bytes(v: &JsonValue, field: &str) -> Result<Vec<u8>, CodecError> {
    let s = v.as_str().ok_or_else(|| malformed(format!("{field} must be a base64 string")))?;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| malformed(format!("{field}: invalid base64: {e}")))
}

/// One `{key, value}` object.
fn key_value(v: &JsonValue) -> Result<pb::KeyValue, CodecError> {
    let obj = require_object(v, "a KeyValue")?;
    let key = str_field(obj, "key", "key")?;
    let value = get(obj, "value", "value").map(any_value).transpose()?;
    Ok(pb::KeyValue { key, value, key_strindex: 0 })
}

/// A `repeated KeyValue` field, e.g. `attributes`.
fn key_values(obj: &JsonMap, camel: &str, snake: &str) -> Result<Vec<pb::KeyValue>, CodecError> {
    array_field(obj, camel, snake)?.iter().map(key_value).collect()
}

/// `AnyValue`'s oneof, dispatched by which single recognized key is present. `Null`/`{}` (no
/// recognized key) both produce OTLP's "empty" `AnyValue { value: None }`, matching
/// `common::any_value_to_value`'s treatment of the protobuf equivalent.
fn any_value(v: &JsonValue) -> Result<pb::AnyValue, CodecError> {
    use pb::any_value::Value as Any;
    let obj = match v {
        JsonValue::Null => return Ok(pb::AnyValue { value: None }),
        JsonValue::Object(o) => o,
        _ => return Err(malformed("an AnyValue must be a JSON object")),
    };
    if let Some(x) = get(obj, "stringValue", "string_value") {
        let s = x.as_str().ok_or_else(|| malformed("stringValue must be a string"))?;
        return Ok(pb::AnyValue { value: Some(Any::StringValue(s.to_string())) });
    }
    if let Some(x) = get(obj, "boolValue", "bool_value") {
        let b = x.as_bool().ok_or_else(|| malformed("boolValue must be a boolean"))?;
        return Ok(pb::AnyValue { value: Some(Any::BoolValue(b)) });
    }
    if let Some(x) = get(obj, "intValue", "int_value") {
        return Ok(pb::AnyValue { value: Some(Any::IntValue(parse_i64(x, "intValue")?)) });
    }
    if let Some(x) = get(obj, "doubleValue", "double_value") {
        return Ok(pb::AnyValue { value: Some(Any::DoubleValue(parse_f64(x, "doubleValue")?)) });
    }
    if let Some(x) = get(obj, "arrayValue", "array_value") {
        let inner = require_object(x, "arrayValue")?;
        let values = array_field(inner, "values", "values")?
            .iter()
            .map(any_value)
            .collect::<Result<_, _>>()?;
        return Ok(pb::AnyValue { value: Some(Any::ArrayValue(pb::ArrayValue { values })) });
    }
    if let Some(x) = get(obj, "kvlistValue", "kvlist_value") {
        let inner = require_object(x, "kvlistValue")?;
        let values = key_values(inner, "values", "values")?;
        return Ok(pb::AnyValue { value: Some(Any::KvlistValue(pb::KeyValueList { values })) });
    }
    if let Some(x) = get(obj, "bytesValue", "bytes_value") {
        return Ok(pb::AnyValue { value: Some(Any::BytesValue(base64_bytes(x, "bytesValue")?)) });
    }
    Ok(pb::AnyValue { value: None })
}

/// `resource`'s `dropped_attributes_count`/`entity_refs` are parsed nowhere here -- `../common.rs`'s
/// `pb_to_resource` (the function every signal's decode path feeds this through) never reads
/// either field, protobuf or JSON.
fn resource(obj: &JsonMap) -> Result<respb::Resource, CodecError> {
    Ok(respb::Resource {
        attributes: key_values(obj, "attributes", "attributes")?,
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    })
}

fn instrumentation_scope(
    v: Option<&JsonValue>,
) -> Result<Option<pb::InstrumentationScope>, CodecError> {
    let obj = match v {
        None | Some(JsonValue::Null) => return Ok(None),
        Some(JsonValue::Object(o)) => o,
        Some(_) => return Err(malformed("scope must be a JSON object")),
    };
    Ok(Some(pb::InstrumentationScope {
        name: str_field(obj, "name", "name")?,
        version: str_field(obj, "version", "version")?,
        attributes: key_values(obj, "attributes", "attributes")?,
        dropped_attributes_count: 0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obj(json: &str) -> JsonValue {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn a_64_bit_field_accepts_both_a_json_string_and_a_number() {
        let as_number = obj(r#"{"timeUnixNano": 42}"#);
        let as_string = obj(r#"{"timeUnixNano": "42"}"#);
        assert_eq!(
            u64_field(as_number.as_object().unwrap(), "timeUnixNano", "time_unix_nano").unwrap(),
            42
        );
        assert_eq!(
            u64_field(as_string.as_object().unwrap(), "timeUnixNano", "time_unix_nano").unwrap(),
            42
        );
    }

    #[test]
    fn an_enum_accepts_both_its_name_and_its_number() {
        use crate::otlp::generated::opentelemetry::proto::trace::v1::span::SpanKind;
        let from_name = |s: &str| SpanKind::from_str_name(s).map(|k| k as i32);
        let as_number = obj(r#"{"kind": 2}"#);
        let as_name = obj(r#"{"kind": "SPAN_KIND_SERVER"}"#);
        assert_eq!(
            enum_field(as_number.as_object().unwrap(), "kind", "kind", from_name).unwrap(),
            2
        );
        assert_eq!(enum_field(as_name.as_object().unwrap(), "kind", "kind", from_name).unwrap(), 2);
    }

    #[test]
    fn snake_case_field_names_are_accepted_alongside_lower_camel_case() {
        let v = obj(r#"{"start_time_unix_nano": 7}"#);
        assert_eq!(
            u64_field(v.as_object().unwrap(), "startTimeUnixNano", "start_time_unix_nano").unwrap(),
            7
        );
    }

    #[test]
    fn an_unknown_key_is_ignored_not_an_error() {
        let v = obj(r#"{"name": "hi", "somethingFromANewerOtlp": {"nested": true}}"#);
        assert_eq!(str_field(v.as_object().unwrap(), "name", "name").unwrap(), "hi");
    }

    #[test]
    fn an_any_value_round_trips_every_oneof_arm() {
        let cases = [
            (r#"{"stringValue": "hi"}"#, "hi string"),
            (r#"{"boolValue": true}"#, "bool"),
            (r#"{"intValue": "42"}"#, "int as string"),
            (r#"{"doubleValue": 3.5}"#, "double"),
        ];
        for (json, label) in cases {
            let v = obj(json);
            assert!(any_value(&v).is_ok(), "{label} should decode");
        }
        let array = obj(r#"{"arrayValue": {"values": [{"intValue": 1}, {"intValue": 2}]}}"#);
        let decoded = any_value(&array).unwrap();
        match decoded.value {
            Some(pb::any_value::Value::ArrayValue(a)) => assert_eq!(a.values.len(), 2),
            other => panic!("expected ArrayValue, got {other:?}"),
        }
    }

    #[test]
    fn a_null_any_value_oneof_arm_decodes_as_an_empty_any_value() {
        for json in [
            r#"{"stringValue": null}"#,
            r#"{"boolValue": null}"#,
            r#"{"intValue": null}"#,
            r#"{"doubleValue": null}"#,
            r#"{"arrayValue": null}"#,
            r#"{"kvlistValue": null}"#,
            r#"{"bytesValue": null}"#,
        ] {
            let v = obj(json);
            let decoded = any_value(&v).unwrap_or_else(|e| panic!("{json} should decode, got {e}"));
            assert_eq!(decoded.value, None, "{json} should decode as an empty AnyValue");
        }
    }

    #[test]
    fn a_null_attribute_value_decodes_as_an_empty_any_value() {
        let v = obj(r#"{"key": "k", "value": null}"#);
        let kv = key_value(&v).expect("a null attribute value must not fail");
        assert_eq!(kv.key, "k");
        assert_eq!(kv.value, None);
    }

    #[test]
    fn a_trace_id_is_hex_decoded_not_base64_decoded() {
        // 32 hex characters -- a real 16-byte OTLP traceId, exactly what a browser SDK sends.
        let hex_id = "01010101010101010101010101010101010101010101010101010101010101"; // 64 chars
        let hex_id = &hex_id[..32];
        let v = JsonValue::String(hex_id.to_string());

        let decoded = hex_bytes(&v, 16, "traceId").unwrap();
        assert_eq!(decoded, vec![0x01u8; 16], "a hex traceId must decode via hex, not base64");

        // The permanent regression guard for the `pbjson` finding in the module doc: a `bytes`
        // field's proto3-JSON-default base64 reading of this same string does NOT agree with the
        // hex reading -- 32 base64 characters decode to 24 bytes, not 16. If this ever started
        // agreeing, `hex_bytes` would have silently become a base64 decoder.
        let as_base64_len =
            base64::engine::general_purpose::STANDARD.decode(hex_id).map(|b| b.len()).unwrap_or(0);
        assert_ne!(
            decoded.len(),
            as_base64_len,
            "hex and base64 readings of a traceId must disagree"
        );
    }
}
