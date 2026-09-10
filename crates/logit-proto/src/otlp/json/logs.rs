//! `LogsData` from OTLP/JSON. See `super`'s module doc for the dialect rules this leans on.

use super::{
    any_value, array_field, enum_field, get, hex_bytes, instrumentation_scope, key_values,
    malformed, object_field, require_object, resource, str_field, u32_field, u64_field, JsonValue,
};
use crate::otlp::generated::opentelemetry::proto::logs::v1 as pb;
use crate::CodecError;

pub(crate) fn logs_data(bytes: &[u8]) -> Result<pb::LogsData, CodecError> {
    let root: JsonValue = serde_json::from_slice(bytes).map_err(|e| malformed(e.to_string()))?;
    let obj = require_object(&root, "the request body")?;
    let resource_logs = array_field(obj, "resourceLogs", "resource_logs")?
        .iter()
        .map(resource_logs)
        .collect::<Result<_, _>>()?;
    Ok(pb::LogsData { resource_logs })
}

fn resource_logs(v: &JsonValue) -> Result<pb::ResourceLogs, CodecError> {
    let obj = require_object(v, "a resourceLogs entry")?;
    let resource = match object_field(obj, "resource", "resource")? {
        Some(r) => Some(resource(r)?),
        None => None,
    };
    let scope_logs = array_field(obj, "scopeLogs", "scope_logs")?
        .iter()
        .map(scope_logs)
        .collect::<Result<_, _>>()?;
    Ok(pb::ResourceLogs {
        resource,
        scope_logs,
        schema_url: str_field(obj, "schemaUrl", "schema_url")?,
    })
}

fn scope_logs(v: &JsonValue) -> Result<pb::ScopeLogs, CodecError> {
    let obj = require_object(v, "a scopeLogs entry")?;
    let scope = instrumentation_scope(get(obj, "scope", "scope"))?;
    let log_records = array_field(obj, "logRecords", "log_records")?
        .iter()
        .map(log_record)
        .collect::<Result<_, _>>()?;
    Ok(pb::ScopeLogs { scope, log_records, schema_url: str_field(obj, "schemaUrl", "schema_url")? })
}

fn log_record(v: &JsonValue) -> Result<pb::LogRecord, CodecError> {
    let obj = require_object(v, "a logRecord")?;
    let trace_id = match get(obj, "traceId", "trace_id") {
        Some(x) => hex_bytes(x, 16, "traceId")?,
        None => Vec::new(),
    };
    let span_id = match get(obj, "spanId", "span_id") {
        Some(x) => hex_bytes(x, 8, "spanId")?,
        None => Vec::new(),
    };
    let body = match get(obj, "body", "body") {
        None | Some(JsonValue::Null) => None,
        Some(v) => Some(any_value(v)?),
    };
    Ok(pb::LogRecord {
        time_unix_nano: u64_field(obj, "timeUnixNano", "time_unix_nano")?,
        observed_time_unix_nano: u64_field(obj, "observedTimeUnixNano", "observed_time_unix_nano")?,
        severity_number: enum_field(obj, "severityNumber", "severity_number", |s| {
            pb::SeverityNumber::from_str_name(s).map(|n| n as i32)
        })?,
        severity_text: str_field(obj, "severityText", "severity_text")?,
        body,
        attributes: key_values(obj, "attributes", "attributes")?,
        dropped_attributes_count: 0,
        flags: u32_field(obj, "flags", "flags")?,
        trace_id,
        span_id,
        event_name: str_field(obj, "eventName", "event_name")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_minimal_log_record_decodes() {
        let json = br#"{
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "1000",
                        "severityNumber": "SEVERITY_NUMBER_INFO",
                        "body": {"stringValue": "hi"}
                    }]
                }]
            }]
        }"#;
        let data = logs_data(json).expect("should decode");
        let record = &data.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(record.time_unix_nano, 1000);
        assert_eq!(record.severity_number, pb::SeverityNumber::Info as i32);
        assert_eq!(record.body.as_ref().unwrap().value, Some(
            crate::otlp::generated::opentelemetry::proto::common::v1::any_value::Value::StringValue("hi".to_string())
        ));
    }

    #[test]
    fn an_absent_time_unix_nano_falls_back_to_observed_time_unix_nano_downstream() {
        // This module doesn't apply the fallback itself -- `../logs.rs`'s `decode_log_record`
        // does, on the decoded `pb::LogRecord` this produces. Verify only that a genuinely absent
        // `timeUnixNano` decodes to the wire's own "unknown" sentinel (0), not an error or a
        // fabricated value, so that downstream fallback has something correct to work with.
        let json = br#"{"resourceLogs": [{"scopeLogs": [{"logRecords": [{
            "observedTimeUnixNano": "4200"
        }]}]}]}"#;
        let data = logs_data(json).expect("should decode");
        let record = &data.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(record.time_unix_nano, 0);
        assert_eq!(record.observed_time_unix_nano, 4200);
    }
}
