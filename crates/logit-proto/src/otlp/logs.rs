//! `Event`/`LogRecord` ↔ OTLP `LogRecord`.
//!
//! **`Severity` ↔ `SeverityNumber`.** Encode: each band's base value (`Trace`→1, `Debug`→5,
//! `Info`→9, `Warn`→13, `Error`→17, `Fatal`→21; `log.severity == None` leaves `severity_number`
//! unset at `SEVERITY_NUMBER_UNSPECIFIED`/`0`), with `severity_text` set to the variant's name --
//! **unless** the event carries an `otel.severity_number`/`otel.severity_text` attribute (see
//! below), each of which independently overrides the band-derived value for that one field. Decode
//! prefers `severity_number`'s band (any of the 4 numbers in a band -- e.g. `TRACE2..TRACE4` -- map
//! to that band's `Severity`), falls back to a case-insensitive `severity_text` match when the
//! number is unspecified or out of range, else `None`.
//!
//! **Raw severity survives alongside the normalized `Severity`**, the same shape `syslog_in`/
//! `syslog_out` already use for `syslog.severity` (`docs/adr/syslog-output.md`'s "Header-field
//! precedence", generalized by `docs/adr/lossless-transit.md` rule (b)): OTLP's 24 raw severity
//! numbers collapse onto this model's 6-variant `Severity` on decode, which is lossy by
//! construction (`INFO2` and `INFO4` both decode to `Severity::Info`). Decode stamps
//! `otel.severity_number` (`Value::I64`, the raw `1..=24`, only when non-zero) and
//! `otel.severity_text` (`Value::Str`, raw, only when non-empty) on the event's own attributes
//! alongside the normalized `Severity`; encode prefers each of those two attributes over the
//! band-derived value when present -- consumed (removed from the emitted attribute set) the same
//! way `traces.rs` handles its own retired status-message attribute convention, so neither raw
//! severity attribute leaks into every other sink's tag set.
//! A log with no OTLP-sourced severity attributes (built by `kv_metrics`, a Lua script, `json`, ...)
//! still encodes the band base + variant name exactly as before.
//!
//! **`BodyFormat` has no OTLP field.** It round-trips through a `logit.body_format` attribute
//! (`"raw" | "json" | "structured"`), inserted on encode and consumed (removed) on decode -- the
//! same "reserved key rides as an attribute" idiom this module now uses for severity too.
//!
//! **`time_unix_nano == 0` falls back to `observed_time_unix_nano`,** per OTLP's own contract for
//! a consumer that (like this one) keeps a single timestamp: `time_unix_nano == 0` means "unknown
//! or missing" (`logs.proto`'s own doc comment), not literally the Unix epoch -- a real collector
//! commonly sends exactly that when the original event time wasn't available, and treating it as
//! epoch would silently corrupt ordering and could push the record outside a downstream retention
//! window. If `observed_time_unix_nano` is also `0`, `Event::timestamp` is `0`; there is no third
//! fallback to reach for.
//!
//! **`observed_time_unix_nano` ↔ `LogRecord::observed_timestamp`, preserved both ways.** Decode
//! copies the wire field verbatim (`0` stays `0`, this model's own "unset" convention -- the same
//! sentinel OTLP itself uses for the field). Encode prefers `log.observed_timestamp` when it is
//! non-zero -- what makes `otlp_in -> otlp_out` a fixed point on this field -- and falls back to
//! the current wall clock (`crate::now_nanos()`) only when it's `0`/unset: OTLP's own definition of
//! the field (`logs.proto`: "Time when the event was observed by the collection system"), and at
//! encode time `logit` *is* that collection system when nothing upstream already set one. This is
//! what makes the `time_unix_nano` fallback above do real work end to end even for a record not
//! sourced from OTLP: a `syslog_in` event with no parseable timestamp carries `Event::timestamp ==
//! 0` and `LogRecord::observed_timestamp == 0`, and exports as `time_unix_nano: 0,
//! observed_time_unix_nano: <now>`, so a downstream consumer -- `logit`'s own `otlp_in` included --
//! recovers a sane timestamp instead of the Unix epoch. Encode is non-deterministic only on the
//! path where `observed_timestamp` is `0` -- no test may assert whole-record equality against a
//! fixed expected value in that case; when `observed_timestamp` is set, the test asserts the wire
//! value equals it exactly.
//!
//! **`trace_id`/`span_id`/`flags` map to `LogRecord::trace` (`Option<TraceRef>`),** not dropped
//! any more (`docs/adr/log-record-trace-context.md`). **Decode is lenient, unlike a `Span`'s ids**
//! (`traces.rs`'s `mod ids`, which rejects a wrong-length id outright): `logs.proto`'s own
//! contract is "receivers SHOULD assume the log record is not associated with a trace" if
//! `trace_id` is absent or invalid, so `TraceRef::from_bytes` degrades a malformed id to `None`
//! rather than failing the whole record -- correlation metadata a log can do without, unlike a
//! `Span`'s own identity. **Encode falls back to the event's `span`** when the log has no trace
//! context of its own: an `Event` carrying both a `log` and a `span` (still `logit`'s primary
//! correlation mechanism -- one `Event`, both payloads) exports the span's `trace_id`/`span_id`
//! onto the `LogRecord` too, `flags: 0`. This is *not* a round-trip fixed point: `encode_signals`
//! splits such an `Event` into a separate `LogRecord` and `Span`, and decode makes one `Event` per
//! record, so the log comes back as its own event, now carrying `trace: Some(..)` where it had
//! `None` going in -- an enrichment, not a lossless mirror.
//!
//! **`event_name` ↔ `LogRecord::event_name`,** interned on decode when non-empty, resolved back to
//! a plain string on encode (empty when `None`).
//!
//! **`dropped_attributes_count` maps directly, both ways** -- no more special handling than any
//! other scalar field.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::logs::v1 as pb;
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, BodyFormat, Event, LogRecord, Severity, TraceRef, Value};

fn severity_number(sev: Severity) -> i32 {
    let n = match sev {
        Severity::Trace => pb::SeverityNumber::Trace,
        Severity::Debug => pb::SeverityNumber::Debug,
        Severity::Info => pb::SeverityNumber::Info,
        Severity::Warn => pb::SeverityNumber::Warn,
        Severity::Error => pb::SeverityNumber::Error,
        Severity::Fatal => pb::SeverityNumber::Fatal,
    };
    n as i32
}

/// The severity_text `logit` writes on encode: the variant's own name, matching every other
/// `{:?}`-derived tag value convention in this codebase.
fn severity_text(sev: Severity) -> &'static str {
    match sev {
        Severity::Trace => "Trace",
        Severity::Debug => "Debug",
        Severity::Info => "Info",
        Severity::Warn => "Warn",
        Severity::Error => "Error",
        Severity::Fatal => "Fatal",
    }
}

/// See the module doc's `Severity ↔ SeverityNumber` section.
fn decode_severity(number: i32, text: &str) -> Option<Severity> {
    match number {
        1..=4 => return Some(Severity::Trace),
        5..=8 => return Some(Severity::Debug),
        9..=12 => return Some(Severity::Info),
        13..=16 => return Some(Severity::Warn),
        17..=20 => return Some(Severity::Error),
        21..=24 => return Some(Severity::Fatal),
        _ => {}
    }
    match text.to_ascii_lowercase().as_str() {
        "trace" => Some(Severity::Trace),
        "debug" => Some(Severity::Debug),
        "info" => Some(Severity::Info),
        "warn" => Some(Severity::Warn),
        "error" => Some(Severity::Error),
        "fatal" => Some(Severity::Fatal),
        _ => None,
    }
}

fn body_format_str(format: BodyFormat) -> &'static str {
    match format {
        BodyFormat::Raw => "raw",
        BodyFormat::Json => "json",
        BodyFormat::Structured => "structured",
    }
}

/// Reads and removes `logit.body_format` from `attrs`, defaulting to `Raw` when absent or
/// unrecognized (e.g. a peer OTLP producer that never set it).
fn decode_body_format(attrs: &mut AttrMap) -> BodyFormat {
    let format = match attrs.get("logit.body_format").and_then(|v| v.as_str()) {
        Some("json") => BodyFormat::Json,
        Some("structured") => BodyFormat::Structured,
        _ => BodyFormat::Raw,
    };
    attrs.remove("logit.body_format");
    format
}

/// `log.trace`, or -- when the log has none of its own -- `event.span`'s ids with `flags: 0`.
/// See the module doc's trace-context paragraph for why this fallback exists and why it isn't a
/// round-trip fixed point.
fn encode_trace(event: &Event, log: &LogRecord) -> (Vec<u8>, Vec<u8>, u32) {
    if let Some(trace) = log.trace {
        let span_id = trace.span_id.map(|id| id.to_vec()).unwrap_or_default();
        return (trace.trace_id.to_vec(), span_id, trace.flags as u32);
    }
    if let Some(span) = &event.span {
        return (span.trace_id.to_vec(), span.span_id.to_vec(), 0);
    }
    (Vec::new(), Vec::new(), 0)
}

/// Reads and removes `otel.severity_number`/`otel.severity_text` from `attrs` (`Value::I64`/
/// `Value::Str` respectively, per the module doc's severity precedence) -- consumed the same way
/// `decode_body_format` consumes `logit.body_format`, so neither leaks into the emitted attribute
/// set as a plain attribute too.
fn take_severity_attrs(attrs: &mut AttrMap) -> (Option<i32>, Option<String>) {
    let number = attrs.remove("otel.severity_number").and_then(|v| match v {
        Value::I64(i) => Some(i as i32),
        _ => None,
    });
    let text = attrs.remove("otel.severity_text").and_then(|v| v.as_str().map(str::to_string));
    (number, text)
}

pub(crate) fn encode_log_record(event: &Event, log: &LogRecord) -> pb::LogRecord {
    let mut attrs = event.attributes.clone();
    let (severity_number_attr, severity_text_attr) = take_severity_attrs(&mut attrs);

    let mut attributes = common::attrs_to_key_values(&attrs);
    attributes.push(crate::otlp::generated::opentelemetry::proto::common::v1::KeyValue {
        key: "logit.body_format".to_string(),
        value: Some(common::value_to_any_value(&Value::str(body_format_str(log.body_format)))),
        key_strindex: 0,
    });

    // Each of the two raw-severity attributes independently overrides the band-derived value for
    // its own field -- see the module doc's severity section.
    let number = severity_number_attr.unwrap_or_else(|| match log.severity {
        Some(sev) => severity_number(sev),
        None => pb::SeverityNumber::Unspecified as i32,
    });
    let text = severity_text_attr.unwrap_or_else(|| match log.severity {
        Some(sev) => severity_text(sev).to_string(),
        None => String::new(),
    });

    let (trace_id, span_id, flags) = encode_trace(event, log);

    let observed_time_unix_nano = if log.observed_timestamp != 0 {
        log.observed_timestamp.max(0) as u64
    } else {
        crate::now_nanos().max(0) as u64
    };

    pb::LogRecord {
        time_unix_nano: event.timestamp.max(0) as u64,
        observed_time_unix_nano,
        severity_number: number,
        severity_text: text,
        body: Some(common::value_to_any_value(&log.message)),
        attributes,
        dropped_attributes_count: log.dropped_attributes_count,
        flags,
        trace_id,
        span_id,
        event_name: log.event_name.map(resolve).unwrap_or_default().to_string(),
    }
}

/// `base_attrs` is the resource-level base (`common`'s own doc), cloned once per record by the
/// caller -- this only layers the record's own attributes on top.
pub(crate) fn decode_log_record(record: pb::LogRecord, mut attrs: AttrMap) -> Event {
    common::key_values_into_attrs(record.attributes, &mut attrs);
    let body_format = decode_body_format(&mut attrs);
    let severity = decode_severity(record.severity_number, &record.severity_text);
    // Raw severity survives alongside the normalized Severity -- see the module doc.
    if record.severity_number != 0 {
        attrs.insert("otel.severity_number", Value::I64(record.severity_number as i64));
    }
    if !record.severity_text.is_empty() {
        attrs.insert("otel.severity_text", record.severity_text.as_str());
    }
    let message = record.body.map(common::any_value_to_value).unwrap_or(Value::Null);
    // See the module doc: 0 means "unknown", not literally the epoch -- prefer the observed time
    // over silently treating a missing original timestamp as 1970-01-01.
    let timestamp = if record.time_unix_nano != 0 {
        record.time_unix_nano as i64
    } else {
        record.observed_time_unix_nano as i64
    };
    // `LOG_RECORD_FLAGS_TRACE_FLAGS_MASK`: the low 8 bits of the `fixed32` are the W3C trace
    // flags; the rest is reserved. Lenient by construction -- see the module doc.
    let trace =
        TraceRef::from_bytes(&record.trace_id, &record.span_id, (record.flags & 0xFF) as u8);
    let event_name =
        if record.event_name.is_empty() { None } else { Some(intern(&record.event_name)) };
    Event::log(
        timestamp,
        attrs,
        LogRecord {
            message,
            severity,
            body_format,
            trace,
            event_name,
            observed_timestamp: record.observed_time_unix_nano as i64,
            dropped_attributes_count: record.dropped_attributes_count,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::SpanRecord;

    #[test]
    fn every_severity_encodes_to_the_base_of_its_otlp_band() {
        let cases = [
            (Severity::Trace, pb::SeverityNumber::Trace as i32),
            (Severity::Debug, pb::SeverityNumber::Debug as i32),
            (Severity::Info, pb::SeverityNumber::Info as i32),
            (Severity::Warn, pb::SeverityNumber::Warn as i32),
            (Severity::Error, pb::SeverityNumber::Error as i32),
            (Severity::Fatal, pb::SeverityNumber::Fatal as i32),
        ];
        for (sev, expected) in cases {
            assert_eq!(
                severity_number(sev),
                expected,
                "{sev:?} should encode to its band's base value"
            );
        }
    }

    #[test]
    fn a_severity_number_inside_a_band_decodes_to_that_bands_severity() {
        // TRACE3 (3), DEBUG4 (8), INFO2 (10), WARN3 (15), ERROR4 (20), FATAL2 (22).
        assert_eq!(decode_severity(3, ""), Some(Severity::Trace));
        assert_eq!(decode_severity(8, ""), Some(Severity::Debug));
        assert_eq!(decode_severity(10, ""), Some(Severity::Info));
        assert_eq!(decode_severity(15, ""), Some(Severity::Warn));
        assert_eq!(decode_severity(20, ""), Some(Severity::Error));
        assert_eq!(decode_severity(22, ""), Some(Severity::Fatal));
    }

    #[test]
    fn severity_text_is_used_when_the_number_is_unspecified() {
        assert_eq!(decode_severity(0, "Warn"), Some(Severity::Warn));
        assert_eq!(decode_severity(0, "WARN"), Some(Severity::Warn), "should be case-insensitive");
        assert_eq!(decode_severity(0, "not-a-severity"), None);
        assert_eq!(decode_severity(0, ""), None);
    }

    #[test]
    fn body_format_survives_a_full_round_trip() {
        for format in [BodyFormat::Raw, BodyFormat::Json, BodyFormat::Structured] {
            // `timestamp: 0` here means `decoded.timestamp` comes back as a real wall-clock value
            // (the encoder's own `observed_time_unix_nano` stamp, decode's fallback for a `0`
            // `time_unix_nano`), not the `0` it would have been before `encode_log_record` started
            // stamping observed time -- this test doesn't assert on `timestamp` so that's not a
            // regression, just worth knowing if a future reader goes looking for why it changed.
            let event = Event::log(
                0,
                AttrMap::new(),
                LogRecord {
                    message: Value::str("hi"),
                    severity: None,
                    body_format: format,
                    trace: None,
                    event_name: None,
                    observed_timestamp: 0,
                    dropped_attributes_count: 0,
                },
            );
            let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
            let decoded = decode_log_record(encoded, AttrMap::new());
            assert_eq!(
                decoded.log.unwrap().body_format,
                format,
                "body_format {format:?} should survive an encode/decode round trip"
            );
        }
    }

    #[test]
    fn encode_stamps_a_nonzero_observed_time_unix_nano() {
        let before = crate::now_nanos();
        let event = Event::log(
            123,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        assert!(
            encoded.observed_time_unix_nano as i64 >= before,
            "observed_time_unix_nano should be stamped with the current wall clock, not left at 0"
        );
    }

    /// The fidelity half of the same field: when `observed_timestamp` is already set, encode
    /// preserves it exactly rather than overwriting it with the current wall clock -- what makes
    /// `otlp_in -> otlp_out` a fixed point on this field.
    #[test]
    fn encode_prefers_a_nonzero_observed_timestamp_over_the_current_wall_clock() {
        let event = Event::log(
            123,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 1_700_000_000_500_000_000,
                dropped_attributes_count: 0,
            },
        );
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        assert_eq!(encoded.observed_time_unix_nano, 1_700_000_000_500_000_000);
    }

    #[test]
    fn an_event_with_an_unknown_timestamp_round_trips_to_the_observed_time() {
        // The case `encode_log_record`'s module doc calls out: `Event::timestamp == 0` (OTLP's
        // own "unknown" sentinel) must not export as the literal Unix epoch -- the encoder's own
        // `observed_time_unix_nano` stamp, recovered by decode's existing fallback, is what makes
        // that true end to end through logit's own encoder now, not just against a hand-built
        // `pb::LogRecord` the way `a_zero_time_unix_nano_falls_back_to_observed_time_unix_nano`
        // above already proves for decode alone.
        let before = crate::now_nanos();
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        let decoded = decode_log_record(encoded, AttrMap::new());
        assert!(
            decoded.timestamp >= before,
            "an unknown source timestamp should decode back to roughly the encode-time wall \
             clock, not the Unix epoch"
        );
    }

    #[test]
    fn a_zero_time_unix_nano_falls_back_to_observed_time_unix_nano() {
        let record = pb::LogRecord {
            time_unix_nano: 0,
            observed_time_unix_nano: 4_200,
            severity_number: 0,
            severity_text: String::new(),
            body: None,
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: Vec::new(),
            span_id: Vec::new(),
            event_name: String::new(),
        };
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(
            decoded.timestamp, 4_200,
            "a missing time_unix_nano (0, OTLP's own 'unknown' sentinel) must fall back to \
             observed_time_unix_nano rather than becoming the literal Unix epoch"
        );
    }

    #[test]
    fn a_present_time_unix_nano_is_preferred_over_observed_time_unix_nano() {
        let record = pb::LogRecord {
            time_unix_nano: 100,
            observed_time_unix_nano: 4_200,
            severity_number: 0,
            severity_text: String::new(),
            body: None,
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: Vec::new(),
            span_id: Vec::new(),
            event_name: String::new(),
        };
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(decoded.timestamp, 100);
    }

    #[test]
    fn a_missing_severity_leaves_severity_number_unspecified_and_text_empty() {
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        assert_eq!(encoded.severity_number, pb::SeverityNumber::Unspecified as i32);
        assert_eq!(encoded.severity_text, "");
    }

    fn log_record(trace: Option<TraceRef>) -> pb::LogRecord {
        let event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        encode_log_record(&event, event.log.as_ref().unwrap())
    }

    #[test]
    fn a_trace_context_survives_a_full_round_trip() {
        let trace = TraceRef { trace_id: [7; 16], span_id: Some([8; 8]), flags: 1 };
        let encoded = log_record(Some(trace));
        assert_eq!(encoded.trace_id, [7; 16].to_vec());
        assert_eq!(encoded.span_id, [8; 8].to_vec());
        assert_eq!(encoded.flags, 1);

        let decoded = decode_log_record(encoded, AttrMap::new());
        assert_eq!(decoded.log.unwrap().trace, Some(trace));
    }

    #[test]
    fn a_trace_with_no_span_survives_a_full_round_trip() {
        let trace = TraceRef { trace_id: [7; 16], span_id: None, flags: 0 };
        let encoded = log_record(Some(trace));
        assert!(encoded.span_id.is_empty());

        let decoded = decode_log_record(encoded, AttrMap::new());
        assert_eq!(decoded.log.unwrap().trace, Some(trace));
    }

    #[test]
    fn no_trace_context_encodes_and_decodes_to_none() {
        let encoded = log_record(None);
        assert!(encoded.trace_id.is_empty());
        assert!(encoded.span_id.is_empty());
        assert_eq!(encoded.flags, 0);

        let decoded = decode_log_record(encoded, AttrMap::new());
        assert_eq!(decoded.log.unwrap().trace, None);
    }

    #[test]
    fn a_wrong_length_wire_trace_id_decodes_to_none_not_an_error() {
        let mut record = log_record(None);
        record.trace_id = vec![1; 15]; // one byte short of valid
        record.span_id = vec![2; 8];
        record.flags = 1;
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(
            decoded.log.unwrap().trace,
            None,
            "an invalid trace_id must degrade to None, per logs.proto -- never fail the record"
        );
    }

    #[test]
    fn an_all_zero_wire_trace_id_decodes_to_none() {
        let mut record = log_record(None);
        record.trace_id = vec![0; 16];
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(decoded.log.unwrap().trace, None);
    }

    #[test]
    fn a_valid_trace_with_an_invalid_span_keeps_the_trace_and_drops_the_span() {
        let mut record = log_record(None);
        record.trace_id = vec![1; 16];
        record.span_id = vec![0; 8]; // all-zero span id is invalid
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(
            decoded.log.unwrap().trace,
            Some(TraceRef { trace_id: [1; 16], span_id: None, flags: 0 })
        );
    }

    #[test]
    fn flags_are_masked_to_the_low_8_bits() {
        let mut record = log_record(None);
        record.trace_id = vec![1; 16];
        record.flags = 0x1FF;
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(decoded.log.unwrap().trace.unwrap().flags, 0xFF);
    }

    #[test]
    fn flags_on_an_invalid_trace_are_dropped_along_with_it() {
        let mut record = log_record(None);
        record.flags = 1; // no trace_id at all
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(decoded.log.unwrap().trace, None);
    }

    #[test]
    fn a_log_with_no_trace_falls_back_to_the_same_events_span() {
        let mut event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        event.span = Some(SpanRecord {
            trace_id: [3; 16],
            span_id: [4; 8],
            parent_span_id: None,
            name: Value::str("op"),
            kind: logit_core::SpanKind::Internal,
            status: logit_core::SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 0,
            flags: 0,
            ext: None,
        });
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        assert_eq!(encoded.trace_id, [3; 16].to_vec());
        assert_eq!(encoded.span_id, [4; 8].to_vec());
        assert_eq!(encoded.flags, 0, "span fallback carries no flags -- SpanRecord has none");
    }

    #[test]
    fn a_logs_own_trace_context_takes_priority_over_the_events_span() {
        let mut event = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hi"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: Some(TraceRef { trace_id: [9; 16], span_id: None, flags: 0 }),
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        event.span = Some(SpanRecord {
            trace_id: [3; 16],
            span_id: [4; 8],
            parent_span_id: None,
            name: Value::str("op"),
            kind: logit_core::SpanKind::Internal,
            status: logit_core::SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 0,
            flags: 0,
            ext: None,
        });
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        assert_eq!(
            encoded.trace_id,
            [9; 16].to_vec(),
            "the log's own trace must win, not the span's"
        );
        assert!(encoded.span_id.is_empty());
    }

    fn plain_log(fields: LogRecord) -> pb::LogRecord {
        let event = Event::log(1000, AttrMap::new(), fields);
        encode_log_record(&event, event.log.as_ref().unwrap())
    }

    #[test]
    fn event_name_round_trips_when_present_and_is_empty_when_absent() {
        let mut record = plain_log(LogRecord {
            message: Value::str("hi"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: Some(logit_core::interner::intern("request_finished")),
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        assert_eq!(record.event_name, "request_finished");
        let decoded = decode_log_record(record.clone(), AttrMap::new());
        assert_eq!(decoded.log.unwrap().event_name.map(resolve), Some("request_finished"));

        record.event_name = String::new();
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(decoded.log.unwrap().event_name, None);
    }

    #[test]
    fn dropped_attributes_count_maps_directly_both_ways() {
        let record = plain_log(LogRecord {
            message: Value::str("hi"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 5,
        });
        assert_eq!(record.dropped_attributes_count, 5);
        let decoded = decode_log_record(record, AttrMap::new());
        assert_eq!(decoded.log.unwrap().dropped_attributes_count, 5);
    }

    /// INFO2 (severity_number 10) round-trips as 10, not collapsed to Info's own band base (9) --
    /// the raw number and text both survive on the event's attributes and win back over the wire
    /// on re-encode. This is the fidelity half of `Severity`'s lossy 24-to-6 collapse.
    #[test]
    fn a_raw_severity_number_and_text_survive_a_decode_reencode_round_trip() {
        let wire = pb::LogRecord {
            time_unix_nano: 1000,
            observed_time_unix_nano: 0,
            severity_number: 10, // INFO2
            severity_text: "INFO2".to_string(),
            body: Some(common::value_to_any_value(&Value::str("hi"))),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: Vec::new(),
            span_id: Vec::new(),
            event_name: String::new(),
        };
        let decoded = decode_log_record(wire, AttrMap::new());
        assert_eq!(decoded.log.as_ref().unwrap().severity, Some(Severity::Info));
        assert_eq!(
            decoded.attributes.get("otel.severity_number"),
            Some(&Value::I64(10)),
            "the raw number must survive on the event's attributes"
        );
        assert_eq!(
            decoded.attributes.get("otel.severity_text").and_then(|v| v.as_str()),
            Some("INFO2"),
            "the raw text must survive on the event's attributes"
        );

        let re_encoded = encode_log_record(&decoded, decoded.log.as_ref().unwrap());
        assert_eq!(
            re_encoded.severity_number, 10,
            "must re-encode the raw 10, not the band base 9"
        );
        assert_eq!(re_encoded.severity_text, "INFO2");
        assert!(
            re_encoded
                .attributes
                .iter()
                .all(|kv| kv.key != "otel.severity_number" && kv.key != "otel.severity_text"),
            "the raw severity attributes must not also leak out as plain attributes"
        );
    }

    /// A log never sourced from OTLP (no `otel.severity_*` attributes at all -- e.g. one built by
    /// `kv_metrics`, a Lua script, or `json`) still encodes the band base + variant name exactly as
    /// it always did.
    #[test]
    fn a_non_otlp_sourced_log_still_encodes_the_band_base() {
        let record = plain_log(LogRecord {
            message: Value::str("hi"),
            severity: Some(Severity::Info),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        assert_eq!(record.severity_number, pb::SeverityNumber::Info as i32, "Info's own band base");
        assert_eq!(record.severity_text, "Info");
    }
}
