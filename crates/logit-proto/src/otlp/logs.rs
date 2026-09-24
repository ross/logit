//! `Event`/`LogRecord` ↔ OTLP `LogRecord`.
//!
//! **`Severity` ↔ `SeverityNumber`.** Decode takes `severity_number`'s band (any of a band's four
//! numbers, e.g. `TRACE2..TRACE4`, maps to that band's `Severity`), then a case-insensitive
//! `severity_text` match when the number is unspecified or out of range, else `None`.
//!
//! Decode's 24-to-6 collapse is lossy (`INFO2` and `INFO4` both become `Info`), so the raw values
//! survive as attributes, the shape `syslog.severity` uses (ADR `syslog-output`'s "Header-field
//! precedence", ADR `lossless-transit` rule (b)): `otel.severity_number` (`Value::I64`, when
//! non-zero) and `otel.severity_text` (`Value::Str`, when non-empty).
//!
//! Encode treats that pair as one unit:
//! - **Neither attribute:** each band's base number (`Trace`→1, `Debug`→5, `Info`→9, `Warn`→13,
//!   `Error`→17, `Fatal`→21) and the variant's name as text. `severity == None` leaves the number
//!   `0` (`SEVERITY_NUMBER_UNSPECIFIED`).
//! - **Either attribute:** the present side's raw value, and OTLP's unset sentinel (`0` or `""`)
//!   for the missing side, never the band-derived value, which would invent a field the producer
//!   never sent (`10`/`"Info"` where the wire had `10`/`""`).
//!
//! `take_severity_attrs` removes an attribute only when it's usable: a number (`I64` or `U64`) in
//! `1..=24`, or any `as_str`-able text. An unusable one stays as an ordinary attribute and counts
//! as absent. So neither raw attribute leaks into the emitted attribute set.
//!
//! **`BodyFormat` has no OTLP field.** It round-trips through a `logit.body_format` attribute
//! (`"raw" | "json" | "structured"`), inserted on encode and removed on decode.
//!
//! **`time_unix_nano == 0` decodes to `observed_time_unix_nano`.** Zero means "unknown or
//! missing" (`logs.proto`), not the epoch, and a collector sends it when the event time wasn't
//! available; treating it as 1970 corrupts ordering and retention. If both are `0`,
//! `Event::timestamp` is `0`.
//!
//! **`observed_time_unix_nano` ↔ `LogRecord::observed_timestamp`.** Decode copies it verbatim
//! (`0` is unset in both models). Encode writes `observed_timestamp` when non-zero, the
//! `otlp_in -> otlp_out` fixed point, and otherwise the current time: `logs.proto` defines it as
//! when the collection system observed the event, and with nothing upstream, `logit` is that
//! system. So a `syslog_in` event with no parseable timestamp exports as `time_unix_nano: 0,
//! observed_time_unix_nano: <now>`, and a consumer recovers a sane timestamp. That path is the
//! only non-deterministic encode; a test there can't assert whole-record equality.
//!
//! **`trace_id`/`span_id`/`flags` ↔ `LogRecord::trace`** (ADR `log-record-trace-context`). `flags`
//! keeps its low 8 bits, the W3C trace flags. **Decode is lenient, unlike a `Span`'s ids**
//! (`traces.rs`'s `mod ids` rejects a wrong-length id): `logs.proto` says to treat an absent or
//! invalid `trace_id` as no trace, so `TraceRef::from_bytes` degrades a malformed id to `None`
//! rather than failing the record. **Encode falls back to the event's `span`** when the log has
//! no trace context: an `Event` carrying both a `log` and a `span` puts the span's ids on the
//! `LogRecord` with `flags: 0`. That isn't a fixed point: `encode_signals` splits the event into a
//! `LogRecord` and a `Span`, decode makes one `Event` per record, and the log comes back with
//! `trace: Some(..)` where it had `None`.
//!
//! **`event_name` ↔ `LogRecord::event_name`,** interned on decode when non-empty; `None` encodes
//! as empty.
//!
//! **`dropped_attributes_count` maps directly, both ways.**

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

/// The `severity_text` encode writes: the variant's name, matching the `{:?}` convention for tag
/// values elsewhere.
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

/// See the module doc's "`Severity` ↔ `SeverityNumber`".
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

/// Reads and removes `logit.body_format` from `attrs`, defaulting to `Raw` when absent (any
/// non-`logit` producer) or unrecognized.
fn decode_body_format(attrs: &mut AttrMap) -> BodyFormat {
    let format = match attrs.get("logit.body_format").and_then(|v| v.as_str()) {
        Some("json") => BodyFormat::Json,
        Some("structured") => BodyFormat::Structured,
        _ => BodyFormat::Raw,
    };
    attrs.remove("logit.body_format");
    format
}

/// `log.trace`, else `event.span`'s ids with `flags: 0` (not a fixed point; see the module doc).
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

/// Removes and returns `otel.severity_number`/`otel.severity_text` from `attrs`.
///
/// **Peeks before removing**: only a `Value::I64`/`Value::U64` number in `1..=24` or an
/// `as_str`-able text is taken. Anything else stays in `attrs` as an ordinary attribute.
fn take_severity_attrs(attrs: &mut AttrMap) -> (Option<i32>, Option<String>) {
    let number = match attrs.get("otel.severity_number") {
        Some(Value::I64(i)) if (1..=24).contains(i) => Some(*i as i32),
        Some(Value::U64(u)) if (1..=24).contains(u) => Some(*u as i32),
        _ => None,
    };
    if number.is_some() {
        attrs.remove("otel.severity_number");
    }
    let text = attrs.get("otel.severity_text").and_then(|v| v.as_str()).map(str::to_string);
    if text.is_some() {
        attrs.remove("otel.severity_text");
    }
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

    // The raw pair is one unit (module doc): neither present means band-derived values; either
    // present means OTLP's unset sentinel for the missing side.
    let (number, text) = match (severity_number_attr, severity_text_attr) {
        (None, None) => match log.severity {
            Some(sev) => (severity_number(sev), severity_text(sev).to_string()),
            None => (pb::SeverityNumber::Unspecified as i32, String::new()),
        },
        (number, text) => {
            (number.unwrap_or(pb::SeverityNumber::Unspecified as i32), text.unwrap_or_default())
        }
    };

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

/// Decodes one record, layering its attributes onto `attrs`; every caller passes an empty map.
pub(crate) fn decode_log_record(record: pb::LogRecord, mut attrs: AttrMap) -> Event {
    common::key_values_into_attrs(record.attributes, &mut attrs);
    let body_format = decode_body_format(&mut attrs);
    let severity = decode_severity(record.severity_number, &record.severity_text);
    // Raw severity survives alongside the normalized Severity (module doc).
    if record.severity_number != 0 {
        attrs.insert("otel.severity_number", Value::I64(record.severity_number as i64));
    }
    if !record.severity_text.is_empty() {
        attrs.insert("otel.severity_text", record.severity_text.as_str());
    }
    let message = record.body.map(common::any_value_to_value).unwrap_or(Value::Null);
    // 0 means "unknown", not the epoch (module doc).
    let timestamp = if record.time_unix_nano != 0 {
        record.time_unix_nano as i64
    } else {
        record.observed_time_unix_nano as i64
    };
    // `LOG_RECORD_FLAGS_TRACE_FLAGS_MASK`: the low 8 bits are the W3C trace flags, the rest is
    // reserved. A malformed id degrades to `None` (module doc).
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
            // `timestamp: 0` decodes as the encoder's wall-clock observed time; not asserted.
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

    /// A set `observed_timestamp` encodes unchanged, not overwritten with the wall clock.
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
        // `Event::timestamp == 0` must not come back as the epoch through logit's own encoder,
        // not only through a hand-built `pb::LogRecord`.
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

    /// INFO2 (severity_number 10) round-trips as 10, not Info's band base (9).
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

    /// A log with no `otel.severity_*` attributes encodes the band base and the variant name.
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

    /// Number only: `{10, ""}` re-encodes as `{10, ""}`, not `{10, "Info"}`.
    #[test]
    fn a_severity_number_with_no_text_round_trips_without_inventing_text() {
        let wire = pb::LogRecord {
            time_unix_nano: 1000,
            observed_time_unix_nano: 0,
            severity_number: 10,
            severity_text: String::new(),
            body: Some(common::value_to_any_value(&Value::str("hi"))),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: Vec::new(),
            span_id: Vec::new(),
            event_name: String::new(),
        };
        let decoded = decode_log_record(wire, AttrMap::new());
        let re_encoded = encode_log_record(&decoded, decoded.log.as_ref().unwrap());
        assert_eq!(re_encoded.severity_number, 10);
        assert_eq!(
            re_encoded.severity_text, "",
            "the missing severity_text must stay unset, not become the band-derived \"Info\""
        );
    }

    /// Text only: `{0, "warn"}` re-encodes as `{0, "warn"}`, not `{13, "warn"}`.
    #[test]
    fn a_severity_text_with_no_number_round_trips_without_inventing_a_number() {
        let wire = pb::LogRecord {
            time_unix_nano: 1000,
            observed_time_unix_nano: 0,
            severity_number: 0,
            severity_text: "warn".to_string(),
            body: Some(common::value_to_any_value(&Value::str("hi"))),
            attributes: Vec::new(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: Vec::new(),
            span_id: Vec::new(),
            event_name: String::new(),
        };
        let decoded = decode_log_record(wire, AttrMap::new());
        let re_encoded = encode_log_record(&decoded, decoded.log.as_ref().unwrap());
        assert_eq!(
            re_encoded.severity_number,
            pb::SeverityNumber::Unspecified as i32,
            "the missing severity_number must stay unset, not become the band-derived 13"
        );
        assert_eq!(re_encoded.severity_text, "warn");
    }

    /// A `Value::U64` number in `1..=24` is a usable raw override, like `Value::I64`.
    #[test]
    fn take_severity_attrs_honours_a_u64_number() {
        let mut attrs = AttrMap::new();
        attrs.insert("otel.severity_number", Value::U64(10));
        let (number, _text) = take_severity_attrs(&mut attrs);
        assert_eq!(number, Some(10));
        assert_eq!(attrs.get("otel.severity_number"), None, "a usable value must be removed");
    }

    /// An out-of-range or wrong-type `otel.severity_number` stays as an ordinary attribute.
    #[test]
    fn take_severity_attrs_leaves_an_unusable_number_in_place() {
        for value in [Value::I64(-1), Value::I64(99), Value::str("10")] {
            let mut attrs = AttrMap::new();
            attrs.insert("otel.severity_number", value.clone());
            let (number, _text) = take_severity_attrs(&mut attrs);
            assert_eq!(number, None, "{value:?} must not be used as a raw override");
            assert_eq!(
                attrs.get("otel.severity_number"),
                Some(&value),
                "{value:?} must be left in place as an ordinary attribute"
            );
        }
    }

    /// A wrong-type `otel.severity_text` (`Bytes`, not `Str`) stays as an ordinary attribute.
    #[test]
    fn take_severity_attrs_leaves_unusable_text_in_place() {
        let mut attrs = AttrMap::new();
        let value = Value::Bytes(bytes::Bytes::from_static(b"\xff\xfe"));
        attrs.insert("otel.severity_text", value.clone());
        let (_number, text) = take_severity_attrs(&mut attrs);
        assert_eq!(text, None, "non-string bytes must not be used as a raw override");
        assert_eq!(
            attrs.get("otel.severity_text"),
            Some(&value),
            "must be left in place as an ordinary attribute"
        );
    }
}
