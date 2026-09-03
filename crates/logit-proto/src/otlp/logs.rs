//! `Event`/`LogRecord` ↔ OTLP `LogRecord`.
//!
//! **`Severity` ↔ `SeverityNumber`.** Encode: each band's base value (`Trace`→1, `Debug`→5,
//! `Info`→9, `Warn`→13, `Error`→17, `Fatal`→21; `log.severity == None` leaves `severity_number`
//! unset at `SEVERITY_NUMBER_UNSPECIFIED`/`0`), with `severity_text` set to the variant's name.
//! Decode prefers `severity_number`'s band (any of the 4 numbers in a band -- e.g. `TRACE2..TRACE4`
//! -- map to that band's `Severity`), falls back to a case-insensitive `severity_text` match when
//! the number is unspecified or out of range, else `None`.
//!
//! **`BodyFormat` has no OTLP field.** It round-trips through a `logit.body_format` attribute
//! (`"raw" | "json" | "structured"`), inserted on encode and consumed (removed) on decode -- the
//! same "reserved key rides as an attribute" idiom `traces.rs` uses for a span's status message.
//!
//! **`time_unix_nano == 0` falls back to `observed_time_unix_nano`,** per OTLP's own contract for
//! a consumer that (like this one) keeps a single timestamp: `time_unix_nano == 0` means "unknown
//! or missing" (`logs.proto`'s own doc comment), not literally the Unix epoch -- a real collector
//! commonly sends exactly that when the original event time wasn't available, and treating it as
//! epoch would silently corrupt ordering and could push the record outside a downstream retention
//! window. If `observed_time_unix_nano` is also `0`, `Event::timestamp` is `0`; there is no third
//! fallback to reach for.
//!
//! **Encode stamps `observed_time_unix_nano` with the current wall clock** (`crate::now_nanos`) --
//! OTLP's own definition of the field (`logs.proto`: "Time when the event was observed by the
//! collection system"), and at encode time `logit` *is* that collection system. This is what makes
//! the fallback above do real work end to end: a `syslog_in` event with no parseable timestamp
//! carries `Event::timestamp == 0` and exports as `time_unix_nano: 0, observed_time_unix_nano:
//! <now>`, so a downstream consumer -- `logit`'s own `otlp_in` included -- recovers a sane
//! timestamp instead of the Unix epoch. Not `event.timestamp` mirrored onto both fields: that
//! would make the fallback above a no-op tautology, and would write `observed = 0` in exactly the
//! "unknown timestamp" case it exists to cover. Makes `encode_log_record` non-deterministic on
//! this one field -- no test may assert whole-record equality against a fixed expected value.
//!
//! **Dropped, documented, not errors** (mirrors `traces.rs`'s precedent for `Span`):
//! `trace_id`/`span_id` (a log and its span correlate today only by sharing one `Event` --
//! `logit_core::LogRecord` has no field of its own to carry a peer's correlation IDs when they
//! arrive on a separate signal or a bare log-only record), `flags`, `dropped_attributes_count`,
//! `event_name`.

use crate::otlp::common;
use crate::otlp::generated::opentelemetry::proto::logs::v1 as pb;
use logit_core::{AttrMap, BodyFormat, Event, LogRecord, Severity, Value};

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

pub(crate) fn encode_log_record(event: &Event, log: &LogRecord) -> pb::LogRecord {
    let mut attributes = common::attrs_to_key_values(&event.attributes);
    attributes.push(crate::otlp::generated::opentelemetry::proto::common::v1::KeyValue {
        key: "logit.body_format".to_string(),
        value: Some(common::value_to_any_value(&Value::str(body_format_str(log.body_format)))),
        key_strindex: 0,
    });

    let (number, text) = match log.severity {
        Some(sev) => (severity_number(sev), severity_text(sev).to_string()),
        None => (pb::SeverityNumber::Unspecified as i32, String::new()),
    };

    pb::LogRecord {
        time_unix_nano: event.timestamp.max(0) as u64,
        observed_time_unix_nano: crate::now_nanos().max(0) as u64,
        severity_number: number,
        severity_text: text,
        body: Some(common::value_to_any_value(&log.message)),
        attributes,
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: Vec::new(),
        span_id: Vec::new(),
        event_name: String::new(),
    }
}

/// `base_attrs` is the scope-derived base (see `common::scope_attrs`), cloned once per record by
/// the caller -- this only layers the record's own attributes on top.
pub(crate) fn decode_log_record(record: pb::LogRecord, mut attrs: AttrMap) -> Event {
    common::key_values_into_attrs(record.attributes, &mut attrs);
    let body_format = decode_body_format(&mut attrs);
    let severity = decode_severity(record.severity_number, &record.severity_text);
    let message = record.body.map(common::any_value_to_value).unwrap_or(Value::Null);
    // See the module doc: 0 means "unknown", not literally the epoch -- prefer the observed time
    // over silently treating a missing original timestamp as 1970-01-01.
    let timestamp = if record.time_unix_nano != 0 {
        record.time_unix_nano as i64
    } else {
        record.observed_time_unix_nano as i64
    };
    Event::log(timestamp, attrs, LogRecord { message, severity, body_format })
}

#[cfg(test)]
mod tests {
    use super::*;

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
                LogRecord { message: Value::str("hi"), severity: None, body_format: format },
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
            LogRecord { message: Value::str("hi"), severity: None, body_format: BodyFormat::Raw },
        );
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        assert!(
            encoded.observed_time_unix_nano as i64 >= before,
            "observed_time_unix_nano should be stamped with the current wall clock, not left at 0"
        );
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
            LogRecord { message: Value::str("hi"), severity: None, body_format: BodyFormat::Raw },
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
            LogRecord { message: Value::str("hi"), severity: None, body_format: BodyFormat::Raw },
        );
        let encoded = encode_log_record(&event, event.log.as_ref().unwrap());
        assert_eq!(encoded.severity_number, pb::SeverityNumber::Unspecified as i32);
        assert_eq!(encoded.severity_text, "");
    }
}
