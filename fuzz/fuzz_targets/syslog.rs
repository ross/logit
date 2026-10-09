//! One datagram, or one framed message, through `SyslogDecoder::decode_into`, as `syslog_in` hands
//! it over. Byte 0's low bit turns line splitting on (the UDP arm) or off (the TCP arm); the rest
//! of the input is the datagram or frame.
//!
//! Oracles, each over every input:
//! - lines, with splitting on: the datagram decodes to the same events as each of its
//!   `\n`-separated pieces, one trailing `\r` stripped, decoded alone with splitting off, appended
//!   after what `out` already held, with the same `received_at`;
//! - UDP and TCP, with splitting on: when the datagram's first byte isn't a digit, it decodes to
//!   the same events as `syslog_in`'s TCP arm gives the same bytes as one stream: each frame of an
//!   `Rfc6587Auto` `Framer` and its `finish` remainder, decoded with splitting off. The input is
//!   under the frame cap, so no line is oversize and the framer never errors;
//! - modes, with splitting off, for a frame `x` holding no `\n`: `x + "\n"` decodes as the
//!   splitting decoder decodes `x`, and `x + "\r\n"` and the splitting decoder's `x + "\r"` both
//!   decode as `x` does. So `x + "\n"`, `x + "\r\n"`, and `x` split agree whenever `x` doesn't end
//!   in `\r`: one `\r` before the counted `\n` comes off, and a `\r` in front of that is payload;
//! - the model (`crates/logit-proto/src/syslog/mod.rs`'s "Mapping"): every event is a log record
//!   at `received_at` with nothing else on it; facility is at most 23 and severity at most 7, and
//!   `log.severity` is severity's mapping; every SD-ID and PARAM-NAME is 1 to 32 PRINTUSASCII bytes
//!   without `=`, `]`, `"`, or a space; a PARAM-VALUE is a `Str`, or an `Array` of two `Str`s or
//!   more; `syslog.pid` is a `U64`, or a `Str` that isn't canonical decimal fitting a `u64`; every
//!   `Str` is valid UTF-8, and a `Bytes` message isn't;
//! - provenance: every header field and every message is a slice of the input. A PARAM-VALUE is
//!   the one copy, built as its escapes are read, so it isn't checked.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_core::interner::resolve;
use logit_core::{subslice, AttrMap, Event, Resource, Severity, Value};
use logit_proto::framing::{Framer, Framing, FramingMode, MAX_FRAME_BYTES};
use logit_proto::syslog::SyslogDecoder;
use logit_proto::Decoder;
use std::sync::Arc;

const RECEIVED_AT: i64 = 1_700_000_000_123_456_789;

struct Syslog(SyslogDecoder);

impl Syslog {
    fn new(line_splitting: bool) -> Self {
        Syslog(
            SyslogDecoder::new(Arc::new(Resource::default())).with_line_splitting(line_splitting),
        )
    }

    fn decode(&mut self, bytes: Bytes, out: &mut Vec<Event>) {
        self.0.decode_into(bytes, RECEIVED_AT, out).expect("syslog decoding never fails");
    }

    fn events(&mut self, bytes: &[u8]) -> Vec<Event> {
        let mut out = Vec::new();
        self.decode(Bytes::copy_from_slice(bytes), &mut out);
        out
    }
}

/// An event `decode_into` never produces, to show `out`'s earlier contents survive.
fn marker() -> Event {
    Event::empty(-1, AttrMap::new())
}

fn concat(a: &[u8], b: &[u8]) -> Vec<u8> {
    [a, b].concat()
}

fn strip_one_cr(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn is_sd_name(name: &str) -> bool {
    (1..=32).contains(&name.len())
        && name.bytes().all(|b| (33..=126).contains(&b) && !matches!(b, b'=' | b']' | b'"'))
}

/// `Some` for canonical decimal fitting a `u64`: what the decoder makes a `U64` PID.
fn canonical_u64(s: &str) -> Option<u64> {
    let canonical = !s.is_empty()
        && s.bytes().all(|b| b.is_ascii_digit())
        && (s.len() == 1 || !s.starts_with('0'));
    canonical.then(|| s.parse().ok()).flatten()
}

fn expected_severity(n: u64) -> Severity {
    match n {
        0..=2 => Severity::Fatal,
        3 => Severity::Error,
        4 => Severity::Warn,
        5 | 6 => Severity::Info,
        7 => Severity::Debug,
        _ => panic!("model: a severity past 7: {n}"),
    }
}

/// A header field: a non-empty, space-free, valid UTF-8 slice of the input.
fn check_field(input: &Bytes, key: &str, value: &Value) -> Bytes {
    let Value::Str(bytes) = value else { panic!("model: {key} is a Str: {value:?}") };
    assert!(std::str::from_utf8(bytes).is_ok(), "model: {key} is valid UTF-8");
    assert!(!bytes.is_empty() && !bytes.contains(&b' '), "model: {key} is one non-empty token");
    assert!(subslice::within(input, bytes), "provenance: {key} slices the input");
    bytes.clone()
}

fn check_sd(sd: &Value) {
    let Value::Map(elements) = sd else { panic!("model: syslog.sd is a Map: {sd:?}") };
    assert!(!elements.is_empty(), "model: syslog.sd holds an element");
    for (id, params) in elements.iter() {
        assert!(is_sd_name(resolve(id)), "model: SD-ID {:?} is an SD-NAME", resolve(id));
        let Value::Map(params) = params else { panic!("model: an SD-ELEMENT is a Map") };
        for (name, value) in params.iter() {
            assert!(is_sd_name(resolve(name)), "model: PARAM-NAME {:?}", resolve(name));
            let values = match value {
                Value::Str(_) => std::slice::from_ref(value),
                Value::Array(values) => {
                    assert!(values.len() >= 2, "model: a folded PARAM holds two values or more");
                    values
                }
                other => panic!("model: a PARAM-VALUE is a Str or an Array: {other:?}"),
            };
            for value in values {
                let Value::Str(bytes) = value else { panic!("model: a PARAM-VALUE is a Str") };
                assert!(std::str::from_utf8(bytes).is_ok(), "model: a PARAM-VALUE is UTF-8");
            }
        }
    }
}

fn check_event(input: &Bytes, event: &Event) {
    assert_eq!(event.timestamp, RECEIVED_AT, "model: an event's timestamp is received_at");
    assert!(event.metrics.is_empty() && event.span.is_none(), "model: syslog decodes a log only");
    let log = event.log.as_ref().expect("model: every event is a log record");

    let mut severity = None;
    for (key, value) in event.attributes.iter() {
        match resolve(key) {
            "syslog.facility" => {
                assert!(matches!(value, Value::U64(0..=23)), "model: facility {value:?}")
            }
            "syslog.severity" => {
                let Value::U64(n) = value else { panic!("model: severity is a U64") };
                severity = Some(expected_severity(*n));
            }
            "syslog.timestamp" => match value {
                Value::Timestamp(_) | Value::Null => {}
                Value::Str(bytes) => {
                    assert_eq!(bytes.len(), 15, "model: an RFC 3164 timestamp is 15 bytes");
                    assert!(bytes.is_ascii(), "model: an RFC 3164 timestamp is ASCII");
                    assert!(subslice::within(input, bytes), "provenance: the timestamp slices");
                }
                other => panic!("model: syslog.timestamp {other:?}"),
            },
            key @ ("syslog.hostname" | "syslog.tag" | "syslog.msgid") => {
                check_field(input, key, value);
            }
            "syslog.pid" => {
                if !matches!(value, Value::U64(_)) {
                    let pid = check_field(input, "syslog.pid", value);
                    let pid = std::str::from_utf8(&pid).unwrap();
                    assert!(pid.bytes().all(|b| (33..=126).contains(&b)), "model: PID {pid:?}");
                    assert!(canonical_u64(pid).is_none(), "model: a decimal PID {pid:?} is a U64");
                }
            }
            "syslog.sd" => check_sd(value),
            other => panic!("model: an attribute syslog never writes: {other}"),
        }
    }
    assert_eq!(log.severity, Some(severity.expect("model: syslog.severity is stamped")));
    match &log.message {
        Value::Str(bytes) => {
            assert!(std::str::from_utf8(bytes).is_ok(), "model: a Str message is UTF-8");
            assert!(subslice::within(input, bytes), "provenance: a message slices the input");
        }
        Value::Bytes(bytes) => {
            assert!(std::str::from_utf8(bytes).is_err(), "model: a Bytes message isn't UTF-8");
            assert!(subslice::within(input, bytes), "provenance: a message slices the input");
        }
        other => panic!("model: a message is Str or Bytes: {other:?}"),
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, data)) = data.split_first() else { return };
    let line_splitting = selector & 1 == 1;
    let input = Bytes::copy_from_slice(data);
    let mut whole = vec![marker()];
    Syslog::new(line_splitting).decode(input.clone(), &mut whole);
    assert_eq!(whole[0], marker(), "decode_into appends after out's contents");
    for event in &whole[1..] {
        check_event(&input, event);
    }

    if line_splitting {
        let mut pieces = vec![marker()];
        let mut framed = Syslog::new(false);
        for piece in data.split(|&b| b == b'\n') {
            framed.decode(Bytes::copy_from_slice(strip_one_cr(piece)), &mut pieces);
        }
        assert_eq!(whole, pieces, "lines: the datagram and its pieces one at a time disagree");

        if data.first().is_some_and(|b| !b.is_ascii_digit()) {
            assert!(data.len() <= MAX_FRAME_BYTES);
            let mut framer = Framer::new(FramingMode::Rfc6587Auto, MAX_FRAME_BYTES);
            framer.push(data);
            assert_eq!(framer.framing(), Some(Framing::NonTransparent));
            let mut stream = vec![marker()];
            while let Some(frame) = framer.next_frame().expect("tcp: under the cap, no error") {
                framed.decode(frame, &mut stream);
            }
            if let Some(frame) = framer.finish().expect("tcp: under the cap, no error") {
                framed.decode(frame, &mut stream);
            }
            assert_eq!(whole, stream, "tcp: the datagram and the same bytes as a stream disagree");
        }
    } else {
        assert!(whole.len() <= 2, "modes: a frame is one message");
        if !data.contains(&b'\n') {
            let mut split = Syslog::new(true);
            let mut framed = Syslog::new(false);
            let x = &whole[1..];
            assert_eq!(
                framed.events(&concat(data, b"\n")),
                split.events(data),
                "modes: a counted LF comes off as the splitting decoder's line end does"
            );
            assert_eq!(framed.events(&concat(data, b"\r\n")), x, "modes: a counted CRLF comes off");
            assert_eq!(split.events(&concat(data, b"\r")), x, "modes: one CR comes off a line");
        }
    }
});
