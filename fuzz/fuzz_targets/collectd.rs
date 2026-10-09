//! One collectd datagram through `CollectdDecoder::decode_into`, as `collectd_in` hands it over.
//! Bytes 0 and 1 are a big-endian cut position, taken modulo the datagram's length plus one; the
//! rest of the input is the datagram.
//!
//! Oracles, each over every input:
//! - failure: an `Err` leaves `out` as it was, since `decode_into` fails only when nothing
//!   decoded (`crates/logit-proto/src/collectd/decode.rs`'s `decode_into`);
//! - prefix: parts are length-delimited, so a cut fails only the part it lands in. The events of
//!   `x[..cut]` are a prefix of the events of `x`; they are the events of `x[..start]`, where
//!   `start` is the start of the part the cut lands in (on a framing walk of `x`, `read_part`
//!   alone); and if `x[..start]` fails, so does `x[..cut]`;
//! - Encryption stops the walk: `x` decodes as `x` up to its first Encryption part, followed by
//!   that part with its payload removed;
//! - Signature is skipped by length: `x` decodes as `x` with its first Signature part's payload
//!   inverted;
//! - the model (`crates/logit-proto/src/collectd/mod.rs`'s "Decode" and "Notifications"): an
//!   event is a value list of 1 to `MAX_VALUES_PER_LIST` records with host, plugin, and type, or
//!   a notification with a host, a non-empty message, and a severity of 1, 2, or 4; every
//!   attribute is a `collectd.*` key, every string a `Str` when valid UTF-8 and a `Bytes` when
//!   not, sliced from the datagram; a record's name is `<plugin>.<type>`, each cut to 127 bytes,
//!   with `.<i>` after it in a list of more than one; a NaN GAUGE is a flagged `0.0`;
//! - the second-generation fixed point: `W = encode(decode(x))` may differ from `x` by the
//!   module doc's "Permitted normalizations", so the property starts at `W`, packed at
//!   `DEFAULT_MAX_PACKET_BYTES`: decoding each datagram of `W` and encoding the events again gives
//!   `W`'s bytes.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_core::interner::resolve;
use logit_core::{
    subslice, AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Severity,
    Temporality, Value,
};
use logit_proto::collectd::part::{self, TYPE_ENCRYPTION, TYPE_SIGNATURE};
use logit_proto::collectd::{
    CollectdDecoder, CollectdEncoder, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN, ATTR_PLUGIN_INSTANCE,
    ATTR_SEVERITY, ATTR_TYPE, ATTR_TYPE_INSTANCE, DATA_MAX_NAME_LEN, DEFAULT_MAX_PACKET_BYTES,
    MAX_VALUES_PER_LIST,
};
use logit_proto::{CodecError, Decoder, FramedEncoder, MessageBuf};
use std::sync::Arc;

/// Off any second, so a wire timestamp can't equal it by accident.
const RECEIVED_AT: i64 = 1_700_000_000_123_456_789;

/// An event `decode_into` never produces, to show `out`'s earlier contents survive.
fn marker() -> Event {
    Event::empty(-1, AttrMap::new())
}

/// `decode_into` over `bytes` into an `out` holding [`marker`], returning the result and the
/// events after the marker.
fn decode(bytes: Bytes) -> (Result<(), CodecError>, Vec<Event>) {
    let mut out = vec![marker()];
    let result = CollectdDecoder::new(Arc::new(Resource::default()))
        .decode_into(bytes, RECEIVED_AT, &mut out)
        .map(|_| ());
    assert_eq!(out[0], marker(), "decode_into appends after out's contents");
    if result.is_err() {
        assert_eq!(out.len(), 1, "failure: an Err leaves out as it was");
    }
    (result, out.split_off(1))
}

/// A framing walk, `read_part` alone: the start, type, and length of every part it reads, and
/// the offset it stops at: the end, a part `read_part` refuses, or the first Encryption part,
/// where the decoder's own walk stops too.
fn framing_walk(datagram: &[u8]) -> (Vec<(usize, u16, usize)>, usize) {
    let mut parts = Vec::new();
    let mut at = 0;
    while let Ok((header, _)) = part::read_part(datagram, at) {
        if header.part_type == TYPE_ENCRYPTION {
            break;
        }
        parts.push((at, header.part_type, header.len));
        at += header.len;
    }
    (parts, at)
}

/// `bytes` as a record name segment, as the decoder writes one: its first 127 bytes, U+FFFD per
/// invalid sequence.
fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(DATA_MAX_NAME_LEN - 1)]).into_owned()
}

fn check_string(datagram: &Bytes, key: &str, value: &Value) -> Bytes {
    let (Value::Str(bytes) | Value::Bytes(bytes)) = value else {
        panic!("model: {key} is a Str or Bytes: {value:?}")
    };
    assert_eq!(
        matches!(value, Value::Str(_)),
        std::str::from_utf8(bytes).is_ok(),
        "model: {key} is a Str if and only if it's valid UTF-8"
    );
    assert!(!bytes.is_empty(), "model: an empty string part leaves {key} absent");
    assert!(subslice::within(datagram, bytes), "zero-copy: {key} slices the datagram");
    bytes.clone()
}

fn check_event(datagram: &Bytes, event: &Event) {
    assert!(event.span.is_none(), "model: collectd decodes no span");
    assert!(event.timestamp > 0, "model: a timestamp is positive");
    let mut strings = std::collections::BTreeMap::new();
    for (key, value) in event.attributes.iter() {
        let key = resolve(key);
        match key {
            ATTR_HOST | ATTR_PLUGIN | ATTR_PLUGIN_INSTANCE | ATTR_TYPE | ATTR_TYPE_INSTANCE => {
                strings.insert(key, check_string(datagram, key, value));
            }
            ATTR_INTERVAL => {
                assert!(event.log.is_none(), "model: a notification carries no interval");
                let Value::F64(seconds) = value else { panic!("model: an interval is an F64") };
                assert!(seconds.is_finite() && *seconds > 0.0, "model: interval {seconds}");
            }
            ATTR_SEVERITY => {
                assert!(event.log.is_some(), "model: only a notification carries a severity")
            }
            other => panic!("model: an attribute collectd never writes: {other}"),
        }
    }
    assert!(strings.contains_key(ATTR_HOST), "model: every event has a host");

    if let Some(log) = &event.log {
        assert!(event.metrics.is_empty(), "model: a notification carries no metric");
        let severity = match event.attributes.get(ATTR_SEVERITY) {
            Some(Value::U64(1)) => Severity::Error,
            Some(Value::U64(2)) => Severity::Warn,
            Some(Value::U64(4)) => Severity::Info,
            other => panic!("model: a notification's severity is 1, 2, or 4: {other:?}"),
        };
        assert_eq!(log.severity, Some(severity), "model: the severity's mapping");
        check_string(datagram, "the message", &log.message);
        return;
    }

    let count = event.metrics.len();
    assert!((1..=MAX_VALUES_PER_LIST).contains(&count), "model: a list of {count} values");
    let (Some(plugin), Some(type_)) = (strings.get(ATTR_PLUGIN), strings.get(ATTR_TYPE)) else {
        panic!("model: a value list has a plugin and a type")
    };
    let base = format!("{}.{}", lossy(plugin), lossy(type_));
    for (index, record) in event.metrics.iter().enumerate() {
        let name = resolve(record.name);
        if count == 1 {
            assert_eq!(name, base, "model: a one-value list's name");
        } else {
            assert_eq!(name, format!("{base}.{index}"), "model: a list's record names");
        }
        match &record.kind {
            MetricKind::Sum(sum) => {
                assert_eq!(record.flags, 0, "model: only a GAUGE is flagged");
                assert!(sum.value.is_finite(), "model: an integer is finite");
                if sum.temporality == Temporality::Delta {
                    assert!(sum.monotonic && sum.value >= 0.0, "model: ABSOLUTE is a u64");
                } else if sum.monotonic {
                    assert!(sum.value >= 0.0, "model: COUNTER is a u64");
                }
            }
            MetricKind::Gauge(v) => {
                assert!(!v.is_nan(), "model: a NaN GAUGE is flagged, never carried");
                if record.flags != 0 {
                    assert_eq!(record.flags, MetricRecord::FLAG_NO_RECORDED_VALUE);
                    assert_eq!(v.to_bits(), 0.0f64.to_bits(), "model: a flagged GAUGE is 0.0");
                }
            }
            other => panic!("model: a kind collectd never decodes: {}", other.name()),
        }
    }
}

fn encode(events: Vec<Event>) -> Vec<Vec<u8>> {
    let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
    let mut out: MessageBuf<usize> = MessageBuf::default();
    CollectdEncoder::new()
        .with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES)
        .encode_into(&batch, &mut out);
    out.iter().map(<[u8]>::to_vec).collect()
}

fuzz_target!(|data: &[u8]| {
    let Some((&[hi, lo], datagram)) = data.split_first_chunk::<2>() else { return };
    let cut = usize::from(u16::from_be_bytes([hi, lo])) % (datagram.len() + 1);
    let input = Bytes::copy_from_slice(datagram);
    let (result, events) = decode(input.clone());

    let (parts, stop) = framing_walk(datagram);
    let (cut_result, cut_events) = decode(input.slice(..cut));
    assert!(events.starts_with(&cut_events), "prefix: the cut's events aren't a prefix");
    let start = parts.iter().map(|&(at, _, _)| at).chain([stop]).rfind(|&at| at <= cut).unwrap();
    let (start_result, start_events) = decode(input.slice(..start));
    assert_eq!(cut_events, start_events, "prefix: a cut costs more than its own part");
    if start_result.is_err() {
        assert!(cut_result.is_err(), "prefix: a cut after a failing part decoded");
    }

    if part::read_part(datagram, stop).is_ok_and(|(h, _)| h.part_type == TYPE_ENCRYPTION) {
        let mut stopped = datagram[..stop].to_vec();
        stopped.extend_from_slice(&[0x02, 0x10, 0x00, 0x04]);
        let (stopped_result, stopped_events) = decode(Bytes::from(stopped));
        assert_eq!(result.is_err(), stopped_result.is_err(), "encryption: a different verdict");
        assert_eq!(events, stopped_events, "encryption: the walk read past Encryption");
    }
    if let Some(&(at, _, len)) = parts.iter().find(|&&(_, kind, _)| kind == TYPE_SIGNATURE) {
        let mut flipped = datagram.to_vec();
        for byte in &mut flipped[at + part::HEADER_LEN..at + len] {
            *byte = !*byte;
        }
        let (flipped_result, flipped_events) = decode(Bytes::from(flipped));
        assert_eq!(result.is_err(), flipped_result.is_err(), "signature: a different verdict");
        assert_eq!(events, flipped_events, "signature: the payload changed what decoded");
    }

    for event in &events {
        check_event(&input, event);
    }

    let w = encode(events);
    let mut again = Vec::new();
    for packet in &w {
        let (result, events) = decode(Bytes::copy_from_slice(packet));
        result.expect("fixed point: everything the encoder writes decodes");
        again.extend(events);
    }
    assert_eq!(encode(again), w, "fixed point: encode(decode(W)) != W");
});
