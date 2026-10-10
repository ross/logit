//! One datagram, or one framed packet, through `StatsdDecoder::decode_into`, as `statsd_in` hands
//! it over. The whole input is the datagram.
//!
//! Oracles, each over every input:
//! - lines: a valid UTF-8 datagram decodes to the same events as its `\n`-separated pieces decoded
//!   one at a time with the same `received_at`, appended after what `out` already held. No state
//!   crosses a line but the tag-key memo and the `bad_line` throttle, and neither changes an event.
//!   A datagram that isn't valid UTF-8 is `Err` and leaves `out` as it was;
//! - the model: every metric has a non-empty name; every `Sum`, `Gauge`, `GaugeDelta`, and sample
//!   value is finite; `Samples` and `SetMembers` are non-empty, and a sample rate is in `(0, 1]`;
//!   only those five kinds and a log record appear; every `Value::Str` is valid UTF-8;
//! - time: an event's timestamp is `received_at`, unless the line carried `|T`/`d:`, and a
//!   `statsd.timestamp` of `U64(secs)` means a timestamp of `secs` seconds;
//! - provenance: every `Value::Str` attribute, tag values and array elements included, and every
//!   set member, is a slice of the datagram. An event's message is too, unless its text had a
//!   `\n` escape to unescape, the one copy path (`crates/logit-proto/src/statsd/mod.rs`'s
//!   "DogStatsD events and service checks"): a message is a slice if and only if it holds no
//!   newline.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_core::interner::resolve;
use logit_core::{subslice, AttrMap, Event, MetricKind, Resource, Value};
use logit_proto::statsd::StatsdDecoder;
use logit_proto::{CodecError, Decoder};
use std::sync::Arc;

/// Off any second, so a wire timestamp can't equal it by accident.
const RECEIVED_AT: i64 = 1_700_000_000_123_456_789;

fn decoder() -> StatsdDecoder {
    StatsdDecoder::new(Arc::new(Resource::default()))
}

/// An event `decode_into` never produces, to show `out`'s earlier contents survive.
fn marker() -> Event {
    Event::empty(-1, AttrMap::new())
}

fn check_str(datagram: &Bytes, value: &Value) {
    match value {
        Value::Str(bytes) => {
            assert!(std::str::from_utf8(bytes).is_ok(), "model: a Str is valid UTF-8");
            assert!(subslice::within(datagram, bytes), "provenance: a Str slices the datagram");
        }
        Value::Array(elements) => {
            assert!(elements.len() >= 2, "model: a tag array has two elements or more");
            for element in elements {
                check_str(datagram, element);
            }
        }
        Value::Bool(_) | Value::U64(_) => {}
        other => panic!("model: an attribute the decoder never writes: {other:?}"),
    }
}

fn check_event(datagram: &Bytes, event: &Event) {
    for (_, value) in event.attributes.iter() {
        check_str(datagram, value);
    }
    match event.attributes.get("statsd.timestamp") {
        Some(Value::U64(secs)) => assert_eq!(
            u128::from(*secs) * 1_000_000_000,
            event.timestamp as u128,
            "time: statsd.timestamp names the event's timestamp"
        ),
        _ => assert!(
            event.timestamp == RECEIVED_AT || event.timestamp % 1_000_000_000 == 0,
            "time: a timestamp is received_at or a whole wire second"
        ),
    }
    if event.timestamp != RECEIVED_AT {
        assert!(event.timestamp >= 0, "time: a wire timestamp is never negative");
    }
    assert!(event.span.is_none(), "model: statsd decodes no span");
    if let Some(log) = &event.log {
        assert!(event.metrics.is_empty(), "model: an event line carries no metric");
        let Value::Str(message) = &log.message else {
            panic!("model: an event's message is a Str");
        };
        assert!(std::str::from_utf8(message).is_ok(), "model: a message is valid UTF-8");
        assert_eq!(
            subslice::within(datagram, message),
            !message.contains(&b'\n'),
            "provenance: a message is a slice if and only if nothing was unescaped"
        );
        return;
    }
    assert_eq!(event.metrics.len(), 1, "model: one metric per statsd event");
    let metric = &event.metrics[0];
    assert!(!resolve(metric.name).is_empty(), "model: a metric name is non-empty");
    match &metric.kind {
        MetricKind::Sum(sum) => assert!(sum.value.is_finite(), "model: a Sum is finite"),
        MetricKind::Gauge(v) | MetricKind::GaugeDelta(v) => {
            assert!(v.is_finite(), "model: a gauge is finite")
        }
        MetricKind::Samples(samples) => {
            assert!(!samples.values.is_empty(), "model: Samples is non-empty");
            assert!(samples.values.iter().all(|v| v.is_finite()), "model: a sample is finite");
            assert!(
                samples.sample_rate > 0.0 && samples.sample_rate <= 1.0,
                "model: a sample rate is in (0, 1]"
            );
        }
        MetricKind::SetMembers(members) => {
            assert!(!members.is_empty(), "model: SetMembers is non-empty");
            for member in members {
                assert!(std::str::from_utf8(member).is_ok(), "model: a member is valid UTF-8");
                assert!(
                    subslice::within(datagram, member),
                    "provenance: a set member slices the datagram"
                );
            }
        }
        other => panic!("model: a kind statsd never decodes: {}", other.name()),
    }
}

fuzz_target!(|data: &[u8]| {
    let datagram = Bytes::copy_from_slice(data);
    let mut whole = vec![marker()];
    let result = decoder().decode_into(datagram.clone(), RECEIVED_AT, &mut whole);
    if std::str::from_utf8(data).is_err() {
        assert!(matches!(result, Err(CodecError::Malformed(_))), "lines: invalid UTF-8 is Err");
        assert_eq!(whole, [marker()], "lines: an Err leaves out as it was");
        return;
    }
    result.expect("lines: valid UTF-8 always decodes");
    assert_eq!(whole[0], marker(), "lines: decode_into appends after out's contents");

    let mut pieces = vec![marker()];
    let mut by_line = decoder();
    let mut start = 0;
    for end in
        data.iter().enumerate().filter(|(_, &b)| b == b'\n').map(|(i, _)| i).chain([data.len()])
    {
        let piece = datagram.slice(start..end);
        by_line.decode_into(piece, RECEIVED_AT, &mut pieces).expect("lines: a piece is UTF-8");
        start = end + 1;
    }
    assert_eq!(whole, pieces, "lines: the datagram and its lines one at a time disagree");

    for event in &whole[1..] {
        check_event(&datagram, event);
    }
});
