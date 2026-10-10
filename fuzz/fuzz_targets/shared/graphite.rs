//! The decode model the two graphite targets check `GraphiteDecoder` against: what one
//! `(path field, value, timestamp)` triple becomes, written from the "Decode: wire → model" table
//! in `crates/logit-proto/src/graphite/mod.rs` rather than from the decoder's code, and the
//! second-generation fixed point both targets run.

use bytes::Bytes;
use logit_core::interner::{intern, resolve};
use logit_core::{subslice, AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Value};
use logit_proto::graphite::{GraphiteDecoder, GraphiteEncoder, Protocol};
use logit_proto::{Decoder, FramedEncoder, MessageBuf};
use std::sync::Arc;

/// Off any second, so a wire timestamp can't equal it by accident.
pub const RECEIVED_AT: i64 = 1_700_000_000_123_456_789;

/// From 2^20 seconds up, `resolve_timestamp`'s sub-second product is exact, so the decoder reads
/// a timestamp as [`exact_nanos`] does. Below it the product rounds once, and the result can sit
/// one nanosecond from the exact reading.
const EXACT_FROM_SECONDS: f64 = (1u64 << 20) as f64;

pub fn decoder(protocol: Protocol) -> GraphiteDecoder {
    GraphiteDecoder::new(Arc::new(Resource::default())).with_protocol(protocol)
}

/// An event `decode_into` never produces, to show `out`'s earlier contents survive.
pub fn marker() -> Event {
    Event::empty(-1, AttrMap::new())
}

/// A positive, finite `seconds` as nanoseconds, from the `f64`'s own mantissa and exponent:
/// rounded half away from zero, as `f64::round` rounds, and saturating at `i64::MAX`.
pub fn exact_nanos(seconds: f64) -> i64 {
    assert!(seconds.is_finite() && seconds > 0.0);
    let bits = seconds.to_bits();
    let biased = ((bits >> 52) & 0x7ff) as i32;
    let fraction = (bits & ((1 << 52) - 1)) as i128;
    let (mantissa, exponent) =
        if biased == 0 { (fraction, -1074) } else { (fraction | (1 << 52), biased - 1075) };
    let scaled = mantissa * 1_000_000_000;
    // A normal mantissa is at least 2^52, so a non-negative exponent is 2^52 seconds or more.
    if exponent >= 0 {
        return i64::MAX;
    }
    let shift = -exponent;
    // `scaled` is under 2^83, so a wider shift leaves less than half a nanosecond.
    if shift > 84 {
        return 0;
    }
    let whole = scaled >> shift;
    let rest = scaled & ((1i128 << shift) - 1);
    let nanos = whole + i128::from(rest >= 1i128 << (shift - 1));
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

/// What the doc's decode table makes of one datapoint: `None` when it's skipped, else the event
/// and whether its timestamp may sit one nanosecond from the exact reading.
pub fn expected_event(path_field: &str, value: f64, seconds: f64) -> Option<(Event, bool)> {
    let (timestamp, approximate) = if seconds == -1.0 {
        (RECEIVED_AT, false)
    } else if seconds.is_finite() && seconds > 0.0 {
        (exact_nanos(seconds), seconds < EXACT_FROM_SECONDS)
    } else {
        return None;
    };
    if !value.is_finite() {
        return None;
    }
    let (path, tags) = match path_field.split_once(';') {
        Some((path, tags)) => (path, Some(tags)),
        None => (path_field, None),
    };
    let mut attributes = AttrMap::new();
    for segment in tags.into_iter().flat_map(|tags| tags.split(';')) {
        let (name, tag_value) = segment.split_once('=')?;
        if name.is_empty() || tag_value.is_empty() {
            return None;
        }
        attributes.insert(name, Value::Str(Bytes::copy_from_slice(tag_value.as_bytes())));
    }
    if path.is_empty() {
        return None;
    }
    let record = MetricRecord::new(intern(path), MetricKind::Gauge(value));
    Some((Event::metric(timestamp, attributes, record), approximate))
}

/// `decoded` is `expected`, with its timestamp within a nanosecond when `approximate`.
pub fn assert_matches(decoded: &Event, expected: &(Event, bool), what: &str) {
    let (expected, approximate) = expected;
    if *approximate {
        let off = decoded.timestamp.abs_diff(expected.timestamp);
        assert!(off <= 1, "{what}: timestamp {} is {off} ns from exact", decoded.timestamp);
        let mut decoded = decoded.clone();
        decoded.timestamp = expected.timestamp;
        assert_eq!(&decoded, expected, "{what}: the event isn't the doc's");
    } else {
        assert_eq!(decoded, expected, "{what}: the event isn't the doc's");
    }
    let (MetricKind::Gauge(got), MetricKind::Gauge(want)) =
        (&decoded.metrics[0].kind, &expected.metrics[0].kind)
    else {
        unreachable!("both are a Gauge, or the comparison above failed")
    };
    assert_eq!(got.to_bits(), want.to_bits(), "{what}: the value's bits changed");
}

/// The shape every decoded event has, whatever produced it: one finite `Gauge` with a path
/// holding no `;`, and `Str` tags whose names hold no `;` or `=` and whose values hold no `;`,
/// each non-empty. When `zero_copy`, every tag value is also a slice of `input`: it is, unless
/// the pickle reader decoded the path field's escapes into its scratch.
pub fn check_event(input: &Bytes, event: &Event, zero_copy: bool) {
    assert!(event.log.is_none() && event.span.is_none(), "model: graphite decodes a metric only");
    assert_eq!(event.metrics.len(), 1, "model: one record per datapoint");
    let record = &event.metrics[0];
    let MetricKind::Gauge(value) = record.kind else {
        panic!("model: a carbon datapoint is a Gauge")
    };
    assert!(value.is_finite(), "model: a Gauge is finite");
    assert_eq!(record.flags, 0, "model: carbon has no flagged datapoint");
    let path = resolve(record.name);
    assert!(!path.is_empty() && !path.contains(';'), "model: path {path:?}");
    assert!(event.timestamp >= 0, "model: a timestamp is never negative");
    for (key, value) in event.attributes.iter() {
        let key = resolve(key);
        assert!(!key.is_empty() && !key.contains([';', '=']), "model: tag name {key:?}");
        let Value::Str(bytes) = value else { panic!("model: a tag value is a Str: {value:?}") };
        let text = std::str::from_utf8(bytes).expect("model: a tag value is valid UTF-8");
        assert!(!text.is_empty() && !text.contains(';'), "model: tag value {text:?}");
        assert!(
            !zero_copy || subslice::within(input, bytes),
            "zero-copy: a tag value slices the input"
        );
    }
}

/// `events` as the batch a decoder's output makes: the default resource, no scope.
fn batch(events: Vec<Event>) -> EventBatch {
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
}

/// `batch` encoded under `protocol`, one entry per line or length-prefixed pickle frame.
pub fn encode(batch: &EventBatch, protocol: Protocol) -> Vec<Vec<u8>> {
    let mut out: MessageBuf<usize> = MessageBuf::default();
    GraphiteEncoder::new().with_protocol(protocol).encode_into(batch, &mut out);
    out.iter().map(<[u8]>::to_vec).collect()
}

/// Every entry decoded in order, the pickle length prefix stripped as `graphite_in`'s framing
/// strips it.
pub fn decode_all(messages: &[Vec<u8>], protocol: Protocol) -> Vec<Event> {
    let mut decoder = decoder(protocol);
    let mut events = Vec::new();
    for message in messages {
        let payload = match protocol {
            Protocol::Plaintext => &message[..],
            Protocol::Pickle => {
                let (prefix, payload) = message.split_at(4);
                let declared = u32::from_be_bytes(prefix.try_into().unwrap()) as usize;
                assert_eq!(declared, payload.len(), "fixed point: the prefix names the payload");
                payload
            }
        };
        decoder
            .decode_into(Bytes::copy_from_slice(payload), RECEIVED_AT, &mut events)
            .expect("fixed point: everything the encoder writes decodes");
    }
    events
}

/// The second-generation fixed point: `W = encode(decode(x))` may differ from `x` by the
/// "Permitted normalizations" list in `crates/logit-proto/src/graphite/mod.rs`, so the property
/// starts at `W`: `encode(decode(W)) == W`, in both protocols, and the two protocols' `W` decode
/// to the same events (normalization 2: a dialect change is a re-spelling).
pub fn assert_second_generation_fixed_point(first: Vec<Event>) {
    let first = batch(first);
    let mut decoded = Vec::new();
    for protocol in [Protocol::Plaintext, Protocol::Pickle] {
        let w = encode(&first, protocol);
        let again = batch(decode_all(&w, protocol));
        assert_eq!(encode(&again, protocol), w, "fixed point: {protocol:?} encode(decode(W)) != W");
        decoded.push(again.events);
    }
    assert_eq!(decoded[0], decoded[1], "fixed point: the two protocols' W decode differently");
}
