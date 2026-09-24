//! Pure-codec Graphite/Carbon fixed-point tests, against [`GraphiteEncoder`]/[`GraphiteDecoder`]
//! with no pipeline or socket: `graphite_in -> graphite_out` is a fixed point modulo the
//! "Permitted normalizations" list in `logit_proto::graphite`'s module doc.
//!
//! Two properties, per fixture and per protocol:
//!
//! 1. **`decode(encode(b)) == b`**, whole-event `PartialEq`. Every fixture has the shape a real
//!    decode produces (a `Gauge` per record, `Value::Str` tags, a whole-second timestamp, a
//!    sanitized path), so the check isn't a tautology.
//! 2. **`encode(decode(encode(b))) == encode(b)`** on bytes, which catches a codec that writes two
//!    byte strings for one batch.
//!
//! A third: the two protocols decode to the *same* events, so a dialect change (normalization 2)
//! is a re-spelling, not a loss.
//!
//! The encoder emits a pickle entry as a **length-prefixed frame** and the decoder takes an
//! **unframed** payload, so [`decode_all`] strips the prefix, as `graphite_in`'s reader does.
//!
//! The two `proptest`s at the bottom generate:
//!
//! - 1-6 events, one record each, paths from `[A-Za-z0-9_.-]{1,20}`, which the sanitizer doesn't
//!   touch (normalization 10 is tested in `encode.rs`);
//! - 0-4 tags per event in arbitrary order (normalization 4), with **distinct** names (a repeat is
//!   normalization 5) from `[A-Za-z0-9_-]{1,20}`, whose lack of `.` keeps a tag from looking like
//!   a `statsd.`/`collectd.` carrier; values from `[A-Za-z0-9_.-]{1,20}`;
//! - any **finite** `f64`, subnormals and `-0.0` included (normalization 8);
//! - whole-second timestamps in `1..=2_000_000_000` (normalization 6; a non-positive second is a
//!   counted drop).

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Sum, Temporality, Value,
};
use logit_proto::graphite::{GraphiteDecoder, GraphiteEncoder, Protocol};
use logit_proto::{Decoder, FramedEncoder, MessageBuf};
use proptest::prelude::*;
use std::sync::Arc;

const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;
/// Both wire protocols, checked for every fixture.
const PROTOCOLS: [Protocol; 2] = [Protocol::Plaintext, Protocol::Pickle];

// -- the harness ---------------------------------------------------------------------------------

fn encode_at(batch: &EventBatch, protocol: Protocol) -> Vec<Vec<u8>> {
    let mut encoder = GraphiteEncoder::new().with_protocol(protocol);
    let mut out: MessageBuf<usize> = MessageBuf::default();
    encoder.encode_into(batch, &mut out);
    out.iter().map(|message| message.to_vec()).collect()
}

/// Decodes every message in order through one decoder, stripping the pickle length prefix as
/// `graphite_in`'s framing does.
fn decode_all(messages: &[Vec<u8>], protocol: Protocol, resource: &Arc<Resource>) -> Vec<Event> {
    let mut decoder = GraphiteDecoder::new(resource.clone()).with_protocol(protocol);
    let mut events = Vec::new();
    for message in messages {
        let payload = match protocol {
            Protocol::Plaintext => Bytes::copy_from_slice(message),
            Protocol::Pickle => Bytes::copy_from_slice(&message[4..]),
        };
        decoder
            .decode_into(payload, RECEIVED_AT, &mut events)
            .expect("every message this encoder writes must decode");
    }
    events
}

/// Both properties for every protocol, plus the two dialects decoding to the same events.
fn assert_fixed_point(batch: EventBatch) {
    let mut decoded_per_protocol = Vec::new();
    for protocol in PROTOCOLS {
        let encoded = encode_at(&batch, protocol);
        assert!(!encoded.is_empty(), "{protocol:?}: the fixture encoded to nothing");

        // Property 1: decode(encode(b)) == b.
        let decoded = decode_all(&encoded, protocol, &batch.resource);
        assert_eq!(decoded, batch.events, "{protocol:?}: decode(encode(b)) must equal b");

        // Property 2: encode(decode(encode(b))) == encode(b), on bytes.
        let round_tripped =
            EventBatch { resource: batch.resource.clone(), scope: None, events: decoded.clone() };
        assert_eq!(
            encode_at(&round_tripped, protocol),
            encoded,
            "{protocol:?}: encode(decode(encode(b))) must equal encode(b) on bytes"
        );
        decoded_per_protocol.push(decoded);
    }

    // Normalization 2: plaintext -> pickle and pickle -> plaintext are re-spellings, not losses.
    assert_eq!(
        decoded_per_protocol[0], decoded_per_protocol[1],
        "the two dialects must decode to the same events"
    );
}

// -- fixtures ------------------------------------------------------------------------------------

fn batch(events: Vec<Event>) -> EventBatch {
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
}

/// One event carrying one record, in the shape a real decode produces.
fn datapoint(path: &str, value: f64, seconds: i64, tags: &[(&str, &str)]) -> Event {
    let mut attributes = AttrMap::new();
    for (name, tag_value) in tags {
        attributes.insert(name, Value::from(*tag_value));
    }
    Event::metric(
        seconds * 1_000_000_000,
        attributes,
        MetricRecord::new(intern(path), MetricKind::Gauge(value)),
    )
}

#[test]
fn a_plain_line_is_a_fixed_point() {
    assert_fixed_point(batch(vec![datapoint("sys.cpu.user", 0.5, 1_700_000_000, &[])]));
}

#[test]
fn a_tagged_line_is_a_fixed_point() {
    assert_fixed_point(batch(vec![datapoint(
        "sys.cpu.user",
        0.5,
        1_700_000_000,
        &[("env", "prod"), ("host", "web-1"), ("dc", "iad")],
    )]));
}

#[test]
fn an_integer_value_is_a_fixed_point() {
    assert_fixed_point(batch(vec![datapoint("page.views", 42.0, 1_700_000_000, &[])]));
}

#[test]
fn a_negative_value_is_a_fixed_point() {
    assert_fixed_point(batch(vec![datapoint("queue.lag", -17.25, 1_700_000_000, &[])]));
}

/// Past 2038, where the pickle writer switches from `BININT` to `LONG1`.
#[test]
fn a_large_timestamp_is_a_fixed_point() {
    assert_fixed_point(batch(vec![datapoint("sys.cpu", 1.0, 2_147_483_653, &[])]));
}

#[test]
fn many_datapoints_are_a_fixed_point() {
    let events = (0..25)
        .map(|i| {
            datapoint(
                &format!("sys.cpu.core{i}"),
                i as f64 / 4.0,
                1_700_000_000 + i,
                &[("host", "web-1")],
            )
        })
        .collect();
    assert_fixed_point(batch(events));
}

/// One event carrying several records comes back as one event per datapoint: carbon has no
/// multi-record datapoint.
#[test]
fn one_event_with_several_records_comes_back_as_several_events() {
    let mut event = datapoint("sys.cpu", 1.0, 1_700_000_000, &[("host", "web-1")]);
    event.metrics.push(MetricRecord::new(intern("sys.mem"), MetricKind::Gauge(2.0)));
    let source = batch(vec![event]);

    let decoded =
        decode_all(&encode_at(&source, Protocol::Plaintext), Protocol::Plaintext, &source.resource);
    assert_eq!(decoded.len(), 2, "one datapoint per record");
    // That shape is the fixed point: a second hop changes nothing.
    assert_fixed_point(batch(decoded));
}

/// Normalization 12: every `Sum` shape comes back as a `Gauge`, which is then a fixed point.
#[test]
fn a_sum_becomes_a_gauge_on_the_first_hop_and_is_then_a_fixed_point() {
    for temporality in [Temporality::Delta, Temporality::Cumulative] {
        for monotonic in [true, false] {
            let mut event = datapoint("page.views", 42.0, 1_700_000_000, &[]);
            event.metrics[0].kind = MetricKind::Sum(Sum { value: 42.0, temporality, monotonic });
            let source = batch(vec![event]);
            let decoded = decode_all(
                &encode_at(&source, Protocol::Plaintext),
                Protocol::Plaintext,
                &source.resource,
            );
            assert_eq!(decoded[0].metrics[0].kind, MetricKind::Gauge(42.0));
            assert_fixed_point(batch(decoded));
        }
    }
}

/// Normalization 4: tags leave in ascending rendered-name order, whatever order they arrived in.
#[test]
fn tag_order_is_canonical_after_the_first_hop() {
    let event =
        datapoint("sys.cpu", 1.0, 1_700_000_000, &[("zulu", "z"), ("alpha", "a"), ("mike", "m")]);
    let encoded = encode_at(&batch(vec![event.clone()]), Protocol::Plaintext);
    assert_eq!(
        std::str::from_utf8(&encoded[0]).unwrap(),
        "sys.cpu;alpha=a;mike=m;zulu=z 1 1700000000"
    );
    assert_fixed_point(batch(vec![event]));
}

/// Normalizations 9 and 5: a `\r\n` ending, whitespace runs, and a repeated tag key decode into a
/// batch that is a fixed point from there on.
#[test]
fn a_wire_shape_the_encoder_never_emits_is_a_fixed_point_once_decoded() {
    for wire in [
        "sys.cpu 1 1700000000\r\n",
        "sys.cpu\t1   1700000000\n",
        "sys.cpu;team=a;team=b 1 1700000000\n",
        "sys.cpu;env=prod 1.50 1700000000\n",
    ] {
        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        decoder
            .decode_into(Bytes::from(wire.to_string()), RECEIVED_AT, &mut events)
            .expect("the fixture must decode");
        assert_eq!(events.len(), 1, "{wire:?}");
        assert_fixed_point(batch(events));
    }
}

// -- properties ----------------------------------------------------------------------------------

/// A path from the alphabet the sanitizer leaves alone.
fn path_strategy() -> impl Strategy<Value = String> {
    "[A-Za-z0-9_.-]{1,20}".prop_map(|s| s)
}

/// 0-4 tags with distinct names, generated in arbitrary order.
fn tags_strategy() -> impl Strategy<Value = Vec<(String, String)>> {
    proptest::collection::vec(("[A-Za-z0-9_-]{1,20}", "[A-Za-z0-9_.-]{1,20}"), 0..5).prop_map(
        |tags| {
            let mut seen = Vec::new();
            tags.into_iter()
                .filter(|(name, _)| {
                    if seen.contains(name) {
                        false
                    } else {
                        seen.push(name.clone());
                        true
                    }
                })
                .collect()
        },
    )
}

fn batch_strategy() -> impl Strategy<Value = EventBatch> {
    proptest::collection::vec(
        (
            path_strategy(),
            tags_strategy(),
            any::<f64>().prop_filter("carbon carries only finite values", |v| v.is_finite()),
            1i64..=2_000_000_000,
        ),
        1..7,
    )
    .prop_map(|rows| {
        let events = rows
            .into_iter()
            .map(|(path, tags, value, seconds)| {
                let pairs: Vec<(&str, &str)> =
                    tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                datapoint(&path, value, seconds, &pairs)
            })
            .collect();
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    })
}

proptest! {
    /// The plaintext fixed point over a generated, already-normalized corpus.
    #[test]
    fn plaintext_round_trips_every_generated_batch(batch in batch_strategy()) {
        let encoded = encode_at(&batch, Protocol::Plaintext);
        let decoded = decode_all(&encoded, Protocol::Plaintext, &batch.resource);
        prop_assert_eq!(&decoded, &batch.events);

        let round_tripped =
            EventBatch { resource: batch.resource.clone(), scope: None, events: decoded };
        prop_assert_eq!(encode_at(&round_tripped, Protocol::Plaintext), encoded);
    }

    /// The same through pickle, and the two dialects decode alike (normalization 2).
    #[test]
    fn pickle_round_trips_every_generated_batch(batch in batch_strategy()) {
        let encoded = encode_at(&batch, Protocol::Pickle);
        let decoded = decode_all(&encoded, Protocol::Pickle, &batch.resource);
        prop_assert_eq!(&decoded, &batch.events);

        let round_tripped =
            EventBatch { resource: batch.resource.clone(), scope: None, events: decoded.clone() };
        prop_assert_eq!(encode_at(&round_tripped, Protocol::Pickle), encoded);

        let via_plaintext = decode_all(
            &encode_at(&batch, Protocol::Plaintext),
            Protocol::Plaintext,
            &batch.resource,
        );
        prop_assert_eq!(decoded, via_plaintext);
    }
}
