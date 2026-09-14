//! Pure-codec Graphite/Carbon fixed-point tests: `docs/adr/lossless-transit.md`'s "round-trip fixed
//! point is the test that proves this" requirement, exercised directly against [`GraphiteEncoder`]/
//! [`GraphiteDecoder`] with no pipeline, listener, sink or socket in between. The mirror of
//! `tests/collectd_fixed_point.rs`, `tests/otlp_fixed_point.rs` and
//! `tests/prometheus_fixed_point.rs`.
//!
//! Two properties, per fixture and per protocol:
//!
//! 1. **`decode(encode(b)) == b`** -- whole-event equality via `PartialEq`
//!    (`docs/adr/metrics-model-v2.md`'s "`PartialEq` on every record type"). Every fixture is built
//!    in the shape a *real decode* already produces -- a `Gauge` per record, tags as `Value::Str`
//!    event attributes, a whole-second timestamp, an already-sanitized path -- so the round trip is
//!    a real fixed-point check rather than a tautology over whatever the encoder happens to emit.
//! 2. **`encode(decode(encode(b))) == encode(b)`** on bytes -- the same fixed point restated at the
//!    wire level, which catches a codec that produces two different byte strings for what it itself
//!    considers the same batch.
//!
//! Both are asserted for **each** wire protocol, and a third property on top: the two protocols
//! decode to the *same* events, which is what makes normalization 2 (an operator-chosen dialect
//! change, in either direction) a re-spelling rather than a loss.
//!
//! One asymmetry the harness has to bridge: [`GraphiteEncoder`] emits a pickle entry that is a
//! **complete, length-prefixed frame** (so a sink's send path is one `write_all`), while
//! [`GraphiteDecoder`] is handed an **unframed** payload (so `graphite_in`'s reader owns the
//! prefix). [`decode_all`] strips the four prefix bytes, which is exactly what that reader does.
//!
//! The two `proptest`s at the bottom do the same over a generated batch *grammar*, so the fixtures
//! above stay readable while the coverage is not limited to what anyone thought to write down.
//! What they generate, precisely:
//!
//! - 1-6 events, each one record, paths from `[A-Za-z0-9_.-]{1,20}` -- deliberately **already
//!   normalized**, i.e. drawn from an alphabet the sanitizer does not touch. A path needing
//!   substitution is *not* a fixed point (normalization 10) and has its own unit tests in
//!   `encode.rs` instead;
//! - 0-4 tags per event with **distinct** names from `[A-Za-z0-9_-]{1,20}` and values from
//!   `[A-Za-z0-9_.-]{1,20}`, generated in arbitrary order so canonical tag ordering
//!   (normalization 4) is exercised rather than assumed. Distinct names because a repeated key is
//!   normalization 5, not a fixed point; an alphabet with no `.` in a name so a generated tag can
//!   never look like a `statsd.`/`collectd.` carrier, which the encoder skips by design;
//! - any **finite** `f64` value -- so the whole range, subnormals and `-0.0` included, goes through
//!   the shortest-round-trip rendering (normalization 8);
//! - timestamps as whole seconds in `1..=2_000_000_000`, since carbon's wire has no sub-second
//!   resolution (normalization 6) and a non-positive second is a counted drop, not a fixed point.

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
/// Both wire protocols, checked for every fixture -- a dialect change is on the
/// permitted-normalization list and must not change what decodes back out.
const PROTOCOLS: [Protocol; 2] = [Protocol::Plaintext, Protocol::Pickle];

// -- the harness ---------------------------------------------------------------------------------

fn encode_at(batch: &EventBatch, protocol: Protocol) -> Vec<Vec<u8>> {
    let mut encoder = GraphiteEncoder::new().with_protocol(protocol);
    let mut out: MessageBuf<usize> = MessageBuf::default();
    encoder.encode_into(batch, &mut out);
    out.iter().map(|message| message.to_vec()).collect()
}

/// Decodes every message in order through one decoder, exactly as `graphite_in` would -- stripping
/// the pickle length prefix the way that listener's framing loop does (see this file's module doc).
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

/// Both properties, for every protocol -- plus the cross-protocol one: the two dialects decode to
/// the same events, in both directions.
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

/// One event carrying one record, in exactly the shape a real decode produces.
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

/// Past 2038, which is where the pickle writer switches from `BININT` to `LONG1` -- the one place
/// the two dialects' encodings of the same second genuinely differ.
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

/// One event carrying several records is one event with several records on the way back -- carbon
/// has no multi-value datapoint, so this is the one shape that genuinely *cannot* survive: it comes
/// back as one event per datapoint. Asserted explicitly rather than left as a surprise.
#[test]
fn one_event_with_several_records_comes_back_as_several_events() {
    let mut event = datapoint("sys.cpu", 1.0, 1_700_000_000, &[("host", "web-1")]);
    event.metrics.push(MetricRecord::new(intern("sys.mem"), MetricKind::Gauge(2.0)));
    let source = batch(vec![event]);

    let decoded =
        decode_all(&encode_at(&source, Protocol::Plaintext), Protocol::Plaintext, &source.resource);
    assert_eq!(decoded.len(), 2, "one datapoint per record");
    // And *that* shape is the fixed point, which is what matters for a relay: a second hop changes
    // nothing.
    assert_fixed_point(batch(decoded));
}

/// Normalization 12: every `Sum` shape leaves as a bare value and comes back as a `Gauge`. Not a
/// fixed point on the first hop (the temporality and monotonicity are genuinely gone), but the
/// decoded shape is -- which is the property a relay actually needs.
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

/// Normalization 4: whatever order the tags arrived in, they leave in ascending rendered-name
/// order -- and stay there.
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

/// Normalizations 9 and 5, from the wire side: a `\r\n` line ending, runs of whitespace and a
/// repeated tag key all decode into a batch that *is* a fixed point from there on.
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

/// A path drawn from the alphabet the sanitizer leaves alone -- see this file's module doc for why
/// "already normalized" is the right generator here.
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

    /// The same, through the pickle dialect -- and the two dialects must agree on what they
    /// decoded (normalization 2).
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
