//! Pure-codec collectd fixed-point tests: `docs/adr/lossless-transit.md`'s "round-trip fixed point
//! is the test that proves this" requirement, exercised directly against [`CollectdEncoder`]/
//! [`CollectdDecoder`] with no pipeline, listener, sink or socket in between. The mirror of
//! `tests/otlp_fixed_point.rs` and `tests/prometheus_fixed_point.rs`.
//!
//! Two properties, per fixture:
//!
//! 1. **`decode(encode(b)) == b`** -- whole-event equality via `PartialEq`
//!    (`docs/adr/metrics-model-v2.md`'s "`PartialEq` on every record type"). Every fixture is built
//!    in the shape a *real decode* already produces -- identity on `collectd.*` attributes, a
//!    positive timestamp, integral values where COUNTER/DERIVE/ABSOLUTE demand them, record names
//!    exactly `<plugin>.<type>[.<i>]` -- so the round trip is a real fixed-point check rather than a
//!    tautology over whatever the encoder happens to emit.
//! 2. **`encode(decode(encode(b))) == encode(b)` on bytes** -- the same fixed point restated at the
//!    wire level, which catches a codec that produces two different byte strings for what it itself
//!    considers the same batch (a non-deterministic elision decision, a stray part it sometimes
//!    writes and sometimes doesn't).
//!
//! Both are asserted at three datagram caps -- 1024 (collectd's own minimum `MaxPacketSize`), 1452
//! (its default) and 65535 (one datagram, no packing at all) -- because re-chosen datagram
//! boundaries are on the permitted-normalization list and must not change what decodes back out.
//!
//! The `proptest` at the bottom does the same over a generated packet *grammar*, starting from wire
//! bytes rather than a hand-built batch, so the fixtures above stay readable while the coverage is
//! not limited to what anyone thought to write down. What it generates, precisely:
//!
//! - 1–8 value lists in one datagram, each naming any subset of the five identity parts (the
//!   opening list always gets a host/plugin/type, since a datagram whose first list has none is one
//!   collectd itself rejects) — so **elision is generated, not assumed**: a list that names no
//!   plugin is inheriting the sticky one, exactly as a real sender's elision does;
//! - 1–7 data sources per list, any mix of all four types, over `any::<u64>`/`any::<i64>`/
//!   `any::<f64>` — so `NaN`, `±inf`, `u64::MAX` and the whole `f64` space are in range;
//! - identity strings from `[A-Za-z0-9._-]{1,20}` — deliberately **excluding** `/` and NUL, which
//!   the encoder substitutes (normalization 8) and which therefore are not a fixed point; that
//!   substitution has its own unit test in `encode.rs` instead;
//! - times as either whole seconds through the legacy `Time` part *or* an arbitrary `cdtime_t` in
//!   `[2^60, 2^61)` with every sub-second bit live, and intervals likewise legacy-seconds or an
//!   arbitrary `cdtime_t` below 2^53 ticks. The high-resolution branch is what reaches
//!   **normalization 2** — a `TimeHR` that may move ≤1 tick on the first hop and is stable after —
//!   which a whole-second `TimeHR` cannot, since it is bit-identical to its own legacy spelling.
//!
//! Because of that last point the property is asserted **from the first hop on**, not from the
//! input bytes: see the test's own doc comment for the exact chain, and for why the model half of
//! it (`d1 == d2`) nonetheless holds unconditionally.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Sum, Temporality, Value,
};
use logit_proto::collectd::types_db::TEST_TYPES_DB;
use logit_proto::collectd::{
    CollectdDecoder, CollectdEncoder, TypesDb, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN,
    ATTR_PLUGIN_INSTANCE, ATTR_TYPE, ATTR_TYPE_INSTANCE, DEFAULT_MAX_PACKET_BYTES,
};
use logit_proto::{Decoder, FramedEncoder, MessageBuf};
use proptest::prelude::*;
use std::sync::Arc;

const TS: i64 = 1_700_000_000_000_000_000;
const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;
/// The three caps every property is checked at -- see this file's module doc.
const CAPS: [usize; 3] = [1024, DEFAULT_MAX_PACKET_BYTES, 65535];

// -- part writing, for the wire-level fixtures and the proptest grammar -------------------------
//
// Deliberately hand-rolled rather than reusing `logit_proto::collectd::part`'s writers: a fixture
// built by the same code the encoder uses could not prove the encoder writes what a real collectd
// sender does, and could not express the legacy-part shapes this file needs.

const TYPE_HOST: u16 = 0x0000;
const TYPE_TIME: u16 = 0x0001;
const TYPE_PLUGIN: u16 = 0x0002;
const TYPE_PLUGIN_INSTANCE: u16 = 0x0003;
const TYPE_TYPE: u16 = 0x0004;
const TYPE_TYPE_INSTANCE: u16 = 0x0005;
const TYPE_VALUES: u16 = 0x0006;
const TYPE_INTERVAL: u16 = 0x0007;
const TYPE_TIME_HR: u16 = 0x0008;
const TYPE_INTERVAL_HR: u16 = 0x0009;

const DS_COUNTER: u8 = 0;
const DS_GAUGE: u8 = 1;
const DS_DERIVE: u8 = 2;
const DS_ABSOLUTE: u8 = 3;

#[derive(Default)]
struct PacketBuilder {
    bytes: Vec<u8>,
}

impl PacketBuilder {
    fn new() -> Self {
        Self::default()
    }

    fn part(mut self, part_type: u16, payload: &[u8]) -> Self {
        let len = (4 + payload.len()) as u16;
        self.bytes.extend_from_slice(&part_type.to_be_bytes());
        self.bytes.extend_from_slice(&len.to_be_bytes());
        self.bytes.extend_from_slice(payload);
        self
    }

    fn string(self, part_type: u16, value: &[u8]) -> Self {
        let mut payload = value.to_vec();
        payload.push(0);
        self.part(part_type, &payload)
    }

    fn number(self, part_type: u16, value: u64) -> Self {
        self.part(part_type, &value.to_be_bytes())
    }

    fn values(self, values: &[(u8, [u8; 8])]) -> Self {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(values.len() as u16).to_be_bytes());
        for (ds_type, _) in values {
            payload.push(*ds_type);
        }
        for (_, raw) in values {
            payload.extend_from_slice(raw);
        }
        self.part(TYPE_VALUES, &payload)
    }

    fn build(self) -> Bytes {
        Bytes::from(self.bytes)
    }
}

// -- the two properties --------------------------------------------------------------------------

fn encode_at(batch: &EventBatch, cap: usize) -> Vec<Vec<u8>> {
    let mut encoder = CollectdEncoder::new().with_max_packet_bytes(cap);
    let mut packets = MessageBuf::default();
    encoder.encode_into(batch, &mut packets);
    packets.iter().map(|bytes| bytes.to_vec()).collect()
}

/// Decodes every datagram in order through one decoder, exactly as `collectd_in` would.
fn decode_all(packets: &[Vec<u8>], resource: &Arc<Resource>) -> Vec<Event> {
    let mut decoder = CollectdDecoder::new(resource.clone());
    let mut events = Vec::new();
    for packet in packets {
        decoder
            .decode_into(Bytes::from(packet.clone()), RECEIVED_AT, &mut events)
            .expect("every datagram this encoder writes must decode");
    }
    events
}

/// One batch over `events`, sharing `resource` -- the `EventBatch` wrapper the encoder wants, built
/// often enough in the property below to be worth naming.
fn rebatch(resource: &Arc<Resource>, events: Vec<Event>) -> EventBatch {
    EventBatch { resource: resource.clone(), scope: None, events }
}

/// Both properties, at every cap in [`CAPS`].
fn assert_fixed_point(batch: EventBatch) {
    for cap in CAPS {
        let encoded = encode_at(&batch, cap);
        assert!(!encoded.is_empty(), "cap {cap}: the fixture encoded to nothing");

        // Property 1: decode(encode(b)) == b.
        let decoded = decode_all(&encoded, &batch.resource);
        assert_eq!(decoded, batch.events, "cap {cap}: decode(encode(b)) must equal b");

        // Property 2: encode(decode(encode(b))) == encode(b), on bytes.
        let round_tripped =
            EventBatch { resource: batch.resource.clone(), scope: None, events: decoded };
        assert_eq!(
            encode_at(&round_tripped, cap),
            encoded,
            "cap {cap}: encode(decode(encode(b))) must equal encode(b) on bytes"
        );
    }
}

/// The same two properties starting from real wire bytes: `packet` is decoded first, so the fixture
/// is decode-shaped by construction rather than by hand. This is the only way to exercise a wire
/// shape the encoder never emits -- a legacy `Time`/`Interval` part, a non-UTF-8 identity string, an
/// elision pattern a sender chose differently than this encoder would.
fn assert_wire_fixed_point(packet: Bytes, expected_events: usize) {
    let resource = Arc::new(Resource::default());
    let mut decoder = CollectdDecoder::new(resource.clone());
    let mut events = Vec::new();
    decoder.decode_into(packet, RECEIVED_AT, &mut events).expect("the fixture must decode");
    assert_eq!(events.len(), expected_events);
    assert_fixed_point(EventBatch { resource, scope: None, events });
}

// -- fixtures ------------------------------------------------------------------------------------

fn identity(type_instance: Option<&str>) -> AttrMap {
    let mut attrs = AttrMap::new();
    attrs.insert(ATTR_HOST, Value::from("web-1"));
    attrs.insert(ATTR_PLUGIN, Value::from("load"));
    attrs.insert(ATTR_PLUGIN_INSTANCE, Value::from("0"));
    attrs.insert(ATTR_TYPE, Value::from("load"));
    if let Some(instance) = type_instance {
        attrs.insert(ATTR_TYPE_INSTANCE, Value::from(instance));
    }
    attrs.insert(ATTR_INTERVAL, Value::F64(10.0));
    attrs
}

/// One event carrying one record named exactly what a decode of a single-data-source
/// `load`/`load` list produces.
fn single(kind: MetricKind) -> EventBatch {
    let mut event = Event::empty(TS, identity(Some("short")));
    event.metrics.push(MetricRecord::new(intern("load.load"), kind));
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] }
}

fn sum(value: f64, temporality: Temporality, monotonic: bool) -> MetricKind {
    MetricKind::Sum(Sum { value, temporality, monotonic })
}

#[test]
fn a_gauge_list_is_a_fixed_point() {
    assert_fixed_point(single(MetricKind::Gauge(0.42)));
}

#[test]
fn a_counter_list_is_a_fixed_point() {
    assert_fixed_point(single(sum(1027.0, Temporality::Cumulative, true)));
}

#[test]
fn a_derive_list_is_a_fixed_point_including_a_negative_value() {
    assert_fixed_point(single(sum(-4096.0, Temporality::Cumulative, false)));
}

#[test]
fn an_absolute_list_is_a_fixed_point() {
    assert_fixed_point(single(sum(5.0, Temporality::Delta, true)));
}

/// The whole range collectd's integer types span, including the two values that sit exactly on the
/// `f64` bounds (`u64::MAX` and `i64::MAX` both round *up* to a power of two as doubles).
#[test]
fn integer_values_at_the_edges_of_their_ranges_are_fixed_points() {
    for value in [0.0, 1.0, 9_007_199_254_740_992.0, u64::MAX as f64] {
        assert_fixed_point(single(sum(value, Temporality::Cumulative, true)));
        assert_fixed_point(single(sum(value, Temporality::Delta, true)));
    }
    for value in [i64::MIN as f64, -1.0, 0.0, i64::MAX as f64] {
        assert_fixed_point(single(sum(value, Temporality::Cumulative, false)));
    }
}

/// NaN is collectd's "no reading this interval," which the model carries as a flagged point -- so
/// the *flag*, not the NaN, is what has to survive (a `Gauge(NaN)` could never compare equal).
#[test]
fn a_nan_gauge_round_trips_as_a_flagged_zero_gauge() {
    let mut event = Event::empty(TS, identity(Some("short")));
    event.metrics.push(MetricRecord {
        flags: MetricRecord::FLAG_NO_RECORDED_VALUE,
        ..MetricRecord::new(intern("load.load"), MetricKind::Gauge(0.0))
    });
    assert_fixed_point(EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![event],
    });
}

#[test]
fn an_infinite_gauge_is_a_fixed_point() {
    assert_fixed_point(single(MetricKind::Gauge(f64::INFINITY)));
    assert_fixed_point(single(MetricKind::Gauge(f64::NEG_INFINITY)));
}

/// A multi-data-source list is **one** event carrying N records in wire order -- the shape that
/// makes it re-encodable as the same single list rather than N single-value ones.
#[test]
fn a_multi_value_list_is_a_fixed_point() {
    let mut event = Event::empty(TS, identity(None));
    for (index, value) in [0.1f64, 0.2, 0.3].into_iter().enumerate() {
        event.metrics.push(MetricRecord::new(
            intern(&format!("load.load.{index}")),
            MetricKind::Gauge(value),
        ));
    }
    assert_fixed_point(EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![event],
    });
}

/// A mix of data-source types in one list, which a real `types.db` type (`ps_state`, `disk_octets`)
/// routinely has.
#[test]
fn a_mixed_data_source_list_is_a_fixed_point() {
    let mut event = Event::empty(TS, identity(None));
    for (index, kind) in [
        MetricKind::Gauge(1.5),
        sum(7.0, Temporality::Cumulative, true),
        sum(-9.0, Temporality::Cumulative, false),
        sum(11.0, Temporality::Delta, true),
    ]
    .into_iter()
    .enumerate()
    {
        event.metrics.push(MetricRecord::new(intern(&format!("load.load.{index}")), kind));
    }
    assert_fixed_point(EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![event],
    });
}

/// An event with no `collectd.interval` at all: it leaves as `IntervalHR 0` and must come back
/// absent, not as `F64(0.0)`.
#[test]
fn a_list_with_no_interval_is_a_fixed_point() {
    let mut attrs = identity(Some("short"));
    attrs.remove(ATTR_INTERVAL);
    let mut event = Event::empty(TS, attrs);
    event.metrics.push(MetricRecord::new(intern("load.load"), MetricKind::Gauge(1.0)));
    assert_fixed_point(EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![event],
    });
}

/// Several events sharing an identity: the encoder elides the repeated parts, and the decoder's own
/// sticky state has to put them back. At cap 1024 this also crosses a datagram boundary, which is
/// where the encoder's re-encode earns its keep.
#[test]
fn many_lists_sharing_an_identity_are_a_fixed_point_across_datagram_boundaries() {
    let events: Vec<Event> = (0..60)
        .map(|i| {
            let mut attrs = identity(Some("short"));
            attrs.insert(ATTR_TYPE_INSTANCE, Value::str(format!("instance-{i}")));
            let mut event = Event::empty(TS + i, attrs);
            event.metrics.push(MetricRecord::new(intern("load.load"), MetricKind::Gauge(i as f64)));
            event
        })
        .collect();
    assert_fixed_point(EventBatch { resource: Arc::new(Resource::default()), scope: None, events });
}

// -- wire-level fixtures -------------------------------------------------------------------------

/// Legacy second-resolution `Time`/`Interval` parts: normalization (1) re-emits both as their
/// high-resolution counterparts, so the *model* is a fixed point even though the bytes are not.
#[test]
fn a_legacy_time_and_interval_packet_is_a_fixed_point_after_the_hr_rewrite() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_TIME, 1_700_000_000)
            .number(TYPE_INTERVAL, 10)
            .string(TYPE_PLUGIN, b"load")
            .string(TYPE_TYPE, b"load")
            .values(&[(DS_GAUGE, 0.5f64.to_le_bytes())])
            .build(),
        1,
    );
}

/// Normalization 2, made concrete and deterministic rather than left to the generator: a `TimeHR`
/// and an `IntervalHR` with real sub-second bits. The *bytes* are entitled to come back one tick
/// (2⁻³⁰ s) away from where they started — `cdtime -> ns -> cdtime` is not quite the identity —
/// while the model's timestamp does not move at all, because `ns -> cdtime -> ns` is.
#[test]
fn a_sub_second_time_and_interval_packet_is_a_fixed_point_from_the_first_hop() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_TIME_HR, (1_700_000_000u64 << 30) | 0x2C0F_FEE1)
            .number(TYPE_INTERVAL_HR, (10u64 << 30) | 0x0001_2345)
            .string(TYPE_PLUGIN, b"load")
            .string(TYPE_TYPE, b"load")
            .values(&[(DS_GAUGE, 0.5f64.to_le_bytes())])
            .build(),
        1,
    );
}

/// A sender's own elision pattern: one Host/Plugin/Type for three lists, each changing only its
/// TypeInstance. The encoder re-derives elision per output datagram and must land on the same
/// events.
#[test]
fn an_elided_identity_packet_is_a_fixed_point() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_TIME_HR, 1_700_000_000u64 << 30)
            .number(TYPE_INTERVAL_HR, 10u64 << 30)
            .string(TYPE_PLUGIN, b"cpu")
            .string(TYPE_PLUGIN_INSTANCE, b"0")
            .string(TYPE_TYPE, b"cpu")
            .string(TYPE_TYPE_INSTANCE, b"user")
            .values(&[(DS_DERIVE, 1234i64.to_be_bytes())])
            .string(TYPE_TYPE_INSTANCE, b"system")
            .values(&[(DS_DERIVE, 56i64.to_be_bytes())])
            .string(TYPE_TYPE_INSTANCE, b"idle")
            .values(&[(DS_DERIVE, 789i64.to_be_bytes())])
            .build(),
        3,
    );
}

/// A non-UTF-8 host: it rides as a `Value::Bytes` and is written back byte-verbatim. Lossily
/// replacing it would make this exact test fail, which is the point of keeping the distinction.
#[test]
fn a_non_utf8_host_packet_is_a_fixed_point() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, &[0xFF, 0xFE, b'w', b'e', b'b'])
            .number(TYPE_TIME_HR, 1_700_000_000u64 << 30)
            .string(TYPE_PLUGIN, b"memory")
            .string(TYPE_TYPE, b"memory")
            .values(&[(DS_GAUGE, 1.5f64.to_le_bytes())])
            .build(),
        1,
    );
}

/// A NaN gauge straight off the wire, rather than a hand-built flagged record.
#[test]
fn a_nan_gauge_packet_is_a_fixed_point() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_TIME_HR, 1_700_000_000u64 << 30)
            .string(TYPE_PLUGIN, b"df")
            .string(TYPE_TYPE, b"df_complex")
            .values(&[(DS_GAUGE, f64::NAN.to_le_bytes()), (DS_GAUGE, 2.5f64.to_le_bytes())])
            .build(),
        1,
    );
}

/// Every data-source type in one packet, each in its own list.
#[test]
fn a_packet_with_one_list_per_data_source_type_is_a_fixed_point() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_TIME_HR, 1_700_000_000u64 << 30)
            .string(TYPE_PLUGIN, b"p")
            .string(TYPE_TYPE, b"t")
            .string(TYPE_TYPE_INSTANCE, b"counter")
            .values(&[(DS_COUNTER, 42u64.to_be_bytes())])
            .string(TYPE_TYPE_INSTANCE, b"gauge")
            .values(&[(DS_GAUGE, (-0.5f64).to_le_bytes())])
            .string(TYPE_TYPE_INSTANCE, b"derive")
            .values(&[(DS_DERIVE, (-42i64).to_be_bytes())])
            .string(TYPE_TYPE_INSTANCE, b"absolute")
            .values(&[(DS_ABSOLUTE, u64::MAX.to_be_bytes())])
            .build(),
        4,
    );
}

/// **Record names cannot affect the fixed point.** The same `load` packet is decoded twice -- once
/// with a `types.db` (naming the records `load.load.shortterm`/`midterm`/`longterm`) and once
/// without (`load.load.0`/`1`/`2`) -- and both encode to byte-identical datagrams, because
/// `collectd_out` builds a value list from the `collectd.*` attributes, the `MetricList`'s order
/// and each record's kind, and never reads a name. This is what makes `types_db:` a display
/// setting rather than a relay-fidelity one (`docs/adr/collectd-binary-relay.md`), and it is why
/// misconfiguring it can never corrupt a relay.
#[test]
fn types_db_names_do_not_affect_the_fixed_point() {
    let packet = PacketBuilder::new()
        .string(TYPE_HOST, b"web-1")
        .number(TYPE_TIME_HR, 1_700_000_000u64 << 30)
        .number(TYPE_INTERVAL_HR, 10u64 << 30)
        .string(TYPE_PLUGIN, b"load")
        .string(TYPE_TYPE, b"load")
        .values(&[
            (DS_GAUGE, 0.1f64.to_le_bytes()),
            (DS_GAUGE, 0.2f64.to_le_bytes()),
            (DS_GAUGE, 0.3f64.to_le_bytes()),
        ])
        .build();
    let types_db = Arc::new(TypesDb::parse(TEST_TYPES_DB).expect("the fixture types.db parses"));

    let resource = Arc::new(Resource::default());
    let mut plain = CollectdDecoder::new(resource.clone());
    let mut named = CollectdDecoder::new(resource.clone()).with_types_db(types_db);
    let (mut plain_events, mut named_events) = (Vec::new(), Vec::new());
    plain.decode_into(packet.clone(), RECEIVED_AT, &mut plain_events).expect("must decode");
    named.decode_into(packet, RECEIVED_AT, &mut named_events).expect("must decode");

    let names = |events: &[Event]| -> Vec<String> {
        events[0]
            .metrics
            .iter()
            .map(|r| logit_core::interner::resolve(r.name).to_string())
            .collect()
    };
    assert_eq!(names(&plain_events), vec!["load.load.0", "load.load.1", "load.load.2"]);
    assert_eq!(
        names(&named_events),
        vec!["load.load.shortterm", "load.load.midterm", "load.load.longterm"],
        "the types.db must actually have changed the names, or this proves nothing"
    );

    let batch = |events: Vec<Event>| EventBatch { resource: resource.clone(), scope: None, events };
    for cap in CAPS {
        assert_eq!(
            encode_at(&batch(named_events.clone()), cap),
            encode_at(&batch(plain_events.clone()), cap),
            "cap {cap}: names must not reach the wire"
        );
    }

    // The `types.db`-named batch is a fixed point too -- against a decoder holding the same file,
    // which is what a real `collectd_in` with `types_db:` configured is. (It is deliberately *not*
    // checked through `assert_fixed_point`, whose decoder has no `types.db`: that round trip comes
    // back index-named, which is the display-only property this test is about rather than a
    // fidelity break -- the bytes above are identical either way.)
    let named_batch = batch(named_events.clone());
    for cap in CAPS {
        let mut decoder = CollectdDecoder::new(resource.clone())
            .with_types_db(Arc::new(TypesDb::parse(TEST_TYPES_DB).expect("parses")));
        let mut decoded = Vec::new();
        for packet in encode_at(&named_batch, cap) {
            decoder
                .decode_into(Bytes::from(packet), RECEIVED_AT, &mut decoded)
                .expect("every datagram this encoder writes must decode");
        }
        assert_eq!(decoded, named_events, "cap {cap}: decode(encode(b)) must equal b");
    }

    // The index-named batch is one against an ordinary decoder, as every other fixture here is.
    assert_fixed_point(batch(plain_events));
}

// -- the generated grammar -------------------------------------------------------------------

/// One generated value list: its identity (each field optional except the three collectd's own
/// receiver requires) and its data sources. `None` for an identity field means "don't write the
/// part," i.e. inherit whatever the datagram's sticky state already holds -- which is exactly how a
/// real sender elides.
#[derive(Debug, Clone)]
struct GenList {
    host: Option<String>,
    plugin: Option<String>,
    plugin_instance: Option<Option<String>>,
    type_: Option<String>,
    type_instance: Option<Option<String>>,
    time: Option<GenNumber>,
    interval: Option<GenNumber>,
    values: Vec<(u8, [u8; 8])>,
}

/// A generated time or interval, in one of the two spellings the wire has for each: whole seconds
/// (the legacy `Time`/`Interval` parts) or raw `cdtime_t` ticks (`TimeHR`/`IntervalHR`). Kept as an
/// enum rather than a `(bool, value)` pair so the two branches can generate genuinely different
/// *values* -- a whole-second `TimeHR` exercises none of the sub-second arithmetic, which is what
/// made the earlier `(bool, whole seconds)` shape inert.
#[derive(Debug, Clone, Copy)]
enum GenNumber {
    Legacy(u64),
    Hr(u64),
}

/// `[A-Za-z0-9._-]{1,20}` -- collectd's own identity alphabet minus the two bytes this codec
/// sanitizes (`/` and NUL), and short enough that the 127-byte truncation never fires. Both
/// exclusions are covered by their own unit tests in `encode.rs`; including them here would test
/// the sanitizer, not the fixed point.
fn identity_string() -> impl Strategy<Value = String> {
    "[A-Za-z0-9._-]{1,20}"
}

fn ds_value() -> impl Strategy<Value = (u8, [u8; 8])> {
    prop_oneof![
        any::<u64>().prop_map(|v| (DS_COUNTER, v.to_be_bytes())),
        any::<f64>().prop_map(|v| (DS_GAUGE, v.to_le_bytes())),
        any::<i64>().prop_map(|v| (DS_DERIVE, v.to_be_bytes())),
        any::<u64>().prop_map(|v| (DS_ABSOLUTE, v.to_be_bytes())),
    ]
}

/// A list's time: whole seconds through the legacy part, or an **arbitrary** `cdtime_t` in
/// `[2^60, 2^61)` — roughly 2004..2038, with every one of the 30 sub-second bits live. That range is
/// what makes the property reach normalization 2 at all: a `cdtime` with real low bits is exactly
/// the case where `cdtime -> ns -> cdtime` may land one tick away from where it started.
fn gen_time() -> impl Strategy<Value = GenNumber> {
    prop_oneof![
        (1u64..2_000_000_000).prop_map(GenNumber::Legacy),
        ((1u64 << 60)..(2u64 << 60)).prop_map(GenNumber::Hr),
    ]
}

/// A list's interval: whole seconds through the legacy part, or an arbitrary `cdtime_t` below 2^53
/// ticks (~97 days). The bound is `f64`'s exact-integer range, which is the honest limit of the
/// `Value::F64` seconds carrier `collectd.interval` uses — above it the tick count itself stops
/// being representable, which is a documented `f64` gap rather than a codec property to assert.
fn gen_interval() -> impl Strategy<Value = GenNumber> {
    prop_oneof![
        (0u64..3600).prop_map(GenNumber::Legacy),
        (0u64..(1u64 << 53)).prop_map(GenNumber::Hr),
    ]
}

fn gen_list() -> impl Strategy<Value = GenList> {
    (
        proptest::option::of(identity_string()),
        proptest::option::of(identity_string()),
        proptest::option::of(proptest::option::of(identity_string())),
        proptest::option::of(identity_string()),
        proptest::option::of(proptest::option::of(identity_string())),
        proptest::option::of(gen_time()),
        proptest::option::of(gen_interval()),
        proptest::collection::vec(ds_value(), 1..8),
    )
        .prop_map(
            |(host, plugin, plugin_instance, type_, type_instance, time, interval, values)| {
                GenList {
                    host,
                    plugin,
                    plugin_instance,
                    type_,
                    type_instance,
                    time,
                    interval,
                    values,
                }
            },
        )
}

/// Renders generated lists into one datagram, writing only the parts each list actually names --
/// so elision within the packet is part of what is generated, not something this helper decides.
/// The first list always gets a full host/plugin/type, since a datagram whose opening list has no
/// identity is one collectd itself rejects (and this codec skips, counted).
fn render(lists: &[GenList]) -> Bytes {
    let mut builder = PacketBuilder::new();
    for (index, list) in lists.iter().enumerate() {
        let first = index == 0;
        let host = list.host.clone().unwrap_or_else(|| "fixture-host".to_string());
        if first || list.host.is_some() {
            builder = builder.string(TYPE_HOST, host.as_bytes());
        }
        builder = match list.time {
            Some(GenNumber::Legacy(seconds)) => builder.number(TYPE_TIME, seconds),
            Some(GenNumber::Hr(cdtime)) => builder.number(TYPE_TIME_HR, cdtime),
            // Only the opening list needs a default: after it the time is sticky like everything
            // else, and a list that names none is a list deliberately inheriting one.
            None if first => builder.number(TYPE_TIME_HR, 1_700_000_000u64 << 30),
            None => builder,
        };
        builder = match list.interval {
            Some(GenNumber::Legacy(seconds)) => builder.number(TYPE_INTERVAL, seconds),
            Some(GenNumber::Hr(cdtime)) => builder.number(TYPE_INTERVAL_HR, cdtime),
            None => builder,
        };
        let plugin = list.plugin.clone().unwrap_or_else(|| "fixture-plugin".to_string());
        if first || list.plugin.is_some() {
            builder = builder.string(TYPE_PLUGIN, plugin.as_bytes());
        }
        if let Some(instance) = &list.plugin_instance {
            builder =
                builder.string(TYPE_PLUGIN_INSTANCE, instance.as_deref().unwrap_or("").as_bytes());
        }
        let type_ = list.type_.clone().unwrap_or_else(|| "fixture-type".to_string());
        if first || list.type_.is_some() {
            builder = builder.string(TYPE_TYPE, type_.as_bytes());
        }
        if let Some(instance) = &list.type_instance {
            builder =
                builder.string(TYPE_TYPE_INSTANCE, instance.as_deref().unwrap_or("").as_bytes());
        }
        builder = builder.values(&list.values);
    }
    builder.build()
}

proptest! {
    // 256 cases x 3 caps x 2 encode passes: comfortably inside the "keep the suite fast" budget
    // `tests/robustness.rs`'s module doc sets for this crate.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The whole point, over generated packets, stated the way normalization 2 requires -- from the
    /// *first hop* on, not from the input bytes:
    ///
    /// - `d1 = decode(p)`, `e1 = encode(d1)`, `d2 = decode(e1)`, `e2 = encode(d2)`;
    /// - `d2 == decode(e2)` -- the model is a fixed point;
    /// - `e2 == encode(decode(e2))` -- and so are the bytes, from `e1` onward.
    ///
    /// `d1 == d2` is asserted too, and holds *unconditionally* rather than only for tick-aligned
    /// input: the ≤1-tick drift normalization 2 permits is a `cdtime -> ns -> cdtime` drift, which
    /// lives entirely in the bytes (`p`'s `TimeHR` may differ from `e1`'s), while `ns -> cdtime ->
    /// ns` is exact -- so the *model's* timestamp never moves at all. Asserting it here is what
    /// would catch that stopping being true.
    #[test]
    fn a_generated_packet_is_a_fixed_point_at_every_cap(
        lists in proptest::collection::vec(gen_list(), 1..8)
    ) {
        let packet = render(&lists);
        let resource = Arc::new(Resource::default());
        let mut decoder = CollectdDecoder::new(resource.clone());
        let mut d1 = Vec::new();
        decoder.decode_into(packet, RECEIVED_AT, &mut d1).expect("a generated packet must decode");
        prop_assert_eq!(d1.len(), lists.len());

        for cap in CAPS {
            let e1 = encode_at(&rebatch(&resource, d1.clone()), cap);
            let d2 = decode_all(&e1, &resource);
            prop_assert_eq!(&d2, &d1, "cap {}: the first hop moved the model", cap);

            let e2 = encode_at(&rebatch(&resource, d2.clone()), cap);
            let d3 = decode_all(&e2, &resource);
            prop_assert_eq!(&d3, &d2, "cap {}: decode(encode(d2)) != d2", cap);
            prop_assert_eq!(
                encode_at(&rebatch(&resource, d3), cap),
                e2,
                "cap {}: the bytes are not idempotent from the first hop on",
                cap
            );
        }
    }
}
