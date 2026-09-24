//! Pure-codec collectd fixed-point tests, against [`CollectdEncoder`]/[`CollectdDecoder`] with no
//! pipeline or socket: `collectd_in -> collectd_out` is a fixed point modulo the "Permitted
//! normalizations" list in `logit_proto::collectd`'s module doc.
//!
//! Two properties, per fixture:
//!
//! 1. **`decode(encode(b)) == b`**, whole-event `PartialEq`. Every fixture has the shape a real
//!    decode produces (identity on `collectd.*` attributes, a positive timestamp, integral values
//!    for COUNTER/DERIVE/ABSOLUTE, names `<plugin>.<type>[.<i>]`), so the check isn't a tautology.
//! 2. **`encode(decode(encode(b))) == encode(b)` on bytes**, which catches a codec that writes two
//!    byte strings for one batch (a non-deterministic elision decision, a sometimes-written part).
//!
//! Both are asserted at three datagram caps: 1024 (collectd's minimum `MaxPacketSize`), 1452 (its
//! default), and 65535 (no packing), because re-chosen boundaries (normalization 3) must not change
//! what decodes.
//!
//! The `proptest` at the bottom starts from a generated packet *grammar*:
//!
//! - 1–8 value lists per datagram, each naming any subset of the five identity parts (the first
//!   always gets host/plugin/type, which collectd requires), so **elision is generated**;
//! - 1–7 data sources per list, all four types, over `any::<u64>`/`any::<i64>`/`any::<f64>`, so
//!   `NaN`, `±inf`, and `u64::MAX` are in range;
//! - identity strings from `[A-Za-z0-9._-]{1,20}`, **excluding** `/` and NUL (normalization 8,
//!   tested in `encode.rs`);
//! - times as legacy whole seconds *or* a `cdtime_t` in `[2^60, 2^61)` with live sub-second bits,
//!   and intervals as legacy seconds or a `cdtime_t` below 2^53 ticks. Only the high-resolution
//!   branch reaches **normalization 2** (a `TimeHR` may move ≤1 tick on the first hop);
//! - an optional notification after any list, with a valid severity and a non-empty message
//!   (the drop cases are tested in `encode.rs`).
//!
//! Because of normalization 2, the property holds **from the first hop on**, not from the input
//! bytes; the test's doc comment has the chain.

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
/// The three caps every property is checked at.
const CAPS: [usize; 3] = [1024, DEFAULT_MAX_PACKET_BYTES, 65535];

// -- part writing, for the wire-level fixtures and the proptest grammar -------------------------
//
// Hand-rolled, not `logit_proto::collectd::part`'s writers: the encoder's own code can't prove
// it writes what a collectd sender does, or express legacy parts.

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
const TYPE_MESSAGE: u16 = 0x0100;
const TYPE_SEVERITY: u16 = 0x0101;

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

/// Decodes every datagram in order through one decoder, as `collectd_in` does.
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

/// One batch over `events`, sharing `resource`.
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

/// The same two properties starting from wire bytes, which reaches shapes the encoder never
/// emits: a legacy `Time`/`Interval` part, a non-UTF-8 identity, a sender's own elision pattern.
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

/// One event carrying one record named as a decoded single-data-source `load`/`load` list is.
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

/// The whole range of collectd's integer types, including `u64::MAX` and `i64::MAX`, which both
/// round *up* to a power of two as doubles.
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

/// A NaN gauge's *flag* survives the round trip.
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

/// A multi-data-source list is **one** event of N records, re-encoded as one list.
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

/// A mix of data-source types in one list.
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

/// No `collectd.interval` leaves as `IntervalHR 0` and comes back absent, not `F64(0.0)`.
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

/// Events sharing an identity: elided parts come back from sticky state, including across the
/// datagram boundary cap 1024 forces.
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

/// Normalization 1: legacy `Time`/`Interval` re-emit as HR parts; the *model* is a fixed point.
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

/// Normalization 2, deterministically: a `TimeHR` and an `IntervalHR` with sub-second bits may
/// come back one tick away in bytes, while the model's timestamp doesn't move.
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

/// A sender's elision pattern (one Host/Plugin/Type for three lists) decodes to the same events
/// after the encoder re-derives elision.
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

/// A non-UTF-8 host rides as a `Value::Bytes` and is written back byte-verbatim.
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

/// A NaN gauge off the wire.
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

/// **Record names cannot affect the fixed point**: a `load` packet decoded with and without a
/// `types.db` encodes to byte-identical datagrams.
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

    // The `types.db`-named batch is a fixed point against a decoder holding the same file. Not
    // via `assert_fixed_point`, whose decoder has no `types.db` and would come back index-named.
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

    // The index-named batch, against an ordinary decoder.
    assert_fixed_point(batch(plain_events));
}

// -- notification fixtures -----------------------------------------------------------------------

/// A notification sharing sticky identity with the value list before it, as a `threshold` plugin
/// produces alongside `load` reads.
fn notification_after_a_list(severity: u64, message: &[u8]) -> Bytes {
    PacketBuilder::new()
        .string(TYPE_HOST, b"web-1")
        .number(TYPE_TIME_HR, 1_700_000_000u64 << 30)
        .string(TYPE_PLUGIN, b"load")
        .string(TYPE_TYPE, b"load")
        .values(&[(DS_GAUGE, 0.5f64.to_le_bytes())])
        .number(TYPE_SEVERITY, severity)
        .string(TYPE_MESSAGE, message)
        .build()
}

#[test]
fn every_severity_notification_is_a_fixed_point() {
    for severity in [1u64, 2, 4] {
        assert_wire_fixed_point(notification_after_a_list(severity, b"threshold exceeded"), 2);
    }
}

/// A notification with no plugin or type, which is legal, unlike a value list.
#[test]
fn a_notification_with_no_plugin_or_type_is_a_fixed_point() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_SEVERITY, 1)
            .string(TYPE_MESSAGE, b"host is down")
            .build(),
        1,
    );
}

/// A notification carrying its own plugin/type/instance.
#[test]
fn a_notification_with_plugin_and_type_is_a_fixed_point() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .string(TYPE_PLUGIN, b"df")
            .string(TYPE_PLUGIN_INSTANCE, b"root")
            .string(TYPE_TYPE, b"df_complex")
            .string(TYPE_TYPE_INSTANCE, b"free")
            .number(TYPE_SEVERITY, 2)
            .string(TYPE_MESSAGE, b"disk almost full")
            .build(),
        1,
    );
}

/// A message at exactly the 255-byte limit (`NOTIF_MAX_MSG_LEN - 1`) round-trips untouched.
#[test]
fn a_message_at_exactly_255_bytes_is_a_fixed_point() {
    let message = vec![b'm'; 255];
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_SEVERITY, 4)
            .string(TYPE_MESSAGE, &message)
            .build(),
        1,
    );
}

/// A non-UTF-8 message rides byte-verbatim, like a non-UTF-8 identity field.
#[test]
fn a_non_utf8_message_is_a_fixed_point() {
    assert_wire_fixed_point(
        PacketBuilder::new()
            .string(TYPE_HOST, b"web-1")
            .number(TYPE_SEVERITY, 2)
            .string(TYPE_MESSAGE, &[0xFF, 0xFE, b'm'])
            .build(),
        1,
    );
}

// -- the generated grammar -------------------------------------------------------------------

/// One generated value list: its identity and data sources. `None` for an identity field means
/// "don't write the part", inheriting the sticky one, which is how a sender elides.
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

/// A generated time or interval, as legacy whole seconds or raw `cdtime_t` ticks (HR). An enum so
/// each branch generates its own *values*: a whole-second `TimeHR` exercises no sub-second
/// arithmetic.
#[derive(Debug, Clone, Copy)]
enum GenNumber {
    Legacy(u64),
    Hr(u64),
}

/// `[A-Za-z0-9._-]{1,20}`: no byte the codec sanitizes, and too short to truncate. Both are tested
/// in `encode.rs`.
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

/// A list's time: legacy whole seconds, or an **arbitrary** `cdtime_t` in `[2^60, 2^61)` (roughly
/// 2004..2038) with all 30 sub-second bits live, which is what reaches normalization 2.
fn gen_time() -> impl Strategy<Value = GenNumber> {
    prop_oneof![
        (1u64..2_000_000_000).prop_map(GenNumber::Legacy),
        ((1u64 << 60)..(2u64 << 60)).prop_map(GenNumber::Hr),
    ]
}

/// A list's interval: legacy whole seconds, or an arbitrary `cdtime_t` below 2^53 ticks (~97
/// days), the exact-integer range of the `Value::F64` that carries `collectd.interval`.
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

/// An optional notification after one generated list: a valid severity and a non-empty message
/// from [`identity_string`]'s alphabet (the drop cases are tested in `encode.rs`).
fn gen_notification() -> impl Strategy<Value = (u64, String)> {
    (prop_oneof![Just(1u64), Just(2u64), Just(4u64)], identity_string())
}

/// `lists` with one optional notification *per list*, dispatched after that list's Values part, so
/// a notification can precede a later list or trail the packet. Generated together so
/// `notifications.len() == lists.len()`.
fn gen_lists_and_notifications() -> impl Strategy<Value = (Vec<GenList>, Vec<Option<(u64, String)>>)>
{
    proptest::collection::vec(gen_list(), 1..8).prop_flat_map(|lists| {
        let len = lists.len();
        proptest::collection::vec(proptest::option::of(gen_notification()), len)
            .prop_map(move |notifications| (lists.clone(), notifications))
    })
}

/// Renders generated lists into one datagram, writing only the parts each list names. The first
/// list always gets a full host/plugin/type (collectd rejects one without), so every notification
/// (`notifications[i]`, after list `i`) has a host.
fn render(lists: &[GenList], notifications: &[Option<(u64, String)>]) -> Bytes {
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
            // Only the first list needs a default; after it, the time is sticky.
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
        if let Some((severity, message)) = &notifications[index] {
            builder =
                builder.number(TYPE_SEVERITY, *severity).string(TYPE_MESSAGE, message.as_bytes());
        }
    }
    builder.build()
}

proptest! {
    // 256 cases x 3 caps x 2 encode passes, inside `tests/robustness.rs`'s "keep this suite
    // fast" budget.
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The fixed point over generated packets, from the *first hop* on (normalization 2):
    ///
    /// - `d1 = decode(p)`, `e1 = encode(d1)`, `d2 = decode(e1)`, `e2 = encode(d2)`;
    /// - `d2 == decode(e2)`: the model is a fixed point;
    /// - `e2 == encode(decode(e2))`: so are the bytes, from `e1` onward.
    ///
    /// `d1 == d2` holds *unconditionally*: the ≤1-tick drift is `cdtime -> ns -> cdtime`, which
    /// lives in the bytes, while `ns -> cdtime -> ns` is exact, so the model's timestamp never
    /// moves.
    #[test]
    fn a_generated_packet_is_a_fixed_point_at_every_cap(
        (lists, notifications) in gen_lists_and_notifications()
    ) {
        let packet = render(&lists, &notifications);
        let resource = Arc::new(Resource::default());
        let mut decoder = CollectdDecoder::new(resource.clone());
        let mut d1 = Vec::new();
        decoder.decode_into(packet, RECEIVED_AT, &mut d1).expect("a generated packet must decode");
        let notif_count = notifications.iter().filter(|n| n.is_some()).count();
        prop_assert_eq!(d1.len(), lists.len() + notif_count);

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
