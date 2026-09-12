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
//! bytes rather than a hand-built batch, so the fixtures above stay readable while the coverage
//! (elision patterns, data-source type mixes, legacy vs. high-resolution parts) is not limited to
//! what anyone thought to write down.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Sum, Temporality, Value,
};
use logit_proto::collectd::{
    CollectdDecoder, CollectdEncoder, Packets, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN,
    ATTR_PLUGIN_INSTANCE, ATTR_TYPE, ATTR_TYPE_INSTANCE, DEFAULT_MAX_PACKET_BYTES,
};
use logit_proto::Decoder;
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
    let mut encoder = CollectdEncoder::new();
    let mut packets = Packets::default();
    encoder.encode_into(batch, cap, &mut packets);
    packets.iter().map(|(bytes, _)| bytes.to_vec()).collect()
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
    time: Option<(bool, u64)>,
    interval: Option<(bool, u64)>,
    values: Vec<(u8, [u8; 8])>,
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

fn gen_list() -> impl Strategy<Value = GenList> {
    (
        proptest::option::of(identity_string()),
        proptest::option::of(identity_string()),
        proptest::option::of(proptest::option::of(identity_string())),
        proptest::option::of(identity_string()),
        proptest::option::of(proptest::option::of(identity_string())),
        // `bool` picks the legacy (seconds) or high-resolution (cdtime) spelling of each part.
        proptest::option::of((any::<bool>(), 1u64..2_000_000_000)),
        proptest::option::of((any::<bool>(), 0u64..3600)),
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
        if let Some((legacy, value)) = list.time {
            builder = if legacy {
                builder.number(TYPE_TIME, value)
            } else {
                builder.number(TYPE_TIME_HR, value << 30)
            };
        } else if first {
            builder = builder.number(TYPE_TIME_HR, 1_700_000_000u64 << 30);
        }
        if let Some((legacy, value)) = list.interval {
            builder = if legacy {
                builder.number(TYPE_INTERVAL, value)
            } else {
                builder.number(TYPE_INTERVAL_HR, value << 30)
            };
        }
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

    /// The whole point, over generated packets: `decode(encode(decode(p))) == decode(p)`, and
    /// `encode` of both is byte-identical -- at every cap, so a re-chosen datagram boundary changes
    /// the bytes but never the events.
    #[test]
    fn a_generated_packet_is_a_fixed_point_at_every_cap(
        lists in proptest::collection::vec(gen_list(), 1..8)
    ) {
        let packet = render(&lists);
        let resource = Arc::new(Resource::default());
        let mut decoder = CollectdDecoder::new(resource.clone());
        let mut first = Vec::new();
        decoder.decode_into(packet, RECEIVED_AT, &mut first).expect("a generated packet must decode");
        prop_assert_eq!(first.len(), lists.len());

        let batch = EventBatch { resource: resource.clone(), scope: None, events: first };
        for cap in CAPS {
            let encoded = encode_at(&batch, cap);
            let decoded = decode_all(&encoded, &resource);
            prop_assert_eq!(&decoded, &batch.events, "cap {}", cap);

            let again = EventBatch { resource: resource.clone(), scope: None, events: decoded };
            prop_assert_eq!(encode_at(&again, cap), encoded, "cap {} is not byte-idempotent", cap);
        }
    }
}
