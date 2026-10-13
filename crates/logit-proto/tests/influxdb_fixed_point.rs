//! Pure-codec InfluxDB line-protocol fixed-point tests, against [`InfluxLineEncoder`] and
//! [`InfluxDecoder`] with no pipeline or socket: `influxdb_in -> influxdb_out` is a fixed point
//! modulo the "Permitted normalizations" list in `logit_proto::influxdb`'s module doc.
//!
//! Two properties, per fixture:
//!
//! 1. **`decode(encode(b)) == b`**, whole-event `PartialEq`. Every fixture has the shape a real
//!    decode produces (the measurement carrier, `Value::Str` tags, typed `Gauge`s named
//!    `<measurement>.<field>`, string fields as `influxdb.field.*`), so the check isn't a
//!    tautology.
//! 2. **`encode(decode(encode(b))) == encode(b)`** on bytes, which catches a codec that writes two
//!    byte strings for one batch.
//!
//! The `proptest` at the bottom generates 1-6 events, each with:
//!
//! - a measurement that starts with a letter (a leading `#` would make a comment) and may hold
//!   `,`, `=`, and spaces, which the encoder escapes;
//! - 0-4 tags with distinct keys outside the reserved names and the `influxdb.`/`statsd.`
//!   carriers, keys and values that may hold `,`, `=`, spaces, and `"`; no backslash in any name,
//!   since a backslash before a delimiter has no spelling (normalization 6);
//! - 1-4 numeric or boolean fields of every type, any finite `f64` (normalization 5), and 0-2
//!   string fields with any characters, quotes, backslashes, and newlines included, all keys
//!   distinct and none `time`;
//! - a timestamp anywhere in influxd's range, distinct per event so no two events share a series
//!   and instant (normalization 2 would merge them).

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, MetricValue, Resource, Value,
};
use logit_proto::influxdb::encode::InfluxLineEncoder;
use logit_proto::influxdb::line::{MAX_TIMESTAMP_NS, MIN_TIMESTAMP_NS};
use logit_proto::influxdb::{InfluxDecoder, Precision, ATTR_MEASUREMENT, FIELD_ATTR_PREFIX};
use logit_proto::Encoder;
use proptest::prelude::*;
use std::sync::Arc;

#[path = "support/influxdb_lines.rs"]
mod influxdb_lines;

use influxdb_lines::INFLUX_LINES;

const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;

// -- the harness ---------------------------------------------------------------------------------

fn encode(batch: &EventBatch) -> Bytes {
    InfluxLineEncoder::default().encode(batch).expect("the encoder never fails a batch")
}

fn decode(body: Bytes, resource: &Arc<Resource>) -> Vec<Event> {
    InfluxDecoder::new()
        .decode_body(body, Precision::Ns, RECEIVED_AT, resource.clone())
        .unwrap_or_else(|err| panic!("every body this encoder writes must decode: {err}"))
        .events
}

fn assert_fixed_point(batch: EventBatch) {
    let encoded = encode(&batch);
    assert!(!encoded.is_empty(), "the fixture encoded to nothing");

    // Property 1: decode(encode(b)) == b.
    let decoded = decode(encoded.clone(), &batch.resource);
    assert_eq!(decoded, batch.events, "decode(encode(b)) must equal b; wire: {encoded:?}");

    // Property 2: encode(decode(encode(b))) == encode(b), on bytes.
    let round_tripped =
        EventBatch { resource: batch.resource.clone(), scope: None, events: decoded };
    assert_eq!(encode(&round_tripped), encoded, "encode(decode(encode(b))) must equal encode(b)");
}

// -- fixtures ------------------------------------------------------------------------------------

fn batch(events: Vec<Event>) -> EventBatch {
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
}

/// One point in the shape a decode produces.
fn point(
    measurement: &str,
    tags: &[(&str, &str)],
    fields: &[(&str, MetricValue)],
    strings: &[(&str, &str)],
    timestamp: i64,
) -> Event {
    let mut attributes = AttrMap::new();
    for (key, value) in tags {
        attributes.insert(key, Value::from(*value));
    }
    attributes.insert(ATTR_MEASUREMENT, Value::from(measurement));
    for (key, value) in strings {
        attributes.insert(&format!("{FIELD_ATTR_PREFIX}{key}"), Value::from(*value));
    }
    let mut event = Event::empty(timestamp, attributes);
    for (key, value) in fields {
        event.metrics.push(MetricRecord::new(
            intern(&format!("{measurement}.{key}")),
            MetricKind::Gauge(*value),
        ));
    }
    event
}

#[test]
fn a_typed_multi_field_point_is_a_fixed_point() {
    assert_fixed_point(batch(vec![point(
        "cpu",
        &[("host", "web-1"), ("cpu", "cpu0")],
        &[
            ("usage_user", MetricValue::F64(1.5)),
            ("count", MetricValue::I64(-3)),
            ("big", MetricValue::U64(u64::MAX)),
            ("up", MetricValue::Bool(true)),
        ],
        &[("note", "hello, \"world\" \\ done")],
        1_700_000_000_000_000_000,
    )]));
}

#[test]
fn a_string_only_point_is_a_fixed_point() {
    assert_fixed_point(batch(vec![point("log", &[], &[], &[("msg", "line1\nline2")], 1)]));
}

#[test]
fn escaped_names_are_a_fixed_point() {
    assert_fixed_point(batch(vec![point(
        "my measurement,with=eq",
        &[("tag key,=", "value with spaces, commas=and \"quotes\"")],
        &[("field key,= x", MetricValue::F64(0.25))],
        &[],
        42,
    )]));
}

#[test]
fn extreme_values_and_timestamps_are_a_fixed_point() {
    assert_fixed_point(batch(vec![
        point("m", &[], &[("f", MetricValue::F64(f64::MAX))], &[], MAX_TIMESTAMP_NS),
        point("m", &[], &[("f", MetricValue::F64(-0.0))], &[], MIN_TIMESTAMP_NS),
        point("m", &[], &[("f", MetricValue::F64(5e-324))], &[], 0),
        point("m", &[], &[("f", MetricValue::I64(i64::MIN))], &[], -1),
    ]));
}

/// The Telegraf-shaped fixture: its first decode applies normalizations 2, 5, and 6 (the
/// `system` lines merge), and from there it's a fixed point.
#[test]
fn the_telegraf_fixture_is_a_fixed_point_once_decoded() {
    let resource = Arc::new(Resource::default());
    let decoded = decode(Bytes::from_static(INFLUX_LINES.as_bytes()), &resource);
    assert_eq!(decoded.len(), 18, "the three `system` lines at one instant merge");
    assert_fixed_point(batch(decoded));
}

/// Wire shapes the encoder never writes decode into a batch that is a fixed point from there on.
#[test]
fn a_wire_shape_the_encoder_never_emits_is_a_fixed_point_once_decoded() {
    for wire in [
        "m,b=2,a=1 f=1.50,g=T 5\n",
        "   m   f=1e3   5   \n# comment\n\n",
        "m f=1i,f=2i 5\n",
        "m x=1i 5\nm y=\"s\" 5\nm x=2i 5\n",
        "m\\=x,t=a\\\\b f=007i 5\n",
    ] {
        let resource = Arc::new(Resource::default());
        let decoded = decode(Bytes::from(wire.to_string()), &resource);
        assert_eq!(decoded.len(), 1, "{wire:?}");
        assert_fixed_point(batch(decoded));
    }
}

// -- properties ----------------------------------------------------------------------------------

fn name_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_ ,=.-]{0,10}"
}

fn tag_value_strategy() -> impl Strategy<Value = String> {
    "[A-Za-z0-9_ ,=.\"-]{1,12}"
}

fn metric_value_strategy() -> impl Strategy<Value = MetricValue> {
    prop_oneof![
        any::<f64>()
            .prop_filter("line protocol carries only finite floats", |v| v.is_finite())
            .prop_map(MetricValue::F64),
        any::<i64>().prop_map(MetricValue::I64),
        any::<u64>().prop_map(MetricValue::U64),
        any::<bool>().prop_map(MetricValue::Bool),
    ]
}

/// Whether a tag key is one the decoder rejects or drops, or the encoder skips.
fn reserved_tag(key: &str) -> bool {
    matches!(key, "time" | "_measurement" | "_field")
        || key.starts_with("influxdb.")
        || key.starts_with("statsd.")
}

#[derive(Debug, Clone)]
struct GenPoint {
    measurement: String,
    tags: Vec<(String, String)>,
    fields: Vec<(String, MetricValue)>,
    strings: Vec<(String, String)>,
}

fn gen_point() -> impl Strategy<Value = GenPoint> {
    (
        "[A-Za-z][A-Za-z0-9_ ,=.-]{0,12}",
        proptest::collection::vec((name_strategy(), tag_value_strategy()), 0..5),
        proptest::collection::vec((name_strategy(), metric_value_strategy()), 1..5),
        proptest::collection::vec((name_strategy(), any::<String>()), 0..3),
    )
        .prop_map(|(measurement, tags, fields, strings)| {
            let mut seen_tags = Vec::new();
            let tags = tags
                .into_iter()
                .filter(|(k, _)| {
                    let keep = !reserved_tag(k) && !seen_tags.contains(k);
                    seen_tags.push(k.clone());
                    keep
                })
                .collect();
            let mut seen_fields: Vec<String> = Vec::new();
            let mut distinct = |k: &String| {
                let keep = k != "time" && !seen_fields.contains(k);
                seen_fields.push(k.clone());
                keep
            };
            let fields: Vec<_> = fields.into_iter().filter(|(k, _)| distinct(k)).collect();
            let strings = strings.into_iter().filter(|(k, _)| distinct(k)).collect();
            GenPoint { measurement, tags, fields, strings }
        })
        .prop_filter("a point needs a field", |p| !p.fields.is_empty() || !p.strings.is_empty())
}

fn batch_strategy() -> impl Strategy<Value = EventBatch> {
    (proptest::collection::vec(gen_point(), 1..7), MIN_TIMESTAMP_NS..=MAX_TIMESTAMP_NS - 10)
        .prop_map(|(points, base)| {
            let events = points
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let tags: Vec<(&str, &str)> =
                        p.tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    let fields: Vec<(&str, MetricValue)> =
                        p.fields.iter().map(|(k, v)| (k.as_str(), *v)).collect();
                    let strings: Vec<(&str, &str)> =
                        p.strings.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                    point(&p.measurement, &tags, &fields, &strings, base + i as i64)
                })
                .collect();
            batch(events)
        })
}

proptest! {
    /// The fixed point over a generated, already-normalized corpus.
    #[test]
    fn every_generated_batch_round_trips(batch in batch_strategy()) {
        let encoded = encode(&batch);
        let decoded = decode(encoded.clone(), &batch.resource);
        prop_assert_eq!(&decoded, &batch.events);

        let round_tripped =
            EventBatch { resource: batch.resource.clone(), scope: None, events: decoded };
        prop_assert_eq!(encode(&round_tripped), encoded);
    }
}

/// Found by `fuzz/fuzz_targets/influxdb_line.rs`. A field key holding `\=` leaves as `\\=`, which
/// influxd's line splitter (mirrored by `logit_proto::influxdb::line::lines`) reads as a skipped
/// `\\` pair and a bare `=`, so a raw `"` later in the key opens a quote and swallows the line's
/// newline. A key holding `\,` miscounts the other way, so a later string value's opening quote
/// is missed and a newline inside it ends the line.
#[test]
fn a_field_key_that_desyncs_the_line_splitter_is_never_written() {
    for key in ["a\\=b\"c", "x\\,"] {
        let neighbor = point("n", &[], &[("f", MetricValue::I64(1))], &[], 6);
        let source = batch(vec![point("m", &[], &[], &[(key, "one\ntwo")], 5), neighbor.clone()]);
        let decoded = decode(encode(&source), &source.resource);
        // Written escaped or skipped, never written so it breaks the next line.
        assert!(decoded.contains(&neighbor), "{key:?}: the next point survives");
    }
}

/// A tag key or value with `\` before `,`, space, or `=` leaves as `\\` and a bare delimiter,
/// so the line splitter leaves the tag section early: a `"` in a tag value then opens a quote
/// that merges the next line, and a string field's newline splits the line. The tag is dropped
/// instead, and the rest of the batch survives.
#[test]
fn a_tag_that_desyncs_the_line_splitter_is_never_written() {
    let neighbor = point("n", &[], &[("value", MetricValue::I64(2))], &[], 6);
    let cases = [
        point("m", &[("a\\ b", "c\"d")], &[("value", MetricValue::I64(1))], &[], 5),
        point("m", &[("t", "x\\ y\\,z")], &[], &[("s", "one\ntwo")], 5),
    ];
    for source in cases {
        let source = batch(vec![source, neighbor.clone()]);
        let encoded = encode(&source);
        let decoded = decode(encoded.clone(), &source.resource);
        assert!(decoded.contains(&neighbor), "the next point survives; wire: {encoded:?}");
        let round_tripped =
            EventBatch { resource: source.resource.clone(), scope: None, events: decoded };
        assert_eq!(encode(&round_tripped), encoded, "encode(decode(encode(b))) == encode(b)");
    }
}

#[test]
fn a_tag_value_starting_with_an_equals_sign_is_a_fixed_point() {
    assert_fixed_point(batch(vec![point(
        "m",
        &[("t", "=v")],
        &[("f", MetricValue::I64(1))],
        &[],
        7,
    )]));
}
