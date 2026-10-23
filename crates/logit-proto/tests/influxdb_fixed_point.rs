//! Pure-codec InfluxDB line-protocol fixed-point tests, against [`InfluxLineEncoder`] and
//! [`InfluxDecoder`] with no pipeline or socket: `influxdb_in -> influxdb_out` is a fixed point
//! modulo the "Permitted normalizations" list in `logit_proto::influxdb`'s module doc.
//!
//! Two properties, per fixture:
//!
//! 1. **`decode(encode(b)) == b`**, whole-event `PartialEq`. Every fixture has the shape a real
//!    decode produces (the measurement carrier, `Value::Str` tags, typed `Gauge`s named
//!    `<measurement>.<field>`, string fields as `_logit.field.*`), so the check isn't a
//!    tautology.
//! 2. **`encode(decode(encode(b))) == encode(b)`** on bytes, which catches a codec that writes two
//!    byte strings for one batch.
//!
//! The `proptest` at the bottom generates 1-6 events, each with:
//!
//! - a measurement that starts with a letter (a leading `#` would make a comment) and may hold
//!   `,`, `=`, and spaces, which the encoder escapes;
//! - 0-4 tags with distinct keys outside the reserved names and the private `_logit.` prefix,
//!   keys and values that may hold `,`, `=`, spaces, and `"`; no backslash in any name, since a backslash before a delimiter has no spelling
//!   (normalization 6);
//! - 1-4 numeric or boolean fields of every type, any finite `f64` (normalization 5), and 0-2
//!   string fields with any characters, quotes, backslashes, and newlines included, all keys
//!   distinct and none `time`;
//! - a timestamp anywhere in influxd's range, distinct per event so no two events share a series
//!   and instant (normalization 2 would merge them).

use bytes::Bytes;
use logit_core::attrs::FIELD_PREFIX;
use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, Event, EventBatch, MetricKind, MetricRecord, MetricValue, Resource, Value,
};
use logit_proto::graphite::GraphiteEncoder;
use logit_proto::influxdb::encode::InfluxLineEncoder;
use logit_proto::influxdb::line::{MAX_TIMESTAMP_NS, MIN_TIMESTAMP_NS};
use logit_proto::influxdb::{
    InfluxDecoder, Precision, ATTR_MEASUREMENT, RESOURCE_ATTR_BUCKET, RESOURCE_ATTR_ORG,
    RESOURCE_ATTR_ORG_ID,
};
use logit_proto::prometheus::{events_to_families, PrometheusEncoder};
use logit_proto::{Encoder, FramedEncoder, MessageBuf};
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
        attributes.insert(&format!("{FIELD_PREFIX}{key}"), Value::from(*value));
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

/// The measurement, the string fields, and the request's org and bucket ride as private
/// `_logit.influxdb.*` carriers: `influxdb_out` rebuilds the line from them, and a foreign sink
/// (`prometheus_out`, `graphite_out`) writes none of them as a label or tag.
#[test]
fn the_carriers_reach_influxdb_out_and_no_foreign_sink() {
    let mut resource = Resource::default();
    resource.attributes.insert(RESOURCE_ATTR_ORG, "acme");
    resource.attributes.insert(RESOURCE_ATTR_ORG_ID, "0123456789abcdef");
    resource.attributes.insert(RESOURCE_ATTR_BUCKET, "metrics");
    let resource = Arc::new(resource);
    let wire = "cpu,host=a usage=1.5,idle=98.5,note=\"free text\" 1700000000000000000\n";
    let decoded = decode(Bytes::from_static(wire.as_bytes()), &resource);
    assert_eq!(decoded.len(), 1);
    let batch = EventBatch { resource: resource.clone(), scope: None, events: decoded };

    let families = events_to_families(
        batch.events.iter().map(|e| (&*resource, e)),
        &mut PrometheusEncoder::new(),
    );
    assert_eq!(families.len(), 2, "{families:?}");
    for family in &families {
        for series in &family.series {
            let names: Vec<&str> = series.labels.iter().map(|(name, _)| name.as_str()).collect();
            assert_eq!(names, ["host"], "prometheus_out labels on {}", family.name);
        }
    }

    let mut graphite = GraphiteEncoder::new();
    let mut lines: MessageBuf<usize> = MessageBuf::default();
    graphite.encode_into(&batch, &mut lines);
    let lines: Vec<String> =
        lines.iter().map(|line| String::from_utf8(line.to_vec()).unwrap()).collect();
    assert_eq!(lines.len(), 2, "{lines:?}");
    for line in &lines {
        let path = line.split(' ').next().unwrap();
        assert!(path.ends_with(";host=a"), "graphite_out tags on {line:?}");
        assert!(!line.contains("influxdb"), "graphite_out tags on {line:?}");
    }

    assert_eq!(encode(&batch), Bytes::from_static(wire.as_bytes()), "the measurement survives");
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
    matches!(key, "time" | "_measurement" | "_field") || logit_core::attrs::is_private(key)
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

/// A tag key or value with `\` before a space leaves as `\\` and a bare space, so the line
/// splitter leaves the tag section early: a `"` in a tag value then opens a quote
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

/// A tag key or value with `\` before `,` or `=` leaves as `\\,` or `\\=`, which reads back as
/// written: the line splitter counts `,` and `=` only after a bare space, so the pair can't end
/// the tag section early. A `"` in the tags and a string field's newline stay in their line.
#[test]
fn a_tag_with_a_backslash_before_a_comma_or_equals_is_a_fixed_point() {
    let neighbor = point("n", &[], &[("value", MetricValue::I64(2))], &[], 6);
    assert_fixed_point(batch(vec![
        point(
            "m",
            &[("a", "v\\=x"), ("k\\,x", "c\"d"), ("k\\=y", "x\\,y\"z")],
            &[("value", MetricValue::I64(1))],
            &[],
            5,
        ),
        neighbor.clone(),
    ]));
    assert_fixed_point(batch(vec![
        point("m", &[("t", "x\\=y\\,z\"")], &[], &[("s", "one\ntwo")], 5),
        neighbor,
    ]));
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

/// A fuzz crash input (`fuzz/seeds/influxdb_line/regress-*`, the first byte the precision
/// selector) decoded, then encoded: each key in `unencodable` is dropped and counted, and what is
/// written decodes and is a fixed point from there.
fn assert_crash_input_is_a_fixed_point(input: &[u8], unencodable: usize) {
    let resource = Arc::new(Resource::default());
    let source = batch(decode(Bytes::copy_from_slice(&input[1..]), &resource));
    let mut encoder = InfluxLineEncoder::default();
    let encoded = encoder.encode(&source).expect("the encoder never fails a batch");
    assert_eq!(encoder.stats().dropped_unencodable_key, unencodable, "wire: {encoded:?}");
    let decoded = decode(encoded.clone(), &resource);
    assert_eq!(encode(&batch(decoded)), encoded, "encode(decode(encode(b))) == encode(b)");
}

/// The field key `\r` has no spelling and drops, which leaves the NUL-led key the line's only
/// field, first after the space before the fields, where influxd skips a NUL as whitespace:
/// written, the line reads as a missing field key, so it drops too.
#[test]
fn a_field_key_starting_with_a_nul_is_never_written() {
    assert_crash_input_is_a_fixed_point(b":a \r=0,\0=0", 2);
}

/// As above, with the dropped key a backslash-escaped newline the line splitter keeps in the line.
#[test]
fn a_field_key_starting_with_a_nul_after_an_escaped_newline_is_never_written() {
    assert_crash_input_is_a_fixed_point(b"\nE \\\n=-0,\0=4", 2);
}

/// Encodes `source` and returns the decoded events, each event's metrics sorted by name, with the
/// `unencodable_key` count: a field whose key starts with a tab or NUL may move (normalization 4).
fn round_trip_by_key(source: &EventBatch) -> (Vec<Event>, usize, Bytes) {
    let mut encoder = InfluxLineEncoder::default();
    let encoded = encoder.encode(source).expect("the encoder never fails a batch");
    let dropped = encoder.stats().dropped_unencodable_key;
    (sorted_by_key(decode(encoded.clone(), &source.resource)), dropped, encoded)
}

fn sorted_by_key(mut events: Vec<Event>) -> Vec<Event> {
    for event in &mut events {
        event.metrics.sort_by_key(|m| resolve(m.name).to_owned());
    }
    events
}

/// A measurement starting with a tab or NUL loses that byte at the line's start, where influxd
/// skips whitespace, so it is dropped and counted; the rest of the batch survives. A field key
/// starting with one beside a plain key is kept, written second.
#[test]
fn a_measurement_starting_with_a_tab_or_nul_is_dropped_and_a_field_key_is_kept() {
    let neighbor = point("n", &[], &[("f", MetricValue::I64(1))], &[], 6);
    let kept = point("m", &[], &[("\tf", MetricValue::I64(1)), ("g", MetricValue::I64(2))], &[], 5);
    let source = batch(vec![
        point("\tm", &[], &[("f", MetricValue::I64(1))], &[], 5),
        point("\0m", &[], &[("f", MetricValue::I64(1))], &[], 5),
        kept.clone(),
        neighbor.clone(),
    ]);
    let (decoded, dropped, encoded) = round_trip_by_key(&source);
    assert_eq!(dropped, 2, "wire: {encoded:?}");
    assert_eq!(decoded, sorted_by_key(vec![kept, neighbor]), "wire: {encoded:?}");
}

/// After the `,` between fields influxd skips nothing, so a field key starting with a tab or NUL
/// reads back whole anywhere but first: the encoder writes a plain-keyed field first, numeric or
/// string, and every field round-trips with nothing dropped.
#[test]
fn a_tab_or_nul_led_field_key_is_written_after_a_plain_one() {
    let source = batch(vec![
        point("m", &[], &[("a", MetricValue::I64(1)), ("\0b", MetricValue::I64(2))], &[], 1),
        point(
            "m",
            &[],
            &[
                ("\0b", MetricValue::I64(2)),
                ("\tc", MetricValue::F64(0.5)),
                ("a", MetricValue::I64(1)),
            ],
            &[],
            2,
        ),
        point("m", &[], &[("\0b", MetricValue::Bool(true))], &[("s", "text")], 3),
        point("m", &[], &[("a", MetricValue::U64(7))], &[("\ts", "text")], 4),
    ]);
    let (decoded, dropped, encoded) = round_trip_by_key(&source);
    assert_eq!(dropped, 0, "wire: {encoded:?}");
    assert_eq!(decoded, sorted_by_key(source.events.clone()), "wire: {encoded:?}");
    let first = encoded.split(|&b| b == b'\n').next().unwrap();
    assert_eq!(first, b"m a=1i,\0b=2i 1", "wire: {encoded:?}");
}

/// A line whose every field key starts with a tab or NUL has no field that can lead it: each
/// field is dropped and counted, and no line is written.
#[test]
fn an_all_tab_or_nul_led_field_set_is_dropped() {
    let neighbor = point("n", &[], &[("f", MetricValue::I64(1))], &[], 6);
    let source = batch(vec![
        point(
            "m",
            &[],
            &[("\tf", MetricValue::I64(1)), ("\0g", MetricValue::I64(1))],
            &[("\0s", "x")],
            5,
        ),
        neighbor.clone(),
    ]);
    let (decoded, dropped, encoded) = round_trip_by_key(&source);
    assert_eq!(dropped, 3, "wire: {encoded:?}");
    assert_eq!(decoded, vec![neighbor], "wire: {encoded:?}");
}
