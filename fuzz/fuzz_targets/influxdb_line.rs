//! One `/api/v2/write` body through `InfluxDecoder::decode_body`, as `influxdb_in` hands it over.
//! The first byte picks the `precision` (`0` ns, `1` us, `2` ms, `3` s, modulo 4); the rest is the
//! body.
//!
//! Oracles, each over every input:
//! - all or nothing: the body decodes if and only if every logical line from `line::lines`
//!   parses alone and its scaled timestamp is in range, and a failure's `line` is the 1-based
//!   index of the first that doesn't, with that line's reason. A line holding no newline also
//!   decodes, or fails, as a body of its own;
//! - merging: the body is one event per distinct measurement, tag set, and timestamp among its
//!   points, and no more events than points;
//! - the model: every event carries `influxdb.measurement` as a `Str`; every attribute is a
//!   `Str`; every metric is a `Gauge` named `<measurement>.<field>`; no tag under `influxdb.`
//!   survives but the carriers the decoder writes;
//! - zero copy: in a body with no backslash, nothing is unescaped, so every attribute is a slice
//!   of the body;
//! - the second-generation fixed point: re-encoding the decoded batch decodes (unless the
//!   encoder's 1 ns nudge pushed a point past the timestamp range), and encoding and decoding
//!   that once more changes neither the events nor the bytes.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_core::interner::resolve;
use logit_core::{subslice, EventBatch, MetricKind, Resource, Value};
use logit_proto::influxdb::encode::InfluxLineEncoder;
use logit_proto::influxdb::line::{self, MAX_TIMESTAMP_NS, MIN_TIMESTAMP_NS};
use logit_proto::influxdb::{InfluxDecoder, Precision, ATTR_MEASUREMENT, FIELD_ATTR_PREFIX};
use logit_proto::Encoder;
use std::collections::HashSet;
use std::sync::Arc;

/// Off any second, so truncation to a precision moves it.
const RECEIVED_AT: i64 = 1_700_000_000_123_456_789;

const PRECISIONS: [Precision; 4] = [Precision::Ns, Precision::Us, Precision::Ms, Precision::S];

fn decode(body: &[u8], precision: Precision) -> Result<EventBatch, line::LineError> {
    InfluxDecoder::new().decode_body(
        Bytes::copy_from_slice(body),
        precision,
        RECEIVED_AT,
        Arc::new(Resource::default()),
    )
}

fn encode(batch: &EventBatch) -> Bytes {
    InfluxLineEncoder::default().encode(batch).expect("fixed point: the encoder never fails")
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else { return };
    let precision = PRECISIONS[usize::from(selector % 4)];
    let shared = Bytes::copy_from_slice(body);
    let whole = InfluxDecoder::new().decode_body(
        shared.clone(),
        precision,
        RECEIVED_AT,
        Arc::new(Resource::default()),
    );

    // What each line makes alone: the first failure, and every point's series and time.
    let arrival = precision.truncate(RECEIVED_AT);
    let mut first_bad = None;
    let mut points = 0usize;
    let mut series = HashSet::new();
    for (number, raw) in line::lines(body) {
        let parsed = line::parse_line(raw).and_then(|parsed| {
            let Some(parsed) = parsed else { return Ok(None) };
            let ts = match parsed.timestamp {
                None => arrival,
                Some(t) => match precision.scale_to_ns(t) {
                    Some(ns) if (MIN_TIMESTAMP_NS..=MAX_TIMESTAMP_NS).contains(&ns) => ns,
                    _ => return Err("time outside range".into()),
                },
            };
            Ok(Some((parsed, ts)))
        });
        if !raw.contains(&b'\n') {
            assert_eq!(
                decode(raw, precision).is_err(),
                parsed.is_err(),
                "all or nothing: line {number} alone disagrees with its parse"
            );
        }
        match parsed {
            Err(reason) => {
                first_bad.get_or_insert((number, reason));
            }
            Ok(Some((parsed, ts))) => {
                points += 1;
                let tags: Vec<(String, String)> =
                    parsed.tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
                series.insert((parsed.measurement.to_string(), tags, ts));
            }
            Ok(None) => {}
        }
    }

    let batch = match (whole, first_bad) {
        (Err(err), Some((number, reason))) => {
            assert_eq!(err.line, number, "all or nothing: the first bad line");
            if !reason.starts_with("time outside range") {
                assert_eq!(err.reason, reason, "all or nothing: the first bad line's reason");
            }
            return;
        }
        (Ok(batch), None) => batch,
        (Err(err), None) => panic!("all or nothing: every line parses, yet the body failed: {err}"),
        (Ok(_), Some((number, reason))) => {
            panic!("all or nothing: line {number} fails ({reason}), yet the body decoded")
        }
    };

    assert_eq!(batch.events.len(), series.len(), "merging: one event per series and time");
    assert!(batch.events.len() <= points, "merging: no more events than points");

    for event in &batch.events {
        let Some(Value::Str(measurement)) = event.attributes.get(ATTR_MEASUREMENT) else {
            panic!("model: no measurement carrier on {event:?}");
        };
        let measurement = std::str::from_utf8(measurement).expect("model: a Str is UTF-8");
        for (key, value) in event.attributes.iter() {
            let Value::Str(bytes) = value else {
                panic!("model: attribute {} isn't a Str", resolve(key));
            };
            if !bytes.is_empty() && !body.contains(&b'\\') {
                assert!(subslice::within(&shared, bytes), "zero copy: {} is a copy", resolve(key));
            }
            let key = resolve(key);
            assert!(
                !key.starts_with("influxdb.")
                    || key == ATTR_MEASUREMENT
                    || key.starts_with(FIELD_ATTR_PREFIX),
                "model: reserved tag {key} survived"
            );
        }
        for metric in &event.metrics {
            assert!(matches!(metric.kind, MetricKind::Gauge(_)), "model: a field is a Gauge");
            let name = resolve(metric.name);
            assert!(
                name.len() > measurement.len()
                    && name.starts_with(measurement)
                    && name.as_bytes()[measurement.len()] == b'.',
                "model: {name} isn't <{measurement}>.<field>"
            );
        }
    }

    // Second generation: the first re-encode may drop what the grammar can't spell; from there
    // on nothing changes.
    let first = encode(&batch);
    let gen1 = match decode(&first, Precision::Ns) {
        Ok(gen1) => gen1,
        // Two events of one series and instant (their only difference a dropped `influxdb.` tag)
        // at the range's end: the encoder's 1 ns nudge moves the second past it.
        Err(err) if err.reason.starts_with("time outside range") => return,
        Err(err) => panic!("fixed point: the encoder's body doesn't decode: {err}"),
    };
    let second = encode(&gen1);
    let gen2 = decode(&second, Precision::Ns).expect("fixed point: the encoder's body decodes");
    assert_eq!(gen1.events, gen2.events, "fixed point: the second generation changed events");
    assert_eq!(encode(&gen2), second, "fixed point: the second generation changed bytes");
});
