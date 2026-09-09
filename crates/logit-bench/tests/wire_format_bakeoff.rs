//! The fidelity gate for the native-wire-format bake-off (`docs/design/wire-protocol.md`'s
//! "Encoding: decide with a benchmark, not up front", `docs/adr/native-wire-format-encoding.md`).
//!
//! Each arm ([`logit_bench::bakeoff`]) must round-trip every representative fixture losslessly
//! *before* any timing number from it is trusted -- a fast, lossy codec isn't a candidate. This
//! file is that gate, run as ordinary tests so a regression fails `script/test`, not just a
//! benchmark report nobody reads. The version-skew gate (an unrecognized field/value tag decoding
//! gracefully) lives closer to what it tests, in `crates/logit-proto/src/native/{value,record}.rs`'s
//! own unit tests -- only the native format claims that property, so there's nothing to compare it
//! against here.

use bytes::Bytes;
use logit_bench::{bakeoff, fixtures};
use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, LogRecord, MetricKind, MetricRecord, Resource,
    Severity, SpanKind, SpanRecord, SpanStatus, Value,
};
use logit_proto::{Signal, SignalEncoder};
use std::sync::Arc;

fn single_event_batch(event: Event) -> EventBatch {
    EventBatch { resource: Arc::new(Resource::default()), events: vec![event] }
}

/// Every fixture this gate runs each arm against -- deliberately the same shapes
/// `crates/logit-bench/src/fixtures.rs`'s own doc comment argues for (mixed, logs-only,
/// wide-JSON, distribution-heavy, span), per `AGENTS.md`'s "don't generalize a measurement from
/// one event shape".
fn representative_batches() -> Vec<EventBatch> {
    vec![
        fixtures::nginx_batch(3),
        single_event_batch(fixtures::statsd_event()),
        single_event_batch(fixtures::distribution_event()),
        single_event_batch(fixtures::distribution_heavy_event()),
        single_event_batch(fixtures::span_event()),
    ]
}

/// A field-level comparison, not a derived `PartialEq` -- `Event` doesn't implement it (nothing in
/// the production code needs to), and `MetricKind::Distribution`'s `DDSketch` has no `PartialEq` at
/// all (`crates/logit-core/src/metric.rs`), so a distribution is compared on `count()`/`quantile()`
/// instead, the same fidelity check `crates/logit-proto/src/native/record.rs`'s own tests use.
fn assert_batches_match(actual: &EventBatch, expected: &EventBatch, arm: &str) {
    assert_eq!(
        actual.resource.attributes, expected.resource.attributes,
        "{arm}: resource attributes"
    );
    assert_eq!(actual.events.len(), expected.events.len(), "{arm}: event count");
    for (a, e) in actual.events.iter().zip(expected.events.iter()) {
        assert_eq!(a.timestamp, e.timestamp, "{arm}: event timestamp");
        assert_eq!(a.attributes, e.attributes, "{arm}: event attributes");
        assert_eq!(a.log.is_some(), e.log.is_some(), "{arm}: log presence");
        if let (Some(al), Some(el)) = (&a.log, &e.log) {
            assert_eq!(al.message, el.message, "{arm}: log message");
            assert_eq!(al.severity, el.severity, "{arm}: log severity");
            assert_eq!(al.body_format, el.body_format, "{arm}: log body_format");
            assert_eq!(al.trace, el.trace, "{arm}: log trace");
        }
        assert_eq!(a.metrics.len(), e.metrics.len(), "{arm}: metric count");
        for (am, em) in a.metrics.iter().zip(e.metrics.iter()) {
            assert_eq!(am.name, em.name, "{arm}: metric name");
            assert_eq!(am.unit, em.unit, "{arm}: metric unit");
            match (&am.kind, &em.kind) {
                (MetricKind::Distribution(a), MetricKind::Distribution(b)) => {
                    assert_eq!(a.count(), b.count(), "{arm}: distribution count");
                    assert_eq!(a.quantile(0.5), b.quantile(0.5), "{arm}: distribution p50");
                    assert_eq!(a.quantile(0.99), b.quantile(0.99), "{arm}: distribution p99");
                }
                (a, b) => assert_eq!(format!("{a:?}"), format!("{b:?}"), "{arm}: metric kind"),
            }
        }
        assert_eq!(a.span.is_some(), e.span.is_some(), "{arm}: span presence");
        if let (Some(asp), Some(esp)) = (&a.span, &e.span) {
            assert_eq!(asp.trace_id, esp.trace_id, "{arm}: span trace_id");
            assert_eq!(asp.span_id, esp.span_id, "{arm}: span span_id");
            assert_eq!(asp.parent_span_id, esp.parent_span_id, "{arm}: span parent_span_id");
            assert_eq!(asp.name, esp.name, "{arm}: span name");
            assert_eq!(asp.kind, esp.kind, "{arm}: span kind");
            assert_eq!(asp.status, esp.status, "{arm}: span status");
            assert_eq!(asp.end_timestamp, esp.end_timestamp, "{arm}: span end_timestamp");
            assert_eq!(asp.events.len(), esp.events.len(), "{arm}: span events");
            assert_eq!(asp.links.len(), esp.links.len(), "{arm}: span links");
        }
    }
}

#[test]
fn native_round_trips_every_representative_fixture_losslessly() {
    for batch in representative_batches() {
        let decoded = bakeoff::native_decode(bakeoff::native_encode(&batch));
        assert_batches_match(&decoded, &batch, "native");
    }
}

#[test]
fn postcard_round_trips_every_representative_fixture_losslessly() {
    for batch in representative_batches() {
        let decoded = bakeoff::postcard_decode(&bakeoff::postcard_encode(&batch));
        assert_batches_match(&decoded, &batch, "postcard");
    }
}

#[test]
fn rkyv_round_trips_every_representative_fixture_losslessly() {
    for batch in representative_batches() {
        let decoded = bakeoff::rkyv_decode(&bakeoff::rkyv_encode(&batch));
        assert_batches_match(&decoded, &batch, "rkyv");
    }
}

// -- Type fidelity: Value::U64 above i64::MAX, and Value::Timestamp staying Timestamp -----------

fn u64_above_i64_max_batch() -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("big", Value::U64(u64::MAX));
    single_event_batch(Event::empty(0, attrs))
}

fn timestamp_value_batch() -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("when", Value::Timestamp(1_725_091_200_123_456_789));
    single_event_batch(Event::empty(0, attrs))
}

#[test]
fn native_postcard_and_rkyv_preserve_u64_above_i64_max_exactly() {
    let batch = u64_above_i64_max_batch();
    for (arm, decoded) in [
        ("native", bakeoff::native_decode(bakeoff::native_encode(&batch))),
        ("postcard", bakeoff::postcard_decode(&bakeoff::postcard_encode(&batch))),
        ("rkyv", bakeoff::rkyv_decode(&bakeoff::rkyv_encode(&batch))),
    ] {
        assert_eq!(
            decoded.events[0].attributes.get("big"),
            Some(&Value::U64(u64::MAX)),
            "{arm} should preserve U64(u64::MAX) exactly, not approximate it as a float"
        );
    }
}

#[test]
fn native_postcard_and_rkyv_preserve_the_timestamp_type_not_just_its_number() {
    let batch = timestamp_value_batch();
    for (arm, decoded) in [
        ("native", bakeoff::native_decode(bakeoff::native_encode(&batch))),
        ("postcard", bakeoff::postcard_decode(&bakeoff::postcard_encode(&batch))),
        ("rkyv", bakeoff::rkyv_decode(&bakeoff::rkyv_encode(&batch))),
    ] {
        assert_eq!(
            decoded.events[0].attributes.get("when"),
            Some(&Value::Timestamp(1_725_091_200_123_456_789)),
            "{arm} should decode back as Value::Timestamp, not Value::I64"
        );
    }
}

/// The disqualifying finding for OTLP as an *internal* transport, pinned as a test: `AnyValue` has
/// no timestamp variant and no unsigned-64 variant at all (`crates/logit-proto/src/otlp/common.rs`'s
/// own module doc), so both round-trip lossy through the OTLP codec that already ships. Not a
/// hypothetical -- this exercises the real `OtlpEncoder`/`OtlpDecoder`.
#[test]
fn otlp_collapses_timestamp_to_i64_and_loses_u64_above_i64_max() {
    let mut attrs = AttrMap::new();
    attrs.insert("when", Value::Timestamp(1_725_091_200_123_456_789));
    attrs.insert("big", Value::U64(u64::MAX));
    let log = Event::log(
        1,
        attrs,
        LogRecord {
            message: Value::str("x"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
        },
    );
    let decoded = bakeoff::otlp_round_trip(&single_event_batch(log));
    assert_eq!(decoded.len(), 1);
    let event = &decoded[0].events[0];
    assert_eq!(
        event.attributes.get("when"),
        Some(&Value::I64(1_725_091_200_123_456_789)),
        "OTLP has no Timestamp variant -- decodes back as a plain I64"
    );
    // u64::MAX exceeds f64's 2^53 exact-integer range, so it round-trips as an approximation --
    // assert it's *not* exact, which is the actual, documented failure mode.
    assert_ne!(
        event.attributes.get("big"),
        Some(&Value::U64(u64::MAX)),
        "OTLP's AnyValue has no unsigned-64 variant -- u64::MAX cannot survive exactly"
    );
}

// -- Multi-payload events: the shape ADR `multi-payload-events` exists for -----------------------

fn multi_payload_event() -> Event {
    let mut attrs = AttrMap::new();
    attrs.insert("service", "orders-api");
    let mut event = Event::empty(1, attrs);
    event.log = Some(LogRecord {
        message: Value::str("order placed"),
        severity: Some(Severity::Info),
        body_format: BodyFormat::Raw,
        trace: None,
    });
    event.metrics.push(MetricRecord {
        name: logit_core::interner::intern("bakeoff_multi_payload_metric"),
        kind: MetricKind::Counter(1.0),
        unit: None,
    });
    event.span = Some(SpanRecord {
        trace_id: [1; 16],
        span_id: [2; 8],
        parent_span_id: None,
        name: Value::str("order.place"),
        kind: SpanKind::Server,
        status: SpanStatus::Ok,
        events: Vec::new(),
        links: Vec::new(),
        end_timestamp: 2,
    });
    event
}

#[test]
fn native_postcard_and_rkyv_keep_a_log_metric_and_span_together_as_one_event() {
    let batch = single_event_batch(multi_payload_event());
    for (arm, decoded) in [
        ("native", bakeoff::native_decode(bakeoff::native_encode(&batch))),
        ("postcard", bakeoff::postcard_decode(&bakeoff::postcard_encode(&batch))),
        ("rkyv", bakeoff::rkyv_decode(&bakeoff::rkyv_encode(&batch))),
    ] {
        assert_eq!(decoded.events.len(), 1, "{arm}: should stay one event");
        let event = &decoded.events[0];
        assert!(event.log.is_some(), "{arm}: log must survive");
        assert_eq!(event.metrics.len(), 1, "{arm}: metric must survive");
        assert!(event.span.is_some(), "{arm}: span must survive");
    }
}

/// The disqualifying finding for OTLP's wire *shape*: one `EventBatch` carrying a log, a metric,
/// and a span at once (ADR `multi-payload-events`) shatters into up to three independent OTLP
/// payloads on the way out, and decodes back as up to three separate batches with no single event
/// carrying all three -- there is no "one event, several payload kinds" shape OTLP's wire protocol
/// can express (`crates/logit-proto/src/otlp/mod.rs`'s own module doc: "OTLP's wire protocol does
/// [split by signal]: logs, metrics, and traces are three separate RPCs").
#[test]
fn otlp_shatters_a_multi_payload_event_across_separate_batches() {
    let batch = single_event_batch(multi_payload_event());
    let decoded = bakeoff::otlp_round_trip(&batch);
    assert_eq!(decoded.len(), 3, "one log + one metric + one span should yield three payloads");
    for b in &decoded {
        assert_eq!(b.events.len(), 1);
        let event = &b.events[0];
        // No decoded batch's event carries more than one of the three payload kinds -- the whole
        // point being demonstrated.
        let payload_kinds = event.log.is_some() as u8
            + (!event.metrics.is_empty()) as u8
            + event.span.is_some() as u8;
        assert_eq!(payload_kinds, 1, "each shattered batch's event should carry exactly one kind");
    }
}

// -- Metrics OTLP can't express at all: GaugeDelta and Set ---------------------------------------

/// Matches the encode-side gap already tabulated in `docs/known-gaps.md`'s "Cross-protocol
/// semantic gaps" entry -- exercised here as a bake-off comparison rather than re-derived, since
/// `crates/logit-proto/src/otlp/metrics.rs` already has its own unit tests for the OTLP side
/// (`a_gauge_delta_is_skipped_and_reports_its_own_diagnostic_key`,
/// `a_set_metric_is_skipped_and_counted_rather_than_encoded_wrongly`).
#[test]
fn native_postcard_and_rkyv_preserve_gauge_delta_and_set_identity_that_otlp_drops() {
    for kind in [MetricKind::GaugeDelta(2.5), MetricKind::Set(logit_core::HyperLogLog::default())] {
        let record = MetricRecord {
            name: logit_core::interner::intern("bakeoff_undroppable_metric"),
            kind,
            unit: None,
        };
        let batch = single_event_batch(Event::metric(0, AttrMap::new(), record));

        for (arm, decoded) in [
            ("native", bakeoff::native_decode(bakeoff::native_encode(&batch))),
            ("postcard", bakeoff::postcard_decode(&bakeoff::postcard_encode(&batch))),
            ("rkyv", bakeoff::rkyv_decode(&bakeoff::rkyv_encode(&batch))),
        ] {
            assert_eq!(decoded.events.len(), 1, "{arm}: event must survive");
            assert_eq!(decoded.events[0].metrics.len(), 1, "{arm}: metric record must survive");
        }

        // The OTLP control arm: encode_signals produces no Metrics payload at all for a batch
        // whose only metric is a kind it can't express -- the record simply disappears.
        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let payloads = encoder.encode_signals(&batch).expect("otlp encode");
        assert!(
            payloads.iter().all(|(signal, _)| *signal != Signal::Metrics),
            "OTLP should produce no Metrics payload for a kind it can't express -- got {:?}",
            payloads.iter().map(|(s, _)| s).collect::<Vec<_>>()
        );
    }
}

#[test]
fn an_empty_batch_round_trips_through_every_arm() {
    let batch = EventBatch { resource: Arc::new(Resource::default()), events: Vec::new() };
    assert_eq!(bakeoff::native_decode(bakeoff::native_encode(&batch)).events.len(), 0);
    assert_eq!(bakeoff::postcard_decode(&bakeoff::postcard_encode(&batch)).events.len(), 0);
    assert_eq!(bakeoff::rkyv_decode(&bakeoff::rkyv_encode(&batch)).events.len(), 0);
    assert!(
        bakeoff::otlp_round_trip(&batch).is_empty(),
        "an empty batch should yield no OTLP payloads at all"
    );
}

/// Not a fidelity assertion -- a sanity check that `native`'s own on-disk story (a plain frame
/// concatenation, `docs/design/wire-protocol.md`) actually holds for a batch this test file
/// exercises the same way the others do, so the bake-off's evidence base covers the file case too,
/// not just the in-memory one.
#[test]
fn native_frames_from_several_batches_concatenate_into_one_decodable_buffer() {
    let mut file = Vec::new();
    let mut expected_event_counts = Vec::new();
    for batch in representative_batches() {
        expected_event_counts.push(batch.events.len());
        file.extend_from_slice(&bakeoff::native_encode(&batch));
    }

    let mut cursor = Bytes::from(file);
    let mut decoded_counts = Vec::new();
    while !cursor.is_empty() {
        let (codec, mut payload) = logit_proto::frame::read_frame(&mut cursor).unwrap();
        assert_eq!(codec, logit_proto::native::CODEC_NATIVE_V1);
        let batch = logit_proto::native::decode_batch(&mut payload).unwrap();
        decoded_counts.push(batch.events.len());
    }
    assert_eq!(decoded_counts, expected_event_counts);
}
