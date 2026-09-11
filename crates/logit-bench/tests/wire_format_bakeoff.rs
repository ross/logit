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
//!
//! **Native/postcard/rkyv are exact codecs** (`docs/plans/lossless-transit.md`'s W1): `EventBatch`
//! now derives `PartialEq`, so these three arms compare with a plain `assert_eq!` on the whole
//! batch rather than a hand-rolled field walk. **OTLP is not** -- W1 pulled three narrow exceptions
//! forward (`Sum`/`Histogram`/`ExponentialHistogram` temporality, `ExponentialHistogram` itself,
//! `Histogram`/`Summary`'s new scalar fields) but everything else (`scope`, `description`,
//! `start_timestamp`, `exemplars`, `event_name`, `observed_timestamp`, dropped counts, span
//! `flags`/`ext`) is W4's; OTLP tests stay field-level, documenting exactly what's still lossy
//! rather than pretending otherwise.

use bytes::Bytes;
use logit_bench::{bakeoff, fixtures};
use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, ExpHistogram, LogRecord, MetricKind, MetricRecord,
    Resource, Samples, Scope, Severity, SpanExt, SpanKind, SpanRecord, SpanStatus, Sum,
    Temporality, Value,
};
use logit_proto::{Signal, SignalEncoder};
use std::sync::Arc;

fn single_event_batch(event: Event) -> EventBatch {
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] }
}

fn metric_batch(kind: MetricKind) -> EventBatch {
    let record = MetricRecord::new(logit_core::interner::intern("bakeoff_kind_test_metric"), kind);
    single_event_batch(Event::metric(0, AttrMap::new(), record))
}

/// [`fixtures::nginx_batch`], with a populated [`Scope`] attached -- exercises the new batch-level
/// section through every arm.
fn scoped_nginx_batch() -> EventBatch {
    let mut batch = fixtures::nginx_batch(2);
    let mut scope_attrs = AttrMap::new();
    scope_attrs.insert("scope.attr", "value");
    batch.scope = Some(Arc::new(Scope {
        name: Bytes::from_static(b"bakeoff_scope"),
        version: Bytes::from_static(b"9.9.9"),
        attributes: scope_attrs,
        dropped_attributes_count: 2,
        schema_url: Some(Bytes::from_static(b"https://example.com/scope-schema")),
    }));
    batch
}

/// [`fixtures::span_event`], with a populated [`SpanExt`] attached -- exercises the boxed,
/// rarely-populated half of span fidelity through every arm.
fn span_with_ext_batch() -> EventBatch {
    let mut event = fixtures::span_event();
    if let Some(span) = event.span.as_mut() {
        span.flags = 1;
        span.ext = Some(Box::new(SpanExt {
            status_message: Some(Bytes::from_static(b"boom")),
            trace_state: Some(Bytes::from_static(b"vendor=value")),
            dropped_attributes_count: 2,
            dropped_events_count: 1,
            dropped_links_count: 1,
        }));
    }
    single_event_batch(event)
}

/// Every fixture this gate runs each arm against -- deliberately the same shapes
/// `crates/logit-bench/src/fixtures.rs`'s own doc comment argues for (mixed, logs-only,
/// wide-JSON, distribution-heavy, span), per `AGENTS.md`'s "don't generalize a measurement from
/// one event shape", plus one batch per new W1 metric kind and one each for a populated
/// `Scope`/`SpanExt`.
fn representative_batches() -> Vec<EventBatch> {
    vec![
        fixtures::nginx_batch(3),
        single_event_batch(fixtures::statsd_event()),
        single_event_batch(fixtures::distribution_event()),
        single_event_batch(fixtures::distribution_heavy_event()),
        single_event_batch(fixtures::span_event()),
        metric_batch(MetricKind::Sum(Sum {
            value: 5.0,
            temporality: Temporality::Cumulative,
            monotonic: false,
        })),
        metric_batch(MetricKind::Samples({
            let mut s = Samples::new([1.0, 2.5, 3.75]);
            s.sample_rate = 0.1;
            s
        })),
        metric_batch(MetricKind::SetMembers(vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ])),
        metric_batch(MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 2,
            zero_count: 1,
            zero_threshold: 0.001,
            positive: (0, vec![1, 2, 3]),
            negative: (-1, vec![4]),
            temporality: Temporality::Cumulative,
            count: 11,
            sum: Some(50.0),
            min: Some(0.1),
            max: Some(9.5),
        })),
        scoped_nginx_batch(),
        span_with_ext_batch(),
    ]
}

#[test]
fn native_round_trips_every_representative_fixture_losslessly() {
    for batch in representative_batches() {
        let decoded = bakeoff::native_decode(bakeoff::native_encode(&batch));
        assert_eq!(decoded, batch, "native");
    }
}

#[test]
fn postcard_round_trips_every_representative_fixture_losslessly() {
    for batch in representative_batches() {
        let decoded = bakeoff::postcard_decode(&bakeoff::postcard_encode(&batch));
        assert_eq!(decoded, batch, "postcard");
    }
}

#[test]
fn rkyv_round_trips_every_representative_fixture_losslessly() {
    for batch in representative_batches() {
        let decoded = bakeoff::rkyv_decode(&bakeoff::rkyv_encode(&batch));
        assert_eq!(decoded, batch, "rkyv");
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
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
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

// -- OTLP's remaining W1 lossy fields (W4 owns closing these) ------------------------------------

/// `scope`/`description`/`start_timestamp`/`exemplars`/dropped counts/span `ext` are filled with
/// their defaults on OTLP decode and ignored on encode until W4
/// (`crates/logit-proto/src/otlp/metrics.rs`'s module doc, `docs/plans/lossless-transit.md`).
/// Field-level, not `assert_eq!` on the whole batch -- unlike native/postcard/rkyv, OTLP is not
/// (yet) an exact codec, so this documents precisely what's still lossy rather than papering over
/// it with a looser comparison.
#[test]
fn otlp_still_drops_metric_record_fields_w4_owns() {
    let record = MetricRecord {
        description: Some(logit_core::interner::intern("a description")),
        start_timestamp: 100,
        ..MetricRecord::new(
            logit_core::interner::intern("bakeoff_otlp_lossy_metric"),
            MetricKind::Sum(Sum {
                value: 3.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
        )
    };
    let batch = single_event_batch(Event::metric(0, AttrMap::new(), record));

    let decoded = bakeoff::otlp_round_trip(&batch);
    assert_eq!(decoded.len(), 1, "a lone Sum metric should produce one Metrics payload");
    let out = &decoded[0].events[0].metrics[0];
    assert_eq!(out.description, None, "OTLP decode fills description with its default until W4");
    assert_eq!(
        out.start_timestamp, 0,
        "OTLP decode fills start_timestamp with its default until W4"
    );
}

#[test]
fn otlp_still_drops_scope_until_w4() {
    let decoded = bakeoff::otlp_round_trip(&scoped_nginx_batch());
    assert!(
        !decoded.is_empty() && decoded.iter().all(|b| b.scope.is_none()),
        "OTLP decode leaves EventBatch::scope None until W4"
    );
}

/// `Samples` degrades into an OTLP `Summary` (same lossy path `Distribution` already took);
/// `SetMembers` is skipped outright, exactly like `Set` already was.
#[test]
fn otlp_degrades_samples_to_a_summary_and_skips_set_members() {
    let samples_batch = metric_batch(MetricKind::Samples(Samples::new([1.0, 2.0, 3.0])));
    let decoded = bakeoff::otlp_round_trip(&samples_batch);
    assert_eq!(decoded.len(), 1, "Samples degrades into a Metrics payload, not dropped outright");
    match &decoded[0].events[0].metrics[0].kind {
        MetricKind::Summary(_) => {}
        other => panic!("Samples should degrade to a Summary over OTLP, got {other:?}"),
    }

    let set_members_batch = metric_batch(MetricKind::SetMembers(vec![Bytes::from_static(b"a")]));
    let mut encoder = logit_proto::otlp::OtlpEncoder::new();
    let payloads = encoder.encode_signals(&set_members_batch).expect("otlp encode");
    assert!(
        payloads.iter().all(|(signal, _)| *signal != Signal::Metrics),
        "SetMembers should be skipped like Set, producing no Metrics payload"
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
        event_name: None,
        observed_timestamp: 0,
        dropped_attributes_count: 0,
    });
    event.metrics.push(MetricRecord::new(
        logit_core::interner::intern("bakeoff_multi_payload_metric"),
        MetricKind::counter(1.0),
    ));
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
        flags: 0,
        ext: None,
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
        let batch = metric_batch(kind);

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
    let batch =
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events: Vec::new() };
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
