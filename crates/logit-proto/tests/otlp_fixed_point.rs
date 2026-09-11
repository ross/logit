//! Pure-codec OTLP fixed-point tests: `docs/adr/lossless-transit.md`'s "round-trip fixed point is
//! the test that proves this" requirement, exercised directly against [`OtlpEncoder`]/
//! [`OtlpDecoder`] with no pipeline, transform, or transport in between.
//!
//! Two properties, per fixture, both over a fully-populated *single-signal* [`EventBatch`] (a
//! batch carrying only logs, only metrics, or only spans -- `encode_signals` returns one payload
//! per non-empty signal, and mixing signals would just mean picking one out of several payloads
//! below, adding nothing):
//!
//! 1. **`decode_signal(encode_signals(b)) == vec![b]`** -- whole-`EventBatch` equality via
//!    `PartialEq` (`docs/adr/metrics-model-v2.md`'s "`PartialEq` on every record type"). Each
//!    fixture is built in the shape a *real decode* would already produce (e.g. a log's
//!    `otel.severity_number`/`otel.severity_text` attributes already match its `Severity`) --
//!    otherwise decode's own enrichment (stamping those two attributes fresh from the wire values
//!    it just wrote) would make the round trip a no-op tautology instead of a real fixed-point
//!    check. Logs set `observed_timestamp` non-zero so `encode_log_record`'s only
//!    non-deterministic path (falling back to the wall clock) never fires -- see `otlp/logs.rs`'s
//!    own module doc.
//! 2. **`encode_signals(decode_signal(encode_signals(b))[0]) == encode_signals(b)` on bytes** --
//!    the same fixed point restated at the wire level, catching a codec that produces two
//!    different byte strings for what it itself considers the same batch (a non-deterministic
//!    field ordering, a stray default it sometimes omits and sometimes doesn't).

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, Exemplar, ExpHistogram, Histogram, LogRecord,
    MetricKind, MetricRecord, Resource, Scope, Severity, SpanEvent, SpanExt, SpanKind, SpanLink,
    SpanRecord, SpanStatus, Sum, Summary, Temporality, TraceRef, Value,
};
use logit_proto::otlp::{OtlpDecoder, OtlpEncoder};
use logit_proto::{Signal, SignalDecoder, SignalEncoder};
use std::sync::Arc;

fn fully_populated_resource() -> Arc<Resource> {
    let mut attrs = AttrMap::new();
    attrs.insert("service.name", "orders-api");
    attrs.insert("service.instance.id", "i-0123456789");
    Arc::new(Resource {
        attributes: attrs,
        dropped_attributes_count: 2,
        schema_url: Some(Bytes::from_static(b"https://example.com/resource-schema")),
    })
}

fn fully_populated_scope() -> Arc<Scope> {
    let mut attrs = AttrMap::new();
    attrs.insert("scope.attr", "scope-value");
    Arc::new(Scope {
        name: Bytes::from_static(b"nginx-otel-module"),
        version: Bytes::from_static(b"1.0.0"),
        attributes: attrs,
        dropped_attributes_count: 1,
        schema_url: Some(Bytes::from_static(b"https://example.com/scope-schema")),
    })
}

/// A fixed point on both sides, not a lossless-transit "does the round trip pin this" tautology:
/// the round-trip test in this file needs `decode(encode(b)) == b`, which only holds if `b`'s own
/// attributes are already the ones a real decode would produce -- see this file's own module doc.
fn log_batch() -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("custom.attr", "value");
    // The raw severity, already matching Severity::Info's INFO2 (10) band member -- what a real
    // decode of a SeverityNumber=10/"INFO2" wire record would itself stamp (otlp/logs.rs).
    attrs.insert("otel.severity_number", Value::I64(10));
    attrs.insert("otel.severity_text", "INFO2");

    let log = LogRecord {
        message: Value::str("fully populated log fixture"),
        severity: Some(Severity::Info),
        body_format: BodyFormat::Json,
        trace: Some(TraceRef { trace_id: [9; 16], span_id: Some([8; 8]), flags: 1 }),
        event_name: Some(intern("request_finished")),
        observed_timestamp: 1_700_000_000_500_000_000,
        dropped_attributes_count: 2,
    };
    let event = Event::log(1_700_000_000_000_000_000, attrs, log);
    EventBatch {
        resource: fully_populated_resource(),
        scope: Some(fully_populated_scope()),
        events: vec![event],
    }
}

fn exemplar() -> Exemplar {
    let mut filtered_attributes = AttrMap::new();
    filtered_attributes.insert("dropped", "attr");
    Exemplar {
        timestamp: 1_700_000_000_100_000_000,
        value: 3.5,
        trace: Some(TraceRef { trace_id: [7; 16], span_id: Some([6; 8]), flags: 0 }),
        filtered_attributes,
    }
}

fn metric_record(name: &str, kind: MetricKind, exemplars: Vec<Exemplar>) -> MetricRecord {
    MetricRecord {
        description: Some(intern("a fully populated metric fixture")),
        start_timestamp: 1_699_000_000_000_000_000,
        exemplars,
        flags: MetricRecord::FLAG_NO_RECORDED_VALUE,
        ..MetricRecord::new(intern(name), kind)
    }
}

/// One metric event per kind (`Sum`/`Gauge`/`Histogram`/`ExponentialHistogram`/`Summary`), each
/// with `description`/`start_timestamp`/`flags` set -- `Summary` alone carries no exemplars, since
/// `SummaryDataPoint` has no wire field for them (`otlp/metrics.rs`'s own module doc): including
/// one there would make this fixture *not* a fixed point, by construction, not by a codec bug.
fn metric_batch() -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("host", "web-1");

    let sum = metric_record(
        "fixture_sum",
        MetricKind::Sum(Sum { value: 12.5, temporality: Temporality::Cumulative, monotonic: true }),
        vec![exemplar()],
    );
    let gauge = metric_record("fixture_gauge", MetricKind::Gauge(-4.5), vec![exemplar()]);
    let histogram = metric_record(
        "fixture_histogram",
        MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (5.0, 3), (10.0, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(12.5),
            min: Some(0.5),
            max: Some(9.9),
        }),
        vec![exemplar()],
    );
    let exp_histogram = metric_record(
        "fixture_exp_histogram",
        MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 3,
            zero_count: 2,
            zero_threshold: 0.5,
            positive: (1, vec![4, 5, 6]),
            negative: (2, vec![7, 8]),
            temporality: Temporality::Delta,
            count: 26,
            sum: Some(100.0),
            min: Some(-5.0),
            max: Some(50.0),
        }),
        vec![exemplar()],
    );
    let summary = metric_record(
        "fixture_summary",
        MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 10.0), (0.99, 42.0)],
            count: 100,
            sum: 543.2,
        }),
        Vec::new(), // no wire field for exemplars on a Summary point -- see this fn's own doc.
    );

    let ts = 1_700_000_000_000_000_000;
    let events = vec![
        Event::metric(ts, attrs.clone(), sum),
        Event::metric(ts, attrs.clone(), gauge),
        Event::metric(ts, attrs.clone(), histogram),
        Event::metric(ts, attrs.clone(), exp_histogram),
        Event::metric(ts, attrs, summary),
    ];
    EventBatch {
        resource: fully_populated_resource(),
        scope: Some(fully_populated_scope()),
        events,
    }
}

/// A bare `MetricRecord::new(..)` metric -- `start_timestamp: 0`, `flags: 0`, no description, no
/// exemplars -- the shape every non-OTLP-sourced producer (statsd, `kv_metrics`, `internal`,
/// `aggregate` output) actually builds. Fix 5 (`docs/adr/lossless-transit.md`) makes this a fixed
/// point: `start_timestamp` writes through verbatim with no fallback to `Event::timestamp`, so a
/// `0` start stays `0` on the wire instead of coming back equal to the event's own timestamp.
fn metric_batch_with_new_defaults() -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("host", "web-1");
    let record = MetricRecord::new(intern("fixture_default_gauge"), MetricKind::Gauge(1.5));
    let event = Event::metric(1_700_000_000_000_000_000, attrs, record);
    EventBatch {
        resource: fully_populated_resource(),
        scope: Some(fully_populated_scope()),
        events: vec![event],
    }
}

#[test]
fn a_metric_record_new_metric_is_a_fixed_point() {
    assert_fixed_point(Signal::Metrics, metric_batch_with_new_defaults());
}

fn span_batch() -> EventBatch {
    let mut event_attrs = AttrMap::new();
    event_attrs.insert("checkpoint.attr", "value");
    let mut link_attrs = AttrMap::new();
    link_attrs.insert("link.attr", "value");
    let mut attrs = AttrMap::new();
    attrs.insert("span.attr", "value");

    let record = SpanRecord {
        trace_id: [1; 16],
        span_id: [2; 8],
        parent_span_id: Some([3; 8]),
        name: Value::str("fully populated span fixture"),
        kind: SpanKind::Server,
        status: SpanStatus::Error,
        events: vec![SpanEvent {
            timestamp: 1_700_000_000_050_000_000,
            name: Value::str("checkpoint"),
            attributes: event_attrs,
            dropped_attributes_count: 2,
        }],
        links: vec![SpanLink {
            trace_id: [4; 16],
            span_id: [5; 8],
            attributes: link_attrs,
            flags: 1,
            trace_state: Some(Bytes::from_static(b"vendor=linkvalue")),
            dropped_attributes_count: 3,
        }],
        end_timestamp: 1_700_000_000_200_000_000,
        flags: 1,
        ext: Some(Box::new(SpanExt {
            status_message: Some(Bytes::from_static(b"boom")),
            trace_state: Some(Bytes::from_static(b"vendor=rootvalue")),
            dropped_attributes_count: 4,
            dropped_events_count: 5,
            dropped_links_count: 6,
        })),
    };
    let event = Event::span(1_700_000_000_000_000_000, attrs, record);
    EventBatch {
        resource: fully_populated_resource(),
        scope: Some(fully_populated_scope()),
        events: vec![event],
    }
}

/// Runs both fixed-point properties (see this file's own module doc) for `batch`, over `signal`.
fn assert_fixed_point(signal: Signal, batch: EventBatch) {
    let mut encoder = OtlpEncoder::new();
    let payloads = encoder.encode_signals(&batch).expect("encode must succeed");
    let (found_signal, bytes) =
        payloads.into_iter().find(|(s, _)| *s == signal).expect("expected signal payload missing");
    assert_eq!(found_signal, signal);

    // Property 1: decode(encode(b)) == vec![b].
    let mut decoder = OtlpDecoder::new();
    let decoded = decoder.decode_signal(signal, bytes.clone()).expect("decode must succeed");
    assert_eq!(decoded, vec![batch], "decode(encode(b)) must equal vec![b]");

    // Property 2: encode(decode(encode(b))) == encode(b), on bytes.
    let mut re_encoder = OtlpEncoder::new();
    let re_payloads = re_encoder.encode_signals(&decoded[0]).expect("re-encode must succeed");
    let (_, re_bytes) = re_payloads
        .into_iter()
        .find(|(s, _)| *s == signal)
        .expect("expected signal payload missing");
    assert_eq!(re_bytes, bytes, "encode(decode(encode(b))) must equal encode(b) on bytes");
}

#[test]
fn a_fully_populated_log_batch_is_a_fixed_point() {
    assert_fixed_point(Signal::Logs, log_batch());
}

#[test]
fn a_fully_populated_metric_batch_is_a_fixed_point() {
    assert_fixed_point(Signal::Metrics, metric_batch());
}

#[test]
fn a_fully_populated_span_batch_is_a_fixed_point() {
    assert_fixed_point(Signal::Traces, span_batch());
}

/// `scope: None` -- every statsd/syslog/native-sourced batch -- has to be a fixed point too: it
/// must decode back to `None`, not `Some(Scope::default())`, or a relay through `otlp_out ->
/// otlp_in` would silently invent scope identity nothing upstream ever had
/// (`common::pb_to_scope`'s own doc comment). Reuses each signal's fully-populated fixture with
/// only `scope` overridden, so a fixture and its no-scope sibling stay identical apart from that
/// one field.
fn without_scope(mut batch: EventBatch) -> EventBatch {
    batch.scope = None;
    batch
}

/// One-sided severity fixtures: only `otel.severity_number` present -- the missing
/// `otel.severity_text` must round-trip as OTLP's own unset sentinel (empty string), not the
/// band-derived variant name (`docs/adr/lossless-transit.md`'s pair-as-a-unit rule).
fn log_batch_with_severity_number_only() -> EventBatch {
    let mut batch = log_batch();
    batch.events[0].attributes.remove("otel.severity_text");
    batch
}

/// The mirror one-sided fixture: only `otel.severity_text` present -- the missing
/// `otel.severity_number` must round-trip as `0` (`SEVERITY_NUMBER_UNSPECIFIED`), not the
/// band-derived number.
fn log_batch_with_severity_text_only() -> EventBatch {
    let mut batch = log_batch();
    let event = &mut batch.events[0];
    event.attributes.remove("otel.severity_number");
    event.attributes.insert("otel.severity_text", "warn");
    event.log.as_mut().unwrap().severity = Some(Severity::Warn);
    batch
}

#[test]
fn a_log_batch_with_only_severity_number_is_a_fixed_point() {
    assert_fixed_point(Signal::Logs, log_batch_with_severity_number_only());
}

#[test]
fn a_log_batch_with_only_severity_text_is_a_fixed_point() {
    assert_fixed_point(Signal::Logs, log_batch_with_severity_text_only());
}

#[test]
fn a_log_batch_with_no_scope_is_a_fixed_point() {
    assert_fixed_point(Signal::Logs, without_scope(log_batch()));
}

#[test]
fn a_metric_batch_with_no_scope_is_a_fixed_point() {
    assert_fixed_point(Signal::Metrics, without_scope(metric_batch()));
}

#[test]
fn a_span_batch_with_no_scope_is_a_fixed_point() {
    assert_fixed_point(Signal::Traces, without_scope(span_batch()));
}
