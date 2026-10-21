//! `format: json` is lossless: `logit_proto::event_json` reads every line `ndjson` writes back to
//! the batch that wrote it (`crates/logit-outputs/src/ndjson.rs`'s module doc,
//! `docs/adr/stream-json-format.md`'s "Amendment: the form is lossless").
//!
//! The batches compare with `EventBatch`'s `PartialEq`, which is strict on a value's type
//! (`F64(5.0) != I64(5) != U64(5)`). `NaN` never equals itself, so the batches holding one are
//! checked by rendering what was read and comparing the text, beside a direct `is_nan` check.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Bin, BodyFormat, DdSketch, Event, EventBatch, Exemplar, ExpHistogram, Histogram,
    HyperLogLog, LogRecord, Mapping, MetricKind, MetricRecord, MetricValue, Resource, Samples,
    Scope, Severity, SketchStats, SpanEvent, SpanExt, SpanKind, SpanLink, SpanRecord, SpanStatus,
    Sum, Summary, Temporality, TraceRef, Value,
};
use logit_outputs::ndjson::render_ndjson;
use logit_proto::event_json::EventJsonDecoder;
use proptest::prelude::*;
use std::sync::Arc;

fn render(batch: &EventBatch) -> String {
    let mut out = String::new();
    render_ndjson(&mut out, batch);
    out
}

fn read(text: &str) -> Vec<EventBatch> {
    EventJsonDecoder::new()
        .decode_batches(text.as_bytes())
        .unwrap_or_else(|e| panic!("reads back ({e}):\n{text}"))
}

fn assert_round_trip(batch: &EventBatch) {
    let text = render(batch);
    let back = read(&text);
    assert_eq!(back.len(), 1, "one resource and scope is one batch:\n{text}");
    assert_eq!(&back[0], batch, "line:\n{text}");
}

fn attrs(pairs: &[(&str, Value)]) -> AttrMap {
    let mut map = AttrMap::new();
    for (k, v) in pairs {
        map.insert(k, v.clone());
    }
    map
}

/// Every `Value` variant, with each spelling a type tag exists for.
fn every_value() -> AttrMap {
    attrs(&[
        ("rt.null", Value::Null),
        ("rt.bool", Value::Bool(false)),
        ("rt.i64", Value::I64(5)),
        ("rt.i64_neg", Value::I64(i64::MIN)),
        ("rt.u64", Value::U64(5)),
        ("rt.u64_big", Value::U64(u64::MAX)),
        ("rt.f64_whole", Value::F64(5.0)),
        ("rt.f64_neg_zero", Value::F64(-0.0)),
        ("rt.f64_big", Value::F64(1e21)),
        ("rt.f64_tiny", Value::F64(f64::MIN_POSITIVE)),
        ("rt.f64_inf", Value::F64(f64::INFINITY)),
        ("rt.f64_neg_inf", Value::F64(f64::NEG_INFINITY)),
        ("rt.str", Value::str("quote \" back \\ \\x41 nl \n é")),
        ("rt.str_like_bytes", Value::str("b\"x\"")),
        ("rt.str_like_time", Value::str("1970-01-01T00:00:00.000000000Z")),
        ("rt.str_inf", Value::str("inf")),
        ("rt.bytes", Value::Bytes(Bytes::from_static(b"ok\xff\x00 \\x41 \\"))),
        ("rt.bytes_empty", Value::Bytes(Bytes::new())),
        ("rt.ts", Value::Timestamp(-1)),
        (
            "rt.nested/~key",
            Value::Array(vec![
                Value::U64(1),
                Value::Map(Box::new(attrs(&[
                    ("rt.inner", Value::Timestamp(7)),
                    ("rt.inner_u", Value::U64(3)),
                ]))),
                Value::Array(vec![]),
            ]),
        ),
        ("rt.empty_map", Value::Map(Box::default())),
    ])
}

fn trace() -> TraceRef {
    TraceRef { trace_id: [0; 16], span_id: Some([0x22; 8]), flags: 1 }
}

fn exemplar(value: MetricValue) -> Exemplar {
    Exemplar {
        timestamp: 6,
        value,
        trace: Some(trace()),
        filtered_attributes: attrs(&[("rt.ex", Value::U64(1)), ("rt.ex_b", Value::str("v"))]),
    }
}

fn every_kind() -> Vec<MetricKind> {
    let mut agent = DdSketch::new();
    for v in [-3.0, 0.0, 1.0, 2.0, 40.0, 1e-12] {
        agent.add(v);
    }
    agent.add_count(7.5, 0.25);
    let logarithmic = DdSketch::from_parts(
        Mapping::logarithmic(1.02, 0.5, 2048),
        vec![Bin { key: -4, count: 2.0 }, Bin { key: 10, count: 1.5 }],
        vec![Bin { key: 3, count: 1.0 }],
        0.5,
        None,
    );
    let decoded = DdSketch::from_stored(
        Mapping::agent(),
        vec![Bin { key: 1500, count: 3.0 }],
        vec![],
        0.0,
        SketchStats { count: 3.0, min: 1.5, max: 9.25, sum: 11.0 },
        false,
    );
    let mut hll = HyperLogLog::new();
    hll.insert(b"a");
    hll.insert(b"b");
    let mut big_hll = HyperLogLog::new();
    for i in 0..500u32 {
        big_hll.insert(&i.to_le_bytes());
    }
    let mut samples = Samples::new([1.5, 2.0, -0.0]);
    samples.sample_rate = 0.25;
    vec![
        MetricKind::Sum(Sum {
            value: MetricValue::F64(3.0),
            temporality: Temporality::Delta,
            monotonic: true,
        }),
        MetricKind::Sum(Sum {
            value: MetricValue::U64(5),
            temporality: Temporality::Cumulative,
            monotonic: false,
        }),
        MetricKind::Gauge(MetricValue::I64(5)),
        MetricKind::Gauge(MetricValue::U64(u64::MAX)),
        MetricKind::Gauge(MetricValue::F64(5.0)),
        MetricKind::Gauge(MetricValue::F64(f64::INFINITY)),
        MetricKind::Gauge(MetricValue::Bool(true)),
        MetricKind::GaugeDelta(MetricValue::F64(-0.0)),
        MetricKind::GaugeDelta(MetricValue::I64(-2)),
        MetricKind::Samples(samples),
        MetricKind::SetMembers(vec![
            Bytes::from_static(b"alice"),
            Bytes::from_static(b"b\xffo\\x41"),
        ]),
        MetricKind::Distribution(agent),
        MetricKind::Distribution(logarithmic),
        MetricKind::Distribution(decoded),
        MetricKind::Distribution(DdSketch::new()),
        MetricKind::Set(hll),
        MetricKind::Set(big_hll),
        MetricKind::Set(HyperLogLog::new()),
        MetricKind::Histogram(Histogram {
            buckets: vec![(f64::NEG_INFINITY, 0), (0.5, 3), (f64::INFINITY, 1)],
            temporality: Temporality::Cumulative,
            sum: Some(2.0),
            min: Some(0.1),
            max: None,
        }),
        MetricKind::ExponentialHistogram(ExpHistogram {
            scale: -3,
            zero_count: 1,
            zero_threshold: 1e-9,
            positive: (-2, vec![1, 300]),
            negative: (4, vec![]),
            temporality: Temporality::Delta,
            count: 302,
            sum: Some(1.5),
            min: None,
            max: Some(8.0),
        }),
        MetricKind::ExponentialHistogram(ExpHistogram {
            scale: 0,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: (0, vec![]),
            negative: (0, vec![]),
            temporality: Temporality::Cumulative,
            count: 0,
            sum: None,
            min: None,
            max: None,
        }),
        MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0), (0.99, 4.25)],
            count: 10,
            sum: 12.0,
            temporality: Temporality::Delta,
        }),
    ]
}

fn every_field_batch() -> EventBatch {
    let log = LogRecord {
        message: Value::Bytes(Bytes::from_static(b"GET /\xfe")),
        severity: Some(Severity::Warn),
        body_format: BodyFormat::Structured,
        trace: Some(TraceRef { trace_id: [0x11; 16], span_id: None, flags: 0 }),
        event_name: Some(intern("http.request")),
        observed_timestamp: 1_700_000_000_000_000_001,
        dropped_attributes_count: 2,
    };
    let log_event = Event::log(1_700_000_000_000_000_000, every_value(), log);

    let mut metric_event = Event::empty(i64::MIN, attrs(&[("rt.m", Value::U64(2))]));
    for (i, kind) in every_kind().into_iter().enumerate() {
        let mut record = MetricRecord::new(intern(&format!("rt.metric.{i}")), kind);
        if i % 2 == 0 {
            record.unit = Some(intern("ms"));
            record.description = Some(intern("a \"description\""));
            record.start_timestamp = 5;
            record.flags = 4;
            record.exemplars = vec![
                exemplar(MetricValue::F64(2.0)),
                exemplar(MetricValue::U64(7)),
                exemplar(MetricValue::I64(7)),
            ];
        }
        metric_event.metrics.push(record);
    }
    // A no-recorded-value point keeps its kind's fields as decoded.
    let mut nrv = MetricRecord::new(
        intern("rt.nrv"),
        MetricKind::Sum(Sum {
            value: MetricValue::I64(0),
            temporality: Temporality::Cumulative,
            monotonic: true,
        }),
    );
    nrv.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
    metric_event.metrics.push(nrv);

    let span = SpanRecord {
        trace_id: [0xab; 16],
        span_id: [0xcd; 8],
        parent_span_id: Some([0; 8]),
        name: Value::U64(9),
        kind: SpanKind::Consumer,
        status: SpanStatus::Error,
        events: vec![SpanEvent {
            timestamp: 1_200,
            name: Value::Timestamp(3),
            attributes: attrs(&[("rt.se", Value::U64(1))]),
            dropped_attributes_count: 4,
        }],
        links: vec![
            SpanLink {
                trace_id: [0x01; 16],
                span_id: [0x02; 8],
                attributes: attrs(&[("rt.sl", Value::Bytes(Bytes::from_static(b"\x00")))]),
                flags: 1,
                trace_state: Some(Bytes::from_static(b"a=b\\c")),
                dropped_attributes_count: 5,
            },
            SpanLink {
                trace_id: [0x03; 16],
                span_id: [0x04; 8],
                attributes: AttrMap::new(),
                flags: 0,
                trace_state: Some(Bytes::new()),
                dropped_attributes_count: 0,
            },
        ],
        end_timestamp: 500,
        flags: 0x301,
        ext: Some(Box::new(SpanExt {
            status_message: Some(Bytes::from_static(b"boom\xfe")),
            trace_state: Some(Bytes::from_static(b"vendor=x")),
            dropped_attributes_count: 1,
            dropped_events_count: 2,
            dropped_links_count: 3,
        })),
    };
    let span_event = Event::span(1_000, AttrMap::new(), span);

    // A span whose extension holds one field.
    let mut small_span = span_event.span.clone().expect("a span");
    small_span.ext = Some(Box::new(SpanExt {
        status_message: None,
        trace_state: None,
        dropped_attributes_count: 0,
        dropped_events_count: 0,
        dropped_links_count: 1,
    }));
    small_span.parent_span_id = None;
    small_span.events.clear();
    small_span.links.clear();
    let small_span_event = Event::span(i64::MAX, AttrMap::new(), small_span);

    let resource = Resource {
        attributes: attrs(&[("service.name", Value::str("api")), ("rt.r", Value::U64(1))]),
        dropped_attributes_count: 7,
        schema_url: Some(Bytes::from_static(b"https://opentelemetry.io/schemas/1.26.0")),
    };
    let scope = Scope {
        name: Bytes::from_static(b"io.logit\xff\\"),
        version: Bytes::from_static(b"1.2.0"),
        attributes: attrs(&[("rt.scope", Value::Timestamp(9))]),
        dropped_attributes_count: 8,
        schema_url: Some(Bytes::new()),
    };
    EventBatch {
        resource: Arc::new(resource),
        scope: Some(Arc::new(scope)),
        events: vec![
            log_event,
            metric_event,
            span_event,
            small_span_event,
            Event::empty(-1, AttrMap::new()),
        ],
    }
}

#[test]
fn every_field_round_trips() {
    assert_round_trip(&every_field_batch());
}

#[test]
fn an_empty_resource_and_no_scope_round_trip() {
    let batch = EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![Event::empty(0, AttrMap::new())],
    };
    assert_round_trip(&batch);
    let empty_scope = EventBatch {
        scope: Some(Arc::new(Scope {
            name: Bytes::new(),
            version: Bytes::new(),
            attributes: AttrMap::new(),
            dropped_attributes_count: 0,
            schema_url: None,
        })),
        ..batch
    };
    assert_round_trip(&empty_scope);
}

#[test]
fn nan_round_trips() {
    let mut event = Event::metric(
        0,
        attrs(&[("rt.nan", Value::F64(f64::NAN))]),
        MetricRecord::new(intern("rt.nan_gauge"), MetricKind::Gauge(MetricValue::F64(f64::NAN))),
    );
    event.metrics.push(MetricRecord::new(
        intern("rt.nan_samples"),
        MetricKind::Samples(Samples::new([f64::NAN])),
    ));
    let batch =
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] };
    let text = render(&batch);
    let back = read(&text);
    assert_eq!(render(&back[0]), text);
    let event = &back[0].events[0];
    assert!(matches!(event.attributes.get("rt.nan"), Some(Value::F64(f)) if f.is_nan()));
    assert!(matches!(event.metrics[0].kind, MetricKind::Gauge(MetricValue::F64(f)) if f.is_nan()));
}

#[test]
fn consecutive_batches_split_where_the_resource_changes() {
    let first = every_field_batch();
    let second = EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![Event::empty(1, AttrMap::new())],
    };
    let mut text = render(&first);
    text.push_str(&render(&second));
    assert_eq!(read(&text), vec![first, second]);
}

// -- generated values -----------------------------------------------------------------------------

fn finite_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        any::<f64>().prop_filter("finite", |f| f.is_finite()),
        (-1_000_000i64..1_000_000).prop_map(|n| n as f64),
        Just(-0.0),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
    ]
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof!["[ -~]{0,12}", "\\PC{0,8}", Just("b\"x\"".to_string()), Just("\\x41".to_string())]
}

fn value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::I64),
        any::<u64>().prop_map(Value::U64),
        finite_f64().prop_map(Value::F64),
        text().prop_map(|s| Value::str(s.as_str())),
        proptest::collection::vec(any::<u8>(), 0..8).prop_map(|b| Value::Bytes(Bytes::from(b))),
        any::<i64>().prop_map(Value::Timestamp),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            proptest::collection::vec(("[a-z~/.]{1,6}", inner), 0..4).prop_map(|pairs| {
                let mut map = AttrMap::new();
                for (k, v) in pairs {
                    map.insert(&format!("rt.gen.{k}"), v);
                }
                Value::Map(Box::new(map))
            }),
        ]
    })
}

fn metric_value() -> impl Strategy<Value = MetricValue> {
    prop_oneof![
        any::<i64>().prop_map(MetricValue::I64),
        any::<u64>().prop_map(MetricValue::U64),
        finite_f64().prop_map(MetricValue::F64),
        any::<bool>().prop_map(MetricValue::Bool),
    ]
}

proptest! {
    #[test]
    fn generated_values_round_trip(
        attributes in proptest::collection::vec(("[a-z~/.]{1,6}", value()), 0..6),
        message in value(),
        gauge in metric_value(),
        samples in proptest::collection::vec(finite_f64(), 0..5),
    ) {
        let mut map = AttrMap::new();
        for (k, v) in attributes {
            map.insert(&format!("rt.gen.{k}"), v);
        }
        let log = LogRecord {
            message,
            severity: Some(Severity::Info),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        };
        let mut event = Event::log(0, map.clone(), log);
        event.metrics.push(MetricRecord::new(intern("rt.gen.gauge"), MetricKind::Gauge(gauge)));
        event.metrics.push(MetricRecord::new(
            intern("rt.gen.samples"),
            MetricKind::Samples(Samples::new(samples)),
        ));
        let batch = EventBatch {
            resource: Arc::new(Resource { attributes: map, ..Resource::default() }),
            scope: None,
            events: vec![event],
        };
        let text = render(&batch);
        let back = EventJsonDecoder::new().decode_batches(text.as_bytes());
        prop_assert!(back.is_ok(), "{:?}: {}", back.err(), text);
        prop_assert_eq!(&back.unwrap()[0], &batch, "line: {}", text);
    }
}
