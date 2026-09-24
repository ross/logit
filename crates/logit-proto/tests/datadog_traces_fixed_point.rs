//! Pure-codec fixed-point tests for Datadog's trace forms (`logit_proto::datadog::traces*`):
//! [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md)'s round-trip requirement,
//! exercised directly against `DatadogDecoder`/`DatadogEncoder`. The mirror of
//! `tests/datadog_metrics_fixed_point.rs`.
//!
//! Three properties:
//!
//! 1. **`decode(encode(decode(b))) == decode(b)`** on whole batches, resource included, for every
//!    form: v0.4, v0.5, v0.7 msgpack and the intake's `AgentPayload` protobuf.
//! 2. **`encode(decode(encode(d))) == encode(d)` on bytes**, for every form.
//! 3. **Cross-form**: the same spans decoded from v0.4, v0.5, and v0.7 are equal, given spans v0.5
//!    can carry (no `meta_struct`, links, or events) and no chunk or payload carriers, which v0.4
//!    and v0.5 have no field for.
//!
//! The hand-written vectors follow `w2b-wire-shapes.md`'s examples: a span with `_dd.p.tid`,
//! links with `trace_id_high`, events with every `AttributeAnyValue` type, `meta_struct`, a chunk
//! with priority/origin/dropped_trace, and an `AgentPayload` with two tracer payloads. The
//! `proptest` generates `AgentPayload`s (prost's own encoder, an independent writer) of random
//! spans and asserts all three properties from them.

use bytes::Bytes;
use logit_core::{EventBatch, SpanKind, SpanStatus, Value};
use logit_proto::datadog::generated::trace::{
    AgentPayload, AttributeAnyValue, AttributeArray, AttributeArrayValue, ContainerDebug, Span,
    SpanEvent, SpanLink, TraceChunk, TracerPayload,
};
use logit_proto::datadog::traces::{
    ATTR_CHUNK_DROPPED_TRACE, ATTR_CHUNK_ORIGIN, ATTR_CHUNK_PRIORITY, ATTR_CHUNK_TAGS,
};
use logit_proto::datadog::{
    DatadogDecoder, DatadogEncoder, ATTR_SERVICE_NAME, RESOURCE_ATTR_AGENT_ENV,
    RESOURCE_ATTR_AGENT_HOSTNAME, RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
};
use logit_proto::msgpack::Writer;
use proptest::prelude::*;
use prost::Message;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
enum Form {
    V04,
    V05,
    V07,
    Agent,
}

const FORMS: [Form; 4] = [Form::V04, Form::V05, Form::V07, Form::Agent];

fn decode(form: Form, body: &[u8]) -> Vec<EventBatch> {
    let mut d = DatadogDecoder::new();
    let result = match form {
        Form::V04 => d.decode_traces_v04(body, 0).map(|b| vec![b]),
        Form::V05 => d.decode_traces_v05(body, 0).map(|b| vec![b]),
        Form::V07 => d.decode_tracer_payload_v07(body, 0).map(|b| vec![b]),
        Form::Agent => d.decode_agent_payload(body, 0),
    };
    result.unwrap_or_else(|e| panic!("{form:?} must decode: {e}"))
}

fn encode(form: Form, batch: &EventBatch) -> Option<Bytes> {
    let mut e = DatadogEncoder::new();
    match form {
        Form::V04 => e.encode_traces_v04(batch),
        Form::V05 => e.encode_traces_v05(batch),
        Form::V07 => e.encode_tracer_payload_v07(batch),
        Form::Agent => e.encode_agent_payload(batch),
    }
}

/// Properties 1 and 2 from wire bytes. Returns the first decode.
fn assert_fixed_point(form: Form, body: &[u8]) -> Vec<EventBatch> {
    let d1 = decode(form, body);
    for batch in &d1 {
        let Some(b1) = encode(form, batch) else {
            assert!(batch.events.is_empty(), "{form:?}: spans decoded but nothing re-encoded");
            continue;
        };
        let d2 = decode(form, &b1);
        assert_eq!(d2, vec![batch.clone()], "{form:?}: decode(encode(decode(b))) != decode(b)");
        let b2 = encode(form, &d2[0]).expect("re-encodes");
        assert_eq!(b1, b2, "{form:?}: encode(decode(encode(d))) != encode(d)");
    }
    d1
}

/// Every form's fixed point, starting from `batch` re-encoded in that form.
fn assert_fixed_point_everywhere(batch: &EventBatch) {
    for form in FORMS {
        if let Some(body) = encode(form, batch) {
            assert_fixed_point(form, &body);
        }
    }
}

/// Property 3: `batch` stripped to what v0.5 carries, then decoded from each msgpack form.
fn assert_cross_form(batch: &EventBatch) {
    let mut stripped = batch.clone();
    stripped.resource = Default::default();
    for e in &mut stripped.events {
        let keys: Vec<_> = e
            .attributes
            .iter()
            .filter(|(k, v)| {
                matches!(v, Value::Bytes(_))
                    || logit_core::interner::resolve(*k).starts_with("datadog.chunk.")
            })
            .map(|(k, _)| k)
            .collect();
        for k in keys {
            e.attributes.remove_sym(k);
        }
        if let Some(span) = &mut e.span {
            span.links.clear();
            span.events.clear();
        }
    }
    let Some(v04) = encode(Form::V04, &stripped) else { return };
    let base = decode(Form::V04, &v04).remove(0);
    for form in [Form::V05, Form::V07] {
        let body = encode(form, &base).expect("encodes");
        let other = decode(form, &body).remove(0);
        assert_eq!(other.events, base.events, "{form:?} disagrees with v0.4");
        assert!(other.resource.attributes.is_empty());
    }
}

// -- hand-written vectors -------------------------------------------------------------------------

fn typed(kind: i32) -> AttributeAnyValue {
    AttributeAnyValue { r#type: kind, ..Default::default() }
}

/// A span with everything: `_dd.p.tid`, `span.kind`, `meta_struct`, a link with `trace_id_high`,
/// and an event carrying every `AttributeAnyValue` type.
fn rich_span() -> Span {
    Span {
        service: "checkout".into(),
        name: "http.request".into(),
        resource: "POST /cart".into(),
        trace_id: 0x1122_3344_5566_7788,
        span_id: 10,
        parent_id: 0,
        start: 1_690_000_000_000_000_000,
        duration: 1_500_000,
        error: 1,
        meta: HashMap::from([
            ("_dd.p.tid".into(), "64f5a1b200000000".into()),
            ("span.kind".into(), "server".into()),
            ("env".into(), "prod".into()),
            ("_dd.p.dm".into(), "-4".into()),
        ]),
        metrics: HashMap::from([
            ("_sampling_priority_v1".into(), 2.0),
            ("_top_level".into(), 1.0),
            ("_dd.measured".into(), 1.0),
        ]),
        r#type: "web".into(),
        meta_struct: HashMap::from([("_dd.stack".into(), vec![0x81, 0xa1, b'a', 0x01])]),
        span_links: vec![SpanLink {
            trace_id: 0x99,
            trace_id_high: 0x1234,
            span_id: 0x42,
            attributes: HashMap::from([("link.name".into(), "retry".into())]),
            tracestate: "dd=s:2;o:rum".into(),
            flags: 0x8000_0001,
        }],
        span_events: vec![SpanEvent {
            time_unix_nano: 1_690_000_000_000_500_000,
            name: "exception".into(),
            attributes: HashMap::from([
                ("str".into(), AttributeAnyValue { string_value: "boom".into(), ..typed(0) }),
                ("bool".into(), AttributeAnyValue { bool_value: true, ..typed(1) }),
                ("int".into(), AttributeAnyValue { int_value: -42, ..typed(2) }),
                ("double".into(), AttributeAnyValue { double_value: 2.5, ..typed(3) }),
                (
                    "array".into(),
                    AttributeAnyValue {
                        array_value: Some(AttributeArray {
                            values: vec![
                                AttributeArrayValue {
                                    r#type: 0,
                                    string_value: "a".into(),
                                    ..Default::default()
                                },
                                AttributeArrayValue {
                                    r#type: 1,
                                    bool_value: true,
                                    ..Default::default()
                                },
                                AttributeArrayValue {
                                    r#type: 2,
                                    int_value: 7,
                                    ..Default::default()
                                },
                                AttributeArrayValue {
                                    r#type: 3,
                                    double_value: 0.5,
                                    ..Default::default()
                                },
                            ],
                        }),
                        ..typed(4)
                    },
                ),
            ]),
        }],
    }
}

fn child_span() -> Span {
    Span {
        service: "checkout".into(),
        name: "postgres.query".into(),
        resource: "SELECT 1".into(),
        trace_id: 0x1122_3344_5566_7788,
        span_id: 11,
        parent_id: 10,
        start: 1_690_000_000_000_100_000,
        duration: 200_000,
        r#type: "sql".into(),
        meta: HashMap::from([("span.kind".into(), "client".into())]),
        ..Default::default()
    }
}

fn tracer_payload() -> TracerPayload {
    TracerPayload {
        container_id: "c0ffee".into(),
        language_name: "python".into(),
        language_version: "3.12.1".into(),
        tracer_version: "2.9.0".into(),
        runtime_id: "8f4e0d9a-1111-2222-3333-444455556666".into(),
        chunks: vec![TraceChunk {
            priority: 2,
            origin: "synthetics".into(),
            spans: vec![rich_span(), child_span()],
            tags: HashMap::from([("_dd.p.dm".into(), "-4".into())]),
            dropped_trace: true,
        }],
        tags: HashMap::from([("_dd.tags.container".into(), "kube_namespace:shop".into())]),
        env: "prod".into(),
        hostname: "web-1".into(),
        app_version: "1.4.2".into(),
        container_debug: Some(ContainerDebug {
            error: "".into(),
            latency_ms: 3,
            was_buffered: true,
            buffer_ms: 0,
            buffer_eviction_reason: "".into(),
        }),
    }
}

fn agent_payload() -> AgentPayload {
    AgentPayload {
        host_name: "agent-host".into(),
        env: "prod".into(),
        tracer_payloads: vec![
            tracer_payload(),
            TracerPayload {
                language_name: "go".into(),
                chunks: vec![TraceChunk {
                    priority: -1,
                    spans: vec![Span {
                        trace_id: 5,
                        span_id: 6,
                        name: "work".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        tags: HashMap::from([("version".into(), "7.83.3".into())]),
        agent_version: "7.83.3".into(),
        target_tps: 10.0,
        error_tps: 10.0,
        rare_sampler_enabled: true,
        idx_tracer_payloads: Vec::new(),
    }
}

/// `w2b-wire-shapes.md` §B1's minimal v0.4 example, verbatim.
fn minimal_v04() -> Vec<u8> {
    let mut w = Writer::new();
    w.write_array_len(1);
    w.write_array_len(1);
    w.write_map_len(12);
    for (k, v) in [("service", "web"), ("name", "http.request"), ("resource", "GET /")] {
        w.write_str(k);
        w.write_str(v);
    }
    for (k, v) in [("trace_id", 1), ("span_id", 2), ("parent_id", 0)] {
        w.write_str(k);
        w.write_u64(v);
    }
    w.write_str("start");
    w.write_i64(1_690_000_000_000_000_000);
    w.write_str("duration");
    w.write_i64(500_000);
    w.write_str("error");
    w.write_i64(0);
    w.write_str("meta");
    w.write_map_len(1);
    w.write_str("env");
    w.write_str("prod");
    w.write_str("metrics");
    w.write_map_len(1);
    w.write_str("_sampling_priority_v1");
    w.write_i64(1);
    w.write_str("type");
    w.write_str("web");
    w.into_inner()
}

/// §B2's all-zero v0.5 span plus one with a dictionary-indexed meta entry.
fn minimal_v05() -> Vec<u8> {
    let mut w = Writer::new();
    w.write_array_len(2);
    w.write_array_len(4);
    for s in ["", "web", "env", "prod"] {
        w.write_str(s);
    }
    w.write_array_len(1);
    w.write_array_len(2);
    w.write_array_len(12);
    for _ in 0..9 {
        w.write_u64(0);
    }
    w.write_map_len(0);
    w.write_map_len(0);
    w.write_u64(0);
    w.write_array_len(12);
    for v in [1u64, 1, 1, 7, 8, 0, 100, 50, 0] {
        w.write_u64(v);
    }
    w.write_map_len(1);
    w.write_u64(2);
    w.write_u64(3);
    w.write_map_len(0);
    w.write_u64(1);
    w.into_inner()
}

#[test]
fn minimal_v04_example() {
    let d = assert_fixed_point(Form::V04, &minimal_v04());
    let e = &d[0].events[0];
    assert_eq!(e.attributes.get(ATTR_SERVICE_NAME), Some(&Value::str("web")));
    assert_eq!(e.attributes.get("_sampling_priority_v1"), Some(&Value::F64(1.0)));
    assert_eq!(e.span.as_ref().unwrap().status, SpanStatus::Unset);
    assert_cross_form(&d[0]);
}

#[test]
fn minimal_v05_example() {
    let d = assert_fixed_point(Form::V05, &minimal_v05());
    assert_eq!(d[0].events.len(), 2);
    assert!(d[0].events[0].attributes.is_empty(), "the all-zero span carries nothing");
    assert_eq!(d[0].events[1].attributes.get("env"), Some(&Value::str("prod")));
    assert_cross_form(&d[0]);
}

#[test]
fn a_rich_tracer_payload_in_every_form() {
    let batches = assert_fixed_point(Form::Agent, &agent_payload().encode_to_vec());
    assert_eq!(batches.len(), 2, "one batch per TracerPayload");
    for b in &batches {
        assert_eq!(
            b.resource.attributes.get(RESOURCE_ATTR_AGENT_HOSTNAME),
            Some(&Value::str("agent-host"))
        );
        assert_eq!(b.resource.attributes.get(RESOURCE_ATTR_AGENT_ENV), Some(&Value::str("prod")));
    }
    assert_eq!(
        batches[1].resource.attributes.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
        Some(&Value::str("go"))
    );
    let rich = &batches[0];
    let root = &rich.events[0];
    let span = root.span.as_ref().unwrap();
    assert_eq!(&span.trace_id[..8], &0x64f5_a1b2_0000_0000u64.to_be_bytes());
    assert_eq!(&span.trace_id[8..], &0x1122_3344_5566_7788u64.to_be_bytes());
    assert_eq!(rich.events[1].span.as_ref().unwrap().trace_id, span.trace_id, "chunk-wide");
    assert_eq!(span.kind, SpanKind::Server);
    assert_eq!(span.links[0].trace_id[..8], 0x1234u64.to_be_bytes());
    assert_eq!(span.events[0].attributes.len(), 5);
    assert!(matches!(root.attributes.get("_dd.stack"), Some(Value::Bytes(_))));
    assert_eq!(root.attributes.get(ATTR_CHUNK_PRIORITY), Some(&Value::I64(2)));
    assert_eq!(root.attributes.get(ATTR_CHUNK_ORIGIN), Some(&Value::str("synthetics")));
    assert_eq!(root.attributes.get(ATTR_CHUNK_DROPPED_TRACE), Some(&Value::Bool(true)));
    assert!(matches!(root.attributes.get(ATTR_CHUNK_TAGS), Some(Value::Map(_))));

    // The same batch through the tracer forms.
    for form in [Form::V07, Form::V04, Form::V05] {
        assert_fixed_point(form, &encode(form, rich).unwrap());
    }
    let v07 = decode(Form::V07, &encode(Form::V07, rich).unwrap()).remove(0);
    let mut want = rich.clone();
    let mut resource = (*want.resource).clone();
    resource.attributes.remove(RESOURCE_ATTR_AGENT_HOSTNAME);
    resource.attributes.remove(RESOURCE_ATTR_AGENT_ENV);
    for key in [
        "datadog.agent.version",
        "datadog.agent.target_tps",
        "datadog.agent.error_tps",
        "datadog.agent.rare_sampler_enabled",
        "datadog.agent.tags",
    ] {
        resource.attributes.remove(key);
    }
    want.resource = resource.into();
    assert_eq!(v07, want, "v0.7 carries everything but the Agent envelope");
    assert_cross_form(rich);
}

#[test]
fn an_agent_payload_with_no_tracer_payloads_is_no_batches() {
    let body = AgentPayload { host_name: "h".into(), ..Default::default() }.encode_to_vec();
    assert!(decode(Form::Agent, &body).is_empty());
}

// -- proptest -------------------------------------------------------------------------------------

/// Keys drawn from Datadog's reserved names, this crate's per-span carrier names, and ordinary
/// ones. Not a chunk carrier's name: a span-level `meta` entry spelled like one is read as the
/// chunk's field, which only one span per chunk supplies on encode.
fn key() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("_dd.p.dm".to_string()),
        Just("_dd.origin".to_string()),
        Just("_sampling_priority_v1".to_string()),
        Just("_top_level".to_string()),
        Just("_dd.measured".to_string()),
        Just("span.kind".to_string()),
        Just("env".to_string()),
        Just("version".to_string()),
        Just("service.name".to_string()),
        Just("datadog.span.error".to_string()),
        "[a-z][a-z0-9_.]{0,8}",
    ]
}

fn text() -> impl Strategy<Value = String> {
    prop_oneof![Just(String::new()), "[ -~]{0,12}", Just("server".into()), Just("client".into())]
}

fn finite() -> impl Strategy<Value = f64> {
    prop_oneof![-1e9..1e9f64, Just(0.0), Just(1.0), (0u32..4).prop_map(f64::from)]
}

fn array_value() -> impl Strategy<Value = AttributeArrayValue> {
    (0i32..5, text(), any::<bool>(), any::<i64>(), finite()).prop_map(|(t, s, b, i, d)| {
        AttributeArrayValue {
            r#type: t,
            string_value: s,
            bool_value: b,
            int_value: i,
            double_value: d,
        }
    })
}

fn any_value() -> impl Strategy<Value = AttributeAnyValue> {
    (
        0i32..6,
        text(),
        any::<bool>(),
        any::<i64>(),
        finite(),
        proptest::option::of(proptest::collection::vec(array_value(), 0..3)),
    )
        .prop_map(|(t, s, b, i, d, arr)| AttributeAnyValue {
            r#type: t,
            string_value: s,
            bool_value: b,
            int_value: i,
            double_value: d,
            array_value: arr.map(|values| AttributeArray { values }),
        })
}

fn link() -> impl Strategy<Value = SpanLink> {
    (
        any::<u64>(),
        prop_oneof![Just(0u64), any::<u64>()],
        any::<u64>(),
        proptest::collection::hash_map(key(), text(), 0..3),
        text(),
        prop_oneof![Just(0u32), Just(0x8000_0001u32), any::<u32>()],
    )
        .prop_map(|(trace_id, trace_id_high, span_id, attributes, tracestate, flags)| {
            SpanLink { trace_id, trace_id_high, span_id, attributes, tracestate, flags }
        })
}

fn span_event() -> impl Strategy<Value = SpanEvent> {
    (any::<u64>(), text(), proptest::collection::hash_map(key(), any_value(), 0..4)).prop_map(
        |(time_unix_nano, name, attributes)| SpanEvent { time_unix_nano, name, attributes },
    )
}

/// A span of trace `trace_id`; `tid` is its `_dd.p.tid`, if it carries one.
fn span(trace_id: u64, tid: Option<String>) -> impl Strategy<Value = Span> {
    (
        (text(), text(), text(), text()),
        (any::<u64>(), prop_oneof![Just(0u64), any::<u64>()]),
        (0i64..2_000_000_000_000_000_000, prop_oneof![0i64..10_000_000, Just(-1i64)]),
        prop_oneof![Just(0i32), Just(1i32), any::<i32>()],
        proptest::collection::hash_map(key(), text(), 0..5),
        proptest::collection::hash_map(key(), finite(), 0..4),
        proptest::collection::hash_map(key(), proptest::collection::vec(any::<u8>(), 0..6), 0..2),
        proptest::collection::vec(link(), 0..2),
        proptest::collection::vec(span_event(), 0..2),
    )
        .prop_map(
            move |(
                (service, name, resource, r#type),
                (span_id, parent_id),
                (start, duration),
                error,
                mut meta,
                metrics,
                meta_struct,
                span_links,
                span_events,
            )| {
                meta.remove("_dd.p.tid");
                if let Some(tid) = &tid {
                    meta.insert("_dd.p.tid".into(), tid.clone());
                }
                Span {
                    service,
                    name,
                    resource,
                    trace_id,
                    span_id,
                    parent_id,
                    start,
                    duration,
                    error,
                    meta,
                    metrics,
                    r#type,
                    meta_struct,
                    span_links,
                    span_events,
                }
            },
        )
}

/// A chunk of 1-3 spans sharing trace `index`'s id (distinct per chunk, so regrouping by trace id
/// keeps each chunk whole), one of them carrying a `_dd.p.tid` half the time: a 64- or 128-bit
/// trace, or an unparseable tid.
fn chunk(index: u64) -> impl Strategy<Value = TraceChunk> {
    let tid = prop_oneof![
        Just(None),
        any::<u64>().prop_map(|h| Some(format!("{h:016x}"))),
        Just(Some("zz".to_string())),
    ];
    (any::<u32>(), tid, 1usize..4)
        .prop_flat_map(move |(salt, tid, n)| {
            let trace_id = (u64::from(salt) << 16) | index;
            let spans: Vec<_> = (0..n)
                .map(|i| span(trace_id, if i == n - 1 { tid.clone() } else { None }))
                .collect();
            (
                spans,
                prop_oneof![Just(-128i32), -1i32..3],
                text(),
                proptest::collection::hash_map(key(), text(), 0..2),
                any::<bool>(),
            )
        })
        .prop_map(|(spans, priority, origin, tags, dropped_trace)| TraceChunk {
            priority,
            origin,
            spans,
            tags,
            dropped_trace,
        })
}

fn tracer(base: u64) -> impl Strategy<Value = TracerPayload> {
    (
        proptest::collection::vec(any::<()>(), 1..4),
        (text(), text(), text(), text(), text(), text(), text(), text()),
        proptest::collection::hash_map(key(), text(), 0..2),
        proptest::option::of((text(), any::<i64>(), any::<bool>(), any::<i64>(), text())),
    )
        .prop_flat_map(move |(n, strings, tags, debug)| {
            let chunks: Vec<_> = (0..n.len() as u64).map(|i| chunk(base + i + 1)).collect();
            (chunks, Just(strings), Just(tags), Just(debug))
        })
        .prop_map(|(chunks, s, tags, debug)| TracerPayload {
            container_id: s.0,
            language_name: s.1,
            language_version: s.2,
            tracer_version: s.3,
            runtime_id: s.4,
            chunks,
            tags,
            env: s.5,
            hostname: s.6,
            app_version: s.7,
            container_debug: debug.map(|(error, latency_ms, was_buffered, buffer_ms, reason)| {
                ContainerDebug {
                    error,
                    latency_ms,
                    was_buffered,
                    buffer_ms,
                    buffer_eviction_reason: reason,
                }
            }),
        })
}

fn agent() -> impl Strategy<Value = AgentPayload> {
    (
        proptest::collection::vec(tracer(0), 0..1),
        proptest::collection::vec(tracer(100), 0..2),
        (text(), text(), text()),
        proptest::collection::hash_map(key(), text(), 0..2),
        (finite(), finite(), any::<bool>()),
    )
        .prop_map(
            |(a, b, (host_name, env, agent_version), tags, (target_tps, error_tps, rare))| {
                AgentPayload {
                    host_name,
                    env,
                    tracer_payloads: a.into_iter().chain(b).collect(),
                    tags,
                    agent_version,
                    target_tps,
                    error_tps,
                    rare_sampler_enabled: rare,
                    idx_tracer_payloads: Vec::new(),
                }
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn generated_agent_payloads_are_a_fixed_point_in_every_form(payload in agent()) {
        let batches = assert_fixed_point(Form::Agent, &payload.encode_to_vec());
        prop_assert_eq!(batches.len(), payload.tracer_payloads.len());
        for batch in &batches {
            assert_fixed_point_everywhere(batch);
            assert_cross_form(batch);
        }
    }
}
