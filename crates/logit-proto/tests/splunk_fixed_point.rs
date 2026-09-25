//! Pure-codec fixed-point tests for Splunk HEC: `docs/adr/lossless-transit.md`'s round-trip
//! requirement, exercised directly against [`SplunkDecoder`]/[`SplunkEncoder`] with no listener,
//! sink, or HTTP in between.
//!
//! Every case starts from wire bytes and asserts, from the first hop on:
//!
//! 1. **`decode(encode(decode(b))) == decode(b)`**, whole-`Vec<EventBatch>` equality (resources,
//!    batch boundaries, events, every attribute, log, metric, and span field);
//! 2. **`encode(decode(encode(decode(b)))) == encode(decode(b))`** on bytes.
//!
//! The first hop applies `splunk/mod.rs`'s permitted normalizations (framing, number spelling, a
//! string `time`, a flattened `fields`, the single-metric form, span-kind spellings), so the input
//! is not a fixed point but its first decode is.
//!
//! The `proptest`s generate bodies from a grammar, so they reach every decode branch a valid body
//! can: array and concatenated framing, every `time` form, each carrier present or absent, string,
//! object, array, number, and bool log bodies, the OpenTelemetry exporter's log fields (valid and
//! invalid trace pairs included), both metric forms under each `metric_type`, and span objects
//! with events, links, and statuses, over flat and nested `fields`. Generated field keys start
//! with `x` so none collides with a reserved or carrier name, generated metric names already
//! satisfy the sanitizer, and no generated value is `NaN`, which equality can't compare.

use logit_core::interner::resolve;
use logit_core::{EventBatch, MetricKind, Severity, SpanKind, SpanStatus, Temporality, Value};
use logit_proto::splunk::{SplunkDecoder, SplunkEncoder};
use logit_proto::Encoder;
use proptest::prelude::*;
use serde_json::{json, Map, Value as Json};

/// An absent `time` is stamped with this.
const RECEIVED_AT: i64 = 1_690_000_000_123_456_789;

fn decode(body: &[u8]) -> Vec<EventBatch> {
    SplunkDecoder::new()
        .decode_events(body, RECEIVED_AT)
        .unwrap_or_else(|e| panic!("decode failed ({e}): {}", String::from_utf8_lossy(body)))
}

fn encode(batches: &[EventBatch]) -> Vec<u8> {
    let mut encoder = SplunkEncoder::new();
    let mut body = Vec::new();
    for batch in batches {
        body.extend_from_slice(&encoder.encode(batch).expect("encode never fails"));
    }
    body
}

fn assert_fixed_point(body: &[u8]) -> Vec<EventBatch> {
    let d1 = decode(body);
    let e1 = encode(&d1);
    let d2 = decode(&e1);
    assert_eq!(d1, d2, "model fixed point; first encode: {}", String::from_utf8_lossy(&e1));
    let e2 = encode(&d2);
    assert_eq!(e1, e2, "wire fixed point");
    d1
}

// -- hand-written vectors -------------------------------------------------------------------------

#[test]
fn every_envelope_key() {
    let batches = assert_fixed_point(
        br#"{"time":1700000000.123456789,"host":"web-1","source":"/var/log/app.log","sourcetype":"app:json","index":"main","event":"hello","fields":{"env":"prod","n":3,"ratio":0.25,"ok":true,"tags":["a","b"],"none":null}}"#,
    );
    let attrs = &batches[0].resource.attributes;
    assert_eq!(attrs.get("host.name"), Some(&Value::str("web-1")));
    assert_eq!(attrs.get("com.splunk.source"), Some(&Value::str("/var/log/app.log")));
    assert_eq!(attrs.get("com.splunk.sourcetype"), Some(&Value::str("app:json")));
    assert_eq!(attrs.get("com.splunk.index"), Some(&Value::str("main")));
    assert_eq!(batches[0].events[0].timestamp, 1_700_000_000_123_456_789);
    assert_eq!(batches[0].events[0].attributes.len(), 6);
}

#[test]
fn array_and_concatenated_framing_and_whitespace() {
    let array = assert_fixed_point(
        b"[ {\"event\":\"a\",\"time\":1} ,\n {\"event\":{\"k\":[1,2]},\"time\":2} ]",
    );
    let concatenated =
        assert_fixed_point(b"{\"event\":\"a\",\"time\":1}\n\n{\"event\":{\"k\":[1,2]},\"time\":2}");
    assert_eq!(array, concatenated);
}

#[test]
fn time_edges() {
    for (time, nanos) in [
        ("0", 0),
        ("-1.5", -1_500_000_000),
        ("1700000000", 1_700_000_000_000_000_000),
        ("\"1700000000.000001\"", 1_700_000_000_000_001_000),
        ("1.7e9", 1_700_000_000_000_000_000),
        ("1700000000.1234567891", 1_700_000_000_123_456_768),
    ] {
        let body = format!(r#"{{"time":{time},"event":"x"}}"#);
        let batches = assert_fixed_point(body.as_bytes());
        let got = batches[0].events[0].timestamp;
        if time.len() > 20 {
            // Past nanosecond precision: read through an f64, so within a microsecond.
            assert!((got - nanos).abs() < 1_000, "{time}: {got}");
        } else {
            assert_eq!(got, nanos, "{time}");
        }
    }
    let absent = assert_fixed_point(br#"{"event":"x"}"#);
    assert_eq!(absent[0].events[0].timestamp, RECEIVED_AT);
}

#[test]
fn both_metric_forms_and_every_metric_type() {
    assert_fixed_point(
        br#"{"time":1,"event":"metric","fields":{"metric_name":"cpu.user","_value":12.5,"host.role":"db"}}
{"time":1,"event":"metric","fields":{"metric_name:mem.free":1024,"metric_name:mem.used":2048,"metric_type":"Gauge"}}
{"time":1,"event":"metric","fields":{"metric_name:requests":17,"metric_type":"Sum","code":"200"}}
{"time":1,"event":"metric","fields":{"metric_name:lat_bucket":3,"metric_type":"Histogram","le":"+Inf"}}
{"time":1,"event":"metric","fields":{"metric_name:inf":"+Inf","metric_name:neg":"-Inf"}}"#,
    );
}

#[test]
fn nested_fields_leave_flattened() {
    let d1 = assert_fixed_point(
        br#"{"time":1,"event":"x","fields":{"a":{"b":{"c":1},"d":[1,2]},"e":{},"l":[{"x":1}]}}"#,
    );
    let attrs = &d1[0].events[0].attributes;
    assert_eq!(attrs.get("a.b.c"), Some(&Value::I64(1)));
    assert_eq!(attrs.get("l"), Some(&Value::str(r#"[{"x":1}]"#)));
    assert_eq!(attrs.len(), 3);
}

#[test]
fn a_blank_or_missing_event_is_skipped_and_the_rest_delivered() {
    let d1 = assert_fixed_point(br#"{"event":""}{"event":"kept","time":1}{"time":2}"#);
    assert_eq!(d1[0].events.len(), 1);
}

#[test]
fn uppercase_hex_ids_leave_lowercase() {
    let body = br#"{"time":1,"event":"log","fields":{"trace_id":"0AF7651916CD43DD8448EB211C80319C","span_id":"B7AD6B7169203331"}}{"time":1,"event":{"trace_id":"4BF92F3577B34DA6A3CE929D0E0E4736","span_id":"00F067AA0BA902B7","parent_span_id":"B7AD6B7169203331","start_time":1,"end_time":2,"links":[{"trace_id":"0AF7651916CD43DD8448EB211C80319C","span_id":"B7AD6B7169203331","trace_state":""}]}}"#;
    let d1 = assert_fixed_point(body);
    assert!(d1[0].events[0].log.as_ref().unwrap().trace.is_some());
    assert!(d1[0].events[1].span.is_some());
    let e1 = String::from_utf8(encode(&d1)).unwrap();
    assert!(e1.contains(r#""trace_id":"0af7651916cd43dd8448eb211c80319c""#), "{e1}");
    assert!(e1.contains(r#""parent_span_id":"b7ad6b7169203331""#), "{e1}");
    assert!(e1.contains(r#""span_id":"00f067aa0ba902b7""#), "{e1}");
    for upper in ["0AF7651916CD43DD", "B7AD6B7169203331", "4BF92F3577B34DA6", "00F067AA0BA902B7"] {
        assert!(!e1.contains(upper), "{e1}");
    }
}

#[test]
fn raw_lines_relay_through_event() {
    let envelope = logit_proto::splunk::Envelope {
        host: Some("h".into()),
        source: Some("udp:514".into()),
        sourcetype: Some("syslog".into()),
        index: None,
    };
    let raw = SplunkDecoder::new().decode_raw(b"line one\r\nline two\n", &envelope, RECEIVED_AT);
    assert_eq!(raw.events.len(), 2);
    let relayed = assert_fixed_point(&encode(std::slice::from_ref(&raw)));
    assert_eq!(relayed, vec![raw]);
}

/// A body in the OpenTelemetry Collector `splunk_hec` exporter's shape, one object per record
/// (its `use_multi_metric_format` default), written by hand from `docs/plans/splunk-relay.md`'s
/// "Third-party HEC conventions" table. It carries what the recorded exporter captures in
/// `testdata/interop/splunk/` don't (`otel.log.name`, a summary quantile, a span event);
/// `tests/splunk_interop.rs` holds the codec to the recorded ones.
const OTEL_EXPORTER_BODY: &str = concat!(
    r#"{"time":1700000000.123,"host":"web-1","source":"app","sourcetype":"otel","index":"main","event":"user logged in","fields":{"service.name":"auth","k8s.pod.name":"auth-7","otel.log.severity.text":"INFO","otel.log.severity.number":9,"otel.log.name":"login","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","user.id":42}}"#,
    r#"{"time":1700000000.2,"host":"web-1","source":"app","sourcetype":"otel","index":"main","event":{"msg":"structured","attempt":2},"fields":{"service.name":"auth"}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Gauge","metric_name:process.memory.usage":123456789}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Sum","metric_name:http.server.requests":1500}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Histogram","metric_name:http.server.duration_sum":42.5}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Histogram","metric_name:http.server.duration_count":10}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Histogram","le":"0.5","metric_name:http.server.duration_bucket":4}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Histogram","le":"+Inf","metric_name:http.server.duration_bucket":10}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Summary","qt":"0.99","metric_name:rpc.latency_0.99":0.8}}"#,
    r#"{"time":1700000002.5,"host":"web-1","source":"app","sourcetype":"otel","index":"traces","event":{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","parent_span_id":"00f067aa0ba902b7","name":"POST /login","attributes":{"http.method":"POST","http.status_code":200},"end_time":1700000002750000000,"kind":"SPAN_KIND_SERVER","status":{"message":"","code":"STATUS_CODE_UNSET"},"start_time":1700000002500000000,"events":[{"name":"auth.check","timestamp":1700000002600000000}]},"fields":{"service.name":"auth","telemetry.sdk.language":"go"}}"#,
);

#[test]
fn an_otel_exporter_body_decodes_into_typed_fields_and_is_a_fixed_point() {
    let batches = assert_fixed_point(OTEL_EXPORTER_BODY.as_bytes());
    // Logs (one resource), metrics (another: no source/sourcetype/index), and the span (its
    // `fields` are its resource).
    assert_eq!(batches.len(), 3);

    let log = &batches[0].events[0];
    let record = log.log.as_ref().unwrap();
    assert_eq!(record.message, Value::str("user logged in"));
    assert_eq!(record.severity, Some(Severity::Info));
    assert_eq!(record.event_name.map(resolve), Some("login"));
    assert!(record.trace.unwrap().span_id.is_some());
    assert_eq!(log.attributes.get("service.name"), Some(&Value::str("auth")));
    assert_eq!(log.attributes.get("otel.log.severity.text"), Some(&Value::str("INFO")));
    assert!(log.attributes.get("trace_id").is_none());
    assert!(matches!(batches[0].events[1].log.as_ref().unwrap().message, Value::Map(_)));

    let metrics = &batches[1].events;
    assert_eq!(metrics.len(), 7);
    let kind = |i: usize| metrics[i].metrics[0].kind.clone();
    assert_eq!(kind(0), MetricKind::Gauge(123_456_789.0));
    assert!(matches!(
        kind(1),
        MetricKind::Sum(logit_core::Sum {
            temporality: Temporality::Cumulative,
            monotonic: true,
            ..
        })
    ));
    assert_eq!(metrics[5].attributes.get("le"), Some(&Value::str("+Inf")));
    assert_eq!(metrics[5].attributes.get("metric_type"), Some(&Value::str("Histogram")));
    assert_eq!(metrics[6].attributes.get("qt"), Some(&Value::str("0.99")));

    let span_batch = &batches[2];
    assert_eq!(
        span_batch.resource.attributes.get("telemetry.sdk.language"),
        Some(&Value::str("go"))
    );
    let span_event = &span_batch.events[0];
    assert_eq!(span_event.timestamp, 1_700_000_002_500_000_000);
    let span = span_event.span.as_ref().unwrap();
    assert_eq!(span.kind, SpanKind::Server);
    assert_eq!(span.status, SpanStatus::Unset);
    assert!(span.parent_span_id.is_some());
    assert_eq!(span_event.attributes.get("http.status_code"), Some(&Value::I64(200)));
}

// -- generated bodies -----------------------------------------------------------------------------

fn opt<T: std::fmt::Debug + Clone>(
    s: impl Strategy<Value = T>,
) -> impl Strategy<Value = Option<T>> {
    prop::option::of(s)
}

fn json_leaf() -> impl Strategy<Value = Json> {
    prop_oneof![
        Just(Json::Null),
        any::<bool>().prop_map(Json::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        any::<u64>().prop_map(|n| json!(n)),
        (-10_000i32..10_000, 0u8..4).prop_map(|(n, q)| json!(n as f64 + q as f64 / 4.0)),
        "\\PC{0,12}".prop_map(Json::String),
    ]
}

fn json_value() -> impl Strategy<Value = Json> {
    json_leaf().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Json::Array),
            prop::collection::btree_map("[a-z.]{1,6}", inner, 0..4)
                .prop_map(|m| Json::Object(m.into_iter().collect())),
        ]
    })
}

/// Extra `fields`, flat or nested, none named like a carrier or a reserved key.
fn extra_fields() -> impl Strategy<Value = Map<String, Json>> {
    prop::collection::btree_map("x[a-z_.]{0,6}", json_value(), 0..4)
        .prop_map(|m| m.into_iter().collect())
}

fn time() -> impl Strategy<Value = Option<Json>> {
    opt(prop_oneof![
        (0i64..4_000_000_000).prop_map(|s| json!(s)),
        (0i64..4_000_000_000, 0u32..1_000_000_000)
            .prop_map(|(s, ns)| { serde_json::from_str::<Json>(&format!("{s}.{ns:09}")).unwrap() }),
        (-4_000_000_000i64..0, 1u32..1_000)
            .prop_map(|(s, ms)| { serde_json::from_str::<Json>(&format!("{s}.{ms:03}")).unwrap() }),
        (0i64..4_000_000_000, 0u32..1000).prop_map(|(s, ms)| json!(format!("{s}.{ms:03}"))),
    ])
}

fn hex(len: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(0u8..16, len).prop_map(|digits| {
        let s: String =
            digits.iter().map(|d| char::from_digit(u32::from(*d), 16).unwrap()).collect();
        // All-zero ids are invalid; keep generated ones valid.
        if s.chars().all(|c| c == '0') {
            format!("{}1", &s[1..])
        } else {
            s
        }
    })
}

fn log_event() -> impl Strategy<Value = (Json, Map<String, Json>)> {
    let body = prop_oneof![
        "\\PC{1,24}".prop_map(Json::String),
        json_value().prop_filter("a log body can't be blank", |v| {
            !matches!(v, Json::Null) && v.as_str() != Some("")
        }),
    ];
    let otel = (
        opt(1i64..=24),
        opt(prop::sample::select(vec!["INFO", "Warn", "error", "custom", ""])),
        opt("[a-z.]{0,8}"),
        opt((hex(32), opt(hex(16)))),
        opt(("[0-9a-z]{0,6}", "[0-9a-z]{0,6}")),
    );
    (body, otel, extra_fields()).prop_map(
        |(body, (number, text, name, trace, bad_trace), mut fields)| {
            let mut put = |key: &str, value: Option<Json>| {
                if let Some(value) = value {
                    fields.insert(key.into(), value);
                }
            };
            put("otel.log.severity.number", number.map(|n| json!(n)));
            put("otel.log.severity.text", text.map(|t| json!(t)));
            put("otel.log.name", name.map(Json::String));
            match (trace, bad_trace) {
                (Some((trace_id, span_id)), _) => {
                    put("trace_id", Some(json!(trace_id)));
                    put("span_id", span_id.map(Json::String));
                }
                (None, Some((trace_id, span_id))) => {
                    put("trace_id", Some(json!(trace_id)));
                    put("span_id", Some(json!(span_id)));
                }
                (None, None) => {}
            }
            (body, fields)
        },
    )
}

fn metric_value() -> impl Strategy<Value = Json> {
    prop_oneof![
        (-1_000_000i64..1_000_000).prop_map(|n| json!(n)),
        (-10_000i32..10_000, 0u8..8).prop_map(|(n, q)| json!(n as f64 + q as f64 / 8.0)),
        any::<f64>().prop_filter("finite", |f| f.is_finite()).prop_map(|f| json!(f)),
        prop::sample::select(vec!["+Inf", "-Inf"]).prop_map(|s| json!(s)),
    ]
}

fn metric_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_.:]{0,10}".prop_filter("no metric_name substring", |n| !n.contains("metric_name"))
}

fn metric_event() -> impl Strategy<Value = (Json, Map<String, Json>)> {
    (
        prop::collection::btree_map(metric_name(), metric_value(), 0..4),
        opt((metric_name(), metric_value())),
        opt(prop::sample::select(vec!["Gauge", "Sum", "Histogram", "Summary"])),
        prop::collection::btree_map("x[a-z_.]{0,6}", "[a-z0-9]{0,6}", 0..4),
    )
        .prop_filter("a metric event needs a measurement", |(multi, single, _, _)| {
            !multi.is_empty() || single.is_some()
        })
        .prop_map(|(multi, single, metric_type, dims)| {
            let mut fields: Map<String, Json> =
                dims.into_iter().map(|(k, v)| (k, Json::String(v))).collect();
            for (name, value) in multi {
                fields.insert(format!("metric_name:{name}"), value);
            }
            if let Some((name, value)) = single {
                fields.insert("metric_name".into(), json!(name));
                fields.insert("_value".into(), value);
            }
            if let Some(t) = metric_type {
                fields.insert("metric_type".into(), json!(t));
            }
            (json!("metric"), fields)
        })
}

fn attributes() -> impl Strategy<Value = Json> {
    prop::collection::btree_map("[a-z.]{1,6}", json_value(), 0..3)
        .prop_map(|m| Json::Object(m.into_iter().collect()))
}

fn span_event() -> impl Strategy<Value = (Json, Map<String, Json>)> {
    let events = prop::collection::vec(
        (opt(attributes()), "\\PC{0,8}", 0i64..4_000_000_000_000_000_000),
        0..3,
    );
    let links = prop::collection::vec((opt(attributes()), hex(32), hex(16), "[a-z=,]{0,6}"), 0..3);
    (
        (hex(32), hex(16), opt(hex(16)), "\\PC{0,12}"),
        (
            prop::sample::select(vec![
                json!("Internal"),
                json!("Server"),
                json!("Client"),
                json!("Producer"),
                json!("Consumer"),
                json!("Unspecified"),
                json!("SPAN_KIND_CLIENT"),
                json!(3),
            ]),
            prop::sample::select(vec![json!("Unset"), json!("Ok"), json!("Error"), json!(2)]),
            "\\PC{0,8}",
        ),
        (0i64..4_000_000_000_000_000_000, 0i64..1_000_000_000),
        opt(attributes()),
        events,
        links,
        prop::collection::btree_map("x[a-z_.]{0,6}", "[a-z0-9]{0,6}", 0..3),
    )
        .prop_map(
            |(
                (trace_id, span_id, parent, name),
                (kind, code, message),
                (start, duration),
                attrs,
                events,
                links,
                resource,
            )| {
                let mut span = Map::new();
                span.insert("trace_id".into(), json!(trace_id));
                span.insert("span_id".into(), json!(span_id));
                span.insert("parent_span_id".into(), json!(parent.unwrap_or_default()));
                span.insert("name".into(), json!(name));
                if let Some(attrs) = attrs {
                    span.insert("attributes".into(), attrs);
                }
                span.insert("end_time".into(), json!(start + duration));
                span.insert("kind".into(), kind);
                span.insert("status".into(), json!({"message": message, "code": code}));
                span.insert("start_time".into(), json!(start));
                if !events.is_empty() {
                    let events: Vec<Json> = events
                        .into_iter()
                        .map(|(attrs, name, ts)| {
                            let mut e = Map::new();
                            if let Some(attrs) = attrs {
                                e.insert("attributes".into(), attrs);
                            }
                            e.insert("name".into(), json!(name));
                            e.insert("timestamp".into(), json!(ts));
                            Json::Object(e)
                        })
                        .collect();
                    span.insert("events".into(), json!(events));
                }
                if !links.is_empty() {
                    let links: Vec<Json> = links
                        .into_iter()
                        .map(|(attrs, trace_id, span_id, state)| {
                            let mut l = Map::new();
                            if let Some(attrs) = attrs {
                                l.insert("attributes".into(), attrs);
                            }
                            l.insert("trace_id".into(), json!(trace_id));
                            l.insert("span_id".into(), json!(span_id));
                            l.insert("trace_state".into(), json!(state));
                            Json::Object(l)
                        })
                        .collect();
                    span.insert("links".into(), json!(links));
                }
                let fields = resource.into_iter().map(|(k, v)| (k, Json::String(v))).collect();
                (Json::Object(span), fields)
            },
        )
}

fn hec_object() -> impl Strategy<Value = Json> {
    (
        prop_oneof![3 => log_event(), 2 => metric_event(), 1 => span_event()],
        time(),
        (
            opt(prop::sample::select(vec!["web-1", "web-2", ""])),
            opt(prop::sample::select(vec!["app", "/var/log/x.log"])),
            opt(prop::sample::select(vec!["otel", "_json"])),
            opt(prop::sample::select(vec!["main", "metrics"])),
        ),
    )
        .prop_map(|((event, fields), time, (host, source, sourcetype, index))| {
            let mut obj = Map::new();
            for (key, value) in [
                ("time", time),
                ("host", host.map(|s| json!(s))),
                ("source", source.map(|s| json!(s))),
                ("sourcetype", sourcetype.map(|s| json!(s))),
                ("index", index.map(|s| json!(s))),
            ] {
                if let Some(value) = value {
                    obj.insert(key.into(), value);
                }
            }
            obj.insert("event".into(), event);
            if !fields.is_empty() {
                obj.insert("fields".into(), Json::Object(fields));
            }
            Json::Object(obj)
        })
}

/// A body of 1 to 5 objects, as an array or concatenated with optional whitespace between.
fn hec_body() -> impl Strategy<Value = Vec<u8>> {
    (prop::collection::vec(hec_object(), 1..6), any::<bool>(), any::<bool>()).prop_map(
        |(objects, as_array, spaced)| {
            if as_array {
                serde_json::to_vec(&objects).unwrap()
            } else {
                let mut body = Vec::new();
                for object in objects {
                    body.extend_from_slice(&serde_json::to_vec(&object).unwrap());
                    if spaced {
                        body.extend_from_slice(b"\r\n ");
                    }
                }
                body
            }
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn generated_bodies_are_a_fixed_point(body in hec_body()) {
        assert_fixed_point(&body);
    }
}
