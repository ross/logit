//! Pure-codec fixed-point tests for Datadog's metric routes (`logit_proto::datadog::series` and
//! `::sketches`): [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md)'s round-trip
//! requirement, exercised directly against `DatadogDecoder`/`DatadogEncoder` with no listener or
//! sink in between. The mirror of `tests/collectd_fixed_point.rs`.
//!
//! Two properties, per route:
//!
//! 1. **`decode(encode(decode(b))) == decode(b)`**, whole-`Event` equality. Starting from wire
//!    bytes rather than a hand-built batch means every permitted normalization (a fractional v1
//!    timestamp truncated, resources reordered host-first, a repeated sketch key summed) is
//!    applied once by the first decode and must be stable afterwards. Sketches compare through
//!    `DdSketch`'s structural `PartialEq`, so the round trip is bin-exact. `logit-proto`'s
//!    `serde_json` dependency enables `float_roundtrip` (`Cargo.toml`, `docs/known-gaps.md`'s
//!    Datadog entry), an exactly-rounded float parser, so the three JSON routes (v1, v2 JSON,
//!    distribution points) are exact `==` too, same as the two protobuf routes (v2 protobuf,
//!    sketches).
//! 2. **`encode(decode(encode(d))) == encode(d)` on bytes**, the same fixed point at the wire
//!    level, which catches an encoder that writes two byte strings for what its own decoder calls
//!    one batch. Asserted on every route.
//!
//! The hand-written vectors start from `w2a-wire-shapes.md`'s examples, including the Agent's v1
//! field set and a `Dogsketch` with negative, zero, positive, and above-65535 counts. The
//! `proptest`s below generate series and sketch payloads from a grammar (values, tags with bare
//! and repeated keys, every type, resources) and assert both properties on every route.

use bytes::Bytes;
use logit_core::{Event, EventBatch, MetricKind};
use logit_proto::datadog::generated::agentpayload::{
    metric_payload::{MetricPoint, MetricSeries, Resource as PbResource},
    sketch_payload::{sketch::Dogsketch, Sketch},
    Metadata, MetricPayload, Origin, SketchPayload,
};
use logit_proto::datadog::{DatadogDecoder, DatadogEncoder};
use proptest::prelude::*;
use prost::Message;
use serde_json::{json, Value as JsonValue};

const RECEIVED_AT: i64 = 1_699_000_000_123_456_789;

#[derive(Clone, Copy, Debug)]
enum Route {
    V1,
    V2Json,
    V2Protobuf,
    DistributionPoints,
    Sketches,
}

fn decode(route: Route, body: &[u8]) -> EventBatch {
    let mut d = DatadogDecoder::new();
    let result = match route {
        Route::V1 => d.decode_series_v1(body, RECEIVED_AT),
        Route::V2Json => d.decode_series_v2_json(body, RECEIVED_AT),
        Route::V2Protobuf => d.decode_series_v2_protobuf(body, RECEIVED_AT),
        Route::DistributionPoints => d.decode_distribution_points(body, RECEIVED_AT),
        Route::Sketches => d.decode_sketches(body, RECEIVED_AT),
    };
    result.unwrap_or_else(|e| panic!("{route:?} must decode: {e}"))
}

fn encode(route: Route, batch: &EventBatch) -> Option<Bytes> {
    let mut e = DatadogEncoder::new();
    match route {
        Route::V1 => e.encode_series_v1(batch),
        Route::V2Json => e.encode_series_v2_json(batch),
        Route::V2Protobuf => e.encode_series_v2_protobuf(batch),
        Route::DistributionPoints => e.encode_distribution_points(batch),
        Route::Sketches => e.encode_sketches(batch),
    }
}

/// Both properties from the module doc, from wire bytes. Returns the first decode.
fn assert_fixed_point(route: Route, body: &[u8]) -> Vec<Event> {
    let d1 = decode(route, body);
    let Some(b1) = encode(route, &d1) else {
        assert!(d1.events.is_empty(), "{route:?}: events decoded but nothing re-encoded");
        return d1.events;
    };
    let d2 = decode(route, &b1);
    assert_eq!(d1.events, d2.events, "{route:?}: decode(encode(decode(b))) != decode(b)");
    let b2 = encode(route, &d2).expect("the same events encode again");
    assert_eq!(b1, b2, "{route:?}: encode(decode(encode(d))) != encode(d) on bytes");
    d1.events
}

fn json_bytes(v: JsonValue) -> Vec<u8> {
    serde_json::to_vec(&v).unwrap()
}

// -- hand-written vectors ------------------------------------------------------------------------

#[test]
fn v1_agent_field_set() {
    let body = br#"{"series":[{"metric":"my.metric","points":[[1700000000,1.0]],"tags":["env:prod"],"host":"myhost","device":"/dev/sda1","type":"rate","interval":20,"source_type_name":"my_check","unit":"byte"}]}"#;
    let events = assert_fixed_point(Route::V1, body);
    assert_eq!(events.len(), 1);
    // The Agent's field set survives a hop: re-encode and compare as JSON.
    let batch = decode(Route::V1, body);
    let out: JsonValue = serde_json::from_slice(&encode(Route::V1, &batch).unwrap()).unwrap();
    let original: JsonValue = serde_json::from_slice(body).unwrap();
    assert_eq!(out, original);
}

#[test]
fn v1_agent_minimal_series_keeps_its_always_present_fields() {
    let body = br#"{"series":[{"metric":"a.count","points":[[1700000000,3]],"tags":[],"host":"","type":"count","interval":0}]}"#;
    assert_fixed_point(Route::V1, body);
    let batch = decode(Route::V1, body);
    let out: JsonValue = serde_json::from_slice(&encode(Route::V1, &batch).unwrap()).unwrap();
    assert_eq!(
        out,
        json!({"series":[{"metric":"a.count","points":[[1700000000,3.0]],"tags":[],"host":"","type":"count","interval":0}]})
    );
}

#[test]
fn v1_public_form() {
    let body = json_bytes(json!({"series":[
        {"metric":"pub.gauge","points":[[1475317847.0,0.7],[1475317857.9,-2]],"type":"",
         "interval":null,"host":"h","tags":["team:a","team:b","urgent","team:a","urgent:1"]},
        {"metric":"pub.default","points":[[1475317847,5]]},
        {"metric":"pub.skipped","points":[[1475317847,null]],"type":"gauge"}
    ]}));
    let events = assert_fixed_point(Route::V1, &body);
    assert_eq!(events.len(), 3);
}

#[test]
fn v2_json_example_and_full_series() {
    assert_fixed_point(
        Route::V2Json,
        br#"{"series":[{"metric":"system.load.1","points":[{"timestamp":1475317847,"value":0.7}],"resources":[{"name":"dummyhost","type":"host"}]}]}"#,
    );
    let body = json_bytes(json!({"series":[{
        "metric":"api.requests","type":1,"interval":10,"unit":"request","source_type_name":"chk",
        "points":[{"timestamp":1700000000,"value":4},{"timestamp":1700000010,"value":6.5}],
        "resources":[{"type":"db","name":"users"},{"type":"host","name":"h"},
                     {"type":"device","name":"sda"},{"type":"host","name":"h2"}],
        "metadata":{"origin":{"product":10,"service":7,"metric_type":2}},
        "tags":["team:a","team:b","bare"]
    },{
        "metric":"api.rate","type":2,"points":[{"timestamp":1700000000,"value":0.25}]
    },{
        "metric":"api.gauge","type":3,"points":[{"timestamp":1700000000,"value":-1e300}]
    }]}));
    assert_eq!(assert_fixed_point(Route::V2Json, &body).len(), 4);
}

fn full_protobuf_payload() -> MetricPayload {
    MetricPayload {
        series: vec![
            MetricSeries {
                resources: vec![
                    PbResource { r#type: "k8s_pod".into(), name: "web-1".into() },
                    PbResource { r#type: "host".into(), name: "myhost".into() },
                ],
                metric: "agent.count".into(),
                tags: vec!["env:prod".into(), "team:a".into(), "team:b".into(), "urgent".into()],
                points: vec![
                    MetricPoint { value: 3.0, timestamp: 1_700_000_000 },
                    MetricPoint { value: 0.0, timestamp: 1_700_000_010 },
                ],
                r#type: 1,
                unit: "request".into(),
                source_type_name: "System".into(),
                interval: 10,
                metadata: Some(Metadata {
                    origin: Some(Origin {
                        origin_product: 10,
                        origin_category: 11,
                        origin_service: 3,
                    }),
                }),
            },
            MetricSeries {
                metric: "agent.rate".into(),
                points: vec![MetricPoint { value: 1.5, timestamp: 1_700_000_000 }],
                r#type: 2,
                interval: 15,
                ..Default::default()
            },
            MetricSeries {
                metric: "agent.unspecified".into(),
                points: vec![MetricPoint { value: -7.25, timestamp: 1_700_000_000 }],
                r#type: 0,
                ..Default::default()
            },
            MetricSeries {
                metric: "agent.gauge".into(),
                resources: vec![PbResource { r#type: "host".into(), name: "".into() }],
                points: vec![MetricPoint { value: 42.0, timestamp: 1_700_000_000 }],
                r#type: 3,
                ..Default::default()
            },
        ],
    }
}

#[test]
fn v2_protobuf_every_field() {
    let events = assert_fixed_point(Route::V2Protobuf, &full_protobuf_payload().encode_to_vec());
    assert_eq!(events.len(), 5);
}

#[test]
fn v2_protobuf_and_json_agree() {
    // The same series through either v2 encoding decodes to the same events, bar the JSON
    // origin's lack of `category`.
    let mut payload = full_protobuf_payload();
    for s in &mut payload.series {
        if let Some(o) = s.metadata.as_mut().and_then(|m| m.origin.as_mut()) {
            o.origin_category = 0;
        }
    }
    let from_proto = decode(Route::V2Protobuf, &payload.encode_to_vec());
    let json = encode(Route::V2Json, &from_proto).unwrap();
    assert_eq!(decode(Route::V2Json, &json).events, from_proto.events);
}

#[test]
fn distribution_points_example() {
    let body = br#"{"series":[{"metric":"system.load.1","points":[[1475317847.0,[1.0,2.0]],[1475317857,[0.5]]],"host":"h","tags":["env:prod","env:dev"],"type":"distribution"}]}"#;
    let events = assert_fixed_point(Route::DistributionPoints, body);
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0].metrics[0].kind, MetricKind::Samples(_)));
}

#[test]
fn dogsketch_with_negative_zero_positive_and_large_counts() {
    let payload = SketchPayload {
        sketches: vec![Sketch {
            metric: "request.latency".into(),
            host: "myhost".into(),
            distributions: Vec::new(),
            tags: vec!["env:prod".into(), "bare".into()],
            dogsketches: vec![
                Dogsketch {
                    ts: 1_700_000_000,
                    cnt: 200_016,
                    min: -40.0,
                    max: 1200.5,
                    avg: 0.0,
                    sum: 123_456.75,
                    // Negative, then zero, then positive; key 1400 split across four entries
                    // the way the Agent splits a count above 65535.
                    k: vec![-1500, -3, 0, 1, 1400, 1400, 1400, 1400],
                    n: vec![5, 1, 7, 3, 65535, 65535, 65535, 3395],
                },
                Dogsketch {
                    ts: 1_700_000_010,
                    cnt: 1,
                    min: 0.0,
                    max: 0.0,
                    avg: 0.0,
                    sum: 0.0,
                    k: vec![0],
                    n: vec![1],
                },
            ],
            metadata: Some(Metadata {
                origin: Some(Origin { origin_product: 10, origin_category: 1, origin_service: 0 }),
            }),
        }],
        metadata: None,
    };
    let body = payload.encode_to_vec();
    let events = assert_fixed_point(Route::Sketches, &body);
    assert_eq!(events.len(), 2);
    let MetricKind::Distribution(sketch) = &events[0].metrics[0].kind else { panic!() };
    assert_eq!(sketch.positive_bins().last().unwrap().count, 200_000.0);
    // And the protobuf itself relays unchanged, `avg` aside (recomputed on encode).
    let relayed =
        SketchPayload::decode(encode(Route::Sketches, &decode(Route::Sketches, &body)).unwrap())
            .unwrap();
    let dog = &relayed.sketches[0].dogsketches[0];
    assert_eq!(dog.k, payload.sketches[0].dogsketches[0].k);
    assert_eq!(dog.n, payload.sketches[0].dogsketches[0].n);
}

// -- generated payloads --------------------------------------------------------------------------

fn tag() -> impl Strategy<Value = String> {
    let key = prop::sample::select(vec!["env", "team", "urgent", "a", "svc"]);
    prop_oneof![
        key.clone().prop_map(String::from),
        (key, "[a-z0-9:/._-]{0,6}").prop_map(|(k, v)| format!("{k}:{v}")),
    ]
}

fn value() -> impl Strategy<Value = f64> {
    any::<f64>().prop_filter("finite", |v| v.is_finite())
}

fn timestamp() -> impl Strategy<Value = i64> {
    1i64..4_000_000_000
}

#[derive(Clone, Debug)]
struct GenSeries {
    metric: String,
    type_code: i32,
    tags: Vec<String>,
    host: Option<String>,
    device: Option<String>,
    resources: Vec<(String, String)>,
    unit: String,
    source_type_name: String,
    interval: i64,
    origin: (u32, u32, u32),
    points: Vec<(i64, f64)>,
}

fn gen_series() -> impl Strategy<Value = GenSeries> {
    (
        "[a-z][a-z0-9._]{0,12}",
        0i32..4,
        prop::collection::vec(tag(), 0..6),
        prop::option::of("[a-z0-9-]{1,8}"),
        prop::option::of("/dev/[a-z]{1,4}"),
        prop::collection::vec(
            (prop::sample::select(vec!["db", "queue", "host", "device", ""]), "[a-z0-9]{0,5}"),
            0..3,
        ),
        "[a-z]{0,6}",
        "[a-zA-Z]{0,6}",
        prop_oneof![Just(0i64), 1i64..3600],
        (0u32..3, 0u32..3, 0u32..3),
        prop::collection::vec((timestamp(), value()), 0..4),
    )
        .prop_map(
            |(
                metric,
                type_code,
                tags,
                host,
                device,
                resources,
                unit,
                source_type_name,
                interval,
                origin,
                points,
            )| GenSeries {
                metric,
                type_code,
                tags,
                host,
                device,
                resources: resources.into_iter().map(|(t, n)| (t.to_string(), n)).collect(),
                unit,
                source_type_name,
                interval,
                origin,
                points,
            },
        )
}

fn v2_protobuf(series: &[GenSeries]) -> Vec<u8> {
    MetricPayload {
        series: series
            .iter()
            .map(|s| {
                let mut resources: Vec<PbResource> = s
                    .resources
                    .iter()
                    .map(|(t, n)| PbResource { r#type: t.clone(), name: n.clone() })
                    .collect();
                if let Some(host) = &s.host {
                    resources.push(PbResource { r#type: "host".into(), name: host.clone() });
                }
                if let Some(device) = &s.device {
                    resources
                        .insert(0, PbResource { r#type: "device".into(), name: device.clone() });
                }
                let (p, c, sv) = s.origin;
                MetricSeries {
                    resources,
                    metric: s.metric.clone(),
                    tags: s.tags.clone(),
                    points: s
                        .points
                        .iter()
                        .map(|&(timestamp, value)| MetricPoint { value, timestamp })
                        .collect(),
                    r#type: s.type_code,
                    unit: s.unit.clone(),
                    source_type_name: s.source_type_name.clone(),
                    interval: s.interval,
                    metadata: (p + c + sv > 0).then_some(Metadata {
                        origin: Some(Origin {
                            origin_product: p,
                            origin_category: c,
                            origin_service: sv,
                        }),
                    }),
                }
            })
            .collect(),
    }
    .encode_to_vec()
}

fn v2_json(series: &[GenSeries]) -> Vec<u8> {
    let items: Vec<JsonValue> = series
        .iter()
        .map(|s| {
            let mut resources: Vec<JsonValue> =
                s.resources.iter().map(|(t, n)| json!({"type": t, "name": n})).collect();
            if let Some(host) = &s.host {
                resources.push(json!({"type": "host", "name": host}));
            }
            if let Some(device) = &s.device {
                resources.push(json!({"type": "device", "name": device}));
            }
            let (p, _, sv) = s.origin;
            json!({
                "metric": s.metric,
                "type": s.type_code,
                "tags": s.tags,
                "resources": resources,
                "unit": s.unit,
                "source_type_name": s.source_type_name,
                "interval": s.interval,
                "metadata": {"origin": {"product": p, "service": sv, "metric_type": s.origin.1}},
                "points": s.points.iter().map(|(t, v)| json!({"timestamp": t, "value": v}))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    json_bytes(json!({ "series": items }))
}

fn v1(series: &[GenSeries]) -> Vec<u8> {
    let items: Vec<JsonValue> = series
        .iter()
        .map(|s| {
            let type_name = ["", "count", "rate", "gauge"][s.type_code as usize];
            let mut item = json!({
                "metric": s.metric,
                "points": s.points.iter().map(|(t, v)| json!([t, v])).collect::<Vec<_>>(),
                "tags": s.tags,
                "host": s.host.clone().unwrap_or_default(),
                "type": type_name,
                "interval": s.interval,
                "source_type_name": s.source_type_name,
                "unit": s.unit,
            });
            if let Some(device) = &s.device {
                item["device"] = json!(device);
            }
            item
        })
        .collect();
    json_bytes(json!({ "series": items }))
}

fn distribution_points(series: &[GenSeries]) -> Vec<u8> {
    let items: Vec<JsonValue> = series
        .iter()
        .map(|s| {
            // Each generated point becomes a distribution point of 1-3 values derived from it.
            let points: Vec<JsonValue> = s
                .points
                .iter()
                .map(|&(t, v)| {
                    let values = &[v, v / 2.0, -v][..1 + (t as usize % 3)];
                    json!([t, values])
                })
                .collect();
            json!({"metric": s.metric, "points": points, "host": s.host, "tags": s.tags,
                   "type": "distribution"})
        })
        .collect();
    json_bytes(json!({ "series": items }))
}

/// `(ts, [(k, n)], min, max, sum)`; `cnt` is the sum of `n`.
type GenDog = (i64, Vec<(i32, u32)>, f64, f64, f64);

#[derive(Clone, Debug)]
struct GenSketch {
    metric: String,
    host: String,
    tags: Vec<String>,
    origin: (u32, u32, u32),
    dogs: Vec<GenDog>,
}

fn key() -> impl Strategy<Value = i32> {
    prop_oneof![
        6 => -60i32..60,
        1 => 32000i32..=32767,
        1 => -32767i32..=-32000,
    ]
}

fn gen_sketch() -> impl Strategy<Value = GenSketch> {
    let dog = (
        timestamp(),
        prop::collection::vec((key(), prop_oneof![1u32..100, 60000u32..200000, Just(0u32)]), 0..10),
        value(),
        value(),
        value(),
    );
    (
        "[a-z][a-z0-9._]{0,12}",
        "[a-z0-9-]{0,8}",
        prop::collection::vec(tag(), 0..6),
        (0u32..3, 0u32..3, 0u32..3),
        prop::collection::vec(dog, 1..4),
    )
        .prop_map(|(metric, host, tags, origin, dogs)| GenSketch {
            metric,
            host,
            tags,
            origin,
            dogs,
        })
}

fn sketches(sketches: &[GenSketch]) -> Vec<u8> {
    SketchPayload {
        sketches: sketches
            .iter()
            .map(|s| {
                let (p, c, sv) = s.origin;
                Sketch {
                    metric: s.metric.clone(),
                    host: s.host.clone(),
                    distributions: Vec::new(),
                    tags: s.tags.clone(),
                    dogsketches: s
                        .dogs
                        .iter()
                        .map(|(ts, bins, min, max, sum)| {
                            let mut bins = bins.clone();
                            bins.sort_by_key(|b| b.0);
                            let cnt = bins.iter().map(|b| i64::from(b.1)).sum();
                            Dogsketch {
                                ts: *ts,
                                cnt,
                                min: *min,
                                max: *max,
                                avg: 0.0,
                                sum: *sum,
                                k: bins.iter().map(|b| b.0).collect(),
                                n: bins.iter().map(|b| b.1).collect(),
                            }
                        })
                        .collect(),
                    metadata: (p + c + sv > 0).then_some(Metadata {
                        origin: Some(Origin {
                            origin_product: p,
                            origin_category: c,
                            origin_service: sv,
                        }),
                    }),
                }
            })
            .collect(),
        metadata: None,
    }
    .encode_to_vec()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn generated_series_are_a_fixed_point_on_every_route(
        series in prop::collection::vec(gen_series(), 1..5)
    ) {
        assert_fixed_point(Route::V2Protobuf, &v2_protobuf(&series));
        assert_fixed_point(Route::V2Json, &v2_json(&series));
        assert_fixed_point(Route::V1, &v1(&series));
        assert_fixed_point(Route::DistributionPoints, &distribution_points(&series));
    }

    #[test]
    fn generated_sketches_are_a_fixed_point(
        payload in prop::collection::vec(gen_sketch(), 1..4)
    ) {
        assert_fixed_point(Route::Sketches, &sketches(&payload));
    }
}
