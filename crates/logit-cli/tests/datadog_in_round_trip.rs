//! `datadog_in` over a real socket: every intake route, posted the way a Datadog Agent posts it
//! (identity, gzip, deflate, and zstd), delivers exactly the batch its body encodes.
//!
//! Each body is built by `logit_proto::datadog::DatadogEncoder` from a batch that is itself one
//! decode of a hand-written wire payload, so the batch is on the codec's fixed point
//! (`crates/logit-proto/tests/datadog_*_fixed_point.rs`): decoding the encoder's output gives the
//! encoder's input back, whole-`EventBatch` equal. What this file adds is the HTTP hop: routing,
//! `Content-Type`, every `Content-Encoding`, and delivery to the `Fanout`. It also covers the
//! `/intake/` host-metadata acknowledgement and the busy contract
//! (`crates/logit-inputs/src/datadog.rs`'s "Backpressure" section).

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Value};
use logit_inputs::datadog::DatadogInput;
use logit_pipeline::{Fanout, Input};
use logit_proto::datadog::events::EventFormat;
use logit_proto::datadog::generated::agentpayload::{
    metric_payload::{MetricPoint, MetricSeries, Resource as PbResource},
    sketch_payload::{sketch::Dogsketch, Sketch},
    MetricPayload, SketchPayload,
};
use logit_proto::datadog::generated::trace::{AgentPayload, Span, TraceChunk, TracerPayload};
use logit_proto::datadog::stats::{
    ATTR_BUCKET_DURATION, ATTR_STATS_NAME, METRIC_DURATION, METRIC_ERRORS, METRIC_HITS,
    METRIC_TOP_LEVEL_HITS,
};
use logit_proto::datadog::{
    DatadogDecoder, DatadogEncoder, ATTR_SERVICE_NAME, RESOURCE_ATTR_TRACER_HOSTNAME,
};
use prost::Message;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Timestamps in every hand-written payload are explicit, so nothing decodes to this.
const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;

/// The busy bound the busy test builds its listener with, via
/// [`DatadogInput::with_busy_after`]'s test/tuning hook, so it doesn't wait out the real 5s
/// default.
const TEST_BUSY_AFTER: Duration = Duration::from_millis(200);

const KEY: &str = "0123456789abcdef0123456789abcdef";

// -------------------------------------------------------------------------------------------------
// Listener and client
// -------------------------------------------------------------------------------------------------

async fn start(capacity: usize) -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    start_with_busy_after(capacity, Duration::from_secs(5)).await
}

async fn start_with_busy_after(
    capacity: usize,
    busy_after: Duration,
) -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = DatadogInput::new("127.0.0.1:0")
        .with_api_keys(vec![KEY.to_string()])
        .with_busy_after(busy_after);
    input.bind().await.expect("binding datadog_in");
    let addr = input.local_addr().expect("bind() leaves an address");
    let (tx, rx) = mpsc::channel(capacity);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });
    (addr, rx)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_gzip().build().expect("a default client always builds")
}

#[derive(Clone, Copy, Debug)]
enum Encoding {
    Identity,
    Gzip,
    Deflate,
    Zstd,
}

const ENCODINGS: [Encoding; 4] =
    [Encoding::Identity, Encoding::Gzip, Encoding::Deflate, Encoding::Zstd];

impl Encoding {
    fn header(self) -> Option<&'static str> {
        match self {
            Self::Identity => None,
            Self::Gzip => Some("gzip"),
            Self::Deflate => Some("deflate"),
            Self::Zstd => Some("zstd"),
        }
    }

    fn compress(self, body: &[u8]) -> Vec<u8> {
        use std::io::Write;
        match self {
            Self::Identity => body.to_vec(),
            Self::Gzip => {
                let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                e.write_all(body).unwrap();
                e.finish().unwrap()
            }
            Self::Deflate => {
                let mut e =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
                e.write_all(body).unwrap();
                e.finish().unwrap()
            }
            Self::Zstd => {
                ruzstd::encoding::compress_to_vec(body, ruzstd::encoding::CompressionLevel::Fastest)
            }
        }
    }
}

/// Posts `body` to `path`, compressed under `encoding`, with the Agent's headers. Returns the
/// status and the response body.
async fn post(
    addr: SocketAddr,
    path: &str,
    content_type: &str,
    encoding: Encoding,
    body: &[u8],
) -> (reqwest::StatusCode, String) {
    let mut request = client()
        .post(format!("http://{addr}{path}"))
        .header("DD-Api-Key", KEY)
        .header("Content-Type", content_type)
        .body(encoding.compress(body));
    if let Some(value) = encoding.header() {
        request = request.header("Content-Encoding", value);
    }
    let response = request.send().await.expect("the request reaches datadog_in");
    let status = response.status();
    (status, response.text().await.unwrap_or_default())
}

async fn recv(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
    let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("datadog_in delivers a batch within 5s")
        .expect("the Fanout channel is open");
    logit_pipeline::unwrap_batch(delivered)
}

// -------------------------------------------------------------------------------------------------
// Routes: one decode of a hand-written payload, re-encoded by `DatadogEncoder`
// -------------------------------------------------------------------------------------------------

/// A route under test: where to post, the `Content-Type`, the batch the encoder is given, and
/// the body it encodes that batch to.
struct Case {
    path: &'static str,
    content_type: &'static str,
    expected: EventBatch,
    body: Bytes,
}

fn decoder() -> DatadogDecoder {
    DatadogDecoder::new()
}

fn series_v2_protobuf() -> Case {
    let seed = MetricPayload {
        series: vec![
            MetricSeries {
                resources: vec![PbResource { r#type: "host".into(), name: "web-1".into() }],
                metric: "requests".into(),
                tags: vec!["env:prod".into(), "urgent".into()],
                points: vec![
                    MetricPoint { value: 3.0, timestamp: 1_700_000_000 },
                    MetricPoint { value: 5.0, timestamp: 1_700_000_010 },
                ],
                r#type: 1,
                unit: "request".into(),
                interval: 10,
                ..Default::default()
            },
            MetricSeries {
                metric: "load".into(),
                points: vec![MetricPoint { value: 0.75, timestamp: 1_700_000_000 }],
                r#type: 3,
                ..Default::default()
            },
        ],
    }
    .encode_to_vec();
    let expected = decoder().decode_series_v2_protobuf(&seed, RECEIVED_AT).unwrap();
    let body = DatadogEncoder::new().encode_series_v2_protobuf(&expected).unwrap();
    Case { path: "/api/v2/series", content_type: "application/x-protobuf", expected, body }
}

fn series_v2_json() -> Case {
    let seed = br#"{"series":[{"metric":"queue.depth","type":3,"points":[{"timestamp":1700000000,"value":12.5}],"resources":[{"type":"host","name":"db-1"}],"tags":["team:data"]},{"metric":"rate.x","type":2,"interval":10,"points":[{"timestamp":1700000000,"value":0.1}]}]}"#;
    let expected = decoder().decode_series_v2_json(seed, RECEIVED_AT).unwrap();
    let body = DatadogEncoder::new().encode_series_v2_json(&expected).unwrap();
    Case { path: "/api/v2/series", content_type: "application/json", expected, body }
}

fn series_v1() -> Case {
    let seed = br#"{"series":[{"metric":"cpu.user","points":[[1700000000,12.5],[1700000010,13]],"tags":["env:prod"],"host":"h1","type":"gauge","interval":10,"device":"sda"},{"metric":"hits","points":[[1700000000,4]],"type":"count","host":"h1","interval":10}]}"#;
    let expected = decoder().decode_series_v1(seed, RECEIVED_AT).unwrap();
    let body = DatadogEncoder::new().encode_series_v1(&expected).unwrap();
    Case { path: "/api/v1/series", content_type: "application/json", expected, body }
}

fn distribution_points() -> Case {
    let seed = br#"{"series":[{"metric":"latency","points":[[1700000000,[1.0,2.5,4.0]]],"host":"h1","tags":["env:prod"]}]}"#;
    let expected = decoder().decode_distribution_points(seed, RECEIVED_AT).unwrap();
    let body = DatadogEncoder::new().encode_distribution_points(&expected).unwrap();
    Case { path: "/api/v1/distribution_points", content_type: "application/json", expected, body }
}

fn sketches() -> Case {
    let seed = SketchPayload {
        sketches: vec![Sketch {
            metric: "request.latency".into(),
            host: "web-1".into(),
            tags: vec!["env:prod".into()],
            dogsketches: vec![Dogsketch {
                ts: 1_700_000_000,
                cnt: 16,
                min: -3.0,
                max: 250.0,
                avg: 0.0,
                sum: 900.0,
                k: vec![-80, 0, 1200, 1300],
                n: vec![2, 3, 10, 1],
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    let expected = decoder().decode_sketches(&seed, RECEIVED_AT).unwrap();
    let body = DatadogEncoder::new().encode_sketches(&expected).unwrap();
    Case { path: "/api/beta/sketches", content_type: "application/x-protobuf", expected, body }
}

fn service_checks() -> Case {
    let seed = br#"[{"check":"app.can_connect","host_name":"h1","status":2,"message":"refused","timestamp":1700000000,"tags":["db:main"]}]"#;
    let expected = decoder().decode_service_checks(seed, RECEIVED_AT).unwrap();
    let body = DatadogEncoder::new().encode_service_checks(&expected).unwrap();
    Case { path: "/api/v1/check_run", content_type: "application/json", expected, body }
}

fn events(path: &'static str) -> Case {
    let seed = br#"{"apiKey":"not-stored","internalHostname":"agent-1","events":{"api":[{"msg_title":"deploy","msg_text":"v2 is out","timestamp":1700000000,"priority":"normal","host":"web-1","alert_type":"info","tags":["env:prod"]}]}}"#;
    let expected = decoder().decode_events(seed, RECEIVED_AT).unwrap();
    let mut bodies = DatadogEncoder::new().encode_events(&expected, EventFormat::AgentEnvelope);
    assert_eq!(bodies.len(), 1, "the envelope carries the whole batch");
    Case { path, content_type: "application/json", expected, body: bodies.remove(0) }
}

fn logs(path: &'static str) -> Case {
    let seed = br#"[{"message":"GET / 200","status":"info","timestamp":1700000000123,"hostname":"web-1","service":"nginx","ddsource":"nginx","ddtags":"env:prod"},{"message":"boom","status":"error","timestamp":1700000001000,"service":"api","http":{"status_code":500}}]"#;
    let expected = decoder().decode_logs(seed, RECEIVED_AT).unwrap();
    let body = DatadogEncoder::new().encode_logs(&expected).unwrap();
    Case { path, content_type: "application/json", expected, body }
}

fn agent_payload() -> AgentPayload {
    let span = |trace_id: u64, span_id: u64, parent_id: u64, name: &str| Span {
        service: "checkout".into(),
        name: name.into(),
        resource: "POST /cart".into(),
        trace_id,
        span_id,
        parent_id,
        start: 1_700_000_000_000_000_000 + span_id as i64,
        duration: 1_500_000,
        meta: HashMap::from([("env".into(), "prod".into())]),
        metrics: HashMap::from([("_top_level".into(), 1.0)]),
        r#type: "web".into(),
        ..Default::default()
    };
    AgentPayload {
        host_name: "agent-host".into(),
        env: "prod".into(),
        agent_version: "7.60.0".into(),
        tracer_payloads: vec![
            TracerPayload {
                language_name: "python".into(),
                hostname: "web-1".into(),
                chunks: vec![TraceChunk {
                    priority: 1,
                    spans: vec![span(7, 1, 0, "http.request"), span(7, 2, 1, "db.query")],
                    ..Default::default()
                }],
                ..Default::default()
            },
            TracerPayload {
                language_name: "go".into(),
                chunks: vec![TraceChunk {
                    priority: 2,
                    spans: vec![span(9, 3, 0, "work")],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

/// One case per `TracerPayload`: the encoder writes one batch as one `AgentPayload`.
fn traces() -> Vec<Case> {
    let batches = decoder().decode_agent_payload(&agent_payload().encode_to_vec(), 0).unwrap();
    assert_eq!(batches.len(), 2);
    batches
        .into_iter()
        .map(|expected| {
            let body = DatadogEncoder::new().encode_agent_payload(&expected).unwrap();
            Case {
                path: "/api/v0.2/traces",
                content_type: "application/x-protobuf",
                expected,
                body,
            }
        })
        .collect()
}

/// A stats batch built on the model, encoded, and decoded once, so it is on the stats codec's
/// fixed point.
fn stats() -> Case {
    let mut resource = Resource::default();
    resource.attributes.insert(RESOURCE_ATTR_TRACER_HOSTNAME, Value::str("web-1"));
    let mut attributes = AttrMap::new();
    attributes.insert(ATTR_STATS_NAME, Value::str("http.request"));
    attributes.insert(ATTR_SERVICE_NAME, Value::str("checkout"));
    attributes.insert(ATTR_BUCKET_DURATION, Value::U64(10_000_000_000));
    let mut event = Event::metric(
        1_700_000_000_000_000_000,
        attributes,
        MetricRecord::new(intern(METRIC_HITS), MetricKind::counter(10.0)),
    );
    for (name, value) in
        [(METRIC_ERRORS, 1.0), (METRIC_TOP_LEVEL_HITS, 10.0), (METRIC_DURATION, 5e7)]
    {
        event.metrics.push(MetricRecord::new(intern(name), MetricKind::counter(value)));
    }
    let seed = EventBatch { resource: Arc::new(resource), scope: None, events: vec![event] };
    let seed_body = DatadogEncoder::new().encode_stats_payload(&seed).unwrap();
    let mut decoded = decoder().decode_stats_payload(&seed_body, 0).unwrap();
    assert_eq!(decoded.len(), 1);
    let expected = decoded.remove(0);
    let body = DatadogEncoder::new().encode_stats_payload(&expected).unwrap();
    Case { path: "/api/v0.2/stats", content_type: "application/msgpack", expected, body }
}

fn every_case() -> Vec<Case> {
    let mut cases = vec![
        series_v2_protobuf(),
        series_v2_json(),
        series_v1(),
        distribution_points(),
        sketches(),
        service_checks(),
        events("/api/v2/events"),
        events("/intake/"),
        logs("/api/v2/logs"),
        logs("/v1/input"),
        stats(),
    ];
    cases.extend(traces());
    cases
}

// -------------------------------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------------------------------

/// Every route, under every encoding, delivers exactly the batch the encoder was given.
#[tokio::test]
async fn every_route_delivers_the_encoded_batch_under_every_encoding() {
    let (addr, mut rx) = start(16).await;
    for case in every_case() {
        assert!(!case.expected.events.is_empty(), "{}: the case carries events", case.path);
        for encoding in ENCODINGS {
            let (status, _) = post(addr, case.path, case.content_type, encoding, &case.body).await;
            assert!(status.is_success(), "{} {encoding:?}: {status}", case.path);
            let delivered = recv(&mut rx).await;
            assert_eq!(delivered, case.expected, "{} {encoding:?}", case.path);
        }
    }
    assert!(rx.try_recv().is_err(), "one batch per single-payload request, nothing more");
}

/// One `AgentPayload` with two tracer payloads is two batches, delivered in order.
#[tokio::test]
async fn an_agent_payload_with_two_tracer_payloads_delivers_two_batches_in_order() {
    let (addr, mut rx) = start(16).await;
    let body = agent_payload().encode_to_vec();
    let expected = decoder().decode_agent_payload(&body, 0).unwrap();
    let (status, text) =
        post(addr, "/api/v0.2/traces", "application/x-protobuf", Encoding::Zstd, &body).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(text, "{}");
    assert_eq!(recv(&mut rx).await, expected[0]);
    assert_eq!(recv(&mut rx).await, expected[1]);
}

/// Host metadata on `/intake/` is acknowledged with the events route's `202` and delivers
/// nothing.
#[tokio::test]
async fn intake_host_metadata_is_acknowledged_and_delivers_nothing() {
    let (addr, mut rx) = start(16).await;
    let body = br#"{"apiKey":"k","agentVersion":"7.60.0","uuid":"u","internalHostname":"agent-1","os":"linux","meta":{"hostname":"agent-1"},"systemStats":{"cpuCores":8}}"#;
    let (status, text) = post(addr, "/intake/", "application/json", Encoding::Zstd, body).await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED);
    assert_eq!(text, r#"{"status":"ok"}"#);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(),
        "host metadata is never sent"
    );
}

/// The busy contract: a one-slot channel with a parked consumer takes the first request's batch;
/// the second request's send waits out `TEST_BUSY_AFTER` and is answered `503` with
/// `Retry-After`, delivering nothing. Once the consumer drains, a third request succeeds. The
/// listener is built with a short busy bound (`with_busy_after`) so this doesn't wait out the
/// real 5s default.
#[tokio::test]
async fn a_stalled_downstream_is_answered_503_and_the_retry_succeeds() {
    let (addr, mut rx) = start_with_busy_after(1, TEST_BUSY_AFTER).await;
    let case = series_v1();

    let (status, _) = post(addr, case.path, case.content_type, Encoding::Gzip, &case.body).await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "the first batch fills the one slot");

    let started = Instant::now();
    let response = client()
        .post(format!("http://{addr}{}", case.path))
        .header("DD-API-KEY", KEY)
        .header("Content-Type", case.content_type)
        .body(case.body.clone())
        .send()
        .await
        .expect("the request reaches datadog_in");
    let elapsed = started.elapsed();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get("retry-after").and_then(|v| v.to_str().ok()), Some("1"));
    assert!(elapsed >= TEST_BUSY_AFTER, "answered after the bound, not before: {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "answered promptly, not after a real 5s wait: {elapsed:?}"
    );
    assert_eq!(response.text().await.unwrap(), r#"{"status":"error","errors":["busy"]}"#);

    // The consumer drains: the first batch, and nothing from the 503'd request.
    assert_eq!(recv(&mut rx).await, case.expected);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(),
        "a 503'd request delivers nothing"
    );

    let (status, _) = post(addr, case.path, case.content_type, Encoding::Zstd, &case.body).await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "the Agent's retry succeeds");
    assert_eq!(recv(&mut rx).await, case.expected);
}

// -------------------------------------------------------------------------------------------------
// Recorded Agent traffic (testdata/interop/datadog/, `script/record-fixtures datadog-agent`)
// -------------------------------------------------------------------------------------------------

/// Every request a real Agent 7.83 sent its intake, replayed with its recorded method, path,
/// headers, and body: zstd and gzip as the Agent compressed them, the key in `DD-API-KEY` or, on
/// the validate probe, in the query string. Each is answered its route's `2xx`, and the data
/// routes deliver what the Agent sent.
#[tokio::test]
async fn every_recorded_agent_request_is_answered_2xx() {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/datadog");
    let mut input = DatadogInput::new("127.0.0.1:0").with_api_keys(vec!["logit-record".into()]);
    input.bind().await.expect("binding datadog_in");
    let addr = input.local_addr().expect("bind() leaves an address");
    let (tx, mut rx) = mpsc::channel(256);
    tokio::spawn(async move {
        let _ = input.run(Fanout::new(vec![tx])).await;
    });

    let mut stems: Vec<String> = std::fs::read_dir(&dir)
        .expect("the recorded corpus")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("agent-") && n.ends_with(".headers"))
        .map(|n| n.trim_end_matches(".headers").to_string())
        .collect();
    stems.sort();
    assert!(stems.len() >= 15, "the whole Agent capture: {stems:?}");
    for stem in &stems {
        let sidecar = std::fs::read_to_string(dir.join(format!("{stem}.headers"))).unwrap();
        let body = std::fs::read(dir.join(format!("{stem}.bin"))).unwrap();
        let mut fields = sidecar.lines().filter_map(|l| l.split_once(": "));
        let (_, method) = fields.next().expect("method first");
        let (_, path) = fields.next().expect("path second");
        let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        let mut request = client().request(method, format!("http://{addr}{path}"));
        for (name, value) in fields {
            // reqwest writes its own framing and host.
            if !matches!(name, "host" | "content-length") {
                request = request.header(name, value);
            }
        }
        let response = request.body(body).send().await.expect("the request reaches datadog_in");
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        assert!(status.is_success(), "{stem} ({path}): {status} {text}");
    }

    let mut names = std::collections::BTreeSet::new();
    while let Ok(Some(delivered)) =
        tokio::time::timeout(Duration::from_millis(300), rx.recv()).await
    {
        for event in logit_pipeline::unwrap_batch(delivered).events {
            if let Some(metric) = event.metrics.first() {
                names.insert(logit_core::interner::resolve(metric.name).to_string());
            }
        }
    }
    for name in ["record.requests.count", "record.response.size", "record.can_connect"] {
        assert!(names.contains(name), "{name} was delivered: {names:?}");
    }
}
