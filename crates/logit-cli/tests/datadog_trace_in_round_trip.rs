//! `datadog_trace_in` over a real socket: every trace form a dd-trace tracer sends (v0.4, v0.5,
//! v0.7) and its client stats (`/v0.6/stats`) deliver exactly the batch the body encodes, with the
//! tracer's request headers on the batch resource, over TCP and over the Unix socket.
//!
//! Each body is built by `logit_proto::datadog::DatadogEncoder` from a batch that is itself one
//! decode of an encoder's output, so the batch is on the codec's fixed point
//! (`crates/logit-proto/tests/datadog_traces_fixed_point.rs`,
//! `datadog_stats_fixed_point.rs`): decoding the body gives the batch back, whole-`EventBatch`
//! equal. What this file adds is the HTTP hop: routing, the headers, the rate reply, and delivery
//! to the `Fanout` (`crates/logit-inputs/src/datadog_trace.rs`'s module doc).

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Value};
use logit_inputs::datadog_trace::DatadogTraceInput;
use logit_pipeline::{Fanout, Input};
use logit_proto::datadog::generated::trace::{AgentPayload, Span, TraceChunk, TracerPayload};
use logit_proto::datadog::stats::{
    ATTR_BUCKET_DURATION, ATTR_STATS_NAME, METRIC_DURATION, METRIC_ERRORS, METRIC_HITS,
    METRIC_TOP_LEVEL_HITS,
};
use logit_proto::datadog::traces::RESOURCE_ATTR_TRACER_LANGUAGE_VERSION;
use logit_proto::datadog::{
    DatadogDecoder, DatadogEncoder, ATTR_SERVICE_NAME, RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS,
    RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL, RESOURCE_ATTR_TRACER_CONTAINER_ID,
    RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS, RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES,
    RESOURCE_ATTR_TRACER_ENTITY_ID, RESOURCE_ATTR_TRACER_HOSTNAME,
    RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER, RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR,
    RESOURCE_ATTR_TRACER_LANGUAGE_NAME, RESOURCE_ATTR_TRACER_VERSION,
};
use prost::Message;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

const RATE_REPLY: &str = r#"{"rate_by_service":{"service:,env:":1.0}}"#;

// -------------------------------------------------------------------------------------------------
// Listener and clients
// -------------------------------------------------------------------------------------------------

async fn start(
    input: DatadogTraceInput,
) -> (Option<SocketAddr>, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = input;
    input.bind().await.expect("binding datadog_trace_in");
    let addr = input.local_addr();
    let (tx, rx) = mpsc::channel(16);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });
    (addr, rx)
}

/// What a dd-trace tracer sends alongside every trace payload.
const TRACER_HEADERS: [(&str, &str); 11] = [
    ("Datadog-Meta-Lang", "python"),
    ("Datadog-Meta-Lang-Version", "3.12.1"),
    ("Datadog-Meta-Lang-Interpreter", "CPython"),
    ("Datadog-Meta-Lang-Interpreter-Vendor", "python.org"),
    ("Datadog-Meta-Tracer-Version", "2.14.0"),
    ("Datadog-Container-ID", "abc123"),
    ("Datadog-Entity-ID", "ci-abc123"),
    ("Datadog-Client-Computed-Top-Level", "yes"),
    ("Datadog-Client-Computed-Stats", "true"),
    ("Datadog-Client-Dropped-P0-Traces", "7"),
    ("Datadog-Client-Dropped-P0-Spans", "21"),
];

/// Posts (or, with `put`, puts) `body` as msgpack with [`TRACER_HEADERS`]. Returns the status,
/// the `Datadog-Rates-Payload-Version` header, and the response body.
async fn send(
    addr: SocketAddr,
    path: &str,
    put: bool,
    body: &[u8],
) -> (reqwest::StatusCode, Option<String>, String) {
    let client = reqwest::Client::new();
    let url = format!("http://{addr}{path}");
    let mut request = if put { client.put(url) } else { client.post(url) };
    request = request.header("Content-Type", "application/msgpack").body(body.to_vec());
    for (name, value) in TRACER_HEADERS {
        request = request.header(name, value);
    }
    let response = request.send().await.expect("the request reaches datadog_trace_in");
    let status = response.status();
    let version = response
        .headers()
        .get("datadog-rates-payload-version")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    (status, version, response.text().await.unwrap_or_default())
}

async fn recv(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
    let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("datadog_trace_in delivers a batch within 5s")
        .expect("the Fanout channel is open");
    logit_pipeline::unwrap_batch(delivered)
}

// -------------------------------------------------------------------------------------------------
// Payloads: one decode of an encoder's output, so each batch is on the codec's fixed point
// -------------------------------------------------------------------------------------------------

/// Two traces of spans with the fields every form carries, the second with a 128-bit id.
fn seed() -> EventBatch {
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
        metrics: HashMap::from([("_sampling_priority_v1".into(), 1.0)]),
        r#type: "web".into(),
        ..Default::default()
    };
    let mut wide = span(9, 3, 0, "work");
    wide.meta.insert("_dd.p.tid".into(), "00000000000000ab".into());
    let payload = AgentPayload {
        tracer_payloads: vec![TracerPayload {
            language_name: "go".into(),
            hostname: "web-1".into(),
            chunks: vec![
                TraceChunk {
                    priority: 1,
                    spans: vec![span(7, 1, 0, "http.request"), span(7, 2, 1, "db.query")],
                    ..Default::default()
                },
                TraceChunk { priority: 2, spans: vec![wide], ..Default::default() },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut batches =
        DatadogDecoder::new().decode_agent_payload(&payload.encode_to_vec(), 0).unwrap();
    assert_eq!(batches.len(), 1);
    batches.remove(0)
}

/// `encode` once to reach the form's fixed point, then again for the body; asserts that decoding
/// the body gives the fixed-point batch back.
fn canonical(
    encode: impl Fn(&EventBatch) -> Bytes,
    decode: impl Fn(&[u8]) -> EventBatch,
    seed: &EventBatch,
) -> (EventBatch, Bytes) {
    let batch = decode(&encode(seed));
    let body = encode(&batch);
    assert_eq!(decode(&body), batch, "the batch is on the codec's fixed point");
    (batch, body)
}

fn v04() -> (EventBatch, Bytes) {
    canonical(
        |b| DatadogEncoder::new().encode_traces_v04(b).unwrap(),
        |body| DatadogDecoder::new().decode_traces_v04(body, 0).unwrap(),
        &seed(),
    )
}

fn v05() -> (EventBatch, Bytes) {
    canonical(
        |b| DatadogEncoder::new().encode_traces_v05(b).unwrap(),
        |body| DatadogDecoder::new().decode_traces_v05(body, 0).unwrap(),
        &seed(),
    )
}

fn v07() -> (EventBatch, Bytes) {
    canonical(
        |b| DatadogEncoder::new().encode_tracer_payload_v07(b).unwrap(),
        |body| DatadogDecoder::new().decode_tracer_payload_v07(body, 0).unwrap(),
        &seed(),
    )
}

fn stats() -> (EventBatch, Bytes) {
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
    canonical(
        |b| DatadogEncoder::new().encode_client_stats_v06(b).unwrap(),
        |body| DatadogDecoder::new().decode_client_stats_v06(body, 0).unwrap(),
        &seed,
    )
}

/// `batch` with every [`TRACER_HEADERS`] carrier its resource doesn't already hold: what the
/// listener delivers.
fn with_headers(mut batch: EventBatch) -> EventBatch {
    let fills = [
        (RESOURCE_ATTR_TRACER_LANGUAGE_NAME, Value::str("python")),
        (RESOURCE_ATTR_TRACER_LANGUAGE_VERSION, Value::str("3.12.1")),
        (RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER, Value::str("CPython")),
        (RESOURCE_ATTR_TRACER_LANGUAGE_INTERPRETER_VENDOR, Value::str("python.org")),
        (RESOURCE_ATTR_TRACER_VERSION, Value::str("2.14.0")),
        (RESOURCE_ATTR_TRACER_CONTAINER_ID, Value::str("abc123")),
        (RESOURCE_ATTR_TRACER_ENTITY_ID, Value::str("ci-abc123")),
        (RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_TOP_LEVEL, Value::Bool(true)),
        (RESOURCE_ATTR_TRACER_CLIENT_COMPUTED_STATS, Value::Bool(true)),
        (RESOURCE_ATTR_TRACER_DROPPED_P0_TRACES, Value::U64(7)),
        (RESOURCE_ATTR_TRACER_DROPPED_P0_SPANS, Value::U64(21)),
    ];
    let resource = Arc::make_mut(&mut batch.resource);
    for (attr, value) in fills {
        if resource.attributes.get(attr).is_none() {
            resource.attributes.insert(attr, value);
        }
    }
    batch
}

// -------------------------------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------------------------------

/// v0.4 and v0.5 carry no tracer payload: the headers fill the whole resource. Both methods a
/// tracer uses are exercised.
#[tokio::test]
async fn v04_and_v05_deliver_the_encoded_batch_with_the_headers_as_its_resource() {
    let (addr, mut rx) = start(DatadogTraceInput::new().with_bind("127.0.0.1:0")).await;
    let addr = addr.unwrap();
    for (path, (batch, body)) in [("/v0.4/traces", v04()), ("/v0.5/traces", v05())] {
        assert!(batch.resource.attributes.is_empty(), "{path}: no payload, no resource");
        for put in [false, true] {
            let (status, version, text) = send(addr, path, put, &body).await;
            assert_eq!(status, reqwest::StatusCode::OK, "{path}");
            assert_eq!(version.as_deref(), Some("logit-1"), "{path}");
            assert_eq!(text, RATE_REPLY, "{path}");
            assert_eq!(recv(&mut rx).await, with_headers(batch.clone()), "{path} put={put}");
        }
    }
}

/// v0.7's `TracerPayload` fields win: `language_name` stays `go` although the header says
/// `python`, and the headers fill only what the payload left empty.
#[tokio::test]
async fn v07_delivers_the_encoded_batch_with_its_own_fields_winning_over_the_headers() {
    let (addr, mut rx) = start(DatadogTraceInput::new().with_bind("127.0.0.1:0")).await;
    let (batch, body) = v07();
    assert_eq!(
        batch.resource.attributes.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
        Some(&Value::str("go"))
    );
    let (status, _, text) = send(addr.unwrap(), "/v0.7/traces", false, &body).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(text, RATE_REPLY);
    let delivered = recv(&mut rx).await;
    assert_eq!(delivered, with_headers(batch));
    assert_eq!(
        delivered.resource.attributes.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
        Some(&Value::str("go"))
    );
}

/// Client stats relay as the stats codec decodes them, and take none of the tracer headers.
#[tokio::test]
async fn v06_stats_deliver_the_encoded_batch() {
    let (addr, mut rx) = start(DatadogTraceInput::new().with_bind("127.0.0.1:0")).await;
    let (batch, body) = stats();
    let (status, version, text) = send(addr.unwrap(), "/v0.6/stats", false, &body).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(version, None, "the rates header is the trace reply's alone");
    assert_eq!(text, "{}");
    assert_eq!(recv(&mut rx).await, batch);
}

/// A fresh directory under the system temp dir, removed on drop. Short, because a Unix socket
/// path is limited to about 100 bytes.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ldtr-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One raw HTTP/1.1 request over the Unix socket at `path`, as a tracer configured with
/// `DD_TRACE_AGENT_URL=unix://...` sends it. Returns the raw response.
async fn unix_request(path: &Path, method: &str, uri: &str, body: &[u8]) -> String {
    let mut stream = tokio::net::UnixStream::connect(path).await.expect("connecting to the socket");
    let mut head = format!(
        "{method} {uri} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/msgpack\r\n\
         Content-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in TRACER_HEADERS {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .expect("a reply within 5s")
        .unwrap();
    String::from_utf8_lossy(&response).into_owned()
}

/// The Unix socket, on its own (no `bind`), end to end: a raw HTTP/1.1 request in, the rate reply
/// out, and the batch delivered with the headers as its resource.
#[tokio::test]
async fn the_unix_socket_delivers_end_to_end() {
    let dir = TempDir::new("e2e");
    let path = dir.0.join("apm.socket");
    let (addr, mut rx) = start(DatadogTraceInput::new().with_socket(&path)).await;
    assert_eq!(addr, None, "no TCP listener without bind");

    let (batch, body) = v04();
    let response = unix_request(&path, "PUT", "/v0.4/traces", &body).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with(RATE_REPLY), "{response}");
    assert_eq!(recv(&mut rx).await, with_headers(batch));

    let (batch, body) = stats();
    let response = unix_request(&path, "POST", "/v0.6/stats", &body).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert_eq!(recv(&mut rx).await, batch);
}
