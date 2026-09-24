//! `datadog_trace_in -> datadog_trace_out -> datadog_trace_in` over real sockets, TCP and Unix: a
//! tracer's request relays through the pair and arrives as the batch the first hop delivered.
//!
//! The first hop is a tracer's request, built as `datadog_trace_in_round_trip.rs` builds it: a body
//! on the codec's fixed point plus a dd-trace tracer's headers. The listener delivers batch `B`;
//! `datadog_trace_out` sends `B` back to a listener, which must deliver `B` again. What this file
//! adds over the codec's fixed-point tests is the header restoration (`datadog.tracer.*` carriers
//! out as `Datadog-Meta-*` and friends, and in again), the routes, and the Unix-socket client.

use bytes::Bytes;
use logit_core::interner::{intern, resolve};
use logit_core::{AttrMap, Event, EventBatch, MetricKind, MetricRecord, Registry, Resource, Value};
use logit_inputs::datadog_trace::DatadogTraceInput;
use logit_outputs::datadog_trace::{DatadogTraceOutput, TracerApiForm};
use logit_pipeline::{Fanout, Input, Output};
use logit_proto::datadog::generated::trace::{AgentPayload, Span, TraceChunk, TracerPayload};
use logit_proto::datadog::stats::{
    ATTR_BUCKET_DURATION, ATTR_STATS_NAME, METRIC_DURATION, METRIC_ERRORS, METRIC_HITS,
    METRIC_TOP_LEVEL_HITS,
};
use logit_proto::datadog::traces::ATTR_CHUNK_PRIORITY;
use logit_proto::datadog::{
    DatadogDecoder, DatadogEncoder, ATTR_SERVICE_NAME, RESOURCE_ATTR_TRACER_HOSTNAME,
    RESOURCE_ATTR_TRACER_LANGUAGE_NAME,
};
use prost::Message;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

// -------------------------------------------------------------------------------------------------
// Listeners, the tracer, and the sink
// -------------------------------------------------------------------------------------------------

async fn start(
    input: DatadogTraceInput,
) -> (Option<SocketAddr>, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = input;
    input.bind().await.expect("binding datadog_trace_in");
    let addr = input.local_addr();
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        let _ = input.run(Fanout::new(vec![tx])).await;
    });
    (addr, rx)
}

async fn tcp_listener() -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    let (addr, rx) = start(DatadogTraceInput::new().with_bind("127.0.0.1:0")).await;
    (addr.expect("bind leaves an address"), rx)
}

async fn recv(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
    let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("datadog_trace_in delivers a batch within 5s")
        .expect("the Fanout channel is open");
    logit_pipeline::unwrap_batch(delivered)
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

/// A tracer's request: `body` to `path` with [`TRACER_HEADERS`], answered `200`.
async fn tracer_sends(addr: SocketAddr, path: &str, body: &[u8]) {
    let mut request = reqwest::Client::new()
        .put(format!("http://{addr}{path}"))
        .header("Content-Type", "application/msgpack")
        .body(body.to_vec());
    for (name, value) in TRACER_HEADERS {
        request = request.header(name, value);
    }
    let status = request.send().await.expect("the request reaches datadog_trace_in").status();
    assert_eq!(status, reqwest::StatusCode::OK, "{path}");
}

fn metered(out: DatadogTraceOutput) -> (Arc<Registry>, DatadogTraceOutput) {
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "datadog_trace_out", "sink");
    (registry, out.with_telemetry(telemetry))
}

/// A counter's total across every drained point tagged `reason`.
fn counted(points: &[Event], metric: &str, reason: &str) -> f64 {
    points
        .iter()
        .filter(|e| e.attributes.get("reason").and_then(Value::as_str) == Some(reason))
        .flat_map(|e| e.metrics.iter())
        .filter(|m| resolve(m.name) == metric)
        .map(|m| match &m.kind {
            MetricKind::Sum(s) => s.value,
            _ => 0.0,
        })
        .sum()
}

// -------------------------------------------------------------------------------------------------
// Payloads: one decode of an encoder's output, so each body is on the codec's fixed point
// -------------------------------------------------------------------------------------------------

/// Two traces with the fields every form carries, the second with a 128-bit id, in a
/// `TracerPayload` with a language and a hostname.
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
    batches.remove(0)
}

fn canonical(
    encode: impl Fn(&EventBatch) -> Bytes,
    decode: impl Fn(&[u8]) -> EventBatch,
    seed: &EventBatch,
) -> Bytes {
    encode(&decode(&encode(seed)))
}

fn v04_body() -> Bytes {
    canonical(
        |b| DatadogEncoder::new().encode_traces_v04(b).unwrap(),
        |body| DatadogDecoder::new().decode_traces_v04(body, 0).unwrap(),
        &seed(),
    )
}

fn v07_body() -> Bytes {
    canonical(
        |b| DatadogEncoder::new().encode_tracer_payload_v07(b).unwrap(),
        |body| DatadogDecoder::new().decode_tracer_payload_v07(body, 0).unwrap(),
        &seed(),
    )
}

fn stats_body() -> Bytes {
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

/// The tracer's request through `datadog_trace_in` at `addr`: the batch `B` the pair relays.
async fn first_hop(
    addr: SocketAddr,
    rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
    path: &str,
    body: &[u8],
) -> EventBatch {
    tracer_sends(addr, path, body).await;
    recv(rx).await
}

// -------------------------------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------------------------------

/// A v0.4 request carries its tracer only in headers; through `version: v0.4` they go back out
/// as the same headers, and nothing is lost.
#[tokio::test]
async fn a_v04_origin_batch_relays_equal_through_v04() {
    let (addr, mut rx) = tcp_listener().await;
    let batch = first_hop(addr, &mut rx, "/v0.4/traces", &v04_body()).await;
    assert_eq!(
        batch.resource.attributes.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
        Some(&Value::str("python")),
        "the header is the only place the language arrives"
    );

    let (registry, mut out) = metered(DatadogTraceOutput::http(format!("http://{addr}")));
    out.send(&batch).await.expect("datadog_trace_in accepts");
    assert_eq!(recv(&mut rx).await, batch);
    let points = registry.drain(0);
    assert_eq!(counted(&points, "logit.output.spans.degraded", "no_wire_form"), 0.0);
}

/// A v0.7 request's chunk and tracer payload fields go back in the payload.
#[tokio::test]
async fn a_v07_origin_batch_relays_equal_through_v07() {
    let (addr, mut rx) = tcp_listener().await;
    let batch = first_hop(addr, &mut rx, "/v0.7/traces", &v07_body()).await;
    assert!(batch.events.iter().all(|e| e.attributes.get(ATTR_CHUNK_PRIORITY).is_some()));

    let mut out =
        DatadogTraceOutput::http(format!("http://{addr}")).with_version(TracerApiForm::V07);
    out.send(&batch).await.expect("datadog_trace_in accepts");
    assert_eq!(recv(&mut rx).await, batch);
}

/// Client stats relay as `/v0.6/stats` decodes them.
#[tokio::test]
async fn a_stats_batch_relays_equal() {
    let (addr, mut rx) = tcp_listener().await;
    let batch = first_hop(addr, &mut rx, "/v0.6/stats", &stats_body()).await;
    DatadogTraceOutput::http(format!("http://{addr}")).send(&batch).await.unwrap();
    assert_eq!(recv(&mut rx).await, batch);
}

/// Under `version: v0.4` a v0.7-origin batch loses what v0.4 has no field for, the chunk
/// priorities and the tracer payload's hostname, and says so: one `no_wire_form` per span's
/// chunk carrier and one for the hostname. The language, which a header carries, survives.
#[tokio::test]
async fn a_v07_origin_batch_through_v04_counts_what_it_cannot_carry() {
    let (addr, mut rx) = tcp_listener().await;
    let batch = first_hop(addr, &mut rx, "/v0.7/traces", &v07_body()).await;

    let (registry, mut out) = metered(DatadogTraceOutput::http(format!("http://{addr}")));
    out.send(&batch).await.unwrap();
    let relayed = recv(&mut rx).await;

    let mut expected = batch.clone();
    Arc::make_mut(&mut expected.resource).attributes.remove(RESOURCE_ATTR_TRACER_HOSTNAME);
    for event in &mut expected.events {
        event.attributes.remove(ATTR_CHUNK_PRIORITY);
    }
    assert_eq!(relayed, expected);
    assert_eq!(
        relayed.resource.attributes.get(RESOURCE_ATTR_TRACER_LANGUAGE_NAME),
        Some(&Value::str("go"))
    );
    let points = registry.drain(0);
    let lost = counted(&points, "logit.output.spans.degraded", "no_wire_form");
    assert_eq!(lost, batch.events.len() as f64 + 1.0, "a priority per span, and the hostname");
}

/// A fresh directory under the system temp dir, removed on drop. Short, because a Unix socket
/// path is limited to about 100 bytes.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("ldtp-{tag}-{}", std::process::id()));
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

/// The same relays with `datadog_trace_out` on the listener's Unix socket.
#[tokio::test]
async fn the_pair_relays_equal_over_the_unix_socket() {
    let dir = TempDir::new("pair");
    let path = dir.0.join("apm.socket");
    let (addr, mut rx) =
        start(DatadogTraceInput::new().with_bind("127.0.0.1:0").with_socket(&path)).await;
    let addr = addr.unwrap();

    let batch = first_hop(addr, &mut rx, "/v0.4/traces", &v04_body()).await;
    DatadogTraceOutput::unix(&path).send(&batch).await.expect("the socket accepts");
    assert_eq!(recv(&mut rx).await, batch);

    let batch = first_hop(addr, &mut rx, "/v0.7/traces", &v07_body()).await;
    let mut out = DatadogTraceOutput::unix(&path).with_version(TracerApiForm::V07);
    out.send(&batch).await.unwrap();
    assert_eq!(recv(&mut rx).await, batch);

    let batch = first_hop(addr, &mut rx, "/v0.6/stats", &stats_body()).await;
    out.send(&batch).await.unwrap();
    assert_eq!(recv(&mut rx).await, batch);
}
