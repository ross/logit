//! `datadog_out -> datadog_in` over a real socket: the sink's `endpoints` point every intake at a
//! listener on `127.0.0.1:0`, and each route's batch arrives exactly as sent.
//!
//! Each batch is one decode of a hand-written wire payload, so it sits on the codec's fixed point
//! (`crates/logit-proto/tests/datadog_*_fixed_point.rs`), and `datadog_out` re-encoding it gives
//! the same batch back through `datadog_in`. Timestamps are relative to now, since `datadog_out`
//! drops points outside Datadog's windows before sending. What this file adds over the codec
//! tests is both HTTP hops: routing, `Content-Type`, gzip and deflate, the `DD-API-KEY` check,
//! and the two things `datadog_out` deliberately doesn't send.

use logit_core::interner::{intern, resolve};
use logit_core::{
    AttrMap, Event, EventBatch, HyperLogLog, MetricKind, MetricRecord, Registry, Resource, Value,
};
use logit_inputs::datadog::DatadogInput;
use logit_outputs::datadog::{DatadogEndpoints, DatadogOutput};
use logit_pipeline::{Fanout, Input, Output};
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
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const KEY: &str = "0123456789abcdef0123456789abcdef";

/// Now, in whole seconds: every metric, log, event, and check below is stamped with it.
fn now_s() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

async fn listener() -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = DatadogInput::new("127.0.0.1:0").with_api_keys(vec![KEY.to_string()]);
    input.bind().await.expect("binding datadog_in");
    let addr = input.local_addr().expect("bind() leaves an address");
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        let _ = input.run(Fanout::new(vec![tx])).await;
    });
    (addr, rx)
}

/// A `datadog_out` with all three intakes on `addr`, reporting into `registry`.
fn sink(addr: SocketAddr, registry: &Registry) -> DatadogOutput {
    let base = format!("http://{addr}");
    DatadogOutput::new(KEY)
        .unwrap()
        .with_endpoints(DatadogEndpoints {
            api: Some(base.clone()),
            logs: Some(base.clone()),
            traces: Some(base),
        })
        .with_telemetry(registry.telemetry_for("out", "datadog_out", "sink"))
}

async fn recv(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
    let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("datadog_in delivers a batch within 5s")
        .expect("the Fanout channel is open");
    logit_pipeline::unwrap_batch(delivered)
}

async fn assert_nothing_delivered(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) {
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(),
        "nothing reaches datadog_in"
    );
}

/// A counter's total across every drained point tagged `reason`.
fn dropped(points: &[Event], reason: &str) -> f64 {
    points
        .iter()
        .filter(|e| e.attributes.get("reason").and_then(Value::as_str) == Some(reason))
        .flat_map(|e| e.metrics.iter())
        .filter(|m| resolve(m.name) == "logit.output.records.dropped")
        .map(|m| match &m.kind {
            MetricKind::Sum(s) => s.value,
            _ => 0.0,
        })
        .sum()
}

// -------------------------------------------------------------------------------------------------
// One batch per route, each one decode of a hand-written payload
// -------------------------------------------------------------------------------------------------

fn decoder() -> DatadogDecoder {
    DatadogDecoder::new()
}

fn series() -> EventBatch {
    let now = now_s();
    let seed = MetricPayload {
        series: vec![
            MetricSeries {
                resources: vec![PbResource { r#type: "host".into(), name: "web-1".into() }],
                metric: "requests".into(),
                tags: vec!["env:prod".into(), "urgent".into()],
                points: vec![MetricPoint { value: 3.0, timestamp: now }],
                r#type: 1,
                unit: "request".into(),
                interval: 10,
                ..Default::default()
            },
            MetricSeries {
                metric: "load".into(),
                points: vec![MetricPoint { value: 0.75, timestamp: now }],
                r#type: 3,
                ..Default::default()
            },
            MetricSeries {
                metric: "queue.rate".into(),
                points: vec![MetricPoint { value: 0.5, timestamp: now }],
                r#type: 2,
                interval: 10,
                ..Default::default()
            },
        ],
    }
    .encode_to_vec();
    decoder().decode_series_v2_protobuf(&seed, 0).unwrap()
}

fn distribution_points() -> EventBatch {
    let seed = format!(
        r#"{{"series":[{{"metric":"latency","points":[[{},[1.0,2.5,4.0]]],"host":"h1","tags":["env:prod"]}}]}}"#,
        now_s()
    );
    decoder().decode_distribution_points(seed.as_bytes(), 0).unwrap()
}

fn sketches() -> EventBatch {
    let seed = SketchPayload {
        sketches: vec![Sketch {
            metric: "request.latency".into(),
            host: "web-1".into(),
            tags: vec!["env:prod".into()],
            dogsketches: vec![Dogsketch {
                ts: now_s(),
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
    decoder().decode_sketches(&seed, 0).unwrap()
}

fn logs() -> EventBatch {
    let ms = now_s() * 1000;
    let seed = format!(
        r#"[{{"message":"GET / 200","status":"info","timestamp":{ms},"hostname":"web-1","service":"nginx","ddsource":"nginx","ddtags":"env:prod"}},{{"message":"boom","status":"error","timestamp":{},"service":"api","http":{{"status_code":500}}}}]"#,
        ms + 123
    );
    decoder().decode_logs(seed.as_bytes(), 0).unwrap()
}

/// The public v1 form, which `datadog_out` sends one event per request.
fn datadog_event() -> EventBatch {
    let seed = format!(
        r#"{{"title":"deploy","text":"v2 is out","date_happened":{},"priority":"normal","host":"web-1","alert_type":"info","aggregation_key":"deploys","tags":["env:prod"]}}"#,
        now_s()
    );
    decoder().decode_events(seed.as_bytes(), 0).unwrap()
}

fn service_check() -> EventBatch {
    let seed = format!(
        r#"[{{"check":"app.can_connect","host_name":"h1","status":2,"message":"refused","timestamp":{},"tags":["db:main"]}}]"#,
        now_s()
    );
    decoder().decode_service_checks(seed.as_bytes(), 0).unwrap()
}

/// One tracer payload, so one batch; `top_level` decides whether the Agent's mark is on every
/// span.
fn traces(top_level: bool) -> EventBatch {
    let span = |span_id: u64, parent_id: u64, name: &str| Span {
        service: "checkout".into(),
        name: name.into(),
        resource: "POST /cart".into(),
        trace_id: 7,
        span_id,
        parent_id,
        start: 1_700_000_000_000_000_000 + span_id as i64,
        duration: 1_500_000,
        meta: HashMap::from([("env".into(), "prod".into())]),
        metrics: if top_level {
            HashMap::from([("_top_level".into(), 1.0)])
        } else {
            HashMap::from([("_sampling_priority_v1".into(), 1.0)])
        },
        r#type: "web".into(),
        ..Default::default()
    };
    let seed = AgentPayload {
        host_name: "agent-host".into(),
        env: "prod".into(),
        agent_version: "7.60.0".into(),
        tracer_payloads: vec![TracerPayload {
            language_name: "python".into(),
            hostname: "web-1".into(),
            chunks: vec![TraceChunk {
                priority: 1,
                spans: vec![span(1, 0, "http.request"), span(2, 1, "db.query")],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    let mut batches = decoder().decode_agent_payload(&seed, 0).unwrap();
    assert_eq!(batches.len(), 1);
    batches.remove(0)
}

/// Built on the model, encoded, and decoded once, so it is on the stats codec's fixed point.
fn stats() -> EventBatch {
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
    let body = DatadogEncoder::new().encode_stats_payload(&seed).unwrap();
    let mut batches = decoder().decode_stats_payload(&body, 0).unwrap();
    assert_eq!(batches.len(), 1);
    batches.remove(0)
}

// -------------------------------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------------------------------

/// Every route: the batch `datadog_in` delivers is the batch `datadog_out` was given.
#[tokio::test]
async fn every_route_relays_its_batch_unchanged() {
    let (addr, mut rx) = listener().await;
    let registry = Registry::new();
    let mut out = sink(addr, &registry);
    let cases = [
        ("series", series()),
        ("distribution points", distribution_points()),
        ("sketches", sketches()),
        ("logs", logs()),
        ("event", datadog_event()),
        ("service check", service_check()),
        ("traces", traces(true)),
        ("stats", stats()),
    ];
    for (name, batch) in cases {
        assert!(!batch.events.is_empty(), "{name}: the case carries events");
        out.send(&batch).await.unwrap_or_else(|err| panic!("{name}: {err:#}"));
        assert_eq!(recv(&mut rx).await, batch, "{name}");
    }
    assert_nothing_delivered(&mut rx).await;
}

/// A `Set` goes out as a gauge of its estimate, the Agent's own `s` semantics: the one series
/// kind that isn't a fixed point.
#[tokio::test]
async fn a_set_arrives_as_a_gauge_of_its_estimate() {
    let (addr, mut rx) = listener().await;
    let registry = Registry::new();
    let mut hll = HyperLogLog::new();
    for member in [b"a".as_slice(), b"b", b"c"] {
        hll.insert(member);
    }
    let estimate = hll.estimate() as f64;
    let batch = EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![Event::metric(
            now_s() * 1_000_000_000,
            AttrMap::new(),
            MetricRecord::new(intern("users"), MetricKind::Set(hll)),
        )],
    };
    sink(addr, &registry).send(&batch).await.unwrap();
    let delivered = recv(&mut rx).await;
    assert_eq!(delivered.events.len(), 1);
    let record = &delivered.events[0].metrics[0];
    assert_eq!(resolve(record.name), "users");
    assert_eq!(record.kind, MetricKind::Gauge(estimate));
}

/// A chunk an Agent hasn't processed (no `_top_level`) is counted and never sent.
#[tokio::test]
async fn an_unprocessed_trace_chunk_is_counted_and_not_delivered() {
    let (addr, mut rx) = listener().await;
    let registry = Registry::new();
    let batch = traces(false);
    sink(addr, &registry).send(&batch).await.expect("nothing to send is not a failure");
    assert_nothing_delivered(&mut rx).await;
    assert_eq!(dropped(&registry.drain(0), "needs_agent_processing"), 2.0, "one per span");
}

/// A point older than Datadog's 1h window is counted and never sent.
#[tokio::test]
async fn a_stale_point_is_counted_and_not_delivered() {
    let (addr, mut rx) = listener().await;
    let registry = Registry::new();
    let two_hours_ago = (now_s() - 2 * 3600) * 1_000_000_000;
    let batch = EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![Event::metric(
            two_hours_ago,
            AttrMap::new(),
            MetricRecord::new(intern("load"), MetricKind::Gauge(1.0)),
        )],
    };
    sink(addr, &registry).send(&batch).await.unwrap();
    assert_nothing_delivered(&mut rx).await;
    assert_eq!(dropped(&registry.drain(0), "stale"), 1.0);
}
