//! `splunk_hec_out -> splunk_hec_in` over a real socket: the sink's `endpoint` points at a
//! listener on `127.0.0.1:0`, and each batch arrives as sent.
//!
//! Each batch is one decode of a hand-written HEC body, so it sits on the codec's fixed point
//! (`crates/logit-proto/tests/splunk_fixed_point.rs`), and `splunk_hec_out` re-encoding it gives
//! the same batch back through `splunk_hec_in`. What this file adds over the codec tests is both
//! HTTP hops: the `/event` route, gzip, the token check, body splitting, and the channel and
//! acknowledgment exchange, which the listener answers per channel as a `useACK` token does.

use logit_core::{Event, EventBatch, MetricKind, Registry, Value};
use logit_inputs::splunk::SplunkHecInput;
use logit_outputs::splunk::{SplunkCompression, SplunkHecOutput};
use logit_pipeline::{Fanout, Input, Output};
use logit_proto::splunk::SplunkDecoder;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;

const TOKEN: &str = "11111111-2222-3333-4444-555555555555";

/// Every object below carries a `time`, so nothing decodes to this.
const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;

async fn listener() -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = SplunkHecInput::new("127.0.0.1:0").with_tokens(vec![TOKEN.to_string()]);
    input.bind().await.expect("binding splunk_hec_in");
    let addr = input.local_addr().expect("bind() leaves an address");
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let _ = input.run(Fanout::new(vec![tx])).await;
    });
    (addr, rx)
}

/// A `splunk_hec_out` pointed at `addr`, reporting into `registry`.
fn sink(addr: SocketAddr, registry: &Registry) -> SplunkHecOutput {
    SplunkHecOutput::new(format!("http://{addr}/services/collector"), TOKEN)
        .unwrap()
        .with_telemetry(registry.telemetry_for("out", "splunk_hec_out", "sink"))
}

async fn recv(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
    let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("splunk_hec_in delivers a batch within 5s")
        .expect("the Fanout channel is open");
    logit_pipeline::unwrap_batch(delivered)
}

async fn assert_nothing_delivered(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) {
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(),
        "nothing more reaches splunk_hec_in"
    );
}

/// The one batch `body` decodes to.
fn seed(body: &str) -> EventBatch {
    let mut batches =
        SplunkDecoder::new().decode_events(body.as_bytes(), RECEIVED_AT).expect("the seed decodes");
    assert_eq!(batches.len(), 1, "one envelope per seed");
    batches.remove(0)
}

/// Logs in the OpenTelemetry exporter's shape: a string body with severity, name, and trace
/// context; an object body; and nested `fields`.
fn logs() -> EventBatch {
    seed(concat!(
        r#"{"time":1700000000.123456789,"host":"web-1","source":"app","sourcetype":"otel","index":"main","event":"user logged in","fields":{"service.name":"auth","otel.log.severity.text":"INFO","otel.log.severity.number":9,"otel.log.name":"login","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","user.id":42}}"#,
        r#"{"time":1700000000.2,"host":"web-1","source":"app","sourcetype":"otel","index":"main","event":{"msg":"structured","attempt":2,"tags":["a","b"]},"fields":{"service.name":"auth","ok":true}}"#,
    ))
}

/// Both metric forms and both `metric_type`s under one envelope.
fn metrics() -> EventBatch {
    seed(concat!(
        r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Gauge","metric_name:process.memory.usage":123456789,"metric_name:cpu.load":0.75}}"#,
        r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Sum","metric_name:http.server.requests":1500}}"#,
        r#"{"time":1700000002,"host":"web-1","event":"metric","fields":{"region":"us-east","metric_name":"queue.depth","_value":17}}"#,
    ))
}

/// A span whose envelope `fields` are its resource.
fn spans() -> EventBatch {
    seed(
        r#"{"time":1700000002.5,"host":"web-1","source":"app","sourcetype":"otel","index":"traces","event":{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","parent_span_id":"00f067aa0ba902b7","name":"POST /login","kind":"SPAN_KIND_SERVER","attributes":{"http.method":"POST","http.status_code":200},"start_time":1700000002500000000,"end_time":1700000002750000000,"status":{"message":"","code":"STATUS_CODE_UNSET"},"events":[{"attributes":{"k":"v"},"name":"retry","timestamp":1700000002600000000}]},"fields":{"service.name":"auth"}}"#,
    )
}

/// Logs, both metric forms, and a span: the batch `splunk_hec_in` delivers is the batch
/// `splunk_hec_out` was given, gzipped and not.
#[tokio::test]
async fn every_signal_relays_its_batch_unchanged() {
    let (addr, mut rx) = listener().await;
    let registry = Registry::new();
    for compression in [SplunkCompression::Gzip, SplunkCompression::None] {
        let mut out = sink(addr, &registry).with_compression(compression);
        for (name, batch) in [("logs", logs()), ("metrics", metrics()), ("spans", spans())] {
            assert!(!batch.events.is_empty(), "{name}: the case carries events");
            out.send(&batch).await.unwrap_or_else(|err| panic!("{name}: {err:#}"));
            assert_eq!(recv(&mut rx).await, batch, "{name} {compression:?}");
        }
    }
    assert_nothing_delivered(&mut rx).await;
}

/// The seeds carry what they claim: the single-metric form, a `Sum`, a span, and the log's
/// typed fields, so the relay test above covers each.
#[test]
fn the_seeds_cover_both_metric_forms_a_span_and_typed_log_fields() {
    let metrics = metrics();
    let kinds: Vec<&MetricKind> =
        metrics.events.iter().flat_map(|e: &Event| e.metrics.iter().map(|m| &m.kind)).collect();
    assert!(kinds.iter().any(|k| matches!(k, MetricKind::Sum(_))));
    assert!(kinds.iter().any(|k| matches!(k, MetricKind::Gauge(_))));
    assert_eq!(metrics.events.len(), 3, "one event per object, the single-metric one included");
    assert!(spans().events[0].span.is_some());
    let log = logs().events[0].log.clone().unwrap();
    assert!(log.trace.is_some() && log.severity.is_some() && log.event_name.is_some());
    assert_eq!(logs().events[0].attributes.get("user.id"), Some(&Value::I64(42)));
}

/// A batch split across several bodies arrives whole, in order, one delivery per body.
#[tokio::test]
async fn a_split_batch_arrives_in_order() {
    let (addr, mut rx) = listener().await;
    let registry = Registry::new();
    let mut batch = logs();
    let template = batch.events[0].clone();
    batch.events = (0..20).map(|_| template.clone()).collect();
    let mut out = sink(addr, &registry).with_max_body_bytes(1_000);
    out.send(&batch).await.unwrap();

    let mut delivered = Vec::new();
    while delivered.len() < batch.events.len() {
        delivered.extend(recv(&mut rx).await.events);
    }
    assert_eq!(delivered, batch.events);
    assert_nothing_delivered(&mut rx).await;
}

/// The `Sum` total of `name` in `events`, over points whose `key` tag is `value`.
fn sum_tagged(events: &[Event], name: &str, key: &str, value: &str) -> f64 {
    events
        .iter()
        .filter(|e| e.attributes.get(key).and_then(Value::as_str) == Some(value))
        .flat_map(|e| e.metrics.iter())
        .filter(|m| logit_core::interner::resolve(m.name) == name)
        .map(|m| match &m.kind {
            MetricKind::Sum(s) => s.value,
            _ => 0.0,
        })
        .sum()
}

/// Under `ack: true` the sink polls `/ack` on its channel with the ids the listener issued on
/// it, and every request is acknowledged once: a one-body batch, then a batch split across
/// several bodies, whose ids the sink polls together.
#[tokio::test]
async fn ack_true_is_acknowledged_by_the_listener() {
    let (addr, mut rx) = listener().await;
    let registry = Registry::new();
    let mut out =
        sink(addr, &registry).with_ack(true, Duration::from_secs(10)).with_max_body_bytes(1_000);
    let batch = logs();
    out.send(&batch).await.expect("acknowledged");
    assert_eq!(recv(&mut rx).await, batch);

    let mut split = logs();
    let template = split.events[0].clone();
    split.events = (0..20).map(|_| template.clone()).collect();
    out.send(&split).await.expect("acknowledged");
    let mut delivered = Vec::new();
    while delivered.len() < split.events.len() {
        delivered.extend(recv(&mut rx).await.events);
    }
    assert_eq!(delivered, split.events);

    let events = registry.drain(0);
    let posted = sum_tagged(&events, "logit.output.requests", "route", "event");
    assert!(posted > 2.0, "the second batch took several bodies: {posted}");
    assert_eq!(sum_tagged(&events, "logit.output.acks", "result", "acked"), posted);
    assert_eq!(sum_tagged(&events, "logit.output.acks", "result", "timeout"), 0.0);
}

/// A token the listener doesn't list is refused permanently, and nothing is delivered.
#[tokio::test]
async fn a_wrong_token_is_a_permanent_fault() {
    let (addr, mut rx) = listener().await;
    let mut out =
        SplunkHecOutput::new(format!("http://{addr}/services/collector"), "wrong").unwrap();
    let err = out.send(&logs()).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), logit_pipeline::Fault::Permanent);
    assert_nothing_delivered(&mut rx).await;
}
