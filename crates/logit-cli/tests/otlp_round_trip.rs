//! `otlp_output_to_otlp_input_round_trips_a_batch_through_http`/`_through_grpc` --
//! `docs/plans/otlp-end-to-end.md`'s "strongest single test, needing no external service" for
//! PR3: stand an [`OtlpInput`] up on an ephemeral port in-process, point an [`OtlpOutput`] at it,
//! and assert what comes out the far end's [`Fanout`] matches what went in. Lives here (an
//! integration test in `logit-cli`, which already depends on both `logit-inputs` and
//! `logit-outputs` as ordinary dependencies -- see that crate's `Cargo.toml`) rather than as a
//! dev-dependency cycle between the two sibling crates, neither of which otherwise has any reason
//! to know about the other.

use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, LogRecord, MetricKind, MetricRecord, Resource,
    SpanKind, SpanRecord, SpanStatus, TraceRef, Value,
};
use logit_inputs::otlp::{OtlpInput, OtlpTransport as InTransport};
use logit_outputs::otlp::{OtlpCompression, OtlpOutput, OtlpTransport as OutTransport};
use logit_pipeline::{Fanout, Input, Output};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Reserves an ephemeral port by binding then immediately dropping a listener, the same
/// bind-drop-rebind idiom `crates/logit-inputs/src/otlp.rs`'s own tests use to learn a free port
/// before constructing the component that will actually bind it.
async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

fn mixed_signal_batch() -> EventBatch {
    let log = Event::log(
        1_000,
        AttrMap::new(),
        LogRecord {
            message: Value::str("hello"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: Some(TraceRef { trace_id: [5; 16], span_id: Some([9; 8]), flags: 1 }),
        },
    );
    let metric = Event::metric(
        2_000,
        AttrMap::new(),
        MetricRecord {
            name: logit_core::interner::intern("requests"),
            kind: MetricKind::Counter(3.0),
            unit: None,
        },
    );
    let span = Event::span(
        3_000,
        AttrMap::new(),
        SpanRecord {
            trace_id: [7; 16],
            span_id: [6; 8],
            parent_span_id: None,
            name: Value::str("round-trip span"),
            kind: SpanKind::Internal,
            status: SpanStatus::Ok,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 4_000,
        },
    );
    let mut resource = Resource::default();
    resource.attributes.insert("host", "roundtrip-host");
    EventBatch { resource: Arc::new(resource), events: vec![log, metric, span] }
}

/// Runs `input` in the background, sends `batch` through `output`, and returns every
/// [`EventBatch`] the input's own `Fanout` received -- one per `Resource*` entry the wire request
/// carried (`logit-proto`'s decode side never collapses several into one).
async fn round_trip(
    mut input: OtlpInput,
    mut output: OtlpOutput,
    batch: &EventBatch,
) -> Vec<EventBatch> {
    let (tx, mut rx) = mpsc::channel(16);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });
    // No readiness signal from `Input::run` (it never returns until the listener errors) -- a
    // short sleep before the first request is the same idiom `crates/logit-inputs/src/otlp.rs`'s
    // own tests use to let the `TcpListener::bind` inside `run` actually happen first.
    tokio::time::sleep(Duration::from_millis(50)).await;

    output.send(batch).await.expect("send should succeed against a live otlp_in");

    let mut received = Vec::new();
    while let Ok(Some(delivered)) =
        tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
    {
        received.push(logit_pipeline::unwrap_batch(delivered));
    }
    received
}

fn assert_round_tripped(received: &[EventBatch]) {
    assert!(!received.is_empty(), "expected at least one batch out the far end");
    let has_log = received.iter().any(|b| b.events.iter().any(|e| e.log.is_some()));
    let has_metric = received.iter().any(|b| b.events.iter().any(|e| !e.metrics.is_empty()));
    let has_span = received
        .iter()
        .any(|b| b.events.iter().any(|e| e.span.as_ref().is_some_and(|s| s.trace_id == [7; 16])));
    let log_trace = received
        .iter()
        .find_map(|b| b.events.iter().find_map(|e| e.log.as_ref().and_then(|l| l.trace)));
    assert!(has_log, "the log signal should have round-tripped");
    assert!(has_metric, "the metric signal should have round-tripped");
    assert!(has_span, "the span signal should have round-tripped, with its trace_id intact");
    assert_eq!(
        log_trace,
        Some(TraceRef { trace_id: [5; 16], span_id: Some([9; 8]), flags: 1 }),
        "the log's own trace context should have round-tripped intact"
    );
    assert!(
        received.iter().any(|b| b.resource.attributes.get("host").and_then(|v| v.as_str())
            == Some("roundtrip-host")),
        "the resource attribute should have round-tripped"
    );
}

#[tokio::test]
async fn otlp_output_to_otlp_input_round_trips_a_batch_through_http() {
    let addr = ephemeral_addr().await;
    let input = OtlpInput::new(addr.clone(), InTransport::Http);
    let output = OtlpOutput::new(format!("http://{addr}"), OutTransport::Http).unwrap();

    let received = round_trip(input, output, &mixed_signal_batch()).await;
    assert_round_tripped(&received);
}

#[tokio::test]
async fn otlp_output_to_otlp_input_round_trips_a_batch_through_grpc() {
    let addr = ephemeral_addr().await;
    let input = OtlpInput::new(addr.clone(), InTransport::Grpc);
    let output = OtlpOutput::new(addr.clone(), OutTransport::Grpc).unwrap();

    let received = round_trip(input, output, &mixed_signal_batch()).await;
    assert_round_tripped(&received);
}

/// The pair `docs/adr/otlp-compression-and-decompression-bounds.md` exists for: proves
/// `otlp_out`'s gzip compression and `otlp_in`'s matching decode landed together, not just that
/// each side's own unit tests pass in isolation.
#[tokio::test]
async fn otlp_output_to_otlp_input_round_trips_a_gzip_compressed_batch_through_http() {
    let addr = ephemeral_addr().await;
    let input = OtlpInput::new(addr.clone(), InTransport::Http);
    let output = OtlpOutput::new(format!("http://{addr}"), OutTransport::Http)
        .unwrap()
        .with_compression(OtlpCompression::Gzip);

    let received = round_trip(input, output, &mixed_signal_batch()).await;
    assert_round_tripped(&received);
}

#[tokio::test]
async fn otlp_output_to_otlp_input_round_trips_a_gzip_compressed_batch_through_grpc() {
    let addr = ephemeral_addr().await;
    let input = OtlpInput::new(addr.clone(), InTransport::Grpc);
    let output = OtlpOutput::new(addr.clone(), OutTransport::Grpc)
        .unwrap()
        .with_compression(OtlpCompression::Gzip);

    let received = round_trip(input, output, &mixed_signal_batch()).await;
    assert_round_tripped(&received);
}
