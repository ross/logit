//! `otlp_out -> otlp_in` over real sockets, the OTLP pair of ADR `lossless-transit`: an
//! [`OtlpInput`] bound on an ephemeral port in-process, an [`OtlpOutput`] pointed at it, and a
//! whole-value check that what leaves the far end's [`Fanout`] matches what went in. Covers HTTP
//! and gRPC, each plain, gzip-compressed, and over TLS. It lives in `logit-cli`, which already
//! depends on both `logit-inputs` and `logit-outputs`, rather than as a dev-dependency between
//! those two sibling crates.

use logit_core::interner::intern;
use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, Exemplar, LogRecord, MetricKind, MetricRecord,
    Resource, Scope, Severity, SpanEvent, SpanExt, SpanKind, SpanLink, SpanRecord, SpanStatus, Sum,
    Temporality, TraceRef, Value,
};
use logit_inputs::otlp::{OtlpInput, OtlpTransport as InTransport, TlsServerSettings};
use logit_outputs::otlp::{
    OtlpCompression, OtlpOutput, OtlpTransport as OutTransport, TlsClientSettings,
};
use logit_pipeline::{Fanout, Input, Output};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Reserves an ephemeral port by binding and dropping a listener (bind-drop-rebind), as
/// `crates/logit-inputs/src/otlp.rs`'s tests do.
async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

// -- The fixture: fully populated, shared by every field this file asserts on -------------------
//
// `mixed_signal_batch` carries one event per signal, plus a second metric event for
// `MetricRecord::flags`, so it splits into three batches on the wire: OTLP sends each signal
// separately (`logit_proto::Signal`'s doc). `assert_round_tripped` finds each signal's batch and
// asserts whole-value equality against these builders.

fn fixture_resource() -> Arc<Resource> {
    let mut attributes = AttrMap::new();
    attributes.insert("host", "roundtrip-host");
    Arc::new(Resource {
        attributes,
        dropped_attributes_count: 2,
        schema_url: Some(bytes::Bytes::from_static(b"https://example.com/resource-schema")),
    })
}

fn fixture_scope() -> Arc<Scope> {
    let mut attributes = AttrMap::new();
    attributes.insert("scope.attr", "scope-value");
    Arc::new(Scope {
        name: bytes::Bytes::from_static(b"otlp-round-trip-scope"),
        version: bytes::Bytes::from_static(b"1.2.3"),
        attributes,
        dropped_attributes_count: 1,
        schema_url: Some(bytes::Bytes::from_static(b"https://example.com/scope-schema")),
    })
}

/// In the shape a real OTLP decode produces: `otel.severity_number`/`otel.severity_text` match
/// `Severity::Info`'s `INFO2` (10) band member. Encode consumes these two attributes and decode
/// re-stamps them from the wire's severity fields, so attributes that disagree with the `Severity`
/// wouldn't round-trip to themselves (the rule `crates/logit-proto/tests/otlp_fixed_point.rs`'s
/// module doc states).
fn fixture_log_attrs() -> AttrMap {
    let mut attrs = AttrMap::new();
    attrs.insert("otel.severity_number", Value::I64(10));
    attrs.insert("otel.severity_text", "INFO2");
    attrs
}

fn fixture_log() -> LogRecord {
    LogRecord {
        message: Value::str("hello"),
        severity: Some(Severity::Info),
        body_format: BodyFormat::Raw,
        trace: Some(TraceRef { trace_id: [5; 16], span_id: Some([9; 8]), flags: 1 }),
        event_name: Some(intern("round_trip_event")),
        observed_timestamp: 1_700_000_000_500_000_000,
        dropped_attributes_count: 3,
    }
}

fn fixture_exemplar(trace: Option<TraceRef>) -> Exemplar {
    let mut filtered_attributes = AttrMap::new();
    filtered_attributes.insert("dropped", "attr");
    Exemplar { timestamp: 1_700_000_000_100_000_000, value: 3.5, trace, filtered_attributes }
}

/// A `Sum` carrying `description`/`start_timestamp`/two exemplars (one with a trace, one
/// without).
fn fixture_described_metric() -> MetricRecord {
    MetricRecord {
        description: Some(intern("a fully described metric")),
        start_timestamp: 1_699_000_000_000_000_000,
        exemplars: vec![
            fixture_exemplar(Some(TraceRef {
                trace_id: [7; 16],
                span_id: Some([6; 8]),
                // OTLP's `Exemplar` has no flags field (`decode_exemplar` always passes 0), so
                // the fixture's must be 0 to be a fixed point.
                flags: 0,
            })),
            fixture_exemplar(None),
        ],
        ..MetricRecord::new(
            intern("requests"),
            MetricKind::Sum(Sum {
                value: 3.0,
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
        )
    }
}

/// `FLAG_NO_RECORDED_VALUE`, with `start_timestamp` at `MetricRecord::new`'s `0`: encode writes
/// `start_timestamp` through verbatim (`crates/logit-proto/src/otlp/metrics.rs`'s module doc), so
/// a zero start time is a fixed point.
fn fixture_flagged_metric() -> MetricRecord {
    MetricRecord {
        flags: MetricRecord::FLAG_NO_RECORDED_VALUE,
        ..MetricRecord::new(intern("round_trip_flagged_metric"), MetricKind::Gauge(0.0))
    }
}

fn fixture_span() -> SpanRecord {
    let mut event_attrs = AttrMap::new();
    event_attrs.insert("checkpoint.attr", "value");
    let mut link_attrs = AttrMap::new();
    link_attrs.insert("link.attr", "value");

    SpanRecord {
        trace_id: [7; 16],
        span_id: [6; 8],
        parent_span_id: Some([3; 8]),
        name: Value::str("round-trip span"),
        kind: SpanKind::Internal,
        status: SpanStatus::Ok,
        events: vec![SpanEvent {
            timestamp: 3_500,
            name: Value::str("checkpoint"),
            attributes: event_attrs,
            dropped_attributes_count: 2,
        }],
        links: vec![SpanLink {
            trace_id: [4; 16],
            span_id: [5; 8],
            attributes: link_attrs,
            flags: 1,
            trace_state: Some(bytes::Bytes::from_static(b"vendor=linkvalue")),
            dropped_attributes_count: 3,
        }],
        end_timestamp: 4_000,
        flags: 1,
        ext: Some(Box::new(SpanExt {
            status_message: Some(bytes::Bytes::from_static(b"boom")),
            trace_state: Some(bytes::Bytes::from_static(b"vendor=rootvalue")),
            dropped_attributes_count: 4,
            dropped_events_count: 5,
            dropped_links_count: 6,
        })),
    }
}

/// One log, two metric events (one described, one flagged), and one span, sharing a fully
/// populated `Resource`/`Scope`. Splits into three batches on the wire.
fn mixed_signal_batch() -> EventBatch {
    let log = Event::log(1_000, fixture_log_attrs(), fixture_log());
    let metric_described = Event::metric(2_000, AttrMap::new(), fixture_described_metric());
    let metric_flagged = Event::metric(2_100, AttrMap::new(), fixture_flagged_metric());
    let span = Event::span(3_000, AttrMap::new(), fixture_span());
    EventBatch {
        resource: fixture_resource(),
        scope: Some(fixture_scope()),
        events: vec![log, metric_described, metric_flagged, span],
    }
}

/// Runs `input` in the background, sends `batch` through `output`, and returns every
/// [`EventBatch`] the input's own `Fanout` received -- one per `Resource*` entry the wire request
/// carried (`logit-proto`'s decode side never collapses several into one).
async fn round_trip(
    mut input: OtlpInput,
    mut output: OtlpOutput,
    batch: &EventBatch,
) -> Vec<EventBatch> {
    // `Input::bind` opens the listener before `run`'s accept loop starts, so `output.send` below
    // can't race the bind.
    input.bind().await.expect("binding the otlp listener");
    let (tx, mut rx) = mpsc::channel(16);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });

    output.send(batch).await.expect("send should succeed against a live otlp_in");

    let mut received = Vec::new();
    while let Ok(Some(delivered)) =
        tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
    {
        received.push(logit_pipeline::unwrap_batch(delivered));
    }
    received
}

/// Reassembles `received` (`mixed_signal_batch`'s three per-signal batches) and asserts each
/// record, and each batch's `resource`/`scope`, whole-value against the fixture, timestamps
/// included. `fixture_log`'s non-zero `observed_timestamp` is what makes `LogRecord`'s encode
/// deterministic (`crates/logit-proto/src/otlp/logs.rs`'s module doc).
fn assert_round_tripped(received: &[EventBatch]) {
    assert!(!received.is_empty(), "expected at least one batch out the far end");

    let logs_batch = received
        .iter()
        .find(|b| b.events.iter().any(|e| e.log.is_some()))
        .expect("the log signal should have round-tripped");
    assert_eq!(logs_batch.events.len(), 1, "the logs batch should carry exactly the one log event");
    assert_eq!(
        logs_batch.events[0].log,
        Some(fixture_log()),
        "LogRecord should round-trip exactly"
    );
    assert_eq!(
        logs_batch.events[0].attributes,
        fixture_log_attrs(),
        "the otel.severity_* attributes should round-trip alongside the normalized Severity"
    );
    assert_eq!(logs_batch.events[0].timestamp, 1_000);
    assert_eq!(logs_batch.resource, fixture_resource(), "the log batch's resource should match");
    assert_eq!(logs_batch.scope, Some(fixture_scope()), "the log batch's scope should match");

    let metrics_batch = received
        .iter()
        .find(|b| b.events.iter().any(|e| !e.metrics.is_empty()))
        .expect("the metric signal should have round-tripped");
    assert_eq!(
        metrics_batch.events.len(),
        2,
        "the metrics batch should carry exactly the two metric events"
    );
    assert_eq!(metrics_batch.events[0].metrics.to_vec(), vec![fixture_described_metric()]);
    assert_eq!(metrics_batch.events[0].timestamp, 2_000);
    assert_eq!(metrics_batch.events[1].metrics.to_vec(), vec![fixture_flagged_metric()]);
    assert_eq!(metrics_batch.events[1].timestamp, 2_100);
    assert_eq!(
        metrics_batch.resource,
        fixture_resource(),
        "the metrics batch's resource should match"
    );
    assert_eq!(
        metrics_batch.scope,
        Some(fixture_scope()),
        "the metrics batch's scope should match"
    );

    let traces_batch = received
        .iter()
        .find(|b| b.events.iter().any(|e| e.span.is_some()))
        .expect("the span signal should have round-tripped");
    assert_eq!(
        traces_batch.events.len(),
        1,
        "the traces batch should carry exactly the one span event"
    );
    assert_eq!(
        traces_batch.events[0].span,
        Some(fixture_span()),
        "SpanRecord should round-trip exactly, flags/ext/links/events included"
    );
    assert_eq!(traces_batch.events[0].timestamp, 3_000);
    assert_eq!(
        traces_batch.resource,
        fixture_resource(),
        "the traces batch's resource should match"
    );
    assert_eq!(traces_batch.scope, Some(fixture_scope()), "the traces batch's scope should match");
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

/// The pair `docs/adr/otlp-compression-and-decompression-bounds.md` exists for: `otlp_out`'s gzip
/// and `otlp_in`'s decode interoperate, beyond each side's own unit tests.
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

/// TLS counterparts to the round trips above, for `docs/adr/otlp-tls-and-pooled-grpc-client.md`:
/// `otlp_out`'s client TLS and `otlp_in`'s server TLS interoperate, beyond each side's own unit
/// tests against a canned peer.
mod tls {
    use super::*;

    fn testdata_dir() -> std::path::PathBuf {
        // The certs are the repo root's `testdata/tls` (`testdata/tls/README.md`), two levels
        // above `CARGO_MANIFEST_DIR`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    #[tokio::test]
    async fn otlp_output_to_otlp_input_round_trips_a_batch_through_https() {
        let addr = ephemeral_addr().await;
        let input = OtlpInput::new(addr.clone(), InTransport::Http)
            .with_tls(
                &TlsServerSettings {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                },
                &testdata_dir(),
            )
            .unwrap();
        let output = OtlpOutput::new(format!("https://{addr}"), OutTransport::Http)
            .unwrap()
            .with_tls(
                &TlsClientSettings { ca_file: Some("ca.pem".to_string()), ..Default::default() },
                &testdata_dir(),
            )
            .unwrap();

        let received = round_trip(input, output, &mixed_signal_batch()).await;
        assert_round_tripped(&received);
    }

    #[tokio::test]
    async fn otlp_output_to_otlp_input_round_trips_a_batch_through_grpc_over_tls() {
        let addr = ephemeral_addr().await;
        let input = OtlpInput::new(addr.clone(), InTransport::Grpc)
            .with_tls(
                &TlsServerSettings {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                },
                &testdata_dir(),
            )
            .unwrap();
        let output = OtlpOutput::new(format!("https://{addr}"), OutTransport::Grpc)
            .unwrap()
            .with_tls(
                &TlsClientSettings { ca_file: Some("ca.pem".to_string()), ..Default::default() },
                &testdata_dir(),
            )
            .unwrap();

        let received = round_trip(input, output, &mixed_signal_batch()).await;
        assert_round_tripped(&received);
    }

    /// Mutual TLS: `otlp_in` requires a client certificate chaining to `ca.pem`, `otlp_out`
    /// presents `client.pem`/`client.key` -- both signed by the same test CA
    /// (`testdata/tls/regen.sh`).
    #[tokio::test]
    async fn otlp_output_to_otlp_input_round_trips_a_batch_through_mutual_tls() {
        let addr = ephemeral_addr().await;
        let input = OtlpInput::new(addr.clone(), InTransport::Http)
            .with_tls(
                &TlsServerSettings {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: Some("ca.pem".to_string()),
                },
                &testdata_dir(),
            )
            .unwrap();
        let output = OtlpOutput::new(format!("https://{addr}"), OutTransport::Http)
            .unwrap()
            .with_tls(
                &TlsClientSettings {
                    ca_file: Some("ca.pem".to_string()),
                    cert_file: Some("client.pem".to_string()),
                    key_file: Some("client.key".to_string()),
                    insecure_skip_verify: false,
                },
                &testdata_dir(),
            )
            .unwrap();

        let received = round_trip(input, output, &mixed_signal_batch()).await;
        assert_round_tripped(&received);
    }
}
