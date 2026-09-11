//! `otlp_output_to_otlp_input_round_trips_a_batch_through_http`/`_through_grpc` --
//! `docs/plans/otlp-end-to-end.md`'s "strongest single test, needing no external service" for
//! PR3: stand an [`OtlpInput`] up on an ephemeral port in-process, point an [`OtlpOutput`] at it,
//! and assert what comes out the far end's [`Fanout`] matches what went in. Lives here (an
//! integration test in `logit-cli`, which already depends on both `logit-inputs` and
//! `logit-outputs` as ordinary dependencies -- see that crate's `Cargo.toml`) rather than as a
//! dev-dependency cycle between the two sibling crates, neither of which otherwise has any reason
//! to know about the other.

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

/// Reserves an ephemeral port by binding then immediately dropping a listener, the same
/// bind-drop-rebind idiom `crates/logit-inputs/src/otlp.rs`'s own tests use to learn a free port
/// before constructing the component that will actually bind it.
async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

// -- The fixture: fully populated, shared by every field this file asserts on -------------------
//
// `mixed_signal_batch` carries one event per signal (plus a second metric event, to exercise
// `MetricRecord::flags`), which means it shatters into three separate batches on the wire
// (`crates/logit-proto/src/otlp/mod.rs`'s module doc: "OTLP's wire protocol does [split by
// signal]") -- by design, not a gap. `assert_round_tripped` below reassembles by finding each
// signal's own batch and asserting whole-value equality against these same builder functions,
// rather than the field-by-field spot checks this file used before W4 closed the remaining gaps.

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

/// Already in the shape a real OTLP decode would itself produce -- `otel.severity_number`/
/// `otel.severity_text` match `Severity::Info`'s `INFO2` (10) band member, the same discipline
/// `crates/logit-proto/tests/otlp_fixed_point.rs`'s own module doc requires and for the same
/// reason: encode consumes these two attributes (removes them from the emitted attribute set) and
/// decode re-stamps them fresh from the wire's real severity fields, so a fixture whose attributes
/// don't already match its `Severity` would round-trip to something other than itself.
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
                // OTLP's own Exemplar message has no flags field at all
                // (`decode_exemplar` always passes 0) -- an exemplar's trace flags cannot
                // round-trip through OTLP, so the fixture must already be 0 to be a fixed
                // point.
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

/// `flags = FLAG_NO_RECORDED_VALUE`, `start_timestamp` left at `MetricRecord::new`'s `0` default --
/// a genuine fixed point now that encode writes `start_timestamp` through verbatim with no
/// fallback to the data point's own `time_unix_nano` (`otlp/metrics.rs`'s `start_time`); no longer
/// needs an explicit non-zero value to dodge that old substitution.
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
/// populated `Resource`/`Scope` -- shatters into three OTLP payloads/batches on the wire (see this
/// section's own doc comment).
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
    // `Input::bind` (docs/plans/operator-surface.md, workstream B) opens the listener before
    // `run`'s accept loop starts -- the readiness primitive this test used to fake with a 50 ms
    // sleep now does the real thing: `output.send` below can't race the bind at all.
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

/// Reassembles `received` (`mixed_signal_batch`'s three shattered payloads) and asserts each
/// record, and each batch's `resource`/`scope`, whole-value against the fixture -- not just a
/// field-by-field spot check the way this test used to, before W4 closed the remaining OTLP
/// fidelity gaps. Timestamps are included in that equality: `fixture_log`'s `observed_timestamp`
/// is non-zero, which is what makes `LogRecord`'s encode path deterministic
/// (`crates/logit-proto/src/otlp/logs.rs`'s module doc).
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

/// The TLS counterpart to the four round trips above -- `docs/adr/otlp-tls-and-pooled-grpc-client.md`'s
/// own strongest single test, the same role the gzip pair plays for compression: proves
/// `otlp_out`'s client TLS and `otlp_in`'s server TLS actually interoperate, not just that each
/// side's own unit tests pass against a hand-built canned peer in isolation.
mod tls {
    use super::*;

    fn testdata_dir() -> std::path::PathBuf {
        // `logit-cli` lives at `crates/logit-cli`; the fixtures live at the repo root's
        // `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`.
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
