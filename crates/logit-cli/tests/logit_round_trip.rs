//! `logit_out -> logit_in` over real sockets (ADR `native-transport-handshake-and-ack`): a
//! [`LogitInput`] bound on an ephemeral port in-process, a [`LogitOutput`] pointed at it, and an
//! exact whole-batch equality check on what leaves the far end's [`Fanout`]. Also covers lz4, TLS
//! and mutual TLS, provenance crossing the wire, and how a refused connect and a wrong CA are
//! classified. Lives in `logit-cli` for the same reason `otlp_round_trip.rs` does.

use bytes::Bytes;
use logit_core::{AttrMap, Event, EventBatch, Resource};
use logit_inputs::logit::{LogitInput, TlsServerSettings};
use logit_inputs::statsd::StatsdDecoder;
use logit_outputs::logit::{LogitOutput, TlsClientSettings};
use logit_pipeline::{classify, Fanout, Fault, Input, Output};
use logit_proto::frame::Compression;
use logit_proto::Decoder;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// An address with nothing listening on it: bind an ephemeral port, read it back, drop the
/// socket. Only [`connect_refused_is_classified_clean_against_a_real_logit_in_torn_down`] uses
/// it; a test that wants a live listener uses [`bound_input`].
async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

/// Builds a `logit_in` on an ephemeral port, applies `configure` (TLS, mostly), and binds it,
/// returning the OS-assigned address and the bound input. Binding before `run` is spawned means a
/// `LogitOutput::send` can't race the bind.
async fn bound_input(configure: impl FnOnce(LogitInput) -> LogitInput) -> (String, LogitInput) {
    let mut input = configure(LogitInput::new("127.0.0.1:0"));
    input.bind().await.expect("binding an ephemeral port should succeed");
    let addr = input.local_addr().expect("bind() leaves a real address behind").to_string();
    (addr, input)
}

fn sample_batch() -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("host", "roundtrip-host");
    let event = Event::log(
        1_234_000,
        attrs,
        logit_core::LogRecord {
            message: logit_core::Value::str("hello, native transport"),
            severity: Some(logit_core::Severity::Info),
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    let mut resource = Resource::default();
    resource.attributes.insert("service.name", "roundtrip-service");
    EventBatch { resource: Arc::new(resource), scope: None, events: vec![event] }
}

/// Runs `input` -- already bound by [`bound_input`] -- in the background, sends `batch` through
/// `output`, and returns every [`EventBatch`] the input's own `Fanout` received.
async fn round_trip(
    mut input: LogitInput,
    mut output: LogitOutput,
    batch: &EventBatch,
) -> Vec<EventBatch> {
    let (tx, mut rx) = mpsc::channel(16);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });

    output.send(batch).await.expect("send should succeed against a live logit_in");

    let mut received = Vec::new();
    while let Ok(Some(delivered)) =
        tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
    {
        received.push(logit_pipeline::unwrap_batch(delivered));
    }
    received
}

fn assert_round_tripped(received: &[EventBatch], batch: &EventBatch) {
    assert_eq!(received.len(), 1, "one send should produce exactly one received batch");
    // The native codec is exact, so one `assert_eq!` on the whole batch covers every field.
    assert_eq!(&received[0], batch, "native round-trip should be exact");
}

/// Like [`round_trip`], but the listener's `Fanout` carries a component id and `output` is primed
/// with `provenance` before sending, so a test can observe the `logit_out -> logit_in` special
/// case in `docs/adr/batch-provenance-on-delivered.md`.
async fn round_trip_with_provenance(
    mut input: LogitInput,
    input_component: &str,
    mut output: LogitOutput,
    provenance: logit_core::Provenance,
    batch: &EventBatch,
) -> Vec<(EventBatch, logit_core::Provenance)> {
    let (tx, mut rx) = mpsc::channel(16);
    let sink = Fanout::new(vec![tx]).with_component(input_component);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });

    output.observe_batch(logit_pipeline::BatchContext {
        trace: logit_pipeline::TraceContext::new_root(),
        provenance,
    });
    output.send(batch).await.expect("send should succeed against a live logit_in");

    let mut received = Vec::new();
    while let Ok(Some(delivered)) =
        tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
    {
        let provenance = delivered.provenance();
        received.push((logit_pipeline::unwrap_batch(delivered), provenance));
    }
    received
}

/// A batch's `origin` (a remote listener) and `previous` (the remote node that fed `logit_out`)
/// cross the wire untouched: `logit_in` overwrites neither, so a split-collection deployment reads
/// as one graph (`docs/adr/batch-provenance-on-delivered.md`).
#[tokio::test]
async fn origin_and_previous_cross_the_wire_untouched_from_a_remote_peer() {
    let (addr, input) = bound_input(|input| input).await;
    let output = LogitOutput::new(addr);

    let sent_provenance = logit_core::Provenance {
        origin: Some(logit_core::interner::intern("remote_listener")),
        previous: Some(logit_core::interner::intern("remote_enrich")),
    };
    let batch = sample_batch();
    let received =
        round_trip_with_provenance(input, "central_logit_in", output, sent_provenance, &batch)
            .await;

    assert_eq!(received.len(), 1);
    let (_batch, provenance) = &received[0];
    assert_eq!(provenance.origin_str(), Some("remote_listener"));
    assert_eq!(
        provenance.previous_str(),
        Some("remote_enrich"),
        "previous should name the remote node that fed logit_out, not logit_in itself"
    );
}

/// The fallback: with no provenance to relay (a v1 peer, or a v2 peer that had none), `logit_in`
/// backfills its own id into both `origin` and `previous`.
#[tokio::test]
async fn a_batch_with_no_provenance_gets_logit_ins_own_id_backfilled() {
    let (addr, input) = bound_input(|input| input).await;
    let output = LogitOutput::new(addr);

    let batch = sample_batch();
    let received = round_trip_with_provenance(
        input,
        "central_logit_in",
        output,
        logit_core::Provenance::default(),
        &batch,
    )
    .await;

    assert_eq!(received.len(), 1);
    let (_batch, provenance) = &received[0];
    assert_eq!(provenance.origin_str(), Some("central_logit_in"));
    assert_eq!(provenance.previous_str(), Some("central_logit_in"));
}

#[tokio::test]
async fn logit_output_to_logit_input_round_trips_a_batch_plaintext() {
    let (addr, input) = bound_input(|input| input).await;
    let output = LogitOutput::new(addr);

    let batch = sample_batch();
    let received = round_trip(input, output, &batch).await;
    assert_round_tripped(&received, &batch);
}

#[tokio::test]
async fn logit_output_to_logit_input_round_trips_a_lz4_compressed_batch() {
    let (addr, input) = bound_input(|input| input).await;
    let output = LogitOutput::new(addr).with_compression(Compression::Lz4);

    let batch = sample_batch();
    let received = round_trip(input, output, &batch).await;
    assert_round_tripped(&received, &batch);
}

/// A batch decoded from a real statsd line, not hand-built `Event`s, crosses a real
/// `logit_out -> logit_in` hop with its timestamp and resource attribute intact. This is the
/// codec/transport path of `fixtures/forwarder-edge.yaml` feeding
/// `fixtures/forwarder-central.yaml`, in-process rather than across two `logit run` processes.
#[tokio::test]
async fn a_statsd_decoded_batch_forwards_through_logit_out_and_logit_in_with_its_timestamp_intact()
{
    let mut resource = Resource::default();
    resource.attributes.insert("host", "edge-host");
    let resource = Arc::new(resource);
    let mut decoder = StatsdDecoder::new(resource.clone());
    let mut events = Vec::new();
    let received_at = 9_999_000_000i64;
    decoder
        .decode_into(Bytes::from_static(b"requests:1|c"), received_at, &mut events)
        .expect("a well-formed statsd line should decode");
    let batch = EventBatch { resource, scope: None, events };
    assert_eq!(batch.events.len(), 1, "one statsd line should decode to one event");

    let (addr, input) = bound_input(|input| input).await;
    let output = LogitOutput::new(addr);

    let received = round_trip(input, output, &batch).await;
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].events[0].timestamp, received_at,
        "the statsd decode's own received_at, already baked into the event's timestamp before \
         it ever reaches logit_out, should survive the native hop unchanged"
    );
    assert_eq!(
        received[0].resource.attributes.get("host").and_then(|v| v.as_str()),
        Some("edge-host")
    );
}

#[tokio::test]
async fn connect_refused_is_classified_clean_against_a_real_logit_in_torn_down() {
    let addr = ephemeral_addr().await; // nothing is listening here
    let mut output = LogitOutput::new(addr).with_timeout(Duration::from_millis(300));
    let err = output.send(&sample_batch()).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Clean);
}

mod tls {
    use super::*;

    fn testdata_dir() -> std::path::PathBuf {
        // The certs are the repo root's `testdata/tls`, two levels above `CARGO_MANIFEST_DIR`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    #[tokio::test]
    async fn logit_output_to_logit_input_round_trips_a_batch_over_server_tls() {
        let (addr, input) = bound_input(|input| {
            input
                .with_tls(
                    &TlsServerSettings {
                        cert_file: "server.pem".to_string(),
                        key_file: "server.key".to_string(),
                        client_ca_file: None,
                    },
                    &testdata_dir(),
                )
                .unwrap()
        })
        .await;
        let output = LogitOutput::new(addr)
            .with_tls(
                &TlsClientSettings { ca_file: Some("ca.pem".to_string()), ..Default::default() },
                &testdata_dir(),
            )
            .unwrap();

        let batch = sample_batch();
        let received = round_trip(input, output, &batch).await;
        assert_round_tripped(&received, &batch);
    }

    /// Mutual TLS: `logit_in` requires a client certificate chaining to `ca.pem`, `logit_out`
    /// presents `client.pem`/`client.key` -- both signed by the same test CA
    /// (`testdata/tls/regen.sh`).
    #[tokio::test]
    async fn logit_output_to_logit_input_round_trips_a_batch_over_mutual_tls() {
        let (addr, input) = bound_input(|input| {
            input
                .with_tls(
                    &TlsServerSettings {
                        cert_file: "server.pem".to_string(),
                        key_file: "server.key".to_string(),
                        client_ca_file: Some("ca.pem".to_string()),
                    },
                    &testdata_dir(),
                )
                .unwrap()
        })
        .await;
        let output = LogitOutput::new(addr)
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

        let batch = sample_batch();
        let received = round_trip(input, output, &batch).await;
        assert_round_tripped(&received, &batch);
    }

    /// A client trusting a CA that never signed the server's certificate is refused at the TLS
    /// handshake, before `Hello` is sent: `Fault::Clean`, since nothing of the batch left the sink.
    #[tokio::test]
    async fn a_client_trusting_the_wrong_ca_is_refused_and_classified_clean() {
        let (addr, mut input) = bound_input(|input| {
            input
                .with_tls(
                    &TlsServerSettings {
                        cert_file: "server.pem".to_string(),
                        key_file: "server.key".to_string(),
                        client_ca_file: None,
                    },
                    &testdata_dir(),
                )
                .unwrap()
        })
        .await;
        let (tx, _rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        let mut output = LogitOutput::new(addr)
            .with_timeout(Duration::from_millis(500))
            .with_tls(
                &TlsClientSettings {
                    ca_file: Some("other-ca.pem".to_string()),
                    ..Default::default()
                },
                &testdata_dir(),
            )
            .unwrap();

        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean);
    }
}
