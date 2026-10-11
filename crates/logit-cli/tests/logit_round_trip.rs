//! `logit_out -> logit_in` over real sockets (ADR `native-transport-handshake-and-ack`): a
//! [`LogitInput`] bound on an ephemeral port in-process, a [`LogitOutput`] pointed at it, and an
//! exact whole-batch equality check on what leaves the far end's [`Fanout`]. Also covers lz4, TLS
//! and mutual TLS, provenance crossing the wire, and how a refused connect and a wrong CA are
//! classified. Lives in `logit-cli` for the same reason `otlp_round_trip.rs` does.

use bytes::Bytes;
use logit_core::{AttrMap, Event, EventBatch, Resource};
use logit_inputs::logit::{LogitInput, TlsServerSettings};
use logit_outputs::logit::{LogitOutput, TlsClientSettings};
use logit_pipeline::{classify, Fanout, Fault, Input, Output};
use logit_proto::frame::Compression;
use logit_proto::statsd::StatsdDecoder;
use logit_proto::Decoder;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// The one batch each test sends, as its sink's store would number it.
const ONE: logit_pipeline::SeqId = logit_pipeline::SeqId { id: *b"logit-round-trip", seq: 1 };

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

    output.observe_batch(logit_pipeline::BatchContext::default(), ONE);
    output.send(batch).await.expect("send should succeed against a live logit_in");

    // `logit_in` writes its `Ack` after `Fanout::send` returns, and `send` returns on that `Ack`,
    // so every batch the send delivered is already queued.
    let mut received = Vec::new();
    while let Ok(delivered) = rx.try_recv() {
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

    output.observe_batch(
        logit_pipeline::BatchContext {
            trace: logit_pipeline::TraceContext::new_root(),
            provenance,
        },
        ONE,
    );
    output.send(batch).await.expect("send should succeed against a live logit_in");

    // Every delivered batch is already queued, as in [`round_trip`].
    let mut received = Vec::new();
    while let Ok(delivered) = rx.try_recv() {
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

/// The fallback: with no provenance to relay (a peer whose batch had none), `logit_in`
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
    // Port 1: nothing listens there, so the connect is refused. A port bound and then released
    // could be taken by another test before the connect.
    let mut output =
        LogitOutput::new("127.0.0.1:1".to_string()).with_timeout(Duration::from_millis(300));
    output.observe_batch(logit_pipeline::BatchContext::default(), ONE);
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
                    &logit_pipeline::tls::TlsReloader::new(),
                )
                .unwrap()
        })
        .await;
        let output = LogitOutput::new(addr)
            .with_tls(
                &TlsClientSettings { ca_file: Some("ca.pem".to_string()), ..Default::default() },
                &testdata_dir(),
                &logit_pipeline::tls::TlsReloader::new(),
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
                    &logit_pipeline::tls::TlsReloader::new(),
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
                &logit_pipeline::tls::TlsReloader::new(),
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
                    &logit_pipeline::tls::TlsReloader::new(),
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
                &logit_pipeline::tls::TlsReloader::new(),
            )
            .unwrap();

        output.observe_batch(logit_pipeline::BatchContext::default(), ONE);
        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean);
    }
}

/// Through the node runtime with a send window (ADR `native-hop-send-window`): a `logit_out`
/// with `window: 32` over a disk spool, driven by `write_loop`, in front of a real `logit_in`.
mod window {
    use super::*;
    use logit_config::{BufferConfig, Component, ComponentKind, Config, ReceiveConfig};
    use logit_pipeline::test_util::{scratch_dir, wait_until_within, TelemetryProbe, RECV_TIMEOUT};
    use logit_pipeline::{
        graph, DiskQueueConfig, InputRuntimeConfig, NodeSpec, OverflowPolicy, SinkStoreConfig,
        WriteLoopConfig,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::watch;

    const BATCHES: usize = 200;

    /// [`sample_batch`] with `mark` as its event's timestamp.
    fn marked(mark: usize) -> EventBatch {
        let mut batch = sample_batch();
        batch.events[0].timestamp = mark as i64;
        batch
    }

    /// Sends each batch once, then hangs until the runtime shuts it down.
    struct BurstInput(Vec<EventBatch>);

    #[async_trait::async_trait]
    impl Input for BurstInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            for batch in self.0.drain(..) {
                sink.send(batch).await;
            }
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }
    }

    /// `in -> out`, where `out` is `output` over a disk spool in `dir`. The `in` kind only names
    /// a listener for graph resolution; its spec is a [`BurstInput`] of [`BATCHES`] marked
    /// batches.
    fn specs(
        output: LogitOutput,
        dir: std::path::PathBuf,
    ) -> (graph::Graph, HashMap<String, NodeSpec>) {
        specs_of(output, dir, (0..BATCHES).map(marked).collect())
    }

    /// [`specs`] with a [`BurstInput`] of `batches`.
    fn specs_of(
        output: LogitOutput,
        dir: std::path::PathBuf,
        batches: Vec<EventBatch>,
    ) -> (graph::Graph, HashMap<String, NodeSpec>) {
        let component = |sources: Vec<String>, kind| Component {
            buffer: BufferConfig::default(),
            receive: ReceiveConfig::default(),
            sources,
            targets: Vec::new(),
            kind,
        };
        let components = HashMap::from([
            (
                "in".to_string(),
                component(
                    vec![],
                    ComponentKind::StatsdIn {
                        bind: "127.0.0.1:0".to_string(),
                        transport: logit_config::StatsdTransport::default(),
                        tls: None,
                        handshake_timeout: logit_config::default_handshake_timeout(),
                        idle_timeout: None,
                        max_connections: logit_config::default_max_connections(),
                        peer: false,
                        reuse_port: false,
                        proxy_protocol: false,
                        socket_mode: None,
                    },
                ),
            ),
            (
                "out".to_string(),
                component(
                    vec!["in".to_string()],
                    ComponentKind::LogitOut {
                        endpoint: "127.0.0.1:1".to_string(),
                        compression: logit_config::Compression::None,
                        tls: None,
                        request_timeout: Duration::from_secs(10),
                        window: 32,
                    },
                ),
            ),
        ]);
        let graph = graph::resolve(Config { components, ..Default::default() })
            .expect("the topology resolves");
        let disk = DiskQueueConfig {
            dir,
            max_bytes: 64 * 1024 * 1024,
            segment_bytes: 64 * 1024,
            overflow: OverflowPolicy::Block,
            compression: Compression::None,
            checkpoint_interval: Duration::from_millis(20),
        };
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(BurstInput(batches)), InputRuntimeConfig::default()),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(output),
                SinkStoreConfig::Disk(disk),
                WriteLoopConfig {
                    retry: logit_pipeline::RetryConfig {
                        base_delay: Duration::from_millis(5),
                        max_delay: Duration::from_millis(50),
                    },
                    ..WriteLoopConfig::default()
                },
            ),
        );
        (graph, specs)
    }

    /// A running `logit_in` counting into `probe`, and every mark its consumer receives, in order.
    async fn spawn_listener(probe: &TelemetryProbe) -> (String, Arc<Mutex<Vec<i64>>>) {
        spawn_listener_capped(probe, logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN).await
    }

    /// [`spawn_listener`] under `max_frame_bytes`.
    async fn spawn_listener_capped(
        probe: &TelemetryProbe,
        max_frame_bytes: u32,
    ) -> (String, Arc<Mutex<Vec<i64>>>) {
        let (addr, mut input) = bound_input(|input| {
            input
                .with_telemetry(probe.telemetry("logit_in", "logit_in", "listener"))
                .with_max_frame_bytes(max_frame_bytes)
        })
        .await;
        let (tx, mut rx) = mpsc::channel(16);
        tokio::spawn(async move { input.run(Fanout::new(vec![tx])).await });
        let marks = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::clone(&marks);
        tokio::spawn(async move {
            while let Some(delivered) = rx.recv().await {
                let batch = logit_pipeline::unwrap_batch(delivered);
                received.lock().unwrap().push(batch.events[0].timestamp);
            }
        });
        (addr, marks)
    }

    /// Runs the graph until `marks` holds every batch, then shuts it down.
    async fn run_until_delivered(
        graph: graph::Graph,
        specs: HashMap<String, NodeSpec>,
        marks: &Arc<Mutex<Vec<i64>>>,
        probe: &mut TelemetryProbe,
    ) {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let run = tokio::spawn(logit_pipeline::run_with_shutdown(graph, specs, async move {
            let _ = shutdown_rx.wait_for(|&fired| fired).await;
        }));
        // Spooling and sending 200 batches through a disk queue that fsyncs on rotation.
        wait_until_within(
            "every batch to reach logit_in's consumer",
            Duration::from_secs(30),
            || marks.lock().unwrap().len() >= BATCHES,
        )
        .await;
        // Read while the pooled connection is live: its drop at shutdown resets the gauge.
        assert_eq!(probe.poll().gauge("logit.output.window", &[]), Some(32.0));
        let _ = shutdown_tx.send(true);
        tokio::time::timeout(RECV_TIMEOUT, run)
            .await
            .expect("run finishes once shutdown fires")
            .unwrap()
            .expect("run completes without error");
    }

    #[tokio::test]
    async fn a_window_of_32_through_the_runtime_forwards_every_spooled_batch_once_in_order() {
        let mut probe = TelemetryProbe::new();
        let (addr, marks) = spawn_listener(&probe).await;
        let output = LogitOutput::new(addr).with_window(32).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));
        let dir = scratch_dir("logit-round-trip-window");
        let (graph, specs) = specs(output, dir.clone());

        run_until_delivered(graph, specs, &marks, &mut probe).await;

        let expected: Vec<i64> = (0..BATCHES as i64).collect();
        assert_eq!(*marks.lock().unwrap(), expected, "each batch once, in order");
        let totals = probe.poll();
        assert_eq!(totals.gauge("logit.output.window", &[]), Some(1.0), "the flush dropped it");
        assert!(!totals.has("logit.input.batches.resends", &[]));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// One batch among [`BATCHES`] that `logit_in` refuses by name (past its decode budget) is
    /// dropped alone through the runtime: counted `rejected` at both ends, its spooled record
    /// committed, and every other batch delivered once, in order, on one connection
    /// (`docs/adr/native-hop-ack-status.md`).
    #[tokio::test]
    async fn a_batch_logit_in_rejects_is_dropped_alone_and_the_window_goes_on() {
        const REFUSED: usize = 57;
        let mut probe = TelemetryProbe::new();
        // A 1 KiB cap's decode budget is 4 KiB: five empty events exceed it in a few dozen bytes.
        let (addr, marks) = spawn_listener_capped(&probe, 1024).await;
        let output = LogitOutput::new(addr).with_window(32).with_telemetry(probe.telemetry(
            "out",
            "logit_out",
            "sink",
        ));
        let batches = (0..BATCHES)
            .map(|mark| match mark {
                REFUSED => {
                    let events =
                        (0..5).map(|_| Event::empty(mark as i64, AttrMap::new())).collect();
                    EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
                }
                _ => marked(mark),
            })
            .collect();
        let dir = scratch_dir("logit-round-trip-window-rejected");
        let (graph, specs) = specs_of(output, dir.clone(), batches);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let run = tokio::spawn(logit_pipeline::run_with_telemetry(
            graph,
            specs,
            HashMap::from([("out".to_string(), probe.telemetry("out", "logit_out", "sink"))]),
            logit_pipeline::Readiness::disabled(),
            async move {
                let _ = shutdown_rx.wait_for(|&fired| fired).await;
            },
        ));
        wait_until_within(
            "every other batch to reach logit_in's consumer",
            Duration::from_secs(30),
            || marks.lock().unwrap().len() >= BATCHES - 1,
        )
        .await;
        probe
            .wait_for("the refused batch committed as dropped", |t| {
                t.sum("logit.component.batches.dropped", &[("reason", "rejected")]) >= 1.0
            })
            .await;
        let _ = shutdown_tx.send(true);
        tokio::time::timeout(RECV_TIMEOUT, run)
            .await
            .expect("run finishes once shutdown fires")
            .unwrap()
            .expect("run completes without error");

        let expected: Vec<i64> =
            (0..BATCHES as i64).filter(|&mark| mark != REFUSED as i64).collect();
        assert_eq!(*marks.lock().unwrap(), expected, "each other batch once, in order");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]), 1.0);
        assert_eq!(totals.sum("logit.input.batches.dropped", &[("reason", "rejected")]), 1.0);
        assert_eq!(totals.sum("logit.output.requests", &[("class", "rejected")]), 1.0);
        assert_eq!(totals.sum("logit.output.requests", &[("class", "ok")]), (BATCHES - 1) as f64);
        assert_eq!(totals.sum("logit.output.reconnects", &[]), 0.0, "one connection throughout");
        assert!(!totals.has("logit.input.batches.resends", &[]));
        assert_eq!(totals.sum("logit.component.buffer.disk.replayed", &[]), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A relay in front of `logit_in` that can hold new connections short of `logit_in`, hold the
    /// acks coming back, and cut every connection open at once. Holding the acks leaves a whole
    /// window forwarded and unacknowledged; the cut then makes the sender take it to a new
    /// connection.
    struct Relay {
        addr: String,
        hold_connections: watch::Sender<bool>,
        acks: Arc<watch::Sender<AckGate>>,
        cut: watch::Sender<u64>,
    }

    /// The relay's ack direction, shared by every connection. An `Ack` passes only while
    /// `forwarded` is below `pass_until`, and `forwarded` is raised under the same lock before
    /// its bytes are written, so once `forwarded` reaches `pass_until` it is frozen and no ack
    /// past it can reach the sender.
    struct AckGate {
        pass_until: u64,
        /// The highest `Ack.seq` let through. Seqs count from 1 in push order, so it is also how
        /// many batches the sender can have seen acknowledged.
        forwarded: u64,
    }

    impl AckGate {
        fn holding(&self) -> bool {
            self.forwarded >= self.pass_until
        }
    }

    async fn spawn_relay(upstream: String) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (hold_connections, connections_rx) = watch::channel(false);
        let acks = Arc::new(watch::Sender::new(AckGate { pass_until: u64::MAX, forwarded: 0 }));
        let (cut, cut_rx) = watch::channel(0u64);
        let gate = Arc::clone(&acks);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(relay_connection(
                    client,
                    upstream.clone(),
                    cut_rx.clone(),
                    connections_rx.clone(),
                    Arc::clone(&gate),
                ));
            }
        });
        Relay { addr, hold_connections, acks, cut }
    }

    /// Reads one control frame off `stream`: its bytes, and its seq when it is an `Ack`.
    async fn read_control_frame(
        stream: &mut tokio::net::tcp::OwnedReadHalf,
    ) -> std::io::Result<(Vec<u8>, Option<u64>)> {
        use logit_proto::frame;
        use logit_proto::native::control::ControlMessage;
        let mut raw = vec![0u8; frame::HEADER_LEN];
        stream.read_exact(&mut raw).await?;
        let header = frame::FrameHeader::read(&mut Bytes::copy_from_slice(&raw))
            .expect("logit_in writes a valid control frame header");
        raw.resize(frame::HEADER_LEN + header.compressed_len as usize, 0);
        stream.read_exact(&mut raw[frame::HEADER_LEN..]).await?;
        let (_, mut payload) = frame::read_frame_with_header(&mut Bytes::from(raw.clone()))
            .expect("logit_in writes a valid control frame");
        let seq = match ControlMessage::decode(&mut payload) {
            Ok(ControlMessage::Ack(ack)) => Some(ack.seq),
            _ => None,
        };
        Ok((raw, seq))
    }

    /// Relays one connection both ways until either side closes or a cut is called. The
    /// `logit_in` side is relayed a control frame at a time, so `acks` gates whole `Ack`s.
    async fn relay_connection(
        client: TcpStream,
        upstream: String,
        mut cut: watch::Receiver<u64>,
        mut hold_connections: watch::Receiver<bool>,
        gate: Arc<watch::Sender<AckGate>>,
    ) {
        let epoch = *cut.borrow_and_update();
        let _ = hold_connections.wait_for(|&held| !held).await;
        let Ok(server) = TcpStream::connect(&upstream).await else { return };
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let frames = tokio::io::copy(&mut client_read, &mut server_write);
        let acks = async {
            let mut released = gate.subscribe();
            loop {
                let Ok((raw, seq)) = read_control_frame(&mut server_read).await else { return };
                if let Some(seq) = seq {
                    loop {
                        let mut passed = false;
                        gate.send_if_modified(|gate| {
                            passed = !gate.holding();
                            if passed {
                                gate.forwarded = gate.forwarded.max(seq);
                            }
                            passed
                        });
                        if passed {
                            break;
                        }
                        let _ = released.wait_for(|gate| !gate.holding()).await;
                    }
                }
                if client_write.write_all(&raw).await.is_err() {
                    return;
                }
            }
        };
        tokio::select! {
            biased;
            _ = cut.wait_for(|&now| now != epoch) => {}
            _ = frames => {}
            () = acks => {}
        }
    }

    /// The connection is cut with a full window forwarded by `logit_in` and unacknowledged at the
    /// sender. The new connection's `Hello` lists the window's identity, `HelloAck` answers its
    /// mark, and every frame of the window commits from it with nothing resent
    /// (`docs/adr/native-hop-named-acks.md`, decision 4).
    #[tokio::test]
    async fn a_connection_cut_mid_window_resumes_the_window_without_resending_it() {
        let mut probe = TelemetryProbe::new();
        let (listener_addr, marks) = spawn_listener(&probe).await;
        let relay = spawn_relay(listener_addr).await;
        let output = LogitOutput::new(relay.addr.clone())
            .with_window(32)
            .with_telemetry(probe.telemetry("out", "logit_out", "sink"));
        let dir = scratch_dir("logit-round-trip-window-cut");
        let (graph, specs) = specs(output, dir.clone());
        // The window fill reads the spool without waiting for a push still on a blocking thread,
        // and refills only when the head is acknowledged. A fill that caught up to the pushes
        // just before the acks are held would leave the window short for good, so the sender
        // gets no connection until the spool holds the burst.
        relay.hold_connections.send_replace(true);
        // Acks stop at the first one covering batch 50, and only the relay can say where. The
        // sender's own counts lag the acks it was sent: one `Ack` can sit unread in its socket,
        // and one cumulative `Ack` marks many frames acked that it then pops one per refill, so
        // its `in_flight` counts acked frames while it drains. A cut in either state lands on
        // refills that are resent rather than resumed.
        relay.acks.send_modify(|gate| gate.pass_until = 50);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let run = tokio::spawn(logit_pipeline::run_with_telemetry(
            graph,
            specs,
            HashMap::from([("out".to_string(), probe.telemetry("out", "logit_out", "sink"))]),
            logit_pipeline::Readiness::disabled(),
            async move {
                let _ = shutdown_rx.wait_for(|&fired| fired).await;
            },
        ));

        // `drain_inbox` counts a batch received before pushing it, so every push but the last
        // has landed.
        wait_until_within("every batch received by out", Duration::from_secs(30), || {
            probe.poll().sum("logit.component.batches.received", &[]) >= BATCHES as f64
        })
        .await;
        relay.hold_connections.send_replace(false);

        // With acks frozen at `forwarded`, the sender can have sent at most `forwarded + 32`
        // batches, so `logit_in` forwarding that many means the sender sent them all and can send
        // nothing more: a full window, forwarded and unacked. Before the ack that froze the gate,
        // at most 49 + 32 batches were sent, so it covers at most 81 and the window ends well
        // inside the 200.
        wait_until_within("a full window forwarded and unacked", Duration::from_secs(30), || {
            let gate = relay.acks.borrow();
            gate.holding() && marks.lock().unwrap().len() == gate.forwarded as usize + 32
        })
        .await;
        relay.cut.send_modify(|epoch| *epoch += 1);
        relay.acks.send_modify(|gate| gate.pass_until = u64::MAX);

        wait_until_within(
            "every batch to reach logit_in's consumer",
            Duration::from_secs(30),
            || marks.lock().unwrap().len() >= BATCHES,
        )
        .await;
        let _ = shutdown_tx.send(true);
        tokio::time::timeout(RECV_TIMEOUT, run)
            .await
            .expect("run finishes once shutdown fires")
            .unwrap()
            .expect("run completes without error");

        let expected: Vec<i64> = (0..BATCHES as i64).collect();
        assert_eq!(*marks.lock().unwrap(), expected, "each batch once, in order");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.batches.resumed", &[]), 32.0, "the cut window");
        assert_eq!(totals.sum("logit.input.batches.resends", &[]), 0.0, "nothing resent");
        assert!(totals.sum("logit.output.reconnects", &[]) >= 1.0);
        std::fs::remove_dir_all(&dir).ok();
    }
}
