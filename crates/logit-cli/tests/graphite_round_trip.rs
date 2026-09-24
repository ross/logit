//! `graphite_in -> graphite_out` round trip over real TCP and UDP sockets. The components run
//! in-process (a `GraphiteOutput` sending to a bound, live `GraphiteInput`), not through
//! `logit run`. This lives in `logit-cli` because it already depends on both `logit-inputs` and
//! `logit-outputs`; a dev-dependency between those two crates would be a cycle.
//!
//! Each byte-for-byte case asserts two things: the bytes on the wire equal the case's `.expected`
//! bytes, and the live `graphite_in`'s decode of those bytes equals the original decode as a whole
//! `EventBatch`. That is the fixed point ADR `lossless-transit` requires, modulo the numbered list
//! in `logit_proto::graphite`'s module doc (`crates/logit-proto/src/graphite/mod.rs`, "Permitted
//! normalizations"), which is the canonical copy. The table below cites that list's numbers.
//!
//! ## Fixture corpus (`tests/fixtures/graphite/`)
//!
//! One file pair per byte-for-byte case: `<name>.in` (the raw wire bytes as a carbon sender puts
//! them on the wire) and `<name>.expected` (the bytes `graphite_out` must emit for that decode, or
//! the marker [`SAME_AS_INPUT`] when the sink reproduces the input verbatim). Where the `.in`
//! files come from:
//!
//! - The plaintext fixtures are hand-written text, a few minimal lines per case, not recordings.
//!   The recorded carbon captures in `testdata/interop/graphite/` aren't copied here;
//!   `crates/logit-inputs/src/graphite/mod.rs`'s `interop_fixture_*` tests replay them in place.
//! - `pickle-protocol-2`/`pickle-protocol-5` are real CPython `pickle.dumps(...,
//!   protocol=2)`/`protocol=-1` dumps of `[('sys.cpu', (1700000000, 0.5))]`, the same bytes as
//!   `crates/logit-proto/src/graphite/pickle.rs`'s `CPYTHON_PROTOCOL_2`/`CPYTHON_PROTOCOL_5`,
//!   framed with carbon's 4-byte big-endian length prefix.
//! - `sanitizer-path`/`sanitizer-tag` are pinned by [`every_committed_in_file_matches_its_builder`]
//!   against the test-side [`PickleBuilder`], which is independent of
//!   `logit_proto::graphite::pickle`'s writer: a fixture built by the encoder's own code couldn't
//!   show the encoder writes what a real payload decodes to.
//!
//! The `.expected` of both CPython cases and of `plaintext-to-pickle` is the same canonical pickle
//! rendering, whichever dialect decoded the datapoint (normalization 2).
//!
//! ## Per-fixture normalizations
//!
//! | Fixture | Normalizations | What it pins |
//! |---|---|---|
//! | `plain-line` | none ([`SAME_AS_INPUT`]) | the baseline: one untagged gauge line, TCP |
//! | `tagged-line` | **4** | two tags inserted out of order relay in ascending rendered-name order |
//! | `integer-value`/`negative-value`/`exponent-value` | **8** | `42.00`/`-3.140`/`1e5` relay as `42`/`-3.14`/`100000` |
//! | `crlf`/`extra-whitespace` | **9** | `\r\n` trims to `\n`; a run of spaces collapses to one |
//! | `duplicate-tag-key` | **5** | `k=1;k=2` decodes to `k=2` (last wins), and relays that way |
//! | `fractional-timestamp` | **6** | `1700000000.75` keeps its sub-second on decode and leaves as `1700000000` |
//! | `tags-drop` | **11** | a tagged line under `tags: drop` loses its tag segment |
//! | `pickle-protocol-2`/`pickle-protocol-5` | **2** | a CPython dump (protocol 2, and protocol -1's `FRAME`/`SHORT_BINUNICODE`/`MEMOIZE` shape) relays as the canonical pickle rendering |
//! | `plaintext-to-pickle` | **2** | the same datapoint as a plaintext line relays to the same canonical pickle bytes: the model keeps no dialect memory |
//! | `many-lines` | **1** | five lines relay unchanged in one UDP datagram under the default cap |
//! | `repacked-at-512` | **1** | 40 lines in one UDP datagram split into >=2 datagrams under a 512-byte cap; no `.expected`, since the boundaries may move |
//! | `sanitizer-path`/`sanitizer-tag` | **10**, wire bytes only | a pickle path `disk/free` and tag name `bad!key` (unreachable from a plaintext decode) leave as `disk_free`/`bad_key` |
//! | `minus-one-timestamp`/`nan-value`/`bad-line` | **7**/none/none, decode-only | `-1` stamps receipt time; a non-finite value and a malformed line are skipped and counted in the `Registry` |
//!
//! ## Cross-protocol
//!
//! `statsd_in -> graphite_out` pins the encoder's array collapse: a repeated DogStatsD tag key
//! decodes to a `Value::Array`, and `graphite_out` renders only its **last** element, counted
//! `logit.output.tags.normalized{reason="multi_value"}` (the module doc's "Encode: model → wire"
//! table, `Value::Array` row). A `graphite_in` decode never produces an array: normalization 5
//! collapses a repeated key first, so this rule never fires inside the pair.
//! `statsd_in -> aggregate -> graphite_out` pins `multi_value: expand`: a timer's samples flush
//! through `aggregate`'s default sketch into a `MetricKind::Distribution`, which `graphite_out`
//! expands into the sub-paths in `crates/logit-proto/src/graphite/mod.rs`'s "`MultiValue::Expand`
//! sub-paths".

use bytes::Bytes;
use logit_core::{Diagnostics, Event, EventBatch, Registry, Resource, Telemetry, Value};
use logit_inputs::graphite::{GraphiteInput, Transport};
use logit_inputs::statsd::StatsdInput;
use logit_outputs::graphite::GraphiteOutput;
use logit_pipeline::{Delivered, Fanout, Input, Output};
use logit_proto::graphite::{GraphiteEncoder, MultiValue, Protocol, Tags};
use logit_proto::Decoder;
use logit_transforms::Aggregator;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;

/// The `.expected` marker meaning "byte-identical to the `.in` file", shared with the other
/// round-trip corpora.
const SAME_AS_INPUT: &[u8] = b"== SAME AS INPUT ==";

/// How long to wait for a batch, datagram, or connection before declaring a hang.
const TIMEOUT: Duration = Duration::from_secs(5);

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/graphite")
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()))
}

fn read_fixture(name: &str, ext: &str) -> Vec<u8> {
    read(&fixtures_dir().join(format!("{name}.{ext}")))
}

/// The bytes a case's sink output must equal: either the literal `.expected` file, or (when that
/// file is [`SAME_AS_INPUT`]) the case's own `.in` bytes.
fn expected_bytes(name: &str, input: &[u8]) -> Vec<u8> {
    let expected = read_fixture(name, "expected");
    if expected.as_slice() == SAME_AS_INPUT {
        input.to_vec()
    } else {
        expected
    }
}

// -- the pickle wire, written by hand ------------------------------------------------------------
//
// Independent of `logit_proto::graphite::pickle`'s writer (see the module doc). Protocol-2 subset,
// no memo.

const OP_PROTO: u8 = 0x80;
const OP_EMPTY_LIST: u8 = 0x5d;
const OP_MARK: u8 = 0x28;
const OP_BINUNICODE: u8 = 0x58;
const OP_BININT: u8 = 0x4a;
const OP_BINFLOAT: u8 = 0x47;
const OP_TUPLE2: u8 = 0x86;
const OP_APPENDS: u8 = 0x65;
const OP_STOP: u8 = 0x2e;

#[derive(Default)]
struct PickleBuilder {
    payload: Vec<u8>,
}

impl PickleBuilder {
    fn new() -> Self {
        Self { payload: vec![OP_PROTO, 0x02, OP_EMPTY_LIST, OP_MARK] }
    }

    /// One `(path, (timestamp, value))` datapoint. `BININT` limits `timestamp` to `i32`; the
    /// writer's `LONG1` fallback for a post-2038 second is covered in `pickle.rs`'s tests.
    fn datapoint(mut self, path: &str, timestamp: i32, value: f64) -> Self {
        let bytes = path.as_bytes();
        self.payload.push(OP_BINUNICODE);
        self.payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        self.payload.extend_from_slice(bytes);
        self.payload.push(OP_BININT);
        self.payload.extend_from_slice(&timestamp.to_le_bytes());
        self.payload.push(OP_BINFLOAT);
        self.payload.extend_from_slice(&value.to_be_bytes());
        self.payload.push(OP_TUPLE2);
        self.payload.push(OP_TUPLE2);
        self
    }

    /// The finished payload behind a 4-byte big-endian length prefix, as it appears on the wire.
    fn build(mut self) -> Vec<u8> {
        self.payload.push(OP_APPENDS);
        self.payload.push(OP_STOP);
        let mut framed = (self.payload.len() as u32).to_be_bytes().to_vec();
        framed.extend_from_slice(&self.payload);
        framed
    }
}

/// The hand-built pickle `.in` files. The CPython dumps aren't rebuilt here: their point is a real
/// pickler's opcode choices, which this builder can't reproduce.
fn build_pickle_in(name: &str) -> Vec<u8> {
    match name {
        "sanitizer-path" => PickleBuilder::new().datapoint("disk/free", 1_700_000_000, 1.0).build(),
        "sanitizer-tag" => {
            PickleBuilder::new().datapoint("cpu.load;bad!key=v", 1_700_000_000, 2.0).build()
        }
        other => panic!("build_pickle_in: unknown fixture {other:?}"),
    }
}

/// Every hand-built pickle `.in` file must equal what [`build_pickle_in`] produces.
#[test]
fn every_committed_in_file_matches_its_builder() {
    for fixture in ["sanitizer-path", "sanitizer-tag"] {
        let committed = read_fixture(fixture, "in");
        let built = build_pickle_in(fixture);
        assert_eq!(committed, built, "{fixture}.in should equal PickleBuilder's own output");
    }
}

// -- TCP capture: a background accept loop, one captured connection per completed send -----------

/// Spawns a TCP listener that forwards each connection's bytes, read to EOF, down the returned
/// channel. A fresh `GraphiteOutput::tcp` dropped after one `send` yields one entry per call.
async fn spawn_tcp_capture() -> (SocketAddr, mpsc::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding the capture listener");
    let addr = listener.local_addr().expect("capture listener should have a local addr");
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = listener.accept().await else { break };
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let _ = conn.read_to_end(&mut buf).await;
                let _ = tx.send(buf).await;
            });
        }
    });
    (addr, rx)
}

/// Either flavor of raw-byte capture a [`Harness`] listens with, matching whichever transport its
/// `graphite_in`/`graphite_out` pair was built with.
enum Capture {
    Tcp(mpsc::Receiver<Vec<u8>>),
    Udp(UdpSocket),
}

/// A plain capture socket (the raw-bytes half of each assertion) beside a bound, live
/// [`GraphiteInput`] draining into a [`Fanout`] channel (the decoded-`EventBatch` half). The input
/// is bound, then `local_addr()` read, then spawned, so there is no bind-drop race. A harness is
/// fixed to one transport and protocol at construction.
struct Harness {
    transport: Transport,
    capture_addr: SocketAddr,
    capture: Capture,
    input_addr: SocketAddr,
    rx: mpsc::Receiver<Delivered>,
}

impl Harness {
    async fn new(transport: Transport, protocol: Protocol) -> Self {
        Self::build(transport, protocol, Diagnostics::default(), Telemetry::default()).await
    }

    /// A harness whose `graphite_in` carries `diag` and `telemetry`, for the decode-only cases.
    /// Both are needed: `GraphiteInput::with_diagnostics` explains why dropping either one
    /// disconnects a class of failure from the `Registry`.
    async fn with_diagnostics(
        transport: Transport,
        protocol: Protocol,
        diag: Diagnostics,
        telemetry: Telemetry,
    ) -> Self {
        Self::build(transport, protocol, diag, telemetry).await
    }

    async fn build(
        transport: Transport,
        protocol: Protocol,
        diag: Diagnostics,
        telemetry: Telemetry,
    ) -> Self {
        let (capture_addr, capture) = match transport {
            Transport::Tcp => {
                let (addr, rx) = spawn_tcp_capture().await;
                (addr, Capture::Tcp(rx))
            }
            Transport::Udp => {
                let socket =
                    UdpSocket::bind("127.0.0.1:0").await.expect("binding the capture socket");
                let addr = socket.local_addr().expect("capture socket should have a local addr");
                (addr, Capture::Udp(socket))
            }
        };

        let mut input = GraphiteInput::new("127.0.0.1:0", transport, protocol)
            .with_diagnostics(diag)
            .with_telemetry(telemetry);
        input.bind().await.expect("binding the graphite_in listener");
        let input_addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, rx) = mpsc::channel(64);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        Self { transport, capture_addr, capture, input_addr, rx }
    }

    /// Sends `batch` once to the capture socket and once to the live `graphite_in`, each through a
    /// fresh [`GraphiteOutput`] passed through `configure`, and returns every captured message and
    /// the decode.
    async fn round_trip_with(
        &mut self,
        batch: &EventBatch,
        configure: impl Fn(GraphiteOutput) -> GraphiteOutput,
    ) -> (Vec<Vec<u8>>, EventBatch) {
        let captured = self.capture_only(batch, &configure).await;

        let mut to_input = configure(self.output_to(self.input_addr));
        to_input.send(batch).await.expect("send to the live graphite_in");
        (captured, self.drain_decoded().await)
    }

    /// [`Harness::round_trip_with`] with `graphite_out`'s defaults.
    async fn round_trip(&mut self, batch: &EventBatch) -> (Vec<Vec<u8>>, EventBatch) {
        self.round_trip_with(batch, |sink| sink).await
    }

    fn output_to(&self, addr: SocketAddr) -> GraphiteOutput {
        match self.transport {
            Transport::Tcp => GraphiteOutput::tcp(addr.to_string(), TIMEOUT),
            Transport::Udp => GraphiteOutput::udp(addr.to_string()).expect("binding graphite_out"),
        }
    }

    /// Sends `batch` to the capture socket only, returning one `Vec<u8>` per TCP connection or per
    /// UDP datagram.
    async fn capture_only(
        &mut self,
        batch: &EventBatch,
        configure: impl Fn(GraphiteOutput) -> GraphiteOutput,
    ) -> Vec<Vec<u8>> {
        let mut to_capture = configure(self.output_to(self.capture_addr));
        to_capture.send(batch).await.expect("send to the capture socket");
        drop(to_capture); // closes a TCP connection, giving the capture side EOF

        match &mut self.capture {
            Capture::Tcp(rx) => {
                let bytes = tokio::time::timeout(TIMEOUT, rx.recv())
                    .await
                    .expect("the capture listener should receive a connection")
                    .expect("the capture channel should not have closed");
                vec![bytes]
            }
            Capture::Udp(socket) => {
                let mut buf = vec![0u8; 65_536];
                let mut datagrams = Vec::new();
                let mut wait = TIMEOUT;
                while let Ok(result) = tokio::time::timeout(wait, socket.recv_from(&mut buf)).await
                {
                    let (n, _) = result.expect("recv_from should succeed");
                    datagrams.push(buf[..n].to_vec());
                    wait = Duration::from_millis(200);
                }
                assert!(!datagrams.is_empty(), "the capture socket received no datagram at all");
                datagrams
            }
        }
    }

    /// Every batch the live `graphite_in` delivered, merged into one: how a multi-message send is
    /// batched isn't this test's concern.
    async fn drain_decoded(&mut self) -> EventBatch {
        let mut batches = Vec::new();
        let mut wait = TIMEOUT;
        while let Ok(delivered) = tokio::time::timeout(wait, self.rx.recv()).await {
            let delivered = delivered.expect("the Fanout channel should not have closed");
            batches.push(logit_pipeline::unwrap_batch(delivered));
            wait = Duration::from_millis(200);
        }
        assert!(!batches.is_empty(), "graphite_in delivered nothing at all");
        let mut merged = batches.remove(0);
        for batch in batches {
            assert_eq!(batch.resource, merged.resource, "one decoder, one shared resource");
            assert!(batch.scope.is_none(), "graphite carries no scope concept");
            merged.events.extend(batch.events);
        }
        merged
    }

    /// Sends `raw` straight to the live `graphite_in` over TCP, for the decode-only cases: bytes
    /// no encoder would produce.
    async fn send_raw(&self, raw: &[u8]) {
        assert_eq!(self.transport, Transport::Tcp, "send_raw is written for the TCP fixtures only");
        let mut stream =
            TcpStream::connect(self.input_addr).await.expect("connecting to graphite_in");
        stream.write_all(raw).await.expect("writing the raw fixture bytes");
        stream.flush().await.expect("flushing the raw fixture bytes");
    }
}

fn direct_plaintext_batch(raw: &[u8]) -> EventBatch {
    let mut decoder = logit_proto::graphite::GraphiteDecoder::new(Arc::new(Resource::default()));
    decoder.decode(Bytes::copy_from_slice(raw)).expect("fixture should decode")
}

fn direct_pickle_batch(raw: &[u8]) -> EventBatch {
    let mut decoder = logit_proto::graphite::GraphiteDecoder::new(Arc::new(Resource::default()))
        .with_protocol(Protocol::Pickle);
    // Strip the length prefix: framing is `graphite_in`'s job, not the codec's.
    let payload = Bytes::copy_from_slice(&raw[4..]);
    decoder.decode(payload).expect("fixture should decode")
}

/// Whether `registry` recorded a point named `metric` carrying `tag`. Drains, so call once.
fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> bool {
    registry.drain(0).into_iter().any(|event| {
        event.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == metric)
            && event.attributes.get(tag.0).and_then(Value::as_str) == Some(tag.1)
    })
}

/// Polls `registry` every 10ms, accumulating each destructive [`Registry::drain`], until a point
/// named `metric` carrying `tag` appears or [`TIMEOUT`] elapses. The decode-only cases need this:
/// `send_raw` returns before `graphite_in` has decoded anything, and a skipped line delivers no
/// batch to wait on. A fixed `sleep` would race the decode.
async fn wait_for_counted(registry: &Registry, metric: &str, tag: (&str, &str)) {
    let mut accumulated = Vec::new();
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        accumulated.extend(registry.drain(0));
        let found = accumulated.iter().any(|event: &Event| {
            event.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == metric)
                && event.attributes.get(tag.0).and_then(Value::as_str) == Some(tag.1)
        });
        if found {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "timed out after {TIMEOUT:?} waiting for {metric}{{{}=\"{}\"}} to be counted",
                tag.0, tag.1
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// -- byte for byte, TCP plaintext, default configuration ------------------------------------------

/// Asserts one plaintext fixture's TCP wire bytes equal its `.expected` bytes and its live decode
/// equals its direct decode.
async fn assert_byte_for_byte(harness: &mut Harness, fixture: &str) {
    let raw = read_fixture(fixture, "in");
    let batch = direct_plaintext_batch(&raw);
    let expected = expected_bytes(fixture, &raw);
    let (captured, decoded) = harness.round_trip(&batch).await;
    assert_eq!(captured.len(), 1, "{fixture}: one TCP connection, one write");
    assert_eq!(
        captured[0], expected,
        "{fixture}: captured bytes should match .expected (modulo the module doc's permitted \
         normalizations)"
    );
    assert_eq!(
        decoded, batch,
        "{fixture}: decode(sink_output) should equal the original decode, as a whole EventBatch"
    );
}

#[tokio::test]
async fn the_corpus_round_trips_byte_for_byte() {
    let mut harness = Harness::new(Transport::Tcp, Protocol::Plaintext).await;
    for fixture in [
        "plain-line",
        "tagged-line",
        "integer-value",
        "negative-value",
        "exponent-value",
        "crlf",
        "extra-whitespace",
        "duplicate-tag-key",
    ] {
        assert_byte_for_byte(&mut harness, fixture).await;
    }
}

/// Normalization 6: the sub-second survives decode but floors on egress, so the far end's decode
/// equals the `.expected` bytes' decode, not the original.
#[tokio::test]
async fn fractional_timestamp_floors_to_a_whole_second_on_egress() {
    let mut harness = Harness::new(Transport::Tcp, Protocol::Plaintext).await;
    let raw = read_fixture("fractional-timestamp", "in");
    let batch = direct_plaintext_batch(&raw);
    assert_eq!(batch.events[0].timestamp, 1_700_000_000_750_000_000, "the fixture's premise");
    let expected = expected_bytes("fractional-timestamp", &raw);
    let expected_batch = direct_plaintext_batch(&expected);

    let (captured, decoded) = harness.round_trip(&batch).await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0], expected);
    assert_eq!(
        decoded, expected_batch,
        "the floored second, not the original sub-second instant, is what a second hop decodes"
    );
}

// -- tags: drop -------------------------------------------------------------------------------

/// Normalization 11: `tags: drop` removes the tag segment. One-way lossy, so only the wire bytes
/// are asserted.
#[tokio::test]
async fn tags_drop_removes_the_tag_segment_entirely() {
    let mut harness = Harness::new(Transport::Tcp, Protocol::Plaintext).await;
    let raw = read_fixture("tags-drop", "in");
    let batch = direct_plaintext_batch(&raw);
    let expected = expected_bytes("tags-drop", &raw);

    let (captured, _decoded) = harness
        .round_trip_with(&batch, |sink| {
            sink.with_encoder(GraphiteEncoder::new().with_tags(Tags::Drop))
        })
        .await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0], expected);
}

// -- pickle dialect (normalization 2) ----------------------------------------------------------

/// A CPython pickle dump relays as the canonical pickle rendering, with decode equality.
async fn assert_pickle_dialect_case(fixture: &str, decoder: impl Fn(&[u8]) -> EventBatch) {
    let mut harness = Harness::new(Transport::Tcp, Protocol::Pickle).await;
    let raw = read_fixture(fixture, "in");
    let batch = decoder(&raw);
    let expected = read_fixture(fixture, "expected");

    let (captured, decoded) = harness
        .round_trip_with(&batch, |sink| {
            sink.with_encoder(GraphiteEncoder::new().with_protocol(Protocol::Pickle))
        })
        .await;
    assert_eq!(captured.len(), 1, "{fixture}: one TCP connection, one write");
    assert_eq!(captured[0], expected, "{fixture}: canonical pickle rendering");
    assert_eq!(decoded, batch, "{fixture}: decode(sink_output) should equal the original decode");
}

#[tokio::test]
async fn pickle_protocol_2_decodes_and_relays_as_canonical_pickle() {
    assert_pickle_dialect_case("pickle-protocol-2", direct_pickle_batch).await;
}

#[tokio::test]
async fn pickle_protocol_5_decodes_and_relays_as_canonical_pickle() {
    assert_pickle_dialect_case("pickle-protocol-5", direct_pickle_batch).await;
}

/// The same datapoint from a plaintext line, sent with `graphite_out` set to pickle, produces the
/// same canonical bytes as the CPython cases.
#[tokio::test]
async fn plaintext_to_pickle_relays_as_the_same_canonical_pickle() {
    // Capture-only: this harness's `graphite_in` speaks plaintext, and the CPython cases cover
    // the pickle decode.
    let mut harness = Harness::new(Transport::Tcp, Protocol::Plaintext).await;
    let raw = read_fixture("plaintext-to-pickle", "in");
    let batch = direct_plaintext_batch(&raw);
    let expected = read_fixture("plaintext-to-pickle", "expected");

    let captured = harness
        .capture_only(&batch, |sink| {
            sink.with_encoder(GraphiteEncoder::new().with_protocol(Protocol::Pickle))
        })
        .await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0], expected);

    // The pickle bytes decode back to the same datapoint.
    assert_eq!(direct_pickle_batch(&captured[0]), batch);
}

// -- sanitizer wire-bytes-only (normalization 10) ------------------------------------------------

/// Normalization 10: a pickle path or tag byte no plaintext decode can produce (`/` in a path, `!`
/// in a tag name) becomes `_` on egress. One-way lossy, so only the wire bytes are asserted.
async fn assert_sanitizer_case(fixture: &str) {
    // Capture-only: the pickle-decoded batch goes out as plaintext.
    let mut harness = Harness::new(Transport::Tcp, Protocol::Plaintext).await;
    let raw = read_fixture(fixture, "in");
    let batch = direct_pickle_batch(&raw);
    let expected = read_fixture(fixture, "expected");

    let captured = harness.capture_only(&batch, |sink| sink).await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0], expected, "{fixture}");
}

#[tokio::test]
async fn sanitizer_path_substitutes_a_forbidden_path_byte() {
    assert_sanitizer_case("sanitizer-path").await;
}

#[tokio::test]
async fn sanitizer_tag_substitutes_a_forbidden_tag_name_byte() {
    assert_sanitizer_case("sanitizer-tag").await;
}

// -- UDP: re-framing (normalization 1) -----------------------------------------------------------

/// Normalization 1, unforced: five lines fit one datagram under the default `max_packet_bytes`.
#[tokio::test]
async fn many_lines_repack_into_one_udp_datagram() {
    let mut harness = Harness::new(Transport::Udp, Protocol::Plaintext).await;
    let raw = read_fixture("many-lines", "in");
    let batch = direct_plaintext_batch(&raw);
    assert_eq!(batch.events.len(), 5);
    let expected = expected_bytes("many-lines", &raw);

    let (captured, decoded) = harness.round_trip(&batch).await;
    assert_eq!(captured.len(), 1, "five short lines fit one default-capped datagram");
    assert_eq!(captured[0], expected);
    assert_eq!(decoded, batch);
}

/// Normalization 1, forced: 40 lines under a 512-byte cap leave as at least two datagrams, none
/// over the cap, and the batch survives. No `.expected`: the datagram boundaries may move.
#[tokio::test]
async fn forty_lines_in_one_datagram_repack_at_a_lower_cap() {
    let raw = read_fixture("repacked-at-512", "in");
    let batch = direct_plaintext_batch(&raw);
    assert_eq!(batch.events.len(), 40);

    let mut harness = Harness::new(Transport::Udp, Protocol::Plaintext).await;
    let (captured, decoded) =
        harness.round_trip_with(&batch, |sink| sink.with_max_packet_bytes(512)).await;

    assert!(
        captured.len() >= 2,
        "40 lines cannot fit one 512-byte datagram, got {}",
        captured.len()
    );
    for (index, datagram) in captured.iter().enumerate() {
        assert!(
            datagram.len() <= 512,
            "datagram {index} is {} bytes, over the cap",
            datagram.len()
        );
    }
    assert_eq!(decoded, batch, "the whole batch must survive re-packing, however it was cut");
}

// -- decode-only, TCP plaintext -------------------------------------------------------------------

async fn diagnostic_harness() -> (Harness, Arc<Registry>) {
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("in/diag", "graphite_in", "input");
    let diag = Diagnostics::new("graphite_in").with_telemetry(telemetry.clone());
    (
        Harness::with_diagnostics(Transport::Tcp, Protocol::Plaintext, diag, telemetry).await,
        registry,
    )
}

/// Normalization 7: carbon's `-1` timestamp becomes receipt time.
#[tokio::test]
async fn minus_one_timestamp_is_stamped_with_receipt_time() {
    let (mut harness, _registry) = diagnostic_harness().await;
    let before =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            as i64;
    harness.send_raw(&read_fixture("minus-one-timestamp", "in")).await;
    let batch = harness.drain_decoded().await;
    let after =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            as i64;

    assert_eq!(batch.events.len(), 1);
    let timestamp = batch.events[0].timestamp;
    assert!(
        (before..=after).contains(&timestamp),
        "expected a receipt-time stamp in {before}..={after}, got {timestamp}"
    );
}

/// A NaN value has no carbon wire form and is dropped, counted `non_finite_value`.
#[tokio::test]
async fn a_nan_value_is_skipped_and_counted() {
    let (harness, registry) = diagnostic_harness().await;
    harness.send_raw(&read_fixture("nan-value", "in")).await;

    wait_for_counted(&registry, "logit.input.metrics.skipped", ("reason", "non_finite_value"))
        .await;
}

/// A line with fewer than three whitespace-separated fields is skipped, counted `bad_line`.
#[tokio::test]
async fn a_malformed_line_is_skipped_and_counted() {
    let (harness, registry) = diagnostic_harness().await;
    harness.send_raw(&read_fixture("bad-line", "in")).await;

    wait_for_counted(&registry, "logit.input.metrics.skipped", ("reason", "bad_line")).await;
}

// -- cross-protocol ---------------------------------------------------------------------------

/// A live `statsd_in` and a TCP capture for `graphite_out`. There's no `graphite_in`: the
/// cross-protocol cases assert wire bytes and counters only.
struct CrossHarness {
    statsd_addr: SocketAddr,
    statsd_rx: mpsc::Receiver<Delivered>,
    capture_addr: SocketAddr,
    capture_rx: mpsc::Receiver<Vec<u8>>,
}

impl CrossHarness {
    async fn new() -> Self {
        let mut input = StatsdInput::new("127.0.0.1:0");
        input.bind().await.expect("binding the statsd_in listener");
        let statsd_addr = input.local_addr().expect("bind() should leave a real address behind");
        let (tx, statsd_rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        let (capture_addr, capture_rx) = spawn_tcp_capture().await;
        Self { statsd_addr, statsd_rx, capture_addr, capture_rx }
    }

    /// The `EventBatch` a live `statsd_in` decodes from `line`.
    async fn statsd_decode(&mut self, line: &[u8]) -> EventBatch {
        let sender =
            UdpSocket::bind("127.0.0.1:0").await.expect("binding an ephemeral sender socket");
        sender.send_to(line, self.statsd_addr).await.expect("sending the statsd line");
        let delivered = tokio::time::timeout(TIMEOUT, self.statsd_rx.recv())
            .await
            .expect("statsd_in should decode and forward the batch")
            .expect("the Fanout channel should not have closed");
        logit_pipeline::unwrap_batch(delivered)
    }

    /// Sends `batch` through a fresh `graphite_out` at the TCP capture socket and returns the
    /// captured bytes.
    async fn graphite_capture(
        &mut self,
        batch: &EventBatch,
        configure: impl Fn(GraphiteOutput) -> GraphiteOutput,
    ) -> Vec<u8> {
        let mut sink = configure(GraphiteOutput::tcp(self.capture_addr.to_string(), TIMEOUT));
        sink.send(batch).await.expect("send to the capture socket");
        drop(sink);
        tokio::time::timeout(TIMEOUT, self.capture_rx.recv())
            .await
            .expect("the capture listener should receive a connection")
            .expect("the capture channel should not have closed")
    }
}

/// A repeated DogStatsD tag key's `Value::Array` renders as its last element, counted
/// `logit.output.tags.normalized{reason="multi_value"}`.
#[tokio::test]
async fn statsd_in_to_graphite_out_renders_dogstatsd_tags_as_carbon_tags() {
    let mut cross = CrossHarness::new().await;
    let batch = cross.statsd_decode(b"x:1|c|#team:a,team:b").await;
    assert_eq!(
        batch.events[0].attributes.get("team"),
        Some(&Value::Array(vec![Value::from("a"), Value::from("b")]))
    );

    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "graphite_out", "sink");
    let captured =
        cross.graphite_capture(&batch, |sink| sink.with_telemetry(telemetry.clone())).await;
    let text = std::str::from_utf8(&captured).expect("ascii output");
    assert!(text.contains(";team=b"), "the last element wins: {text}");
    assert!(!text.contains("team=a"), "the non-last element must not survive: {text}");
    assert!(
        counted(&registry, "logit.output.tags.normalized", ("reason", "multi_value")),
        "the array collapse must be counted"
    );
}

/// `statsd_in -> aggregate -> graphite_out` with `multi_value: expand`: a timer's `Distribution`
/// expands into the documented sub-paths.
#[tokio::test]
async fn statsd_in_to_aggregate_to_graphite_out_expands_a_timer_into_the_documented_sub_paths() {
    let mut cross = CrossHarness::new().await;
    let raw = b"page.latency:10|ms\npage.latency:20|ms\npage.latency:30|ms";
    let batch = cross.statsd_decode(raw).await;
    assert_eq!(batch.events.len(), 3, "one event per line");

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    let forwarded: Vec<Event> = batch
        .events
        .into_iter()
        .filter_map(|mut event| aggregator.process(&resource, &mut event).then_some(event))
        .collect();
    assert!(forwarded.is_empty(), "every timer sample is absorbed by aggregate's default sketch");

    let mut flushed = aggregator.flush(1_700_000_000_000_000_000);
    assert_eq!(flushed.len(), 1, "one (resource, scope) group");
    let (flush_resource, flush_scope, events) = flushed.remove(0);
    assert!(flush_scope.is_none(), "statsd carries no scope concept");
    let out_batch = EventBatch {
        resource: flush_resource,
        scope: flush_scope,
        events: events.into_iter().map(|(event, _links)| event).collect(),
    };
    assert_eq!(out_batch.events.len(), 1, "one flushed Distribution event");

    let captured = cross
        .graphite_capture(&out_batch, |sink| {
            sink.with_encoder(GraphiteEncoder::new().with_multi_value(MultiValue::Expand))
        })
        .await;
    let text = std::str::from_utf8(&captured).expect("ascii output");
    // Parsed into (path, value) pairs so the values are pinned, not just the paths.
    // `DdSketch::count` and `::sum` are exact, so `.count`/`.sum` are deterministic.
    let lines: Vec<(&str, f64)> = text
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut fields = line.split_whitespace();
            let path = fields.next().unwrap_or_else(|| panic!("no path field in {line:?}"));
            let value: f64 = fields
                .next()
                .unwrap_or_else(|| panic!("no value field in {line:?}"))
                .parse()
                .unwrap_or_else(|e| panic!("{line:?}'s value field should parse: {e}"));
            (path, value)
        })
        .collect();
    let value_of = |suffix: &str| -> f64 {
        let path = format!("page.latency{suffix}");
        lines
            .iter()
            .find(|(p, _)| *p == path)
            .map(|(_, v)| *v)
            .unwrap_or_else(|| panic!("expected a {path} line, got:\n{text}"))
    };

    assert_eq!(value_of(".count"), 3.0, "three timer samples");
    assert_eq!(value_of(".sum"), 60.0, "10 + 20 + 30");

    // The five quantiles carry `DdSketch`'s relative-error bound rather than an exact value, so
    // pin the two properties that bound is guaranteed to hold: non-decreasing, and within the
    // sample range.
    let quantiles: Vec<f64> = [".q0_5", ".q0_75", ".q0_9", ".q0_95", ".q0_99"]
        .iter()
        .map(|suffix| value_of(suffix))
        .collect();
    for window in quantiles.windows(2) {
        assert!(window[0] <= window[1], "quantiles should be non-decreasing, got {quantiles:?}");
    }
    for q in &quantiles {
        assert!(
            (10.0..=30.0).contains(q),
            "quantile {q} outside the sample range, got {quantiles:?}"
        );
    }
}
