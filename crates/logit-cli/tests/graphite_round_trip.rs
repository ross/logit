//! `graphite_in -> graphite_out` round trip, over real TCP and UDP sockets -- the graphite
//! counterpart to `collectd_round_trip.rs`/`statsd_round_trip.rs`. Lives here (not a
//! dev-dependency cycle between `logit-inputs`/`logit-outputs`) for the same reason those do:
//! `logit-cli` already depends on both as ordinary dependencies.
//!
//! `docs/plans/graphite-carbon-relay.md`'s W4b workstream, "Round trip + closeout" row.
//!
//! ## Fixture corpus (`tests/fixtures/graphite/`)
//!
//! One file pair per byte-for-byte case: `<name>.in` (the raw wire bytes, exactly as a real carbon
//! sender would put them on the wire) and `<name>.expected` (the exact bytes `graphite_out` must
//! emit for that input's decode, or the literal marker [`SAME_AS_INPUT`] when the sink's own
//! canonicalization happens to reproduce the input verbatim) -- the same convention
//! `collectd_round_trip.rs`'s/`statsd_round_trip.rs`'s corpora use. Plaintext fixtures are
//! human-readable text; the four pickle fixtures (`pickle-protocol-2`, `pickle-protocol-5`,
//! `sanitizer-path`, `sanitizer-tag`) are binary and are pinned against a test-side
//! [`PickleBuilder`] -- deliberately independent of `logit_proto::graphite::pickle`'s own writer,
//! by [`every_committed_in_file_matches_its_builder`] -- for the same reason
//! `collectd_round_trip.rs`'s `PacketBuilder` is independent of that codec's own writers: a
//! fixture built by the same code the encoder uses could not prove the encoder writes what a real
//! sender's bytes decode to.
//!
//! `pickle-protocol-2`/`pickle-protocol-5`'s `.in` files are real CPython `pickle.dumps(...,
//! protocol=2)`/`protocol=-1` dumps of `[('sys.cpu', (1700000000, 0.5))]` -- the identical byte
//! literals `crates/logit-proto/src/graphite/pickle.rs`'s own `CPYTHON_PROTOCOL_2`/
//! `CPYTHON_PROTOCOL_5` constants carry, framed here with the 4-byte big-endian length prefix a
//! real wire capture would show. Their `.expected` (and `plaintext-to-pickle`'s) is the *one*
//! canonical pickle rendering our own writer produces for that datapoint -- byte-identical across
//! all three regardless of which dialect decoded it, since the model retains no provenance of
//! which wire shape a datapoint arrived in (normalization 2).
//!
//! ## Permitted normalizations
//!
//! Transcribed from `logit_proto::graphite`'s module doc, which is the codec's own spec (ADR
//! [`graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md) and
//! `docs/plans/graphite-carbon-relay.md` carry the identical numbered list):
//!
//! 1. Re-framing: lines are repacked into datagrams or stream writes, and datapoints into pickle
//!    frames of at most `max_frame_bytes`, so one input frame may leave as several and vice versa.
//! 2. An operator-chosen dialect change: plaintext <-> pickle, in either direction.
//! 3. Datapoint reordering within a batch.
//! 4. Tag order is canonicalized to ascending rendered name.
//! 5. A repeated tag key collapses to its **last** occurrence at decode, counted.
//! 6. Timestamps floor to whole seconds on egress (`div_euclid`).
//! 7. A `-1` timestamp becomes receipt time on ingress, and leaves as that absolute second.
//! 8. Number formatting becomes the shortest round-trip `f64` rendering.
//! 9. Field separators collapse to a single space, `\r\n` to `\n`; a TCP write terminates every
//!    line including the last, a UDP datagram terminates none.
//! 10. Sanitizer substitutions, empty-tag drops and rendered-name collision drops (all counted).
//! 11. `tags: drop` drops the tag set entirely (counted).
//! 12. A `Sum`'s temporality and monotonicity are dropped -- the value goes on the wire bare.
//!
//! ## Per-fixture normalizations
//!
//! | Fixture | Normalizations | What it pins |
//! |---|---|---|
//! | `plain-line` | none ([`SAME_AS_INPUT`]) | the baseline: one untagged gauge line, TCP |
//! | `tagged-line` | **4** | two tags inserted out of order relay in ascending rendered-name order |
//! | `integer-value`/`negative-value`/`exponent-value` | **8** | `42.00`/`-3.140`/`1e5` relay as `42`/`-3.14`/`100000` -- shortest round-trip `f64::Display` |
//! | `crlf`/`extra-whitespace` | **9** | `\r\n` trims to `\n`; a run of spaces collapses to one |
//! | `duplicate-tag-key` | **5** | `k=1;k=2` decodes to `k=2` (last wins), and relays that way |
//! | `fractional-timestamp` | **6** | `1700000000.75` decodes keeping the sub-second, floors back to `1700000000` on egress |
//! | `tags-drop` | **11** | the same tagged line under `tags: drop` loses its tag segment entirely |
//! | `pickle-protocol-2`/`pickle-protocol-5` | **2** | a real CPython pickle dump (protocol 2, and protocol -1's `FRAME`/`SHORT_BINUNICODE`/`MEMOIZE` shape) decodes and relays as our own canonical pickle rendering |
//! | `plaintext-to-pickle` | **2** | the identical datapoint, sent as a plaintext line, relays to the *same* canonical pickle bytes as the two cases above -- the model carries no dialect memory |
//! | `many-lines` | **1** | five single-datapoint UDP messages (`Meta = 1` each) repack into one UDP datagram, unchanged since they already fit under the default cap |
//! | `repacked-at-512` | **1** | 40 lines in one UDP datagram split into >=2 datagrams under a 512-byte cap, no `.expected` -- datagram boundaries are exactly what this case lets move |
//! | `sanitizer-path`/`sanitizer-tag` | **10**, wire-bytes-only | a pickle path of `disk/free` and a pickle tag name of `bad!key` (bytes no plaintext line's decode could ever carry) sanitize to `disk_free`/`bad_key` on egress -- one-way lossy, so no decoded-equality claim is made |
//! | `minus-one-timestamp`/`nan-value`/`bad-line` | **7**/none/none, decode-only | `-1` stamps receipt time; a non-finite value and a malformed line are both skipped and counted, asserted against the delivered events and the `Registry`'s throttled-diagnostic counters, never against a `.expected` |
//!
//! ## Cross-protocol
//!
//! `statsd_in -> graphite_out` exercises the encoder's tag-folding path: a repeated DogStatsD tag
//! key decodes to a `Value::Array` (`statsd_in`'s own `insert_tags`), and `graphite_out` renders
//! only its **last** element as the carbon tag, counted
//! `logit.output.tags.normalized{reason="multi_value"}` (normalization from the codec's own
//! encode table, the same array-collapsing rule `influxdb_out`/`statsd_out` apply, distinct from
//! this pair's own decode-side normalization 5 -- decision 9 in the plan is what keeps the two
//! from ever colliding *inside* the pair itself). `statsd_in -> aggregate -> graphite_out` pins
//! `multi_value: expand`: a DogStatsD timer's raw samples flush through `aggregate`'s default
//! sketch accumulator into a `MetricKind::Distribution`, which `graphite_out` expands into the
//! `.count`/`.sum`/`.q0_5`/`.q0_75`/`.q0_9`/`.q0_95`/`.q0_99` sub-paths
//! `logit_proto::graphite::encode`'s module doc documents.

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

/// The `.expected` marker meaning "byte-identical to the `.in` file" -- see this file's module
/// doc, mirroring `collectd_round_trip.rs`'s/`statsd_round_trip.rs`'s identical constant.
const SAME_AS_INPUT: &[u8] = b"== SAME AS INPUT ==";

/// How long to wait for a batch/datagram/connection event before declaring a hang -- the same
/// budget `collectd_round_trip.rs`/`crates/logit-inputs/src/graphite/mod.rs`'s own socket tests
/// use.
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
/// file is exactly [`SAME_AS_INPUT`]) the case's own `.in` bytes.
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
// Deliberately *not* `logit_proto::graphite::pickle`'s writer: a fixture built by the same code
// the encoder uses could not prove the encoder writes what a real payload decodes to. Same stance
// `collectd_round_trip.rs`'s `PacketBuilder` and `crates/logit-proto/tests/robustness.rs` take.
// Protocol-2 subset only, no memo -- `docs/plans/graphite-carbon-relay.md`'s writer opcode list.

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

    /// One `(path, (timestamp, value))` datapoint. `timestamp` is assumed to fit `i32` -- every
    /// fixture in this corpus is well inside that range; a real writer's `LONG1` fallback for a
    /// post-2038 second has its own dedicated coverage in `pickle.rs`'s own tests.
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

    /// The complete, length-prefixed frame a real wire capture would show: a 4-byte big-endian
    /// length prefix around the finished payload.
    fn build(mut self) -> Vec<u8> {
        self.payload.push(OP_APPENDS);
        self.payload.push(OP_STOP);
        let mut framed = (self.payload.len() as u32).to_be_bytes().to_vec();
        framed.extend_from_slice(&self.payload);
        framed
    }
}

/// Every pickle `.in` file in the corpus that this test-side builder can reproduce, pinned against
/// the committed bytes by [`every_committed_in_file_matches_its_builder`]. `pickle-protocol-2`/
/// `pickle-protocol-5` are real CPython dumps (see this file's module doc) and are not rebuilt
/// here -- there is no independent writer for *this* codec's job to prove itself against a real
/// unpickler's own opcode choices, which is exactly why those two are committed from a real
/// interpreter rather than generated.
fn build_pickle_in(name: &str) -> Vec<u8> {
    match name {
        "sanitizer-path" => PickleBuilder::new().datapoint("disk/free", 1_700_000_000, 1.0).build(),
        "sanitizer-tag" => {
            PickleBuilder::new().datapoint("cpu.load;bad!key=v", 1_700_000_000, 2.0).build()
        }
        other => panic!("build_pickle_in: unknown fixture {other:?}"),
    }
}

/// Every hand-built pickle `.in` file must equal what [`build_pickle_in`] produces -- the
/// byte-level pin `collectd_round_trip.rs`'s `every_committed_in_file_matches_its_builder` performs
/// for its own binary corpus.
#[test]
fn every_committed_in_file_matches_its_builder() {
    for fixture in ["sanitizer-path", "sanitizer-tag"] {
        let committed = read_fixture(fixture, "in");
        let built = build_pickle_in(fixture);
        assert_eq!(committed, built, "{fixture}.in should equal PickleBuilder's own output");
    }
}

// -- TCP capture: a background accept loop, one captured connection per completed send -----------

/// Spawns a plain TCP listener that captures whatever bytes each connection writes before closing.
/// Each accepted connection reads until EOF (the peer closing, exactly what `GraphiteOutput::tcp`
/// does once its sink is dropped after one `send`) and forwards the bytes down `tx`, so a test that
/// creates one fresh sink per round trip gets exactly one entry per call -- no explicit listener
/// juggling the way a per-call `TcpListener::bind` would need.
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

/// Real socket harness: a plain "byte capture" listener (for the raw-wire half of every
/// byte-for-byte assertion) alongside a real, bound [`GraphiteInput`] draining into a [`Fanout`]
/// channel (for the decoded-`EventBatch` half). Mirrors `collectd_round_trip.rs`'s/
/// `statsd_round_trip.rs`'s `Harness`: `bind()`-then-`local_addr()`-then-spawn, no bind-drop race
/// and no sleep-based readiness guess. One harness is built per transport/protocol combination a
/// test needs, since both the listener and the capture side are wired to it at construction.
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

    /// A harness whose `graphite_in` carries `diag`/`telemetry` -- for the decode-only cases, built
    /// separately so the byte-for-byte corpus isn't paying for a `Registry` it never reads. Both
    /// handles matter: `diag` is what a decode failure diagnoses through, `telemetry` is what its
    /// `logit.input.metrics.skipped` counter actually reaches -- `GraphiteInput::with_diagnostics`'s
    /// own doc comment (mirroring `crate::graphite::mod::tests::start`) explains why dropping either
    /// half silently disconnects one class of failure from the `Registry`.
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

    /// Sends `batch` through two fresh [`GraphiteOutput`]s built the same way -- one aimed at the
    /// raw capture socket, one at the live `graphite_in` -- and returns every captured message
    /// alongside the [`EventBatch`] the real input decoded from the other copy. `configure` is
    /// applied to both, so a non-default encoder is applied identically to each leg.
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

    /// [`Harness::round_trip_with`] with the sink left exactly as `graphite_out`'s own defaults
    /// build it.
    async fn round_trip(&mut self, batch: &EventBatch) -> (Vec<Vec<u8>>, EventBatch) {
        self.round_trip_with(batch, |sink| sink).await
    }

    fn output_to(&self, addr: SocketAddr) -> GraphiteOutput {
        match self.transport {
            Transport::Tcp => GraphiteOutput::tcp(addr.to_string(), TIMEOUT),
            Transport::Udp => GraphiteOutput::udp(addr.to_string()).expect("binding graphite_out"),
        }
    }

    /// Sends `batch` at the capture socket only, returning every message it received (one `Vec<u8>`
    /// per TCP connection's full write, or one per UDP datagram). Split out of
    /// [`Harness::round_trip_with`], its main caller, so the two legs of a round trip read as the
    /// two separate sends they are, and so cross-protocol tests that only care about the wire can
    /// call it directly.
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

    /// Every batch the live `graphite_in` delivered, flattened into one -- a multi-message send may
    /// arrive as one accumulated batch or as several, which is a batching decision this test has no
    /// business asserting on.
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

    /// Sends `raw` straight at the live `graphite_in`, bypassing `graphite_out` entirely -- for the
    /// decode-only cases, which are about what the *listener* does with bytes no encoder would ever
    /// produce. TCP only: every decode-only fixture in this corpus is plaintext.
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
    // `raw` is the length-prefixed wire capture; the decoder wants the unframed payload (framing
    // is `graphite_in`'s job, not the codec's -- `logit_proto::graphite::decode`'s module doc).
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

// -- byte for byte, TCP plaintext, default configuration ------------------------------------------

/// One deterministic byte-for-byte case over the default TCP-plaintext harness: the fixture's own
/// decode round-trips through a live `graphite_in` unchanged (whole-`EventBatch` equality), and the
/// bytes the far end actually received equal the case's `.expected` bytes (this file's module doc
/// normalization list covers every place the two legitimately differ).
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

/// Normalization 6: a fractional-second timestamp decodes keeping the sub-second (an ns-precise
/// `Event::timestamp`), but floors back to a whole second on egress -- so unlike every fixture in
/// [`the_corpus_round_trips_byte_for_byte`], `decode(sink_output)` is **not** expected to equal the
/// *original* decode (which still carries the sub-second), only the `.expected` bytes' own decode.
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

/// The same tagged line under `tags: drop`: the tag segment disappears entirely from the wire
/// (normalization 11), and the decoded batch on the far end therefore genuinely differs from the
/// original (no tag attribute survives) -- this is a one-way lossy case, so only the wire bytes are
/// asserted, not decode equality.
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

/// A real CPython pickle dump decodes through the live `graphite_in` and relays as our own
/// canonical pickle rendering -- byte-identical for protocol 2 and protocol 5 dumps of the same
/// datapoint, and for the plaintext-sourced version below, since the model retains no memory of
/// which dialect it decoded from.
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

/// The same datapoint, sourced from a *plaintext* line this time: `graphite_in` decodes plaintext,
/// `graphite_out` is configured for pickle -- an operator-chosen dialect change, and the resulting
/// bytes are the identical canonical rendering the two CPython-sourced cases above produce.
#[tokio::test]
async fn plaintext_to_pickle_relays_as_the_same_canonical_pickle() {
    // Capture-only: the point is the *encoder's* dialect switch, and there is no live `graphite_in`
    // that could decode both the plaintext `.in` and the pickle wire this sink emits at once -- the
    // pickle leg's own decode fidelity is already covered by the two CPython-sourced cases above.
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

    // And the pickle bytes decode back to the identical datapoint through the real codec.
    assert_eq!(direct_pickle_batch(&captured[0]), batch);
}

// -- sanitizer wire-bytes-only (normalization 10) ------------------------------------------------

/// A pickle path/tag carrying a byte no plaintext line's decode could ever produce (`/` in a path,
/// `!` in a tag name) sanitizes to `_` on egress -- one-way lossy, so only the wire bytes are
/// asserted, matching `statsd_round_trip.rs`'s own sanitizer cases.
async fn assert_sanitizer_case(fixture: &str) {
    // Capture-only, plaintext out: the point is the sanitizer, and this harness's own (unused)
    // `graphite_in` is plaintext-configured, which a pickle datapoint's decode could never reach
    // anyway -- proves the sanitizer fires regardless of which wire shape the forbidden byte
    // arrived through.
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

/// Five single-datapoint UDP messages (`Meta = 1` each) repack into one UDP datagram unchanged,
/// since all five already fit comfortably under the default `max_packet_bytes` cap -- the license
/// normalization 1 grants, observed rather than forced the way `repacked-at-512` forces it.
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

/// 40 lines in one UDP datagram, capped at 512 bytes on the way out: normalization 1 forces at
/// least two datagrams, none over the cap, and the whole batch survives however the boundaries
/// moved. No `.expected` file -- datagram boundaries are exactly what this case lets move, the
/// same shape `collectd_round_trip.rs`'s `repacked-at-1024` takes.
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

/// Normalization 7 from the decode side: carbon's `-1` sentinel stamps the datagram's receipt
/// time.
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

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        counted(&registry, "logit.input.metrics.skipped", ("reason", "non_finite_value")),
        "the drop must be counted, not silent"
    );
}

/// A line with fewer than three whitespace-separated fields is skipped, counted `bad_line`.
#[tokio::test]
async fn a_malformed_line_is_skipped_and_counted() {
    let (harness, registry) = diagnostic_harness().await;
    harness.send_raw(&read_fixture("bad-line", "in")).await;

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        counted(&registry, "logit.input.metrics.skipped", ("reason", "bad_line")),
        "the drop must be counted, not silent"
    );
}

// -- cross-protocol ---------------------------------------------------------------------------

/// `statsd_in` + a TCP capture for `graphite_out`, with no `graphite_in` at all -- both
/// cross-protocol cases below only assert on the captured wire bytes and the codec's own counters,
/// never on a decoded round trip through a live `graphite_in`.
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

/// A repeated DogStatsD tag key decodes to a `Value::Array` (`statsd_in`'s own `insert_tags`), and
/// `graphite_out` renders only its **last** element as the carbon tag, counted
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

/// `statsd_in -> aggregate -> graphite_out`, `multi_value: expand`: a DogStatsD timer's raw samples
/// flush through `aggregate`'s default sketch accumulator into a `MetricKind::Distribution`, and
/// `graphite_out` expands it into the documented sub-paths.
#[tokio::test]
async fn statsd_in_to_aggregate_to_graphite_out_expands_a_timer_into_the_documented_sub_paths() {
    let mut cross = CrossHarness::new().await;
    let raw = b"page.latency:10|ms\npage.latency:20|ms\npage.latency:30|ms";
    let batch = cross.statsd_decode(raw).await;
    assert_eq!(batch.events.len(), 3, "one event per line");

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    let forwarded: Vec<Event> =
        batch.events.into_iter().filter_map(|event| aggregator.process(&resource, event)).collect();
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
    for suffix in [".count", ".sum", ".q0_5", ".q0_75", ".q0_9", ".q0_95", ".q0_99"] {
        assert!(
            text.contains(&format!("page.latency{suffix} ")),
            "expected a page.latency{suffix} line, got:\n{text}"
        );
    }
}
