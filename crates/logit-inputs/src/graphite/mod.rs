//! Graphite/Carbon metric ingress -- the listener half of
//! [ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md)'s
//! `graphite_in -> graphite_out` lossless-relay pair
//! (`docs/plans/graphite-carbon-relay.md`'s W2).
//!
//! The wire grammar, the `| Wire | Model |` mapping table and the permitted normalizations all
//! live with the codec, in [`logit_proto::graphite`]'s module doc -- that doc is the spec, and
//! this one deliberately does not restate it. What lives here is the *component*: its
//! configuration, its socket behaviour, and what it reports.
//!
//! ```yaml
//! components:
//!   graphite_in:
//!     type: graphite_in
//!     bind: "0.0.0.0:2003"           # carbon's own plaintext port
//!     transport: tcp                 # tcp (default, carbon's own) | udp
//!     protocol: plaintext            # plaintext (default) | pickle (tcp only)
//!     max_line_bytes: "8192"         # tcp plaintext: drain-to-newline past this
//!     max_frame_bytes: "1MiB"        # pickle: Twisted's Int32StringReceiver.MAX_LENGTH
//!     tls:                           # tcp only; presence turns TLS on and makes it required
//!       cert_file: server.pem
//!       key_file: server.key
//!     handshake_timeout: 5s          # tcp only; per pre-message phase
//!     idle_timeout: 5m               # tcp only; optional, off unless set
//!     receive:                       # optional; see "Transports" below for which half applies
//!       batch_max_events: 1000
//! ```
//!
//! ## Transports
//!
//! One component, two shared drivers, chosen by `transport:` -- this type is just the decoder
//! choice plus the public builder surface `logit-cli::pipeline` and these tests depend on, exactly
//! as [`crate::syslog::SyslogInput`] is.
//!
//! **`udp`** wraps [`UdpListener<GraphiteDecoder>`](crate::udp::UdpListener), the same shape
//! [`crate::collectd::CollectdInput`] is: the read/decode split, the receive queue, the
//! datagram→batch assembly, `SO_RCVBUF`, multicast auto-join and the shutdown drain all come free
//! from that driver (`docs/adr/decoupled-listener-io.md`), and the whole `receive:` block applies.
//! `protocol: pickle` is rejected here by graph rule 46, not by this module: a 4-byte big-endian
//! length prefix has no meaning in a datagram that already delimits itself.
//!
//! **`tcp`** wraps [`TcpListener<GraphiteDecoder>`](crate::tcp::TcpListener), the shared stream
//! driver `syslog_in` runs on (`docs/adr/syslog-tcp-ingress-and-tls.md`). The accept loop, the
//! connection cap, the per-connection decoder clone and batch assembly, the first-byte deadline
//! and TLS termination are all that driver's; what this component picks is the **framing mode**
//! (see below) and the bound that goes with it. It has **no
//! [`ReceiveQueue`](crate::udp::ReceiveQueue)**: TCP's own flow control already is the
//! backpressure, and ADR `decoupled-listener-io` exists for UDP's *silent* drops, which a stream
//! cannot have. So only `receive:`'s batch-assembly fields (`batch_max_events`, `batch_max_bytes`,
//! `batch_flush_interval`) and `shutdown_grace` apply to a TCP listener -- graph rule 17 rejects
//! the queue-bounding ones by name.
//!
//! Each connection owns a read buffer, its own [`GraphiteDecoder`] clone and its own
//! [`logit_pipeline::BatchAccumulator`], and sends into a clone of the shared [`Fanout`]: one slow
//! or hostile connection stalls only itself. All of them share one `Arc<Resource>` (the clone
//! keeps it -- `logit_proto::graphite::GraphiteDecoder`'s own `Clone` impl says why) so batches
//! from different connections can still merge downstream: `BatchAccumulator::absorb` keys on
//! `Arc::ptr_eq`.
//!
//! ## Framing
//!
//! Framing is the listener's job, not the decoder's ([`logit_proto::graphite::decode`]'s module
//! doc says so from the other side) -- here, the driver's [`Framer`](crate::tcp::Framer) under a
//! [`FramingMode`] this component chooses from `protocol:`. **Never
//! [`FramingMode::Rfc6587Auto`]**: a carbon path may legitimately begin with a digit, which that
//! mode would read as an RFC 6587 octet count and reframe the whole connection on.
//!
//! | Protocol | Frame | Over the bound |
//! |---|---|---|
//! | plaintext, UDP | the datagram | nothing to bound: a datagram is already one read |
//! | plaintext, TCP | [`FramingMode::Lines`] with [`Oversize::DrainToNextLine`]: one `\n`-delimited line (a trailing `\r` stripped) per `decode_into` | a line past `max_line_bytes` is dropped and counted **once** as `logit.input.frames.dropped{reason="oversize"}`; the connection stays open and the line after it still decodes |
//! | pickle, TCP | [`FramingMode::LengthPrefixed`]: a 4-byte big-endian length prefix then that many payload bytes, handed to `decode_into` unframed | a frame declaring more than `max_frame_bytes` **closes the connection** (`logit.input.frames.dropped{reason="oversize"}`, diagnostic `framing_error`) -- a length-framed stream has no resync point to skip forward to |
//!
//! A pickle payload that fails to decode (a disallowed opcode, a depth or item cap) drops that
//! frame and keeps the connection: unlike an oversize length, a *decoded* length has already told
//! the reader where the next frame starts.
//!
//! **A terminator-less line at EOF is dropped, not ingested.** Carbon's `\n` is the only signal a
//! line is complete, so a sender that dies mid-line leaves a truncation, not a short datapoint --
//! and `svc.web01.cpu 42.5 17000` without its newline would otherwise parse perfectly and produce
//! a gauge stamped 1970. [`FramingMode::Lines`] therefore makes [`crate::tcp::Framer::finish`]
//! return `Truncated` (the driver counts `logit.input.frames.dropped{reason="truncated"}`), so a
//! clean FIN and an abrupt RST agree about identical bytes. [`FramingMode::Rfc6587Auto`] does the
//! opposite, because RFC 6587 §3.4.2 says a final syslog message needs no terminator.
//!
//! **One `decode_into` per line, not per read.** The driver frames first and hands the decoder one
//! delimited message at a time, where the old bespoke loop handed it everything through the read
//! buffer's last `\n` in a single call. `decode_plaintext` splits on `\n` internally either way, so
//! the result is identical; the cost is one call (plus its `Arc` clone and `absorb`) per line
//! rather than per read on carbon's hottest path. Measured, not assumed --
//! `docs/adr/graphite-carbon-relay.md`'s 2026-09-14 amendment records it, and a `LineChunk` framing
//! mode is the reserve fix if it ever stops being acceptable.
//!
//! ## Connections
//!
//! The driver's: 1024 at a time, the permit taken non-blockingly *after* `accept`, a past-the-cap
//! connection closed immediately and counted `logit.input.connections.rejected{reason="limit"}`
//! (carbon's wire has no way to say "try later"). `handshake_timeout:` bounds each pre-message
//! phase independently -- the TLS accept when `tls:` is set, then the wait for the connection's
//! very first byte -- and is **not** an idle timeout. The gaps *after* that first byte are
//! bounded by the separate, opt-in `idle_timeout:`, off unless set
//! (`docs/adr/idle-connection-timeout.md`, and the driver module's "Idle timeout" section).
//!
//! ## Diagnostics
//!
//! Every one is throttled (`logit.component.diagnostics{key}`,
//! `docs/design/internal-telemetry.md`), and every clone of this component's [`Diagnostics`]
//! shares one set of counts, so a key throttles per *listener* rather than per connection. The
//! decoder's own -- `bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value`,
//! `duplicate_tag_key`, `bad_pickle` -- reach it through [`GraphiteInput::with_diagnostics`],
//! which propagates into the decoder as well as the driver. The driver adds:
//!
//! | Key | Meaning |
//! |---|---|
//! | `bound` | info: the socket is open (the shared pre-bind pass, `docs/deploying.md`) |
//! | `framing_error` | TCP: a line past `max_line_bytes` (skipped, connection kept) or a pickle frame declaring more than `max_frame_bytes` (connection closed), or a partial frame discarded by an abrupt close |
//! | `bad_frame` | TCP: one framed payload the decoder rejected outright -- a pickle frame only, since the plaintext path isolates every failure per line and never returns `Err` |
//! | `connection_error` | one connection's I/O failed -- never fatal to the listener or its siblings, exactly as `otlp_in`/`logit_in` treat theirs |
//! | `bad_datagram` | UDP only, and the datagram driver's own: a whole datagram that failed to decode |
//!
//! ## Telemetry
//!
//! All of it comes from the shared drivers; this component adds none of its own, exactly like
//! `collectd_in`. Under `transport: udp` that is `logit.input.datagrams`/`.datagram.bytes`, the
//! `logit.component.receive.*` queue gauges and `logit.input.receive_buffer.bytes`. Under
//! `transport: tcp` it is `logit.input.connections` (gauge),
//! `logit.input.connections.rejected{reason="limit"}`, `logit.input.frames`/`.frame.bytes` (one
//! *frame* is one plaintext line or one pickle payload -- the prefix's own 4 bytes are not counted,
//! since the decoder never sees them), `logit.input.frames.dropped{reason}`, and
//! `logit.component.receive.flushed{reason}` from the per-connection batch assembly. The decoder's
//! own `logit.input.metrics.skipped{reason}` is reported under both.

use crate::tcp::{FramingMode, Oversize, TcpListener, TcpListenerConfig, TlsServerSettings};
use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::graphite::{
    GraphiteDecoder, Protocol, DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_LINE_BYTES,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Which socket a [`GraphiteInput`] listens on. Carbon's own default listener is TCP (plaintext,
/// port 2003), so [`Transport::Tcp`] is the default here and in `logit_config::GraphiteTransport`.
///
/// Its own type rather than `logit_config::GraphiteTransport`: `logit-inputs` never depends on
/// `logit-config` (`docs/design/pipeline-graph.md`'s crate layout), the same split
/// [`crate::otlp::OtlpTransport`] already lives on.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    #[default]
    Tcp,
    Udp,
}

/// Which driver a [`GraphiteInput`] is wrapping. Chosen once, by `transport:`
/// (`crates/logit-cli/src/pipeline.rs`'s `GraphiteIn` arm), and never changed afterwards -- an
/// enum rather than a `Box<dyn Input>` so each arm keeps its own concrete builder surface
/// ([`TcpListener::with_tls`], [`UdpListener::with_config`]) reachable through this wrapper.
/// [`crate::syslog::SyslogInput`]'s own `Inner`, for the same reasons.
enum Inner {
    Udp(UdpListener<GraphiteDecoder>),
    Tcp(TcpListener<GraphiteDecoder>),
}

/// A carbon receiver: plaintext or pickle, over TCP or UDP. See this module's doc.
pub struct GraphiteInput {
    inner: Inner,
    /// Which carbon wire protocol this listener speaks. Held here as well as inside the decoder
    /// because it is what picks the TCP framing mode, and the decoder's copy is not reachable
    /// through the driver.
    protocol: Protocol,
    /// The two operator-facing frame bounds. Held as fields and applied to the stream driver
    /// lazily, in [`Input::bind`], rather than at the moment they are set: `with_max_line_bytes`,
    /// `with_max_frame_bytes` and the protocol together decide one `with_framing` call, and
    /// applying it eagerly would make the *order* of this type's builder methods load-bearing.
    max_line_bytes: usize,
    max_frame_bytes: usize,
    /// The batch-assembly (and, under UDP, receive-queue) knobs a `receive:` block sets. Held here
    /// as well as inside whichever driver it was pushed into, so [`Self::receive_config`] can read
    /// it back without knowing which one is in play.
    receive: UdpListenerConfig,
}

impl GraphiteInput {
    /// A listener on `bind` speaking `protocol` over `transport`. `protocol: Pickle` with
    /// `transport: Udp` is rejected by graph rule 46 before a config ever reaches here; a direct
    /// caller that builds one anyway gets a pickle decoder fed whole datagrams, which is a
    /// meaningful (if unused) thing to do rather than something worth a panic.
    pub fn new(bind: impl Into<String>, transport: Transport, protocol: Protocol) -> Self {
        // One resource for every event this component ever produces, shared across every TCP
        // connection's decoder clone as well as the UDP one. **Not** one per connection:
        // `logit_pipeline::BatchAccumulator::absorb` keys accumulation on `Arc::ptr_eq`, so a
        // resource per connection would stop two connections' events ever sharing a batch
        // downstream (`GraphiteDecoder`'s own `resource` field documents the same constraint, and
        // its `Clone` impl is what preserves it across the driver's per-connection clone).
        let decoder = GraphiteDecoder::new(Arc::new(Resource::default())).with_protocol(protocol);
        let bind = bind.into();
        let inner = match transport {
            Transport::Udp => {
                Inner::Udp(UdpListener::new(bind, decoder, UdpListenerConfig::default()))
            }
            Transport::Tcp => {
                Inner::Tcp(TcpListener::new(bind, decoder, TcpListenerConfig::default()))
            }
        };
        Self {
            inner,
            protocol,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            receive: UdpListenerConfig::default(),
        }
    }

    /// Attaches a component id to this listener's diagnostics -- and to the [`GraphiteDecoder`]
    /// the driver wraps, so both report under the same id. Both halves matter for exactly
    /// [`crate::collectd::CollectdInput::with_diagnostics`]'s reason: the driver's own `diag` is
    /// what a transport-level failure reports through (`bad_datagram` on UDP;
    /// `framing_error`/`bad_frame`/`connection_error` on TCP), while the decoder's own carries
    /// everything finer-grained (`bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value`,
    /// `bad_pickle`) -- two distinct `Diagnostics` values that must both carry the same id and
    /// telemetry handle, or one whole class of decode failure silently reports under no component
    /// id and with telemetry disabled. On the TCP arm the decoder set here is the one every
    /// connection's clone is made from, and a `Diagnostics` clone shares its original's throttle
    /// counts, so `bad_line` throttles per listener rather than per connection.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(
                listener.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag)),
            ),
            Inner::Tcp(listener) => Inner::Tcp(
                listener.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag)),
            ),
        };
        self
    }

    /// Attaches a telemetry handle -- the datagram driver's wire-level counters under UDP, the
    /// stream driver's connection/frame counters under TCP, and the decoder's own skip counters
    /// under both (`docs/design/internal-telemetry.md`'s "layer 3").
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(
                listener
                    .with_telemetry(telemetry.clone())
                    .map_decoder(|d| d.with_telemetry(telemetry)),
            ),
            Inner::Tcp(listener) => Inner::Tcp(
                listener
                    .with_telemetry(telemetry.clone())
                    .map_decoder(|d| d.with_telemetry(telemetry)),
            ),
        };
        self
    }

    /// Overrides the batch-assembly/shutdown-grace knobs (and, under UDP, the receive-queue ones)
    /// a `receive:` config block sets. Defaults to [`UdpListenerConfig::default`] when never
    /// called. Graph rule 17 is what guarantees a TCP listener never arrives here carrying a
    /// queue-bounding field it would silently ignore.
    ///
    /// One setter taking the datagram config on both arms, unlike
    /// [`crate::syslog::SyslogInput::with_receive`]/`with_tcp_receive`: `graphite_in` has always
    /// had one, `logit-cli`'s `GraphiteIn` arm calls it without branching on transport, and the
    /// four fields a TCP listener does read mean the same thing in both structs -- so the
    /// conversion below is total and there is nothing for a caller to get wrong.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        self.receive = config;
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(listener.with_config(config)),
            Inner::Tcp(listener) => Inner::Tcp(listener.with_config(TcpListenerConfig {
                batch_max_events: config.batch_max_events,
                batch_max_bytes: config.batch_max_bytes,
                batch_flush_interval: config.batch_flush_interval,
                shutdown_grace: config.shutdown_grace,
            })),
        };
        self
    }

    /// The currently-configured `receive:`-derived knobs -- for test introspection
    /// (`logit-cli::pipeline`'s `build_spec` wiring tests), exactly like
    /// [`crate::collectd::CollectdInput::receive_config`].
    pub fn receive_config(&self) -> UdpListenerConfig {
        self.receive
    }

    /// Bounds one TCP plaintext line. Ignored under `transport: udp`, where a datagram is already
    /// its own frame. See this module's "Framing" table.
    pub fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.max_line_bytes = max_line_bytes;
        self
    }

    /// Bounds one pickle frame's declared payload length. Ignored under `protocol: plaintext`.
    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    /// Overrides a **TCP** listener's per-phase pre-message budget (`handshake_timeout:` in
    /// config): the TLS accept when `tls:` is set, and the wait for the connection's first byte.
    /// Delegates straight to [`TcpListener::with_handshake_timeout`].
    ///
    /// A UDP listener is left untouched rather than failing, exactly as
    /// [`crate::syslog::SyslogInput::with_handshake_timeout`] does: there is no connection on that
    /// transport for the value to bound, so there is nothing to apply and nothing to refuse. Graph
    /// rule 45 is what tells an operator who set a non-default value under `transport: udp` that
    /// it could never take effect -- unlike `tls:`, whose [`Self::with_tls`] arm does fail,
    /// because `tls:` has no default and its mere presence is an instruction.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_handshake_timeout(handshake_timeout));
        }
        self
    }

    /// Bounds how long a **TCP** connection may stay quiet once it is past its first byte
    /// (`idle_timeout:` in config) before this listener closes it and hands its permit back --
    /// delegates straight to [`TcpListener::with_idle_timeout`], whose doc comment and the driver
    /// module's "Idle timeout" section describe what resets the clock. `None` (the default) is no
    /// idle timeout at all.
    ///
    /// A UDP listener is left untouched for exactly the reason
    /// [`Self::with_handshake_timeout`] leaves it untouched: there is no connection on that
    /// transport to time out. Graph rule 53 is what tells an operator who set the field under
    /// `transport: udp` that it could never take effect.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_idle_timeout(idle_timeout));
        }
        self
    }

    /// Terminates TLS on a TCP listener (`tls:` in config) -- delegates straight to
    /// [`TcpListener::with_tls`], which resolves every path in `settings` against `base_dir`.
    ///
    /// A UDP listener fails here rather than ignoring the block: carbon has no DTLS receiver of
    /// any kind, so there is nothing this could mean. Graph rule 43 rejects the same combination
    /// at config-validation time and is what an operator actually sees; this arm is the
    /// belt-and-braces backstop for a caller that skipped validation, not the primary diagnostic.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.inner = match self.inner {
            Inner::Tcp(listener) => Inner::Tcp(listener.with_tls(settings, base_dir)?),
            Inner::Udp(_) => anyhow::bail!(
                "graphite_in: 'tls:' needs 'transport: tcp' -- carbon has no DTLS receiver \
                 (docs/adr/graphite-carbon-relay.md)"
            ),
        };
        Ok(self)
    }

    /// The address actually bound, once [`Input::bind`] has run -- lets a caller (a round-trip
    /// test) learn the OS-assigned port with no bind-drop-rebind race, under either transport.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.inner {
            Inner::Udp(listener) => listener.local_addr(),
            Inner::Tcp(listener) => listener.local_addr(),
        }
    }

    /// Test-only override of the driver's connection cap -- opening 1025 real TCP connections in a
    /// test to exercise it would be slow and flaky; this makes the cap reachable with two. A UDP
    /// listener has no connections and is left untouched.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_max_connections(max_connections));
        }
        self
    }

    /// How a TCP connection's messages are delimited, and the bound on one of them -- `protocol:`
    /// plus whichever of the two size fields it makes meaningful. **Never
    /// [`FramingMode::Rfc6587Auto`]**: see this module's "Framing" section.
    fn framing(&self) -> (FramingMode, usize) {
        match self.protocol {
            Protocol::Plaintext => {
                (FramingMode::Lines { oversize: Oversize::DrainToNextLine }, self.max_line_bytes)
            }
            Protocol::Pickle => (FramingMode::LengthPrefixed, self.max_frame_bytes),
        }
    }
}

#[async_trait::async_trait]
impl Input for GraphiteInput {
    /// Idempotent, per `Input::bind`'s contract -- and the point at which this component's framing
    /// decision reaches the stream driver (see the `max_line_bytes` field's doc comment).
    async fn bind(&mut self) -> anyhow::Result<()> {
        let (mode, max_frame_bytes) = self.framing();
        match &mut self.inner {
            Inner::Udp(listener) => listener.bind().await,
            Inner::Tcp(listener) => {
                listener.set_framing(mode, max_frame_bytes);
                listener.bind().await
            }
        }
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        // Never exercised in production -- `run_input` always calls `run_until_shutdown`. Present
        // because the trait requires it, mirroring `crate::udp::UdpListener::run`.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        // Explicitly, before delegating: the driver's own `run_until_shutdown` binds too, but only
        // this one applies the framing, and a caller that never called `bind()` would otherwise
        // get a stream driver still set to its `Rfc6587Auto` default.
        self.bind().await?;
        match &mut self.inner {
            Inner::Udp(listener) => listener.run_until_shutdown(sink, shutdown).await,
            Inner::Tcp(listener) => listener.run_until_shutdown(sink, shutdown).await,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use logit_core::interner::resolve;
    use logit_core::telemetry::Registry;
    use logit_core::{Event, MetricKind, Value};
    use logit_pipeline::unwrap_batch;
    use logit_proto::Decoder as _;
    use rustls_pki_types::pem::PemObject;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;

    /// Well past the 2038 problem and nothing like a receipt time, so every assertion below is
    /// really reading the wire's own timestamp.
    const TIMESTAMP: &str = "1700000000";
    const TIMESTAMP_NANOS: i64 = 1_700_000_000_000_000_000;

    /// A running listener plus everything a test needs to talk to it and shut it down.
    struct Running {
        addr: SocketAddr,
        rx: mpsc::Receiver<logit_pipeline::Delivered>,
        shutdown: watch::Sender<bool>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
        registry: Arc<Registry>,
    }

    impl Running {
        /// The next delivered batch, or a panic naming what was being waited for. Five seconds is
        /// the same budget `collectd.rs`'s own socket tests use -- long enough that a loaded CI box
        /// doesn't flake, short enough that a genuine hang fails rather than hanging the suite.
        async fn next_batch(&mut self, what: &str) -> logit_core::EventBatch {
            let delivered = tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
                .expect("the channel should not have closed");
            unwrap_batch(delivered)
        }

        async fn connect(&self) -> TcpStream {
            TcpStream::connect(self.addr).await.expect("the listener should accept")
        }
    }

    /// Binds `input` on an ephemeral port, runs it, and hands back the pieces. The registry is
    /// wired through both the component's telemetry and its diagnostics, so a test can assert on
    /// either without a second setup path.
    async fn start(
        build: impl FnOnce(GraphiteInput) -> GraphiteInput,
        transport: Transport,
        protocol: Protocol,
    ) -> Running {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("graphite_in", "graphite_in", "listener");
        let input = GraphiteInput::new("127.0.0.1:0", transport, protocol)
            .with_diagnostics(Diagnostics::new("graphite_in").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        let mut input = build(input);
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, rx) = mpsc::channel(64);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });
        Running { addr, rx, shutdown, handle, registry }
    }

    /// A `receive:` block with no flush timer at all, so a batch is only ever completed by a bound
    /// or by shutdown -- what the drain test needs, and harmless everywhere it isn't the point.
    fn no_flush_timer() -> UdpListenerConfig {
        UdpListenerConfig { batch_flush_interval: Duration::ZERO, ..UdpListenerConfig::default() }
    }

    /// The summed value of every counter point named `metric` in an already-drained `events`,
    /// optionally narrowed to one tag. Takes the drained slice rather than the `Registry` --
    /// `drain` empties it, so several assertions need one shared drain
    /// (`crates/logit-outputs/src/collectd.rs`'s own `metric_sum`).
    fn metric_sum(events: &[Event], metric: &str, tag: Option<(&str, &str)>) -> f64 {
        events
            .iter()
            .filter(|event| match tag {
                Some((key, value)) => {
                    event.attributes.get(key).and_then(Value::as_str) == Some(value)
                }
                None => true,
            })
            .flat_map(|event| &event.metrics)
            .filter(|m| resolve(m.name) == metric)
            .map(|m| match &m.kind {
                MetricKind::Sum(sum) => sum.value,
                MetricKind::Gauge(v) => *v,
                other => panic!("{metric} should be a counter or a gauge, got {other:?}"),
            })
            .sum()
    }

    fn line(path: &str) -> String {
        format!("{path} 1.5 {TIMESTAMP}\n")
    }

    /// One complete, length-prefixed carbon pickle frame carrying `datapoints`. Built through the
    /// codec's own writer: unlike a wire-format *fixture* (which is deliberately hand-rolled so it
    /// states the format independently -- `crates/logit-bench/src/fixtures.rs`), what these tests
    /// are about is the listener's **framing**, and hand-rolling a pickle payload here would only
    /// re-test `pickle.rs`'s own round trip in a worse place.
    fn pickle_frame(datapoints: &[(&str, i64, f64)]) -> Vec<u8> {
        let mut payload = Vec::new();
        logit_proto::graphite::pickle::write_datapoints(
            &mut payload,
            datapoints.iter().map(|(p, t, v)| (*p, *t, *v)),
        );
        let mut framed = Vec::new();
        logit_proto::graphite::pickle::write_length_prefix(&mut framed, payload.len());
        framed.extend_from_slice(&payload);
        framed
    }

    // -- UDP --------------------------------------------------------------------------------

    /// The whole component against a real socket, through the shared datagram driver: one datagram
    /// of three lines is one batch of three gauge events, with the wire's own timestamp.
    #[tokio::test]
    async fn a_udp_datagram_decodes_into_one_delivered_batch() {
        let mut running = start(|input| input, Transport::Udp, Protocol::Plaintext).await;
        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.expect("sender should bind");
        let datagram = format!("{}{}{}", line("a.b"), line("c.d"), line("e.f"));
        sender.send_to(datagram.as_bytes(), running.addr).await.expect("send_to should succeed");

        let batch = running.next_batch("the datagram").await;
        let names: Vec<&str> = batch.events.iter().map(|e| resolve(e.metrics[0].name)).collect();
        assert_eq!(names, ["a.b", "c.d", "e.f"], "one line is one event, in wire order");
        assert_eq!(batch.events[0].timestamp, TIMESTAMP_NANOS);
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::Gauge(1.5));

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// Two datagrams, one batch: the `receive:` block's batch assembly is the shared driver's, and
    /// a UDP `graphite_in` gets it unchanged (`docs/adr/decoupled-listener-io.md`). Pinned because
    /// the TCP half deliberately reimplements the same knobs -- if these ever diverged, `receive:`
    /// would mean two different things on one component.
    #[tokio::test]
    async fn udp_still_accumulates_across_datagrams_through_the_receive_queue() {
        let mut running = start(
            |input| {
                input.with_receive(UdpListenerConfig {
                    batch_max_events: 4,
                    batch_flush_interval: Duration::ZERO,
                    ..UdpListenerConfig::default()
                })
            },
            Transport::Udp,
            Protocol::Plaintext,
        )
        .await;
        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.expect("sender should bind");
        for path in ["a.b", "c.d"] {
            let datagram = format!("{}{}", line(path), line(path));
            sender
                .send_to(datagram.as_bytes(), running.addr)
                .await
                .expect("send_to should succeed");
        }

        let batch = running.next_batch("the accumulated batch").await;
        assert_eq!(
            batch.events.len(),
            4,
            "two 2-line datagrams fill one batch_max_events: 4 batch -- no flush timer fired"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// `with_receive` is what a `receive:` block reaches, and `receive_config` is how
    /// `logit-cli`'s `build_spec` test reads it back -- for both transports, since a TCP listener
    /// takes the batch-assembly half of the same block.
    #[test]
    fn with_receive_round_trips_through_receive_config_on_both_transports() {
        for transport in [Transport::Tcp, Transport::Udp] {
            let config =
                UdpListenerConfig { batch_max_events: 4242, ..UdpListenerConfig::default() };
            let input = GraphiteInput::new("127.0.0.1:0", transport, Protocol::Plaintext)
                .with_receive(config);
            assert_eq!(input.receive_config().batch_max_events, 4242, "{transport:?}");
        }
    }

    /// The guard every sibling input carries (`collectd.rs`/`statsd.rs`'s own
    /// `with_diagnostics_reaches_the_wrapped_decoder_too`): dropping `with_diagnostics`'s
    /// `.map_decoder(..)` half compiles fine and silently leaves every decoder-side diagnostic
    /// (`bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value`) reporting under no component
    /// id and with telemetry disabled.
    ///
    /// Both transports, now that both wrap a driver that holds the decoder: on the TCP arm the
    /// decoder this reads is the one every connection's clone is made from.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_on_both_transports() {
        for transport in [Transport::Tcp, Transport::Udp] {
            let input = GraphiteInput::new("127.0.0.1:0", transport, Protocol::Plaintext)
                .with_diagnostics(Diagnostics::new("my-id"));
            let id = match &input.inner {
                Inner::Udp(listener) => listener.decoder().diag().component_id().to_string(),
                Inner::Tcp(listener) => listener.decoder().diag().component_id().to_string(),
            };
            assert_eq!(id, "my-id", "{transport:?}");
        }
    }

    // -- TCP: binding -----------------------------------------------------------------------

    /// `bind()` makes the port live before `run` ever starts, is idempotent, and leaves the real
    /// ephemeral address behind -- `Input::bind`'s three-part contract, which the pre-bind pass
    /// (`docs/plans/operator-surface.md`) depends on.
    #[tokio::test]
    async fn tcp_bind_makes_the_port_live_before_run_and_is_idempotent() {
        let mut input = GraphiteInput::new("127.0.0.1:0", Transport::Tcp, Protocol::Plaintext);
        assert_eq!(input.local_addr(), None, "no address before bind()");
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        // Live with `run` never called: the whole point of the pre-bind pass.
        TcpStream::connect(addr).await.expect("the port should already be accepting");

        input.bind().await.expect("a second bind() is a no-op, per Input::bind's contract");
        assert_eq!(input.local_addr(), Some(addr), "and it does not rebind to a new port");
    }

    /// An unbindable address fails startup rather than surfacing later as a listener that quietly
    /// never accepts. Uses a port this test itself already holds, so the failure is deterministic
    /// rather than dependent on the sandbox's privileges.
    #[tokio::test]
    async fn tcp_bind_reports_an_unbindable_address() {
        let mut held = GraphiteInput::new("127.0.0.1:0", Transport::Tcp, Protocol::Plaintext);
        held.bind().await.expect("the first bind should succeed");
        let addr = held.local_addr().expect("bind() leaves an address behind");

        let mut clash = GraphiteInput::new(addr.to_string(), Transport::Tcp, Protocol::Plaintext);
        let err = clash.bind().await.expect_err("the port is already held");
        assert!(
            err.to_string().to_lowercase().contains("address"),
            "the error should name the address problem, got {err}"
        );
    }

    // -- TCP: plaintext framing -------------------------------------------------------------

    /// Two connections at once, each delivering: the per-connection `Fanout` clone and
    /// per-connection accumulator are what make this work, and a listener that served connections
    /// one at a time would still pass a single-connection test.
    #[tokio::test]
    async fn two_concurrent_tcp_connections_both_deliver() {
        let mut running = start(|input| input, Transport::Tcp, Protocol::Plaintext).await;
        let mut first = running.connect().await;
        let mut second = running.connect().await;
        first.write_all(line("first.path").as_bytes()).await.unwrap();
        second.write_all(line("second.path").as_bytes()).await.unwrap();

        let mut seen = Vec::new();
        while seen.len() < 2 {
            let batch = running.next_batch("a batch from each connection").await;
            for event in &batch.events {
                seen.push(resolve(event.metrics[0].name).to_string());
            }
        }
        seen.sort();
        assert_eq!(seen, ["first.path", "second.path"]);

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// A line split across two writes (and so, almost certainly, across two reads) is reassembled:
    /// the reader hands `decode_into` only the bytes through the last newline and keeps the
    /// remainder. The `.5` deliberately straddles the split, so a reader that decoded the partial
    /// buffer would produce a *different valid number* rather than an obvious error.
    #[tokio::test]
    async fn a_tcp_line_split_across_writes_is_reassembled() {
        let mut running = start(|input| input, Transport::Tcp, Protocol::Plaintext).await;
        let mut stream = running.connect().await;
        stream.write_all(b"split.path 1").await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        stream.write_all(format!(".5 {TIMESTAMP}\n").as_bytes()).await.unwrap();

        let batch = running.next_batch("the reassembled line").await;
        assert_eq!(batch.events.len(), 1);
        assert_eq!(resolve(batch.events[0].metrics[0].name), "split.path");
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::Gauge(1.5));

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The oversize-line row of this module's framing table, which is the listener's to enforce: a
    /// line past `max_line_bytes` with no newline is abandoned and counted **once**, and -- the
    /// part that matters -- the next line still decodes, because [`Oversize::DrainToNextLine`]
    /// drains to the newline rather than closing (as [`Oversize::Fatal`] would) or
    /// resynchronizing at an arbitrary byte.
    #[tokio::test]
    async fn an_oversize_tcp_line_is_skipped_and_the_next_one_still_decodes() {
        let mut running =
            start(|input| input.with_max_line_bytes(64), Transport::Tcp, Protocol::Plaintext).await;
        let mut stream = running.connect().await;
        let long_path = "x".repeat(200);
        stream.write_all(long_path.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        stream.write_all(format!(" 1 {TIMESTAMP}\n").as_bytes()).await.unwrap();
        stream.write_all(line("after.oversize").as_bytes()).await.unwrap();

        let batch = running.next_batch("the line after the oversize one").await;
        assert_eq!(batch.events.len(), 1, "only the survivor is delivered");
        assert_eq!(resolve(batch.events[0].metrics[0].name), "after.oversize");

        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.input.frames.dropped", Some(("reason", "oversize"))),
            1.0,
            "counted once when the bound was crossed, not once per byte drained"
        );
        assert_eq!(
            metric_sum(&drained, "logit.component.diagnostics", Some(("key", "framing_error"))),
            1.0,
            "and diagnosed on the driver's own framing key"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// One connection ending cleanly must not take the listener (or a sibling connection) down --
    /// the property `otlp_in`/`logit_in` both state and the reason a connection's error is
    /// `connection_error` rather than a `run` failure.
    #[tokio::test]
    async fn a_closed_tcp_connection_does_not_take_the_listener_down() {
        let mut running = start(|input| input, Transport::Tcp, Protocol::Plaintext).await;
        let mut first = running.connect().await;
        first.write_all(line("before.close").as_bytes()).await.unwrap();
        let batch = running.next_batch("the first connection's line").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "before.close");
        drop(first);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut second = running.connect().await;
        second.write_all(line("after.close").as_bytes()).await.unwrap();
        let batch = running.next_batch("the second connection's line").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "after.close");
        assert!(!running.handle.is_finished(), "the accept loop is still running");

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// A connection arriving past the cap is closed immediately and counted. `max_connections: 1`
    /// rather than the driver's real 1024 so two sockets reach it, exactly as `logit.rs`'s own cap
    /// test does. The cap itself is the driver's now; this keeps the wiring pinned from the
    /// component's side.
    #[tokio::test]
    async fn a_tcp_connection_past_the_cap_is_rejected_and_counted() {
        let mut running =
            start(|input| input.with_max_connections(1), Transport::Tcp, Protocol::Plaintext).await;
        let mut held = running.connect().await;
        held.write_all(line("holds.the.permit").as_bytes()).await.unwrap();
        // Waiting for its batch is what guarantees the permit is actually taken before the second
        // connection races the accept loop for it.
        running.next_batch("the first connection's line").await;

        let mut rejected = running.connect().await;
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), rejected.read(&mut byte))
            .await
            .expect("a rejected connection is closed, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener closed it without writing anything");

        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.input.connections.rejected", Some(("reason", "limit"))),
            1.0
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// `with_diagnostics`/`with_telemetry` have to survive the driver's *per-connection decoder
    /// clone*, which is made at accept time -- so unlike the static test above this one drives a
    /// real socket and asserts that a decoder-side skip actually showed up under this component's
    /// handles. A malformed line counts through the decoder's telemetry and diagnoses through its
    /// `Diagnostics`, so this pins both halves at once, on the far side of `GraphiteDecoder`'s own
    /// hand-written `Clone`.
    #[tokio::test]
    async fn with_diagnostics_and_telemetry_reach_a_tcp_connections_decoder() {
        let mut running = start(|input| input, Transport::Tcp, Protocol::Plaintext).await;
        let mut stream = running.connect().await;
        stream.write_all(b"only.two.fields 1\n").await.unwrap();
        stream.write_all(line("good.path").as_bytes()).await.unwrap();
        running.next_batch("the good line after the bad one").await;

        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.input.metrics.skipped", Some(("reason", "bad_line"))),
            1.0,
            "the decoder's own telemetry handle"
        );
        assert_eq!(
            metric_sum(&drained, "logit.component.diagnostics", Some(("key", "bad_line"))),
            1.0,
            "the decoder's own diagnostics, mirrored by the Diagnostics->Telemetry bridge"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// Shutdown flushes what a connection had accumulated but not yet sent. Run with no flush
    /// timer at all, so the only thing that can complete this batch is the shutdown drain --
    /// `Input::run_until_shutdown`'s contract, and what `InputRuntimeConfig::shutdown_grace`
    /// bounds.
    #[tokio::test]
    async fn shutdown_drains_a_tcp_connections_accumulated_batch() {
        let mut running = start(
            |input| input.with_receive(no_flush_timer()),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;
        let mut stream = running.connect().await;
        stream.write_all(line("drained.path").as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            running.rx.try_recv().is_err(),
            "with no flush timer and a bound of 1000 events, nothing should have been sent yet"
        );

        running.shutdown.send(true).expect("the receiver is alive");
        let batch = running.next_batch("the shutdown drain").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "drained.path");
        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.component.receive.flushed", Some(("reason", "shutdown"))),
            1.0,
            "the component going away is the one thing that is really a shutdown"
        );

        let joined = tokio::time::timeout(Duration::from_secs(5), running.handle)
            .await
            .expect("run_until_shutdown must return, not hang, once the signal fires");
        joined.expect("the task should not panic").expect("and the listener should exit cleanly");
    }

    /// A client hanging up is **not** a component shutdown, and the connection's final flush has
    /// to say so: `FlushReason::Closed` is "one source among several ended while the listener
    /// keeps running" (`logit_pipeline::accumulator`), which is exactly this. Without the split a
    /// healthy listener reports `receive.flushed{reason="shutdown"}` on every disconnect, and an
    /// operator watching for a real shutdown sees nothing but noise.
    #[tokio::test]
    async fn a_clean_disconnect_flushes_as_closed_not_shutdown() {
        let mut running = start(
            |input| input.with_receive(no_flush_timer()),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;
        let mut stream = running.connect().await;
        stream.write_all(line("closed.path").as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(stream); // a clean FIN, the ordinary way a carbon sender ends a connection

        let batch = running.next_batch("the end-of-connection flush").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "closed.path");

        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.component.receive.flushed", Some(("reason", "closed"))),
            1.0
        );
        assert_eq!(
            metric_sum(&drained, "logit.component.receive.flushed", Some(("reason", "shutdown"))),
            0.0,
            "the listener is still running -- nothing here is a shutdown"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// A sender that dies mid-line and then closes **cleanly** must not have its half-line
    /// ingested. Carbon's own receiver drops a terminator-less tail, and so did the bespoke loop
    /// this listener replaced -- it only ever decoded through the last `\n`. The shared driver's
    /// `Framer::finish` would emit it under `Rfc6587Auto` (RFC 6587 says a final message needs no
    /// terminator), which is why `FramingMode::Lines` overrides that and returns `Truncated`.
    ///
    /// What makes this worth a socket test rather than only a framer one: the bytes below are a
    /// *valid* carbon line up to the point they stop. `svc.web01.cpu 42.5 17000` parses as three
    /// whitespace-separated fields, so emitting it produces a perfectly well-formed gauge stamped
    /// 1970-01-01 -- silent corruption, not a visible error. And a clean FIN has to agree with the
    /// RST case below, which `report_buffered_tail` already counts `truncated`.
    #[tokio::test]
    async fn an_unterminated_tail_at_a_clean_close_is_dropped_and_counted_truncated() {
        let running = start(
            |input| input.with_receive(no_flush_timer()),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;
        let mut running = running;
        let mut stream = running.connect().await;
        // No trailing newline: the sender got this far and stopped.
        stream.write_all(b"svc.web01.cpu 42.5 17000").await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(stream); // a clean FIN, not an RST

        assert!(
            tokio::time::timeout(Duration::from_millis(500), running.rx.recv()).await.is_err(),
            "half a line is not a datapoint -- nothing should be delivered"
        );

        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.input.frames.dropped", Some(("reason", "truncated"))),
            1.0,
            "and the loss is counted, exactly as an abrupt close's is"
        );
        assert_eq!(
            metric_sum(&drained, "logit.input.frames", None),
            0.0,
            "the remainder never became a frame"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The regression test for the one exit that used to skip the final flush. A read **error**
    /// (not a clean FIN) has to reach the same `accumulator.take()` every other exit does, or
    /// everything decoded since the last flush is lost silently -- up to `batch_max_events` of it,
    /// and with no flush timer that is the only bound.
    ///
    /// `SO_LINGER 0` makes the close a TCP RST rather than a FIN, which is what turns the server's
    /// next read into `ECONNRESET` instead of `Ok(0)`. The sleep is load-bearing: it lets the
    /// listener actually read and accumulate the line before the reset arrives, since an RST
    /// discards whatever is still sitting in the receive buffer -- which is a real property of
    /// RST, not something this listener could recover from.
    #[tokio::test]
    async fn a_read_error_still_flushes_what_the_connection_accumulated() {
        let mut running = start(
            |input| input.with_receive(no_flush_timer()),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;
        let stream = running.connect().await;
        let mut stream = stream;
        stream.write_all(line("reset.path").as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        #[allow(deprecated)]
        stream.set_linger(Some(Duration::ZERO)).expect("SO_LINGER should be settable on loopback");
        drop(stream); // RST, not FIN

        let batch = running.next_batch("the flush after a reset connection").await;
        assert_eq!(
            resolve(batch.events[0].metrics[0].name),
            "reset.path",
            "a read error must not swallow what was already decoded"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    // -- TCP: pickle framing ----------------------------------------------------------------

    /// The pickle framing round trip through a real socket: one length-prefixed frame of three
    /// datapoints, written in two pieces so the 4-byte big-endian prefix itself straddles a read.
    /// A reader that assumed a frame arrives whole -- or read the prefix little-endian -- fails
    /// this and passes a single-write test.
    #[tokio::test]
    async fn a_pickle_frame_split_across_writes_is_reassembled() {
        let mut running = start(|input| input, Transport::Tcp, Protocol::Pickle).await;
        let mut stream = running.connect().await;
        let frame = pickle_frame(&[
            ("pickled.a", 1_700_000_000, 1.5),
            ("pickled.b", 1_700_000_001, 2.5),
            ("pickled.c", 1_700_000_002, 3.5),
        ]);
        stream.write_all(&frame[..2]).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        stream.write_all(&frame[2..]).await.unwrap();

        let batch = running.next_batch("the reassembled pickle frame").await;
        let names: Vec<&str> = batch.events.iter().map(|e| resolve(e.metrics[0].name)).collect();
        assert_eq!(names, ["pickled.a", "pickled.b", "pickled.c"]);
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::Gauge(1.5));
        assert_eq!(batch.events[2].timestamp, 1_700_000_002_000_000_000);

        let drained = running.registry.drain(0);
        assert_eq!(metric_sum(&drained, "logit.input.frames", None), 1.0, "one frame, not three");
        assert_eq!(
            metric_sum(&drained, "logit.input.frame.bytes", None),
            (frame.len() - logit_proto::graphite::pickle::LENGTH_PREFIX_BYTES) as f64,
            "the payload the decoder was handed -- the shared driver counts the frame, not the \
             length prefix it stripped to find it (`logit.input.frame.bytes` means the same thing \
             on every listener on that driver)"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// An oversize pickle frame closes the connection rather than skipping it: a length-framed
    /// stream has no resync point, so there is nothing to skip *to* (this module's "Framing"
    /// table). Asserted on the socket, not just on a counter -- "closes the connection" is the
    /// behaviour a sender actually observes.
    #[tokio::test]
    async fn an_oversize_pickle_frame_closes_the_connection() {
        let running =
            start(|input| input.with_max_frame_bytes(64), Transport::Tcp, Protocol::Pickle).await;
        let mut stream = running.connect().await;
        stream.write_all(&1_000_000u32.to_be_bytes()).await.unwrap();

        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
            .await
            .expect("the connection should be closed, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up rather than waiting for a megabyte");
        assert!(!running.handle.is_finished(), "and the listener itself is untouched");

        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.input.frames.dropped", Some(("reason", "oversize"))),
            1.0,
            "and the refusal is counted, not just acted on"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    // -- TCP: TLS ---------------------------------------------------------------------------
    //
    // `graphite_in` terminates TLS through the shared driver, so these are the *wiring* tests --
    // that `with_tls` reaches `TcpListener::with_tls` and that a carbon payload survives the
    // wrapper. The driver's own tests (`crate::tcp`) cover mTLS, the client-certificate cases and
    // the handshake's own timeout.

    fn testdata_dir() -> std::path::PathBuf {
        // `logit-inputs` lives at `crates/logit-inputs`; the fixtures live at the repo root's
        // `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`,
        // the same path `crate::tcp`/`crate::logit`'s own TLS tests use.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings() -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        }
    }

    /// A `tokio-rustls` client trusting exactly `ca_file` under `testdata/tls` --
    /// `other-ca.pem` is what makes the "wrong CA" test a real trust failure rather than a
    /// certificate-name mismatch.
    async fn tls_connector(ca_file: &str) -> tokio_rustls::TlsConnector {
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<rustls_pki_types::CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_file_iter(testdata_dir().join(ca_file))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        roots.add_parsable_certificates(ca);
        let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(cfg))
    }

    /// `testdata/tls/server.pem` carries a `localhost` SAN (`testdata/tls/README.md`), so that is
    /// the name every TLS client here presents.
    fn server_name() -> rustls_pki_types::ServerName<'static> {
        rustls_pki_types::ServerName::try_from("localhost").unwrap()
    }

    /// Carbon plaintext over TLS: the combination `graphite_in` could not express at all before it
    /// moved onto the shared driver.
    #[tokio::test]
    async fn a_tls_connection_round_trips_a_carbon_line() {
        let mut running = start(
            |input| input.with_tls(&test_tls_settings(), &testdata_dir()).expect("tcp takes tls"),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;

        let connector = tls_connector("ca.pem").await;
        let stream = TcpStream::connect(running.addr).await.expect("the listener should accept");
        let mut client =
            tokio::time::timeout(Duration::from_secs(5), connector.connect(server_name(), stream))
                .await
                .expect("the TLS handshake should complete within 5s")
                .expect("the TLS handshake should succeed");
        client.write_all(line("over.tls").as_bytes()).await.unwrap();
        client.flush().await.unwrap();

        let batch = running.next_batch("the line sent over TLS").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "over.tls");
        assert_eq!(batch.events[0].metrics[0].kind, MetricKind::Gauge(1.5));

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// A client that trusts the wrong CA is refused, and -- the half that matters for a relay --
    /// the listener keeps serving everyone else.
    #[tokio::test]
    async fn a_tls_client_trusting_the_wrong_ca_is_refused_and_the_listener_keeps_serving() {
        let mut running = start(
            |input| input.with_tls(&test_tls_settings(), &testdata_dir()).expect("tcp takes tls"),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;

        let wrong = tls_connector("other-ca.pem").await;
        let stream = TcpStream::connect(running.addr).await.expect("the listener should accept");
        let refused =
            tokio::time::timeout(Duration::from_secs(5), wrong.connect(server_name(), stream))
                .await
                .expect("the handshake should resolve within 5s");
        assert!(refused.is_err(), "a client trusting only other-ca.pem must not complete");

        let connector = tls_connector("ca.pem").await;
        let stream = TcpStream::connect(running.addr).await.expect("the listener should accept");
        let mut client =
            tokio::time::timeout(Duration::from_secs(5), connector.connect(server_name(), stream))
                .await
                .expect("the TLS handshake should complete within 5s")
                .expect("the TLS handshake should succeed");
        client.write_all(line("still.serving").as_bytes()).await.unwrap();
        client.flush().await.unwrap();
        let batch = running.next_batch("a line after the refused handshake").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "still.serving");

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// `handshake_timeout:`'s whole purpose, and the gap `graphite_in` carried in
    /// `docs/known-gaps.md` until it moved onto this driver: a client that connects and never
    /// sends a byte must not pin a connection-limit permit. Under `max_connections(1)` the second
    /// client can only be served if the first one's permit genuinely came back.
    #[tokio::test]
    async fn a_silent_connection_releases_its_permit_after_the_handshake_timeout() {
        let mut running = start(
            |input| input.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50)),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;

        // Connected, not a byte sent, and held (not dropped) past the deadline -- so nothing but
        // the deadline itself could free the permit.
        let mut silent = running.connect().await;
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), silent.read(&mut byte))
            .await
            .expect("a silent connection is closed within the handshake timeout, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that said nothing");

        let mut client = running.connect().await;
        client.write_all(line("permit.came.back").as_bytes()).await.unwrap();
        let batch = running.next_batch("a line on the connection after the silent one").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "permit.came.back");

        drop(silent);
        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The `idle_timeout:` twin of the test above, and for the same reason: when the clock fires
    /// and what resets it are the driver's own tests' business
    /// (`docs/adr/idle-connection-timeout.md`); what is under test here is
    /// `GraphiteInput::with_idle_timeout` reaching that driver at all -- a wrapper whose method
    /// did nothing would leave the second client waiting on a permit forever.
    #[tokio::test]
    async fn an_idle_tcp_connection_releases_its_permit_after_the_idle_timeout() {
        let mut running = start(
            |input| {
                input.with_max_connections(1).with_idle_timeout(Some(Duration::from_millis(50)))
            },
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;

        // One datapoint, so the first-byte deadline is behind us and only the idle clock can
        // close this -- then nothing, with the socket held open.
        let mut quiet = running.connect().await;
        quiet.write_all(line("quiet.then.idle").as_bytes()).await.unwrap();
        let batch = running.next_batch("the datapoint before going quiet").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "quiet.then.idle");

        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), quiet.read(&mut byte))
            .await
            .expect("a connection quiet past its idle_timeout is closed, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that went quiet");

        let mut client = running.connect().await;
        client.write_all(line("permit.came.back").as_bytes()).await.unwrap();
        let batch = running.next_batch("a line on the connection after the quiet one").await;
        assert_eq!(resolve(batch.events[0].metrics[0].name), "permit.came.back");

        drop(quiet);
        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    // ---- recorded interop fixtures (testdata/interop/graphite/) --------------------------------
    //
    // Real bytes from real producers -- not this codec's own encoder, not a hand-built socket
    // write -- recorded by `script/record-fixtures graphite`: W4a of
    // `docs/plans/graphite-carbon-relay.md`. See testdata/interop/graphite/README.md for the
    // provenance table and docs/plans/recorded-interop-fixtures.md for why this corpus exists at
    // all.
    //
    // Both producers write `logit-fixture.`-prefixed paths (the collectd config's `Hostname`, and
    // the Python producer's own hard-coded prefix), so every assertion below can check that
    // prefix without caring which producer wrote a given fixture. `crates/logit-inputs/src/
    // collectd.rs`'s own interop tests are the model this follows: assert on **decoded,
    // identifiable values**, never on the fixture's raw bytes.
    //
    // These live here rather than in `logit-proto` beside `GraphiteDecoder`'s own unit tests for
    // the same reason `collectd.rs`'s do: this is the component an operator actually points a
    // real collectd or a real carbon pickle sender at.

    /// Well past the 2038 problem and nothing like these fixtures' own real capture-time
    /// timestamps, so every assertion below is really reading the wire's own `ts` field, not a
    /// receipt-time fallback.
    const INTEROP_RECEIVED_AT: i64 = 1_700_000_000_000_000_000;

    fn interop_fixture(name: &str) -> Bytes {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/graphite")
            .join(name);
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()));
        Bytes::from(raw)
    }

    /// Every `logit.component.diagnostics{key}` the registry saw. Drains, so call it once -- the
    /// same helper shape as `crates/logit-inputs/src/collectd.rs`'s `diagnostic_keys`.
    fn interop_diagnostic_keys(registry: &Registry) -> Vec<String> {
        let drained = registry.drain(0);
        drained
            .iter()
            .filter(|event| {
                event.metrics.iter().any(|m| resolve(m.name) == "logit.component.diagnostics")
            })
            .filter_map(|event| {
                event.attributes.get("key").and_then(Value::as_str).map(str::to_owned)
            })
            .collect()
    }

    fn interop_decoder(protocol: Protocol) -> (GraphiteDecoder, Arc<Registry>) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("graphite_in", "graphite_in", "listener");
        let diag = Diagnostics::new("graphite_in").with_telemetry(telemetry.clone());
        let decoder = GraphiteDecoder::new(Arc::new(Resource::default()))
            .with_protocol(protocol)
            .with_diagnostics(diag)
            .with_telemetry(telemetry);
        (decoder, registry)
    }

    /// Decodes a whole recorded TCP connection's plaintext stream as **one buffer**.
    ///
    /// A live connection now frames first and hands `decode_into` one line at a time (this
    /// module's "Framing" section), but `decode_plaintext` (`logit_proto::graphite::decode`)
    /// splits on `\n` internally either way, so the decoded events are identical and this stays
    /// the honest shape for a fixture: a raw capture records what actually arrived on the wire,
    /// and what arrived was one connection's stream, not one file per line. What it exercises is
    /// the *decoder* against real `write_graphite` bytes; the socket test
    /// `a_tcp_line_split_across_writes_is_reassembled` and the driver's own framer tests cover
    /// the framing.
    fn decode_interop_plaintext(name: &str) -> (Vec<Event>, Arc<Registry>) {
        let (mut decoder, registry) = interop_decoder(Protocol::Plaintext);
        let mut events = Vec::new();
        decoder
            .decode_into(interop_fixture(name), INTEROP_RECEIVED_AT, &mut events)
            .unwrap_or_else(|e| {
                panic!("{name} is a real carbon plaintext stream and must decode: {e}")
            });
        (events, registry)
    }

    /// Decodes every length-prefixed pickle frame a recorded connection holds.
    ///
    /// `GraphiteDecoder::decode_into`'s pickle path expects one already-**unframed** payload per
    /// call -- framing is the listener's job (`logit_proto::graphite::decode`'s module doc) -- so
    /// this strips each 4-byte big-endian length prefix itself, the same way
    /// `crate::tcp::Framer`'s `next_length_prefixed` does on a live connection.
    /// Looping rather than assuming exactly one frame is what makes this correct even if a future
    /// re-record's connection ever carries more than one (today's fixtures each hold exactly one).
    fn decode_interop_pickle(name: &str) -> (Vec<Event>, Arc<Registry>) {
        let (mut decoder, registry) = interop_decoder(Protocol::Pickle);
        let raw = interop_fixture(name);
        let mut events = Vec::new();
        let mut offset = 0;
        while offset < raw.len() {
            assert!(raw.len() - offset >= 4, "{name}: a truncated pickle length prefix");
            let mut prefix = [0u8; 4];
            prefix.copy_from_slice(&raw[offset..offset + 4]);
            let frame_len = u32::from_be_bytes(prefix) as usize;
            offset += 4;
            assert!(raw.len() - offset >= frame_len, "{name}: a truncated pickle frame payload");
            let payload = raw.slice(offset..offset + frame_len);
            offset += frame_len;
            decoder
                .decode_into(payload, INTEROP_RECEIVED_AT, &mut events)
                .unwrap_or_else(|e| panic!("{name} is a real pickle frame and must decode: {e}"));
        }
        (events, registry)
    }

    /// `tools/record-fixtures/collectd-write-graphite.conf`'s real collectd, sending real carbon
    /// plaintext lines through its `write_graphite` plugin -- not this codec's own encoder.
    /// `write_graphite`'s lines are `\r\n`-terminated (Twisted's `LineReceiver` default
    /// delimiter), so this also exercises normalization 9 (CRLF -> LF) against a real sender
    /// rather than a hand-built one.
    #[test]
    fn interop_fixture_write_graphite_plaintext_decodes() {
        let (events, registry) = decode_interop_plaintext("write-graphite-000.raw");
        assert!(!events.is_empty(), "a recorded write_graphite connection carries datapoints");
        assert_eq!(
            interop_diagnostic_keys(&registry),
            Vec::<String>::new(),
            "a real collectd write_graphite connection must decode with no bad_line/bad_tag/\
             bad_timestamp/non_finite_value diagnostic"
        );
        for event in &events {
            assert_eq!(event.metrics.len(), 1, "one line is one event with one metric");
            let name = resolve(event.metrics[0].name);
            assert!(
                name.starts_with("logit-fixture."),
                "tools/record-fixtures/collectd-write-graphite.conf sets `Hostname \
                 \"logit-fixture\"`, got {name:?}"
            );
            assert!(
                matches!(event.metrics[0].kind, MetricKind::Gauge(_)),
                "carbon's wire has no type, so every datapoint decodes to a bare Gauge, got {:?}",
                event.metrics[0].kind
            );
        }
    }

    /// Both pickle fixtures pickle the exact same `tools/record-fixtures/
    /// python_graphite_pickle_producer.py::DATAPOINTS` list, just at a different protocol -- so
    /// both must decode to identical events whether the wire opcodes are protocol 2's plain
    /// `BINUNICODE`/`BININT`/`BINFLOAT` or protocol 5's `FRAME`/`SHORT_BINUNICODE`/`MEMOIZE`
    /// wrapping the same values. Asserting the exact decoded datapoints (not just "some events
    /// arrived") is what actually checks the restricted reader against real CPython pickle output
    /// rather than against this crate's own writer.
    fn assert_pickle_fixture_decodes(name: &str) {
        let (events, registry) = decode_interop_pickle(name);
        assert_eq!(
            interop_diagnostic_keys(&registry),
            Vec::<String>::new(),
            "{name}: a real CPython pickle frame must decode with no bad_shape/non_finite_value/\
             bad_timestamp diagnostic"
        );
        let got: Vec<(&str, i64, MetricKind)> = events
            .iter()
            .map(|e| {
                assert_eq!(e.metrics.len(), 1, "one pickle datapoint is one event with one metric");
                (resolve(e.metrics[0].name), e.timestamp, e.metrics[0].kind.clone())
            })
            .collect();
        // Exactly `python_graphite_pickle_producer.py`'s `DATAPOINTS`, in order (pickle's own
        // list preserves send order, and `graphite_in` never reorders within one frame): an `int`
        // timestamp/value pair, a fractional timestamp with a float value, a negative float
        // value, and a large float value -- covering the int/float encoding split pickle itself
        // makes (`BININT1`/`BININT` vs. `BINFLOAT`) on both fields independently.
        assert_eq!(
            got,
            vec![
                (
                    "logit-fixture.pickle.int_value",
                    1_700_000_000_000_000_000,
                    MetricKind::Gauge(42.0)
                ),
                (
                    "logit-fixture.pickle.float_value",
                    1_700_000_001_500_000_000,
                    MetricKind::Gauge(12.75)
                ),
                (
                    "logit-fixture.pickle.negative_value",
                    1_700_000_002_000_000_000,
                    MetricKind::Gauge(-17.5)
                ),
                (
                    "logit-fixture.pickle.large_value",
                    1_700_000_003_000_000_000,
                    MetricKind::Gauge(1_234_567.0)
                ),
            ],
            "{name}: must decode to exactly python_graphite_pickle_producer.py's DATAPOINTS"
        );
    }

    #[test]
    fn interop_fixture_pickle_protocol_2_decodes() {
        assert_pickle_fixture_decodes("graphite-pickle-p2-000.raw");
    }

    #[test]
    fn interop_fixture_pickle_protocol_5_decodes() {
        assert_pickle_fixture_decodes("graphite-pickle-p5-000.raw");
    }
}
