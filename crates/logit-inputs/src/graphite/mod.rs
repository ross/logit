//! Graphite/Carbon metric ingress: the listener half of
//! [ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md)'s
//! `graphite_in -> graphite_out` lossless-relay pair.
//!
//! The wire grammar, the model mapping, and the permitted normalizations are the codec's, in
//! [`logit_proto::graphite`]'s module doc. This doc covers the component: its configuration,
//! socket behaviour, and reporting.
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
//! `transport:` picks one of two shared drivers; this type adds the decoder choice and the
//! builder surface `logit-cli::pipeline` calls, as [`crate::syslog::SyslogInput`] does.
//!
//! **`udp`** wraps [`UdpListener<GraphiteDecoder>`](crate::udp::UdpListener), as
//! [`crate::collectd::CollectdInput`] does. The read/decode split, receive queue, batch assembly,
//! `SO_RCVBUF`, multicast auto-join, and shutdown drain are the driver's
//! (`docs/adr/decoupled-listener-io.md`), and the whole `receive:` block applies. Graph rule 46
//! rejects `protocol: pickle` here: a 4-byte length prefix means nothing in a datagram that
//! already delimits itself.
//!
//! **`tcp`** wraps [`TcpListener<GraphiteDecoder>`](crate::tcp::TcpListener), `syslog_in`'s stream
//! driver (`docs/adr/syslog-tcp-ingress-and-tls.md`). The accept loop, connection cap,
//! per-connection decoder clone and batch assembly, first-byte deadline, and TLS termination are
//! the driver's; this component picks the framing mode and its bound (see "Framing"). There is no
//! [`ReceiveQueue`](crate::udp::ReceiveQueue): TCP's flow control is the backpressure, and a
//! stream can't drop silently the way a UDP socket does. So only `receive:`'s batch-assembly
//! fields (`batch_max_events`, `batch_max_bytes`, `batch_flush_interval`) and `shutdown_grace`
//! apply; graph rule 17 rejects the queue-bounding ones by name.
//!
//! Each connection owns a read buffer, its own [`GraphiteDecoder`] clone and
//! [`logit_pipeline::BatchAccumulator`], and a clone of the shared [`Fanout`], so a slow or hostile
//! connection stalls only itself. All of them share one `Arc<Resource>` (`GraphiteDecoder`'s
//! `Clone` impl says why) so batches from different connections still merge downstream:
//! `BatchAccumulator::absorb` keys on `Arc::ptr_eq`.
//!
//! ## Framing
//!
//! Framing is the listener's job, not the decoder's: the driver's [`Framer`](crate::tcp::Framer)
//! runs under a [`FramingMode`] chosen from `protocol:`. **Never [`FramingMode::Rfc6587Auto`]**: a
//! carbon path may begin with a digit, which that mode would read as an RFC 6587 octet count and
//! reframe the whole connection on.
//!
//! | Protocol | Frame | Over the bound |
//! |---|---|---|
//! | plaintext, UDP | the datagram | nothing to bound: a datagram is already one read |
//! | plaintext, TCP | [`FramingMode::Lines`] with [`Oversize::DrainToNextLine`]: one `\n`-delimited line (a trailing `\r` stripped) per `decode_into` | a line past `max_line_bytes` is dropped and counted **once** as `logit.input.frames.dropped{reason="oversize"}`; the connection stays open and the next line still decodes |
//! | pickle, TCP | [`FramingMode::LengthPrefixed`]: a 4-byte big-endian length prefix then that many payload bytes, handed to `decode_into` unframed | a frame declaring more than `max_frame_bytes` **closes the connection** (`logit.input.frames.dropped{reason="oversize"}`, diagnostic `framing_error`): a length-framed stream has no resync point |
//!
//! A pickle payload that fails to decode (a disallowed opcode, a depth or item cap) drops that
//! frame and keeps the connection: a decoded length has already said where the next frame starts.
//!
//! **A terminator-less line at EOF is dropped, not ingested.** Carbon's `\n` is the only signal a
//! line is complete, so a sender that dies mid-line leaves a truncation. Without its newline,
//! `svc.web01.cpu 42.5 17000` would parse cleanly and produce a gauge stamped 1970.
//! [`FramingMode::Lines`] makes [`crate::tcp::Framer::finish`] return `Truncated` (counted
//! `logit.input.frames.dropped{reason="truncated"}`), so a clean FIN and an abrupt RST agree about
//! identical bytes. [`FramingMode::Rfc6587Auto`] does the opposite, because RFC 6587 §3.4.2 says a
//! final syslog message needs no terminator.
//!
//! **One `decode_into` per line, not per read**, on carbon's hottest path: one `Arc` clone and one
//! `absorb` per line. The ADR's "`graphite_in` TCP is now `logit_inputs::tcp`" section records the
//! cost, and a `LineChunk` framing mode as the fix if it stops being acceptable.
//!
//! ## Connections
//!
//! The driver's: 1024 at a time, the permit taken non-blockingly after `accept`, and a
//! past-the-cap connection closed immediately and counted
//! `logit.input.connections.rejected{reason="limit"}` (carbon's wire has no way to say "try
//! later"). `handshake_timeout:` bounds each pre-message phase independently (the TLS accept when
//! `tls:` is set, then the wait for the first byte) and is **not** an idle timeout. The gaps after
//! the first byte are bounded by the opt-in `idle_timeout:` (`docs/adr/idle-connection-timeout.md`,
//! and the driver module's "Idle timeout" section).
//!
//! ## Diagnostics
//!
//! Every one is throttled (`logit.component.diagnostics{key}`,
//! `docs/design/internal-telemetry.md`), and clones of this component's [`Diagnostics`] share one
//! set of counts, so a key throttles per listener, not per connection. The decoder's own
//! (`bad_line`, `bad_tag`, `bad_timestamp`, `non_finite_value`, `duplicate_tag_key`, `bad_pickle`)
//! reach it through [`GraphiteInput::with_diagnostics`]. The drivers add:
//!
//! | Key | Meaning |
//! |---|---|
//! | `bound` | info: the socket is open (the shared pre-bind pass, `docs/deploying.md`) |
//! | `framing_error` | TCP: a line past `max_line_bytes` (skipped, connection kept), a pickle frame declaring more than `max_frame_bytes` (connection closed), or a partial frame discarded by an abrupt close |
//! | `bad_frame` | TCP: a framed payload the decoder rejected outright; pickle only, since the plaintext path isolates every failure per line and never returns `Err` |
//! | `connection_error` | one connection's I/O failed; never fatal to the listener or its siblings |
//! | `bad_datagram` | UDP only: a whole datagram that failed to decode |
//!
//! ## Telemetry
//!
//! All from the shared drivers; this component adds none. Under `transport: udp`:
//! `logit.input.datagrams`/`.datagram.bytes`, the `logit.component.receive.*` queue gauges, and
//! `logit.input.receive_buffer.bytes`. Under `transport: tcp`: `logit.input.connections` (gauge),
//! `logit.input.connections.rejected{reason="limit"}`, `logit.input.frames`/`.frame.bytes` (a
//! frame is one plaintext line or one pickle payload; the prefix's 4 bytes aren't counted),
//! `logit.input.frames.dropped{reason}`, and `logit.component.receive.flushed{reason}`. The
//! decoder's `logit.input.metrics.skipped{reason}` is reported under both.

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
/// Not `logit_config::GraphiteTransport` because `logit-inputs` never depends on `logit-config`
/// (`docs/design/pipeline-graph.md`'s "Crate layout"), as with [`crate::otlp::OtlpTransport`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    #[default]
    Tcp,
    Udp,
}

/// Which driver a [`GraphiteInput`] wraps, fixed by `transport:` at construction. An enum rather
/// than a `Box<dyn Input>` so each arm's concrete builder surface ([`TcpListener::with_tls`],
/// [`UdpListener::with_config`]) stays reachable, as in [`crate::syslog::SyslogInput`].
enum Inner {
    Udp(UdpListener<GraphiteDecoder>),
    Tcp(TcpListener<GraphiteDecoder>),
}

/// A carbon receiver: plaintext or pickle, over TCP or UDP. See this module's doc.
pub struct GraphiteInput {
    inner: Inner,
    /// Picks the TCP framing mode; the decoder's own copy isn't reachable through the driver.
    protocol: Protocol,
    /// The frame bounds, applied to the stream driver in [`Input::bind`] rather than when set:
    /// both bounds and the protocol decide one `set_framing` call, and applying it eagerly would
    /// make the order of the builder calls matter.
    max_line_bytes: usize,
    max_frame_bytes: usize,
    /// A copy of what was pushed into the driver, so [`Self::receive_config`] reads it back
    /// without knowing which driver is in play.
    receive: UdpListenerConfig,
}

impl GraphiteInput {
    /// A listener on `bind` speaking `protocol` over `transport`.
    ///
    /// Graph rule 46 rejects pickle over UDP; a direct caller that builds one anyway gets a
    /// pickle decoder fed whole datagrams, which is meaningful enough not to panic over.
    pub fn new(bind: impl Into<String>, transport: Transport, protocol: Protocol) -> Self {
        // One resource for the whole component, never one per connection:
        // `BatchAccumulator::absorb` keys on `Arc::ptr_eq`, so per-connection resources would stop
        // connections sharing a batch downstream. `GraphiteDecoder`'s `Clone` impl preserves it
        // across the driver's per-connection clone.
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

    /// Attaches a component id to the driver's diagnostics and to the [`GraphiteDecoder`]'s.
    ///
    /// They are two distinct `Diagnostics` values: the driver's carries transport failures
    /// (`bad_datagram` on UDP; `framing_error`/`bad_frame`/`connection_error` on TCP), the
    /// decoder's the finer-grained ones (`bad_line`, `bad_pickle`, ...). Setting only one leaves a
    /// whole class of failure reporting under no component id. On TCP every connection clones the
    /// decoder set here, and a `Diagnostics` clone shares its throttle counts, so a key throttles
    /// per listener.
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

    /// Attaches a telemetry handle to the driver (datagram or connection/frame counters) and to
    /// the decoder (skip counters).
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

    /// Sets the `receive:` block: batch assembly and shutdown grace, plus the receive queue under
    /// UDP. Defaults to [`UdpListenerConfig::default`].
    ///
    /// One setter for both transports, unlike [`crate::syslog::SyslogInput::with_receive`]: the
    /// three fields TCP reads mean the same in both structs, and graph rule 17 keeps the
    /// queue-bounding fields off a TCP listener, so the TCP arm ignores nothing it was given.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        self.receive = config;
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(listener.with_config(config)),
            Inner::Tcp(listener) => Inner::Tcp(listener.with_config(TcpListenerConfig {
                batch_max_events: config.batch_max_events,
                batch_max_bytes: config.batch_max_bytes,
                batch_flush_interval: config.batch_flush_interval,
            })),
        };
        self
    }

    /// The configured `receive:` knobs, for `logit-cli::pipeline`'s wiring tests.
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

    /// Sets a TCP listener's per-phase pre-message budget (`handshake_timeout:`): the TLS accept
    /// when `tls:` is set, then the wait for the first byte. See
    /// [`TcpListener::with_handshake_timeout`].
    ///
    /// A no-op under UDP, which has no connection to bound; graph rule 45 rejects a non-default
    /// value there. [`Self::with_tls`] fails instead, because `tls:` has no default and its
    /// presence is an instruction.
    pub fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_handshake_timeout(handshake_timeout));
        }
        self
    }

    /// Bounds how long a TCP connection may stay quiet after its first byte (`idle_timeout:`)
    /// before it is closed and its permit returned; `None` (the default) disables it. See
    /// [`TcpListener::with_idle_timeout`] for what resets the clock.
    ///
    /// A no-op under UDP, which has no connection to time out; graph rule 53 rejects the field
    /// there.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_idle_timeout(idle_timeout));
        }
        self
    }

    /// Terminates TLS on a TCP listener (`tls:`); [`TcpListener::with_tls`] resolves every path
    /// in `settings` against `base_dir`.
    ///
    /// Fails under UDP: carbon has no DTLS receiver. Graph rule 43 is what an operator sees; this
    /// arm backstops a caller that skipped validation.
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

    /// The bound address once [`Input::bind`] has run, so a test learns an OS-assigned port with
    /// no bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.inner {
            Inner::Udp(listener) => listener.local_addr(),
            Inner::Tcp(listener) => listener.local_addr(),
        }
    }

    /// Lowers the driver's connection cap so a test reaches it with two connections, not 1025.
    /// A no-op under UDP.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_max_connections(max_connections));
        }
        self
    }

    /// The TCP framing mode and its bound, from `protocol:`. Never [`FramingMode::Rfc6587Auto`]:
    /// see this module's "Framing" section.
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
    /// Idempotent, per `Input::bind`'s contract. Also where the framing reaches the stream driver
    /// (see the `max_line_bytes` field).
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
        // Unused in production: `run_input` calls `run_until_shutdown`. The trait requires it.
        let (_tx, rx) = watch::channel(false);
        self.run_until_shutdown(sink, rx).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        // The driver's `run_until_shutdown` binds too, but only this `bind` applies the framing;
        // without it an unbound caller would get the driver's `Rfc6587Auto` default.
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

    /// Nothing like a receipt time, so an assertion on it is reading the wire's timestamp.
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
        /// The next delivered batch, or a panic naming what was awaited. Five seconds is long
        /// enough for a loaded CI box and short enough that a hang fails instead of stalling.
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

    /// Binds `input` on an ephemeral port and runs it, with one registry behind both its
    /// telemetry and its diagnostics.
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

    /// A `receive:` block with no flush timer, so only a bound or shutdown completes a batch.
    fn no_flush_timer() -> UdpListenerConfig {
        UdpListenerConfig { batch_flush_interval: Duration::ZERO, ..UdpListenerConfig::default() }
    }

    /// The sum of every point named `metric` in `events`, optionally narrowed to one tag. Takes
    /// the drained slice, not the `Registry`, because `drain` empties it.
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

    /// One length-prefixed pickle frame carrying `datapoints`, built with the codec's writer:
    /// these tests are about framing, and a hand-rolled payload would only re-test `pickle.rs`.
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

    /// A three-line datagram is one batch of three gauges carrying the wire's timestamp.
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

    /// Under UDP, `receive:` batch assembly accumulates two datagrams into one batch.
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

    /// `receive_config` reads back what `with_receive` set, under both transports.
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

    /// `with_diagnostics` reaches the decoder too; dropping `.map_decoder(..)` still compiles.
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

    /// `Input::bind`'s contract: live before `run`, idempotent, and the real address left behind.
    #[tokio::test]
    async fn tcp_bind_makes_the_port_live_before_run_and_is_idempotent() {
        let mut input = GraphiteInput::new("127.0.0.1:0", Transport::Tcp, Protocol::Plaintext);
        assert_eq!(input.local_addr(), None, "no address before bind()");
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        TcpStream::connect(addr).await.expect("the port should already be accepting");

        input.bind().await.expect("a second bind() is a no-op, per Input::bind's contract");
        assert_eq!(input.local_addr(), Some(addr), "and it does not rebind to a new port");
    }

    /// Binding a port this test already holds fails `bind()` with an address error.
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

    /// Two concurrent connections both deliver, which a one-at-a-time listener would fail.
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

    /// A line split across two writes is reassembled; the split falls before `.5`, so decoding
    /// the partial buffer would yield a different valid number rather than an error.
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

    /// A line past `max_line_bytes` is counted once and the next line still decodes.
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

    /// A connection closing leaves the listener accepting.
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

    /// A connection past the cap is closed immediately and counted.
    #[tokio::test]
    async fn a_tcp_connection_past_the_cap_is_rejected_and_counted() {
        let mut running =
            start(|input| input.with_max_connections(1), Transport::Tcp, Protocol::Plaintext).await;
        let mut held = running.connect().await;
        held.write_all(line("holds.the.permit").as_bytes()).await.unwrap();
        // Waiting for its batch guarantees the permit is taken before the second connection races
        // the accept loop for it.
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

    /// Diagnostics and telemetry survive the per-connection decoder clone made at accept time.
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

    /// Shutdown flushes a connection's accumulated batch, counted `reason="shutdown"`.
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

    /// A client hanging up flushes as `reason="closed"`, not `"shutdown"`.
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
        drop(stream); // a clean FIN

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

    /// A terminator-less tail at a clean FIN is dropped and counted `truncated`, as an RST's is.
    /// The tail parses as a valid 1970 gauge, which is why this is worth a socket test.
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

    /// A read error (an RST, via `SO_LINGER 0`) still flushes what the connection accumulated.
    /// The sleep lets the listener read the line first: an RST discards the receive buffer.
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

    /// A pickle frame whose 4-byte prefix straddles two writes is reassembled into one frame.
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

    /// An oversize pickle frame closes the connection, observed on the socket, and is counted.
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
    // Wiring tests only: `crate::tcp`'s own tests cover mTLS, client certificates, and the
    // handshake timeout.

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`).
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn test_tls_settings() -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        }
    }

    /// A client trusting only `ca_file` under `testdata/tls`; `other-ca.pem` makes a real trust
    /// failure rather than a name mismatch.
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

    /// `testdata/tls/server.pem`'s SAN.
    fn server_name() -> rustls_pki_types::ServerName<'static> {
        rustls_pki_types::ServerName::try_from("localhost").unwrap()
    }

    /// A carbon plaintext line round-trips over TLS.
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

    /// A client trusting the wrong CA is refused and the listener keeps serving others.
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

    /// A silent client's permit comes back after `handshake_timeout:`, so under a cap of one a
    /// second client is served.
    #[tokio::test]
    async fn a_silent_connection_releases_its_permit_after_the_handshake_timeout() {
        let mut running = start(
            |input| input.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50)),
            Transport::Tcp,
            Protocol::Plaintext,
        )
        .await;

        // Held open without a byte sent, so only the deadline can free the permit.
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

    /// `with_idle_timeout` reaches the driver: a connection quiet past it gives its permit back.
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

        // One datapoint gets past the first-byte deadline, so only the idle clock can close this.
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
    // Real producers' bytes, recorded by `script/record-fixtures graphite`; provenance in
    // testdata/interop/graphite/README.md, rationale in docs/plans/recorded-interop-fixtures.md.
    // Both producers write `logit-fixture.`-prefixed paths (collectd's `Hostname`, the Python
    // producer's hard-coded prefix). Assertions are on decoded values, never raw bytes. They
    // live here, not in `logit-proto`, because this is the component an operator points a real
    // carbon sender at.

    /// A receipt time unlike `write_graphite`'s capture-time stamps. The pickle fixtures' first
    /// datapoint happens to share it; the rest differ, so the pickle assertion still reads `ts`.
    const INTEROP_RECEIVED_AT: i64 = 1_700_000_000_000_000_000;

    fn interop_fixture(name: &str) -> Bytes {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/graphite")
            .join(name);
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()));
        Bytes::from(raw)
    }

    /// Every `logit.component.diagnostics{key}` the registry saw. Drains, so call it once.
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

    /// Decodes a recorded connection's plaintext stream as one buffer. A live connection feeds
    /// one line per call, but `decode_plaintext` splits on `\n` either way, so the events match.
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

    /// Decodes every length-prefixed pickle frame a recorded connection holds, stripping each
    /// prefix as `crate::tcp::Framer` does: the decoder expects one unframed payload per call.
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

    /// Real collectd `write_graphite` output (`tools/record-fixtures/collectd-write-graphite.conf`)
    /// decodes cleanly; its `\r\n` terminators exercise normalization 9 (CRLF to LF).
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

    /// Asserts a pickle fixture decodes to exactly
    /// `tools/record-fixtures/python_graphite_pickle_producer.py`'s `DATAPOINTS`. The protocol 2
    /// and 5 fixtures pickle the same list with different opcodes, so both must match.
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
        // In send order; the four cover pickle's int/float encoding split (`BININT1`/`BININT` vs.
        // `BINFLOAT`) on timestamp and value independently.
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
