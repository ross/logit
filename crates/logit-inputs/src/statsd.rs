//! statsd / DogStatsD-tagged metrics over UDP, TCP, or a Unix socket: the `statsd_in` listener, the
//! input half of the `statsd_in -> statsd_out` lossless-relay pair (`docs/adr/lossless-transit.md`;
//! the mirror is `docs/adr/statsd-output.md`).
//!
//! This module is the listener: transports, framing, and telemetry. The line grammar and what
//! each line shape maps to on an event are [`StatsdDecoder`]'s, in `logit_proto::statsd`'s module
//! doc (`crates/logit-proto/src/statsd/mod.rs`).
//!
//! **The driver adds sender attributes after decode.** Under `peer:` or `proxy_protocol:`,
//! `network.peer.*` or `client.*` is added to every event, an opt-in addition the sender never
//! sent, and one of ADR `lossless-transit`'s "Permitted normalizations". [`StatsdDecoder`] never
//! sees a peer.
//!
//! ## Transports
//!
//! One component, two shared drivers, chosen by `transport:`; each serves an IP socket or a Unix
//! one. This type is the decoder choice plus
//! the builder surface `logit-cli::pipeline` and these tests use, as
//! [`crate::syslog::SyslogInput`] and [`crate::graphite::GraphiteInput`] are.
//!
//! | `transport:` | Driver | What it brings |
//! |---|---|---|
//! | `udp` (the default) | [`UdpListener<StatsdDecoder>`](crate::udp::UdpListener) | the read/decode split, the receive queue, datagram->batch assembly, `SO_RCVBUF` (`docs/adr/decoupled-listener-io.md`); the whole `receive:` block applies |
//! | `tcp` | [`TcpListener<StatsdDecoder>`](crate::tcp::TcpListener) | an accept loop, the `max_connections:` cap, a per-connection decoder clone and batch accumulator, the first-byte deadline, and, with a `tls:` block, TLS termination (`docs/adr/syslog-tcp-ingress-and-tls.md`) |
//! | `unix` | [`UdpListener::unix`](crate::udp::UdpListener::unix) | everything `udp` brings, on a `SOCK_DGRAM` Unix socket: the Datadog Agent's `dogstatsd_socket` |
//! | `unix_stream` | [`TcpListener::unix`](crate::tcp::TcpListener::unix) | everything `tcp` brings but TLS and the accept-queue gauges, on a `SOCK_STREAM` Unix socket: the Agent's `dogstatsd_stream_socket` |
//!
//! Under both Unix transports `bind:` is the socket's path. [`crate::unix`] prepares it (the
//! directory must exist, a stale socket is replaced, anything else is refused) and the file is made
//! mode `socket_mode:` ([`StatsdInput::with_socket_mode`]), `0722` by default. The file isn't
//! removed on shutdown. ADR `datadog-agent-and-intake-relay`, decision 12, has why the path lives
//! in `bind:` and why the default mode is `0722`.
//!
//! A TCP listener has **no [`ReceiveQueue`](crate::udp::ReceiveQueue)**: TCP's flow control is
//! the backpressure, and ADR `decoupled-listener-io` exists for UDP's silent drops, which a stream
//! cannot have. So only `receive:`'s batch-assembly fields (`batch_max_events`,
//! `batch_max_bytes`, `batch_flush_interval`) and `shutdown_grace` apply to one; graph rule 17
//! rejects the queue fields and `read_batch` by name.
//!
//! There is no statsd-over-TCP specification. The Etsy reference server, the Datadog agent and
//! every TCP-capable client speak the line grammar in `logit_proto::statsd`'s module doc,
//! LF-delimited on a stream; that is what this listener accepts and what `statsd_out`'s
//! `transport: tcp` emits.
//!
//! ## Framing
//!
//! **`unix_stream` is length-prefixed, not LF-delimited**: [`FramingMode::LengthPrefixedLe`], a
//! 4-byte little-endian length and then one packet, which decodes as one datagram does (any number
//! of newline-separated lines), as the `datadog` Python client writes it to a real Agent's socket
//! (`testdata/interop/datadog/README.md`; `crates/logit-proto/src/statsd/decode.rs`'s
//! `interop_fixture_a_unix_stream_capture_*` replays it). A packet declaring more than
//! [`MAX_FRAME_BYTES`](logit_proto::framing::MAX_FRAME_BYTES) closes the connection, counted
//! `logit.input.frames.dropped{reason="oversize"}`: a length-framed stream has no resync point.
//! The rest of this section is `tcp`'s.
//!
//! **Under `tcp`, LF-delimited lines, always**: [`FramingMode::Lines`] with [`Oversize::DrainToNextLine`],
//! never [`FramingMode::Rfc6587Auto`]. That mode reads a leading ASCII digit as an RFC 6587 octet
//! count, which is right for syslog (every non-transparent message starts `<`) and wrong here:
//! `1.hits:1|c` is an ordinary statsd line, and latching octet counting on it would reframe the
//! whole connection. `a_tcp_line_starting_with_a_digit_is_not_read_as_an_octet_count` pins this.
//!
//! **The LF is the completeness signal, at the end of the stream too.** An unterminated final line
//! on a clean close is not emitted: it is dropped and counted
//! `logit.input.frames.dropped{reason="truncated"}` (diagnostic `framing_error`), the same as an
//! abrupt close or a shutdown mid-line. A whitespace-only remainder (trailing padding, a bare `CR`)
//! is not counted, since nothing was lost. Emitting a half-line would turn a sender dying
//! mid-write into a plausible datapoint with a truncated name or value. This differs from
//! `syslog_in`, because RFC 6587 §3.4.2 permits a terminator-less final message and statsd has no
//! such licence.
//!
//! Oversize is **recoverable**: a line past the driver's 64 KiB
//! [`MAX_FRAME_BYTES`](logit_proto::framing::MAX_FRAME_BYTES) is dropped, counted once as
//! `logit.input.frames.dropped{reason="oversize"}`, and the connection resynchronizes at the next
//! `LF`. `graphite_in` makes the same call for carbon plaintext
//! (`docs/adr/graphite-carbon-relay.md`): one pathological line must not cost every other metric
//! on the connection, and an LF-delimited stream has an unambiguous resync point. There is **no
//! `max_line_bytes` field**: unlike carbon, no statsd server has such a knob for an operator to
//! match.
//!
//! ## Telemetry and diagnostics
//!
//! All of it comes from the shared drivers; this component adds none. Under `transport: udp`:
//! `logit.input.datagrams`/`.datagram.bytes`, the `logit.component.receive.*` queue gauges,
//! `logit.input.receive_buffer.bytes`, and the driver's `bad_datagram` for a whole-datagram decode
//! failure. Under `transport: tcp`: `logit.input.connections` (gauge),
//! `logit.input.connections.rejected{reason="limit"}`, `logit.input.frames`/`.frame.bytes` (one
//! frame is one statsd line), `logit.input.frames.dropped{reason="oversize"|"truncated"}`,
//! `logit.component.receive.flushed{reason}` from the per-connection batch assembly, and
//! `framing_error`/`connection_error` diagnostics. `unix` reports what `udp` does, but a full
//! Unix datagram queue blocks or refuses the sender instead of dropping, so
//! `logit.input.kernel.drops` stays at zero there (`crate::udp`'s module doc). `unix_stream`
//! reports what `tcp` does, less the `logit.input.accept_queue.*` gauges.
//!
//! The decoder's own `bad_line` diagnostic (`logit_proto::statsd`'s module doc, "Malformed input")
//! is the same under both. It throttles per listener, not per connection, because every
//! connection's decoder clone shares one set of [`Diagnostics`] counts (`logit_core::Diagnostics`'
//! type doc). The driver's `bad_frame` fires only for the one whole-frame failure
//! [`decode_into`](logit_proto::Decoder::decode_into) returns, a frame that is not valid UTF-8.

use crate::tcp::{TcpListener, TcpListenerConfig, TlsServerSettings};
use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::framing::{FramingMode, Oversize};
use logit_proto::statsd::StatsdDecoder;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;

/// Which driver a [`StatsdInput`] wraps, chosen once by `transport:`. An enum rather than a
/// `Box<dyn Input>` so each arm's concrete builders ([`TcpListener::with_tls`],
/// [`UdpListener::with_config`]) stay reachable; [`crate::syslog::SyslogInput`] does the same.
/// `Udp` also covers `transport: unix` and `Tcp` covers `unix_stream`: the drivers own the socket
/// family.
enum Inner {
    Udp(UdpListener<StatsdDecoder>),
    Tcp(TcpListener<StatsdDecoder>),
}

/// The `statsd_in` listener: a [`StatsdDecoder`] over [`UdpListener`] or [`TcpListener`].
///
/// All transport behavior lives in the drivers; see this module's "Transports" section.
pub struct StatsdInput {
    inner: Inner,
}

impl StatsdInput {
    /// A UDP listener, the default transport.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Udp(UdpListener::new(
                bind,
                StatsdDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            )),
        }
    }

    /// A TCP listener (`transport: tcp`), plaintext until [`Self::with_tls`] is called.
    ///
    /// Framing is fixed here, at construction: [`FramingMode::Lines`], never
    /// [`FramingMode::Rfc6587Auto`], with oversize draining to the next `LF` (this module's
    /// "Framing" section). Unlike `graphite_in`, framing needn't wait for `bind()`: there is no
    /// `max_line_bytes` field for a later builder to set.
    pub fn tcp(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Tcp(
                TcpListener::new(
                    bind,
                    StatsdDecoder::new(Arc::new(Resource::default())),
                    TcpListenerConfig::default(),
                )
                .with_framing(
                    FramingMode::Lines { oversize: Oversize::DrainToNextLine },
                    logit_proto::framing::MAX_FRAME_BYTES,
                ),
            ),
        }
    }

    /// A listener on a Unix datagram socket at `path` (`transport: unix`), on the UDP driver.
    pub fn unix(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: Inner::Udp(UdpListener::unix(
                "statsd_in",
                path,
                crate::unix::DEFAULT_SOCKET_MODE,
                StatsdDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            )),
        }
    }

    /// A listener on a Unix stream socket at `path` (`transport: unix_stream`), on the stream
    /// driver with [`FramingMode::LengthPrefixedLe`] (this module's "Framing"). [`Self::with_tls`]
    /// fails on it.
    pub fn unix_stream(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: Inner::Tcp(
                TcpListener::unix(
                    "statsd_in",
                    path,
                    crate::unix::DEFAULT_SOCKET_MODE,
                    StatsdDecoder::new(Arc::new(Resource::default())),
                    TcpListenerConfig::default(),
                )
                .with_framing(FramingMode::LengthPrefixedLe, logit_proto::framing::MAX_FRAME_BYTES),
            ),
        }
    }

    /// Attaches a component id to the driver's diagnostics and to the wrapped [`StatsdDecoder`]'s.
    ///
    /// Both must carry it: the driver reports transport failures (`bad_datagram` on UDP;
    /// `framing_error`/`bad_frame`/`connection_error` on TCP) and the decoder reports `bad_line`.
    /// Miss one and that class of failure reports under no component id with telemetry disabled.
    /// On TCP every connection clones this decoder, sharing its throttle counts, so `bad_line`
    /// throttles per listener.
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

    /// Attaches a telemetry handle for the drivers' layer-3 counters
    /// (`docs/design/internal-telemetry.md`): datagrams and bytes on UDP, connections and frames on
    /// TCP, which `Fanout`-level `events.sent` can't tell apart from one busy client.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(listener.with_telemetry(telemetry)),
            Inner::Tcp(listener) => Inner::Tcp(listener.with_telemetry(telemetry)),
        };
        self
    }

    /// Sets a **UDP** listener's `receive:` block (`docs/adr/decoupled-listener-io.md`); leaves a
    /// TCP listener untouched.
    ///
    /// Two transport-specific setters, as [`crate::syslog::SyslogInput::with_receive`] has,
    /// because the configs aren't interchangeable: a TCP listener has no receive queue (graph rule
    /// 17), so one setter would have to decide at runtime what to do with a queue bound it can't
    /// honour. [`Self::with_tcp_receive`] is the counterpart.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        if let Inner::Udp(listener) = self.inner {
            self.inner = Inner::Udp(listener.with_config(config));
        }
        self
    }

    /// [`Self::with_receive`]'s TCP counterpart; leaves a UDP listener untouched.
    pub fn with_tcp_receive(mut self, config: TcpListenerConfig) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_config(config));
        }
        self
    }

    /// Sets a **TCP** listener's per-phase pre-message budget (`handshake_timeout:`): the PROXY
    /// header under `proxy_protocol:`, the TLS accept when `tls:` is set, then the wait for the
    /// first byte (`crate::tcp`'s "Pre-handshake timeout").
    ///
    /// A UDP listener is left untouched rather than failing, since it has no connection to bound;
    /// graph rule 45 rejects a non-default value there. `tls:` differs ([`Self::with_tls`] fails):
    /// it has no default, so its presence is an instruction.
    pub fn with_handshake_timeout(mut self, handshake_timeout: std::time::Duration) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_handshake_timeout(handshake_timeout));
        }
        self
    }

    /// Bounds how long a **TCP** connection may stay quiet past its first byte (`idle_timeout:`)
    /// before it is closed and its permit returned; `None` (the default) disables it. See
    /// `crate::tcp`'s "Idle timeout" for what resets the clock.
    ///
    /// A UDP listener is left untouched, as in [`Self::with_handshake_timeout`]; graph rule 53
    /// rejects the field there.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<std::time::Duration>) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_idle_timeout(idle_timeout));
        }
        self
    }

    /// Stamps each event with the address of the peer that sent it (`peer:`); see
    /// [`crate::peer::PeerAttrs`].
    pub fn with_peer(mut self, peer: bool) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(listener.with_peer(peer)),
            Inner::Tcp(listener) => Inner::Tcp(listener.with_peer(peer)),
        };
        self
    }

    /// Sets `SO_REUSEPORT` on the listening socket (`reuse_port:`); see
    /// [`UdpListener::with_reuse_port`] and [`TcpListener::with_reuse_port`]. Off by default.
    pub fn with_reuse_port(mut self, reuse_port: bool) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(listener.with_reuse_port(reuse_port)),
            Inner::Tcp(listener) => Inner::Tcp(listener.with_reuse_port(reuse_port)),
        };
        self
    }

    /// Requires a PROXY protocol header on every TCP connection (`proxy_protocol:`); see
    /// [`TcpListener::with_proxy_protocol`]. A datagram listener is left untouched, and graph rule
    /// 79 rejects the option there and on a Unix stream socket.
    pub fn with_proxy_protocol(mut self, proxy_protocol: bool) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_proxy_protocol(proxy_protocol));
        }
        self
    }

    /// Terminates TLS on a TCP listener (`tls:`); paths in `settings` resolve against `base_dir`.
    ///
    /// Fails on a UDP listener: DTLS is out of scope (`docs/adr/syslog-tcp-ingress-and-tls.md`'s
    /// Alternatives) and no statsd client speaks it. Fails on either Unix socket too, which is
    /// always plaintext. Graph rules 43 and 65 are what an operator sees; this backstops a caller
    /// that skipped validation.
    ///
    /// Registers the files with `reloader` under this listener's diagnostics and telemetry as
    /// they are when this runs, so call it after `with_diagnostics` and `with_telemetry`.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
        reloader: &logit_pipeline::tls::TlsReloader,
    ) -> anyhow::Result<Self> {
        self.inner = match self.inner {
            Inner::Tcp(listener) => Inner::Tcp(listener.with_tls(settings, base_dir, reloader)?),
            Inner::Udp(_) => anyhow::bail!(
                "statsd_in: 'tls:' needs 'transport: tcp' -- TLS is defined over a byte stream, \
                 and DTLS is out of scope (docs/adr/syslog-tcp-ingress-and-tls.md); a Unix socket \
                 is always plaintext"
            ),
        };
        Ok(self)
    }

    /// Caps the connections a stream listener serves at once, overriding
    /// [`crate::DEFAULT_MAX_CONNECTIONS`]; `max_connections:` in config. Graph rule 74 rejects `0`
    /// before it gets here. A datagram listener is left untouched: it has no connections, and
    /// graph rule 74 rejects a non-default value there.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_max_connections(max_connections));
        }
        self
    }

    /// Sets the socket file's mode under `transport: unix`/`unix_stream` (`socket_mode:`),
    /// overriding the default `0722`. An IP listener is left untouched: it has no socket file, and
    /// graph rule 78 rejects the field there.
    pub fn with_socket_mode(mut self, socket_mode: u32) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => Inner::Udp(listener.with_socket_mode(socket_mode)),
            Inner::Tcp(listener) => Inner::Tcp(listener.with_socket_mode(socket_mode)),
        };
        self
    }

    /// The bound address after `bind()`, so a caller learns an ephemeral port with no bind-drop
    /// race. `None` on a Unix socket.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.inner {
            Inner::Udp(listener) => listener.local_addr(),
            Inner::Tcp(listener) => listener.local_addr(),
        }
    }

    /// The socket path under `transport: unix`/`unix_stream`; `None` otherwise.
    pub fn socket_path(&self) -> Option<&Path> {
        match &self.inner {
            Inner::Udp(listener) => listener.socket_path(),
            Inner::Tcp(listener) => listener.socket_path(),
        }
    }
}

#[async_trait::async_trait]
impl Input for StatsdInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        match &mut self.inner {
            Inner::Udp(listener) => listener.bind().await,
            Inner::Tcp(listener) => listener.bind().await,
        }
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        match &mut self.inner {
            Inner::Udp(listener) => listener.run(sink).await,
            Inner::Tcp(listener) => listener.run(sink).await,
        }
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        match &mut self.inner {
            Inner::Udp(listener) => listener.run_until_shutdown(sink, shutdown).await,
            Inner::Tcp(listener) => listener.run_until_shutdown(sink, shutdown).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{Event, MetricKind, Value};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// `with_diagnostics` reaches the UDP decoder as well as the driver, so `bad_line` reports
    /// under the component id.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = StatsdInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        match &input.inner {
            Inner::Udp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "my-id");
                assert_eq!(listener.diag().component_id(), "my-id");
            }
            Inner::Tcp(_) => panic!("StatsdInput::new must build a UDP listener"),
        }
    }

    /// The same on the TCP arm, which has its own `map_decoder` call.
    #[test]
    fn with_diagnostics_reaches_a_tcp_connections_decoder() {
        let input = StatsdInput::tcp("127.0.0.1:0").with_diagnostics(Diagnostics::new("tcp-id"));
        match &input.inner {
            Inner::Tcp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "tcp-id");
                assert_eq!(listener.diag().component_id(), "tcp-id");
            }
            Inner::Udp(_) => panic!("StatsdInput::tcp must build a TCP listener"),
        }
    }

    /// No address before `bind()`, a real one after.
    #[tokio::test]
    async fn local_addr_is_available_after_bind() {
        let mut input = StatsdInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");

        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
    }

    // ---- transport: tcp (`StatsdInput::tcp`) ---------------------------------------------------
    //
    // `crate::tcp`'s own tests cover the driver. These cover what is statsd-specific: the framing
    // mode, and that the wrapper's builders reach the driver.

    /// A running TCP listener, ready once `bind()` returns (no sleep-based guess); modelled on
    /// `crate::graphite`'s `Running`/`start`.
    struct RunningTcp {
        addr: std::net::SocketAddr,
        rx: tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>,
        shutdown: watch::Sender<bool>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
        registry: Arc<logit_core::telemetry::Registry>,
    }

    impl RunningTcp {
        /// The next delivered batch's events, or a panic naming `what`. Five seconds, the budget
        /// every socket test in this crate uses.
        async fn next_events(&mut self, what: &str) -> Vec<Event> {
            let delivered = tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
                .expect("the channel should not have closed");
            logit_pipeline::unwrap_batch(delivered).events
        }

        async fn connect(&self) -> TcpStream {
            TcpStream::connect(self.addr).await.expect("the listener should accept")
        }

        /// Waits for the `logit.input.connections` gauge to read 0. A connection's task drops
        /// its gauge guard and then its permit with no `.await` between, so on a current-thread
        /// runtime a 0 means every permit is back. Drains the registry.
        async fn wait_for_no_connections(&self) {
            logit_pipeline::test_util::TelemetryProbe::with_registry(self.registry.clone())
                .wait_for("the connections gauge to read 0", |t| {
                    t.gauge("logit.input.connections", &[]) == Some(0.0)
                })
                .await;
        }
    }

    /// Binds `build`'s listener on an ephemeral port and runs it, one event per batch with no
    /// flush timer, so each delivery is one line.
    async fn start_tcp(build: impl FnOnce(StatsdInput) -> StatsdInput) -> RunningTcp {
        let registry = logit_core::telemetry::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let input = StatsdInput::tcp("127.0.0.1:0")
            .with_diagnostics(Diagnostics::new("statsd_in").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry)
            .with_tcp_receive(TcpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                ..TcpListenerConfig::default()
            });
        let mut input = build(input);
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });
        RunningTcp { addr, rx, shutdown, handle, registry }
    }

    /// The sum of every counter point named `metric`, optionally narrowed to one tag.
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
            .filter(|m| m.name == intern(metric))
            .map(|m| match &m.kind {
                MetricKind::Sum(sum) => sum.value,
                MetricKind::Gauge(v) => *v,
                other => panic!("{metric} should be a counter or a gauge, got {other:?}"),
            })
            .sum()
    }

    fn metric_name(event: &Event) -> &'static str {
        logit_core::interner::resolve(event.metrics[0].name)
    }

    fn counter_value(event: &Event) -> f64 {
        match &event.metrics[0].kind {
            MetricKind::Sum(sum) => sum.value,
            other => panic!("expected a counter, got {other:?}"),
        }
    }

    /// The pin for this module's "Framing" section: under the driver's `Rfc6587Auto` default,
    /// `1.hits:1|c`'s leading digit would latch octet counting and mis-frame the connection.
    #[tokio::test]
    async fn a_tcp_line_starting_with_a_digit_is_not_read_as_an_octet_count() {
        let mut running = start_tcp(|input| input).await;
        let mut client = running.connect().await;
        client.write_all(b"1.hits:7|c\n").await.unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the digit-leading line").await;
        assert_eq!(events.len(), 1);
        assert_eq!(metric_name(&events[0]), "1.hits");
        assert_eq!(counter_value(&events[0]), 7.0);

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// `peer: true` stamps the observed sender over a DogStatsD tag of the same name, and leaves
    /// the line's other tags alone.
    #[tokio::test]
    async fn peer_replaces_a_tag_of_the_same_name() {
        use crate::peer::{PEER_ADDRESS, PEER_PORT};

        let mut running = start_tcp(|input| input.with_peer(true)).await;
        let mut client = running.connect().await;
        let port = client.local_addr().unwrap().port();
        client
            .write_all(b"hits:1|c|#network.peer.address:203.0.113.9,network.peer.port:1,env:prod\n")
            .await
            .unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the tagged line").await;
        let attrs = &events[0].attributes;
        assert_eq!(attrs.get(PEER_ADDRESS).and_then(Value::as_str), Some("127.0.0.1"));
        assert_eq!(attrs.get(PEER_PORT), Some(&Value::I64(i64::from(port))));
        assert_eq!(attrs.get("env").and_then(Value::as_str), Some("prod"));

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// `proxy_protocol: true` stamps the header's origin over a DogStatsD tag of the same name.
    #[tokio::test]
    async fn a_proxy_header_origin_replaces_a_tag_of_the_same_name() {
        use crate::peer::{CLIENT_ADDRESS, CLIENT_PORT};

        let mut running = start_tcp(|input| input.with_proxy_protocol(true)).await;
        let mut client = running.connect().await;
        client
            .write_all(
                b"PROXY TCP4 198.51.100.7 192.0.2.1 40000 8125\r\n\
                  hits:1|c|#client.address:203.0.113.9,client.port:1,env:prod\n",
            )
            .await
            .unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the tagged line").await;
        let attrs = &events[0].attributes;
        assert_eq!(attrs.get(CLIENT_ADDRESS).and_then(Value::as_str), Some("198.51.100.7"));
        assert_eq!(attrs.get(CLIENT_PORT), Some(&Value::I64(40000)));
        assert_eq!(attrs.get("env").and_then(Value::as_str), Some("prod"));

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The same over UDP: the datagram driver's stamp replaces the decoded tag too.
    #[tokio::test]
    async fn peer_replaces_a_tag_of_the_same_name_over_udp() {
        use crate::peer::{PEER_ADDRESS, PEER_PORT};
        use logit_pipeline::test_util::{fanout_channel, recv_events, spawn_input};

        let mut input = StatsdInput::new("127.0.0.1:0").with_peer(true);
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("a bound UDP listener has an address");
        let (fanout, mut rx) = fanout_channel(8);
        let running = spawn_input(input, fanout).await;
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = client.local_addr().unwrap().port();
        client
            .send_to(
                b"hits:1|c|#network.peer.address:203.0.113.9,network.peer.port:1,env:prod",
                addr,
            )
            .await
            .unwrap();

        let events = recv_events(&mut rx, 1).await;
        let attrs = &events[0].attributes;
        assert_eq!(attrs.get(PEER_ADDRESS).and_then(Value::as_str), Some("127.0.0.1"));
        assert_eq!(attrs.get(PEER_PORT), Some(&Value::I64(i64::from(port))));
        assert_eq!(attrs.get("env").and_then(Value::as_str), Some("prod"));
        running.stop().await;
    }

    /// Two concurrent clients both deliver (`crate::tcp`'s "Batching is per connection"): the
    /// per-connection `StatsdDecoder` clone works, not merely compiles.
    #[tokio::test]
    async fn two_concurrent_tcp_connections_both_deliver() {
        let mut running = start_tcp(|input| input).await;

        let mut first = running.connect().await;
        let mut second = running.connect().await;
        first.write_all(b"from.first:1|c\n").await.unwrap();
        first.flush().await.unwrap();
        second.write_all(b"from.second:2|c\n").await.unwrap();
        second.flush().await.unwrap();

        let mut seen = vec![
            metric_name(&running.next_events("the first connection's line").await[0]).to_string(),
            metric_name(&running.next_events("the second connection's line").await[0]).to_string(),
        ];
        seen.sort();
        assert_eq!(
            seen,
            ["from.first", "from.second"],
            "both connections deliver -- the order between them is the scheduler's, not \
             something to pin"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// A line split across two writes is one event: the driver frames across reads, where a
    /// per-read decoder would pass a single-write test and fail this.
    #[tokio::test]
    async fn a_tcp_line_split_across_writes_is_reassembled() {
        let mut running = start_tcp(|input| input).await;
        let mut client = running.connect().await;
        client.write_all(b"split.across:12").await.unwrap();
        client.flush().await.unwrap();
        // A half line must yield no event. With `batch_max_events: 1` and no flush timer, a
        // wrongly emitted one would arrive within a loopback round trip (well under 1 ms), so
        // 100 ms is a margin, not a multiple of a tick. The window also makes it likely, not
        // certain, that the listener read the first write on its own.
        logit_pipeline::test_util::assert_no_batch(
            &mut running.rx,
            Duration::from_millis(100),
            "the half line",
        )
        .await;
        client.write_all(b"3|c\n").await.unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the reassembled line").await;
        assert_eq!(events.len(), 1);
        assert_eq!(metric_name(&events[0]), "split.across");
        assert_eq!(
            counter_value(&events[0]),
            123.0,
            "the two halves must be one line, not two malformed ones"
        );

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up from
    /// `CARGO_MANIFEST_DIR`.
    fn testdata_tls_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// Wiring only: `with_tls` reaches the driver and a statsd line survives TLS; `crate::tcp`'s
    /// tests cover mTLS, client certificates and the handshake timeout.
    #[tokio::test]
    async fn a_tls_tcp_connection_round_trips_a_line() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let mut running = start_tcp(|input| {
            input
                .with_tls(&settings, &testdata_tls_dir(), &logit_pipeline::tls::TlsReloader::new())
                .expect("a tcp listener takes tls")
        })
        .await;

        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<rustls_pki_types::CertificateDer<'static>> =
            <rustls_pki_types::CertificateDer as rustls_pki_types::pem::PemObject>::pem_file_iter(
                testdata_tls_dir().join("ca.pem"),
            )
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

        let stream = TcpStream::connect(running.addr).await.expect("the listener should accept");
        // `testdata/tls/server.pem` carries a `localhost` SAN.
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut client =
            tokio::time::timeout(Duration::from_secs(5), connector.connect(name, stream))
                .await
                .expect("the TLS handshake should complete within 5s")
                .expect("the TLS handshake should succeed");
        client.write_all(b"over.tls:4|c|#env:prod\n").await.unwrap();
        client.flush().await.unwrap();

        let events = running.next_events("the line sent over TLS").await;
        assert_eq!(events.len(), 1);
        assert_eq!(metric_name(&events[0]), "over.tls");
        assert_eq!(events[0].attributes.get("env").and_then(Value::as_str), Some("prod"));

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// An unterminated final line on a clean close is dropped, not emitted (this module's
    /// "Framing" section). The remainder here still looks decodable, so emitting it would produce
    /// a plausible counter rather than a visible error.
    #[tokio::test]
    async fn an_unterminated_tail_at_a_clean_close_is_dropped_and_counted_truncated() {
        let mut running = start_tcp(|input| input).await;
        let mut client = running.connect().await;
        // No trailing newline: the sender got this far and stopped.
        client.write_all(b"page.views:1|c").await.unwrap();
        client.flush().await.unwrap();
        drop(client); // a clean FIN, not an RST

        // The driver counts the truncated tail when it sees the close, so waiting on the count
        // also waits for the close to be processed; no sleep orders the write before the FIN.
        let mut probe =
            logit_pipeline::test_util::TelemetryProbe::with_registry(running.registry.clone());
        let totals = probe
            .wait_for("the truncated tail to be counted", |t| {
                t.sum("logit.input.frames.dropped", &[("reason", "truncated")]) >= 1.0
            })
            .await;
        assert_eq!(
            totals.sum("logit.input.frames.dropped", &[("reason", "truncated")]),
            1.0,
            "the loss is counted once, as an abrupt close's is"
        );
        assert_eq!(
            totals.sum("logit.input.frames", &[]),
            0.0,
            "the remainder never became a frame"
        );
        // An event emitted for the tail would be sent with `batch_max_events: 1` and no flush
        // timer, so it would already be queued or arrive within a loopback round trip. 100 ms is
        // a margin for that, not a multiple of a tick.
        logit_pipeline::test_util::assert_no_batch(
            &mut running.rx,
            Duration::from_millis(100),
            "half a line is not a metric",
        )
        .await;

        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// `with_handshake_timeout` reaches the driver: under `with_max_connections(1)`, a second
    /// client is served only if a silent first one's permit came back.
    #[tokio::test]
    async fn a_silent_tcp_connection_releases_its_permit_after_the_handshake_timeout() {
        let mut running = start_tcp(|input| {
            input.with_max_connections(1).with_handshake_timeout(Duration::from_millis(50))
        })
        .await;

        // Held open past the deadline, so only the deadline can free the permit.
        let mut silent = running.connect().await;
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), silent.read(&mut byte))
            .await
            .expect("a silent connection is closed within the handshake timeout, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that said nothing");
        running.wait_for_no_connections().await;

        let mut client = running.connect().await;
        client.write_all(b"permit.came.back:1|c\n").await.unwrap();
        client.flush().await.unwrap();
        let events = running.next_events("a line on the connection after the silent one").await;
        assert_eq!(metric_name(&events[0]), "permit.came.back");

        drop(silent);
        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    /// The same for `with_idle_timeout`; the driver's tests cover the clock itself.
    #[tokio::test]
    async fn an_idle_tcp_connection_releases_its_permit_after_the_idle_timeout() {
        let mut running = start_tcp(|input| {
            input.with_max_connections(1).with_idle_timeout(Some(Duration::from_millis(50)))
        })
        .await;

        // One line passes the first-byte deadline, so only the idle clock can close this.
        let mut quiet = running.connect().await;
        quiet.write_all(b"quiet.then.idle:1|c\n").await.unwrap();
        quiet.flush().await.unwrap();
        let events = running.next_events("the line before going quiet").await;
        assert_eq!(metric_name(&events[0]), "quiet.then.idle");

        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), quiet.read(&mut byte))
            .await
            .expect("a connection quiet past its idle_timeout is closed, not left hanging")
            .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that went quiet");
        running.wait_for_no_connections().await;

        let mut client = running.connect().await;
        client.write_all(b"permit.came.back:1|c\n").await.unwrap();
        client.flush().await.unwrap();
        let events = running.next_events("a line on the connection after the quiet one").await;
        assert_eq!(metric_name(&events[0]), "permit.came.back");

        drop(quiet);
        running.shutdown.send(true).ok();
        running.handle.abort();
    }

    // ---- transport: unix / unix_stream (`StatsdInput::{unix, unix_stream}`) ----------------------
    //
    // `crate::unix` covers the path rules and `crate::tcp`/`crate::udp` the drivers. These cover
    // the wiring: the family, the mode, the stream framing, and the kernel-counter sampler.

    use crate::unix::tests::TempDir;

    /// A running Unix listener; `RunningTcp`'s twin without an address.
    struct RunningUnix {
        rx: tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>,
        shutdown: watch::Sender<bool>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
        registry: Arc<logit_core::telemetry::Registry>,
    }

    impl RunningUnix {
        /// Events from deliveries until `n` have arrived, or a panic naming `what`.
        async fn events(&mut self, n: usize, what: &str) -> Vec<Event> {
            let mut events = Vec::new();
            while events.len() < n {
                let delivered = tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
                    .await
                    .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
                    .expect("the channel should not have closed");
                events.extend(logit_pipeline::unwrap_batch(delivered).events);
            }
            events
        }

        fn stop(self) {
            self.shutdown.send(true).ok();
            self.handle.abort();
        }
    }

    async fn start_unix(input: StatsdInput) -> RunningUnix {
        let registry = logit_core::telemetry::Registry::new();
        let telemetry = registry.telemetry_for("statsd_in", "statsd_in", "listener");
        let mut input = input
            .with_diagnostics(Diagnostics::new("statsd_in").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry)
            .with_receive(UdpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                ..UdpListenerConfig::default()
            })
            .with_tcp_receive(TcpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                ..TcpListenerConfig::default()
            });
        input.bind().await.expect("binding the Unix socket should succeed");
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });
        RunningUnix { rx, shutdown, handle, registry }
    }

    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    fn names(events: &[Event]) -> Vec<&'static str> {
        events.iter().map(metric_name).collect()
    }

    /// One LE-length-prefixed `unix_stream` packet.
    fn packet(body: &[u8]) -> Vec<u8> {
        let mut framed = (body.len() as u32).to_le_bytes().to_vec();
        framed.extend_from_slice(body);
        framed
    }

    #[tokio::test]
    async fn a_unix_listener_has_a_socket_path_and_no_address() {
        let dir = TempDir::new("statsd-addr");
        let path = dir.path().join("dsd.socket");
        for mut input in [StatsdInput::unix(&path), StatsdInput::unix_stream(&path)] {
            input.bind().await.expect("bind");
            assert_eq!(input.local_addr(), None);
            assert_eq!(input.socket_path(), Some(path.as_path()));
        }
        assert_eq!(StatsdInput::new("127.0.0.1:0").socket_path(), None);
    }

    /// A multi-line datagram decodes as it would over UDP, the file is mode `0722`, and the
    /// kernel-counter sampler reads `SO_MEMINFO` off the Unix socket (the `used.bytes` gauge is
    /// emitted only by a successful read).
    #[tokio::test]
    async fn a_unix_datagram_round_trips_a_multi_line_packet_and_is_sampled() {
        let dir = TempDir::new("statsd-dgram");
        let path = dir.path().join("dsd.socket");
        let mut running = start_unix(StatsdInput::unix(&path)).await;
        assert_eq!(mode_of(&path), crate::unix::DEFAULT_SOCKET_MODE);

        let client = tokio::net::UnixDatagram::unbound().unwrap();
        client.send_to(b"a:1|c\nb:2|g|e:ext|card:low\nc:3:4|ms", &path).await.unwrap();

        let events = running.events(3, "the three lines of one datagram").await;
        assert_eq!(names(&events), vec!["a", "b", "c"]);
        assert_eq!(
            events[1].attributes.get("statsd.cardinality").and_then(Value::as_str),
            Some("low")
        );

        let drained = running.registry.drain(0);
        assert_eq!(metric_sum(&drained, "logit.input.datagrams", None), 1.0);
        assert!(
            drained
                .iter()
                .flat_map(|e| &e.metrics)
                .any(|m| { m.name == intern("logit.input.receive_buffer.used.bytes") }),
            "SO_MEMINFO should be readable on an AF_UNIX socket"
        );
        assert!(
            !drained.iter().any(|e| {
                e.log.as_ref().is_some_and(|log| {
                    log.message.as_str().is_some_and(|m| m.contains("not available"))
                })
            }),
            "the sampler must not have disabled itself"
        );
        running.stop();
    }

    /// Two length-prefixed packets, the second split across writes, decode as three events; the
    /// file is mode `0722`.
    #[tokio::test]
    async fn a_unix_stream_round_trips_length_prefixed_packets() {
        let dir = TempDir::new("statsd-stream");
        let path = dir.path().join("dsd-stream.socket");
        let mut running = start_unix(StatsdInput::unix_stream(&path)).await;
        assert_eq!(mode_of(&path), crate::unix::DEFAULT_SOCKET_MODE);

        let mut client = tokio::net::UnixStream::connect(&path).await.unwrap();
        client.write_all(&packet(b"a:1|c\nb:2|c")).await.unwrap();
        let second = packet(b"1.c:3|c");
        client.write_all(&second[..3]).await.unwrap();
        client.flush().await.unwrap();
        // The first packet's two lines are delivered; the half packet must add nothing.
        let first = running.events(2, "the first packet's lines").await;
        assert_eq!(names(&first), vec!["a", "b"]);
        // With `batch_max_events: 1` and no flush timer, a wrongly emitted event would arrive
        // within a loopback round trip (well under 1 ms), so 100 ms is a margin, not a multiple
        // of a tick. The window also makes it likely, not certain, that the listener read the
        // first part on its own.
        logit_pipeline::test_util::assert_no_batch(
            &mut running.rx,
            Duration::from_millis(100),
            "the half packet",
        )
        .await;
        client.write_all(&second[3..]).await.unwrap();
        client.flush().await.unwrap();

        let events = running.events(1, "the split packet's line").await;
        assert_eq!(names(&events), vec!["1.c"], "a leading digit is not an octet count");
        running.stop();
    }

    /// A declared length past the frame bound closes the connection and is counted; nothing is
    /// delivered.
    #[tokio::test]
    async fn an_oversize_unix_stream_packet_closes_the_connection_and_is_counted() {
        let dir = TempDir::new("statsd-oversize");
        let path = dir.path().join("dsd-stream.socket");
        let mut running = start_unix(StatsdInput::unix_stream(&path)).await;

        let mut client = tokio::net::UnixStream::connect(&path).await.unwrap();
        let declared = (logit_proto::framing::MAX_FRAME_BYTES as u32 + 1).to_le_bytes();
        client.write_all(&declared).await.unwrap();
        client.flush().await.unwrap();
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf)).await;
        assert!(matches!(read, Ok(Ok(0))), "the listener closes the connection: {read:?}");

        // The close is already observed, and an oversize frame is dropped before decoding, so a
        // delivery would have been sent first. 200 ms is a margin, not a multiple of a tick.
        logit_pipeline::test_util::assert_no_batch(
            &mut running.rx,
            Duration::from_millis(200),
            "an oversize packet delivers nothing",
        )
        .await;
        let drained = running.registry.drain(0);
        assert_eq!(
            metric_sum(&drained, "logit.input.frames.dropped", Some(("reason", "oversize"))),
            1.0
        );
        running.stop();
    }

    /// A configured `socket_mode:` replaces the default on both Unix transports.
    #[tokio::test]
    async fn with_socket_mode_sets_the_socket_files_mode_on_both_unix_transports() {
        let dir = TempDir::new("statsd-mode");
        let path = dir.path().join("dsd.socket");
        for input in [StatsdInput::unix(&path), StatsdInput::unix_stream(&path)] {
            let mut input = input.with_socket_mode(0o660);
            input.bind().await.expect("bind");
            assert_eq!(mode_of(&path), 0o660);
        }
    }

    /// A Unix socket is always plaintext: `with_tls` fails on both transports.
    #[test]
    fn with_tls_fails_on_either_unix_transport() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        for input in [StatsdInput::unix("/tmp/x.socket"), StatsdInput::unix_stream("/tmp/x.socket")]
        {
            let err = input
                .with_tls(&settings, &testdata_tls_dir(), &logit_pipeline::tls::TlsReloader::new())
                .err()
                .expect("must fail");
            assert!(err.to_string().contains("plaintext"), "{err}");
        }
    }

    // ---- `reuse_port` (`StatsdInput::with_reuse_port`) --------------------------------------

    use logit_pipeline::test_util::{fanout_channel, spawn_input, Running, RECV_TIMEOUT};
    use logit_pipeline::unwrap_batch;

    /// Lines sent to a pair of `reuse_port` listeners, each from a fresh source port.
    const SPLIT_LINES: usize = 64;

    type Rx = tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>;

    /// Binds two `reuse_port` listeners on one ephemeral port, A first so B can learn the port.
    async fn reuse_port_pair(
        build: fn(String) -> StatsdInput,
    ) -> (std::net::SocketAddr, Running, Rx, Running, Rx) {
        let mut a = build("127.0.0.1:0".to_string()).with_reuse_port(true);
        a.bind().await.expect("A should bind");
        let addr = a.local_addr().expect("a bound listener has an address");
        let b = build(addr.to_string()).with_reuse_port(true);
        let (fanout_a, rx_a) = fanout_channel(SPLIT_LINES);
        let (fanout_b, rx_b) = fanout_channel(SPLIT_LINES);
        let running_a = spawn_input(a, fanout_a).await;
        let running_b = spawn_input(b, fanout_b).await;
        (addr, running_a, rx_a, running_b, rx_b)
    }

    /// Counts events off both receivers until `SPLIT_LINES` have arrived, within one
    /// `RECV_TIMEOUT`. Returns how many each listener delivered.
    async fn count_split(rx_a: &mut Rx, rx_b: &mut Rx) -> (usize, usize) {
        let (mut a, mut b) = (0, 0);
        let all_arrived = tokio::time::timeout(RECV_TIMEOUT, async {
            while a + b < SPLIT_LINES {
                tokio::select! {
                    Some(delivered) = rx_a.recv() => a += unwrap_batch(delivered).events.len(),
                    Some(delivered) = rx_b.recv() => b += unwrap_batch(delivered).events.len(),
                }
            }
        })
        .await;
        assert!(all_arrived.is_ok(), "only {a} + {b} of {SPLIT_LINES} lines arrived");
        (a, b)
    }

    /// The kernel hashes each datagram's source port to one of the two sockets, so 64 senders
    /// reach both. Loopback with the default receive buffer drops none of 64 small datagrams,
    /// which is what makes the total exact; a one-sided split has probability 2 × 2⁻⁶⁴.
    #[tokio::test]
    async fn two_reuse_port_listeners_on_one_udp_port_both_deliver() {
        let (addr, running_a, mut rx_a, running_b, mut rx_b) =
            reuse_port_pair(StatsdInput::new).await;
        for i in 0..SPLIT_LINES {
            let sender = std::net::UdpSocket::bind("127.0.0.1:0").expect("sender should bind");
            sender.send_to(format!("split.{i}:1|c").as_bytes(), addr).expect("send_to");
        }
        let (a, b) = count_split(&mut rx_a, &mut rx_b).await;
        assert_eq!(a + b, SPLIT_LINES);
        assert!(a > 0 && b > 0, "both listeners should take a share, got {a} and {b}");
        running_a.stop().await;
        running_b.stop().await;
    }

    /// The TCP twin: the kernel hashes each connection's 4-tuple to one of the two listeners.
    /// Each connection carries one line and closes; a one-sided split has probability 2 × 2⁻⁶⁴.
    #[tokio::test]
    async fn two_reuse_port_listeners_on_one_tcp_port_both_deliver() {
        let (addr, running_a, mut rx_a, running_b, mut rx_b) =
            reuse_port_pair(StatsdInput::tcp).await;
        for i in 0..SPLIT_LINES {
            let mut stream = TcpStream::connect(addr).await.expect("connect");
            stream.write_all(format!("split.{i}:1|c\n").as_bytes()).await.expect("write");
            stream.shutdown().await.expect("shutdown");
        }
        let (a, b) = count_split(&mut rx_a, &mut rx_b).await;
        assert_eq!(a + b, SPLIT_LINES);
        assert!(a > 0 && b > 0, "both listeners should take a share, got {a} and {b}");
        running_a.stop().await;
        running_b.stop().await;
    }

    /// Source sockets in [`a_stopped_reuse_port_listener_hands_its_flows_to_the_survivor`]. A
    /// split that leaves A no flow fails the test's premise with probability 2⁻³².
    const STOP_SOURCES: usize = 32;

    /// Lines each source sends before A stops, so A queues a backlog behind its parked consumer.
    const PRE_STOP_ROUNDS: usize = 4;

    /// When one of a `reuse_port` pair stops while its downstream is still busy, every flow the
    /// kernel hashed to it reaches the survivor before the stopped listener has drained its own
    /// queue, and that queue is still delivered in full.
    #[tokio::test]
    async fn a_stopped_reuse_port_listener_hands_its_flows_to_the_survivor() {
        use logit_pipeline::test_util::{recv_batch, wait_until, TelemetryProbe};

        let one_event_per_send = UdpListenerConfig {
            batch_max_events: 1,
            batch_flush_interval: Duration::ZERO,
            ..UdpListenerConfig::default()
        };
        let mut probe_a = TelemetryProbe::new();
        let mut a = StatsdInput::new("127.0.0.1:0")
            .with_reuse_port(true)
            .with_receive(one_event_per_send)
            .with_telemetry(probe_a.telemetry("a", "statsd_in", "listener"));
        a.bind().await.expect("A should bind");
        let addr = a.local_addr().expect("a bound listener has an address");
        let b = StatsdInput::new(addr.to_string()).with_reuse_port(true);
        // A's consumer holds one batch and isn't read until the end, so A's `decode_loop` parks
        // on its second send.
        let (fanout_a, mut rx_a) = fanout_channel(1);
        let (fanout_b, mut rx_b) = fanout_channel(1024);
        let running_a = spawn_input(a, fanout_a).await;
        let running_b = spawn_input(b, fanout_b).await;

        let sources: Vec<std::net::UdpSocket> = (0..STOP_SOURCES)
            .map(|_| std::net::UdpSocket::bind("127.0.0.1:0").expect("source should bind"))
            .collect();
        for round in 0..PRE_STOP_ROUNDS {
            for (i, source) in sources.iter().enumerate() {
                source.send_to(format!("pre.{i}.{round}:1|c").as_bytes(), addr).expect("send_to");
            }
        }
        let sent = STOP_SOURCES * PRE_STOP_ROUNDS;
        let mut at_b = 0usize;
        wait_until("every pre-stop line to be read by A or delivered by B", || {
            while let Ok(delivered) = rx_b.try_recv() {
                at_b += unwrap_batch(delivered).events.len();
            }
            at_b as f64 + probe_a.sum("logit.input.datagrams", &[]) == sent as f64
        })
        .await;
        let at_a = sent - at_b;
        assert!(at_a > 0, "the premise: the kernel hashed at least one source to A");

        running_a.shutdown.send(true).expect("A should still be running");
        let mut reached_b = std::collections::BTreeSet::new();
        wait_until("a probe from every source to reach B", || {
            for (i, source) in sources.iter().enumerate() {
                if !reached_b.contains(&i) {
                    source.send_to(format!("probe.{i}:1|c").as_bytes(), addr).expect("send_to");
                }
            }
            while let Ok(delivered) = rx_b.try_recv() {
                for event in unwrap_batch(delivered).events {
                    if let Some(i) = metric_name(&event).strip_prefix("probe.") {
                        reached_b.insert(i.parse::<usize>().expect("a probe names its source"));
                    }
                }
            }
            reached_b.len() == STOP_SOURCES
        })
        .await;
        assert!(
            !running_a.handle.is_finished(),
            "the premise: A is still parked on its full downstream, so its socket closed before \
             its drain finished"
        );

        // A probe A read before it saw shutdown is delivered too; only the pre-stop lines count.
        let mut pre_at_a = 0;
        while pre_at_a < at_a {
            let batch = recv_batch(&mut rx_a).await;
            pre_at_a +=
                batch.events.iter().filter(|event| metric_name(event).starts_with("pre.")).count();
        }
        assert_eq!(pre_at_a, at_a, "A must deliver every line it read before it stopped");
        running_a.stop().await;
        running_b.stop().await;
    }

    fn is_addr_in_use(err: &anyhow::Error) -> bool {
        err.chain().any(|cause| {
            cause
                .downcast_ref::<std::io::Error>()
                .is_some_and(|io| io.kind() == std::io::ErrorKind::AddrInUse)
        })
    }

    /// Without `reuse_port`, a second listener on a held address fails to bind, on either
    /// transport.
    #[tokio::test]
    async fn a_second_listener_without_reuse_port_is_refused() {
        for build in [StatsdInput::new as fn(String) -> StatsdInput, StatsdInput::tcp] {
            let mut a = build("127.0.0.1:0".to_string());
            a.bind().await.expect("A should bind");
            let addr = a.local_addr().expect("a bound listener has an address");
            let err = build(addr.to_string()).bind().await.expect_err("the port is held");
            assert!(is_addr_in_use(&err), "expected EADDRINUSE, got {err:#}");
        }
    }

    /// Both Unix transports refuse `reuse_port` at bind, behind the graph rule that rejects it
    /// first.
    #[tokio::test]
    async fn reuse_port_on_either_unix_transport_fails_the_bind() {
        let dir = TempDir::new("statsd-reuse-port");
        let path = dir.path().join("dsd.socket");
        for input in [StatsdInput::unix(&path), StatsdInput::unix_stream(&path)] {
            let err = input.with_reuse_port(true).bind().await.expect_err("must refuse");
            assert!(err.to_string().contains("a Unix socket has no port to share"), "{err}");
        }
        assert!(!path.exists(), "a refused bind creates no socket file");
    }
}
