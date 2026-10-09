//! RFC 3164 / RFC 5424 syslog over UDP or TCP, the input half of the `syslog_in -> syslog_out`
//! lossless-relay pair (`docs/adr/lossless-transit.md`). nginx's `access_log syslog:` writer
//! speaks it over UDP.
//!
//! This module is the listener: transports, framing, and telemetry. The dialect sniff, the header
//! grammar, and what each field maps to on an event are [`SyslogDecoder`]'s, in
//! `logit_proto::syslog`'s module doc (`crates/logit-proto/src/syslog/mod.rs`).
//!
//! **The driver adds sender attributes after decode.** Under `peer:` or `proxy_protocol:`,
//! `network.peer.*` or `client.*` is added to every event, an opt-in addition the sender never
//! sent, and one of ADR `lossless-transit`'s "Permitted normalizations". [`SyslogDecoder`] never
//! sees a peer.
//!
//! ## Transports and framing
//!
//! **Both transports, one decoder.** UDP is the default. `transport: tcp`
//! (`docs/adr/syslog-tcp-ingress-and-tls.md`) runs the same [`SyslogDecoder`] behind
//! [`crate::tcp::TcpListener`] instead of [`crate::udp::UdpListener`], adding an accept loop, RFC
//! 6587 framing and, with a `tls:` block, TLS termination. RFC 5425 syslog over TLS is RFC 6587
//! framing carried over TLS.
//!
//! The TCP framing is auto-detected from each connection's first byte and latched for its life:
//! an ASCII digit starts an **octet count** (`MSG-LEN SP MSG`), and anything else is
//! **non-transparent** (LF-delimited), since such a frame always starts with `<`. A final
//! LF-framed message with no terminator is emitted on a clean close, as RFC 6587 §3.4.2 permits;
//! after an abrupt close or a shutdown it is counted `truncated`. A frame past the driver's 64 KiB
//! [`MAX_FRAME_BYTES`](logit_proto::framing::MAX_FRAME_BYTES) closes the connection, counted
//! `logit.input.frames.dropped{reason="oversize"}`, under either framing, since octet counting has
//! no resync point; an octet-counted frame cut short by a close is
//! `logit.input.frames.dropped{reason="truncated"}`. As in `statsd_in`, only `receive:`'s
//! batch-assembly fields and `shutdown_grace` apply under TCP (graph rule 17).
//!
//! The decoder differs between the transports only in **line splitting**. A UDP datagram may carry
//! several LF-separated messages, so the UDP arm splits on `\n`. A TCP frame is already one
//! message, and an octet-counted one may contain `\n` as MSG content, so [`SyslogInput::tcp`] turns
//! splitting off ([`SyslogDecoder::with_line_splitting`]) and the framer is the sole delimiter.
//!
//! ## Telemetry and diagnostics
//!
//! The drivers own every `logit.input.*` counter, as for `statsd_in` (`crate::statsd`'s
//! "Telemetry and diagnostics"). **A malformed message is skipped and reported as a throttled
//! `bad_line`**, and the rest of its datagram still decodes. `decode_into` never fails, so the
//! drivers' `bad_datagram`/`bad_frame` never fire here. The decoder's other diagnostics keep the
//! event: `sniff_fallback`, `timestamp_out_of_range`, and `hostname_not_utf8`, each described in
//! `logit_proto::syslog`'s module doc.

use crate::tcp::{TcpListener, TcpListenerConfig, TlsServerSettings};
use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::syslog::SyslogDecoder;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;

/// Which driver a [`SyslogInput`] wraps, chosen once by `transport:`. An enum rather than a
/// `Box<dyn Input>` so each arm's concrete builders ([`TcpListener::with_tls`],
/// [`UdpListener::with_config`]) stay reachable.
enum Inner {
    Udp(UdpListener<SyslogDecoder>),
    Tcp(TcpListener<SyslogDecoder>),
}

/// The `syslog_in` listener: a [`SyslogDecoder`] over [`UdpListener`] or [`TcpListener`].
///
/// All transport behavior lives in the drivers; see this module's "Transports and framing".
pub struct SyslogInput {
    inner: Inner,
}

impl SyslogInput {
    /// A UDP listener, the default transport, with line splitting on: one datagram may carry
    /// several LF-separated messages.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Udp(UdpListener::new(
                bind,
                SyslogDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            )),
        }
    }

    /// A TCP listener (`transport: tcp`), plaintext until [`Self::with_tls`] is called.
    ///
    /// Line splitting is **off** ([`SyslogDecoder::with_line_splitting`]): the framer delimits one
    /// message per frame, and an octet-counted MSG may contain a `\n` that re-splitting would shred
    /// into spurious events. Under LF framing the `\n` is already gone, so splitting could only be
    /// a no-op or a bug there too.
    pub fn tcp(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Tcp(TcpListener::new(
                bind,
                SyslogDecoder::new(Arc::new(Resource::default())).with_line_splitting(false),
                TcpListenerConfig::default(),
            )),
        }
    }

    /// Attaches a component id to the driver's diagnostics and to the wrapped [`SyslogDecoder`]'s.
    ///
    /// Both must carry it: the driver reports transport failures (`framing_error`/
    /// `connection_error` on TCP) and the decoder reports every rejected message as `bad_line`,
    /// on either transport, since [`Decoder::decode_into`] never fails here. Miss one and that
    /// class of failure reports under no component id with telemetry disabled.
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
    /// TCP.
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
    /// Two transport-specific setters because the configs aren't interchangeable: a TCP listener
    /// has no receive queue (graph rule 17), so one setter would have to decide at runtime what to
    /// do with a queue bound it can't honour. [`Self::with_tcp_receive`] is the counterpart.
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
    /// [`TcpListener::with_proxy_protocol`]. A UDP listener is left untouched, and graph rule 79
    /// rejects the option there.
    pub fn with_proxy_protocol(mut self, proxy_protocol: bool) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_proxy_protocol(proxy_protocol));
        }
        self
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

    /// Terminates TLS (RFC 5425) on a TCP listener (`tls:`); paths in `settings` resolve against
    /// `base_dir`.
    ///
    /// Fails on a UDP listener: DTLS (RFC 6012) is out of scope
    /// (`docs/adr/syslog-tcp-ingress-and-tls.md`'s Alternatives). Graph rule 43 is what an operator
    /// sees; this arm backstops a caller that skipped validation.
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
                "syslog_in: 'tls:' needs 'transport: tcp' -- there is no syslog-over-DTLS support \
                 (docs/adr/syslog-tcp-ingress-and-tls.md)"
            ),
        };
        Ok(self)
    }

    /// The bound address after `bind()`, so a caller learns an ephemeral port with no bind-drop
    /// race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match &self.inner {
            Inner::Udp(listener) => listener.local_addr(),
            Inner::Tcp(listener) => listener.local_addr(),
        }
    }
}

#[async_trait::async_trait]
impl Input for SyslogInput {
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
    use logit_core::{Event, Severity, Value};
    use std::time::Duration;

    fn message_str(event: &Event) -> &str {
        event.log.as_ref().expect("event should carry a log").message.as_str().unwrap()
    }

    /// `with_peer` reaches the UDP driver: the sender's address lands on the decoded event.
    #[tokio::test]
    async fn with_peer_stamps_a_udp_senders_address() {
        use crate::peer::{PEER_ADDRESS, PEER_PORT};
        use logit_pipeline::test_util::{fanout_channel, recv_events, spawn_input};

        let mut input = SyslogInput::new("127.0.0.1:0").with_peer(true);
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("a bound UDP listener has an address");
        let (fanout, mut rx) = fanout_channel(8);
        let running = spawn_input(input, fanout).await;
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = client.local_addr().unwrap().port();
        client.send_to(b"<134>1 - host app - - - hello", addr).await.unwrap();

        let events = recv_events(&mut rx, 1).await;
        let attrs = &events[0].attributes;
        assert_eq!(attrs.get(PEER_ADDRESS).and_then(Value::as_str), Some("127.0.0.1"));
        assert_eq!(attrs.get(PEER_PORT), Some(&Value::I64(i64::from(port))));
        running.stop().await;
    }

    /// `with_diagnostics` reaches the UDP decoder as well as the driver, so `bad_line` reports
    /// under the component id.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = SyslogInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        match &input.inner {
            Inner::Udp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "my-id");
                assert_eq!(listener.diag().component_id(), "my-id");
            }
            Inner::Tcp(_) => panic!("SyslogInput::new must build a UDP listener"),
        }
    }

    /// The same on the TCP arm, which has its own `map_decoder` call.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_on_the_tcp_arm_too() {
        let input = SyslogInput::tcp("127.0.0.1:0").with_diagnostics(Diagnostics::new("tcp-id"));
        match &input.inner {
            Inner::Tcp(listener) => {
                assert_eq!(listener.decoder().diag().component_id(), "tcp-id");
                assert_eq!(listener.diag().component_id(), "tcp-id");
                assert!(
                    !listener.decoder().line_splitting(),
                    "with_diagnostics must not undo SyslogInput::tcp's line-splitting choice"
                );
            }
            Inner::Udp(_) => panic!("SyslogInput::tcp must build a TCP listener"),
        }
    }

    // ---- TCP end to end (`transport: tcp`) ----------------------------------------------------

    /// Binds an ephemeral TCP port through `Input::bind`, then starts the listener, with no
    /// bind-drop race.
    async fn running_tcp_input(
        tls: Option<&TlsServerSettings>,
    ) -> (String, tokio::task::JoinHandle<()>, tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>)
    {
        running_tcp_input_with(tls, None).await
    }

    /// [`running_tcp_input`] with a `Diagnostics` attached, so a test can read the component's
    /// own occurrence counts back.
    async fn running_tcp_input_with(
        tls: Option<&TlsServerSettings>,
        diag: Option<Diagnostics>,
    ) -> (String, tokio::task::JoinHandle<()>, tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>)
    {
        let mut input = SyslogInput::tcp("127.0.0.1:0").with_tcp_receive(TcpListenerConfig {
            // One event per batch, no timer: a multiline message split into two events arrives
            // as two batches, not one.
            batch_max_events: 1,
            batch_flush_interval: Duration::ZERO,
            ..TcpListenerConfig::default()
        });
        if let Some(settings) = tls {
            input = input
                .with_tls(settings, &testdata_tls_dir(), &logit_pipeline::tls::TlsReloader::new())
                .expect("the committed testdata/tls fixtures should load");
        }
        if let Some(diag) = diag {
            input = input.with_diagnostics(diag);
        }
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() leaves a real address behind").to_string();

        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            // Held for the task's life: dropping it would fail every `changed()` await in the
            // driver. Tests abort the handle instead.
            let _shutdown_tx = shutdown_tx;
            let _ = input.run_until_shutdown(sink, shutdown_rx).await;
        });
        (addr, handle, rx)
    }

    async fn recv_events(
        rx: &mut tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>,
    ) -> Vec<Event> {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch should be delivered within 5s")
            .expect("the fanout should not have closed");
        logit_pipeline::unwrap_batch(delivered).events
    }

    fn assert_nginx_line(event: &Event) {
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("nginx"));
        assert_eq!(event.log.as_ref().unwrap().severity, Some(Severity::Info)); // 134 % 8 = 6
        assert_eq!(message_str(event), "hello over tcp");
    }

    /// `bad_line` throttles listener-wide: two connections rejecting two messages each leave the
    /// component's count at 4, where per-connection counting would leave it at 0.
    ///
    /// Each connection's final good line orders the assertion: a connection decodes in order, so
    /// its event proves the bad lines ahead of it were absorbed.
    #[tokio::test]
    async fn bad_line_throttles_across_connections() {
        let diag = Diagnostics::new("syslog_in");
        let (addr, handle, mut rx) = running_tcp_input_with(None, Some(diag.clone())).await;

        for connection in 0..2 {
            let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
            tokio::io::AsyncWriteExt::write_all(
                &mut client,
                b"not a syslog line\nnor is this one\n\
                  <134>Aug 30 10:00:00 myhost nginx: hello over tcp\n",
            )
            .await
            .unwrap();

            let events = recv_events(&mut rx).await;
            assert_eq!(
                events.len(),
                1,
                "connection {connection}: only the well-formed line becomes an event"
            );
            assert_nginx_line(&events[0]);
        }

        assert_eq!(
            diag.occurrences("bad_line"),
            4,
            "two connections rejecting two messages each must count on one listener-wide \
             throttle -- a decoder clone with counts of its own would leave this at 0, having \
             counted 2 in each throwaway copy"
        );
        handle.abort();
    }

    /// rsyslog's `omfwd` default framing (RFC 6587 section 3.4.2) through a real listener.
    #[tokio::test]
    async fn tcp_decodes_an_lf_framed_message_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(
            &mut client,
            b"<134>Aug 30 10:00:00 myhost nginx: hello over tcp\n",
        )
        .await
        .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// `syslog_out`'s TCP framing (RFC 6587 section 3.4.1), detected from the leading digit.
    #[tokio::test]
    async fn tcp_decodes_an_octet_counted_message_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let msg = "<134>Aug 30 10:00:00 myhost nginx: hello over tcp";
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// A message ending in CR, LF-framed as `...\r\r\n`, keeps that byte, as over UDP. Decoder
    /// twin: `with_line_splitting_off_keeps_a_payload_cr_the_framer_already_unwrapped`.
    #[tokio::test]
    async fn tcp_keeps_a_payload_cr_on_an_lf_framed_message_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(
            &mut client,
            b"<134>Aug 30 10:00:00 myhost nginx: hello over tcp\r\r\n",
        )
        .await
        .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_eq!(message_str(&events[0]), "hello over tcp\r");
        handle.abort();
    }

    /// An octet-counted MSG-LEN covering its own `\r\n` has it stripped, matching UDP.
    #[tokio::test]
    async fn tcp_strips_a_counted_crlf_terminator_end_to_end() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let msg = "<134>Aug 30 10:00:00 myhost nginx: hello over tcp\r\n";
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// An octet-counted MSG containing a newline is one event with the newline intact, not a
    /// first half plus a PRI-less remainder dropped as `bad_line`.
    #[tokio::test]
    async fn tcp_keeps_a_multiline_octet_counted_message_as_exactly_one_event() {
        let (addr, handle, mut rx) = running_tcp_input(None).await;
        let msg = "<134>Aug 30 10:00:00 myhost nginx: line one\nline two\nline three";
        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1, "a multiline MSG must not be shredded into several events");
        assert_eq!(message_str(&events[0]), "line one\nline two\nline three");

        // A second event would arrive as its own batch.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), rx.recv()).await.is_err(),
            "the multiline message must produce exactly one event"
        );
        handle.abort();
    }

    /// `with_idle_timeout` reaches the driver: under `with_max_connections(1)`, a second client
    /// is served only if the quiet first one is closed. The driver's tests cover the clock.
    #[tokio::test]
    async fn an_idle_tcp_connection_releases_its_permit_after_the_idle_timeout() {
        const LINE: &[u8] = b"<134>Aug 30 10:00:00 myhost nginx: hello over tcp\n";

        let input = SyslogInput::tcp("127.0.0.1:0")
            .with_tcp_receive(TcpListenerConfig {
                batch_max_events: 1,
                batch_flush_interval: Duration::ZERO,
                ..TcpListenerConfig::default()
            })
            .with_max_connections(1)
            .with_idle_timeout(Some(Duration::from_millis(50)));
        let mut probe = logit_pipeline::test_util::TelemetryProbe::new();
        let mut input = input.with_telemetry(probe.telemetry("syslog_in", "syslog_in", "listener"));
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() leaves a real address behind").to_string();
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let _shutdown_tx = shutdown_tx;
            let _ = input.run_until_shutdown(sink, shutdown_rx).await;
        });

        // One frame passes the first-byte deadline, so only the idle clock can close this.
        let mut quiet = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut quiet, LINE).await.unwrap();
        assert_nginx_line(&recv_events(&mut rx).await[0]);

        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::io::AsyncReadExt::read(&mut quiet, &mut byte),
        )
        .await
        .expect("a connection quiet past its idle_timeout is closed, not left hanging")
        .expect("reading a closed socket is Ok(0), not an error");
        assert_eq!(read, 0, "the listener hung up on a connection that went quiet");
        // The connection's task drops its gauge guard and then its permit with no `.await`
        // between, so on this current-thread runtime a 0 means the permit is back.
        probe
            .wait_for("the connections gauge to read 0", |t| {
                t.gauge("logit.input.connections", &[]) == Some(0.0)
            })
            .await;

        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, LINE).await.unwrap();
        assert_nginx_line(&recv_events(&mut rx).await[0]);

        drop(quiet);
        handle.abort();
    }

    /// The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up from
    /// `CARGO_MANIFEST_DIR`.
    fn testdata_tls_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// RFC 5425 end to end: a real `TlsConnector` trusting exactly `testdata/tls/ca.pem` hands an
    /// octet-counted frame to a TLS-terminating `syslog_in`.
    #[tokio::test]
    async fn tcp_over_tls_decodes_a_message_end_to_end() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let (addr, handle, mut rx) = running_tcp_input(Some(&settings)).await;

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

        let tcp = tokio::net::TcpStream::connect(&addr).await.unwrap();
        // `testdata/tls/server.pem` carries a `localhost` SAN.
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut client = tokio::time::timeout(Duration::from_secs(5), connector.connect(name, tcp))
            .await
            .expect("the TLS handshake should complete within 5s")
            .expect("the TLS handshake should succeed");

        let msg = "<134>Aug 30 10:00:00 myhost nginx: hello over tcp";
        tokio::io::AsyncWriteExt::write_all(&mut client, format!("{} {msg}", msg.len()).as_bytes())
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::flush(&mut client).await.unwrap();

        let events = recv_events(&mut rx).await;
        assert_eq!(events.len(), 1);
        assert_nginx_line(&events[0]);
        handle.abort();
    }

    /// The builder refuses `tls:` on UDP rather than ignoring it, backing graph rule 43.
    #[test]
    fn with_tls_on_a_udp_listener_is_a_clear_error() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        // `SyslogInput` isn't `Debug`, so `expect_err` is out -- match the `Result` by hand.
        let err = match SyslogInput::new("127.0.0.1:0").with_tls(
            &settings,
            &testdata_tls_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        ) {
            Ok(_) => panic!("tls on a UDP syslog listener must not be accepted"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("transport: tcp"), "got: {err:?}");
    }
}
