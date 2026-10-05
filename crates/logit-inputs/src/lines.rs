//! `lines_in`: newline-delimited plain text in, one raw log event per line out, with nothing
//! parsed. A pipeline gives the line structure downstream with `json`, `logfmt`, `kv`, or `regex`.
//!
//! ## Transports
//!
//! One component, two shared drivers, chosen by `transport:`, as [`crate::statsd::StatsdInput`]
//! does:
//!
//! | `transport:` | Driver | What it brings |
//! |---|---|---|
//! | `tcp` (the default) | [`TcpListener<LinesDecoder>`](crate::tcp::TcpListener) | an accept loop, the `max_connections:` cap, a per-connection decoder clone and batch accumulator, the first-byte deadline, and, with a `tls:` block, TLS termination |
//! | `udp` | [`UdpListener<LinesDecoder>`](crate::udp::UdpListener) | the read/decode split, the receive queue, datagram->batch assembly, `SO_RCVBUF` (`docs/adr/decoupled-listener-io.md`); the whole `receive:` block applies |
//! | `unix` | [`UdpListener::unix`](crate::udp::UdpListener::unix) | everything `udp` brings, on a `SOCK_DGRAM` Unix socket |
//! | `unix_stream` | [`TcpListener::unix`](crate::tcp::TcpListener::unix) | everything `tcp` brings but TLS and the accept-queue gauges, on a `SOCK_STREAM` Unix socket |
//!
//! Under both Unix transports `bind:` is the socket's path, prepared by [`crate::unix`], and the
//! file is made mode [`SOCKET_MODE`]. The file isn't removed on shutdown.
//!
//! ## Framing
//!
//! **`tcp` and `unix_stream`**: [`FramingMode::Lines`] with [`Oversize::DrainToNextLine`] and
//! `max_line_bytes` as the bound. The framer strips one `CR` before each `LF` and hands
//! [`LinesDecoder`] one line at a time. A line past the bound is dropped, counted once as
//! `logit.input.frames.dropped{reason="oversize"}`, and the connection resynchronizes at the next
//! `LF`. An unterminated final line at a close is dropped and counted `reason="truncated"`, the
//! driver's rule for every line-framed listener: a sender that stopped mid-line never finished it.
//! Never [`FramingMode::Rfc6587Auto`], which would read a line opening with a digit as an octet
//! count.
//!
//! **`udp` and `unix`**: the driver hands [`LinesDecoder`] a whole datagram, which it splits on
//! `LF` itself. A datagram's end also ends its last line, so an unterminated tail is emitted. The
//! decoder enforces `max_line_bytes` here, because no framer runs: an oversize line is dropped and
//! counted as `logit.input.frames.dropped{reason="oversize"}` on the listener's own telemetry,
//! the series the stream framer uses, with a throttled `oversize_line` diagnostic, and the rest
//! of the datagram still decodes.
//!
//! Under every transport a trailing `CR` is stripped and an empty line is skipped.
//!
//! ## The event
//!
//! [`Event::log`] at the driver's `received_at`, no attributes, a [`LogRecord`] with
//! [`BodyFormat::Raw`] and no severity, trace, or event name. The message is a zero-copy
//! `Bytes` slice of the frame or datagram: [`Value::Str`] when the line is valid UTF-8,
//! [`Value::Bytes`] otherwise, as `crate::syslog` keeps a MSG. Every event shares one
//! `Arc<Resource>`, empty, across every connection's decoder clone. Nothing about the peer (its
//! address, a hostname) is attached; a `set` stage per listener stamps whatever the operator knows.
//!
//! ## Telemetry and diagnostics
//!
//! All of it comes from the shared drivers (`crate::statsd`'s module doc lists them per
//! transport), plus the datagram-side oversize count above.

use crate::tcp::{FramingMode, Oversize, TcpListener, TcpListenerConfig, TlsServerSettings};
use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use bytes::Bytes;
use logit_core::{
    AttrMap, BodyFormat, Diagnostics, Event, LogRecord, Resource, Scope, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::{CodecError, Decoder};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::watch;

/// The socket file's mode under `transport: unix`/`unix_stream`: any local user may send, and
/// the directory's permissions restrict access, as for `statsd_in`.
pub const SOCKET_MODE: u32 = 0o722;

/// `max_line_bytes`' default, the stream driver's own frame bound.
/// `logit_config::default_lines_max_line_bytes` mirrors it by hand.
pub const DEFAULT_MAX_LINE_BYTES: usize = crate::tcp::MAX_FRAME_BYTES;

/// Which driver a [`LinesInput`] wraps, chosen once by `transport:`. `Udp` also covers
/// `transport: unix` and `Tcp` covers `unix_stream`: the drivers own the socket family.
enum Inner {
    Udp(UdpListener<LinesDecoder>),
    Tcp(TcpListener<LinesDecoder>),
}

/// The `lines_in` listener: a [`LinesDecoder`] over [`UdpListener`] or [`TcpListener`]. See this
/// module's doc.
pub struct LinesInput {
    inner: Inner,
}

/// The stream framing every `lines_in` stream listener uses, bounded at `max_line_bytes`.
const LINES: FramingMode = FramingMode::Lines { oversize: Oversize::DrainToNextLine };

impl LinesInput {
    /// A TCP listener (`transport: tcp`, the default), plaintext until [`Self::with_tls`].
    pub fn tcp(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Tcp(
                TcpListener::new(bind, LinesDecoder::new(), TcpListenerConfig::default())
                    .with_framing(LINES, DEFAULT_MAX_LINE_BYTES),
            ),
        }
    }

    /// A UDP listener (`transport: udp`).
    pub fn udp(bind: impl Into<String>) -> Self {
        Self {
            inner: Inner::Udp(UdpListener::new(
                bind,
                LinesDecoder::new(),
                UdpListenerConfig::default(),
            )),
        }
    }

    /// A listener on a Unix datagram socket at `path` (`transport: unix`), on the UDP driver.
    pub fn unix(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: Inner::Udp(UdpListener::unix(
                "lines_in",
                path,
                SOCKET_MODE,
                LinesDecoder::new(),
                UdpListenerConfig::default(),
            )),
        }
    }

    /// A listener on a Unix stream socket at `path` (`transport: unix_stream`), on the stream
    /// driver with the same line framing as TCP. [`Self::with_tls`] fails on it.
    pub fn unix_stream(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            inner: Inner::Tcp(
                TcpListener::unix(
                    "lines_in",
                    path,
                    SOCKET_MODE,
                    LinesDecoder::new(),
                    TcpListenerConfig::default(),
                )
                .with_framing(LINES, DEFAULT_MAX_LINE_BYTES),
            ),
        }
    }

    /// Sets `max_line_bytes`: the stream framer's bound, and the decoder's for a datagram's lines.
    /// Graph rule 77 rejects `0` before it gets here.
    pub fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.inner = match self.inner {
            Inner::Udp(listener) => {
                Inner::Udp(listener.map_decoder(|d| d.with_max_line_bytes(max_line_bytes)))
            }
            Inner::Tcp(listener) => Inner::Tcp(
                listener
                    .with_framing(LINES, max_line_bytes)
                    .map_decoder(|d| d.with_max_line_bytes(max_line_bytes)),
            ),
        };
        self
    }

    /// Attaches a component id to the driver's diagnostics and to the wrapped [`LinesDecoder`]'s,
    /// which reports `oversize_line`. Every connection's decoder clone shares the decoder's
    /// throttle counts, so it throttles per listener.
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

    /// Attaches a telemetry handle to the driver and to the decoder, which counts a datagram's
    /// oversize lines on it.
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

    /// Sets a datagram listener's `receive:` block; leaves a stream listener untouched.
    /// [`Self::with_tcp_receive`] is the counterpart (`crate::statsd::StatsdInput::with_receive`
    /// says why there are two).
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        if let Inner::Udp(listener) = self.inner {
            self.inner = Inner::Udp(listener.with_config(config));
        }
        self
    }

    /// [`Self::with_receive`]'s stream counterpart; leaves a datagram listener untouched.
    pub fn with_tcp_receive(mut self, config: TcpListenerConfig) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_config(config));
        }
        self
    }

    /// Sets a stream listener's per-phase pre-message budget (`handshake_timeout:`). A datagram
    /// listener is left untouched; graph rule 45 rejects a non-default value there.
    pub fn with_handshake_timeout(mut self, handshake_timeout: std::time::Duration) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_handshake_timeout(handshake_timeout));
        }
        self
    }

    /// Bounds how long a stream connection may stay quiet past its first byte (`idle_timeout:`);
    /// `None` disables it. A datagram listener is left untouched; graph rule 53 rejects the field
    /// there.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<std::time::Duration>) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_idle_timeout(idle_timeout));
        }
        self
    }

    /// Terminates TLS on a TCP listener (`tls:`); paths in `settings` resolve against `base_dir`.
    /// Fails on a datagram listener and on a Unix socket. Graph rules 43 and 65 are what an
    /// operator sees; this backstops a caller that skipped validation.
    pub fn with_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        self.inner = match self.inner {
            Inner::Tcp(listener) => Inner::Tcp(listener.with_tls(settings, base_dir)?),
            Inner::Udp(_) => anyhow::bail!(
                "lines_in: 'tls:' needs 'transport: tcp' -- TLS is defined over a byte stream, \
                 and DTLS is out of scope (docs/adr/syslog-tcp-ingress-and-tls.md); a Unix socket \
                 is always plaintext"
            ),
        };
        Ok(self)
    }

    /// Caps the connections a stream listener serves at once (`max_connections:`). A datagram
    /// listener is left untouched; graph rule 74 rejects a non-default value there.
    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        if let Inner::Tcp(listener) = self.inner {
            self.inner = Inner::Tcp(listener.with_max_connections(max_connections));
        }
        self
    }

    /// The bound address after `bind()`; `None` on a Unix socket.
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
impl Input for LinesInput {
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

/// Splits its input on `LF` into one raw log event per non-empty line; testable without a socket.
///
/// `Clone` because [`TcpListener`] gives every connection its own decoder. A clone shares the one
/// `Arc<Resource>`, which must stay shared: `logit_pipeline::BatchAccumulator::absorb` keys on
/// `Arc::ptr_eq`, so a resource per connection would stop two connections' events sharing a batch.
/// It also shares its `Diagnostics` throttle counts.
#[derive(Clone)]
pub struct LinesDecoder {
    resource: Arc<Resource>,
    diag: Diagnostics,
    /// Counts a datagram's oversize lines; the stream framer counts its own.
    telemetry: Telemetry,
    /// The longest line kept, not counting its `LF`. On a stream the framer already dropped
    /// anything longer, so only a datagram's lines can reach this bound.
    max_line_bytes: usize,
}

impl Default for LinesDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl LinesDecoder {
    pub fn new() -> Self {
        Self {
            resource: Arc::new(Resource::default()),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.max_line_bytes = max_line_bytes;
        self
    }

    /// Test-only: confirms `LinesInput::with_diagnostics` reached this decoder.
    #[cfg(test)]
    pub(crate) fn diag(&self) -> &Diagnostics {
        &self.diag
    }

    /// One line, `[start, end)` of `bytes` with its `LF` already excluded, into `out`.
    fn push_line(
        &mut self,
        bytes: &Bytes,
        start: usize,
        end: usize,
        received_at: i64,
        out: &mut Vec<Event>,
    ) {
        // Measured before the `CR` strip, as the stream framer measures it.
        let len = end - start;
        if len > self.max_line_bytes {
            self.telemetry.count("logit.input.frames.dropped", 1.0, &[("reason", "oversize")]);
            self.diag.warn_throttled(
                "oversize_line",
                format_args!(
                    "a line of {len} bytes is over the {}-byte max_line_bytes; dropping it",
                    self.max_line_bytes
                ),
            );
            return;
        }
        let end = if end > start && bytes[end - 1] == b'\r' { end - 1 } else { end };
        if end == start {
            return;
        }
        let line = bytes.slice(start..end);
        let message = match std::str::from_utf8(&line) {
            Ok(_) => Value::Str(line),
            Err(_) => Value::Bytes(line),
        };
        out.push(Event::log(
            received_at,
            AttrMap::new(),
            LogRecord {
                message,
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        ));
    }
}

impl Decoder for LinesDecoder {
    fn decode_into(
        &mut self,
        bytes: Bytes,
        received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
        let mut start = 0;
        for lf in memchr::memchr_iter(b'\n', &bytes) {
            self.push_line(&bytes, start, lf, received_at, out);
            start = lf + 1;
        }
        // A stream frame has no `LF` left in it, and a datagram's end ends its last line.
        if start < bytes.len() {
            self.push_line(&bytes, start, bytes.len(), received_at, out);
        }
        Ok((self.resource.clone(), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_pipeline::test_util::{
        assert_no_batch, recv_events, scratch_dir, Running, TelemetryProbe,
    };
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    fn decode(input: &[u8]) -> Vec<Event> {
        LinesDecoder::new()
            .decode(Bytes::copy_from_slice(input))
            .expect("decoding lines never fails")
            .events
    }

    fn message(event: &Event) -> &Value {
        &event.log.as_ref().expect("every lines_in event is a log").message
    }

    fn texts(events: &[Event]) -> Vec<&str> {
        events.iter().map(|e| message(e).as_str().expect("a UTF-8 line is a Str")).collect()
    }

    #[test]
    fn a_line_becomes_a_raw_log_event_with_no_attributes() {
        let mut decoder = LinesDecoder::new();
        let mut out = Vec::new();
        let (resource, scope) =
            decoder.decode_into(Bytes::from_static(b"hello world"), 42, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        let event = &out[0];
        assert_eq!(event.timestamp, 42, "stamped with the caller's received_at");
        assert!(event.attributes.is_empty());
        assert!(event.metrics.is_empty() && event.span.is_none());
        let log = event.log.as_ref().unwrap();
        assert_eq!(log.message, Value::Str(Bytes::from_static(b"hello world")));
        assert_eq!(log.body_format, BodyFormat::Raw);
        assert_eq!(log.severity, None);
        assert_eq!(log.observed_timestamp, 0);
        assert!(resource.attributes.is_empty());
        assert!(scope.is_none());
    }

    #[test]
    fn one_trailing_cr_is_stripped_and_empty_lines_are_skipped() {
        let events = decode(b"a\r\n\r\n\nb\r\r\nc");
        assert_eq!(texts(&events), vec!["a", "b\r", "c"]);
    }

    #[test]
    fn invalid_utf8_is_kept_as_bytes() {
        let events = decode(b"ok\n\xff\xfebad\n");
        assert_eq!(events.len(), 2);
        assert_eq!(message(&events[0]).as_str(), Some("ok"));
        assert_eq!(*message(&events[1]), Value::Bytes(Bytes::from_static(b"\xff\xfebad")));
    }

    #[test]
    fn a_datagram_of_several_lines_emits_its_unterminated_tail() {
        let events = decode(b"one\ntwo\nthree");
        assert_eq!(texts(&events), vec!["one", "two", "three"]);
    }

    #[test]
    fn the_message_is_a_zero_copy_slice_of_the_input() {
        let input = Bytes::from_static(b"first\nsecond\n");
        let events = LinesDecoder::new().decode(input.clone()).unwrap().events;
        let Value::Str(second) = message(&events[1]) else { panic!("a UTF-8 line is a Str") };
        assert_eq!(second.as_ptr(), input[6..].as_ptr(), "a slice, not a copy");
    }

    #[test]
    fn clones_share_one_resource() {
        let mut first = LinesDecoder::new();
        let mut second = first.clone();
        let mut out = Vec::new();
        let (a, _) = first.decode_into(Bytes::from_static(b"x"), 0, &mut out).unwrap();
        let (b, _) = second.decode_into(Bytes::from_static(b"y"), 0, &mut out).unwrap();
        assert!(Arc::ptr_eq(&a, &b), "BatchAccumulator::absorb batches on Arc::ptr_eq");
    }

    /// The datagram path's own bound: the oversize line is dropped and counted, its neighbors
    /// still decode, and a line at the bound is kept.
    #[test]
    fn an_oversize_datagram_line_is_dropped_and_counted() {
        let mut probe = TelemetryProbe::new();
        let diag = Diagnostics::new("lines_in");
        let mut decoder = LinesDecoder::new()
            .with_telemetry(probe.telemetry("lines_in", "lines_in", "listener"))
            .with_diagnostics(diag.clone())
            .with_max_line_bytes(4);
        let events = decoder.decode(Bytes::from_static(b"abcd\nabcde\nabc\r\nxyz")).unwrap().events;
        assert_eq!(texts(&events), vec!["abcd", "abc", "xyz"]);
        assert_eq!(probe.sum("logit.input.frames.dropped", &[("reason", "oversize")]), 1.0);
        assert_eq!(diag.occurrences("oversize_line"), 1);
    }

    #[test]
    fn with_diagnostics_reaches_the_decoder_on_every_transport() {
        for input in [LinesInput::tcp("127.0.0.1:0"), LinesInput::unix_stream("/tmp/x.socket")] {
            let input = input.with_diagnostics(Diagnostics::new("tcp-id"));
            match &input.inner {
                Inner::Tcp(listener) => {
                    assert_eq!(listener.decoder().diag().component_id(), "tcp-id");
                    assert_eq!(listener.diag().component_id(), "tcp-id");
                }
                Inner::Udp(_) => panic!("a stream transport must build a TCP listener"),
            }
        }
        for input in [LinesInput::udp("127.0.0.1:0"), LinesInput::unix("/tmp/x.socket")] {
            let input = input.with_diagnostics(Diagnostics::new("udp-id"));
            match &input.inner {
                Inner::Udp(listener) => {
                    assert_eq!(listener.decoder().diag().component_id(), "udp-id");
                    assert_eq!(listener.diag().component_id(), "udp-id");
                }
                Inner::Tcp(_) => panic!("a datagram transport must build a UDP listener"),
            }
        }
    }

    #[test]
    fn with_tls_fails_off_tcp() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        for input in [
            LinesInput::udp("127.0.0.1:0"),
            LinesInput::unix("/tmp/x.socket"),
            LinesInput::unix_stream("/tmp/x.socket"),
        ] {
            let err = input.with_tls(&settings, &testdata_tls_dir()).err().expect("must fail");
            assert!(err.to_string().contains("plaintext"), "{err}");
        }
    }

    // ---- running listeners -------------------------------------------------------------------

    /// A bound, running listener: one event per batch with no flush timer, so each line is
    /// delivered as soon as it is decoded.
    struct Started {
        addr: Option<std::net::SocketAddr>,
        rx: tokio::sync::mpsc::Receiver<logit_pipeline::Delivered>,
        running: Running,
        probe: TelemetryProbe,
    }

    async fn start(input: LinesInput) -> Started {
        let probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("lines_in", "lines_in", "listener");
        let input = input
            .with_diagnostics(Diagnostics::new("lines_in").with_telemetry(telemetry.clone()))
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
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let mut input = input;
        input.bind().await.expect("bind should succeed");
        let addr = input.local_addr();
        let running = spawn_bound(input, Fanout::new(vec![tx]));
        Started { addr, rx, running, probe }
    }

    /// `test_util::spawn_input` for an input already bound, so its address was readable first.
    fn spawn_bound(mut input: LinesInput, sink: Fanout) -> Running {
        let (shutdown, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { input.run_until_shutdown(sink, rx).await });
        Running { shutdown, handle }
    }

    fn testdata_tls_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    #[tokio::test]
    async fn a_tcp_line_becomes_an_event_and_crlf_is_stripped() {
        let mut started = start(LinesInput::tcp("127.0.0.1:0")).await;
        let mut client = TcpStream::connect(started.addr.unwrap()).await.unwrap();
        client.write_all(b"{\"a\":1}\r\n12345 starts with digits\n\n").await.unwrap();
        client.flush().await.unwrap();

        let events = recv_events(&mut started.rx, 2).await;
        assert_eq!(texts(&events), vec!["{\"a\":1}", "12345 starts with digits"]);
        started.running.stop().await;
    }

    #[tokio::test]
    async fn a_tcp_line_split_across_writes_is_reassembled() {
        let mut started = start(LinesInput::tcp("127.0.0.1:0")).await;
        let mut client = TcpStream::connect(started.addr.unwrap()).await.unwrap();
        client.write_all(b"split ").await.unwrap();
        client.flush().await.unwrap();
        // The first half must reach the listener alone; this window only orders the two reads.
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.write_all(b"across writes\n").await.unwrap();
        client.flush().await.unwrap();

        let events = recv_events(&mut started.rx, 1).await;
        assert_eq!(texts(&events), vec!["split across writes"]);
        started.running.stop().await;
    }

    /// The framer bound is `max_line_bytes`: the long line is skipped and counted, and the next
    /// line on the same connection still decodes.
    #[tokio::test]
    async fn an_oversize_tcp_line_is_skipped_and_the_connection_continues() {
        let mut started = start(LinesInput::tcp("127.0.0.1:0").with_max_line_bytes(8)).await;
        let mut client = TcpStream::connect(started.addr.unwrap()).await.unwrap();
        client.write_all(b"much too long a line\nshort\n").await.unwrap();
        client.flush().await.unwrap();

        let events = recv_events(&mut started.rx, 1).await;
        assert_eq!(texts(&events), vec!["short"], "the oversize line is never delivered");
        assert_eq!(started.probe.sum("logit.input.frames.dropped", &[("reason", "oversize")]), 1.0);
        started.running.stop().await;
    }

    /// An unterminated final line at a clean close is dropped and counted, the stream driver's
    /// rule; a datagram's tail differs (`a_datagram_of_several_lines_emits_its_unterminated_tail`).
    #[tokio::test]
    async fn an_unterminated_tcp_tail_is_dropped_and_counted_truncated() {
        let mut started = start(LinesInput::tcp("127.0.0.1:0")).await;
        let mut client = TcpStream::connect(started.addr.unwrap()).await.unwrap();
        client.write_all(b"whole\nhalf a li").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(texts(&recv_events(&mut started.rx, 1).await), vec!["whole"]);
        drop(client);

        started
            .probe
            .wait_for("the truncated tail to be counted", |t| {
                t.sum("logit.input.frames.dropped", &[("reason", "truncated")]) == 1.0
            })
            .await;
        assert_no_batch(&mut started.rx, Duration::from_millis(100), "the half line").await;
        started.running.stop().await;
    }

    #[tokio::test]
    async fn a_tls_tcp_connection_round_trips_a_line() {
        let settings = TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        };
        let input = LinesInput::tcp("127.0.0.1:0")
            .with_tls(&settings, &testdata_tls_dir())
            .expect("a tcp listener takes tls");
        let mut started = start(input).await;

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
        let stream = TcpStream::connect(started.addr.unwrap()).await.unwrap();
        // `testdata/tls/server.pem` carries a `localhost` SAN.
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut client =
            tokio::time::timeout(Duration::from_secs(5), connector.connect(name, stream))
                .await
                .expect("the TLS handshake should complete within 5s")
                .expect("the TLS handshake should succeed");
        client.write_all(b"over tls\n").await.unwrap();
        client.flush().await.unwrap();

        assert_eq!(texts(&recv_events(&mut started.rx, 1).await), vec!["over tls"]);
        started.running.stop().await;
    }

    #[tokio::test]
    async fn a_udp_datagram_of_several_lines_round_trips() {
        let mut started = start(LinesInput::udp("127.0.0.1:0")).await;
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"one\r\ntwo\nthree", started.addr.unwrap()).await.unwrap();

        let events = recv_events(&mut started.rx, 3).await;
        assert_eq!(texts(&events), vec!["one", "two", "three"]);
        started.running.stop().await;
    }

    /// `with_max_line_bytes` reaches a UDP listener's decoder.
    #[tokio::test]
    async fn an_oversize_udp_line_is_dropped_and_counted() {
        let mut started = start(LinesInput::udp("127.0.0.1:0").with_max_line_bytes(4)).await;
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client.send_to(b"too long\nok", started.addr.unwrap()).await.unwrap();

        assert_eq!(texts(&recv_events(&mut started.rx, 1).await), vec!["ok"]);
        assert_eq!(started.probe.sum("logit.input.frames.dropped", &[("reason", "oversize")]), 1.0);
        started.running.stop().await;
    }

    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[tokio::test]
    async fn a_unix_datagram_round_trips_and_the_socket_has_its_mode() {
        let path = scratch_dir("lines-dgram").join("lines.sock");
        let mut started = start(LinesInput::unix(&path)).await;
        assert_eq!(started.addr, None);
        assert_eq!(mode_of(&path), SOCKET_MODE);

        let client = tokio::net::UnixDatagram::unbound().unwrap();
        client.send_to(b"a\nb", &path).await.unwrap();
        assert_eq!(texts(&recv_events(&mut started.rx, 2).await), vec!["a", "b"]);
        started.running.stop().await;
    }

    /// `unix_stream` is newline-framed, not `statsd_in`'s length prefix.
    #[tokio::test]
    async fn a_unix_stream_round_trips_newline_delimited_lines() {
        let path = scratch_dir("lines-stream").join("lines.sock");
        let input = LinesInput::unix_stream(&path);
        assert_eq!(input.socket_path(), Some(path.as_path()));
        let mut started = start(input).await;
        assert_eq!(mode_of(&path), SOCKET_MODE);

        let mut client = tokio::net::UnixStream::connect(&path).await.unwrap();
        client.write_all(b"first\nsecond\n").await.unwrap();
        client.flush().await.unwrap();
        assert_eq!(texts(&recv_events(&mut started.rx, 2).await), vec!["first", "second"]);
        started.running.stop().await;
    }
}
