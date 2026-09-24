//! `graphite_out`: carbon plaintext/pickle over UDP or TCP, the mirror of `graphite_in`
//! ([ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md)).
//!
//! [`logit_proto::graphite::GraphiteEncoder`] makes every mapping, sanitization, timestamp, and
//! per-record allocation decision, and emits its own metric/tag/name counters and diagnostics
//! through the `Telemetry`/`Diagnostics` this sink's builders hand it; `logit_proto::graphite`'s
//! module doc is the spec. This module is the transport: [`GraphiteOutput`] owns the socket and
//! writes the encoder's [`logit_proto::MessageBuf`]`<usize>`, one entry per plaintext line or per
//! length-prefixed pickle frame, meta = that entry's datapoint count. It implements
//! [`logit_proto::FramedEncoder`] for the reason `statsd_out` does (ADR `framed-encoder`).
//!
//! The TCP half ports `StatsdOutput`'s `Conn`/lazy-connect/reconnect-once/partial-write shape,
//! cited at each borrowed item; there's no shared TCP sink driver yet.
//!
//! ## Config
//!
//! - `endpoint`: `host:port`, resolved once per batch, never at config load.
//! - `transport`: `tcp` (default) or `udp`.
//! - `protocol`: `plaintext` (default) or `pickle`, TCP only (graph rule 46).
//! - `tags`/`multi_value`: forwarded to the encoder; no sink-level meaning.
//! - `max_packet_bytes`: default `1432`, UDP only; TCP has no datagram to overflow.
//! - `max_frame_bytes`: default 1 MiB, Twisted's `Int32StringReceiver.MAX_LENGTH`.
//! - `connect_timeout`: TCP only, default `5s`.
//!
//! ## Packing
//!
//! - **Plaintext UDP**: lines are packed newline-joined, **no trailing newline**, into as few
//!   datagrams as fit under `max_packet_bytes`. The encoder already dropped any single line over
//!   the cap, so every line fits a datagram on its own.
//! - **Plaintext TCP**: every line `\n`-terminated, **including the last**.
//! - **Pickle** (TCP only): frames concatenated with no separator; the receiver parses each off
//!   its own length prefix.
//!
//! On TCP, the whole batch is one buffer and one write (partial, then `write_all`).
//!
//! ## Faults
//!
//! - UDP `EMSGSIZE` (raw OS error 90, or `ErrorKind::InvalidInput` where the shim never reaches
//!   the syscall) drops that datagram's datapoints under
//!   `logit.output.messages.dropped{reason="oversize_datagram"}` plus a throttled diagnostic, and
//!   sending continues. Any other UDP send error is [`Fault::Clean`] if no datagram of the batch
//!   was sent yet, else [`Fault::Ambiguous`].
//! - TCP: a connect failure or timeout is [`Fault::Clean`]. A write failing before any byte left
//!   is retried with one reconnect. A write failing after a byte left is [`Fault::Ambiguous`] and
//!   never retried by this sink.
//!
//! ## Telemetry
//!
//! Transport-level only; the codec documents its own. `logit.output.batch.bytes` (only when there
//! is something to send), `logit.output.request.duration`,
//! `logit.output.requests{class="ok"|"error"}`, `logit.output.messages` (entries sent),
//! `logit.output.datapoints` (Σ sent entries' meta; equals `messages` for plaintext),
//! `logit.output.datagrams` (UDP only), and the `oversize_datagram` drop above.
//!
//! ## Duplicate safety
//!
//! [`GraphiteOutput::duplicate_safe`] is `true` because whisper is last-write-wins per
//! `(path, second)`: a redelivered datapoint overwrites the same number rather than accumulating
//! like a collectd COUNTER or statsd `|c`. That's whisper's behavior, not the carbon wire's; a
//! non-whisper receiver on the same wire could add instead, and this sink can't tell.

use crate::tls::{poll_pending_close, PendingClose};
use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{Fault, Output};
use logit_proto::graphite::{GraphiteEncoder, Protocol};
use logit_proto::{FramedEncoder, MessageBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpStream, UdpSocket};

/// Which transport a `graphite_out` was configured with: the target of `build_spec`'s
/// `graphite_out_transport` converter, which picks [`GraphiteOutput::udp`] or
/// [`GraphiteOutput::tcp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Udp,
    Tcp,
}

/// `statsd`'s `Conn`, in shape. `Tcp`'s `stream` starts `None`: an eager connect would turn "the
/// destination isn't up yet" into a startup failure instead of a retryable `send`-time one.
enum Conn {
    Udp(UdpSocket),
    Tcp { stream: Option<TcpStream>, connect_timeout: Duration },
}

/// `logit_pipeline::Output` for `graphite_out`, built via [`GraphiteOutput::udp`] or
/// [`GraphiteOutput::tcp`].
pub struct GraphiteOutput {
    endpoint: String,
    conn: Conn,
    encoder: GraphiteEncoder,
    /// Kept here too so [`GraphiteOutput::with_encoder`] can re-apply it in any builder order.
    /// UDP datagram cap only ([`GraphiteOutput::encoder_cap`]).
    max_packet_bytes: usize,
    /// Reused across `send`s: the encoder's output, meta = each entry's datapoint count.
    buf: MessageBuf<usize>,
    /// Reused across `send`s: the packed UDP datagram, or the whole TCP write buffer.
    packet_buf: Vec<u8>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl GraphiteOutput {
    /// Binds an ephemeral local UDP socket eagerly; `endpoint` is resolved per `send`.
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")
            .context("binding graphite_out's local UDP socket")?;
        socket.set_nonblocking(true).context("configuring graphite_out's UDP socket")?;
        let socket =
            UdpSocket::from_std(socket).context("registering graphite_out's UDP socket")?;
        Ok(Self::new(endpoint, Conn::Udp(socket)))
    }

    /// Never connects here -- see [`Conn`]'s doc comment.
    pub fn tcp(endpoint: impl Into<String>, connect_timeout: Duration) -> Self {
        Self::new(endpoint, Conn::Tcp { stream: None, connect_timeout })
    }

    fn new(endpoint: impl Into<String>, conn: Conn) -> Self {
        Self {
            endpoint: endpoint.into(),
            conn,
            encoder: GraphiteEncoder::new(),
            max_packet_bytes: logit_proto::graphite::DEFAULT_MAX_PACKET_BYTES,
            buf: MessageBuf::default(),
            packet_buf: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
        .with_max_packet_bytes(logit_proto::graphite::DEFAULT_MAX_PACKET_BYTES)
    }

    /// The encoder's line cap: `max_packet_bytes` on UDP, none on TCP.
    fn encoder_cap(&self) -> usize {
        if matches!(self.conn, Conn::Udp(_)) {
            self.max_packet_bytes
        } else {
            usize::MAX
        }
    }

    /// Installs `encoder` with this sink's line cap, diagnostics, and telemetry re-applied, so
    /// builder order doesn't matter (`CollectdOutput::with_encoder` says what goes wrong
    /// otherwise).
    pub fn with_encoder(mut self, encoder: GraphiteEncoder) -> Self {
        self.encoder = encoder
            .with_max_packet_bytes(self.encoder_cap())
            .with_diagnostics(self.diag.clone())
            .with_telemetry(self.telemetry.clone());
        self
    }

    /// Bounds one UDP datagram of packed lines; no effect on TCP.
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        let cap = self.encoder_cap();
        self.encoder = self.encoder.with_max_packet_bytes(cap);
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry.clone();
        self.encoder = self.encoder.with_telemetry(telemetry);
        self
    }
}

#[async_trait::async_trait]
impl Output for GraphiteOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // `Stats` discarded: the encoder already reported them through its own handles.
        self.encoder.encode_into(batch, &mut self.buf);

        if self.buf.is_empty() {
            return Ok(());
        }

        self.telemetry.count("logit.output.batch.bytes", self.buf.total_bytes() as f64, &[]);
        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = match &mut self.conn {
            Conn::Udp(socket) => {
                Self::send_udp(
                    socket,
                    &self.endpoint,
                    &self.buf,
                    self.max_packet_bytes,
                    &mut self.packet_buf,
                    &mut self.diag,
                    &self.telemetry,
                )
                .await
            }
            Conn::Tcp { stream, connect_timeout } => {
                Self::send_tcp(
                    stream,
                    &self.endpoint,
                    *connect_timeout,
                    &self.buf,
                    self.encoder.protocol(),
                    &mut self.packet_buf,
                )
                .await
            }
        };
        drop(request_timer);

        match &result {
            Ok((messages, datapoints, datagrams)) => {
                self.telemetry.count("logit.output.messages", *messages as f64, &[]);
                self.telemetry.count("logit.output.datapoints", *datapoints as f64, &[]);
                if matches!(self.conn, Conn::Udp(_)) {
                    self.telemetry.count("logit.output.datagrams", *datagrams as f64, &[]);
                }
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "ok")]);
            }
            Err(_) => {
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "error")]);
            }
        }
        result.map(|_| ())
    }

    /// `send` buffers nothing between calls; this only flushes an open TCP stream.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { stream: Some(stream), .. } = &mut self.conn {
            stream.flush().await.context("flushing graphite_out TCP stream")?;
        }
        Ok(())
    }

    /// Whisper is last-write-wins per `(path, second)`; the module doc's "Duplicate safety" has
    /// the boundary.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// Running totals for one [`GraphiteOutput::send_udp`] call: `statsd`'s `UdpSendCounts` plus
/// `datapoints`. An `oversize_datagram` drop counts datapoints (Σ `meta`), not entries; the two
/// coincide for plaintext, the only protocol UDP allows.
#[derive(Default)]
struct UdpSendCounts {
    /// Entries written to the socket: `logit.output.messages`.
    messages: usize,
    /// Σ written entries' `meta`: `logit.output.datapoints`.
    datapoints: usize,
    /// Datagrams written to the socket: `logit.output.datagrams`.
    datagrams: usize,
    /// Entries appended to `packet_buf` since the last flush; reset by every flush.
    entries_in_packet: usize,
    /// Σ `meta` appended to `packet_buf` since the last flush; reset by every flush.
    datapoints_in_packet: usize,
}

impl GraphiteOutput {
    /// Packs `buf` into as few datagrams as fit under `max_packet_bytes`, one `send_to` each, as
    /// `StatsdOutput::send_udp` does. Returns `(messages, datapoints, datagrams)` sent.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        buf: &MessageBuf<usize>,
        max_packet_bytes: usize,
        packet_buf: &mut Vec<u8>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize, usize)> {
        // Once per batch: a non-numeric host must not be re-resolved per datagram.
        let mut addrs = lookup_host(endpoint)
            .await
            .context("resolving graphite_out endpoint")
            .context(Fault::Clean)?;
        let addr = addrs
            .next()
            .context("graphite_out endpoint resolved to no addresses")
            .context(Fault::Clean)?;

        let mut counts = UdpSendCounts::default();
        packet_buf.clear();
        for (msg, meta) in buf.iter_with() {
            let needs_sep = !packet_buf.is_empty();
            let extra = msg.len() + usize::from(needs_sep);
            if !packet_buf.is_empty() && packet_buf.len() + extra > max_packet_bytes {
                Self::flush_datagram(socket, addr, packet_buf, &mut counts, diag, telemetry)
                    .await?;
            }
            if needs_sep && !packet_buf.is_empty() {
                packet_buf.push(b'\n');
            }
            packet_buf.extend_from_slice(msg);
            counts.entries_in_packet += 1;
            counts.datapoints_in_packet += *meta;
        }
        if !packet_buf.is_empty() {
            Self::flush_datagram(socket, addr, packet_buf, &mut counts, diag, telemetry).await?;
        }
        Ok((counts.messages, counts.datapoints, counts.datagrams))
    }

    /// Sends one packed datagram, then clears `packet_buf` and the per-packet counters.
    async fn flush_datagram(
        socket: &UdpSocket,
        addr: std::net::SocketAddr,
        packet_buf: &mut Vec<u8>,
        counts: &mut UdpSendCounts,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<()> {
        match socket.send_to(packet_buf, addr).await {
            Ok(_) => {
                counts.messages += counts.entries_in_packet;
                counts.datapoints += counts.datapoints_in_packet;
                counts.datagrams += 1;
            }
            Err(err) if is_message_too_large(&err) => {
                telemetry.count(
                    "logit.output.messages.dropped",
                    counts.datapoints_in_packet as f64,
                    &[("reason", "oversize_datagram")],
                );
                diag.warn_throttled(
                    "oversize_datagram",
                    format_args!("graphite_out: packed datagram too large for one send: {err}"),
                );
            }
            Err(err) => {
                let fault = if counts.datagrams > 0 { Fault::Ambiguous } else { Fault::Clean };
                packet_buf.clear();
                counts.entries_in_packet = 0;
                counts.datapoints_in_packet = 0;
                return Err(anyhow::Error::new(err).context(fault));
            }
        }
        packet_buf.clear();
        counts.entries_in_packet = 0;
        counts.datapoints_in_packet = 0;
        Ok(())
    }

    /// One write (partial, then `write_all`) per batch, with at most one reconnect-and-retry:
    /// `StatsdOutput::send_tcp`'s control flow, including cancellation safety via `stream.take()`
    /// and never resending once a byte has left this host. Only the buffer differs (module doc,
    /// "Packing"). Returns `(messages, datapoints, 0)`; TCP has no datagram count.
    ///
    /// **A reused connection is probed before the first write.** The receiver may have closed it
    /// since the last `send` (a restart, a far-end `idle_timeout:`), and carbon has no ack to say
    /// so: the write would land in the local socket buffer and the datapoints would be lost. So a
    /// connection taken from `*stream`, never a fresh one, gets one non-consuming poll
    /// ([`crate::tls::poll_pending_close`]); anything but open is replaced before anything is
    /// written. That doesn't use up the post-write-failure retry
    /// (`docs/adr/idle-connection-timeout.md`).
    async fn send_tcp(
        stream: &mut Option<TcpStream>,
        endpoint: &str,
        connect_timeout: Duration,
        buf: &MessageBuf<usize>,
        protocol: Protocol,
        frame_buf: &mut Vec<u8>,
    ) -> anyhow::Result<(usize, usize, usize)> {
        frame_buf.clear();
        let mut datapoints = 0usize;
        for (msg, meta) in buf.iter_with() {
            frame_buf.extend_from_slice(msg);
            if protocol == Protocol::Plaintext {
                frame_buf.push(b'\n');
            }
            datapoints += *meta;
        }

        let mut retried_after_a_zero_byte_failure = false;
        loop {
            let mut conn = match stream.take() {
                // The probe (doc comment). A closed connection was never written to, so
                // replacing it doesn't consume `retried_after_a_zero_byte_failure`.
                Some(mut conn) => {
                    let mut probe = [0u8; 1];
                    let pending = poll_pending_close(&mut conn, &mut probe).await;
                    match pending {
                        PendingClose::Open => conn,
                        _closed => {
                            drop(conn);
                            connect(endpoint, connect_timeout).await?
                        }
                    }
                }
                None => connect(endpoint, connect_timeout).await?,
            };

            let first_write = match conn.write(frame_buf).await {
                Ok(0) if !frame_buf.is_empty() => {
                    Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "wrote zero bytes"))
                }
                Ok(n) => Ok(n),
                Err(err) => Err(err),
            };

            match first_write {
                Ok(n) => {
                    let rest_result = if n < frame_buf.len() {
                        conn.write_all(&frame_buf[n..]).await
                    } else {
                        Ok(())
                    };
                    return match rest_result {
                        Ok(()) => {
                            *stream = Some(conn);
                            Ok((buf.len(), datapoints, 0))
                        }
                        // Never resent: a byte already left, so the peer may apply it twice.
                        Err(err) => Err(anyhow::Error::new(err).context(Fault::Ambiguous)),
                    };
                }
                Err(_) if !retried_after_a_zero_byte_failure => {
                    retried_after_a_zero_byte_failure = true;
                    continue;
                }
                Err(err) => return Err(anyhow::Error::new(err).context(Fault::Clean)),
            }
        }
    }
}

/// One fresh TCP connection to `endpoint`, raced against `connect_timeout`. Always
/// `Fault::Clean`: nothing of a batch has left while connecting. Unlike `statsd_out`/
/// `syslog_out`'s `TcpDial::connect`, there's no TLS phase and no `logit.output.reconnects`.
async fn connect(endpoint: &str, connect_timeout: Duration) -> anyhow::Result<TcpStream> {
    tokio::time::timeout(connect_timeout, TcpStream::connect(endpoint))
        .await
        .context("connecting to graphite_out endpoint timed out")
        .and_then(|r| r.context("connecting to graphite_out endpoint"))
        .context(Fault::Clean)
}

/// `90` is `EMSGSIZE` on Linux, the only platform `logit` ships for; a copy of
/// `statsd::is_message_too_large`.
fn is_message_too_large(err: &std::io::Error) -> bool {
    matches!(err.raw_os_error(), Some(errno) if errno == 90 /* EMSGSIZE, Linux */)
        || err.kind() == std::io::ErrorKind::InvalidInput
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{
        AttrMap, BodyFormat, Event, LogRecord, MetricKind, MetricRecord, Registry, Resource, Value,
    };
    use logit_proto::graphite::GraphiteDecoder;
    use logit_proto::Decoder;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::{TcpListener, UdpSocket as TokioUdpSocket};
    use tokio::sync::Mutex;

    const TS: i64 = 1_700_000_000_000_000_000;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    /// A single-gauge, single-tag event, decode-shaped so whole-`EventBatch` equality against a
    /// real decode is meaningful.
    fn tagged_event(name: &str, value: f64, tag: (&str, &str)) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(tag.0, Value::from(tag.1));
        let mut event = Event::empty(TS, attrs);
        event.metrics.push(MetricRecord::new(intern(name), MetricKind::Gauge(value)));
        event
    }

    fn gauge_event(name: &str, value: f64) -> Event {
        Event::metric(TS, AttrMap::new(), MetricRecord::new(intern(name), MetricKind::Gauge(value)))
    }

    fn log_event() -> Event {
        Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("x"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    /// Whether `registry` recorded a point named `metric` carrying `tag`. Drains, so call once.
    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> bool {
        registry.drain(0).into_iter().any(|event| {
            event.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == metric)
                && event.attributes.get(tag.0).and_then(|v| v.as_str()) == Some(tag.1)
        })
    }

    /// The summed value of every point named `metric` in an already-drained `events`, any tags.
    fn metric_sum(events: &[Event], metric: &str) -> f64 {
        events
            .iter()
            .flat_map(|e| &e.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == metric)
            .map(|m| match &m.kind {
                MetricKind::Sum(s) => s.value,
                MetricKind::Gauge(v) => *v,
                other => panic!("{metric} must be a counter or gauge, got {other:?}"),
            })
            .sum()
    }

    // -- Socket ---------------------------------------------------------------------------------

    async fn udp_collector() -> (SocketAddr, Arc<TokioUdpSocket>) {
        let socket = TokioUdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        (addr, Arc::new(socket))
    }

    async fn tcp_collector() -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        {
            let received = Arc::clone(&received);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else { break };
                    use tokio::io::AsyncReadExt;
                    let mut buf = Vec::new();
                    let _ = stream.read_to_end(&mut buf).await;
                    received.lock().await.push(buf);
                }
            });
        }
        (addr, received)
    }

    /// Whole-`EventBatch` equality against the input: a fixed-point assertion.
    #[tokio::test]
    async fn a_line_round_trips_through_a_real_collector_and_the_real_decoder() {
        let (addr, collector) = udp_collector().await;
        let mut output = GraphiteOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![tagged_event("app.requests", 42.0, ("env", "prod"))]);

        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, _) = collector.recv_from(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();

        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        let (resource, scope) = decoder
            .decode_into(bytes::Bytes::from(received), TS, &mut events)
            .expect("the real decoder must accept what this sink sent");
        let decoded = EventBatch { resource, scope, events };
        assert_eq!(decoded, batch, "decode(send(b)) must equal b, not just a few fields");
    }

    #[tokio::test]
    async fn tcp_terminates_every_line_including_the_last_one() {
        let (addr, received) = tcp_collector().await;
        let mut output = GraphiteOutput::tcp(addr.to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![gauge_event("a.metric", 1.0), gauge_event("b.metric", 2.0)]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let got = received.lock().await;
        let text = String::from_utf8_lossy(&got[0]);
        assert!(text.ends_with('\n'), "the last line must be newline-terminated too: {text:?}");
        assert_eq!(text.lines().count(), 2);
    }

    #[tokio::test]
    async fn udp_datagrams_carry_no_trailing_newline() {
        let (addr, collector) = udp_collector().await;
        let mut output = GraphiteOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![gauge_event("a.metric", 1.0), gauge_event("b.metric", 2.0)]);

        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, _) = collector.recv_from(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();
        assert!(!received.ends_with(b"\n"), "a UDP datagram must not end with a trailing newline");
    }

    /// Checks both the decoded events and each datagram's size, since a packing bug can bleed
    /// across datagrams and still decode correctly.
    #[tokio::test]
    async fn a_low_cap_packs_several_events_into_several_datagrams_none_over_cap() {
        let (addr, collector) = udp_collector().await;
        const CAP: usize = 32;
        let mut output = GraphiteOutput::udp(addr.to_string()).unwrap().with_max_packet_bytes(CAP);
        let events: Vec<Event> = (0..10).map(|i| gauge_event(&format!("m{i}"), i as f64)).collect();
        let batch = batch_with(events);

        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let mut got = Vec::new();
            while let Ok(Ok((n, _))) =
                tokio::time::timeout(Duration::from_millis(300), collector.recv_from(&mut buf))
                    .await
            {
                got.push(buf[..n].to_vec());
            }
            got
        });
        output.send(&batch).await.expect("send should succeed");
        let datagrams = recv_task.await.unwrap();
        assert!(datagrams.len() > 1, "10 lines cannot fit one 32-byte datagram");
        for datagram in &datagrams {
            assert!(
                datagram.len() <= CAP,
                "a datagram of {} bytes exceeded the cap",
                datagram.len()
            );
        }

        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()));
        let mut decoded = Vec::new();
        for datagram in datagrams {
            decoder
                .decode_into(bytes::Bytes::from(datagram), TS, &mut decoded)
                .expect("every datagram must decode");
        }
        assert_eq!(decoded, batch.events, "decode(send(b)) must equal b, not just a count");
    }

    /// Strips the 4-byte length prefix and decodes the payload with the real pickle decoder.
    #[tokio::test]
    async fn a_pickle_send_writes_one_length_prefixed_frame_a_real_reader_accepts() {
        let (addr, received) = tcp_collector().await;
        let mut output = GraphiteOutput::tcp(addr.to_string(), Duration::from_secs(2))
            .with_encoder(GraphiteEncoder::new().with_protocol(Protocol::Pickle));
        let batch = batch_with(vec![tagged_event("app.requests", 42.0, ("env", "prod"))]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;

        let got = received.lock().await;
        let frame = &got[0];
        assert!(frame.len() > 4, "a frame must carry at least its own length prefix");
        let declared_len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(
            declared_len,
            frame.len() - 4,
            "the prefix must declare exactly the payload len"
        );

        let mut decoder =
            GraphiteDecoder::new(Arc::new(Resource::default())).with_protocol(Protocol::Pickle);
        let mut events = Vec::new();
        decoder
            .decode_into(bytes::Bytes::copy_from_slice(&frame[4..]), TS, &mut events)
            .expect("the real decoder in pickle mode must accept what this sink sent");
        assert_eq!(events, batch.events);
    }

    #[tokio::test]
    async fn a_batch_with_nothing_encodable_performs_no_io_at_all() {
        let mut output = GraphiteOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![log_event()]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    #[tokio::test]
    async fn a_tcp_batch_with_nothing_encodable_performs_no_io_at_all() {
        let mut output = GraphiteOutput::tcp("127.0.0.1:1", Duration::from_secs(1));
        let batch = batch_with(vec![log_event()]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    // -- Faults ---------------------------------------------------------------------------------

    #[tokio::test]
    async fn an_unresolvable_udp_endpoint_is_classified_as_a_clean_fault() {
        let mut output = GraphiteOutput::udp("no-port-in-this-endpoint").unwrap();
        let batch = batch_with(vec![gauge_event("hits", 1.0)]);
        let err = output.send(&batch).await.expect_err("resolution should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn tcp_connect_refused_is_classified_as_a_clean_fault() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let mut output = GraphiteOutput::tcp(addr.to_string(), Duration::from_millis(500));
        let batch = batch_with(vec![gauge_event("hits", 1.0)]);
        let err = output.send(&batch).await.expect_err("connect should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    /// A first write that fails with zero bytes sent reconnects and retries once.
    #[tokio::test]
    async fn tcp_reconnects_exactly_once_after_the_peer_resets_an_inherited_connection() {
        let (addr, received) = tcp_collector().await;
        let mut output = GraphiteOutput::tcp(addr.to_string(), Duration::from_secs(2));

        let batch1 = batch_with(vec![gauge_event("first", 1.0)]);
        output.send(&batch1).await.expect("first send should succeed against a fresh connection");

        if let Conn::Tcp { stream: Some(stream), .. } = &mut output.conn {
            stream.shutdown().await.expect("local shutdown should succeed");
        }

        let batch2 = batch_with(vec![gauge_event("second", 1.0)]);
        output
            .send(&batch2)
            .await
            .expect("second send should reconnect once and succeed, not surface the failure");

        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let got = received.lock().await;
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("second")));
    }

    /// `tcp_collector`, but each connection closes (a clean FIN) once it has read anything, as a
    /// restarting or idle-timing-out carbon receiver would.
    async fn tcp_collector_that_closes_after_one_read(
    ) -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let accepts = Arc::new(AtomicUsize::new(0));
        {
            let received = Arc::clone(&received);
            let accepts = Arc::clone(&accepts);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else { break };
                    accepts.fetch_add(1, Ordering::SeqCst);
                    use tokio::io::AsyncReadExt;
                    let mut buf = vec![0u8; 8192];
                    if let Ok(n) = stream.read(&mut buf).await {
                        buf.truncate(n);
                        received.lock().await.push(buf);
                    }
                    // `stream` drops here: one batch read, then a clean close.
                }
            });
        }
        (addr, received, accepts)
    }

    /// After the receiver closes the pooled connection, the next batch still arrives. Asserts on
    /// the collector's second accept and the line, since a write into a FIN'd socket returns `Ok`.
    #[tokio::test]
    async fn a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost(
    ) {
        let (addr, received, accepts) = tcp_collector_that_closes_after_one_read().await;
        let mut output = GraphiteOutput::tcp(addr.to_string(), Duration::from_secs(2));

        output
            .send(&batch_with(vec![gauge_event("first", 1.0)]))
            .await
            .expect("first send should succeed against a fresh connection");

        // Let the FIN land before the probe looks for it.
        tokio::time::sleep(Duration::from_millis(100)).await;

        output
            .send(&batch_with(vec![gauge_event("second", 1.0)]))
            .await
            .expect("the probe should reconnect rather than write into a closed socket");

        drop(output);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the probe must have dialled a second connection for the second batch"
        );
        let got = received.lock().await;
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(
            got.iter().any(|b| String::from_utf8_lossy(b).contains("second")),
            "the second batch must actually have reached the receiver: {got:?}"
        );
    }

    /// A write failing after a byte already left is `Fault::Ambiguous`, never retried. The
    /// collector resets the connection after the first partial `write` of a large batch.
    // `set_linger` blocks the thread on drop; acceptable for a one-shot loopback RST in a test.
    #[allow(deprecated)]
    #[tokio::test]
    async fn a_write_failing_after_bytes_already_left_this_host_is_an_ambiguous_fault() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                // Long enough for the sink's first `write()` to return a partial count.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = stream.set_linger(Some(Duration::ZERO));
                drop(stream);
            }
        });

        let mut output = GraphiteOutput::tcp(addr.to_string(), Duration::from_secs(2));
        // Larger than any default send buffer or receive window, so the first `write()` is
        // partial whatever the collector does.
        let events: Vec<Event> =
            (0..300_000).map(|i| gauge_event(&format!("m{i}"), i as f64)).collect();
        let batch = batch_with(events);

        let err = tokio::time::timeout(Duration::from_secs(10), output.send(&batch))
            .await
            .expect("send must not hang")
            .expect_err("a reset mid-write must surface as an error, not a silent success");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
    }

    /// A real `EMSGSIZE` from `send_to` is counted and skipped, not a `send` error. Reachable
    /// because rule 38's upper bound applies to `collectd_out` only, not `graphite_out`.
    #[tokio::test]
    async fn an_emsgsize_datagram_is_counted_not_faulted() {
        let (addr, _collector) = udp_collector().await;
        let registry = Registry::new();
        let mut output = GraphiteOutput::udp(addr.to_string())
            .unwrap()
            .with_max_packet_bytes(100_000) // past the real UDP payload ceiling (65507)
            .with_telemetry(registry.telemetry_for("out", "graphite_out", "sink"));
        let events: Vec<Event> = (0..3000)
            .map(|i| gauge_event(&format!("some.long.metric.path.{i}"), i as f64))
            .collect();
        let batch = batch_with(events);
        output.send(&batch).await.expect("an EMSGSIZE datagram must be dropped, not surfaced");

        let events = registry.drain(0);
        assert!(
            metric_sum(&events, "logit.output.messages.dropped") > 0.0,
            "expected the oversize datagram's datapoints to be counted dropped"
        );
    }

    #[tokio::test]
    async fn duplicate_safe_is_true() {
        let output = GraphiteOutput::udp("127.0.0.1:0").unwrap();
        assert!(output.duplicate_safe());
    }

    /// `with_encoder`/`with_max_packet_bytes` are order-independent: a 4-byte cap makes the
    /// encoder count the line as `oversize_line` in either order. A UDP send never errors, so only
    /// the codec's counter can show the cap was applied.
    #[tokio::test]
    async fn the_encoder_cap_is_order_independent_with_with_encoder() {
        let registry_cap_then_encoder = Registry::new();
        let cap_then_encoder = GraphiteOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_max_packet_bytes(4)
            .with_encoder(GraphiteEncoder::new())
            .with_telemetry(registry_cap_then_encoder.telemetry_for("out", "graphite_out", "sink"));
        let registry_encoder_then_cap = Registry::new();
        let encoder_then_cap = GraphiteOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_encoder(GraphiteEncoder::new())
            .with_max_packet_bytes(4)
            .with_telemetry(registry_encoder_then_cap.telemetry_for("out", "graphite_out", "sink"));
        for (mut output, registry) in [
            (cap_then_encoder, registry_cap_then_encoder),
            (encoder_then_cap, registry_encoder_then_cap),
        ] {
            let batch = batch_with(vec![gauge_event("a.long.enough.metric.name", 1.0)]);
            output.send(&batch).await.expect("an all-dropped batch must not attempt any I/O");
            assert!(
                counted(&registry, "logit.output.metrics.skipped", ("reason", "oversize_line")),
                "expected the encoder's own cap to have dropped the line as oversize in this \
                 ordering"
            );
        }
    }

    /// `with_encoder` must not drop the diagnostics/telemetry handles a caller already installed.
    #[tokio::test]
    async fn diagnostics_and_telemetry_survive_with_encoder_called_afterward() {
        let registry = Registry::new();
        let diag_registry = Registry::new();
        let diag = Diagnostics::new("out").with_telemetry(diag_registry.telemetry_for(
            "out/diag",
            "graphite_out",
            "sink",
        ));
        let mut output = GraphiteOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "graphite_out", "sink"))
            .with_diagnostics(diag)
            .with_encoder(
                GraphiteEncoder::new().with_multi_value(logit_proto::graphite::MultiValue::Skip),
            );
        // `multi_value: skip` counts a `Samples` record through both handles, if they survived.
        let batch = batch_with(vec![Event::metric(
            TS,
            AttrMap::new(),
            MetricRecord::new(intern("timer"), MetricKind::Samples(logit_core::Samples::default())),
        )]);
        output.send(&batch).await.expect("nothing encodable means no I/O");

        assert!(counted(&registry, "logit.output.metrics.skipped", ("metric_kind", "samples")));
        assert!(counted(
            &diag_registry,
            "logit.component.diagnostics",
            ("key", "unsupported_metric_kind")
        ));
    }

    // -- Telemetry --------------------------------------------------------------------------

    #[tokio::test]
    async fn a_successful_send_reports_batch_bytes_messages_datapoints_and_an_ok_request() {
        let (addr, collector) = udp_collector().await;
        let registry = Registry::new();
        let mut output = GraphiteOutput::udp(addr.to_string())
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "graphite_out", "sink"));
        let batch = batch_with(vec![gauge_event("app.requests", 42.0)]);

        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            collector.recv_from(&mut buf).await.unwrap();
        });
        output.send(&batch).await.expect("send should succeed");
        tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();

        let events = registry.drain(0);
        assert!(metric_sum(&events, "logit.output.batch.bytes") > 0.0);
        assert_eq!(metric_sum(&events, "logit.output.messages"), 1.0);
        assert_eq!(metric_sum(&events, "logit.output.datapoints"), 1.0);
        assert_eq!(metric_sum(&events, "logit.output.datagrams"), 1.0);
        assert_eq!(metric_sum(&events, "logit.output.requests"), 1.0);
    }

    /// A codec-counted drop is visible through the `Telemetry` this sink was built with.
    #[tokio::test]
    async fn a_skipped_kind_is_counted_by_the_codec_through_the_shared_telemetry_handle() {
        let registry = Registry::new();
        let mut output = GraphiteOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "graphite_out", "sink"));
        let batch = batch_with(vec![Event::metric(
            TS,
            AttrMap::new(),
            MetricRecord::new(intern("timer"), MetricKind::Samples(logit_core::Samples::default())),
        )]);
        output.send(&batch).await.expect("nothing encodable means no I/O");

        assert!(
            counted(&registry, "logit.output.metrics.skipped", ("metric_kind", "samples")),
            "expected the codec's own skipped-kind counter, fed through this sink's shared \
             Telemetry"
        );
    }
}
