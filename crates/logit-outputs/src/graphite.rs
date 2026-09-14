//! Carbon plaintext/pickle egress over UDP or TCP -- the mirror of `graphite_in`, and a real
//! relay: path, tags, value and timestamp round-trip through the real `GraphiteDecoder` on the
//! other end (this module's own tests). See
//! [ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md) and
//! [`docs/plans/graphite-carbon-relay.md`](../../../../docs/plans/graphite-carbon-relay.md).
//!
//! Split the way `statsd.rs`/`collectd.rs` are: the pure [`logit_proto::graphite::GraphiteEncoder`]
//! does every mapping, sanitization, and packing decision (that module's own doc is the spec for
//! all of it), and this module is only the thin transport wrapper: [`GraphiteOutput`] owns the
//! socket, hands the encoder a batch, and turns its already-packed
//! [`logit_proto::MessageBuf`]`<usize>` into UDP or TCP writes. **The codec emits every
//! metric/tag/name counter and diagnostic itself** (it holds its own `Telemetry`/`Diagnostics`,
//! fed by this sink's own builders below, collectd's model -- `crate::collectd`'s own module doc)
//! -- this module adds only the transport-level counters a socket send can produce that the codec
//! has no way to know about: total bytes, request timing, datagrams/messages actually written, and
//! an oversize-datagram drop.
//!
//! **This implements [`logit_proto::FramedEncoder`], not `logit_proto::Encoder`** -- the identical
//! reason `statsd_out`/`collectd_out`'s encoders do (`crate::statsd`'s module doc, ADR
//! `framed-encoder`). `GraphiteEncoder::encode_into` fills a [`logit_proto::MessageBuf`]`<usize>`
//! with message boundaries already decided (one plaintext line, or one complete
//! already-length-prefixed pickle frame -- encoder state, per the trait, rather than a per-call
//! argument), so this sink's only remaining job is turning each entry into bytes on the wire.
//!
//! Unlike `collectd_out` (UDP only) this sink supports **both transports**, carbon's own plaintext
//! listener speaks either -- the TCP half is a near-verbatim port of
//! `crates/logit-outputs/src/statsd.rs:1638-2045`'s `Conn`/lazy-connect/reconnect-once/
//! partial-write machinery, copied and cited at each borrowed shape below rather than
//! reinvented, since no shared transport module exists yet (`docs/plans/graphite-carbon-relay.md`
//! decision 16 -- there is nothing to extract a driver from until a second TCP sink needs the same
//! shape).
//!
//! ## Config
//!
//! `endpoint` (`host:port`, resolved once per batch, never at config-load time -- same
//! `statsd_out`/`collectd_out` precedent), `transport` (`tcp`, the default, or `udp`), `protocol`
//! (`plaintext`, the default, or `pickle` -- TCP only, rejected on UDP by
//! `crates/logit-pipeline/src/graph.rs` rule 46), `tags`/`multi_value` (forwarded straight to the
//! encoder, no sink-level meaning), `max_packet_bytes` (default `1432`, `statsd_out`'s own figure
//! -- UDP only, ignored on TCP, which has no datagram to overflow), `max_frame_bytes` (default
//! `1MiB`, Twisted's own `Int32StringReceiver.MAX_LENGTH`), and `connect_timeout` (TCP only,
//! default `5s`, `statsd_out`/`syslog_out`'s own default).
//!
//! ## Packing
//!
//! Plaintext UDP: the encoder emits one [`logit_proto::MessageBuf`] entry per line (meta always
//! `1`), and this sink packs them into as few datagrams as fit under `max_packet_bytes`
//! (newline-joined, **no trailing newline**) -- `StatsdOutput::send_udp`/`flush_datagram`'s exact
//! shape (`crates/logit-outputs/src/statsd.rs:1880-1958`), copied here because a single line
//! already longer than `max_packet_bytes` was dropped by the encoder itself (its own line cap,
//! `with_max_packet_bytes`), so every line this sink ever sees already fits in its own datagram
//! at minimum. Plaintext TCP: every line is written `\n`-terminated, **including the last** --
//! `send_tcp` builds one buffer for the whole batch and issues it as one write (partial-then-
//! `write_all`), `StatsdOutput::send_tcp`'s own shape. Pickle (TCP only): the encoder already
//! produced complete, self-delimited, length-prefixed frames, so no join separator is needed at
//! all; this sink concatenates them into the same per-batch write buffer with no separator between
//! them -- TCP is an ordered byte stream, so writing N already-framed messages back to back in one
//! `write`/`write_all` sequence is indistinguishable on the wire from N separate `write_all` calls,
//! and Twisted's `Int32StringReceiver` on the other end parses each frame off its own declared
//! length regardless of how the bytes arrived in individual `recv`s. This buys the same
//! "one write (or one partial-write-then-retry) per batch" property `send_tcp` already gives
//! plaintext, rather than issuing a separate `write_all` per frame.
//!
//! ## Faults
//!
//! `endpoint` is resolved once per batch (a non-numeric host must not be re-resolved once per
//! datagram/write -- `statsd::send_udp`'s doc comment). UDP: `EMSGSIZE` (raw OS error 90, or
//! `ErrorKind::InvalidInput` on a platform where the shim never reaches the syscall) on one
//! datagram counts that datagram's datapoints under
//! `logit.output.messages.dropped{reason="oversize_datagram"}` plus a throttled diagnostic, and
//! sending continues with the next datagram; any other send error is [`Fault::Clean`] if no
//! datagram in this batch has been sent yet, else [`Fault::Ambiguous`]. TCP: a fresh/lazy connect
//! failure or timeout is [`Fault::Clean`]; a write failing before any byte of the batch has left
//! this host is retried with exactly **one** reconnect (`StatsdOutput::send_tcp`'s own invariant);
//! a write failing after at least one byte has already gone out is [`Fault::Ambiguous`] and is
//! **never** retried -- resending would duplicate whatever the peer already has (this is also why
//! [`GraphiteOutput::duplicate_safe`] is `true`: see "Duplicate safety" below, a property about the
//! *destination*, not about whether this sink itself ever double-sends).
//!
//! ## Telemetry (transport-level; the codec's own counters are documented on it, not here)
//!
//! `logit.output.batch.bytes` (total bytes across every message in the batch, emitted only when
//! there is something to send), `logit.output.request.duration` (one timer per `send` call that
//! actually touches the socket), `logit.output.requests{class="ok"|"error"}`,
//! `logit.output.messages` (entries -- lines or pickle frames -- actually sent),
//! `logit.output.datapoints` (Σ each sent entry's `usize` meta -- datapoints actually sent, which
//! for plaintext equals `messages` since every line's meta is `1`, and can differ for pickle, whose
//! frames carry several datapoints each), `logit.output.datagrams` (UDP only, datagrams actually
//! sent), and `logit.output.messages.dropped{reason="oversize_datagram"}` plus a throttled
//! `oversize_datagram` diagnostic for the `EMSGSIZE` case above.
//!
//! ## Duplicate safety
//!
//! [`GraphiteOutput::duplicate_safe`] is `true`: whisper (carbon's own storage backend) is
//! last-write-wins **per `(path, second)`** -- a redelivered datapoint for a second whisper already
//! holds a value for simply overwrites it with the same number, rather than accumulating like a
//! collectd COUNTER or a statsd `|c` would (`influxdb.rs:191-199`'s identical argument for
//! InfluxDB's own idempotent-overwrite semantics). This is the first non-HTTP sink with a real
//! destination to claim `true` (`null_out` claims it too, but trivially -- it has no destination
//! to redeliver to). **The boundary**: this is whisper's behavior specifically, not a property of the
//! carbon wire protocol itself -- a non-whisper Graphite-protocol receiver (a different storage
//! engine listening on the same wire) could treat a redelivered datapoint as an addition instead,
//! and this sink would have no way to tell. State this plainly rather than silently assuming every
//! receiver is whisper.

use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{Fault, Output};
use logit_proto::graphite::{GraphiteEncoder, Protocol};
use logit_proto::{FramedEncoder, MessageBuf};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{lookup_host, TcpStream, UdpSocket};

/// Which transport a `graphite_out` component was configured with -- the target of
/// [`crate`]'s CLI-side `graphite_out_transport` converter
/// (`crates/logit-cli/src/pipeline.rs`), kept as its own small public enum (rather than matching
/// `logit_config::GraphiteTransport` directly there) so that conversion has a named, stable
/// signature. Namespaced `graphite_out_*` on the CLI side specifically because `graphite_in`
/// converts the same `logit_config::GraphiteTransport` onto its own, different type -- a bare
/// `graphite_transport` name would collide. Not used internally beyond selecting
/// [`GraphiteOutput::udp`]/[`GraphiteOutput::tcp`]; [`Conn`] is this module's own internal choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Udp,
    Tcp,
}

/// `crates/logit-outputs/src/statsd.rs:1638-2045`'s `Conn` -- copied verbatim in shape. `Tcp`'s
/// `stream` starts `None`: connecting eagerly at construction would turn "the destination isn't up
/// yet" into a startup failure instead of the retryable `send`-time one every other sink gives it
/// (`StatsdOutput::tcp`'s own doc comment).
enum Conn {
    Udp(UdpSocket),
    Tcp { stream: Option<TcpStream>, connect_timeout: Duration },
}

/// `logit_pipeline::Output` for `graphite_out`. Built via [`GraphiteOutput::udp`] or
/// [`GraphiteOutput::tcp`] -- never a bare constructor, mirroring `StatsdOutput`.
pub struct GraphiteOutput {
    endpoint: String,
    conn: Conn,
    encoder: GraphiteEncoder,
    /// Kept on the sink, not just the encoder, so [`GraphiteOutput::with_encoder`] can re-apply it
    /// to a replacement encoder regardless of builder order -- `CollectdOutput::with_encoder`'s own
    /// doc comment explains why. UDP datagram cap only; see [`GraphiteOutput::encoder_cap`].
    max_packet_bytes: usize,
    /// Reused across `send` calls: the codec's own packing buffer, one entry per line (plaintext)
    /// or per already-prefixed frame (pickle), whose `usize` meta is that entry's datapoint count.
    buf: MessageBuf<usize>,
    /// Reused across `send` calls: the packed UDP datagram, or the whole TCP write buffer.
    packet_buf: Vec<u8>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl GraphiteOutput {
    /// Binds an ephemeral local UDP socket eagerly -- see `StatsdOutput::udp`'s doc comment for
    /// why `endpoint` itself is resolved per `send`, not here.
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

    /// The line cap the encoder enforces for this transport: the configured datagram size on UDP,
    /// none on TCP (no datagram to overflow -- `StatsdOutput::encoder_cap`'s identical reasoning
    /// and shape).
    fn encoder_cap(&self) -> usize {
        if matches!(self.conn, Conn::Udp(_)) {
            self.max_packet_bytes
        } else {
            usize::MAX
        }
    }

    /// Installs `encoder`, with this sink's transport-appropriate line cap, diagnostics, and
    /// telemetry re-applied on top of it -- `CollectdOutput::with_encoder`'s own doc comment gives
    /// the full argument for why a plain `self.encoder = encoder` would be order-dependent and
    /// would silently drop both handles.
    pub fn with_encoder(mut self, encoder: GraphiteEncoder) -> Self {
        self.encoder = encoder
            .with_max_packet_bytes(self.encoder_cap())
            .with_diagnostics(self.diag.clone())
            .with_telemetry(self.telemetry.clone());
        self
    }

    /// Bounds one UDP **datagram** (several packed lines); ignored by the encoder on TCP (see
    /// [`GraphiteOutput::encoder_cap`]).
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
        // Discarded: every counter/diagnostic this produces, the encoder has already emitted
        // itself through the `Telemetry`/`Diagnostics` handles `with_telemetry`/`with_diagnostics`
        // fed it -- this module's doc, "Telemetry".
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

    /// Implemented explicitly for the same reason `statsd_out`/`syslog_out` do: `send` performs
    /// one write per batch and retains nothing between calls, so there's nothing buffered here at
    /// shutdown -- for TCP, this simply flushes the underlying stream.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { stream: Some(stream), .. } = &mut self.conn {
            stream.flush().await.context("flushing graphite_out TCP stream")?;
        }
        Ok(())
    }

    /// `true`: whisper is last-write-wins per `(path, second)` -- see this module's doc comment,
    /// "Duplicate safety", for the full argument and its stated boundary.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// Running totals for one [`GraphiteOutput::send_udp`] call -- `StatsdOutput`'s own
/// `UdpSendCounts`, with a `datapoints` column added: a dropped `oversize_datagram` datagram
/// attributes its **datapoint** count (Σ `meta`), not just its entry count, to
/// `logit.output.messages.dropped` -- the two coincide for plaintext (every line's meta is `1`)
/// but this is written generically so the same counting logic would still be correct if this sink
/// ever packed pickle frames into a UDP datagram (it never legally does -- graph rule 46 -- but the
/// counting code itself makes no such assumption).
#[derive(Default)]
struct UdpSendCounts {
    /// [`MessageBuf`] entries actually written to the socket -- `logit.output.messages`.
    messages: usize,
    /// Σ of each written entry's `meta` -- `logit.output.datapoints`.
    datapoints: usize,
    /// Datagrams actually written to the socket -- `logit.output.datagrams`.
    datagrams: usize,
    /// Entries appended to `packet_buf` since the last flush; reset by every flush.
    entries_in_packet: usize,
    /// Σ `meta` appended to `packet_buf` since the last flush; reset by every flush.
    datapoints_in_packet: usize,
}

impl GraphiteOutput {
    /// Packs `buf`'s entries into as few UDP datagrams as fit under `max_packet_bytes`
    /// (newline-joined, no trailing newline), then sends one `send_to` per datagram --
    /// `StatsdOutput::send_udp`'s exact shape (`crates/logit-outputs/src/statsd.rs:1880-1918`).
    /// Returns `(messages sent, datapoints sent, datagrams sent)`.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        buf: &MessageBuf<usize>,
        max_packet_bytes: usize,
        packet_buf: &mut Vec<u8>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize, usize)> {
        // Resolved once per batch, not once per datagram -- see `statsd::send_udp`'s doc comment
        // for why a non-numeric host must not be re-resolved on every call.
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

    /// Sends one packed datagram, clearing `packet_buf` and the per-packet counters after --
    /// `StatsdOutput::flush_datagram`'s exact shape.
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

    /// One write (partial-then-`write_all`) per **batch**, with at most one internal
    /// reconnect-and-retry -- `StatsdOutput::send_tcp`'s exact control flow
    /// (`crates/logit-outputs/src/statsd.rs:1983-2038`), including both correctness properties
    /// documented there: cancellation safety via `stream.take()`, and never resending once a byte
    /// has left this host. The only difference from `StatsdOutput::send_tcp` is what goes into the
    /// write buffer: a plaintext batch newline-terminates every line (including the last);
    /// a pickle batch concatenates its already-length-prefixed frames with **no** separator --
    /// see this module's doc comment, "Packing", for why one write of the concatenation is
    /// equivalent to one `write_all` per frame here. Returns `(messages sent, datapoints sent, 0)`
    /// -- there's no datagram count on TCP.
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
                Some(conn) => conn,
                None => tokio::time::timeout(connect_timeout, TcpStream::connect(endpoint))
                    .await
                    .context("connecting to graphite_out endpoint timed out")
                    .and_then(|r| r.context("connecting to graphite_out endpoint"))
                    .context(Fault::Clean)?,
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
                        // Never resent: at least one byte of this batch already left this host,
                        // so retrying (even against a fresh connection) risks the peer applying
                        // it twice.
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

/// `90` is `EMSGSIZE` on Linux specifically -- see `statsd::is_message_too_large`'s doc comment
/// (copied rather than shared: it isn't `pub`); this repo only ever ships/runs inside the Linux
/// containers it builds.
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

    /// A single-gauge, single-tag event -- decode-shaped, deliberately, so whole-`EventBatch`
    /// equality against a real decode is a meaningful assertion (`collectd.rs`'s `relay_event`
    /// doc comment makes the identical argument).
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

    /// The summed value of every point named `metric` in an already-drained `events`, regardless
    /// of tags -- `collectd.rs`'s `metric_sum` (one shared drain, since `drain` empties the
    /// registry).
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

    /// Whole-`EventBatch` equality against the input, not a spot check of a couple of fields --
    /// `tagged_event`'s decode shape makes this a real fixed-point assertion.
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

    /// A boundary bug that let one datagram's content bleed into the next could still decode to
    /// the right events while violating the cap it was supposed to honor -- so this checks both.
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

    /// Strips the 4-byte length prefix and decodes the payload with the real decoder in pickle
    /// mode -- proving what this sink writes is exactly what a real Twisted `Int32StringReceiver`
    /// consumer would also accept.
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

    /// "Exactly one reconnect before any byte is written": the peer resets an inherited
    /// connection, so the very first write against it fails with zero bytes sent -- safe to
    /// reconnect and retry once, `StatsdOutput`'s own precedent test.
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

    /// A write that fails only *after* at least one byte of the batch already left this host must
    /// be `Fault::Ambiguous`, never retried. Forced deterministically: the collector accepts the
    /// connection, waits briefly (long enough for this sink's first, necessarily-partial `write`
    /// of a several-megabyte batch to land locally), then resets the connection -- the follow-up
    /// `write_all` for the remainder then fails against a connection that already has bytes in
    /// flight, exactly the "some bytes may have already landed" case `Fault::Ambiguous` exists for.
    // `set_linger` blocks the thread on drop -- accepted here, a test-only, one-shot loopback
    // close, for the deterministic RST this test needs.
    #[allow(deprecated)]
    #[tokio::test]
    async fn a_write_failing_after_bytes_already_left_this_host_is_an_ambiguous_fault() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                // Long enough for the sink's first `write()` call (which only needs local buffer
                // space, not a peer read) to have already returned a partial byte count.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = stream.set_linger(Some(Duration::ZERO));
                drop(stream);
            }
        });

        let mut output = GraphiteOutput::tcp(addr.to_string(), Duration::from_secs(2));
        // Several megabytes of short lines -- comfortably larger than any default socket send
        // buffer or advertised receive window, so the very first `write()` call is guaranteed to
        // return fewer bytes than the whole frame regardless of what the collector does.
        let events: Vec<Event> =
            (0..300_000).map(|i| gauge_event(&format!("m{i}"), i as f64)).collect();
        let batch = batch_with(events);

        let err = tokio::time::timeout(Duration::from_secs(10), output.send(&batch))
            .await
            .expect("send must not hang")
            .expect_err("a reset mid-write must surface as an error, not a silent success");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
    }

    /// A UDP datagram genuinely too large for the kernel to send (`EMSGSIZE`) is counted and
    /// skipped, not surfaced as a `send` error -- real `send_to` against a real socket, not a
    /// simulated error, since `max_packet_bytes` has no graph-rule upper bound for `graphite_out`
    /// (unlike `collectd_out`'s rule 38 range clamp).
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

    /// `with_encoder`/`with_max_packet_bytes` must be order-independent --
    /// `CollectdOutput`'s own precedent test. A cap of 4 bytes is smaller than any real line, so
    /// in either order the event below must be dropped whole as oversize by the *encoder* (not
    /// merely fail to error, which a send to an unconnected UDP socket never does regardless of
    /// whether the cap was actually applied) -- asserted via the codec's own
    /// `logit.output.metrics.skipped{reason="oversize_line"}` counter, fed through a
    /// `Registry`-backed `Telemetry` installed on each ordering, so this test is load-bearing:
    /// dropping `with_encoder`'s own `.with_max_packet_bytes(self.encoder_cap())`
    /// re-application would leave the cap at `GraphiteEncoder::new()`'s uncapped `usize::MAX` in
    /// the `with_encoder`-called-last ordering, the line would encode instead of being dropped,
    /// and this assertion would fail.
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
        // A `Samples` record is skipped by default under `multi_value: skip`, counted through
        // both handles only if `with_encoder` kept feeding them into the codec.
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

    /// A drop the codec counts (an unsupported multi-value kind) is visible through the same
    /// shared `Telemetry` handle this sink was built with.
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
