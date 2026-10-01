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
//! TCP sends through the pooled-stream driver in `crate::stream`, shared with `statsd_out` and
//! `syslog_out`: lazy connect, the probe of a reused connection, one write then `write_all`, a
//! flush, and one reconnect after a plaintext write that accepted nothing. There's no `tls:`
//! option.
//!
//! ## Config
//!
//! - `endpoint`: `host:port`, resolved once per batch, never at config load; on UDP, sent to the
//!   first IPv4 address it resolves to, else the first IPv6 one.
//! - `transport`: `tcp` (default) or `udp`.
//! - `protocol`: `plaintext` (default) or `pickle`, TCP only (graph rule 46, and
//!   [`GraphiteOutput::with_encoder`]).
//! - `tags`/`multi_value`: forwarded to the encoder; no sink-level meaning.
//! - `max_packet_bytes`: default `1432`, UDP only, at most 65507 (graph rule 38); TCP has no
//!   datagram to overflow.
//! - `max_frame_bytes`: default 1 MiB, Twisted's `Int32StringReceiver.MAX_LENGTH`.
//! - `connect_timeout`: TCP only, default `5s`.
//!
//! ## Packing
//!
//! - **Plaintext UDP**: lines are packed newline-joined, **no trailing newline**, into as few
//!   datagrams as fit under `max_packet_bytes`, by the packer `statsd_out` shares
//!   (`crate::datagram`). The encoder already dropped any single line over the cap, so every line
//!   fits a datagram on its own.
//! - **Plaintext TCP**: every line `\n`-terminated, **including the last**.
//! - **Pickle** (TCP only): frames concatenated with no separator; the receiver parses each off
//!   its own length prefix.
//!
//! On TCP, the whole batch is one frame handed to the driver.
//!
//! ## Faults
//!
//! - UDP: `crate::datagram`'s module doc lists the rules. A datagram the kernel refuses with
//!   `EMSGSIZE` drops its datapoints under
//!   `logit.output.messages.dropped{reason="oversize_datagram"}`, and sending continues. Any other
//!   send error is [`logit_pipeline::Fault::Clean`] if no datagram of the batch was sent yet, else
//!   [`logit_pipeline::Fault::Ambiguous`].
//! - TCP: `crate::stream`'s module doc lists the fault rules. A connect failure or timeout is
//!   `Clean`. A first write that accepted nothing is retried once on a fresh connection. A
//!   failure after a byte left, or of the flush, is `Ambiguous` and never retried by this sink.
//!
//! ## Telemetry
//!
//! Transport-level only; the codec documents its own. `logit.output.batch.bytes` (only when there
//! is something to send), `logit.output.request.duration`,
//! `logit.output.requests{class="ok"|"clean"|"ambiguous"|"permanent"}`, `logit.output.messages`
//! (entries sent), `logit.output.datapoints` (Σ sent entries' meta; equals `messages` for
//! plaintext), `logit.output.datagrams` (UDP only), `logit.output.reconnects` (TCP, every connect
//! after the first), and the `oversize_datagram` drop above. On UDP the sent counts include what
//! reached the kernel before a failure; on TCP they count only a delivered frame.
//!
//! ## Delivery posture
//!
//! The default, `at_least_once` (`docs/adr/delivery-semantics.md`, item 5), retries an
//! `Ambiguous` attempt, and whisper is last-write-wins per `(path, second)`: a resent datapoint
//! overwrites the same number rather than accumulating like a collectd `ABSOLUTE` or a statsd
//! `|c`. That's whisper's behavior, not the carbon wire's; a non-whisper receiver on the same wire
//! could add instead, and this sink can't tell. `buffer.delivery: at_most_once` drops the batch
//! instead.

use crate::accounting::BatchAccounting;
use crate::count_request;
use crate::datagram::{Datagrams, Framing, Report, UdpDest};
use crate::stream::{Dial, PooledStream, Target};
use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{BatchContext, Output, SeqId};
use logit_proto::graphite::{GraphiteEncoder, Protocol};
use logit_proto::{FramedEncoder, MessageBuf};
use std::time::Duration;

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
    Udp(UdpDest),
    Tcp { pool: PooledStream, connect_timeout: Duration },
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
    /// Ungated: the transport's counts, the `oversize_datagram` drops, and the sink's own
    /// warnings. The encoder holds views gated by `accounting` (`crate::accounting`).
    diag: Diagnostics,
    telemetry: Telemetry,
    accounting: BatchAccounting,
    /// Replaces the stream transports' dial target with scripted connections.
    #[cfg(test)]
    dial_script: Option<std::sync::Arc<crate::test_support::ScriptedDial>>,
}

impl GraphiteOutput {
    /// Binds an ephemeral local IPv4 UDP socket eagerly; `endpoint` is resolved per `send`
    /// (`crate::datagram`'s module doc).
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self::new(endpoint, Conn::Udp(UdpDest::bind("graphite_out")?)))
    }

    /// Never connects here -- see [`Conn`]'s doc comment.
    pub fn tcp(endpoint: impl Into<String>, connect_timeout: Duration) -> Self {
        Self::new(endpoint, Conn::Tcp { pool: PooledStream::default(), connect_timeout })
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
            accounting: BatchAccounting::default(),
            #[cfg(test)]
            dial_script: None,
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

    /// Installs `encoder` with this sink's line cap and its diagnostics and telemetry (gated)
    /// re-applied, so builder order doesn't matter (`CollectdOutput::with_encoder` says what goes
    /// wrong otherwise).
    ///
    /// Errors on a pickle encoder over UDP, as graph validation does: a pickle frame is bounded
    /// by `max_frame_bytes`, not by the datagram cap, and its length prefix means nothing in a
    /// datagram.
    pub fn with_encoder(mut self, encoder: GraphiteEncoder) -> anyhow::Result<Self> {
        if matches!(self.conn, Conn::Udp(_)) && encoder.protocol() == Protocol::Pickle {
            anyhow::bail!("graphite_out: protocol: pickle requires transport: tcp");
        }
        self.encoder = encoder
            .with_max_packet_bytes(self.encoder_cap())
            .with_diagnostics(self.diag.gated(self.accounting.gate()))
            .with_telemetry(self.telemetry.gated(self.accounting.gate()));
        Ok(self)
    }

    /// Bounds one UDP datagram of packed lines; no effect on TCP.
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        let cap = self.encoder_cap();
        self.encoder = self.encoder.with_max_packet_bytes(cap);
        self
    }

    /// The encoder gets a view gated by this sink's batch accounting.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag.gated(self.accounting.gate()));
        self.diag = diag;
        self
    }

    /// The encoder gets a view gated by this sink's batch accounting.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.encoder = self.encoder.with_telemetry(telemetry.gated(self.accounting.gate()));
        self.telemetry = telemetry;
        self
    }
}

impl GraphiteOutput {
    /// One `Output::send` attempt.
    async fn attempt(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // `Stats` discarded: the encoder already reported them through its own gated handles.
        let (first, _stats) =
            self.accounting.encode(0, || self.encoder.encode_into(batch, &mut self.buf));

        if self.buf.is_empty() {
            return Ok(());
        }

        if first {
            self.telemetry.count("logit.output.batch.bytes", self.buf.total_bytes() as f64, &[]);
        }
        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = match &mut self.conn {
            Conn::Udp(udp) => {
                let batch = Datagrams {
                    entries: &self.buf,
                    weight: |datapoints| *datapoints,
                    cap: self.max_packet_bytes,
                    framing: Framing::Packed,
                };
                let mut report = Report {
                    sink: "graphite_out",
                    diag: &mut self.diag,
                    telemetry: &self.telemetry,
                };
                let (sent, result) =
                    udp.send(&self.endpoint, batch, &mut self.packet_buf, &mut report).await;
                // What reached the kernel, even when the batch then failed.
                self.telemetry.count("logit.output.messages", sent.entries as f64, &[]);
                self.telemetry.count("logit.output.datapoints", sent.weight as f64, &[]);
                self.telemetry.count("logit.output.datagrams", sent.datagrams as f64, &[]);
                count_request(&self.telemetry, &result);
                result
            }
            // The driver counts `requests` for this arm.
            Conn::Tcp { pool, connect_timeout } => {
                let datapoints =
                    build_tcp_frame(&self.buf, self.encoder.protocol(), &mut self.packet_buf);
                let target = Target::Tcp { endpoint: &self.endpoint, tls: None };
                #[cfg(test)]
                let target = crate::stream::scripted_or(target, &self.dial_script);
                let dial = Dial {
                    target,
                    connect_timeout: *connect_timeout,
                    sink: "graphite_out",
                    nodelay: false,
                };
                let result = pool.send(&dial, &self.packet_buf, &self.telemetry).await;
                if result.is_ok() {
                    self.telemetry.count("logit.output.messages", self.buf.len() as f64, &[]);
                    self.telemetry.count("logit.output.datapoints", datapoints as f64, &[]);
                }
                result
            }
        };
        drop(request_timer);
        result
    }
}

#[async_trait::async_trait]
impl Output for GraphiteOutput {
    /// Arms this sink's batch accounting (`crate::accounting`).
    fn observe_batch(&mut self, _ctx: BatchContext, _seq: Option<SeqId>) {
        self.accounting.observe();
    }

    /// One attempt ([`GraphiteOutput::attempt`]). An `Ok` disarms the batch accounting on every
    /// path, a batch that encoded to nothing included.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let result = self.attempt(batch).await;
        self.accounting.finish(result)
    }

    /// `send` pools a connection only after flushing it; this is the shutdown backstop.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Conn::Tcp { pool, .. } = &mut self.conn {
            pool.flush().await.context("flushing graphite_out TCP stream")?;
        }
        Ok(())
    }
}

/// Builds the TCP frame for `buf` into `frame` (module doc's "Packing") and returns its datapoint
/// count, Σ `meta`.
fn build_tcp_frame(buf: &MessageBuf<usize>, protocol: Protocol, frame: &mut Vec<u8>) -> usize {
    frame.clear();
    let mut datapoints = 0usize;
    for (msg, meta) in buf.iter_with() {
        frame.extend_from_slice(msg);
        if protocol == Protocol::Plaintext {
            frame.push(b'\n');
        }
        datapoints += *meta;
    }
    datapoints
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        assert_counted_once_per_batch, assert_direct_sends_count_after_an_empty_batch, fast_retry,
        sum_of, sums_through_write_loop, Collector, DialStep, FakeStream, ReadMode, ScriptedDest,
        ScriptedDial, SendStep,
    };
    use logit_core::interner::intern;
    use logit_core::{
        AttrMap, BodyFormat, Event, LogRecord, MetricKind, MetricRecord, Registry, Resource, Value,
    };
    use logit_pipeline::test_util::TelemetryProbe;
    use logit_pipeline::Fault;
    use logit_proto::graphite::GraphiteDecoder;
    use logit_proto::Decoder;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

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

    /// Whole-`EventBatch` equality against the input: a fixed-point assertion.
    #[tokio::test]
    async fn a_line_round_trips_through_a_real_collector_and_the_real_decoder() {
        let mut collector = Collector::udp().await;
        let mut output = GraphiteOutput::udp(collector.addr().to_string()).unwrap();
        let batch = batch_with(vec![tagged_event("app.requests", 42.0, ("env", "prod"))]);
        output.send(&batch).await.expect("send should succeed");
        let received = collector.next().await;

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
        let mut collector = Collector::tcp(ReadMode::ToEof).await;
        let mut output = GraphiteOutput::tcp(collector.addr().to_string(), Duration::from_secs(2));
        let batch = batch_with(vec![gauge_event("a.metric", 1.0), gauge_event("b.metric", 2.0)]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        let got = collector.next().await;
        let text = String::from_utf8_lossy(&got);
        assert!(text.ends_with('\n'), "the last line must be newline-terminated too: {text:?}");
        assert_eq!(text.lines().count(), 2);
    }

    #[tokio::test]
    async fn udp_datagrams_carry_no_trailing_newline() {
        let mut collector = Collector::udp().await;
        let mut output = GraphiteOutput::udp(collector.addr().to_string()).unwrap();
        let batch = batch_with(vec![gauge_event("a.metric", 1.0), gauge_event("b.metric", 2.0)]);
        output.send(&batch).await.expect("send should succeed");
        let received = collector.next().await;
        assert!(!received.ends_with(b"\n"), "a UDP datagram must not end with a trailing newline");
    }

    /// Checks both the decoded events and each datagram's size, since a packing bug can bleed
    /// across datagrams and still decode correctly. Reads datagrams until every event has
    /// decoded, rather than draining on a quiet window: the packing itself decides the count.
    #[tokio::test]
    async fn a_low_cap_packs_several_events_into_several_datagrams_none_over_cap() {
        let mut collector = Collector::udp().await;
        const CAP: usize = 32;
        let mut output =
            GraphiteOutput::udp(collector.addr().to_string()).unwrap().with_max_packet_bytes(CAP);
        let events: Vec<Event> = (0..10).map(|i| gauge_event(&format!("m{i}"), i as f64)).collect();
        let batch = batch_with(events);
        output.send(&batch).await.expect("send should succeed");

        let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default()));
        let mut datagrams = Vec::new();
        let mut decoded = Vec::new();
        while decoded.len() < batch.events.len() {
            let datagram = collector.next().await;
            assert!(
                datagram.len() <= CAP,
                "a datagram of {} bytes exceeded the cap",
                datagram.len()
            );
            decoder
                .decode_into(bytes::Bytes::from(datagram.clone()), TS, &mut decoded)
                .expect("every datagram must decode");
            datagrams.push(datagram);
        }
        assert!(datagrams.len() > 1, "10 lines cannot fit one 32-byte datagram");
        assert_eq!(decoded, batch.events, "decode(send(b)) must equal b, not just a count");
    }

    /// Strips the 4-byte length prefix and decodes the payload with the real pickle decoder.
    #[tokio::test]
    async fn a_pickle_send_writes_one_length_prefixed_frame_a_real_reader_accepts() {
        let mut collector = Collector::tcp(ReadMode::ToEof).await;
        let mut output = GraphiteOutput::tcp(collector.addr().to_string(), Duration::from_secs(2))
            .with_encoder(GraphiteEncoder::new().with_protocol(Protocol::Pickle))
            .unwrap();
        let batch = batch_with(vec![tagged_event("app.requests", 42.0, ("env", "prod"))]);
        output.send(&batch).await.expect("send should succeed");
        drop(output);
        let frame = collector.next().await;

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

    /// The kernel refuses a UDP send to port 0 with `EINVAL`: a clean error, not every datapoint
    /// counted `oversize_datagram` under an `ok` request.
    #[tokio::test]
    async fn a_udp_endpoint_with_port_zero_fails_clean_and_counts_no_oversize() {
        let mut probe = TelemetryProbe::new();
        let mut output = GraphiteOutput::udp("127.0.0.1:0")
            .unwrap()
            .with_telemetry(probe.telemetry("out", "graphite_out", "sink"));
        let batch = batch_with(vec![gauge_event("hits", 1.0)]);
        let err = output.send(&batch).await.expect_err("the kernel refuses port 0");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        assert_eq!(crate::test_support::errno_in(&err), Some(22), "EINVAL on Linux: {err:#}");
        let oversize = [("reason", "oversize_datagram")];
        assert_eq!(probe.sum("logit.output.messages.dropped", &oversize), 0.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "clean")]), 1.0);
    }

    /// An IPv6 endpoint goes out over an IPv6 socket.
    #[tokio::test]
    async fn an_ipv6_udp_endpoint_is_delivered() {
        let Ok(mut collector) = Collector::udp_at("[::1]:0").await else {
            println!("skipping: this environment has no usable IPv6 loopback");
            return;
        };
        let mut output = GraphiteOutput::udp(collector.addr().to_string()).unwrap();
        let batch = batch_with(vec![gauge_event("a.metric", 1.0)]);
        output.send(&batch).await.expect("an IPv6 endpoint must be reachable");
        assert!(String::from_utf8_lossy(&collector.next().await).starts_with("a.metric 1 "));
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
        let mut collector = Collector::tcp(ReadMode::ToEof).await;
        let mut output = GraphiteOutput::tcp(collector.addr().to_string(), Duration::from_secs(2));

        let batch1 = batch_with(vec![gauge_event("first", 1.0)]);
        output.send(&batch1).await.expect("first send should succeed against a fresh connection");

        let Conn::Tcp { pool, .. } = &mut output.conn else { unreachable!() };
        let pooled = pool.stream_mut().expect("a successful send pools its connection");
        pooled.shutdown().await.expect("local shutdown should succeed");

        let batch2 = batch_with(vec![gauge_event("second", 1.0)]);
        output
            .send(&batch2)
            .await
            .expect("second send should reconnect once and succeed, not surface the failure");

        drop(output);
        let got = collector.take(2).await;
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("first")));
        assert!(got.iter().any(|b| String::from_utf8_lossy(b).contains("second")));
    }

    /// After the receiver closes the pooled connection, the next batch still arrives. Asserts on
    /// the collector's second accept and the line, since a write into a FIN'd socket returns `Ok`.
    #[tokio::test]
    async fn a_pooled_connection_the_peer_closed_is_reconnected_before_writing_and_the_message_is_not_lost(
    ) {
        let mut collector = Collector::tcp(ReadMode::FirstReadThenClose).await;
        let mut output = GraphiteOutput::tcp(collector.addr().to_string(), Duration::from_secs(2));

        output
            .send(&batch_with(vec![gauge_event("first", 1.0)]))
            .await
            .expect("first send should succeed against a fresh connection");

        // The collector closes before it reports, so its FIN is sent before the probe looks.
        let first = collector.next().await;
        assert!(String::from_utf8_lossy(&first).contains("first"));

        output
            .send(&batch_with(vec![gauge_event("second", 1.0)]))
            .await
            .expect("the probe should reconnect rather than write into a closed socket");

        drop(output);
        let second = collector.next().await;
        assert_eq!(
            collector.accepts(),
            2,
            "the probe must have dialled a second connection for the second batch"
        );
        assert!(
            String::from_utf8_lossy(&second).contains("second"),
            "the second batch must actually have reached the receiver: {second:?}"
        );
    }

    /// The probe-driven redial is an ordinary reconnect, counted like `statsd_out`'s and
    /// `syslog_out`'s; the first connect isn't.
    #[tokio::test]
    async fn tcp_counts_every_connect_after_the_first_as_a_reconnect() {
        let mut collector = Collector::tcp(ReadMode::FirstReadThenClose).await;
        let mut probe = TelemetryProbe::new();
        let mut output = GraphiteOutput::tcp(collector.addr().to_string(), Duration::from_secs(2))
            .with_telemetry(probe.telemetry("out", "graphite_out", "sink"));

        output.send(&batch_with(vec![gauge_event("first", 1.0)])).await.expect("first send");
        assert_eq!(probe.sum("logit.output.reconnects", &[]), 0.0, "the first connect isn't one");
        // The collector closes before it reports, so its FIN is sent before the probe looks.
        collector.next().await;

        output.send(&batch_with(vec![gauge_event("second", 1.0)])).await.expect("second send");
        drop(output);
        collector.next().await;
        assert_eq!(collector.accepts(), 2);
        assert_eq!(probe.sum("logit.output.reconnects", &[]), 1.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ok")]), 2.0);
    }

    /// A TCP batch is reported delivered only after the driver flushed it, and the flushed
    /// connection is kept for the next batch.
    #[tokio::test]
    async fn a_tcp_batch_is_flushed_before_it_is_reported_delivered() {
        let fake = FakeStream::new();
        let mut output = GraphiteOutput::tcp("127.0.0.1:1", Duration::from_millis(500));
        output.conn = Conn::Tcp {
            pool: PooledStream::pooled(Box::new(fake.clone())),
            connect_timeout: Duration::from_millis(500),
        };

        output.send(&batch_with(vec![gauge_event("a.metric", 1.0)])).await.expect("send");

        let state = fake.state();
        assert_eq!(state.flushes, 1, "the success path must flush once");
        assert!(state.unflushed.is_empty());
        assert!(String::from_utf8_lossy(&state.flushed).starts_with("a.metric 1 "));
        drop(state);
        let Conn::Tcp { pool, .. } = &output.conn else { unreachable!() };
        assert!(!pool.is_empty(), "a flushed connection is reusable");
    }

    /// Plaintext and pickle each hand the driver their own framing and report their own
    /// message and datapoint counts.
    #[tokio::test]
    async fn plaintext_and_pickle_over_tcp_report_their_own_counts() {
        for protocol in [Protocol::Plaintext, Protocol::Pickle] {
            let mut collector = Collector::tcp(ReadMode::ToEof).await;
            let mut probe = TelemetryProbe::new();
            let mut output =
                GraphiteOutput::tcp(collector.addr().to_string(), Duration::from_secs(2))
                    .with_encoder(GraphiteEncoder::new().with_protocol(protocol))
                    .unwrap()
                    .with_telemetry(probe.telemetry("out", "graphite_out", "sink"));
            let batch =
                batch_with(vec![gauge_event("a.metric", 1.0), gauge_event("b.metric", 2.0)]);
            output.send(&batch).await.expect("send");
            drop(output);
            let got = collector.next().await;

            let messages = match protocol {
                // One line per datapoint, every line terminated.
                Protocol::Plaintext => {
                    let text = String::from_utf8_lossy(&got).into_owned();
                    assert!(text.ends_with('\n') && text.lines().count() == 2, "{text:?}");
                    2.0
                }
                // One length-prefixed frame carrying both datapoints.
                Protocol::Pickle => {
                    let declared = u32::from_be_bytes(got[..4].try_into().unwrap()) as usize;
                    assert_eq!(declared, got.len() - 4, "one frame, no separator");
                    1.0
                }
            };
            assert_eq!(probe.sum("logit.output.messages", &[]), messages, "{protocol:?}");
            assert_eq!(probe.sum("logit.output.datapoints", &[]), 2.0, "{protocol:?}");
            assert_eq!(probe.sum("logit.output.requests", &[("class", "ok")]), 1.0);
            assert_eq!(probe.sum("logit.output.datagrams", &[]), 0.0, "TCP has no datagrams");
        }
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
        let Conn::Tcp { pool, .. } = &output.conn else { unreachable!() };
        assert!(pool.is_empty(), "a connection that failed mid-frame is never pooled");
    }

    /// The kernel refuses a datagram over the largest UDP payload with `EMSGSIZE`. A builder can
    /// set a cap config validation refuses, so the refused datagram's datapoint is dropped and
    /// counted, the send is `ok`, and the datagrams on either side arrive.
    #[tokio::test]
    async fn a_real_emsgsize_drops_one_datagram_and_the_ones_around_it_arrive() {
        let mut collector = Collector::udp().await;
        let mut probe = TelemetryProbe::new();
        let mut output = GraphiteOutput::udp(collector.addr().to_string())
            .unwrap()
            .with_max_packet_bytes(100_000)
            .with_telemetry(probe.telemetry("out", "graphite_out", "sink"));
        // `<name> 1 1700000000`: lines of 40 013, 70 013, and 40 013 bytes. No two share a
        // datagram, and only the middle one is over 65 507.
        let batch = batch_with(vec![
            gauge_event(&"a".repeat(40_000), 1.0),
            gauge_event(&"b".repeat(70_000), 1.0),
            gauge_event(&"c".repeat(40_000), 1.0),
        ]);
        output.send(&batch).await.expect("an EMSGSIZE datagram is a drop, not a fault");

        let got = collector.take(2).await;
        assert!(got[0].starts_with(b"aaaa") && got[0].len() == 40_013);
        assert!(got[1].starts_with(b"cccc") && got[1].len() == 40_013);
        let dropped =
            probe.sum("logit.output.messages.dropped", &[("reason", "oversize_datagram")]);
        assert_eq!(dropped, 1.0, "one datapoint");
        assert_eq!(probe.sum("logit.output.datapoints", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.messages", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.datagrams", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ok")]), 1.0);
        assert_eq!(dropped + probe.sum("logit.output.datapoints", &[]), 3.0, "sent + dropped");
    }

    /// A batch that fails after two datagrams counts the two datagrams' messages and datapoints
    /// before it returns the error.
    #[tokio::test]
    async fn a_udp_failure_after_two_datagrams_counts_what_reached_the_wire() {
        let script = ScriptedDest::new([
            SendStep::Accept,
            SendStep::Accept,
            SendStep::Fail(std::io::ErrorKind::ConnectionRefused),
        ]);
        let mut probe = TelemetryProbe::new();
        let mut output = GraphiteOutput::udp("127.0.0.1:2003")
            .unwrap()
            .with_max_packet_bytes(14) // one `x 1 1700000000` line per datagram
            .with_telemetry(probe.telemetry("out", "graphite_out", "sink"));
        output.conn = Conn::Udp(UdpDest::Scripted(Arc::clone(&script)));
        let batch = batch_with(["a", "b", "c"].map(|name| gauge_event(name, 1.0)).to_vec());
        let err = output.send(&batch).await.expect_err("the third datagram fails");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(probe.sum("logit.output.messages", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.datapoints", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.datagrams", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ambiguous")]), 1.0);
    }

    /// Pickle frames are bounded by `max_frame_bytes`, not the datagram cap, so a pickle encoder
    /// can't be installed on the UDP transport, as graph validation refuses the config.
    #[tokio::test]
    async fn with_encoder_refuses_pickle_on_udp() {
        let pickle = || GraphiteEncoder::new().with_protocol(Protocol::Pickle);
        let err = GraphiteOutput::udp("127.0.0.1:2003").unwrap().with_encoder(pickle()).err();
        let err = err.expect("pickle over UDP is refused");
        assert!(err.to_string().contains("pickle requires transport: tcp"), "{err}");
        GraphiteOutput::tcp("127.0.0.1:2003", Duration::from_secs(1))
            .with_encoder(pickle())
            .expect("pickle over TCP is fine");
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
            .unwrap()
            .with_telemetry(registry_cap_then_encoder.telemetry_for("out", "graphite_out", "sink"));
        let registry_encoder_then_cap = Registry::new();
        let encoder_then_cap = GraphiteOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_encoder(GraphiteEncoder::new())
            .unwrap()
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
            )
            .unwrap();
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
        let mut collector = Collector::udp().await;
        let registry = Registry::new();
        let mut output = GraphiteOutput::udp(collector.addr().to_string())
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "graphite_out", "sink"));
        let batch = batch_with(vec![gauge_event("app.requests", 42.0)]);

        output.send(&batch).await.expect("send should succeed");
        collector.next().await;

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

    // -- Attempt accounting (ADR `sink-send-path-and-attempt-accounting`, decision 2) ----------

    /// A gauge that encodes, and a gauge delta the codec skips with its diagnostic.
    fn encode_side_batch() -> EventBatch {
        let delta = MetricRecord::new(intern("conns"), MetricKind::GaugeDelta(5.0));
        batch_with(vec![
            tagged_event("load", 1.0, ("host", "a")),
            Event::metric(TS, AttrMap::new(), delta),
        ])
    }

    const ENCODE_SIDE: [(&str, &[(&str, &str)]); 3] = [
        ("logit.output.metrics.skipped", &[("metric_kind", "gauge_delta")]),
        ("logit.component.diagnostics", &[("key", "gauge_delta_unresolved")]),
        ("logit.output.batch.bytes", &[]),
    ];

    /// Runs [`encode_side_batch`] through the write loop over the sink `build` makes, once with a
    /// first attempt that fails `Fault::Clean` and once without, with the builders `build_spec`
    /// calls, and compares.
    async fn assert_a_retry_counts_encode_side_once(build: impl Fn(bool) -> GraphiteOutput) {
        let mut runs = Vec::new();
        for fail_first in [false, true] {
            let mut probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("out", "graphite_out", "sink");
            let mut output = build(fail_first)
                .with_encoder(GraphiteEncoder::new())
                .unwrap()
                .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry);
            let batches = vec![encode_side_batch()];
            runs.push(
                sums_through_write_loop(
                    &mut output,
                    &mut probe,
                    "graphite_out",
                    batches,
                    fast_retry(),
                )
                .await,
            );
        }
        let (single, retried) = (&runs[0], &runs[1]);
        assert_eq!(sum_of(single, "logit.output.requests", &[("class", "ok")]), 1.0);
        assert_eq!(sum_of(retried, "logit.output.requests", &[("class", "clean")]), 1.0);
        assert_eq!(sum_of(retried, "logit.output.requests", &[("class", "ok")]), 1.0);
        assert_counted_once_per_batch(single, retried, &ENCODE_SIDE, &[]);
    }

    #[tokio::test]
    async fn a_udp_retry_counts_encode_side_counters_once() {
        assert_a_retry_counts_encode_side_once(|fail_first| {
            let steps = fail_first.then_some(SendStep::Fail(std::io::ErrorKind::ConnectionRefused));
            let mut output = GraphiteOutput::udp("127.0.0.1:2003").unwrap();
            output.conn = Conn::Udp(UdpDest::Scripted(ScriptedDest::new(steps)));
            output
        })
        .await;
    }

    #[tokio::test]
    async fn a_tcp_retry_counts_encode_side_counters_once() {
        assert_a_retry_counts_encode_side_once(|fail_first| {
            let connect = DialStep::Connect(Box::new(FakeStream::new()));
            let steps = if fail_first { vec![DialStep::Refuse, connect] } else { vec![connect] };
            let mut output = GraphiteOutput::tcp("127.0.0.1:2003", Duration::from_secs(1));
            output.dial_script = Some(Arc::new(ScriptedDial::new(false, steps)));
            output
        })
        .await;
    }

    /// Either builder order leaves the encoder counting through the gated view: the sink's
    /// handles first and the encoder last, as a test or tool might.
    #[tokio::test]
    async fn an_encoder_installed_after_the_handles_counts_encode_side_once_too() {
        let mut runs = Vec::new();
        for fail_first in [false, true] {
            let mut probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("out", "graphite_out", "sink");
            let steps = fail_first.then_some(SendStep::Fail(std::io::ErrorKind::ConnectionRefused));
            let mut output = GraphiteOutput::udp("127.0.0.1:2003")
                .unwrap()
                .with_telemetry(telemetry.clone())
                .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry))
                .with_encoder(GraphiteEncoder::new())
                .unwrap();
            output.conn = Conn::Udp(UdpDest::Scripted(ScriptedDest::new(steps)));
            let batches = vec![encode_side_batch()];
            runs.push(
                sums_through_write_loop(
                    &mut output,
                    &mut probe,
                    "graphite_out",
                    batches,
                    fast_retry(),
                )
                .await,
            );
        }
        assert_counted_once_per_batch(&runs[0], &runs[1], &ENCODE_SIDE, &[]);
    }

    /// A batch the encoder skips whole returns `Ok` early and still leaves the accounting
    /// disarmed, so later direct sends count.
    #[tokio::test]
    async fn direct_sends_after_a_batch_that_encoded_nothing_count_every_time() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "graphite_out", "sink");
        let mut output = GraphiteOutput::udp("127.0.0.1:2003")
            .unwrap()
            .with_encoder(GraphiteEncoder::new())
            .unwrap()
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        output.conn = Conn::Udp(UdpDest::Scripted(ScriptedDest::new([])));
        assert_direct_sends_count_after_an_empty_batch(
            &mut output,
            &mut probe,
            "graphite_out",
            batch_with(vec![log_event()]),
            encode_side_batch,
            &ENCODE_SIDE,
        )
        .await;
    }
}
