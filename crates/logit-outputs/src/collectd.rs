//! `collectd_out`: collectd's binary `network` protocol over UDP, the mirror of `collectd_in`
//! ([ADR `collectd-binary-relay`](../../../../docs/adr/collectd-binary-relay.md)).
//!
//! [`logit_proto::collectd::CollectdEncoder`] makes every mapping, sanitization, and packing
//! decision, and emits its own metric/identity counters and diagnostics through the
//! [`Telemetry`]/[`Diagnostics`] this sink's builders hand it; `logit_proto::collectd`'s module
//! doc is the spec. It implements [`logit_proto::FramedEncoder`] (ADR `framed-encoder`): each
//! [`logit_proto::MessageBuf`]`<usize>` entry is one already-packed datagram, meta = its message
//! count (value lists, or `1` for a notification's datagram), because the receiver resets its
//! sticky state at each datagram edge. This module sends each entry with one `send_to` and adds
//! only transport-level telemetry.
//!
//! ## Config
//!
//! - `endpoint`: `host:port`, resolved once per batch, never at config load.
//! - `max_packet_bytes`: default `1452` (collectd's `MaxPacketSize`,
//!   [`logit_proto::collectd::DEFAULT_MAX_PACKET_BYTES`]), bounded to `1024..=65535` by graph
//!   rule 38.
//! - `hostname:`: optional, used only when an event carries neither `collectd.host` nor
//!   `host.name`. With neither, the list is dropped and counted (`no_host`); the sink never reads
//!   the OS hostname or invents one (`CollectdEncoder::with_hostname`).
//!
//! UDP only: collectd's `network` plugin has no TCP mode.
//!
//! ## Faults
//!
//! - A resolution failure is [`Fault::Clean`].
//! - `EMSGSIZE` (raw OS error 90, or `ErrorKind::InvalidInput` where the shim never reaches the
//!   syscall) on one datagram is a counted drop, not a `Fault`: its message count goes to
//!   `logit.output.messages.dropped{reason="oversize_datagram"}` with a throttled diagnostic, and
//!   sending continues. Because `send` still reports success, a cap above 65535 would fail every
//!   datagram while counting `requests{class="ok"}`, which is why rule 38 bounds it.
//! - Any other send error is [`Fault::Clean`] if no datagram of the batch was sent yet, else
//!   [`Fault::Ambiguous`].
//!
//! ## Telemetry
//!
//! Transport-level only; the codec documents its own. `logit.output.batch.bytes` (only when there
//! is something to send), `logit.output.request.duration`,
//! `logit.output.requests{class="ok"|"error"}`, `logit.output.messages` (Σ sent entries' meta),
//! `logit.output.datagrams`, and the `oversize_datagram` drop above.
//!
//! ## Duplicate safety
//!
//! [`CollectdOutput::duplicate_safe`] is `false`: collectd has no idempotency key, so a
//! redelivered value list double-counts every COUNTER/DERIVE/ABSOLUTE it carries, as a statsd `|c`
//! would.

use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{Fault, Output};
use logit_proto::collectd::{CollectdEncoder, DEFAULT_MAX_PACKET_BYTES};
use logit_proto::{FramedEncoder, MessageBuf};
use tokio::net::{lookup_host, UdpSocket};

/// `logit_pipeline::Output` for `collectd_out`, built via [`CollectdOutput::udp`].
pub struct CollectdOutput {
    endpoint: String,
    socket: UdpSocket,
    encoder: CollectdEncoder,
    /// Kept here too so [`CollectdOutput::with_encoder`] can re-apply it in any builder order.
    max_packet_bytes: usize,
    /// Reused across `send`s: one entry per packed datagram, meta = its message count.
    buf: MessageBuf<usize>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl CollectdOutput {
    /// Binds an ephemeral local UDP socket eagerly: a bad local bind is a config error, but a
    /// destination that isn't up yet isn't, so `endpoint` is resolved per `send`.
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")
            .context("binding collectd_out's local UDP socket")?;
        socket.set_nonblocking(true).context("configuring collectd_out's UDP socket")?;
        let socket =
            UdpSocket::from_std(socket).context("registering collectd_out's UDP socket")?;
        Ok(Self {
            endpoint: endpoint.into(),
            socket,
            // `CollectdEncoder::new()` is uncapped; with no TCP branch, the datagram cap always
            // applies.
            encoder: CollectdEncoder::new().with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES),
            max_packet_bytes: DEFAULT_MAX_PACKET_BYTES,
            buf: MessageBuf::default(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        })
    }

    /// Installs `encoder` with this sink's cap, diagnostics, and telemetry re-applied, so builder
    /// order doesn't matter. A bare assignment would revert a prior `with_max_packet_bytes` to the
    /// encoder's uncapped default (every datagram then fails `EMSGSIZE` while counting
    /// `requests{class="ok"}`) and drop both handles, silencing the codec's own counters.
    pub fn with_encoder(mut self, encoder: CollectdEncoder) -> Self {
        self.encoder = encoder
            .with_max_packet_bytes(self.max_packet_bytes)
            .with_diagnostics(self.diag.clone())
            .with_telemetry(self.telemetry.clone());
        self
    }

    /// Sets the datagram cap on the encoder, keeping a copy for [`CollectdOutput::with_encoder`].
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        self.encoder = self.encoder.with_max_packet_bytes(max_packet_bytes);
        self
    }

    /// Shared with the encoder, so the sink's `oversize_datagram` and the codec's `no_host` etc.
    /// report under one component id.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    /// Shared with the encoder, which emits its own `logit.output.*` counters directly rather than
    /// leaving them to the sink as `StatsdOutput`'s encoder does.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry.clone();
        self.encoder = self.encoder.with_telemetry(telemetry);
        self
    }
}

#[async_trait::async_trait]
impl Output for CollectdOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        // `Stats` discarded: the encoder already reported them through its own handles.
        self.encoder.encode_into(batch, &mut self.buf);

        if self.buf.is_empty() {
            return Ok(());
        }

        self.telemetry.count("logit.output.batch.bytes", self.buf.total_bytes() as f64, &[]);
        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = Self::send_udp(
            &self.socket,
            &self.endpoint,
            &self.buf,
            &mut self.diag,
            &self.telemetry,
        )
        .await;
        drop(request_timer);

        match &result {
            Ok((messages, datagrams)) => {
                self.telemetry.count("logit.output.messages", *messages as f64, &[]);
                self.telemetry.count("logit.output.datagrams", *datagrams as f64, &[]);
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "ok")]);
            }
            Err(_) => {
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "error")]);
            }
        }
        result.map(|_| ())
    }

    /// A redelivered value list double-counts every COUNTER/DERIVE/ABSOLUTE; there's no
    /// idempotency key.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

impl CollectdOutput {
    /// Sends each already-packed datagram in `buf` with one `send_to`; the encoder chose every
    /// boundary. Returns `(messages, datagrams)` sent.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        buf: &MessageBuf<usize>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize)> {
        // Once per batch: a non-numeric host must not be re-resolved per datagram.
        let mut addrs = lookup_host(endpoint)
            .await
            .context("resolving collectd_out endpoint")
            .context(Fault::Clean)?;
        let addr = addrs
            .next()
            .context("collectd_out endpoint resolved to no addresses")
            .context(Fault::Clean)?;

        let mut messages = 0usize;
        let mut datagrams = 0usize;
        for (bytes, lists) in buf.iter_with() {
            let lists = *lists;
            match socket.send_to(bytes, addr).await {
                Ok(_) => {
                    messages += lists;
                    datagrams += 1;
                }
                Err(err) if is_message_too_large(&err) => {
                    telemetry.count(
                        "logit.output.messages.dropped",
                        lists as f64,
                        &[("reason", "oversize_datagram")],
                    );
                    diag.warn_throttled(
                        "oversize_datagram",
                        format_args!("collectd_out: packed datagram too large for one send: {err}"),
                    );
                }
                Err(err) => {
                    let fault = if datagrams > 0 { Fault::Ambiguous } else { Fault::Clean };
                    return Err(anyhow::Error::new(err).context(fault));
                }
            }
        }
        Ok((messages, datagrams))
    }
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
        AttrMap, BodyFormat, Event, LogRecord, MetricKind, MetricRecord, Registry, Resource, Sum,
        Temporality, Value,
    };
    use logit_proto::collectd::{
        CollectdDecoder, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN, ATTR_TYPE,
    };
    use logit_proto::Decoder;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::UdpSocket as TokioUdpSocket;

    const TS: i64 = 1_700_000_000_000_000_000;

    fn attrs(pairs: &[(&str, Value)]) -> AttrMap {
        let mut map = AttrMap::new();
        for (key, value) in pairs {
            map.insert(key, value.clone());
        }
        map
    }

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    /// A like-relay event: `collectd.*` identity and one gauge. Decode-shaped so whole-`Event`
    /// equality against a real decode is meaningful: the record name is `<plugin>.<type>`, the
    /// decoder's naming rule for a single-data-source list.
    fn relay_event(host: &str, plugin: &str, value: f64) -> Event {
        let mut event = Event::empty(
            TS,
            attrs(&[
                (ATTR_HOST, Value::from(host)),
                (ATTR_PLUGIN, Value::from(plugin)),
                (ATTR_TYPE, Value::from(plugin)),
                (ATTR_INTERVAL, Value::F64(10.0)),
            ]),
        );
        event.metrics.push(MetricRecord::new(
            intern(&format!("{plugin}.{plugin}")),
            MetricKind::Gauge(value),
        ));
        event
    }

    /// A fallback-shaped event: no `collectd.*` identity, so it encodes only with a configured
    /// `hostname:`, via the fallback naming path.
    fn counter_event(name: &str, value: f64) -> Event {
        Event::metric(
            TS,
            AttrMap::new(),
            MetricRecord::new(
                intern(name),
                MetricKind::Sum(Sum {
                    value,
                    temporality: Temporality::Cumulative,
                    monotonic: true,
                }),
            ),
        )
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

    /// The summed value of every counter point named `metric` in an already-drained `events`, any
    /// tags. Takes the slice because `drain` empties the registry, so several metrics need one
    /// shared drain.
    fn metric_sum(events: &[Event], metric: &str) -> f64 {
        events
            .iter()
            .flat_map(|e| &e.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == metric)
            .map(|m| match &m.kind {
                MetricKind::Sum(s) => s.value,
                other => panic!("{metric} must be a counter, got {other:?}"),
            })
            .sum()
    }

    // -- Socket ---------------------------------------------------------------------------------

    async fn udp_collector() -> (SocketAddr, Arc<TokioUdpSocket>) {
        let socket = TokioUdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        (addr, Arc::new(socket))
    }

    /// Whole-`Event` equality against the input: a fixed-point assertion that also covers the
    /// interval, timestamp, and record kind/name.
    #[tokio::test]
    async fn a_packed_datagram_round_trips_through_a_real_collector_and_decoder() {
        let (addr, collector) = udp_collector().await;
        let mut output = CollectdOutput::udp(addr.to_string()).unwrap();
        let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);

        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let (n, _) = collector.recv_from(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });
        output.send(&batch).await.expect("send should succeed");
        let received =
            tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();

        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        decoder
            .decode_into(bytes::Bytes::from(received), TS, &mut events)
            .expect("the real decoder must accept what this sink sent");
        assert_eq!(events, batch.events, "decode(send(b)) must equal b, not just a few fields");
    }

    /// Whole-`Event` equality plus each datagram's size against the cap, since a packing bug can
    /// bleed identity across datagrams and still decode correctly.
    #[tokio::test]
    async fn a_low_cap_packs_several_events_into_several_datagrams() {
        let (addr, collector) = udp_collector().await;
        const CAP: usize = 64;
        let mut output = CollectdOutput::udp(addr.to_string()).unwrap().with_max_packet_bytes(CAP);
        let events: Vec<Event> =
            (0..10).map(|i| relay_event("web-1", &format!("p{i}"), i as f64)).collect();
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
        assert!(datagrams.len() > 1, "10 lists cannot fit one 64-byte datagram");
        for datagram in &datagrams {
            assert!(
                datagram.len() <= CAP,
                "a datagram of {} bytes exceeded the cap",
                datagram.len()
            );
        }

        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut decoded = Vec::new();
        for datagram in datagrams {
            decoder
                .decode_into(bytes::Bytes::from(datagram), TS, &mut decoded)
                .expect("every datagram must decode");
        }
        assert_eq!(decoded, batch.events, "decode(send(b)) must equal b, not just a count");
    }

    #[tokio::test]
    async fn a_batch_with_nothing_encodable_performs_no_io_at_all() {
        let mut output = CollectdOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![log_event()]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    /// With no host anywhere, the codec drops the event (`no_host`): zero I/O, not a send error.
    #[tokio::test]
    async fn a_batch_with_no_resolvable_host_performs_no_io_at_all() {
        let mut output = CollectdOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![counter_event("hits", 1.0)]);
        output.send(&batch).await.expect("a fully-dropped batch must not attempt any I/O");
    }

    // -- Faults ---------------------------------------------------------------------------------

    /// No `:port`: `lookup_host` rejects it without a DNS lookup, so the test needs no network.
    #[tokio::test]
    async fn an_unresolvable_endpoint_is_classified_as_a_clean_fault() {
        let mut output = CollectdOutput::udp("no-port-in-this-endpoint")
            .unwrap()
            .with_encoder(CollectdEncoder::new().with_hostname("fixture-host"));
        // The hostname makes the event encode, so the failure is resolution, not an empty batch.
        let batch = batch_with(vec![counter_event("hits", 1.0)]);
        let err = output.send(&batch).await.expect_err("resolution should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn duplicate_safe_is_false() {
        let output = CollectdOutput::udp("127.0.0.1:0").unwrap();
        assert!(!output.duplicate_safe());
    }

    /// `with_encoder`/`with_max_packet_bytes` are order-independent: an 8-byte cap drops the event
    /// as oversize in either order.
    #[tokio::test]
    async fn the_encoder_cap_is_order_independent_with_with_encoder() {
        let cap_then_encoder = CollectdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_max_packet_bytes(8)
            .with_encoder(CollectdEncoder::new().with_hostname("fixture-host"));
        let encoder_then_cap = CollectdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_encoder(CollectdEncoder::new().with_hostname("fixture-host"))
            .with_max_packet_bytes(8);
        for mut output in [cap_then_encoder, encoder_then_cap] {
            let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);
            output.send(&batch).await.expect("an all-dropped batch must not attempt any I/O");
        }
    }

    /// `with_encoder` must not drop the diagnostics/telemetry handles a caller already installed.
    #[tokio::test]
    async fn diagnostics_and_telemetry_survive_with_encoder_called_afterward() {
        let registry = Registry::new();
        let diag_registry = Registry::new();
        let diag = Diagnostics::new("out").with_telemetry(diag_registry.telemetry_for(
            "out/diag",
            "collectd_out",
            "sink",
        ));
        let mut output = CollectdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "collectd_out", "sink"))
            .with_diagnostics(diag)
            .with_encoder(CollectdEncoder::new());
        // No host anywhere: `no_host` is counted through both handles, if they survived.
        let batch = batch_with(vec![counter_event("hits", 1.0)]);
        output.send(&batch).await.expect("nothing encodable means no I/O");

        assert!(counted(&registry, "logit.output.metrics.skipped", ("reason", "no_host")));
        assert!(counted(&diag_registry, "logit.component.diagnostics", ("key", "no_host")));
    }

    // -- Telemetry --------------------------------------------------------------------------

    #[tokio::test]
    async fn a_successful_send_reports_batch_bytes_messages_datagrams_and_an_ok_request() {
        let (addr, collector) = udp_collector().await;
        let registry = Registry::new();
        let mut output = CollectdOutput::udp(addr.to_string())
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "collectd_out", "sink"));
        let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);

        let recv_task = tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            collector.recv_from(&mut buf).await.unwrap();
        });
        output.send(&batch).await.expect("send should succeed");
        tokio::time::timeout(Duration::from_secs(2), recv_task).await.unwrap().unwrap();

        // One shared drain for every `metric_sum` below.
        let events = registry.drain(0);
        assert!(metric_sum(&events, "logit.output.batch.bytes") > 0.0);
        assert_eq!(metric_sum(&events, "logit.output.messages"), 1.0);
        assert_eq!(metric_sum(&events, "logit.output.datagrams"), 1.0);
        assert_eq!(metric_sum(&events, "logit.output.requests"), 1.0);
    }

    #[tokio::test]
    async fn a_no_host_drop_is_counted_by_the_codec_through_the_shared_telemetry_handle() {
        let registry = Registry::new();
        let mut output = CollectdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "collectd_out", "sink"));
        let batch = batch_with(vec![counter_event("hits", 1.0)]);
        output.send(&batch).await.expect("nothing encodable means no I/O");

        assert!(
            counted(&registry, "logit.output.metrics.skipped", ("reason", "no_host")),
            "expected the codec's own no_host counter, fed through this sink's shared Telemetry"
        );
    }

    #[tokio::test]
    async fn a_no_host_diagnostic_is_reported_through_the_shared_diagnostics_handle() {
        let registry = Registry::new();
        let diag_registry = Registry::new();
        let diag = Diagnostics::new("out").with_telemetry(diag_registry.telemetry_for(
            "out/diag",
            "collectd_out",
            "sink",
        ));
        let mut output = CollectdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "collectd_out", "sink"))
            .with_diagnostics(diag);
        let batch = batch_with(vec![counter_event("hits", 1.0)]);
        output.send(&batch).await.expect("nothing encodable means no I/O");

        assert!(
            counted(&diag_registry, "logit.component.diagnostics", ("key", "no_host")),
            "expected a no_host diagnostic via the shared Diagnostics handle"
        );
    }
}
