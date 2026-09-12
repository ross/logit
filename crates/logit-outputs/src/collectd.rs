//! collectd binary-protocol egress over UDP -- the mirror of `collectd_in` (W2), and a real relay:
//! identity, values, and kinds round-trip through the real `CollectdDecoder` on the other end (this
//! module's own tests). See [ADR `collectd-binary-relay`](../../../../docs/adr/collectd-binary-relay.md)
//! and [`docs/plans/collectd-binary-relay.md`](../../../../docs/plans/collectd-binary-relay.md).
//!
//! Split the way `statsd.rs`/`syslog.rs` are: the pure [`logit_proto::collectd::CollectdEncoder`]
//! does every mapping, sanitization, and packing decision (that module's own doc is the spec for
//! all of it -- decode/encode tables, sanitization rules, permitted normalizations), and this
//! module is only the thin transport wrapper around it: [`CollectdOutput`] owns the socket, hands
//! the encoder a batch, and turns its already-packed [`logit_proto::MessageBuf`]`<usize>` into UDP
//! sends. **The codec emits every metric/tag/identity counter and diagnostic itself** (it holds its
//! own [`Telemetry`]/[`Diagnostics`], fed by this sink's own builders below) -- this module adds
//! only the transport-level counters a socket send can produce that the codec has no way to know
//! about: total bytes, request timing, datagrams actually written, and an `EMSGSIZE` drop.
//!
//! **This implements [`logit_proto::FramedEncoder`], not `logit_proto::Encoder`** -- the identical
//! reason `statsd_out`/`syslog_out`/`prometheus_out`'s encoders do too (`crate::statsd`'s module
//! doc, ADR `framed-encoder`). [`logit_proto::collectd::CollectdEncoder::encode_into`] fills a
//! [`logit_proto::MessageBuf`]`<usize>` with datagram boundaries already decided (the codec's own
//! packing loop, elision, and `max_packet_bytes` cap -- encoder state, per the trait, rather than a
//! per-call argument), so this sink's only remaining job is one `send_to` per datagram already
//! built.
//!
//! ## Config
//!
//! `endpoint` (`host:port`, resolved once per batch, never at config-load time -- same
//! `statsd_out`/`syslog_out` precedent), `max_packet_bytes` (default `1452`, collectd's own
//! `MaxPacketSize` default -- [`logit_proto::collectd::DEFAULT_MAX_PACKET_BYTES`]), and an optional
//! `hostname:` used only when an event carries neither `collectd.host` nor `host.name` -- exactly
//! `syslog_out`'s own `hostname:` field, not a mandatory one: this sink neither reads the OS
//! hostname nor invents a placeholder (`CollectdEncoder::with_hostname`'s own doc explains why), so
//! with nothing configured and nothing on the event, the list is dropped and counted rather than
//! silently mislabeled. UDP only -- collectd's `network` plugin has no TCP mode to relay onto.
//!
//! ## Packing
//!
//! Entirely the codec's job (`CollectdEncoder::encode_into`'s own doc): this sink calls it once per
//! batch and then just walks the resulting [`logit_proto::MessageBuf`]`<usize>`, one `send_to` per
//! already-packed datagram. Unlike `statsd_out`, there is no live packing-against-a-cap in the send
//! path at all -- the datagram boundaries are fixed before this module ever touches a socket.
//!
//! ## Faults
//!
//! `endpoint` is resolved once per batch (a non-numeric host must not be re-resolved once per
//! datagram); a resolution failure is [`Fault::Clean`]. `EMSGSIZE` (raw OS error 90, or
//! `ErrorKind::InvalidInput` on a platform where the `send_to` shim never reaches the syscall) on
//! one datagram counts that datagram's value-list count under
//! `logit.output.messages.dropped{reason="oversize_datagram"}` plus a throttled diagnostic, and
//! sending continues with the next datagram -- one oversize datagram must not sink an otherwise
//! deliverable batch. Any other send error is [`Fault::Clean`] if no datagram in this batch has
//! been sent yet, else [`Fault::Ambiguous`] (some datagrams may have already landed).
//!
//! ## Telemetry (transport-level; the codec's own counters are documented on it, not here)
//!
//! `logit.output.batch.bytes` (total bytes across every datagram in the batch, emitted only when
//! there is something to send), `logit.output.request.duration` (one timer per `send` call that
//! actually touches the socket), `logit.output.requests{class="ok"|"error"}`,
//! `logit.output.messages` (value lists actually sent -- the per-datagram list count each
//! [`logit_proto::MessageBuf`] entry's `usize` meta carries, summed), `logit.output.datagrams`
//! (datagrams actually sent), and `logit.output.messages.dropped{reason="oversize_datagram"}` plus
//! a throttled `oversize_datagram` diagnostic for the `EMSGSIZE` case above.
//!
//! ## Duplicate safety
//!
//! [`CollectdOutput::duplicate_safe`] is `false`: a redelivered value list re-applied at the
//! receiver double-counts every COUNTER/DERIVE/ABSOLUTE it carries (collectd has no idempotency key
//! of any kind) -- the identical reasoning `statsd_out` gives for its own counters, and collectd's
//! ABSOLUTE type in particular is a delta that would double-apply exactly like a statsd `|c` would.

use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{Fault, Output};
use logit_proto::collectd::{CollectdEncoder, DEFAULT_MAX_PACKET_BYTES};
use logit_proto::{FramedEncoder, MessageBuf};
use tokio::net::{lookup_host, UdpSocket};

/// `logit_pipeline::Output` for `collectd_out`. Built via [`CollectdOutput::udp`] -- never a bare
/// constructor, mirroring `StatsdOutput`/`SyslogOutput`.
pub struct CollectdOutput {
    endpoint: String,
    socket: UdpSocket,
    encoder: CollectdEncoder,
    /// Reused across `send` calls: the codec's own packing buffer, one entry per datagram, whose
    /// `usize` meta is that datagram's value-list count.
    buf: MessageBuf<usize>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl CollectdOutput {
    /// Binds an ephemeral local UDP socket eagerly -- see `StatsdOutput::udp`'s doc comment for why
    /// `endpoint` itself is resolved per `send`, not here: a `collectd_out` pointed at a destination
    /// that isn't up yet is not a config error, but a bad local bind is.
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let socket = std::net::UdpSocket::bind("0.0.0.0:0")
            .context("binding collectd_out's local UDP socket")?;
        socket.set_nonblocking(true).context("configuring collectd_out's UDP socket")?;
        let socket =
            UdpSocket::from_std(socket).context("registering collectd_out's UDP socket")?;
        Ok(Self {
            endpoint: endpoint.into(),
            socket,
            // UDP only, always -- unlike `StatsdEncoder`'s uncapped `usize::MAX` default (shared
            // with a TCP transport that has no datagram to overflow), `collectd_out` has no TCP
            // branch at all, so its own default datagram cap applies unconditionally.
            encoder: CollectdEncoder::new().with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES),
            buf: MessageBuf::default(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        })
    }

    pub fn with_encoder(mut self, encoder: CollectdEncoder) -> Self {
        self.encoder = encoder;
        self
    }

    /// Forwards straight to the encoder -- [`CollectdEncoder::with_max_packet_bytes`] is encoder
    /// state now ([`FramedEncoder`]'s one `encode_into` signature has no room for a per-call cap),
    /// so this sink has no cap of its own to keep in sync. Call after
    /// [`CollectdOutput::with_encoder`] if both are used together, so the cap lands on the encoder
    /// that is actually kept.
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.encoder = self.encoder.with_max_packet_bytes(max_packet_bytes);
        self
    }

    /// Kept on this sink for its own transport-level diagnostic (`oversize_datagram`), and also fed
    /// into the encoder -- the codec emits its own diagnostics (`no_host`, `unencodable_value`,
    /// ...) through this same handle, so both halves of one `send` report through one component id.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.encoder = self.encoder.with_diagnostics(diag);
        self
    }

    /// Kept on this sink for its own transport-level counters, and also fed into the encoder --
    /// unlike `StatsdOutput`, whose encoder returns an `EncodeStats` for the sink to turn into
    /// telemetry itself, `CollectdEncoder` emits its own `logit.output.*` counters directly (this
    /// module's doc, "Telemetry"), so it needs the same handle this sink uses for the rest.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry.clone();
        self.encoder = self.encoder.with_telemetry(telemetry);
        self
    }
}

#[async_trait::async_trait]
impl Output for CollectdOutput {
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

    /// `false`: a redelivered value list re-applied at the receiver double-counts every
    /// COUNTER/DERIVE/ABSOLUTE it carries -- collectd has no idempotency key of any kind, the same
    /// reasoning `StatsdOutput::duplicate_safe` gives for its own counters.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

impl CollectdOutput {
    /// Sends every already-packed datagram in `buf`, one `send_to` per datagram -- no packing
    /// decision left to make here, unlike `StatsdOutput::send_udp`: `CollectdEncoder::encode_into`
    /// already chose every datagram boundary against its own `max_packet_bytes`. Returns `(value
    /// lists sent, datagrams sent)`.
    async fn send_udp(
        socket: &UdpSocket,
        endpoint: &str,
        buf: &MessageBuf<usize>,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) -> anyhow::Result<(usize, usize)> {
        // Resolved once per batch, not once per datagram -- see `statsd::send_udp`'s doc comment
        // for why a non-numeric host must not be re-resolved on every call.
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

/// `90` is `EMSGSIZE` on Linux specifically -- see `statsd::is_message_too_large`'s doc comment
/// (copied rather than shared: it isn't `pub`, and this sink has no other reason to reach into
/// `crate::statsd`); this repo only ever ships/runs inside the Linux containers it builds.
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

    /// A like-relay event: `collectd.*` identity present, one gauge record, so the encoder emits it
    /// as a single Values part with no `hostname:` fallback needed.
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
        event.metrics.push(MetricRecord::new(intern(plugin), MetricKind::Gauge(value)));
        event
    }

    /// A fallback-shaped event: no `collectd.*` identity at all, so this only survives encoding
    /// with a configured `hostname:` -- and even then only via the fallback naming path.
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

    fn attr(event: &Event, key: &str) -> Option<Value> {
        event.attributes.get(key).cloned()
    }

    /// Whether `registry` recorded a point named `metric` carrying `tag`. Drains, so call once.
    fn counted(registry: &Registry, metric: &str, tag: (&str, &str)) -> bool {
        registry.drain(0).into_iter().any(|event| {
            event.metrics.iter().any(|m| logit_core::interner::resolve(m.name) == metric)
                && event.attributes.get(tag.0).and_then(|v| v.as_str()) == Some(tag.1)
        })
    }

    /// The summed value of every counter point named `metric` in an already-drained `events`,
    /// regardless of tags. Takes the drained slice rather than the `Registry` itself -- `drain`
    /// empties the registry, so computing this for several metrics needs one shared drain, not
    /// one per metric (each of which would find nothing after the first).
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
        assert_eq!(events.len(), 1);
        assert_eq!(attr(&events[0], ATTR_HOST), Some(Value::from("web-1")));
        assert_eq!(attr(&events[0], ATTR_PLUGIN), Some(Value::from("load")));
        assert_eq!(events[0].metrics[0].kind, MetricKind::Gauge(0.5));
    }

    #[tokio::test]
    async fn a_low_cap_packs_several_events_into_several_datagrams() {
        let (addr, collector) = udp_collector().await;
        let mut output = CollectdOutput::udp(addr.to_string()).unwrap().with_max_packet_bytes(64);
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

        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        for datagram in datagrams {
            decoder
                .decode_into(bytes::Bytes::from(datagram), TS, &mut events)
                .expect("every datagram must decode");
        }
        assert_eq!(events.len(), 10);
    }

    #[tokio::test]
    async fn a_batch_with_nothing_encodable_performs_no_io_at_all() {
        let mut output = CollectdOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![log_event()]);
        output.send(&batch).await.expect("an all-skipped batch must not attempt any I/O");
    }

    /// A fallback-shaped event has no `collectd.host`/`collectd.type`, and this sink was built with
    /// no `hostname:` configured -- the codec drops it (`no_host`), which is still zero I/O, not a
    /// send error, even against an address nothing is listening on.
    #[tokio::test]
    async fn a_batch_with_no_resolvable_host_performs_no_io_at_all() {
        let mut output = CollectdOutput::udp("127.0.0.1:1").unwrap();
        let batch = batch_with(vec![counter_event("hits", 1.0)]);
        output.send(&batch).await.expect("a fully-dropped batch must not attempt any I/O");
    }

    // -- Faults ---------------------------------------------------------------------------------

    /// No `:port` at all -- `lookup_host` rejects this synchronously, with no DNS lookup and so no
    /// dependency on this test environment having network access, the same failure mode a
    /// genuinely unresolvable hostname produces one layer further in (both are `Fault::Clean`).
    #[tokio::test]
    async fn an_unresolvable_endpoint_is_classified_as_a_clean_fault() {
        let mut output = CollectdOutput::udp("no-port-in-this-endpoint")
            .unwrap()
            .with_encoder(CollectdEncoder::new().with_hostname("fixture-host"));
        // A configured hostname is what lets this fallback-shaped event survive encoding at all,
        // so the failure below is really about endpoint resolution, not an empty batch.
        let batch = batch_with(vec![counter_event("hits", 1.0)]);
        let err = output.send(&batch).await.expect_err("resolution should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn duplicate_safe_is_false() {
        let output = CollectdOutput::udp("127.0.0.1:0").unwrap();
        assert!(!output.duplicate_safe());
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

        // One shared drain: `metric_sum` reads an already-drained slice for exactly this reason.
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
