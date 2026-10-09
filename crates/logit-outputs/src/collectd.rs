//! `collectd_out`: collectd's binary `network` protocol over UDP, the mirror of `collectd_in`
//! ([ADR `collectd-binary-relay`](../../../../docs/adr/collectd-binary-relay.md)).
//!
//! [`logit_proto::collectd::CollectdEncoder`] makes every mapping, sanitization, and packing
//! decision, and emits its own metric/identity counters and diagnostics through the
//! [`Telemetry`]/[`Diagnostics`] this sink's builders hand it; `logit_proto::collectd`'s module
//! doc is the spec. It implements [`logit_proto::FramedEncoder`] (ADR `framed-encoder`): each
//! [`logit_proto::MessageBuf`]`<usize>` entry is one already-packed datagram, meta = its message
//! count (value lists, or `1` for a notification's datagram), because the receiver resets its
//! sticky state at each datagram edge. This module sends each entry as one datagram through the
//! datagram path the UDP sinks share (`crate::datagram`) and adds only transport-level telemetry.
//!
//! ## Config
//!
//! - `endpoint`: `host:port`, resolved once per batch, never at config load, and sent to the
//!   first IPv4 address it resolves to, else the first IPv6 one.
//! - `max_packet_bytes`: default `1452` (collectd's `MaxPacketSize`,
//!   [`logit_proto::collectd::DEFAULT_MAX_PACKET_BYTES`]), bounded to `1024..=65507` by graph
//!   rule 38: collectd's own floor, and the largest UDP payload.
//! - `hostname:`: optional, used only when an event carries neither `collectd.host` nor
//!   `host.name`. With neither, the list is dropped and counted (`no_host`); the sink never reads
//!   the OS hostname or invents one (`CollectdEncoder::with_hostname`).
//!
//! UDP only: collectd's `network` plugin has no TCP mode.
//!
//! ## Faults
//!
//! `crate::datagram`'s module doc lists the rules.
//!
//! - A resolution failure is `Fault::Clean`.
//! - A datagram the kernel refuses with `EMSGSIZE` is a counted drop, not a `Fault`: its message
//!   count goes to `logit.output.messages.dropped{reason="oversize_datagram"}` with a throttled
//!   diagnostic, and sending continues. Because `send` still reports success, a cap above 65507
//!   would fail every datagram while counting `requests{class="ok"}`, which is why rule 38 bounds
//!   it.
//! - Any other send error is `Fault::Clean` if no datagram of the batch was sent yet, else
//!   `Fault::Ambiguous`.
//!
//! ## Telemetry
//!
//! Transport-level only; the codec documents its own. `logit.output.batch.bytes` (only when there
//! is something to send), `logit.output.request.duration`,
//! `logit.output.requests{class="ok"|"clean"|"ambiguous"|"rejected"|"refused"}`, `logit.output.messages`
//! (Σ sent entries' meta) and `logit.output.datagrams`, both counting what reached the kernel
//! before a failure too, and the `oversize_datagram` drop above.
//!
//! ## Delivery posture
//!
//! The default, `at_least_once` (`docs/adr/delivery-semantics.md`, item 5), retries an
//! `Ambiguous` attempt, and collectd has no idempotency key, so a receiving collectd adds a resent
//! `ABSOLUTE` value to its rate. A monotonic `Sum` takes the upstream remedy: an `aggregate` with
//! `temporality: cumulative` makes it a `COUNTER`, whose running total a resend repeats rather
//! than adds. Every `Histogram` is dropped at encode, so none is resent.
//! `buffer.delivery: at_most_once` drops the batch instead.

use crate::accounting::BatchAccounting;
use crate::count_request;
use crate::datagram::{Datagrams, Framing, Report, UdpDest};
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{BatchContext, Output, SeqId};
use logit_proto::collectd::{CollectdEncoder, DEFAULT_MAX_PACKET_BYTES};
use logit_proto::{FramedEncoder, MessageBuf};

/// `logit_pipeline::Output` for `collectd_out`, built via [`CollectdOutput::udp`].
pub struct CollectdOutput {
    endpoint: String,
    udp: UdpDest,
    encoder: CollectdEncoder,
    /// Kept here too so [`CollectdOutput::with_encoder`] can re-apply it in any builder order.
    max_packet_bytes: usize,
    /// Reused across `send`s: one entry per packed datagram, meta = its message count.
    buf: MessageBuf<usize>,
    /// Handed to the shared datagram path, which only clears it: each entry is already a datagram.
    packet_buf: Vec<u8>,
    /// Ungated: the transport's counts, the `oversize_datagram` drops, and the sink's own
    /// warnings. The encoder holds views gated by `accounting` (`crate::accounting`).
    diag: Diagnostics,
    telemetry: Telemetry,
    accounting: BatchAccounting,
}

impl CollectdOutput {
    /// Binds an ephemeral local IPv4 UDP socket eagerly: a bad local bind is a config error, but a
    /// destination that isn't up yet isn't, so `endpoint` is resolved per `send`
    /// (`crate::datagram`'s module doc).
    pub fn udp(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            endpoint: endpoint.into(),
            udp: UdpDest::bind("collectd_out")?,
            // `CollectdEncoder::new()` is uncapped; with no TCP branch, the datagram cap always
            // applies.
            encoder: CollectdEncoder::new().with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES),
            max_packet_bytes: DEFAULT_MAX_PACKET_BYTES,
            buf: MessageBuf::default(),
            packet_buf: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            accounting: BatchAccounting::default(),
        })
    }

    /// Installs `encoder` with this sink's cap and its diagnostics and telemetry (gated)
    /// re-applied, so builder order doesn't matter. A bare assignment would revert a prior
    /// `with_max_packet_bytes` to the encoder's uncapped default (every datagram then fails
    /// `EMSGSIZE` while counting `requests{class="ok"}`) and drop both handles, silencing the
    /// codec's own counters.
    pub fn with_encoder(mut self, encoder: CollectdEncoder) -> Self {
        self.encoder = encoder
            .with_max_packet_bytes(self.max_packet_bytes)
            .with_diagnostics(self.diag.gated(self.accounting.gate()))
            .with_telemetry(self.telemetry.gated(self.accounting.gate()));
        self
    }

    /// Sets the datagram cap on the encoder, keeping a copy for [`CollectdOutput::with_encoder`].
    pub fn with_max_packet_bytes(mut self, max_packet_bytes: usize) -> Self {
        self.max_packet_bytes = max_packet_bytes;
        self.encoder = self.encoder.with_max_packet_bytes(max_packet_bytes);
        self
    }

    /// Shared with the encoder, gated by this sink's batch accounting, so the sink's
    /// `oversize_datagram` and the codec's `no_host` etc. report under one component id.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag.gated(self.accounting.gate()));
        self.diag = diag;
        self
    }

    /// Shared with the encoder, gated by this sink's batch accounting. The encoder emits its own
    /// `logit.output.*` counters rather than leaving them to the sink as `StatsdOutput`'s does.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.encoder = self.encoder.with_telemetry(telemetry.gated(self.accounting.gate()));
        self.telemetry = telemetry;
        self
    }
}

impl CollectdOutput {
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
        // The encoder chose every datagram boundary, so each entry goes out as it is.
        let datagrams = Datagrams {
            entries: &self.buf,
            weight: |lists| *lists,
            cap: self.max_packet_bytes,
            framing: Framing::OnePerEntry,
        };
        let mut report =
            Report { sink: "collectd_out", diag: &mut self.diag, telemetry: &self.telemetry };
        let (sent, result) =
            self.udp.send(&self.endpoint, datagrams, &mut self.packet_buf, &mut report).await;
        drop(request_timer);

        // What reached the kernel, even when the batch then failed. A message is a value list,
        // or a notification.
        self.telemetry.count("logit.output.messages", sent.weight as f64, &[]);
        self.telemetry.count("logit.output.datagrams", sent.datagrams as f64, &[]);
        count_request(&self.telemetry, &result);
        result
    }
}

/// No `flush` override: `send` retains nothing between calls, and a datagram leaves in its
/// `send_to`, so the trait's no-op is right.
#[async_trait::async_trait]
impl Output for CollectdOutput {
    /// Arms this sink's batch accounting (`crate::accounting`).
    fn observe_batch(&mut self, _ctx: BatchContext, _seq: SeqId) {
        self.accounting.observe();
    }

    /// One attempt ([`CollectdOutput::attempt`]). A final result (`Ok`, or a fault
    /// `write_loop` won't retry) disarms the batch accounting on every path, a batch that encoded
    /// to nothing included.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let result = self.attempt(batch).await;
        self.accounting.finish(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        assert_counted_once_per_batch, assert_direct_sends_count_after_an_empty_batch, fast_retry,
        sum_of, sums_through_write_loop, Collector, ScriptedDest, SendStep,
    };
    use logit_core::interner::intern;
    use logit_core::{
        AttrMap, BodyFormat, Event, LogRecord, MetricKind, MetricRecord, Registry, Resource, Sum,
        Temporality, Value,
    };
    use logit_pipeline::test_util::TelemetryProbe;
    use logit_pipeline::Fault;
    use logit_proto::collectd::{
        CollectdDecoder, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN, ATTR_TYPE,
    };
    use logit_proto::Decoder;
    use std::sync::Arc;

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

    /// Whole-`Event` equality against the input: a fixed-point assertion that also covers the
    /// interval, timestamp, and record kind/name.
    #[tokio::test]
    async fn a_packed_datagram_round_trips_through_a_real_collector_and_decoder() {
        let mut collector = Collector::udp().await;
        let mut output = CollectdOutput::udp(collector.addr().to_string()).unwrap();
        let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);
        output.send(&batch).await.expect("send should succeed");
        let received = collector.next().await;

        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
        let mut events = Vec::new();
        decoder
            .decode_into(bytes::Bytes::from(received), TS, &mut events)
            .expect("the real decoder must accept what this sink sent");
        assert_eq!(events, batch.events, "decode(send(b)) must equal b, not just a few fields");
    }

    /// Whole-`Event` equality plus each datagram's size against the cap, since a packing bug can
    /// bleed identity across datagrams and still decode correctly. Reads datagrams until every
    /// event has decoded, rather than draining on a quiet window: the packing itself decides the
    /// count.
    #[tokio::test]
    async fn a_low_cap_packs_several_events_into_several_datagrams() {
        let mut collector = Collector::udp().await;
        const CAP: usize = 64;
        let mut output =
            CollectdOutput::udp(collector.addr().to_string()).unwrap().with_max_packet_bytes(CAP);
        let events: Vec<Event> =
            (0..10).map(|i| relay_event("web-1", &format!("p{i}"), i as f64)).collect();
        let batch = batch_with(events);
        output.send(&batch).await.expect("send should succeed");

        let mut decoder = CollectdDecoder::new(Arc::new(Resource::default()));
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
        assert!(datagrams.len() > 1, "10 lists cannot fit one 64-byte datagram");
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

    /// The kernel refuses a UDP send to port 0 with `EINVAL`: a clean error, not every value list
    /// counted `oversize_datagram` under an `ok` request.
    #[tokio::test]
    async fn a_udp_endpoint_with_port_zero_fails_clean_and_counts_no_oversize() {
        let mut probe = TelemetryProbe::new();
        let mut output = CollectdOutput::udp("127.0.0.1:0")
            .unwrap()
            .with_telemetry(probe.telemetry("out", "collectd_out", "sink"));
        let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);
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
        let mut output = CollectdOutput::udp(collector.addr().to_string()).unwrap();
        let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);
        output.send(&batch).await.expect("an IPv6 endpoint must be reachable");
        collector.next().await;
    }

    /// 2200 lists under distinct plugins: at a 100 000-byte cap the encoder packs one datagram
    /// over the largest UDP payload and one under it.
    fn wide_batch() -> EventBatch {
        batch_with((0..2200).map(|i| relay_event("web-1", &format!("p{i}"), i as f64)).collect())
    }

    /// The kernel refuses a datagram over the largest UDP payload with `EMSGSIZE`. A builder can
    /// set a cap config validation refuses, so the refused datagram's value lists are dropped and
    /// counted, the send is `ok`, and the datagram after it arrives. The expected counts come from
    /// the encoder's own packing.
    #[tokio::test]
    async fn a_real_emsgsize_drops_one_datagram_and_the_one_after_it_arrives() {
        const CAP: usize = 100_000;
        let mut packed = MessageBuf::default();
        CollectdEncoder::new().with_max_packet_bytes(CAP).encode_into(&wide_batch(), &mut packed);
        let (mut refused, mut fits) = ((0, 0), (0, 0));
        for (datagram, lists) in packed.iter_with() {
            let tally = if datagram.len() > 65_507 { &mut refused } else { &mut fits };
            *tally = (tally.0 + 1, tally.1 + lists);
        }
        assert!(
            refused.0 >= 1 && fits.0 >= 1,
            "the batch must pack both kinds: {refused:?} {fits:?}"
        );

        let mut collector = Collector::udp().await;
        let mut probe = TelemetryProbe::new();
        let mut output = CollectdOutput::udp(collector.addr().to_string())
            .unwrap()
            .with_max_packet_bytes(CAP)
            .with_telemetry(probe.telemetry("out", "collectd_out", "sink"));
        output.send(&wide_batch()).await.expect("an EMSGSIZE datagram is a drop, not a fault");

        collector.take(fits.0).await;
        let dropped =
            probe.sum("logit.output.messages.dropped", &[("reason", "oversize_datagram")]);
        assert_eq!(dropped, refused.1 as f64, "value lists");
        assert_eq!(probe.sum("logit.output.messages", &[]), fits.1 as f64);
        assert_eq!(probe.sum("logit.output.datagrams", &[]), fits.0 as f64);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ok")]), 1.0);
        let messages = probe.sum("logit.output.messages", &[]);
        assert_eq!(dropped + messages, 2200.0, "sent + dropped");
    }

    /// A batch that fails after two datagrams counts the two datagrams' value lists before it
    /// returns the error, under `collectd_out`'s own `requests` class.
    #[tokio::test]
    async fn a_failure_after_two_datagrams_counts_what_reached_the_wire() {
        let script = ScriptedDest::new([
            SendStep::Accept,
            SendStep::Accept,
            SendStep::Fail(std::io::ErrorKind::ConnectionRefused),
        ]);
        let mut probe = TelemetryProbe::new();
        let mut output = CollectdOutput::udp("127.0.0.1:25826")
            .unwrap()
            .with_max_packet_bytes(1024)
            .with_telemetry(probe.telemetry("out", "collectd_out", "sink"));
        output.udp = UdpDest::Scripted(Arc::clone(&script));
        let err = output.send(&wide_batch()).await.expect_err("the third datagram fails");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);

        let mut packed = MessageBuf::default();
        CollectdEncoder::new().with_max_packet_bytes(1024).encode_into(&wide_batch(), &mut packed);
        let first_two: usize = packed.iter_with().take(2).map(|(_, lists)| lists).sum();
        assert_eq!(probe.sum("logit.output.messages", &[]), first_two as f64);
        assert_eq!(probe.sum("logit.output.datagrams", &[]), 2.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "ambiguous")]), 1.0);
        assert_eq!(probe.sum("logit.output.requests", &[("class", "error")]), 0.0);
    }

    /// `with_encoder`/`with_max_packet_bytes` are order-independent: an 8-byte cap makes the
    /// encoder count the value list as `oversize_value_list` in either order. A UDP send never
    /// errors, so only the codec's counter can show the cap was applied.
    #[tokio::test]
    async fn the_encoder_cap_is_order_independent_with_with_encoder() {
        let registry_cap_then_encoder = Registry::new();
        let cap_then_encoder = CollectdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_max_packet_bytes(8)
            .with_encoder(CollectdEncoder::new().with_hostname("fixture-host"))
            .with_telemetry(registry_cap_then_encoder.telemetry_for("out", "collectd_out", "sink"));
        let registry_encoder_then_cap = Registry::new();
        let encoder_then_cap = CollectdOutput::udp("127.0.0.1:1")
            .unwrap()
            .with_encoder(CollectdEncoder::new().with_hostname("fixture-host"))
            .with_max_packet_bytes(8)
            .with_telemetry(registry_encoder_then_cap.telemetry_for("out", "collectd_out", "sink"));
        for (mut output, registry) in [
            (cap_then_encoder, registry_cap_then_encoder),
            (encoder_then_cap, registry_encoder_then_cap),
        ] {
            let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);
            output.send(&batch).await.expect("an all-dropped batch must not attempt any I/O");
            assert!(
                counted(
                    &registry,
                    "logit.output.metrics.skipped",
                    ("reason", "oversize_value_list")
                ),
                "expected the encoder's own cap to have dropped the value list as oversize in \
                 this ordering"
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
        let mut collector = Collector::udp().await;
        let registry = Registry::new();
        let mut output = CollectdOutput::udp(collector.addr().to_string())
            .unwrap()
            .with_telemetry(registry.telemetry_for("out", "collectd_out", "sink"));
        let batch = batch_with(vec![relay_event("web-1", "load", 0.5)]);

        output.send(&batch).await.expect("send should succeed");
        collector.next().await;

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

    // -- Attempt accounting (ADR `sink-send-path-and-attempt-accounting`, decision 2) ----------

    #[tokio::test]
    async fn a_retry_counts_encode_side_counters_once() {
        // A value list that encodes, and one the codec skips for want of a host, with its
        // diagnostic.
        let batch = || batch_with(vec![relay_event("web1", "load", 1.0), counter_event("x", 1.0)]);
        const ENCODE_SIDE: [(&str, &[(&str, &str)]); 3] = [
            ("logit.output.metrics.skipped", &[("reason", "no_host")]),
            ("logit.component.diagnostics", &[("key", "no_host")]),
            ("logit.output.batch.bytes", &[]),
        ];
        let mut runs = Vec::new();
        for fail_first in [false, true] {
            let mut probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("out", "collectd_out", "sink");
            let steps = fail_first.then_some(SendStep::Fail(std::io::ErrorKind::ConnectionRefused));
            let mut output = CollectdOutput::udp("127.0.0.1:25826")
                .unwrap()
                .with_encoder(CollectdEncoder::new())
                .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry);
            output.udp = UdpDest::Scripted(ScriptedDest::new(steps));
            runs.push(
                sums_through_write_loop(
                    &mut output,
                    &mut probe,
                    "collectd_out",
                    vec![batch()],
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

    /// A batch the encoder skips whole returns `Ok` early and still leaves the accounting
    /// disarmed, so later direct sends count.
    #[tokio::test]
    async fn direct_sends_after_a_batch_that_encoded_nothing_count_every_time() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "collectd_out", "sink");
        let mut output = CollectdOutput::udp("127.0.0.1:25826")
            .unwrap()
            .with_encoder(CollectdEncoder::new())
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        output.udp = UdpDest::Scripted(ScriptedDest::new([]));
        let batch = || batch_with(vec![relay_event("web1", "load", 1.0), counter_event("x", 1.0)]);
        assert_direct_sends_count_after_an_empty_batch(
            &mut output,
            &mut probe,
            "collectd_out",
            batch_with(vec![log_event()]),
            batch,
            &[
                ("logit.output.metrics.skipped", &[("reason", "no_host")]),
                ("logit.component.diagnostics", &[("key", "no_host")]),
                ("logit.output.batch.bytes", &[]),
            ],
        )
        .await;
    }
}
