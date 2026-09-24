//! collectd's binary `network` plugin protocol over UDP: the listener half of
//! [ADR `collectd-binary-relay`](../../../docs/adr/collectd-binary-relay.md)'s
//! `collectd_in -> collectd_out` lossless-relay pair.
//!
//! The wire format, the sticky-identity state machine, the model mapping, and the permitted
//! normalizations are the codec's, in [`logit_proto::collectd`]'s module doc. This doc covers the
//! component: its configuration, socket behaviour, and reporting.
//!
//! ```yaml
//! components:
//!   collectd_in:
//!     type: collectd_in
//!     bind: "0.0.0.0:25826"          # collectd's own default port
//!     types_db:                      # optional; index naming without it
//!       - /usr/share/collectd/types.db
//!     receive:                       # optional; the shared datagram-listener block
//!       max_datagrams: 8192
//! ```
//!
//! ## `bind`, and multicast
//!
//! An ordinary `host:port` binds that address. A multicast group (collectd's defaults are
//! `239.192.74.66` and `ff18::efc0:4a42`, what a bare `Server "239.192.74.66"` sends to) is
//! detected from the address and joined: `SO_REUSEADDR`, a bind of the unspecified address on that
//! port, then `IP_ADD_MEMBERSHIP`/`IPV6_JOIN_GROUP` on the default interface. There is no
//! `multicast:` field. A failed join fails startup (see [`crate::udp`]'s `bind_one`). The `bound`
//! info line names the group.
//!
//! Because the socket binds the unspecified address, a `bind: 239.192.74.66:25826` listener also
//! accepts unicast datagrams sent to that port from any source, and
//! [`CollectdInput::local_addr`] reports `0.0.0.0:<port>`. Joining a group is additive, not a
//! filter.
//!
//! ## `types_db`
//!
//! Zero or more collectd `types.db` paths (relative ones resolve against the config file's
//! directory), read once at startup and merged in order, a later file overriding an earlier one's
//! type. An unreadable or unparseable file fails startup. They supply data-source names only; the
//! naming rule and the `types_db_mismatch` case are
//! [`logit_proto::collectd::CollectdDecoder::with_types_db`]'s, and the file format is
//! [`logit_proto::collectd::types_db`]'s.
//!
//! **`types_db` never changes what a `collectd_in -> collectd_out` relay puts back on the wire**:
//! `collectd_out` re-encodes from the `collectd.*` attributes, the `MetricList` order, and each
//! record's kind, never the record name. It changes only what other sinks call the series.
//!
//! collectd's `types.db` is GPL-licensed and not shipped with `logit`; point this at the copy the
//! collectd installation already has.
//!
//! ## Diagnostics
//!
//! Every one is throttled (`logit.component.diagnostics{key}`,
//! `docs/design/internal-telemetry.md`). The decoder's keys (`bad_part`, `incomplete_identity`,
//! `encrypted_packet_dropped`, `types_db_mismatch`, `notification_dropped`) are in
//! [`logit_proto::collectd`]'s mapping table. The driver adds `bad_datagram`: a datagram that
//! failed with nothing salvaged, which a malformed first part causes.
//!
//! ## Telemetry
//!
//! All from the shared UDP driver, as for `statsd_in`/`syslog_in`: `logit.input.datagrams`/
//! `logit.input.datagram.bytes`, the `logit.component.receive.*` queue gauges, and
//! `logit.input.receive_buffer.bytes`. This component adds none.

use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::collectd::{CollectdDecoder, TypesDb};
use std::sync::Arc;
use tokio::sync::watch;

/// A `collectd_in` listener: a thin wrapper over [`UdpListener<CollectdDecoder>`], which owns the
/// read/decode split, batch assembly, and multicast-aware bind. See this module's doc.
pub struct CollectdInput {
    inner: UdpListener<CollectdDecoder>,
}

impl CollectdInput {
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            inner: UdpListener::new(
                bind,
                CollectdDecoder::new(Arc::new(Resource::default())),
                UdpListenerConfig::default(),
            ),
        }
    }

    /// Attaches a component id to the driver's diagnostics and to the [`CollectdDecoder`]'s.
    ///
    /// They are two distinct `Diagnostics` values: the driver's carries `bad_datagram`, the
    /// decoder's everything finer-grained (`bad_part`, `types_db_mismatch`, ...). Setting only one
    /// leaves a whole class of failure reporting under no component id.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner =
            self.inner.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag));
        self
    }

    /// Attaches a telemetry handle for the shared listener's datagram and byte counters.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = self.inner.with_telemetry(telemetry);
        self
    }

    /// Gives the decoder an already-loaded `types.db` (see this module's doc). `logit-cli` loads
    /// one per component, so two listeners naming the same file each parse their own copy.
    pub fn with_types_db(mut self, types_db: Arc<TypesDb>) -> Self {
        self.inner = self.inner.map_decoder(|d| d.with_types_db(types_db));
        self
    }

    /// Sets the `receive:` block; every field applies. Defaults to [`UdpListenerConfig::default`].
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }

    /// The configured `receive:` knobs, for `logit-cli::pipeline`'s wiring tests.
    pub fn receive_config(&self) -> UdpListenerConfig {
        self.inner.config()
    }

    /// The bound address once `bind()` has run, so a test learns an ephemeral port with no
    /// bind-drop race.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.inner.local_addr()
    }
}

#[async_trait::async_trait]
impl Input for CollectdInput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        self.inner.bind().await
    }

    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.inner.run(sink).await
    }

    async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.inner.run_until_shutdown(sink, shutdown).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::telemetry::Registry;
    use logit_core::{Event, MetricKind, Severity, Temporality, Value};
    use logit_pipeline::unwrap_batch;
    use logit_proto::collectd::part;
    use logit_proto::Decoder as _;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// One `memory`/`memory`/`used` gauge list from `web-1`, hand-built rather than encoded, so
    /// this test depends on the wire format and not on `CollectdEncoder` agreeing with itself.
    fn single_gauge_packet() -> Vec<u8> {
        let mut bytes = Vec::new();
        string_part(&mut bytes, part::TYPE_HOST, b"web-1");
        number_part(&mut bytes, part::TYPE_TIME_HR, 1_700_000_000 << 30);
        string_part(&mut bytes, part::TYPE_PLUGIN, b"memory");
        string_part(&mut bytes, part::TYPE_TYPE, b"memory");
        string_part(&mut bytes, part::TYPE_TYPE_INSTANCE, b"used");
        // One GAUGE value: count, type byte, then the little-endian `f64`.
        let mut payload = vec![0x00, 0x01, part::DS_GAUGE];
        payload.extend_from_slice(&1.5f64.to_le_bytes());
        part_header(&mut bytes, part::TYPE_VALUES, payload.len());
        bytes.extend_from_slice(&payload);
        bytes
    }

    fn part_header(out: &mut Vec<u8>, part_type: u16, payload_len: usize) {
        out.extend_from_slice(&part_type.to_be_bytes());
        out.extend_from_slice(&((payload_len + 4) as u16).to_be_bytes());
    }

    fn string_part(out: &mut Vec<u8>, part_type: u16, value: &[u8]) {
        part_header(out, part_type, value.len() + 1);
        out.extend_from_slice(value);
        out.push(0);
    }

    fn number_part(out: &mut Vec<u8>, part_type: u16, value: u64) {
        part_header(out, part_type, 8);
        out.extend_from_slice(&value.to_be_bytes());
    }

    /// A hand-built datagram sent to a real socket is delivered through `CollectdInput`.
    #[tokio::test]
    async fn a_real_datagram_decodes_into_delivered_events() {
        let mut input = CollectdInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, mut rx) = mpsc::channel(8);
        let fanout = Fanout::new(vec![tx]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });

        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.expect("sender should bind");
        sender.send_to(&single_gauge_packet(), addr).await.expect("send_to should succeed");

        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a datagram sent to the bound address should be delivered")
            .expect("the channel should not have closed");
        let batch = unwrap_batch(delivered);
        assert_eq!(batch.events.len(), 1, "one Values part is one event");
        let event = &batch.events[0];
        assert_eq!(event.timestamp, 1_700_000_000_000_000_000);
        assert_eq!(event.metrics.len(), 1);
        assert_eq!(resolve(event.metrics[0].name), "memory.memory");
        assert_eq!(event.metrics[0].kind, MetricKind::Gauge(1.5));
        assert_eq!(event.attributes.get("collectd.host"), Some(&Value::from("web-1")));
        assert_eq!(event.attributes.get("collectd.type_instance"), Some(&Value::from("used")));

        handle.abort();
    }

    /// `with_types_db` reaches the decoder, observed as the record name through a real socket.
    #[tokio::test]
    async fn with_types_db_reaches_the_decoder_and_names_data_sources() {
        let types_db = Arc::new(
            TypesDb::parse(logit_proto::collectd::types_db::TEST_TYPES_DB)
                .expect("the fixture must parse"),
        );
        let mut input = CollectdInput::new("127.0.0.1:0").with_types_db(types_db);
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, mut rx) = mpsc::channel(8);
        let fanout = Fanout::new(vec![tx]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle =
            tokio::spawn(async move { input.run_until_shutdown(fanout, shutdown_rx).await });

        // A three-data-source `load`/`load` list, which the fixture types.db names
        // shortterm/midterm/longterm.
        let mut bytes = Vec::new();
        string_part(&mut bytes, part::TYPE_HOST, b"web-1");
        string_part(&mut bytes, part::TYPE_PLUGIN, b"load");
        string_part(&mut bytes, part::TYPE_TYPE, b"load");
        let mut payload = vec![0x00, 0x03, part::DS_GAUGE, part::DS_GAUGE, part::DS_GAUGE];
        for value in [0.1f64, 0.2, 0.3] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        part_header(&mut bytes, part::TYPE_VALUES, payload.len());
        bytes.extend_from_slice(&payload);

        let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.expect("sender should bind");
        sender.send_to(&bytes, addr).await.expect("send_to should succeed");

        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("the datagram should be delivered")
            .expect("the channel should not have closed");
        let batch = unwrap_batch(delivered);
        let names: Vec<&str> =
            batch.events[0].metrics.iter().map(|record| resolve(record.name)).collect();
        assert_eq!(names, vec!["load.load.shortterm", "load.load.midterm", "load.load.longterm"]);

        handle.abort();
    }

    /// `with_diagnostics` reaches the decoder too; dropping `.map_decoder(..)` still compiles.
    #[test]
    fn with_diagnostics_reaches_the_wrapped_decoder_too() {
        let input = CollectdInput::new("127.0.0.1:0").with_diagnostics(Diagnostics::new("my-id"));
        assert_eq!(input.inner.decoder().diag().component_id(), "my-id");
    }

    #[tokio::test]
    async fn local_addr_is_available_after_bind() {
        let mut input = CollectdInput::new("127.0.0.1:0");
        assert_eq!(input.local_addr(), None, "no address before bind()");
        input.bind().await.expect("binding an ephemeral port should succeed");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
    }

    /// `receive_config` reads back what `with_receive` set.
    #[test]
    fn with_receive_round_trips_through_receive_config() {
        let config = UdpListenerConfig { max_datagrams: 4242, ..UdpListenerConfig::default() };
        let input = CollectdInput::new("127.0.0.1:0").with_receive(config);
        assert_eq!(input.receive_config().max_datagrams, 4242);
    }

    // ---- recorded interop fixtures (testdata/interop/collectd/) --------------------------------
    //
    // Real datagrams from a real collectd's `network` plugin, recorded by
    // `script/record-fixtures collectd`; provenance in testdata/interop/collectd/README.md,
    // rationale in docs/plans/recorded-interop-fixtures.md.
    //
    // Assertions are on decoded, identifiable values (the host, which plugins arrived, a list's
    // data-source count and kinds, the interval), never on the bytes: a re-record changes every
    // value, every timestamp, and which lists land in which datagram
    // (testdata/interop/README.md's "Consuming these fixtures").
    //
    // They live here, not in `logit-proto`, because `CollectdDecoder` plus `with_types_db` is the
    // surface `collectd_in` configures and an operator points at a real collectd.

    const INTEROP_FIXTURES: [&str; 3] =
        ["collectd-000.raw", "collectd-001.raw", "collectd-002.raw"];

    /// A hand-written `types.db` for the six types these fixtures carry, in stock collectd's
    /// data-source layout. collectd's own file is GPL-licensed and never copied into this repo.
    const FIXTURE_TYPES_DB: &str = "\
# hand-written for crates/logit-inputs/src/collectd.rs's interop tests -- not collectd's own file
load\t\tshortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000
memory\t\tvalue:GAUGE:0:281474976710656
if_octets\trx:DERIVE:0:U, tx:DERIVE:0:U
if_packets\trx:DERIVE:0:U, tx:DERIVE:0:U
if_errors\trx:DERIVE:0:U, tx:DERIVE:0:U
if_dropped\trx:DERIVE:0:U, tx:DERIVE:0:U
";

    /// 2023-11-14, before the capture window, so a fallback to receipt time fails the timestamp
    /// assertions: every list collectd sends carries a TimeHR part.
    const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;
    /// 2026-09-12T00:00:00Z, the day these fixtures were recorded: a lower bound on every decoded
    /// timestamp.
    const CAPTURED_ON_OR_AFTER: i64 = 1_789_171_200_000_000_000;
    /// 2100-01-01T00:00:00Z. Loose because a re-record only moves the capture date forward.
    const CAPTURED_BEFORE: i64 = 4_102_444_800_000_000_000;

    fn interop_fixture(name: &str) -> bytes::Bytes {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/collectd")
            .join(name);
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()));
        bytes::Bytes::from(raw)
    }

    /// Decodes one recorded datagram, with diagnostics mirrored into a drainable registry so a
    /// test can assert a real sender's output decodes clean.
    fn decode_interop(name: &str, types_db: Option<&str>) -> (Vec<Event>, Arc<Registry>) {
        let registry = Registry::new();
        let diag = Diagnostics::new("collectd_in").with_telemetry(registry.telemetry_for(
            "collectd_in",
            "collectd_in",
            "listener",
        ));
        let mut decoder =
            CollectdDecoder::new(Arc::new(Resource::default())).with_diagnostics(diag);
        if let Some(text) = types_db {
            decoder = decoder.with_types_db(Arc::new(
                TypesDb::parse(text).expect("the hand-written fixture types.db must parse"),
            ));
        }
        let mut events = Vec::new();
        decoder
            .decode_into(interop_fixture(name), RECEIVED_AT, &mut events)
            .unwrap_or_else(|e| panic!("{name} is a real collectd datagram and must decode: {e}"));
        (events, registry)
    }

    /// Every `logit.component.diagnostics{key}` the registry saw. Drains, so call it once.
    fn diagnostic_keys(registry: &Registry) -> Vec<String> {
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

    fn attr_str<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(Value::as_str)
    }

    /// The first `plugin`/`type_` list anywhere in the corpus, decoded with `types_db`.
    ///
    /// Searches the whole corpus, not one named file: collectd's packing decides which lists land
    /// in which datagram, and no one datagram is guaranteed a complete read cycle. The corpus
    /// spans several cycles, so every plugin's lists are somewhere in it.
    fn first_list_across_fixtures(plugin: &str, type_: &str, types_db: Option<&str>) -> Event {
        for name in INTEROP_FIXTURES {
            let (events, _) = decode_interop(name, types_db);
            if let Some(event) = events.into_iter().find(|e| {
                attr_str(e, "collectd.plugin") == Some(plugin)
                    && attr_str(e, "collectd.type") == Some(type_)
            }) {
                return event;
            }
        }
        panic!("no {plugin}/{type_} list anywhere in {INTEROP_FIXTURES:?}")
    }

    /// How many parts of `part_type` the raw datagram carries, walked with the codec's reader.
    fn count_parts(raw: &bytes::Bytes, part_type: u16) -> usize {
        let mut at = 0usize;
        let mut found = 0usize;
        while at < raw.len() {
            let (header, _) = part::read_part(raw, at).expect("a recorded datagram is well framed");
            if header.part_type == part_type {
                found += 1;
            }
            at += header.len;
        }
        found
    }

    #[test]
    fn interop_fixture_every_datagram_decodes_clean_with_the_expected_identity() {
        for name in INTEROP_FIXTURES {
            let (events, registry) = decode_interop(name, Some(FIXTURE_TYPES_DB));
            assert!(!events.is_empty(), "{name}: a recorded datagram carries value lists");
            assert_eq!(
                diagnostic_keys(&registry),
                Vec::<String>::new(),
                "{name}: a real collectd's own output must decode with no bad_part, \
                 incomplete_identity, types_db_mismatch or encrypted_packet_dropped diagnostic"
            );
            for event in &events {
                assert_eq!(
                    attr_str(event, "collectd.host"),
                    Some("logit-fixture"),
                    "{name}: tools/record-fixtures/collectd.conf sets `Hostname \"logit-fixture\"`"
                );
                let plugin =
                    attr_str(event, "collectd.plugin").expect("every list carries a plugin");
                assert!(
                    matches!(plugin, "load" | "memory" | "interface"),
                    "{name}: the config loads exactly these three read plugins, got {plugin:?}"
                );
                assert_eq!(
                    event.attributes.get("collectd.interval"),
                    Some(&Value::F64(1.0)),
                    "{name}: `Interval 1`, carried as an IntervalHR part of 2^30 ticks"
                );
                assert!(
                    (CAPTURED_ON_OR_AFTER..CAPTURED_BEFORE).contains(&event.timestamp),
                    "{name}: the TimeHR part off the wire, not RECEIVED_AT -- got {}",
                    event.timestamp
                );
                assert!(!event.metrics.is_empty(), "{name}: a value list has at least one value");
            }
        }
    }

    #[test]
    fn interop_fixture_sender_elided_identity_parts_across_a_packed_datagram() {
        // Identity elision: collectd writes an identity part only when it differs from the last
        // one in the same datagram, so ~25 lists from three plugins carry one Host part. Asserted
        // on the raw bytes and the decoded events together; many events and one Host part is the
        // elision.
        for name in INTEROP_FIXTURES {
            let raw = interop_fixture(name);
            let (events, _) = decode_interop(name, None);
            assert!(
                events.len() > 1,
                "{name}: collectd packs value lists up to MaxPacketSize, so one datagram is many \
                 lists -- got {}",
                events.len()
            );
            assert_eq!(
                count_parts(&raw, part::TYPE_HOST),
                1,
                "{name}: one Host part for all {} lists is the sender-side elision",
                events.len()
            );
            assert!(
                count_parts(&raw, part::TYPE_PLUGIN) < events.len(),
                "{name}: fewer Plugin parts than value lists means identity was elided, not \
                 repeated per list"
            );
            assert_eq!(
                count_parts(&raw, part::TYPE_VALUES),
                events.len(),
                "{name}: one Values part is one event"
            );
        }
    }

    #[test]
    fn interop_fixture_load_is_three_gauges_index_named_without_a_types_db() {
        // Multi-data-source: `load`'s three GAUGEs, index-named without a types.db.
        let load = first_list_across_fixtures("load", "load", None);
        let names: Vec<&str> = load.metrics.iter().map(|r| resolve(r.name)).collect();
        assert_eq!(names, ["load.load.0", "load.load.1", "load.load.2"]);
        for record in &load.metrics {
            assert!(
                matches!(record.kind, MetricKind::Gauge(v) if v >= 0.0),
                "the load average is a GAUGE, got {:?}",
                record.kind
            );
        }
        assert_eq!(load.attributes.get("collectd.plugin_instance"), None);
        assert_eq!(load.attributes.get("collectd.type_instance"), None);
    }

    #[test]
    fn interop_fixture_load_is_named_from_a_types_db_and_memory_stays_single_data_source() {
        // With names, `load` gets shortterm/midterm/longterm suffixes; single-data-source
        // `memory` is `memory.memory` either way.
        let load = first_list_across_fixtures("load", "load", Some(FIXTURE_TYPES_DB));
        let names: Vec<&str> = load.metrics.iter().map(|r| resolve(r.name)).collect();
        assert_eq!(names, ["load.load.shortterm", "load.load.midterm", "load.load.longterm"]);

        let memory = first_list_across_fixtures("memory", "memory", Some(FIXTURE_TYPES_DB));
        assert_eq!(memory.metrics.len(), 1);
        assert_eq!(resolve(memory.metrics[0].name), "memory.memory");
        assert!(
            attr_str(&memory, "collectd.type_instance").is_some(),
            "the memory plugin distinguishes used/free/cached/... by type_instance"
        );
    }

    #[test]
    fn interop_fixture_if_octets_is_two_non_monotonic_cumulative_sums() {
        // DERIVE, from `interface`: a non-monotonic cumulative Sum, since a NIC reset or a vanished
        // interface can reset it.
        let if_octets =
            first_list_across_fixtures("interface", "if_octets", Some(FIXTURE_TYPES_DB));
        assert_eq!(if_octets.metrics.len(), 2, "if_octets is rx/tx");
        for record in &if_octets.metrics {
            let MetricKind::Sum(sum) = record.kind else {
                panic!("a DERIVE data source decodes to a Sum, got {:?}", record.kind)
            };
            assert_eq!(sum.temporality, Temporality::Cumulative);
            assert!(!sum.monotonic, "DERIVE is the non-monotonic cumulative case");
        }
        let names: Vec<&str> = if_octets.metrics.iter().map(|r| resolve(r.name)).collect();
        assert_eq!(names, ["interface.if_octets.rx", "interface.if_octets.tx"]);
        assert!(
            attr_str(&if_octets, "collectd.plugin_instance").is_some(),
            "the interface plugin puts the interface name in plugin_instance"
        );
        assert_eq!(if_octets.attributes.get("collectd.type_instance"), None);
    }

    /// A real `threshold`-plugin notification decodes to one FAILURE log record. Provenance:
    /// `tools/record-fixtures/collectd.conf`'s `<Plugin threshold>` sets `WarningMax` and
    /// `FailureMax` to `0.0` on `load`'s `shortterm`, so the first read breaches both.
    #[test]
    fn interop_fixture_notification_decodes_to_a_log_record() {
        let (events, registry) = decode_interop("collectd-notification-000.raw", None);
        assert_eq!(
            diagnostic_keys(&registry),
            Vec::<String>::new(),
            "a real collectd notification must decode with no notification_dropped diagnostic"
        );
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(event.metrics.is_empty(), "a notification carries no metrics");
        assert_eq!(attr_str(event, "collectd.host"), Some("logit-fixture"));
        assert_eq!(attr_str(event, "collectd.plugin"), Some("load"));
        assert_eq!(attr_str(event, "collectd.type"), Some("load"));
        assert_eq!(
            event.attributes.get("collectd.severity"),
            Some(&Value::U64(1)),
            "both WarningMax and FailureMax are breached; collectd reports the more severe"
        );
        let log = event.log.as_ref().expect("a Message part must decode to a log record");
        assert_eq!(log.severity, Some(Severity::Error));
        let message = log.message.as_str().expect("collectd's own message is valid UTF-8");
        assert!(!message.is_empty());
        assert!(
            message.contains("shortterm"),
            "the real message names the breached data source: {message:?}"
        );
        assert!(
            (CAPTURED_ON_OR_AFTER..CAPTURED_BEFORE).contains(&event.timestamp),
            "the TimeHR part off the wire, not RECEIVED_AT -- got {}",
            event.timestamp
        );
    }
}
