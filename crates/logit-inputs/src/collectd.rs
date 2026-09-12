//! collectd's binary "`network` plugin" protocol over UDP -- the listener half of
//! [ADR `collectd-binary-relay`](../../../docs/adr/collectd-binary-relay.md)'s
//! `collectd_in -> collectd_out` lossless-relay pair
//! (`docs/plans/collectd-binary-relay.md`'s W2).
//!
//! The wire format, the sticky-identity state machine, the `| Wire | Model |` mapping table and the
//! permitted normalizations all live with the codec, in
//! [`logit_proto::collectd`]'s module doc -- that doc is the spec, and this one deliberately does
//! not restate it. What lives here is the *component*: its configuration, its socket behaviour, and
//! what it reports.
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
//! An ordinary `host:port` binds that address. A **multicast group** -- collectd's own defaults are
//! `239.192.74.66` (IPv4) and `ff18::efc0:4a42` (IPv6), which is what a sender configured with a
//! bare `Server "239.192.74.66"` writes to -- is detected from the address itself and joined
//! automatically: `SO_REUSEADDR`, a bind of the unspecified address on that port, then an
//! `IP_ADD_MEMBERSHIP`/`IPV6_JOIN_GROUP` on the default interface. There is no `multicast:` field,
//! because the address already says everything there is to say. A failed join fails startup rather
//! than warning -- see [`crate::udp`]'s `bind_one`. The `bound` info line names the group.
//!
//! One consequence worth stating outright, since the config line does not: because the socket is
//! bound to the **unspecified** address rather than to the group, a `bind: 239.192.74.66:25826`
//! listener also accepts ordinary *unicast* datagrams sent to that port from any source, and
//! [`CollectdInput::local_addr`] reports `0.0.0.0:<port>` rather than the group. Joining a group is
//! additive -- it is not a filter that narrows what else the port receives.
//!
//! ## `types_db`
//!
//! Zero or more paths to collectd `types.db` files (relative ones resolve against the config
//! file's directory), read once at startup and merged in order -- a later file overrides an
//! earlier file's definition of the same type. They supply **data-source names**, nothing else:
//! a resolved multi-data-source list is named `<plugin>.<type>.<ds_name>` instead of
//! `<plugin>.<type>.<i>`, and a resolved single-data-source list is `<plugin>.<type>` either way.
//! A type the files do not define is index-named, silently; a type they define *differently* from
//! what arrived is index-named with a `types_db_mismatch` diagnostic. See
//! [`logit_proto::collectd::types_db`] for the file format and
//! [`logit_proto::collectd::CollectdDecoder::with_types_db`] for the naming rule.
//!
//! Record names are display/cross-protocol only: `collectd_out` re-encodes from the `collectd.*`
//! attributes, the `MetricList` order and each record's kind, so configuring `types_db` (or not)
//! never changes what a `collectd_in -> collectd_out` relay puts back on the wire. It changes what
//! an InfluxDB/Prometheus/statsd sink calls the series.
//!
//! collectd's own `types.db` is GPL-licensed and is **not** shipped with `logit`; an operator
//! points this at the copy their collectd installation already has.
//!
//! ## Diagnostics
//!
//! Every one is throttled (`logit.component.diagnostics{key}`,
//! `docs/design/internal-telemetry.md`):
//!
//! | Key | Meaning |
//! |---|---|
//! | `bad_datagram` | the whole datagram failed to decode and nothing was salvaged -- the shared listener's own key (`crate::udp`), raised here by a malformed first part |
//! | `bad_part` | a malformed part *behind* at least one decoded value list: the earlier lists are kept, the rest of the datagram is abandoned |
//! | `incomplete_identity` | a value list arrived with an empty host, plugin or type; skipped, exactly as collectd's own receiver rejects it |
//! | `encrypted_packet_dropped` | a `SecurityLevel Encrypt` datagram: this codec holds no keys, so the rest of the datagram is dropped (`docs/known-gaps.md`) |
//! | `types_db_mismatch` | the configured `types_db` defines this list's type with a different data-source count or kinds than arrived; index naming is used instead |
//!
//! ## Telemetry
//!
//! All of it comes from the shared UDP listener driver, identically to `statsd_in`/`syslog_in`:
//! `logit.input.datagrams`/`logit.input.datagram.bytes` (what actually arrived on the wire, which
//! the `Fanout`-level `events.sent` cannot tell apart from one busy sender), the
//! `logit.component.receive.*` receive-queue gauges, and `logit.input.receive_buffer.bytes`.
//! This component adds none of its own.

use crate::udp::{UdpListener, UdpListenerConfig};
use crate::Input;
use logit_core::{Diagnostics, Resource, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::collectd::{CollectdDecoder, TypesDb};
use std::sync::Arc;
use tokio::sync::watch;

/// Thin wrapper over [`UdpListener<CollectdDecoder>`] -- the read/decode split, the datagram-\>batch
/// assembly and the multicast-aware bind all live there
/// (`docs/adr/decoupled-listener-io.md`, [`crate::udp`]); this type is the decoder choice plus the
/// builder surface `logit-cli::pipeline` wires a `collectd_in` component through. The direct
/// counterpart of [`crate::statsd::StatsdInput`].
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

    /// Attaches a component id to this listener's diagnostics -- and to the [`CollectdDecoder`] it
    /// wraps, so both report under the same id. Both halves matter, for the same reason
    /// [`crate::statsd::StatsdInput::with_diagnostics`] documents: `UdpListener`'s own `diag` is
    /// what a whole-datagram decode failure reports through (`bad_datagram`), while the decoder's
    /// own is what everything finer-grained goes through (`bad_part`, `incomplete_identity`,
    /// `encrypted_packet_dropped`, `types_db_mismatch`) -- two distinct `Diagnostics` values that
    /// must both carry the same id and telemetry handle, or one class of decode failure silently
    /// reports under no component id and with telemetry disabled.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.inner =
            self.inner.with_diagnostics(diag.clone()).map_decoder(|d| d.with_diagnostics(diag));
        self
    }

    /// Attaches a telemetry handle -- the wire-level datagram/byte counters the shared listener
    /// emits (`docs/design/internal-telemetry.md`'s "layer 3").
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.inner = self.inner.with_telemetry(telemetry);
        self
    }

    /// Gives the wrapped decoder an already-loaded `types.db` -- see this module's doc. An `Arc`
    /// because the decoder holds it for the process's lifetime while the caller keeps its own
    /// handle; `logit-cli::pipeline`'s `build_spec` does one `TypesDb::load` per `collectd_in`
    /// component and shares that one map with that component's decoder (two listeners naming the
    /// same file each parse it, into two independent maps).
    pub fn with_types_db(mut self, types_db: Arc<TypesDb>) -> Self {
        self.inner = self.inner.map_decoder(|d| d.with_types_db(types_db));
        self
    }

    /// Overrides the receive-queue/batching/shutdown-grace knobs a `receive:` config block sets
    /// (`docs/adr/decoupled-listener-io.md`). Defaults to [`UdpListenerConfig::default`] when
    /// never called.
    pub fn with_receive(mut self, config: UdpListenerConfig) -> Self {
        self.inner = self.inner.with_config(config);
        self
    }

    /// The currently-configured receive-queue/batching/shutdown-grace knobs -- for test
    /// introspection (`logit-cli::pipeline`'s `build_spec` wiring tests).
    pub fn receive_config(&self) -> UdpListenerConfig {
        self.inner.config()
    }

    /// Passthrough to the wrapped [`UdpListener::local_addr`]: lets a caller (a round-trip test)
    /// learn the real ephemeral port after `bind()`, with no bind-drop race.
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
    use logit_core::{Event, MetricKind, Temporality, Value};
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

    /// The whole component against a real socket: bind an ephemeral port, run it, send one
    /// hand-built datagram, and drain the `Fanout` it delivers into -- the same shape
    /// `crates/logit-inputs/src/udp.rs`'s `bind_then_run_delivers_a_real_datagram` uses for the
    /// driver itself, but through `CollectdInput`'s own decoder and builder surface.
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

    /// `with_types_db` has to reach the *decoder*, not sit on the wrapper: the record name is the
    /// only observable difference, so this asserts it through a real socket rather than by
    /// introspection.
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

    /// The guard both sibling inputs carry (`statsd.rs`/`syslog.rs`'s own
    /// `with_diagnostics_reaches_the_wrapped_decoder_too`): dropping `with_diagnostics`'s
    /// `.map_decoder(..)` half compiles fine and silently leaves every decoder-side diagnostic
    /// (`bad_part`, `incomplete_identity`, `encrypted_packet_dropped`, `types_db_mismatch`)
    /// reporting under no component id and with telemetry disabled.
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

    /// `with_receive` is what a `receive:` block reaches, and `receive_config` is how
    /// `logit-cli`'s `build_spec` test reads it back.
    #[test]
    fn with_receive_round_trips_through_receive_config() {
        let config = UdpListenerConfig { max_datagrams: 4242, ..UdpListenerConfig::default() };
        let input = CollectdInput::new("127.0.0.1:0").with_receive(config);
        assert_eq!(input.receive_config().max_datagrams, 4242);
    }

    // ---- recorded interop fixtures (testdata/interop/collectd/) --------------------------------
    //
    // Real datagrams from a real collectd's own `network` plugin -- not this codec's encoder, not a
    // hand-built `PacketBuilder` packet -- recorded by `script/record-fixtures collectd`. See
    // testdata/interop/collectd/README.md for the provenance table and
    // docs/plans/recorded-interop-fixtures.md for why the corpus exists at all.
    //
    // These assert on **decoded, identifiable values** (the host, which plugins arrived, a list's
    // data-source count and kinds, the interval), never on the fixture bytes: re-running the
    // recorder changes every measured value, every timestamp, and even which lists land in which
    // datagram, and a test pinned to any of that would be testing this directory's stability rather
    // than the decoder (testdata/interop/README.md's "Consuming these fixtures").
    //
    // They live here rather than in `logit-proto` beside the codec's own unit tests for the same
    // reason `syslog.rs`'s do: this is the component an operator actually points at a collectd, and
    // `CollectdDecoder` plus `with_types_db` is exactly the surface `collectd_in` configures.

    const INTEROP_FIXTURES: [&str; 3] =
        ["collectd-000.raw", "collectd-001.raw", "collectd-002.raw"];

    /// A hand-written `types.db` covering exactly the six types these fixtures carry, in stock
    /// collectd's own data-source layout. Hand-written on purpose: collectd's own `types.db` is
    /// GPL-licensed and is never copied into this repo (see this module's doc and
    /// [`logit_proto::collectd::types_db`]), and a fixture that only has to cover six types is
    /// clearer than 200 lines of someone else's file anyway.
    const FIXTURE_TYPES_DB: &str = "\
# hand-written for crates/logit-inputs/src/collectd.rs's interop tests -- not collectd's own file
load\t\tshortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000
memory\t\tvalue:GAUGE:0:281474976710656
if_octets\trx:DERIVE:0:U, tx:DERIVE:0:U
if_packets\trx:DERIVE:0:U, tx:DERIVE:0:U
if_errors\trx:DERIVE:0:U, tx:DERIVE:0:U
if_dropped\trx:DERIVE:0:U, tx:DERIVE:0:U
";

    /// Deliberately *before* the capture window (2023-11-14), so every "the timestamp came off the
    /// wire" assertion below would fail loudly if the decoder ever fell back to receipt time for
    /// these datagrams -- every list collectd sends carries a TimeHR part.
    const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;
    /// 2026-09-12T00:00:00Z, the day these fixtures were recorded: a lower bound on every decoded
    /// timestamp.
    const CAPTURED_ON_OR_AFTER: i64 = 1_789_171_200_000_000_000;
    /// 2100-01-01T00:00:00Z. Deliberately loose at this end: the capture date only ever moves
    /// forward on a re-record, so a tight upper bound would be a test that expires.
    const CAPTURED_BEFORE: i64 = 4_102_444_800_000_000_000;

    fn interop_fixture(name: &str) -> bytes::Bytes {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/collectd")
            .join(name);
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("reading interop fixture {}: {e}", path.display()));
        bytes::Bytes::from(raw)
    }

    /// Decodes one recorded datagram through a real [`CollectdDecoder`], with the diagnostics
    /// mirrored into a drainable registry so a test can assert that *nothing* was diagnosed -- the
    /// point of a recorded fixture being that a real sender's output should decode clean.
    fn decode_interop(
        name: &str,
        types_db: Option<&str>,
    ) -> (Vec<Event>, Arc<Registry>) {
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

    /// The first event decoded from `name` whose plugin and type match -- collectd packs unrelated
    /// lists into one datagram, so picking one out by identity is how these tests address a list.
    fn list_of<'a>(
        events: &'a [Event],
        plugin: &str,
        type_: &str,
    ) -> &'a Event {
        events
            .iter()
            .find(|e| {
                attr_str(e, "collectd.plugin") == Some(plugin)
                    && attr_str(e, "collectd.type") == Some(type_)
            })
            .unwrap_or_else(|| panic!("no {plugin}/{type_} list in this datagram"))
    }

    /// How many parts of `part_type` the raw datagram carries, walked with the codec's own framing
    /// reader. Used to state the identity-elision claim concretely, in terms of the wire.
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
                let plugin = attr_str(event, "collectd.plugin").expect("every list carries a plugin");
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
        // The elision rule this codec's sticky-identity state machine exists for: collectd writes
        // an identity part only when it differs from the last one written *in the same datagram*,
        // so a packet holding ~25 value lists from three plugins carries exactly one Host part.
        // Asserted against the raw bytes and the decoded events together -- either one alone would
        // miss the point (many events, one Host part *is* the elision).
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
        // The multi-data-source case, from the real `load` plugin: three GAUGEs in one list, which
        // a types.db-less deployment names by index.
        let (events, _) = decode_interop("collectd-000.raw", None);
        let load = list_of(&events, "load", "load");
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
        // The same real list, with names: `load` resolves to three data sources, so the suffix
        // becomes shortterm/midterm/longterm. `memory` is single-data-source, so it is
        // `memory.memory` either way -- the omission rule, checked against a real single-DS list
        // rather than a hand-built one.
        let (events, _) = decode_interop("collectd-000.raw", Some(FIXTURE_TYPES_DB));
        let load = list_of(&events, "load", "load");
        let names: Vec<&str> = load.metrics.iter().map(|r| resolve(r.name)).collect();
        assert_eq!(names, ["load.load.shortterm", "load.load.midterm", "load.load.longterm"]);

        let memory = list_of(&events, "memory", "memory");
        assert_eq!(memory.metrics.len(), 1);
        assert_eq!(resolve(memory.metrics[0].name), "memory.memory");
        assert!(
            attr_str(memory, "collectd.type_instance").is_some(),
            "the memory plugin distinguishes used/free/cached/... by type_instance"
        );
    }

    #[test]
    fn interop_fixture_if_octets_is_two_non_monotonic_cumulative_sums() {
        // The other data-source kind, from the real `interface` plugin: DERIVE, which the model
        // carries as a non-monotonic cumulative Sum (a counter that can be reset by a NIC reset or
        // an interface going away, which is exactly why collectd has DERIVE and not just COUNTER).
        let (events, _) = decode_interop("collectd-000.raw", Some(FIXTURE_TYPES_DB));
        let if_octets = list_of(&events, "interface", "if_octets");
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
            attr_str(if_octets, "collectd.plugin_instance").is_some(),
            "the interface plugin puts the interface name in plugin_instance"
        );
        assert_eq!(if_octets.attributes.get("collectd.type_instance"), None);
    }
}
