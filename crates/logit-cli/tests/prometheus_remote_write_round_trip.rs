//! `prometheus_out(endpoint) -> prometheus_in(bind)` over real sockets -- the remote-write
//! counterpart to [`prometheus_round_trip.rs`](prometheus_round_trip.rs), which does the same job
//! for the scrape/exposition pair. `docs/plans/prometheus-remote-write.md`'s W6 workstream.
//!
//! The topology under test is four real components and three real sockets:
//!
//! ```text
//! canned hyper target  --scrape-->  PrometheusInput
//!                                        |
//!                                     Fanout
//!                                        v
//!                                 RemoteWriteOutput  --POST-->  PrometheusReceiver
//!                                                                     |
//!                                                                  Fanout
//!                                                                     v
//!                                                               ExposeOutput  <--GET-- reqwest
//! ```
//!
//! and the assertion is that the exposition falling out of the right-hand side equals what
//! `prometheus_round_trip.rs`'s *direct* scrape -> expose pipeline already produces for the same
//! fixture, modulo the normalizations below. The corpus is deliberately the same ten fixtures and
//! the same `.expected` files (`tests/fixtures/prometheus/`, described in that file's own module
//! doc): reusing them is what makes this file a statement about the remote-write *transport*
//! rather than a second, independently-drifting account of the codec.
//!
//! ## What the remote-write leg adds on top of that file's eleven normalizations
//!
//! Everything `prometheus_round_trip.rs`'s module doc lists still applies -- family/series/label
//! reordering, float formatting, the synthesized `untyped`, the text-0.0.4 drops, `# EOF`, the
//! added `instance` label, the `_total` suffix, the cross-dialect type substitution, and the three
//! synthetic scrape families excluded by name. Putting a remote-write hop in the middle adds
//! exactly two more, both of them decided in
//! [ADR `prometheus-remote-write`](../../../docs/adr/prometheus-remote-write.md) rather than
//! discovered here:
//!
//! 12. **Explicit sample timestamps are dropped.** The sender always emits a timestamp (the wire
//!     has no way to omit one, `PrometheusEncoder::with_timestamps_always(true)`), and the
//!     receiver sets `Event::timestamp` from it but deliberately does **not** set the
//!     `prometheus.timestamp: true` marker attribute
//!     (`PrometheusDecoder::with_timestamp_marker(false)`). That marker records a *producer's
//!     choice* to expose a timestamp on a line, and a transport that mandates one is not that
//!     choice -- so an exposition of a received request carries unstamped lines, exactly as the
//!     ADR's "Timestamps" section says it must. [`canonicalize`] clears every series' timestamp on
//!     **both** sides of the comparison rather than skipping the fixtures that carry one, so the
//!     values themselves are still compared.
//! 13. **`_created` does not survive remote-write 1.0.** 1.0's `prometheus.WriteRequest` has no
//!     created-timestamp field at all -- 2.0 carries it as `Sample.start_timestamp` (field 3),
//!     which is why the same fixtures are byte-identical on `version: 2`. [`canonicalize`] drops
//!     `Series::created` from both sides for [`Version::V1`] only, so the 2.0 runs still prove the
//!     round trip preserves it.
//!
//! Nothing else diverges, and that is the point of asserting on the whole rendered body rather
//! than on a handful of `contains` probes: `# HELP`, `# UNIT`, family types (`info`, `stateset`,
//! `gaugehistogram`, `unknown` included), label sets, exemplars with their trace/span references,
//! histogram buckets and summary quantiles all cross both wire versions unchanged, and a
//! regression in any of them fails one of the twenty cases below with a readable diff.
//!
//! ## The rest of the file
//!
//! Beyond the corpus sweep: a `statsd_in -> aggregate(cumulative) -> prometheus_out(endpoint)`
//! pipeline (the ADR's "Temporality is `aggregate`'s job" worked example, pushed over the wire
//! instead of exposed in place); a stale marker; an exemplar; the metadata cache typing a
//! sample-only 1.0 request off a metadata-only one that preceded it; and the backpressure
//! ordering the ADR's response table promises -- the batch reaches the `Fanout` *before* the `204`
//! is built, so a stalled downstream throttles the sender rather than being dropped behind an
//! early acknowledgement.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use logit_core::interner::intern;
use logit_core::{AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource};
use logit_inputs::prometheus::{PrometheusInput, PrometheusReceiver};
use logit_inputs::statsd::StatsdInput;
use logit_outputs::prometheus::{ExposeOutput, RemoteWriteOutput};
use logit_pipeline::{Fanout, Input, Output};
use logit_proto::prometheus::generated::prometheus as pb1;
use logit_proto::prometheus::remote_write::Version;
use logit_proto::prometheus::text::{self, Dialect};
use logit_transforms::{AggregateTemporality, Aggregator};
use prost::Message;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

// -------------------------------------------------------------------------------------------------
// Fixtures and canonicalization
// -------------------------------------------------------------------------------------------------

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/prometheus")
}

fn read_fixture(name: &str, ext: &str) -> Vec<u8> {
    let path = fixtures_dir().join(format!("{name}.{ext}"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()))
}

fn read_fixture_str(name: &str, ext: &str) -> String {
    String::from_utf8(read_fixture(name, ext)).expect("fixture must be utf-8")
}

/// The three families `prometheus_in`'s scrape mode always synthesizes -- excluded from every
/// comparison here by name, exactly as `prometheus_round_trip.rs` excludes them (its normalization
/// 11). They ride the remote-write leg like any other series; they are simply not what this file
/// is asserting about.
const SYNTHETIC_FAMILIES: [&str; 3] = ["up", "scrape_duration_seconds", "scrape_samples_scraped"];

/// Reparses an exposition body, applies the module doc's normalizations 11, 12 and 13, and
/// re-renders it. Applied identically to the *actual* body and to the `.expected` fixture, so the
/// comparison is still byte-for-byte over everything a remote-write hop does not touch -- rather
/// than the fixture being edited to match a run, or whole fixtures being skipped because one line
/// of them carries a timestamp.
fn canonicalize(body: &str, dialect: Dialect, version: Version) -> String {
    let mut families = text::parse(body.as_bytes(), dialect)
        .unwrap_or_else(|e| panic!("a prometheus exposition body must parse: {e}\nbody:\n{body}"));
    families.retain(|family| !SYNTHETIC_FAMILIES.contains(&family.name.as_str()));
    for family in &mut families {
        for series in &mut family.series {
            // 12: the receiver never sets the `prometheus.timestamp` marker, so no line an
            // exposition of a received request writes carries one.
            series.timestamp = None;
            // 13: 1.0 has no created-timestamp field to carry it in.
            if version == Version::V1 {
                series.created = None;
            }
        }
    }
    let mut out = Vec::new();
    text::write(&families, dialect, &mut out);
    String::from_utf8(out).expect("exposition must be utf-8")
}

const ACCEPT_TEXT: &str = "text/plain";
const ACCEPT_OM: &str = "application/openmetrics-text";

fn dialect_of(ext: &str) -> Dialect {
    match ext {
        "text" => Dialect::Text0_0_4,
        "om" => Dialect::OpenMetrics1_0,
        other => panic!("unknown fixture dialect extension {other:?}"),
    }
}

fn content_type_of(ext: &str) -> &'static str {
    dialect_of(ext).content_type()
}

fn accept_of(ext: &str) -> &'static str {
    match ext {
        "text" => ACCEPT_TEXT,
        "om" => ACCEPT_OM,
        other => panic!("unknown fixture dialect extension {other:?}"),
    }
}

// -------------------------------------------------------------------------------------------------
// Real components, real sockets
// -------------------------------------------------------------------------------------------------

/// What a canned connection serves: one fixed body under one fixed `Content-Type`, forever -- the
/// scraped target the corpus sweep pretends to be. The same helper `prometheus_round_trip.rs`
/// carries, duplicated rather than shared for the reason that file's own module doc gives for
/// these round-trip tests living in `logit-cli` at all.
async fn canned_server(body: Bytes, content_type: &'static str) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let io = TokioIo::new(stream);
            let body = body.clone();
            tokio::spawn(async move {
                let service = service_fn(move |_req: Request<Incoming>| {
                    let body = body.clone();
                    async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(200)
                                .header("content-type", content_type)
                                .body(Full::new(body))
                                .unwrap(),
                        )
                    }
                });
                let _ =
                    hyper::server::conn::http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });
    addr
}

/// A plain `reqwest` client with gzip transparently disabled -- a gzipped body should arrive
/// exactly as `prometheus_out` wrote it, not silently inflated by the client.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder().no_gzip().build().expect("a default client always builds")
}

/// The write path this file's receivers bind, and the one `prometheus_in`'s `path:` defaults to.
const WRITE_PATH: &str = "/api/v1/write";

/// A real, bound [`PrometheusReceiver`] serving [`WRITE_PATH`], plus the receiving end of the
/// `Fanout` its batches land in. `channel_capacity` is the knob the backpressure case turns down;
/// `metadata_cache` is `Some((max_families, ttl))` only where a test is about the cache.
async fn start_receiver(
    channel_capacity: usize,
    metadata_cache: Option<(usize, Duration)>,
) -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = PrometheusReceiver::new("127.0.0.1:0", WRITE_PATH);
    if let Some((max_families, ttl)) = metadata_cache {
        input = input.with_metadata_cache(max_families, ttl);
    }
    input.bind().await.expect("binding prometheus_in's receiver");
    let addr = input.local_addr().expect("bind() should leave a real address behind");
    let (tx, rx) = mpsc::channel(channel_capacity);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });
    (addr, rx)
}

/// A real [`RemoteWriteOutput`] pointed at `receiver_addr`, writing `version`.
fn remote_write_sender(receiver_addr: SocketAddr, version: Version) -> RemoteWriteOutput {
    RemoteWriteOutput::new(format!("http://{receiver_addr}{WRITE_PATH}")).with_version(version)
}

/// Scrapes `target_addr` with a real, bound [`PrometheusInput`] on a very short interval and
/// returns the one `EventBatch` it forwards through a bare [`Fanout`].
async fn scrape_once(target_addr: SocketAddr) -> EventBatch {
    let mut input = PrometheusInput::new(
        vec![format!("http://{target_addr}/metrics")],
        Duration::from_millis(20),
    );
    let (tx, mut rx) = mpsc::channel(16);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });

    // `PrometheusInput::run` swallows its first (immediate) tick, so the first real scrape lands
    // after one full `interval` -- a generous timeout well past that single 20ms wait.
    let delivered = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("prometheus_in should scrape and forward a batch")
        .expect("the Fanout channel should not have closed");
    logit_pipeline::unwrap_batch(delivered)
}

/// Waits for exactly `want` batches to come out of a receiver's `Fanout` and returns them. Fails
/// the test rather than hanging if the receiver never delivers.
async fn collect_batches(
    rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
    want: usize,
) -> Vec<EventBatch> {
    let mut batches = Vec::with_capacity(want);
    while batches.len() < want {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("prometheus_in's receiver delivered {}/{want} batches", batches.len())
            })
            .expect("the Fanout channel should not have closed");
        batches.push(logit_pipeline::unwrap_batch(delivered));
    }
    batches
}

/// Binds a fresh [`ExposeOutput`], sends every batch in `batches` into it, then fetches its
/// exposition with `accept`.
async fn expose_and_fetch(batches: &[EventBatch], accept: &str) -> String {
    let mut output = ExposeOutput::new("127.0.0.1:0");
    output.bind().await.expect("binding prometheus_out's exposition");
    let addr = output.local_addr().expect("bind() should leave a real address behind");
    for batch in batches {
        output.send(batch).await.expect("send never fails");
    }

    let response = http_client()
        .get(format!("http://{addr}/metrics"))
        .header("accept", accept)
        .send()
        .await
        .expect("the scrape request should reach prometheus_out");
    assert_eq!(response.status(), 200);
    response.text().await.expect("body should be text")
}

// -------------------------------------------------------------------------------------------------
// The corpus sweep: scrape -> remote-write -> expose, both wire versions
// -------------------------------------------------------------------------------------------------

/// `(fixture name, input dialect, requested output dialect)` -- exactly
/// `prometheus_round_trip.rs`'s ten cases, in the order that file declares them. Table-driven
/// rather than twenty `#[tokio::test]`s: the same ten cases run twice, once per wire version, and
/// `crates/logit-inputs/src/collectd.rs`'s own `ALL_FIXTURES` sweep is the precedent for a corpus
/// loop naming its case in the assertion message.
const CASES: [(&str, &str, &str); 10] = [
    ("counter_no_total", "text", "text"),
    ("counter_created", "om", "om"),
    ("gauge", "text", "text"),
    ("histogram", "om", "om"),
    ("summary", "text", "text"),
    ("untyped_no_type", "text", "text"),
    ("escaped_and_special", "text", "text"),
    ("full_openmetrics", "om", "om"),
    ("dialect_text_untyped_to_om", "text", "om"),
    ("dialect_om_types_to_text", "om", "text"),
];

/// One case: scrape `name`'s `.{in_ext}.in` body, relay it over remote-write `version`, request the
/// exposition back in `out_ext`'s dialect, and assert the result equals `name`'s
/// `.{out_ext}.expected` under [`canonicalize`].
async fn assert_fixture_round_trips(name: &str, in_ext: &str, out_ext: &str, version: Version) {
    let target_addr = canned_server(
        Bytes::from(read_fixture(name, &format!("{in_ext}.in"))),
        content_type_of(in_ext),
    )
    .await;
    let scraped = scrape_once(target_addr).await;

    let (receiver_addr, mut rx) = start_receiver(16, None).await;
    let mut sender = remote_write_sender(receiver_addr, version);
    sender.send(&scraped).await.expect("the receiver should accept the write");
    let received = collect_batches(&mut rx, 1).await;

    let raw = expose_and_fetch(&received, accept_of(out_ext)).await;
    // The scraped target's real ephemeral `instance` value becomes the placeholder the fixtures
    // carry (`prometheus_round_trip.rs`'s normalization 8), applied to the *actual* body before
    // canonicalizing -- never to the fixture.
    let actual = canonicalize(
        &raw.replace(&target_addr.to_string(), "{port}"),
        dialect_of(out_ext),
        version,
    );
    let expected = canonicalize(
        &read_fixture_str(name, &format!("{out_ext}.expected")),
        dialect_of(out_ext),
        version,
    );
    assert_eq!(actual, expected, "{name}: {in_ext} -> remote-write {version:?} -> {out_ext}");
}

#[tokio::test]
async fn every_fixture_round_trips_over_remote_write_1_0() {
    for (name, in_ext, out_ext) in CASES {
        assert_fixture_round_trips(name, in_ext, out_ext, Version::V1).await;
    }
}

#[tokio::test]
async fn every_fixture_round_trips_over_remote_write_2_0() {
    for (name, in_ext, out_ext) in CASES {
        assert_fixture_round_trips(name, in_ext, out_ext, Version::V2).await;
    }
}

/// 2.0 carries the created timestamp per sample (`Sample.start_timestamp`), so a counter's
/// `_created` line survives the wire -- the half of module-doc normalization 13 that is *not* a
/// normalization. Asserted directly rather than left implicit in the sweep above, because the
/// sweep's `canonicalize` keeps `created` on 2.0 only by not dropping it, which is easy to break
/// silently.
#[tokio::test]
async fn remote_write_2_0_preserves_a_created_timestamp() {
    let target_addr =
        canned_server(Bytes::from(read_fixture("counter_created", "om.in")), content_type_of("om"))
            .await;
    let scraped = scrape_once(target_addr).await;

    let (receiver_addr, mut rx) = start_receiver(16, None).await;
    let mut sender = remote_write_sender(receiver_addr, Version::V2);
    sender.send(&scraped).await.expect("the receiver should accept the write");
    let received = collect_batches(&mut rx, 1).await;

    let body = expose_and_fetch(&received, ACCEPT_OM).await;
    assert!(
        body.contains("requests_created{") && body.contains("} 1605281325.123"),
        "remote-write 2.0 must carry Sample.start_timestamp through to a _created line, got:\n{body}"
    );
}

/// An OpenMetrics bucket exemplar -- labels, value, and (in `full_openmetrics`) a real trace/span
/// reference -- crosses the wire and comes back out on the exposition. The sweep asserts this as
/// part of whole-body equality; this names it, so a regression reads as "the exemplar was lost"
/// rather than as a diff in one of ten fixtures.
#[tokio::test]
async fn an_exemplar_with_a_trace_reference_survives_the_wire() {
    for version in [Version::V1, Version::V2] {
        let target_addr = canned_server(
            Bytes::from(read_fixture("full_openmetrics", "om.in")),
            content_type_of("om"),
        )
        .await;
        let scraped = scrape_once(target_addr).await;

        let (receiver_addr, mut rx) = start_receiver(16, None).await;
        let mut sender = remote_write_sender(receiver_addr, version);
        sender.send(&scraped).await.expect("the receiver should accept the write");
        let received = collect_batches(&mut rx, 1).await;

        let body = expose_and_fetch(&received, ACCEPT_OM).await;
        assert!(
            body.contains(
                "# {span_id=\"fedcba9876543210\",trace_id=\"0123456789abcdef0123456789abcdef\"} 0.5"
            ),
            "{version:?}: the counter exemplar's trace reference must survive, got:\n{body}"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// Pipeline cases: real components, no fixtures
// -------------------------------------------------------------------------------------------------

/// `statsd_in -> aggregate(temporality: cumulative) -> prometheus_out(endpoint)`: the ADR's
/// "Temporality is `aggregate`'s job" worked example with the exposition replaced by a real
/// remote-write hop. Two `hits:1|c` datagrams absorb into one cumulative counter, which crosses
/// the wire and renders as `hits_total 2` on the far side.
#[tokio::test]
async fn statsd_through_cumulative_aggregate_writes_a_cumulative_counter_over_the_wire() {
    // Bind-drop-rebind: `StatsdInput` exposes no `local_addr()` accessor (its `UdpListener` is
    // private), so an ephemeral port is reserved with a throwaway socket first -- the same idiom
    // `prometheus_round_trip.rs`'s own statsd case uses.
    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let mut input = StatsdInput::new(addr.to_string());
    input.bind().await.expect("binding statsd_in");
    let (tx, mut rx) = mpsc::channel(16);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });

    let sender_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender_socket.send_to(b"hits:1|c", addr).await.expect("send should reach statsd_in");
    sender_socket.send_to(b"hits:1|c", addr).await.expect("send should reach statsd_in");

    let mut aggregator = Aggregator::new(Duration::from_secs(3600))
        .with_temporality(AggregateTemporality::Cumulative)
        .with_series_retention(5, 10_000);

    // `UdpListenerConfig::default`'s `batch_flush_interval` may coalesce both datagrams into one
    // batch, so this collects *events* until both increments have arrived rather than assuming a
    // 1:1 batch:datagram correspondence.
    let mut absorbed_events = 0;
    while absorbed_events < 2 {
        let delivered = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("statsd_in should forward both increments")
            .expect("the Fanout channel should not have closed");
        let batch = logit_pipeline::unwrap_batch(delivered);
        let resource = batch.resource.clone();
        for mut event in batch.events {
            absorbed_events += 1;
            let forwarded = aggregator.process(&resource, &mut event);
            assert!(!forwarded, "a delta Sum under cumulative temporality must be absorbed");
        }
    }

    let flushed = aggregator.flush(1_700_000_000_000_000_000);
    assert!(!flushed.is_empty(), "two absorbed increments must produce a flushed batch");
    let batches: Vec<EventBatch> = flushed
        .into_iter()
        .map(|(resource, scope, events)| EventBatch {
            resource,
            scope,
            events: events.into_iter().map(|(event, _links)| event).collect(),
        })
        .collect();

    let (receiver_addr, mut received_rx) = start_receiver(16, None).await;
    let mut sender = remote_write_sender(receiver_addr, Version::V2);
    for batch in &batches {
        sender.send(batch).await.expect("the receiver should accept the write");
    }
    let received = collect_batches(&mut received_rx, batches.len()).await;

    let body = expose_and_fetch(&received, ACCEPT_OM).await;
    assert!(
        body.contains("hits_total 2"),
        "expected a cumulative hits_total 2 sample on the far side of the wire, got:\n{body}"
    );
}

/// A stale marker end to end: a `Gauge` carrying `FLAG_NO_RECORDED_VALUE` leaves the sender as
/// Prometheus's stale NaN (`PrometheusEncoder::with_stale_markers(true)`, which
/// `RemoteWriteOutput` sets unconditionally) and arrives at the receiver as the same flag on the
/// same series -- the ADR's "Stale markers <-> `FLAG_NO_RECORDED_VALUE`" section, over a socket.
/// Both wire versions, since the marker is a sample *value* and therefore version-independent.
#[tokio::test]
async fn a_stale_marker_crosses_the_wire_as_no_recorded_value() {
    for version in [Version::V1, Version::V2] {
        let mut attributes = AttrMap::new();
        attributes.insert("room", "attic");
        let record = MetricRecord {
            flags: MetricRecord::FLAG_NO_RECORDED_VALUE,
            ..MetricRecord::new(intern("temperature_celsius"), MetricKind::Gauge(0.0))
        };
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::metric(1_700_000_000_000_000_000, attributes, record)],
        };

        let (receiver_addr, mut rx) = start_receiver(16, None).await;
        let mut sender = remote_write_sender(receiver_addr, version);
        sender.send(&batch).await.expect("the receiver should accept the write");
        let received = collect_batches(&mut rx, 1).await;

        let events: Vec<&Event> = received.iter().flat_map(|batch| batch.events.iter()).collect();
        let stale = events
            .iter()
            .flat_map(|event| event.metrics.iter())
            .find(|record| logit_core::interner::resolve(record.name) == "temperature_celsius")
            .unwrap_or_else(|| panic!("{version:?}: the stale series must arrive, got {events:?}"));
        assert!(
            stale.is_no_recorded_value(),
            "{version:?}: the stale NaN must decode back to FLAG_NO_RECORDED_VALUE, got {stale:?}"
        );
    }
}

// -------------------------------------------------------------------------------------------------
// The metadata cache, over the wire
// -------------------------------------------------------------------------------------------------

/// Snappy-block-compresses `body` and `POST`s it to a receiver as remote-write 1.0, returning the
/// response status. Hand-built rather than routed through [`RemoteWriteOutput`] because the two
/// requests this exercises -- a metadata-only one and a samples-only one -- are shapes *only* a
/// real Prometheus 1.0 sender produces: `logit`'s own sender always attaches `metadata[]` to the
/// samples it describes, which is exactly the case the cache is not needed for.
async fn post_v1(receiver_addr: SocketAddr, request: &pb1::WriteRequest) -> reqwest::StatusCode {
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy-compressing a hand-built request");
    let response = http_client()
        .post(format!("http://{receiver_addr}{WRITE_PATH}"))
        .header("content-type", Version::V1.content_type())
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", Version::V1.header_version())
        .body(compressed)
        .send()
        .await
        .expect("the write should reach prometheus_in's receiver");
    response.status()
}

fn v1_label(name: &str, value: &str) -> pb1::Label {
    pb1::Label { name: name.to_string(), value: value.to_string() }
}

/// One 1.0 `TimeSeries`: `__name__` plus one ordinary label, carrying a single sample.
fn v1_series(name: &str, value: f64, timestamp_ms: i64) -> pb1::TimeSeries {
    pb1::TimeSeries {
        labels: vec![v1_label("__name__", name), v1_label("job", "fixture")],
        samples: vec![pb1::Sample { value, timestamp: timestamp_ms }],
        ..Default::default()
    }
}

/// Prometheus's own `metadata_config` request: one `MetricMetadata`, no series at all.
fn latency_seconds_metadata_request() -> pb1::WriteRequest {
    pb1::WriteRequest {
        timeseries: Vec::new(),
        metadata: vec![pb1::MetricMetadata {
            r#type: pb1::metric_metadata::MetricType::Histogram as i32,
            metric_family_name: "latency_seconds".to_string(),
            help: "Request latency.".to_string(),
            unit: "seconds".to_string(),
        }],
    }
}

/// The flat samples one scrape of that histogram produces, and nothing else -- one shared builder
/// so the cache test and its no-cache control are demonstrably the same bytes, rather than two
/// similar-looking literals.
fn latency_seconds_samples_request() -> pb1::WriteRequest {
    pb1::WriteRequest {
        timeseries: vec![
            pb1::TimeSeries {
                labels: vec![
                    v1_label("__name__", "latency_seconds_bucket"),
                    v1_label("job", "fixture"),
                    v1_label("le", "+Inf"),
                ],
                samples: vec![pb1::Sample { value: 4.0, timestamp: 1_700_000_000_000 }],
                ..Default::default()
            },
            v1_series("latency_seconds_sum", 2.5, 1_700_000_000_000),
            v1_series("latency_seconds_count", 4.0, 1_700_000_000_000),
        ],
        metadata: Vec::new(),
    }
}

/// The W5 metadata cache doing the one job it exists for: a real Prometheus 1.0 sender ships
/// `MetricMetadata` in requests of its own (`metadata_config.send_interval`, a minute by default)
/// rather than attached to the samples it describes, so the sample-only requests that follow carry
/// no `# TYPE` equivalent anywhere. Stateless, they decode as three unrelated `Unknown` series;
/// against the cache they reassemble into the one `Histogram` the sender meant.
#[tokio::test]
async fn a_metadata_only_1_0_request_types_the_samples_that_follow_it() {
    let (receiver_addr, mut rx) =
        start_receiver(16, Some((10_000, Duration::from_secs(600)))).await;

    // Request one: metadata, no series at all -- Prometheus's own `metadata_config` request.
    assert_eq!(post_v1(receiver_addr, &latency_seconds_metadata_request()).await, 204);
    // Request two: the flat samples one scrape of that histogram produces, and nothing else.
    assert_eq!(post_v1(receiver_addr, &latency_seconds_samples_request()).await, 204);

    // The metadata-only request declares a family and carries no group, so it produces no events
    // at all -- only the samples request delivers a batch.
    let received = collect_batches(&mut rx, 1).await;
    let records: Vec<&MetricRecord> = received
        .iter()
        .flat_map(|batch| batch.events.iter())
        .flat_map(|e| e.metrics.iter())
        .collect();
    assert_eq!(
        records.len(),
        1,
        "the three flat series must reassemble into one histogram record, got {records:?}"
    );
    assert!(
        matches!(records[0].kind, MetricKind::Histogram(_)),
        "the cached `# TYPE latency_seconds histogram` must type the samples, got {:?}",
        records[0].kind
    );
    assert_eq!(logit_core::interner::resolve(records[0].name), "latency_seconds");
}

/// **Exactly** the two requests above against a receiver with no cache at all, so the difference
/// the cache makes is asserted rather than assumed. `start_receiver`'s `None` is what an operator
/// spells `metadata_cache: {max_families: 0}`: `PrometheusReceiver::with_metadata_cache` turns a
/// zero cap into no table rather than an empty one, so the two are the same receiver and this is
/// the control for [`a_metadata_only_1_0_request_types_the_samples_that_follow_it`], not a
/// near-miss.
///
/// Stateless, the three flat series stay three unrelated untyped records -- and **all three**
/// arrive. What the cache buys is typing, never samples: the request is not lossier without it,
/// only flatter (`crates/logit-proto/src/prometheus/assemble.rs`'s "Only a declared base claims a
/// suffix").
#[tokio::test]
async fn without_the_cache_the_same_samples_stay_three_untyped_series() {
    let (receiver_addr, mut rx) = start_receiver(16, None).await;

    assert_eq!(post_v1(receiver_addr, &latency_seconds_metadata_request()).await, 204);
    assert_eq!(post_v1(receiver_addr, &latency_seconds_samples_request()).await, 204);

    let received = collect_batches(&mut rx, 1).await;
    let records: Vec<&MetricRecord> = received
        .iter()
        .flat_map(|batch| batch.events.iter())
        .flat_map(|e| e.metrics.iter())
        .collect();
    assert_eq!(
        records.len(),
        3,
        "stateless, the three series stay separate -- and none of them is dropped: {records:?}"
    );
    let mut names: Vec<&str> =
        records.iter().map(|record| logit_core::interner::resolve(record.name)).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        ["latency_seconds_bucket", "latency_seconds_count", "latency_seconds_sum"],
        "each suffixed name is its own untyped family with no metadata to attach it to"
    );
    for record in records {
        assert!(
            matches!(record.kind, MetricKind::Gauge(_)),
            "an untyped remote-write series decodes as a bare gauge, got {:?}",
            record.kind
        );
    }
}

// -------------------------------------------------------------------------------------------------
// Backpressure
// -------------------------------------------------------------------------------------------------

/// The ADR's response table promises the batch reaches the `Fanout` **before** the `204` is built,
/// so a stalled downstream throttles the sender instead of being acknowledged and dropped. With a
/// one-slot channel that nothing is reading, the second `send` cannot complete; once the queue is
/// drained it completes, and exactly the two batches that were written arrive -- no duplicate from
/// a sink-level retry, because the sink does not retry (one `send` is one attempt; retry is
/// `write_loop`'s job, and this test is what makes "does not duplicate" a fact about the sink
/// rather than an assumption).
#[tokio::test]
async fn a_stalled_downstream_delays_the_204_without_duplicating_a_write() {
    let (receiver_addr, mut rx) = start_receiver(1, None).await;

    let batch_of = |name: &str, value: f64| EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![Event::metric(
            1_700_000_000_000_000_000,
            AttrMap::new(),
            MetricRecord::new(intern(name), MetricKind::Gauge(value)),
        )],
    };
    let first = batch_of("first", 1.0);
    let second = batch_of("second", 2.0);

    let writer = tokio::spawn(async move {
        let mut sender = remote_write_sender(receiver_addr, Version::V2);
        sender.send(&first).await.expect("the first write is accepted");
        sender.send(&second).await.expect("the second write is accepted once the queue drains");
    });

    // The one-slot channel takes the first batch and nothing takes it out, so the second request's
    // `sink.send` blocks inside the handler and its `204` never arrives.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!writer.is_finished(), "the second write must still be waiting on its 204");

    let received = collect_batches(&mut rx, 2).await;
    tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .expect("the writer finishes once the queue drains")
        .expect("neither write should fail");

    let names: Vec<String> = received
        .iter()
        .flat_map(|batch| batch.events.iter())
        .flat_map(|event| event.metrics.iter())
        .map(|record| logit_core::interner::resolve(record.name).to_string())
        .collect();
    assert_eq!(names, vec!["first".to_string(), "second".to_string()]);

    // Nothing more: a delayed acknowledgement is not a failed one, so the sink neither retried nor
    // re-sent anything.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(),
        "a delayed 204 must not produce a duplicate write"
    );
}
