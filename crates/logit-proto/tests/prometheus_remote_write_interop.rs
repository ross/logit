//! Recorded interop fixtures: real remote-write requests a real Prometheus 3.14.0 `POST`ed,
//! replayed through `logit_proto::prometheus::remote_write::decode`.
//!
//! **Real bytes from a real producer** -- not this codec's own encoder, which will happily agree
//! with itself even where both sides share a misreading of the spec. `remote_write.rs` was written
//! from the two remote-write specs and the vendored `prompb`; these fixtures are what checks that
//! reading against the sender every deployment actually runs. See
//! `testdata/interop/prometheus/README.md` for the provenance table and
//! `testdata/interop/README.md` for why this corpus exists at all; regenerate with
//! `script/record-fixtures prometheus`.
//!
//! This is the one interop corpus replayed at the **codec** entry point rather than through the
//! input component, unlike `crates/logit-inputs/src/{collectd,graphite,syslog}.rs`'s own
//! `interop_fixture_*` tests. Two reasons: a remote-write body is only half of what arrived -- the
//! wire version lives in the request's `Content-Type`, which the capture recorded in a `.headers`
//! sidecar beside each body -- and `prometheus_in`'s receiver is a thin HTTP shell over exactly
//! this call, with `crates/logit-cli/tests/prometheus_remote_write_round_trip.rs` already making
//! the socket-level statement about that component.
//!
//! Assertions are on **decoded, identifiable values** -- family names, types, series counts, what a
//! request declared -- never on the fixture's bytes, which change on every re-record (timestamps,
//! whatever Prometheus happened to have measured). That is `testdata/interop/README.md`'s
//! "Consuming these fixtures" rule.
//!
//! ## What this corpus turned up
//!
//! Two things worth knowing before reading the assertions, both recorded in the provenance README
//! as well:
//!
//! - **Prometheus 3.14.0's 1.0 sender attaches no metadata to a sample request at all.** Every
//!   `prometheus-v1-*` sample capture decodes with zero declarations; the types arrive in
//!   `prometheus-v1-metadata-*`, separate requests on `metadata_config.send_interval`'s own
//!   ticker. That is precisely the case `prometheus_in`'s `metadata_cache:` exists for, and
//!   [`recorded_metadata_types_a_recorded_sample_request`] is it, end to end, on real bytes.
//! - **Its 2.0 sender attaches an *empty* `Metadata` to every series** -- `type: UNSPECIFIED`, no
//!   help or unit reference -- in this topology. That held with and without
//!   `--enable-feature=metadata-wal-records`, on the first request of a process's life and on its
//!   tenth, with and without `write_relabel_configs`. So the 2.0 captures here do **not** exercise
//!   the inline-metadata path; `prometheus_remote_write_fixed_point.rs` and the round-trip test
//!   cover that against `logit`'s own sender, which does populate it.

use logit_core::Registry;
use logit_proto::prometheus::remote_write::{decode_with, Declarations, Version};
use logit_proto::prometheus::{FamilyType, MetricFamily, PrometheusDecoder};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn interop_dir() -> PathBuf {
    // `crates/logit-proto/` -> repository root.
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/prometheus")
}

/// One recorded request: the captured body, and the header sidecar recorded beside it.
struct Capture {
    name: String,
    body: Vec<u8>,
    headers: BTreeMap<String, String>,
}

impl Capture {
    /// The wire version this request announced, read off the capture rather than assumed -- which
    /// is the whole reason the `.headers` sidecar is part of the fixture.
    fn version(&self) -> Version {
        let content_type = self
            .headers
            .get("content-type")
            .unwrap_or_else(|| panic!("{}: the capture recorded no Content-Type", self.name));
        Version::from_content_type(content_type).unwrap_or_else(|| {
            panic!("{}: unrecognised remote-write Content-Type {content_type:?}", self.name)
        })
    }

    /// Snappy **block** decompression -- what both specs mean by `Content-Encoding: snappy`, and
    /// the only transformation this test performs on a captured body.
    fn decompressed(&self) -> Vec<u8> {
        assert_eq!(
            self.headers.get("content-encoding").map(String::as_str),
            Some("snappy"),
            "{}: every remote-write request is snappy-compressed",
            self.name
        );
        snap::raw::Decoder::new().decompress_vec(&self.body).unwrap_or_else(|e| {
            panic!("{}: a real Prometheus body must decompress: {e}", self.name)
        })
    }
}

fn read_capture(name: &str) -> Capture {
    let dir = interop_dir();
    let body = std::fs::read(dir.join(format!("{name}.bin")))
        .unwrap_or_else(|e| panic!("reading interop fixture {name}.bin: {e}"));
    let raw = std::fs::read_to_string(dir.join(format!("{name}.headers")))
        .unwrap_or_else(|e| panic!("reading interop fixture {name}.headers: {e}"));
    let headers = raw
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    Capture { name: name.to_string(), body, headers }
}

/// What one capture decoded to: its groups flattened into one family list, what the request itself
/// declared, and every `logit.input.metrics.{skipped,degraded}` reason the decode recorded. A
/// conforming sender's request should produce none of the last: a reason there means this codec
/// dropped or downgraded something Prometheus genuinely sent.
struct Replayed {
    families: Vec<MetricFamily>,
    declarations: Declarations,
    reasons: Vec<String>,
}

fn replay(capture: &Capture) -> Replayed {
    replay_with(capture, &Declarations::default())
}

fn replay_with(capture: &Capture, seed: &Declarations) -> Replayed {
    let registry = Registry::new();
    let mut decoder = PrometheusDecoder::new().with_telemetry(registry.telemetry_for(
        "prometheus",
        "prometheus_in",
        "source",
    ));
    let decoded = decode_with(&capture.decompressed(), capture.version(), &mut decoder, seed)
        .unwrap_or_else(|e| panic!("{}: a real Prometheus request must decode: {e}", capture.name));

    let reasons = registry
        .drain(0)
        .iter()
        .filter(|event| {
            event.metrics.iter().any(|metric| {
                let name = logit_core::interner::resolve(metric.name);
                name == "logit.input.metrics.skipped" || name == "logit.input.metrics.degraded"
            })
        })
        .filter_map(|event| {
            event.attributes.get("reason").and_then(logit_core::Value::as_str).map(str::to_owned)
        })
        .collect();

    Replayed {
        families: decoded.groups.into_iter().flatten().collect(),
        declarations: decoded.declarations,
        reasons,
    }
}

/// Every capture `script/record-fixtures prometheus` writes, split the way the provenance README
/// splits them: two 1.0 sample requests, three 1.0 metadata-only requests, two 2.0 sample requests.
const V1_SAMPLES: [&str; 2] = ["prometheus-v1-000", "prometheus-v1-001"];
const V1_METADATA: [&str; 3] =
    ["prometheus-v1-metadata-000", "prometheus-v1-metadata-001", "prometheus-v1-metadata-002"];
const V2_SAMPLES: [&str; 2] = ["prometheus-v2-000", "prometheus-v2-001"];

fn all_captures() -> Vec<&'static str> {
    V1_SAMPLES.iter().chain(V1_METADATA.iter()).chain(V2_SAMPLES.iter()).copied().collect()
}

/// What `tools/record-fixtures/prometheus.yml`'s `write_relabel_configs` lets onto the wire, as the
/// **decoder** sees it with no metadata to type it by: four families in Prometheus's own
/// exposition -- a gauge, a counter, a histogram and a summary -- arriving as eight flat, untyped
/// ones, because a remote-write series is a label set and a number and nothing else.
///
/// Every suffixed name becomes a family of its own (the histogram's three, the summary's two) and
/// the summary's `quantile` and the histogram's `le` stay ordinary labels: exactly the "the model
/// kinds are flatter than the producer's" paragraph in [ADR `prometheus-remote-write`], stated
/// against a real sender rather than against this codec's own encoder. **Flatter, and nothing
/// else** -- every sample the request carried is in one of these eight, which is what that
/// paragraph's "Nothing is lost" means and what the assembler's "only a *declared* base claims a
/// suffix" rule is there to guarantee.
///
/// [ADR `prometheus-remote-write`]: ../../../docs/adr/prometheus-remote-write.md
const FLAT_SAMPLE_FAMILIES: [&str; 8] = [
    "go_gc_duration_seconds",
    "go_gc_duration_seconds_count",
    "go_gc_duration_seconds_sum",
    "prometheus_build_info",
    "prometheus_tsdb_compaction_duration_seconds_bucket",
    "prometheus_tsdb_compaction_duration_seconds_count",
    "prometheus_tsdb_compaction_duration_seconds_sum",
    "prometheus_tsdb_wal_page_flushes_total",
];

/// Every capture decodes -- nothing `Malformed`, nothing rejected -- and **nothing is skipped or
/// degraded on the way**, which is the ADR's own claim about a metadata-less request: the model
/// kinds come out flatter than the producer's, and that is the whole of it.
///
/// This assertion is the one the corpus was worth having for. Written against the codec as it
/// stood when these fixtures were recorded, it read `["unknown_suffix"]` for every sample capture:
/// with no metadata, `go_gc_duration_seconds{quantile=..}` opened the implicit family
/// `go_gc_duration_seconds` (remote-write sorts its series by name, so the bare one always arrives
/// first), and `go_gc_duration_seconds_sum`/`_count` were then matched against that implicit base
/// by the suffix scan and thrown away, because no suffix has a role under an untyped family. Two
/// real samples per scrape, from a real Prometheus. The fix is in the assembler -- only a
/// *declared* base claims a suffix -- and this test is what holds it.
#[test]
fn every_recorded_request_decodes_with_nothing_skipped_or_degraded() {
    for name in all_captures() {
        let capture = read_capture(name);
        let replayed = replay(&capture);
        assert_eq!(
            replayed.reasons,
            Vec::<String>::new(),
            "{name}: a real Prometheus request must decode with nothing skipped or degraded -- \
             see this test's own doc for the bug this corpus found"
        );
        assert!(
            !replayed.families.is_empty() || !replayed.declarations.is_empty(),
            "{name}: a capture carries either samples or metadata, never neither"
        );
        assert_eq!(capture.headers.get("method").map(String::as_str), Some("POST"), "{name}");
        assert_eq!(
            capture.headers.get("path").map(String::as_str),
            Some("/api/v1/write"),
            "{name}: the capture records the receiver route prometheus_in's `path:` defaults to"
        );
        assert_eq!(
            capture.headers.get("user-agent").map(String::as_str),
            Some("Prometheus/3.14.0"),
            "{name}: the provenance table's version and the capture must agree"
        );
    }
}

/// The wire version comes from the request's own `Content-Type`, with nothing configured -- which
/// is exactly what `prometheus_in`'s receiver does, per request, on one `bind:`.
#[test]
fn the_recorded_content_types_select_the_wire_version() {
    for name in V1_SAMPLES.iter().chain(V1_METADATA.iter()) {
        assert_eq!(read_capture(name).version(), Version::V1, "{name}");
    }
    for name in V2_SAMPLES {
        assert_eq!(read_capture(name).version(), Version::V2, "{name}");
    }
}

/// Both versions' sample requests decode to the same eight flat, untyped families -- the wire
/// carries a label set and a number, and neither Prometheus 3.14.0 sender attached usable metadata
/// to these requests (see this module's doc). A summary's `quantile` and a histogram's `le` survive
/// as ordinary labels rather than being reassembled, and each suffixed name is its own family,
/// because nothing in the request says which family any of them belongs to.
#[test]
fn a_recorded_sample_request_decodes_to_flat_untyped_families() {
    for name in V1_SAMPLES.iter().chain(V2_SAMPLES.iter()) {
        let replayed = replay(&read_capture(name));
        let names: Vec<&str> =
            replayed.families.iter().map(|family| family.name.as_str()).collect();
        assert_eq!(names, FLAT_SAMPLE_FAMILIES, "{name}");
        for family in &replayed.families {
            assert_eq!(
                family.kind,
                FamilyType::Unknown,
                "{name}/{}: a series with no metadata behind it is untyped",
                family.name
            );
        }

        let quantiles: BTreeSet<&str> = replayed
            .families
            .iter()
            .find(|family| family.name == "go_gc_duration_seconds")
            .expect("the summary family is on the wire")
            .series
            .iter()
            .flat_map(|series| series.labels.iter())
            .filter(|(key, _)| key == "quantile")
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(
            quantiles.len(),
            5,
            "{name}: go_gc_duration_seconds' five quantiles stay ordinary labels, got {quantiles:?}"
        );
    }
}

/// `instance`, `job` and Prometheus's own `external_labels` arrive as ordinary labels on every
/// series and stay that way -- the ADR's "Labels stay labels" decision, checked against a real
/// sender. A receiver never touched the target these name, so promoting one to resource identity
/// would be inventing structure the wire did not carry.
#[test]
fn a_recorded_sample_request_carries_scrape_identity_as_ordinary_labels() {
    for name in V1_SAMPLES.iter().chain(V2_SAMPLES.iter()) {
        let replayed = replay(&read_capture(name));
        for series in replayed.families.iter().flat_map(|family| family.series.iter()) {
            let labels: BTreeMap<&str, &str> =
                series.labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            assert_eq!(
                labels.get("monitor").copied(),
                Some("logit-fixture"),
                "{name}: tools/record-fixtures/prometheus.yml sets external_labels.monitor"
            );
            assert_eq!(labels.get("job").copied(), Some("prometheus"), "{name}");
            assert_eq!(labels.get("instance").copied(), Some("localhost:9090"), "{name}");
        }
    }
}

/// The 1.0 shape the metadata cache exists for, recorded from the sender that produces it: a
/// request that is metadata and nothing else. Decoded stateless it yields declarations and **no
/// groups at all** -- there is nothing in it to turn into an event.
#[test]
fn a_recorded_1_0_metadata_request_declares_families_with_no_groups() {
    let mut kinds = BTreeSet::new();
    for name in V1_METADATA {
        let replayed = replay(&read_capture(name));
        assert!(
            replayed.families.is_empty(),
            "{name}: a metadata-only request carries no samples, so it decodes to no groups"
        );
        assert!(
            !replayed.declarations.is_empty(),
            "{name}: a metadata-only request is nothing but declarations"
        );
        for (family, declaration) in replayed.declarations.iter() {
            assert_ne!(
                declaration.kind,
                FamilyType::Unknown,
                "{name}/{family}: an UNKNOWN metadata entry is not a declaration and must not \
                 reach the table"
            );
            kinds.insert(format!("{:?}", declaration.kind));
        }
    }
    assert!(
        kinds.len() > 1,
        "the metadata captures should carry more than one metric type between them, got {kinds:?}"
    );
}

/// The whole mechanism, on real bytes from both halves of one real sender: the declarations a
/// recorded **metadata-only** request reported, used as the seed for a recorded **sample** request
/// that carries none of its own, fold eight flat untyped families back into the four Prometheus
/// meant.
///
/// What the cache buys is **typing, never samples**. Stateless, the same request already decodes
/// losslessly ([`every_recorded_request_decodes_with_nothing_skipped_or_degraded`]) -- it just
/// decodes to eight flat families where the producer had four, with `quantile` and `le` sitting on
/// the label set as ordinary labels and `_sum`/`_count` standing alone. The seed is what collapses
/// them, gives each family its type, and moves `quantile`/`le` into the point.
///
/// The overlap between the two halves is guaranteed by the recipe rather than left to luck: the
/// metadata capture scrapes a small static target declaring exactly these four families
/// (`tools/record-fixtures/prometheus-metadata-target.prom`), because metadata is not subject to
/// `write_relabel_configs` and a self-scrape would put several hundred families behind
/// `metadata_config`'s ticker in map order. That is also why all three metadata captures fit in
/// ~400 bytes each.
#[test]
fn recorded_metadata_types_a_recorded_sample_request() {
    let mut seed = Declarations::default();
    for name in V1_METADATA {
        for (family, declaration) in replay(&read_capture(name)).declarations.iter() {
            seed.insert(
                family,
                declaration.kind,
                declaration.help.clone(),
                declaration.unit.clone(),
            );
        }
    }

    let capture = read_capture(V1_SAMPLES[0]);
    let seeded = replay_with(&capture, &seed);

    // Still nothing skipped -- the stateless decode loses nothing either, so a seed that changed
    // that in *either* direction would be the thing to notice.
    assert_eq!(seeded.reasons, Vec::<String>::new(), "typing a request must not cost it a sample");

    let typed: BTreeMap<&str, FamilyType> =
        seeded.families.iter().map(|family| (family.name.as_str(), family.kind)).collect();
    // Eight flat families became the producer's four, each with the type Prometheus gives it: the
    // suffixed names are gone, folded into the families they were always parts of.
    assert_eq!(
        typed,
        BTreeMap::from([
            ("go_gc_duration_seconds", FamilyType::Summary),
            ("prometheus_build_info", FamilyType::Gauge),
            ("prometheus_tsdb_compaction_duration_seconds", FamilyType::Histogram),
            ("prometheus_tsdb_wal_page_flushes_total", FamilyType::Counter),
        ]),
        "the recorded metadata should type every family the recorded samples carry"
    );
    assert!(
        seeded.families.len() < FLAT_SAMPLE_FAMILIES.len(),
        "the seed folds families together; it never adds any"
    );

    // The summary really did reassemble, rather than merely being relabelled: its quantiles are
    // part of the point now, not leftover labels.
    let summary = seeded
        .families
        .iter()
        .find(|family| family.name == "go_gc_duration_seconds")
        .expect("the summary family is on the wire");
    assert_eq!(summary.series.len(), 1, "five quantiles are one series, not five");
    assert!(
        summary.series[0].labels.iter().all(|(key, _)| key != "quantile"),
        "`quantile` is consumed into the point, not left on the label set: {:?}",
        summary.series[0].labels
    );

    // And the histogram, whose flat form is the `_bucket`/`_sum`/`_count` trio the ADR's own prose
    // uses as the example of what comes apart without a cache.
    let histogram = seeded
        .families
        .iter()
        .find(|family| family.name == "prometheus_tsdb_compaction_duration_seconds")
        .expect("the histogram family is on the wire");
    assert_eq!(histogram.series.len(), 1, "fifteen buckets plus sum and count are one series");
    assert!(
        histogram.series[0].labels.iter().all(|(key, _)| key != "le"),
        "`le` is consumed into the point: {:?}",
        histogram.series[0].labels
    );

    // A family the seed says nothing about is untouched by it -- the seed types what it knows and
    // stays out of the way otherwise.
    let mut narrow = Declarations::default();
    narrow.insert("go_gc_duration_seconds", FamilyType::Summary, None, None);
    let partial = replay_with(&capture, &narrow);
    let partial_typed: BTreeMap<&str, FamilyType> =
        partial.families.iter().map(|family| (family.name.as_str(), family.kind)).collect();
    assert_eq!(partial_typed.get("go_gc_duration_seconds").copied(), Some(FamilyType::Summary));
    assert_eq!(
        partial_typed.get("prometheus_build_info").copied(),
        Some(FamilyType::Unknown),
        "nothing declares prometheus_build_info here, so it stays untyped"
    );
    assert_eq!(partial.reasons, Vec::<String>::new(), "a partial seed still loses nothing");
}
