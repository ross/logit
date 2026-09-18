//! Pure-codec Prometheus **remote-write** fixed-point tests, the sibling of
//! `prometheus_fixed_point.rs`: [ADR `lossless-transit`]'s "round-trip fixed point is the test that
//! proves this" requirement, exercised directly against
//! `logit_proto::prometheus::remote_write` with no pipeline, sink, or HTTP in between.
//!
//! The properties, over both wire versions:
//!
//! 1. **`decode(encode(groups)) == groups`** over a `proptest` generator covering every [`Point`]
//!    variant including `Stale`, `created`, exemplars, and several timestamp groups with
//!    overlapping series -- the remote-write analogue of that file's property 3, stated on the
//!    family list rather than on bytes (protobuf has no canonical-bytes question to ask: field
//!    order and varint widths are prost's, not this codec's).
//! 2. **the exposition corpus survives a remote-write detour**: the same fixture bodies
//!    `prometheus_fixed_point.rs` uses, parsed to families, sent through `encode`/`decode`, and
//!    written back out equal to what writing them directly produces.
//! 3. hand-built wire cases for the things a round trip cannot reach on its own: a 1.0 request
//!    with no metadata at all, the 2.0 symbol-table errors, the label-validation skips, the label
//!    sort order, and the stale NaN.
//!
//! **What the generators deliberately leave out**, for the reasons `prometheus_fixed_point.rs`'s
//! own module doc gives, plus two this transport adds:
//!
//! - `NaN` as an ordinary sample value (`f64::NAN != f64::NAN`, so `PartialEq` cannot express the
//!   round trip). The *stale* NaN is a different question -- it is a bit pattern, not a value --
//!   and is generated and asserted.
//! - sub-millisecond timestamps: both versions carry milliseconds, which is a named normalization.
//! - a label value that is empty, or a label name that is not strictly ascending: both specs forbid
//!   a sender from producing either, and this codec drops them on the way out and skips them on the
//!   way in. Their own hand-built tests below cover that.
//! - more than one exemplar per series, and exemplars on anything but a counter: an exemplar hangs
//!   off a `TimeSeries` rather than a sample in both versions, so a series with several timestamps
//!   cannot say which sample an exemplar came from. Hand-built tests cover placement instead.
//!
//! [ADR `lossless-transit`]: ../../../docs/adr/lossless-transit.md

use logit_core::interner::resolve;
use logit_core::{AttrMap, Exemplar, MetricKind, Registry, TraceRef, Value};
use logit_proto::prometheus::generated::io::prometheus::write::v2 as pb2;
use logit_proto::prometheus::generated::prometheus as pb1;
use logit_proto::prometheus::remote_write::{decode, encode, wire_samples, Decoded, Version};
use logit_proto::prometheus::text::{parse, write, Dialect};
use logit_proto::prometheus::{
    is_stale_nan, FamilyType, MetricFamily, Point, PrometheusDecoder, PrometheusEncoder, Series,
    STALE_NAN_BITS,
};
use proptest::prelude::*;
use prost::Message;

/// A whole number of milliseconds, which is the only resolution either version carries.
const TIMESTAMP: i64 = 1_605_281_325_000_000_000;

fn round_trip(groups: &[Vec<MetricFamily>], version: Version) -> Decoded {
    let body = encode(groups, version, &mut PrometheusEncoder::new());
    decode(&body, version, &mut PrometheusDecoder::new()).expect("our own encoding must decode")
}

/// Decodes with telemetry attached and returns every `logit.input.metrics.{skipped,degraded}`
/// reason it recorded. `round_trip` compares families only, which cannot see a decoder that reaches
/// the right answer while counting input it did not drop -- a counter an operator reads as data
/// loss.
fn decode_reasons(body: &[u8], version: Version) -> (Decoded, Vec<(String, u64)>) {
    let registry = Registry::new();
    let mut decoder = PrometheusDecoder::new().with_telemetry(registry.telemetry_for(
        "prometheus",
        "prometheus_in",
        "source",
    ));
    let decoded = decode(body, version, &mut decoder).expect("must decode");
    let reasons = reasons_from(&registry, "logit.input.metrics.");
    (decoded, reasons)
}

/// Every `reason` a drained registry recorded against a counter whose name starts with `prefix`,
/// **with its value** and sorted by reason, so an assertion pins how much was counted rather than
/// only that something was. The registry aggregates repeats of one `(metric, reason)` into a single
/// point, so two dropped exemplars are one entry valued `2`, never two entries.
fn reasons_from(registry: &Registry, prefix: &str) -> Vec<(String, u64)> {
    let mut reasons: Vec<(String, u64)> = registry
        .drain(0)
        .iter()
        .filter_map(|event| {
            let value = event.metrics.iter().find_map(|metric| {
                if !resolve(metric.name).starts_with(prefix) {
                    return None;
                }
                Some(match &metric.kind {
                    MetricKind::Sum(sum) => sum.value,
                    MetricKind::Gauge(value) => *value,
                    _ => 0.0,
                })
            })?;
            let reason = event.attributes.get("reason").and_then(|value| value.as_str())?;
            Some((reason.to_string(), value as u64))
        })
        .collect();
    reasons.sort();
    reasons
}

// -------------------------------------------------------------------------------------------------
// Property 2: the exposition corpus survives a remote-write detour
// -------------------------------------------------------------------------------------------------

/// Every text 0.0.4 feature in one body -- the same fixture `prometheus_fixed_point.rs` uses, minus
/// its untimestamped-series variety, which this transport has no way to express (see the module
/// doc). Timestamps are stamped onto every series below rather than written into the fixture.
const TEXT_FIXTURE: &str = concat!(
    "# HELP requests_total Total requests.\n",
    "# TYPE requests_total counter\n",
    "requests_total{code=\"200\",handler=\"/\"} 1027\n",
    "requests_total{code=\"500\",handler=\"/\"} 3\n",
    "# TYPE temperature_celsius gauge\n",
    "temperature_celsius{room=\"kitchen\"} 21.5\n",
    "temperature_celsius{room=\"attic\"} -Inf\n",
    "# TYPE latency_seconds histogram\n",
    "latency_seconds_bucket{le=\"0.1\"} 1\n",
    "latency_seconds_bucket{le=\"1\"} 3\n",
    "latency_seconds_bucket{le=\"+Inf\"} 4\n",
    "latency_seconds_sum 2.5\n",
    "latency_seconds_count 4\n",
    "# TYPE rpc_seconds summary\n",
    "rpc_seconds{quantile=\"0.5\"} 0.2\n",
    "rpc_seconds{quantile=\"0.99\"} 0.9\n",
    "rpc_seconds_sum 12.5\n",
    "rpc_seconds_count 100\n",
    "# TYPE build_info untyped\n",
    "build_info{version=\"1.2.3\"} 1\n",
    "bare_metric 42\n",
    "escaped{path=\"C:\\\\tmp\",note=\"line\\nbreak \\\"quoted\\\"\"} 1\n",
    "weird{problem=\"division by zero\"} +Inf\n",
);

/// Every OpenMetrics 1.0 feature in one body: the four types text 0.0.4 doesn't have, `# UNIT`,
/// `_created`, exemplars on a `_total` and on a `_bucket`, and `# EOF`.
const OPENMETRICS_FIXTURE: &str = concat!(
    "# TYPE request_duration_seconds counter\n",
    "# UNIT request_duration_seconds seconds\n",
    "# HELP request_duration_seconds Total request duration.\n",
    "request_duration_seconds_total{code=\"200\"} 1027\n",
    "request_duration_seconds_created{code=\"200\"} 1605281325.123\n",
    "request_duration_seconds_total{code=\"500\"} 3 # {trace_id=\"0123456789abcdef0123456789abcdef\",\
     span_id=\"fedcba9876543210\"} 0.5 1605281325.5\n",
    "# TYPE temperature_celsius gauge\n",
    "temperature_celsius{room=\"kitchen\"} 21.5\n",
    "# TYPE latency_seconds histogram\n",
    "# UNIT latency_seconds seconds\n",
    "latency_seconds_bucket{le=\"0.1\"} 1 # {slow=\"no\"} 0.05\n",
    "latency_seconds_bucket{le=\"1\"} 3\n",
    "latency_seconds_bucket{le=\"+Inf\"} 4\n",
    "latency_seconds_sum 2.5\n",
    "latency_seconds_count 4\n",
    "latency_seconds_created 1605281325\n",
    "# TYPE sizes gaugehistogram\n",
    "sizes_bucket{le=\"10\"} 5\n",
    "sizes_bucket{le=\"+Inf\"} 7\n",
    "sizes_gsum 300.5\n",
    "sizes_gcount 7\n",
    "# TYPE rpc_seconds summary\n",
    "rpc_seconds{quantile=\"0.5\"} 0.2\n",
    "rpc_seconds_sum 12.5\n",
    "rpc_seconds_count 100\n",
    "rpc_seconds_created 1605281325\n",
    "# TYPE build info\n",
    "# HELP build Build metadata.\n",
    "build_info{version=\"1.2.3\"} 1\n",
    "# TYPE state stateset\n",
    "state{state=\"starting\"} 0\n",
    "state{state=\"running\"} 1\n",
    "# TYPE unannotated unknown\n",
    "unannotated 42\n",
    "# EOF\n",
);

/// Every series gets the same timestamp -- remote-write has no way to omit one -- and, for 1.0,
/// loses its `_created`, which that version has no field for.
fn stamped(mut families: Vec<MetricFamily>, version: Version) -> Vec<MetricFamily> {
    for family in &mut families {
        for series in &mut family.series {
            series.timestamp = Some(TIMESTAMP);
            if version == Version::V1 {
                series.created = None;
            }
        }
    }
    families
}

fn rendered(families: &[MetricFamily], dialect: Dialect) -> String {
    let mut out = Vec::new();
    write(families, dialect, &mut out);
    String::from_utf8(out).expect("exposition must be utf-8")
}

/// Parse a fixture, take it through remote-write, and write it back out: the bytes must equal what
/// writing the parsed families directly produces. Stated on the rendered exposition rather than on
/// the family list because one normalization is invisible there and load-bearing here -- neither
/// version has a spelling for text 0.0.4's `untyped`, so an `untyped` family comes back `unknown`,
/// and both write as `# TYPE x untyped` in text 0.0.4.
fn assert_corpus_survives(body: &str, dialect: Dialect, version: Version) {
    let families = stamped(parse(body.as_bytes(), dialect).expect("fixture must parse"), version);
    let direct = rendered(&families, dialect);

    let decoded = round_trip(std::slice::from_ref(&families), version);
    assert_eq!(decoded.groups.len(), 1, "one timestamp, one group");
    assert_eq!(
        rendered(&decoded.groups[0], dialect),
        direct,
        "{version:?} {dialect:?} changed the exposition"
    );
}

#[test]
fn the_text_0_0_4_corpus_survives_a_remote_write_detour() {
    assert_corpus_survives(TEXT_FIXTURE, Dialect::Text0_0_4, Version::V1);
    assert_corpus_survives(TEXT_FIXTURE, Dialect::Text0_0_4, Version::V2);
}

#[test]
fn the_openmetrics_corpus_survives_a_remote_write_detour() {
    assert_corpus_survives(OPENMETRICS_FIXTURE, Dialect::OpenMetrics1_0, Version::V1);
    assert_corpus_survives(OPENMETRICS_FIXTURE, Dialect::OpenMetrics1_0, Version::V2);
}

/// 2.0 carries a created timestamp and 1.0 does not, so the OpenMetrics corpus' `_created` lines
/// survive the 2.0 detour and are the one thing 1.0 drops.
#[test]
fn version_2_keeps_created_timestamps_and_version_1_drops_them() {
    let families =
        parse(OPENMETRICS_FIXTURE.as_bytes(), Dialect::OpenMetrics1_0).expect("must parse");
    let created: Vec<Option<i64>> = families
        .iter()
        .flat_map(|family| family.series.iter().map(|series| series.created))
        .collect();
    assert!(created.iter().any(Option::is_some), "the fixture must carry some `_created`");

    let two = round_trip(&[stamped(families.clone(), Version::V2)], Version::V2);
    assert_eq!(
        two.groups[0]
            .iter()
            .flat_map(|family| family.series.iter().map(|series| series.created))
            .collect::<Vec<_>>(),
        created
    );

    let one = round_trip(&[stamped(families, Version::V1)], Version::V1);
    assert!(one.groups[0]
        .iter()
        .flat_map(|family| family.series.iter())
        .all(|series| series.created.is_none()));
}

// -------------------------------------------------------------------------------------------------
// Property 3: hand-built wire cases
// -------------------------------------------------------------------------------------------------

fn label(name: &str, value: &str) -> pb1::Label {
    pb1::Label { name: name.to_string(), value: value.to_string() }
}

fn v1_series(labels: &[(&str, &str)], value: f64) -> pb1::TimeSeries {
    pb1::TimeSeries {
        labels: labels.iter().map(|(name, value)| label(name, value)).collect(),
        samples: vec![pb1::Sample { value, timestamp: 1_605_281_325_000 }],
        ..Default::default()
    }
}

fn decode_v1(request: pb1::WriteRequest) -> Decoded {
    decode(&request.encode_to_vec(), Version::V1, &mut PrometheusDecoder::new())
        .expect("must decode")
}

/// A 1.0 sender that ships metadata in separate requests -- Prometheus' own default -- leaves the
/// receiver with flat, untyped series. Nothing is lost, but a histogram is three families rather
/// than one: the stateless-receiver limitation the ADR names, pinned so it is a decision rather
/// than a surprise.
#[test]
fn version_1_without_metadata_decodes_flat_untyped_families() {
    let decoded = decode_v1(pb1::WriteRequest {
        timeseries: vec![
            v1_series(&[("__name__", "foo_bucket"), ("le", "1")], 3.0),
            v1_series(&[("__name__", "foo_count")], 4.0),
            v1_series(&[("__name__", "foo_sum")], 2.5),
        ],
        metadata: Vec::new(),
    });
    let families = &decoded.groups[0];
    assert_eq!(families.len(), 3, "three unrelated families: {families:#?}");
    for family in families {
        assert_eq!(family.kind, FamilyType::Unknown);
    }
    assert_eq!(
        families.iter().map(|family| family.name.as_str()).collect::<Vec<_>>(),
        ["foo_bucket", "foo_count", "foo_sum"]
    );
    // `le` was not stripped: nothing declared `foo` a histogram, so it is an ordinary label on an
    // ordinary series.
    assert_eq!(families[0].series[0].labels, [("le".to_string(), "1".to_string())]);
    assert_eq!(decoded.samples, 3);
}

/// The same three series *with* metadata: one histogram, assembled.
#[test]
fn version_1_with_metadata_assembles_one_histogram() {
    let decoded = decode_v1(pb1::WriteRequest {
        timeseries: vec![
            v1_series(&[("__name__", "foo_bucket"), ("le", "1")], 3.0),
            v1_series(&[("__name__", "foo_bucket"), ("le", "+Inf")], 4.0),
            v1_series(&[("__name__", "foo_count")], 4.0),
            v1_series(&[("__name__", "foo_sum")], 2.5),
        ],
        metadata: vec![pb1::MetricMetadata {
            r#type: pb1::metric_metadata::MetricType::Histogram as i32,
            metric_family_name: "foo".to_string(),
            help: "A histogram.".to_string(),
            unit: String::new(),
        }],
    });
    let families = &decoded.groups[0];
    assert_eq!(families.len(), 1, "{families:#?}");
    assert_eq!(families[0].name, "foo");
    assert_eq!(families[0].kind, FamilyType::Histogram);
    assert_eq!(families[0].help.as_deref(), Some("A histogram."));
    assert_eq!(families[0].unit, None, "an empty unit is absent, not Some(\"\")");
    assert_eq!(
        families[0].series[0].point,
        Point::Histogram { buckets: vec![(1.0, 3), (f64::INFINITY, 4)], sum: Some(2.5), count: 4 }
    );
}

/// A bad label set is one skipped series, not a `400`: a real sender's other series are worth
/// keeping. Each case is counted `logit.input.metrics.skipped{reason="invalid_labels"}`, which the
/// codec's own unit tests assert on the counter; here the question is that the request still
/// decodes and the good series survives.
#[test]
fn an_invalid_label_set_skips_one_series_rather_than_the_request() {
    let bad: [&[(&str, &str)]; 5] = [
        // no `__name__`
        &[("code", "200")],
        // an empty `__name__`
        &[("__name__", "")],
        // not ascending -- `_` is 0x5f, so `__name__` sorts *below* `code` and this pair is
        // the wrong way round
        &[("code", "200"), ("__name__", "foo")],
        // an empty label value
        &[("__name__", "foo"), ("zzz", "")],
        // a repeated label name (which "strictly ascending" also rules out)
        &[("__name__", "foo"), ("code", "200"), ("code", "500")],
    ];
    for labels in bad {
        let decoded = decode_v1(pb1::WriteRequest {
            timeseries: vec![v1_series(labels, 1.0), v1_series(&[("__name__", "good")], 2.0)],
            metadata: Vec::new(),
        });
        let families = &decoded.groups[0];
        assert_eq!(families.len(), 1, "{labels:?} produced {families:#?}");
        assert_eq!(families[0].name, "good");
        assert_eq!(decoded.samples, 1, "{labels:?}");
    }
}

/// `__name__` last, not first: `_` is `0x5f`, so an uppercase-initial label name precedes it, and a
/// sender that sorted `__name__` first would be writing an unsorted label set.
#[test]
fn encoded_label_names_are_strictly_ascending_by_byte_order() {
    let families = vec![vec![MetricFamily {
        series: vec![Series {
            timestamp: Some(TIMESTAMP),
            ..Series::new(
                vec![
                    ("Zone".to_string(), "b".to_string()),
                    ("code".to_string(), "200".to_string()),
                ],
                Point::Gauge(1.0),
            )
        }],
        ..MetricFamily::new("m", FamilyType::Gauge)
    }]];
    let body = encode(&families, Version::V1, &mut PrometheusEncoder::new());
    let request = pb1::WriteRequest::decode(body.as_slice()).expect("must decode");
    let names: Vec<&str> = request.timeseries[0].labels.iter().map(|l| l.name.as_str()).collect();
    assert_eq!(names, ["Zone", "__name__", "code"]);
    assert!(names.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes()));

    // And it survives: the decoder requires the same order it produced.
    assert_eq!(round_trip(&families, Version::V1).groups, families);
}

fn decode_v2_err(request: pb2::Request) -> String {
    decode(&request.encode_to_vec(), Version::V2, &mut PrometheusDecoder::new())
        .expect_err("must be malformed")
        .to_string()
}

/// 2.0's symbol table is the whole request's string storage: a bad index doesn't mean one bad
/// series, it means the table is being read wrongly, so it fails the request (the receiver's
/// `400`).
#[test]
fn version_2_symbol_table_errors_fail_the_whole_request() {
    let series = |refs: Vec<u32>| pb2::TimeSeries {
        labels_refs: refs,
        samples: vec![pb2::Sample { value: 1.0, timestamp: 1, start_timestamp: 0 }],
        ..Default::default()
    };

    let not_empty = pb2::Request {
        symbols: vec!["__name__".to_string(), "foo".to_string()],
        timeseries: vec![series(vec![0, 1])],
    };
    assert!(decode_v2_err(not_empty).contains("symbols[0]"));

    let odd = pb2::Request {
        symbols: vec![String::new(), "__name__".to_string(), "foo".to_string()],
        timeseries: vec![series(vec![1, 2, 1])],
    };
    assert!(decode_v2_err(odd).contains("odd length"));

    let out_of_range = pb2::Request {
        symbols: vec![String::new(), "__name__".to_string(), "foo".to_string()],
        timeseries: vec![series(vec![1, 9])],
    };
    assert!(decode_v2_err(out_of_range).contains("out of range"));

    let bad_help = pb2::Request {
        symbols: vec![String::new(), "__name__".to_string(), "foo".to_string()],
        timeseries: vec![pb2::TimeSeries {
            metadata: Some(pb2::Metadata {
                r#type: pb2::metadata::MetricType::Gauge as i32,
                help_ref: 9,
                unit_ref: 0,
            }),
            ..series(vec![1, 2])
        }],
    };
    assert!(decode_v2_err(bad_help).contains("out of range"));
}

/// The `help_ref`/`unit_ref` convention: `0` points at the mandatory empty symbol and means the
/// field is absent, not `Some("")`.
#[test]
fn version_2_help_and_unit_refs_resolve_through_the_symbol_table() {
    let request = pb2::Request {
        symbols: vec![
            String::new(),
            "__name__".to_string(),
            "foo_seconds".to_string(),
            "A gauge.".to_string(),
            "seconds".to_string(),
        ],
        timeseries: vec![
            pb2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb2::Sample { value: 1.0, timestamp: 1, start_timestamp: 0 }],
                metadata: Some(pb2::Metadata {
                    r#type: pb2::metadata::MetricType::Gauge as i32,
                    help_ref: 3,
                    unit_ref: 4,
                }),
                ..Default::default()
            },
            pb2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![pb2::Sample { value: 2.0, timestamp: 2, start_timestamp: 0 }],
                // No metadata message at all: prost renders 2.0's non-nullable singular field as
                // `Option`, so a decoder must read `None` as "unspecified", not as an error.
                metadata: None,
                ..Default::default()
            },
        ],
    };
    let decoded = decode(&request.encode_to_vec(), Version::V2, &mut PrometheusDecoder::new())
        .expect("must decode");
    assert_eq!(decoded.groups.len(), 2, "two timestamps, two groups");
    for group in &decoded.groups {
        assert_eq!(group[0].name, "foo_seconds");
        assert_eq!(group[0].kind, FamilyType::Gauge);
        assert_eq!(group[0].help.as_deref(), Some("A gauge."));
        assert_eq!(group[0].unit.as_deref(), Some("seconds"));
    }
}

/// 2.0 requires per-series metadata, and the upstream Go type is by-value (`nullable = false`), so
/// Prometheus always writes field 5. This encoder does too, whatever the family has to say.
#[test]
fn every_encoded_version_2_series_carries_a_metadata_message() {
    let families = stamped(
        parse(OPENMETRICS_FIXTURE.as_bytes(), Dialect::OpenMetrics1_0).expect("must parse"),
        Version::V2,
    );
    let body = encode(&[families], Version::V2, &mut PrometheusEncoder::new());
    let request = pb2::Request::decode(body.as_slice()).expect("must decode");
    assert!(!request.timeseries.is_empty());
    assert!(
        request.timeseries.iter().all(|series| series.metadata.is_some()),
        "every 2.0 series must carry a metadata message"
    );
    assert_eq!(request.symbols.first().map(String::as_str), Some(""));
}

/// The stale marker is a bit pattern, not a value: it must survive a protobuf double round trip
/// bit for bit, and must not be confused with an ordinary `NaN` reading.
#[test]
fn a_stale_marker_round_trips_as_its_exact_bit_pattern() {
    for version in [Version::V1, Version::V2] {
        let families = vec![vec![MetricFamily {
            series: vec![Series {
                timestamp: Some(TIMESTAMP),
                ..Series::new(vec![("shard".to_string(), "1".to_string())], Point::Stale)
            }],
            ..MetricFamily::new("m_total", FamilyType::Counter)
        }]];
        let body = encode(&families, version, &mut PrometheusEncoder::new());
        let on_the_wire = match version {
            Version::V1 => {
                pb1::WriteRequest::decode(body.as_slice()).expect("must decode").timeseries[0]
                    .samples[0]
                    .value
            }
            Version::V2 => {
                pb2::Request::decode(body.as_slice()).expect("must decode").timeseries[0].samples[0]
                    .value
            }
        };
        assert_eq!(on_the_wire.to_bits(), STALE_NAN_BITS, "{version:?}");
        assert!(is_stale_nan(on_the_wire) && !is_stale_nan(f64::NAN));
        assert_eq!(round_trip(&families, version).groups, families, "{version:?}");
    }
}

/// A histogram's exemplars land on the bucket their own value falls in, all of them -- unlike an
/// OpenMetrics `_bucket` line, remote-write's `exemplars` is a repeated field with no cap.
#[test]
fn histogram_exemplars_are_placed_on_the_bucket_their_value_falls_in() {
    let exemplar = |value: f64| Exemplar {
        timestamp: TIMESTAMP,
        value,
        trace: None,
        filtered_attributes: AttrMap::new(),
    };
    let families = vec![vec![MetricFamily {
        series: vec![Series {
            timestamp: Some(TIMESTAMP),
            exemplars: vec![exemplar(0.05), exemplar(0.07), exemplar(5.0)],
            ..Series::new(
                vec![],
                Point::Histogram {
                    buckets: vec![(0.1, 2), (f64::INFINITY, 3)],
                    sum: Some(5.1),
                    count: 3,
                },
            )
        }],
        ..MetricFamily::new("h", FamilyType::Histogram)
    }]];
    let body = encode(&families, Version::V1, &mut PrometheusEncoder::new());
    let request = pb1::WriteRequest::decode(body.as_slice()).expect("must decode");
    let placed: Vec<(String, Vec<f64>)> = request
        .timeseries
        .iter()
        .map(|series| {
            let le = series
                .labels
                .iter()
                .find(|l| l.name == "le")
                .map(|l| l.value.clone())
                .unwrap_or_default();
            (le, series.exemplars.iter().map(|e| e.value).collect())
        })
        .filter(|(_, exemplars): &(String, Vec<f64>)| !exemplars.is_empty())
        .collect();
    assert_eq!(
        placed,
        [("+Inf".to_string(), vec![5.0]), ("0.1".to_string(), vec![0.05, 0.07])],
        "two under 0.1, one over -- and series come out in label-set order, so `+Inf` first"
    );
}

/// Encodes with telemetry attached and returns every `logit.output.*` reason it recorded --
/// `metrics.{skipped,degraded}` and `labels.dropped` alike -- alongside the body.
fn encode_reasons(groups: &[Vec<MetricFamily>], version: Version) -> (Vec<u8>, Vec<(String, u64)>) {
    let registry = Registry::new();
    let mut encoder = PrometheusEncoder::new().with_telemetry(registry.telemetry_for(
        "prometheus",
        "prometheus_out",
        "sink",
    ));
    let body = encode(groups, version, &mut encoder);
    let reasons = reasons_from(&registry, "logit.output.");
    (body, reasons)
}

/// The wire has millisecond resolution and the model has nanosecond, so two readings of one series
/// a nanosecond apart truncate onto one timestamp. A `TimeSeries` may not carry two samples at one
/// timestamp -- Prometheus and Mimir answer `400 duplicate sample for timestamp`, which a sender
/// classifies as permanent and drops the *whole batch* over -- so the later reading wins and the
/// earlier is dropped and counted. Any sub-millisecond source (`statsd_in` gauges, `internal`)
/// reaches this through `prometheus_out endpoint:`.
#[test]
fn two_readings_on_one_millisecond_collapse_to_the_later_one() {
    let reading = |timestamp: i64, value: f64| {
        vec![MetricFamily {
            series: vec![Series {
                timestamp: Some(timestamp),
                ..Series::new(vec![("shard".to_string(), "1".to_string())], Point::Gauge(value))
            }],
            ..MetricFamily::new("m", FamilyType::Gauge)
        }]
    };
    // Two `Event::timestamp`s one nanosecond apart, so two groups that truncate onto one
    // millisecond -- exactly what a batch of statsd gauges looks like.
    let groups = vec![reading(TIMESTAMP, 1.0), reading(TIMESTAMP + 1, 2.0)];

    for version in [Version::V1, Version::V2] {
        let (body, reasons) = encode_reasons(&groups, version);
        assert_eq!(reasons, [("sub_ms_collapsed".to_string(), 1)], "{version:?}");

        // One `TimeSeries`, and crucially one `Sample` in it: two would be the 400.
        let samples: Vec<(f64, i64)> = match version {
            Version::V1 => {
                let request = pb1::WriteRequest::decode(body.as_slice()).expect("must decode");
                assert_eq!(request.timeseries.len(), 1);
                request.timeseries[0]
                    .samples
                    .iter()
                    .map(|sample| (sample.value, sample.timestamp))
                    .collect()
            }
            Version::V2 => {
                let request = pb2::Request::decode(body.as_slice()).expect("must decode");
                assert_eq!(request.timeseries.len(), 1);
                request.timeseries[0]
                    .samples
                    .iter()
                    .map(|sample| (sample.value, sample.timestamp))
                    .collect()
            }
        };
        assert_eq!(samples, [(2.0, TIMESTAMP / 1_000_000)], "{version:?}: the later reading wins");

        // And it reads back as one group holding that reading.
        let decoded = decode(&body, version, &mut PrometheusDecoder::new()).expect("must decode");
        assert_eq!(decoded.groups, vec![reading(TIMESTAMP, 2.0)], "{version:?}");
        assert_eq!(decoded.samples, 1);
    }

    // A millisecond apart is not a collision: both readings survive, nothing is counted.
    let apart = vec![reading(TIMESTAMP, 1.0), reading(TIMESTAMP + 1_000_000, 2.0)];
    for version in [Version::V1, Version::V2] {
        let (body, reasons) = encode_reasons(&apart, version);
        assert!(reasons.is_empty(), "{version:?} counted {reasons:?}");
        let decoded = decode(&body, version, &mut PrometheusDecoder::new()).expect("must decode");
        assert_eq!(decoded.groups.len(), 2, "{version:?}");
        assert_eq!(decoded.samples, 2, "{version:?}");
    }
}

/// Protobuf skips fields it does not recognise, and the two versions' field numbers are disjoint
/// (2.0 reserves 1-3, which is where 1.0 keeps its `timeseries` and `metadata`), so each version's
/// body decodes as an *empty* request of the other rather than as an error. Left alone, a sender
/// that posts 1.0 bytes under the 2.0 `Content-Type` gets a `204` reporting nothing written, which
/// reads as "accepted"; a receiver has to answer `400`.
#[test]
fn a_body_of_the_other_version_is_malformed_rather_than_an_empty_request() {
    let families = vec![vec![MetricFamily {
        series: vec![Series {
            timestamp: Some(TIMESTAMP),
            ..Series::new(vec![("shard".to_string(), "1".to_string())], Point::Gauge(1.0))
        }],
        ..MetricFamily::new("m", FamilyType::Gauge)
    }]];

    for (sent, claimed) in [(Version::V1, Version::V2), (Version::V2, Version::V1)] {
        let body = encode(&families, sent, &mut PrometheusEncoder::new());
        assert!(!body.is_empty());
        // It really does decode "successfully" as the wrong version -- that is the trap.
        match claimed {
            Version::V1 => {
                let wrong = pb1::WriteRequest::decode(body.as_slice()).expect("prost accepts it");
                assert!(wrong.timeseries.is_empty() && wrong.metadata.is_empty());
            }
            Version::V2 => {
                let wrong = pb2::Request::decode(body.as_slice()).expect("prost accepts it");
                assert!(wrong.symbols.is_empty() && wrong.timeseries.is_empty());
            }
        }
        let error = decode(&body, claimed, &mut PrometheusDecoder::new())
            .expect_err("a body of the other version must not decode as an empty request");
        assert!(
            error.to_string().contains("body is not"),
            "{sent:?} body read as {claimed:?} gave {error}"
        );
        // And the right version still reads it.
        assert_eq!(round_trip(&families, sent).groups, families, "{sent:?}");
    }
}

/// A zero-byte body is a genuinely empty request, not a wrong-version one -- an empty 1.0
/// `WriteRequest` encodes to exactly that.
#[test]
fn an_empty_body_is_an_empty_request_in_both_versions() {
    for version in [Version::V1, Version::V2] {
        let decoded =
            decode(&[], version, &mut PrometheusDecoder::new()).expect("must decode: {version:?}");
        assert_eq!(decoded, Decoded::default(), "{version:?}");
    }
    // 1.0 with nothing in it *is* a zero-byte body; 2.0's is the one mandatory empty symbol, which
    // is not zero bytes and is recognised on its own.
    let empty_v1 = encode(&[], Version::V1, &mut PrometheusEncoder::new());
    assert!(empty_v1.is_empty());
    let empty_v2 = encode(&[], Version::V2, &mut PrometheusEncoder::new());
    assert!(!empty_v2.is_empty());
    assert_eq!(
        decode(&empty_v2, Version::V2, &mut PrometheusDecoder::new()).expect("must decode"),
        Decoded::default()
    );
}

/// The `Content-Type` table, including the tolerances a real sender needs.
#[test]
fn from_content_type_recognizes_both_versions_and_rejects_the_rest() {
    let v1 = Some(Version::V1);
    let v2 = Some(Version::V2);
    let cases: [(&str, Option<Version>); 16] = [
        ("application/x-protobuf", v1),
        // A parameter with no `=` is one more parameter this codec has no use for, not a reason to
        // answer 415: a trailing `;` (which `split` yields as an empty part), a bare word, and a
        // `proto=` that still has to win from behind one.
        ("application/x-protobuf;", v1),
        ("application/x-protobuf; ", v1),
        ("application/x-protobuf; charset", v1),
        ("application/x-protobuf; charset; proto=io.prometheus.write.v2.Request;", v2),
        ("application/x-protobuf;proto=prometheus.WriteRequest", v1),
        ("APPLICATION/X-PROTOBUF; PROTO=prometheus.WriteRequest", v1),
        ("application/x-protobuf ; proto = prometheus.WriteRequest", v1),
        ("application/x-protobuf;proto=io.prometheus.write.v2.Request", v2),
        ("application/x-protobuf; proto=io.prometheus.write.v2.Request; charset=utf-8", v2),
        ("application/x-protobuf;charset=utf-8", v1),
        ("application/x-protobuf;proto=\"io.prometheus.write.v2.Request\"", v2),
        ("application/x-protobuf;proto=something.Else", None),
        ("application/json", None),
        ("text/plain; version=0.0.4", None),
        ("", None),
    ];
    for (header, expected) in cases {
        assert_eq!(Version::from_content_type(header), expected, "{header:?}");
    }
    assert_eq!(Version::V1.header_version(), "0.1.0");
    assert_eq!(Version::V2.header_version(), "2.0.0");
    assert_eq!(Version::from_content_type(Version::V1.content_type()), v1);
    assert_eq!(Version::from_content_type(Version::V2.content_type()), v2);
}

/// Native histograms are counted, not decoded, and the count is what a 2.0 receiver reports in
/// `X-Prometheus-Remote-Write-Histograms-Written` (as zero written, this many seen).
#[test]
fn native_histograms_are_counted_and_skipped() {
    let decoded = decode_v1(pb1::WriteRequest {
        timeseries: vec![pb1::TimeSeries {
            labels: vec![label("__name__", "native")],
            histograms: vec![pb1::Histogram::default(), pb1::Histogram::default()],
            ..Default::default()
        }],
        metadata: Vec::new(),
    });
    assert_eq!(decoded.histograms_skipped, 2);
    assert_eq!(decoded.samples, 0);
    assert!(decoded.groups.is_empty(), "nothing to put in a group");
}

/// A stale marker has to survive on *every* family type, not only the ones `events_to_families`
/// can build one for: a stale NaN on `foo_bucket{le=…}` flags the whole series stale, so a relay
/// hands this encoder stale histograms and summaries too. The bare family name is not a sample name
/// for those types, so the marker rides `_count`/`_sum` (`_gcount`/`_gsum`) instead.
#[test]
fn a_stale_marker_survives_on_every_family_type() {
    for kind in [
        FamilyType::Counter,
        FamilyType::Gauge,
        FamilyType::Unknown,
        FamilyType::Info,
        FamilyType::StateSet,
        FamilyType::Histogram,
        FamilyType::GaugeHistogram,
        FamilyType::Summary,
    ] {
        let name = match kind {
            FamilyType::Counter => "m_total",
            _ => "m",
        };
        let families = vec![vec![MetricFamily {
            series: vec![Series {
                timestamp: Some(TIMESTAMP),
                ..Series::new(vec![("shard".to_string(), "1".to_string())], Point::Stale)
            }],
            ..MetricFamily::new(name, kind)
        }]];
        for version in [Version::V1, Version::V2] {
            let body = encode(&families, version, &mut PrometheusEncoder::new());
            let (decoded, reasons) = decode_reasons(&body, version);
            assert_eq!(decoded.groups, families, "{kind:?} {version:?}");
            assert!(reasons.is_empty(), "{kind:?} {version:?} counted {reasons:?}");
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Declarations are materialized on demand, and only what a sample asks for
// -------------------------------------------------------------------------------------------------

/// A request's declaration count and its distinct-timestamp count are both attacker-controlled, so
/// a decoder that replayed every declaration into every timestamp group would let a small body ask
/// for `declarations x groups` accumulators. Declaring on demand makes the cost proportional to the
/// samples the request actually carries: 200 declarations and 50 timestamps over one series is 50
/// groups of **one** family, not 50 of 201.
#[test]
fn declarations_materialize_only_where_a_sample_routes_to_them() {
    const DECLARATIONS: usize = 200;
    const TIMESTAMPS: i64 = 50;

    let metadata = (0..DECLARATIONS)
        .map(|i| pb1::MetricMetadata {
            r#type: pb1::metric_metadata::MetricType::Counter as i32,
            metric_family_name: format!("declared_{i}_total"),
            help: "Never sampled.".to_string(),
            unit: String::new(),
        })
        .collect();
    let samples =
        (0..TIMESTAMPS).map(|i| pb1::Sample { value: i as f64, timestamp: 1_000 + i }).collect();
    let decoded = decode_v1(pb1::WriteRequest {
        timeseries: vec![pb1::TimeSeries {
            labels: vec![label("__name__", "sampled_total")],
            samples,
            ..Default::default()
        }],
        metadata,
    });

    assert_eq!(decoded.groups.len(), TIMESTAMPS as usize);
    for group in &decoded.groups {
        assert_eq!(group.len(), 1, "a group must hold only the families its own samples touched");
        assert_eq!(group[0].name, "sampled_total");
    }
    // And the 200 declared-but-unsampled families appear nowhere at all -- not as empty families,
    // not as a family with a `help` and no series.
    assert!(!decoded.groups.iter().flatten().any(|family| family.name.starts_with("declared_")));
}

/// The narrow statement of the same property, with the numbers small enough to read.
#[test]
fn a_declared_but_unsampled_family_never_appears_in_any_group() {
    let decoded = decode_v1(pb1::WriteRequest {
        timeseries: vec![v1_series(&[("__name__", "present")], 1.0)],
        metadata: vec![
            pb1::MetricMetadata {
                r#type: pb1::metric_metadata::MetricType::Histogram as i32,
                metric_family_name: "absent".to_string(),
                help: "Declared, never sampled.".to_string(),
                unit: "seconds".to_string(),
            },
            pb1::MetricMetadata {
                r#type: pb1::metric_metadata::MetricType::Gauge as i32,
                metric_family_name: "present".to_string(),
                help: "Sampled.".to_string(),
                unit: String::new(),
            },
        ],
    });
    assert_eq!(decoded.groups.len(), 1);
    assert_eq!(decoded.groups[0].len(), 1);
    assert_eq!(decoded.groups[0][0].name, "present");
    assert_eq!(decoded.groups[0][0].kind, FamilyType::Gauge);
    assert_eq!(decoded.groups[0][0].help.as_deref(), Some("Sampled."));
}

/// 2.0 repeats a family's `Metadata` on every one of its wire series, so a decoder that treated
/// each repeat as a second declaration would count `duplicate_metadata` once per series -- a
/// counter operators read as "input was dropped" firing on a request nothing was dropped from.
/// Asserted over both fixture corpora and the 100-series bench shape, in both versions.
#[test]
fn a_faithful_round_trip_counts_nothing_as_skipped_or_degraded() {
    for (label, body) in
        [("text 0.0.4 corpus", TEXT_FIXTURE), ("openmetrics corpus", OPENMETRICS_FIXTURE)]
    {
        let dialect =
            if body == TEXT_FIXTURE { Dialect::Text0_0_4 } else { Dialect::OpenMetrics1_0 };
        for version in [Version::V1, Version::V2] {
            let families = stamped(parse(body.as_bytes(), dialect).expect("must parse"), version);
            let encoded = encode(&[families], version, &mut PrometheusEncoder::new());
            let (_, reasons) = decode_reasons(&encoded, version);
            assert!(reasons.is_empty(), "{label} {version:?} counted {reasons:?}");
        }
    }

    // And the wide shape: one family, 100 series, which in 2.0 is 100 repeats of one `Metadata`.
    // Series are sorted by label set because that is the canonical order a decode comes back in
    // ("shard=10" before "shard=2"); an unsorted fixture would be testing its own construction.
    let mut series: Vec<Series> = (0..100)
        .map(|i| Series {
            timestamp: Some(TIMESTAMP),
            ..Series::new(vec![("shard".to_string(), i.to_string())], Point::Gauge(i as f64))
        })
        .collect();
    series.sort_by(|a, b| a.labels.cmp(&b.labels));
    let wide = vec![MetricFamily {
        help: Some("Bench gauge.".to_string()),
        unit: Some("seconds".to_string()),
        series,
        ..MetricFamily::new("prom_bench_gauge_seconds", FamilyType::Gauge)
    }];
    for version in [Version::V1, Version::V2] {
        let encoded = encode(std::slice::from_ref(&wide), version, &mut PrometheusEncoder::new());
        let (decoded, reasons) = decode_reasons(&encoded, version);
        assert_eq!(decoded.groups, vec![wide.clone()], "{version:?}");
        assert!(reasons.is_empty(), "100-series {version:?} counted {reasons:?}");
    }
}

/// A metadata entry that *disagrees* with an earlier one for the same family is the real duplicate:
/// first wins, and it is counted.
#[test]
fn a_conflicting_metadata_entry_is_counted_and_loses() {
    let request = pb1::WriteRequest {
        timeseries: vec![v1_series(&[("__name__", "foo")], 1.0)],
        metadata: vec![
            pb1::MetricMetadata {
                r#type: pb1::metric_metadata::MetricType::Gauge as i32,
                metric_family_name: "foo".to_string(),
                help: "First.".to_string(),
                unit: String::new(),
            },
            pb1::MetricMetadata {
                r#type: pb1::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "foo".to_string(),
                help: "Second.".to_string(),
                unit: String::new(),
            },
        ],
    };
    let (decoded, reasons) = decode_reasons(&request.encode_to_vec(), Version::V1);
    assert_eq!(decoded.groups[0][0].kind, FamilyType::Gauge, "the first entry wins");
    assert_eq!(decoded.groups[0][0].help.as_deref(), Some("First."));
    assert_eq!(
        reasons,
        [("duplicate_metadata".to_string(), 1), ("duplicate_type".to_string(), 1)],
        "one of each, from the one entry that disagreed"
    );
}

// -------------------------------------------------------------------------------------------------
// Exemplars are placed against their own series' samples, not against whatever group exists
// -------------------------------------------------------------------------------------------------

/// An exemplar goes to a group where *its own* series has a sample. The failure this pins is
/// order-dependence: with one series sampled at 2000 and another at 1000 carrying an exemplar
/// timestamped 2000, a decoder that matched the exemplar against groups-so-far would put it in the
/// 2000 group (inventing a reading-less series there) or not, depending purely on `TimeSeries`
/// order, which neither spec constrains.
#[test]
fn an_exemplar_lands_on_its_own_series_whichever_order_the_series_arrive_in() {
    let at = |name: &str, ms: i64| pb1::TimeSeries {
        labels: vec![label("__name__", name)],
        samples: vec![pb1::Sample { value: 1.0, timestamp: ms }],
        ..Default::default()
    };
    let b_with_exemplar = pb1::TimeSeries {
        exemplars: vec![pb1::Exemplar {
            labels: vec![label("detail", "kept")],
            value: 0.5,
            timestamp: 2_000,
        }],
        ..at("b", 1_000)
    };

    let forwards = decode_v1(pb1::WriteRequest {
        timeseries: vec![at("a", 2_000), b_with_exemplar.clone()],
        metadata: Vec::new(),
    });
    let backwards = decode_v1(pb1::WriteRequest {
        timeseries: vec![b_with_exemplar, at("a", 2_000)],
        metadata: Vec::new(),
    });
    assert_eq!(forwards, backwards, "the result must not depend on TimeSeries order");

    assert_eq!(forwards.exemplars, 1);
    assert_eq!(forwards.groups.len(), 2, "two timestamps, two groups");
    // Group 0 is 1000, where `b` lives; group 1 is 2000, where `a` does. The exemplar is on `b`,
    // and `a`'s group holds only `a` -- no phantom `b` series, and so no `incomplete_series` skip.
    assert_eq!(forwards.groups[0].len(), 1);
    assert_eq!(forwards.groups[0][0].name, "b");
    assert_eq!(forwards.groups[0][0].series[0].exemplars.len(), 1);
    assert_eq!(forwards.groups[1].len(), 1);
    assert_eq!(forwards.groups[1][0].name, "a");
    assert!(forwards.groups[1][0].series[0].exemplars.is_empty());
}

/// An exemplar whose series carried no sample at all has no reading to be an example of. It is
/// dropped and counted, never stored, and `Decoded::exemplars` -- which becomes
/// `X-Prometheus-Remote-Write-Exemplars-Written` -- does not claim it.
#[test]
fn an_exemplar_with_no_sample_to_sit_on_is_dropped_and_counted() {
    let request = pb1::WriteRequest {
        timeseries: vec![pb1::TimeSeries {
            labels: vec![label("__name__", "orphan")],
            exemplars: vec![pb1::Exemplar {
                labels: vec![label("detail", "lost")],
                value: 0.5,
                timestamp: 2_000,
            }],
            ..Default::default()
        }],
        metadata: Vec::new(),
    };
    let (decoded, reasons) = decode_reasons(&request.encode_to_vec(), Version::V1);
    assert_eq!(decoded.exemplars, 0, "nothing was stored, so nothing is reported as written");
    assert!(decoded.groups.is_empty(), "no samples means no groups and no phantom series");
    assert_eq!(reasons, [("exemplar_dropped".to_string(), 1)]);
}

/// An exemplar on a series whose labels were rejected is unwritable too, and is counted as such.
/// The two counters answer different questions -- how many series went, and how much of what the
/// sender sent was not stored -- so they are deliberately not additive, and a sender reconciling
/// its own exemplar count against `X-Prometheus-Remote-Write-Exemplars-Written` can always find the
/// difference in `exemplar_dropped` alone.
#[test]
fn exemplars_on_a_series_with_invalid_labels_are_counted_as_dropped() {
    let exemplar = |value: f64| pb1::Exemplar {
        labels: vec![label("detail", "lost")],
        value,
        timestamp: 1_605_281_325_000,
    };
    let request = pb1::WriteRequest {
        timeseries: vec![
            pb1::TimeSeries {
                // No `__name__`: the series is skipped as `invalid_labels`, and its two exemplars
                // go with it.
                labels: vec![label("code", "200")],
                samples: vec![pb1::Sample { value: 1.0, timestamp: 1_605_281_325_000 }],
                exemplars: vec![exemplar(0.1), exemplar(0.2)],
                ..Default::default()
            },
            v1_series(&[("__name__", "good")], 2.0),
        ],
        metadata: Vec::new(),
    };
    let (decoded, reasons) = decode_reasons(&request.encode_to_vec(), Version::V1);

    assert_eq!(decoded.groups.len(), 1);
    assert_eq!(decoded.groups[0].len(), 1, "only the good series survives");
    assert_eq!(decoded.groups[0][0].name, "good");
    assert_eq!(decoded.samples, 1);
    assert_eq!(decoded.exemplars, 0, "nothing stored, so nothing reported as written");
    assert_eq!(
        reasons,
        [("exemplar_dropped".to_string(), 2), ("invalid_labels".to_string(), 1)],
        "one skip for the series, and one degrade per exemplar it took with it"
    );
}

// -------------------------------------------------------------------------------------------------
// 2.0's per-series metadata names no family, so an unspecified type must not invent one
// -------------------------------------------------------------------------------------------------

/// A 2.0 series with help but no *type* says nothing about which family it belongs to: `foo_bucket`
/// might be a histogram's bucket line or a gauge that happens to be called that. Declaring a family
/// from it would create a `foo_bucket` family that beats the sibling's `HISTOGRAM` declaration of
/// `foo` -- `route` prefers an exact name over the suffix scan -- and leave the histogram
/// bucket-less. The help still lands, on the family the sample actually routed to.
#[test]
fn an_unspecified_type_describes_the_family_its_samples_land_in_rather_than_declaring_one() {
    let request = pb2::Request {
        symbols: vec![
            String::new(),
            "__name__".to_string(),
            "foo_bucket".to_string(),
            "le".to_string(),
            "1".to_string(),
            "foo_sum".to_string(),
            "Latency.".to_string(),
            "foo_count".to_string(),
        ],
        timeseries: vec![
            pb2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![pb2::Sample { value: 3.0, timestamp: 1, start_timestamp: 0 }],
                // Help, no type -- the case the guard used not to cover.
                metadata: Some(pb2::Metadata {
                    r#type: pb2::metadata::MetricType::Unspecified as i32,
                    help_ref: 6,
                    unit_ref: 0,
                }),
                ..Default::default()
            },
            pb2::TimeSeries {
                labels_refs: vec![1, 5],
                samples: vec![pb2::Sample { value: 2.5, timestamp: 1, start_timestamp: 0 }],
                metadata: Some(pb2::Metadata {
                    r#type: pb2::metadata::MetricType::Histogram as i32,
                    help_ref: 0,
                    unit_ref: 0,
                }),
                ..Default::default()
            },
            pb2::TimeSeries {
                labels_refs: vec![1, 7],
                samples: vec![pb2::Sample { value: 3.0, timestamp: 1, start_timestamp: 0 }],
                metadata: Some(pb2::Metadata {
                    r#type: pb2::metadata::MetricType::Histogram as i32,
                    help_ref: 0,
                    unit_ref: 0,
                }),
                ..Default::default()
            },
        ],
    };
    let (decoded, reasons) = decode_reasons(&request.encode_to_vec(), Version::V2);
    assert_eq!(decoded.groups.len(), 1);
    let families = &decoded.groups[0];
    assert_eq!(
        families.len(),
        1,
        "one histogram, not a histogram plus a stray gauge: {families:#?}"
    );
    assert_eq!(families[0].name, "foo");
    assert_eq!(families[0].kind, FamilyType::Histogram);
    assert_eq!(families[0].help.as_deref(), Some("Latency."), "the help still lands");
    assert_eq!(
        families[0].series[0].point,
        Point::Histogram { buckets: vec![(1.0, 3), (f64::INFINITY, 3)], sum: Some(2.5), count: 3 }
    );
    assert!(reasons.is_empty(), "{reasons:?}");
}

// -------------------------------------------------------------------------------------------------
// Property 1: a generator over the wire model
// -------------------------------------------------------------------------------------------------

const INTERESTING_CHARS: [char; 8] = ['a', 'B', '7', ' ', '\\', '"', ':', '-'];

fn text_blob(max: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(INTERESTING_CHARS.to_vec()), 1..max)
        .prop_map(|chars| chars.into_iter().collect())
}

fn label_name() -> impl Strategy<Value = String> {
    // `le` and `quantile` are generated by the codec itself for the types that need them, so an
    // attribute of either name would be dropped rather than round-tripped (counted `reserved`).
    "[a-z][a-z0-9_]{0,4}"
        .prop_filter("the codec generates these itself", |s: &String| s != "le" && s != "quantile")
}

fn labels() -> impl Strategy<Value = Vec<(String, String)>> {
    proptest::collection::vec((label_name(), text_blob(6)), 0..3).prop_map(|mut pairs| {
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|a, b| a.0 == b.0);
        pairs
    })
}

/// A finite or infinite sample value -- never `NaN`, for the reason this file's module doc gives.
fn sample_value() -> impl Strategy<Value = f64> {
    prop_oneof![
        9 => (-1e6f64..1e6).prop_filter("finite", |v| v.is_finite()),
        1 => Just(f64::INFINITY),
        1 => Just(f64::NEG_INFINITY),
    ]
}

fn histogram_point() -> impl Strategy<Value = Point> {
    (
        proptest::collection::vec((-1000.0f64..1000.0, 0u64..1000), 1..4),
        proptest::option::of(-1e6f64..1e6),
        0u64..1000,
    )
        .prop_map(|(raw, sum, tail)| {
            let mut bounds: Vec<(f64, u64)> = raw;
            bounds.sort_by(|a, b| a.0.total_cmp(&b.0));
            bounds.dedup_by(|a, b| a.0 == b.0);
            let mut buckets = Vec::with_capacity(bounds.len() + 1);
            let mut running = 0u64;
            for (bound, delta) in bounds {
                running += delta;
                buckets.push((bound, running));
            }
            running += tail;
            buckets.push((f64::INFINITY, running));
            Point::Histogram { buckets, sum, count: running }
        })
}

fn summary_point() -> impl Strategy<Value = Point> {
    (
        proptest::collection::vec((0.0f64..=1.0, -1e6f64..1e6), 0..4),
        proptest::option::of(-1e6f64..1e6),
        proptest::option::of(0u64..1000),
    )
        .prop_map(|(raw, sum, count)| {
            let mut quantiles: Vec<(f64, f64)> = raw;
            quantiles.sort_by(|a, b| a.0.total_cmp(&b.0));
            quantiles.dedup_by(|a, b| a.0 == b.0);
            // A summary with nothing in it at all is not a series; give it a count so the
            // assembler has something to rebuild it from.
            let count = if quantiles.is_empty() && sum.is_none() { Some(0) } else { count };
            Point::Summary { quantiles, sum, count }
        })
}

/// Every [`Point`] variant its family type can carry, `Stale` included -- which is the one this
/// transport can express and the exposition dialects cannot.
fn point_for(kind: FamilyType) -> BoxedStrategy<Point> {
    let valued = match kind {
        FamilyType::Counter => sample_value().prop_map(Point::Counter).boxed(),
        FamilyType::Gauge => sample_value().prop_map(Point::Gauge).boxed(),
        FamilyType::Unknown | FamilyType::Untyped => {
            sample_value().prop_map(Point::Unknown).boxed()
        }
        FamilyType::Histogram | FamilyType::GaugeHistogram => histogram_point().boxed(),
        FamilyType::Summary => summary_point().boxed(),
        FamilyType::Info => Just(Point::Info).boxed(),
        FamilyType::StateSet => any::<bool>().prop_map(Point::StateSet).boxed(),
    };
    // Every family type, `Stale` included. `events_to_families` only ever builds a `Stale` for the
    // single-series kinds, but *this decoder* builds one for any of them -- a stale NaN on
    // `foo_bucket{le=…}` flags the whole series stale -- so a relay (1.0 to 2.0, or W3 to W4) can
    // and does hand the encoder a stale histogram.
    prop_oneof![9 => valued, 1 => Just(Point::Stale)].boxed()
}

/// Whole milliseconds: both versions' only resolution.
fn instant() -> impl Strategy<Value = i64> {
    (1i64..2_000_000_000).prop_map(|ms| ms * 1_000_000)
}

/// At most one exemplar, on a counter, timestamped to its own sample -- see this file's module doc
/// for why the generator does not go further.
fn exemplars(kind: FamilyType, timestamp: i64) -> BoxedStrategy<Vec<Exemplar>> {
    if kind != FamilyType::Counter {
        return Just(Vec::new()).boxed();
    }
    proptest::option::of((
        -1e6f64..1e6,
        proptest::option::of((any::<u8>(), proptest::option::of(any::<u8>()))),
        proptest::collection::vec((label_name(), text_blob(4)), 0..2),
    ))
    .prop_map(move |generated| match generated {
        None => Vec::new(),
        Some((value, ids, attrs)) => {
            let trace = ids.map(|(trace_byte, span_byte)| TraceRef {
                // Never all-zero: `TraceRef`'s own validity rule rejects that.
                trace_id: [trace_byte | 1; 16],
                span_id: span_byte.map(|b| [b | 1; 8]),
                flags: 0,
            });
            let mut filtered_attributes = AttrMap::new();
            for (key, value) in attrs {
                // `trace_id`/`span_id` are the trace reference's own spelling on the wire.
                if key != "trace_id" && key != "span_id" {
                    filtered_attributes.insert(&key, Value::str(value));
                }
            }
            vec![Exemplar { timestamp, value, trace, filtered_attributes }]
        }
    })
    .boxed()
}

fn series(kind: FamilyType, version: Version, timestamp: i64) -> BoxedStrategy<Series> {
    let created = if version == Version::V2 && kind.has_created() {
        proptest::option::of(instant()).boxed()
    } else {
        Just(None).boxed()
    };
    (labels(), point_for(kind), created, exemplars(kind, timestamp))
        .prop_map(move |(labels, point, created, exemplars)| {
            // A stale series has no reading for an exemplar to be an example of.
            let exemplars = if point == Point::Stale { Vec::new() } else { exemplars };
            Series { labels, point, timestamp: Some(timestamp), created, exemplars }
        })
        .boxed()
}

fn family_type() -> BoxedStrategy<FamilyType> {
    prop_oneof![
        Just(FamilyType::Counter),
        Just(FamilyType::Gauge),
        Just(FamilyType::Histogram),
        Just(FamilyType::GaugeHistogram),
        Just(FamilyType::Summary),
        Just(FamilyType::Info),
        Just(FamilyType::StateSet),
        // `Untyped` is left out: neither version has a second spelling of "no type given", so it
        // comes back `Unknown` -- a named normalization, not a round trip.
        Just(FamilyType::Unknown),
    ]
    .boxed()
}

/// A family's request-wide facts. These belong to the *family*, not to a timestamp: 1.0 carries one
/// `MetricMetadata` per family for the whole request and 2.0 carries it on a `TimeSeries` that
/// spans every group the series appeared in, so a family whose help differed between two timestamps
/// could not be expressed on either wire at all.
fn family_spec() -> BoxedStrategy<(FamilyType, Option<String>, Option<String>)> {
    (
        family_type(),
        proptest::option::of(
            text_blob(12)
                .prop_map(|s| s.trim().to_string())
                .prop_filter("help must survive trimming", |s: &String| !s.is_empty()),
        ),
        proptest::option::of("[a-z]{1,6}".prop_map(|s: String| s)),
    )
        .boxed()
}

fn family(
    spec: &(FamilyType, Option<String>, Option<String>),
    version: Version,
    timestamp: i64,
) -> BoxedStrategy<MetricFamily> {
    let (kind, help, unit) = spec.clone();
    proptest::collection::vec(series(kind, version, timestamp), 1..3)
        .prop_map(move |mut series| {
            series.sort_by(|a, b| a.labels.cmp(&b.labels));
            series.dedup_by(|a, b| a.labels == b.labels);
            MetricFamily {
                name: String::new(),
                kind,
                help: help.clone(),
                unit: unit.clone(),
                series,
            }
        })
        .boxed()
}

/// Several timestamp groups over **one** set of families: the same names and types at each
/// timestamp, which is what a batch of consecutive scrapes looks like and the case the encoder's
/// cross-group series merge exists for.
fn groups(version: Version) -> impl Strategy<Value = Vec<Vec<MetricFamily>>> {
    (proptest::collection::vec(family_spec(), 1..4), proptest::collection::vec(instant(), 1..3))
        .prop_flat_map(move |(specs, mut timestamps)| {
            timestamps.sort_unstable();
            timestamps.dedup();
            let per_group: Vec<BoxedStrategy<Vec<MetricFamily>>> = timestamps
                .iter()
                .map(|timestamp| {
                    let families: Vec<BoxedStrategy<MetricFamily>> =
                        specs.iter().map(|spec| family(spec, version, *timestamp)).collect();
                    families.boxed()
                })
                .collect();
            per_group
        })
        .prop_map(|mut groups: Vec<Vec<MetricFamily>>| {
            for families in &mut groups {
                for (i, family) in families.iter_mut().enumerate() {
                    // Names are assigned rather than generated so no two families share one and no
                    // family's name can collide with another's generated sample names (`m0` vs
                    // `m0_sum`) -- a naming clash the formats themselves forbid, not a codec
                    // question.
                    let stem = match &family.unit {
                        Some(unit) => format!("m{i}_{unit}"),
                        None => format!("m{i}"),
                    };
                    family.name = match family.kind {
                        // A counter's value sample always carries `_total`, so a name that lacks it
                        // would gain it on the way out -- a named normalization.
                        FamilyType::Counter => format!("{stem}_total"),
                        _ => stem,
                    };
                }
                families.sort_by(|a, b| a.name.cmp(&b.name));
            }
            groups
        })
}

/// Every `Sample` in an encoded request, across every `TimeSeries` -- what
/// [`wire_samples`] claims a group set is spelled as.
fn encoded_sample_count(groups: &[Vec<MetricFamily>], version: Version) -> u64 {
    let body = encode(groups, version, &mut PrometheusEncoder::new());
    match version {
        Version::V1 => pb1::WriteRequest::decode(body.as_slice())
            .expect("our own encoder's output must decode")
            .timeseries
            .iter()
            .map(|series| series.samples.len() as u64)
            .sum(),
        Version::V2 => pb2::Request::decode(body.as_slice())
            .expect("our own encoder's output must decode")
            .timeseries
            .iter()
            .map(|series| series.samples.len() as u64)
            .sum(),
    }
}

fn claimed_sample_count(groups: &[Vec<MetricFamily>], version: Version) -> u64 {
    groups
        .iter()
        .flatten()
        .flat_map(|family| family.series.iter().map(move |series| (family.kind, series)))
        .map(|(kind, series)| wire_samples(kind, series, version))
        .sum()
}

proptest! {
    #[test]
    fn decoding_an_encoded_group_set_is_the_identity_in_version_1(
        groups in groups(Version::V1),
    ) {
        prop_assert_eq!(round_trip(&groups, Version::V1).groups, groups);
    }

    #[test]
    fn decoding_an_encoded_group_set_is_the_identity_in_version_2(
        groups in groups(Version::V2),
    ) {
        prop_assert_eq!(round_trip(&groups, Version::V2).groups, groups);
    }

    /// `wire_samples` is the exactness proof behind a receiver's
    /// `X-Prometheus-Remote-Write-Samples-Written`: whatever it says a series is spelled as, the
    /// encoder writes exactly that many `Sample`s for it. Asserted over the same generated group
    /// sets the identity properties above use, so every `Point` variant, both `_sum`/`_count`
    /// spellings, a gaugehistogram's `_gcount` rule and a stale marker on every family type are all
    /// covered by construction rather than by a list of hand-written cases.
    #[test]
    fn wire_samples_counts_exactly_what_version_1_encodes(groups in groups(Version::V1)) {
        prop_assert_eq!(
            claimed_sample_count(&groups, Version::V1),
            encoded_sample_count(&groups, Version::V1),
        );
    }

    #[test]
    fn wire_samples_counts_exactly_what_version_2_encodes(groups in groups(Version::V2)) {
        prop_assert_eq!(
            claimed_sample_count(&groups, Version::V2),
            encoded_sample_count(&groups, Version::V2),
        );
    }
}

/// The one place `wire_samples` and `encode` deliberately disagree, and the reason it takes a
/// `Version` at all. 1.0 spells a created timestamp as a `_created` sample of its own, which
/// `decode` reads and counts -- so a receiver kept it and must report it -- while `encode` drops it,
/// 1.0 having no field to put it in (this module's permitted-normalization list). 2.0 carries it as
/// `Sample.start_timestamp`, a field *on* a sample rather than a sample, so it adds nothing there.
#[test]
fn a_created_timestamp_is_a_wire_sample_in_version_1_and_a_field_in_version_2() {
    let mut family = MetricFamily::new("requests", FamilyType::Counter);
    family.series.push(Series {
        labels: vec![("job".to_string(), "api".to_string())],
        point: Point::Counter(7.0),
        timestamp: Some(1_605_281_325_000_000_000),
        created: Some(1_605_281_000_000_000_000),
        exemplars: Vec::new(),
    });
    let groups = vec![vec![family]];

    // 1.0: `requests_total` and `requests_created`, two samples on the wire, and `decode` counts
    // both -- which the round trip below confirms rather than assumes.
    assert_eq!(claimed_sample_count(&groups, Version::V1), 2);
    // 2.0: one sample carrying its own `start_timestamp`.
    assert_eq!(claimed_sample_count(&groups, Version::V2), 1);
    assert_eq!(encoded_sample_count(&groups, Version::V2), 1);

    // And `encode` writes only the value sample on 1.0, which is the disagreement being pinned.
    assert_eq!(encoded_sample_count(&groups, Version::V1), 1);

    // A 1.0 request that really does carry a `_created` sample: `decode` counts two, so a receiver
    // reporting `wire_samples` reports two.
    let decoded = decode_v1(pb1::WriteRequest {
        timeseries: vec![
            v1_series(&[("__name__", "requests_total")], 7.0),
            v1_series(&[("__name__", "requests_created")], 1_605_281_000.0),
        ],
        metadata: vec![pb1::MetricMetadata {
            r#type: pb1::metric_metadata::MetricType::Counter as i32,
            metric_family_name: "requests".to_string(),
            help: String::new(),
            unit: String::new(),
        }],
    });
    assert_eq!(decoded.samples, 2);
    assert_eq!(claimed_sample_count(&decoded.groups, Version::V1), 2, "{:#?}", decoded.groups);
}
