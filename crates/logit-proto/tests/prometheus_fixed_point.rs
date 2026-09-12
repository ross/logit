//! Pure-codec Prometheus fixed-point tests: [ADR `lossless-transit`]'s "round-trip fixed point is
//! the test that proves this" requirement, exercised directly against
//! `logit_proto::prometheus` with no pipeline, sink, or HTTP in between.
//!
//! Three properties, over a fixture corpus covering every wire feature
//! `docs/design/telemetry-landscape.md`'s "Prometheus exposition format / OpenMetrics" section
//! names (all five text 0.0.4 types, all four OpenMetrics-only ones, `# HELP`/`# TYPE`/`# UNIT`,
//! `_created`, exemplars, escaping, `+Inf`/`-Inf`, sample timestamps in both units, and a negative
//! pre-epoch timestamp):
//!
//! 1. **`events_to_families(families_to_events(f)) == f`** -- whole-value `PartialEq` on the family
//!    list, the model mapping's own fixed point. This is what makes
//!    `prometheus_in -> prometheus_out` a relay rather than a reinterpretation, and it holds
//!    independently of text syntax, so a future remote-write module inherits it.
//! 2. **`write(parse(x)) == canonical(x)`** on bytes, in both dialects -- the syntax layer's fixed
//!    point, stated at the level a scraper actually sees. `canonical(x)` is `x` with exactly the
//!    normalizations `logit_proto::prometheus`'s module doc permits applied: families sorted by
//!    name, series by label set, labels by name, floats shortest-round-trip, blank lines and
//!    non-metadata comments gone, `# TYPE ... untyped`/`unknown` supplied where the input had no
//!    metadata, a counter's value sample carrying `_total`. Re-running the pass on its own output
//!    changes nothing, which the third assertion in each test pins.
//! 3. **`parse(write(f)) == f`** over a `proptest` generator across the text grammar -- names, label
//!    sets, every family type, optional timestamps/`_created`/exemplars -- so the corpus above
//!    doesn't get to pick the easy cases.
//!
//! **What the fixtures deliberately leave out**, because including it would make the fixture *not*
//! a fixed point by construction rather than by a codec bug (the same discipline
//! `otlp_fixed_point.rs` applies to a `Summary`'s exemplars):
//!
//! - `NaN`, anywhere: `f64::NAN != f64::NAN`, so whole-value `PartialEq` cannot express a round trip
//!   through it. `text.rs`'s own `special_floats_round_trip_in_both_directions` covers `NaN` on the
//!   bytes, where the question is answerable.
//! - a `summary` with no `_sum`/`_count`: `logit_core::Summary` holds `sum: f64`/`count: u64`, not
//!   `Option`s, so property 1 cannot distinguish absent from zero there (the module doc's
//!   normalization list says so, and `text.rs` covers the wire behavior).
//! - cross-dialect conversion: an OpenMetrics body written as text 0.0.4 loses `_created`,
//!   `# UNIT`, exemplars and the four OpenMetrics-only types by the operator's own dialect choice.
//!   That is a *named normalization*, not a fixed point, and `text.rs`'s
//!   `openmetrics_only_*` tests pin each conversion.
//!
//! [ADR `lossless-transit`]: ../../../docs/adr/lossless-transit.md

use logit_core::{AttrMap, Exemplar, Resource, TraceRef, Value};
use logit_proto::prometheus::text::{parse, write, Dialect};
use logit_proto::prometheus::{
    events_to_families, families_to_events, FamilyType, MetricFamily, Point, PrometheusDecoder,
    PrometheusEncoder, Series,
};
use proptest::prelude::*;

const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;

/// Every text 0.0.4 feature in one body: all five types, both metadata lines, escaping, `+Inf`,
/// `-Inf`, a millisecond timestamp, a negative (pre-epoch) one, an implicitly-`untyped` family,
/// blank lines and a non-metadata comment.
const TEXT_FIXTURE: &str = concat!(
    "# HELP requests_total Total requests.\n",
    "# TYPE requests_total counter\n",
    "requests_total{code=\"200\",handler=\"/\"} 1027 1395066363000\n",
    "requests_total{code=\"500\",handler=\"/\"}    3\n",
    "\n",
    "# a comment that carries no metadata\n",
    "# HELP temperature_celsius Current temperature.\n",
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
    "weird{problem=\"division by zero\"} +Inf -3982045\n",
);

const TEXT_CANONICAL: &str = concat!(
    "# TYPE bare_metric untyped\n",
    "bare_metric 42\n",
    "# TYPE build_info untyped\n",
    "build_info{version=\"1.2.3\"} 1\n",
    "# TYPE escaped untyped\n",
    "escaped{note=\"line\\nbreak \\\"quoted\\\"\",path=\"C:\\\\tmp\"} 1\n",
    "# TYPE latency_seconds histogram\n",
    "latency_seconds_bucket{le=\"0.1\"} 1\n",
    "latency_seconds_bucket{le=\"1\"} 3\n",
    "latency_seconds_bucket{le=\"+Inf\"} 4\n",
    "latency_seconds_sum 2.5\n",
    "latency_seconds_count 4\n",
    "# HELP requests_total Total requests.\n",
    "# TYPE requests_total counter\n",
    "requests_total{code=\"200\",handler=\"/\"} 1027 1395066363000\n",
    "requests_total{code=\"500\",handler=\"/\"} 3\n",
    "# TYPE rpc_seconds summary\n",
    "rpc_seconds{quantile=\"0.5\"} 0.2\n",
    "rpc_seconds{quantile=\"0.99\"} 0.9\n",
    "rpc_seconds_sum 12.5\n",
    "rpc_seconds_count 100\n",
    "# HELP temperature_celsius Current temperature.\n",
    "# TYPE temperature_celsius gauge\n",
    "temperature_celsius{room=\"attic\"} -Inf\n",
    "temperature_celsius{room=\"kitchen\"} 21.5\n",
    "# TYPE weird untyped\n",
    "weird{problem=\"division by zero\"} +Inf -3982045\n",
);

/// Every OpenMetrics 1.0 feature in one body: the four types text 0.0.4 doesn't have, `# UNIT`,
/// `_created`, exemplars (on a `_total` and on a `_bucket`, with and without a trace reference and a
/// timestamp), fractional-second timestamps, and `# EOF`.
const OPENMETRICS_FIXTURE: &str = concat!(
    "# TYPE request_duration_seconds counter\n",
    "# UNIT request_duration_seconds seconds\n",
    "# HELP request_duration_seconds Total request duration.\n",
    "request_duration_seconds_total{code=\"200\"} 1027 1395066363.5\n",
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

const OPENMETRICS_CANONICAL: &str = concat!(
    "# TYPE build info\n",
    "# HELP build Build metadata.\n",
    "build_info{version=\"1.2.3\"} 1\n",
    "# TYPE latency_seconds histogram\n",
    "# UNIT latency_seconds seconds\n",
    "latency_seconds_bucket{le=\"0.1\"} 1 # {slow=\"no\"} 0.05\n",
    "latency_seconds_bucket{le=\"1\"} 3\n",
    "latency_seconds_bucket{le=\"+Inf\"} 4\n",
    "latency_seconds_sum 2.5\n",
    "latency_seconds_count 4\n",
    "latency_seconds_created 1605281325\n",
    "# TYPE request_duration_seconds counter\n",
    "# UNIT request_duration_seconds seconds\n",
    "# HELP request_duration_seconds Total request duration.\n",
    "request_duration_seconds_total{code=\"200\"} 1027 1395066363.5\n",
    "request_duration_seconds_created{code=\"200\"} 1605281325.123\n",
    "request_duration_seconds_total{code=\"500\"} 3 # {span_id=\"fedcba9876543210\",\
     trace_id=\"0123456789abcdef0123456789abcdef\"} 0.5 1605281325.5\n",
    "# TYPE rpc_seconds summary\n",
    "rpc_seconds{quantile=\"0.5\"} 0.2\n",
    "rpc_seconds_sum 12.5\n",
    "rpc_seconds_count 100\n",
    "rpc_seconds_created 1605281325\n",
    "# TYPE sizes gaugehistogram\n",
    "sizes_bucket{le=\"10\"} 5\n",
    "sizes_bucket{le=\"+Inf\"} 7\n",
    "sizes_gsum 300.5\n",
    "sizes_gcount 7\n",
    "# TYPE state stateset\n",
    "state{state=\"running\"} 1\n",
    "state{state=\"starting\"} 0\n",
    "# TYPE temperature_celsius gauge\n",
    "temperature_celsius{room=\"kitchen\"} 21.5\n",
    "# TYPE unannotated unknown\n",
    "unannotated 42\n",
    "# EOF\n",
);

fn rendered(families: &[MetricFamily], dialect: Dialect) -> String {
    let mut out = Vec::new();
    write(families, dialect, &mut out);
    String::from_utf8(out).expect("exposition must be utf-8")
}

/// Property 1: the model mapping is a fixed point on the family list.
fn assert_model_fixed_point(families: &[MetricFamily]) {
    let events = families_to_events(families, RECEIVED_AT, &mut PrometheusDecoder::new());
    let resource = Resource::default();
    let round_tripped = events_to_families(
        events.iter().map(|event| (&resource, event)),
        &mut PrometheusEncoder::new(),
    );
    assert_eq!(round_tripped, families, "events_to_families(families_to_events(f)) must equal f");
}

/// Properties 2 and 3 for one fixture: canonical bytes, idempotence, and the model fixed point over
/// the same corpus.
fn assert_fixture(body: &str, dialect: Dialect, canonical: &str) {
    let families = parse(body.as_bytes(), dialect).expect("fixture must parse");
    assert_eq!(rendered(&families, dialect), canonical, "write(parse(x)) must be canonical(x)");

    let reparsed = parse(canonical.as_bytes(), dialect).expect("canonical form must parse");
    assert_eq!(reparsed, families, "parsing the canonical form must give the same families");
    assert_eq!(rendered(&reparsed, dialect), canonical, "the canonical form is a fixed point");

    assert_model_fixed_point(&families);
}

#[test]
fn the_text_0_0_4_fixture_is_a_fixed_point_in_every_sense() {
    assert_fixture(TEXT_FIXTURE, Dialect::Text0_0_4, TEXT_CANONICAL);
}

#[test]
fn the_openmetrics_fixture_is_a_fixed_point_in_every_sense() {
    assert_fixture(OPENMETRICS_FIXTURE, Dialect::OpenMetrics1_0, OPENMETRICS_CANONICAL);
}

/// A hand-built family list exercising the two model-level extras no exposition body can carry on
/// its own: an exemplar with both a trace reference *and* filtered attributes, and a series that
/// carries a timestamp alongside one that doesn't.
#[test]
fn a_hand_built_family_list_is_a_model_fixed_point() {
    let mut filtered_attributes = AttrMap::new();
    filtered_attributes.insert("detail", Value::str("kept"));
    let exemplar = Exemplar {
        timestamp: 1_605_281_325_500_000_000,
        value: 0.5,
        trace: Some(TraceRef { trace_id: [7; 16], span_id: Some([6; 8]), flags: 0 }),
        filtered_attributes,
    };
    let mut counter = MetricFamily::new("requests_total", FamilyType::Counter);
    counter.help = Some("Total requests.".to_string());
    counter.unit = Some("requests".to_string());
    counter.series = vec![
        Series {
            labels: vec![("code".to_string(), "200".to_string())],
            point: Point::Counter(1027.0),
            timestamp: Some(1_395_066_363_500_000_000),
            created: Some(1_605_281_325_123_000_000),
            exemplars: vec![exemplar],
        },
        Series::new(vec![("code".to_string(), "500".to_string())], Point::Counter(3.0)),
    ];
    assert_model_fixed_point(&[counter]);
}

// -------------------------------------------------------------------------------------------------
// Property 3: a generator over the text grammar
// -------------------------------------------------------------------------------------------------

/// Characters a generated label value or `# HELP` string is built from: letters, digits, a space,
/// and the three that have to be escaped on the way out.
const INTERESTING_CHARS: [char; 9] = ['a', 'B', '7', ' ', '\\', '"', '\n', ':', '-'];

fn text_blob(max: usize) -> impl Strategy<Value = String> {
    proptest::collection::vec(proptest::sample::select(INTERESTING_CHARS.to_vec()), 0..max)
        .prop_map(|chars| chars.into_iter().collect())
}

/// `# HELP` text: non-empty, and with no leading or trailing whitespace -- a metadata line's
/// surrounding whitespace is not part of its value, so a generated one would not survive the trip
/// and would be testing the fixture, not the codec.
fn help_text() -> impl Strategy<Value = Option<String>> {
    proptest::option::of(
        text_blob(12)
            .prop_map(|s| s.trim().to_string())
            .prop_filter("help must survive trimming", |s| !s.is_empty()),
    )
}

fn label_name() -> impl Strategy<Value = String> {
    // `le` is the label a histogram generates for itself, so an attribute of that name would be
    // dropped rather than round-tripped (the codec counts it); every other shape is fair game.
    "[a-z][a-z0-9_]{0,4}".prop_filter("`le` is reserved for bucket lines", |s| s != "le")
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

/// Cumulative bucket counts with strictly increasing bounds and a trailing `+Inf`, which is what
/// both formats require and what the model mapping's successive difference assumes.
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
            // A summary with nothing at all in it is not a series; give it a count so the parser has
            // something to rebuild it from.
            let count = if quantiles.is_empty() && sum.is_none() { Some(0) } else { count };
            Point::Summary { quantiles, sum, count }
        })
}

fn point_for(kind: FamilyType) -> BoxedStrategy<Point> {
    match kind {
        FamilyType::Counter => sample_value().prop_map(Point::Counter).boxed(),
        FamilyType::Gauge => sample_value().prop_map(Point::Gauge).boxed(),
        FamilyType::Unknown | FamilyType::Untyped => {
            sample_value().prop_map(Point::Unknown).boxed()
        }
        FamilyType::Histogram | FamilyType::GaugeHistogram => histogram_point().boxed(),
        FamilyType::Summary => summary_point().boxed(),
        FamilyType::Info => Just(Point::Info).boxed(),
        FamilyType::StateSet => any::<bool>().prop_map(Point::StateSet).boxed(),
    }
}

/// A timestamp this dialect can carry exactly: whole milliseconds in text 0.0.4 (its own unit),
/// anything in OpenMetrics, which carries nanosecond-resolution decimal seconds.
fn instant(dialect: Dialect) -> BoxedStrategy<i64> {
    match dialect {
        Dialect::Text0_0_4 => {
            (-2_000_000_000i64..2_000_000_000).prop_map(|ms| ms * 1_000_000).boxed()
        }
        Dialect::OpenMetrics1_0 => {
            (-2_000_000_000_000_000_000i64..2_000_000_000_000_000_000).boxed()
        }
    }
}

fn exemplars(dialect: Dialect, kind: FamilyType) -> BoxedStrategy<Vec<Exemplar>> {
    // Exemplars are OpenMetrics-only and live on `_total`/`_bucket` lines; a counter's one exemplar
    // is the case a generator can pin without also having to reason about which bucket a value lands
    // in, which the OpenMetrics spec's own histogram example covers by hand.
    if dialect != Dialect::OpenMetrics1_0 || kind != FamilyType::Counter {
        return Just(Vec::new()).boxed();
    }
    proptest::option::of((
        (-1e6f64..1e6),
        prop_oneof![Just(0i64), instant(dialect)],
        proptest::option::of((any::<u8>(), proptest::option::of(any::<u8>()))),
        proptest::collection::vec((label_name(), text_blob(4)), 0..2),
    ))
    .prop_map(|generated| match generated {
        None => Vec::new(),
        Some((value, timestamp, ids, attrs)) => {
            let trace = ids.map(|(trace_byte, span_byte)| TraceRef {
                // Never all-zero: `TraceRef`'s own validity rule rejects that, so a generated
                // all-zero id would come back `None` and fail for the wrong reason.
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

fn series(dialect: Dialect, kind: FamilyType) -> BoxedStrategy<Series> {
    let created = if dialect == Dialect::OpenMetrics1_0 && kind.has_created() {
        proptest::option::of(instant(dialect)).boxed()
    } else {
        Just(None).boxed()
    };
    (
        labels(),
        point_for(kind),
        proptest::option::of(instant(dialect)),
        created,
        exemplars(dialect, kind),
    )
        .prop_map(|(labels, point, timestamp, created, exemplars)| Series {
            labels,
            point,
            timestamp,
            created,
            exemplars,
        })
        .boxed()
}

/// The family types this dialect can express natively. The others are written as their nearest
/// shape, which is a *normalization*, not a round trip -- see this file's module doc.
fn family_type(dialect: Dialect) -> BoxedStrategy<FamilyType> {
    match dialect {
        Dialect::Text0_0_4 => prop_oneof![
            Just(FamilyType::Counter),
            Just(FamilyType::Gauge),
            Just(FamilyType::Histogram),
            Just(FamilyType::Summary),
            Just(FamilyType::Untyped),
        ]
        .boxed(),
        Dialect::OpenMetrics1_0 => prop_oneof![
            Just(FamilyType::Counter),
            Just(FamilyType::Gauge),
            Just(FamilyType::Histogram),
            Just(FamilyType::GaugeHistogram),
            Just(FamilyType::Summary),
            Just(FamilyType::Info),
            Just(FamilyType::StateSet),
            Just(FamilyType::Unknown),
        ]
        .boxed(),
    }
}

fn family(dialect: Dialect) -> BoxedStrategy<MetricFamily> {
    let unit = if dialect == Dialect::OpenMetrics1_0 {
        proptest::option::of("[a-z]{1,6}".prop_map(|s: String| s)).boxed()
    } else {
        Just(None).boxed()
    };
    family_type(dialect)
        .prop_flat_map(move |kind| {
            (
                Just(kind),
                help_text(),
                unit.clone(),
                proptest::collection::vec(series(dialect, kind), 1..3),
            )
        })
        .prop_map(|(kind, help, unit, mut series)| {
            series.sort_by(|a, b| a.labels.cmp(&b.labels));
            series.dedup_by(|a, b| a.labels == b.labels);
            MetricFamily { name: String::new(), kind, help, unit, series }
        })
        .boxed()
}

/// A canonically-ordered family list with distinct names. Names are assigned rather than generated
/// so no two families can share one (the parser groups by name) and no family's name can collide
/// with another's generated sample names (`m0` vs `m0_sum`), which is an exposition-level naming
/// clash the OpenMetrics spec itself forbids, not a codec question.
fn family_set(dialect: Dialect) -> impl Strategy<Value = Vec<MetricFamily>> {
    proptest::collection::vec(family(dialect), 1..4).prop_map(|mut families| {
        for (i, family) in families.iter_mut().enumerate() {
            // OpenMetrics requires `_<unit>` to suffix the family name and Prometheus fails the
            // whole scrape when it doesn't, so a unit the writer would have to drop isn't a round
            // trip either -- the generated name carries the generated unit.
            let stem = match &family.unit {
                Some(unit) => format!("m{i}_{unit}"),
                None => format!("m{i}"),
            };
            family.name = match family.kind {
                // A counter's value sample always carries `_total`, so a name that lacks it would
                // gain it on the way out -- a named normalization, not a round trip.
                FamilyType::Counter => format!("{stem}_total"),
                _ => stem,
            };
        }
        families.sort_by(|a, b| a.name.cmp(&b.name));
        families
    })
}

/// [`family_set`] with the one shape the *model* mapping cannot round-trip filled in: a wire
/// `summary` may omit `_sum`/`_count`, and [`logit_core::Summary`] holds `sum: f64`/`count: u64`
/// rather than `Option`s, so "absent" and "zero" are the same model value there. That asymmetry is
/// a named normalization (see this file's module doc and the codec's own); the generator states it
/// explicitly instead of rediscovering it on every run.
fn model_family_set() -> impl Strategy<Value = Vec<MetricFamily>> {
    family_set(Dialect::OpenMetrics1_0).prop_map(|mut families| {
        for family in &mut families {
            for series in &mut family.series {
                if let Point::Summary { quantiles, sum, count } = &series.point {
                    series.point = Point::Summary {
                        quantiles: quantiles.clone(),
                        sum: Some(sum.unwrap_or(0.0)),
                        count: Some(count.unwrap_or(0)),
                    };
                }
            }
        }
        families
    })
}

proptest! {
    #[test]
    fn parsing_a_written_family_set_is_the_identity_in_text_0_0_4(
        families in family_set(Dialect::Text0_0_4),
    ) {
        let mut out = Vec::new();
        write(&families, Dialect::Text0_0_4, &mut out);
        let reparsed = parse(&out, Dialect::Text0_0_4)
            .map_err(|e| TestCaseError::fail(format!("{e} in {}", String::from_utf8_lossy(&out))))?;
        prop_assert_eq!(reparsed, families, "body was {}", String::from_utf8_lossy(&out));
    }

    #[test]
    fn parsing_a_written_family_set_is_the_identity_in_openmetrics(
        families in family_set(Dialect::OpenMetrics1_0),
    ) {
        let mut out = Vec::new();
        write(&families, Dialect::OpenMetrics1_0, &mut out);
        let reparsed = parse(&out, Dialect::OpenMetrics1_0)
            .map_err(|e| TestCaseError::fail(format!("{e} in {}", String::from_utf8_lossy(&out))))?;
        prop_assert_eq!(reparsed, families, "body was {}", String::from_utf8_lossy(&out));
    }

    /// And the model mapping over the same generated corpus -- property 1 with the fixtures' own
    /// choices taken away.
    #[test]
    fn a_generated_family_set_is_a_model_fixed_point(
        families in model_family_set(),
    ) {
        let events = families_to_events(&families, RECEIVED_AT, &mut PrometheusDecoder::new());
        let resource = Resource::default();
        let round_tripped = events_to_families(
            events.iter().map(|event| (&resource, event)),
            &mut PrometheusEncoder::new(),
        );
        prop_assert_eq!(round_tripped, families);
    }
}
