//! Pure-codec fixed-point tests for Datadog's APM stats routes (`logit_proto::datadog::stats`):
//! [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md)'s round-trip requirement,
//! exercised directly against `DatadogDecoder`/`DatadogEncoder`. The mirror of
//! `tests/datadog_metrics_fixed_point.rs`.
//!
//! Two properties, on both forms (a tracer's `/v0.6/stats` `ClientStatsPayload` and the intake's
//! `StatsPayload`):
//!
//! 1. **`decode(encode(decode(b))) == decode(b)`**, whole-batch equality, resource included.
//!    Starting from wire bytes means every permitted normalization (a DDSketch's contiguous bins
//!    re-sent sparse, an omitted key re-sent at its zero value, keys reordered) is applied once
//!    by the first decode and must be stable afterwards. Sketches compare through `DdSketch`'s
//!    structural `PartialEq`, so the round trip is bin-exact.
//! 2. **`encode(decode(encode(d))) == encode(d)` on bytes.**
//!
//! The wire bodies here are written by a test-local msgpack writer that differs from the
//! encoder on purpose: it writes keys in reverse Go field order and omits zero-valued keys, as a
//! tracer's own serializer may, so the decoder's tolerance is on the path.

use logit_core::{Bin, DdSketch, EventBatch, Mapping, MetricKind, Value};
use logit_proto::datadog::generated::ddsketch::{DdSketch as PbSketch, IndexMapping, Store};
use logit_proto::datadog::generated::trace::{
    ClientGroupedStats, ClientStatsBucket, ClientStatsPayload, StatsPayload,
};
use logit_proto::datadog::stats::{ATTR_STATS_NAME, METRIC_ERROR_SUMMARY, METRIC_OK_SUMMARY};
use logit_proto::datadog::{DatadogDecoder, DatadogEncoder};
use logit_proto::msgpack::Writer;
use proptest::prelude::*;
use prost::Message;

const RECEIVED_AT: i64 = 1_699_000_000_123_456_789;
/// `sketches-go`'s `NewLogarithmicMapping(0.01)`: `(1 + 0.01) / (1 - 0.01)`.
const GAMMA: f64 = 1.0202020202020203;
const BIN_LIMIT: u32 = 2048;

// ------------------------------------------------------------------------------------------
// A tracer-style msgpack writer: reverse field order, zero values omitted.
// ------------------------------------------------------------------------------------------

enum F<'a> {
    S(&'a str),
    U(u64),
    I(i64),
    B(bool),
    Bin(&'a [u8]),
    Strs(&'a [String]),
}

impl F<'_> {
    fn is_zero(&self) -> bool {
        match self {
            F::S(s) => s.is_empty(),
            F::U(u) => *u == 0,
            F::I(i) => *i == 0,
            F::B(b) => !b,
            F::Bin(b) => b.is_empty(),
            F::Strs(v) => v.is_empty(),
        }
    }

    fn write(&self, w: &mut Writer) {
        match self {
            F::S(s) => w.write_str(s),
            F::U(u) => w.write_u64(*u),
            F::I(i) => w.write_i64(*i),
            F::B(b) => w.write_bool(*b),
            F::Bin(b) => w.write_bin(b),
            F::Strs(v) => {
                w.write_array_len(v.len());
                for s in *v {
                    w.write_str(s);
                }
            }
        }
    }
}

/// Writes the non-zero `fields` in reverse, then `Stats` (if any) via `stats`.
fn write_map(w: &mut Writer, fields: &[(&str, F<'_>)], stats: Option<&dyn Fn(&mut Writer)>) {
    let kept: Vec<_> = fields.iter().rev().filter(|(_, f)| !f.is_zero()).collect();
    w.write_map_len(kept.len() + usize::from(stats.is_some()));
    if let Some(stats) = stats {
        w.write_str("Stats");
        stats(w);
    }
    for (k, f) in kept {
        w.write_str(k);
        f.write(w);
    }
}

fn write_group(w: &mut Writer, g: &ClientGroupedStats) {
    write_map(
        w,
        &[
            ("Service", F::S(&g.service)),
            ("Name", F::S(&g.name)),
            ("Resource", F::S(&g.resource)),
            ("HTTPStatusCode", F::U(u64::from(g.http_status_code))),
            ("Type", F::S(&g.r#type)),
            ("DBType", F::S(&g.db_type)),
            ("Hits", F::U(g.hits)),
            ("Errors", F::U(g.errors)),
            ("Duration", F::U(g.duration)),
            ("OkSummary", F::Bin(&g.ok_summary)),
            ("ErrorSummary", F::Bin(&g.error_summary)),
            ("Synthetics", F::B(g.synthetics)),
            ("TopLevelHits", F::U(g.top_level_hits)),
            ("SpanKind", F::S(&g.span_kind)),
            ("PeerTags", F::Strs(&g.peer_tags)),
            ("IsTraceRoot", F::I(i64::from(g.is_trace_root))),
            ("GRPCStatusCode", F::S(&g.grpc_status_code)),
            ("HTTPMethod", F::S(&g.http_method)),
            ("HTTPEndpoint", F::S(&g.http_endpoint)),
            ("srv_src", F::S(&g.service_source)),
            ("SpanDerivedPrimaryTags", F::Strs(&g.span_derived_primary_tags)),
            ("AdditionalMetricTags", F::Strs(&g.additional_metric_tags)),
        ],
        None,
    );
}

fn write_bucket(w: &mut Writer, b: &ClientStatsBucket) {
    let stats = |w: &mut Writer| {
        w.write_array_len(b.stats.len());
        for g in &b.stats {
            write_group(w, g);
        }
    };
    write_map(
        w,
        &[
            ("Start", F::U(b.start)),
            ("Duration", F::U(b.duration)),
            ("AgentTimeShift", F::I(b.agent_time_shift)),
        ],
        Some(&stats),
    );
}

fn write_client(w: &mut Writer, p: &ClientStatsPayload) {
    let stats = |w: &mut Writer| {
        w.write_array_len(p.stats.len());
        for b in &p.stats {
            write_bucket(w, b);
        }
    };
    write_map(
        w,
        &[
            ("Hostname", F::S(&p.hostname)),
            ("Env", F::S(&p.env)),
            ("Version", F::S(&p.version)),
            ("Lang", F::S(&p.lang)),
            ("TracerVersion", F::S(&p.tracer_version)),
            ("RuntimeID", F::S(&p.runtime_id)),
            ("Sequence", F::U(p.sequence)),
            ("AgentAggregation", F::S(&p.agent_aggregation)),
            ("Service", F::S(&p.service)),
            ("ContainerID", F::S(&p.container_id)),
            ("Tags", F::Strs(&p.tags)),
            ("GitCommitSha", F::S(&p.git_commit_sha)),
            ("ImageTag", F::S(&p.image_tag)),
            ("ProcessTagsHash", F::U(p.process_tags_hash)),
            ("ProcessTags", F::S(&p.process_tags)),
        ],
        Some(&stats),
    );
}

fn client_body(p: &ClientStatsPayload) -> Vec<u8> {
    let mut w = Writer::new();
    write_client(&mut w, p);
    w.into_inner()
}

fn stats_body(p: &StatsPayload) -> Vec<u8> {
    let mut w = Writer::new();
    let stats = |w: &mut Writer| {
        w.write_array_len(p.stats.len());
        for c in &p.stats {
            write_client(w, c);
        }
    };
    write_map(
        &mut w,
        &[
            ("AgentHostname", F::S(&p.agent_hostname)),
            ("AgentEnv", F::S(&p.agent_env)),
            ("AgentVersion", F::S(&p.agent_version)),
            ("ClientComputed", F::B(p.client_computed)),
            ("SplitPayload", F::B(p.split_payload)),
        ],
        Some(&stats),
    );
    w.into_inner()
}

/// A DDSketch protobuf the way `sketches-go`'s `ToProto` writes one: the positive store
/// contiguous, the negative one sparse.
fn pb_sketch(gamma: f64, offset: f64, positive: &[Bin], negative: &[Bin], zero: f64) -> Vec<u8> {
    let contiguous = |bins: &[Bin]| {
        let Some(first) = bins.first() else { return Store::default() };
        let last = bins.last().unwrap().key;
        let mut counts = vec![0.0; (last - first.key + 1) as usize];
        for b in bins {
            counts[(b.key - first.key) as usize] += b.count;
        }
        Store {
            contiguous_bin_counts: counts,
            contiguous_bin_index_offset: first.key,
            ..Store::default()
        }
    };
    PbSketch {
        mapping: Some(IndexMapping { gamma, index_offset: offset, interpolation: 0 }),
        positive_values: Some(contiguous(positive)),
        negative_values: Some(Store {
            bin_counts: negative.iter().map(|b| (b.key, b.count)).collect(),
            ..Store::default()
        }),
        zero_count: zero,
    }
    .encode_to_vec()
}

fn sketch_of(sketch: &DdSketch, gamma: f64, offset: f64) -> Vec<u8> {
    pb_sketch(gamma, offset, sketch.positive_bins(), sketch.negative_bins(), sketch.zero_count())
}

fn log_sketch(values: &[f64]) -> DdSketch {
    let mut s = DdSketch::with_mapping(Mapping::logarithmic(GAMMA, 0.0, BIN_LIMIT));
    for v in values {
        s.add(*v);
    }
    s
}

// ------------------------------------------------------------------------------------------
// The two properties.
// ------------------------------------------------------------------------------------------

fn decode_v06(body: &[u8]) -> EventBatch {
    DatadogDecoder::new().decode_client_stats_v06(body, RECEIVED_AT).expect("decodes")
}

fn decode_intake(body: &[u8]) -> Vec<EventBatch> {
    DatadogDecoder::new().decode_stats_payload(body, RECEIVED_AT).expect("decodes")
}

fn assert_v06_fixed_point(body: &[u8]) -> EventBatch {
    let first = decode_v06(body);
    let mut e = DatadogEncoder::new();
    let Some(encoded) = e.encode_client_stats_v06(&first) else {
        assert!(first.events.is_empty(), "stats events were not encoded");
        return first;
    };
    let second = decode_v06(&encoded);
    assert_eq!(second, first, "decode(encode(decode(b))) == decode(b)");
    let again = e.encode_client_stats_v06(&second).expect("still stats");
    assert_eq!(again, encoded, "encode(decode(encode(d))) == encode(d)");
    first
}

fn assert_intake_fixed_point(body: &[u8]) -> Vec<EventBatch> {
    let first = decode_intake(body);
    let mut e = DatadogEncoder::new();
    for batch in &first {
        let Some(encoded) = e.encode_stats_payload(batch) else {
            assert!(batch.events.is_empty(), "stats events were not encoded");
            continue;
        };
        let second = decode_intake(&encoded);
        assert_eq!(second, std::slice::from_ref(batch), "decode(encode(decode(b))) == decode(b)");
        let again = e.encode_stats_payload(&second[0]).expect("still stats");
        assert_eq!(again, encoded, "encode(decode(encode(d))) == encode(d)");
    }
    first
}

// ------------------------------------------------------------------------------------------
// Hand-written vectors.
// ------------------------------------------------------------------------------------------

/// Every grouped field set, both summaries present, the ok one contiguous-encoded.
fn full_group(name: &str, hits: u64) -> ClientGroupedStats {
    let ok = log_sketch(&[800_000.0, 1_200_000.0, 1_250_000.0, 9_000_000.0]);
    let err = log_sketch(&[30_000_000.0]);
    ClientGroupedStats {
        service: "web-store".into(),
        name: name.into(),
        resource: "GET /products/:id".into(),
        http_status_code: 200,
        r#type: "web".into(),
        db_type: "postgresql".into(),
        hits,
        errors: 1,
        duration: 12_250_000,
        ok_summary: sketch_of(&ok, GAMMA, 0.0),
        error_summary: sketch_of(&err, GAMMA, 0.0),
        synthetics: true,
        top_level_hits: hits - 1,
        span_kind: "server".into(),
        peer_tags: vec!["peer.service:db".into(), "db.instance:main".into()],
        is_trace_root: 1,
        grpc_status_code: "0".into(),
        http_method: "GET".into(),
        http_endpoint: "/products/:id".into(),
        service_source: "opt.service_mapping".into(),
        span_derived_primary_tags: vec!["region:eu-west-1".into()],
        additional_metric_tags: vec!["team:checkout".into()],
    }
}

fn two_bucket_client() -> ClientStatsPayload {
    ClientStatsPayload {
        hostname: "web-1".into(),
        env: "prod".into(),
        version: "2.4.1".into(),
        stats: vec![
            ClientStatsBucket {
                start: 1_700_000_000_000_000_000,
                duration: 10_000_000_000,
                stats: vec![
                    full_group("http.request", 5),
                    ClientGroupedStats {
                        service: "web-store".into(),
                        name: "postgres.query".into(),
                        resource: "SELECT ?".into(),
                        r#type: "sql".into(),
                        hits: 9,
                        duration: 900_000,
                        is_trace_root: 2,
                        span_kind: "client".into(),
                        ..Default::default()
                    },
                ],
                agent_time_shift: 0,
            },
            ClientStatsBucket {
                start: 1_700_000_010_000_000_000,
                duration: 10_000_000_000,
                stats: vec![full_group("http.request", 3)],
                agent_time_shift: -2_000_000_000,
            },
        ],
        lang: "python".into(),
        tracer_version: "2.9.0".into(),
        runtime_id: "c0ffee00-0000-4000-8000-000000000001".into(),
        sequence: 17,
        agent_aggregation: "distributions".into(),
        service: "web-store".into(),
        container_id: "3f0a8e".into(),
        tags: vec!["env:prod".into(), "version:2.4.1".into()],
        git_commit_sha: "a1b2c3d".into(),
        image_tag: "v2.4.1".into(),
        process_tags_hash: 0xdead_beef,
        process_tags: "entrypoint.name:gunicorn".into(),
    }
}

#[test]
fn v06_client_stats_with_two_buckets_is_a_fixed_point() {
    let batch = assert_v06_fixed_point(&client_body(&two_bucket_client()));
    assert_eq!(batch.events.len(), 3);
    assert_eq!(batch.events[2].timestamp, 1_700_000_010_000_000_000);
    let first = &batch.events[0];
    assert_eq!(first.metrics.len(), 6);
    let summary = |name: &str| {
        first
            .metrics
            .iter()
            .find(|m| logit_core::interner::resolve(m.name) == name)
            .map(|m| match &m.kind {
                MetricKind::Distribution(d) => d.clone(),
                other => panic!("{name}: {other:?}"),
            })
            .unwrap()
    };
    // Bin-exact: the contiguous positive store decodes to the sketch's own bins.
    let ok = log_sketch(&[800_000.0, 1_200_000.0, 1_250_000.0, 9_000_000.0]);
    assert_eq!(summary(METRIC_OK_SUMMARY).positive_bins(), ok.positive_bins());
    assert_eq!(summary(METRIC_ERROR_SUMMARY).count(), 1);
    // The second group left most fields empty: only what's set, plus its name, is carried.
    let second = &batch.events[1];
    assert_eq!(second.metrics.len(), 4);
    assert_eq!(second.attributes.get(ATTR_STATS_NAME), Some(&Value::str("postgres.query")));
}

#[test]
fn intake_stats_payload_with_two_clients_is_a_fixed_point() {
    let mut other = two_bucket_client();
    other.hostname = "worker-1".into();
    other.lang = "go".into();
    other.stats.truncate(1);
    let payload = StatsPayload {
        agent_hostname: "agent-7".into(),
        agent_env: "prod".into(),
        stats: vec![two_bucket_client(), other],
        agent_version: "7.83.3".into(),
        client_computed: true,
        split_payload: false,
    };
    let batches = assert_intake_fixed_point(&stats_body(&payload));
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].events.len(), 3);
    assert_eq!(batches[1].events.len(), 2);
    for b in &batches {
        let a = &b.resource.attributes;
        assert_eq!(a.get("datadog.agent.hostname"), Some(&Value::str("agent-7")));
        assert_eq!(a.get("datadog.stats.client_computed"), Some(&Value::Bool(true)));
        assert_eq!(a.get("datadog.stats.split_payload"), None);
    }
}

#[test]
fn the_same_client_payload_decodes_alike_on_both_forms() {
    let client = two_bucket_client();
    let v06 = decode_v06(&client_body(&client));
    let intake =
        decode_intake(&stats_body(&StatsPayload { stats: vec![client], ..Default::default() }));
    // An envelope with every field empty adds nothing to the resource.
    assert_eq!(intake, [v06]);
}

#[test]
fn an_agent_mapped_sketch_relays_as_its_logarithmic_reading() {
    let mut agent = DdSketch::new();
    for v in [0.5, 3.0, 3.0, 1e6] {
        agent.add(v);
    }
    let offset = Mapping::agent().index_offset() + 0.5;
    let mut client = two_bucket_client();
    client.stats[0].stats[0].ok_summary = sketch_of(&agent, Mapping::AGENT_GAMMA, offset);
    let batch = assert_v06_fixed_point(&client_body(&client));
    let MetricKind::Distribution(ok) = &batch.events[0].metrics[4].kind else { panic!() };
    assert_eq!(ok.positive_bins(), agent.positive_bins(), "keys unchanged");
    assert_eq!(*ok.mapping(), Mapping::logarithmic(Mapping::AGENT_GAMMA, offset, BIN_LIMIT));
}

// ------------------------------------------------------------------------------------------
// Property tests.
// ------------------------------------------------------------------------------------------

fn text() -> impl Strategy<Value = String> {
    prop_oneof![Just(String::new()), "[a-z./:_ -]{1,12}"]
}

fn texts() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-z]{1,6}:[a-z0-9]{1,6}", 0..3)
}

/// `(wire bytes, the model sketch they came from, whether it's logarithmic-mapped)`, or empty.
fn summary() -> impl Strategy<Value = (Vec<u8>, Option<DdSketch>)> {
    let values =
        prop::collection::vec(prop_oneof![-1e9..-1e-3f64, Just(0.0), 1e-3..1e12f64], 0..40);
    prop_oneof![
        1 => Just((Vec::new(), None)),
        3 => (values.clone(), 1.001..1.2f64, -5.0..5.0f64).prop_map(|(vs, gamma, offset)| {
            let mut s = DdSketch::with_mapping(Mapping::logarithmic(gamma, offset, BIN_LIMIT));
            for v in vs {
                s.add(v);
            }
            (sketch_of(&s, gamma, offset), Some(s))
        }),
        2 => values.prop_map(|vs| {
            let mut s = DdSketch::new();
            for v in vs {
                s.add(v);
            }
            let bytes = sketch_of(&s, Mapping::AGENT_GAMMA, Mapping::agent().index_offset() + 0.5);
            (bytes, None)
        }),
    ]
}

const MAX_EXACT: u64 = 1 << 53;

prop_compose! {
    fn group()(
        strings in prop::collection::vec(text(), 9),
        name in "[a-z.]{0,10}",
        http_status_code in prop_oneof![Just(0u32), 100..600u32],
        counts in prop::collection::vec(0..MAX_EXACT, 4),
        ok in summary(),
        err in summary(),
        synthetics: bool,
        is_trace_root in 0..3i32,
        peer_tags in texts(),
        span_derived_primary_tags in texts(),
        additional_metric_tags in texts(),
    ) -> (ClientGroupedStats, Vec<DdSketch>) {
        let sketches = ok.1.iter().chain(err.1.iter()).cloned().collect();
        let g = ClientGroupedStats {
            service: strings[0].clone(),
            name,
            resource: strings[1].clone(),
            http_status_code,
            r#type: strings[2].clone(),
            db_type: strings[3].clone(),
            hits: counts[0],
            errors: counts[1],
            duration: counts[2],
            ok_summary: ok.0,
            error_summary: err.0,
            synthetics,
            top_level_hits: counts[3],
            span_kind: strings[4].clone(),
            peer_tags,
            is_trace_root,
            grpc_status_code: strings[5].clone(),
            http_method: strings[6].clone(),
            http_endpoint: strings[7].clone(),
            service_source: strings[8].clone(),
            span_derived_primary_tags,
            additional_metric_tags,
        };
        (g, sketches)
    }
}

prop_compose! {
    fn bucket()(
        start in 0..i64::MAX as u64,
        duration in prop_oneof![Just(10_000_000_000u64), any::<u64>()],
        agent_time_shift in prop_oneof![Just(0i64), any::<i64>()],
        groups in prop::collection::vec(group(), 0..4),
    ) -> (ClientStatsBucket, Vec<DdSketch>) {
        let sketches = groups.iter().flat_map(|(_, s)| s.clone()).collect();
        let stats = groups.into_iter().map(|(g, _)| g).collect();
        (ClientStatsBucket { start, duration, stats, agent_time_shift }, sketches)
    }
}

prop_compose! {
    fn client()(
        strings in prop::collection::vec(text(), 12),
        sequence in prop_oneof![Just(0u64), any::<u64>()],
        process_tags_hash in prop_oneof![Just(0u64), any::<u64>()],
        tags in texts(),
        buckets in prop::collection::vec(bucket(), 0..4),
    ) -> (ClientStatsPayload, Vec<DdSketch>) {
        let sketches = buckets.iter().flat_map(|(_, s)| s.clone()).collect();
        let p = ClientStatsPayload {
            hostname: strings[0].clone(),
            env: strings[1].clone(),
            version: strings[2].clone(),
            stats: buckets.into_iter().map(|(b, _)| b).collect(),
            lang: strings[3].clone(),
            tracer_version: strings[4].clone(),
            runtime_id: strings[5].clone(),
            sequence,
            agent_aggregation: strings[6].clone(),
            service: strings[7].clone(),
            container_id: strings[8].clone(),
            tags,
            git_commit_sha: strings[9].clone(),
            image_tag: strings[10].clone(),
            process_tags_hash,
            process_tags: strings[11].clone(),
        };
        (p, sketches)
    }
}

/// Every logarithmic-mapped model sketch shows up, bin-for-bin and under its own mapping, among
/// the decoded summaries.
fn assert_log_sketches_survive(batches: &[EventBatch], sketches: &[DdSketch]) {
    let decoded: Vec<&DdSketch> = batches
        .iter()
        .flat_map(|b| b.events.iter())
        .flat_map(|e| e.metrics.iter())
        .filter_map(|m| match &m.kind {
            MetricKind::Distribution(d) => Some(d),
            _ => None,
        })
        .collect();
    for s in sketches {
        assert!(
            decoded.iter().any(|d| d.mapping() == s.mapping()
                && d.positive_bins() == s.positive_bins()
                && d.negative_bins() == s.negative_bins()
                && d.zero_count() == s.zero_count()),
            "a logarithmic sketch lost bins: {s:?}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn generated_v06_stats_are_a_fixed_point((client, sketches) in client()) {
        let batch = assert_v06_fixed_point(&client_body(&client));
        let groups: usize = client.stats.iter().map(|b| b.stats.len()).sum();
        prop_assert_eq!(batch.events.len(), groups);
        assert_log_sketches_survive(std::slice::from_ref(&batch), &sketches);
    }

    #[test]
    fn generated_intake_stats_are_a_fixed_point(
        clients in prop::collection::vec(client(), 1..3),
        agent_hostname in text(),
        agent_env in text(),
        agent_version in text(),
        client_computed: bool,
        split_payload: bool,
    ) {
        let sketches: Vec<DdSketch> = clients.iter().flat_map(|(_, s)| s.clone()).collect();
        let payload = StatsPayload {
            agent_hostname,
            agent_env,
            stats: clients.into_iter().map(|(c, _)| c).collect(),
            agent_version,
            client_computed,
            split_payload,
        };
        let batches = assert_intake_fixed_point(&stats_body(&payload));
        prop_assert_eq!(batches.len(), payload.stats.len());
        assert_log_sketches_survive(&batches, &sketches);
    }
}
