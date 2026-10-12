//! `kafka_out` under `format: otlp_proto` and `otlp_json` against the in-process fake broker: each signal on its
//! topic as a valid export request, the multi-topic facade's verdict and postures across topics,
//! the keys, the size cap, and the counters (`docs/adr/kafka-otlp-formats.md`).

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use kafka_protocol::indexmap::IndexMap;
use logit_core::{
    AttrMap, Diagnostics, Event, EventBatch, LogRecord, MetricKind, MetricRecord, Resource,
    SpanRecord, TraceRef, Value,
};
use logit_pipeline::test_util::{TelemetryProbe, RECV_TIMEOUT};
use logit_pipeline::{BatchContext, Fault, Output, SeqId};
use logit_proto::otlp::generated::opentelemetry::proto::logs::v1::LogsData;
use logit_proto::otlp::generated::opentelemetry::proto::metrics::v1::MetricsData;
use logit_proto::otlp::generated::opentelemetry::proto::trace::v1::TracesData;
use logit_proto::otlp::{OtlpDecoder, OtlpEncoder, OtlpEncoding};
use logit_proto::{Signal, SignalDecoder, SignalEncoder};
use prost::Message;

use super::*;
use crate::kafka::batch::value_budget;
use crate::kafka::fake_broker::{Action, FakeCluster, Step};
use crate::kafka::partitioner::partition_for_key;
use crate::test_support::{at_most_once, fast_retry, sums_through_write_loop, Sums};

const LOGS: &str = "otlp_logs";
const METRICS: &str = "otlp_metrics";
const SPANS: &str = "otlp_spans";
const TS: i64 = 1_791_739_946_221_123_456;

/// A one-node cluster holding `topics`, each with `partitions` partitions.
async fn cluster(topics: &[&str], partitions: usize) -> FakeCluster {
    let cluster = FakeCluster::start(1).await;
    for topic in topics {
        cluster.state().add_topic(topic, &vec![0; partitions]);
    }
    cluster
}

fn settings(cluster: &FakeCluster) -> KafkaSettings {
    KafkaSettings {
        timeout: RECV_TIMEOUT,
        format: RecordFormat::OtlpProto,
        topics: Some(SignalTopics::default()),
        ..KafkaSettings::new(vec![cluster.addr(0)], "")
    }
}

fn json_settings(cluster: &FakeCluster) -> KafkaSettings {
    KafkaSettings { format: RecordFormat::OtlpJson, ..settings(cluster) }
}

fn json_encoder() -> OtlpEncoder {
    OtlpEncoder::new().with_encoding(OtlpEncoding::Json)
}

/// The log bodies of an OTLP/JSON logs request, read back by `otlp_in`'s decoder.
fn json_log_bodies(value: &[u8]) -> Vec<Value> {
    OtlpDecoder::new()
        .decode_signal_json(Signal::Logs, Bytes::copy_from_slice(value))
        .expect("a valid OTLP/JSON logs request")
        .into_iter()
        .flat_map(|batch| batch.events)
        .map(|event| event.log.expect("a log").message)
        .collect()
}

fn sink(settings: KafkaSettings, probe: &TelemetryProbe) -> KafkaOutput {
    let telemetry = probe.telemetry("out", "kafka_out", "sink");
    KafkaOutput::new(settings)
        .unwrap()
        .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
        .with_telemetry(telemetry)
}

fn log_record(message: &str, trace: Option<[u8; 16]>) -> LogRecord {
    LogRecord {
        message: Value::str(message),
        severity: None,
        body_format: logit_core::BodyFormat::Raw,
        trace: trace.map(|trace_id| TraceRef { trace_id, span_id: None, flags: 0 }),
        event_name: None,
        // A fixed observed time keeps a request's length the same on every encode.
        observed_timestamp: 1,
        dropped_attributes_count: 0,
    }
}

fn log(message: &str, trace: Option<[u8; 16]>) -> Event {
    Event::log(TS, AttrMap::new(), log_record(message, trace))
}

fn span_record(trace_id: [u8; 16], span: u8) -> SpanRecord {
    SpanRecord {
        trace_id,
        span_id: [span; 8],
        parent_span_id: None,
        name: Value::str("op"),
        kind: logit_core::SpanKind::Internal,
        status: logit_core::SpanStatus::Ok,
        events: Vec::new(),
        links: Vec::new(),
        end_timestamp: TS + 1,
        flags: 0,
        ext: None,
    }
}

fn span(trace_id: [u8; 16], span: u8) -> Event {
    Event::span(TS, AttrMap::new(), span_record(trace_id, span))
}

fn metric(kind: MetricKind) -> Event {
    Event::metric(TS, AttrMap::new(), MetricRecord::new(logit_core::interner::intern("m"), kind))
}

fn batch(events: Vec<Event>) -> EventBatch {
    let mut resource = Resource::default();
    resource.attributes.insert("service.name", Value::str("api"));
    EventBatch { resource: Arc::new(resource), scope: None, events }
}

fn mixed() -> EventBatch {
    batch(vec![log("a", None), metric(MetricKind::Gauge(1.0)), span([7; 16], 1), log("b", None)])
}

fn hex(id: [u8; 16]) -> Vec<u8> {
    let mut text = String::new();
    logit_core::trace::push_hex(&mut text, &id);
    text.into_bytes()
}

/// Every record on `topic`, across its partitions: `(partition, key, value, timestamp)`.
fn records_on(cluster: &FakeCluster, topic: &str) -> Vec<(i32, Option<Bytes>, Bytes, i64)> {
    let state = cluster.state();
    let Some(partitions) = state.topics.get(topic) else { return Vec::new() };
    let mut out = Vec::new();
    for (p, partition) in partitions.iter().enumerate() {
        for r in partition.records() {
            out.push((p as i32, r.key.clone(), r.value.clone().unwrap(), r.timestamp));
        }
    }
    out
}

/// How many record batches the broker received for `topic`.
fn received_on(cluster: &FakeCluster, topic: &str) -> usize {
    cluster.state().received.iter().filter(|r| r.topic == topic).count()
}

fn sum(sums: &Sums, name: &str, tags: &[(&str, &str)]) -> f64 {
    sums.iter()
        .filter(|((n, t), _)| {
            n == name && tags.iter().all(|(k, v)| t.iter().any(|(tk, tv)| tk == k && tv == v))
        })
        .map(|(_, v)| v)
        .sum()
}

async fn run(out: &mut KafkaOutput, probe: &mut TelemetryProbe, batches: Vec<EventBatch>) -> Sums {
    sums_through_write_loop(out, probe, "kafka_out", batches, fast_retry()).await
}

fn log_bodies(value: &[u8]) -> Vec<String> {
    let data = LogsData::decode(value).expect("a LogsData request");
    data.resource_logs
        .iter()
        .flat_map(|r| &r.scope_logs)
        .flat_map(|s| &s.log_records)
        .map(|r| match r.body.as_ref().and_then(|b| b.value.as_ref()) {
            Some(
                logit_proto::otlp::generated::opentelemetry::proto::common::v1::any_value::Value::StringValue(s),
            ) => s.clone(),
            other => panic!("a string body, got {other:?}"),
        })
        .collect()
}

/// Each signal's records land on its own default topic, each value an export request that
/// decodes to what `otlp_out`'s encoder writes for the batch, stamped with the time it was
/// written rather than the events' time.
#[tokio::test]
async fn each_signal_goes_to_its_topic_as_the_request_otlp_out_writes() {
    let cluster = cluster(&[LOGS, METRICS, SPANS], 1).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(settings(&cluster), &probe);
    let input = mixed();
    let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let sums = run(&mut out, &mut probe, vec![input.clone()]).await;
    let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

    let want = OtlpEncoder::new().encode_signals(&input).unwrap();
    for payload in want {
        let topic = match payload.signal {
            Signal::Logs => LOGS,
            Signal::Metrics => METRICS,
            Signal::Traces => SPANS,
        };
        let records = records_on(&cluster, topic);
        assert_eq!(records.len(), 1, "{topic}");
        let (_, key, value, timestamp) = &records[0];
        assert_eq!(*key, None, "{topic}: no key by default");
        assert!((before..=after).contains(timestamp), "{topic}: produce time, got {timestamp}");
        match payload.signal {
            Signal::Logs => {
                assert_eq!(
                    LogsData::decode(value.clone()).unwrap(),
                    LogsData::decode(payload.bytes.clone()).unwrap()
                );
            }
            Signal::Metrics => assert_eq!(
                MetricsData::decode(value.clone()).unwrap(),
                MetricsData::decode(payload.bytes.clone()).unwrap()
            ),
            Signal::Traces => assert_eq!(
                TracesData::decode(value.clone()).unwrap(),
                TracesData::decode(payload.bytes.clone()).unwrap()
            ),
        }
    }
    assert_eq!(sum(&sums, "logit.output.records", &[]), 3.0, "one record per signal");
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "ok")]), 1.0);
}

/// `topic:` under an OTLP format sends every signal to that one topic.
#[tokio::test]
async fn one_topic_takes_every_signal() {
    let cluster = cluster(&["all"], 1).await;
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { topic: "all".into(), topics: None, ..settings(&cluster) }, &probe);
    run(&mut out, &mut probe, vec![mixed()]).await;
    let records = records_on(&cluster, "all");
    assert_eq!(records.len(), 3);
    assert_eq!(out.producer.opened(), vec!["all".to_string()], "one producer for the topic");
}

/// A topic's producer opens on its first record: a logs-only pipeline never asks the broker
/// about the metrics or spans topic, so a broker that auto-creates topics never creates them.
#[tokio::test]
async fn a_signal_that_never_arrives_never_resolves_its_topic() {
    let cluster = cluster(&[LOGS], 1).await;
    cluster.state().auto_create = Some(1);
    let mut probe = TelemetryProbe::new();
    let mut out = sink(settings(&cluster), &probe);
    run(&mut out, &mut probe, vec![batch(vec![log("a", None), log("b", None)])]).await;
    let state = cluster.state();
    assert!(state.metadata_topics.iter().all(|t| t == LOGS), "{:?}", state.metadata_topics);
    assert_eq!(state.topics.keys().collect::<Vec<_>>(), vec![LOGS], "nothing auto-created");
    drop(state);
    assert_eq!(out.producer.opened(), vec![LOGS.to_string()]);
}

/// Logs accepted and metrics refused: the retry resends the metrics only, and every record is
/// counted once.
#[tokio::test]
async fn a_retry_resends_only_the_topic_the_broker_didnt_settle() {
    let cluster = cluster(&[LOGS, METRICS, SPANS], 1).await;
    cluster.script(
        Step::produce(None, Action::Error(fault::code::TOPIC_AUTHORIZATION_FAILED))
            .on_topic(METRICS),
    );
    let mut probe = TelemetryProbe::new();
    let mut out = sink(settings(&cluster), &probe);
    let sums = run(&mut out, &mut probe, vec![mixed()]).await;
    assert_eq!(received_on(&cluster, LOGS), 1, "logs sent once");
    assert_eq!(received_on(&cluster, SPANS), 1, "spans sent once");
    assert_eq!(received_on(&cluster, METRICS), 2, "metrics refused, then resent");
    assert_eq!(records_on(&cluster, LOGS).len(), 1);
    assert_eq!(records_on(&cluster, METRICS).len(), 1);
    assert_eq!(sum(&sums, "logit.output.records", &[]), 3.0);
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "refused")]), 1.0);
}

/// A topic whose prepare fails isn't read as success because another topic's records were
/// accepted: the send fails with the prepare's class, and the next attempt prepares only that
/// topic and resends nothing the broker took.
#[tokio::test]
async fn a_topic_that_failed_to_prepare_holds_the_send() {
    let cluster = cluster(&[LOGS, METRICS], 1).await;
    cluster.script(Step::metadata(METRICS, Action::Error(fault::code::TOPIC_AUTHORIZATION_FAILED)));
    let probe = TelemetryProbe::new();
    let mut out = sink(settings(&cluster), &probe);
    let input = batch(vec![log("a", None), metric(MetricKind::Gauge(1.0))]);
    out.observe_batch(BatchContext::default(), SeqId { id: [0; 16], seq: 1 });
    let err = out.send(&input).await.expect_err("the metrics topic didn't prepare");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
    assert_eq!(records_on(&cluster, LOGS).len(), 1, "the logs went out on the first attempt");
    assert!(out.pending.as_ref().unwrap().prepared(METRICS).is_none());

    out.send(&input).await.expect("the metrics topic prepares on the retry");
    assert_eq!(received_on(&cluster, LOGS), 1, "logs not resent");
    assert_eq!(records_on(&cluster, METRICS).len(), 1);
}

/// One topic `Rejected` and another `Clean` and unsettled is a retryable `Clean`, never a
/// `Rejected` that would drop the batch while the other topic can still deliver.
#[tokio::test]
async fn a_rejected_topic_beside_an_unsettled_one_is_retried() {
    let cluster = cluster(&[LOGS, METRICS], 1).await;
    cluster.script(Step::produce(None, Action::Error(fault::code::CORRUPT_MESSAGE)).on_topic(LOGS));
    cluster.script(
        Step::produce(None, Action::Error(fault::code::NOT_ENOUGH_REPLICAS)).on_topic(METRICS),
    );
    let probe = TelemetryProbe::new();
    let mut out = sink(settings(&cluster), &probe);
    let input = batch(vec![log("a", None), metric(MetricKind::Gauge(1.0))]);
    out.observe_batch(BatchContext::default(), SeqId { id: [0; 16], seq: 1 });
    let err = out.send(&input).await.expect_err("the metrics topic is unsettled");
    assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
    out.send(&input).await.expect("the metrics are accepted on the retry");
    assert_eq!(records_on(&cluster, METRICS).len(), 1);
    assert_eq!(received_on(&cluster, LOGS), 1, "the rejected logs aren't resent");
}

/// Under `at_most_once`, an `Ambiguous` logs topic beside a `Refused` metrics topic is dropped
/// and counted, and the retry resends only the metrics.
#[tokio::test]
async fn at_most_once_drops_an_ambiguous_topic_beside_a_refused_one() {
    let cluster = cluster(&[LOGS, METRICS], 1).await;
    cluster.script(
        Step::produce(None, Action::AppendThenError(fault::code::REQUEST_TIMED_OUT)).on_topic(LOGS),
    );
    cluster.script(
        Step::produce(None, Action::Error(fault::code::TOPIC_AUTHORIZATION_FAILED))
            .on_topic(METRICS),
    );
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings { acks: KafkaAcks::Leader, idempotent: false, ..settings(&cluster) },
        &probe,
    );
    let input = batch(vec![log("a", None), log("b", None), metric(MetricKind::Gauge(1.0))]);
    let sums =
        sums_through_write_loop(&mut out, &mut probe, "kafka_out", vec![input], at_most_once())
            .await;
    assert_eq!(received_on(&cluster, LOGS), 1, "the ambiguous logs aren't resent");
    assert_eq!(received_on(&cluster, METRICS), 2);
    assert_eq!(
        sum(&sums, "logit.output.records.dropped", &[("reason", "ambiguous_at_most_once")]),
        1.0,
        "one logs record"
    );
    assert_eq!(sum(&sums, "logit.output.records", &[]), 1.0, "the metrics record");
}

/// Under `at_most_once`, a final `Ambiguous` send counts every topic's unsettled records lost.
#[tokio::test]
async fn at_most_once_counts_every_topics_lost_records() {
    let cluster = cluster(&[LOGS, SPANS], 1).await;
    cluster.script(Step::produce(None, Action::Close).on_topic(LOGS));
    cluster.script(Step::produce(None, Action::Close).on_topic(SPANS));
    let mut probe = TelemetryProbe::new();
    let mut out = sink(settings(&cluster), &probe);
    let input = batch(vec![log("a", None), span([1; 16], 1)]);
    let sums =
        sums_through_write_loop(&mut out, &mut probe, "kafka_out", vec![input], at_most_once())
            .await;
    assert!(out.pending.is_none());
    assert_eq!(
        sum(&sums, "logit.output.records.dropped", &[("reason", "ambiguous_at_most_once")]),
        2.0
    );
}

/// `key: trace_id` splits logs and spans into a request per trace id, each keyed with the id as
/// 32 lowercase hex characters, from that signal's own field: a log's `TraceRef`, a span's own
/// id. The rest go out unkeyed and count once per record; metrics stay one unkeyed request and
/// don't count.
#[tokio::test]
async fn trace_id_keys_each_signals_request_by_its_own_trace_id() {
    let (a, b, c, d) = ([0xa1; 16], [0xb2; 16], [0xc3; 16], [0xd4; 16]);
    let cluster = cluster(&[LOGS, METRICS, SPANS], 4).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(KafkaSettings { key: RecordKey::TraceId, ..settings(&cluster) }, &probe);
    let mut both = log("both", Some(c));
    both.span = Some(span_record(d, 9));
    let input = batch(vec![
        log("a1", Some(a)),
        log("b1", Some(b)),
        log("none", None),
        log("zero", Some([0; 16])),
        log("a2", Some(a)),
        span(a, 1),
        both,
        metric(MetricKind::Gauge(1.0)),
    ]);
    let sums = run(&mut out, &mut probe, vec![input]).await;

    let by_key = |topic| {
        let mut got: Vec<(Option<Vec<u8>>, i32, Bytes)> = records_on(&cluster, topic)
            .into_iter()
            .map(|(p, k, v, _)| (k.map(|k| k.to_vec()), p, v))
            .collect();
        got.sort_by(|x, y| x.0.cmp(&y.0));
        got
    };
    let logs = by_key(LOGS);
    let keyed: Vec<(Option<Vec<u8>>, Vec<String>)> =
        logs.iter().map(|(k, _, v)| (k.clone(), log_bodies(v))).collect();
    assert_eq!(
        keyed,
        vec![
            (None, vec!["none".into(), "zero".into()]),
            (Some(hex(a)), vec!["a1".into(), "a2".into()]),
            (Some(hex(b)), vec!["b1".into()]),
            (Some(hex(c)), vec!["both".into()]),
        ]
    );
    assert_eq!(hex(a), b"a1".repeat(16), "32 lowercase hex characters");
    for (key, partition, _) in &logs {
        if let Some(key) = key {
            assert_eq!(*partition, partition_for_key(key, 4), "the Java partition for the key");
        }
    }
    let spans: Vec<Option<Vec<u8>>> = by_key(SPANS).into_iter().map(|(k, _, _)| k).collect();
    assert_eq!(spans, vec![Some(hex(a)), Some(hex(d))], "a span keys by its own trace id");
    let metrics = by_key(METRICS);
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].0, None);
    assert_eq!(sum(&sums, "logit.output.records.unkeyed", &[]), 1.0, "the unkeyed logs record");
}

/// `key: {resource: NAME}` keys every request of a batch with the resource attribute's text;
/// a resource without it sends each request unkeyed, counted.
#[tokio::test]
async fn a_resource_key_keys_every_request_alike() {
    let cluster = cluster(&[LOGS, METRICS, SPANS], 3).await;
    let mut probe = TelemetryProbe::new();
    let key = RecordKey::Resource("service.name".into());
    let mut out = sink(KafkaSettings { key, ..settings(&cluster) }, &probe);
    let mut bare = mixed();
    bare.resource = Arc::new(Resource::default());
    let sums = run(&mut out, &mut probe, vec![mixed(), bare]).await;
    for topic in [LOGS, METRICS, SPANS] {
        let keys: Vec<Option<Bytes>> =
            records_on(&cluster, topic).into_iter().map(|(_, k, _, _)| k).collect();
        assert!(keys.contains(&Some(Bytes::from_static(b"api"))), "{topic}: {keys:?}");
        assert!(keys.contains(&None), "{topic}: {keys:?}");
    }
    assert_eq!(sum(&sums, "logit.output.records.unkeyed", &[]), 3.0);
}

/// The encoder's budget leaves room for the record's key: a keyed request that fits the value
/// budget only without its key is split by the encoder, so the producer never drops it as over
/// `max_request_bytes`.
#[tokio::test]
async fn a_keyed_request_fits_the_budget_with_its_key() {
    let id = [0x5a; 16];
    let input = batch(vec![log("first", Some(id)), log("second", Some(id))]);
    let whole = OtlpEncoder::new()
        .request(Signal::Logs, &input.resource, None, &input.events)
        .unwrap()
        .encoded_len();
    let overhead = 10_000 - value_budget(10_000, &IndexMap::new());
    // The unkeyed budget holds the whole request; less the 32-byte key, it doesn't.
    let cap = whole + 10 + overhead;
    let cluster = cluster(&[LOGS], 1).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings { key: RecordKey::TraceId, max_request_bytes: cap, ..settings(&cluster) },
        &probe,
    );
    let sums = run(&mut out, &mut probe, vec![input]).await;
    let bodies: Vec<Vec<String>> =
        records_on(&cluster, LOGS).iter().map(|(_, _, v, _)| log_bodies(v)).collect();
    assert_eq!(bodies, vec![vec!["first".to_string()], vec!["second".to_string()]]);
    assert_eq!(sum(&sums, "logit.output.records.dropped", &[("reason", "oversize")]), 0.0);
}

/// A request over the budget goes out as several, each the longest run of the remaining events
/// that fits, every one a valid request with the events in order. An event too large alone is
/// dropped and counted once per signal record it would have formed.
#[tokio::test]
async fn an_oversize_request_is_split_and_an_event_too_large_alone_is_dropped() {
    let mut events: Vec<Event> = (0..20).map(|i| log(&format!("event {i:02}"), None)).collect();
    let mut huge = log("huge", None);
    huge.attributes.insert("blob", Value::str("x".repeat(4000)));
    huge.span = Some(span_record([3; 16], 3));
    events.insert(7, huge);
    let input = batch(events);
    let one = OtlpEncoder::new()
        .request(Signal::Logs, &input.resource, None, &input.events[..1])
        .unwrap()
        .encoded_len();
    let overhead = 10_000 - value_budget(10_000, &IndexMap::new());
    let budget = 4 * one;
    let cluster = cluster(&[LOGS, SPANS], 1).await;
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { max_request_bytes: budget + overhead, ..settings(&cluster) }, &probe);
    let sums = run(&mut out, &mut probe, vec![input]).await;

    let records = records_on(&cluster, LOGS);
    assert!(records.len() > 4, "split into several: {}", records.len());
    let mut bodies = Vec::new();
    for (_, _, value, _) in &records {
        assert!(value.len() <= budget, "{} over {budget}", value.len());
        bodies.extend(log_bodies(value));
    }
    let want: Vec<String> = (0..20).map(|i| format!("event {i:02}")).collect();
    assert_eq!(bodies, want, "every event but the huge one, in order");
    assert!(records_on(&cluster, SPANS).is_empty(), "the huge event's span is over too");
    assert_eq!(
        sum(&sums, "logit.output.records.dropped", &[("reason", "oversize")]),
        2.0,
        "one per signal of the huge event"
    );
    assert_eq!(sum(&sums, "logit.output.records", &[]), records.len() as f64);
}

/// The OTLP mapping's side-effect counters count each event once: not again for the bisect's
/// probes, and not again when a topic whose prepare failed is prepared on the retry.
#[tokio::test]
async fn the_mappings_counters_count_once_across_probes_and_a_retried_prepare() {
    let mut events = Vec::new();
    for i in 0..12 {
        events.push(metric(MetricKind::Gauge(f64::from(i))));
        events.push(metric(MetricKind::GaugeDelta(1.0)));
    }
    let input = batch(events);
    let one = OtlpEncoder::new()
        .request(Signal::Metrics, &input.resource, None, &input.events[..1])
        .unwrap()
        .encoded_len();
    let overhead = 10_000 - value_budget(10_000, &IndexMap::new());
    let cluster = cluster(&[METRICS], 1).await;
    cluster.script(Step::metadata(METRICS, Action::Error(fault::code::LEADER_NOT_AVAILABLE)));
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { max_request_bytes: 3 * one + overhead, ..settings(&cluster) }, &probe);
    let sums = run(&mut out, &mut probe, vec![input]).await;
    assert!(records_on(&cluster, METRICS).len() > 2, "the request was split");
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "clean")]), 1.0, "a retry");
    assert_eq!(sum(&sums, "logit.output.metrics.skipped", &[("metric_kind", "gauge_delta")]), 12.0);
}

/// `record:` and an attribute key are refused under an OTLP format, and `topics` under another.
#[test]
fn an_otlp_format_refuses_a_record_mode_and_an_attribute_key() {
    let base = KafkaSettings {
        format: RecordFormat::OtlpProto,
        topics: Some(SignalTopics::default()),
        ..KafkaSettings::new(vec!["127.0.0.1:9".into()], "")
    };
    for (settings, want) in [
        (KafkaSettings { record: Some(RecordMode::Event), ..base.clone() }, "takes no 'record'"),
        (KafkaSettings { key: RecordKey::Attribute("k".into()), ..base.clone() }, "attribute"),
        (KafkaSettings { format: RecordFormat::Json, ..base.clone() }, "'topics' needs"),
        (KafkaSettings { topics: None, ..base.clone() }, "no topic"),
    ] {
        let err = KafkaOutput::new(settings).err().expect("refused");
        assert!(err.to_string().contains(want), "{err}");
    }
    KafkaOutput::new(KafkaSettings { key: RecordKey::TraceId, ..base }).expect("a trace id key");
}

/// Under `otlp_json` each signal lands on its topic as the OTLP/JSON `otlp_out`'s encoder writes
/// for the batch, which `otlp_in`'s decoder reads back to the events the protobuf request holds.
#[tokio::test]
async fn otlp_json_writes_each_signal_as_the_json_request_otlp_out_writes() {
    let cluster = cluster(&[LOGS, METRICS, SPANS], 1).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(json_settings(&cluster), &probe);
    let input = mixed();
    let sums = run(&mut out, &mut probe, vec![input.clone()]).await;

    let want_json = json_encoder().encode_signals(&input).unwrap();
    let want_proto = OtlpEncoder::new().encode_signals(&input).unwrap();
    assert_eq!(want_json.len(), 3);
    for (json, proto) in want_json.iter().zip(&want_proto) {
        let topic = match json.signal {
            Signal::Logs => LOGS,
            Signal::Metrics => METRICS,
            Signal::Traces => SPANS,
        };
        let records = records_on(&cluster, topic);
        assert_eq!(records.len(), 1, "{topic}");
        let value = &records[0].2;
        assert_eq!(value, &json.bytes, "{topic}: the bytes otlp_out's JSON encoder writes");
        assert_eq!(value[0], b'{', "{topic}: a JSON object, not protobuf");
        let from_json = OtlpDecoder::new().decode_signal_json(json.signal, value.clone()).unwrap();
        let from_proto =
            OtlpDecoder::new().decode_signal(proto.signal, proto.bytes.clone()).unwrap();
        assert_eq!(from_json, from_proto, "{topic}");
    }
    assert_eq!(sum(&sums, "logit.output.records", &[]), 3.0);
}

/// A trace id key splits an `otlp_json` request as it splits an `otlp_proto` one, and each
/// piece is a valid OTLP/JSON request.
#[tokio::test]
async fn otlp_json_keys_by_trace_id_like_otlp_proto() {
    let (a, b) = ([0xa1; 16], [0xb2; 16]);
    let cluster = cluster(&[LOGS], 4).await;
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { key: RecordKey::TraceId, ..json_settings(&cluster) }, &probe);
    let input = batch(vec![
        log("a1", Some(a)),
        log("b1", Some(b)),
        log("none", None),
        log("a2", Some(a)),
    ]);
    let sums = run(&mut out, &mut probe, vec![input]).await;
    let mut got: Vec<(Option<Vec<u8>>, Vec<Value>)> = records_on(&cluster, LOGS)
        .into_iter()
        .map(|(_, k, v, _)| (k.map(|k| k.to_vec()), json_log_bodies(&v)))
        .collect();
    got.sort_by(|x, y| x.0.cmp(&y.0));
    let strs = |names: &[&str]| names.iter().map(|n| Value::str(*n)).collect::<Vec<_>>();
    assert_eq!(
        got,
        vec![
            (None, strs(&["none"])),
            (Some(hex(a)), strs(&["a1", "a2"])),
            (Some(hex(b)), strs(&["b1"])),
        ]
    );
    assert_eq!(sum(&sums, "logit.output.records.unkeyed", &[]), 1.0);
}

/// The budget under `otlp_json` is the JSON length: a request is split into runs that each fit
/// the budget as JSON, which is longer than the protobuf the same events encode to.
#[tokio::test]
async fn an_oversize_otlp_json_request_is_split_by_its_json_length() {
    let input = batch((0..20).map(|i| log(&format!("event {i:02}"), None)).collect());
    let one = {
        let mut body = Vec::new();
        let request = OtlpEncoder::new()
            .request(Signal::Logs, &input.resource, None, &input.events[..1])
            .unwrap();
        request.write_json(&mut body);
        assert!(body.len() > request.encoded_len(), "JSON is longer than protobuf");
        body.len()
    };
    let overhead = 10_000 - value_budget(10_000, &IndexMap::new());
    let budget = 4 * one;
    let cluster = cluster(&[LOGS], 1).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings { max_request_bytes: budget + overhead, ..json_settings(&cluster) },
        &probe,
    );
    let sums = run(&mut out, &mut probe, vec![input]).await;

    let records = records_on(&cluster, LOGS);
    assert!(records.len() > 1, "split into several: {}", records.len());
    let mut bodies = Vec::new();
    for (_, _, value, _) in &records {
        assert!(value.len() <= budget, "{} over {budget}", value.len());
        bodies.extend(json_log_bodies(value));
    }
    let want: Vec<Value> = (0..20).map(|i| Value::str(format!("event {i:02}"))).collect();
    assert_eq!(bodies, want, "every event, in order");
    assert_eq!(sum(&sums, "logit.output.records.dropped", &[("reason", "oversize")]), 0.0);
}

/// `otlp_json` refuses what `otlp_proto` refuses.
#[test]
fn otlp_json_refuses_a_record_mode_and_an_attribute_key() {
    let base = KafkaSettings {
        format: RecordFormat::OtlpJson,
        topics: Some(SignalTopics::default()),
        ..KafkaSettings::new(vec!["127.0.0.1:9".into()], "")
    };
    for (settings, want) in [
        (KafkaSettings { record: Some(RecordMode::Event), ..base.clone() }, "takes no 'record'"),
        (KafkaSettings { key: RecordKey::Attribute("k".into()), ..base.clone() }, "attribute"),
        (KafkaSettings { topics: None, ..base.clone() }, "no topic"),
    ] {
        let err = KafkaOutput::new(settings).err().expect("refused");
        assert!(err.to_string().contains(want), "{err}");
    }
    KafkaOutput::new(KafkaSettings { key: RecordKey::TraceId, ..base }).expect("a trace id key");
}
