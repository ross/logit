//! `kafka_out` through the runtime's `write_loop` against the in-process fake broker: what each
//! record carries, the pending batch kept across retries and let go between batches, and the
//! counters.

use logit_core::{AttrMap, Diagnostics, Event, EventBatch, LogRecord, Resource, Value};
use logit_pipeline::test_util::{TelemetryProbe, RECV_TIMEOUT};
use logit_pipeline::{BatchContext, Fault, Output, SeqId, WriteLoopConfig};

use super::*;
use crate::kafka::fake_broker::{self, Action, FakeCluster, Received, Step};
use crate::kafka::partitioner::partition_for_key;
use crate::kafka::OAuthToken;
use crate::test_support::{at_most_once, fast_retry, sums_through_write_loop, Sums};

const TOPIC: &str = "logs";
const TS: i64 = 1_791_739_946_221_123_456;

async fn cluster(leaders: &[i32]) -> FakeCluster {
    let cluster = FakeCluster::start(1).await;
    cluster.state().add_topic(TOPIC, leaders);
    cluster
}

fn settings(cluster: &FakeCluster) -> KafkaSettings {
    KafkaSettings { timeout: RECV_TIMEOUT, ..KafkaSettings::new(vec![cluster.addr(0)], TOPIC) }
}

fn sink(settings: KafkaSettings, probe: &TelemetryProbe) -> KafkaOutput {
    let telemetry = probe.telemetry("out", "kafka_out", "sink");
    KafkaOutput::new(settings)
        .unwrap()
        .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
        .with_telemetry(telemetry)
}

/// A log event with `k` set to `key`.
fn event(key: &str, message: &str) -> Event {
    let mut attributes = AttrMap::new();
    attributes.insert("k", Value::str(key));
    Event::log(
        TS,
        attributes,
        LogRecord {
            message: Value::str(message),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

fn batch(events: Vec<Event>) -> EventBatch {
    EventBatch { resource: std::sync::Arc::new(Resource::default()), scope: None, events }
}

/// `n` events keyed `{tag}-0`.. whose keys all land on `partition` of `partitions`.
fn keyed_for(tag: &str, partition: i32, partitions: usize, n: usize) -> Vec<Event> {
    (0..)
        .map(|i| format!("{tag}-{i}"))
        .filter(|k| partition_for_key(k.as_bytes(), partitions) == partition)
        .take(n)
        .map(|k| event(&k, &format!("message {k}")))
        .collect()
}

fn messages_in(cluster: &FakeCluster, partition: i32) -> Vec<String> {
    let state = cluster.state();
    state
        .partition(TOPIC, partition)
        .records()
        .iter()
        .map(|r| {
            let value: serde_json::Value =
                serde_json::from_slice(r.value.as_ref().expect("a value")).unwrap();
            value["log"]["message"].as_str().unwrap().to_string()
        })
        .collect()
}

fn received(cluster: &FakeCluster) -> Vec<Received> {
    cluster.state().received.clone()
}

fn sum(sums: &Sums, name: &str, tags: &[(&str, &str)]) -> f64 {
    sums.iter()
        .filter(|((n, t), _)| {
            n == name && tags.iter().all(|(k, v)| t.iter().any(|(tk, tv)| tk == k && tv == v))
        })
        .map(|(_, v)| v)
        .sum()
}

async fn run(
    out: &mut KafkaOutput,
    probe: &mut TelemetryProbe,
    batches: Vec<EventBatch>,
    config: WriteLoopConfig,
) -> Sums {
    sums_through_write_loop(out, probe, "kafka_out", batches, config).await
}

/// Each record carries the event's JSON object as its value, its key, the static headers in
/// order, and the event's timestamp in milliseconds, on the partition the key's murmur2 picks.
#[tokio::test]
async fn a_record_carries_the_events_json_key_headers_and_timestamp() {
    let cluster = cluster(&[0, 0, 0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings {
            key: RecordKey::Attribute("k".into()),
            headers: vec![("source".into(), "logit".into()), ("env".into(), "test".into())],
            ..settings(&cluster)
        },
        &probe,
    );
    let events: Vec<Event> = (0..6).map(|i| event(&format!("key-{i}"), &format!("m{i}"))).collect();
    let input = batch(events.clone());
    let sums = run(&mut out, &mut probe, vec![input.clone()], fast_retry()).await;

    let state = cluster.state();
    for (i, e) in events.iter().enumerate() {
        let key = format!("key-{i}");
        let partition = partition_for_key(key.as_bytes(), 3);
        let records = state.partition(TOPIC, partition).records();
        let record = records
            .iter()
            .find(|r| r.key.as_deref() == Some(key.as_bytes()))
            .unwrap_or_else(|| panic!("{key} on partition {partition}"));
        let mut want = String::new();
        crate::ndjson::render_event(
            &mut want,
            &input.resource,
            None,
            e,
            Some(crate::kafka::CARRIER_PREFIX),
        );
        assert_eq!(record.value.as_deref(), Some(want.as_bytes()));
        assert_eq!(record.timestamp, TS / 1_000_000);
        let headers: Vec<(&str, &[u8])> = record
            .headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_deref().unwrap_or_default()))
            .collect();
        assert_eq!(headers, vec![("source", &b"logit"[..]), ("env", &b"test"[..])]);
    }
    drop(state);
    assert_eq!(sum(&sums, "logit.output.records", &[]), 6.0);
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "ok")]), 1.0);
    assert!(sum(&sums, "logit.output.batch.bytes", &[]) > 0.0);
}

/// An `Ambiguous` attempt whose append landed is retried with the same pending batch: the same
/// sequences, so the broker deduplicates the resend and each record is stored and counted once.
#[tokio::test]
async fn a_retry_resends_the_same_pending_batch_and_the_broker_deduplicates_it() {
    let cluster = cluster(&[0, 0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { key: RecordKey::Attribute("k".into()), ..settings(&cluster) }, &probe);
    cluster.script(Step::produce(None, Action::AppendThenClose));
    let mut events = keyed_for("a", 0, 2, 3);
    events.extend(keyed_for("b", 1, 2, 2));
    let sums = run(&mut out, &mut probe, vec![batch(events)], fast_retry()).await;

    assert_eq!(messages_in(&cluster, 0).len(), 3, "appended once");
    assert_eq!(messages_in(&cluster, 1).len(), 2, "appended once");
    let got = received(&cluster);
    let stamp = |r: &Received| (r.partition, r.producer_epoch, r.base_sequence);
    assert_eq!(got.len(), 4);
    assert_eq!(
        got[..2].iter().map(stamp).collect::<Vec<_>>(),
        got[2..].iter().map(stamp).collect::<Vec<_>>()
    );
    assert_eq!(sum(&sums, "logit.output.records", &[]), 5.0);
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "ambiguous")]), 1.0);
    assert!(sum(&sums, "logit.output.batch.bytes", &[]) > 0.0);
}

/// A partition the broker settled isn't resent on a retry: only the unsettled one is.
#[tokio::test]
async fn a_retry_resends_only_the_unsettled_partition() {
    let cluster = cluster(&[0, 0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { key: RecordKey::Attribute("k".into()), ..settings(&cluster) }, &probe);
    cluster.script(Step::produce(Some(1), Action::Error(fault::code::NOT_ENOUGH_REPLICAS)));
    let mut events = keyed_for("a", 0, 2, 2);
    events.extend(keyed_for("b", 1, 2, 2));
    run(&mut out, &mut probe, vec![batch(events)], fast_retry()).await;
    let partitions: Vec<i32> = received(&cluster).iter().map(|r| r.partition).collect();
    assert_eq!(partitions.iter().filter(|&&p| p == 0).count(), 1, "{partitions:?}");
    assert_eq!(partitions.iter().filter(|&&p| p == 1).count(), 2, "{partitions:?}");
}

/// A batch `write_loop` let go of with a partition batch unsettled (here a direct caller moving
/// on, as after a shutdown cut) is released at the next `observe_batch`: the next batch goes out
/// itself, under a bumped epoch from sequence 0.
#[tokio::test]
async fn the_next_batch_releases_an_unsettled_one_and_bumps_the_epoch() {
    let cluster = cluster(&[0]).await;
    let probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { key: RecordKey::Attribute("k".into()), ..settings(&cluster) }, &probe);
    let seq = |n| SeqId { id: [0; 16], seq: n };
    cluster.script(Step::produce(None, Action::Close));
    out.observe_batch(BatchContext::default(), seq(1));
    let err = out.send(&batch(keyed_for("a", 0, 1, 2))).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);

    out.observe_batch(BatchContext::default(), seq(2));
    out.send(&batch(keyed_for("b", 0, 1, 3))).await.unwrap();
    assert_eq!(messages_in(&cluster, 0).len(), 3, "the new batch, not the old one");
    assert!(messages_in(&cluster, 0).iter().all(|m| m.contains("b-")));
    let last = received(&cluster).pop().unwrap();
    assert_eq!((last.producer_epoch, last.base_sequence), (1, 0));
}

/// Under `at_most_once`, an `Ambiguous` batch is dropped, its unsettled records counted, and
/// the next batch goes out under a bumped epoch.
#[tokio::test]
async fn at_most_once_drops_an_ambiguous_batch_and_the_next_bumps_the_epoch() {
    let cluster = cluster(&[0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { key: RecordKey::Attribute("k".into()), ..settings(&cluster) }, &probe);
    cluster.script(Step::produce(None, Action::Close));
    let batches = vec![batch(keyed_for("a", 0, 1, 2)), batch(keyed_for("b", 0, 1, 3))];
    let sums = run(&mut out, &mut probe, batches, at_most_once()).await;
    assert!(out.pending.is_none(), "no batch's records are held past its final send");
    assert_eq!(
        sum(&sums, "logit.output.records.dropped", &[("reason", "ambiguous_at_most_once")]),
        2.0
    );
    assert_eq!(sum(&sums, "logit.output.records", &[]), 3.0);
    assert_eq!(sum(&sums, "logit.output.reconnects", &[]), 1.0, "the closed leader's redial");
    let last = received(&cluster).pop().expect("the second batch");
    assert_eq!((last.records, last.producer_epoch, last.base_sequence), (3, 1, 0));
}

/// Under `at_most_once`, an `Ambiguous` partition in an attempt another partition answers
/// `Refused` is dropped and counted, not resent with the `Refused` one: without idempotence the
/// resend would append its records twice.
#[tokio::test]
async fn at_most_once_drops_an_ambiguous_partition_when_another_is_refused() {
    let cluster = FakeCluster::start(2).await;
    cluster.state().add_topic(TOPIC, &[0, 1]);
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings {
            key: RecordKey::Attribute("k".into()),
            acks: KafkaAcks::Leader,
            idempotent: false,
            ..settings(&cluster)
        },
        &probe,
    );
    cluster.script(Step::produce(Some(0), Action::AppendThenError(fault::code::REQUEST_TIMED_OUT)));
    cluster.script(Step::produce(Some(1), Action::Error(fault::code::TOPIC_AUTHORIZATION_FAILED)));
    let mut events = keyed_for("a", 0, 2, 3);
    events.extend(keyed_for("b", 1, 2, 2));
    let sums = run(&mut out, &mut probe, vec![batch(events)], at_most_once()).await;

    let partitions: Vec<i32> = received(&cluster).iter().map(|r| r.partition).collect();
    assert_eq!(partitions.iter().filter(|&&p| p == 0).count(), 1, "{partitions:?}");
    assert_eq!(partitions.iter().filter(|&&p| p == 1).count(), 2, "{partitions:?}");
    assert_eq!(messages_in(&cluster, 0).len(), 3, "appended once");
    assert_eq!(messages_in(&cluster, 1).len(), 2);
    assert_eq!(
        sum(&sums, "logit.output.records.dropped", &[("reason", "ambiguous_at_most_once")]),
        3.0
    );
    assert_eq!(sum(&sums, "logit.output.records", &[]), 2.0);
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "refused")]), 1.0);
}

/// The drops each have their reason: a record over `max_request_bytes`, records the broker
/// named, and a partition batch it rejected. An unkeyed record is counted, and goes out.
#[tokio::test]
async fn each_drop_and_an_unkeyed_record_is_counted_by_its_reason() {
    let cluster = cluster(&[0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings {
            key: RecordKey::Attribute("k".into()),
            max_request_bytes: 600,
            ..settings(&cluster)
        },
        &probe,
    );
    let mut events = keyed_for("a", 0, 1, 3);
    events[2].log.as_mut().unwrap().message = Value::str("x".repeat(600));
    let mut unkeyed = event("", "no key");
    unkeyed.attributes = AttrMap::new();
    events.push(unkeyed);
    cluster
        .script(Step::produce(None, Action::RecordErrors(fault::code::INVALID_TIMESTAMP, vec![0])));
    let named = batch(events);
    cluster.state().script.push(Step::produce(None, Action::Error(fault::code::CORRUPT_MESSAGE)));
    let rejected = batch(keyed_for("c", 0, 1, 2));
    let sums = run(&mut out, &mut probe, vec![named, rejected], fast_retry()).await;

    let dropped = |reason| sum(&sums, "logit.output.records.dropped", &[("reason", reason)]);
    assert_eq!(dropped("oversize"), 1.0);
    assert_eq!(dropped("invalid_timestamp"), 1.0);
    assert_eq!(dropped("rejected"), 2.0);
    assert_eq!(sum(&sums, "logit.output.records.unkeyed", &[]), 1.0);
    assert_eq!(sum(&sums, "logit.output.records", &[]), 2.0, "the rest of the named batch");
    assert_eq!(messages_in(&cluster, 0).len(), 2);
    for key in ["oversize", "invalid_timestamp", "rejected"] {
        assert!(
            sum(&sums, "logit.component.diagnostics", &[("key", key)]) >= 1.0,
            "a {key} diagnostic: {sums:?}"
        );
    }
}

/// A retry after a failed metadata lookup resends the pending batch the first attempt encoded,
/// so it encodes nothing again and its encode-side points count once.
#[tokio::test]
async fn a_retry_resends_the_kept_encode_and_counts_encode_side_points_once() {
    let cluster = cluster(&[0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings { key: RecordKey::Attribute("absent".into()), ..settings(&cluster) },
        &probe,
    );
    cluster.script(Step {
        api: kafka_protocol::messages::ApiKey::Metadata,
        partition: None,
        topic: None,
        action: Action::Error(fault::code::LEADER_NOT_AVAILABLE),
    });
    let sums = run(&mut out, &mut probe, vec![batch(keyed_for("a", 0, 1, 2))], fast_retry()).await;
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "clean")]), 1.0);
    assert_eq!(sum(&sums, "logit.output.records.unkeyed", &[]), 2.0);
    let first: f64 = {
        let mut once = String::new();
        crate::ndjson::render_ndjson(&mut once, &batch(keyed_for("a", 0, 1, 2)));
        (once.len() - 2) as f64
    };
    assert_eq!(sum(&sums, "logit.output.batch.bytes", &[]), first);
}

/// A second encode of a batch already encoded counts no encode-side point again, and the next
/// batch's first encode counts them all. Calls `prepare` directly: `write_loop` keeps the
/// encoded batch across retries, so no retry repeats the encode the gate guards.
#[tokio::test]
async fn a_repeated_encode_of_one_batch_counts_encode_side_points_once() {
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings {
            format: RecordFormat::Text,
            key: RecordKey::Attribute("absent".into()),
            ..KafkaSettings::new(vec!["127.0.0.1:9".into()], TOPIC)
        },
        &probe,
    );
    let mut mixed = event("a", "m");
    mixed.metrics.push(logit_core::MetricRecord::new(
        logit_core::interner::intern("m"),
        logit_core::MetricKind::Gauge(1.0.into()),
    ));
    mixed.span = Some(logit_core::SpanRecord {
        trace_id: [1; 16],
        span_id: [1; 8],
        parent_span_id: None,
        name: Value::str("s"),
        kind: logit_core::SpanKind::Internal,
        status: logit_core::SpanStatus::Unset,
        events: vec![],
        links: vec![],
        end_timestamp: TS,
        flags: 0,
        ext: None,
    });
    let input = batch(vec![mixed, metric_event()]);
    let seq = |n| SeqId { id: [0; 16], seq: n };
    let points = [
        ("logit.output.batch.bytes", &[][..], 1.0), // the one body, `m`
        ("logit.output.records.unkeyed", &[][..], 1.0),
        ("logit.output.metrics.omitted", &[("reason", "format")][..], 1.0),
        ("logit.output.spans.omitted", &[("reason", "format")][..], 1.0),
        ("logit.output.events.skipped", &[("reason", "no_log")][..], 1.0),
    ];
    for (n, batches) in [(1, 1.0), (2, 2.0)] {
        out.observe_batch(BatchContext::default(), seq(n));
        assert!(out.prepare(&input).is_some());
        assert!(out.prepare(&input).is_some(), "a repeat encodes the same records");
        let totals = probe.poll();
        for (name, tags, per_batch) in points {
            assert_eq!(totals.sum(name, tags), per_batch * batches, "{name} after batch {n}");
        }
    }
}

/// A topic the broker won't create is `Refused` once a refresh confirms it, which holds the
/// queue; one it creates costs no failure.
#[tokio::test]
async fn a_missing_topic_is_refused_and_a_created_one_is_sent() {
    let cluster = FakeCluster::start(1).await;
    let probe = TelemetryProbe::new();
    let short =
        KafkaSettings { timeout: std::time::Duration::from_millis(300), ..settings(&cluster) };
    let mut out = sink(short, &probe);
    let err = out.send(&batch(vec![event("a", "m")])).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");

    let cluster = FakeCluster::start(1).await;
    cluster.state().auto_create = Some(1);
    let mut out = sink(settings(&cluster), &probe);
    out.send(&batch(vec![event("a", "m")])).await.expect("a created topic is sent at once");
}

/// An empty batch sends nothing.
#[tokio::test]
async fn an_empty_batch_sends_nothing() {
    let cluster = cluster(&[0]).await;
    let probe = TelemetryProbe::new();
    let mut out = sink(settings(&cluster), &probe);
    out.send(&batch(vec![])).await.unwrap();
    assert!(cluster.state().requests.is_empty());
}

#[test]
fn idempotence_without_acks_all_is_refused_at_construction() {
    let settings = KafkaSettings {
        acks: KafkaAcks::Leader,
        ..KafkaSettings::new(vec!["k:9092".into()], TOPIC)
    };
    assert!(KafkaOutput::new(settings).is_err());
}

/// A metric event, which `format: text` has no body for.
fn metric_event() -> Event {
    Event::metric(
        TS,
        AttrMap::new(),
        logit_core::MetricRecord::new(
            logit_core::interner::intern("m"),
            logit_core::MetricKind::gauge(1.0),
        ),
    )
}

/// Under `record: batch`, one record holds the batch's events as NDJSON lines, unkeyed, stamped
/// with its first event's timestamp, and counted once.
#[tokio::test]
async fn a_batch_record_carries_the_events_as_ndjson() {
    let cluster = cluster(&[0, 0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out =
        sink(KafkaSettings { record: Some(RecordMode::Batch), ..settings(&cluster) }, &probe);
    let input = batch((0..4).map(|i| event(&format!("k{i}"), &format!("m{i}"))).collect());
    let sums = run(&mut out, &mut probe, vec![input.clone()], fast_retry()).await;

    let state = cluster.state();
    let records: Vec<_> = (0..2).flat_map(|p| state.partition(TOPIC, p).records()).collect();
    assert_eq!(records.len(), 1, "one record for the batch");
    let mut want = String::new();
    crate::ndjson::render_ndjson(&mut want, &input);
    assert_eq!(records[0].value.as_deref(), Some(want.as_bytes()));
    assert_eq!(records[0].key, None);
    assert_eq!(records[0].timestamp, TS / 1_000_000);
    drop(state);
    assert_eq!(sum(&sums, "logit.output.records", &[]), 1.0);
}

/// A key needs one event per record: a batch record, set or `native`'s default, refuses one.
#[test]
fn a_key_with_a_batch_record_is_refused() {
    let base = KafkaSettings {
        key: RecordKey::TraceId,
        ..KafkaSettings::new(vec!["127.0.0.1:9".into()], TOPIC)
    };
    for settings in [
        KafkaSettings { record: Some(RecordMode::Batch), ..base.clone() },
        KafkaSettings { format: RecordFormat::Native, ..base.clone() },
    ] {
        let err = KafkaOutput::new(settings).err().expect("refused");
        assert!(err.to_string().contains("'record: event'"), "{err}");
    }
    KafkaOutput::new(KafkaSettings {
        format: RecordFormat::Native,
        record: Some(RecordMode::Event),
        ..base
    })
    .expect("native with record: event takes a key");
}

/// `format: text` skips an event with no log record and counts it; a batch with nothing left to
/// send succeeds without a request.
#[tokio::test]
async fn text_skips_an_event_without_a_log_and_sends_nothing_for_none() {
    let cluster = cluster(&[0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(KafkaSettings { format: RecordFormat::Text, ..settings(&cluster) }, &probe);
    let metrics_only = batch(vec![metric_event(), metric_event()]);
    let sums = run(&mut out, &mut probe, vec![metrics_only], fast_retry()).await;
    assert!(cluster.state().requests.is_empty(), "no request for a batch with no record");
    assert_eq!(sum(&sums, "logit.output.events.skipped", &[("reason", "no_log")]), 2.0);
    assert_eq!(sum(&sums, "logit.output.requests", &[("class", "ok")]), 1.0);

    let mixed = batch(vec![event("a", "first"), metric_event(), event("b", "second")]);
    let sums = run(&mut out, &mut probe, vec![mixed], fast_retry()).await;
    let values: Vec<_> = cluster
        .state()
        .partition(TOPIC, 0)
        .records()
        .iter()
        .map(|r| r.value.clone().unwrap())
        .collect();
    assert_eq!(values, vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")]);
    assert_eq!(
        sum(&sums, "logit.output.events.skipped", &[("reason", "no_log")]),
        3.0,
        "both runs"
    );
}

/// Under `record: batch`, an event too large for a record alone is dropped and counted as
/// `oversize`, and the rest of the batch goes out.
#[tokio::test]
async fn an_event_over_the_cap_alone_is_dropped_in_batch_mode() {
    let cluster = cluster(&[0]).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink(
        KafkaSettings {
            format: RecordFormat::Msgpack,
            record: Some(RecordMode::Batch),
            max_request_bytes: 600,
            ..settings(&cluster)
        },
        &probe,
    );
    let input = batch(vec![event("a", "small"), event("b", &"x".repeat(600)), event("c", "small")]);
    let sums = run(&mut out, &mut probe, vec![input], fast_retry()).await;

    let state = cluster.state();
    let records = state.partition(TOPIC, 0).records();
    assert_eq!(records.len(), 1);
    let value = records[0].value.clone().unwrap();
    let mut reader = logit_proto::msgpack::Reader::new(&value);
    let mut maps = 0;
    while !reader.is_empty() {
        reader.skip_value().unwrap();
        maps += 1;
    }
    assert_eq!(maps, 2, "the two small events");
    drop(state);
    assert_eq!(sum(&sums, "logit.output.records.dropped", &[("reason", "oversize")]), 1.0);
    assert!(sum(&sums, "logit.component.diagnostics", &[("key", "oversize")]) >= 1.0);
}

/// A broker refusing the credentials holds the queue: `Refused`, retried with nothing dropped,
/// `logit.component.retrying` at 1, and no error naming the password. Once the broker takes the
/// sink's credentials, the held batch is delivered.
#[tokio::test]
async fn refused_sasl_credentials_hold_the_queue_until_the_broker_takes_them() {
    let cluster = cluster(&[0]).await;
    cluster.state().sasl = Some(fake_broker::Sasl::new("alice", "rotated-on-the-broker"));
    let mut probe = TelemetryProbe::new();
    let sasl =
        KafkaSasl::ScramSha256 { username: "alice".into(), password: "sink-password".into() };
    let mut out = sink(KafkaSettings { sasl: Some(sasl), ..settings(&cluster) }, &probe);
    assert!(!format!("{:?}", settings(&cluster)).contains("sink-password"));

    let err = out.send(&batch(vec![event("a", "held")])).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{message}");
    assert!(message.contains("SCRAM-SHA-256 as user \"alice\""), "{message}");
    assert!(!message.contains("sink-password"), "{message}");

    let telemetry = probe.telemetry("out", "kafka_out", "sink");
    let delivery = tokio::spawn(async move {
        logit_pipeline::test_util::drive_write_loop(
            &mut out,
            vec![batch(vec![event("a", "held")])],
            fast_retry(),
            telemetry,
        )
        .await;
    });
    probe
        .wait_for("the sink retries the refused batch", |t| {
            t.gauge("logit.component.retrying", &[]) == Some(1.0)
        })
        .await;
    assert!(messages_in(&cluster, 0).is_empty());
    cluster.state().sasl.as_mut().unwrap().password = "sink-password".into();
    delivery.await.unwrap();

    assert_eq!(messages_in(&cluster, 0), vec!["held".to_string()]);
    let totals = probe.poll();
    assert_eq!(totals.gauge("logit.component.retrying", &[]), Some(0.0));
    assert_eq!(totals.sum("logit.output.batches.dropped", &[]), 0.0);
    assert_eq!(totals.sum("logit.output.records.dropped", &[]), 0.0);
    assert!(totals.sum("logit.output.requests", &[("class", "refused")]) >= 1.0);
}

/// Each mechanism's sink delivers through `write_loop` with the broker requiring it.
#[tokio::test]
async fn each_sasl_mechanism_delivers_through_the_sink() {
    let make = |m: &str| {
        let (username, password) = ("alice".to_string(), "pw".to_string());
        match m {
            "plain" => KafkaSasl::Plain { username, password },
            "scram_sha_256" => KafkaSasl::ScramSha256 { username, password },
            _ => KafkaSasl::ScramSha512 { username, password },
        }
    };
    for mechanism in ["plain", "scram_sha_256", "scram_sha_512"] {
        let cluster = cluster(&[0]).await;
        cluster.state().sasl = Some(fake_broker::Sasl::new("alice", "pw"));
        let mut probe = TelemetryProbe::new();
        let mut out =
            sink(KafkaSettings { sasl: Some(make(mechanism)), ..settings(&cluster) }, &probe);
        let sums =
            run(&mut out, &mut probe, vec![batch(vec![event("a", "m")])], fast_retry()).await;
        assert_eq!(sum(&sums, "logit.output.records", &[]), 1.0, "{mechanism}");
        assert_eq!(messages_in(&cluster, 0), vec!["m".to_string()], "{mechanism}");
    }
}

/// `OAUTHBEARER` with the client-credentials grant delivers through `write_loop`, fetching its
/// token from the endpoint once.
#[tokio::test]
async fn oauthbearer_client_credentials_deliver_through_the_sink() {
    let endpoint = fake_broker::TokenEndpoint::start().await;
    let cluster = cluster(&[0]).await;
    let mut broker_sasl = fake_broker::Sasl::new("alice", "pw");
    broker_sasl.oauth_token = Some("token-0".into());
    cluster.state().sasl = Some(broker_sasl);
    let sasl = KafkaSasl::OAuthBearer {
        token: OAuthToken::ClientCredentials {
            endpoint: endpoint.url.clone(),
            client_id: "logit".into(),
            client_secret: "client-secret".into(),
            scope: None,
        },
        extensions: vec![("logicalCluster".into(), "lkc-1".into())],
    };
    let shown = format!("{sasl:?}");
    assert!(shown.contains("logicalCluster") && !shown.contains("client-secret"), "{shown}");
    let mut probe = TelemetryProbe::new();
    let mut out = sink(KafkaSettings { sasl: Some(sasl), ..settings(&cluster) }, &probe);
    let sums = run(&mut out, &mut probe, vec![batch(vec![event("a", "m")])], fast_retry()).await;
    assert_eq!(sum(&sums, "logit.output.records", &[]), 1.0);
    assert_eq!(messages_in(&cluster, 0), vec!["m".to_string()]);
    assert_eq!(endpoint.fetches(), 1);
}
