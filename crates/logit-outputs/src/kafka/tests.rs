//! The producer against the in-process fake broker (`super::fake_broker`): partitioning and
//! leader routing, idempotent sequences across retries and epoch bumps, and each row of the
//! module doc's response-class table.

use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::records::Compression;
use logit_pipeline::test_util::{wait_until, TelemetryProbe, RECV_TIMEOUT};
use logit_pipeline::Fault;

use super::batch::ProducerRecord;
use super::fake_broker::{Action, FakeCluster, HttpStub, Received, Sasl, Step, TokenEndpoint};
use super::fault::code;
use super::multi::Attempt;
use super::partitioner::partition_for_key;
use super::producer::{Acks, Identity, PartState, Pending, Producer, ProducerConfig, Settlement};
use super::sasl::{authenticator, msk_iam_with, Endpoints, Env, KafkaSasl, OAuthToken};

const TOPIC: &str = "logs";
/// The first PID the fake broker hands out.
const FIRST_PID: i64 = 1000;
const RECONNECTS: &str = "logit.output.reconnects";

fn config(cluster: &FakeCluster) -> ProducerConfig {
    ProducerConfig {
        brokers: vec![cluster.addr(0)],
        topic: TOPIC.to_string(),
        acks: Acks::All,
        idempotent: true,
        compression: Compression::None,
        timeout: RECV_TIMEOUT,
        max_request_bytes: 1 << 20,
        tls: None,
        client_id: "logit-test".to_string(),
        sasl: None,
        telemetry: Default::default(),
    }
}

fn record(key: Option<&str>, value: &str) -> ProducerRecord {
    ProducerRecord {
        key: key.map(|k| Bytes::from(k.to_string())),
        value: Some(Bytes::from(value.to_string())),
        headers: IndexMap::new(),
        timestamp: 1_791_739_946_221,
    }
}

/// `n` records whose key sends them to `partition` of `partitions`.
fn keyed_for(partition: i32, partitions: usize, n: usize) -> Vec<ProducerRecord> {
    (0..)
        .map(|i| format!("key-{i}"))
        .filter(|k| partition_for_key(k.as_bytes(), partitions) == partition)
        .take(n)
        .enumerate()
        .map(|(i, k)| record(Some(&k), &format!("value {i}")))
        .collect()
}

/// A cluster of `nodes` with the topic's partitions led by `leaders`.
async fn cluster(nodes: usize, leaders: &[i32]) -> FakeCluster {
    let cluster = FakeCluster::start(nodes).await;
    cluster.state().add_topic(TOPIC, leaders);
    cluster
}

async fn prepare(producer: &mut Producer, mut records: Vec<ProducerRecord>) -> Pending {
    producer.prepare(&mut records).await.expect("prepare")
}

/// The attempt's class: `None` for `Ok`.
fn class(attempt: &Attempt) -> Option<Fault> {
    attempt.result.as_ref().err().map(logit_pipeline::classify)
}

fn received(cluster: &FakeCluster) -> Vec<Received> {
    cluster.state().received.clone()
}

fn requests_of(cluster: &FakeCluster, api: ApiKey) -> usize {
    cluster.state().requests.iter().filter(|(_, k, _)| *k == api).count()
}

fn values_in(cluster: &FakeCluster, partition: i32) -> Vec<Bytes> {
    let state = cluster.state();
    state.partition(TOPIC, partition).records().iter().filter_map(|r| r.value.clone()).collect()
}

/// Sends `records` as one pending batch until it's final, asserting each attempt's class.
async fn send_expecting(
    producer: &mut Producer,
    records: Vec<ProducerRecord>,
    classes: &[Option<Fault>],
) -> Pending {
    let mut pending = prepare(producer, records).await;
    for (n, want) in classes.iter().enumerate() {
        let attempt = producer.attempt(&mut pending).await;
        assert_eq!(class(&attempt), *want, "attempt {n}: {:?}", attempt.result);
    }
    pending
}

#[tokio::test]
async fn keyed_records_land_on_their_java_partition_through_each_leader() {
    let cluster = cluster(2, &[0, 1, 0, 1]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    let records: Vec<ProducerRecord> =
        (0..16).map(|i| record(Some(&format!("k{i}")), &format!("v{i}"))).collect();
    let mut pending = prepare(&mut producer, records.clone()).await;
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), None, "{:?}", attempt.result);

    for r in &records {
        let p = partition_for_key(r.key.as_ref().unwrap(), 4);
        assert!(values_in(&cluster, p).contains(r.value.as_ref().unwrap()), "{r:?} on {p}");
    }
    for got in received(&cluster) {
        assert_eq!(got.node, got.partition % 2, "partition {} went to its leader", got.partition);
        assert_eq!(got.version, 12, "Produce v12 against a broker offering v13");
        assert_eq!((got.producer_id, got.producer_epoch, got.base_sequence), (FIRST_PID, 0, 0));
        assert_eq!(got.acks, -1);
    }
    let accepted: usize = attempt.settled.iter().map(|s| s.records).sum();
    assert_eq!(accepted, 16);
}

#[tokio::test]
async fn unkeyed_records_stick_to_one_led_partition_per_send() {
    let cluster = cluster(1, &[0, -1, 0, -1]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    for _ in 0..8 {
        let records = (0..5).map(|i| record(None, &format!("v{i}"))).collect();
        send_expecting(&mut producer, records, &[None]).await;
    }
    let got = received(&cluster);
    assert_eq!(got.len(), 8, "one partition batch per send");
    assert!(got.iter().all(|r| r.records == 5 && (r.partition == 0 || r.partition == 2)));
}

/// A keyed record whose partition has no leader waits for one instead of moving.
#[tokio::test]
async fn a_keyed_record_waits_for_its_leaderless_partition() {
    let cluster = cluster(1, &[0, -1]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    let mut pending = prepare(&mut producer, keyed_for(1, 2, 2)).await;
    assert_eq!(class(&producer.attempt(&mut pending).await), Some(Fault::Clean));
    assert!(received(&cluster).is_empty());

    cluster.state().topics.get_mut(TOPIC).unwrap()[1].leader = 0;
    let metadata = requests_of(&cluster, ApiKey::Metadata);
    assert_eq!(class(&producer.attempt(&mut pending).await), None);
    assert_eq!(requests_of(&cluster, ApiKey::Metadata), metadata + 1, "refreshed first");
    assert_eq!(values_in(&cluster, 1).len(), 2);
}

/// A lost answer after the append: the retry carries the same PID, epoch, and base sequence
/// for every partition, and the broker deduplicates it.
#[tokio::test]
async fn a_retry_reuses_the_pid_epoch_and_base_sequences_and_is_deduplicated() {
    let cluster = cluster(1, &[0, 0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    // Advances both partitions' sequences, so the batches under test don't start at 0.
    let mut first = keyed_for(0, 2, 3);
    first.extend(keyed_for(1, 2, 2));
    send_expecting(&mut producer, first, &[None]).await;

    cluster.script(Step::produce(None, Action::AppendThenClose));
    let mut records = keyed_for(0, 2, 4);
    records.extend(keyed_for(1, 2, 4));
    send_expecting(&mut producer, records, &[Some(Fault::Ambiguous), None]).await;

    let got = received(&cluster);
    assert_eq!(got.len(), 6);
    let stamp = |r: &Received| (r.partition, r.producer_id, r.producer_epoch, r.base_sequence);
    let lost: Vec<_> = got[2..4].iter().map(stamp).collect();
    let retry: Vec<_> = got[4..6].iter().map(stamp).collect();
    assert_eq!(lost, vec![(0, FIRST_PID, 0, 3), (1, FIRST_PID, 0, 2)]);
    assert_eq!(retry, lost, "the retry is the same batches");
    assert_eq!(values_in(&cluster, 0).len(), 7, "deduplicated, not appended twice");
    assert_eq!(values_in(&cluster, 1).len(), 6);
}

/// `REQUEST_TIMED_OUT` after the leader appended: `Ambiguous`, and the retry is deduplicated.
#[tokio::test]
async fn request_timed_out_after_an_append_is_ambiguous_and_the_retry_deduplicates() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::AppendThenError(code::REQUEST_TIMED_OUT)));
    send_expecting(&mut producer, keyed_for(0, 1, 3), &[Some(Fault::Ambiguous), None]).await;
    assert_eq!(values_in(&cluster, 0).len(), 3);
    assert!(received(&cluster).iter().all(|r| r.base_sequence == 0 && r.producer_epoch == 0));
}

/// A rejected partition batch leaves a sequence gap, so the producer bumps the epoch: the same
/// PID at epoch 1, and every partition's next batch at sequence 0. The broker enforces the
/// sequence rule, so without the bump the next batch would be `OUT_OF_ORDER_SEQUENCE_NUMBER`.
#[tokio::test]
async fn a_rejected_partition_batch_bumps_the_epoch_and_sequences_restart_at_zero() {
    let cluster = cluster(1, &[0, 0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    let mut first = keyed_for(0, 2, 2);
    first.extend(keyed_for(1, 2, 2));
    send_expecting(&mut producer, first, &[None]).await;

    cluster.script(Step::produce(Some(0), Action::Error(code::CORRUPT_MESSAGE)));
    let pending = send_expecting(&mut producer, keyed_for(0, 2, 3), &[Some(Fault::Rejected)]).await;
    assert_eq!(pending.parts[0].state, PartState::Rejected);

    let mut next = keyed_for(0, 2, 2);
    next.extend(keyed_for(1, 2, 2));
    send_expecting(&mut producer, next, &[None]).await;
    let got = received(&cluster);
    let last: Vec<_> = got[got.len() - 2..]
        .iter()
        .map(|r| (r.partition, r.producer_id, r.producer_epoch, r.base_sequence, r.answered))
        .collect();
    assert_eq!(last, vec![(0, FIRST_PID, 1, 0, 0), (1, FIRST_PID, 1, 0, 0)]);
    assert_eq!(requests_of(&cluster, ApiKey::InitProducerId), 1, "the bump sends no request");
}

/// A batch dropped with a numbered partition batch unsettled (`at_most_once` after an
/// `Ambiguous` attempt) leaves a gap too.
#[tokio::test]
async fn abandoning_a_numbered_unsettled_batch_bumps_the_epoch() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::Error(code::REQUEST_TIMED_OUT)));
    let pending =
        send_expecting(&mut producer, keyed_for(0, 1, 3), &[Some(Fault::Ambiguous)]).await;
    producer.abandon(&pending);

    send_expecting(&mut producer, keyed_for(0, 1, 2), &[None]).await;
    let last = received(&cluster).pop().unwrap();
    assert_eq!((last.producer_epoch, last.base_sequence, last.answered), (1, 0, code::NONE));
}

#[tokio::test]
async fn the_epoch_at_i16_max_reinitializes_with_a_new_pid() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    producer.force_identity(Identity { producer_id: 7, epoch: i16::MAX }, true);
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    assert_eq!(producer.identity(), Some(Identity { producer_id: FIRST_PID, epoch: 0 }));
    assert_eq!(requests_of(&cluster, ApiKey::InitProducerId), 1);
}

/// `MESSAGE_TOO_LARGE` halves the batch, and each half goes out under the new epoch with fresh
/// consecutive sequences; a half that's too large again is rejected.
#[tokio::test]
async fn message_too_large_halves_the_batch_once() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::Error(code::MESSAGE_TOO_LARGE)));
    let pending =
        send_expecting(&mut producer, keyed_for(0, 1, 4), &[Some(Fault::Clean), None]).await;
    assert_eq!(pending.parts.len(), 2);
    let got: Vec<_> = received(&cluster)
        .iter()
        .map(|r| (r.producer_epoch, r.base_sequence, r.records, r.answered))
        .collect();
    assert_eq!(got, vec![(0, 0, 4, 10), (1, 0, 2, 0), (1, 2, 2, 0)]);
    assert_eq!(values_in(&cluster, 0).len(), 4);

    // Too large again on the first half: it's rejected, and the second half, behind it in the
    // same partition, waits for the next attempt and the next epoch.
    cluster.script(Step::produce(None, Action::Error(code::MESSAGE_TOO_LARGE)));
    cluster.script(Step::produce(None, Action::Error(code::MESSAGE_TOO_LARGE)));
    let mut pending = prepare(&mut producer, keyed_for(0, 1, 2)).await;
    assert_eq!(class(&producer.attempt(&mut pending).await), Some(Fault::Clean));
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), Some(Fault::Clean));
    assert_eq!(
        attempt.settled[0].settlement,
        Settlement::Rejected { code: code::MESSAGE_TOO_LARGE }
    );
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), None, "the other half was accepted");
    let last = received(&cluster).pop().unwrap();
    assert_eq!((last.producer_epoch, last.base_sequence, last.records), (3, 0, 1));
}

/// A half of several records answered `MESSAGE_TOO_LARGE` again is rejected, not halved again:
/// the split happens once per partition batch.
#[tokio::test]
async fn message_too_large_on_a_half_is_rejected_not_split_again() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::Error(code::MESSAGE_TOO_LARGE)));
    cluster.script(Step::produce(None, Action::Error(code::MESSAGE_TOO_LARGE)));
    let mut pending = prepare(&mut producer, keyed_for(0, 1, 4)).await;
    assert_eq!(class(&producer.attempt(&mut pending).await), Some(Fault::Clean));
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), Some(Fault::Clean), "the second half waits behind the first");
    assert_eq!(pending.parts.len(), 2, "no second split");
    assert_eq!(
        (attempt.settled[0].records, &attempt.settled[0].settlement),
        (2, &Settlement::Rejected { code: code::MESSAGE_TOO_LARGE })
    );
    assert_eq!(class(&producer.attempt(&mut pending).await), None);
    assert_eq!(values_in(&cluster, 0).len(), 2);
}

#[tokio::test]
async fn message_too_large_on_one_record_is_rejected() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::Error(code::MESSAGE_TOO_LARGE)));
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[Some(Fault::Rejected)]).await;
}

/// `INVALID_TIMESTAMP` naming one record: it's dropped and counted, and the rest go out once
/// under the new epoch.
#[tokio::test]
async fn named_records_are_dropped_and_the_rest_resent_once() {
    for invalid in [code::INVALID_TIMESTAMP, code::INVALID_RECORD] {
        let cluster = cluster(1, &[0]).await;
        let mut producer = Producer::new(config(&cluster)).unwrap();
        cluster.script(Step::produce(None, Action::RecordErrors(invalid, vec![1])));
        let mut pending = prepare(&mut producer, keyed_for(0, 1, 3)).await;
        let attempt = producer.attempt(&mut pending).await;
        assert_eq!(class(&attempt), Some(Fault::Clean));
        assert_eq!(attempt.settled[0].settlement, Settlement::Dropped { code: invalid });
        assert_eq!(attempt.settled[0].records, 1);
        assert_eq!(class(&producer.attempt(&mut pending).await), None);
        assert_eq!(values_in(&cluster, 0), vec![Bytes::from("value 0"), Bytes::from("value 2")]);
        let last = received(&cluster).pop().unwrap();
        assert_eq!((last.producer_epoch, last.base_sequence), (1, 0));

        // Records named again in the resend: the remainder is rejected whole, not trimmed and
        // resent a second time.
        cluster.script(Step::produce(None, Action::RecordErrors(invalid, vec![0])));
        cluster.script(Step::produce(None, Action::RecordErrors(invalid, vec![0])));
        let mut pending = prepare(&mut producer, keyed_for(0, 1, 3)).await;
        assert_eq!(class(&producer.attempt(&mut pending).await), Some(Fault::Clean));
        let attempt = producer.attempt(&mut pending).await;
        assert_eq!(class(&attempt), Some(Fault::Rejected), "{:?}", attempt.result);
        assert_eq!(
            (attempt.settled[0].records, &attempt.settled[0].settlement),
            (2, &Settlement::Rejected { code: invalid })
        );
    }
}

/// Below Produce v8 no record is named, so the partition batch is rejected.
#[tokio::test]
async fn named_records_below_produce_v8_are_rejected() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().produce_max = 7;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::RecordErrors(code::INVALID_TIMESTAMP, vec![1])));
    send_expecting(&mut producer, keyed_for(0, 1, 3), &[Some(Fault::Rejected)]).await;
}

/// One row per error code of the module doc's table: the attempt's class, whether the next
/// attempt refreshes metadata first, and that the retry of an unsettled batch is accepted.
#[tokio::test]
async fn each_error_code_row_classifies_the_partition_batch() {
    use code::*;
    struct Row {
        code: i16,
        class: Option<Fault>,
        refresh: bool,
    }
    let row = |code, class, refresh| Row { code, class, refresh };
    let rows = [
        row(CORRUPT_MESSAGE, Some(Fault::Rejected), false),
        row(UNKNOWN_TOPIC_OR_PARTITION, Some(Fault::Clean), true),
        row(LEADER_NOT_AVAILABLE, Some(Fault::Clean), true),
        row(NOT_LEADER_OR_FOLLOWER, Some(Fault::Clean), true),
        row(REQUEST_TIMED_OUT, Some(Fault::Ambiguous), false),
        row(NETWORK_EXCEPTION, Some(Fault::Ambiguous), false),
        row(RECORD_LIST_TOO_LARGE, Some(Fault::Rejected), false),
        row(NOT_ENOUGH_REPLICAS, Some(Fault::Clean), false),
        row(NOT_ENOUGH_REPLICAS_AFTER_APPEND, Some(Fault::Ambiguous), false),
        row(INVALID_REQUIRED_ACKS, Some(Fault::Refused), false),
        row(TOPIC_AUTHORIZATION_FAILED, Some(Fault::Refused), false),
        row(CLUSTER_AUTHORIZATION_FAILED, Some(Fault::Refused), false),
        row(UNSUPPORTED_VERSION, Some(Fault::Refused), false),
        row(POLICY_VIOLATION, Some(Fault::Rejected), false),
        row(OUT_OF_ORDER_SEQUENCE_NUMBER, Some(Fault::Ambiguous), false),
        row(DUPLICATE_SEQUENCE_NUMBER, None, false),
        row(INVALID_PRODUCER_EPOCH, Some(Fault::Clean), false),
        row(KAFKA_STORAGE_ERROR, Some(Fault::Clean), true),
        row(UNKNOWN_PRODUCER_ID, Some(Fault::Clean), false),
        row(UNSUPPORTED_COMPRESSION_TYPE, Some(Fault::Refused), false),
        row(THROTTLING_QUOTA_EXCEEDED, Some(Fault::Clean), false),
        row(UNKNOWN_TOPIC_ID, Some(Fault::Clean), true),
        row(INCONSISTENT_TOPIC_ID, Some(Fault::Clean), true),
        // GROUP_AUTHORIZATION_FAILED, a code with no row of its own.
        row(30, Some(Fault::Rejected), false),
    ];
    for Row { code, class: want, refresh } in rows {
        let name = super::fault::name(code);
        let cluster = cluster(1, &[0]).await;
        let mut producer = Producer::new(config(&cluster)).unwrap();
        cluster.script(Step::produce(None, Action::Error(code)));
        let mut pending = prepare(&mut producer, keyed_for(0, 1, 2)).await;
        let attempt = producer.attempt(&mut pending).await;
        assert_eq!(class(&attempt), want, "{name}: {:?}", attempt.result);
        if matches!(want, None | Some(Fault::Rejected)) {
            continue;
        }
        let metadata = requests_of(&cluster, ApiKey::Metadata);
        let retry = producer.attempt(&mut pending).await;
        assert_eq!(class(&retry), None, "{name}: the retry is accepted: {:?}", retry.result);
        let refreshed = requests_of(&cluster, ApiKey::Metadata) > metadata;
        assert_eq!(refreshed, refresh, "{name}: metadata refreshed before the retry");
        assert_eq!(values_in(&cluster, 0).len(), 2, "{name}");
    }
}

/// `OUT_OF_ORDER_SEQUENCE_NUMBER` bumps the epoch; `INVALID_PRODUCER_EPOCH` gets a new PID.
#[tokio::test]
async fn sequence_and_epoch_errors_renew_the_producer_before_the_retry() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::Error(code::OUT_OF_ORDER_SEQUENCE_NUMBER)));
    send_expecting(&mut producer, keyed_for(0, 1, 2), &[Some(Fault::Ambiguous), None]).await;
    assert_eq!(producer.identity(), Some(Identity { producer_id: FIRST_PID, epoch: 1 }));

    cluster.script(Step::produce(None, Action::Error(code::INVALID_PRODUCER_EPOCH)));
    send_expecting(&mut producer, keyed_for(0, 1, 2), &[Some(Fault::Clean), None]).await;
    assert_eq!(producer.identity(), Some(Identity { producer_id: FIRST_PID + 1, epoch: 0 }));
    assert_eq!(values_in(&cluster, 0).len(), 4);
}

/// The broker refuses the stale epoch itself: a batch under epoch 0 after another producer
/// instance moved the same PID to epoch 1.
#[tokio::test]
async fn the_fake_broker_fences_a_stale_epoch() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    let mut newer = Producer::new(config(&cluster)).unwrap();
    newer.force_identity(Identity { producer_id: FIRST_PID, epoch: 1 }, false);
    send_expecting(&mut newer, keyed_for(0, 1, 1), &[None]).await;
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[Some(Fault::Clean), None]).await;
    assert_eq!(producer.identity().unwrap().producer_id, FIRST_PID + 1);
}

/// The class of a send covers only its unsettled partitions, strongest first, and a retry
/// resends only those.
#[tokio::test]
async fn settled_partitions_are_not_resent_and_the_strongest_class_wins() {
    let cluster = cluster(1, &[0, 0, 0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(Some(1), Action::Error(code::REQUEST_TIMED_OUT)));
    cluster.script(Step::produce(Some(2), Action::Error(code::TOPIC_AUTHORIZATION_FAILED)));
    let mut records = keyed_for(0, 3, 1);
    records.extend(keyed_for(1, 3, 1));
    records.extend(keyed_for(2, 3, 1));
    send_expecting(&mut producer, records, &[Some(Fault::Refused), None]).await;
    let per_partition = |p: i32| received(&cluster).iter().filter(|r| r.partition == p).count();
    assert_eq!(per_partition(0), 1, "accepted on the first attempt, never resent");
    assert_eq!(per_partition(1), 2);
    assert_eq!(per_partition(2), 2);
}

/// With every partition batch rejected the send is `Rejected`; with one accepted beside a
/// rejection it succeeds.
#[tokio::test]
async fn a_send_fails_rejected_only_when_nothing_was_accepted() {
    let cluster = cluster(1, &[0, 0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(Some(0), Action::Error(code::POLICY_VIOLATION)));
    let mut records = keyed_for(0, 2, 1);
    records.extend(keyed_for(1, 2, 1));
    send_expecting(&mut producer, records, &[None]).await;

    cluster.script(Step::produce(None, Action::Error(code::POLICY_VIOLATION)));
    let mut pending = prepare(&mut producer, keyed_for(1, 2, 1)).await;
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), Some(Fault::Rejected));
    let message = format!("{:#}", attempt.result.unwrap_err());
    assert!(message.contains("PolicyViolation (44)"), "{message}");
}

/// A partition's records cut into several batches go out in order, one request each; once one
/// fails, the batches behind it wait for the next attempt.
#[tokio::test]
async fn several_batches_of_one_partition_go_in_order_and_stop_behind_a_failure() {
    let cluster = cluster(1, &[0]).await;
    let mut producer =
        Producer::new(ProducerConfig { max_request_bytes: 220, ..config(&cluster) }).unwrap();
    cluster.script(Step::produce(None, Action::Error(code::NOT_ENOUGH_REPLICAS)));
    let pending =
        send_expecting(&mut producer, keyed_for(0, 1, 6), &[Some(Fault::Clean), None]).await;
    assert!(pending.parts.len() > 1, "cut into {} batches", pending.parts.len());
    let got = received(&cluster);
    assert_eq!(got.len(), 1 + pending.parts.len(), "only the first went out on the failure");
    let mut next = 0;
    for r in &got[1..] {
        assert_eq!((r.producer_epoch, r.base_sequence), (0, next));
        next += r.records as i32;
    }
    assert_eq!(values_in(&cluster, 0).len(), 6);
}

#[tokio::test]
async fn a_record_over_max_request_bytes_is_dropped_before_any_request() {
    let cluster = cluster(1, &[0]).await;
    let mut producer =
        Producer::new(ProducerConfig { max_request_bytes: 200, ..config(&cluster) }).unwrap();
    let mut records = keyed_for(0, 1, 2);
    records[1].value = Some(Bytes::from(vec![b'x'; 200]));
    let pending = send_expecting(&mut producer, records, &[None]).await;
    assert_eq!(pending.oversize, 1);
    assert_eq!(values_in(&cluster, 0).len(), 1);
}

/// The leader moved: the old one answers `NOT_LEADER_OR_FOLLOWER`, metadata is refreshed, and
/// the retry goes to the new leader with the same sequence.
#[tokio::test]
async fn a_moved_leader_is_followed_after_a_refresh() {
    let cluster = cluster(2, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    let mut pending = prepare(&mut producer, keyed_for(0, 1, 2)).await;
    cluster.state().topics.get_mut(TOPIC).unwrap()[0].leader = 1;
    assert_eq!(class(&producer.attempt(&mut pending).await), Some(Fault::Clean));
    assert_eq!(class(&producer.attempt(&mut pending).await), None);
    let got: Vec<_> =
        received(&cluster).iter().map(|r| (r.node, r.base_sequence, r.answered)).collect();
    assert_eq!(got, vec![(0, 0, code::NOT_LEADER_OR_FOLLOWER), (1, 0, code::NONE)]);
}

/// Transport rows: a refused connect to the bootstrap broker is `Clean`.
#[tokio::test]
async fn a_refused_bootstrap_connect_is_clean() {
    // A port nothing listens on: bound, read, and closed.
    let port = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
    let mut producer = Producer::new(ProducerConfig {
        brokers: vec![port.to_string()],
        topic: TOPIC.to_string(),
        acks: Acks::All,
        idempotent: true,
        compression: Compression::None,
        timeout: RECV_TIMEOUT,
        max_request_bytes: 1 << 20,
        tls: None,
        client_id: "logit-test".to_string(),
        sasl: None,
        telemetry: Default::default(),
    })
    .unwrap();
    let err = producer.prepare(&mut vec![record(None, "v")]).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
}

/// A refused connect to a partition's leader is `Clean`, and the other leader's partition is
/// still accepted.
#[tokio::test]
async fn a_refused_leader_connect_is_clean() {
    let mut cluster = cluster(2, &[0, 1]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    let mut records = keyed_for(0, 2, 1);
    records.extend(keyed_for(1, 2, 1));
    let mut pending = prepare(&mut producer, records).await;
    cluster.stop(1).await;
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), Some(Fault::Clean), "{:?}", attempt.result);
    assert_eq!(values_in(&cluster, 0).len(), 1);
}

/// The broker reads the request and closes without answering: `Ambiguous`.
#[tokio::test]
async fn a_close_after_the_request_is_written_is_ambiguous() {
    let cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    cluster.script(Step::produce(None, Action::Close));
    send_expecting(&mut producer, keyed_for(0, 1, 2), &[Some(Fault::Ambiguous), None]).await;
    assert_eq!(values_in(&cluster, 0).len(), 2);
}

/// The broker never answers: the response wait times out, `Ambiguous`.
#[tokio::test]
async fn a_response_timeout_is_ambiguous() {
    let cluster = cluster(1, &[0]).await;
    // The timeout under test: long enough for loopback connects and the three requests before
    // the produce, short enough not to slow the suite.
    let timeout = Duration::from_millis(500);
    let mut producer = Producer::new(ProducerConfig { timeout, ..config(&cluster) }).unwrap();
    let mut pending = prepare(&mut producer, keyed_for(0, 1, 2)).await;
    cluster.script(Step::produce(None, Action::Hang));
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), Some(Fault::Ambiguous), "{:?}", attempt.result);
    let message = format!("{:#}", attempt.result.unwrap_err());
    assert!(message.contains("timed out"), "{message}");
}

/// A pooled connection the broker closed is probed and redialed before the request is
/// written, so the attempt succeeds instead of failing `Ambiguous`.
#[tokio::test]
async fn a_pooled_connection_the_broker_closed_is_redialed() {
    let mut cluster = cluster(1, &[0]).await;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    let accepted = cluster.state().accepted_connections;
    cluster.restart(0).await;
    wait_until("the broker closed every connection", || cluster.state().open_connections == 0)
        .await;
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    assert!(cluster.state().accepted_connections > accepted);
}

#[tokio::test]
async fn a_broker_without_produce_v3_is_refused() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().produce_max = 2;
    let mut producer = Producer::new(config(&cluster)).unwrap();
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
}

#[tokio::test]
async fn zstd_needs_produce_v7() {
    for (max, want) in [(6, Some(Fault::Refused)), (7, None)] {
        let cluster = cluster(1, &[0]).await;
        cluster.state().produce_max = max;
        let mut producer =
            Producer::new(ProducerConfig { compression: Compression::Zstd, ..config(&cluster) })
                .unwrap();
        send_expecting(&mut producer, keyed_for(0, 1, 2), &[want]).await;
        if want.is_none() {
            assert_eq!(received(&cluster)[0].compression, Compression::Zstd);
            assert_eq!(received(&cluster)[0].version, 7);
        }
    }
}

/// Every codec through the fake broker, which decompresses with the same codecs.
#[tokio::test]
async fn every_codec_is_accepted_by_the_broker() {
    for codec in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        let cluster = cluster(1, &[0]).await;
        let mut producer =
            Producer::new(ProducerConfig { compression: codec, ..config(&cluster) }).unwrap();
        send_expecting(&mut producer, keyed_for(0, 1, 3), &[None]).await;
        assert_eq!(received(&cluster)[0].compression, codec);
        assert_eq!(values_in(&cluster, 0).len(), 3, "{codec:?}");
    }
}

#[tokio::test]
async fn a_missing_topic_is_refused_only_after_a_refresh_confirms_it() {
    // The first answer schedules a refresh, which polls in the same attempt until `timeout`;
    // the topic is still missing after it, so the attempt is `Refused`, and so is the next, with
    // one request and no second poll.
    let cluster = FakeCluster::start(1).await;
    let short = ProducerConfig { timeout: Duration::from_millis(300), ..config(&cluster) };
    let mut producer = Producer::new(short).unwrap();
    let err = producer.prepare(&mut vec![record(None, "v")]).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
    let polled = requests_of(&cluster, ApiKey::Metadata);
    assert!(polled >= 2, "the refresh ran: {polled} Metadata requests");
    let err = producer.prepare(&mut vec![record(None, "v")]).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
    assert_eq!(requests_of(&cluster, ApiKey::Metadata), polled + 1);

    // A broker that creates the topic answers the request that created it as missing; the
    // refresh finds it, and the attempt goes through.
    let cluster = FakeCluster::start(1).await;
    cluster.state().auto_create = Some(3);
    let mut producer = Producer::new(config(&cluster)).unwrap();
    send_expecting(&mut producer, vec![record(None, "v")], &[None]).await;
    assert_eq!(cluster.state().topics[TOPIC].len(), 3);
    assert_eq!(requests_of(&cluster, ApiKey::Metadata), 2);

    // Still being created on the refresh: the poll waits for it.
    let cluster = FakeCluster::start(1).await;
    cluster.state().auto_create = Some(1);
    for answer in [code::UNKNOWN_TOPIC_OR_PARTITION, code::LEADER_NOT_AVAILABLE] {
        cluster.script(Step {
            api: ApiKey::Metadata,
            partition: None,
            topic: None,
            action: Action::Error(answer),
        });
    }
    let mut producer = Producer::new(config(&cluster)).unwrap();
    send_expecting(&mut producer, vec![record(None, "v")], &[None]).await;
}

/// A coordinator that's loading or moving answers `InitProducerId` with an error a later poll
/// clears, so the attempt polls instead of failing, and is `Clean` only once `timeout` passes.
#[tokio::test]
async fn a_loading_coordinator_is_polled_inside_the_attempt() {
    let coordinator = |cluster: &FakeCluster, answer| {
        cluster.script(Step {
            api: ApiKey::InitProducerId,
            partition: None,
            topic: None,
            action: Action::Error(answer),
        });
    };
    for answer in
        [code::COORDINATOR_LOAD_IN_PROGRESS, code::COORDINATOR_NOT_AVAILABLE, code::NOT_COORDINATOR]
    {
        let cluster = cluster(1, &[0]).await;
        coordinator(&cluster, answer);
        coordinator(&cluster, answer);
        let mut producer = Producer::new(config(&cluster)).unwrap();
        send_expecting(&mut producer, vec![record(None, "v")], &[None]).await;
        assert_eq!(requests_of(&cluster, ApiKey::InitProducerId), 3, "code {answer}");
        assert_eq!(values_in(&cluster, 0).len(), 1);
    }

    // Still loading at `timeout`: the attempt is `Clean`, and the next one asks again.
    let cluster = cluster(1, &[0]).await;
    for _ in 0..64 {
        coordinator(&cluster, code::COORDINATOR_LOAD_IN_PROGRESS);
    }
    let short = ProducerConfig { timeout: Duration::from_millis(300), ..config(&cluster) };
    let mut producer = Producer::new(short).unwrap();
    let mut pending = prepare(&mut producer, vec![record(None, "v")]).await;
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), Some(Fault::Clean), "{:?}", attempt.result);
    let polled = requests_of(&cluster, ApiKey::InitProducerId);
    assert!(polled >= 2, "polled before the deadline: {polled} requests");
    assert!(polled < 64, "the deadline ended the poll: {polled} requests");
}

/// The same rule for a `Produce` answer: the first is `Clean`, one that persists after the
/// refresh is `Refused`, and an accepted batch starts the count again.
#[tokio::test]
async fn a_missing_topic_produce_answer_is_refused_only_after_a_refresh() {
    for code in [code::UNKNOWN_TOPIC_OR_PARTITION, code::UNKNOWN_TOPIC_ID] {
        let cluster = cluster(1, &[0]).await;
        let mut producer = Producer::new(config(&cluster)).unwrap();
        cluster.script(Step::produce(None, Action::Error(code)));
        cluster.script(Step::produce(None, Action::Error(code)));
        let metadata = requests_of(&cluster, ApiKey::Metadata);
        send_expecting(
            &mut producer,
            keyed_for(0, 1, 2),
            &[Some(Fault::Clean), Some(Fault::Refused), None],
        )
        .await;
        assert!(requests_of(&cluster, ApiKey::Metadata) > metadata, "refreshed before the retry");
        cluster.script(Step::produce(None, Action::Error(code)));
        send_expecting(&mut producer, keyed_for(0, 1, 2), &[Some(Fault::Clean), None]).await;
    }
}

#[tokio::test]
async fn acks_none_counts_a_written_request_as_accepted() {
    let cluster = cluster(1, &[0]).await;
    let mut producer =
        Producer::new(ProducerConfig { acks: Acks::None, idempotent: false, ..config(&cluster) })
            .unwrap();
    let mut pending = prepare(&mut producer, keyed_for(0, 1, 2)).await;
    let attempt = producer.attempt(&mut pending).await;
    assert_eq!(class(&attempt), None);
    assert_eq!(attempt.settled[0].settlement, Settlement::Accepted { base_offset: -1 });
    wait_until("the broker appended the acks: none batch", || values_in(&cluster, 0).len() == 2)
        .await;
    let got = received(&cluster);
    assert_eq!((got[0].acks, got[0].producer_id, got[0].base_sequence), (0, -1, -1));
    assert_eq!(requests_of(&cluster, ApiKey::InitProducerId), 0);
}

#[tokio::test]
async fn acks_leader_without_idempotence_sends_no_producer_id() {
    let cluster = cluster(1, &[0]).await;
    let mut producer =
        Producer::new(ProducerConfig { acks: Acks::Leader, idempotent: false, ..config(&cluster) })
            .unwrap();
    send_expecting(&mut producer, keyed_for(0, 1, 2), &[None]).await;
    let got = received(&cluster);
    assert_eq!((got[0].acks, got[0].producer_id, got[0].base_sequence), (1, -1, -1));
}

#[test]
fn idempotence_without_acks_all_is_a_construction_error() {
    let config = ProducerConfig {
        brokers: vec!["kafka:9092".to_string()],
        topic: TOPIC.to_string(),
        acks: Acks::Leader,
        idempotent: true,
        compression: Compression::None,
        timeout: RECV_TIMEOUT,
        max_request_bytes: 1 << 20,
        tls: None,
        client_id: "logit-test".to_string(),
        sasl: None,
        telemetry: Default::default(),
    };
    assert!(Producer::new(config).is_err());
}

/// TLS to every broker, the leader's server name its advertised host (`localhost`) rather than
/// the bootstrap address (`127.0.0.1`), with and without a client certificate.
#[tokio::test]
async fn tls_and_mutual_tls_reach_the_leader_by_its_advertised_host() {
    use crate::test_support::{client_config, server_tls_config, tls_settings};
    for mutual in [false, true] {
        let cluster =
            FakeCluster::start_with(1, Some(server_tls_config(mutual)), "localhost").await;
        cluster.state().add_topic(TOPIC, &[0]);
        let settings = tls_settings(|s| {
            s.ca_file = Some("ca.pem".to_string());
            if mutual {
                s.cert_file = Some("client.pem".to_string());
                s.key_file = Some("client.key".to_string());
            }
        });
        let tls = Some(std::sync::Arc::new(client_config(&settings)));
        let mut producer = Producer::new(ProducerConfig {
            brokers: vec![format!("127.0.0.1:{}", cluster.port(0))],
            tls,
            ..config(&cluster)
        })
        .unwrap();
        send_expecting(&mut producer, keyed_for(0, 1, 2), &[None]).await;
        assert_eq!(values_in(&cluster, 0).len(), 2, "mutual: {mutual}");
    }
}

/// A TLS handshake that fails verification is `Clean`.
#[tokio::test]
async fn a_failed_tls_handshake_is_clean() {
    use crate::test_support::{client_config, server_tls_config, tls_settings};
    let cluster = FakeCluster::start_with(1, Some(server_tls_config(false)), "localhost").await;
    cluster.state().add_topic(TOPIC, &[0]);
    // The bundled Mozilla roots don't trust the fixture CA.
    let tls = Some(std::sync::Arc::new(client_config(&tls_settings(|_| {}))));
    let mut producer = Producer::new(ProducerConfig { tls, ..config(&cluster) }).unwrap();
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
    assert!(format!("{err:#}").contains("TLS handshake"), "{err:#}");
}

// ---- SASL (docs/adr/kafka-sasl.md) ---------------------------------------------------------

const SASL_USER: &str = "alice";
const SASL_PASSWORD: &str = "pw-never-printed";

fn sasl_settings(mechanism: &str, password: &str) -> KafkaSasl {
    let (username, password) = (SASL_USER.to_string(), password.to_string());
    match mechanism {
        "PLAIN" => KafkaSasl::Plain { username, password },
        "SCRAM-SHA-256" => KafkaSasl::ScramSha256 { username, password },
        "SCRAM-SHA-512" => KafkaSasl::ScramSha512 { username, password },
        other => panic!("no mechanism {other}"),
    }
}

/// A cluster of `nodes` requiring SASL for [`SASL_USER`] and [`SASL_PASSWORD`].
async fn sasl_cluster(nodes: usize, leaders: &[i32]) -> FakeCluster {
    let cluster = cluster(nodes, leaders).await;
    cluster.state().sasl = Some(Sasl::new(SASL_USER, SASL_PASSWORD));
    cluster
}

fn sasl_producer(cluster: &FakeCluster, mechanism: &str, password: &str) -> Producer {
    let sasl = Some(authenticator(&sasl_settings(mechanism, password), RECV_TIMEOUT));
    Producer::new(ProducerConfig { sasl, ..config(cluster) }).unwrap()
}

/// The error a first send fails with, after asserting its class and that it names the mechanism
/// and the user but never the password.
async fn sasl_refusal(producer: &mut Producer, mechanism: &str, want: Fault) -> String {
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), want, "{message}");
    assert!(message.contains(mechanism) && message.contains(SASL_USER), "{message}");
    assert!(!message.contains(SASL_PASSWORD), "{message}");
    message
}

/// Each mechanism authenticates every connection, the bootstrap one and each leader's, before
/// any other request.
#[tokio::test]
async fn each_sasl_mechanism_authenticates_every_connection_and_delivers() {
    for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let cluster = sasl_cluster(2, &[0, 1]).await;
        let mut producer = sasl_producer(&cluster, mechanism, SASL_PASSWORD);
        let mut records = keyed_for(0, 2, 2);
        records.extend(keyed_for(1, 2, 2));
        send_expecting(&mut producer, records, &[None]).await;
        assert_eq!((values_in(&cluster, 0).len(), values_in(&cluster, 1).len()), (2, 2));
        let state = cluster.state();
        assert_eq!(state.authentications, state.accepted_connections, "{mechanism}");
        assert_eq!(state.accepted_connections, 3, "{mechanism}: bootstrap and both leaders");
        let handshakes: Vec<i16> = state
            .requests
            .iter()
            .filter(|(_, k, _)| *k == ApiKey::SaslHandshake)
            .map(|(_, _, v)| *v)
            .collect();
        assert_eq!(handshakes, vec![1; 3], "{mechanism}: SaslHandshake v1 per connection");
        assert!(
            state
                .requests
                .iter()
                .filter(|(_, k, _)| *k == ApiKey::SaslAuthenticate)
                .all(|(_, _, v)| *v == 2),
            "{mechanism}: SaslAuthenticate v2"
        );
    }
}

/// Response-class rows `SASL_AUTHENTICATION_FAILED` (58), `UNSUPPORTED_SASL_MECHANISM` (33),
/// `ILLEGAL_SASL_STATE` (34), and a broker offering no SASL: each is `Refused`, and the
/// message carries the broker's reason and no password.
#[tokio::test]
async fn sasl_refusals_are_refused_and_never_print_the_password() {
    for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let cluster = sasl_cluster(1, &[0]).await;
        let mut producer = sasl_producer(&cluster, mechanism, "wrong-password");
        let message = sasl_refusal(&mut producer, mechanism, Fault::Refused).await;
        assert!(message.contains("SaslAuthenticationFailed (58)"), "{message}");
        assert!(message.contains("Authentication failed"), "the broker's text: {message}");
        assert!(!message.contains("wrong-password"), "{message}");
        assert!(values_in(&cluster, 0).is_empty());
    }

    let cluster = sasl_cluster(1, &[0]).await;
    cluster.state().sasl.as_mut().unwrap().mechanisms = vec!["PLAIN".into()];
    let mut producer = sasl_producer(&cluster, "SCRAM-SHA-512", SASL_PASSWORD);
    let message = sasl_refusal(&mut producer, "SCRAM-SHA-512", Fault::Refused).await;
    assert!(message.contains("UnsupportedSaslMechanism (33)"), "{message}");
    assert!(message.contains("it offers [PLAIN]"), "{message}");

    // A listener without SASL answers the handshake ILLEGAL_SASL_STATE.
    let plaintext = self::cluster(1, &[0]).await;
    let mut producer = sasl_producer(&plaintext, "PLAIN", SASL_PASSWORD);
    let message = sasl_refusal(&mut producer, "PLAIN", Fault::Refused).await;
    assert!(message.contains("IllegalSaslState (34)"), "{message}");

    let cluster = sasl_cluster(1, &[0]).await;
    cluster.script(Step {
        api: ApiKey::SaslAuthenticate,
        partition: None,
        topic: None,
        action: Action::Error(code::ILLEGAL_SASL_STATE),
    });
    let mut producer = sasl_producer(&cluster, "SCRAM-SHA-256", SASL_PASSWORD);
    let message = sasl_refusal(&mut producer, "SCRAM-SHA-256", Fault::Refused).await;
    assert!(message.contains("IllegalSaslState (34): scripted refusal"), "{message}");

    let cluster = sasl_cluster(1, &[0]).await;
    cluster.state().hide_sasl = true;
    let mut producer = sasl_producer(&cluster, "PLAIN", SASL_PASSWORD);
    let message = sasl_refusal(&mut producer, "PLAIN", Fault::Refused).await;
    assert!(message.contains("doesn't offer SaslHandshake v1"), "{message}");
}

/// A broker whose SCRAM server signature doesn't verify doesn't hold the user's credential, and
/// every connection to it gets the same answer: `Refused`, with no produce request sent.
#[tokio::test]
async fn a_scram_server_signature_mismatch_is_refused() {
    for mechanism in ["SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let cluster = sasl_cluster(1, &[0]).await;
        cluster.state().sasl.as_mut().unwrap().forge_signature = true;
        let mut producer = sasl_producer(&cluster, mechanism, SASL_PASSWORD);
        let message = sasl_refusal(&mut producer, mechanism, Fault::Refused).await;
        assert!(message.contains("server signature doesn't match"), "{message}");
        assert_eq!(requests_of(&cluster, ApiKey::Metadata), 0, "{mechanism}");
    }
}

/// A transport failure during the exchange is `Clean`: nothing of a produce request has been
/// written on the connection. The next attempt authenticates and delivers.
#[tokio::test]
async fn a_transport_failure_during_the_sasl_exchange_is_clean() {
    for api in [ApiKey::SaslHandshake, ApiKey::SaslAuthenticate] {
        let cluster = sasl_cluster(1, &[0]).await;
        cluster.script(Step { api, partition: None, topic: None, action: Action::Close });
        let mut producer = sasl_producer(&cluster, "SCRAM-SHA-256", SASL_PASSWORD);
        let message = sasl_refusal(&mut producer, "SCRAM-SHA-256", Fault::Clean).await;
        assert!(message.contains(&format!("{api:?}")), "{message}");
        send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
        assert_eq!(values_in(&cluster, 0).len(), 1, "{api:?}");
    }
}

/// A connection past its SASL session's deadline is redialed before the next request instead of
/// being written to, so the broker never closes it for an expired session and the send
/// succeeds on its first attempt, with each record appended once.
#[tokio::test]
async fn an_expiring_sasl_session_is_redialed_before_the_next_send() {
    let cluster = sasl_cluster(1, &[0]).await;
    // The lifetime under test. The connection's deadline falls at half of it (a lifetime of
    // twice the timeout or less keeps half as its margin), 1 s after authentication: that
    // covers the first send's four loopback round trips by far, and the wait below is 2 s.
    cluster.state().sasl.as_mut().unwrap().session_lifetime_ms = 2000;
    let sasl = Some(authenticator(&sasl_settings("SCRAM-SHA-512", SASL_PASSWORD), RECV_TIMEOUT));
    let mut probe = TelemetryProbe::new();
    let telemetry = probe.telemetry("out", "kafka_out", "sink");
    let mut producer =
        Producer::new(ProducerConfig { sasl, telemetry, ..config(&cluster) }).unwrap();
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    assert_eq!(probe.sum(RECONNECTS, &[]), 0.0, "a first dial to each node isn't a reconnect");
    let authenticated = cluster.state().authentications;

    let ends = cluster.state().session_ends.clone();
    assert_eq!(ends.len(), authenticated, "every session has a lifetime");
    wait_until("every SASL session has ended at the broker", || {
        ends.iter().all(|&end| tokio::time::Instant::now() >= end)
    })
    .await;
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;

    let state = cluster.state();
    assert_eq!(state.expired_closes, 0, "nothing was written to an expired session");
    assert!(state.authentications > authenticated, "a redial authenticated again");
    assert_eq!(state.partition(TOPIC, 0).records().len(), 2, "each record once");
    drop(state);
    // The second send's metadata is fresh, so only the leader's connection is redialed.
    assert_eq!(probe.sum(RECONNECTS, &[]), 1.0, "the redial counts");
}

/// One job writes its requests one after another, so the deadline is checked before each: the
/// broker holds the first request's answer until the session ends, and the requests after it
/// wait for the next attempt's connection instead of reaching the expired session.
#[tokio::test]
async fn a_sasl_deadline_reached_between_requests_leaves_the_rest_for_a_new_connection() {
    let cluster = sasl_cluster(1, &[0]).await;
    // The deadline falls at half the lifetime, so the held answer arrives past it, and well
    // inside the timeout.
    cluster.state().sasl.as_mut().unwrap().session_lifetime_ms = 1000;
    let sasl = Some(authenticator(&sasl_settings("SCRAM-SHA-512", SASL_PASSWORD), RECV_TIMEOUT));
    let mut producer =
        Producer::new(ProducerConfig { sasl, max_request_bytes: 220, ..config(&cluster) }).unwrap();
    cluster.script(Step::produce(None, Action::AnswerAtSessionEnd));
    let pending =
        send_expecting(&mut producer, keyed_for(0, 1, 6), &[Some(Fault::Clean), None]).await;
    assert!(pending.parts.len() > 1, "cut into {} batches", pending.parts.len());

    let state = cluster.state();
    assert_eq!(state.expired_closes, 0, "nothing was written to an expired session");
    assert_eq!(state.received.len(), pending.parts.len(), "every batch went out once");
    assert_eq!(state.partition(TOPIC, 0).records().len(), 6, "each record once");
}

// ---- OAUTHBEARER (docs/adr/kafka-sasl.md) ----------------------------------------------------

const CLIENT_ID: &str = "logit-client";
const CLIENT_SECRET: &str = "client-secret-never-printed";

fn client_credentials(endpoint: &TokenEndpoint, extensions: &[(&str, &str)]) -> KafkaSasl {
    KafkaSasl::OAuthBearer {
        token: OAuthToken::ClientCredentials {
            endpoint: endpoint.url.clone(),
            client_id: CLIENT_ID.to_string(),
            client_secret: CLIENT_SECRET.to_string(),
            scope: Some("kafka".to_string()),
        },
        extensions: extensions.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
    }
}

fn oauth_producer(cluster: &FakeCluster, settings: &KafkaSasl) -> Producer {
    let sasl = Some(authenticator(settings, RECV_TIMEOUT));
    Producer::new(ProducerConfig { sasl, ..config(cluster) }).unwrap()
}

/// A cluster of `nodes` requiring `OAUTHBEARER` with `token`, or any token for `None`.
async fn oauth_cluster(nodes: usize, leaders: &[i32], token: Option<&str>) -> FakeCluster {
    let cluster = cluster(nodes, leaders).await;
    let mut sasl = Sasl::new(SASL_USER, SASL_PASSWORD);
    sasl.oauth_token = token.map(String::from);
    cluster.state().sasl = Some(sasl);
    cluster
}

/// The token each `OAUTHBEARER` message carried, in arrival order.
fn tokens_presented(cluster: &FakeCluster) -> Vec<String> {
    cluster
        .state()
        .oauth_messages
        .iter()
        .map(|m| {
            let text = String::from_utf8_lossy(m);
            let rest = text.split_once("auth=Bearer ").unwrap().1;
            rest[..rest.find('\u{1}').unwrap()].to_string()
        })
        .collect()
}

/// A fixed token authenticates every connection, and its extensions ride in every message.
#[tokio::test]
async fn oauthbearer_with_a_fixed_token_delivers_with_its_extensions() {
    let cluster = oauth_cluster(2, &[0, 1], Some("fixed.token-1")).await;
    let extensions = vec![
        ("identityPoolId".to_string(), "pool-1".to_string()),
        ("logicalCluster".to_string(), "lkc-1".to_string()),
    ];
    let settings = KafkaSasl::OAuthBearer {
        token: OAuthToken::Fixed("fixed.token-1".into()),
        extensions: extensions.clone(),
    };
    let mut producer = oauth_producer(&cluster, &settings);
    let mut records = keyed_for(0, 2, 2);
    records.extend(keyed_for(1, 2, 2));
    send_expecting(&mut producer, records, &[None]).await;
    assert_eq!((values_in(&cluster, 0).len(), values_in(&cluster, 1).len()), (2, 2));
    let state = cluster.state();
    assert_eq!(state.authentications, 3, "bootstrap and both leaders");
    assert_eq!(
        state.oauth_messages,
        vec![
            b"n,,\x01auth=Bearer fixed.token-1\x01identityPoolId=pool-1\x01logicalCluster=lkc-1\x01\x01"
                .to_vec();
            3
        ]
    );
}

/// The client-credentials request is a form POST with the client in Basic authentication, and
/// its token serves every connection of the sink: one fetch for three connections, and one for
/// several dials that start at once.
#[tokio::test]
async fn client_credentials_fetch_one_token_for_every_connection() {
    let endpoint = TokenEndpoint::start().await;
    let cluster = oauth_cluster(2, &[0, 1], Some("token-0")).await;
    let mut producer = oauth_producer(&cluster, &client_credentials(&endpoint, &[]));
    let mut records = keyed_for(0, 2, 1);
    records.extend(keyed_for(1, 2, 1));
    send_expecting(&mut producer, records, &[None]).await;
    assert_eq!(cluster.state().authentications, 3);
    assert_eq!(tokens_presented(&cluster), vec!["token-0"; 3]);
    assert_eq!(endpoint.fetches(), 1);
    let request = endpoint.state().requests[0].clone();
    assert_eq!(request.body, "grant_type=client_credentials&scope=kafka");
    assert_eq!(request.content_type, "application/x-www-form-urlencoded");
    assert_eq!(
        request.authorization,
        format!(
            "Basic {}",
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{CLIENT_ID}:{CLIENT_SECRET}")
            )
        )
    );

    let endpoint = TokenEndpoint::start().await;
    let auth = authenticator(&client_credentials(&endpoint, &[]), RECV_TIMEOUT);
    let mut starts = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let auth = auth.clone();
        starts.spawn(async move { auth.start("broker").await.map(|_| ()) });
    }
    while let Some(started) = starts.join_next().await {
        started.unwrap().unwrap();
    }
    assert_eq!(endpoint.fetches(), 1, "concurrent dials wait on one fetch");
}

/// A token is reused until 80% of its `expires_in` has passed; a connection dialed after that,
/// here the redial of a session the broker gave a lifetime, presents a newly fetched token.
#[tokio::test]
async fn a_redial_after_the_token_is_due_presents_a_new_one() {
    let endpoint = TokenEndpoint::start().await;
    // Reused for 800 ms.
    endpoint.state().answer =
        Box::new(|n| (200, format!(r#"{{"access_token":"token-{n}","expires_in":1}}"#)));
    let cluster = oauth_cluster(1, &[0], None).await;
    // The connection's deadline falls at half the lifetime.
    cluster.state().sasl.as_mut().unwrap().session_lifetime_ms = 1000;
    let mut producer = oauth_producer(&cluster, &client_credentials(&endpoint, &[]));
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    let first_sent = tokio::time::Instant::now();
    assert_eq!(endpoint.fetches(), 1);

    let ends = cluster.state().session_ends.clone();
    let due = first_sent + Duration::from_millis(800);
    wait_until("the token is due and every session has ended", || {
        let now = tokio::time::Instant::now();
        now >= due && ends.iter().all(|&end| now >= end)
    })
    .await;
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;

    assert_eq!(endpoint.fetches(), 2);
    let presented = tokens_presented(&cluster);
    assert_eq!(presented.last().map(String::as_str), Some("token-1"), "{presented:?}");
    assert!(!presented.contains(&"token-2".to_string()), "{presented:?}");
    assert_eq!(cluster.state().expired_closes, 0);
    assert_eq!(values_in(&cluster, 0).len(), 2);
}

/// Token endpoint rows: unreachable, `429`, and `5xx` are `Clean`; any other answer and a `2xx`
/// without a usable token are `Refused`. No message carries the client secret, and the broker
/// sees no `SaslAuthenticate` for a token it was never sent.
#[tokio::test]
async fn token_endpoint_faults_are_clean_when_transient_and_refused_otherwise() {
    let cases: Vec<(u16, &str, Fault, &str)> = vec![
        (
            401,
            r#"{"error":"invalid_client"}"#,
            Fault::Refused,
            "401 Unauthorized: \"invalid_client\"",
        ),
        (
            400,
            r#"{"error":"invalid_scope","error_description":"no kafka"}"#,
            Fault::Refused,
            "\"invalid_scope\" (\"no kafka\")",
        ),
        (403, "forbidden", Fault::Refused, "403 Forbidden"),
        (302, "", Fault::Refused, "302 Found"),
        (200, "<html>sign in</html>", Fault::Refused, "isn't JSON"),
        (200, r#"{"access_token":"a b"}"#, Fault::Refused, "can't carry"),
        (429, "", Fault::Clean, "429 Too Many Requests"),
        (500, "", Fault::Clean, "500 Internal Server Error"),
        (503, "", Fault::Clean, "503 Service Unavailable"),
    ];
    for (status, body, want, detail) in cases {
        let endpoint = TokenEndpoint::start().await;
        let body = body.to_string();
        endpoint.state().answer = Box::new(move |_| (status, body.clone()));
        let cluster = oauth_cluster(1, &[0], None).await;
        let mut producer = oauth_producer(&cluster, &client_credentials(&endpoint, &[]));
        let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
        let message = format!("{err:#} {err:?}");
        assert_eq!(logit_pipeline::classify(&err), want, "{status}: {message}");
        assert!(message.contains(detail), "{status}: {message}");
        assert!(message.contains(&endpoint.url) && message.contains(CLIENT_ID), "{message}");
        assert!(!message.contains(CLIENT_SECRET), "{message}");
        assert_eq!(requests_of(&cluster, ApiKey::SaslAuthenticate), 0, "{status}");

        // Once the endpoint issues a token, the next attempt delivers.
        endpoint.state().answer =
            Box::new(|n| (200, format!(r#"{{"access_token":"token-{n}","expires_in":60}}"#)));
        send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
        assert_eq!(values_in(&cluster, 0).len(), 1, "{status}");
    }

    // Nothing listening: a port bound and released.
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}/oauth2/token");
    let cluster = oauth_cluster(1, &[0], None).await;
    let settings = KafkaSasl::OAuthBearer {
        token: OAuthToken::ClientCredentials {
            endpoint: url.clone(),
            client_id: CLIENT_ID.into(),
            client_secret: CLIENT_SECRET.into(),
            scope: None,
        },
        extensions: Vec::new(),
    };
    let mut producer = oauth_producer(&cluster, &settings);
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{message}");
    assert!(message.contains(&format!("requesting a token from {url}")), "{message}");
    assert!(!message.contains(CLIENT_SECRET), "{message}");

    // An endpoint that accepts the connection and never answers, against a short timeout.
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/oauth2/token", silent.local_addr().unwrap());
    let held = tokio::spawn(async move {
        let mut open = Vec::new();
        while let Ok((stream, _)) = silent.accept().await {
            open.push(stream);
        }
    });
    let settings = KafkaSasl::OAuthBearer {
        token: OAuthToken::ClientCredentials {
            endpoint: url.clone(),
            client_id: CLIENT_ID.into(),
            client_secret: CLIENT_SECRET.into(),
            scope: None,
        },
        extensions: Vec::new(),
    };
    let sasl = Some(authenticator(&settings, Duration::from_millis(200)));
    let mut producer = Producer::new(ProducerConfig { sasl, ..config(&cluster) }).unwrap();
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{message}");
    assert!(message.contains(&format!("token endpoint {url} timed out after 200ms")), "{message}");
    held.abort();
}

/// A `200` whose body the endpoint cuts off, by closing the connection short of its
/// `Content-Length` or by stalling past the sink's timeout, is a transport fault (`Clean`), not
/// a token response that isn't JSON (`Refused`).
#[tokio::test]
async fn a_cut_off_token_response_is_clean() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for (close, detail) in [(true, "reading token endpoint"), (false, "timed out after 200ms")] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/oauth2/token", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut open = Vec::new();
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = vec![0; 4096];
                let _ = stream.read(&mut request).await;
                let _ = stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                          Content-Length: 100\r\n\r\n{\"access_tok",
                    )
                    .await;
                if close {
                    drop(stream);
                } else {
                    open.push(stream);
                }
            }
        });
        let settings = KafkaSasl::OAuthBearer {
            token: OAuthToken::ClientCredentials {
                endpoint: url.clone(),
                client_id: CLIENT_ID.into(),
                client_secret: CLIENT_SECRET.into(),
                scope: None,
            },
            extensions: Vec::new(),
        };
        let cluster = oauth_cluster(1, &[0], None).await;
        let sasl = Some(authenticator(&settings, Duration::from_millis(200)));
        let mut producer = Producer::new(ProducerConfig { sasl, ..config(&cluster) }).unwrap();
        let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
        let message = format!("{err:#} {err:?}");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "close {close}: {message}");
        assert!(message.contains(detail), "close {close}: {message}");
        assert!(!message.contains("isn't JSON"), "close {close}: {message}");
        assert_eq!(requests_of(&cluster, ApiKey::SaslAuthenticate), 0);
        server.abort();
    }
}

/// A broker refusing the token sends Kafka's error challenge; the client ends the exchange with
/// a `kvsep`, and the refusal is `Refused` with the challenge's status and no token in it. The
/// refused token leaves the cache, so the next attempt fetches another and delivers.
#[tokio::test]
async fn a_refused_token_is_refused_with_its_status_and_fetched_again() {
    let endpoint = TokenEndpoint::start().await;
    let cluster = oauth_cluster(1, &[0], Some("token-1")).await;
    let mut producer = oauth_producer(&cluster, &client_credentials(&endpoint, &[]));
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{message}");
    assert!(message.contains("SaslAuthenticationFailed (58)"), "{message}");
    assert!(
        message.contains(r#"the broker refused the token: status "invalid_token""#),
        "{message}"
    );
    assert!(message.contains("OAUTHBEARER as client \"logit-client\""), "{message}");
    for secret in ["token-0", CLIENT_SECRET] {
        assert!(!message.contains(secret), "{message}");
    }
    let exchange: Vec<Vec<u8>> = cluster.state().oauth_messages.clone();
    assert_eq!(exchange.len(), 1);
    assert_eq!(requests_of(&cluster, ApiKey::SaslAuthenticate), 2, "the token, then a kvsep");

    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    assert_eq!(endpoint.fetches(), 2);
    assert_eq!(tokens_presented(&cluster).last().map(String::as_str), Some("token-1"));

    // A fixed token's refusal names no token either.
    let cluster = oauth_cluster(1, &[0], Some("the-right-token")).await;
    let settings = KafkaSasl::OAuthBearer {
        token: OAuthToken::Fixed("wrong-token".into()),
        extensions: vec![],
    };
    let mut producer = oauth_producer(&cluster, &settings);
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{message}");
    assert!(message.contains("OAUTHBEARER as a fixed token"), "{message}");
    assert!(message.contains("status \"invalid_token\""), "{message}");
    assert!(!message.contains("wrong-token"), "{message}");
}

/// Redpanda refuses a token with `SASL_AUTHENTICATION_FAILED` on the first message and no error
/// challenge. The refused token still leaves the cache, so the next attempt fetches another
/// rather than resending it until its refresh time.
#[tokio::test]
async fn a_token_refused_without_a_challenge_is_fetched_again() {
    let endpoint = TokenEndpoint::start().await;
    let cluster = oauth_cluster(1, &[0], Some("token-1")).await;
    cluster.state().sasl.as_mut().unwrap().oauth_no_challenge = true;
    let mut producer = oauth_producer(&cluster, &client_credentials(&endpoint, &[]));
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{message}");
    assert!(message.contains("SaslAuthenticationFailed (58)"), "{message}");
    assert!(!message.contains("the broker refused the token"), "{message}");
    assert_eq!(requests_of(&cluster, ApiKey::SaslAuthenticate), 1, "no kvsep without a challenge");

    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    assert_eq!(endpoint.fetches(), 2);
    let presented = tokens_presented(&cluster);
    assert_eq!(presented[0], "token-0", "{presented:?}");
    assert!(presented[1..].iter().all(|t| t == "token-1"), "{presented:?}");
}

// ---- AWS_MSK_IAM (docs/adr/kafka-sasl.md) ----------------------------------------------------

const AWS_KEY_ID: &str = "AKIDLOGITTEST";
const AWS_SECRET: &str = "aws-secret-never-printed";
const AWS_SESSION: &str = "aws-session-token-never-printed";

/// An [`Env`] over `vars`, so no test changes the process environment.
fn aws_env(vars: &[(&str, &str)]) -> Env {
    let vars: std::collections::BTreeMap<String, String> =
        vars.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    std::sync::Arc::new(move |name| vars.get(name).cloned())
}

/// Endpoints for the chain's HTTP sources that answer nothing: a closed port.
fn no_endpoints() -> Endpoints {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let base = format!("http://127.0.0.1:{port}");
    Endpoints { imds: base.clone(), container: base.clone(), sts: Some(base) }
}

/// A cluster of `nodes` requiring `AWS_MSK_IAM` for [`AWS_KEY_ID`] and [`AWS_SECRET`].
async fn msk_cluster(nodes: usize, leaders: &[i32]) -> FakeCluster {
    let cluster = cluster(nodes, leaders).await;
    cluster.state().sasl = Some(Sasl::new(AWS_KEY_ID, AWS_SECRET));
    cluster
}

fn msk_producer(
    cluster: &FakeCluster,
    region: Option<&str>,
    vars: &[(&str, &str)],
    endpoints: Endpoints,
) -> Producer {
    let sasl = Some(msk_iam_with(region, aws_env(vars), endpoints, RECV_TIMEOUT));
    Producer::new(ProducerConfig { sasl, ..config(cluster) }).unwrap()
}

/// `AWS_MSK_IAM` signs a payload per connection, the bootstrap one and each leader's, with the
/// environment's credentials; the fake broker checks every field and recomputes the signature.
#[tokio::test]
async fn aws_msk_iam_authenticates_every_connection_and_delivers() {
    let cluster = msk_cluster(2, &[0, 1]).await;
    cluster.state().sasl.as_mut().unwrap().msk_session_token = Some(AWS_SESSION.into());
    let vars = [
        ("AWS_ACCESS_KEY_ID", AWS_KEY_ID),
        ("AWS_SECRET_ACCESS_KEY", AWS_SECRET),
        ("AWS_SESSION_TOKEN", AWS_SESSION),
    ];
    let mut producer = msk_producer(&cluster, Some("eu-west-1"), &vars, no_endpoints());
    let mut records = keyed_for(0, 2, 2);
    records.extend(keyed_for(1, 2, 2));
    send_expecting(&mut producer, records, &[None]).await;
    assert_eq!((values_in(&cluster, 0).len(), values_in(&cluster, 1).len()), (2, 2));
    let state = cluster.state();
    assert_eq!(state.authentications, 3, "bootstrap and both leaders");
    assert_eq!(state.msk_payloads.len(), 3);
    for payload in &state.msk_payloads {
        assert_eq!(payload["host"], "127.0.0.1");
        assert_eq!(payload["x-amz-security-token"], AWS_SESSION);
        let credential = payload["x-amz-credential"].as_str().unwrap();
        assert!(credential.ends_with("/eu-west-1/kafka-cluster/aws4_request"), "{credential}");
    }
}

/// Without `region:` and an MSK host, the region comes from `AWS_REGION`, then
/// `AWS_DEFAULT_REGION`; with none of them, the exchange is `Refused` before any credential
/// lookup.
#[tokio::test]
async fn aws_msk_iam_reads_the_region_from_the_environment_or_refuses() {
    for (region_var, want) in [("AWS_REGION", "ap-south-1"), ("AWS_DEFAULT_REGION", "sa-east-1")] {
        let cluster = msk_cluster(1, &[0]).await;
        let vars = [
            ("AWS_ACCESS_KEY_ID", AWS_KEY_ID),
            ("AWS_SECRET_ACCESS_KEY", AWS_SECRET),
            (region_var, want),
        ];
        let mut producer = msk_producer(&cluster, None, &vars, no_endpoints());
        send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
        let credential = cluster.state().msk_payloads[0]["x-amz-credential"].clone();
        assert!(credential.as_str().unwrap().contains(&format!("/{want}/")), "{credential}");
    }

    let cluster = msk_cluster(1, &[0]).await;
    let vars = [("AWS_ACCESS_KEY_ID", AWS_KEY_ID), ("AWS_SECRET_ACCESS_KEY", AWS_SECRET)];
    let mut producer = msk_producer(&cluster, None, &vars, no_endpoints());
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{message}");
    assert!(message.contains("no AWS region"), "{message}");
    assert_eq!(requests_of(&cluster, ApiKey::SaslAuthenticate), 0);
}

/// Credentials the broker refuses are `Refused`, with MSK's message and no secret; ones fetched
/// from an endpoint leave the cache, so the next attempt fetches them again and delivers once
/// the endpoint hands out credentials the broker takes.
#[tokio::test]
async fn refused_aws_credentials_are_refused_and_fetched_again() {
    let cluster = msk_cluster(1, &[0]).await;
    let vars = [("AWS_ACCESS_KEY_ID", AWS_KEY_ID), ("AWS_SECRET_ACCESS_KEY", "wrong-secret-value")];
    let mut producer = msk_producer(&cluster, Some("us-east-1"), &vars, no_endpoints());
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{message}");
    assert!(message.contains("SaslAuthenticationFailed (58)"), "{message}");
    assert!(message.contains("Access denied"), "{message}");
    assert!(message.contains("AWS_MSK_IAM as AWS credentials from the environment"), "{message}");
    assert!(!message.contains("wrong-secret-value"), "{message}");

    let stub = HttpStub::start().await;
    stub.answer(|_, n| {
        let secret = if n == 0 { "stale-secret-value" } else { AWS_SECRET };
        (
            200,
            format!(
                r#"{{"AccessKeyId":"{AWS_KEY_ID}","SecretAccessKey":"{secret}",
                    "Token":"{AWS_SESSION}","Expiration":"2099-01-01T00:00:00Z"}}"#
            ),
        )
    });
    let full = format!("{}/v2/credentials", stub.url);
    let vars = [("AWS_CONTAINER_CREDENTIALS_FULL_URI", full.as_str())];
    let mut producer = msk_producer(&cluster, Some("us-east-1"), &vars, no_endpoints());
    let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
    let message = format!("{err:#} {err:?}");
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{message}");
    for secret in ["stale-secret-value", AWS_SESSION] {
        assert!(!message.contains(secret), "{message}");
    }
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    assert_eq!(stub.requests().len(), 2, "the refused credentials were fetched again");
    assert_eq!(values_in(&cluster, 0).len(), 1);
}

/// A credential endpoint's transient fault is `Clean`, its refusal `Refused`, and the broker
/// sees no `SaslAuthenticate` for either.
#[tokio::test]
async fn aws_credential_endpoint_faults_reach_the_send_with_their_class() {
    for (status, want) in [(503, Fault::Clean), (403, Fault::Refused)] {
        let cluster = msk_cluster(1, &[0]).await;
        let stub = HttpStub::start().await;
        stub.answer(move |_, _| (status, String::new()));
        let full = format!("{}/creds", stub.url);
        let vars = [("AWS_CONTAINER_CREDENTIALS_FULL_URI", full.as_str())];
        let mut producer = msk_producer(&cluster, Some("us-east-1"), &vars, no_endpoints());
        let err = producer.prepare(&mut keyed_for(0, 1, 1)).await.unwrap_err();
        let message = format!("{err:#} {err:?}");
        assert_eq!(logit_pipeline::classify(&err), want, "{status}: {message}");
        assert!(message.contains(&format!("answered {status}")), "{message}");
        assert_eq!(requests_of(&cluster, ApiKey::SaslAuthenticate), 0);
    }
}

/// MSK ties a session's lifetime to the credentials' expiry. Credentials are reused until 5
/// minutes before it, so the redial of a session the broker ended signs with fresh ones.
#[tokio::test]
async fn a_redial_after_the_credentials_are_due_signs_with_fresh_ones() {
    let stub = HttpStub::start().await;
    // Reused for 1 to 2 s: they expire 5 minutes and 2 seconds after they're issued, in whole
    // seconds.
    stub.answer(|_, n| {
        let expires = std::time::SystemTime::now() + Duration::from_secs(302);
        let nanos = expires.duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64;
        let expiration = logit_core::time::format_rfc3339_utc(nanos);
        (
            200,
            format!(
                r#"{{"AccessKeyId":"{AWS_KEY_ID}","SecretAccessKey":"{AWS_SECRET}",
                    "Token":"session-{n}","Expiration":"{}Z"}}"#,
                &expiration[..19]
            ),
        )
    });
    let cluster = msk_cluster(1, &[0]).await;
    // The connection's deadline falls at half the lifetime.
    cluster.state().sasl.as_mut().unwrap().session_lifetime_ms = 1000;
    let full = format!("{}/creds", stub.url);
    let vars = [("AWS_CONTAINER_CREDENTIALS_FULL_URI", full.as_str())];
    let mut producer = msk_producer(&cluster, Some("us-east-1"), &vars, no_endpoints());
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;
    let first_sent = tokio::time::Instant::now();
    assert_eq!(stub.requests().len(), 1);

    let ends = cluster.state().session_ends.clone();
    let due = first_sent + Duration::from_secs(2);
    wait_until("the credentials are due and every session has ended", || {
        let now = tokio::time::Instant::now();
        now >= due && ends.iter().all(|&end| now >= end)
    })
    .await;
    send_expecting(&mut producer, keyed_for(0, 1, 1), &[None]).await;

    assert_eq!(stub.requests().len(), 2);
    let state = cluster.state();
    let last = state.msk_payloads.last().unwrap();
    assert_eq!(last["x-amz-security-token"], "session-1");
    assert_eq!(state.expired_closes, 0);
    assert_eq!(state.partition(TOPIC, 0).records().len(), 2);
}
