//! The producer against the in-process fake broker (`super::fake_broker`): partitioning and
//! leader routing, idempotent sequences across retries and epoch bumps, and each row of the
//! module doc's response-class table.

use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::records::Compression;
use logit_pipeline::test_util::{wait_until, RECV_TIMEOUT};
use logit_pipeline::Fault;

use super::batch::ProducerRecord;
use super::fake_broker::{Action, FakeCluster, Received, Step};
use super::fault::code;
use super::partitioner::partition_for_key;
use super::producer::{
    Acks, Attempt, Identity, PartState, Pending, Producer, ProducerConfig, Settlement,
};

const TOPIC: &str = "logs";
/// The first PID the fake broker hands out.
const FIRST_PID: i64 = 1000;

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

async fn prepare(producer: &mut Producer, records: Vec<ProducerRecord>) -> Pending {
    producer.prepare(records).await.expect("prepare")
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
    })
    .unwrap();
    let err = producer.prepare(vec![record(None, "v")]).await.unwrap_err();
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
    let err = producer.prepare(keyed_for(0, 1, 1)).await.unwrap_err();
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
    let err = producer.prepare(vec![record(None, "v")]).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{err:#}");
    let polled = requests_of(&cluster, ApiKey::Metadata);
    assert!(polled >= 2, "the refresh ran: {polled} Metadata requests");
    let err = producer.prepare(vec![record(None, "v")]).await.unwrap_err();
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
            action: Action::Error(answer),
        });
    }
    let mut producer = Producer::new(config(&cluster)).unwrap();
    send_expecting(&mut producer, vec![record(None, "v")], &[None]).await;
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
    let err = producer.prepare(keyed_for(0, 1, 1)).await.unwrap_err();
    assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
    assert!(format!("{err:#}").contains("TLS handshake"), "{err:#}");
}
