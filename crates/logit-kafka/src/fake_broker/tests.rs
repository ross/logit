use super::*;

use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{FetchResponse, GroupId};
use logit_pipeline::test_util::{wait_until, RECV_TIMEOUT};
use logit_proto::kafka::batch::{encode, ProducerRecord, Stamp};

use crate::conn::{Conn, Dialer};

const TOPIC: &str = "t";
const GROUP: &str = "g";

fn info(
    producer_epoch: i16,
    base_sequence: i32,
) -> (kafka_protocol::records::BatchDecodeInfo, Bytes) {
    let stamp = Stamp { producer_id: 7, producer_epoch, base_sequence };
    let bytes = encode(&[record("x")], stamp, Compression::None).unwrap();
    (RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap().remove(0), bytes)
}

fn record(value: &str) -> ProducerRecord {
    ProducerRecord {
        key: None,
        value: Some(Bytes::from(value.to_string())),
        headers: Default::default(),
        timestamp: 0,
    }
}

#[test]
fn unknown_producer_is_accepted_at_any_sequence() {
    let mut p = Partition::default();
    let (info, bytes) = info(0, 5);
    assert_eq!(append(&mut p, &info, bytes), Ok(0));
}

#[test]
fn known_producer_at_a_new_epoch_must_start_at_zero() {
    let mut p = Partition::default();
    let (first, bytes) = info(0, 0);
    append(&mut p, &first, bytes).unwrap();
    let (skipped, bytes) = info(1, 5);
    assert_eq!(append(&mut p, &skipped, bytes), Err(code::OUT_OF_ORDER_SEQUENCE_NUMBER));
    let (restarted, bytes) = info(1, 0);
    assert_eq!(append(&mut p, &restarted, bytes), Ok(1));
}

/// A stored batch carries its assigned base offset in bytes 0..8 and its records' offsets.
#[test]
fn a_stored_batch_is_rebased_to_its_offset() {
    let mut state = State::default();
    state.add_topic(TOPIC, &[0]);
    state.append(TOPIC, 0, &[record("a"), record("b")], Compression::None);
    state.append(TOPIC, 0, &[record("c")], Compression::Gzip);
    let p = state.partition(TOPIC, 0);
    let second = &p.log[1];
    assert_eq!((second.base_offset, second.last_offset), (2, 2));
    assert_eq!(second.raw[..8], 2i64.to_be_bytes());
    let offsets: Vec<i64> = p.records().iter().map(|r| r.offset).collect();
    assert_eq!(offsets, [0, 1, 2]);
    assert_eq!(p.high_watermark(), 3);
}

async fn dial(cluster: &FakeCluster, node: usize, client_id: &'static str) -> Conn {
    let dialer = Dialer {
        tls: None,
        timeout: RECV_TIMEOUT,
        client_id: StrBytes::from_static_str(client_id),
        sasl: None,
        required: &[],
    };
    dialer.connect("127.0.0.1", cluster.port(node)).await.unwrap()
}

fn s(text: &str) -> StrBytes {
    StrBytes::from_string(text.to_string())
}

fn join_request(group: &str, member: &str, protocols: &[(&str, &[u8])]) -> JoinGroupRequest {
    JoinGroupRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(10_000)
        .with_member_id(s(member))
        .with_protocol_type(s("consumer"))
        .with_protocols(
            protocols
                .iter()
                .map(|(name, metadata)| {
                    JoinGroupRequestProtocol::default()
                        .with_name(s(name))
                        .with_metadata(Bytes::copy_from_slice(metadata))
                })
                .collect(),
        )
}

fn sync_request(
    group: &str,
    generation: i32,
    member: &str,
    assignments: &[(&str, &[u8])],
) -> SyncGroupRequest {
    SyncGroupRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_generation_id(generation)
        .with_member_id(s(member))
        .with_assignments(
            assignments
                .iter()
                .map(|(id, a)| {
                    SyncGroupRequestAssignment::default()
                        .with_member_id(s(id))
                        .with_assignment(Bytes::copy_from_slice(a))
                })
                .collect(),
        )
}

fn heartbeat_request(group: &str, generation: i32, member: &str) -> HeartbeatRequest {
    HeartbeatRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_generation_id(generation)
        .with_member_id(s(member))
}

fn commit_request(group: &str, generation: i32, member: &str, offset: i64) -> OffsetCommitRequest {
    OffsetCommitRequest::default()
        .with_group_id(GroupId(s(group)))
        .with_generation_id_or_member_epoch(generation)
        .with_member_id(s(member))
        .with_topics(vec![OffsetCommitRequestTopic::default()
            .with_name(TopicName(s(TOPIC)))
            .with_partitions(vec![OffsetCommitRequestPartition::default()
                .with_partition_index(0)
                .with_committed_offset(offset)])])
}

fn commit_code(response: &OffsetCommitResponse) -> i16 {
    response.topics[0].partitions[0].error_code
}

fn fetch_request(offset: i64, isolation: i8) -> FetchRequest {
    FetchRequest::default()
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1 << 20)
        .with_isolation_level(isolation)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![FetchTopic::default().with_topic(TopicName(s(TOPIC))).with_partitions(
            vec![FetchPartition::default()
                .with_partition(0)
                .with_fetch_offset(offset)
                .with_partition_max_bytes(1 << 20)],
        )])
}

fn fetched(response: &FetchResponse) -> &kafka_protocol::messages::fetch_response::PartitionData {
    &response.responses[0].partitions[0]
}

/// The base offset of each batch in a fetched records section.
fn base_offsets(mut records: &[u8]) -> Vec<i64> {
    let mut out = Vec::new();
    while records.len() >= 12 {
        out.push(i64::from_be_bytes(records[..8].try_into().unwrap()));
        let len = i32::from_be_bytes(records[8..12].try_into().unwrap()) as usize;
        records = &records[12 + len..];
    }
    out
}

/// Joins `group` alone at v5 through the `MEMBER_ID_REQUIRED` round trip and syncs; returns the
/// member id, at generation 1.
async fn join_alone(conn: &mut Conn, group: &str) -> String {
    let first = conn.request(&join_request(group, "", &[("range", b"a")]), 5).await.unwrap();
    assert_eq!(first.error_code, code::MEMBER_ID_REQUIRED);
    let id = first.member_id.to_string();
    let joined = conn.request(&join_request(group, &id, &[("range", b"a")]), 5).await.unwrap();
    assert_eq!((joined.error_code, joined.generation_id), (code::NONE, 1));
    let synced =
        conn.request(&sync_request(group, 1, &id, &[(id.as_str(), b"mine")]), 3).await.unwrap();
    assert_eq!(synced.error_code, code::NONE);
    id
}

#[tokio::test]
async fn a_first_join_from_v4_needs_a_member_id_and_below_v4_joins_at_once() {
    let cluster = FakeCluster::start(1).await;
    let mut conn = dial(&cluster, 0, "client").await;

    let first = conn.request(&join_request(GROUP, "", &[("range", b"m")]), 4).await.unwrap();
    assert_eq!(first.error_code, code::MEMBER_ID_REQUIRED);
    assert_eq!(first.member_id.as_str(), "client-0");
    assert!(cluster.state().groups[GROUP].pending.contains_key("client-0"));
    let joined =
        conn.request(&join_request(GROUP, "client-0", &[("range", b"m")]), 4).await.unwrap();
    assert_eq!(joined.error_code, code::NONE);
    assert_eq!(joined.generation_id, 1);
    assert_eq!(joined.leader.as_str(), "client-0");
    assert_eq!(joined.protocol_name.as_ref().map(StrBytes::as_str), Some("range"));

    let old = conn.request(&join_request("other", "", &[("range", b"m")]), 3).await.unwrap();
    assert_eq!(old.error_code, code::NONE);
    assert_eq!(old.generation_id, 1);
    assert_eq!(old.member_id.as_str(), "client-1");
    let state = cluster.state();
    assert!(state.groups["other"].pending.is_empty());
    let answered: Vec<Option<i16>> = state.joins.iter().map(|j| j.answered).collect();
    assert_eq!(answered, [Some(code::MEMBER_ID_REQUIRED), Some(0), Some(0)]);
}

#[tokio::test]
async fn a_join_is_held_until_every_member_has_joined_and_the_leader_gets_every_subscription() {
    let cluster = FakeCluster::start(1).await;
    let mut a = dial(&cluster, 0, "a").await;
    let mut b = dial(&cluster, 0, "b").await;
    let a_id = a.request(&join_request(GROUP, "", &[("range", b"A")]), 9).await.unwrap().member_id;
    let b_id = b.request(&join_request(GROUP, "", &[("range", b"B")]), 9).await.unwrap().member_id;

    let request = join_request(GROUP, a_id.as_str(), &[("range", b"A")]);
    let a_join = tokio::spawn(async move { a.request(&request, 9).await.map(|r| (r, a)) });
    wait_until("a's join to arrive", || cluster.state().joins.len() == 3).await;
    assert!(!a_join.is_finished(), "a's join is held while b is pending");
    assert_eq!(cluster.state().groups[GROUP].state, GroupState::PreparingRebalance);

    let request = join_request(GROUP, b_id.as_str(), &[("roundrobin", b"x"), ("range", b"B")]);
    let b_joined = b.request(&request, 9).await.unwrap();
    let (a_joined, _a) = a_join.await.unwrap().unwrap();
    assert_eq!((a_joined.generation_id, b_joined.generation_id), (1, 1));
    assert_eq!(a_joined.leader, a_id);
    assert_eq!(b_joined.leader, a_id);
    assert_eq!(a_joined.protocol_type.as_ref().map(StrBytes::as_str), Some("consumer"));
    assert_eq!(b_joined.protocol_name.as_ref().map(StrBytes::as_str), Some("range"));
    let subscriptions: Vec<(String, Bytes)> =
        a_joined.members.iter().map(|m| (m.member_id.to_string(), m.metadata.clone())).collect();
    assert_eq!(
        subscriptions,
        [
            (a_id.to_string(), Bytes::from_static(b"A")),
            (b_id.to_string(), Bytes::from_static(b"B"))
        ]
    );
    assert!(b_joined.members.is_empty());
    let state = cluster.state();
    assert_eq!(state.groups[GROUP].generation, 1);
    assert_eq!(state.groups[GROUP].state, GroupState::CompletingRebalance);
}

/// A follower's SyncGroup waits for the leader's, then gets what the leader assigned it.
#[tokio::test]
async fn a_follower_sync_is_held_until_the_leader_assigns() {
    let cluster = FakeCluster::start(1).await;
    let mut a = dial(&cluster, 0, "a").await;
    let mut b = dial(&cluster, 0, "b").await;
    let a_id = a.request(&join_request(GROUP, "", &[("range", b"A")]), 3).await.unwrap().member_id;
    let request = join_request(GROUP, "", &[("range", b"B")]);
    let b_join = tokio::spawn(async move { b.request(&request, 3).await.map(|r| (r, b)) });
    wait_until("b's join", || cluster.state().joins.len() == 2).await;
    let a_rejoined =
        a.request(&join_request(GROUP, a_id.as_str(), &[("range", b"A")]), 3).await.unwrap();
    let (b_joined, mut b) = b_join.await.unwrap().unwrap();
    assert_eq!(a_rejoined.generation_id, 2);
    let b_id = b_joined.member_id.to_string();

    let request = sync_request(GROUP, 2, &b_id, &[]);
    let b_sync = tokio::spawn(async move { b.request(&request, 5).await });
    wait_until("b's sync to be held", || {
        cluster.state().requests.iter().filter(|r| r.1 == ApiKey::SyncGroup).count() == 1
    })
    .await;
    assert!(!b_sync.is_finished());
    let assignments = [(a_id.as_str(), &b"to-a"[..]), (b_id.as_str(), &b"to-b"[..])];
    let a_synced = a.request(&sync_request(GROUP, 2, a_id.as_str(), &assignments), 5).await;
    assert_eq!(a_synced.unwrap().assignment, Bytes::from_static(b"to-a"));
    let b_synced = b_sync.await.unwrap().unwrap();
    assert_eq!(b_synced.assignment, Bytes::from_static(b"to-b"));
    assert_eq!(b_synced.protocol_name.as_ref().map(StrBytes::as_str), Some("range"));
    assert_eq!(cluster.state().groups[GROUP].state, GroupState::Stable);
}

/// A new member's join puts the group in a rebalance: the old member's heartbeat is answered
/// `REBALANCE_IN_PROGRESS`, and a commit at its current generation is still accepted, then
/// refused `ILLEGAL_GENERATION` once the generation moves.
#[tokio::test]
async fn a_rebalance_answers_heartbeats_27_and_takes_current_commits_until_the_generation_moves() {
    let cluster = FakeCluster::start(1).await;
    cluster.state().add_topic(TOPIC, &[0]);
    let mut a = dial(&cluster, 0, "a").await;
    let mut b = dial(&cluster, 0, "b").await;
    let a_id = join_alone(&mut a, GROUP).await;
    let beat = a.request(&heartbeat_request(GROUP, 1, &a_id), 4).await.unwrap();
    assert_eq!(beat.error_code, code::NONE);

    let request = join_request(GROUP, "", &[("range", b"B")]);
    let b_join = tokio::spawn(async move { b.request(&request, 3).await });
    wait_until("the rebalance", || {
        cluster.state().groups[GROUP].state == GroupState::PreparingRebalance
    })
    .await;
    let beat = a.request(&heartbeat_request(GROUP, 1, &a_id), 4).await.unwrap();
    assert_eq!(beat.error_code, code::REBALANCE_IN_PROGRESS);
    let during = a.request(&commit_request(GROUP, 1, &a_id, 10), 8).await.unwrap();
    assert_eq!(commit_code(&during), code::NONE);

    let rejoined = a.request(&join_request(GROUP, &a_id, &[("range", b"A")]), 5).await.unwrap();
    assert_eq!(rejoined.generation_id, 2);
    assert_eq!(b_join.await.unwrap().unwrap().generation_id, 2);
    let stale = a.request(&commit_request(GROUP, 1, &a_id, 20), 8).await.unwrap();
    assert_eq!(commit_code(&stale), code::ILLEGAL_GENERATION);

    let state = cluster.state();
    assert_eq!(state.groups[GROUP].committed[&(TOPIC.to_string(), 0)], 10);
    let answered: Vec<(i32, i64, i16)> =
        state.commits.iter().map(|c| (c.generation, c.offsets[0].2, c.answered[0])).collect();
    assert_eq!(answered, [(1, 10, code::NONE), (1, 20, code::ILLEGAL_GENERATION)]);
}

/// A member silent past its session is removed on the paused clock and the group rebalances;
/// the member that kept heartbeating rejoins alone.
#[tokio::test(start_paused = true)]
async fn a_silent_member_expires_and_the_group_rebalances() {
    let cluster = FakeCluster::start(1).await;
    cluster.state().group_session_bounds = (Duration::from_millis(1), Duration::from_secs(3600));
    let mut a = dial(&cluster, 0, "a").await;
    let mut b = dial(&cluster, 0, "b").await;
    let a_id = join_alone(&mut a, GROUP).await;
    let short = join_request(GROUP, "", &[("range", b"B")]).with_session_timeout_ms(2_000);
    let b_join = tokio::spawn(async move { b.request(&short, 3).await.map(|r| (r, b)) });
    wait_until("the rebalance", || {
        cluster.state().groups[GROUP].state == GroupState::PreparingRebalance
    })
    .await;
    let long = join_request(GROUP, &a_id, &[("range", b"A")]).with_session_timeout_ms(600_000);
    let rejoined = a.request(&long, 3).await.unwrap();
    assert_eq!(rejoined.generation_id, 2);
    let (b_joined, _b) = b_join.await.unwrap().unwrap();
    let b_id = b_joined.member_id.to_string();
    let assignments = [(a_id.as_str(), &b"x"[..]), (b_id.as_str(), &b"y"[..])];
    a.request(&sync_request(GROUP, 2, &a_id, &assignments), 3).await.unwrap();

    wait_until("b's session to expire", || {
        let state = cluster.state();
        let group = &state.groups[GROUP];
        !group.members.contains_key(&b_id) && group.state == GroupState::PreparingRebalance
    })
    .await;
    let beat = a.request(&heartbeat_request(GROUP, 2, &a_id), 4).await.unwrap();
    assert_eq!(beat.error_code, code::REBALANCE_IN_PROGRESS);
    let alone = a.request(&long, 3).await.unwrap();
    assert_eq!(alone.generation_id, 3);
    assert_eq!(alone.members.len(), 1);
}

#[tokio::test]
async fn a_fetch_outside_the_log_is_out_of_range_and_at_the_end_is_empty() {
    let cluster = FakeCluster::start(1).await;
    {
        let mut state = cluster.state();
        state.add_topic(TOPIC, &[0]);
        state.append(TOPIC, 0, &[record("a"), record("b")], Compression::None);
        state.append(TOPIC, 0, &[record("c"), record("d")], Compression::None);
        state.truncate(TOPIC, 0, 2);
    }
    let mut conn = dial(&cluster, 0, "c").await;
    for version in [4, 12] {
        let below = conn.request(&fetch_request(1, 0), version).await.unwrap();
        assert_eq!(fetched(&below).error_code, code::OFFSET_OUT_OF_RANGE, "v{version}");
        let above = conn.request(&fetch_request(5, 0), version).await.unwrap();
        assert_eq!(fetched(&above).error_code, code::OFFSET_OUT_OF_RANGE, "v{version}");
        let end = conn.request(&fetch_request(4, 0), version).await.unwrap();
        assert_eq!(fetched(&end).error_code, code::NONE);
        assert_eq!(fetched(&end).records.as_ref().map(Bytes::len), Some(0));
        let start = conn.request(&fetch_request(2, 0), version).await.unwrap();
        let data = fetched(&start);
        assert_eq!(base_offsets(data.records.as_ref().unwrap()), [2], "v{version}");
        assert_eq!(data.high_watermark, 4);
        if version >= 5 {
            assert_eq!(data.log_start_offset, 2);
        }
    }
}

#[tokio::test]
async fn read_committed_stops_at_the_lso_and_lists_the_aborted_transaction() {
    let cluster = FakeCluster::start(1).await;
    {
        let mut state = cluster.state();
        state.add_topic(TOPIC, &[0]);
        state.append(TOPIC, 0, &[record("plain")], Compression::None);
        assert_eq!(state.append_transactional(TOPIC, 0, 7, 0, &[record("x"), record("y")]), 1);
        assert_eq!(state.end_transaction(TOPIC, 0, 7, 0, false), 3);
        assert_eq!(state.append_transactional(TOPIC, 0, 8, 0, &[record("open")]), 4);
        state.append(TOPIC, 0, &[record("after")], Compression::None);
        let p = state.partition(TOPIC, 0);
        assert_eq!((p.last_stable_offset(), p.high_watermark()), (4, 6));
        assert_eq!(p.aborted, [(7, 1, 3)]);
        assert!(p.log[2].control && p.log[2].transactional);
    }
    let mut conn = dial(&cluster, 0, "c").await;
    let committed = conn.request(&fetch_request(0, 1), 12).await.unwrap();
    let data = fetched(&committed);
    assert_eq!(base_offsets(data.records.as_ref().unwrap()), [0, 1, 3]);
    assert_eq!((data.last_stable_offset, data.high_watermark), (4, 6));
    let aborted: Vec<(i64, i64)> = data
        .aborted_transactions
        .iter()
        .flatten()
        .map(|a| (a.producer_id.0, a.first_offset))
        .collect();
    assert_eq!(aborted, [(7, 1)]);

    let uncommitted = conn.request(&fetch_request(0, 0), 12).await.unwrap();
    let data = fetched(&uncommitted);
    assert_eq!(base_offsets(data.records.as_ref().unwrap()), [0, 1, 3, 4, 5]);
    assert_eq!(data.aborted_transactions, None);

    let request = ListOffsetsRequest::default().with_isolation_level(1).with_topics(vec![
        ListOffsetsTopic::default().with_name(TopicName(s(TOPIC))).with_partitions(vec![
            ListOffsetsPartition::default().with_partition_index(0).with_timestamp(-1),
        ]),
    ]);
    let latest = conn.request(&request, 2).await.unwrap();
    assert_eq!(latest.topics[0].partitions[0].offset, 4);
}

#[tokio::test]
async fn a_fetch_with_no_data_is_held_until_an_append() {
    let cluster = FakeCluster::start(1).await;
    cluster.state().add_topic(TOPIC, &[0]);
    let mut conn = dial(&cluster, 0, "c").await;
    let request = fetch_request(0, 0).with_max_wait_ms(60_000);
    let fetch = tokio::spawn(async move { conn.request(&request, 12).await });
    wait_until("the fetch to arrive", || cluster.state().fetches.len() == 1).await;
    assert!(!fetch.is_finished(), "an empty fetch waits for data");
    cluster.state().append(TOPIC, 0, &[record("late")], Compression::None);
    let response = fetch.await.unwrap().unwrap();
    assert_eq!(base_offsets(fetched(&response).records.as_ref().unwrap()), [0]);
}

#[tokio::test]
async fn kafka_2_4s_ranges_are_advertised_as_given() {
    let cluster = FakeCluster::start(1).await;
    cluster.state().api_versions = Some(KAFKA_2_4.to_vec());
    let mut conn = dial(&cluster, 0, "c").await;
    let response = conn.request(&ApiVersionsRequest::default(), 3).await.unwrap();
    let rows: Vec<(i16, i16, i16)> =
        response.api_keys.iter().map(|k| (k.api_key, k.min_version, k.max_version)).collect();
    let expected: Vec<(i16, i16, i16)> =
        KAFKA_2_4.iter().map(|&(key, min, max)| (key as i16, min, max)).collect();
    assert_eq!(rows, expected);
}

/// `CorruptCrc` flips a CRC bit in the first batch served for its partition, once.
#[tokio::test]
async fn a_scripted_crc_corruption_hits_one_served_batch() {
    let cluster = FakeCluster::start(1).await;
    cluster.state().add_topic(TOPIC, &[0]);
    cluster.script(Step::api(ApiKey::Fetch, Action::CorruptCrc).on_partition(0));
    let mut conn = dial(&cluster, 0, "c").await;
    let empty = conn.request(&fetch_request(0, 0), 12).await.unwrap();
    assert_eq!(fetched(&empty).records.as_ref().map(Bytes::len), Some(0));
    assert_eq!(cluster.state().script.len(), 1, "no batch served, so the step stays");
    cluster.state().append(TOPIC, 0, &[record("a")], Compression::None);
    let stored = cluster.state().partition(TOPIC, 0).log[0].raw.clone();
    let corrupt = conn.request(&fetch_request(0, 0), 12).await.unwrap();
    let served = fetched(&corrupt).records.clone().unwrap();
    assert_eq!(served[17], stored[17] ^ 1);
    assert_eq!(served[..17], stored[..17]);
    assert_eq!(served[18..], stored[18..]);
    let clean = conn.request(&fetch_request(0, 0), 12).await.unwrap();
    assert_eq!(fetched(&clean).records.clone().unwrap(), stored);
}

/// One member's life through every API at each version a client may pick, so each response
/// encodes at each version with its version-dependent shape.
#[tokio::test]
async fn a_member_runs_every_group_and_read_api_at_every_version() {
    let cluster = FakeCluster::start(1).await;
    {
        let mut state = cluster.state();
        state.add_topic(TOPIC, &[0]);
        state.append(TOPIC, 0, &[record("a")], Compression::None);
    }
    let mut conn = dial(&cluster, 0, "c").await;
    for v in 0..=12i16 {
        let group = format!("g{v}");
        let find = v.min(6);
        let request = FindCoordinatorRequest::default()
            .with_key(if find < 4 { s(&group) } else { StrBytes::default() })
            .with_coordinator_keys(if find >= 4 { vec![s(&group)] } else { vec![] });
        let found = conn.request(&request, find).await.unwrap();
        let port = if find >= 4 { found.coordinators[0].port } else { found.port };
        assert_eq!(port, i32::from(cluster.port(0)), "FindCoordinator v{find}");

        let join = v.clamp(2, 9);
        let mut joined = conn.request(&join_request(&group, "", &[("range", b"m")]), join).await;
        if join >= 4 {
            let id = joined.unwrap().member_id;
            joined =
                conn.request(&join_request(&group, id.as_str(), &[("range", b"m")]), join).await;
        }
        let joined = joined.unwrap();
        assert_eq!(joined.error_code, code::NONE, "JoinGroup v{join}");
        let id = joined.member_id.to_string();
        let sync = v.min(5);
        let request = sync_request(&group, 1, &id, &[(id.as_str(), b"a")]);
        let synced = conn.request(&request, sync).await;
        assert_eq!(synced.unwrap().assignment, Bytes::from_static(b"a"), "v{sync}");
        let beat = conn.request(&heartbeat_request(&group, 1, &id), v.min(4)).await.unwrap();
        assert_eq!(beat.error_code, code::NONE);

        let commit = v.clamp(2, 9);
        let committed = conn.request(&commit_request(&group, 1, &id, 1), commit).await.unwrap();
        assert_eq!(commit_code(&committed), code::NONE, "OffsetCommit v{commit}");

        let fetch_offsets = v.clamp(1, 9);
        let offset = if fetch_offsets >= 8 {
            let request =
                OffsetFetchRequest::default().with_groups(vec![OffsetFetchRequestGroup::default()
                    .with_group_id(GroupId(s(&group)))
                    .with_topics(Some(vec![OffsetFetchRequestTopics::default()
                        .with_name(TopicName(s(TOPIC)))
                        .with_partition_indexes(vec![0, 1])]))]);
            let response = conn.request(&request, fetch_offsets).await.unwrap();
            let partitions = &response.groups[0].topics[0].partitions;
            assert_eq!(partitions[1].committed_offset, -1);
            partitions[0].committed_offset
        } else {
            let request = OffsetFetchRequest::default()
                .with_group_id(GroupId(s(&group)))
                .with_topics(Some(vec![OffsetFetchRequestTopic::default()
                    .with_name(TopicName(s(TOPIC)))
                    .with_partition_indexes(vec![0, 1])]));
            let response = conn.request(&request, fetch_offsets).await.unwrap();
            let partitions = &response.topics[0].partitions;
            assert_eq!(partitions[1].committed_offset, -1);
            partitions[0].committed_offset
        };
        assert_eq!(offset, 1, "OffsetFetch v{fetch_offsets}");

        let list = v.clamp(1, 10);
        let request = ListOffsetsRequest::default().with_topics(vec![ListOffsetsTopic::default()
            .with_name(TopicName(s(TOPIC)))
            .with_partitions(vec![ListOffsetsPartition::default()
                .with_partition_index(0)
                .with_timestamp(-1)])]);
        let listed = conn.request(&request, list).await.unwrap();
        assert_eq!(listed.topics[0].partitions[0].offset, 1, "ListOffsets v{list}");

        let fetch = v.clamp(4, 12);
        let response = conn.request(&fetch_request(0, 0), fetch).await.unwrap();
        assert_eq!(base_offsets(fetched(&response).records.as_ref().unwrap()), [0], "v{fetch}");

        let leave = v.min(5);
        let request = if leave >= 3 {
            LeaveGroupRequest::default()
                .with_group_id(GroupId(s(&group)))
                .with_members(vec![MemberIdentity::default().with_member_id(s(&id))])
        } else {
            LeaveGroupRequest::default().with_group_id(GroupId(s(&group))).with_member_id(s(&id))
        };
        let left = conn.request(&request, leave).await.unwrap();
        let code = if leave >= 3 { left.members[0].error_code } else { left.error_code };
        assert_eq!(code, code::NONE, "LeaveGroup v{leave}");
        assert_eq!(cluster.state().groups[&group].state, GroupState::Empty);
    }
}

/// A group request to a node that isn't the coordinator is `NOT_COORDINATOR`, and a scripted
/// error answers without touching the group.
#[tokio::test]
async fn group_requests_off_the_coordinator_and_scripted_errors_leave_the_group_alone() {
    let cluster = FakeCluster::start(2).await;
    cluster.state().coordinator = 1;
    let mut off = dial(&cluster, 0, "c").await;
    let refused = off.request(&join_request(GROUP, "", &[("range", b"m")]), 5).await.unwrap();
    assert_eq!(refused.error_code, code::NOT_COORDINATOR);
    let mut on = dial(&cluster, 1, "c").await;
    cluster.script(Step::api(ApiKey::JoinGroup, Action::Error(code::COORDINATOR_NOT_AVAILABLE)));
    let scripted = on.request(&join_request(GROUP, "", &[("range", b"m")]), 5).await.unwrap();
    assert_eq!(scripted.error_code, code::COORDINATOR_NOT_AVAILABLE);
    assert!(!cluster.state().groups.contains_key(GROUP));
    cluster.script(Step::api(ApiKey::Heartbeat, Action::Throttle(250)));
    let throttled = on.request(&heartbeat_request(GROUP, 1, "x"), 4).await.unwrap();
    assert_eq!((throttled.error_code, throttled.throttle_time_ms), (code::UNKNOWN_MEMBER_ID, 250));
}
