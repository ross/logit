//! The consumer against the fake broker: one test per row of the outcome table in this module's
//! parent doc, and the group, commit, and version behavior `docs/adr/kafka-input.md` items 3 to 8
//! settle.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::records::Compression;
use logit_core::Diagnostics;
use logit_pipeline::test_util::{wait_until, TelemetryProbe, RECV_TIMEOUT};
use logit_proto::kafka::batch::ProducerRecord;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};

use super::*;
use crate::error::code;
use crate::fake_broker::{Action, FakeCluster, GroupState, Step, KAFKA_2_4};

const GROUP: &str = "g";

/// A scripted 25 or 82 leaves the old member in the fake's group, so a rejoin under a new id
/// waits the rebalance timeout for the old one to rejoin before the coordinator removes it.
const STALE_MEMBER_WAIT: Duration = Duration::from_millis(200);

fn values(values: &[&str]) -> Vec<ProducerRecord> {
    values
        .iter()
        .map(|v| ProducerRecord {
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: Default::default(),
            timestamp: 1_000,
        })
        .collect()
}

/// Settings scaled for a test: short waits, heartbeats, and commits, from the log start.
fn settings(cluster: &FakeCluster, topics: &[&str]) -> ConsumerSettings {
    let mut s = ConsumerSettings::new(
        vec![cluster.addr(0)],
        GROUP,
        topics.iter().map(|t| t.to_string()).collect(),
    );
    s.start = StartPosition::Earliest;
    s.session_timeout = Duration::from_secs(6);
    s.heartbeat_interval = Duration::from_millis(50);
    s.rebalance_timeout = Duration::from_secs(3);
    s.commit_interval = Duration::from_millis(50);
    s.timeout = Duration::from_secs(2);
    s.fetch.max_wait = Duration::from_millis(50);
    s
}

fn consumer(probe: &TelemetryProbe, id: &str, settings: ConsumerSettings) -> Consumer {
    let telemetry = probe.telemetry(id, "kafka_in", "listener");
    let diag = Diagnostics::new(id).with_telemetry(telemetry.clone());
    Consumer::new(settings, telemetry, diag).unwrap()
}

/// A cluster of `nodes` with topic `t` of one partition per leader in `leaders`.
async fn cluster(nodes: usize, leaders: &[i32]) -> FakeCluster {
    let cluster = FakeCluster::start(nodes).await;
    cluster.state().add_topic("t", leaders);
    cluster
}

async fn next(c: &mut Consumer) -> Next {
    timeout(RECV_TIMEOUT, c.next()).await.expect("the consumer answers within RECV_TIMEOUT")
}

/// Takes records, accepting each run, until `n` values have arrived; returns them in order of
/// arrival.
async fn take(c: &mut Consumer, n: usize) -> Vec<String> {
    let mut out = Vec::new();
    while out.len() < n {
        match next(c).await {
            Next::Records(f) => {
                c.mark_accepted(&f.tp, f.epoch, f.cut(0..f.records.len()));
                out.extend(f.records.iter().map(text));
            }
            Next::Revoke(reason) => panic!("an unexpected revoke: {reason:?}"),
        }
    }
    out
}

fn text(r: &logit_proto::kafka::Record) -> String {
    String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into_owned()
}

fn committed(cluster: &FakeCluster, partition: i32) -> Option<i64> {
    cluster.state().groups.get(GROUP)?.committed.get(&("t".to_string(), partition)).copied()
}

fn requests(cluster: &FakeCluster, api: ApiKey) -> usize {
    cluster.state().requests.iter().filter(|(_, a, _)| *a == api).count()
}

fn errors(probe: &mut TelemetryProbe, api: &str, error: &str) -> f64 {
    probe.sum("logit.input.broker.errors", &[("api", api), ("error", error)])
}

fn diagnostics(probe: &mut TelemetryProbe, key: &str) -> f64 {
    probe.sum("logit.component.diagnostics", &[("key", key)])
}

/// A consumer run on its own task, accepting what it takes, reporting each step.
struct Driven {
    seen: mpsc::UnboundedReceiver<Seen>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Consumer>,
}

#[derive(Debug)]
enum Seen {
    Records,
    Revoke(RevokeReason),
}

fn drive(mut c: Consumer) -> Driven {
    let (tx, seen) = mpsc::unbounded_channel();
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => {
                    c.leave().await;
                    return c;
                }
                n = c.next() => match n {
                    Next::Records(f) => {
                        c.mark_accepted(&f.tp, f.epoch, f.cut(0..f.records.len()));
                        let _ = tx.send(Seen::Records);
                    }
                    Next::Revoke(reason) => {
                        let _ = tx.send(Seen::Revoke(reason));
                    }
                },
            }
        }
    });
    Driven { seen, stop: Some(stop), task }
}

impl Driven {
    async fn leave(mut self) -> Consumer {
        self.stop.take().unwrap().send(()).unwrap();
        timeout(RECV_TIMEOUT, self.task).await.unwrap().unwrap()
    }

    fn drain(&mut self) -> Vec<Seen> {
        let mut out = Vec::new();
        while let Ok(s) = self.seen.try_recv() {
            out.push(s);
        }
        out
    }
}

/// The partitions each member of the group holds now, from the assignments the leader sent.
fn owners(cluster: &FakeCluster) -> BTreeMap<String, Vec<(String, i32)>> {
    let state = cluster.state();
    let group = &state.groups[GROUP];
    group
        .members
        .iter()
        .map(|(id, m)| {
            let parts =
                m.assignment.clone().map(|a| decode_assignment(a).unwrap()).unwrap_or_default();
            (id.clone(), parts)
        })
        .collect()
}

fn stable_with(cluster: &FakeCluster, members: usize) -> bool {
    cluster.state().groups.get(GROUP).is_some_and(|g| {
        g.state == GroupState::Stable
            && g.members.len() == members
            && g.members.values().all(|m| m.assignment.is_some())
    })
}

/// Every partition of `t` has one owner, and no partition two.
fn assert_one_owner_each(cluster: &FakeCluster, partitions: i32) {
    let mut seen = BTreeSet::new();
    for parts in owners(cluster).values() {
        for p in parts {
            assert!(seen.insert(p.clone()), "{p:?} has two owners");
        }
    }
    let all: BTreeSet<_> = (0..partitions).map(|p| ("t".to_string(), p)).collect();
    assert_eq!(seen, all);
}

// --- Joining, versions, and the group ---

#[tokio::test]
async fn a_first_join_follows_member_id_required_and_reads_from_the_log_start() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a", "b"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 2).await, ["a", "b"]);

    let joins = cluster.state().joins.clone();
    assert_eq!(joins[0].member_id, "");
    assert_eq!(joins[0].answered, Some(code::MEMBER_ID_REQUIRED));
    assert!(!joins[1].member_id.is_empty());
    assert_eq!(joins[1].answered, Some(code::NONE));
    assert_eq!(errors(&mut probe, "join_group", "member_id_required"), 0.0);
    assert_eq!(probe.sum("logit.input.rebalances", &[("reason", "start")]), 1.0);
    assert_eq!(probe.sum("logit.input.offsets.reset", &[("reason", "no_commit")]), 1.0);
    assert_eq!(probe.gauge("logit.input.partitions", &[]), Some(1.0));
    assert_eq!(probe.sum("logit.input.records", &[]), 2.0);
    assert_eq!(probe.sum("logit.input.record.bytes", &[]), 2.0);
    let list = cluster.state().list_offsets.clone();
    assert_eq!(list[0].4, offsets::EARLIEST);
    assert!(cluster.state().metadata_auto_create.iter().all(|auto| !auto));
}

#[tokio::test]
async fn a_partition_with_no_commit_starts_at_the_log_end_by_default() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["old"]), Compression::None);
    let probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.start = StartPosition::Latest;
    let mut c = consumer(&probe, "in", s);
    let reading = tokio::spawn(async move { take(&mut c, 1).await });
    wait_until("a fetch at the log end", || {
        cluster.state().fetches.iter().any(|f| f.partitions.iter().any(|p| p.2 == 1))
    })
    .await;
    assert_eq!(cluster.state().list_offsets[0].4, offsets::LATEST);
    cluster.state().append("t", 0, &values(&["new"]), Compression::None);
    assert_eq!(reading.await.unwrap(), ["new"]);
}

/// A partition whose `latest` ListOffsets hasn't answered has no offset to commit: committing
/// its tracker's placeholder would move a new group's start.
#[tokio::test]
async fn an_unresolved_start_position_is_never_committed() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a", "b", "c", "d"]), Compression::None);
    for _ in 0..5 {
        cluster.script(Step::api(ApiKey::ListOffsets, Action::Error(code::LEADER_NOT_AVAILABLE)));
    }
    let probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.start = StartPosition::Latest;
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    c.rejoin().await;
    assert_eq!(c.commit_all().await, CommitOutcome::Nothing);
    assert_eq!(committed(&cluster, 0), None);
}

fn used_versions(cluster: &FakeCluster) -> std::collections::HashMap<ApiKey, i16> {
    let mut out = std::collections::HashMap::new();
    for &(_, api, version) in &cluster.state().requests {
        out.insert(api, version);
    }
    out
}

#[tokio::test]
async fn versions_negotiate_to_kafka_4_0s_ranges() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    take(&mut c, 1).await;
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    c.leave().await;
    let used = used_versions(&cluster);
    assert_eq!(used[&ApiKey::Fetch], 12);
    assert_eq!(used[&ApiKey::ListOffsets], 10);
    assert_eq!(used[&ApiKey::OffsetCommit], 9);
    assert_eq!(used[&ApiKey::OffsetFetch], 9);
    assert_eq!(used[&ApiKey::FindCoordinator], 6);
    assert_eq!(used[&ApiKey::JoinGroup], 9);
    assert_eq!(used[&ApiKey::SyncGroup], 5);
    assert_eq!(used[&ApiKey::LeaveGroup], 5);
    let fetch = cluster.state().fetches[0].clone();
    assert_eq!((fetch.session_id, fetch.session_epoch), (0, -1));
}

#[tokio::test]
async fn versions_negotiate_within_kafka_2_4s_ranges() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().api_versions = Some(KAFKA_2_4.to_vec());
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    wait_until("a heartbeat", || requests(&cluster, ApiKey::Heartbeat) > 0).await;
    c.leave().await;
    let used = used_versions(&cluster);
    assert_eq!(used[&ApiKey::Metadata], 9);
    assert_eq!(used[&ApiKey::Fetch], 11);
    assert_eq!(used[&ApiKey::ListOffsets], 5);
    assert_eq!(used[&ApiKey::OffsetCommit], 8);
    assert_eq!(used[&ApiKey::OffsetFetch], 6);
    assert_eq!(used[&ApiKey::FindCoordinator], 3);
    assert_eq!(used[&ApiKey::JoinGroup], 6);
    assert_eq!(used[&ApiKey::SyncGroup], 4);
    assert_eq!(used[&ApiKey::Heartbeat], 4);
    assert_eq!(used[&ApiKey::LeaveGroup], 4);
}

#[tokio::test]
async fn a_broker_without_a_required_api_backs_off_as_unsupported_version() {
    let cluster = cluster(1, &[0]).await;
    let rows: Vec<_> =
        KAFKA_2_4.iter().copied().filter(|(api, _, _)| *api != ApiKey::JoinGroup).collect();
    cluster.state().api_versions = Some(rows);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    let joining = tokio::spawn(async move { c.next().await });
    probe
        .wait_for("two refused dials", |t| {
            t.sum("logit.input.broker.errors", &[("error", "unsupported_version")]) >= 2.0
        })
        .await;
    assert!(diagnostics(&mut probe, "unsupported_version") >= 1.0);
    assert_eq!(requests(&cluster, ApiKey::JoinGroup), 0);
    assert!(!joining.is_finished());
    joining.abort();
}

#[tokio::test]
async fn two_members_split_the_partitions_through_a_late_join_and_a_leave() {
    let cluster = cluster(1, &[0, 0, 0, 0]).await;
    for p in 0..4 {
        cluster.state().append("t", p, &values(&[&format!("p{p}")]), Compression::None);
    }
    let mut probe = TelemetryProbe::new();
    let mut a = drive(consumer(&probe, "a", settings(&cluster, &["t"])));
    wait_until("a alone holds the group", || stable_with(&cluster, 1)).await;
    assert_one_owner_each(&cluster, 4);
    wait_until("a commits every partition", || (0..4).all(|p| committed(&cluster, p) == Some(1)))
        .await;

    let generation = cluster.state().groups[GROUP].generation;
    let b = drive(consumer(&probe, "b", settings(&cluster, &["t"])));
    wait_until("both hold the group", || stable_with(&cluster, 2)).await;
    assert_eq!(cluster.state().groups[GROUP].generation, generation + 1);
    assert_one_owner_each(&cluster, 4);
    assert!(owners(&cluster).values().all(|parts| parts.len() == 2));
    assert!(a.drain().iter().any(|s| matches!(s, Seen::Revoke(RevokeReason::Rebalance))));
    probe
        .wait_for("a's rejoin counted", |t| {
            t.sum("logit.input.rebalances", &[("reason", "rebalance")]) >= 1.0
        })
        .await;

    for p in 0..4 {
        cluster.state().append("t", p, &values(&[&format!("q{p}")]), Compression::None);
    }
    wait_until("both commit what they read", || (0..4).all(|p| committed(&cluster, p) == Some(2)))
        .await;

    b.leave().await;
    wait_until("a holds the group again", || stable_with(&cluster, 1)).await;
    assert_one_owner_each(&cluster, 4);
    assert_eq!(requests(&cluster, ApiKey::LeaveGroup), 1);
    a.leave().await;
    assert_eq!(cluster.state().groups[GROUP].state, GroupState::Empty);
}

#[tokio::test]
async fn a_caller_that_never_accepts_keeps_its_session() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().group_session_bounds = (Duration::from_millis(100), Duration::from_secs(60));
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.session_timeout = Duration::from_millis(300);
    let mut c = consumer(&probe, "in", s);
    let Next::Records(_) = next(&mut c).await else { panic!("records first") };
    let generation = cluster.state().groups[GROUP].generation;
    let before = requests(&cluster, ApiKey::Heartbeat);
    // 30 heartbeats at 50 ms span five sessions.
    wait_until("heartbeats through five sessions", || {
        requests(&cluster, ApiKey::Heartbeat) >= before + 30
    })
    .await;
    assert_eq!(c.revoke_pending(), None);
    let state = cluster.state();
    assert_eq!(state.groups[GROUP].generation, generation);
    assert_eq!(state.groups[GROUP].state, GroupState::Stable);
    assert!(state.commits.is_empty(), "nothing accepted, nothing committed");
}

#[tokio::test]
async fn the_commit_never_passes_an_unaccepted_offset() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a", "b", "c", "d"]), Compression::None);
    let probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    let Next::Records(f) = next(&mut c).await else { panic!("records first") };
    assert_eq!(f.records.len(), 4);
    let tp = f.tp.clone();
    // The start position commits as Java's does, pinning where a `latest` start began.
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(0));
    // The second and fourth records accepted, the first not.
    c.mark_accepted(&tp, f.epoch, f.cut(1..2));
    c.mark_accepted(&tp, f.epoch, f.cut(3..4));
    assert_eq!(c.commit_all().await, CommitOutcome::Nothing);
    assert_eq!(committed(&cluster, 0), Some(0));
    c.mark_accepted(&tp, f.epoch, f.cut(0..1));
    assert_eq!(c.candidate(&tp), Some(2));
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(2));
    c.mark_accepted(&tp, f.epoch, f.cut(2..3));
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(4));
}

#[tokio::test]
async fn a_committed_position_is_where_the_next_member_starts() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a", "b"]), Compression::None);
    let probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "a", settings(&cluster, &["t"]));
    take(&mut c, 2).await;
    c.leave().await;
    assert_eq!(committed(&cluster, 0), Some(2));
    cluster.state().append("t", 0, &values(&["c"]), Compression::None);
    let mut d = consumer(&probe, "b", settings(&cluster, &["t"]));
    assert_eq!(take(&mut d, 1).await, ["c"]);
}

// --- Item 9: transport and authentication ---

#[tokio::test]
async fn a_broker_restart_redials_and_counts_a_reconnect() {
    let mut cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    take(&mut c, 1).await;
    cluster.restart(0).await;
    cluster.state().append("t", 0, &values(&["b"]), Compression::None);
    assert_eq!(take(&mut c, 1).await, ["b"]);
    assert!(errors(&mut probe, "fetch", "transport") >= 1.0);
    assert!(probe.sum("logit.input.reconnects", &[]) >= 1.0);
}

#[tokio::test]
async fn a_refused_sasl_login_backs_off_with_no_fallback() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().sasl = Some(crate::fake_broker::Sasl::new("user", "right"));
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.sasl = Some(crate::sasl::authenticator(
        &crate::sasl::KafkaSasl::Plain { username: "user".into(), password: "wrong".into() },
        Duration::from_secs(1),
    ));
    let mut c = consumer(&probe, "in", s);
    let joining = tokio::spawn(async move { c.next().await });
    probe
        .wait_for("two refused logins", |t| {
            t.sum("logit.input.broker.errors", &[("error", "sasl")]) >= 2.0
        })
        .await;
    assert!(diagnostics(&mut probe, "auth_failed") >= 1.0);
    assert_eq!(requests(&cluster, ApiKey::FindCoordinator), 0);
    joining.abort();
}

// --- Item 9: fetch outcomes ---

#[tokio::test]
async fn a_position_below_the_log_start_moves_to_it_and_counts_the_gap_as_retention() {
    let cluster = cluster(1, &[0]).await;
    for v in ["a", "b", "c", "d", "e", "f"] {
        cluster.state().append("t", 0, &values(&[v]), Compression::None);
    }
    cluster.state().groups.entry(GROUP.into()).or_default().committed.insert(("t".into(), 0), 1);
    cluster.state().truncate("t", 0, 4);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 2).await, ["e", "f"]);
    assert_eq!(probe.sum("logit.input.offsets.reset", &[("reason", "out_of_range")]), 1.0);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "retention")]), 3.0);
    assert_eq!(probe.sum("logit.input.records", &[]), 5.0);
    assert_eq!(errors(&mut probe, "fetch", "offset_out_of_range"), 1.0);
    assert!(diagnostics(&mut probe, "records_dropped") >= 1.0);
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(6));
}

#[tokio::test]
async fn a_position_above_the_log_end_moves_back_to_it() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a", "b"]), Compression::None);
    cluster.state().groups.entry(GROUP.into()).or_default().committed.insert(("t".into(), 0), 50);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    let reading = tokio::spawn(async move { take(&mut c, 1).await });
    wait_until("a fetch at the log end", || {
        cluster.state().fetches.iter().any(|f| f.partitions.iter().any(|p| p.2 == 2))
    })
    .await;
    cluster.state().append("t", 0, &values(&["c"]), Compression::None);
    assert_eq!(reading.await.unwrap(), ["c"]);
    assert_eq!(probe.sum("logit.input.offsets.reset", &[("reason", "out_of_range")]), 1.0);
    assert_eq!(probe.sum("logit.input.records.dropped", &[]), 0.0);
}

/// A reset to a log end below the commit candidate's range keeps the candidate, and a range
/// fetched before the reset still moves it up to the reset point.
#[tokio::test]
async fn a_reset_below_a_handed_out_range_keeps_the_commit_movable() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a", "b"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    take(&mut c, 2).await;
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    cluster.state().append("t", 0, &values(&["c", "d", "e", "f"]), Compression::None);
    let Next::Records(early) = next(&mut c).await else { panic!("records") };
    let range = early.cut(0..early.records.len());
    assert_eq!(range, 2..6);
    let tp = early.tp.clone();

    // The topic is recreated with three offsets: the fetch at 6 is past its end.
    cluster.state().add_topic("t", &[0]);
    cluster.state().append("t", 0, &values(&["x", "y", "z"]), Compression::None);
    wait_until("the reset to the new log end", || {
        probe.sum("logit.input.offsets.reset", &[("reason", "out_of_range")]) == 1.0
    })
    .await;
    assert_eq!(c.candidate(&tp), Some(2));
    c.mark_accepted(&tp, early.epoch, range);
    assert_eq!(c.candidate(&tp), Some(3));

    cluster.state().append("t", 0, &values(&["w"]), Compression::None);
    assert_eq!(take(&mut c, 1).await, ["w"]);
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(4));
}

#[tokio::test]
async fn a_crc_mismatch_refetches_once_and_reads_the_batch() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    cluster.script(Step::api(ApiKey::Fetch, Action::CorruptCrc).on_partition(0));
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[]), 0.0);
    assert_eq!(cluster.state().fetches.len(), 2);
}

#[tokio::test]
async fn a_second_crc_mismatch_skips_the_batch_as_corrupt() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a", "b"]), Compression::None);
    cluster.state().append("t", 0, &values(&["c"]), Compression::None);
    cluster.script(Step::api(ApiKey::Fetch, Action::CorruptCrc).on_partition(0));
    cluster.script(Step::api(ApiKey::Fetch, Action::CorruptCrc).on_partition(0));
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 1).await, ["c"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "corrupt")]), 2.0);
    assert_eq!(probe.sum("logit.input.records", &[]), 3.0);
    assert!(diagnostics(&mut probe, "records_dropped") >= 1.0);
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(3));
}

/// A batch of `values` whose `last_offset_delta` reads `delta`, its CRC left stale.
/// Its record count reads 50 too, so neither header field matches the offsets the batch holds.
fn bad_delta(values_: &[&str], delta: i32) -> Bytes {
    use logit_proto::kafka::batch::{encode, Stamp};
    let mut raw = encode(&values(values_), Stamp::NONE, Compression::None).unwrap().to_vec();
    raw[23..27].copy_from_slice(&delta.to_be_bytes());
    raw[57..61].copy_from_slice(&50i32.to_be_bytes());
    Bytes::from(raw)
}

/// A failed CRC leaves the header's offsets untrusted: the skip ends at the next batch's base.
#[tokio::test]
async fn a_corrupt_batch_with_a_far_delta_skips_to_the_next_batch() {
    for delta in [1_000, -5] {
        let cluster = cluster(1, &[0]).await;
        cluster.state().append_raw("t", 0, bad_delta(&["a", "b", "c"], delta), 3);
        cluster.state().append("t", 0, &values(&["d"]), Compression::None);
        let mut probe = TelemetryProbe::new();
        let mut s = settings(&cluster, &["t"]);
        s.commit_interval = Duration::from_secs(60);
        let mut c = consumer(&probe, "in", s);
        assert_eq!(take(&mut c, 1).await, ["d"], "delta {delta}");
        assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "corrupt")]), 3.0);
        assert_eq!(c.commit_all().await, CommitOutcome::Committed);
        assert_eq!(committed(&cluster, 0), Some(4));
    }
}

/// An answer cut inside the batch after a corrupt one still holds that batch's base offset in
/// its first 12 bytes: the skip lands there, not on the high watermark.
#[tokio::test]
async fn a_corrupt_batch_before_a_cut_one_skips_to_the_cut_batchs_base() {
    use logit_proto::kafka::batch::{encode, Stamp};
    let cluster = cluster(1, &[0]).await;
    let mut next = encode(&values(&["d"]), Stamp::NONE, Compression::None).unwrap().to_vec();
    next[..8].copy_from_slice(&3i64.to_be_bytes());
    let blob = [bad_delta(&["a", "b", "c"], 1_000).to_vec(), next[..40].to_vec()].concat();
    cluster.state().append_raw("t", 0, Bytes::from(blob), 3);
    cluster.state().append("t", 0, &values(&["d"]), Compression::None);
    cluster.state().append("t", 0, &values(&["e"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    // One stored entry per answer, so the cut batch is the last bytes of the first one.
    s.fetch.max_partition_bytes = 1;
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 1).await, ["d"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "corrupt")]), 3.0);
}

/// With no batch after it in the answer, the skip ends at the high watermark, not at the
/// offset the header names past the log end.
#[tokio::test]
async fn a_corrupt_last_batch_skips_to_the_high_watermark() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append_raw("t", 0, bad_delta(&["a", "b", "c"], 1_000), 3);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    let reading = tokio::spawn(async move { take(&mut c, 1).await });
    wait_until("a fetch at the high watermark", || fetches_at(&cluster, 3) > 0).await;
    cluster.state().append("t", 0, &values(&["d"]), Compression::None);
    assert_eq!(reading.await.unwrap(), ["d"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "corrupt")]), 3.0);
    assert_eq!(probe.sum("logit.input.offsets.reset", &[("reason", "out_of_range")]), 0.0);
}

/// A failed batch returned alone, past `max_partition_bytes`, has nothing verified after it:
/// the partition is refetched once with `max_batch_bytes` of room, which brings the next batch
/// whole beside it, and the skip lands on that batch's base rather than the high watermark.
#[tokio::test]
async fn a_lone_corrupt_batch_is_refetched_widened_and_skips_to_the_next_batch() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append_raw("t", 0, bad_delta(&["a", "b", "c"], 1_000), 3);
    cluster.state().append("t", 0, &values(&["d"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.fetch.max_partition_bytes = 1;
    let (max_bytes, room) = (s.fetch.max_bytes, s.max_batch_bytes as i32);
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 1).await, ["d"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "corrupt")]), 3.0);
    let widened = cluster.state().fetches.iter().any(|f| {
        f.partitions.iter().any(|p| p.2 == 0 && p.3 == room) && f.max_bytes == max_bytes + room
    });
    assert!(widened, "a fetch at 0 with max_batch_bytes of room");
    // The room is for one refetch: the fetch after `d` asks for the configured bounds again.
    wait_until("a fetch past d", || fetches_at(&cluster, 4) > 0).await;
    for f in cluster.state().fetches.iter().filter(|f| f.partitions.iter().any(|p| p.2 == 4)) {
        assert_eq!(f.max_bytes, max_bytes);
        assert!(f.partitions.iter().all(|p| p.3 == 1), "{:?}", f.partitions);
    }
}

#[tokio::test]
async fn corrupt_message_on_fetch_refetches_after_a_backoff() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    cluster.script(Step::api(ApiKey::Fetch, Action::Error(code::CORRUPT_MESSAGE)));
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(errors(&mut probe, "fetch", "corrupt_message"), 1.0);
    assert_eq!(probe.sum("logit.input.records.dropped", &[]), 0.0);
    let fetches = cluster.state().fetches.clone();
    assert_eq!((fetches[0].partitions[0].2, fetches[1].partitions[0].2), (0, 0));
}

#[tokio::test]
async fn a_leader_move_refreshes_metadata_and_fetches_from_the_new_leader() {
    let cluster = FakeCluster::start(2).await;
    cluster.state().add_topic("t", &[0]);
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    take(&mut c, 1).await;
    cluster.state().topics.get_mut("t").unwrap()[0].leader = 1;
    cluster.state().append("t", 0, &values(&["b"]), Compression::None);
    assert_eq!(take(&mut c, 1).await, ["b"]);
    assert!(errors(&mut probe, "fetch", "not_leader_or_follower") >= 1.0);
    assert!(cluster.state().fetches.iter().any(|f| f.node == 1));
}

#[tokio::test]
async fn every_refresh_code_on_fetch_refreshes_and_refetches() {
    let cluster = cluster(1, &[0]).await;
    let refresh = [
        code::UNKNOWN_TOPIC_OR_PARTITION,
        code::LEADER_NOT_AVAILABLE,
        code::REPLICA_NOT_AVAILABLE,
        code::KAFKA_STORAGE_ERROR,
        code::FENCED_LEADER_EPOCH,
        code::UNKNOWN_LEADER_EPOCH,
        code::OFFSET_NOT_AVAILABLE,
    ];
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    cluster.state().append("t", 0, &values(&["warm"]), Compression::None);
    take(&mut c, 1).await;
    for (i, &error) in refresh.iter().enumerate() {
        let metadata = requests(&cluster, ApiKey::Metadata);
        cluster.script(Step::api(ApiKey::Fetch, Action::Error(error)).on_partition(0));
        let value = format!("v{i}");
        cluster.state().append("t", 0, &values(&[&value]), Compression::None);
        assert_eq!(take(&mut c, 1).await, [value]);
        assert_eq!(errors(&mut probe, "fetch", code_tag(error)), 1.0, "{error}");
        assert!(requests(&cluster, ApiKey::Metadata) > metadata, "{error} refreshed");
    }
}

#[tokio::test]
async fn topic_authorization_failed_on_fetch_backs_off_with_not_authorized() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    cluster.script(Step::api(ApiKey::Fetch, Action::Error(code::TOPIC_AUTHORIZATION_FAILED)));
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(errors(&mut probe, "fetch", "topic_authorization_failed"), 1.0);
    assert_eq!(diagnostics(&mut probe, "not_authorized"), 1.0);
    assert_eq!(c.assignment().len(), 1);
}

#[tokio::test]
async fn a_throttled_answer_mutes_the_connection_until_it_passes() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    cluster.script(Step::api(ApiKey::Fetch, Action::Throttle(400)));
    let probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    let reading = tokio::spawn(async move { take(&mut c, 2).await });
    wait_until("the throttled fetch", || !cluster.state().fetches.is_empty()).await;
    let throttled = Instant::now();
    wait_until("the next fetch", || cluster.state().fetches.len() >= 2).await;
    assert!(throttled.elapsed() >= Duration::from_millis(350), "{:?}", throttled.elapsed());
    cluster.state().append("t", 0, &values(&["b"]), Compression::None);
    assert_eq!(reading.await.unwrap(), ["a", "b"]);
}

#[tokio::test]
async fn a_batch_over_max_batch_bytes_is_skipped_as_oversize() {
    let cluster = cluster(1, &[0]).await;
    let big = "x".repeat(4096);
    cluster.state().append("t", 0, &values(&[&big, &big]), Compression::Gzip);
    cluster.state().append("t", 0, &values(&["small"]), Compression::Gzip);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.max_batch_bytes = 1024;
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 1).await, ["small"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "oversize")]), 2.0);
    assert!(diagnostics(&mut probe, "records_dropped") >= 1.0);
}

/// Fetch reads an answer up to `fetch.max_bytes` plus `max_batch_bytes`, past the 64 MiB other
/// requests' answers are held to, so a batch under `max_batch_bytes` is never refused as a
/// transport failure.
#[tokio::test]
async fn an_answer_over_64_mib_holding_a_batch_under_max_batch_bytes_is_read() {
    let cluster = cluster(1, &[0]).await;
    let big = "x".repeat(65 << 20);
    cluster.state().append("t", 0, &values(&[&big]), Compression::None);
    cluster.state().append("t", 0, &values(&["small"]), Compression::None);
    let probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.max_batch_bytes = 128 << 20;
    let mut c = consumer(&probe, "in", s);
    let taken = take(&mut c, 2).await;
    assert_eq!(taken[0].len(), big.len());
    assert_eq!(taken[1], "small");
}

/// An answer over the Fetch cap can't be read past: the batch in it has no offsets to skip by,
/// so the fetch backs off and asks again, counted and named.
#[tokio::test]
async fn an_answer_over_the_fetch_cap_backs_off_as_response_too_large() {
    let cluster = cluster(1, &[0]).await;
    let big = "x".repeat(2 << 20);
    cluster.state().append("t", 0, &values(&[&big]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.fetch.max_bytes = 1024;
    s.fetch.max_partition_bytes = 1024;
    s.fetch.min_bytes = 1;
    s.max_batch_bytes = 1024;
    let mut c = consumer(&probe, "in", s);
    let reading = tokio::spawn(async move { timeout(Duration::from_secs(60), c.next()).await });
    wait_until("two refused answers", || errors(&mut probe, "fetch", "response_too_large") >= 2.0)
        .await;
    assert!(diagnostics(&mut probe, "response_too_large") >= 1.0);
    assert_eq!(errors(&mut probe, "fetch", "transport"), 0.0);
    assert!(fetches_at(&cluster, 0) >= 2);
    reading.abort();
}

/// A batch of `records` whose attributes claim gzip over an uncompressed records section, its
/// CRC valid.
fn undecompressable(records: &[ProducerRecord]) -> Bytes {
    use logit_proto::kafka::batch::{encode, Stamp};
    let mut raw = encode(records, Stamp::NONE, Compression::None).unwrap().to_vec();
    raw[22] |= 1;
    let crc = crc32c::crc32c(&raw[21..]);
    raw[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(raw)
}

#[tokio::test]
async fn a_batch_that_does_not_decompress_is_skipped_as_corrupt() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append_raw("t", 0, undecompressable(&values(&["a", "b", "c"])), 3);
    cluster.state().append("t", 0, &values(&["d"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["d"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "corrupt")]), 3.0);
    assert_eq!(fetches_at(&cluster, 0), 2);
}

/// The fetches that asked for partition 0 at `offset`.
fn fetches_at(cluster: &FakeCluster, offset: i64) -> usize {
    cluster.state().fetches.iter().filter(|f| f.partitions.iter().any(|p| p.2 == offset)).count()
}

/// A magic 1 message set of one message.
fn legacy_message_set() -> Bytes {
    let mut body = vec![1u8, 0];
    body.extend_from_slice(&1_000i64.to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&1i32.to_be_bytes());
    body.push(b'v');
    let mut out = 0i64.to_be_bytes().to_vec();
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend(body);
    Bytes::from(out)
}

#[tokio::test]
async fn a_legacy_message_set_is_skipped_as_legacy_format() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append_raw("t", 0, legacy_message_set(), 1);
    cluster.state().append("t", 0, &values(&["b"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 1).await, ["b"]);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "legacy_format")]), 1.0);
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(2));
}

#[tokio::test]
async fn read_committed_skips_aborted_transactions_and_control_records() {
    let cluster = cluster(1, &[0]).await;
    {
        let mut state = cluster.state();
        state.append_transactional("t", 0, 7, 0, &values(&["aborted-1"]));
        state.append_transactional("t", 0, 8, 0, &values(&["kept-1"]));
        state.append_transactional("t", 0, 7, 0, &values(&["aborted-2"]));
        state.end_transaction("t", 0, 7, 0, false);
        state.end_transaction("t", 0, 8, 0, true);
        state.append("t", 0, &values(&["plain"]), Compression::None);
    }
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.isolation = Isolation::ReadCommitted;
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 2).await, ["kept-1", "plain"]);
    assert_eq!(probe.sum("logit.input.records.skipped", &[("reason", "aborted")]), 2.0);
    assert_eq!(probe.sum("logit.input.records", &[]), 4.0);
    assert!(cluster.state().fetches.iter().all(|f| f.isolation_level == 1));
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(6));
}

#[tokio::test]
async fn read_committed_stops_at_an_open_transaction() {
    let cluster = cluster(1, &[0]).await;
    {
        let mut state = cluster.state();
        state.append("t", 0, &values(&["before"]), Compression::None);
        state.append_transactional("t", 0, 7, 0, &values(&["open"]));
        state.append("t", 0, &values(&["after"]), Compression::None);
    }
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.isolation = Isolation::ReadCommitted;
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 1).await, ["before"]);
    assert_eq!(probe.gauge("logit.input.lag", &[("topic", "t"), ("partition", "0")]), Some(0.0));
    cluster.state().end_transaction("t", 0, 7, 0, true);
    assert_eq!(take(&mut c, 2).await, ["open", "after"]);
}

// --- Item 9: metadata and group outcomes ---

#[tokio::test]
async fn a_missing_topic_is_subscribed_once_it_appears() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t", "u"]);
    s.metadata_max_age = Duration::from_millis(100);
    let mut c = consumer(&probe, "in", s);
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert!(diagnostics(&mut probe, "topic_missing") >= 1.0);
    assert!(errors(&mut probe, "metadata", "unknown_topic_or_partition") >= 1.0);
    cluster.state().add_topic("u", &[0]);
    cluster.state().append("u", 0, &values(&["b"]), Compression::None);
    let Next::Revoke(RevokeReason::Metadata) = next(&mut c).await else {
        panic!("a metadata revoke")
    };
    assert_eq!(take(&mut c, 1).await, ["b"]);
    assert_eq!(probe.sum("logit.input.rebalances", &[("reason", "metadata")]), 1.0);
    assert_eq!(c.assignment().len(), 2);
}

#[tokio::test]
async fn coordinator_load_in_progress_backs_off_on_the_same_coordinator() {
    let cluster = cluster(1, &[0]).await;
    cluster.script(Step::api(ApiKey::JoinGroup, Action::Error(code::COORDINATOR_LOAD_IN_PROGRESS)));
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(errors(&mut probe, "join_group", "coordinator_load_in_progress"), 1.0);
    assert_eq!(requests(&cluster, ApiKey::FindCoordinator), 1);
}

#[tokio::test]
async fn not_coordinator_and_coordinator_not_available_rediscover() {
    for error in [code::NOT_COORDINATOR, code::COORDINATOR_NOT_AVAILABLE] {
        let cluster = cluster(1, &[0]).await;
        cluster.script(Step::api(ApiKey::SyncGroup, Action::Error(error)));
        cluster.state().append("t", 0, &values(&["a"]), Compression::None);
        let mut probe = TelemetryProbe::new();
        let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
        assert_eq!(take(&mut c, 1).await, ["a"]);
        assert_eq!(errors(&mut probe, "sync_group", code_tag(error)), 1.0);
        assert_eq!(requests(&cluster, ApiKey::FindCoordinator), 2, "{error}");
    }
}

#[tokio::test]
async fn rebalance_in_progress_on_a_heartbeat_revokes_commits_and_rejoins() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    let Next::Records(f) = next(&mut c).await else { panic!("records first") };
    c.mark_accepted(&f.tp, f.epoch, f.cut(0..1));
    cluster.script(Step::api(ApiKey::Heartbeat, Action::Error(code::REBALANCE_IN_PROGRESS)));
    let Next::Revoke(RevokeReason::Rebalance) = next(&mut c).await else { panic!("a revoke") };
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(1));
    c.rejoin().await;
    assert_eq!(probe.sum("logit.input.rebalances", &[("reason", "rebalance")]), 1.0);
    assert_eq!(errors(&mut probe, "heartbeat", "rebalance_in_progress"), 1.0);
    cluster.state().append("t", 0, &values(&["b"]), Compression::None);
    assert_eq!(take(&mut c, 1).await, ["b"]);
}

/// Scripts `error` on the next heartbeat and follows the revoke through; returns the member id
/// before it, the joins after it, and the probe.
async fn heartbeat_fences(
    error: i16,
) -> (String, Vec<crate::fake_broker::JoinSeen>, TelemetryProbe) {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    s.rebalance_timeout = STALE_MEMBER_WAIT;
    let mut c = consumer(&probe, "in", s);
    let Next::Records(f) = next(&mut c).await else { panic!("records first") };
    let member = c.membership().unwrap().0.to_string();
    let joins = cluster.state().joins.len();
    c.mark_accepted(&f.tp, f.epoch, f.cut(0..1));
    cluster.script(Step::api(ApiKey::Heartbeat, Action::Error(error)));
    let Next::Revoke(RevokeReason::MembershipLost) = next(&mut c).await else {
        panic!("a membership revoke")
    };
    assert_eq!(c.commit_all().await, CommitOutcome::Skipped);
    c.rejoin().await;
    assert!(cluster.state().commits.is_empty(), "no commit after a lost membership");
    assert_eq!(probe.sum("logit.input.rebalances", &[("reason", "membership_lost")]), 1.0);
    let after = cluster.state().joins[joins..].to_vec();
    (member, after, probe)
}

#[tokio::test]
async fn illegal_generation_revokes_without_a_commit_and_rejoins_under_the_same_id() {
    let (member, joins, mut probe) = heartbeat_fences(code::ILLEGAL_GENERATION).await;
    assert_eq!(joins[0].member_id, member);
    assert_eq!(errors(&mut probe, "heartbeat", "illegal_generation"), 1.0);
}

#[tokio::test]
async fn unknown_member_id_and_fenced_instance_id_rejoin_with_an_empty_id() {
    for error in [code::UNKNOWN_MEMBER_ID, code::FENCED_INSTANCE_ID] {
        let (_, joins, mut probe) = heartbeat_fences(error).await;
        assert_eq!(joins[0].member_id, "", "{error}");
        assert_eq!(errors(&mut probe, "heartbeat", code_tag(error)), 1.0);
    }
}

#[tokio::test]
async fn an_expired_session_rejoins_with_an_empty_id() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().group_session_bounds = (Duration::from_millis(100), Duration::from_secs(60));
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.session_timeout = Duration::from_millis(300);
    let mut c = consumer(&probe, "in", s);
    take(&mut c, 1).await;
    let joins = cluster.state().joins.len();
    // Every heartbeat goes unanswered for longer than the session.
    for _ in 0..50 {
        cluster.script(Step::api(ApiKey::Heartbeat, Action::Close));
    }
    wait_until("the member is removed", || cluster.state().groups[GROUP].members.is_empty()).await;
    cluster.state().script.clear();
    let Next::Revoke(RevokeReason::MembershipLost) = next(&mut c).await else {
        panic!("a membership revoke")
    };
    c.rejoin().await;
    assert_eq!(cluster.state().joins[joins].member_id, "");
    // The heartbeat or the periodic commit, whichever reaches the coordinator first, hears 25.
    assert!(probe.sum("logit.input.broker.errors", &[("error", "unknown_member_id")]) >= 1.0);
    assert!(errors(&mut probe, "heartbeat", "transport") >= 1.0);
}

#[tokio::test]
async fn group_refusals_back_off_and_rejoin_with_group_refused() {
    for error in [
        code::INCONSISTENT_GROUP_PROTOCOL,
        code::INVALID_SESSION_TIMEOUT,
        code::GROUP_MAX_SIZE_REACHED,
    ] {
        let cluster = cluster(1, &[0]).await;
        cluster.script(Step::api(ApiKey::JoinGroup, Action::Error(error)));
        cluster.state().append("t", 0, &values(&["a"]), Compression::None);
        let mut probe = TelemetryProbe::new();
        let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
        assert_eq!(take(&mut c, 1).await, ["a"]);
        assert_eq!(errors(&mut probe, "join_group", code_tag(error)), 1.0);
        assert_eq!(diagnostics(&mut probe, "group_refused"), 1.0, "{error}");
    }
}

#[tokio::test]
async fn group_authorization_failed_backs_off_with_not_authorized() {
    let cluster = cluster(1, &[0]).await;
    cluster.script(Step::api(
        ApiKey::FindCoordinator,
        Action::Error(code::GROUP_AUTHORIZATION_FAILED),
    ));
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(errors(&mut probe, "find_coordinator", "group_authorization_failed"), 1.0);
    assert_eq!(diagnostics(&mut probe, "not_authorized"), 1.0);
}

#[tokio::test]
async fn unsupported_version_from_a_group_request_backs_off() {
    let cluster = cluster(1, &[0]).await;
    cluster.script(Step::api(ApiKey::SyncGroup, Action::Error(code::UNSUPPORTED_VERSION)));
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(errors(&mut probe, "sync_group", "unsupported_version"), 1.0);
    assert_eq!(diagnostics(&mut probe, "unsupported_version"), 1.0);
}

#[tokio::test]
async fn unstable_offset_commit_on_offset_fetch_backs_off() {
    let cluster = cluster(1, &[0]).await;
    for _ in 0..3 {
        cluster.script(Step::api(ApiKey::OffsetFetch, Action::Error(code::UNSTABLE_OFFSET_COMMIT)));
    }
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    assert_eq!(take(&mut c, 1).await, ["a"]);
    assert_eq!(errors(&mut probe, "offset_fetch", "unstable_offset_commit"), 3.0);
    assert_eq!(requests(&cluster, ApiKey::OffsetFetch), 4);
    // A leader's rejoin would rebalance the whole group; the retry keeps the membership.
    assert_eq!(requests(&cluster, ApiKey::JoinGroup), 2);
    assert_eq!(diagnostics(&mut probe, "broker_error"), 0.0);
}

// --- Item 9: commit outcomes ---

/// A caller that drops `next` while the offset fetch after SyncGroup retries leaves no
/// membership behind: the next call joins again and reads, rather than holding partitions it
/// never fetches.
#[tokio::test]
async fn a_join_dropped_during_the_offset_fetch_joins_again() {
    let cluster = cluster(1, &[0]).await;
    cluster.script(Step::api(ApiKey::OffsetFetch, Action::Error(code::UNSTABLE_OFFSET_COMMIT)));
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut c = consumer(&probe, "in", settings(&cluster, &["t"]));
    {
        let pending = c.next();
        tokio::pin!(pending);
        tokio::select! {
            _ = &mut pending => panic!("records before the offset fetch was answered"),
            () = wait_until("the refused offset fetch", || {
                errors(&mut probe, "offset_fetch", "unstable_offset_commit") == 1.0
            }) => {}
        }
    }
    assert_eq!(take(&mut c, 1).await, ["a"]);
}

#[tokio::test]
async fn a_failed_commit_keeps_the_positions_for_the_next_one() {
    let cluster = cluster(1, &[0]).await;
    cluster.state().append("t", 0, &values(&["a"]), Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, &["t"]);
    s.commit_interval = Duration::from_secs(60);
    let mut c = consumer(&probe, "in", s);
    take(&mut c, 1).await;
    cluster.script(Step::api(ApiKey::OffsetCommit, Action::Close));
    cluster
        .script(Step::api(ApiKey::OffsetCommit, Action::Error(code::COORDINATOR_LOAD_IN_PROGRESS)));
    assert_eq!(c.commit_all().await, CommitOutcome::Committed);
    assert_eq!(committed(&cluster, 0), Some(1));
    assert_eq!(probe.sum("logit.input.commits", &[("class", "error")]), 2.0);
    assert_eq!(probe.sum("logit.input.commits", &[("class", "ok")]), 1.0);
    assert_eq!(errors(&mut probe, "offset_commit", "transport"), 1.0);
    assert_eq!(errors(&mut probe, "offset_commit", "coordinator_load_in_progress"), 1.0);
}

#[tokio::test]
async fn a_commit_refused_for_membership_never_lands() {
    for error in [code::ILLEGAL_GENERATION, code::UNKNOWN_MEMBER_ID, code::FENCED_INSTANCE_ID] {
        let cluster = cluster(1, &[0]).await;
        cluster.state().append("t", 0, &values(&["a"]), Compression::None);
        let mut probe = TelemetryProbe::new();
        let mut s = settings(&cluster, &["t"]);
        s.commit_interval = Duration::from_secs(60);
        s.rebalance_timeout = STALE_MEMBER_WAIT;
        let mut c = consumer(&probe, "in", s);
        take(&mut c, 1).await;
        cluster.script(Step::api(ApiKey::OffsetCommit, Action::Error(error)));
        assert_eq!(c.commit_all().await, CommitOutcome::Fenced, "{error}");
        assert_eq!(c.revoke_pending(), Some(RevokeReason::MembershipLost));
        assert_eq!(c.commit_all().await, CommitOutcome::Skipped);
        let Next::Revoke(RevokeReason::MembershipLost) = next(&mut c).await else {
            panic!("a membership revoke")
        };
        c.rejoin().await;
        assert_eq!(committed(&cluster, 0), None, "{error}");
        assert_eq!(cluster.state().commits.len(), 1);
        assert_eq!(probe.sum("logit.input.commits", &[("class", "fenced")]), 1.0);
    }
}
