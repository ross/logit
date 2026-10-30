//! The consumer facade `kafka_in` drives: group membership, assignment, the fetch loops, and the
//! commit of what the caller accepted.
//!
//! The caller's loop, which `kafka_in` runs:
//!
//! 1. [`Consumer::next`] joins the group when the consumer isn't a member, commits on
//!    `commit_interval`, refreshes metadata, and returns a partition's buffered records
//!    ([`Next::Records`]) or a pending revoke ([`Next::Revoke`]).
//! 2. The caller cuts the records into batches, sends each, and calls
//!    [`Consumer::mark_accepted`] with the `Fetched`'s epoch and [`Fetched::cut`]'s range for each
//!    batch accepted.
//!    Between attempts at a send it reads [`Consumer::revoke_pending`]: once a revoke is
//!    pending, the batch in flight gets the drain deadline and the rest are deferred.
//! 3. On a revoke (`docs/adr/kafka-input.md`, item 8), the fetch loops have stopped and buffered
//!    records are discarded ([`Consumer::stop_fetching`], which `next` runs before returning
//!    it). The caller settles its batch in flight, then calls [`Consumer::commit_all`] and
//!    [`Consumer::rejoin`]; a `next` after a returned revoke runs both itself.
//! 4. At shutdown, [`Consumer::leave`]: stop, commit, LeaveGroup.
//!
//! Membership: the first join counts `logit.input.rebalances{reason="start"}`, and every later
//! one the reason its revoke carried. A lost membership (22, 25, 82) skips the commit, because
//! the coordinator refuses it and the partitions already belong to the next generation; 25 and
//! 82 rejoin under an empty member id, 22 under the same one. The leader runs the assignor over
//! every member's subscription, with partition counts from a Metadata request for every topic
//! any member subscribes to. A refresh that shows a subscribed topic appearing or changing its
//! partition count revokes with reason `metadata`, as Java's `ConsumerCoordinator` rejoins on a
//! changed metadata snapshot.
//!
//! Commits: each assigned partition's [`Tracker`] holds its candidate. A commit sends every
//! partition whose candidate moved, with the generation and member id. A transport failure or a
//! retriable code leaves the candidates pending for the next commit
//! (`logit.input.commits{class="error"}`); a fenced answer drops them and revokes
//! (`class="fenced"`).
//!
//! Records the caller takes count `logit.input.records` and `logit.input.record.bytes` (value
//! bytes) when `next` hands them out, so the caller decodes every record of a [`Fetched`] it
//! takes, and a revoke's discarded buffers aren't counted.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use kafka_protocol::messages::ApiKey;
use logit_core::{Diagnostics, Telemetry};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::assignor::{
    decode_assignment, decode_subscription, encode_assignment, encode_subscription,
};
use super::fetch::{self, Fetched, LoopSettings, Part, Position, Shared, Wakes};
use super::group::{self, Failure, JoinRequest, Joined};
use super::heartbeat::{self, Heartbeat};
use super::metadata::{self, Cluster};
use super::offsets::{self, Tracker};
use super::pool::{Link, Pool, Slot, Target};
use super::{group_code, Backoff, ConsumerSettings, Counters, GroupCode, TopicPartition};
use crate::error::code;

/// Why the group takes the assignment back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevokeReason {
    /// The coordinator answered `REBALANCE_IN_PROGRESS`: another member joined or left.
    Rebalance,
    /// The membership ended (22, 25, 82): no commit, and the next owner reads on.
    MembershipLost,
    /// A subscribed topic appeared or changed its partition count.
    Metadata,
}

impl RevokeReason {
    fn tag(self) -> &'static str {
        match self {
            RevokeReason::Rebalance => "rebalance",
            RevokeReason::MembershipLost => "membership_lost",
            RevokeReason::Metadata => "metadata",
        }
    }
}

/// A pending revoke, as the heartbeat task and the consumer raise it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Revoke {
    pub(crate) reason: RevokeReason,
    /// Rejoin under an empty member id.
    pub(crate) reset_member: bool,
}

impl Revoke {
    pub(crate) fn new(reason: RevokeReason) -> Self {
        Self { reason, reset_member: false }
    }
}

/// Raises `revoke` unless one is pending; a membership loss replaces any other reason, since
/// it changes what the revoke may do.
pub(crate) fn raise(signal: &watch::Sender<Option<Revoke>>, revoke: Revoke) {
    signal.send_if_modified(|current| match current {
        None => {
            *current = Some(revoke);
            true
        }
        Some(c)
            if revoke.reason == RevokeReason::MembershipLost
                && c.reason != RevokeReason::MembershipLost =>
        {
            *current = Some(revoke);
            true
        }
        Some(_) => false,
    });
}

/// What [`Consumer::next`] returns.
#[derive(Debug)]
pub enum Next {
    Records(Fetched),
    Revoke(RevokeReason),
}

/// How a commit went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Every pending candidate committed.
    Committed,
    /// No candidate had moved.
    Nothing,
    /// The positions stay pending for the next commit.
    Failed,
    /// The coordinator refused the generation or member: the positions are dropped.
    Fenced,
    /// No commit was sent: the consumer isn't a member, or its membership is lost.
    Skipped,
}

/// The joined member.
#[derive(Debug, Clone)]
struct Member {
    member_id: String,
    generation: i32,
}

/// A Kafka consumer-group member. See the module doc for the call sequence.
pub struct Consumer {
    settings: ConsumerSettings,
    pool: Pool,
    counters: Counters,
    control: Link,
    coordinator: Link,
    /// Whether `coordinator` points at a coordinator FindCoordinator named.
    located: bool,
    cluster: Option<Cluster>,
    member: Option<Member>,
    /// The id the next join sends; empty for a new member.
    member_id: String,
    /// Partition counts of the subscribed topics when the assignment was made.
    snapshot: BTreeMap<String, i32>,
    assignment: Vec<TopicPartition>,
    heartbeat: Option<Heartbeat>,
    signal: watch::Sender<Option<Revoke>>,
    signal_rx: watch::Receiver<Option<Revoke>>,
    /// The revoke [`Consumer::next`] returned, until the rejoin.
    revoking: Option<Revoke>,
    /// The reason the next completed join counts.
    join_reason: &'static str,
    shared: Arc<Mutex<Shared>>,
    wakes: Arc<Wakes>,
    loops: HashMap<i32, JoinHandle<()>>,
    next_commit: Instant,
    next_metadata: Instant,
    /// Where `next` looks first, so partitions take turns.
    turn: usize,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        for task in self.loops.values() {
            task.abort();
        }
    }
}

impl Consumer {
    /// A consumer that joins on its first [`Consumer::next`]. Fails only on settings it can't
    /// use: no bootstrap broker, or one that isn't `host:port`.
    pub fn new(
        settings: ConsumerSettings,
        telemetry: Telemetry,
        diag: Diagnostics,
    ) -> anyhow::Result<Self> {
        let counters = Counters { telemetry, diag };
        let pool = Pool::new(&settings, counters.clone())?;
        let (signal, signal_rx) = watch::channel(None);
        let now = Instant::now();
        Ok(Self {
            pool,
            counters,
            control: Link::new(Slot::Control, Target::Any),
            coordinator: Link::new(Slot::Coordinator, Target::Any),
            located: false,
            cluster: None,
            member: None,
            member_id: String::new(),
            snapshot: BTreeMap::new(),
            assignment: Vec::new(),
            heartbeat: None,
            signal,
            signal_rx,
            revoking: None,
            join_reason: "start",
            shared: Default::default(),
            wakes: Default::default(),
            loops: HashMap::new(),
            next_commit: now + settings.commit_interval,
            next_metadata: now + settings.metadata_max_age,
            turn: 0,
            settings,
        })
    }

    /// The partitions assigned now.
    pub fn assignment(&self) -> &[TopicPartition] {
        &self.assignment
    }

    /// The member id and generation, while a member.
    pub fn membership(&self) -> Option<(&str, i32)> {
        self.member.as_ref().map(|m| (m.member_id.as_str(), m.generation))
    }

    /// The revoke pending now, if any: the caller's cue to give its batch in flight the drain
    /// deadline.
    pub fn revoke_pending(&self) -> Option<RevokeReason> {
        self.signal_rx.borrow().map(|r| r.reason)
    }

    /// The next partition's records, or a revoke. Joins first when not a member. Cancel-safe
    /// between joins: a join it was running when dropped is begun again by the next call.
    pub async fn next(&mut self) -> Next {
        loop {
            if self.revoking.is_some() {
                self.commit_all().await;
                self.rejoin().await;
                continue;
            }
            let pending = *self.signal_rx.borrow();
            if let Some(revoke) = pending {
                self.stop_fetching();
                self.revoking = Some(revoke);
                return Next::Revoke(revoke.reason);
            }
            if self.member.is_none() {
                self.rejoin().await;
                continue;
            }
            let now = Instant::now();
            let refresh = std::mem::take(&mut self.shared.lock().unwrap().refresh);
            if refresh || now >= self.next_metadata {
                self.refresh().await;
                continue;
            }
            if now >= self.next_commit {
                self.commit().await;
                self.next_commit = Instant::now() + self.settings.commit_interval;
                continue;
            }
            if let Some(fetched) = self.take_buffer() {
                return Next::Records(fetched);
            }
            let data = self.wakes.data.notified();
            tokio::pin!(data);
            data.as_mut().enable();
            if self.has_buffer() || self.shared.lock().unwrap().refresh {
                continue;
            }
            let until = self.next_commit.min(self.next_metadata);
            tokio::select! {
                _ = data => {}
                _ = self.signal_rx.changed() => {}
                _ = tokio::time::sleep_until(until) => {}
            }
        }
    }

    fn has_buffer(&self) -> bool {
        self.shared.lock().unwrap().parts.values().any(|p| p.buffer.is_some())
    }

    /// Takes one partition's buffered records, partitions in turn, and wakes its loop.
    fn take_buffer(&mut self) -> Option<Fetched> {
        let mut state = self.shared.lock().unwrap();
        let n = state.parts.len();
        if n == 0 {
            return None;
        }
        let keys: Vec<TopicPartition> = state.parts.keys().cloned().collect();
        for i in 0..n {
            let tp = &keys[(self.turn + i) % n];
            if let Some(fetched) = state.parts.get_mut(tp).and_then(|p| p.buffer.take()) {
                self.turn = (self.turn + i + 1) % n;
                drop(state);
                let bytes: usize =
                    fetched.records.iter().map(|r| r.value.as_ref().map_or(0, Bytes::len)).sum();
                self.counters.count("logit.input.records", fetched.records.len() as f64, &[]);
                self.counters.count("logit.input.record.bytes", bytes as f64, &[]);
                self.wakes.loops.notify_waiters();
                return Some(fetched);
            }
        }
        None
    }

    /// Marks `range` of `tp` accepted (or otherwise done): the commit candidate moves across it
    /// once every offset before it is done too. `epoch` is the [`Fetched::epoch`] the range came
    /// from: a range from before the partition's last reset is cut at the offset the reset moved
    /// to, since the offsets above it may now name records never read, and a range from an
    /// earlier assignment is ignored.
    pub fn mark_accepted(&mut self, tp: &TopicPartition, epoch: u64, range: Range<i64>) {
        if let Some(part) = self.shared.lock().unwrap().parts.get_mut(tp) {
            part.accept(epoch, range);
        }
    }

    /// A partition's commit candidate, for tests and diagnostics.
    pub fn candidate(&self, tp: &TopicPartition) -> Option<i64> {
        self.shared.lock().unwrap().parts.get(tp).map(|p| p.tracker.candidate())
    }

    /// Revoke step 1: stops every fetch loop and discards buffered records; the next owner
    /// reads them. The heartbeat keeps running until the rejoin.
    pub fn stop_fetching(&mut self) {
        for (_, task) in self.loops.drain() {
            task.abort();
        }
        let mut state = self.shared.lock().unwrap();
        state.epoch += 1;
        for part in state.parts.values_mut() {
            part.buffer = None;
        }
    }

    /// Revoke step 3: a synchronous commit of every partition whose candidate moved, retried
    /// through a moved coordinator or a failed connection for up to the timeout. Skipped while
    /// not a member or once the membership is lost.
    pub async fn commit_all(&mut self) -> CommitOutcome {
        let pending = *self.signal_rx.borrow();
        let lost =
            self.revoking.or(pending).is_some_and(|r| r.reason == RevokeReason::MembershipLost);
        if self.member.is_none() || lost {
            return CommitOutcome::Skipped;
        }
        let deadline = Instant::now() + self.settings.timeout;
        let mut backoff = Backoff::new();
        loop {
            match self.commit().await {
                CommitOutcome::Failed if Instant::now() < deadline => {
                    tokio::time::sleep_until((Instant::now() + backoff.step()).min(deadline)).await;
                }
                outcome => return outcome,
            }
        }
    }

    /// One commit of every pending candidate.
    async fn commit(&mut self) -> CommitOutcome {
        let Some(member) = self.member.clone() else { return CommitOutcome::Skipped };
        let offsets: Vec<(TopicPartition, i64)> = {
            let state = self.shared.lock().unwrap();
            state
                .parts
                .iter()
                // A partition whose start ListOffsets hasn't resolved has no position to commit.
                .filter(|(_, p)| !matches!(p.position, Position::Start(_)) && p.tracker.pending())
                .map(|(tp, p)| (tp.clone(), p.tracker.candidate()))
                .collect()
        };
        if offsets.is_empty() {
            return CommitOutcome::Nothing;
        }
        if !self.locate().await {
            self.counters.count("logit.input.commits", 1.0, &[("class", "error")]);
            return CommitOutcome::Failed;
        }
        let answer = offsets::commit(
            &mut self.coordinator,
            &self.pool,
            &self.settings.group,
            member.generation,
            &member.member_id,
            &offsets,
        )
        .await;
        let codes = match answer {
            Ok(codes) => codes,
            Err(failure) => {
                group::count(&self.pool, ApiKey::OffsetCommit, &failure);
                if matches!(failure, Failure::Call(_)) {
                    self.located = false;
                }
                self.counters.count("logit.input.commits", 1.0, &[("class", "error")]);
                return CommitOutcome::Failed;
            }
        };
        let mut outcome = CommitOutcome::Committed;
        let mut state = self.shared.lock().unwrap();
        for (tp, error) in codes {
            if error == code::NONE {
                if let Some((_, offset)) = offsets.iter().find(|(t, _)| *t == tp) {
                    if let Some(part) = state.parts.get_mut(&tp) {
                        part.tracker.committed(*offset);
                    }
                }
                continue;
            }
            self.counters.broker_code(ApiKey::OffsetCommit, error);
            match group_code(error) {
                GroupCode::Fenced { reset_member } => {
                    outcome = CommitOutcome::Fenced;
                    raise(
                        &self.signal,
                        Revoke { reason: RevokeReason::MembershipLost, reset_member },
                    );
                }
                GroupCode::Rebalance => {
                    raise(&self.signal, Revoke::new(RevokeReason::Rebalance));
                    if outcome == CommitOutcome::Committed {
                        outcome = CommitOutcome::Failed;
                    }
                }
                GroupCode::Rediscover => {
                    self.located = false;
                    if outcome == CommitOutcome::Committed {
                        outcome = CommitOutcome::Failed;
                    }
                }
                GroupCode::BackOff(diag) => {
                    if let Some(key) = diag {
                        self.counters.warn(
                            key,
                            format_args!(
                                "Kafka {tp}: OffsetCommit answered {}",
                                crate::error::name(error)
                            ),
                        );
                    }
                    if outcome == CommitOutcome::Committed {
                        outcome = CommitOutcome::Failed;
                    }
                }
                GroupCode::Ok => {}
            }
        }
        drop(state);
        let class = match outcome {
            CommitOutcome::Committed => "ok",
            CommitOutcome::Fenced => "fenced",
            _ => "error",
        };
        self.counters.count("logit.input.commits", 1.0, &[("class", class)]);
        outcome
    }

    /// Finds the coordinator when the link doesn't point at one; `false` when it can't.
    async fn locate(&mut self) -> bool {
        if self.located {
            return true;
        }
        match group::find_coordinator(&mut self.control, &self.pool, &self.settings.group).await {
            Ok(target) => {
                self.coordinator.retarget(target);
                self.located = true;
                true
            }
            Err(failure) => {
                group::count(&self.pool, ApiKey::FindCoordinator, &failure);
                if let Failure::Code(c) = failure {
                    if let GroupCode::BackOff(Some(key)) = group_code(c) {
                        self.counters.warn(
                            key,
                            format_args!(
                                "Kafka group {:?}: FindCoordinator answered {}",
                                self.settings.group,
                                crate::error::name(c)
                            ),
                        );
                    }
                }
                false
            }
        }
    }

    /// Revoke step 4: joins the group, retrying with a backoff until it's a member with an
    /// assignment whose positions are known or pending a ListOffsets.
    pub async fn rejoin(&mut self) {
        self.stop_fetching();
        self.heartbeat = None;
        let pending = *self.signal_rx.borrow();
        let revoke = self.revoking.take().or(pending);
        if let Some(revoke) = revoke {
            self.join_reason = revoke.reason.tag();
            if revoke.reset_member {
                self.member_id.clear();
            }
        }
        self.signal.send_replace(None);
        self.member = None;
        self.assignment.clear();
        self.shared.lock().unwrap().parts.clear();
        let mut backoff = Backoff::new();
        loop {
            match self.try_join().await {
                Ok(()) => break,
                Err(Retry::Now) => {}
                Err(Retry::Later) => backoff.wait().await,
            }
        }
        self.counters.count("logit.input.rebalances", 1.0, &[("reason", self.join_reason)]);
        self.join_reason = "rebalance";
        self.next_commit = Instant::now() + self.settings.commit_interval;
    }

    /// One join, sync, and offset fetch.
    async fn try_join(&mut self) -> Result<(), Retry> {
        if !self.locate().await {
            return Err(Retry::Later);
        }
        let cluster =
            self.fetch_metadata(&self.settings.topics.clone()).await.ok_or(Retry::Later)?;
        let request = JoinRequest {
            group: &self.settings.group,
            member_id: self.member_id.clone(),
            session_timeout_ms: millis(self.settings.session_timeout),
            rebalance_timeout_ms: millis(self.settings.rebalance_timeout),
            protocol: self.settings.assignment.name(),
            metadata: encode_subscription(&self.settings.topics),
            wait: self.settings.rebalance_timeout + self.settings.timeout,
        };
        let joined = match group::join(&mut self.coordinator, &self.pool, request).await {
            Ok(joined) => joined,
            Err((failure, member_id)) => {
                self.member_id = member_id;
                return Err(self.group_failure(ApiKey::JoinGroup, failure));
            }
        };
        self.member_id = joined.member_id.clone();
        let assignments = if joined.leader == joined.member_id {
            self.assign(&joined, &cluster).await.ok_or(Retry::Later)?
        } else {
            Vec::new()
        };
        let bytes = match group::sync(
            &mut self.coordinator,
            &self.pool,
            &self.settings.group,
            &joined,
            assignments,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(failure) => return Err(self.group_failure(ApiKey::SyncGroup, failure)),
        };
        let assigned = match decode_assignment(bytes) {
            Ok(assigned) => assigned,
            Err(err) => {
                self.counters.warn(
                    "group_refused",
                    format_args!(
                        "Kafka group {:?}: an assignment that doesn't decode: {err:#}",
                        self.settings.group
                    ),
                );
                return Err(Retry::Later);
            }
        };
        let assignment: Vec<TopicPartition> =
            assigned.iter().map(|(t, p)| TopicPartition::new(t, *p)).collect();
        self.member =
            Some(Member { member_id: joined.member_id.clone(), generation: joined.generation });
        self.heartbeat = Some(heartbeat::spawn(
            self.pool.clone(),
            heartbeat::Member {
                group: self.settings.group.clone(),
                member_id: joined.member_id.clone(),
                generation: joined.generation,
                coordinator: self.coordinator.target.clone(),
            },
            self.settings.heartbeat_interval,
            self.signal.clone(),
        ));
        let committed = self.fetch_committed(&assignment).await?;
        self.install(cluster, assignment, committed);
        Ok(())
    }

    /// The committed offsets of `assignment`, retried with a backoff while the membership holds,
    /// as Java's `ConsumerCoordinator.fetchCommittedOffsets` does: a leader's JoinGroup would
    /// rebalance the whole group, so only a revoke, or a fenced or rebalance code, joins again.
    async fn fetch_committed(
        &mut self,
        assignment: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, Option<i64>>, Retry> {
        if assignment.is_empty() {
            return Ok(HashMap::new());
        }
        let mut backoff = Backoff::new();
        loop {
            let pending = *self.signal_rx.borrow();
            if let Some(revoke) = pending {
                self.abandon(revoke);
                return Err(Retry::Now);
            }
            if self.locate().await {
                let answer = offsets::fetch_committed(
                    &mut self.coordinator,
                    &self.pool,
                    &self.settings.group,
                    assignment,
                )
                .await;
                match answer {
                    Ok(committed) => return Ok(committed),
                    Err(failure) => {
                        group::count(&self.pool, ApiKey::OffsetFetch, &failure);
                        match failure {
                            Failure::Call(_) => self.located = false,
                            Failure::Code(c) => match group_code(c) {
                                GroupCode::Fenced { reset_member } => {
                                    let reason = RevokeReason::MembershipLost;
                                    self.abandon(Revoke { reason, reset_member });
                                    return Err(Retry::Now);
                                }
                                GroupCode::Rebalance => {
                                    self.abandon(Revoke::new(RevokeReason::Rebalance));
                                    return Err(Retry::Now);
                                }
                                GroupCode::Rediscover => self.located = false,
                                GroupCode::BackOff(Some(key)) => self.counters.warn(
                                    key,
                                    format_args!(
                                        "Kafka group {:?}: OffsetFetch answered {}",
                                        self.settings.group,
                                        crate::error::name(c)
                                    ),
                                ),
                                GroupCode::BackOff(None) | GroupCode::Ok => {}
                            },
                        }
                    }
                }
            }
            backoff.wait().await;
        }
    }

    /// Drops a membership the join hadn't finished installing, for `revoke`'s rejoin.
    fn abandon(&mut self, revoke: Revoke) {
        self.heartbeat = None;
        self.member = None;
        if revoke.reset_member {
            self.member_id.clear();
        }
        self.join_reason = revoke.reason.tag();
        self.signal.send_replace(None);
    }

    /// Counts and classifies a group request's failure: what the join loop does next.
    fn group_failure(&mut self, api: ApiKey, failure: Failure) -> Retry {
        group::count(&self.pool, api, &failure);
        match failure {
            Failure::Call(_) => {
                self.located = false;
                Retry::Later
            }
            Failure::Code(c) => match group_code(c) {
                GroupCode::Ok => Retry::Now,
                GroupCode::Rediscover => {
                    self.located = false;
                    Retry::Later
                }
                GroupCode::Rebalance => Retry::Now,
                GroupCode::Fenced { reset_member } => {
                    if reset_member {
                        self.member_id.clear();
                    }
                    Retry::Now
                }
                GroupCode::BackOff(diag) => {
                    if let Some(key) = diag {
                        self.counters.warn(
                            key,
                            format_args!(
                                "Kafka group {:?}: {api:?} answered {}",
                                self.settings.group,
                                crate::error::name(c)
                            ),
                        );
                    }
                    Retry::Later
                }
            },
        }
    }

    /// The leader's assignment: every member's subscription decoded, partition counts for every
    /// topic any member names, the assignor run, each member's share encoded.
    async fn assign(&mut self, joined: &Joined, own: &Cluster) -> Option<Vec<(String, Bytes)>> {
        let mut members = BTreeMap::new();
        for (id, bytes) in &joined.members {
            match decode_subscription(bytes.clone()) {
                Ok(topics) => {
                    members.insert(id.clone(), topics);
                }
                Err(err) => {
                    self.counters.warn(
                        "group_refused",
                        format_args!(
                            "Kafka group {:?}: a member subscription that doesn't decode: {err:#}",
                            self.settings.group
                        ),
                    );
                    members.insert(id.clone(), Vec::new());
                }
            }
        }
        let all: BTreeSet<String> = members.values().flatten().cloned().collect();
        let mut counts = own.partition_counts();
        let others: Vec<String> = all.into_iter().filter(|t| !own.topics.contains_key(t)).collect();
        if !others.is_empty() {
            let cluster = self.fetch_metadata(&others).await?;
            counts.extend(cluster.partition_counts());
        }
        let assignor = match joined.protocol.as_str() {
            "roundrobin" => super::Assignor::RoundRobin,
            _ => super::Assignor::Range,
        };
        Some(
            assignor
                .assign(&members, &counts)
                .into_iter()
                .map(|(member, partitions)| (member, encode_assignment(&partitions)))
                .collect(),
        )
    }

    /// Installs `assignment` starting at `committed`, for the member `try_join` made.
    fn install(
        &mut self,
        cluster: Cluster,
        assignment: Vec<TopicPartition>,
        committed: HashMap<TopicPartition, Option<i64>>,
    ) {
        let mut no_commit = 0;
        {
            let mut state = self.shared.lock().unwrap();
            state.epoch += 1;
            state.parts.clear();
            for tp in &assignment {
                let part = match committed.get(tp).copied().flatten() {
                    Some(offset) => {
                        Part::new(Position::At(offset), Tracker::new(offset, Some(offset)))
                    }
                    None => {
                        no_commit += 1;
                        Part::new(
                            Position::Start(self.settings.start.timestamp()),
                            Tracker::new(0, None),
                        )
                    }
                };
                state.parts.insert(tp.clone(), part);
            }
        }
        if no_commit > 0 {
            self.counters.count(
                "logit.input.offsets.reset",
                no_commit as f64,
                &[("reason", "no_commit")],
            );
        }
        self.counters.telemetry.gauge("logit.input.partitions", assignment.len() as f64, &[]);
        self.snapshot = self.subscribed_counts(&cluster);
        self.assignment = assignment;
        self.apply_cluster(cluster);
    }

    /// The partition counts of the subscribed topics that exist.
    fn subscribed_counts(&self, cluster: &Cluster) -> BTreeMap<String, i32> {
        cluster
            .partition_counts()
            .into_iter()
            .filter(|(t, _)| self.settings.topics.contains(t))
            .collect()
    }

    /// Metadata for `topics` through the control link; `None` after a counted failure.
    async fn fetch_metadata(&mut self, topics: &[String]) -> Option<Cluster> {
        metadata::fetch(&mut self.control, &self.pool, topics).await.ok()
    }

    /// Refreshes metadata: new leaders for the fetch loops, and a revoke when a subscribed
    /// topic appeared or changed its partition count.
    async fn refresh(&mut self) {
        let topics = self.settings.topics.clone();
        let Some(cluster) = self.fetch_metadata(&topics).await else {
            self.shared.lock().unwrap().refresh = false;
            self.next_metadata = Instant::now() + Backoff::MIN.max(self.settings.timeout / 10);
            return;
        };
        self.next_metadata = Instant::now() + self.settings.metadata_max_age;
        if self.member.is_some() && self.subscribed_counts(&cluster) != self.snapshot {
            raise(&self.signal, Revoke::new(RevokeReason::Metadata));
        }
        self.apply_cluster(cluster);
    }

    /// Points each assigned partition at its leader and runs one loop per leader.
    fn apply_cluster(&mut self, cluster: Cluster) {
        let mut leaders = BTreeSet::new();
        let mut leaderless = false;
        {
            let mut state = self.shared.lock().unwrap();
            for (tp, part) in state.parts.iter_mut() {
                part.leader = cluster.leader(&tp.topic, tp.partition);
                match part.leader {
                    Some(l) => {
                        leaders.insert(l);
                    }
                    None => leaderless = true,
                }
            }
        }
        if leaderless {
            self.next_metadata = self.next_metadata.min(Instant::now() + Backoff::MAX);
        }
        self.loops.retain(|node, task| {
            let keep = leaders.contains(node) && !task.is_finished();
            if !keep {
                task.abort();
            }
            keep
        });
        let settings = LoopSettings {
            bounds: self.settings.fetch,
            isolation: self.settings.isolation,
            max_batch_bytes: self.settings.max_batch_bytes,
        };
        for node in leaders {
            self.loops.entry(node).or_insert_with(|| {
                tokio::spawn(fetch::run(
                    node,
                    self.pool.clone(),
                    self.shared.clone(),
                    self.wakes.clone(),
                    settings.clone(),
                ))
            });
        }
        self.wakes.loops.notify_waiters();
        self.cluster = Some(cluster);
    }

    /// Shutdown: stops fetching, commits, and leaves the group so it rebalances at once.
    pub async fn leave(&mut self) {
        self.stop_fetching();
        if self.member.is_some() {
            self.commit_all().await;
            self.heartbeat = None;
            let member_id = self.member.as_ref().map(|m| m.member_id.clone()).unwrap_or_default();
            if let Err(failure) =
                group::leave(&mut self.coordinator, &self.pool, &self.settings.group, &member_id)
                    .await
            {
                group::count(&self.pool, ApiKey::LeaveGroup, &failure);
            }
        }
        self.heartbeat = None;
        self.member = None;
        self.assignment.clear();
        self.shared.lock().unwrap().parts.clear();
    }
}

/// What the join loop does after a failed attempt.
enum Retry {
    Now,
    Later,
}

fn millis(d: std::time::Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX)
}
