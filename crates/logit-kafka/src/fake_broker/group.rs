//! The group coordinator: Kafka's classic `GroupCoordinator` (`ClassicGroup` in 4.x), eager,
//! for one node, [`State::coordinator`]. Any other node answers a group request
//! `NOT_COORDINATOR`.
//!
//! - **Join.** From v4 an empty member id is answered `MEMBER_ID_REQUIRED` with a new id,
//!   `{client_id}-{n}`, pending until the member joins under it; below v4 it joins at once. A
//!   join moves the group to `PreparingRebalance` and is held until every member and no pending
//!   id has a join held, or until the longest member `rebalance_timeout` since the rebalance
//!   began, when members with no join held are removed. Then the generation increments, the
//!   protocol is chosen by vote, the leader stays if it rejoined (else the member whose join
//!   arrived first), and every held join is answered, the leader's with every member's metadata.
//!   A known member rejoining a `Stable` group as a follower, or a `CompletingRebalance` group,
//!   with unchanged protocols gets the current generation back at once, as Kafka answers it.
//! - **Sync.** In `CompletingRebalance` a follower's SyncGroup is held until the leader's
//!   arrives with the assignments; then the group is `Stable` and every held one is answered.
//!   A rebalance starting meanwhile answers a held SyncGroup `REBALANCE_IN_PROGRESS`.
//! - **Sessions.** A member not heard from (a heartbeat, a join, a sync, or a commit) within its
//!   session timeout is removed and the group rebalances, unless a join or sync of its is held.
//!   [`coordinator_task`] expires members, pending ids, and rebalances on the tokio clock, so a
//!   test under `start_paused` drives it.
//! - **Commits.** As `ClassicGroup.validateOffsetCommit`: generation -1 is accepted on an
//!   `Empty` group or an unknown one, which it creates; otherwise the member must be known (else
//!   `UNKNOWN_MEMBER_ID`, which a generation -1 commit to a group with members also gets) and
//!   at the group's generation (else `ILLEGAL_GENERATION`), and a `CompletingRebalance` group
//!   answers `REBALANCE_IN_PROGRESS`. A commit with a generation to an unknown group is
//!   `ILLEGAL_GENERATION`, `GROUP_ID_NOT_FOUND` from v9.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{
    BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, JoinGroupRequest,
    JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, SyncGroupRequest,
    SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{State, Taken};
use crate::error::code;

/// `GROUP_ID_NOT_FOUND`: OffsetCommit v9's answer for a generation sent to an unknown group.
const GROUP_ID_NOT_FOUND: i16 = 69;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GroupState {
    #[default]
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

/// One consumer group.
#[derive(Debug, Default)]
pub struct Group {
    pub state: GroupState,
    pub generation: i32,
    /// The protocol type the first member joined with: `consumer` for a consumer group.
    pub protocol_type: Option<String>,
    /// The protocol the last completed rebalance chose.
    pub protocol: Option<String>,
    pub leader: Option<String>,
    pub members: BTreeMap<String, Member>,
    /// Ids answered `MEMBER_ID_REQUIRED` and not yet joined, each with when it's forgotten.
    pub pending: BTreeMap<String, Instant>,
    /// The committed offset per `(topic, partition)`.
    pub committed: BTreeMap<(String, i32), i64>,
    rebalance_began: Option<Instant>,
}

/// One group member.
#[derive(Debug)]
pub struct Member {
    pub client_id: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    /// `(name, metadata)` per protocol, in the member's order of preference.
    pub protocols: Vec<(String, Bytes)>,
    /// What the leader's SyncGroup assigned it this generation.
    pub assignment: Option<Bytes>,
    /// When the member was last heard from; its session ends `session_timeout` after.
    pub last_seen: Instant,
    join: Option<HeldJoin>,
    sync: Option<oneshot::Sender<SyncResult>>,
}

#[derive(Debug)]
struct HeldJoin {
    answer: oneshot::Sender<JoinResult>,
    /// Arrival order across the cluster, for choosing a leader.
    order: u64,
    /// Where the join's record is in [`State::joins`].
    seen: usize,
}

/// One `OffsetCommit` request as it arrived and was answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSeen {
    pub node: i32,
    pub version: i16,
    pub group: String,
    pub generation: i32,
    pub member_id: String,
    /// `(topic, partition, offset)`, in request order.
    pub offsets: Vec<(String, i32, i64)>,
    /// The code answered for each entry of `offsets`.
    pub answered: Vec<i16>,
}

/// One `JoinGroup` request as it arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSeen {
    pub node: i32,
    pub version: i16,
    pub group: String,
    /// The member id as sent, empty on a first join.
    pub member_id: String,
    /// Protocol names in the member's order.
    pub protocols: Vec<String>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    /// The code answered, `None` while the answer is held.
    pub answered: Option<i16>,
}

/// An answer now, or one the coordinator sends when the group moves.
pub(super) enum Held<T> {
    Now(T),
    Later(oneshot::Receiver<T>),
}

/// A JoinGroup answer before it's encoded at the request's version.
#[derive(Debug)]
pub(super) struct JoinResult {
    error: i16,
    generation: i32,
    protocol_type: Option<String>,
    protocol: Option<String>,
    leader: String,
    member_id: String,
    members: Vec<(String, Bytes)>,
}

impl JoinResult {
    fn error(error: i16, member_id: &str) -> Self {
        Self {
            error,
            generation: -1,
            protocol_type: None,
            protocol: None,
            leader: String::new(),
            member_id: member_id.to_string(),
            members: Vec::new(),
        }
    }

    pub(super) fn response(self, version: i16) -> JoinGroupResponse {
        // `protocol_name` is nullable from v7 only, and `protocol_type` appears at v7.
        let name = match self.protocol {
            Some(p) => Some(StrBytes::from_string(p)),
            None if version >= 7 => None,
            None => Some(StrBytes::default()),
        };
        let protocol_type = self.protocol_type.filter(|_| version >= 7).map(StrBytes::from_string);
        JoinGroupResponse::default()
            .with_error_code(self.error)
            .with_generation_id(self.generation)
            .with_protocol_type(protocol_type)
            .with_protocol_name(name)
            .with_leader(StrBytes::from_string(self.leader))
            .with_member_id(StrBytes::from_string(self.member_id))
            .with_members(
                self.members
                    .into_iter()
                    .map(|(id, metadata)| {
                        JoinGroupResponseMember::default()
                            .with_member_id(StrBytes::from_string(id))
                            .with_metadata(metadata)
                    })
                    .collect(),
            )
    }
}

/// A SyncGroup answer before it's encoded at the request's version.
#[derive(Debug)]
pub(super) struct SyncResult {
    error: i16,
    protocol_type: Option<String>,
    protocol: Option<String>,
    assignment: Bytes,
}

impl SyncResult {
    fn error(error: i16) -> Self {
        Self { error, protocol_type: None, protocol: None, assignment: Bytes::new() }
    }

    pub(super) fn response(self, version: i16) -> SyncGroupResponse {
        let v5 = |s: Option<String>| s.filter(|_| version >= 5).map(StrBytes::from_string);
        SyncGroupResponse::default()
            .with_error_code(self.error)
            .with_protocol_type(v5(self.protocol_type))
            .with_protocol_name(v5(self.protocol))
            .with_assignment(self.assignment)
    }
}

impl Group {
    fn rebalance_deadline(&self) -> Option<Instant> {
        let began = self.rebalance_began?;
        let longest = self.members.values().map(|m| m.rebalance_timeout).max().unwrap_or_default();
        Some(began + longest)
    }

    /// Whether every member supports one of `protocols`, as `ClassicGroup.supportsProtocols`.
    fn supports(&self, protocols: &[(String, Bytes)]) -> bool {
        protocols.iter().any(|(name, _)| {
            self.members.values().all(|m| m.protocols.iter().any(|(n, _)| n == name))
        })
    }

    /// The current generation's answer to `member_id`: the leader's carries every member.
    fn current(&self, member_id: &str) -> JoinResult {
        let protocol = self.protocol.clone();
        let leader = self.leader.clone().unwrap_or_default();
        let members = if leader == member_id {
            self.members
                .iter()
                .map(|(id, m)| (id.clone(), metadata_for(m, protocol.as_deref())))
                .collect()
        } else {
            Vec::new()
        };
        JoinResult {
            error: code::NONE,
            generation: self.generation,
            protocol_type: self.protocol_type.clone(),
            protocol,
            leader,
            member_id: member_id.to_string(),
            members,
        }
    }

    /// Starts a rebalance, failing every held SyncGroup.
    fn prepare_rebalance(&mut self, now: Instant) {
        if self.state == GroupState::CompletingRebalance {
            for member in self.members.values_mut() {
                member.assignment = None;
                if let Some(sync) = member.sync.take() {
                    let _ = sync.send(SyncResult::error(code::REBALANCE_IN_PROGRESS));
                }
            }
        }
        self.state = GroupState::PreparingRebalance;
        self.rebalance_began = Some(now);
    }

    /// Completes the rebalance once every member and no pending id has a join held, or at the
    /// rebalance deadline, removing the members that didn't rejoin.
    fn try_complete_join(&mut self, joins: &mut [JoinSeen], now: Instant) {
        if self.state != GroupState::PreparingRebalance {
            return;
        }
        let all_joined = self.members.values().all(|m| m.join.is_some()) && self.pending.is_empty();
        let expired = self.rebalance_deadline().is_some_and(|deadline| now >= deadline);
        if !all_joined && !expired {
            return;
        }
        self.members.retain(|_, m| m.join.is_some());
        self.generation += 1;
        self.rebalance_began = None;
        if self.members.is_empty() {
            self.state = GroupState::Empty;
            self.protocol = None;
            self.protocol_type = None;
            self.leader = None;
            return;
        }
        self.protocol = Some(self.select_protocol());
        if self.leader.as_ref().is_none_or(|l| !self.members.contains_key(l)) {
            self.leader = self
                .members
                .iter()
                .min_by_key(|(_, m)| m.join.as_ref().map(|j| j.order))
                .map(|(id, _)| id.clone());
        }
        self.state = GroupState::CompletingRebalance;
        let ids: Vec<String> = self.members.keys().cloned().collect();
        for id in ids {
            let result = self.current(&id);
            let member = self.members.get_mut(&id).expect("a member listed above");
            member.last_seen = now;
            member.assignment = None;
            let held = member.join.take().expect("every remaining member has a join held");
            joins[held.seen].answered = Some(code::NONE);
            let _ = held.answer.send(result);
        }
    }

    /// The protocol every member supports that the most members list first among those; a tie
    /// goes to the one the leader lists first.
    fn select_protocol(&self) -> String {
        let candidates: Vec<&str> = self
            .members
            .values()
            .next()
            .map(|m| m.protocols.iter().map(|(n, _)| n.as_str()).collect::<Vec<&str>>())
            .unwrap_or_default()
            .into_iter()
            .filter(|name| {
                self.members.values().all(|m| m.protocols.iter().any(|(n, _)| n == name))
            })
            .collect();
        let mut votes: BTreeMap<&str, usize> = BTreeMap::new();
        for member in self.members.values() {
            if let Some((name, _)) =
                member.protocols.iter().find(|(n, _)| candidates.contains(&n.as_str()))
            {
                *votes.entry(name.as_str()).or_default() += 1;
            }
        }
        let most = votes.values().copied().max().unwrap_or(0);
        let leader = self
            .leader
            .as_ref()
            .and_then(|l| self.members.get(l))
            .or_else(|| self.members.values().min_by_key(|m| m.join.as_ref().map(|j| j.order)));
        let order: Vec<&str> = leader
            .map(|m| m.protocols.iter().map(|(n, _)| n.as_str()).collect())
            .unwrap_or_default();
        order
            .into_iter()
            .find(|name| votes.get(name) == Some(&most))
            .or_else(|| candidates.first().copied())
            .unwrap_or_default()
            .to_string()
    }

    /// Removes `id`, answering anything of its held `UNKNOWN_MEMBER_ID`, and rebalances what's
    /// left, as `ClassicGroup` does on a leave or an expiry.
    fn remove_member(&mut self, id: &str, joins: &mut [JoinSeen], now: Instant) {
        let Some(member) = self.members.remove(id) else { return };
        if let Some(held) = member.join {
            joins[held.seen].answered = Some(code::UNKNOWN_MEMBER_ID);
            let _ = held.answer.send(JoinResult::error(code::UNKNOWN_MEMBER_ID, id));
        }
        if let Some(sync) = member.sync {
            let _ = sync.send(SyncResult::error(code::UNKNOWN_MEMBER_ID));
        }
        match self.state {
            GroupState::Stable | GroupState::CompletingRebalance => {
                self.prepare_rebalance(now);
                self.try_complete_join(joins, now);
            }
            GroupState::PreparingRebalance => self.try_complete_join(joins, now),
            GroupState::Empty => {}
        }
    }

    /// Expires sessions, pending ids, and an overdue rebalance; returns the next deadline.
    fn expire(&mut self, joins: &mut [JoinSeen], now: Instant) -> Option<Instant> {
        let had_pending = self.pending.len();
        self.pending.retain(|_, until| *until > now);
        if self.pending.len() != had_pending {
            self.try_complete_join(joins, now);
        }
        let silent: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| m.join.is_none() && m.sync.is_none())
            .filter(|(_, m)| m.last_seen + m.session_timeout <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in silent {
            self.remove_member(&id, joins, now);
        }
        self.try_complete_join(joins, now);
        let sessions = self
            .members
            .values()
            .filter(|m| m.join.is_none() && m.sync.is_none())
            .map(|m| m.last_seen + m.session_timeout);
        let rebalance =
            self.rebalance_deadline().filter(|_| self.state == GroupState::PreparingRebalance);
        sessions.chain(self.pending.values().copied()).chain(rebalance).min()
    }
}

/// The member's metadata for `protocol`, empty when it has none under that name.
fn metadata_for(member: &Member, protocol: Option<&str>) -> Bytes {
    member
        .protocols
        .iter()
        .find(|(n, _)| Some(n.as_str()) == protocol)
        .map(|(_, m)| m.clone())
        .unwrap_or_default()
}

/// Expires group sessions and rebalances on the tokio clock for the life of the cluster: wakes
/// at the next deadline any group has, or when a group request may have moved one.
pub(super) async fn coordinator_task(shared: Arc<Mutex<State>>) {
    let wake = shared.lock().unwrap().group_wake.clone();
    loop {
        let next = {
            let mut state = shared.lock().unwrap();
            let now = Instant::now();
            let State { groups, joins, .. } = &mut *state;
            groups.values_mut().filter_map(|g| g.expire(joins, now)).min()
        };
        match next {
            Some(at) => {
                tokio::select! {
                    () = tokio::time::sleep_until(at) => {}
                    () = wake.notified() => {}
                }
            }
            None => wake.notified().await,
        }
    }
}

/// Answers one `FindCoordinator`: one key to v3, every key in `coordinator_keys` from v4.
pub(super) fn find_coordinator(
    state: &State,
    version: i16,
    request: &FindCoordinatorRequest,
    taken: &Option<Taken>,
) -> FindCoordinatorResponse {
    let error = Taken::whole_error(taken).unwrap_or(code::NONE);
    let (node, host, port) = if error == code::NONE {
        let port = state.ports.get(state.coordinator as usize).copied().map_or(-1, i32::from);
        (state.coordinator, StrBytes::from_string(state.advertised_host.clone()), port)
    } else {
        (-1, StrBytes::default(), -1)
    };
    let response = FindCoordinatorResponse::default().with_throttle_time_ms(Taken::throttle(taken));
    if version < 4 {
        return response
            .with_error_code(error)
            .with_node_id(BrokerId(node))
            .with_host(host)
            .with_port(port);
    }
    response.with_coordinators(
        request
            .coordinator_keys
            .iter()
            .map(|key| {
                Coordinator::default()
                    .with_key(key.clone())
                    .with_node_id(BrokerId(node))
                    .with_host(host.clone())
                    .with_port(port)
                    .with_error_code(error)
            })
            .collect(),
    )
}

/// The `(topic, partition)` pairs an `OffsetFetch` names, for step matching.
pub(super) fn offset_fetch_asked(version: i16, request: &OffsetFetchRequest) -> Vec<(String, i32)> {
    if version >= 8 {
        request
            .groups
            .iter()
            .flat_map(|g| g.topics.iter().flatten())
            .flat_map(|t| t.partition_indexes.iter().map(|&p| (t.name.0.to_string(), p)))
            .collect()
    } else {
        request
            .topics
            .iter()
            .flatten()
            .flat_map(|t| t.partition_indexes.iter().map(|&p| (t.name.0.to_string(), p)))
            .collect()
    }
}

/// The `(topic, partitions)` one OffsetFetch group asks about, or every committed partition when
/// it asks about all.
fn asked_or_committed(
    group: Option<&Group>,
    asked: Option<Vec<(String, Vec<i32>)>>,
) -> Vec<(String, Vec<i32>)> {
    asked.unwrap_or_else(|| {
        let mut all: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for (topic, partition) in group.iter().flat_map(|g| g.committed.keys()) {
            all.entry(topic.clone()).or_default().push(*partition);
        }
        all.into_iter().collect()
    })
}

impl State {
    pub(super) fn join_group(
        &mut self,
        node: i32,
        version: i16,
        client_id: &str,
        request: &JoinGroupRequest,
        taken: &Option<Taken>,
    ) -> Held<JoinResult> {
        let group_id = request.group_id.0.to_string();
        let member_id = request.member_id.to_string();
        let protocols: Vec<(String, Bytes)> =
            request.protocols.iter().map(|p| (p.name.to_string(), p.metadata.clone())).collect();
        let session_ms = request.session_timeout_ms;
        // v0 has no rebalance timeout; the session timeout serves as one.
        let rebalance_ms = if version == 0 || request.rebalance_timeout_ms < 0 {
            session_ms
        } else {
            request.rebalance_timeout_ms
        };
        let seen = self.joins.len();
        self.joins.push(JoinSeen {
            node,
            version,
            group: group_id.clone(),
            member_id: member_id.clone(),
            protocols: protocols.iter().map(|(n, _)| n.clone()).collect(),
            session_timeout_ms: session_ms,
            rebalance_timeout_ms: rebalance_ms,
            answered: None,
        });
        let now = Instant::now();
        let session = Duration::from_millis(session_ms.max(0) as u64);
        let rebalance = Duration::from_millis(rebalance_ms.max(0) as u64);
        let (min, max) = self.group_session_bounds;
        let refused = if let Some(code) = Taken::whole_error(taken) {
            Some(code)
        } else if node != self.coordinator {
            Some(code::NOT_COORDINATOR)
        } else if session < min || session > max {
            Some(code::INVALID_SESSION_TIMEOUT)
        } else {
            None
        };
        if let Some(code) = refused {
            self.joins[seen].answered = Some(code);
            return Held::Now(JoinResult::error(code, &member_id));
        }
        let protocol_type = request.protocol_type.to_string();
        let order = self.next_join;
        self.next_join += 1;
        let State { groups, joins, next_member, .. } = self;
        let group = groups.entry(group_id).or_default();
        let consistent = !protocol_type.is_empty()
            && !protocols.is_empty()
            && (group.members.is_empty()
                || (group.protocol_type.as_deref() == Some(protocol_type.as_str())
                    && group.supports(&protocols)));
        let now_answer = |joins: &mut Vec<JoinSeen>, result: JoinResult| {
            joins[seen].answered = Some(result.error);
            Held::Now(result)
        };
        if !consistent {
            return now_answer(
                joins,
                JoinResult::error(code::INCONSISTENT_GROUP_PROTOCOL, &member_id),
            );
        }
        let new_member = |answer| Member {
            client_id: client_id.to_string(),
            session_timeout: session,
            rebalance_timeout: rebalance,
            protocols: protocols.clone(),
            assignment: None,
            last_seen: now,
            join: Some(HeldJoin { answer, order, seen }),
            sync: None,
        };
        let (tx, rx) = oneshot::channel();
        if member_id.is_empty() {
            let id = format!("{client_id}-{next_member}");
            *next_member += 1;
            if version >= 4 {
                group.pending.insert(id.clone(), now + session);
                return now_answer(joins, JoinResult::error(code::MEMBER_ID_REQUIRED, &id));
            }
            group.members.insert(id, new_member(tx));
        } else if group.pending.remove(&member_id).is_some() {
            group.members.insert(member_id, new_member(tx));
        } else {
            let Some(member) = group.members.get_mut(&member_id) else {
                return now_answer(joins, JoinResult::error(code::UNKNOWN_MEMBER_ID, &member_id));
            };
            let unchanged = member.protocols == protocols;
            let leader = group.leader.as_deref() == Some(member_id.as_str());
            match group.state {
                GroupState::CompletingRebalance if unchanged => {
                    return now_answer(joins, group.current(&member_id));
                }
                GroupState::Stable if unchanged && !leader => {
                    member.last_seen = now;
                    return now_answer(joins, group.current(&member_id));
                }
                _ => {
                    member.protocols = protocols.clone();
                    member.session_timeout = session;
                    member.rebalance_timeout = rebalance;
                    member.last_seen = now;
                    if let Some(old) = member.join.replace(HeldJoin { answer: tx, order, seen }) {
                        joins[old.seen].answered = Some(code::UNKNOWN_MEMBER_ID);
                    }
                }
            }
        }
        if group.protocol_type.is_none() {
            group.protocol_type = Some(protocol_type);
        }
        if group.state != GroupState::PreparingRebalance {
            group.prepare_rebalance(now);
        }
        group.try_complete_join(joins, now);
        Held::Later(rx)
    }

    pub(super) fn sync_group(
        &mut self,
        node: i32,
        version: i16,
        request: &SyncGroupRequest,
        taken: &Option<Taken>,
    ) -> Held<SyncResult> {
        if let Some(code) = Taken::whole_error(taken) {
            return Held::Now(SyncResult::error(code));
        }
        if node != self.coordinator {
            return Held::Now(SyncResult::error(code::NOT_COORDINATOR));
        }
        let member_id = request.member_id.to_string();
        let now = Instant::now();
        let Some(group) = self.groups.get_mut(&request.group_id.0.to_string()) else {
            return Held::Now(SyncResult::error(code::UNKNOWN_MEMBER_ID));
        };
        if !group.members.contains_key(&member_id) {
            return Held::Now(SyncResult::error(code::UNKNOWN_MEMBER_ID));
        }
        if request.generation_id != group.generation {
            return Held::Now(SyncResult::error(code::ILLEGAL_GENERATION));
        }
        let mismatch = |sent: &Option<StrBytes>, ours: &Option<String>| {
            version >= 5 && sent.as_ref().is_some_and(|s| Some(s.as_str()) != ours.as_deref())
        };
        if mismatch(&request.protocol_type, &group.protocol_type)
            || mismatch(&request.protocol_name, &group.protocol)
        {
            return Held::Now(SyncResult::error(code::INCONSISTENT_GROUP_PROTOCOL));
        }
        let result = |group: &Group, id: &str| SyncResult {
            error: code::NONE,
            protocol_type: group.protocol_type.clone(),
            protocol: group.protocol.clone(),
            assignment: group.members[id].assignment.clone().unwrap_or_default(),
        };
        group.members.get_mut(&member_id).expect("checked above").last_seen = now;
        match group.state {
            GroupState::Empty => Held::Now(SyncResult::error(code::UNKNOWN_MEMBER_ID)),
            GroupState::PreparingRebalance => {
                Held::Now(SyncResult::error(code::REBALANCE_IN_PROGRESS))
            }
            GroupState::Stable => Held::Now(result(group, &member_id)),
            GroupState::CompletingRebalance => {
                if group.leader.as_deref() != Some(member_id.as_str()) {
                    let (tx, rx) = oneshot::channel();
                    group.members.get_mut(&member_id).expect("checked above").sync = Some(tx);
                    return Held::Later(rx);
                }
                let assigned: BTreeMap<String, Bytes> = request
                    .assignments
                    .iter()
                    .map(|a| (a.member_id.to_string(), a.assignment.clone()))
                    .collect();
                for (id, member) in &mut group.members {
                    member.assignment = Some(assigned.get(id).cloned().unwrap_or_default());
                }
                group.state = GroupState::Stable;
                let waiting: Vec<String> = group
                    .members
                    .iter()
                    .filter(|(_, m)| m.sync.is_some())
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in waiting {
                    let answer = result(group, &id);
                    if let Some(sync) = group.members.get_mut(&id).and_then(|m| m.sync.take()) {
                        let _ = sync.send(answer);
                    }
                }
                Held::Now(result(group, &member_id))
            }
        }
    }

    pub(super) fn heartbeat(
        &mut self,
        node: i32,
        request: &HeartbeatRequest,
        taken: &Option<Taken>,
    ) -> i16 {
        if let Some(code) = Taken::whole_error(taken) {
            return code;
        }
        if node != self.coordinator {
            return code::NOT_COORDINATOR;
        }
        let Some(group) = self.groups.get_mut(&request.group_id.0.to_string()) else {
            return code::UNKNOWN_MEMBER_ID;
        };
        let generation = group.generation;
        let state = group.state;
        let Some(member) = group.members.get_mut(&request.member_id.to_string()) else {
            return code::UNKNOWN_MEMBER_ID;
        };
        if request.generation_id != generation {
            return code::ILLEGAL_GENERATION;
        }
        member.last_seen = Instant::now();
        match state {
            GroupState::PreparingRebalance => code::REBALANCE_IN_PROGRESS,
            GroupState::Empty => code::UNKNOWN_MEMBER_ID,
            GroupState::CompletingRebalance | GroupState::Stable => code::NONE,
        }
    }

    pub(super) fn leave_group(
        &mut self,
        node: i32,
        version: i16,
        request: &LeaveGroupRequest,
        taken: &Option<Taken>,
    ) -> LeaveGroupResponse {
        let response = LeaveGroupResponse::default().with_throttle_time_ms(Taken::throttle(taken));
        if let Some(code) = Taken::whole_error(taken) {
            return response.with_error_code(code);
        }
        if node != self.coordinator {
            return response.with_error_code(code::NOT_COORDINATOR);
        }
        let leaving: Vec<String> = if version >= 3 {
            request.members.iter().map(|m| m.member_id.to_string()).collect()
        } else {
            vec![request.member_id.to_string()]
        };
        let now = Instant::now();
        let State { groups, joins, .. } = self;
        let group = groups.get_mut(&request.group_id.0.to_string());
        let mut codes = Vec::new();
        match group {
            None => codes.extend(leaving.iter().map(|_| code::UNKNOWN_MEMBER_ID)),
            Some(group) => {
                for id in &leaving {
                    if group.members.contains_key(id) {
                        group.remove_member(id, joins, now);
                        codes.push(code::NONE);
                    } else {
                        codes.push(code::UNKNOWN_MEMBER_ID);
                    }
                }
            }
        }
        if version < 3 {
            return response.with_error_code(codes[0]);
        }
        response.with_members(
            leaving
                .into_iter()
                .zip(codes)
                .map(|(id, error)| {
                    MemberResponse::default()
                        .with_member_id(StrBytes::from_string(id))
                        .with_error_code(error)
                })
                .collect(),
        )
    }

    pub(super) fn offset_commit(
        &mut self,
        node: i32,
        version: i16,
        request: &OffsetCommitRequest,
        taken: &Option<Taken>,
    ) -> OffsetCommitResponse {
        let group_id = request.group_id.0.to_string();
        let generation = request.generation_id_or_member_epoch;
        let member_id = request.member_id.to_string();
        let now = Instant::now();
        let verdict = if node != self.coordinator {
            code::NOT_COORDINATOR
        } else {
            match self.groups.get_mut(&group_id) {
                None if generation < 0 && member_id.is_empty() => {
                    self.groups.insert(group_id.clone(), Group::default());
                    code::NONE
                }
                None if version >= 9 => GROUP_ID_NOT_FOUND,
                None => code::ILLEGAL_GENERATION,
                Some(group) if generation < 0 && group.state == GroupState::Empty => code::NONE,
                Some(group) if generation >= 0 || !member_id.is_empty() => {
                    match group.members.get_mut(&member_id) {
                        None => code::UNKNOWN_MEMBER_ID,
                        Some(_) if generation != group.generation => code::ILLEGAL_GENERATION,
                        Some(_) if group.state == GroupState::CompletingRebalance => {
                            code::REBALANCE_IN_PROGRESS
                        }
                        Some(member) => {
                            member.last_seen = now;
                            code::NONE
                        }
                    }
                }
                Some(_) => code::UNKNOWN_MEMBER_ID,
            }
        };
        let mut seen = CommitSeen {
            node,
            version,
            group: group_id.clone(),
            generation,
            member_id,
            offsets: Vec::new(),
            answered: Vec::new(),
        };
        let mut topics = Vec::new();
        for topic in &request.topics {
            let name = topic.name.0.to_string();
            let mut partitions = Vec::new();
            for p in &topic.partitions {
                let error = Taken::error_for(taken, &name, p.partition_index).unwrap_or(verdict);
                if error == code::NONE {
                    if let Some(group) = self.groups.get_mut(&group_id) {
                        group
                            .committed
                            .insert((name.clone(), p.partition_index), p.committed_offset);
                    }
                }
                seen.offsets.push((name.clone(), p.partition_index, p.committed_offset));
                seen.answered.push(error);
                partitions.push(
                    OffsetCommitResponsePartition::default()
                        .with_partition_index(p.partition_index)
                        .with_error_code(error),
                );
            }
            topics.push(
                OffsetCommitResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(partitions),
            );
        }
        self.commits.push(seen);
        OffsetCommitResponse::default()
            .with_throttle_time_ms(Taken::throttle(taken))
            .with_topics(topics)
    }

    pub(super) fn offset_fetch(
        &mut self,
        node: i32,
        version: i16,
        request: &OffsetFetchRequest,
        taken: &Option<Taken>,
    ) -> OffsetFetchResponse {
        let response = OffsetFetchResponse::default().with_throttle_time_ms(Taken::throttle(taken));
        let group_code = Taken::whole_error(taken).unwrap_or(if node == self.coordinator {
            code::NONE
        } else {
            code::NOT_COORDINATOR
        });
        // `(topic, partition, offset, code)` per partition the group asks about.
        let answer = |group_id: &str, asked: Option<Vec<(String, Vec<i32>)>>| {
            let group = self.groups.get(group_id);
            let mut out = Vec::new();
            for (topic, partitions) in asked_or_committed(group, asked) {
                let rows: Vec<(i32, i64, i16)> = partitions
                    .into_iter()
                    .map(|p| {
                        let offset = group
                            .and_then(|g| g.committed.get(&(topic.clone(), p)))
                            .copied()
                            .unwrap_or(-1);
                        (p, offset, Taken::error_for(taken, &topic, p).unwrap_or(code::NONE))
                    })
                    .collect();
                out.push((topic, rows));
            }
            out
        };
        let metadata = Some(StrBytes::default());
        if version >= 8 {
            let groups = request
                .groups
                .iter()
                .map(|g| {
                    let asked = g.topics.as_ref().map(|ts| {
                        ts.iter()
                            .map(|t| (t.name.0.to_string(), t.partition_indexes.clone()))
                            .collect()
                    });
                    let topics = if group_code == code::NONE {
                        answer(&g.group_id.0.to_string(), asked)
                    } else {
                        Vec::new()
                    };
                    OffsetFetchResponseGroup::default()
                        .with_group_id(g.group_id.clone())
                        .with_error_code(group_code)
                        .with_topics(
                            topics
                                .into_iter()
                                .map(|(name, rows)| {
                                    OffsetFetchResponseTopics::default()
                                        .with_name(TopicName(StrBytes::from_string(name)))
                                        .with_partitions(
                                            rows.into_iter()
                                                .map(|(p, offset, error)| {
                                                    OffsetFetchResponsePartitions::default()
                                                        .with_partition_index(p)
                                                        .with_committed_offset(offset)
                                                        .with_committed_leader_epoch(-1)
                                                        .with_metadata(metadata.clone())
                                                        .with_error_code(error)
                                                })
                                                .collect(),
                                        )
                                })
                                .collect(),
                        )
                })
                .collect();
            return response.with_groups(groups);
        }
        let asked = request.topics.as_ref().map(|ts| {
            ts.iter().map(|t| (t.name.0.to_string(), t.partition_indexes.clone())).collect()
        });
        // v1 has no group-level code, so a group's refusal goes on each partition; from v2 it's
        // the top-level code, with no partitions.
        let topics = if group_code == code::NONE || version < 2 {
            answer(&request.group_id.0.to_string(), asked)
        } else {
            Vec::new()
        };
        let topics = topics
            .into_iter()
            .map(|(name, rows)| {
                OffsetFetchResponseTopic::default()
                    .with_name(TopicName(StrBytes::from_string(name)))
                    .with_partitions(
                        rows.into_iter()
                            .map(|(p, offset, error)| {
                                let error = if version < 2 && error == code::NONE {
                                    group_code
                                } else {
                                    error
                                };
                                OffsetFetchResponsePartition::default()
                                    .with_partition_index(p)
                                    .with_committed_offset(offset)
                                    .with_committed_leader_epoch(-1)
                                    .with_metadata(metadata.clone())
                                    .with_error_code(error)
                            })
                            .collect(),
                    )
            })
            .collect();
        let response = response.with_topics(topics);
        if version >= 2 {
            response.with_error_code(group_code)
        } else {
            response
        }
    }
}
