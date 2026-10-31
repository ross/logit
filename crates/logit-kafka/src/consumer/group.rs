//! Classic-protocol group membership: finding the coordinator, the JoinGroup and SyncGroup
//! round, LeaveGroup, and Heartbeat, each one request on a [`Link`] (`docs/adr/kafka-input.md`,
//! item 4). The member state lives in [`super::consumer`]; this module holds the requests.
//!
//! Wire facts:
//!
//! - FindCoordinator below v4 names the group in `key` and answers in top-level fields; from v4
//!   it names it in `coordinator_keys` and answers in `coordinators`
//!   ([`FindCoordinatorRequest.json` at 4.0.0](https://github.com/apache/kafka/blob/4.0.0/clients/src/main/resources/common/message/FindCoordinatorRequest.json)).
//! - JoinGroup from v4 answers a first join, sent with an empty member id,
//!   `MEMBER_ID_REQUIRED` (79) with the id to join under; [`join`] sends it again with that id,
//!   and doesn't count the round trip as an error. The coordinator holds a join's answer until
//!   every member has rejoined, so the wait is `rebalance_timeout` plus the timeout, as Java's
//!   `JoinGroupRequest` timeout is.
//! - The protocol type is `consumer` with one protocol, the assignor's name, whose metadata is
//!   a `ConsumerProtocolSubscription` ([`super::assignor`]).
//! - LeaveGroup below v3 names one `member_id`; from v3 a `members` list, whose per-member code
//!   is read beside the top-level one.

use bytes::Bytes;
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    ApiKey, FindCoordinatorRequest, GroupId, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest,
    SyncGroupRequest,
};
use kafka_protocol::protocol::StrBytes;

use super::pool::{CallError, Link, Pool, Target};
use crate::error::code;

/// The protocol type every consumer group member names.
const PROTOCOL_TYPE: &str = "consumer";

/// How many `MEMBER_ID_REQUIRED` answers one [`join`] follows; a coordinator sends one.
const MEMBER_ID_ROUNDS: usize = 3;

/// A group request that failed: no answer, or an error code.
#[derive(Debug)]
pub(crate) enum Failure {
    Call(CallError),
    Code(i16),
}

impl From<CallError> for Failure {
    fn from(err: CallError) -> Self {
        Failure::Call(err)
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Call(err) => write!(f, "{err}"),
            Failure::Code(c) => write!(f, "{}", crate::error::name(*c)),
        }
    }
}

/// Counts a code failure under `api`; a call failure is counted where it happened.
pub(crate) fn count(pool: &Pool, api: ApiKey, failure: &Failure) {
    if let Failure::Code(c) = failure {
        pool.counters.broker_code(api, *c);
    }
}

/// Where the group's coordinator is, as a link target.
pub(crate) async fn find_coordinator(
    link: &mut Link,
    pool: &Pool,
    group: &str,
) -> Result<Target, Failure> {
    let key = StrBytes::from_string(group.to_string());
    let (version, response) = link
        .call(pool, ApiKey::FindCoordinator, None, |version| {
            if version >= 4 {
                FindCoordinatorRequest::default().with_coordinator_keys(vec![key])
            } else {
                FindCoordinatorRequest::default().with_key(key)
            }
        })
        .await?;
    let (error, host, port) = if version >= 4 {
        match response.coordinators.first() {
            Some(c) => (c.error_code, c.host.to_string(), c.port),
            None => (code::COORDINATOR_NOT_AVAILABLE, String::new(), -1),
        }
    } else {
        (response.error_code, response.host.to_string(), response.port)
    };
    if error != code::NONE {
        return Err(Failure::Code(error));
    }
    let port = u16::try_from(port).map_err(|_| Failure::Code(code::COORDINATOR_NOT_AVAILABLE))?;
    Ok(Target::Addr(host, port))
}

/// What a completed JoinGroup gives.
#[derive(Debug, Clone)]
pub(crate) struct Joined {
    pub(crate) member_id: String,
    pub(crate) generation: i32,
    pub(crate) leader: String,
    pub(crate) protocol: String,
    /// Every member's subscription bytes; empty unless this member leads.
    pub(crate) members: Vec<(String, Bytes)>,
}

/// What a join sends.
pub(crate) struct JoinRequest<'a> {
    pub(crate) group: &'a str,
    pub(crate) member_id: String,
    pub(crate) session_timeout_ms: i32,
    pub(crate) rebalance_timeout_ms: i32,
    pub(crate) protocol: &'static str,
    pub(crate) metadata: Bytes,
    pub(crate) wait: std::time::Duration,
}

/// JoinGroup, through the `MEMBER_ID_REQUIRED` round trip. On a failure the member id to use
/// next comes back beside it: the one the coordinator handed out, else the one sent.
pub(crate) async fn join(
    link: &mut Link,
    pool: &Pool,
    request: JoinRequest<'_>,
) -> Result<Joined, (Failure, String)> {
    let mut member_id = request.member_id;
    for _ in 0..MEMBER_ID_ROUNDS {
        let protocol = JoinGroupRequestProtocol::default()
            .with_name(StrBytes::from_static_str(request.protocol))
            .with_metadata(request.metadata.clone());
        let sent = member_id.clone();
        let answer = link
            .call(pool, ApiKey::JoinGroup, Some(request.wait), |_| {
                JoinGroupRequest::default()
                    .with_group_id(GroupId(StrBytes::from_string(request.group.to_string())))
                    .with_session_timeout_ms(request.session_timeout_ms)
                    .with_rebalance_timeout_ms(request.rebalance_timeout_ms)
                    .with_member_id(StrBytes::from_string(sent))
                    .with_protocol_type(StrBytes::from_static_str(PROTOCOL_TYPE))
                    .with_protocols(vec![protocol])
            })
            .await;
        let response = match answer {
            Ok((_, response)) => response,
            Err(err) => return Err((Failure::Call(err), member_id)),
        };
        match response.error_code {
            code::NONE => {
                return Ok(Joined {
                    member_id: response.member_id.to_string(),
                    generation: response.generation_id,
                    leader: response.leader.to_string(),
                    protocol: response
                        .protocol_name
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| request.protocol.to_string()),
                    members: response
                        .members
                        .into_iter()
                        .map(|m| (m.member_id.to_string(), m.metadata))
                        .collect(),
                })
            }
            code::MEMBER_ID_REQUIRED => member_id = response.member_id.to_string(),
            other => return Err((Failure::Code(other), member_id)),
        }
    }
    Err((Failure::Code(code::MEMBER_ID_REQUIRED), member_id))
}

/// SyncGroup: the leader sends `assignments`, a follower none; returns this member's
/// assignment bytes.
pub(crate) async fn sync(
    link: &mut Link,
    pool: &Pool,
    group: &str,
    joined: &Joined,
    assignments: Vec<(String, Bytes)>,
) -> Result<Bytes, Failure> {
    let (_, response) = link
        .call(pool, ApiKey::SyncGroup, None, |_| {
            SyncGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
                .with_generation_id(joined.generation)
                .with_member_id(StrBytes::from_string(joined.member_id.clone()))
                .with_protocol_type(Some(StrBytes::from_static_str(PROTOCOL_TYPE)))
                .with_protocol_name(Some(StrBytes::from_string(joined.protocol.clone())))
                .with_assignments(
                    assignments
                        .into_iter()
                        .map(|(member, bytes)| {
                            SyncGroupRequestAssignment::default()
                                .with_member_id(StrBytes::from_string(member))
                                .with_assignment(bytes)
                        })
                        .collect(),
                )
        })
        .await?;
    match response.error_code {
        code::NONE => Ok(response.assignment),
        other => Err(Failure::Code(other)),
    }
}

/// One Heartbeat for `member_id` in `generation`.
pub(crate) async fn heartbeat(
    link: &mut Link,
    pool: &Pool,
    group: &str,
    generation: i32,
    member_id: &str,
) -> Result<(), Failure> {
    let (_, response) = link
        .call(pool, ApiKey::Heartbeat, None, |_| {
            HeartbeatRequest::default()
                .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
                .with_generation_id(generation)
                .with_member_id(StrBytes::from_string(member_id.to_string()))
        })
        .await?;
    match response.error_code {
        code::NONE => Ok(()),
        other => Err(Failure::Code(other)),
    }
}

/// LeaveGroup for `member_id`.
pub(crate) async fn leave(
    link: &mut Link,
    pool: &Pool,
    group: &str,
    member_id: &str,
) -> Result<(), Failure> {
    let id = StrBytes::from_string(member_id.to_string());
    let (version, response) = link
        .call(pool, ApiKey::LeaveGroup, None, |version| {
            let request = LeaveGroupRequest::default()
                .with_group_id(GroupId(StrBytes::from_string(group.to_string())));
            if version >= 3 {
                request.with_members(vec![MemberIdentity::default().with_member_id(id)])
            } else {
                request.with_member_id(id)
            }
        })
        .await?;
    let member = if version >= 3 {
        response.members.first().map_or(code::NONE, |m| m.error_code)
    } else {
        code::NONE
    };
    match (response.error_code, member) {
        (code::NONE, code::NONE) => Ok(()),
        (code::NONE, other) | (other, _) => Err(Failure::Code(other)),
    }
}
