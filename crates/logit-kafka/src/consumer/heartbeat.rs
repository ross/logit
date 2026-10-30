//! The heartbeat task: one Heartbeat every `heartbeat_interval` on its own connection to the
//! coordinator, independent of fetch and delivery, so a pipeline that doesn't accept records
//! never expires the session (`docs/adr/kafka-input.md`, item 7, and item 2's "No poll-interval
//! liveness").
//!
//! It reports through the consumer's revoke signal: `REBALANCE_IN_PROGRESS` raises a rebalance
//! and keeps heartbeating, so the session holds while the caller drains; a code that ends the
//! membership (22, 25, 82) raises a membership loss and ends the task. A coordinator that moved
//! (15, 16) or a connection that failed is found again with FindCoordinator. The consumer aborts
//! the task when it rejoins or leaves.

use std::time::Duration;

use kafka_protocol::messages::ApiKey;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::client::{raise, Revoke, RevokeReason};
use super::group::{self, Failure};
use super::pool::{Link, Pool, Slot, Target};
use super::{group_code, GroupCode};

/// A running heartbeat task; dropping it stops the task.
pub(crate) struct Heartbeat(JoinHandle<()>);

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The member a heartbeat task speaks for.
pub(crate) struct Member {
    pub(crate) group: String,
    pub(crate) member_id: String,
    pub(crate) generation: i32,
    pub(crate) coordinator: Target,
}

pub(crate) fn spawn(
    pool: Pool,
    member: Member,
    interval: Duration,
    signal: watch::Sender<Option<Revoke>>,
) -> Heartbeat {
    Heartbeat(tokio::spawn(run(pool, member, interval, signal)))
}

async fn run(
    pool: Pool,
    member: Member,
    interval: Duration,
    signal: watch::Sender<Option<Revoke>>,
) {
    let mut link = Link::new(Slot::Heartbeat, member.coordinator.clone());
    let mut locate = Link::new(Slot::Locate, Target::Any);
    loop {
        tokio::time::sleep(interval).await;
        let result =
            group::heartbeat(&mut link, &pool, &member.group, member.generation, &member.member_id)
                .await;
        let Err(failure) = result else { continue };
        group::count(&pool, ApiKey::Heartbeat, &failure);
        let rediscover = match failure {
            Failure::Call(_) => true,
            Failure::Code(c) => match group_code(c) {
                GroupCode::Ok => false,
                GroupCode::Rebalance => {
                    raise(&signal, Revoke::new(RevokeReason::Rebalance));
                    false
                }
                GroupCode::Fenced { reset_member } => {
                    raise(&signal, Revoke { reason: RevokeReason::MembershipLost, reset_member });
                    return;
                }
                GroupCode::Rediscover => true,
                GroupCode::BackOff(diag) => {
                    if let Some(key) = diag {
                        pool.counters.warn(
                            key,
                            format_args!(
                                "Kafka group {:?}: Heartbeat answered {}",
                                member.group,
                                crate::error::name(c)
                            ),
                        );
                    }
                    false
                }
            },
        };
        if rediscover {
            match group::find_coordinator(&mut locate, &pool, &member.group).await {
                Ok(target) => link.retarget(target),
                Err(failure) => group::count(&pool, ApiKey::FindCoordinator, &failure),
            }
        }
    }
}
