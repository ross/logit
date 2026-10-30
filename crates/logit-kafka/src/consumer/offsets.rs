//! Offsets: where a partition starts (OffsetFetch, then ListOffsets for one with no commit),
//! the OffsetCommit that acknowledges what was delivered, and the [`Tracker`] that decides what
//! may be committed (`docs/adr/kafka-input.md`, item 6).
//!
//! Wire facts:
//!
//! - OffsetFetch below v8 names the group and topics at the top level; from v8 inside a `groups`
//!   array. From v7 it sets `require_stable`, so a partition whose offset a pending transaction
//!   holds answers `UNSTABLE_OFFSET_COMMIT` (88) rather than a stale offset. v9's member id and
//!   epoch stay null and -1: they belong to KIP-848 groups.
//! - OffsetCommit carries the generation and member id, so a coordinator refuses a member that
//!   lost its membership (22 or 25) instead of letting it commit over the next owner. No
//!   metadata string is sent beyond the empty one, so `OFFSET_METADATA_TOO_LARGE` can't occur.
//! - ListOffsets sends `replica_id` -1, a consumer's (the crate's default is 0, a broker's), and
//!   the configured isolation level, so `latest` under `read_committed` is the last stable
//!   offset. Timestamp -1 asks for the log end, -2 for the log start.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;

use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::{
    ApiKey, BrokerId, GroupId, ListOffsetsRequest, OffsetCommitRequest, OffsetFetchRequest,
    TopicName,
};
use kafka_protocol::protocol::StrBytes;

use super::group::Failure;
use super::pool::{Link, Pool};
use super::{Isolation, TopicPartition};
use crate::error::code;

/// ListOffsets' timestamp for the log end (the last stable offset under `read_committed`).
pub(crate) const LATEST: i64 = -1;
/// ListOffsets' timestamp for the log start.
pub(crate) const EARLIEST: i64 = -2;

/// One partition's commit candidate: the highest offset below which every offset is done.
///
/// An offset is done once a batch holding it was accepted ([`Tracker::mark`] from the caller),
/// or once it needs no delivery: a control record, an aborted transaction's record, a record or
/// batch skipped and counted, or a gap left by retention (the consumer marks those itself). The
/// candidate moves only across contiguous done ranges, so it never passes an offset whose batch
/// wasn't accepted, whatever order ranges are marked in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tracker {
    candidate: i64,
    /// Done ranges above the candidate: start to end.
    done: BTreeMap<i64, i64>,
    /// The offset the last successful commit carried, if any.
    committed: Option<i64>,
}

impl Tracker {
    /// A tracker whose candidate starts at `position`; `committed` is what the group holds.
    pub fn new(position: i64, committed: Option<i64>) -> Self {
        Self { candidate: position, done: BTreeMap::new(), committed }
    }

    /// Marks `range` done and moves the candidate across every contiguous done range.
    pub fn mark(&mut self, range: Range<i64>) {
        if range.end <= range.start || range.end <= self.candidate {
            return;
        }
        let end = self.done.entry(range.start).or_insert(range.end);
        *end = (*end).max(range.end);
        while let Some((&start, &end)) = self.done.first_key_value() {
            if start > self.candidate {
                break;
            }
            self.done.pop_first();
            self.candidate = self.candidate.max(end);
        }
    }

    /// The offset a commit now would carry.
    pub fn candidate(&self) -> i64 {
        self.candidate
    }

    /// Whether the candidate differs from the last commit.
    pub fn pending(&self) -> bool {
        self.committed != Some(self.candidate)
    }

    /// Records a commit of `offset`.
    pub fn committed(&mut self, offset: i64) {
        self.committed = Some(offset);
    }

    /// Moves the candidate down to `position` if it's above it, and forgets what was done at or
    /// above `position`: a log-end reset after the log was truncated below the fetch position.
    /// Never moves the candidate up, since records handed out below `position` may not be
    /// accepted yet.
    pub fn reset(&mut self, position: i64) {
        self.candidate = self.candidate.min(position);
        self.done.retain(|&start, _| start < position);
        for end in self.done.values_mut() {
            *end = (*end).min(position);
        }
    }
}

fn topic_name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_string()))
}

/// Groups partitions by topic, in order.
fn by_topic<T: Clone>(items: &[(TopicPartition, T)]) -> BTreeMap<&str, Vec<(i32, T)>> {
    let mut out: BTreeMap<&str, Vec<(i32, T)>> = BTreeMap::new();
    for (tp, v) in items {
        out.entry(&tp.topic).or_default().push((tp.partition, v.clone()));
    }
    out
}

/// Each partition's committed offset in `group`, `None` for one with no commit. A group-level
/// code or any partition's code fails the whole fetch.
pub(crate) async fn fetch_committed(
    link: &mut Link,
    pool: &Pool,
    group: &str,
    partitions: &[TopicPartition],
) -> Result<HashMap<TopicPartition, Option<i64>>, Failure> {
    let items: Vec<(TopicPartition, ())> = partitions.iter().map(|tp| (tp.clone(), ())).collect();
    let topics = by_topic(&items);
    let group_id = GroupId(StrBytes::from_string(group.to_string()));
    let (version, response) = link
        .call(pool, ApiKey::OffsetFetch, None, |version| {
            let request = OffsetFetchRequest::default().with_require_stable(version >= 7);
            if version >= 8 {
                let topics = topics
                    .iter()
                    .map(|(t, ps)| {
                        OffsetFetchRequestTopics::default()
                            .with_name(topic_name(t))
                            .with_partition_indexes(ps.iter().map(|(p, _)| *p).collect())
                    })
                    .collect();
                request.with_groups(vec![OffsetFetchRequestGroup::default()
                    .with_group_id(group_id)
                    .with_topics(Some(topics))])
            } else {
                let topics = topics
                    .iter()
                    .map(|(t, ps)| {
                        OffsetFetchRequestTopic::default()
                            .with_name(topic_name(t))
                            .with_partition_indexes(ps.iter().map(|(p, _)| *p).collect())
                    })
                    .collect();
                request.with_group_id(group_id).with_topics(Some(topics))
            }
        })
        .await?;
    let mut answers: Vec<(String, i32, i64, i16)> = Vec::new();
    let group_error = if version >= 8 {
        let Some(g) = response.groups.first() else {
            return Err(Failure::Code(code::COORDINATOR_NOT_AVAILABLE));
        };
        for t in &g.topics {
            for p in &t.partitions {
                answers.push((
                    t.name.0.to_string(),
                    p.partition_index,
                    p.committed_offset,
                    p.error_code,
                ));
            }
        }
        g.error_code
    } else {
        for t in &response.topics {
            for p in &t.partitions {
                answers.push((
                    t.name.0.to_string(),
                    p.partition_index,
                    p.committed_offset,
                    p.error_code,
                ));
            }
        }
        response.error_code
    };
    if group_error != code::NONE {
        return Err(Failure::Code(group_error));
    }
    let mut out = HashMap::new();
    for (topic, partition, offset, error) in answers {
        if error != code::NONE {
            return Err(Failure::Code(error));
        }
        out.insert(TopicPartition::new(&topic, partition), (offset >= 0).then_some(offset));
    }
    Ok(out)
}

/// One OffsetCommit of `offsets` under `generation` and `member_id`; returns each partition's
/// answered code.
pub(crate) async fn commit(
    link: &mut Link,
    pool: &Pool,
    group: &str,
    generation: i32,
    member_id: &str,
    offsets: &[(TopicPartition, i64)],
) -> Result<Vec<(TopicPartition, i16)>, Failure> {
    let topics = by_topic(offsets);
    let (_, response) = link
        .call(pool, ApiKey::OffsetCommit, None, |_| {
            OffsetCommitRequest::default()
                .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
                .with_generation_id_or_member_epoch(generation)
                .with_member_id(StrBytes::from_string(member_id.to_string()))
                .with_topics(
                    topics
                        .iter()
                        .map(|(t, ps)| {
                            OffsetCommitRequestTopic::default()
                                .with_name(topic_name(t))
                                .with_partitions(
                                    ps.iter()
                                        .map(|(p, offset)| {
                                            OffsetCommitRequestPartition::default()
                                                .with_partition_index(*p)
                                                .with_committed_offset(*offset)
                                        })
                                        .collect(),
                                )
                        })
                        .collect(),
                )
        })
        .await?;
    Ok(response
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(|p| (TopicPartition::new(t.name.0.as_str(), p.partition_index), p.error_code))
        })
        .collect())
}

/// ListOffsets for `asks` (partition and timestamp) under `isolation`; returns each partition's
/// code and offset.
pub(crate) async fn list(
    link: &mut Link,
    pool: &Pool,
    isolation: Isolation,
    asks: &[(TopicPartition, i64)],
) -> Result<Vec<(TopicPartition, i16, i64)>, Failure> {
    let topics = by_topic(asks);
    let timeout_ms = i32::try_from(pool.timeout().as_millis()).unwrap_or(i32::MAX);
    let (_, response) = link
        .call(pool, ApiKey::ListOffsets, None, |_| {
            ListOffsetsRequest::default()
                .with_replica_id(BrokerId(-1))
                .with_isolation_level(isolation.wire())
                .with_timeout_ms(timeout_ms)
                .with_topics(
                    topics
                        .iter()
                        .map(|(t, ps)| {
                            ListOffsetsTopic::default().with_name(topic_name(t)).with_partitions(
                                ps.iter()
                                    .map(|(p, ts)| {
                                        ListOffsetsPartition::default()
                                            .with_partition_index(*p)
                                            .with_timestamp(*ts)
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                )
        })
        .await?;
    Ok(response
        .topics
        .iter()
        .flat_map(|t| {
            t.partitions.iter().map(|p| {
                (TopicPartition::new(t.name.0.as_str(), p.partition_index), p.error_code, p.offset)
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The candidate never passes an unmarked range, whatever order ranges arrive in.
    #[test]
    fn the_candidate_moves_only_across_contiguous_done_ranges() {
        let mut t = Tracker::new(100, None);
        t.mark(150..200);
        assert_eq!(t.candidate(), 100, "150..200 sits behind the unaccepted 100..150");
        t.mark(200..210);
        assert_eq!(t.candidate(), 100);
        t.mark(100..150);
        assert_eq!(t.candidate(), 210, "the gap closed, so every done range counts");
    }

    #[test]
    fn overlapping_and_stale_ranges_change_nothing_they_shouldnt() {
        let mut t = Tracker::new(10, None);
        t.mark(5..8);
        assert_eq!(t.candidate(), 10);
        t.mark(10..12);
        t.mark(11..15);
        assert_eq!(t.candidate(), 15);
        t.mark(20..20);
        assert_eq!(t.candidate(), 15);
    }

    #[test]
    fn pending_follows_the_last_commit_and_reset_drops_done_ranges() {
        let mut t = Tracker::new(0, Some(0));
        assert!(!t.pending());
        t.mark(0..5);
        assert!(t.pending());
        t.committed(5);
        assert!(!t.pending());
        t.mark(9..12);
        t.reset(7);
        t.mark(7..9);
        assert_eq!(t.candidate(), 5, "a reset above the candidate leaves it");
        t.mark(5..7);
        assert_eq!(t.candidate(), 9, "the done range above the reset went with it");
    }

    /// A log truncated below the fetch position: the handed-out records under the new end aren't
    /// accepted yet, so the candidate stays, and what was done below the end survives.
    #[test]
    fn a_reset_below_the_fetch_position_never_moves_the_candidate_up() {
        let mut t = Tracker::new(100, Some(100));
        t.mark(105..110);
        t.mark(112..140);
        t.reset(120);
        assert_eq!(t.candidate(), 100);
        t.mark(100..105);
        assert_eq!(t.candidate(), 110);
        t.mark(110..112);
        assert_eq!(t.candidate(), 120, "112..140 was cut at the new end");
        t.reset(90);
        assert_eq!(t.candidate(), 90, "a reset below the candidate moves it down");
    }
}
