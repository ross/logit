//! The consumer's view of the cluster: each broker's address and each subscribed topic's
//! partitions and their leaders, from a `Metadata` request that names the topics and clears
//! `allow_auto_topic_creation`, so reading a topic never creates it on a broker with
//! `auto.create.topics.enable` (`docs/adr/kafka-input.md`, item 2's divergences).
//!
//! The view is refreshed every `metadata_max_age` (5 minutes, Java's `metadata.max.age.ms`) and
//! sooner when a fetch loop meets a leader error (item 9's leader row). A topic answered
//! `UNKNOWN_TOPIC_OR_PARTITION` is missing: assigned to nobody until a refresh finds it.

use std::collections::{BTreeMap, HashMap};

use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponsePartition;
use kafka_protocol::messages::{ApiKey, MetadataRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use super::pool::{CallError, Link, Pool};
use crate::error::code;

/// One topic as the last metadata answered it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TopicState {
    /// Each partition's leader by index; `-1` while a partition has none.
    Partitions(Vec<i32>),
    /// `UNKNOWN_TOPIC_OR_PARTITION`, or a topic with no partitions.
    Missing,
    /// Any other topic-level code.
    Error(i16),
}

/// The cluster view one `Metadata` answer gives.
#[derive(Debug, Clone)]
pub(crate) struct Cluster {
    pub(crate) brokers: HashMap<i32, (String, u16)>,
    pub(crate) topics: BTreeMap<String, TopicState>,
}

impl Cluster {
    /// Partition counts of the topics that exist, for an assignor.
    pub(crate) fn partition_counts(&self) -> BTreeMap<String, i32> {
        self.topics
            .iter()
            .filter_map(|(t, s)| match s {
                TopicState::Partitions(p) => Some((t.clone(), p.len() as i32)),
                _ => None,
            })
            .collect()
    }

    /// The leader of `topic`'s `partition`, when it has one.
    pub(crate) fn leader(&self, topic: &str, partition: i32) -> Option<i32> {
        match self.topics.get(topic)? {
            TopicState::Partitions(leaders) => {
                let leader = *leaders.get(usize::try_from(partition).ok()?)?;
                (leader >= 0 && self.brokers.contains_key(&leader)).then_some(leader)
            }
            _ => None,
        }
    }
}

/// Each partition's leader by index, `-1` for one answered with an error or no leader; `None`
/// when an index isn't one of `0..partitions.len()`, which no broker sends. The vector is sized
/// by the entries, never by an index the answer carries.
fn leaders(partitions: &[MetadataResponsePartition]) -> Option<Vec<i32>> {
    let mut leaders = vec![-1; partitions.len()];
    for p in partitions {
        let i = usize::try_from(p.partition_index).ok().filter(|&i| i < partitions.len())?;
        leaders[i] = if p.error_code == code::NONE { p.leader_id.0 } else { -1 };
    }
    Some(leaders)
}

/// Asks any broker for `topics`; counts each topic-level error, and reports a missing topic
/// with the throttled diagnostic `topic_missing`.
pub(crate) async fn fetch(
    link: &mut Link,
    pool: &Pool,
    topics: &[String],
) -> Result<Cluster, CallError> {
    let names: Vec<MetadataRequestTopic> = topics
        .iter()
        .map(|t| {
            MetadataRequestTopic::default()
                .with_name(Some(TopicName(StrBytes::from_string(t.clone()))))
        })
        .collect();
    let (_, response) = link
        .call(pool, ApiKey::Metadata, None, |_| {
            MetadataRequest::default()
                .with_topics(Some(names))
                .with_allow_auto_topic_creation(false)
        })
        .await?;
    let brokers: HashMap<i32, (String, u16)> = response
        .brokers
        .iter()
        .filter_map(|b| Some((b.node_id.0, (b.host.to_string(), u16::try_from(b.port).ok()?))))
        .collect();
    pool.set_brokers(brokers.clone());
    let mut out = BTreeMap::new();
    for name in topics {
        let answer = response
            .topics
            .iter()
            .find(|t| t.name.as_ref().is_some_and(|n| n.0.as_str() == name.as_str()));
        let state = match answer {
            None => TopicState::Missing,
            Some(t) if t.error_code == code::UNKNOWN_TOPIC_OR_PARTITION => {
                pool.counters.broker_code(ApiKey::Metadata, t.error_code);
                TopicState::Missing
            }
            Some(t) if t.error_code != code::NONE => {
                pool.counters.broker_code(ApiKey::Metadata, t.error_code);
                if t.error_code == code::TOPIC_AUTHORIZATION_FAILED {
                    pool.counters.warn(
                        "not_authorized",
                        format_args!("Kafka topic {name:?}: not authorized to describe it"),
                    );
                }
                TopicState::Error(t.error_code)
            }
            Some(t) => match leaders(&t.partitions) {
                Some(leaders) if leaders.is_empty() => TopicState::Missing,
                Some(leaders) => TopicState::Partitions(leaders),
                None => {
                    pool.counters.broker_error(ApiKey::Metadata, "malformed");
                    pool.counters.warn(
                        "broker_error",
                        format_args!(
                            "Kafka topic {name:?}: Metadata answered a partition index outside \
                             its {} entries",
                            t.partitions.len()
                        ),
                    );
                    TopicState::Error(code::UNKNOWN_SERVER_ERROR)
                }
            },
        };
        if state == TopicState::Missing {
            pool.counters.warn(
                "topic_missing",
                format_args!("Kafka topic {name:?} doesn't exist; waiting for it"),
            );
        }
        out.insert(name.clone(), state);
    }
    Ok(Cluster { brokers, topics: out })
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::BrokerId;

    use super::*;

    fn partition(index: i32, leader: i32, error: i16) -> MetadataResponsePartition {
        MetadataResponsePartition::default()
            .with_partition_index(index)
            .with_leader_id(BrokerId(leader))
            .with_error_code(error)
    }

    #[test]
    fn leaders_are_placed_by_index_and_an_errored_partition_has_none() {
        let got =
            leaders(&[partition(1, 7, code::NONE), partition(0, 5, code::LEADER_NOT_AVAILABLE)]);
        assert_eq!(got, Some(vec![-1, 7]));
    }

    /// An index past the entries would size the vector from the wire: near `i32::MAX`, about
    /// 8 GiB.
    #[test]
    fn an_index_outside_the_entries_is_malformed() {
        assert_eq!(leaders(&[partition(i32::MAX - 1, 0, code::NONE)]), None);
        assert_eq!(leaders(&[partition(0, 0, code::NONE), partition(2, 0, code::NONE)]), None);
        assert_eq!(leaders(&[partition(-1, 0, code::NONE)]), None);
    }
}
