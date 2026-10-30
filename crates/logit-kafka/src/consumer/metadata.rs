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
            Some(t) => {
                let count = t
                    .partitions
                    .iter()
                    .filter_map(|p| usize::try_from(p.partition_index).ok())
                    .map(|i| i + 1)
                    .max()
                    .unwrap_or(0);
                let mut leaders = vec![-1; count];
                for p in &t.partitions {
                    if let Ok(i) = usize::try_from(p.partition_index) {
                        leaders[i] = if p.error_code == code::NONE { p.leader_id.0 } else { -1 };
                    }
                }
                if count == 0 {
                    TopicState::Missing
                } else {
                    TopicState::Partitions(leaders)
                }
            }
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
