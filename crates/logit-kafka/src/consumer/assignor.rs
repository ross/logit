//! The two eager assignors a group leader runs, `range` and `roundrobin`, and the embedded
//! consumer protocol their input and output travel in.
//!
//! The JoinGroup metadata and the SyncGroup assignment use the Java client's format so a Java
//! consumer and `kafka_in` can share a group: a big-endian `i16` version, then the
//! `ConsumerProtocolSubscription` or `ConsumerProtocolAssignment` body at that version
//! ([`ConsumerProtocol.java`](https://raw.githubusercontent.com/apache/kafka/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerProtocol.java),
//! `MessageUtil.toVersionPrefixedByteBuffer`). `logit` writes subscription v0 (topics, null user
//! data) and assignment v0. A subscription or assignment above v3 reads as v3, as Java's
//! `checkSubscriptionVersion` caps it; a negative version is an error.
//!
//! Members sort by member id, compared as strings ([`MemberInfo`'s order without a group
//! instance id](https://raw.githubusercontent.com/apache/kafka/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractPartitionAssignor.java)).
//! A topic missing from the metadata the leader passes is assigned to nobody.
//!
//! - [`range`], per topic: the topic's subscribers in member order each take
//!   `partitions / subscribers` consecutive partitions, the first `partitions % subscribers` one
//!   more ([`RangeAssignor`](https://raw.githubusercontent.com/apache/kafka/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/RangeAssignor.java),
//!   without its rack-aware pass, since `kafka_in` sends no rack).
//! - [`round_robin`]: every subscribed topic-partition, topics in name order, dealt from one
//!   circular iterator over the members, which skips a member not subscribed to the partition's
//!   topic and keeps its place between partitions
//!   ([`RoundRobinAssignor`](https://raw.githubusercontent.com/apache/kafka/4.1.0/clients/src/main/java/org/apache/kafka/clients/consumer/RoundRobinAssignor.java)).

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, Context};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition as AssignedTopic;
use kafka_protocol::messages::{
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

/// The highest embedded-protocol version `kafka-protocol` 0.18.0 reads.
const HIGHEST: i16 = 3;

/// The assignment strategy, named on the wire as Java names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Assignor {
    Range,
    RoundRobin,
}

impl Assignor {
    /// The JoinGroup protocol name.
    pub fn name(self) -> &'static str {
        match self {
            Assignor::Range => "range",
            Assignor::RoundRobin => "roundrobin",
        }
    }

    /// Runs the assignor over `members` (member id to subscribed topics) and `partitions` (topic
    /// to partition count); returns each member's partitions, every member present.
    pub fn assign(
        self,
        members: &BTreeMap<String, Vec<String>>,
        partitions: &BTreeMap<String, i32>,
    ) -> BTreeMap<String, Vec<(String, i32)>> {
        match self {
            Assignor::Range => range(members, partitions),
            Assignor::RoundRobin => round_robin(members, partitions),
        }
    }
}

/// `RangeAssignor`'s assignment.
pub fn range(
    members: &BTreeMap<String, Vec<String>>,
    partitions: &BTreeMap<String, i32>,
) -> BTreeMap<String, Vec<(String, i32)>> {
    let mut out: BTreeMap<String, Vec<(String, i32)>> =
        members.keys().map(|m| (m.clone(), Vec::new())).collect();
    for (topic, &count) in partitions {
        let subscribers: Vec<&String> =
            members.iter().filter(|(_, topics)| topics.contains(topic)).map(|(m, _)| m).collect();
        if subscribers.is_empty() || count <= 0 {
            continue;
        }
        let n = subscribers.len() as i32;
        let (each, extra) = (count / n, count % n);
        let mut next = 0;
        for (i, member) in subscribers.into_iter().enumerate() {
            let take = each + i32::from((i as i32) < extra);
            let list = out.get_mut(member).expect("every member has an entry");
            list.extend((next..next + take).map(|p| (topic.clone(), p)));
            next += take;
        }
    }
    out
}

/// `RoundRobinAssignor`'s assignment.
pub fn round_robin(
    members: &BTreeMap<String, Vec<String>>,
    partitions: &BTreeMap<String, i32>,
) -> BTreeMap<String, Vec<(String, i32)>> {
    let mut out: BTreeMap<String, Vec<(String, i32)>> =
        members.keys().map(|m| (m.clone(), Vec::new())).collect();
    let order: Vec<(&String, &Vec<String>)> = members.iter().collect();
    if order.is_empty() {
        return out;
    }
    let topics: BTreeSet<&String> = members.values().flatten().collect();
    let mut at = 0;
    for topic in topics {
        let Some(&count) = partitions.get(topic) else { continue };
        for p in 0..count {
            while !order[at % order.len()].1.contains(topic) {
                at += 1;
            }
            let member = order[at % order.len()].0;
            out.get_mut(member).expect("every member has an entry").push((topic.clone(), p));
            at += 1;
        }
    }
    out
}

/// A member's `ConsumerProtocolSubscription` v0 for `topics`, version-prefixed.
pub fn encode_subscription(topics: &[String]) -> Bytes {
    let body = ConsumerProtocolSubscription::default()
        .with_topics(topics.iter().map(|t| StrBytes::from_string(t.clone())).collect());
    prefixed(0, |buf| body.encode(buf, 0))
}

/// The topics a member's version-prefixed subscription names, any version from 0.
pub fn decode_subscription(mut bytes: Bytes) -> anyhow::Result<Vec<String>> {
    let version = version(&mut bytes).context("a consumer subscription")?;
    let body = ConsumerProtocolSubscription::decode(&mut bytes, version)
        .with_context(|| format!("a consumer subscription v{version}"))?;
    Ok(body.topics.iter().map(|t| t.to_string()).collect())
}

/// A member's `ConsumerProtocolAssignment` v0 for `partitions`, grouped by topic in order.
pub fn encode_assignment(partitions: &[(String, i32)]) -> Bytes {
    let mut topics: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for (topic, p) in partitions {
        topics.entry(topic).or_default().push(*p);
    }
    let body = ConsumerProtocolAssignment::default().with_assigned_partitions(
        topics
            .into_iter()
            .map(|(topic, partitions)| {
                AssignedTopic::default()
                    .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
                    .with_partitions(partitions)
            })
            .collect(),
    );
    prefixed(0, |buf| body.encode(buf, 0))
}

/// The partitions a version-prefixed assignment gives; an empty buffer is no partitions, as a
/// coordinator answers a member the leader assigned nothing.
pub fn decode_assignment(mut bytes: Bytes) -> anyhow::Result<Vec<(String, i32)>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let version = version(&mut bytes).context("a consumer assignment")?;
    let body = ConsumerProtocolAssignment::decode(&mut bytes, version)
        .with_context(|| format!("a consumer assignment v{version}"))?;
    Ok(body
        .assigned_partitions
        .iter()
        .flat_map(|t| t.partitions.iter().map(|&p| (t.topic.0.to_string(), p)))
        .collect())
}

fn prefixed(version: i16, encode: impl FnOnce(&mut BytesMut) -> anyhow::Result<()>) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(version);
    encode(&mut buf).expect("an embedded consumer protocol body encodes");
    buf.freeze()
}

fn version(bytes: &mut Bytes) -> anyhow::Result<i16> {
    if bytes.remaining() < 2 {
        return Err(anyhow!("no version prefix"));
    }
    let version = bytes.get_i16();
    anyhow::ensure!(version >= 0, "version {version} is below 0");
    Ok(version.min(HIGHEST))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members(list: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
        list.iter()
            .map(|(m, topics)| (m.to_string(), topics.iter().map(|t| t.to_string()).collect()))
            .collect()
    }

    fn counts(list: &[(&str, i32)]) -> BTreeMap<String, i32> {
        list.iter().map(|(t, n)| (t.to_string(), *n)).collect()
    }

    fn parts(list: &[(&str, i32)]) -> Vec<(String, i32)> {
        list.iter().map(|(t, p)| (t.to_string(), *p)).collect()
    }

    /// `RangeAssignor`'s javadoc: C0 `[t0p0, t0p1, t1p0, t1p1]`, C1 `[t0p2, t1p2]`.
    #[test]
    fn range_matches_the_java_javadoc_example() {
        let got = range(
            &members(&[("C0", &["t0", "t1"]), ("C1", &["t0", "t1"])]),
            &counts(&[("t0", 3), ("t1", 3)]),
        );
        assert_eq!(got["C0"], parts(&[("t0", 0), ("t0", 1), ("t1", 0), ("t1", 1)]));
        assert_eq!(got["C1"], parts(&[("t0", 2), ("t1", 2)]));
    }

    /// The javadoc's shuffle after a bounce: C3 sorts after C2, so C2 takes the larger share.
    #[test]
    fn range_orders_members_by_id() {
        let got = range(
            &members(&[("C3", &["t0", "t1"]), ("C2", &["t0", "t1"])]),
            &counts(&[("t0", 3), ("t1", 3)]),
        );
        assert_eq!(got["C2"], parts(&[("t0", 0), ("t0", 1), ("t1", 0), ("t1", 1)]));
        assert_eq!(got["C3"], parts(&[("t0", 2), ("t1", 2)]));
    }

    /// `RoundRobinAssignor`'s first javadoc example: C0 `[t0p0, t0p2, t1p1]`, C1
    /// `[t0p1, t1p0, t1p2]`.
    #[test]
    fn roundrobin_matches_the_java_javadoc_example() {
        let got = round_robin(
            &members(&[("C0", &["t0", "t1"]), ("C1", &["t0", "t1"])]),
            &counts(&[("t0", 3), ("t1", 3)]),
        );
        assert_eq!(got["C0"], parts(&[("t0", 0), ("t0", 2), ("t1", 1)]));
        assert_eq!(got["C1"], parts(&[("t0", 1), ("t1", 0), ("t1", 2)]));
    }

    /// The javadoc's unequal subscriptions: C0 `[t0p0]`, C1 `[t1p0]`, C2
    /// `[t1p1, t2p0, t2p1, t2p2]`.
    #[test]
    fn roundrobin_skips_a_member_not_subscribed_to_the_topic() {
        let got = round_robin(
            &members(&[("C0", &["t0"]), ("C1", &["t0", "t1"]), ("C2", &["t0", "t1", "t2"])]),
            &counts(&[("t0", 1), ("t1", 2), ("t2", 3)]),
        );
        assert_eq!(got["C0"], parts(&[("t0", 0)]));
        assert_eq!(got["C1"], parts(&[("t1", 0)]));
        assert_eq!(got["C2"], parts(&[("t1", 1), ("t2", 0), ("t2", 1), ("t2", 2)]));
    }

    /// The javadoc's three members over two three-partition topics.
    #[test]
    fn roundrobin_deals_three_members_one_partition_per_topic() {
        let got = round_robin(
            &members(&[("C0", &["t0", "t1"]), ("C1", &["t0", "t1"]), ("C2", &["t0", "t1"])]),
            &counts(&[("t0", 3), ("t1", 3)]),
        );
        assert_eq!(got["C0"], parts(&[("t0", 0), ("t1", 0)]));
        assert_eq!(got["C1"], parts(&[("t0", 1), ("t1", 1)]));
        assert_eq!(got["C2"], parts(&[("t0", 2), ("t1", 2)]));
    }

    #[test]
    fn a_topic_without_metadata_is_assigned_to_nobody() {
        let m = members(&[("a", &["t0", "missing"])]);
        let c = counts(&[("t0", 2)]);
        for assignor in [Assignor::Range, Assignor::RoundRobin] {
            assert_eq!(assignor.assign(&m, &c)["a"], parts(&[("t0", 0), ("t0", 1)]));
        }
    }

    /// The bytes Java's `ConsumerProtocol.serializeSubscription` writes at v0 for `["t0"]`: version
    /// 0, a one-entry array of `t0`, and null user data.
    #[test]
    fn a_v0_subscription_has_the_java_layout() {
        let bytes = encode_subscription(&["t0".to_string()]);
        assert_eq!(&bytes[..], &[0, 0, 0, 0, 0, 1, 0, 2, b't', b'0', 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(decode_subscription(bytes).unwrap(), vec!["t0".to_string()]);
    }

    /// A Java 4.x member writes v3 (owned partitions, generation, rack); its topics read back,
    /// and a version above 3 reads as 3.
    #[test]
    fn subscriptions_up_to_v3_and_beyond_read_their_topics() {
        for written in [1i16, 2, 3] {
            let body = ConsumerProtocolSubscription::default()
                .with_topics(vec![StrBytes::from_static_str("t0")])
                .with_generation_id(4);
            let bytes = prefixed(written, |b| body.encode(b, written));
            assert_eq!(decode_subscription(bytes).unwrap(), vec!["t0".to_string()], "v{written}");
        }
        let body = ConsumerProtocolSubscription::default()
            .with_topics(vec![StrBytes::from_static_str("t0")]);
        let mut bytes = BytesMut::from(&prefixed(3, |b| body.encode(b, 3))[..]);
        bytes[..2].copy_from_slice(&7i16.to_be_bytes());
        assert_eq!(decode_subscription(bytes.freeze()).unwrap(), vec!["t0".to_string()]);
        assert!(decode_subscription(Bytes::from_static(&[0xff, 0xff])).is_err());
    }

    #[test]
    fn an_assignment_round_trips_and_an_empty_one_is_no_partitions() {
        let want = parts(&[("a", 0), ("a", 2), ("b", 1)]);
        assert_eq!(decode_assignment(encode_assignment(&want)).unwrap(), want);
        assert_eq!(decode_assignment(Bytes::new()).unwrap(), vec![]);
    }
}
